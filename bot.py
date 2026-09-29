"""
bot.py - Telegram multi-feature video copy & channel monitoring bot.

Two Telethon clients:
  user_client  -> logged in via SESSION_STRING (Real user account, member of target & source channels)
  bot_client   -> logged in via BOT_TOKEN (Owner commands and updates)

Features & Workflows:
  1. /add_channel <message_link>
     - Extract channel_id & start message_id.
     - Saves channel into database for live monitoring & 24h cron scan.
  2. /copy <message_link>
     - Instant copying task from the given message_id up to the latest post.
  3. Real-Time Monitor
     - Monitors only /add_channel registered channels in real-time.
  4. Daily 24h Scan
     - Sequentially processes channels one-by-one starting strictly after the saved message_id.
"""

import os
import re
import asyncio
import logging
import signal

from dotenv import load_dotenv
from telethon import TelegramClient, events, utils, types
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import db
from health_check import start_health_server, self_ping_loop

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("bot")
logging.getLogger("telethon").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))
TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")

VIDEO_SIZE_LIMIT_BYTES = 10 * 1024 * 1024  # 10 MB
COPY_GAP_SECONDS = 10
PROGRESS_EVERY = 10

user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot_client = TelegramClient("bot_session", API_ID, API_HASH)

_locks = {}
_running_tasks = {}


def get_lock(channel_id: int) -> asyncio.Lock:
    if channel_id not in _locks:
        _locks[channel_id] = asyncio.Lock()
    return _locks[channel_id]


def is_owner(event) -> bool:
    return event.sender_id == OWNER_ID


async def ensure_user_connected():
    """Ensures user_client is connected; attempts reconnection if dropped."""
    if not user_client.is_connected():
        logger.warning("user_client disconnected. Attempting reconnection...")
        try:
            await user_client.connect()
            if not await user_client.is_user_authorized():
                await user_client.start()
            logger.info("user_client reconnected successfully.")
        except Exception as e:
            logger.error("Failed to reconnect user_client: %s", e)


def parse_telegram_link(link: str):
    """
    Parses public and private Telegram message links.
    Examples:
      - Private: https://t.me/c/3888815428/843  -> (-1003888815428, 843)
      - Public:  https://t.me/channelname/843    -> ('channelname', 843)
    """
    private_pattern = r"t\.me/c/(\d+)/(\d+)"
    public_pattern = r"t\.me/([^/]+)/(\d+)"

    priv_match = re.search(private_pattern, link)
    if priv_match:
        raw_id = priv_match.group(1)
        cid = int(f"-100{raw_id}") if not raw_id.startswith("-100") else int(raw_id)
        msg_id = int(priv_match.group(2))
        return cid, msg_id

    pub_match = re.search(public_pattern, link)
    if pub_match:
        username = pub_match.group(1)
        msg_id = int(pub_match.group(2))
        return username, msg_id

    return None, None


def is_qualifying_video(message) -> bool:
    if not message or not message.video:
        return False
    size = message.file.size if message.file else 0
    return size > VIDEO_SIZE_LIMIT_BYTES


def get_file_key(message):
    if message and message.document:
        return str(message.document.id)
    return None


def build_progress_text(channel_title, start_id, copied, failed, skipped, duplicate, finished=False):
    text = (
        f"📊 **Copying Progress**\n"
        f"📌 **Channel:** {channel_title}\n"
        f"🆔 **Start Msg ID:** {start_id}\n\n"
        f"✅ Copied: {copied}\n"
        f"❌ Failed: {failed}\n"
        f"⏩ Skipped: {skipped}\n"
        f"♻️ Duplicate: {duplicate}"
    )
    if finished:
        text += "\n\n🏁 **Process Finished!**"
    return text


# ---------------------------------------------------------------------------
# Core Copy Logic
# ---------------------------------------------------------------------------
async def _copy_one(message, target_entity) -> bool:
    await ensure_user_connected()
    try:
        await user_client.send_file(
            target_entity,
            file=message.media,
            caption=message.message or "",
            formatting_entities=message.entities,
        )
        return True
    except FloodWaitError as e:
        logger.warning("FloodWait: sleeping %s seconds", e.seconds)
        await asyncio.sleep(e.seconds + 1)
        try:
            await user_client.send_file(
                target_entity,
                file=message.media,
                caption=message.message or "",
                formatting_entities=message.entities,
            )
            return True
        except Exception as e2:
            logger.error("Retry failed for msg %s: %s", message.id, e2)
            return False
    except (ConnectionError, OSError) as e:
        logger.warning("Connection lost on copy for msg %s: %s. Retrying...", message.id, e)
        await asyncio.sleep(3)
        await ensure_user_connected()
        try:
            await user_client.send_file(
                target_entity,
                file=message.media,
                caption=message.message or "",
                formatting_entities=message.entities,
            )
            return True
        except Exception as e3:
            logger.error("Retry after reconnect failed for msg %s: %s", message.id, e3)
            return False
    except Exception as e:
        logger.error("Copy failed for msg %s: %s", message.id, e)
        return False


async def process_channel_copy(channel_id: int, start_msg_id: int, progress_chat_id: int):
    """
    Copies qualifying videos sequentially from start_msg_id onwards to latest message.
    Used by /copy and daily automatic revisit scan.
    """
    lock = get_lock(channel_id)

    if lock.locked():
        logger.info("Channel %s is currently locked/busy.", channel_id)
        await bot_client.send_message(progress_chat_id, "⚠️ Is channel ka process pehle se chal raha hai.")
        return

    async with lock:
        await db.set_in_progress(channel_id, True)
        copied = failed = skipped = duplicate = 0
        progress_msg = None

        try:
            await ensure_user_connected()
            source_entity = await user_client.get_entity(channel_id)
            target_entity = await user_client.get_entity(TARGET_CHANNEL_ID)
            title = getattr(source_entity, "title", str(channel_id))
        except Exception as e:
            await bot_client.send_message(
                progress_chat_id,
                f"❌ Channel access fail: {e}",
            )
            await db.set_in_progress(channel_id, False)
            return

        progress_msg = await bot_client.send_message(
            progress_chat_id,
            build_progress_text(title, start_msg_id, copied, failed, skipped, duplicate),
        )

        last_scanned_id = start_msg_id - 1
        try:
            while True:
                try:
                    await ensure_user_connected()
                    async for message in user_client.iter_messages(
                        source_entity, min_id=last_scanned_id, reverse=True
                    ):
                        if is_qualifying_video(message):
                            file_key = get_file_key(message)
                            if file_key and await db.is_duplicate_file(file_key):
                                duplicate += 1
                            else:
                                success = await _copy_one(message, target_entity)
                                if success:
                                    copied += 1
                                    if file_key:
                                        await db.save_file_hash(file_key, channel_id, message.id)
                                    await asyncio.sleep(COPY_GAP_SECONDS)
                                else:
                                    failed += 1
                        else:
                            skipped += 1

                        last_scanned_id = message.id
                        await db.update_last_message_id(channel_id, last_scanned_id)

                        if (copied + failed + skipped + duplicate) % PROGRESS_EVERY == 0:
                            try:
                                await bot_client.edit_message(
                                    progress_chat_id,
                                    progress_msg,
                                    build_progress_text(title, start_msg_id, copied, failed, skipped, duplicate),
                                )
                            except Exception:
                                pass
                    break
                except (ConnectionError, OSError) as conn_err:
                    logger.warning("Disconnected inside iter_messages loop: %s. Reconnecting...", conn_err)
                    await asyncio.sleep(5)
                    await ensure_user_connected()

            final_text = build_progress_text(
                title, start_msg_id, copied, failed, skipped, duplicate, finished=True
            )
            try:
                await bot_client.edit_message(progress_chat_id, progress_msg, final_text)
            except Exception:
                await bot_client.send_message(progress_chat_id, final_text)

        except asyncio.CancelledError:
            cancel_text = (
                build_progress_text(title, start_msg_id, copied, failed, skipped, duplicate)
                + "\n\n🛑 **Process Cancelled!**"
            )
            try:
                if progress_msg:
                    await bot_client.edit_message(progress_chat_id, progress_msg, cancel_text)
            except Exception:
                pass
            raise

        finally:
            await db.set_in_progress(channel_id, False)


# ---------------------------------------------------------------------------
# Real-Time Monitoring
# ---------------------------------------------------------------------------
@user_client.on(events.NewMessage)
async def live_channel_monitor(event):
    """Monitors live incoming posts in registered channels."""
    channel_id = event.chat_id
    if not channel_id:
        return

    tracked_channels = await db.get_all_channels()
    channel_doc = next((c for c in tracked_channels if c["_id"] == channel_id), None)

    if not channel_doc:
        return

    message = event.message
    if message.id <= channel_doc.get("last_message_id", 0):
        return

    lock = get_lock(channel_id)
    async with lock:
        if is_qualifying_video(message):
            file_key = get_file_key(message)
            if file_key and await db.is_duplicate_file(file_key):
                logger.info("Realtime: Duplicate video skipped for %s (msg %s)", channel_id, message.id)
                await db.update_last_message_id(channel_id, message.id)
                return

            logger.info("Realtime: Copying new video from %s (msg %s)", channel_id, message.id)
            try:
                await ensure_user_connected()
                target_entity = await user_client.get_entity(TARGET_CHANNEL_ID)
                success = await _copy_one(message, target_entity)
                if success:
                    if file_key:
                        await db.save_file_hash(file_key, channel_id, message.id)
                    await db.update_last_message_id(channel_id, message.id)
                    
                    title = channel_doc.get("title", str(channel_id))
                    await bot_client.send_message(
                        OWNER_ID,
                        f"⚡ **Live Video Copied!**\n📌 **Channel:** {title}\n🆔 **Msg ID:** `{message.id}`"
                    )
            except Exception as e:
                logger.error("Live copy failed for channel %s: %s", channel_id, e)
        else:
            await db.update_last_message_id(channel_id, message.id)


# ---------------------------------------------------------------------------
# 24-Hour Sequential Cron Job
# ---------------------------------------------------------------------------
async def daily_revisit():
    """Scans tracked channels one-by-one every 24 hours."""
    logger.info("Starting daily 24-hour scan for all tracked channels sequentially...")
    channels = await db.get_all_channels()

    for ch in channels:
        cid = ch["_id"]
        if ch.get("in_progress"):
            continue

        last_id = ch.get("last_message_id", 0)
        start_id = last_id + 1

        try:
            logger.info("Scanning channel %s from msg ID %s", cid, start_id)
            task = asyncio.create_task(process_channel_copy(cid, start_id, OWNER_ID))
            _running_tasks[cid] = task
            await task
        except asyncio.CancelledError:
            logger.info("Daily scan cancelled for channel %s", cid)
        except Exception as e:
            logger.error("Daily scan failed for channel %s: %s", cid, e)
        finally:
            _running_tasks.pop(cid, None)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
@bot_client.on(events.NewMessage(pattern=r"^/start$"))
async def start_handler(event):
    if not is_owner(event):
        return
    text = (
        "🤖 **Video Copy & Monitor Bot**\n\n"
        "📌 **Commands:**\n"
        "• `/add_channel <link>` - Track & monitor new channel\n"
        "• `/copy <link>` - Copy channel videos from link message ID\n"
        "• `/all_channels` - View tracked channels list\n"
        "• `/remove_channel <id>` - Remove channel tracking\n"
        "• `/cancel` - Stop ongoing tasks"
    )
    await event.respond(text)


@bot_client.on(events.NewMessage(pattern=r"^/add_channel(?:\s+(.+))?"))
async def add_channel_handler(event):
    if not is_owner(event):
        return

    link = event.pattern_match.group(1)
    if not link:
        await event.respond("❌ Usage: `/add_channel <message_link>`")
        return

    target_channel, msg_id = parse_telegram_link(link)
    if not target_channel or not msg_id:
        await event.respond("❌ Invalid Telegram message link.")
        return

    try:
        await ensure_user_connected()
        entity = await user_client.get_entity(target_channel)
        channel_id = utils.get_peer_id(entity)
        title = getattr(entity, "title", str(channel_id))
    except Exception as e:
        await event.respond(f"❌ Access error: {e}")
        return

    await db.add_channel(channel_id, title, msg_id)
    await event.respond(
        f"✅ **Channel Registered!**\n\n"
        f"📌 **Title:** {title}\n"
        f"🆔 **Channel ID:** `{channel_id}`\n"
        f"🔢 **Start Msg ID:** `{msg_id}`"
    )


@bot_client.on(events.NewMessage(pattern=r"^/copy(?:\s+(.+))?"))
async def copy_handler(event):
    if not is_owner(event):
        return

    link = event.pattern_match.group(1)
    if not link:
        await event.respond("❌ Usage: `/copy <message_link>`")
        return

    target_channel, msg_id = parse_telegram_link(link)
    if not target_channel or not msg_id:
        await event.respond("❌ Invalid Telegram message link.")
        return

    try:
        await ensure_user_connected()
        entity = await user_client.get_entity(target_channel)
        channel_id = utils.get_peer_id(entity)
    except Exception as e:
        await event.respond(f"❌ Access error: {e}")
        return

    await event.respond(f"🚀 **Copying Started** from Msg ID `{msg_id}`...")
    
    task = asyncio.create_task(process_channel_copy(channel_id, msg_id, event.chat_id))
    _running_tasks[channel_id] = task

    def _cleanup(t):
        if _running_tasks.get(channel_id) is t:
            del _running_tasks[channel_id]

    task.add_done_callback(_cleanup)


@bot_client.on(events.NewMessage(pattern="/all_channels"))
async def all_channels_handler(event):
    if not is_owner(event):
        return
    channels = await db.get_all_channels()
    if not channels:
        await event.respond("Koi channel track nahi ho raha.")
        return
    lines = [
        f"• **{c.get('title', 'Unknown')}** — `{c['_id']}` (last_id: `{c.get('last_message_id')}`)"
        for c in channels
    ]
    await event.respond("📋 **Tracked Channels:**\n\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r"^/remove_channel(?:\s+(.+))?"))
async def remove_channel_handler(event):
    if not is_owner(event):
        return
    parts = event.raw_text.split()
    if len(parts) != 2:
        await event.respond("Usage: `/remove_channel <channel_id>`")
        return
    try:
        cid = int(parts[1])
    except ValueError:
        await event.respond("❌ Invalid channel ID.")
        return
    removed = await db.remove_channel(cid)
    if removed:
        task = _running_tasks.get(cid)
        if task:
            task.cancel()
    await event.respond("✅ Channel removed." if removed else "❌ Channel ID nahi mili.")


@bot_client.on(events.NewMessage(pattern="/cancel"))
async def cancel_handler(event):
    if not is_owner(event):
        return
    parts = event.raw_text.split()

    if len(parts) == 1:
        if not _running_tasks:
            await event.respond("Koi active running task nahi hai.")
            return
        count = 0
        for cid, task in list(_running_tasks.items()):
            task.cancel()
            count += 1
        await event.respond(f"🛑 {count} task(s) cancel kiye ja rahe hain...")
        return

    try:
        cid = int(parts[1])
    except ValueError:
        await event.respond("Usage: `/cancel` ya `/cancel <channel_id>`")
        return

    task = _running_tasks.get(cid)
    if not task:
        await event.respond("Is channel ka koi task active nahi hai.")
        return

    task.cancel()
    await event.respond("🛑 Process cancel kiya gaya.")


# ---------------------------------------------------------------------------
# Startup & Shutdown
# ---------------------------------------------------------------------------
async def _connect_user_client():
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            await user_client.start()
            logger.info("user_client started.")
            return
        except Exception as e:
            logger.warning("user_client start attempt %s/%s failed: %s", attempt, attempts, e)
            if attempt == attempts:
                raise
            await asyncio.sleep(5)


async def _shutdown():
    logger.info("Shutting down gracefully...")
    try:
        if user_client.is_connected():
            await user_client.disconnect()
        if bot_client.is_connected():
            await bot_client.disconnect()
    except Exception as e:
        logger.warning("Shutdown error: %s", e)


async def main():
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        loop.create_task(_shutdown())
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    await _connect_user_client()
    await bot_client.start(bot_token=BOT_TOKEN)

    try:
        target_entity = await user_client.get_entity(TARGET_CHANNEL_ID)
        logger.info("Target channel OK: %s", getattr(target_entity, "title", TARGET_CHANNEL_ID))
    except Exception as e:
        logger.error("Target channel error: %s", e)

    await start_health_server()
    asyncio.create_task(self_ping_loop())

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(lambda: asyncio.create_task(daily_revisit()), "cron", hour=0, minute=0)
    scheduler.start()

    logger.info("Bot is running...")
    disconnected_task = asyncio.create_task(bot_client.run_until_disconnected())
    stop_task = asyncio.create_task(stop_event.wait())
    await asyncio.wait([disconnected_task, stop_task], return_when=asyncio.FIRST_COMPLETED)


if __name__ == "__main__":
    asyncio.run(main())
