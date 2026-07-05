"""
bot.py - Telegram multi-feature video copy bot.

Two Telethon clients are used:
  user_client  -> logged in via SESSION_STRING (a real user account).
                  Used to read message history from source channels and to
                  copy ("send by file reference", no download needed) videos
                  into the target channel. This account must be a MEMBER
                  (or admin, for the target channel) of every channel involved.
  bot_client   -> logged in via BOT_TOKEN.
                  Used only to talk to the owner: commands + progress updates.

Flow:
  1. Target channel is fixed via the TARGET_CHANNEL_ID env variable.
  2. Owner forwards any message from a source channel to this bot.
  3. Bot registers that channel (starting at the forwarded message's id) and
     immediately starts copying all qualifying videos (size > 10MB) from that
     point onward, in order, skipping every other media type.
  4. Every day at 00:00 (Asia/Kolkata by default) the bot automatically
     re-checks all registered channels for new messages and copies any new
     qualifying videos the same way.
  5. Progress is posted to the owner every 10 processed messages, and there
     is a 10 second gap after every successfully copied file.
"""

import os
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
# Telethon logs a lot of harmless INFO noise (reconnects, "got difference for
# updates" after catching up on missed events, etc.) - keep only warnings+.
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

# in-memory guard so the same channel is never processed twice at once
_locks = {}


def get_lock(channel_id: int) -> asyncio.Lock:
    if channel_id not in _locks:
        _locks[channel_id] = asyncio.Lock()
    return _locks[channel_id]


def is_owner(event) -> bool:
    return event.sender_id == OWNER_ID


def is_qualifying_video(message) -> bool:
    """Only real video files over 10MB qualify. Everything else is skipped."""
    if not message.video:
        return False
    size = message.file.size if message.file else 0
    return size > VIDEO_SIZE_LIMIT_BYTES


def get_file_key(message):
    """Telegram's own document id uniquely identifies identical file content
    (Telegram dedups uploads server-side), so we use it as a lightweight
    'file hash' for duplicate detection without downloading the video."""
    if message.document:
        return str(message.document.id)
    return None


def build_progress_text(channel_title, start_id, copied, failed, skipped, duplicate, finished=False):
    text = (
        f"Copying Started From {channel_title}\n"
        f"Starting Message ID : {start_id}\n"
        f"Total Copied : {copied}\n"
        f"Failed : {failed}\n"
        f"Skipped : {skipped}\n"
        f"Duplicate : {duplicate}"
    )
    if finished:
        text += "\n\n🏁 Finished"
    return text


# ---------------------------------------------------------------------------
# Core copy logic
# ---------------------------------------------------------------------------
async def process_channel(channel_doc: dict, progress_chat_id: int):
    """Copies all qualifying videos from a channel, starting right after
    its stored last_message_id, up to the newest message. Posts ONE
    progress message that gets edited every 10 successful copies."""
    channel_id = channel_doc["_id"]
    title = channel_doc.get("title", str(channel_id))
    lock = get_lock(channel_id)

    if lock.locked():
        logger.info("Channel %s already being processed, skipping trigger.", channel_id)
        return

    async with lock:
        target_id = TARGET_CHANNEL_ID

        fresh = await db.get_channel(channel_id)
        last_id = fresh.get("last_message_id", 0) if fresh else channel_doc.get("last_message_id", 0)
        start_id = last_id + 1

        await db.set_in_progress(channel_id, True)
        copied = failed = skipped = duplicate = 0
        progress_msg = None

        try:
            try:
                source_entity = await user_client.get_entity(channel_id)
                target_entity = await user_client.get_entity(target_id)
            except Exception as e:
                await bot_client.send_message(
                    progress_chat_id,
                    f"❌ '{title}' access nahi ho paya (user account member hai check karo): {e}",
                )
                return

            progress_msg = await bot_client.send_message(
                progress_chat_id,
                build_progress_text(title, start_id, copied, failed, skipped, duplicate),
            )

            async for message in user_client.iter_messages(
                source_entity, min_id=last_id, reverse=True
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

                last_id = message.id
                await db.update_last_message_id(channel_id, last_id)

                if copied > 0 and copied % PROGRESS_EVERY == 0:
                    text = build_progress_text(title, start_id, copied, failed, skipped, duplicate)
                    try:
                        await bot_client.edit_message(progress_chat_id, progress_msg, text)
                    except Exception as e:
                        logger.warning("Progress edit failed: %s", e)

            final_text = build_progress_text(
                title, start_id, copied, failed, skipped, duplicate, finished=True
            )
            try:
                await bot_client.edit_message(progress_chat_id, progress_msg, final_text)
            except Exception:
                await bot_client.send_message(progress_chat_id, final_text)

        finally:
            await db.set_in_progress(channel_id, False)


async def _copy_one(message, target_entity) -> bool:
    """Copies a single video message to target, preserving caption exactly.
    Uses the existing file reference (no download) for speed."""
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
            logger.error("Retry after FloodWait failed for msg %s: %s", message.id, e2)
            return False
    except Exception as e:
        logger.error("Copy failed for msg %s: %s", message.id, e)
        return False


async def daily_revisit():
    """Triggered every day at midnight: re-checks every tracked channel."""
    logger.info("Running scheduled daily revisit of all channels...")
    channels = await db.get_all_channels()
    for ch in channels:
        if ch.get("in_progress"):
            continue
        try:
            await process_channel(ch, OWNER_ID)
        except Exception as e:
            logger.error("Daily revisit failed for %s: %s", ch.get("_id"), e)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
@bot_client.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    if not is_owner(event):
        return
    text = (
        "🎬 **Video Copy Bot**\n\n"
        "**Commands:**\n"
        "`/start` - ye help message\n"
        "`/all_channels` - sabhi tracked source channels ki list (name + id)\n"
        "`/remove_channel <channel_id>` - channel ko tracking se hata do\n\n"
        "**Kaise use karein:**\n"
        "1️⃣ Target channel already `.env` me `TARGET_CHANNEL_ID` se set hai "
        "(is bot ka user account us channel me member/admin hona chahiye).\n"
        "2️⃣ Jis channel se video copy karni hai, wahan se jis message se "
        "shuru karna hai, wo message seedha is bot ko **forward** kar do.\n"
        "3️⃣ Bot khud us channel ko register kar lega aur us message id se "
        "copying shuru kar dega.\n"
        "4️⃣ Sirf **video files jinka size 10MB se zyada hai** copy hote hain; "
        "image, gif, document, audio - sab skip.\n"
        "5️⃣ Caption bilkul as-is rehta hai.\n"
        "6️⃣ Har din raat 12:00 baje (IST) bot khud sabhi channels check "
        "karega naye messages ke liye, aur automatically nayi videos copy "
        "kar dega.\n"
        "7️⃣ Har copy ke baad 10 second ka gap, aur har 10 successful copy "
        "ke baad ek hi progress message update hota hai.\n"
        "8️⃣ Forward kiya hua message channel-id/message-id nikalne ke baad "
        "khud delete ho jata hai.\n"
        "9️⃣ Duplicate videos (already copied) skip ho jati hain, file-hash "
        "check se."
    )
    await event.respond(text)


@bot_client.on(events.NewMessage(pattern="/all_channels"))
async def all_channels_handler(event):
    if not is_owner(event):
        return
    channels = await db.get_all_channels()
    if not channels:
        await event.respond("Abhi koi channel track nahi ho raha.")
        return
    lines = [
        f"• {c.get('title', 'Unknown')} — `{c['_id']}` (last_id: {c.get('last_message_id')})"
        for c in channels
    ]
    await event.respond("📋 **Tracked Channels:**\n\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern="/remove_channel"))
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
        await event.respond("❌ Invalid channel id.")
        return
    removed = await db.remove_channel(cid)
    await event.respond("✅ Channel removed." if removed else "❌ Ye channel id mili nahi.")


@bot_client.on(events.NewMessage(func=lambda e: e.message.fwd_from is not None))
async def forward_handler(event):
    """Registers a new channel when the owner forwards a message from it."""
    if not is_owner(event):
        return

    fwd = event.message.fwd_from
    if not fwd or fwd.channel_post is None or fwd.from_id is None:
        await event.respond(
            "⚠️ Ye forward kisi channel post jaisa nahi lag raha. "
            "Kisi channel se directly message forward karo."
        )
        return

    peer = fwd.from_id
    if not isinstance(peer, types.PeerChannel):
        await event.respond("⚠️ Sirf channel se forward kiya hua message support hai.")
        return

    channel_id = utils.get_peer_id(peer)  # normalizes to -100xxxxxxxxxx
    message_id = fwd.channel_post

    # We've extracted what we need (channel id + message id) from this
    # forwarded message, so clean it up from the chat.
    try:
        await event.message.delete()
    except Exception as e:
        logger.warning("Could not delete forwarded message: %s", e)

    existing = await db.get_channel(channel_id)
    if existing:
        await bot_client.send_message(event.chat_id, "ℹ️ Ye channel already tracked hai.")
        return

    try:
        entity = await user_client.get_entity(channel_id)
        title = getattr(entity, "title", str(channel_id))
    except Exception as e:
        await bot_client.send_message(
            event.chat_id,
            f"❌ User account (SESSION_STRING) is channel ka member nahi hai ya access nahi: {e}",
        )
        return

    await db.add_channel(channel_id, title, message_id)

    channel_doc = await db.get_channel(channel_id)
    asyncio.create_task(process_channel(channel_doc, event.chat_id))


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
async def _connect_user_client():
    """Connects the user client with a couple of retries. This helps with
    transient network errors during platform redeploys/restarts."""
    from telethon.errors import AuthKeyDuplicatedError

    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            await user_client.start()
            logger.info("user_client (SESSION_STRING) started.")
            return
        except AuthKeyDuplicatedError:
            logger.error(
                "AuthKeyDuplicatedError: is SESSION_STRING ka istemal ek se "
                "zyada jagah/IP se ho raha hai (ya pichla instance cleanly "
                "band nahi hua tha). Agar ye baar baar aa raha hai to naya "
                "fresh SESSION_STRING generate karke .env update karo."
            )
            if attempt == attempts:
                raise
            await asyncio.sleep(5)
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
    except Exception as e:
        logger.warning("Error disconnecting user_client: %s", e)
    try:
        if bot_client.is_connected():
            await bot_client.disconnect()
    except Exception as e:
        logger.warning("Error disconnecting bot_client: %s", e)


async def main():
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Shutdown signal received.")
        loop.create_task(_shutdown())
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # add_signal_handler isn't available on some platforms (e.g. Windows)
            pass

    await _connect_user_client()

    await bot_client.start(bot_token=BOT_TOKEN)
    logger.info("bot_client (BOT_TOKEN) started.")

    try:
        target_entity = await user_client.get_entity(TARGET_CHANNEL_ID)
        logger.info("Target channel OK: %s", getattr(target_entity, "title", TARGET_CHANNEL_ID))
    except Exception as e:
        logger.error(
            "Could not access TARGET_CHANNEL_ID (%s). Make sure the user "
            "account is a member/admin there. Error: %s", TARGET_CHANNEL_ID, e,
        )

    await start_health_server()
    asyncio.create_task(self_ping_loop())

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(lambda: asyncio.create_task(daily_revisit()), "cron", hour=0, minute=0)
    scheduler.start()
    logger.info("Scheduler started - daily revisit at 00:00 %s", TIMEZONE)

    logger.info("Bot is up and running.")
    disconnected_task = asyncio.create_task(bot_client.run_until_disconnected())
    stop_task = asyncio.create_task(stop_event.wait())
    await asyncio.wait(
        [disconnected_task, stop_task], return_when=asyncio.FIRST_COMPLETED
    )


if __name__ == "__main__":
    asyncio.run(main())
