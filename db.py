"""
db.py - MongoDB (async, via motor) helper functions for the video copy bot.

Collections:
  channels: {
      _id: <channel_id int, e.g. -100xxxxxxxxxx>,
      title: str,
      start_message_id: int,
      last_message_id: int,
      in_progress: bool,
      added_at: datetime
  }
  settings: {
      _id: "config",
      target_channel: int
  }
"""

import os
import datetime
import motor.motor_asyncio

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "video_copy_bot")

_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
_db = _client[DB_NAME]

channels_col = _db["channels"]
settings_col = _db["settings"]


async def add_channel(channel_id: int, title: str, start_message_id: int) -> bool:
    """Register a new channel. Returns False if it already exists."""
    existing = await channels_col.find_one({"_id": channel_id})
    if existing:
        return False
    await channels_col.insert_one(
        {
            "_id": channel_id,
            "title": title,
            "start_message_id": start_message_id,
            # last_message_id is set to (start-1) so that the first iteration
            # (which excludes ids <= last_message_id) includes start_message_id itself.
            "last_message_id": start_message_id - 1,
            "in_progress": False,
            "added_at": datetime.datetime.utcnow(),
        }
    )
    return True


async def get_all_channels():
    return await channels_col.find().to_list(length=None)


async def get_channel(channel_id: int):
    return await channels_col.find_one({"_id": channel_id})


async def remove_channel(channel_id: int) -> bool:
    result = await channels_col.delete_one({"_id": channel_id})
    return result.deleted_count > 0


async def update_last_message_id(channel_id: int, message_id: int):
    await channels_col.update_one(
        {"_id": channel_id}, {"$set": {"last_message_id": message_id}}
    )


async def set_in_progress(channel_id: int, value: bool):
    await channels_col.update_one(
        {"_id": channel_id}, {"$set": {"in_progress": value}}
    )
