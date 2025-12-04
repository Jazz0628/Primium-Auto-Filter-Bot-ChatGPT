import asyncio
import logging
import pytz
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, enums
from info import CHANNELS, LOG_CHANNEL
from database.ia_filterdb import save_file
from Jisshu.bot import JisshuBot

# Configure Logger
logger = logging.getLogger(__name__)

# Initialize Scheduler with IST Timezone
scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")

async def auto_nightly_index():
    """
    Runs automatically at 10 PM.
    Scans the last 500 messages of configured CHANNELS to ensure files are indexed.
    """
    # Notify Log Channel that indexing started
    start_msg = f"⏰ <b>Auto-Index Started</b>\nScanning last 500 messages in {len(CHANNELS)} channel(s)..."
    try:
        await JisshuBot.send_message(LOG_CHANNEL, start_msg)
    except Exception as e:
        logger.error(f"Failed to send start log: {e}")

    total_files = 0
    duplicate = 0
    errors = 0
    
    for channel_id in CHANNELS:
        try:
            # We assume daily uploads won't exceed 500 files. 
            # Limiting to 500 prevents the bot from scanning the whole DB every night (which causes lag).
            async for message in JisshuBot.get_chat_history(chat_id=int(channel_id), limit=500):
                
                if message.media:
                    # Filter for Video or Document
                    if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT]:
                        continue
                    
                    media = getattr(message, message.media.value, None)
                    if not media:
                        continue
                        
                    # Filter for specific mime types (mp4/mkv)
                    if getattr(media, "mime_type", "") not in ["video/mp4", "video/x-matroska"]:
                        continue

                    # Attempt to save to DB
                    media.caption = message.caption
                    sts = await save_file(media)
                    
                    if sts == "suc":
                        total_files += 1
                    elif sts == "dup":
                        duplicate += 1
                    elif sts == "err":
                        errors += 1
                        
        except Exception as e:
            logger.error(f"Error auto-indexing channel {channel_id}: {e}")
            continue

    # Notify Log Channel that indexing is done
    end_msg = (
        f"✅ <b>Auto-Index Completed</b>\n\n"
        f"🆕 New Files Added: {total_files}\n"
        f"♻️ Duplicates Skipped: {duplicate}\n"
        f"⚠️ Errors: {errors}\n\n"
        f"<i>Next scan scheduled for tomorrow at 10:00 PM.</i>"
    )
    try:
        await JisshuBot.send_message(LOG_CHANNEL, end_msg)
    except Exception as e:
        logger.error(f"Failed to send end log: {e}")

# Schedule the job to run every day at 22:00 (10 PM) IST
scheduler.add_job(auto_nightly_index, "cron", hour=22, minute=0)

# Start the scheduler
scheduler.start()
