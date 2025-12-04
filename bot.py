import sys
import glob
import importlib
from pathlib import Path
from pyrogram import idle
import logging
import logging.config
import os
import asyncio
from datetime import date, datetime
import pytz
from aiohttp import web

# Scheduler imports
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone

# Index function import
from plugins.pm_filter import index_files

# Logging config
logging.config.fileConfig("logging.conf")
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("imdbpy").setLevel(logging.ERROR)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)


from pyrogram import __version__
from pyrogram.raw.all import layer
from database.ia_filterdb import Media
from database.users_chats_db import db
from info import *
from utils import temp
from Script import script
from plugins import web_server, check_expired_premium
from Jisshu.bot import JisshuBot
from Jisshu.util.keepalive import ping_server
from Jisshu.bot.clients import initialize_clients


# -------- AUTO INDEX SCHEDULER -------- #
IST = timezone("Asia/Kolkata")
scheduler = AsyncIOScheduler(timezone=IST)

DB_CHANNEL = int(os.environ.get("DB_CHANNEL", ""))

async def get_last_id():
    data = await db.col.find_one({"_id": "LAST_INDEX_ID"})
    return data.get("msg_id", 0) if data else 0

async def save_last_id(msg_id: int):
    await db.col.update_one(
        {"_id": "LAST_INDEX_ID"},
        {"$set": {"msg_id": msg_id}},
        upsert=True
    )

async def auto_index():
    print("🔍 Auto indexing started...")
    last_id = await get_last_id()
    new_last_id = last_id

    async for msg in JisshuBot.get_chat_history(DB_CHANNEL, offset_id=last_id, reverse=True):
        if msg.document or msg.video or msg.audio:
            await index_files(msg)
            new_last_id = msg.id

    await save_last_id(new_last_id)
    print(f"✨ Auto indexing finished up to message ID: {new_last_id}")

def schedule_index():
    scheduler.add_job(auto_index, "interval", minutes=2)  # 10 PM IST
    scheduler.start()
    print("⏱️ Auto Index Scheduler Started (10PM IST)")
# -------------------------------------- #


ppath = "plugins/*.py"
files = glob.glob(ppath)

loop = asyncio.get_event_loop()
pyrogram.utils.MIN_CHANNEL_ID = -1009147483647


async def Jisshu_start():
    print("\n")
    print("Credit - Telegram Pro Botz")
    bot_info = await JisshuBot.get_me()
    JisshuBot.username = bot_info.username
    await initialize_clients()

    for name in files:
        with open(name) as a:
            patt = Path(a.name)
            plugin_name = patt.stem.replace(".py", "")
            plugins_dir = Path(f"plugins/{plugin_name}.py")
            import_path = "plugins.{}".format(plugin_name)
            spec = importlib.util.spec_from_file_location(import_path, plugins_dir)
            load = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(load)
            sys.modules["plugins." + plugin_name] = load
            print("JisshuBot Imported => " + plugin_name)
    
    if ON_HEROKU:
        asyncio.create_task(ping_server())
    
    b_users, b_chats = await db.get_banned()
    temp.BANNED_USERS = b_users
    temp.BANNED_CHATS = b_chats
    
    await Media.ensure_indexes()
    me = await JisshuBot.get_me()
    temp.ME = me.id
    temp.U_NAME = me.username
    temp.B_NAME = me.first_name
    temp.B_LINK = me.mention
    
    JisshuBot.username = "@" + me.username
    JisshuBot.loop.create_task(check_expired_premium(JisshuBot))
    
    logging.info(
        f"{me.first_name} with for Pyrogram v{__version__} (Layer {layer}) started on {me.username}."
    )
    logging.info(script.LOGO)
    
    tz = pytz.timezone("Asia/Kolkata")
    today = date.today()
    now = datetime.now(tz)
    time = now.strftime("%H:%M:%S %p")
    
    await JisshuBot.send_message(
        chat_id=LOG_CHANNEL, text=script.RESTART_TXT.format(me.mention, today, time)
    )
    await JisshuBot.send_message(
        chat_id=SUPPORT_GROUP, text=f"<b>{me.mention} restarted 🤖</b>"
    )
    
    app = web.AppRunner(await web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, PORT).start()
    await idle()


if __name__ == "__main__":
    try:
        loop.run_until_complete(Jisshu_start())
        schedule_index()  # Scheduler starts AFTER bot fully online
    except KeyboardInterrupt:
        logging.info("Service Stopped Bye 👋")
