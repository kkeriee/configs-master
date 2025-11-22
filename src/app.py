import os
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from .telegram_client import TelegramClient
from .workers.job_manager import JobManager
from .db import get_engine, init_db

load_dotenv()
TELEGRAM_WEBHOOK_SECRET = os.getenv('TELEGRAM_WEBHOOK_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

if not TELEGRAM_WEBHOOK_SECRET or not TELEGRAM_BOT_TOKEN:
    raise RuntimeError('Please set TELEGRAM_WEBHOOK_SECRET and TELEGRAM_BOT_TOKEN in env')

app = FastAPI()
tg = TelegramClient(bot_token=TELEGRAM_BOT_TOKEN)
job_manager = JobManager(tg)

@app.on_event('startup')
async def startup():
    # init DB
    await init_db()

@app.on_event('shutdown')
async def shutdown():
    # close aiohttp session
    try:
        await tg.session.close()
    except Exception:
        pass

@app.get('/healthz')
async def health():
    return JSONResponse({'status':'ok'})

@app.post(f'/webhook/{TELEGRAM_WEBHOOK_SECRET}')
async def telegram_webhook(request: Request):
    data = await request.json()
    # spawn worker to avoid blocking
    try:
        asyncio.create_task(job_manager.handle_update(data))
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)
    return JSONResponse({'ok': True})
