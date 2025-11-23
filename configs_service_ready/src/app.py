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

# Helper to schedule background tasks safely
def schedule_task(coro):
    """Schedule coro as a task and attach a done callback to log exceptions."""
    task = asyncio.create_task(coro)
    def _cb(fut):
        try:
            exc = fut.exception()
            if exc is not None:
                print('Background task exception:', repr(exc))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print('Error in task callback:', e)
    task.add_done_callback(_cb)
    return task

# Install a global exception handler on the event loop to avoid unhandled futures crashing the process
@app.on_event('startup')
async def _install_loop_exception_handler():
    loop = asyncio.get_event_loop()
    def _exc_handler(loop, context):
        try:
            msg = context.get('message') or str(context.get('exception'))
            print('Loop exception:', msg)
        except Exception:
            print('Loop exception (unable to format)')
    try:
        loop.set_exception_handler(_exc_handler)
    except Exception:
        pass

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
        schedule_task(job_manager.handle_update(data))
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)
    return JSONResponse({'ok': True})
