# src/app.py
import os
import asyncio
import traceback
import inspect
from typing import Optional

from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# локальные импорты делаем в startup чтобы избежать циклических импортов
# from .telegram_client import TelegramClient
# from .workers.job_manager import JobManager
from .db import init_db

load_dotenv()

TELEGRAM_WEBHOOK_SECRET = os.getenv('TELEGRAM_WEBHOOK_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

if not TELEGRAM_WEBHOOK_SECRET or not TELEGRAM_BOT_TOKEN:
    raise RuntimeError('Please set TELEGRAM_WEBHOOK_SECRET and TELEGRAM_BOT_TOKEN in env')

app = FastAPI(title="Configs service")

# --- Background task scheduler with robust exception handling ---
_background_tasks = set()

def schedule_task(coro):
    """
    Schedule coro as a background task and attach a done-callback that
    logs exceptions and removes the task from the active set.
    """
    if not asyncio.iscoroutine(coro):
        # if user passed a function, call it (but prefer passing coroutine)
        try:
            coro = coro()
        except Exception as e:
            print("schedule_task: failed to call provided callable:", e)
            raise

    task = asyncio.create_task(coro)
    _background_tasks.add(task)

    def _cb(t):
        try:
            _background_tasks.discard(t)
            exc = None
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                # task was cancelled — ignore
                return
            except Exception as e:
                # couldn't get exception object
                print("schedule_task callback: problem reading exception:", e)
            if exc is not None:
                print("Background task exception:", repr(exc))
                # detailed traceback logging
                tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                print(tb)
        except Exception as e:
            print("Error in task callback:", e, traceback.format_exc())

    task.add_done_callback(_cb)
    return task

# --- Install a global exception handler for the loop to avoid unhandled futures crashing the process ---
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
        # if setting fails, continue silently
        pass

# Placeholders for singletons that will be created on startup
app.state.tg_client = None
app.state.job_manager = None

# --- Startup: init DB, telegram client and job manager safely ---
@app.on_event('startup')
async def startup():
    # 1) init DB
    try:
        await init_db()
        print("DB initialized")
    except Exception:
        print("DB init failed:", traceback.format_exc())

    # 2) lazy import TelegramClient and JobManager to avoid import cycles
    try:
        from .telegram_client import TelegramClient
        from .workers.job_manager import JobManager
    except Exception as e:
        print("Failed to import TelegramClient/JobManager on startup:", e, traceback.format_exc())
        return

    # 3) instantiate TelegramClient (supporting both sync and async init patterns)
    try:
        tg = TelegramClient(bot_token=TELEGRAM_BOT_TOKEN)
        # if TelegramClient has an async init/start method, call it
        init_maybe = None
        for candidate in ('init', 'start', 'connect'):
            init_maybe = getattr(tg, candidate, None)
            if callable(init_maybe):
                try:
                    res = init_maybe()
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    print(f"TelegramClient.{candidate}() raised:", traceback.format_exc())
                break
        app.state.tg_client = tg
        print("TelegramClient initialized")
    except Exception:
        print("Failed to initialize TelegramClient:", traceback.format_exc())
        app.state.tg_client = None

    # 4) create job manager singleton
    try:
        if app.state.tg_client is not None:
            job_manager = JobManager(app.state.tg_client)
            app.state.job_manager = job_manager
            print("JobManager initialized")
        else:
            print("JobManager not initialized because tg_client is None")
    except Exception:
        print("Failed to initialize JobManager:", traceback.format_exc())
        app.state.job_manager = None

# --- Shutdown: close sessions and cancel background tasks gracefully ---
@app.on_event('shutdown')
async def shutdown():
    # try to close telegram client session if exists
    try:
        tg = getattr(app.state, "tg_client", None)
        if tg is not None:
            # attempt several common close patterns
            closed = False
            for attr in ("close", "session_close", "shutdown"):
                fn = getattr(tg, attr, None)
                if callable(fn):
                    try:
                        res = fn()
                        if inspect.isawaitable(res):
                            await res
                        closed = True
                        break
                    except Exception:
                        # continue trying other methods
                        print(f"Error while calling tg.{attr}()", traceback.format_exc())
            # attempt direct session close if available
            sess = getattr(tg, "session", None)
            if sess is not None:
                try:
                    res = getattr(sess, "close", None)
                    if callable(res):
                        maybe = res()
                        if inspect.isawaitable(maybe):
                            await maybe
                except Exception:
                    print("Error closing tg.session:", traceback.format_exc())
            if closed:
                print("Telegram client closed")
    except Exception:
        print("Error during shutdown tg cleanup:", traceback.format_exc())

    # cancel background tasks (allow them short time to finish)
    try:
        tasks = list(_background_tasks)
        if tasks:
            for t in tasks:
                try:
                    t.cancel()
                except Exception:
                    pass
            # give tasks a short grace period to finish cancellations
            await asyncio.sleep(0.2)
    except Exception:
        print("Error while cancelling background tasks:", traceback.format_exc())

# --- Health endpoint ---
@app.get('/healthz')
async def health():
    return JSONResponse({'status':'ok'})

# --- Webhook handler ---
@app.post('/webhook/{secret}')
async def telegram_webhook(request: Request, secret: str, x_telegram_secret: Optional[str] = Header(None, alias="X-Telegram-Bot-Api-Secret-Token")):
    """
    Secure webhook endpoint.
    - Validates secret (both in path and in X-Telegram-Bot-Api-Secret-Token header)
    - Parses JSON; returns 400 on invalid JSON
    - Delegates processing to JobManager.handle_update via schedule_task (non-blocking)
    - Returns 200 quickly so Telegram won't retry indefinitely
    """

    # 1) validate secret
    try:
        if TELEGRAM_WEBHOOK_SECRET:
            if secret != TELEGRAM_WEBHOOK_SECRET or x_telegram_secret != TELEGRAM_WEBHOOK_SECRET:
                # not authorized
                return JSONResponse(status_code=401, content={"ok": False, "error": "invalid secret"})
    except Exception:
        print("Secret check failure:", traceback.format_exc())
        return JSONResponse(status_code=401, content={"ok": False, "error": "secret check error"})

    # 2) parse JSON body
    try:
        update = await request.json()
    except Exception:
        # invalid payload
        print("Webhook: invalid JSON payload:", traceback.format_exc())
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid json"})

    # 3) schedule background processing
    try:
        jm = getattr(app.state, "job_manager", None)
        if jm is None:
            # lazy fallback: try to create job_manager on the fly (best-effort)
            try:
                from .telegram_client import TelegramClient
                from .workers.job_manager import JobManager
                tg = getattr(app.state, "tg_client", None)
                if tg is None:
                    tg = TelegramClient(bot_token=TELEGRAM_BOT_TOKEN)
                    init_maybe = getattr(tg, "init", None)
                    if callable(init_maybe):
                        maybe = init_maybe()
                        if inspect.isawaitable(maybe):
                            await maybe
                    app.state.tg_client = tg
                jm = JobManager(app.state.tg_client)
                app.state.job_manager = jm
                print("Webhook: lazily created JobManager")
            except Exception:
                print("Webhook: failed to lazily init JobManager:", traceback.format_exc())
                # schedule a simple logger task and return 200 to avoid repeated deliveries
                async def _log_and_return():
                    print("Webhook: received update but job manager missing; logging update summary")
                try:
                    schedule_task(_log_and_return())
                except Exception:
                    print("Webhook: failed to schedule fallback logger task")
                return JSONResponse(status_code=200, content={"ok": True})

        # schedule the actual handling (non-blocking)
        try:
            schedule_task(jm.handle_update(update))
        except Exception:
            print("Webhook: scheduling jm.handle_update failed:", traceback.format_exc())
            # don't return 500; return 200 so Telegram won't keep retrying
            return JSONResponse(status_code=200, content={"ok": True})
    except Exception:
        print("Webhook: unexpected failure:", traceback.format_exc())
        return JSONResponse(status_code=200, content={"ok": True})

    # Return success quickly
    return JSONResponse(status_code=200, content={"ok": True})
