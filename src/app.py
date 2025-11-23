# src/app.py
import os
import asyncio
import traceback
import inspect
from typing import Optional, Any, Dict

from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# local DB init (keeps original behaviour)
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
    Accepts either a coroutine or a callable that returns a coroutine.
    """
    try:
        if not asyncio.iscoroutine(coro):
            # if user passed a callable that returns coroutine, call it
            if callable(coro):
                maybe = coro()
                if inspect.isawaitable(maybe):
                    coroutine = maybe
                else:
                    # if callable returned non-awaitable, wrap it
                    async def _wrap(): return maybe
                    coroutine = _wrap()
            else:
                # not coroutine nor callable
                raise RuntimeError("schedule_task expects coroutine or callable returning coroutine")
        else:
            coroutine = coro
    except Exception as e:
        print("schedule_task: failed to prepare coroutine:", e, traceback.format_exc())
        raise

    task = asyncio.create_task(coroutine)
    _background_tasks.add(task)

    def _cb(t):
        try:
            _background_tasks.discard(t)
            exc = None
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                return
            except Exception as e:
                print("schedule_task callback: error getting exception:", e)
            if exc is not None:
                print("Background task exception:", repr(exc))
                print("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
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
        pass

# placeholders for singletons
app.state.tg_client = None
app.state.job_manager = None

# --- Startup: init DB, telegram client and job manager safely ---
@app.on_event('startup')
async def startup():
    # init DB (best-effort)
    try:
        await init_db()
        print("DB initialized")
    except Exception:
        print("DB init failed:", traceback.format_exc())

    # import TelegramClient and JobManager lazily to avoid import cycles
    try:
        from .telegram_client import TelegramClient
        from .workers.job_manager import JobManager
    except Exception as e:
        print("Failed to import TelegramClient or JobManager on startup:", e, traceback.format_exc())
        return

    # instantiate telegram client
    try:
        tg = TelegramClient(bot_token=TELEGRAM_BOT_TOKEN)
        # if the client requires async init/start, call it if present
        for candidate in ('init', 'start', 'connect'):
            fn = getattr(tg, candidate, None)
            if callable(fn):
                try:
                    res = fn()
                    if inspect.isawaitable(res):
                        await res
                except Exception:
                    print(f"TelegramClient.{candidate}() raised:", traceback.format_exc())
                break
        app.state.tg_client = tg
        print("TelegramClient initialized")
    except Exception:
        print("Failed to initialize TelegramClient:", traceback.format_exc())
        app.state.tg_client = None

    # create job manager
    try:
        if app.state.tg_client is not None:
            job_manager = JobManager(app.state.tg_client)
            app.state.job_manager = job_manager
            print("JobManager initialized")
        else:
            print("JobManager not created because tg_client is None")
    except Exception:
        print("Failed to initialize JobManager:", traceback.format_exc())
        app.state.job_manager = None

# --- Shutdown: close sessions and cancel background tasks gracefully ---
@app.on_event('shutdown')
async def shutdown():
    # try closing telegram client/session
    try:
        tg = getattr(app.state, "tg_client", None)
        if tg is not None:
            closed = False
            for attr in ("close", "shutdown", "session_close", "stop"):
                fn = getattr(tg, attr, None)
                if callable(fn):
                    try:
                        res = fn()
                        if inspect.isawaitable(res):
                            await res
                        closed = True
                        break
                    except Exception:
                        print(f"Error calling tg.{attr}():", traceback.format_exc())
            sess = getattr(tg, "session", None)
            if sess is not None:
                try:
                    c = getattr(sess, "close", None)
                    if callable(c):
                        maybe = c()
                        if inspect.isawaitable(maybe):
                            await maybe
                except Exception:
                    print("Error closing tg.session:", traceback.format_exc())
            if closed:
                print("Telegram client closed")
    except Exception:
        print("Error during shutdown tg cleanup:", traceback.format_exc())

    # cancel background tasks (give short grace period)
    try:
        tasks = list(_background_tasks)
        if tasks:
            for t in tasks:
                try:
                    t.cancel()
                except Exception:
                    pass
            await asyncio.sleep(0.2)
    except Exception:
        print("Error while cancelling background tasks:", traceback.format_exc())

# --- Health endpoint ---
@app.get('/healthz')
async def health():
    return JSONResponse({'status':'ok'})

# helper to extract chat_id/message_id from update for fallback
def extract_chat_info(update: Dict[str, Any]) -> Dict[str, Optional[int]]:
    try:
        if not isinstance(update, dict):
            return {'chat_id': None, 'message_id': None}
        if 'message' in update and isinstance(update['message'], dict):
            msg = update['message']
            chat = msg.get('chat', {})
            return {'chat_id': chat.get('id'), 'message_id': msg.get('message_id')}
        if 'callback_query' in update and isinstance(update['callback_query'], dict):
            cq = update['callback_query']
            if 'message' in cq and isinstance(cq['message'], dict):
                msg = cq['message']
                chat = msg.get('chat', {})
                return {'chat_id': chat.get('id'), 'message_id': msg.get('message_id')}
        # other update types can be added here
    except Exception:
        print("extract_chat_info error:", traceback.format_exc())
    return {'chat_id': None, 'message_id': None}

# --- Webhook handler with robust fallback if job_manager lacks handle_update ---
@app.post('/webhook/{secret}')
async def telegram_webhook(request: Request, secret: str, x_telegram_secret: Optional[str] = Header(None, alias="X-Telegram-Bot-Api-Secret-Token")):
    # validate secret (both path and header must match)
    try:
        if TELEGRAM_WEBHOOK_SECRET:
            if secret != TELEGRAM_WEBHOOK_SECRET or x_telegram_secret != TELEGRAM_WEBHOOK_SECRET:
                return JSONResponse(status_code=401, content={"ok": False, "error": "invalid secret"})
    except Exception:
        print("Secret check failure:", traceback.format_exc())
        return JSONResponse(status_code=401, content={"ok": False, "error": "secret check error"})

    # parse json body
    try:
        update = await request.json()
    except Exception:
        print("Webhook: invalid JSON payload:", traceback.format_exc())
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid json"})

    # schedule background processing with graceful fallback
    try:
        jm = getattr(app.state, "job_manager", None)
        if jm is None:
            # try lazy init (best-effort) if job_manager missing
            try:
                from .telegram_client import TelegramClient
                from .workers.job_manager import JobManager
                tg = getattr(app.state, "tg_client", None)
                if tg is None:
                    tg = TelegramClient(bot_token=TELEGRAM_BOT_TOKEN)
                    # attempt to call init methods if present
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
                # schedule a logger task and return 200 to avoid repeated deliveries
                async def _log_update():
                    print("Webhook: received update but job_manager missing; update keys:", list(update.keys())[:8])
                try:
                    schedule_task(_log_update())
                except Exception:
                    print("Webhook: failed to schedule fallback logger task")
                return JSONResponse(status_code=200, content={"ok": True})

        # if job_manager has handle_update — use it (preferred)
        if hasattr(jm, "handle_update") and callable(getattr(jm, "handle_update")):
            try:
                schedule_task(jm.handle_update(update))
            except Exception:
                print("Webhook: scheduling jm.handle_update failed:", traceback.format_exc())
                return JSONResponse(status_code=200, content={"ok": True})
            return JSONResponse(status_code=200, content={"ok": True})

        # fallback: if job_manager implements run_collection(chat_id, edit_chat=None, edit_msg=None), call it with extracted chat_id
        chat_info = extract_chat_info(update)
        chat_id = chat_info.get('chat_id')

        if hasattr(jm, "run_collection") and callable(getattr(jm, "run_collection")) and chat_id is not None:
            try:
                # call run_collection(chat_id) in background
                schedule_task(jm.run_collection(chat_id))
            except Exception:
                print("Webhook: scheduling jm.run_collection failed:", traceback.format_exc())
                # log and return 200
            return JSONResponse(status_code=200, content={"ok": True})

        # last resort: try a generic handler names that might exist
        for alt in ("process_update", "handle", "process", "on_update"):
            if hasattr(jm, alt) and callable(getattr(jm, alt)):
                try:
                    schedule_task(getattr(jm, alt)(update))
                except Exception:
                    print(f"Webhook: scheduling jm.{alt} failed:", traceback.format_exc())
                return JSONResponse(status_code=200, content={"ok": True})

        # nothing to handle: schedule lightweight logger and return ok to Telegram
        async def _log_unknown():
            print("Webhook: job_manager lacks handler methods; update keys:", list(update.keys())[:12])
        try:
            schedule_task(_log_unknown())
        except Exception:
            print("Webhook: failed to schedule logger for unknown handler")
        return JSONResponse(status_code=200, content={"ok": True})

    except Exception:
        print("Webhook: unexpected failure:", traceback.format_exc())
        # return 200 to acknowledge receipt, avoid retries
        return JSONResponse(status_code=200, content={"ok": True})
