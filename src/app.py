import os
import asyncio
import traceback
import re
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import sqlalchemy as sa

from .telegram_client import TelegramClient
from .workers.job_manager import JobManager
from .db import get_engine, init_db
from .db.models import configs

load_dotenv()

TELEGRAM_WEBHOOK_SECRET = os.getenv('TELEGRAM_WEBHOOK_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

if not TELEGRAM_WEBHOOK_SECRET or not TELEGRAM_BOT_TOKEN:
    raise RuntimeError('Please set TELEGRAM_WEBHOOK_SECRET and TELEGRAM_BOT_TOKEN in env')

app = FastAPI(title="Configs service - interactive")

# singletons
tg_client: Optional[TelegramClient] = None
job_manager: Optional[JobManager] = None

# simple in-memory conversation state: chat_id -> state dict
# state = {'expecting': 'count', 'country': 'DE'}
chat_state: Dict[int, Dict[str, Any]] = {}

# helper for country flag conversions
def country_code_to_flag(cc: str) -> str:
    cc = (cc or "").upper()
    if len(cc) != 2:
        return cc
    return chr(ord(cc[0]) - ord('A') + 0x1F1E6) + chr(ord(cc[1]) - ord('A') + 0x1F1E6)

def flag_to_country_code(flag: str) -> Optional[str]:
    # match two regional indicators
    RIS_BASE = 0x1F1E6
    if not flag:
        return None
    # extract codepoints
    codepoints = [ord(ch) for ch in flag if ord(ch) >= RIS_BASE and ord(ch) <= RIS_BASE+26]
    if len(codepoints) < 2:
        return None
    try:
        a = chr(codepoints[0] - RIS_BASE + ord('A'))
        b = chr(codepoints[1] - RIS_BASE + ord('A'))
        return (a + b)
    except Exception:
        return None

async def send_country_list(chat_id: int):
    """Query DB for counts by country and send a message with flags and counts."""
    eng = get_engine()
    async with eng.connect() as conn:
        q = sa.select([configs.c.country, sa.func.count(configs.c.id).label('cnt')]).where(configs.c.ok==True).group_by(configs.c.country).order_by(sa.func.count(configs.c.id).desc())
        res = await conn.execute(q)
        rows = res.fetchall()
    if not rows:
        await tg_client.send_message(chat_id, "База пуста. Запустите новый поиск через «Новый поиск».")
        return
    lines = []
    buttons = []
    for row in rows:
        country = row[0] or 'Unknown'
        cnt = row[1]
        # try to use country code (2 letters) else show name
        if isinstance(country,str) and len(country)==2:
            flag = country_code_to_flag(country)
            label = f"{flag} {country} | {cnt}"
            # button callback uses country code
            buttons.append([{"text": f"{flag} {country} ({cnt})", "callback_data": f"pick:{country}"}])
        else:
            label = f"{country} | {cnt}"
            buttons.append([{"text": f"{country} ({cnt})", "callback_data": f"pick:{country}"}])
        lines.append(label)
    summary = "Доступные конфиги по странам:\n" + "\n".join(lines) + "\n\nНажмите на флаг или название страны в кнопках ниже, чтобы запросить конфиги."
    # split buttons into pages if too many (Telegram limits ~100 buttons)
    # send as keyboard
    try:
        await tg_client.send_keyboard(chat_id, summary, buttons)
    except Exception:
        # fallback to plain text
        await tg_client.send_message(chat_id, summary)

# schedule helper
_background_tasks = set()
def schedule_task(coro):
    if not asyncio.iscoroutine(coro):
        coro = coro()
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    def _cb(t):
        try:
            _background_tasks.discard(t)
            exc = None
            try:
                exc = t.exception()
            except Exception:
                pass
            if exc:
                print("Background task exception:", exc)
        except Exception:
            print("Error in task callback", traceback.format_exc())
    task.add_done_callback(_cb)
    return task

@app.on_event('startup')
async def startup():
    global tg_client, job_manager
    try:
        await init_db()
    except Exception:
        print("DB init failed", traceback.format_exc())
    tg_client = TelegramClient(bot_token=TELEGRAM_BOT_TOKEN)
    job_manager = JobManager(tg_client)
    print("Startup complete: tg_client and job_manager ready")

@app.post('/webhook/{secret}')
async def webhook(request: Request, secret: str, x_telegram_secret: Optional[str] = Header(None, alias="X-Telegram-Bot-Api-Secret-Token")):
    # validate secret
    try:
        if secret != TELEGRAM_WEBHOOK_SECRET or x_telegram_secret != TELEGRAM_WEBHOOK_SECRET:
            return JSONResponse(status_code=401, content={"ok": False, "error": "invalid secret"})
    except Exception:
        return JSONResponse(status_code=401, content={"ok": False, "error": "secret check failed"})

    try:
        update = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid json"})

    # handle callback_query (inline buttons)
    if 'callback_query' in update:
        cq = update['callback_query']
        data = cq.get('data')
        from_user = cq.get('from',{})
        chat = cq.get('message',{}).get('chat',{})
        chat_id = chat.get('id') or from_user.get('id')
        # answer callback quickly
        try:
            await tg_client.answer_callback(cq.get('id'), text="Обработка...")
        except Exception:
            pass
        if data == 'configs:new':
            # start new search
            # send initial message to user and get message ids
            resp = await tg_client.send_message(chat_id, "Запуск нового поиска конфигов...")
            edit_chat, edit_msg = (resp if isinstance(resp, tuple) else (chat_id, None))
            # schedule collection
            try:
                schedule_task(job_manager.run_collection(chat_id, edit_chat, edit_msg))
            except Exception:
                print("Failed to schedule run_collection", traceback.format_exc())
            return JSONResponse({'ok': True})
        if data == 'configs:use':
            # send list of countries and buttons
            schedule_task(send_country_list(chat_id))
            return JSONResponse({'ok': True})
        if data and data.startswith('pick:'):
            country = data.split(':',1)[1]
            # store state expecting count
            chat_state[chat_id] = {'expecting':'count', 'country': country}
            await tg_client.send_message(chat_id, f"Вы выбрали {country}. Сколько конфигов прислать? Введите число (например: 10)")
            return JSONResponse({'ok': True})

    # handle normal message
    msg = update.get('message') or update.get('edited_message') or {}
    text = (msg.get('text') or '').strip() if isinstance(msg, dict) else ''
    from_user = msg.get('from',{})
    chat = msg.get('chat',{})
    chat_id = chat.get('id') or from_user.get('id')

    # if user sends /configs -> show inline options
    if text.startswith('/configs'):
        # send inline keyboard: New search / Use DB
        buttons = [
            [{"text":"Новый поиск", "callback_data":"configs:new"}],
            [{"text":"Использовать базу", "callback_data":"configs:use"}]
        ]
        try:
            await tg_client.send_keyboard(chat_id, "Хотите произвести новый поиск или воспользоваться уже существующей базой?", buttons)
        except Exception:
            await tg_client.send_message(chat_id, "Хотите произвести новый поиск или воспользоваться уже существующей базой?\nReply with 'new' or 'use'")
        return JSONResponse({'ok': True})

    # if expecting a count after selecting country
    state = chat_state.get(chat_id)
    if state and state.get('expecting') == 'count':
        # check if text is an integer
        try:
            count = int(re.findall(r'\d+', text)[0])
        except Exception:
            await tg_client.send_message(chat_id, "Пожалуйста, введите корректное число (например: 10)")
            return JSONResponse({'ok': True})
        country = state.get('country')
        # clear state
        chat_state.pop(chat_id, None)
        # fetch configs and send
        schedule_task(send_configs_for_country(chat_id, country, count))
        return JSONResponse({'ok': True})

    # if user sends a flag emoji directly, interpret it
    if text and any('\U0001F1E6' <= ch <= '\U0001F1FF' for ch in text):
        # try to get country code from flag
        country = flag_to_country_code(text)
        if country:
            chat_state[chat_id] = {'expecting':'count', 'country': country}
            await tg_client.send_message(chat_id, f"Вы выбрали {country}. Сколько конфигов прислать? Введите число (например: 10)")
            return JSONResponse({'ok': True})

    # otherwise, default processing: let existing pipeline handle
    try:
        schedule_task(job_manager.handle_update(update))
    except Exception:
        # fallback: log and return ok
        print("Failed to schedule handle_update:", traceback.format_exc())
    return JSONResponse({'ok': True})

async def send_configs_for_country(chat_id: int, country: str, count: int):
    """Fetch configs from DB and send to user in chunks (or as file)"""
    eng = get_engine()
    async with eng.connect() as conn:
        q = sa.select([configs.c.raw]).where(sa.func.coalesce(configs.c.country, 'Unknown') == country).where(configs.c.ok==True).limit(count)
        res = await conn.execute(q)
        rows = [r[0] for r in res.fetchall()]
    if not rows:
        await tg_client.send_message(chat_id, f"Нет конфигов для страны {country}")
        return
    # prepare formatted text (each raw on new line)
    lines = rows
    # chunk into messages of <=3900 chars
    cur = []
    cur_len = 0
    chunks = []
    for line in lines:
        ln = len(line) + 1
        if cur_len + ln > 3800 and cur:
            chunks.append('\\n'.join(cur))
            cur = [line]
            cur_len = ln
        else:
            cur.append(line)
            cur_len += ln
    if cur:
        chunks.append('\\n'.join(cur))
    # send chunks or file
    if len(chunks) <= 8:
        for c in chunks:
            await tg_client.send_message(chat_id, c)
    else:
        # send as .txt document
        all_text = '\\n'.join(lines)
        fname = f'configs_{country}_{count}.txt'
        await tg_client.send_document(chat_id, fname, all_text.encode('utf-8'), caption=f'Configs {country} ({count})')
    await tg_client.send_message(chat_id, "Готово. Конфиги отправлены.")