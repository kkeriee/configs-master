# src/workers/job_manager.py
import os, asyncio, time, hashlib
from typing import Dict, Any, List, Tuple
from ..collectors import collector_registry
from ..validators.tcp_check import tcp_check
from ..geo.ipapi import ipapi_batch_lookup
from ..utils.dedupe import dedupe_by_raw
from ..db import get_engine, init_db
from ..db.models import configs
from ..parsers.parse_config import parse_raw_config
import sqlalchemy as sa
import aiodns
import socket
import math

MAX_TG_CHUNK = 3900

def chunks_from_lines(lines: List[str], max_chars: int = MAX_TG_CHUNK):
    out = []
    cur = []
    cur_len = 0
    for line in lines:
        ln = len(line) + 1
        if cur_len + ln > max_chars and cur:
            out.append('\n'.join(cur))
            cur = [line]
            cur_len = ln
        else:
            cur.append(line)
            cur_len += ln
    if cur:
        out.append('\n'.join(cur))
    return out

class JobManager:
    def __init__(self, tg_client):
        self.tg = tg_client
        self.max_concurrency = int(os.getenv('MAX_CONCURRENCY', '50'))
        self.ip_batch = int(os.getenv('IPAPI_BATCH_SIZE', '100'))
        self.resolver = aiodns.DNSResolver()
        self._db_initialized = False
        self.tcp_batch_size = int(os.getenv('TCP_BATCH_SIZE','200'))
        self.tcp_retry = int(os.getenv('TCP_RETRY','2'))
        self._pending: Dict[int, str] = {}

    async def ensure_db(self):
        if not self._db_initialized:
            try:
                await init_db()
            except Exception as e:
                print('DB init error', e)
            self._db_initialized = True

    async def resolve_host(self, host: str) -> str:
        try:
            res = await self.resolver.gethostbyname(host, socket.AF_INET)
            if res and getattr(res, 'addresses', None):
                return res.addresses[0]
        except Exception:
            pass
        try:
            infos = socket.getaddrinfo(host, None)
            for info in infos:
                addr = info[4][0]
                if addr:
                    return addr
        except Exception:
            pass
        return None

    async def run_collection(self, chat_id:int, edit_chat=None, edit_msg=None):
        try:
            provider = collector_registry.get('github_public_lists')
            if not provider:
                await self.tg.send_message(chat_id, 'Нет провайдера коллектора.')
                return
            entries = await provider.collect()
            if edit_chat is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Собрано: {len(entries)} строк. Удаляю дубликаты...')
            uniq = dedupe_by_raw(entries)
            if edit_chat is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Уникальных: {len(uniq)}. Запускаю TCP-проверки...')
            sem = asyncio.Semaphore(self.max_concurrency)
            ok_items = []

            async def check_item(item):
                async with sem:
                    for attempt in range(self.tcp_retry):
                        try:
                            ok = await tcp_check(item)
                            if ok:
                                return True
                        except Exception:
                            pass
                        await asyncio.sleep(0.1 * (attempt+1))
                    return False

            total = len(uniq)
            checked = 0
            for i in range(0, total, self.tcp_batch_size):
                batch = uniq[i:i+self.tcp_batch_size]
                tasks = [asyncio.create_task(check_item(it)) for it in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for it, res in zip(batch, results):
                    checked += 1
                    if isinstance(res, Exception):
                        continue
                    if res:
                        it['ok'] = True
                        ok_items.append(it)
                    else:
                        it['ok'] = False
                if edit_chat is not None:
                    try:
                        await self.tg.edit_message(edit_chat, edit_msg, f'Проверено {checked}/{total} конфигов. Доступно: {len(ok_items)}')
                    except Exception:
                        pass
                await asyncio.sleep(0.2)

            if edit_chat is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'После TCP-проверок доступно: {len(ok_items)} конфигов. Разрешаю хосты...')

            host_map = {}
            ips = []
            for item in ok_items:
                host = item.get('host')
                if not host:
                    parsed = parse_raw_config(item.get('raw',''))
                    host = parsed.get('host')
                    item.setdefault('host', host)
                ip = None
                if host:
                    try:
                        ip = await self.resolve_host(host)
                    except Exception:
                        ip = None
                item['ip'] = ip
                if ip:
                    ips.append(ip)
                    host_map[ip] = item
            ips = list(dict.fromkeys(ips))

            if edit_chat is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Найдено {len(ips)} IP. Запрашиваю геоданные пачками...')

            geo_results = []
            for j in range(0, len(ips), self.ip_batch):
                batch_ips = ips[j:j+self.ip_batch]
                try:
                    resp = await ipapi_batch_lookup(batch_ips)
                except Exception:
                    resp = []
                geo_results.extend(resp)
                if edit_chat is not None:
                    try:
                        await self.tg.edit_message(edit_chat, edit_msg, f'IP-lookup: обработано {min(j+self.ip_batch, len(ips))}/{len(ips)} IP')
                    except Exception:
                        pass
                await asyncio.sleep(0.3)

            eng = get_engine()
            inserted = 0
            async with eng.begin() as conn:
                for g in geo_results:
                    ip = g.get('query')
                    country = g.get('countryCode') or g.get('country') or None
                    item = host_map.get(ip)
                    if not item:
                        continue
                    raw = item.get('raw') or ''
                    raw_hash = hashlib.sha256(raw.encode('utf-8')).hexdigest()
                    values = {
                        'raw': raw,
                        'raw_hash': raw_hash,
                        'protocol': item.get('protocol'),
                        'host': item.get('host'),
                        'port': item.get('port'),
                        'ip': ip,
                        'country': country,
                        'ok': True if item.get('ok') else False,
                        'collected_from': item.get('collected_from')
                    }
                    try:
                        await conn.execute(sa.insert(configs).values(**values))
                        inserted += 1
                    except Exception:
                        continue

            if edit_chat is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Вставлено {inserted} записей. Формирую таблицу по странам...')

            async with eng.connect() as conn:
                q = sa.select([configs.c.country, sa.func.count(configs.c.id).label('cnt')]).where(configs.c.ok==True).group_by(configs.c.country).order_by(sa.desc('cnt'))
                res = await conn.execute(q)
                rows = res.fetchall()
            if not rows:
                if edit_chat is not None:
                    await self.tg.edit_message(edit_chat, edit_msg, 'После обработки нет доступных конфигов.')
                return
            lines = []
            for row in rows:
                country = row[0] or 'Unknown'
                lines.append(f'{country} | {row[1]}')
            summary = '\n'.join(lines)
            text = 'Обработка завершена. Доступные страны и количество конфигов:\n\n' + summary + '\n\nИспользуйте команду /get <COUNTRY_CODE> <COUNT> или откройте /configs и выберите страну.'
            await self.tg.send_message(chat_id, text)
        except Exception as e:
            try:
                await self.tg.send_message(chat_id, f'Ошибка в run_collection: {e}')
            except Exception:
                print('Error while reporting run_collection exception', e)

    async def handle_update(self, update: Dict[str, Any]):
        # callback handling
        if 'callback_query' in update:
            cq = update['callback_query']
            data = cq.get('data')
            user_id = cq['from']['id']
            try:
                await self.tg.answer_callback(cq.get('id'), text='Обработка...')
            except Exception:
                pass
            if data == 'configs_new':
                msg = await self.tg.send_message(user_id, 'Запуск сбора конфигов...')
                if msg:
                    edit_chat, edit_msg = msg
                else:
                    edit_chat, edit_msg = user_id, None
                try:
                    await self.run_collection(user_id, edit_chat=edit_chat, edit_msg=edit_msg)
                except Exception as e:
                    await self.tg.send_message(user_id, f'Ошибка в процессе сбора: {e}')
                return
            if data == 'configs_use':
                await self.ensure_db()
                eng = get_engine()
                async with eng.connect() as conn:
                    q = sa.select([configs.c.country, sa.func.count(configs.c.id).label('cnt')]).where(configs.c.ok==True).group_by(configs.c.country).order_by(sa.desc('cnt'))
                    res = await conn.execute(q)
                    rows = res.fetchall()
                if not rows:
                    await self.tg.send_message(user_id, 'База пуста.')
                    return
                kb = []
                for country, cnt in rows:
                    code = country or 'Unknown'
                    label = f"{code} — {cnt}"
                    kb.append([{'text': label, 'callback_data': f'pick_{code}'}])
                await self.tg.send_inline_keyboard(user_id, 'Выберите страну (нажмите кнопку):', kb)
                return
            if data and data.startswith('pick_'):
                code = data.split('pick_',1)[1]
                self._pending[user_id] = code
                await self.tg.send_message(user_id, f'Вы выбрали {code}. Введите желаемое количество конфигов (числом).')
                return

        message = update.get('message') or update.get('edited_message')
        if not message:
            return
        text = (message.get('text') or '').strip()
        chat_id = message['chat']['id']
        message_id = message['message_id']

        await self.ensure_db()

        if text.startswith('/configs'):
            try:
                kb = [
                    [{"text":"Новый поиск 🔍","callback_data":"configs_new"}],
                    [{"text":"Использовать базу 🗂️","callback_data":"configs_use"}]
                ]
                await self.tg.send_inline_keyboard(chat_id, "Хотите произвести новый поиск или воспользоваться уже существующей базой?", kb)
            except Exception as e:
                await self.tg.send_message(chat_id, f'Ошибка при отправке клавиатуры: {e}')
            return

        if message.get('from') and message['from'].get('id') in self._pending:
            pending_country = self._pending.pop(message['from']['id'])
            if text.isdigit():
                count = int(text)
                eng = get_engine()
                async with eng.connect() as conn:
                    q = sa.select([configs.c.raw]).where(sa.func.coalesce(configs.c.country, 'Unknown') == pending_country).where(configs.c.ok==True).limit(count)
                    res = await conn.execute(q)
                    rows = [r[0] for r in res.fetchall()]
                if not rows:
                    await self.tg.send_message(chat_id, f'Нет конфигов для страны {pending_country}')
                else:
                    lines = rows
                    chunks = chunks_from_lines(lines)
                    if len(chunks) <= 8:
                        for c in chunks:
                            await self.tg.send_message(chat_id, c)
                    else:
                        all_text = '\n'.join(lines)
                        fname = f'configs_{pending_country}_{count}.txt'
                        await self.tg.send_document(chat_id, fname, all_text.encode('utf-8'), caption=f'Configs {pending_country} ({count})')
                await self.tg.send_message(chat_id, 'Готово. Конфиги отправлены.')
                return
            else:
                await self.tg.send_message(chat_id, 'Ожидаю число. Попробуйте ещё раз.')
                return

        if text.startswith('/get'):
            parts = text.split()
            if len(parts) < 3:
                await self.tg.send_message(chat_id, 'Неправильный формат. Используйте /get <COUNTRY_CODE> <COUNT>')
                return
            country = parts[1]
            try:
                count = int(parts[2])
            except:
                await self.tg.send_message(chat_id, 'Количество должно быть числом.')
                return
            await self.ensure_db()
            eng = get_engine()
            async with eng.connect() as conn:
                q = sa.select([configs.c.raw]).where(sa.func.coalesce(configs.c.country, 'Unknown') == country).where(configs.c.ok==True).limit(count)
                res = await conn.execute(q)
                rows = [r[0] for r in res.fetchall()]
            if not rows:
                await self.tg.send_message(chat_id, f'Нет конфигов для страны {country}')
                return
            lines = rows
            chunks = chunks_from_lines(lines)
            if len(chunks) <= 8:
                for c in chunks:
                    await self.tg.send_message(chat_id, c)
            else:
                all_text = '\n'.join(lines)
                fname = f'configs_{country}_{count}.txt'
                await self.tg.send_document(chat_id, fname, all_text.encode('utf-8'), caption=f'Configs {country} ({count})')
            await self.tg.send_message(chat_id, 'Готово. Конфиги отправлены.')
