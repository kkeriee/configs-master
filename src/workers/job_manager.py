# src/workers/job_manager.py
import os
import asyncio
import time
import hashlib
from typing import Dict, Any, List, Tuple
from ..collectors import collector_registry
from ..validators.tcp_check import tcp_check
from ..geo.ipapi import ipapi_batch_lookup
from ..utils.dedupe import dedupe_by_raw
from ..db import get_engine, init_db
from ..db.models import configs
import sqlalchemy as sa
import aiodns
import socket
import math

MAX_TG_CHUNK = 3900

def raw_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()

def chunks_from_lines(lines: List[str], max_chars: int = MAX_TG_CHUNK) -> List[str]:
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
        self.tcp_batch_size = int(os.getenv('TCP_BATCH_SIZE', '500'))
        self.tcp_retry = int(os.getenv('TCP_RETRY', '2'))
        # pending map for interactive flows (user_id -> country_code)
        self._pending: Dict[int, str] = {}

    async def ensure_db(self):
        if not self._db_initialized:
            try:
                await init_db()
            except Exception as e:
                print('DB init error', e)
            self._db_initialized = True

    async def run_collection(self, chat_id: int, edit_chat=None, edit_msg=None):
        """
        Run full collection pipeline:
         - collect provider lists
         - dedupe
         - tcp checks (batched)
         - resolve hosts -> ip -> geo lookup
         - insert to DB
         - send summary back via edit_message
        edit_chat/edit_msg are used for progress updates (if provided).
        """
        # send_message already called by caller; progress updates will be edit_message
        provider = collector_registry.get('github_public_lists')
        try:
            entries = await provider.collect()
        except Exception as e:
            await self.tg.send_message(chat_id, f'Ошибка сбора: {e}')
            return

        try:
            if edit_chat is not None and edit_msg is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Собрано: {len(entries)} строк. Удаляю дубликаты...')
        except Exception:
            pass

        uniq = dedupe_by_raw(entries)
        try:
            if edit_chat is not None and edit_msg is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Уникальных: {len(uniq)}. Запускаю TCP-проверки...')
        except Exception:
            pass

        # TCP checks in controlled batches to avoid overloading free instances
        batch_size = int(getattr(self, 'tcp_batch_size', 500))
        retry_count = int(getattr(self, 'tcp_retry', 2))
        total = len(uniq)
        checked = 0
        oks: List[Dict[str, Any]] = []

        async def try_check(cfg: Dict[str, Any]) -> bool:
            for attempt in range(retry_count):
                try:
                    ok = await tcp_check(cfg)
                    if ok:
                        return True
                except Exception:
                    # swallow and retry
                    pass
                await asyncio.sleep(0.1 * (attempt + 1))
            return False

        # process in batches
        for i in range(0, total, batch_size):
            batch = uniq[i:i+batch_size]
            sem = asyncio.Semaphore(self.max_concurrency)

            async def worker(cfg):
                async with sem:
                    ok = await try_check(cfg)
                    item = dict(cfg)  # make a shallow copy
                    item['ok'] = ok
                    return item

            tasks = [asyncio.create_task(worker(c)) for c in batch]
            for t in asyncio.as_completed(tasks):
                try:
                    r = await t
                except Exception:
                    # one task failed — ignore but continue
                    r = None
                checked += 1
                if r and r.get('ok'):
                    oks.append(r)
                # update progress occasionally (about every ~5% or at least every 10)
                try:
                    if edit_chat is not None and edit_msg is not None:
                        if total > 0 and (checked % max(1, total // 20) == 0 or checked % 10 == 0):
                            await self.tg.edit_message(edit_chat, edit_msg, f'Проверено {checked}/{total} конфигов. Активных: {len(oks)}')
                except Exception:
                    pass
            # small pause between batches to release resources
            await asyncio.sleep(0.25)

        if not oks:
            try:
                if edit_chat is not None and edit_msg is not None:
                    await self.tg.edit_message(edit_chat, edit_msg, 'Нет активных конфигов после проверки.')
                else:
                    await self.tg.send_message(chat_id, 'Нет активных конфигов после проверки.')
            except Exception:
                pass
            return

        try:
            if edit_chat is not None and edit_msg is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Резултат: {len(oks)} активных. Разрешаю хосты...')
        except Exception:
            pass

        # resolve hosts -> ips and map
        ips: List[str] = []
        host_map: Dict[str, Dict[str, Any]] = {}
        for item in oks:
            host = item.get('host')
            ip = None
            if host:
                try:
                    res = await self.resolver.gethostbyname(host, socket.AF_INET)
                    ip = res.addresses[0] if getattr(res, 'addresses', None) else None
                except Exception:
                    ip = None
            item['ip'] = ip
            if ip:
                ips.append(ip)
                host_map[ip] = item
        # unique ips, preserve order
        ips = list(dict.fromkeys(ips))

        try:
            if edit_chat is not None and edit_msg is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'Найдено {len(ips)} IP. Запрашиваю геоданные пачками...')
        except Exception:
            pass

        # ip-api batch lookup
        geo_results: List[Dict[str, Any]] = []
        for j in range(0, len(ips), self.ip_batch):
            batch_ips = ips[j:j + self.ip_batch]
            try:
                resp = await ipapi_batch_lookup(batch_ips)
                if resp:
                    geo_results.extend(resp)
            except Exception:
                # on failure, extend with empty list and continue
                resp = []
            # update progress
            try:
                if edit_chat is not None and edit_msg is not None:
                    await self.tg.edit_message(edit_chat, edit_msg, f'Геопоиск: обработано {min(j + self.ip_batch, len(ips))}/{len(ips)} IP')
            except Exception:
                pass
            await asyncio.sleep(0.3)

        # insert into DB, skip duplicates by raw_hash
        eng = get_engine()
        inserted = 0
        try:
            async with eng.begin() as conn:
                for g in geo_results:
                    ip = g.get('query')
                    country = g.get('countryCode') or g.get('country') or None
                    item = host_map.get(ip)
                    if not item:
                        continue
                    raw = item.get('raw') or ''
                    rh = raw_hash(raw)
                    # check existing
                    q = sa.select([configs.c.id]).where(configs.c.raw_hash == rh)
                    try:
                        res = await conn.execute(q)
                        exists = res.fetchone()
                    except Exception:
                        exists = None
                    if exists:
                        continue
                    ins = configs.insert().values(
                        raw=raw,
                        raw_hash=rh,
                        protocol=item.get('protocol'),
                        host=item.get('host'),
                        port=item.get('port'),
                        ip=ip,
                        country=country,
                        ok=True,
                        collected_from=item.get('collected_from')
                    )
                    try:
                        await conn.execute(ins)
                        inserted += 1
                    except Exception:
                        # ignore insertion errors (unique constraint / concurrency)
                        continue
        except Exception as e:
            # If DB insertion fails entirely, log and continue to summary
            print("DB insertion error:", e)

        try:
            if edit_chat is not None and edit_msg is not None:
                await self.tg.edit_message(edit_chat, edit_msg, f'В БД вставлено {inserted} записей. Формирую таблицу по странам...')
        except Exception:
            pass

        # prepare summary
        try:
            async with eng.connect() as conn:
                q = sa.select([configs.c.country, sa.func.count(configs.c.id).label('cnt')])\
                      .where(configs.c.ok == True)\
                      .group_by(configs.c.country)\
                      .order_by(sa.func.count(configs.c.id).desc())
                res = await conn.execute(q)
                rows = res.fetchall()
        except Exception:
            rows = []

        if not rows:
            try:
                if edit_chat is not None and edit_msg is not None:
                    await self.tg.edit_message(edit_chat, edit_msg, 'После обработки нет доступных конфигов.')
                else:
                    await self.tg.send_message(chat_id, 'После обработки нет доступных конфигов.')
            except Exception:
                pass
            return

        lines = []
        for row in rows:
            country = row[0] or 'Unknown'
            lines.append(f'{country} | {row[1]}')
        summary = '\n'.join(lines)
        try:
            if edit_chat is not None and edit_msg is not None:
                await self.tg.edit_message(edit_chat, edit_msg, 'Готово. Список доступных конфигов по странам:\n' + summary +
                                           '\n\nЧтобы получить конфиги: отправьте команду:\n/get <COUNTRY_CODE> <COUNT>\nнапример: /get DE 10')
            else:
                await self.tg.send_message(chat_id, 'Готово. Список доступных конфигов по странам:\n' + summary +
                                           '\n\nЧтобы получить конфиги: отправьте команду:\n/get <COUNTRY_CODE> <COUNT>\nнапример: /get DE 10')
        except Exception:
            pass

    async def handle_update(self, update: Dict[str, Any]):
        """
        Handle incoming Telegram update (message / edited_message).
        Supports:
         - /configs : triggers run_collection and shows progress
         - /get <COUNTRY> <COUNT> : returns configs
         - interactive pending flow is handled elsewhere (if used)
        """
        message = update.get('message') or update.get('edited_message')
        if not message:
            return
        text = (message.get('text') or '').strip()
        chat_id = message['chat']['id']
        message_id = message.get('message_id')

        # Ensure DB
        await self.ensure_db()

        # /configs command - start pipeline and send progress edits
        if text.startswith('/configs'):
            msg = await self.tg.send_message(chat_id, 'Запуск сбора конфигов...') or (chat_id, message_id)
            if isinstance(msg, tuple):
                edit_chat, edit_msg = msg
            else:
                # If send_message returned None or unexpected, fallback
                edit_chat, edit_msg = chat_id, None
            await self.run_collection(chat_id, edit_chat=edit_chat, edit_msg=edit_msg)
            return

        # /get command
        if text.startswith('/get'):
            parts = text.split()
            if len(parts) < 3:
                await self.tg.send_message(chat_id, 'Неправильный формат. Используйте /get <COUNTRY_CODE> <COUNT>')
                return
            country = parts[1]
            try:
                count = int(parts[2])
            except Exception:
                await self.tg.send_message(chat_id, 'Количество должно быть числом.')
                return
            # fetch from DB
            await self.ensure_db()
            eng = get_engine()
            try:
                async with eng.connect() as conn:
                    q = sa.select([configs.c.raw])\
                          .where(sa.func.coalesce(configs.c.country, 'Unknown') == country)\
                          .where(configs.c.ok == True).limit(count)
                    res = await conn.execute(q)
                    rows = [r[0] for r in res.fetchall()]
            except Exception:
                rows = []
            if not rows:
                await self.tg.send_message(chat_id, f'Нет конфигов для страны {country}')
                return
            lines = rows
            chunks = chunks_from_lines(lines)
            # If small number of chunks, send as messages; otherwise send as document
            if len(chunks) <= 8:
                for c in chunks:
                    await self.tg.send_message(chat_id, c)
            else:
                all_text = '\n'.join(lines)
                fname = f'configs_{country}_{count}.txt'
                await self.tg.send_document(chat_id, fname, all_text.encode('utf-8'),
                                           caption=f'Configs {country} ({count})')
            await self.tg.send_message(chat_id, 'Готово. Конфиги отправлены.')
            return

        # If user responded with a number after an interactive pick (pending), handle that
        # (This assumes some external flow set self._pending[user_id] = country)
        from_id = message['from']['id']
        if from_id in self._pending:
            pending_country = self._pending.pop(from_id)
            if text.isdigit():
                count = int(text)
                # fetch and send (same as /get)
                await self.ensure_db()
                eng = get_engine()
                try:
                    async with eng.connect() as conn:
                        q = sa.select([configs.c.raw])\
                              .where(sa.func.coalesce(configs.c.country, 'Unknown') == pending_country)\
                              .where(configs.c.ok == True).limit(count)
                        res = await conn.execute(q)
                        rows = [r[0] for r in res.fetchall()]
                except Exception:
                    rows = []
                if not rows:
                    await self.tg.send_message(chat_id, f'Нет конфигов для страны {pending_country}')
                    return
                lines = rows
                chunks = chunks_from_lines(lines)
                if len(chunks) <= 8:
                    for c in chunks:
                        await self.tg.send_message(chat_id, c)
                else:
                    all_text = '\n'.join(lines)
                    fname = f'configs_{pending_country}_{count}.txt'
                    await self.tg.send_document(chat_id, fname, all_text.encode('utf-8'),
                                                caption=f'Configs {pending_country} ({count})')
                await self.tg.send_message(chat_id, 'Готово. Конфиги отправлены.')
                return
            else:
                await self.tg.send_message(chat_id, 'Ожидаю число. Попробуйте ещё раз.')
                return

        # If nothing matched, ignore or send help
        # await self.tg.send_message(chat_id, 'Неизвестная команда. Используйте /configs или /get <COUNTRY> <COUNT>')
        return
