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
        """
        Collection pipeline with checkpointing into raw_entries and jobs.
        Works in batches and updates progress to Telegram via edit_message if edit_chat/edit_msg provided.
        """
        JOB_BATCH = int(os.getenv('JOB_BATCH_LIMIT', '200'))
        RUN_TIME_LIMIT = int(os.getenv('RUN_TIME_LIMIT_SEC', '900'))
        start_ts = time.time()

        # get provider
        provider = collector_registry.get('github_public_lists')
        if not provider:
            await self.tg.send_message(chat_id, 'Нет провайдера коллектора.')
            return

        # collect entries
        try:
            entries = await provider.collect()
        except Exception as e:
            await self.tg.send_message(chat_id, f'Ошибка сбора: {e}')
            return

        # dedupe in memory first
        uniq = dedupe_by_raw(entries)

        # write uniq into raw_entries table if not exists
        await self.ensure_db()
        eng = get_engine()
        total = len(uniq)
        inserted_raw = 0
        async with eng.begin() as conn:
            for item in uniq:
                raw = item.get('raw') or item.get('data') or ''
                raw_hash = hashlib.sha256(raw.encode('utf-8')).hexdigest()
                # insert if not exists in raw_entries
                q = sa.select([raw_entries.c.id]).where(raw_entries.c.raw_hash == raw_hash)
                try:
                    res = await conn.execute(q)
                    exists = res.fetchone()
                except Exception:
                    exists = None
                if exists:
                    continue
                ins = raw_entries.insert().values(
                    raw=raw,
                    raw_hash=raw_hash,
                    collected_from=item.get('collected_from')
                )
                try:
                    await conn.execute(ins)
                    inserted_raw += 1
                except Exception:
                    # ignore unique constraint or others
                    continue

        # create job row
        async with eng.begin() as conn:
            total_q = await conn.execute(sa.select([sa.func.count(raw_entries.c.id)]))
            total_entries = int(total_q.scalar() or 0)
            job_ins = jobs.insert().values(name='collection', status='running', total_entries=total_entries, processed_count=0, inserted=0)
            try:
                res = await conn.execute(job_ins)
                job_id = res.inserted_primary_key[0] if hasattr(res, 'inserted_primary_key') else None
            except Exception:
                job_id = None

        # process in batches until none left or time limit reached
        processed = 0
        total_inserted = 0
        while True:
            # check time budget
            if time.time() - start_ts > RUN_TIME_LIMIT:
                # pause job
                try:
                    async with eng.begin() as conn:
                        await conn.execute(jobs.update().where(jobs.c.id == job_id).values(status='paused', processed_count=processed, inserted=total_inserted))
                except Exception:
                    pass
                if edit_chat is not None and edit_msg is not None:
                    await self.tg.edit_message(edit_chat, edit_msg, f'Прервано по таймауту после {processed} обработанных. Введите /configs чтобы продолжить.')
                return

            # fetch a batch of unprocessed raw_entries
            async with eng.connect() as conn:
                q = sa.select([raw_entries.c.id, raw_entries.c.raw, raw_entries.c.raw_hash]).where(raw_entries.c.processed == False).limit(JOB_BATCH)
                res = await conn.execute(q)
                rows = res.fetchall()
            if not rows:
                # mark job done
                try:
                    async with eng.begin() as conn:
                        await conn.execute(jobs.update().where(jobs.c.id == job_id).values(status='done', processed_count=processed, inserted=total_inserted))
                except Exception:
                    pass
                if edit_chat is not None and edit_msg is not None:
                    await self.tg.edit_message(edit_chat, edit_msg, f'Обработка завершена. Всего обработано: {processed}. Вставлено: {total_inserted}')
                return

            # perform TCP checks concurrently
            sem = asyncio.Semaphore(self.max_concurrency)
            async def check_row(rid, raw):
                async with sem:
                    item = {'raw': raw}
                    try:
                        ok = await tcp_check(item)
                    except Exception:
                        ok = False
                    return rid, raw, ok

            tasks = [asyncio.create_task(check_row(r[0], r[1])) for r in rows]
            results = []
            for t in asyncio.as_completed(tasks):
                try:
                    res = await t
                except Exception:
                    res = None
                if res:
                    results.append(res)

            # map ok rows and resolve hosts, collect ips
            ok_map = {}
            ips = []
            host_map = {}
            for rid, raw, ok in results:
                # try to parse host/port/protocol from raw via existing parser if available
                parsed = None
                try:
                    parsed = None
                    # some parsers may exist; skip heavy parsing here
                except Exception:
                    parsed = None
                if ok:
                    # attempt to extract host via simple heuristics in tcp_check may have filled host; but we fallback to parsing
                    host = None
                    # store ip later
                    ok_map[rid] = {'raw': raw, 'host': host, 'protocol': None, 'port': None, 'collected_from': None}
            # resolve IPs for ok_map entries
            for rid, info in ok_map.items():
                host = info.get('host')
                ip = None
                if host:
                    try:
                        ip = await self.resolve_host(host)
                    except Exception:
                        ip = None
                info['ip'] = ip
                if ip:
                    ips.append(ip)
                    host_map[ip] = info

            # geo lookup for ips in batches
            geo_results = []
            for i in range(0, len(ips), self.ip_batch):
                batch_ips = ips[i:i+self.ip_batch]
                try:
                    resp = await ipapi_batch_lookup(batch_ips)
                    if resp:
                        geo_results.extend(resp)
                except Exception:
                    pass
                await asyncio.sleep(0.25)

            # insert into configs and mark raw_entries processed
            async with eng.begin() as conn:
                for g in geo_results:
                    ip = g.get('query')
                    country = g.get('countryCode') or g.get('country') or None
                    item = host_map.get(ip)
                    if not item:
                        continue
                    raw = item.get('raw') or ''
                    rh = hashlib.sha256(raw.encode('utf-8')).hexdigest()
                    # skip if exists in configs
                    q = sa.select([configs.c.id]).where(configs.c.raw_hash == rh)
                    try:
                        res = await conn.execute(q)
                        exists = res.fetchone()
                    except Exception:
                        exists = None
                    if exists:
                        # mark processed
                        await conn.execute(raw_entries.update().where(raw_entries.c.raw_hash == rh).values(processed=True))
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
                        total_inserted += 1
                        # mark processed
                        await conn.execute(raw_entries.update().where(raw_entries.c.raw_hash == rh).values(processed=True))
                    except Exception:
                        # mark processed anyway to avoid infinite retry loops
                        try:
                            await conn.execute(raw_entries.update().where(raw_entries.c.raw_hash == rh).values(processed=True))
                        except Exception:
                            pass

            processed += len(rows)
            # update job progress
            try:
                async with eng.begin() as conn:
                    await conn.execute(jobs.update().where(jobs.c.id == job_id).values(processed_count=processed, inserted=total_inserted))
            except Exception:
                pass

            # report progress
            if edit_chat is not None and edit_msg is not None:
                try:
                    await self.tg.edit_message(edit_chat, edit_msg, f'Проверено ~{processed}/{total_entries} (вставлено {total_inserted})')
                except Exception:
                    pass

            # small pause
            await asyncio.sleep(0.2)
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
