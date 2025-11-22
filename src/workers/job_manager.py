import os, asyncio, time, hashlib
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
        self.tcp_batch_size = int(os.getenv('TCP_BATCH_SIZE','500'))
        self.tcp_retry = int(os.getenv('TCP_RETRY','2'))

    async def ensure_db(self):
        if not self._db_initialized:
            try:
                await init_db()
            except Exception as e:
                print('DB init error', e)
            self._db_initialized = True

    async def handle_update(self, update: Dict[str, Any]):
        message = update.get('message') or update.get('edited_message')
        if not message:
            return
        text = (message.get('text') or '').strip()
        chat_id = message['chat']['id']
        message_id = message['message_id']

        # ensure DB init when any user triggers /configs
        await self.ensure_db()

        if text.startswith('/configs'):
            # start pipeline and send progress via edit_message
            msg = await self.tg.send_message(chat_id, 'Запуск сбора конфигов...') or (chat_id, message_id)
            # record message id for edits
            edit_chat, edit_msg = msg

            # collect
            provider = collector_registry.get('github_public_lists')
            try:
                entries = await provider.collect()
            except Exception as e:
                await self.tg.send_message(chat_id, f'Ошибка сбора: {e}')
                return

            await self.tg.edit_message(edit_chat, edit_msg, f'Собрано: {len(entries)} строк. Удаляю дубликаты...')
            # dedupe raw
            uniq = dedupe_by_raw(entries)
            await self.tg.edit_message(edit_chat, edit_msg, f'Уникальных: {len(uniq)}. Запускаю TCP-проверки...')

            # TCP checks with concurrency
            
# TCP checks in controlled batches to avoid overloading the free Render instance.
batch_size = int(getattr(self, 'tcp_batch_size', 500))
retry_count = int(getattr(self, 'tcp_retry', 2))
total = len(uniq)
checked = 0
oks = []

async def try_check(cfg):
    # perform retries with small backoff
    for attempt in range(retry_count):
        try:
            ok = await tcp_check(cfg)
            if ok:
                return True
        except Exception:
            pass
        # backoff a bit before retrying
        await asyncio.sleep(0.1 * (attempt + 1))
    return False

# process in batches
for i in range(0, total, batch_size):
    batch = uniq[i:i+batch_size]
    sem = asyncio.Semaphore(self.max_concurrency)
    async def worker(cfg):
        async with sem:
            ok = await try_check(cfg)
            return dict(cfg, ok=ok)
    tasks = [asyncio.create_task(worker(c)) for c in batch]
    for t in asyncio.as_completed(tasks):
        r = await t
        checked += 1
        if r.get('ok'):
            oks.append(r)
        # update progress every ~5%
        if checked % max(1, total//20) == 0:
            try:
                await self.tg.edit_message(edit_chat, edit_msg, f'Проверено {checked}/{total} конфигов. Активных: {len(oks)}')
            except Exception:
                pass
    # allow small pause between batches to release resources
    await asyncio.sleep(0.25)

            if not oks:
                await self.tg.edit_message(edit_chat, edit_msg, 'Нет активных конфигов после проверки.')
                return

            await self.tg.edit_message(edit_chat, edit_msg, f'Резултат: {len(oks)} активных. Разрешаю хосты...')
            # resolve hosts
            ips = []
            host_map = {}
            for item in oks:
                host = item.get('host')
                ip = None
                if host:
                    try:
                        res = await self.resolver.gethostbyname(host, socket.AF_INET)
                        ip = res.addresses[0] if res.addresses else None
                    except Exception:
                        ip = None
                item['ip'] = ip
                if ip:
                    ips.append(ip)
                    host_map[ip] = item
            ips = list(dict.fromkeys(ips))

            await self.tg.edit_message(edit_chat, edit_msg, f'Найдено {len(ips)} IP. Запрашиваю геоданные пачками...')
            # ip-api batch lookup
            geo_results = []
            for i in range(0, len(ips), self.ip_batch):
                batch = ips[i:i+self.ip_batch]
                resp = await ipapi_batch_lookup(batch)
                geo_results.extend(resp)
                # update progress
                await self.tg.edit_message(edit_chat, edit_msg, f'Геопоиск: обработано {min(i+self.ip_batch, len(ips))}/{len(ips)} IP')
                await asyncio.sleep(0.3)

            # insert into DB, skip duplicates by raw_hash
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
                    rh = raw_hash(raw)
                    # check existing
                    q = sa.select([configs.c.id]).where(configs.c.raw_hash == rh)
                    res = await conn.execute(q)
                    exists = res.fetchone()
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
                        # ignore insertion errors (concurrency/unique)
                        continue

            await self.tg.edit_message(edit_chat, edit_msg, f'В БД вставлено {inserted} записей. Формирую таблицу по странам...')
            # prepare summary
            async with eng.connect() as conn:
                q = sa.select([configs.c.country, sa.func.count(configs.c.id).label('cnt')]).where(configs.c.ok==True).group_by(configs.c.country).order_by(sa.func.count(configs.c.id).desc())
                res = await conn.execute(q)
                rows = res.fetchall()
            if not rows:
                await self.tg.edit_message(edit_chat, edit_msg, 'После обработки нет доступных конфигов.')
                return
            lines = []
            for row in rows:
                country = row[0] or 'Unknown'
                lines.append(f'{country} | {row[1]}')
            summary = '\n'.join(lines)
            await self.tg.edit_message(edit_chat, edit_msg, 'Готово. Список доступных конфигов по странам:\n' + summary + '\n\nЧтобы получить конфиги: отправьте команду:\n/get <COUNTRY_CODE> <COUNT>\nнапример: /get DE 10')

        elif text.startswith('/get'):
            # parse /get COUNTRY COUNT
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
            # fetch from DB
            await self.ensure_db()
            eng = get_engine()
            async with eng.connect() as conn:
                q = sa.select([configs.c.raw]).where(sa.func.coalesce(configs.c.country, 'Unknown') == country).where(configs.c.ok==True).limit(count)
                res = await conn.execute(q)
                rows = [r[0] for r in res.fetchall()]
            if not rows:
                await self.tg.send_message(chat_id, f'Нет конфигов для страны {country}')
                return
            # build text lines and send chunked or as document if too large
            lines = rows
            chunks = chunks_from_lines(lines)
            if len(chunks) <= 8:
                # send as messages (up to some chunk count)
                for c in chunks:
                    await self.tg.send_message(chat_id, c)
            else:
                # if too many chunks, create a .txt and send as document
                all_text = '\n'.join(lines)
                fname = f'configs_{country}_{count}.txt'
                await self.tg.send_document(chat_id, fname, all_text.encode('utf-8'), caption=f'Configs {country} ({count})')
            await self.tg.send_message(chat_id, 'Готово. Конфиги отправлены.')
