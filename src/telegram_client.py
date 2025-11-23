import os, asyncio, aiohttp
from typing import Optional, Tuple
from aiohttp import FormData

class TelegramClient:
    def __init__(self, bot_token: str, session: Optional[aiohttp.ClientSession]=None):
        self.bot_token = bot_token
        self.api = f'https://api.telegram.org/bot{bot_token}'
        self.session = session or aiohttp.ClientSession()
        self._lock = asyncio.Semaphore(3)  # crude rate limiting
        self.branding = 'by Jeremih333 — все авторские права сохранены.'

    async def send_message(self, chat_id: int, text: str, reply_to_message_id=None) -> Optional[Tuple[int,int]]:
        # chunk text if > 3900 characters
        chunks = [text[i:i+3900] for i in range(0, len(text), 3900)]
        last_msg = None
        for idx,chunk in enumerate(chunks):
            # append branding only to the last chunk
            if idx == len(chunks)-1 and self.branding and not chunk.strip().endswith('by Jeremih333'):
                chunk = chunk + '\n\n' + self.branding
            payload = {'chat_id': chat_id, 'text': chunk}
            if reply_to_message_id: payload['reply_to_message_id'] = reply_to_message_id
            async with self._lock:
                async with self.session.post(f'{self.api}/sendMessage', json=payload) as resp:
                    try:
                        data = await resp.json()
                    except Exception:
                        data = {}
                    if data.get('ok'):
                        last_msg = (data['result']['chat']['id'], data['result']['message_id'])
                    else:
                        last_msg = None
        return last_msg

    async def edit_message(self, chat_id:int, message_id:int, text:str):
        async with self._lock:
            async with self.session.post(f'{self.api}/editMessageText', json={
                'chat_id': chat_id, 'message_id': message_id, 'text': text
            }) as resp:
                try:
                    return await resp.json()
                except Exception:
                    return {}

    async def send_document(self, chat_id: int, filename: str, data_bytes: bytes, caption: Optional[str]=None) -> Optional[Tuple[int,int]]:
        form = FormData()
        form.add_field('chat_id', str(chat_id))
        form.add_field('document', data_bytes, filename=filename, content_type='text/plain')
        if caption:
            if self.branding and not caption.strip().endswith('by Jeremih333'):
                caption = caption + '\n\n' + self.branding
            
            form.add_field('caption', caption)
        async with self._lock:
            async with self.session.post(f'{self.api}/sendDocument', data=form) as resp:
                try:
                    d = await resp.json()
                except Exception:
                    d = {}
                if d.get('ok'):
                    return (d['result']['chat']['id'], d['result']['message_id'])
                return None


    async def send_inline_keyboard(self, chat_id:int, text:str, keyboard: list, reply_to_message_id=None):
        import json as _json
        payload = {'chat_id': chat_id, 'text': text, 'reply_markup': _json.dumps({'inline_keyboard': keyboard})}
        if reply_to_message_id:
            payload['reply_to_message_id'] = reply_to_message_id
        async with self._lock:
            async with self.session.post(f'{self.api}/sendMessage', json=payload) as resp:
                try:
                    d = await resp.json()
                except Exception:
                    d = {}
                if d.get('ok'):
                    return (d['result']['chat']['id'], d['result']['message_id'])
                return None

    async def answer_callback(self, callback_query_id: str, text: str = None, show_alert: bool = False):
        data = {'callback_query_id': callback_query_id}
        if text:
            data['text'] = text
            data['show_alert'] = show_alert
        async with self._lock:
            async with self.session.post(f'{self.api}/answerCallbackQuery', json=data) as resp:
                try:
                    return await resp.json()
                except Exception:
                    return {}
