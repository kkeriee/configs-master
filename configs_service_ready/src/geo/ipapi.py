import aiohttp, asyncio, os
from typing import List, Dict

IPAPI_BATCH = int(os.getenv('IPAPI_BATCH_SIZE', '100'))
IPAPI_URL = 'http://ip-api.com/batch'

async def ipapi_batch_lookup(ips: List[str]) -> List[Dict]:
    # ips expected to be list of IP strings
    results = []
    if not ips:
        return results
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(ips), IPAPI_BATCH):
            batch = ips[i:i+IPAPI_BATCH]
            payload = [{'query': ip} for ip in batch]
            try:
                async with session.post(IPAPI_URL, json=payload, timeout=20) as resp:
                    data = await resp.json()
                    results.extend(data)
            except Exception:
                # on error, append placeholders
                for ip in batch:
                    results.append({'query': ip, 'status': 'fail'})
    return results
