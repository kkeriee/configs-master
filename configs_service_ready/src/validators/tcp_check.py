import asyncio
from typing import Dict

async def tcp_check(config: Dict, timeout: float = 3.0) -> bool:
    # Try to extract host/port from config dict; if not present, fail fast.
    host = config.get('host')
    port = config.get('port') or 443
    if not host:
        return False
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except Exception:
        return False
