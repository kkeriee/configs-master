import aiohttp, asyncio
from typing import List, Dict
from ..parsers.parse_config import parse_raw_config

class GithubCollector:
    def __init__(self):
        # raw.githubusercontent URLs for the three provided lists
        self.sample_urls = [
            'https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/all_extracted_configs.txt',
            'https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt',
            'https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt',
        ]

    async def collect(self) -> List[Dict]:
        results = []
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in self.sample_urls:
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            continue
                        text = await resp.text()
                        # naive split lines, filter comments
                        for line in text.splitlines():
                            line = line.strip()
                            if not line or line.startswith('#'):
                                continue
                            parsed = parse_raw_config(line)
                            parsed['raw'] = line
                            parsed['collected_from'] = url
                            results.append(parsed)
                except Exception:
                    continue
        return results
