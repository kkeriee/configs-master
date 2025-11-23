import re, base64, json
from typing import Dict, Optional
from urllib.parse import urlparse, unquote

# detect and parse common proxy URIs (vmess, vless, trojan, ss, ssr, hysteria)
def parse_raw_config(raw: str) -> Dict:
    data = {'raw': raw, 'protocol': None, 'host': None, 'port': None, 'meta': {}}
    s = raw.strip()
    low = s.lower()
    try:
        if low.startswith('vmess://'):
            data['protocol'] = 'vmess'
            b = s[8:]
            # vmess may be base64 of JSON
            try:
                dec = base64.b64decode(b + '=' * ((4 - len(b) % 4) % 4)).decode('utf-8', errors='ignore')
                j = json.loads(dec)
                # vmess fields: add host/port
                if 'add' in j:
                    data['host'] = j.get('add')
                if 'port' in j:
                    try:
                        data['port'] = int(j.get('port'))
                    except Exception:
                        data['port'] = None
                data['meta'] = j
            except Exception:
                pass
        elif low.startswith('vless://'):
            data['protocol'] = 'vless'
            # URL-like: vless://uuid@host:port?params
            try:
                p = urlparse(s)
                if p.hostname:
                    data['host'] = p.hostname
                if p.port:
                    data['port'] = p.port
            except Exception:
                pass
        elif low.startswith('trojan://'):
            data['protocol'] = 'trojan'
            try:
                p = urlparse(s)
                if p.hostname:
                    data['host'] = p.hostname
                if p.port:
                    data['port'] = p.port
            except Exception:
                pass
        elif low.startswith('ss://'):
            data['protocol'] = 'shadowsocks'
            # ss://<base64-params>@host:port or ss://method:pass@host:port
            try:
                part = s[5:]
                if '@' in part:
                    creds, hostpart = part.split('@',1)
                    hp = hostpart
                    if ':' in hp:
                        host, port = hp.split(':',1)
                        data['host'] = host
                        try:
                            data['port'] = int(port)
                        except:
                            pass
                else:
                    # sometimes ss://base64(method:passwd)@host:port - attempt to parse
                    m = re.search(r'@(\[\w\.-]+):(\d+)', s)
                    if m:
                        data['host'] = m.group(1)
                        data['port'] = int(m.group(2))
            except Exception:
                pass
        elif low.startswith('ssr://'):
            data['protocol'] = 'shadowsocksr'
        elif 'hysteria' in low:
            data['protocol'] = 'hysteria'
        # generic IP:port pattern fallback
        if not data['host']:
            m = re.search(r'\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})\b', s)
            if m:
                data['host'] = m.group(1)
                data['port'] = int(m.group(2))
            else:
                # attempt to extract hostname from host:port or URL
                m2 = re.search(r'([\w\.-]+)[:](\d{2,5})', s)
                if m2:
                    data['host'] = m2.group(1)
                    try:
                        data['port'] = int(m2.group(2))
                    except:
                        pass
    except Exception:
        pass
    return data
