# V2 Config Collector - Master Service (Ready for GitHub+Render)

This repository is a near-complete implementation of the V2 config collector service:
- FastAPI webhook for Telegram
- Collectors using provided GitHub public lists (preconfigured)
- Parsers for common proxies (vmess/vless/trojan/ss/ssr/hysteria)
- Async TCP port checks
- DNS resolution and batch GeoIP lookup using ip-api.com
- Async SQLAlchemy storage (Postgres recommended)
- On-demand execution triggered by /configs (suitable for Render free tier)

## Environments (add to Render/CI)
Required:
- TELEGRAM_BOT_TOKEN - Telegram bot token
- TELEGRAM_WEBHOOK_SECRET - secret path segment for webhook URL
- DATABASE_URL - SQLAlchemy-compatible DB url (recommended: postgresql+asyncpg://user:pass@host:5432/dbname)
Optional:
- MAX_CONCURRENCY - concurrency for TCP checks (default 50)
- IPAPI_BATCH_SIZE - how many IPs to batch to ip-api (default 100)
- LOG_LEVEL - debug/info

## Commands
- `/configs` — запускает сбор конфигов, проверку, геолокацию и заполняет БД. Возвращает таблицу доступных стран и количество конфигов.
- `/get <COUNTRY_CODE> <COUNT>` — запрашивает указанное количество конфигов для страны. Пример: `/get DE 10`.

## Preconfigured collectors
The service will initially use these public lists:
- https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/all_extracted_configs.txt
- https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt
- https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt

## Deploy to Render
1. Push repo to GitHub.
2. Create new Web Service on Render, connect to repo.
3. Add environment variables (TELEGRAM_BOT_TOKEN, TELEGRAM_WEBHOOK_SECRET, DATABASE_URL).
4. Use build command: `pip install -r requirements.txt`
5. Start command (Render provides $PORT): `uvicorn src.app:app --host 0.0.0.0 --port $PORT`

## Notes & Safety
- The collector uses ip-api.com. Be mindful of their rate limits and add further pacing if needed.
- The service performs **checks only** (TCP/connect, DNS resolution, geo lookup). It does NOT forward user traffic through collected proxies.
- To avoid hitting rate limits and resource exhaustion on Render free tier, the service processes in batched and rate-limited manner and persists results to DB incrementally.
