import os
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from .models import metadata
from sqlalchemy import text

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///./v2collector.db')
engine: AsyncEngine = None

def get_engine():
    global engine
    if engine is None:
        engine = create_async_engine(DATABASE_URL, future=True, echo=False)
    return engine

async def init_db():
    eng = get_engine()
    # create tables (run sync via run_sync)
    async with eng.begin() as conn:
        await conn.run_sync(metadata.create_all)
