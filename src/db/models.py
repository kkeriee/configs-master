from sqlalchemy import Table, Column, Integer, String, MetaData, Text, Boolean, DateTime, func, Index
metadata = MetaData()

configs = Table(
    'configs',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('raw', Text, nullable=False),
    Column('raw_hash', String(64), nullable=False),
    Column('protocol', String(50)),
    Column('host', String(255)),
    Column('port', Integer),
    Column('ip', String(45)),
    Column('country', String(10)),
    Column('ok', Boolean, default=False),
    Column('collected_from', String(255)),
    Column('created_at', DateTime, server_default=func.now()),
)

# index on raw_hash to ensure uniqueness lookups are fast
Index('ix_configs_raw_hash', configs.c.raw_hash, unique=False)


# --- Added tables for checkpointing jobs and raw entries ---
from sqlalchemy import Table, Column, Integer, String, Boolean, Text, DateTime, MetaData, func
from datetime import datetime

# ensure metadata exists
try:
    metadata
except NameError:
    metadata = MetaData()

raw_entries = Table(
    'raw_entries', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('raw', Text, nullable=False),
    Column('raw_hash', String(128), nullable=False, index=True),
    Column('collected_from', String(256)),
    Column('processed', Boolean, default=False, index=True),
    Column('ok', Boolean, default=None),
    Column('host', String(256)),
    Column('port', Integer),
    Column('ip', String(64)),
    Column('country', String(8)),
    Column('created_at', DateTime, server_default=func.now()),
)

jobs = Table(
    'jobs', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(128)),
    Column('status', String(32), default='pending'),  # pending, running, paused, done, failed
    Column('total_entries', Integer, default=0),
    Column('processed_count', Integer, default=0),
    Column('inserted', Integer, default=0),
    Column('started_at', DateTime, default=datetime.utcnow),
    Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
    Column('meta', Text, default=''),
)
