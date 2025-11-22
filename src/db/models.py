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
