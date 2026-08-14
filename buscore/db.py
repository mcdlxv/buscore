from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from buscore.config import get_settings

class Base(DeclarativeBase):
    pass

engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
SessionMaker = async_sessionmaker(engine, expire_on_commit=False)
