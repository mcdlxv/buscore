import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from buscore.config import get_settings

@pytest.fixture
async def engine():
    eng = create_async_engine(get_settings().database_url)
    yield eng
    await eng.dispose()

@pytest.fixture
async def clean_tables(engine):
    """Чистим данные перед тестом (таблицы создаёт alembic)."""
    from sqlalchemy import text
    async with engine.begin() as conn:
        for t in ["events", "signals", "shifts", "route_stops", "stops",
                  "routes", "clips", "rule_configs", "users", "drivers", "buses"]:
            await conn.execute(text(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE'))
    yield

@pytest.fixture
async def session(engine, clean_tables):
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
