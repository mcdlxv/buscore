from sqlalchemy import select, func
from scripts.seed_demo import seed
from buscore import models as m

async def test_seed_creates_demo_world(session):
    await seed(session)
    assert (await session.scalar(select(func.count(m.Bus.id)))) == 1
    assert (await session.scalar(select(func.count(m.Stop.id)))) == 6   # 5 остановок + депо
    assert (await session.scalar(select(func.count(m.User.id)))) == 3
    cfg = await session.get(m.RuleConfig, "speeding_margin_kmh")
    assert cfg.value == 10
