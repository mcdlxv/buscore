from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func
from scripts.seed_demo import seed
from buscore import models as m
from buscore.maintenance import purge_old_signals

async def test_purge_deletes_only_old_signals(session):
    await seed(session)
    bus = await session.scalar(select(m.Bus))
    now = datetime.now(timezone.utc)
    session.add_all([
        m.SignalRow(bus_id=bus.id, ts=now - timedelta(days=100), lat=1, lon=1,
                    speed_kmh=0),
        m.SignalRow(bus_id=bus.id, ts=now - timedelta(days=1), lat=1, lon=1,
                    speed_kmh=0)])
    await session.commit()
    deleted = await purge_old_signals(session, now)
    assert deleted == 1
    assert (await session.scalar(select(func.count(m.SignalRow.id)))) == 1
