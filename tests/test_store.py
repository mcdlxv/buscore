from datetime import datetime, timezone
from sqlalchemy import select
from scripts.seed_demo import seed
from buscore import models as m
from buscore.events.store import EventStore
from buscore.schemas import EventCandidate

async def test_publish_creates_event_resolves_driver_and_broadcasts(session):
    await seed(session)
    store = EventStore()
    sub = store.subscribe()
    cand = EventCandidate(type="speeding",
                          ts_start=datetime.now(timezone.utc),
                          lat=43.24, lon=76.945, details={"speed_max": 67})
    event_id = await store.publish(session, "117", cand)
    ev = await session.get(m.Event, event_id)
    driver = await session.scalar(select(m.Driver))
    assert ev.type == "speeding" and ev.status == "new"
    assert ev.driver_id == driver.id            # водитель найден по смене
    got = await sub.get()
    assert got["id"] == event_id

async def test_verdict_reject_requires_reason(session):
    await seed(session)
    store = EventStore()
    cand = EventCandidate(type="speeding", ts_start=datetime.now(timezone.utc),
                          lat=43.24, lon=76.945)
    event_id = await store.publish(session, "117", cand)
    user = await session.scalar(select(m.User).where(m.User.login == "sb"))
    ok = await store.verdict(session, event_id, "rejected", user.id, reason=None)
    assert ok is False
    ok = await store.verdict(session, event_id, "rejected", user.id,
                             reason="ложное срабатывание")
    assert ok is True
    ev = await session.get(m.Event, event_id)
    assert ev.status == "rejected" and ev.reject_reason == "ложное срабатывание"
