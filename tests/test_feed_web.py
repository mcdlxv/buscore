import httpx
from datetime import datetime, timezone
from sqlalchemy import select
from scripts.seed_demo import seed
from buscore import models as m
from buscore.schemas import EventCandidate
from buscore.web.app import create_app

async def _client(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")

async def _publish_one(app, session):
    cand = EventCandidate(type="speeding", ts_start=datetime.now(timezone.utc),
                          lat=43.24, lon=76.945,
                          details={"speed_max": 67, "limit_kmh": 50})
    return await app.state.store.publish(session, "117", cand)

async def test_feed_page_renders_event_card(session):
    await seed(session)
    app = create_app()
    event_id = await _publish_one(app, session)
    async with await _client(app) as c:
        r = await c.get("/feed/cards")
    assert r.status_code == 200
    assert "117" in r.text and "Превышение скорости" in r.text
    assert f'id="card-{event_id}"' in r.text

async def test_verdict_endpoint_confirms(session):
    await seed(session)
    app = create_app()
    event_id = await _publish_one(app, session)
    async with await _client(app) as c:
        r = await c.post(f"/events/{event_id}/verdict",
                         data={"status": "confirmed"})
    assert r.status_code == 200
    ev = await session.get(m.Event, event_id)
    await session.refresh(ev)
    assert ev.status == "confirmed"

async def test_reject_without_reason_is_400(session):
    await seed(session)
    app = create_app()
    event_id = await _publish_one(app, session)
    async with await _client(app) as c:
        r = await c.post(f"/events/{event_id}/verdict",
                         data={"status": "rejected"})
    assert r.status_code == 400
