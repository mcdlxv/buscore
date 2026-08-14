import httpx
from datetime import datetime, timezone
from scripts.seed_demo import seed
from buscore.schemas import EventCandidate
from buscore.web.app import create_app

def _client(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test",
                             follow_redirects=False)

async def _login(c, login, pwd):
    r = await c.post("/login", data={"login": login, "password": pwd})
    c.cookies.update(r.cookies)

async def _make_events(app, session, n_confirmed=2, n_rejected=1):
    store = app.state.store
    ids = []
    for _ in range(n_confirmed + n_rejected):
        ids.append(await store.publish(session, "117", EventCandidate(
            type="speeding", ts_start=datetime.now(timezone.utc),
            lat=43.24, lon=76.945)))
    for i, event_id in enumerate(ids):
        status = "confirmed" if i < n_confirmed else "rejected"
        await store.verdict(session, event_id, status, user_id=1,
                            reason=None if status == "confirmed" else "дубль")

async def test_report_counts_confirmed_by_type(session):
    await seed(session)
    app = create_app()
    await _make_events(app, session)
    async with _client(app) as c:
        await _login(c, "boss", "boss123")
        r = await c.get("/report?days=7")
    assert r.status_code == 200
    assert "Превышение скорости" in r.text and ">2<" in r.text

async def test_csv_export(session):
    await seed(session)
    app = create_app()
    await _make_events(app, session)
    async with _client(app) as c:
        await _login(c, "boss", "boss123")
        r = await c.get("/report.csv?days=7")
    assert r.status_code == 200
    lines = r.text.strip().splitlines()
    assert lines[0] == "type;bus;driver;route;confirmed;rejected"
    assert any("speeding" in line and ";2;" in line for line in lines[1:])
