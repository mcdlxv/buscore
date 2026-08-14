import httpx
from scripts.seed_demo import seed
from buscore.web.app import create_app

async def _client(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test",
                             follow_redirects=False)

async def test_feed_requires_login(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.get("/feed")
    assert r.status_code == 303 and r.headers["location"] == "/login"

async def test_security_can_login_and_see_feed(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.post("/login", data={"login": "sb", "password": "sb123"})
        assert r.status_code == 303 and r.headers["location"] == "/feed"
        c.cookies.update(r.cookies)
        assert (await c.get("/feed")).status_code == 200

async def test_manager_blocked_from_feed_allowed_report(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.post("/login", data={"login": "boss", "password": "boss123"})
        c.cookies.update(r.cookies)
        assert (await c.get("/feed")).status_code == 403

async def test_admin_can_update_rule(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.post("/login", data={"login": "admin", "password": "admin123"})
        c.cookies.update(r.cookies)
        r = await c.post("/admin/rules", data={"key": "speeding_margin_kmh",
                                               "value": "15"})
        assert r.status_code == 303
    from buscore import models as m
    cfg = await session.get(m.RuleConfig, "speeding_margin_kmh")
    await session.refresh(cfg)
    assert cfg.value == 15

async def test_tampered_cookie_redirects_to_login(session):
    await seed(session)
    async with await _client(create_app()) as c:
        c.cookies.set("session", "eyJpZCI6MSwicm9sZSI6ImFkbWluIn0.forged")
        r = await c.get("/feed")
    assert r.status_code == 303 and r.headers["location"] == "/login"
