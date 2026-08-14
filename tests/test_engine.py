from datetime import datetime, timezone, timedelta
from buscore.rules.engine import FilterChain
from buscore.schemas import EventCandidate

T0 = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)

def _cand(sec, type_="speeding"):
    return EventCandidate(type=type_, ts_start=T0 + timedelta(seconds=sec),
                          lat=43.24, lon=76.945)

def test_cooldown_suppresses_repeats():
    f = FilterChain(cooldowns={"speeding": 120}, daily_budget=30, tz="Asia/Almaty")
    assert f.allow("117", _cand(0)) is True
    assert f.allow("117", _cand(60)) is False    # внутри кулдауна
    assert f.allow("117", _cand(130)) is True    # кулдаун истёк

def test_cooldown_is_per_bus_and_type():
    f = FilterChain(cooldowns={"speeding": 120}, daily_budget=30, tz="Asia/Almaty")
    assert f.allow("117", _cand(0)) is True
    assert f.allow("203", _cand(1)) is True                       # другой борт
    assert f.allow("117", _cand(2, type_="long_idle")) is True    # другой тип

def test_daily_budget_caps_publications():
    f = FilterChain(cooldowns={}, daily_budget=2, tz="Asia/Almaty")
    assert f.allow("117", _cand(0)) is True
    assert f.allow("117", _cand(10)) is True
    assert f.allow("117", _cand(20)) is False    # бюджет исчерпан
    assert f.budget_exceeded_today(_cand(20).ts_start) is True

def test_day_rollover_only_moves_forward():
    """Ретроспективные точки (догрузка истории) не сбрасывают бюджет дня."""
    f = FilterChain(cooldowns={}, daily_budget=2, tz="Asia/Almaty")
    assert f.allow("117", _cand(0)) is True
    assert f.allow("117", _cand(10)) is True
    assert f.allow("117", _cand(20)) is False            # бюджет исчерпан
    yesterday = _cand(0)
    yesterday.ts_start -= timedelta(days=1)
    f.allow("117", yesterday)                            # вчерашняя точка
    assert f.allow("117", _cand(30)) is False            # счётчик не сброшен
    assert f.suppressed_today >= 2

async def test_engine_calls_budget_callback():
    from buscore.rules.engine import EventEngine
    calls = []
    async def publish(bus, cand):
        pass
    async def on_budget(cand):
        calls.append(cand.type)
    class AlwaysFire:
        event_type = "speeding"
        def process(self, sig, ctx):
            return _cand(int(sig.ts.timestamp()) % 100000)
    f = FilterChain(cooldowns={}, daily_budget=1, tz="Asia/Almaty")
    eng = EventEngine([AlwaysFire()], f, publish, on_budget_exceeded=on_budget)
    from buscore.rules.engine import BusContext
    from buscore.geo import RouteGeometry
    eng.contexts["117"] = BusContext(
        bus_code="117", route_limit_kmh=50,
        geometry=RouteGeometry([[43.0, 76.0], [43.1, 76.0]]), stops=[], config={})
    from buscore.schemas import Signal
    s1 = Signal(bus_code="117", ts=T0, lat=43.0, lon=76.0, speed_kmh=0)
    s2 = Signal(bus_code="117", ts=T0 + timedelta(seconds=5), lat=43.0,
                lon=76.0, speed_kmh=0)
    await eng.process(s1)     # публикуется (бюджет 1)
    await eng.process(s2)     # блок по бюджету -> колбэк
    assert calls == ["speeding"]
