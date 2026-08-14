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
