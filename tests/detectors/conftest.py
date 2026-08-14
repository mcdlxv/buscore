import pytest
from datetime import datetime, timezone, timedelta
from buscore.geo import RouteGeometry
from buscore.rules.engine import BusContext, StopZone
from buscore.schemas import Signal
from scripts.seed_demo import RULE_DEFAULTS, POLY, STOPS

T0 = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)

@pytest.fixture
def ctx() -> BusContext:
    stops = [StopZone(stop_id=i, name=n, lat=lat, lon=lon, radius_m=60,
                      mandatory=not depot, is_depot=depot)
             for i, (n, lat, lon, depot) in enumerate(STOPS)]
    return BusContext(bus_code="117", route_limit_kmh=50,
                      geometry=RouteGeometry(POLY), stops=stops,
                      config=dict(RULE_DEFAULTS))

def sig(sec: float, speed: float, lat: float = 43.2455, lon: float = 76.9450,
        doors: bool = False) -> Signal:
    return Signal(bus_code="117", ts=T0 + timedelta(seconds=sec),
                  lat=lat, lon=lon, speed_kmh=speed, doors_open=doors)
