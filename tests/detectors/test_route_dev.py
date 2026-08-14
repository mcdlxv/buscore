from buscore.rules.detectors.route_dev import RouteDeviationDetector
from tests.detectors.conftest import sig

OFF = 76.9450 + 0.004   # ~325 м восточнее маршрута

def _run(det, ctx, signals):
    return [c for s in signals if (c := det.process(s, ctx)) is not None]

def test_sustained_deviation_fires_once(ctx):
    det = RouteDeviationDetector()
    signals = [sig(t, 30.0, lat=43.2455, lon=OFF) for t in range(0, 150, 10)]
    events = _run(det, ctx, signals)
    assert len(events) == 1
    assert events[0].type == "route_deviation"
    assert events[0].details["distance_m"] > 300

def test_on_route_is_silent(ctx):
    det = RouteDeviationDetector()
    signals = [sig(t, 30.0) for t in range(0, 150, 10)]
    assert _run(det, ctx, signals) == []

def test_short_detour_is_silent(ctx):
    det = RouteDeviationDetector()
    signals = [sig(0, 30.0, lat=43.2455, lon=OFF),
               sig(60, 30.0, lat=43.2455, lon=OFF),
               sig(90, 30.0)]                      # вернулся до 120 с
    assert _run(det, ctx, signals) == []
