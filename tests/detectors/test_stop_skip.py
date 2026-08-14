from buscore.rules.detectors.stop_skip import StopSkipDetector
from tests.detectors.conftest import sig

# остановка «Рынок»: 43.2492, 76.9450, радиус 60 м
BEFORE = dict(lat=43.2480, lon=76.9450)   # ~130 м до — вне зоны
INSIDE = dict(lat=43.2492, lon=76.9450)
AFTER  = dict(lat=43.2505, lon=76.9450)   # ~145 м после — вне зоны

def _run(det, ctx, signals):
    return [c for s in signals if (c := det.process(s, ctx)) is not None]

def test_fast_pass_without_doors_fires(ctx):
    det = StopSkipDetector()
    signals = [sig(0, 40, **BEFORE), sig(5, 40, **INSIDE), sig(10, 40, **AFTER)]
    events = _run(det, ctx, signals)
    assert len(events) == 1
    ev = events[0]
    assert ev.type == "stop_skip"
    assert ev.details["stop"] == "Ост. Рынок"
    assert ev.details["min_speed_kmh"] == 40

def test_stop_with_doors_is_silent(ctx):
    det = StopSkipDetector()
    signals = [sig(0, 40, **BEFORE), sig(5, 0, **INSIDE, doors=True),
               sig(30, 40, **AFTER)]
    assert _run(det, ctx, signals) == []

def test_slow_rolling_counts_as_stopped(ctx):
    det = StopSkipDetector()
    # min скорость 3 км/ч < порога 5 — «остановился», даже без дверей (спека §4: «и»)
    signals = [sig(0, 40, **BEFORE), sig(5, 3, **INSIDE), sig(10, 40, **AFTER)]
    assert _run(det, ctx, signals) == []
