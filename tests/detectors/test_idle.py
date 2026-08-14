from buscore.rules.detectors.idle import LongIdleDetector
from tests.detectors.conftest import sig

MID = dict(lat=43.2455, lon=76.9450)      # на маршруте, вне зон остановок
AT_STOP = dict(lat=43.2492, lon=76.9450)  # остановка «Рынок»

def _run(det, ctx, signals):
    return [c for s in signals if (c := det.process(s, ctx)) is not None]

def test_idle_outside_stops_fires_once(ctx):
    det = LongIdleDetector()
    signals = [sig(t, 0.0, **MID) for t in range(0, 330, 30)]  # 300+ c стоим
    events = _run(det, ctx, signals)
    assert len(events) == 1
    assert events[0].type == "long_idle"
    assert events[0].details["idle_s"] >= 300

def test_idle_at_stop_zone_is_silent(ctx):
    det = LongIdleDetector()
    signals = [sig(t, 0.0, **AT_STOP) for t in range(0, 330, 30)]
    assert _run(det, ctx, signals) == []

def test_moving_resets_timer(ctx):
    det = LongIdleDetector()
    signals = [sig(0, 0.0, **MID), sig(150, 0.0, **MID),
               sig(180, 20.0, **MID),                    # поехали — сброс
               sig(200, 0.0, **MID), sig(400, 0.0, **MID)]
    assert _run(det, ctx, signals) == []                 # каждый отрезок < 300 с
