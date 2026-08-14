from buscore.rules.detectors.speeding import SpeedingDetector
from tests.detectors.conftest import sig

def _run(det, ctx, signals):
    return [c for s in signals if (c := det.process(s, ctx)) is not None]

def test_sustained_overspeed_fires_once(ctx):
    det = SpeedingDetector()
    # лимит 50 + допуск 10 = порог 60; едем 65 дольше 10 с
    events = _run(det, ctx, [sig(0, 65), sig(5, 66), sig(10, 67), sig(15, 65)])
    assert len(events) == 1
    ev = events[0]
    assert ev.type == "speeding"
    assert ev.details["speed_max"] == 67
    assert ev.details["limit_kmh"] == 50
    assert ev.ts_start.second == 0          # начало эпизода, не момент срабатывания

def test_short_burst_does_not_fire(ctx):
    det = SpeedingDetector()
    events = _run(det, ctx, [sig(0, 65), sig(5, 40), sig(10, 40)])
    assert events == []

def test_new_episode_fires_again(ctx):
    det = SpeedingDetector()
    signals = [sig(0, 65), sig(10, 65), sig(15, 40),       # эпизод 1 + сброс
               sig(300, 70), sig(311, 70)]                  # эпизод 2
    assert len(_run(det, ctx, signals)) == 2
