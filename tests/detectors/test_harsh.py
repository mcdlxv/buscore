from buscore.rules.detectors.harsh import HarshDetector
from tests.detectors.conftest import sig

def test_hard_brake_fires(ctx):
    det = HarshDetector()
    assert det.process(sig(0, 50), ctx) is None      # первая точка — только запомнить
    ev = det.process(sig(2, 20), ctx)                # -30 км/ч за 2 с = -4.17 м/с²
    assert ev is not None and ev.type == "harsh_brake"
    assert ev.details["accel_ms2"] < -4

def test_hard_accel_fires(ctx):
    det = HarshDetector()
    det.process(sig(0, 10), ctx)
    ev = det.process(sig(2, 32), ctx)                # +22 км/ч за 2 с = +3.06 м/с²
    assert ev is not None and ev.type == "harsh_accel"

def test_smooth_driving_silent(ctx):
    det = HarshDetector()
    det.process(sig(0, 40), ctx)
    assert det.process(sig(5, 45), ctx) is None

def test_large_gap_ignored(ctx):
    det = HarshDetector()
    det.process(sig(0, 60), ctx)
    assert det.process(sig(60, 0), ctx) is None      # разрыв данных — не манёвр
