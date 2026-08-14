from buscore.rules.engine import BusContext
from buscore.schemas import EventCandidate, Signal

MAX_GAP_S = 15  # больше — считаем разрывом данных, ускорение не считаем

class HarshDetector:
    event_type = "harsh"   # публикует harsh_brake | harsh_accel

    def process(self, sig: Signal, ctx: BusContext) -> EventCandidate | None:
        st = ctx.state.setdefault(self.event_type, {"prev": None})
        prev = st["prev"]
        st["prev"] = (sig.ts, sig.speed_kmh)
        if prev is None:
            return None
        dt = (sig.ts - prev[0]).total_seconds()
        if dt <= 0 or dt > MAX_GAP_S:
            return None
        accel = (sig.speed_kmh - prev[1]) / 3.6 / dt   # м/с²
        if accel <= ctx.cfg("harsh_brake_ms2"):
            kind = "harsh_brake"
        elif accel >= ctx.cfg("harsh_accel_ms2"):
            kind = "harsh_accel"
        else:
            return None
        return EventCandidate(type=kind, ts_start=sig.ts, lat=sig.lat,
                              lon=sig.lon,
                              details={"accel_ms2": round(accel, 2),
                                       "from_kmh": prev[1], "to_kmh": sig.speed_kmh})
