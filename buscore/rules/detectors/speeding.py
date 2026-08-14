from buscore.rules.engine import BusContext
from buscore.schemas import EventCandidate, Signal

class SpeedingDetector:
    event_type = "speeding"

    def process(self, sig: Signal, ctx: BusContext) -> EventCandidate | None:
        st = ctx.state.setdefault(self.event_type,
                                  {"over_since": None, "max": 0.0, "fired": False})
        threshold = ctx.route_limit_kmh + ctx.cfg("speeding_margin_kmh")
        if sig.speed_kmh > threshold:
            if st["over_since"] is None:
                st.update(over_since=sig.ts, max=sig.speed_kmh, fired=False)
            st["max"] = max(st["max"], sig.speed_kmh)
            duration = (sig.ts - st["over_since"]).total_seconds()
            if duration >= ctx.cfg("speeding_min_s") and not st["fired"]:
                st["fired"] = True
                return EventCandidate(
                    type=self.event_type, ts_start=st["over_since"], ts_end=sig.ts,
                    lat=sig.lat, lon=sig.lon,
                    details={"speed_max": st["max"],
                             "limit_kmh": ctx.route_limit_kmh,
                             "duration_s": round(duration)})
        else:
            st.update(over_since=None, max=0.0, fired=False)
        return None
