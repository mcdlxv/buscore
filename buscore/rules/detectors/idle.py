from buscore.rules.engine import BusContext
from buscore.schemas import EventCandidate, Signal

STANDSTILL_KMH = 1.0

class LongIdleDetector:
    event_type = "long_idle"

    def _in_any_zone(self, sig: Signal, ctx: BusContext) -> bool:
        return any(ctx.geometry.haversine_m(sig.lat, sig.lon, z.lat, z.lon)
                   <= z.radius_m for z in ctx.stops)

    def process(self, sig: Signal, ctx: BusContext) -> EventCandidate | None:
        st = ctx.state.setdefault(self.event_type,
                                  {"idle_since": None, "fired": False})
        if sig.speed_kmh < STANDSTILL_KMH and not self._in_any_zone(sig, ctx):
            if st["idle_since"] is None:
                st.update(idle_since=sig.ts, fired=False)
            idle_s = (sig.ts - st["idle_since"]).total_seconds()
            if idle_s >= ctx.cfg("idle_min_s") and not st["fired"]:
                st["fired"] = True
                return EventCandidate(type=self.event_type,
                                      ts_start=st["idle_since"], ts_end=sig.ts,
                                      lat=sig.lat, lon=sig.lon,
                                      details={"idle_s": round(idle_s)})
        else:
            st.update(idle_since=None, fired=False)
        return None
