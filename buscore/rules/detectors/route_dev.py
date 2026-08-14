from buscore.rules.engine import BusContext
from buscore.schemas import EventCandidate, Signal

class RouteDeviationDetector:
    event_type = "route_deviation"

    def process(self, sig: Signal, ctx: BusContext) -> EventCandidate | None:
        st = ctx.state.setdefault(self.event_type,
                                  {"dev_since": None, "max_m": 0.0, "fired": False})
        distance = ctx.geometry.distance_m(sig.lat, sig.lon)
        if distance > ctx.cfg("route_dev_m"):
            if st["dev_since"] is None:
                st.update(dev_since=sig.ts, max_m=distance, fired=False)
            st["max_m"] = max(st["max_m"], distance)
            duration = (sig.ts - st["dev_since"]).total_seconds()
            if duration >= ctx.cfg("route_dev_min_s") and not st["fired"]:
                st["fired"] = True
                return EventCandidate(type=self.event_type,
                                      ts_start=st["dev_since"], ts_end=sig.ts,
                                      lat=sig.lat, lon=sig.lon,
                                      details={"distance_m": round(st["max_m"]),
                                               "duration_s": round(duration)})
        else:
            st.update(dev_since=None, max_m=0.0, fired=False)
        return None
