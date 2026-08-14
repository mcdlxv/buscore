from buscore.rules.engine import BusContext, StopZone
from buscore.schemas import EventCandidate, Signal

class StopSkipDetector:
    """Стейт-машина на обязательную остановку: вход в зону → трекинг
    min-скорости и дверей → выход из зоны → вердикт (спека §4:
    пропуск = не остановился И двери не открылись)."""

    event_type = "stop_skip"

    def process(self, sig: Signal, ctx: BusContext) -> EventCandidate | None:
        tracking: dict = ctx.state.setdefault(self.event_type, {})
        result: EventCandidate | None = None
        for zone in ctx.stops:
            if not zone.mandatory:
                continue
            inside = (ctx.geometry.haversine_m(sig.lat, sig.lon, zone.lat, zone.lon)
                      <= zone.radius_m)
            tr = tracking.get(zone.stop_id)
            if inside:
                if tr is None:
                    tracking[zone.stop_id] = {"min_speed": sig.speed_kmh,
                                              "doors": sig.doors_open,
                                              "entered": sig.ts}
                else:
                    tr["min_speed"] = min(tr["min_speed"], sig.speed_kmh)
                    tr["doors"] = tr["doors"] or sig.doors_open
            elif tr is not None:
                stopped = tr["min_speed"] <= ctx.cfg("stop_speed_floor_kmh")
                if not stopped and not tr["doors"]:
                    result = EventCandidate(
                        type=self.event_type, ts_start=tr["entered"], ts_end=sig.ts,
                        lat=zone.lat, lon=zone.lon,
                        details={"stop": zone.name,
                                 "min_speed_kmh": round(tr["min_speed"], 1),
                                 "doors_opened": tr["doors"]})
                del tracking[zone.stop_id]
        return result
