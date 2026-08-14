"""Синтетический день борта 117 с заложенными нарушениями.

Запуск: python -m scripts.gen_demo_day [out.jsonl]

Заложено (EXPECTED): 1 speeding (65 км/ч 20 с), 1 stop_skip («Ост. Парк» на 40
без дверей), 1 harsh_brake (50→15 за 2 с), 1 long_idle (320 с вне остановок),
1 route_deviation (~330 м восточнее 130 с). Всё остальное движение образцовое:
на каждой обслуживаемой остановке плавный тормоз, скорость 0 и открытые двери.
Плавные разгоны/торможения держатся ниже порога 2.5/-3.0 м/с².
"""
import json
import sys
from datetime import datetime, timedelta, timezone

EXPECTED = {"speeding": 1, "stop_skip": 1, "harsh_brake": 1,
            "long_idle": 1, "route_deviation": 1}

LON = 76.9450
LAT_PER_M = 1 / 110_540          # градусов широты на метр

class Day:
    def __init__(self):
        self.ts = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)  # 09:00 Алматы
        self.lat = 43.2360        # депо
        self.lon = LON
        self.rows: list[dict] = []

    def emit(self, speed_kmh: float, doors: bool = False, dt_s: float = 5.0,
             move: bool = True) -> None:
        """Один сигнал через dt_s секунд; при move=True продвигаемся на север
        со скоростью speed_kmh."""
        self.ts += timedelta(seconds=dt_s)
        if move and speed_kmh > 0:
            self.lat += speed_kmh / 3.6 * dt_s * LAT_PER_M
        self.rows.append({"bus_code": "117", "ts": self.ts.isoformat(),
                          "lat": round(self.lat, 6), "lon": round(self.lon, 6),
                          "speed_kmh": speed_kmh, "doors_open": doors})

    def drive_to(self, lat_target: float, speed_kmh: float = 40.0) -> None:
        while self.lat < lat_target:
            self.emit(speed_kmh)

    def serve_stop(self) -> None:
        """Плавно остановиться, открыть двери, поехать дальше (-1.1 м/с² макс)."""
        self.emit(20.0)
        self.emit(0.0)
        for _ in range(3):
            self.emit(0.0, doors=True)
        self.emit(20.0)

def generate() -> list[dict]:
    d = Day()
    d.emit(0.0)                       # стартовая точка в депо
    d.drive_to(43.2415)               # подъезд к «Театру» (43.2420, r=60м)
    d.serve_stop()                    # «Театр» — образцово
    d.drive_to(43.2440)

    # SPEEDING: 65 км/ч четыре тика = 15 с выше порога 60
    for _ in range(4):
        d.emit(65.0)

    d.drive_to(43.2487)               # подъезд к «Рынку» (43.2492)
    d.serve_stop()                    # «Рынок» — образцово
    d.drive_to(43.2510)               # вышли из зоны «Рынка»

    # HARSH_BRAKE: 50 → 15 за 2 с = -4.86 м/с²
    d.emit(50.0)
    d.emit(15.0, dt_s=2.0)
    d.emit(30.0)

    # LONG_IDLE: стоим 320 с между «Рынком» и «Парком» (43.2528 — вне зон)
    d.drive_to(43.2528)
    d.emit(20.0)
    d.emit(0.0)
    for _ in range(10):
        d.emit(0.0, dt_s=32.0)        # 320 секунд простоя
    d.emit(20.0)

    # STOP_SKIP: пролетаем «Парк» (43.2564) на 40 без дверей
    d.drive_to(43.2600)

    # ROUTE_DEVIATION: уход на ~330 м восточнее на 130 с, скорость 30
    for _ in range(4):
        d.lon += 0.001
        d.emit(30.0, move=False)
    for _ in range(26):
        d.emit(30.0, move=False, dt_s=5.0)   # стоим на месте по долготе — 130 с вне маршрута
    for _ in range(4):
        d.lon -= 0.001
        d.emit(30.0, move=False)

    d.drive_to(43.2631)               # подъезд к «Школе» (43.2636)
    d.serve_stop()                    # «Школа» — образцово
    d.drive_to(43.2703)               # подъезд к «Вокзалу» (43.2708)
    d.serve_stop()                    # «Вокзал» — образцово
    d.drive_to(43.2725)               # ушли из зоны «Вокзала»
    return d.rows

def main() -> None:
    out = sys.argv[1] if len(sys.argv) > 1 else "demo_day.jsonl"
    rows = generate()
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r) for r in rows))
    print(f"{out}: {len(rows)} signals")

if __name__ == "__main__":
    main()
