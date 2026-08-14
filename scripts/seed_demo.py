"""Демо-мир: маршрут 45 (прямая ~4 км в Алматы), борт 117, смена, пользователи.

Запуск: python -m scripts.seed_demo
Пароли демо (СМЕНИТЬ на проде, см. README-RUNBOOK): admin/admin123, sb/sb123, boss/boss123
"""
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from passlib.hash import bcrypt
from buscore.config import get_settings
from buscore.db import SessionMaker
from buscore import models as m

RULE_DEFAULTS = {
    "speeding_margin_kmh": 10, "speeding_min_s": 10,
    "harsh_brake_ms2": -3.0, "harsh_accel_ms2": 2.5,
    "idle_min_s": 300, "route_dev_m": 200, "route_dev_min_s": 120,
    "stop_speed_floor_kmh": 5, "daily_budget": 30,
    "cooldown_s.speeding": 120, "cooldown_s.harsh_brake": 60,
    "cooldown_s.harsh_accel": 60, "cooldown_s.route_deviation": 600,
    "cooldown_s.stop_skip": 0, "cooldown_s.long_idle": 600,
}
# прямая на север: 4 км, остановки каждые ~800 м; 1-я точка — депо
POLY = [[43.2380, 76.9450], [43.2740, 76.9450]]
STOPS = [
    ("Депо", 43.2360, 76.9450, True),
    ("Ост. Театр", 43.2420, 76.9450, False),
    ("Ост. Рынок", 43.2492, 76.9450, False),
    ("Ост. Парк", 43.2564, 76.9450, False),
    ("Ост. Школа", 43.2636, 76.9450, False),
    ("Ост. Вокзал", 43.2708, 76.9450, False),
]

async def seed(session) -> None:
    route = m.Route(name="45", speed_limit_kmh=50, polyline=POLY)
    session.add(route)
    await session.flush()
    for i, (name, lat, lon, depot) in enumerate(STOPS):
        stop = m.Stop(name=name, lat=lat, lon=lon, radius_m=60, is_depot=depot)
        session.add(stop)
        await session.flush()
        if not depot:
            session.add(m.RouteStop(route_id=route.id, stop_id=stop.id,
                                    seq=i, mandatory=True))
    bus = m.Bus(code="117", plate="KZ 117 ABC 02", route_id=route.id)
    driver = m.Driver(full_name="Ахметов С.", personnel_no="D-045")
    session.add_all([bus, driver])
    await session.flush()
    today_local = datetime.now(ZoneInfo(get_settings().tz)).date()
    session.add(m.Shift(bus_id=bus.id, driver_id=driver.id, day=today_local))
    for key, value in RULE_DEFAULTS.items():
        session.add(m.RuleConfig(key=key, value=float(value),
                                 updated_at=datetime.now(timezone.utc)))
    admin_chat = get_settings().tg_admin_chat_id   # если задан — админ сразу получает TG
    for login, pwd, role in [("admin", "admin123", "admin"),
                             ("sb", "sb123", "security"),
                             ("boss", "boss123", "manager")]:
        session.add(m.User(login=login, password_hash=bcrypt.hash(pwd), role=role,
                           tg_chat_id=admin_chat if role == "admin" else ""))
    await session.commit()

async def main() -> None:
    async with SessionMaker() as session:
        await seed(session)
    print("Demo data seeded.")

if __name__ == "__main__":
    asyncio.run(main())
