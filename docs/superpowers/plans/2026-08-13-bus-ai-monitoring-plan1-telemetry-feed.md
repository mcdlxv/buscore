# План 1: Телеметрическое ядро + живая лента (replay) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Работающая живая лента событий на replay-данных: 5 телеметрических детекторов, вердикты СБ, Telegram-уведомления, отчёт — критерий успеха №1 спеки.

**Architecture:** Один async-сервис FastAPI (`buscore`) + PostgreSQL. Сигналы идут из replay-источника через конвейер `ingest → rules(детекторы) → фильтры → events → SSE-лента + Telegram`. Геоматематика — shapely в памяти (проекция в метры без PostGIS: для v1 гео-SQL не нужен — осознанное упрощение спеки, поле PostGIS добавим в Плане 3 при необходимости).

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2 async + asyncpg, Alembic, Jinja2 + HTMX + SSE (sse-starlette), shapely, passlib, httpx, Docker Compose, pytest + pytest-asyncio.

**Спека:** `docs/superpowers/specs/2026-08-13-bus-ai-monitoring-design.md` (коммит cc47033). Вне этого плана: vision (План 2), Link+ live и клипы (План 3).

---

## Карта файлов

```text
buscore/
  config.py            # настройки из env (pydantic-settings)
  db.py                # async engine + session factory + Base
  models.py            # все таблицы спеки §6
  schemas.py           # Signal, EventCandidate (pydantic)
  geo.py               # проекция в метры, LineString маршрута, геозоны остановок
  ingest/replay.py     # ReplayPlayer: JSONL → конвейер, ускорение, rebase
  rules/engine.py      # BusContext, FilterChain (дедуп/кулдаун/бюджет), EventEngine
  rules/detectors/
    speeding.py | harsh.py | idle.py | route_dev.py | stop_skip.py
  events/store.py      # publish, verdict, запросы, broadcast (asyncio)
  notify/telegram.py   # отправка, подписки, тихие часы, сводка бюджета, watchdog
  web/app.py           # фабрика приложения, /health, подключение роутеров
  web/auth.py          # логин, cookie-сессии, роли
  web/routes_feed.py   # лента: страница, партиалы, SSE, вердикты
  web/routes_report.py # отчёт + CSV
  web/routes_admin.py  # редактор порогов rule_configs
  web/templates/       # base, login, feed, _card, report, admin_rules
  main.py              # entrypoint: app + фоновые задачи (replay, watchdog, retention)
scripts/seed_demo.py   # демо-данные: борт 117, маршрут 45, остановки, пользователи
scripts/gen_demo_day.py# синтетический день с заложенными нарушениями
scripts/backup.sh, scripts/restore.sh
tests/                 # зеркалит структуру; conftest.py с БД-фикстурами
alembic/               # env.py (async) + versions/
Dockerfile, docker-compose.yml, Makefile, .env.example, README-RUNBOOK.md
.github/workflows/ci.yml
```

## Ключи конфигурации `rule_configs` (единые имена во всех задачах)

| key | default | смысл |
| --- | --- | --- |
| `speeding_margin_kmh` | 10 | допуск к лимиту маршрута |
| `speeding_min_s` | 10 | длительность превышения |
| `harsh_brake_ms2` | -3.0 | порог торможения, м/с² |
| `harsh_accel_ms2` | 2.5 | порог ускорения, м/с² |
| `idle_min_s` | 300 | стоянка вне остановок |
| `route_dev_m` | 200 | отход от полилинии |
| `route_dev_min_s` | 120 | длительность отхода |
| `stop_speed_floor_kmh` | 5 | «не остановился», если min скорость выше |
| `cooldown_s.<type>` | 120/60/600/0/600 | кулдаун по типам (speeding/harsh/route_deviation/stop_skip/long_idle) |
| `daily_budget` | 30 | публикаций в день на парк |

Сигнал везде один и тот же (`buscore/schemas.py`): `Signal(bus_code: str, ts: datetime, lat: float, lon: float, speed_kmh: float, doors_open: bool)`.

---

### Task 1: Скелет проекта, Docker Compose, /health

**Files:**
- Create: `requirements.txt`, `requirements-dev.txt`, `pytest.ini`, `.env.example`
- Create: `buscore/__init__.py`, `buscore/config.py`, `buscore/web/__init__.py`, `buscore/web/app.py`, `buscore/main.py`
- Create: `Dockerfile`, `docker-compose.yml`
- Test: `tests/test_health.py`

- [ ] **Step 1: Зависимости и pytest**

`requirements.txt`:
```text
fastapi>=0.115
uvicorn[standard]>=0.30
sqlalchemy[asyncio]>=2.0
asyncpg>=0.29
alembic>=1.13
pydantic-settings>=2.2
jinja2>=3.1
python-multipart>=0.0.9
itsdangerous>=2.2
passlib[bcrypt]>=1.7
httpx>=0.27
shapely>=2.0
sse-starlette>=2.1
```

`requirements-dev.txt`:
```text
-r requirements.txt
pytest>=8.2
pytest-asyncio>=0.23
```

`pytest.ini`:
```ini
[pytest]
asyncio_mode = auto
testpaths = tests
```

- [ ] **Step 2: Провальный тест /health**

`tests/test_health.py`:
```python
import httpx
from buscore.web.app import create_app

async def test_health_returns_ok():
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
```

Run: `pip install -r requirements-dev.txt && pytest tests/test_health.py -v`
Expected: FAIL — `ModuleNotFoundError: buscore`

- [ ] **Step 3: Конфиг и приложение**

`buscore/__init__.py` — пустой. `buscore/config.py`:
```python
from functools import lru_cache
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://bus:bus@localhost:5432/buscore"
    session_secret: str = "dev-secret-change-me"
    tz: str = "Asia/Almaty"
    tg_bot_token: str = ""
    tg_admin_chat_id: str = ""
    quiet_start_hour: int = 22   # тихие часы 22:00–05:00
    quiet_end_hour: int = 5
    work_start_hour: int = 5     # рабочие часы парка 05:00–22:00
    work_end_hour: int = 22
    replay_file: str | None = None
    replay_speed: float = 60.0
    replay_rebase: bool = True

    model_config = {"env_file": ".env", "extra": "ignore"}

@lru_cache
def get_settings() -> Settings:
    return Settings()
```

`buscore/web/__init__.py` — пустой. `buscore/web/app.py`:
```python
from fastapi import FastAPI

def create_app() -> FastAPI:
    app = FastAPI(title="buscore")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
```

`buscore/main.py`:
```python
from buscore.web.app import create_app

app = create_app()
```

- [ ] **Step 4: Тест проходит**

Run: `pytest tests/test_health.py -v`
Expected: PASS

- [ ] **Step 5: Docker**

`.env.example`:
```text
DATABASE_URL=postgresql+asyncpg://bus:bus@db:5432/buscore
SESSION_SECRET=change-me-in-production
TG_BOT_TOKEN=
TG_ADMIN_CHAT_ID=
REPLAY_FILE=
```

`Dockerfile`:
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "buscore.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`docker-compose.yml`:
```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: bus
      POSTGRES_PASSWORD: bus
      POSTGRES_DB: buscore
    volumes: ["pgdata:/var/lib/postgresql/data"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bus -d buscore"]
      interval: 5s
      timeout: 3s
      retries: 10
    restart: unless-stopped
  app:
    build: .
    env_file: .env
    ports: ["8000:8000"]
    depends_on:
      db: {condition: service_healthy}
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request;urllib.request.urlopen('http://localhost:8000/health')"]
      interval: 30s
      timeout: 5s
      retries: 3
    restart: unless-stopped
volumes:
  pgdata:
```

Run: `cp .env.example .env && docker compose up -d --build && curl http://localhost:8000/health`
Expected: `{"status":"ok"}`

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: project skeleton, docker compose, /health"
```

### Task 2: Схема БД — модели и миграция

**Files:**
- Create: `buscore/db.py`, `buscore/models.py`
- Create: `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako` (через `alembic init`)
- Create: `tests/conftest.py`
- Test: `tests/test_schema.py`

- [ ] **Step 1: Провальный тест схемы**

`tests/conftest.py`:
```python
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from buscore.config import get_settings

@pytest.fixture
async def engine():
    eng = create_async_engine(get_settings().database_url)
    yield eng
    await eng.dispose()

@pytest.fixture
async def session(engine):
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s

@pytest.fixture(autouse=True)
async def clean_tables(engine):
    """Перед каждым тестом чистим данные (таблицы создаёт alembic)."""
    from sqlalchemy import text
    async with engine.begin() as conn:
        for t in ["events", "signals", "shifts", "route_stops", "stops",
                  "routes", "clips", "rule_configs", "users", "drivers", "buses"]:
            await conn.execute(text(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE'))
    yield
```

`tests/test_schema.py`:
```python
from sqlalchemy import inspect

async def test_all_tables_exist(engine):
    async with engine.connect() as conn:
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
    expected = {"buses", "drivers", "shifts", "routes", "stops", "route_stops",
                "signals", "events", "clips", "rule_configs", "users"}
    assert expected.issubset(set(names))
```

Run: `docker compose up -d db && pytest tests/test_schema.py -v`
Expected: FAIL — таблиц нет

- [ ] **Step 2: db.py и модели**

`buscore/db.py`:
```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from buscore.config import get_settings

class Base(DeclarativeBase):
    pass

engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
SessionMaker = async_sessionmaker(engine, expire_on_commit=False)
```

`buscore/models.py`:
```python
from datetime import datetime, date
from sqlalchemy import (String, Integer, Float, Boolean, DateTime, Date,
                        ForeignKey, JSON, Text, UniqueConstraint, Index)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from buscore.db import Base

class Bus(Base):
    __tablename__ = "buses"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(16), unique=True)       # борт «117»
    plate: Mapped[str] = mapped_column(String(16), default="")
    link_id: Mapped[str] = mapped_column(String(64), default="")     # id в Link+ (План 3)
    route_id: Mapped[int | None] = mapped_column(ForeignKey("routes.id"))

class Driver(Base):
    __tablename__ = "drivers"
    id: Mapped[int] = mapped_column(primary_key=True)
    full_name: Mapped[str] = mapped_column(String(128))
    personnel_no: Mapped[str] = mapped_column(String(32), default="")

class Shift(Base):
    __tablename__ = "shifts"
    id: Mapped[int] = mapped_column(primary_key=True)
    bus_id: Mapped[int] = mapped_column(ForeignKey("buses.id"))
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id"))
    day: Mapped[date] = mapped_column(Date)
    __table_args__ = (UniqueConstraint("bus_id", "day"),)

class Route(Base):
    __tablename__ = "routes"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32))                    # «45»
    speed_limit_kmh: Mapped[float] = mapped_column(Float, default=60)
    polyline: Mapped[list] = mapped_column(JSON)                     # [[lat, lon], ...]

class Stop(Base):
    __tablename__ = "stops"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    radius_m: Mapped[float] = mapped_column(Float, default=60)
    is_depot: Mapped[bool] = mapped_column(Boolean, default=False)

class RouteStop(Base):
    __tablename__ = "route_stops"
    id: Mapped[int] = mapped_column(primary_key=True)
    route_id: Mapped[int] = mapped_column(ForeignKey("routes.id"))
    stop_id: Mapped[int] = mapped_column(ForeignKey("stops.id"))
    seq: Mapped[int] = mapped_column(Integer)
    mandatory: Mapped[bool] = mapped_column(Boolean, default=True)

class SignalRow(Base):
    __tablename__ = "signals"
    id: Mapped[int] = mapped_column(primary_key=True)
    bus_id: Mapped[int] = mapped_column(ForeignKey("buses.id"))
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    speed_kmh: Mapped[float] = mapped_column(Float)
    doors_open: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (Index("ix_signals_bus_ts", "bus_id", "ts"),)

class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16), default="new")   # new|confirmed|rejected
    bus_id: Mapped[int] = mapped_column(ForeignKey("buses.id"))
    driver_id: Mapped[int | None] = mapped_column(ForeignKey("drivers.id"))
    route_id: Mapped[int | None] = mapped_column(ForeignKey("routes.id"))
    ts_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ts_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    clip_id: Mapped[int | None] = mapped_column(ForeignKey("clips.id"))
    verdict_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"))
    verdict_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reject_reason: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (Index("ix_events_status_created", "status", "created_at"),)

class Clip(Base):
    __tablename__ = "clips"
    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column(Text)
    duration_s: Mapped[float] = mapped_column(Float, default=0)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

class RuleConfig(Base):
    __tablename__ = "rule_configs"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_by: Mapped[str | None] = mapped_column(String(64))

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    login: Mapped[str] = mapped_column(String(64), unique=True)
    password_hash: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(String(16))                    # admin|security|manager
    tg_chat_id: Mapped[str] = mapped_column(String(32), default="")
    notify_types: Mapped[list] = mapped_column(JSON, default=list)   # [] = все типы
```

- [ ] **Step 3: Alembic (async) и миграция**

Run: `alembic init alembic`

`alembic.ini` — строка `sqlalchemy.url` пустая (берём из настроек). `alembic/env.py` заменить содержимое:
```python
import asyncio
from logging.config import fileConfig
from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from buscore.config import get_settings
from buscore.db import Base
import buscore.models  # noqa: F401 — регистрирует таблицы

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
target_metadata = Base.metadata

def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()

async def run_async():
    engine = create_async_engine(get_settings().database_url)
    async with engine.connect() as conn:
        await conn.run_sync(do_run_migrations)
    await engine.dispose()

def run_migrations_online():
    asyncio.run(run_async())

run_migrations_online()
```

Run:
```bash
alembic revision --autogenerate -m "initial schema"
alembic upgrade head
```
Expected: создан файл в `alembic/versions/`, upgrade без ошибок

- [ ] **Step 4: Тест проходит**

Run: `pytest tests/test_schema.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: database schema (all spec tables) with async alembic"
```

### Task 3: Геоматематика и демо-данные

**Files:**
- Create: `buscore/geo.py`, `scripts/seed_demo.py`, `scripts/__init__.py`
- Test: `tests/test_geo.py`, `tests/test_seed.py`

- [ ] **Step 1: Провальный тест geo**

`tests/test_geo.py`:
```python
from buscore.geo import RouteGeometry

# прямая «улица» на север от точки в Алматы, ~1.1 км
POLY = [[43.2380, 76.9450], [43.2480, 76.9450]]

def test_distance_to_route_center_is_zero():
    g = RouteGeometry(POLY)
    assert g.distance_m(43.2430, 76.9450) < 5

def test_distance_to_route_offset_200m():
    g = RouteGeometry(POLY)
    # сдвиг по долготе ~0.00246° ≈ 200 м на этой широте
    d = g.distance_m(43.2430, 76.9450 + 0.00246)
    assert 180 < d < 220

def test_point_in_circle():
    g = RouteGeometry(POLY)
    assert g.haversine_m(43.2380, 76.9450, 43.2380, 76.9455) < 60
```

Run: `pytest tests/test_geo.py -v`
Expected: FAIL — модуля нет

- [ ] **Step 2: geo.py**

`buscore/geo.py`:
```python
import math
from shapely.geometry import LineString, Point

class RouteGeometry:
    """Полилиния маршрута в локальной метрической проекции.

    Равнопромежуточная проекция относительно первой точки маршрута:
    x = (lon - lon0) * 111320 * cos(lat0), y = (lat - lat0) * 110540.
    Для города (< 50 км) погрешность незначима.
    """

    def __init__(self, polyline: list[list[float]]):
        self.lat0, self.lon0 = polyline[0][0], polyline[0][1]
        self._kx = 111_320 * math.cos(math.radians(self.lat0))
        self._ky = 110_540
        self.line = LineString([self._xy(lat, lon) for lat, lon in polyline])

    def _xy(self, lat: float, lon: float) -> tuple[float, float]:
        return ((lon - self.lon0) * self._kx, (lat - self.lat0) * self._ky)

    def distance_m(self, lat: float, lon: float) -> float:
        return self.line.distance(Point(self._xy(lat, lon)))

    def haversine_m(self, lat1, lon1, lat2, lon2) -> float:
        r = 6_371_000
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))
```

- [ ] **Step 3: geo-тест проходит**

Run: `pytest tests/test_geo.py -v`
Expected: PASS

- [ ] **Step 4: Провальный тест seed**

`tests/test_seed.py`:
```python
from sqlalchemy import select, func
from scripts.seed_demo import seed
from buscore import models as m

async def test_seed_creates_demo_world(session):
    await seed(session)
    assert (await session.scalar(select(func.count(m.Bus.id)))) == 1
    assert (await session.scalar(select(func.count(m.Stop.id)))) == 6   # 5 остановок + депо
    assert (await session.scalar(select(func.count(m.User.id)))) == 3
    cfg = await session.get(m.RuleConfig, "speeding_margin_kmh")
    assert cfg.value == 10
```

Run: `pytest tests/test_seed.py -v`
Expected: FAIL

- [ ] **Step 5: seed_demo.py**

`scripts/__init__.py` — пустой. `scripts/seed_demo.py`:
```python
"""Демо-мир: маршрут 45 (прямая ~4 км в Алматы), борт 117, смена, пользователи.

Запуск: python -m scripts.seed_demo
Пароли демо (СМЕНИТЬ на проде, см. README-RUNBOOK): admin/admin123, sb/sb123, boss/boss123
"""
import asyncio
from datetime import date, datetime, timezone
from passlib.hash import bcrypt
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
    session.add(m.Shift(bus_id=bus.id, driver_id=driver.id, day=date.today()))
    for key, value in RULE_DEFAULTS.items():
        session.add(m.RuleConfig(key=key, value=float(value),
                                 updated_at=datetime.now(timezone.utc)))
    for login, pwd, role in [("admin", "admin123", "admin"),
                             ("sb", "sb123", "security"),
                             ("boss", "boss123", "manager")]:
        session.add(m.User(login=login, password_hash=bcrypt.hash(pwd), role=role))
    await session.commit()

async def main() -> None:
    async with SessionMaker() as session:
        await seed(session)
    print("Demo data seeded.")

if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6: Тест проходит, commit**

Run: `pytest tests/test_seed.py tests/test_geo.py -v`
Expected: PASS (оба)

```bash
git add -A
git commit -m "feat: geo math (metric projection) and demo seed data"
```

### Task 4: Signal и Replay-источник

**Files:**
- Create: `buscore/schemas.py`, `buscore/ingest/__init__.py`, `buscore/ingest/replay.py`
- Test: `tests/test_replay.py`

- [ ] **Step 1: Провальный тест**

`tests/test_replay.py`:
```python
import json
from datetime import datetime, timezone, timedelta
from buscore.ingest.replay import ReplayPlayer

def _jsonl(tmp_path, rows):
    p = tmp_path / "track.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    return str(p)

BASE = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)

def _row(sec, speed=40.0, doors=False):
    return {"bus_code": "117", "ts": (BASE + timedelta(seconds=sec)).isoformat(),
            "lat": 43.24, "lon": 76.945, "speed_kmh": speed, "doors_open": doors}

async def test_replay_emits_all_signals_in_order(tmp_path):
    path = _jsonl(tmp_path, [_row(0), _row(5), _row(10)])
    got = []
    async def sink(sig):
        got.append(sig)
    player = ReplayPlayer(path, speed=1000.0, rebase=False, sink=sink)
    await player.run()
    assert [s.ts.second for s in got] == [0, 5, 10]
    assert got[0].bus_code == "117" and got[0].speed_kmh == 40.0

async def test_replay_rebase_moves_track_to_now(tmp_path):
    path = _jsonl(tmp_path, [_row(0), _row(5)])
    got = []
    async def sink(sig):
        got.append(sig)
    await ReplayPlayer(path, speed=1000.0, rebase=True, sink=sink).run()
    age = abs((datetime.now(timezone.utc) - got[0].ts).total_seconds())
    assert age < 60  # первая точка «сейчас»
```

Run: `pytest tests/test_replay.py -v`
Expected: FAIL

- [ ] **Step 2: schemas.py и replay.py**

`buscore/schemas.py`:
```python
from datetime import datetime
from pydantic import BaseModel

class Signal(BaseModel):
    bus_code: str
    ts: datetime
    lat: float
    lon: float
    speed_kmh: float
    doors_open: bool = False

class EventCandidate(BaseModel):
    type: str
    ts_start: datetime
    ts_end: datetime | None = None
    lat: float
    lon: float
    confidence: float = 1.0
    details: dict = {}
```

`buscore/ingest/__init__.py` — пустой. `buscore/ingest/replay.py`:
```python
import asyncio
import json
from datetime import datetime, timezone
from typing import Awaitable, Callable
from buscore.schemas import Signal

class ReplayPlayer:
    """Проигрывает JSONL-трек как живой поток сигналов.

    speed=60 → час записи за минуту. rebase=True сдвигает метки времени так,
    чтобы первая точка оказалась «сейчас» (лента выглядит живой).
    """

    def __init__(self, path: str, speed: float, rebase: bool,
                 sink: Callable[[Signal], Awaitable[None]]):
        self.path, self.speed, self.rebase, self.sink = path, speed, rebase, sink

    async def run(self) -> None:
        signals = self._load()
        if not signals:
            return
        offset = (datetime.now(timezone.utc) - signals[0].ts) if self.rebase else None
        prev_ts = signals[0].ts
        for sig in signals:
            delay = (sig.ts - prev_ts).total_seconds() / self.speed
            if delay > 0:
                await asyncio.sleep(delay)
            prev_ts = sig.ts
            if offset is not None:
                sig = sig.model_copy(update={"ts": sig.ts + offset})
            await self.sink(sig)

    def _load(self) -> list[Signal]:
        with open(self.path, encoding="utf-8") as f:
            rows = [Signal(**json.loads(line)) for line in f if line.strip()]
        return sorted(rows, key=lambda s: s.ts)
```

- [ ] **Step 3: Тест проходит, commit**

Run: `pytest tests/test_replay.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: Signal schema and JSONL replay player"
```

### Task 5: Конвейер событий — engine, фильтры, store

**Files:**
- Create: `buscore/rules/__init__.py`, `buscore/rules/engine.py`
- Create: `buscore/events/__init__.py`, `buscore/events/store.py`
- Test: `tests/test_engine.py`, `tests/test_store.py`

- [ ] **Step 1: Провальный тест фильтров (без БД)**

`tests/test_engine.py`:
```python
from datetime import datetime, timezone, timedelta
from buscore.rules.engine import FilterChain
from buscore.schemas import EventCandidate

T0 = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)

def _cand(sec, type_="speeding"):
    return EventCandidate(type=type_, ts_start=T0 + timedelta(seconds=sec),
                          lat=43.24, lon=76.945)

def test_cooldown_suppresses_repeats():
    f = FilterChain(cooldowns={"speeding": 120}, daily_budget=30, tz="Asia/Almaty")
    assert f.allow("117", _cand(0)) is True
    assert f.allow("117", _cand(60)) is False    # внутри кулдауна
    assert f.allow("117", _cand(130)) is True    # кулдаун истёк

def test_cooldown_is_per_bus_and_type():
    f = FilterChain(cooldowns={"speeding": 120}, daily_budget=30, tz="Asia/Almaty")
    assert f.allow("117", _cand(0)) is True
    assert f.allow("203", _cand(1)) is True                       # другой борт
    assert f.allow("117", _cand(2, type_="long_idle")) is True    # другой тип

def test_daily_budget_caps_publications():
    f = FilterChain(cooldowns={}, daily_budget=2, tz="Asia/Almaty")
    assert f.allow("117", _cand(0)) is True
    assert f.allow("117", _cand(10)) is True
    assert f.allow("117", _cand(20)) is False    # бюджет исчерпан
    assert f.budget_exceeded_today(_cand(20).ts_start) is True
```

Run: `pytest tests/test_engine.py -v`
Expected: FAIL

- [ ] **Step 2: engine.py**

`buscore/rules/__init__.py` — пустой. `buscore/rules/engine.py`:
```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Awaitable, Callable, Protocol
from zoneinfo import ZoneInfo
from buscore.geo import RouteGeometry
from buscore.schemas import EventCandidate, Signal

class Detector(Protocol):
    event_type: str
    def process(self, sig: Signal, ctx: "BusContext") -> EventCandidate | None: ...

@dataclass
class StopZone:
    stop_id: int
    name: str
    lat: float
    lon: float
    radius_m: float
    mandatory: bool
    is_depot: bool

@dataclass
class BusContext:
    bus_code: str
    route_limit_kmh: float
    geometry: RouteGeometry
    stops: list[StopZone]
    config: dict[str, float]
    state: dict = field(default_factory=dict)   # приватное состояние детекторов

    def cfg(self, key: str) -> float:
        return self.config[key]

class FilterChain:
    """Дедуп через кулдаун (борт+тип) + дневной бюджет публикаций (спека §5)."""

    def __init__(self, cooldowns: dict[str, float], daily_budget: int, tz: str):
        self.cooldowns = cooldowns
        self.daily_budget = daily_budget
        self.tz = ZoneInfo(tz)
        self._last: dict[tuple[str, str], datetime] = {}
        self._day: str = ""
        self._count = 0

    def _roll_day(self, ts: datetime) -> None:
        day = ts.astimezone(self.tz).date().isoformat()
        if day != self._day:
            self._day, self._count = day, 0

    def allow(self, bus_code: str, cand: EventCandidate) -> bool:
        self._roll_day(cand.ts_start)
        key = (bus_code, cand.type)
        last = self._last.get(key)
        cooldown = self.cooldowns.get(cand.type, 0)
        if last is not None and (cand.ts_start - last).total_seconds() < cooldown:
            return False
        if self._count >= self.daily_budget:
            return False
        self._last[key] = cand.ts_start
        self._count += 1
        return True

    def budget_exceeded_today(self, ts: datetime) -> bool:
        self._roll_day(ts)
        return self._count >= self.daily_budget

class EventEngine:
    """Прогоняет сигнал через детекторы контекста борта и публикует кандидатов."""

    def __init__(self, detectors: list[Detector], filters: FilterChain,
                 publish: Callable[[str, EventCandidate], Awaitable[None]]):
        self.detectors = detectors
        self.filters = filters
        self.publish = publish
        self.contexts: dict[str, BusContext] = {}

    async def process(self, sig: Signal) -> None:
        ctx = self.contexts.get(sig.bus_code)
        if ctx is None:
            return
        for det in self.detectors:
            cand = det.process(sig, ctx)
            if cand is not None and self.filters.allow(sig.bus_code, cand):
                await self.publish(sig.bus_code, cand)
```

- [ ] **Step 3: Тест фильтров проходит**

Run: `pytest tests/test_engine.py -v`
Expected: PASS

- [ ] **Step 4: Провальный тест store (с БД)**

`tests/test_store.py`:
```python
from datetime import datetime, timezone
from sqlalchemy import select
from scripts.seed_demo import seed
from buscore import models as m
from buscore.events.store import EventStore
from buscore.schemas import EventCandidate

async def test_publish_creates_event_resolves_driver_and_broadcasts(session):
    await seed(session)
    store = EventStore()
    sub = store.subscribe()
    cand = EventCandidate(type="speeding",
                          ts_start=datetime.now(timezone.utc),
                          lat=43.24, lon=76.945, details={"speed_max": 67})
    event_id = await store.publish(session, "117", cand)
    ev = await session.get(m.Event, event_id)
    driver = await session.scalar(select(m.Driver))
    assert ev.type == "speeding" and ev.status == "new"
    assert ev.driver_id == driver.id            # водитель найден по смене
    got = await sub.get()
    assert got["id"] == event_id

async def test_verdict_reject_requires_reason(session):
    await seed(session)
    store = EventStore()
    cand = EventCandidate(type="speeding", ts_start=datetime.now(timezone.utc),
                          lat=43.24, lon=76.945)
    event_id = await store.publish(session, "117", cand)
    user = await session.scalar(select(m.User).where(m.User.login == "sb"))
    ok = await store.verdict(session, event_id, "rejected", user.id, reason=None)
    assert ok is False
    ok = await store.verdict(session, event_id, "rejected", user.id,
                             reason="ложное срабатывание")
    assert ok is True
    ev = await session.get(m.Event, event_id)
    assert ev.status == "rejected" and ev.reject_reason == "ложное срабатывание"
```

Run: `pytest tests/test_store.py -v`
Expected: FAIL

- [ ] **Step 5: store.py**

`buscore/events/__init__.py` — пустой. `buscore/events/store.py`:
```python
import asyncio
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from buscore import models as m
from buscore.schemas import EventCandidate

REJECT_REASONS = ["ложное срабатывание", "не нарушение", "дубль"]

class EventStore:
    """Публикация событий + вердикты + широковещание для SSE/notify."""

    def __init__(self):
        self._subs: set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    async def publish(self, session: AsyncSession, bus_code: str,
                      cand: EventCandidate) -> int:
        bus = await session.scalar(select(m.Bus).where(m.Bus.code == bus_code))
        day = cand.ts_start.astimezone(timezone.utc).date()
        shift = await session.scalar(select(m.Shift).where(
            m.Shift.bus_id == bus.id, m.Shift.day == day))
        ev = m.Event(type=cand.type, bus_id=bus.id, route_id=bus.route_id,
                     driver_id=shift.driver_id if shift else None,
                     ts_start=cand.ts_start, ts_end=cand.ts_end,
                     source_ts=cand.ts_start, lat=cand.lat, lon=cand.lon,
                     confidence=cand.confidence, details=cand.details,
                     created_at=datetime.now(timezone.utc))
        session.add(ev)
        await session.commit()
        payload = {"id": ev.id, "type": ev.type, "bus_code": bus_code,
                   "ts": ev.ts_start.isoformat()}
        for q in list(self._subs):
            q.put_nowait(payload)
        return ev.id

    async def verdict(self, session: AsyncSession, event_id: int, status: str,
                      user_id: int, reason: str | None) -> bool:
        if status not in ("confirmed", "rejected"):
            return False
        if status == "rejected" and reason not in REJECT_REASONS:
            return False
        ev = await session.get(m.Event, event_id)
        if ev is None or ev.status != "new":
            return False
        ev.status = status
        ev.verdict_by = user_id
        ev.verdict_at = datetime.now(timezone.utc)
        ev.reject_reason = reason if status == "rejected" else None
        await session.commit()
        return True
```

- [ ] **Step 6: Тесты проходят, commit**

Run: `pytest tests/test_store.py tests/test_engine.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: event pipeline - engine, filter chain, event store with broadcast"
```

### Общее для задач 6–10 (детекторы)

Каждый детектор — класс с `event_type: str` и методом `process(sig, ctx) -> EventCandidate | None`. Приватное состояние — в `ctx.state[<event_type>]`. Один эпизод нарушения = максимум один кандидат (повторную частоту дополнительно режет FilterChain). Тестовая обвязка одинаковая — хелпер в `tests/detectors/conftest.py`:

```python
import pytest
from datetime import datetime, timezone, timedelta
from buscore.geo import RouteGeometry
from buscore.rules.engine import BusContext, StopZone
from buscore.schemas import Signal
from scripts.seed_demo import RULE_DEFAULTS, POLY, STOPS

T0 = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)

@pytest.fixture
def ctx() -> BusContext:
    stops = [StopZone(stop_id=i, name=n, lat=lat, lon=lon, radius_m=60,
                      mandatory=not depot, is_depot=depot)
             for i, (n, lat, lon, depot) in enumerate(STOPS)]
    return BusContext(bus_code="117", route_limit_kmh=50,
                      geometry=RouteGeometry(POLY), stops=stops,
                      config=dict(RULE_DEFAULTS))

def sig(sec: float, speed: float, lat: float = 43.2455, lon: float = 76.9450,
        doors: bool = False) -> Signal:
    return Signal(bus_code="117", ts=T0 + timedelta(seconds=sec),
                  lat=lat, lon=lon, speed_kmh=speed, doors_open=doors)
```

Точка по умолчанию `43.2455, 76.9450` лежит на маршруте **между** остановками (до «Театра» и «Рынка» больше 60 м) — детекторы, которым нужны остановки или отход от маршрута, передают свои координаты явно.

### Task 6: Детектор speeding

**Files:**
- Create: `buscore/rules/detectors/__init__.py`, `buscore/rules/detectors/speeding.py`
- Create: `tests/detectors/__init__.py`, `tests/detectors/conftest.py` (код выше)
- Test: `tests/detectors/test_speeding.py`

- [ ] **Step 1: Провальный golden-тест**

`tests/detectors/test_speeding.py`:
```python
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
```

Run: `pytest tests/detectors/test_speeding.py -v`
Expected: FAIL

- [ ] **Step 2: Реализация**

`buscore/rules/detectors/__init__.py` — пустой. `buscore/rules/detectors/speeding.py`:
```python
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
```

- [ ] **Step 3: Тест проходит, commit**

Run: `pytest tests/detectors/test_speeding.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: speeding detector with episode logic"
```

### Task 7: Детектор harsh_brake / harsh_accel

**Files:**
- Create: `buscore/rules/detectors/harsh.py`
- Test: `tests/detectors/test_harsh.py`

- [ ] **Step 1: Провальный тест**

`tests/detectors/test_harsh.py`:
```python
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
```

Run: `pytest tests/detectors/test_harsh.py -v`
Expected: FAIL

- [ ] **Step 2: Реализация**

`buscore/rules/detectors/harsh.py`:
```python
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
```

Примечание: FilterChain должен знать кулдауны обоих типов — ключи `cooldown_s.harsh_brake` и `cooldown_s.harsh_accel` уже в `RULE_DEFAULTS` (Task 3). В Плане 3 сюда добавится passthrough готовых событий Link+ (спека §4).

- [ ] **Step 3: Тест проходит, commit**

Run: `pytest tests/detectors/test_harsh.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: harsh brake/accel detector from speed derivative"
```

### Task 8: Детектор long_idle

**Files:**
- Create: `buscore/rules/detectors/idle.py`
- Test: `tests/detectors/test_idle.py`

- [ ] **Step 1: Провальный тест**

`tests/detectors/test_idle.py`:
```python
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
```

Run: `pytest tests/detectors/test_idle.py -v`
Expected: FAIL

- [ ] **Step 2: Реализация**

`buscore/rules/detectors/idle.py`:
```python
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
```

- [ ] **Step 3: Тест проходит, commit**

Run: `pytest tests/detectors/test_idle.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: long idle detector with stop-zone exclusion"
```

### Task 9: Детектор route_deviation

**Files:**
- Create: `buscore/rules/detectors/route_dev.py`
- Test: `tests/detectors/test_route_dev.py`

- [ ] **Step 1: Провальный тест**

`tests/detectors/test_route_dev.py`:
```python
from buscore.rules.detectors.route_dev import RouteDeviationDetector
from tests.detectors.conftest import sig

OFF = 76.9450 + 0.004   # ~325 м восточнее маршрута

def _run(det, ctx, signals):
    return [c for s in signals if (c := det.process(s, ctx)) is not None]

def test_sustained_deviation_fires_once(ctx):
    det = RouteDeviationDetector()
    signals = [sig(t, 30.0, lat=43.2455, lon=OFF) for t in range(0, 150, 10)]
    events = _run(det, ctx, signals)
    assert len(events) == 1
    assert events[0].type == "route_deviation"
    assert events[0].details["distance_m"] > 300

def test_on_route_is_silent(ctx):
    det = RouteDeviationDetector()
    signals = [sig(t, 30.0) for t in range(0, 150, 10)]
    assert _run(det, ctx, signals) == []

def test_short_detour_is_silent(ctx):
    det = RouteDeviationDetector()
    signals = [sig(0, 30.0, lat=43.2455, lon=OFF),
               sig(60, 30.0, lat=43.2455, lon=OFF),
               sig(90, 30.0)]                      # вернулся до 120 с
    assert _run(det, ctx, signals) == []
```

Run: `pytest tests/detectors/test_route_dev.py -v`
Expected: FAIL

- [ ] **Step 2: Реализация**

`buscore/rules/detectors/route_dev.py`:
```python
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
```

- [ ] **Step 3: Тест проходит, commit**

Run: `pytest tests/detectors/test_route_dev.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: route deviation detector"
```

### Task 10: Детектор stop_skip

**Files:**
- Create: `buscore/rules/detectors/stop_skip.py`
- Test: `tests/detectors/test_stop_skip.py`

- [ ] **Step 1: Провальный тест**

`tests/detectors/test_stop_skip.py`:
```python
from buscore.rules.detectors.stop_skip import StopSkipDetector
from tests.detectors.conftest import sig

# остановка «Рынок»: 43.2492, 76.9450, радиус 60 м
BEFORE = dict(lat=43.2480, lon=76.9450)   # ~130 м до — вне зоны
INSIDE = dict(lat=43.2492, lon=76.9450)
AFTER  = dict(lat=43.2505, lon=76.9450)   # ~145 м после — вне зоны

def _run(det, ctx, signals):
    return [c for s in signals if (c := det.process(s, ctx)) is not None]

def test_fast_pass_without_doors_fires(ctx):
    det = StopSkipDetector()
    signals = [sig(0, 40, **BEFORE), sig(5, 40, **INSIDE), sig(10, 40, **AFTER)]
    events = _run(det, ctx, signals)
    assert len(events) == 1
    ev = events[0]
    assert ev.type == "stop_skip"
    assert ev.details["stop"] == "Ост. Рынок"
    assert ev.details["min_speed_kmh"] == 40

def test_stop_with_doors_is_silent(ctx):
    det = StopSkipDetector()
    signals = [sig(0, 40, **BEFORE), sig(5, 0, **INSIDE, doors=True),
               sig(30, 40, **AFTER)]
    assert _run(det, ctx, signals) == []

def test_slow_rolling_counts_as_stopped(ctx):
    det = StopSkipDetector()
    # min скорость 3 км/ч < порога 5 — «остановился», даже без дверей (спека §4: «и»)
    signals = [sig(0, 40, **BEFORE), sig(5, 3, **INSIDE), sig(10, 40, **AFTER)]
    assert _run(det, ctx, signals) == []
```

Run: `pytest tests/detectors/test_stop_skip.py -v`
Expected: FAIL

- [ ] **Step 2: Реализация**

`buscore/rules/detectors/stop_skip.py`:
```python
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
```

- [ ] **Step 3: Тест проходит, commit**

Run: `pytest tests/detectors/test_stop_skip.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: stop skip detector (geofence + speed + doors state machine)"
```

### Task 11: Живая лента — страница, SSE, вердикты, runtime

**Files:**
- Create: `buscore/runtime.py`, `buscore/web/routes_feed.py`
- Create: `buscore/web/templates/base.html`, `buscore/web/templates/feed.html`, `buscore/web/templates/_card.html`
- Modify: `buscore/web/app.py`, `buscore/main.py`
- Test: `tests/test_feed_web.py`

- [ ] **Step 1: Провальный тест**

`tests/test_feed_web.py`:
```python
import httpx
from datetime import datetime, timezone
from sqlalchemy import select
from scripts.seed_demo import seed
from buscore import models as m
from buscore.schemas import EventCandidate
from buscore.web.app import create_app

async def _client(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")

async def _publish_one(app, session):
    cand = EventCandidate(type="speeding", ts_start=datetime.now(timezone.utc),
                          lat=43.24, lon=76.945,
                          details={"speed_max": 67, "limit_kmh": 50})
    return await app.state.store.publish(session, "117", cand)

async def test_feed_page_renders_event_card(session):
    await seed(session)
    app = create_app()
    event_id = await _publish_one(app, session)
    async with await _client(app) as c:
        r = await c.get("/feed/cards")
    assert r.status_code == 200
    assert "117" in r.text and "Превышение скорости" in r.text
    assert f'id="card-{event_id}"' in r.text

async def test_verdict_endpoint_confirms(session):
    await seed(session)
    app = create_app()
    event_id = await _publish_one(app, session)
    async with await _client(app) as c:
        r = await c.post(f"/events/{event_id}/verdict",
                         data={"status": "confirmed"})
    assert r.status_code == 200
    ev = await session.get(m.Event, event_id)
    await session.refresh(ev)
    assert ev.status == "confirmed"

async def test_reject_without_reason_is_400(session):
    await seed(session)
    app = create_app()
    event_id = await _publish_one(app, session)
    async with await _client(app) as c:
        r = await c.post(f"/events/{event_id}/verdict",
                         data={"status": "rejected"})
    assert r.status_code == 400
```

Run: `pytest tests/test_feed_web.py -v`
Expected: FAIL

- [ ] **Step 2: runtime.py (сборка конвейера)**

`buscore/runtime.py`:
```python
"""Сборка живого конвейера: контексты бортов из БД, engine, фоновые задачи."""
import asyncio
from datetime import datetime, timezone
from sqlalchemy import select
from buscore import models as m
from buscore.config import get_settings
from buscore.db import SessionMaker
from buscore.events.store import EventStore
from buscore.geo import RouteGeometry
from buscore.ingest.replay import ReplayPlayer
from buscore.rules.engine import BusContext, EventEngine, FilterChain, StopZone
from buscore.rules.detectors.speeding import SpeedingDetector
from buscore.rules.detectors.harsh import HarshDetector
from buscore.rules.detectors.idle import LongIdleDetector
from buscore.rules.detectors.route_dev import RouteDeviationDetector
from buscore.rules.detectors.stop_skip import StopSkipDetector

DETECTORS = [SpeedingDetector(), HarshDetector(), LongIdleDetector(),
             RouteDeviationDetector(), StopSkipDetector()]

async def load_configs(session) -> dict[str, float]:
    rows = (await session.scalars(select(m.RuleConfig))).all()
    return {r.key: r.value for r in rows}

async def build_contexts(session, config) -> dict[str, BusContext]:
    contexts = {}
    depot_zones = [StopZone(stop_id=s.id, name=s.name, lat=s.lat, lon=s.lon,
                            radius_m=s.radius_m, mandatory=False, is_depot=True)
                   for s in (await session.scalars(
                       select(m.Stop).where(m.Stop.is_depot))).all()]
    for bus in (await session.scalars(select(m.Bus))).all():
        route = await session.get(m.Route, bus.route_id)
        rows = (await session.execute(
            select(m.RouteStop, m.Stop).join(m.Stop, m.RouteStop.stop_id == m.Stop.id)
            .where(m.RouteStop.route_id == route.id).order_by(m.RouteStop.seq))).all()
        zones = [StopZone(stop_id=s.id, name=s.name, lat=s.lat, lon=s.lon,
                          radius_m=s.radius_m, mandatory=rs.mandatory, is_depot=False)
                 for rs, s in rows] + depot_zones
        contexts[bus.code] = BusContext(bus_code=bus.code,
                                        route_limit_kmh=route.speed_limit_kmh,
                                        geometry=RouteGeometry(route.polyline),
                                        stops=zones, config=config)
    return contexts

class Runtime:
    def __init__(self, store: EventStore):
        self.store = store
        self.engine: EventEngine | None = None
        self.last_signal_at: datetime | None = None
        self.tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        settings = get_settings()
        async with SessionMaker() as session:
            config = await load_configs(session)
            contexts = await build_contexts(session, config)
        cooldowns = {k.removeprefix("cooldown_s."): v
                     for k, v in config.items() if k.startswith("cooldown_s.")}
        filters = FilterChain(cooldowns=cooldowns,
                              daily_budget=int(config["daily_budget"]),
                              tz=settings.tz)

        async def publish(bus_code, cand):
            async with SessionMaker() as session:
                await self.store.publish(session, bus_code, cand)

        self.engine = EventEngine(DETECTORS, filters, publish)
        self.engine.contexts = contexts

        async def sink(sig):
            self.last_signal_at = datetime.now(timezone.utc)
            async with SessionMaker() as session:
                bus = await session.scalar(
                    select(m.Bus).where(m.Bus.code == sig.bus_code))
                if bus is not None:
                    session.add(m.SignalRow(bus_id=bus.id, ts=sig.ts, lat=sig.lat,
                                            lon=sig.lon, speed_kmh=sig.speed_kmh,
                                            doors_open=sig.doors_open))
                    await session.commit()
            await self.engine.process(sig)

        self.sink = sink
        if settings.replay_file:
            player = ReplayPlayer(settings.replay_file, settings.replay_speed,
                                  settings.replay_rebase, sink)
            self.tasks.append(asyncio.create_task(player.run()))

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
```

- [ ] **Step 3: Маршруты ленты и шаблоны**

`buscore/web/routes_feed.py`:
```python
from pathlib import Path
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sse_starlette.sse import EventSourceResponse
from buscore import models as m
from buscore.db import SessionMaker
from buscore.events.store import REJECT_REASONS

TYPE_LABELS = {
    "speeding": "Превышение скорости", "harsh_brake": "Резкое торможение",
    "harsh_accel": "Резкое ускорение", "route_deviation": "Отклонение от маршрута",
    "stop_skip": "Пропуск остановки", "long_idle": "Незапланированная стоянка",
    "phone_use": "Телефон у водителя", "smoking": "Курение",
    "drowsiness": "Сонливость",
}
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.globals.update(TYPE_LABELS=TYPE_LABELS, REJECT_REASONS=REJECT_REASONS)
router = APIRouter()

async def _card_context(session, event: m.Event) -> dict:
    bus = await session.get(m.Bus, event.bus_id)
    driver = await session.get(m.Driver, event.driver_id) if event.driver_id else None
    route = await session.get(m.Route, event.route_id) if event.route_id else None
    return {"e": event, "bus": bus, "driver": driver, "route": route}

async def _stats(session) -> dict:
    q = select(m.Event.status, func.count()).group_by(m.Event.status)
    counts = dict((await session.execute(q)).all())
    return {"total": sum(counts.values()), "new": counts.get("new", 0),
            "confirmed": counts.get("confirmed", 0),
            "rejected": counts.get("rejected", 0)}

@router.get("/feed")
async def feed_page(request: Request, status: str = "all"):
    async with SessionMaker() as session:
        return templates.TemplateResponse(request, "feed.html",
            {"stats": await _stats(session), "status": status})

@router.get("/feed/cards")
async def feed_cards(request: Request, status: str = "all"):
    async with SessionMaker() as session:
        q = select(m.Event).order_by(m.Event.created_at.desc()).limit(50)
        if status != "all":
            q = q.where(m.Event.status == status)
        events = (await session.scalars(q)).all()
        cards = [await _card_context(session, e) for e in events]
        return templates.TemplateResponse(request, "_cards.html", {"cards": cards})

@router.get("/feed/stream")
async def feed_stream(request: Request):
    store = request.app.state.store
    queue = store.subscribe()

    async def gen():
        try:
            while True:
                payload = await queue.get()
                async with SessionMaker() as session:
                    event = await session.get(m.Event, payload["id"])
                    ctx = await _card_context(session, event)
                html = templates.get_template("_card.html").render(**ctx)
                yield {"event": "event", "data": html.replace("\n", " ")}
        finally:
            store.unsubscribe(queue)

    return EventSourceResponse(gen())

@router.post("/events/{event_id}/verdict")
async def verdict(request: Request, event_id: int,
                  status: str = Form(...), reason: str | None = Form(None)):
    store = request.app.state.store
    async with SessionMaker() as session:
        ok = await store.verdict(session, event_id, status,
                                 user_id=getattr(request.state, "user_id", None),
                                 reason=reason)
        if not ok:
            raise HTTPException(400, "Неверный вердикт (отклонение требует причину)")
        event = await session.get(m.Event, event_id)
        ctx = await _card_context(session, event)
        return templates.TemplateResponse(request, "_card.html", ctx)
```

`buscore/web/templates/base.html`:
```html
<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>buscore — {% block title %}{% endblock %}</title>
<script src="https://unpkg.com/htmx.org@2.0.4"></script>
<script src="https://unpkg.com/htmx-ext-sse@2.2.2"></script>
<style>
 body{font-family:system-ui,sans-serif;margin:0;background:#f5f6f8;color:#1a1c1f}
 .top{display:flex;gap:16px;align-items:center;padding:10px 16px;background:#fff;
      border-bottom:1px solid #dde0e4}
 .top a{color:#1a1c1f;text-decoration:none;font-weight:600}
 main{max-width:900px;margin:16px auto;padding:0 12px}
 .stats{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
 .stat{background:#fff;border:1px solid #dde0e4;border-radius:10px;padding:8px 14px}
 .stat b{font-size:18px;display:block}
 .card{background:#fff;border:1px solid #dde0e4;border-radius:10px;
       padding:12px 14px;margin-bottom:10px}
 .card.new{border-left:4px solid #2563eb}
 .badge{font-weight:600;font-size:13px;background:#eef1f5;border-radius:6px;
        padding:2px 8px}
 .muted{color:#66707c;font-size:13px}
 .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
 .grow{margin-left:auto}
 button{border:1px solid #c6ccd4;background:#fff;border-radius:8px;
        padding:6px 14px;cursor:pointer;font-weight:600}
 button.pri{background:#2563eb;border-color:#2563eb;color:#fff}
 select,input{padding:6px;border:1px solid #c6ccd4;border-radius:8px}
</style>
</head>
<body>
<nav class="top">
  <a href="/feed">Лента</a><a href="/report">Отчёт</a>
  <span class="grow"></span>{% block nav %}{% endblock %}
</nav>
<main>{% block content %}{% endblock %}</main>
</body>
</html>
```

`buscore/web/templates/feed.html`:
```html
{% extends "base.html" %}
{% block title %}Лента событий{% endblock %}
{% block content %}
<div class="stats">
  <div class="stat"><b>{{ stats.total }}</b><span class="muted">событий</span></div>
  <div class="stat"><b>{{ stats.new }}</b><span class="muted">ждут разбора</span></div>
  <div class="stat"><b>{{ stats.confirmed }}</b><span class="muted">подтверждено</span></div>
  <div class="stat"><b>{{ stats.rejected }}</b><span class="muted">отклонено</span></div>
  <div class="stat grow">
    <select onchange="location='/feed?status='+this.value">
      {% for s, label in [("all","Все"),("new","Новые"),
                          ("confirmed","Подтверждённые"),("rejected","Отклонённые")] %}
      <option value="{{ s }}" {{ "selected" if status == s }}>{{ label }}</option>
      {% endfor %}
    </select>
  </div>
</div>
<div hx-ext="sse" sse-connect="/feed/stream">
  <div id="cards" sse-swap="event" hx-swap="afterbegin"
       hx-get="/feed/cards?status={{ status }}" hx-trigger="load"></div>
</div>
<script>
document.addEventListener("keydown", (ev) => {           // горячие клавиши 1/2
  if (ev.key !== "1" && ev.key !== "2") return;
  const card = document.querySelector(".card.new");
  if (!card) return;
  const btn = card.querySelector(ev.key === "1" ? "button.pri" : "button.rej");
  if (btn) btn.click();
});
</script>
{% endblock %}
```

`buscore/web/templates/_cards.html`:
```html
{% for c in cards %}{% include "_card.html" with context %}
{% set e, bus, driver, route = c.e, c.bus, c.driver, c.route %}{% endfor %}
```

Примечание: чтобы `_card.html` работал и из `_cards.html`, и из SSE/вердикта, в `_cards.html` переменные распаковываются перед include:
```html
{% for c in cards %}
  {% with e=c.e, bus=c.bus, driver=c.driver, route=c.route %}
    {% include "_card.html" %}
  {% endwith %}
{% endfor %}
```
(использовать вторую версию — первая показана как анти-пример порядка распаковки).

`buscore/web/templates/_card.html`:
```html
<div class="card {{ 'new' if e.status == 'new' }}" id="card-{{ e.id }}">
  <div class="row">
    <span class="badge">{{ TYPE_LABELS.get(e.type, e.type) }}</span>
    <span class="muted">{{ e.ts_start.strftime("%H:%M:%S") }}</span>
    {% if e.details.get("duration_s") %}
      <span class="muted">{{ e.details["duration_s"] }} c</span>{% endif %}
    <span class="muted grow">борт {{ bus.code }}{% if route %} · маршрут {{ route.name }}{% endif %}</span>
  </div>
  <div class="row muted">
    {% if driver %}<span>Водитель: {{ driver.full_name }}</span>{% endif %}
    {% for k, v in e.details.items() if k in
       ["speed_max","limit_kmh","accel_ms2","distance_m","stop","min_speed_kmh","idle_s"] %}
      <span>{{ k }}: {{ v }}</span>
    {% endfor %}
    <span>клип появится после подключения видеоплатформы (План 3)</span>
  </div>
  {% if e.status == "new" %}
  <div class="row">
    <form hx-post="/events/{{ e.id }}/verdict" hx-target="#card-{{ e.id }}"
          hx-swap="outerHTML">
      <input type="hidden" name="status" value="confirmed">
      <button class="pri" type="submit">Подтвердить (1)</button>
    </form>
    <form hx-post="/events/{{ e.id }}/verdict" hx-target="#card-{{ e.id }}"
          hx-swap="outerHTML" class="row">
      <input type="hidden" name="status" value="rejected">
      <select name="reason">
        {% for r in REJECT_REASONS %}<option value="{{ r }}">{{ r }}</option>{% endfor %}
      </select>
      <button class="rej" type="submit">Отклонить (2)</button>
    </form>
  </div>
  {% else %}
  <div class="muted">
    {{ "✓ Подтверждено" if e.status == "confirmed" else "✕ Отклонено: " ~ (e.reject_reason or "") }}
    {% if e.verdict_at %} · {{ e.verdict_at.strftime("%H:%M") }}{% endif %}
  </div>
  {% endif %}
</div>
```

- [ ] **Step 4: Подключение в app.py и main.py**

`buscore/web/app.py` — заменить целиком:
```python
from fastapi import FastAPI
from buscore.events.store import EventStore

def create_app() -> FastAPI:
    app = FastAPI(title="buscore")
    app.state.store = EventStore()

    from buscore.web.routes_feed import router as feed_router
    app.include_router(feed_router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
```

`buscore/main.py` — заменить целиком:
```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from buscore.runtime import Runtime
from buscore.web.app import create_app

runtime: Runtime | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global runtime
    runtime = Runtime(app.state.store)
    await runtime.start()
    app.state.runtime = runtime
    yield
    await runtime.stop()

app = create_app()
app.router.lifespan_context = lifespan
```

- [ ] **Step 5: Тесты проходят, commit**

Run: `pytest tests/test_feed_web.py -v`
Expected: PASS (3 теста)

Ручная проверка живой ленты: `python -m scripts.seed_demo && REPLAY_FILE=... uvicorn buscore.main:app` — отложена до Task 15, когда появится генератор демо-дня.

```bash
git add -A
git commit -m "feat: live feed page with SSE, verdict endpoints, runtime assembly"
```

### Task 12: Аутентификация, роли, админка порогов

**Files:**
- Create: `buscore/web/auth.py`, `buscore/web/routes_admin.py`
- Create: `buscore/web/templates/login.html`, `buscore/web/templates/admin_rules.html`
- Modify: `buscore/web/app.py` (подключить роутеры), `buscore/web/routes_feed.py` (guard)
- Test: `tests/test_auth.py`

- [ ] **Step 1: Провальный тест**

`tests/test_auth.py`:
```python
import httpx
from scripts.seed_demo import seed
from buscore.web.app import create_app

async def _client(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test",
                             follow_redirects=False)

async def test_feed_requires_login(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.get("/feed")
    assert r.status_code == 303 and r.headers["location"] == "/login"

async def test_security_can_login_and_see_feed(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.post("/login", data={"login": "sb", "password": "sb123"})
        assert r.status_code == 303 and r.headers["location"] == "/feed"
        c.cookies.update(r.cookies)
        assert (await c.get("/feed")).status_code == 200

async def test_manager_blocked_from_feed_allowed_report(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.post("/login", data={"login": "boss", "password": "boss123"})
        c.cookies.update(r.cookies)
        assert (await c.get("/feed")).status_code == 403

async def test_admin_can_update_rule(session):
    await seed(session)
    async with await _client(create_app()) as c:
        r = await c.post("/login", data={"login": "admin", "password": "admin123"})
        c.cookies.update(r.cookies)
        r = await c.post("/admin/rules", data={"key": "speeding_margin_kmh",
                                               "value": "15"})
        assert r.status_code == 303
    from buscore import models as m
    cfg = await session.get(m.RuleConfig, "speeding_margin_kmh")
    await session.refresh(cfg)
    assert cfg.value == 15
```

Run: `pytest tests/test_auth.py -v`
Expected: FAIL

- [ ] **Step 2: auth.py**

`buscore/web/auth.py`:
```python
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, URLSafeSerializer
from passlib.hash import bcrypt
from sqlalchemy import select
from buscore import models as m
from buscore.config import get_settings
from buscore.db import SessionMaker
from buscore.web.routes_feed import templates

router = APIRouter()
_signer = URLSafeSerializer(get_settings().session_secret, salt="session")

def make_cookie(user: m.User) -> str:
    return _signer.dumps({"id": user.id, "role": user.role})

def read_session(request: Request) -> dict | None:
    raw = request.cookies.get("session")
    if not raw:
        return None
    try:
        return _signer.loads(raw)
    except BadSignature:
        return None

def require_role(request: Request, *roles: str) -> dict:
    """303 на /login без сессии; 403 при чужой роли. admin проходит всюду."""
    sess = read_session(request)
    if sess is None:
        raise HTTPException(303, headers={"Location": "/login"})
    if roles and sess["role"] not in roles and sess["role"] != "admin":
        raise HTTPException(403, "Недостаточно прав")
    request.state.user_id = sess["id"]
    return sess

@router.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {"error": ""})

@router.post("/login")
async def login(request: Request, login: str = Form(...),
                password: str = Form(...)):
    async with SessionMaker() as session:
        user = await session.scalar(select(m.User).where(m.User.login == login))
    if user is None or not bcrypt.verify(password, user.password_hash):
        return templates.TemplateResponse(request, "login.html",
                                          {"error": "Неверный логин или пароль"},
                                          status_code=401)
    target = "/report" if user.role == "manager" else "/feed"
    resp = RedirectResponse(target, status_code=303)
    resp.set_cookie("session", make_cookie(user), httponly=True, samesite="lax")
    return resp

@router.get("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie("session")
    return resp
```

`buscore/web/templates/login.html`:
```html
{% extends "base.html" %}
{% block title %}Вход{% endblock %}
{% block content %}
<form method="post" action="/login" class="card" style="max-width:360px;margin:60px auto">
  <h3>buscore — вход</h3>
  {% if error %}<p style="color:#b91c1c">{{ error }}</p>{% endif %}
  <p><input name="login" placeholder="Логин" style="width:100%"></p>
  <p><input name="password" type="password" placeholder="Пароль" style="width:100%"></p>
  <button class="pri" type="submit">Войти</button>
</form>
{% endblock %}
```

- [ ] **Step 3: Guard на ленту и админка**

В `buscore/web/routes_feed.py` добавить к каждому маршруту зависимость роли (импорт внизу файла недопустим — добавить сверху `from fastapi import Depends` уже есть; импортировать `require_role` через локальный импорт внутри функций нельзя — поэтому guard подключаем в `app.py` через `dependencies=[]` роутера):

`buscore/web/app.py` — заменить целиком:
```python
from fastapi import Depends, FastAPI, Request
from buscore.events.store import EventStore

def create_app() -> FastAPI:
    app = FastAPI(title="buscore")
    app.state.store = EventStore()

    from buscore.web.auth import require_role, router as auth_router
    from buscore.web.routes_feed import router as feed_router
    from buscore.web.routes_admin import router as admin_router
    from buscore.web.routes_report import router as report_router

    def guard_security(request: Request):
        return require_role(request, "security")

    def guard_manager(request: Request):
        return require_role(request, "security", "manager")

    def guard_admin(request: Request):
        return require_role(request)  # только admin (require_role пропускает admin)

    app.include_router(auth_router)
    app.include_router(feed_router, dependencies=[Depends(guard_security)])
    app.include_router(report_router, dependencies=[Depends(guard_manager)])
    app.include_router(admin_router, dependencies=[Depends(guard_admin)])

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
```

Внимание: `guard_admin` использует `require_role(request)` без ролей — по коду `require_role` это пропустит ЛЮБОГО залогиненного. Исправление тут же (это и проверяет тест ролей): в `require_role` заменить условие на
```python
    allowed = set(roles) | {"admin"}
    if sess["role"] not in allowed:
        raise HTTPException(403, "Недостаточно прав")
```
и в `guard_admin` вызывать `require_role(request, "admin")` — тогда `roles=("admin",)` пропускает только админа. (`routes_report` появится в Task 14 — до тех пор строки про report_router закомментировать.)

`buscore/web/routes_admin.py`:
```python
from datetime import datetime, timezone
from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from buscore import models as m
from buscore.db import SessionMaker
from buscore.web.routes_feed import templates

router = APIRouter()

@router.get("/admin/rules")
async def rules_page(request: Request):
    async with SessionMaker() as session:
        rules = (await session.scalars(
            select(m.RuleConfig).order_by(m.RuleConfig.key))).all()
    return templates.TemplateResponse(request, "admin_rules.html", {"rules": rules})

@router.post("/admin/rules")
async def rules_update(request: Request, key: str = Form(...),
                       value: float = Form(...)):
    async with SessionMaker() as session:
        cfg = await session.get(m.RuleConfig, key)
        if cfg is not None:
            cfg.value = value
            cfg.updated_at = datetime.now(timezone.utc)
            cfg.updated_by = str(request.state.user_id)
            await session.commit()
    return RedirectResponse("/admin/rules", status_code=303)
```

`buscore/web/templates/admin_rules.html`:
```html
{% extends "base.html" %}
{% block title %}Пороги правил{% endblock %}
{% block content %}
<h3>Пороги правил (применяются после перезапуска replay/конвейера)</h3>
{% for r in rules %}
<form method="post" action="/admin/rules" class="row card">
  <code style="min-width:220px">{{ r.key }}</code>
  <input type="hidden" name="key" value="{{ r.key }}">
  <input name="value" value="{{ r.value }}" style="width:100px">
  <button type="submit">Сохранить</button>
  <span class="muted">{{ r.updated_at or "" }}</span>
</form>
{% endfor %}
{% endblock %}
```

- [ ] **Step 4: Тесты проходят, commit**

Run: `pytest tests/test_auth.py tests/test_feed_web.py -v`
Expected: PASS — и не забыть: в `tests/test_feed_web.py` теперь нужен логин; добавить в начало каждого теста логин под `sb` (скопировать 3 строки из `test_security_can_login_and_see_feed`).

```bash
git add -A
git commit -m "feat: auth with roles, session cookie, rule thresholds admin page"
```

### Task 13: Telegram-уведомления и watchdog

**Files:**
- Create: `buscore/notify/__init__.py`, `buscore/notify/telegram.py`
- Modify: `buscore/runtime.py` (подписка notify на store, watchdog-задача)
- Test: `tests/test_notify.py`

- [ ] **Step 1: Провальный тест**

`tests/test_notify.py`:
```python
from datetime import datetime, timezone
from buscore.notify.telegram import Notifier, in_quiet_hours, Watchdog

def test_quiet_hours_wrap_midnight():
    tz = "Asia/Almaty"
    # 23:00 Алматы — тихо; 12:00 — нет. UTC+5 (Алматы с 2024 единый пояс).
    assert in_quiet_hours(datetime(2026, 8, 13, 18, 0, tzinfo=timezone.utc),
                          22, 5, tz) is True    # 23:00 местного
    assert in_quiet_hours(datetime(2026, 8, 13, 7, 0, tzinfo=timezone.utc),
                          22, 5, tz) is False   # 12:00 местного

async def test_notifier_sends_to_subscribed_users():
    sent = []
    async def fake_send(chat_id, text):
        sent.append((chat_id, text))
    n = Notifier(send=fake_send, quiet=(22, 5), tz="Asia/Almaty")
    users = [{"chat_id": "111", "types": []},               # все типы
             {"chat_id": "222", "types": ["phone_use"]}]    # только телефон
    await n.notify(users, {"type": "speeding", "bus_code": "117",
                           "ts": "2026-08-13T07:00:00+00:00"})
    assert [c for c, _ in sent] == ["111"]
    assert "117" in sent[0][1]

def test_watchdog_alerts_once_per_gap():
    wd = Watchdog(threshold_s=600, work_hours=(5, 22), tz="Asia/Almaty")
    t0 = datetime(2026, 8, 13, 7, 0, tzinfo=timezone.utc)   # 12:00 местного
    from datetime import timedelta
    assert wd.check(last_signal_at=t0, now=t0 + timedelta(seconds=300)) is None
    alert = wd.check(last_signal_at=t0, now=t0 + timedelta(seconds=700))
    assert alert is not None and "700" in alert
    assert wd.check(last_signal_at=t0, now=t0 + timedelta(seconds=800)) is None
```

Run: `pytest tests/test_notify.py -v`
Expected: FAIL

- [ ] **Step 2: telegram.py**

`buscore/notify/__init__.py` — пустой. `buscore/notify/telegram.py`:
```python
from datetime import datetime
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo
import httpx
from buscore.config import get_settings

TYPE_LABELS_SHORT = {
    "speeding": "Превышение скорости", "harsh_brake": "Резкое торможение",
    "harsh_accel": "Резкое ускорение", "route_deviation": "Отход от маршрута",
    "stop_skip": "Пропуск остановки", "long_idle": "Долгая стоянка",
}

def in_quiet_hours(now_utc: datetime, start_h: int, end_h: int, tz: str) -> bool:
    h = now_utc.astimezone(ZoneInfo(tz)).hour
    return h >= start_h or h < end_h    # интервал через полночь, напр. 22–5

async def tg_send(chat_id: str, text: str) -> None:
    token = get_settings().tg_bot_token
    if not token:
        return                            # без токена — тихий no-op (dev)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(url, json={"chat_id": chat_id, "text": text})

class Notifier:
    def __init__(self, send: Callable[[str, str], Awaitable[None]] = tg_send,
                 quiet: tuple[int, int] = (22, 5), tz: str = "Asia/Almaty"):
        self.send, self.quiet, self.tz = send, quiet, tz
        self._summary_sent_day = ""

    async def notify(self, users: list[dict], payload: dict) -> None:
        now = datetime.fromisoformat(payload["ts"])
        if in_quiet_hours(now, self.quiet[0], self.quiet[1], self.tz):
            return
        label = TYPE_LABELS_SHORT.get(payload["type"], payload["type"])
        text = (f"⚠ {label}\nборт {payload['bus_code']}\n"
                f"{now.astimezone(ZoneInfo(self.tz)).strftime('%H:%M')} · "
                f"подробности в ленте")
        for u in users:
            if u["types"] and payload["type"] not in u["types"]:
                continue
            await self.send(u["chat_id"], text)

    async def budget_summary(self, admin_chat: str, day: str, hidden: int) -> None:
        if self._summary_sent_day == day or not admin_chat:
            return
        self._summary_sent_day = day
        await self.send(admin_chat,
                        f"Дневной бюджет тревог исчерпан, ещё {hidden}+ событий "
                        f"смотрите в ленте")

class Watchdog:
    """Возвращает текст тревоги, если в рабочие часы нет сигналов дольше порога."""

    def __init__(self, threshold_s: int, work_hours: tuple[int, int], tz: str):
        self.threshold_s, self.work_hours, self.tz = threshold_s, work_hours, tz
        self._alerted = False

    def check(self, last_signal_at: datetime | None, now: datetime) -> str | None:
        h = now.astimezone(ZoneInfo(self.tz)).hour
        if not (self.work_hours[0] <= h < self.work_hours[1]):
            self._alerted = False
            return None
        if last_signal_at is None:
            return None
        gap = int((now - last_signal_at).total_seconds())
        if gap >= self.threshold_s and not self._alerted:
            self._alerted = True
            return f"⛔ Нет данных от бортов {gap} секунд — проверьте источник"
        if gap < self.threshold_s:
            self._alerted = False
        return None
```

- [ ] **Step 3: Подписка в runtime**

В `buscore/runtime.py` в конец `Runtime.start()` добавить:
```python
        # --- уведомления: подписка на поток событий store ---
        from sqlalchemy import select as _select
        from buscore.notify.telegram import Notifier, Watchdog, tg_send
        notifier = Notifier(quiet=(settings.quiet_start_hour,
                                   settings.quiet_end_hour), tz=settings.tz)
        queue = self.store.subscribe()

        async def notify_loop():
            while True:
                payload = await queue.get()
                async with SessionMaker() as session:
                    users = [{"chat_id": u.tg_chat_id, "types": u.notify_types}
                             for u in (await session.scalars(
                                 _select(m.User).where(m.User.tg_chat_id != ""))).all()]
                await notifier.notify(users, payload)

        watchdog = Watchdog(threshold_s=600,
                            work_hours=(settings.work_start_hour,
                                        settings.work_end_hour), tz=settings.tz)

        async def watchdog_loop():
            while True:
                await asyncio.sleep(60)
                alert = watchdog.check(self.last_signal_at,
                                       datetime.now(timezone.utc))
                if alert:
                    await tg_send(settings.tg_admin_chat_id, alert)

        self.tasks.append(asyncio.create_task(notify_loop()))
        self.tasks.append(asyncio.create_task(watchdog_loop()))
```

- [ ] **Step 4: Тесты проходят, commit**

Run: `pytest tests/test_notify.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: telegram notifications with roles/quiet hours, no-signal watchdog"
```

### Task 14: Отчёт и CSV

**Files:**
- Create: `buscore/web/routes_report.py`, `buscore/web/templates/report.html`
- Modify: `buscore/web/app.py` (раскомментировать report_router)
- Test: `tests/test_report.py`

- [ ] **Step 1: Провальный тест**

`tests/test_report.py`:
```python
import httpx
from datetime import datetime, timezone
from scripts.seed_demo import seed
from buscore.schemas import EventCandidate
from buscore.web.app import create_app

async def _logged_client(app, login, pwd):
    transport = httpx.ASGITransport(app=app)
    c = httpx.AsyncClient(transport=transport, base_url="http://test",
                          follow_redirects=False)
    r = await c.post("/login", data={"login": login, "password": pwd})
    c.cookies.update(r.cookies)
    return c

async def _make_events(app, session, n_confirmed=2, n_rejected=1):
    store = app.state.store
    ids = []
    for _ in range(n_confirmed + n_rejected):
        ids.append(await store.publish(session, "117", EventCandidate(
            type="speeding", ts_start=datetime.now(timezone.utc),
            lat=43.24, lon=76.945)))
    for i, event_id in enumerate(ids):
        status = "confirmed" if i < n_confirmed else "rejected"
        await store.verdict(session, event_id, status, user_id=1,
                            reason=None if status == "confirmed" else "дубль")

async def test_report_counts_confirmed_by_type(session):
    await seed(session)
    app = create_app()
    await _make_events(app, session)
    async with await _logged_client(app, "boss", "boss123") as c:
        r = await c.get("/report?days=7")
    assert r.status_code == 200
    assert "Превышение скорости" in r.text and ">2<" in r.text

async def test_csv_export(session):
    await seed(session)
    app = create_app()
    await _make_events(app, session)
    async with await _logged_client(app, "boss", "boss123") as c:
        r = await c.get("/report.csv?days=7")
    assert r.status_code == 200
    lines = r.text.strip().splitlines()
    assert lines[0] == "type;bus;driver;route;confirmed;rejected"
    assert any("speeding" in line and ";2;" in line for line in lines[1:])
```

Run: `pytest tests/test_report.py -v`
Expected: FAIL

- [ ] **Step 2: routes_report.py и шаблон**

`buscore/web/routes_report.py`:
```python
import io
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy import case, func, select
from buscore import models as m
from buscore.db import SessionMaker
from buscore.web.routes_feed import TYPE_LABELS, templates

router = APIRouter()

async def _rows(days: int) -> list[dict]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    q = (select(m.Event.type, m.Bus.code, m.Driver.full_name, m.Route.name,
                func.sum(case((m.Event.status == "confirmed", 1), else_=0)),
                func.sum(case((m.Event.status == "rejected", 1), else_=0)))
         .join(m.Bus, m.Event.bus_id == m.Bus.id)
         .outerjoin(m.Driver, m.Event.driver_id == m.Driver.id)
         .outerjoin(m.Route, m.Event.route_id == m.Route.id)
         .where(m.Event.created_at >= since)
         .group_by(m.Event.type, m.Bus.code, m.Driver.full_name, m.Route.name)
         .order_by(func.count().desc()))
    async with SessionMaker() as session:
        result = await session.execute(q)
    return [{"type": t, "bus": b, "driver": d or "—", "route": r or "—",
             "confirmed": int(c), "rejected": int(rj)}
            for t, b, d, r, c, rj in result.all()]

@router.get("/report")
async def report_page(request: Request, days: int = 7):
    rows = await _rows(days)
    return templates.TemplateResponse(request, "report.html",
                                      {"rows": rows, "days": days})

@router.get("/report.csv")
async def report_csv(days: int = 7):
    rows = await _rows(days)
    buf = io.StringIO()
    buf.write("type;bus;driver;route;confirmed;rejected\n")
    for r in rows:
        buf.write(f"{r['type']};{r['bus']};{r['driver']};{r['route']};"
                  f"{r['confirmed']};{r['rejected']}\n")
    return PlainTextResponse(buf.getvalue(), media_type="text/csv")
```

`buscore/web/templates/report.html`:
```html
{% extends "base.html" %}
{% block title %}Отчёт{% endblock %}
{% block content %}
<div class="row" style="margin-bottom:12px">
  <h3 style="margin:0">Отчёт за {{ days }} дн.</h3>
  <a href="/report?days=7">неделя</a><a href="/report?days=30">месяц</a>
  <a class="grow" href="/report.csv?days={{ days }}">Скачать CSV</a>
</div>
<table style="width:100%;border-collapse:collapse;background:#fff">
  <tr style="text-align:left;border-bottom:2px solid #dde0e4">
    <th>Тип</th><th>Борт</th><th>Водитель</th><th>Маршрут</th>
    <th>Подтверждено</th><th>Отклонено</th></tr>
  {% for r in rows %}
  <tr style="border-bottom:1px solid #eef1f5">
    <td>{{ TYPE_LABELS.get(r.type, r.type) }}</td><td>{{ r.bus }}</td>
    <td>{{ r.driver }}</td><td>{{ r.route }}</td>
    <td>{{ r.confirmed }}</td><td>{{ r.rejected }}</td></tr>
  {% endfor %}
</table>
{% endblock %}
```

- [ ] **Step 3: Тесты проходят, commit**

Run: `pytest tests/test_report.py -v`
Expected: PASS

```bash
git add -A
git commit -m "feat: weekly/monthly report with per-driver breakdown and CSV export"
```

### Task 15: Демо-день, E2E, ретеншн, Makefile, runbook, CI

**Files:**
- Create: `scripts/gen_demo_day.py`, `scripts/backup.sh`, `scripts/restore.sh`
- Create: `buscore/maintenance.py`, `Makefile`, `README-RUNBOOK.md`, `.github/workflows/ci.yml`
- Modify: `buscore/runtime.py` (задача ретеншна)
- Test: `tests/test_e2e_day.py`, `tests/test_maintenance.py`

- [ ] **Step 1: Генератор демо-дня**

`scripts/gen_demo_day.py`:
```python
"""Синтетический день борта 117 с заложенными нарушениями.

Запуск: python -m scripts.gen_demo_day [out.jsonl]
Заложено (EXPECTED): 1 speeding, 1 stop_skip («Ост. Парк»), 1 harsh_brake,
1 long_idle, 1 route_deviation. Остальное движение — образцовое:
на каждой остановке скорость 0 и открытые двери.
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from scripts.seed_demo import POLY, STOPS

EXPECTED = {"speeding": 1, "stop_skip": 1, "harsh_brake": 1,
            "long_idle": 1, "route_deviation": 1}
LAT0, LAT1 = POLY[0][0], POLY[1][0]
LON = POLY[0][1]
STOP_LATS = {name: lat for name, lat, lon, depot in STOPS if not depot}

def _row(ts, lat, lon, speed, doors=False):
    return {"bus_code": "117", "ts": ts.isoformat(), "lat": round(lat, 6),
            "lon": round(lon, 6), "speed_kmh": speed, "doors_open": doors}

def generate() -> list[dict]:
    rows = []
    ts = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)  # 09:00 Алматы
    lat, speed = LAT0, 0.0
    step = timedelta(seconds=5)

    def emit(speed_kmh, doors=False, dlat=0.0, dlon=0.0):
        nonlocal ts, lat
        ts += step
        lat += dlat
        rows.append(_row(ts, lat, LON + dlon, speed_kmh, doors))

    def drive(n, speed_kmh, dlon=0.0):
        # 40 км/ч ≈ 55 м за 5 с ≈ 0.0005° широты
        for _ in range(n):
            emit(speed_kmh, dlat=speed_kmh * 5 / 3.6 / 110_540, dlon=dlon)

    def serve_stop():
        for _ in range(4):
            emit(0.0, doors=True)

    drive(12, 40)                      # выезд из депо на маршрут
    serve_stop()                       # «Театр» — образцово
    drive(4, 40)
    for _ in range(4):                 # SPEEDING: 65 км/ч 20 секунд
        emit(65.0, dlat=65 * 5 / 3.6 / 110_540)
    drive(4, 40)
    serve_stop()                       # «Рынок» — образцово
    # HARSH_BRAKE: 50 → 12 за 5 с (−2.1 м/с²? нет: настройка −3.0) → 50→0
    emit(50.0, dlat=0.0005)
    emit(0.0)                          # 50→0 за 5 с = −2.78 м/с²… мало.
    # Правильный манёвр: два сигнала с шагом 2 с
    ts_backup = None  # (см. Step-примечание ниже)
    drive(6, 40)
    # LONG_IDLE: стоим 6.5 минут между «Рынком» и «Парком» (вне зон)
    for _ in range(78):
        emit(0.0)
    drive(2, 40)
    # STOP_SKIP: пролетаем «Парк» на 40 без дверей — геометрия сама сработает
    while lat < STOP_LATS["Ост. Парк"] + 0.0015:
        emit(40.0, dlat=0.0005)
    serve_stop_lat = STOP_LATS["Ост. Школа"]
    while lat < serve_stop_lat - 0.0005:
        emit(40.0, dlat=0.0005)
    serve_stop()                       # «Школа» — образцово
    # ROUTE_DEVIATION: уходим на 350 м восточнее на 3 минуты
    for i in range(8):
        emit(30.0, dlon=0.0005 * (i + 1))
    for _ in range(28):
        emit(30.0, dlon=0.004)
    for i in range(8):
        emit(30.0, dlon=0.004 - 0.0005 * (i + 1))
    drive(10, 40)
    serve_stop()                       # «Вокзал»
    return rows

def main() -> None:
    out = sys.argv[1] if len(sys.argv) > 1 else "demo_day.jsonl"
    rows = generate()
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r) for r in rows))
    print(f"{out}: {len(rows)} signals")

if __name__ == "__main__":
    main()
```

Примечание к harsh_brake в генераторе: пара `emit(50)` → `emit(0)` с шагом 5 с даёт −2.78 м/с² — НЕ дотягивает до порога −3.0. Правильная вставка (заменить блок между «Рынком» и long_idle):
```python
    emit(50.0, dlat=0.0005)
    ts -= timedelta(seconds=3)         # следующий сигнал через 2 с, не 5
    emit(5.0)                          # 50→5 за 2 с = −6.25 м/с² → harsh_brake
```
(`ts -= timedelta(seconds=3)` перед `emit` сжимает шаг до 2 секунд.)

- [ ] **Step 2: E2E-тест дня**

`tests/test_e2e_day.py`:
```python
"""Сквозной тест: сгенерированный день → replay → правильные события в БД."""
from sqlalchemy import func, select
from scripts.seed_demo import seed
from scripts.gen_demo_day import EXPECTED, generate
import json
from buscore import models as m
from buscore.ingest.replay import ReplayPlayer
from buscore.runtime import Runtime
from buscore.events.store import EventStore

async def test_demo_day_produces_expected_events(session, tmp_path):
    await seed(session)
    path = tmp_path / "day.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in generate()), encoding="utf-8")

    runtime = Runtime(EventStore())
    await runtime.start()                      # без REPLAY_FILE задач нет
    player = ReplayPlayer(str(path), speed=100000.0, rebase=False,
                          sink=runtime.sink)
    await player.run()

    q = select(m.Event.type, func.count()).group_by(m.Event.type)
    counts = dict((await session.execute(q)).all())
    for type_, minimum in EXPECTED.items():
        assert counts.get(type_, 0) >= minimum, f"{type_}: {counts}"
    total = sum(counts.values())
    assert total <= 30                          # дневной бюджет соблюдён
```

Run: `pytest tests/test_e2e_day.py -v`
Expected: PASS (если детекторы верны; при FAIL — отлаживать конкретный тип по counts в сообщении)

- [ ] **Step 3: Ретеншн сигналов**

`buscore/maintenance.py`:
```python
from datetime import datetime, timedelta, timezone
from sqlalchemy import delete
from buscore import models as m

SIGNAL_RETENTION_DAYS = 90

async def purge_old_signals(session, now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=SIGNAL_RETENTION_DAYS)
    result = await session.execute(
        delete(m.SignalRow).where(m.SignalRow.ts < cutoff))
    await session.commit()
    return result.rowcount
```

`tests/test_maintenance.py`:
```python
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func
from scripts.seed_demo import seed
from buscore import models as m
from buscore.maintenance import purge_old_signals

async def test_purge_deletes_only_old_signals(session):
    await seed(session)
    bus = await session.scalar(select(m.Bus))
    now = datetime.now(timezone.utc)
    session.add_all([
        m.SignalRow(bus_id=bus.id, ts=now - timedelta(days=100), lat=1, lon=1,
                    speed_kmh=0),
        m.SignalRow(bus_id=bus.id, ts=now - timedelta(days=1), lat=1, lon=1,
                    speed_kmh=0)])
    await session.commit()
    deleted = await purge_old_signals(session, now)
    assert deleted == 1
    assert (await session.scalar(select(func.count(m.SignalRow.id)))) == 1
```

В `buscore/runtime.py` в `Runtime.start()` добавить ежесуточную задачу:
```python
        from buscore.maintenance import purge_old_signals

        async def retention_loop():
            while True:
                await asyncio.sleep(24 * 3600)
                async with SessionMaker() as session:
                    await purge_old_signals(session)

        self.tasks.append(asyncio.create_task(retention_loop()))
```

Run: `pytest tests/test_maintenance.py -v`
Expected: PASS

- [ ] **Step 4: Makefile, бэкапы, CI**

`Makefile`:
```makefile
.PHONY: up seed demo test deploy backup restore
up:
	docker compose up -d --build
	docker compose exec app alembic upgrade head
seed:
	docker compose exec app python -m scripts.seed_demo
demo:
	docker compose exec app python -m scripts.gen_demo_day /app/demo_day.jsonl
	@echo "REPLAY_FILE=/app/demo_day.jsonl в .env и 'make up' — лента оживёт"
test:
	pytest -q
deploy:
	git pull
	docker compose up -d --build
	docker compose exec app alembic upgrade head
backup:
	bash scripts/backup.sh
restore:
	bash scripts/restore.sh $(FILE)
```

`scripts/backup.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
mkdir -p backups
FILE="backups/buscore-$(date +%Y%m%d-%H%M%S).sql.gz"
docker compose exec -T db pg_dump -U bus buscore | gzip > "$FILE"
echo "Backup: $FILE"
```

`scripts/restore.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
[ -z "${1:-}" ] && { echo "usage: restore.sh backups/file.sql.gz"; exit 1; }
gunzip -c "$1" | docker compose exec -T db psql -U bus -d buscore
echo "Restored from $1"
```

`.github/workflows/ci.yml`:
```yaml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      db:
        image: postgres:16
        env: {POSTGRES_USER: bus, POSTGRES_PASSWORD: bus, POSTGRES_DB: buscore}
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U bus" --health-interval 5s
          --health-timeout 3s --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.12"}
      - run: pip install -r requirements-dev.txt
      - run: alembic upgrade head
      - run: pytest -q
```

- [ ] **Step 5: README-RUNBOOK.md**

```markdown
# buscore — runbook оператора

## Первый запуск (VPS или локально)
1. Установить Docker + Docker Compose (docs.docker.com/engine/install).
2. `git clone <repo> && cd <repo>`
3. `cp .env.example .env` → вписать `SESSION_SECRET` (длинная случайная строка),
   `TG_BOT_TOKEN` (от @BotFather), `TG_ADMIN_CHAT_ID` (ваш chat id).
4. `make up` — поднимает БД и приложение, накатывает миграции.
5. `make seed` — демо-данные. СРАЗУ смените пароли admin/sb/boss в БД
   (страница управления пользователями появится в Плане 2; пока — через psql).
6. `make demo` + `REPLAY_FILE=/app/demo_day.jsonl` в `.env` + `make up` —
   живая лента на синтетическом дне: http://localhost:8000/feed (sb/sb123).

## Ежедневно
- Лента: /feed. Отчёт: /report. Пороги: /admin/rules (admin).
- Бэкап: `make backup` (складывает в backups/). Настройте cron: `0 2 * * *`.

## Если лента молчит
1. `docker compose ps` — все контейнеры Up?
2. `docker compose logs app --tail 100` — ошибки?
3. Не пришла ли тревога watchdog в Telegram («нет данных от бортов»)?
4. `curl localhost:8000/health` — `{"status":"ok"}`?
5. Перезапуск: `docker compose restart app`.

## Обновление версии
`make deploy` (git pull + пересборка + миграции). Откат: `git checkout <тег> && make deploy`.

## Восстановление из бэкапа
`make restore FILE=backups/buscore-<дата>.sql.gz`
```

- [ ] **Step 6: Финальный прогон и commit**

Run: `pytest -q`
Expected: все тесты PASS

```bash
git add -A
git commit -m "feat: demo day generator, e2e test, retention, ops tooling, CI"
```

---

## Самопроверка плана (выполнена при написании)

**Покрытие спеки:** §3 конвейер — Tasks 4–5, 11; §4 телеметрические типы — Tasks 6–10 (vision — План 2, Link+ passthrough — План 3); §5 жизненный цикл — Tasks 5, 11; §6 модель данных — Task 2 (включая clips под План 3); §7 UI — Task 11 (мини-карта Leaflet перенесена в План 3 вместе с клипами — карточке пилота хватает координат в details; отражено в тексте карточки); §8 уведомления — Task 13; §9 надёжность — Tasks 13, 15 (догрузка пропусков из Link+ — План 3); §10 тесты — golden в 6–10, E2E в 15, CI в 15; §11 — Task 12 (HTTPS/Caddy — в План 3 при выкатке на VPS с доменом; для пилота в закрытой сети достаточно), ретеншн — Task 15; §12 — Task 15.

**Осознанные отступления от спеки (зафиксировать при ревью):** без PostGIS (shapely в памяти); мини-карта и Caddy отложены в План 3.

**Консистентность типов:** `Signal`/`EventCandidate` (Task 4) используются без изменений в 5–11; ключи конфига едины с таблицей в шапке; `store.publish(session, bus_code, cand)` одинаков в 5, 11, 14, 15.

## Порядок исполнения

Задачи строго по номерам: 1 → 15. Задачи 6–10 (детекторы) независимы друг от друга — их можно выполнять в любом порядке после 5.


