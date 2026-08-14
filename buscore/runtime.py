"""Сборка живого конвейера: контексты бортов из БД, engine, фоновые задачи."""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Coroutine
from zoneinfo import ZoneInfo
from sqlalchemy import select
from buscore import models as m
from buscore.config import get_settings
from buscore.db import SessionMaker
from buscore.events.store import EventStore
from buscore.geo import RouteGeometry
from buscore.ingest.replay import ReplayPlayer
from buscore.notify.telegram import Notifier, Watchdog, tg_send
from buscore.maintenance import purge_old_signals
from buscore.rules.engine import BusContext, EventEngine, FilterChain, StopZone
from buscore.rules.detectors.speeding import SpeedingDetector
from buscore.rules.detectors.harsh import HarshDetector
from buscore.rules.detectors.idle import LongIdleDetector
from buscore.rules.detectors.route_dev import RouteDeviationDetector
from buscore.rules.detectors.stop_skip import StopSkipDetector

log = logging.getLogger("buscore.runtime")

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

    def _spawn(self, coro: Coroutine, name: str) -> None:
        """Фоновая задача с журналированием падений: тишина — не вариант."""
        task = asyncio.create_task(coro, name=name)

        def _done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log.error("фоновая задача %s упала", name, exc_info=exc)
            else:
                log.info("фоновая задача %s завершилась", name)

        task.add_done_callback(_done)
        self.tasks.append(task)

    async def start(self) -> None:
        settings = get_settings()
        tz = ZoneInfo(settings.tz)
        async with SessionMaker() as session:
            config = await load_configs(session)
            contexts = await build_contexts(session, config)
        cooldowns = {k.removeprefix("cooldown_s."): v
                     for k, v in config.items() if k.startswith("cooldown_s.")}
        filters = FilterChain(cooldowns=cooldowns,
                              daily_budget=int(config["daily_budget"]),
                              tz=settings.tz)
        notifier = Notifier(quiet=(settings.quiet_start_hour,
                                   settings.quiet_end_hour), tz=settings.tz)

        async def publish(bus_code, cand):
            async with SessionMaker() as session:
                event_id = await self.store.publish(session, bus_code, cand)
            log.info("событие #%s %s борт %s", event_id, cand.type, bus_code)

        async def on_budget_exceeded(cand):
            day = cand.ts_start.astimezone(tz).date().isoformat()
            log.warning("бюджет тревог исчерпан, скрыто событий: %s",
                        filters.suppressed_today)
            await notifier.budget_summary(settings.tg_admin_chat_id, day,
                                          filters.suppressed_today)

        self.engine = EventEngine(DETECTORS, filters, publish,
                                  on_budget_exceeded=on_budget_exceeded)
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
            log.info("replay: файл %s, скорость x%s", settings.replay_file,
                     settings.replay_speed)
            player = ReplayPlayer(settings.replay_file, settings.replay_speed,
                                  settings.replay_rebase, sink)
            self._spawn(player.run(), "replay")

        queue = self.store.subscribe()

        async def notify_loop():
            while True:
                payload = await queue.get()
                try:
                    async with SessionMaker() as session:
                        users = [{"chat_id": u.tg_chat_id, "types": u.notify_types}
                                 for u in (await session.scalars(
                                     select(m.User)
                                     .where(m.User.tg_chat_id != ""))).all()]
                    await notifier.notify(users, payload)
                except Exception:
                    log.exception("notify_loop: ошибка отправки, продолжаю")

        watchdog = Watchdog(threshold_s=600,
                            work_hours=(settings.work_start_hour,
                                        settings.work_end_hour), tz=settings.tz)

        async def watchdog_loop():
            while True:
                await asyncio.sleep(60)
                try:
                    alert = watchdog.check(self.last_signal_at,
                                           datetime.now(timezone.utc))
                    if alert:
                        log.warning("watchdog: %s", alert)
                        await tg_send(settings.tg_admin_chat_id, alert)
                except Exception:
                    log.exception("watchdog_loop: ошибка, продолжаю")

        async def retention_loop():
            while True:
                await asyncio.sleep(24 * 3600)
                try:
                    async with SessionMaker() as session:
                        deleted = await purge_old_signals(session)
                    log.info("ретеншн: удалено сигналов старше 90 дней: %s", deleted)
                except Exception:
                    log.exception("retention_loop: ошибка, продолжаю")

        self._spawn(notify_loop(), "notify")
        self._spawn(watchdog_loop(), "watchdog")
        self._spawn(retention_loop(), "retention")
        log.info("конвейер запущен: бортов %s, детекторов %s",
                 len(contexts), len(DETECTORS))

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
