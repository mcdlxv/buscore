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

        # --- уведомления: подписка на поток событий store ---
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
                                 select(m.User).where(m.User.tg_chat_id != ""))).all()]
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

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
