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
        self._suppressed = 0
        self.last_block: str | None = None   # None | "cooldown" | "budget"

    def _roll_day(self, ts: datetime) -> None:
        day = ts.astimezone(self.tz).date().isoformat()
        # только вперёд: ретроспективная догрузка старых точек (План 3)
        # не должна сбрасывать сегодняшний счётчик бюджета
        if self._day == "" or day > self._day:
            self._day, self._count, self._suppressed = day, 0, 0

    def allow(self, bus_code: str, cand: EventCandidate) -> bool:
        self._roll_day(cand.ts_start)
        key = (bus_code, cand.type)
        last = self._last.get(key)
        cooldown = self.cooldowns.get(cand.type, 0)
        if last is not None and (cand.ts_start - last).total_seconds() < cooldown:
            self.last_block = "cooldown"
            return False
        if self._count >= self.daily_budget:
            self.last_block = "budget"
            self._suppressed += 1
            return False
        self._last[key] = cand.ts_start
        self._count += 1
        self.last_block = None
        return True

    @property
    def suppressed_today(self) -> int:
        return self._suppressed

    def budget_exceeded_today(self, ts: datetime) -> bool:
        self._roll_day(ts)
        return self._count >= self.daily_budget

class EventEngine:
    """Прогоняет сигнал через детекторы контекста борта и публикует кандидатов."""

    def __init__(self, detectors: list[Detector], filters: FilterChain,
                 publish: Callable[[str, EventCandidate], Awaitable[None]],
                 on_budget_exceeded: Callable[[EventCandidate], Awaitable[None]]
                 | None = None):
        self.detectors = detectors
        self.filters = filters
        self.publish = publish
        self.on_budget_exceeded = on_budget_exceeded
        self.contexts: dict[str, BusContext] = {}

    async def process(self, sig: Signal) -> None:
        ctx = self.contexts.get(sig.bus_code)
        if ctx is None:
            return
        for det in self.detectors:
            cand = det.process(sig, ctx)
            if cand is None:
                continue
            if self.filters.allow(sig.bus_code, cand):
                await self.publish(sig.bus_code, cand)
            elif (self.filters.last_block == "budget"
                  and self.on_budget_exceeded is not None):
                await self.on_budget_exceeded(cand)
