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
