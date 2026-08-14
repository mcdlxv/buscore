from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sse_starlette.sse import EventSourceResponse
from buscore import models as m
from buscore.config import get_settings
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
    """Счётчики за текущий день по времени парка (спека §7: статистика дня)."""
    tz = ZoneInfo(get_settings().tz)
    day_start = (datetime.now(tz).replace(hour=0, minute=0, second=0,
                                          microsecond=0))
    q = (select(m.Event.status, func.count())
         .where(m.Event.created_at >= day_start)
         .group_by(m.Event.status))
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
