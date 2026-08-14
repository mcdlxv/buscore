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
