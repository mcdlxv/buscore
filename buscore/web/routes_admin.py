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
