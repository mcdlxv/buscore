from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from passlib.hash import bcrypt
from sqlalchemy import select
from buscore import models as m
from buscore.config import get_settings
from buscore.db import SessionMaker
from buscore.web.routes_feed import templates

SESSION_MAX_AGE_S = 12 * 3600   # смена СБ; после — перелогин

router = APIRouter()
_signer = URLSafeTimedSerializer(get_settings().session_secret, salt="session")

def make_cookie(user: m.User) -> str:
    return _signer.dumps({"id": user.id, "role": user.role})

def read_session(request: Request) -> dict | None:
    raw = request.cookies.get("session")
    if not raw:
        return None
    try:
        return _signer.loads(raw, max_age=SESSION_MAX_AGE_S)
    except (BadSignature, SignatureExpired):
        return None

def require_role(request: Request, *roles: str) -> dict:
    """303 на /login без сессии; 403 при чужой роли. admin проходит всюду."""
    sess = read_session(request)
    if sess is None:
        raise HTTPException(303, headers={"Location": "/login"})
    allowed = set(roles) | {"admin"}
    if sess["role"] not in allowed:
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
    resp.set_cookie("session", make_cookie(user), httponly=True, samesite="lax",
                    max_age=SESSION_MAX_AGE_S)
    return resp

@router.get("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie("session")
    return resp
