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
        return require_role(request, "admin")

    app.include_router(auth_router)
    app.include_router(feed_router, dependencies=[Depends(guard_security)])
    app.include_router(admin_router, dependencies=[Depends(guard_admin)])
    app.include_router(report_router, dependencies=[Depends(guard_manager)])

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
