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
