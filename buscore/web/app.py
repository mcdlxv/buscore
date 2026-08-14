from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="buscore")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
