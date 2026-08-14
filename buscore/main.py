from contextlib import asynccontextmanager
from fastapi import FastAPI
from buscore.runtime import Runtime
from buscore.web.app import create_app

runtime: Runtime | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global runtime
    runtime = Runtime(app.state.store)
    await runtime.start()
    app.state.runtime = runtime
    yield
    await runtime.stop()

app = create_app()
app.router.lifespan_context = lifespan
