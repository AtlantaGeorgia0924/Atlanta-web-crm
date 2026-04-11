from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routers import assets, billing, clients, name_fix, stock, sync
from backend.runtime import BackendRuntime


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime = BackendRuntime()
    runtime.start()
    app.state.runtime = runtime
    try:
        yield
    finally:
        runtime.stop()


def create_app():
    app = FastAPI(
        title='Atlanta Georgia Tech API',
        version='0.1.0',
        description='HTTP facade over the extracted business-service modules.',
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    @app.get('/health', tags=['system'])
    def health():
        return {'status': 'ok'}

    app.include_router(billing.router)
    app.include_router(stock.router)
    app.include_router(clients.router)
    app.include_router(sync.router)
    app.include_router(name_fix.router)
    app.include_router(assets.router)
    return app


app = create_app()
