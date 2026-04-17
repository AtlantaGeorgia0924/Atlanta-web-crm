import threading
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.auth import AuthService
from backend.dependencies import get_current_user
from backend.routers import assets, auth, billing, clients, financial_foundation, name_fix, stock, sync, users
from backend.runtime import BackendRuntime


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime = BackendRuntime()
    auth_service = AuthService(runtime.base_dir)
    auth_service.initialize()
    auth_service.ensure_default_admin()
    startup_thread = threading.Thread(
        target=runtime.start,
        name='backend-runtime-startup',
        daemon=True,
    )
    startup_thread.start()
    app.state.auth_service = auth_service
    app.state.runtime = runtime
    app.state.runtime_startup_thread = startup_thread
    try:
        yield
    finally:
        auth_service.close()
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
    def health(_=Depends(get_current_user)):
        return {'status': 'ok'}

    app.include_router(auth.router)
    app.include_router(billing.router)
    app.include_router(stock.router)
    app.include_router(clients.router)
    app.include_router(sync.router)
    app.include_router(name_fix.router)
    app.include_router(financial_foundation.router)
    app.include_router(assets.router)
    app.include_router(users.router)
    return app


app = create_app()
