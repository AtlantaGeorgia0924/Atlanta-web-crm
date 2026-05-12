from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json

import asyncio

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.auth import AuthService
from backend.routers import assets, auth, billing, clients, financial_foundation, name_fix, stock, sync, users
from backend.runtime import BackendRuntime

WRITE_METHODS = {'POST', 'PUT', 'PATCH', 'DELETE'}
WRITE_METADATA_EXCLUDE_PATHS = (
    '/api/billing/outstanding-items/from-values',
    '/api/billing/outstanding-items/from-records',
    '/api/billing/debtors/compute',
    '/api/billing/payment-plan',
    '/api/billing/payment-plan/live',
    '/api/billing/bill/generate',
    '/api/billing/sales-snapshot',
    '/api/stock/live/stolen-devices/check',
)


def _normalize_table_name(path: str) -> str:
    normalized = str(path or '').lower()
    if normalized.startswith('/api/stock'):
        return 'operational_stock_rows'
    if normalized.startswith('/api/billing'):
        return 'operational_billing_rows'
    if normalized.startswith('/api/foundation/expenses'):
        return 'manual_expenses'
    if normalized.startswith('/api/foundation/allowance'):
        return 'allowance_withdrawals'
    if normalized.startswith('/api/foundation/cashflow'):
        return 'cashflow_summary'
    if normalized.startswith('/api/users'):
        return 'users'
    if normalized.startswith('/api/clients'):
        return 'clients'
    if normalized.startswith('/api/sync'):
        return 'sync_queue'
    return 'unknown'


def _extract_record_id(payload):
    if isinstance(payload, dict):
        for key in ('record_id', 'id', 'user_id', 'queued_operation_id', 'queue_id', 'row_num'):
            value = payload.get(key)
            if value not in (None, ''):
                return str(value)
        nested_item = payload.get('item')
        if isinstance(nested_item, dict):
            nested_id = _extract_record_id(nested_item)
            if nested_id:
                return nested_id
        for key in ('created', 'updated', 'result'):
            nested = payload.get(key)
            nested_id = _extract_record_id(nested)
            if nested_id:
                return nested_id
        queued_ids = payload.get('queued_operation_ids')
        if isinstance(queued_ids, list) and queued_ids:
            first = queued_ids[0]
            if first not in (None, ''):
                return str(first)
    elif isinstance(payload, list) and payload:
        return _extract_record_id(payload[0])
    return ''


def _is_write_metadata_target(method: str, path: str) -> bool:
    if str(method or '').upper() not in WRITE_METHODS:
        return False
    normalized_path = str(path or '')
    return not any(normalized_path.startswith(item) for item in WRITE_METADATA_EXCLUDE_PATHS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime = BackendRuntime()
    auth_service = AuthService(runtime.base_dir)
    auth_service.initialize()
    auth_service.ensure_default_admin()
    runtime.start()
    app.state.auth_service = auth_service
    app.state.runtime = runtime
    app.state.runtime_startup_thread = None
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
        allow_credentials=False,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    @app.middleware('http')
    async def add_request_timing(request, call_next):
        """Track request timing and add X-Response-Time header."""
        import time
        start_time = time.monotonic()
        response = await call_next(request)
        duration_ms = round((time.monotonic() - start_time) * 1000)
        response.headers['X-Response-Time-Ms'] = str(duration_ms)
        runtime = getattr(app.state, 'runtime', None)
        if runtime is not None and hasattr(runtime, 'record_endpoint_timing'):
            try:
                runtime.record_endpoint_timing(
                    request.method,
                    request.url.path,
                    response.status_code,
                    duration_ms,
                )
            except Exception:
                pass
        return response

    @app.middleware('http')
    async def enrich_write_metadata(request, call_next):
        response = await call_next(request)

        if not _is_write_metadata_target(request.method, request.url.path):
            return response

        content_type = str(response.headers.get('content-type') or '').lower()
        if 'application/json' not in content_type:
            return response

        body = b''
        async for chunk in response.body_iterator:
            body += chunk

        try:
            payload = json.loads(body.decode('utf-8') or '{}')
        except Exception:
            payload = {}

        saved_at = datetime.now(timezone.utc).isoformat()
        table_name = _normalize_table_name(request.url.path)
        record_id = _extract_record_id(payload)
        is_success = int(response.status_code) < 400

        if isinstance(payload, dict):
            payload['success'] = is_success
            payload['table'] = str(payload.get('table') or table_name)
            payload['record_id'] = str(payload.get('record_id') or record_id)
            payload['saved_at'] = str(payload.get('saved_at') or saved_at)
        else:
            payload = {
                'data': payload,
                'success': is_success,
                'table': table_name,
                'record_id': record_id,
                'saved_at': saved_at,
            }

        headers = dict(response.headers)
        headers.pop('content-length', None)
        return JSONResponse(
            content=payload,
            status_code=response.status_code,
            headers=headers,
            background=response.background,
        )

    @app.get('/health', tags=['system'])
    def health():
        runtime = getattr(app.state, 'runtime', None)
        if runtime is None:
            return {'status': 'starting'}
        return runtime.get_production_health()

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
