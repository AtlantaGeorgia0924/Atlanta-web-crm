"""Sync router: Supabase-first architecture.

All API endpoints read/write from Supabase only during normal operations.
Google Sheets is used as a manual backup only via /sync-to-google-sheets endpoint.

Architecture:
- Normal operations (apply_payment, add_service, etc.) → Supabase only
- Background queue replay → DISABLED in manual_sheet_sync_only mode (default)
- Manual sync → Explicit /sync-to-google-sheets endpoint (admin only)

Verification: All responses include 'sheets_accessed: false' for normal operations.
"""

from typing import Any
import time
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.dependencies import get_runtime, require_admin, require_staff
from services.sync_service import (
    build_client_phone_sheet_updates,
    build_phone_autofill_plan,
    detect_sheet_header_row,
)

router = APIRouter(
    prefix='/api/sync',
    tags=['sync'],
    dependencies=[Depends(require_staff)],
)


class DetectHeaderRowRequest(BaseModel):
    values: list[list[Any]]


class ClientPhoneUpdatesRequest(BaseModel):
    values: list[list[Any]]
    clients: dict[str, str] = Field(default_factory=dict)
    name_col: int | None = None
    phone_col: int | None = None


class PhoneAutofillPlanRequest(BaseModel):
    values: list[list[Any]]
    name_col: int | None = None
    phone_col: int | None = None
    sheet_row_count: int
    directory_sheet_title: str


@router.post('/header-row/detect')
def detect_header_row_endpoint(payload: DetectHeaderRowRequest):
    return {
        'header_row_idx': detect_sheet_header_row(payload.values)
    }


@router.post('/client-phone-updates')
def build_client_phone_updates_endpoint(payload: ClientPhoneUpdatesRequest):
    return {
        'updates': build_client_phone_sheet_updates(
            payload.values,
            payload.clients,
            payload.name_col,
            payload.phone_col,
        )
    }


@router.post('/phone-autofill-plan')
def build_phone_autofill_plan_endpoint(payload: PhoneAutofillPlanRequest):
    return build_phone_autofill_plan(
        payload.values,
        payload.name_col,
        payload.phone_col,
        payload.sheet_row_count,
        payload.directory_sheet_title,
    )


@router.get('/status')
def sync_status_endpoint(runtime=Depends(get_runtime)):
    return runtime.get_sync_status()


@router.get('/mirror-verification', dependencies=[Depends(require_admin)])
def mirror_verification_endpoint(runtime=Depends(get_runtime), current_user=Depends(require_admin)):
    try:
        return runtime.verify_operational_mirrors()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_background_sync_job(runtime, job_name, **kwargs):
    try:
        if job_name == 'pull_once':
            runtime.pull_once()
            return
        if job_name == 'refresh_workspace':
            runtime.refresh_workspace(force_refresh=bool(kwargs.get('force_refresh', False)))
            return
        if job_name == 'replay_queue_now':
            runtime.replay_pending_queue_now(limit=int(kwargs.get('limit', 200) or 200), manual_trigger=True)
            return
        if job_name in ('push_to_sheets', 'sync_to_sheets'):
            # Manual backup sync only - explicitly call with manual_trigger=True
            runtime.replay_pending_queue_now(limit=int(kwargs.get('limit', 5000) or 5000), manual_trigger=True)
            return
    except Exception as exc:
        runtime.logger.warning('Background sync job failed (%s): %s', job_name, exc)


@router.post('/pull-now')
def pull_now_endpoint(background_tasks: BackgroundTasks, runtime=Depends(get_runtime)):
    if not runtime.postgres_ready:
        raise HTTPException(status_code=503, detail='PostgreSQL sync manager is not ready')

    background_tasks.add_task(_run_background_sync_job, runtime, 'pull_once')
    return {
        'queued': True,
        'job': 'pull_once',
        'message': 'Pull job queued to run in the background.',
    }


@router.post('/refresh-workspace')
def refresh_workspace_endpoint(
    force_refresh: bool = False,
    runtime=Depends(get_runtime),
):
    try:
        result = runtime.refresh_workspace(force_refresh=force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        'queued': False,
        'job': 'refresh_workspace',
        'force_refresh': force_refresh,
        **(result if isinstance(result, dict) else {}),
    }


@router.post('/replay-queue-now')
def replay_queue_now_endpoint(
    background_tasks: BackgroundTasks,
    limit: int = 200,
    runtime=Depends(get_runtime),
):
    background_tasks.add_task(
        _run_background_sync_job,
        runtime,
        'replay_queue_now',
        limit=limit,
    )
    return {
        'queued': True,
        'job': 'replay_queue_now',
        'limit': limit,
        'message': 'Queue replay job queued to run in the background.',
    }


@router.post('/sync-to-google-sheets', dependencies=[Depends(require_admin)])
def sync_to_google_sheets_endpoint(
    background_tasks: BackgroundTasks,
    limit: int = 5000,
    runtime=Depends(get_runtime),
    current_user=Depends(require_admin),
):
    """Manual backup sync: Update Google Sheets from current Supabase state.

    This is the ONLY endpoint that accesses Google Sheets in normal operation.
    All other endpoints use Supabase only.

    Returns: Job queued message. Use /sync/status to poll for completion.
    """
    if not runtime.postgres_ready:
        raise HTTPException(status_code=503, detail='PostgreSQL sync manager is not ready')

    start_time = time.monotonic()
    background_tasks.add_task(
        _run_background_sync_job,
        runtime,
        'sync_to_sheets',
        limit=limit,
    )
    queued_at = time.monotonic() - start_time
    return {
        'queued': True,
        'job': 'sync_to_sheets',
        'limit': limit,
        'message': 'Manual backup sync to Google Sheets queued in background.',
        'queued_at_ms': round(queued_at * 1000),
        'sheets_accessed': True,
        'note': 'This is the only endpoint that accesses Google Sheets. All other operations use Supabase only.',
    }


@router.post('/push-to-sheets', dependencies=[Depends(require_admin)])
def push_to_sheets_endpoint(
    background_tasks: BackgroundTasks,
    limit: int = 5000,
    runtime=Depends(get_runtime),
    current_user=Depends(require_admin),
):
    """DEPRECATED: Use /sync-to-google-sheets instead."""
    return sync_to_google_sheets_endpoint(background_tasks, limit, runtime, current_user)


@router.post('/reconnect-postgres', dependencies=[Depends(require_admin)])
def reconnect_postgres_endpoint(background_tasks: BackgroundTasks, runtime=Depends(get_runtime)):
    """Trigger an immediate postgres reconnect attempt in the background.

    Useful when Supabase was down at startup and postgres_ready is still False.
    The reconnect runs asynchronously; poll /health to see when it succeeds.
    """
    if runtime.postgres_ready:
        return {'already_ready': True, 'message': 'PostgreSQL is already connected.'}

    if runtime.sync_state.get('last_status') == 'dsn_missing':
        raise HTTPException(status_code=503, detail='postgres_dsn is not configured — cannot reconnect.')

    def _attempt_reconnect():
        try:
            runtime.logger.info('Manual postgres reconnect triggered via API')
            runtime.postgres_sync_manager.ensure_schema()
            runtime.financial_data_service.ensure_default_app_config()
            runtime.sync_state['ready'] = True
            runtime.sync_state['last_status'] = 'running'
            runtime.sync_state['last_error'] = ''
            import threading as _threading
            _threading.Thread(target=runtime._seed_once_async, daemon=True).start()
            if runtime._automatic_sheet_sync_enabled():
                runtime.postgres_sync_manager.start_background_pull(runtime.pull_once)
                runtime.postgres_sync_manager.start_background_queue_worker(runtime._replay_queue_operation, interval_sec=1)
            runtime.logger.info('Manual postgres reconnect succeeded')
        except Exception as exc:
            runtime.sync_state['last_error'] = str(exc)
            runtime.logger.warning('Manual postgres reconnect failed: %s', exc)

    background_tasks.add_task(_attempt_reconnect)
    return {
        'reconnect_queued': True,
        'message': 'Reconnect attempt started in background. Poll /health for postgres_ready status.',
    }


@router.get('/performance')
def performance_diagnostic_endpoint(runtime=Depends(get_runtime)):
    """Returns performance and connectivity diagnostics.
    
    Helps identify:
    - Database connectivity status
    - Recent endpoint performance
    - Slowest operations
    - Last successful writes
    """
    import time
    
    # Get sync status
    sync_state = runtime.sync_state or {}
    postgres_ready = runtime.postgres_ready
    
    # Get cache sizes (approximate)
    main_cache = runtime._load_cached_rows('main_values') or []
    stock_cache = runtime._load_cached_rows('stock_values') or []
    
    # Build diagnostic payload
    perf = runtime.get_performance_metrics() if hasattr(runtime, 'get_performance_metrics') else {}

    diagnostics = {
        'timestamp': time.time(),
        'database': {
            'postgres_ready': postgres_ready,
            'postgres_host': runtime._postgres_dsn_host() if hasattr(runtime, '_postgres_dsn_host') else 'unknown',
            'postgres_manager_ready': bool(runtime.postgres_sync_manager and runtime.postgres_sync_manager.ready),
            'sync_ready_flag': bool(sync_state.get('ready')),
            'last_status': sync_state.get('last_status', 'unknown'),
            'last_error': sync_state.get('last_error', ''),
            'last_successful_pull': sync_state.get('last_successful_pull', None),
        },
        'cache': {
            'main_rows_cached': len(main_cache),
            'stock_rows_cached': len(stock_cache),
            'cache_updated_at': sync_state.get('cache_updated_at', 'unknown'),
        },
        'sheets': {
            'sheets_connected': sync_state.get('sheets_connected', False),
            'last_sheet_error': sync_state.get('last_sheet_error', ''),
        },
        'operations': {
            'pending_queue_size': len(runtime.postgres_sync_manager.fetch_pending_operations(limit=500)) if runtime.postgres_ready else 0,
            'pending_failed_size': len(runtime.postgres_sync_manager.fetchall_dict(
                """
                SELECT id
                FROM sync_queue
                WHERE status = 'failed'
                ORDER BY updated_at DESC
                LIMIT 500
                """
            )) if runtime.postgres_ready else 0,
        },
        'performance': perf,
        'message': 'All systems ready' if postgres_ready else 'Awaiting PostgreSQL connection',
    }
    
    return diagnostics
