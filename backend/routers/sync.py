from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.dependencies import get_runtime, require_admin
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
            runtime.replay_pending_queue_now(limit=int(kwargs.get('limit', 200) or 200))
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
    background_tasks: BackgroundTasks,
    force_refresh: bool = False,
    runtime=Depends(get_runtime),
):
    background_tasks.add_task(
        _run_background_sync_job,
        runtime,
        'refresh_workspace',
        force_refresh=force_refresh,
    )
    return {
        'queued': True,
        'job': 'refresh_workspace',
        'force_refresh': force_refresh,
        'message': 'Workspace refresh queued to run in the background.',
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
