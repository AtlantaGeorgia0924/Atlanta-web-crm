from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.dependencies import get_runtime
from services.sync_service import (
    build_client_phone_sheet_updates,
    build_phone_autofill_plan,
    detect_sheet_header_row,
)

router = APIRouter(prefix='/api/sync', tags=['sync'])


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


@router.post('/pull-now')
def pull_now_endpoint(runtime=Depends(get_runtime)):
    try:
        return runtime.pull_once()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post('/refresh-workspace')
def refresh_workspace_endpoint(force_refresh: bool = False, runtime=Depends(get_runtime)):
    try:
        return runtime.refresh_workspace(force_refresh=force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post('/replay-queue-now')
def replay_queue_now_endpoint(limit: int = 200, runtime=Depends(get_runtime)):
    try:
        return runtime.replay_pending_queue_now(limit=limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
