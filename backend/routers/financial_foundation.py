from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi import Header
from pydantic import BaseModel, Field

from backend.dependencies import get_current_user, get_runtime, require_admin

router = APIRouter(
    prefix='/api/foundation',
    tags=['financial-foundation'],
    dependencies=[Depends(get_current_user)],
)


class CreateExpenseRequest(BaseModel):
    amount: float = Field(ge=0)
    category: str = ''
    description: str = ''
    date: str = ''


class AppConfigUpsertRequest(BaseModel):
    value: Any


class CashflowPinChangeRequest(BaseModel):
    current_pin: str = Field(min_length=4, max_length=4, pattern=r'^\d{4}$')
    new_pin: str = Field(min_length=4, max_length=4, pattern=r'^\d{4}$')


def _normalize_cashflow_pin(value: Any) -> str:
    text = ''.join(ch for ch in str(value or '').strip() if ch.isdigit())
    return text if len(text) == 4 else ''


def _get_cashflow_pin(runtime) -> str:
    item = runtime.financial_data_service.get_app_config('cashflow_pin', actor_role='admin')
    return _normalize_cashflow_pin((item or {}).get('value') if isinstance(item, dict) else '') or '1111'


def require_cashflow_pin(
    x_cashflow_pin: str = Header(default='', alias='X-Cashflow-PIN'),
    runtime=Depends(get_runtime),
    current_user=Depends(get_current_user),
):
    expected_pin = _get_cashflow_pin(runtime)
    provided_pin = _normalize_cashflow_pin(x_cashflow_pin)
    if not expected_pin or provided_pin != expected_pin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Cash flow PIN is required.',
        )
    return current_user


@router.post('/expenses', dependencies=[Depends(require_cashflow_pin)])
def create_expense(payload: CreateExpenseRequest, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        sheet_item = runtime.append_cashflow_expense_record(
            amount=payload.amount,
            category=payload.category,
            description=payload.description,
            date_text=payload.date,
            created_by=str((current_user or {}).get('username') or ''),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return {'expense': sheet_item, 'sheet_expense': sheet_item}


@router.get('/expenses', dependencies=[Depends(require_cashflow_pin)])
def list_expenses(limit: int = 200, offset: int = 0, runtime=Depends(get_runtime)):
    try:
        payload = runtime.get_cashflow_expense_records(force_refresh=False)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    items = payload.get('items') or []
    limited_items = items[offset:offset + max(1, int(limit or 200))]
    return {'expenses': limited_items, 'count': len(items), 'source': payload.get('source', 'database')}


@router.get('/audit-log', dependencies=[Depends(require_admin)])
def list_audit_log(
    limit: int = 200,
    offset: int = 0,
    runtime=Depends(get_runtime),
    current_user=Depends(get_current_user),
):
    try:
        items = runtime.financial_data_service.list_audit_logs(
            limit=limit,
            offset=offset,
            actor_role=str((current_user or {}).get('role') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'audit_log': items, 'count': len(items)}


@router.get('/app-config', dependencies=[Depends(require_admin)])
def list_app_config(runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        items = runtime.financial_data_service.list_app_config(
            actor_role=str((current_user or {}).get('role') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'items': items, 'count': len(items)}


@router.get('/app-config/{key}', dependencies=[Depends(require_admin)])
def get_app_config(key: str, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        item = runtime.financial_data_service.get_app_config(
            key=key,
            actor_role=str((current_user or {}).get('role') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Config key not found.')

    return {'item': item}


@router.put('/app-config/{key}', dependencies=[Depends(require_admin)])
def set_app_config(
    key: str,
    payload: AppConfigUpsertRequest,
    runtime=Depends(get_runtime),
    current_user=Depends(get_current_user),
):
    try:
        item = runtime.financial_data_service.set_app_config(
            key=key,
            value=payload.value,
            actor_role=str((current_user or {}).get('role') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'item': item}


@router.get('/cashflow-summary', dependencies=[Depends(require_admin), Depends(require_cashflow_pin)])
def get_cashflow_summary(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        summary = runtime.get_cashflow_summary_from_sheet(force_refresh=force_refresh)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'summary': summary}


@router.get('/cashflow-dashboard', dependencies=[Depends(require_admin), Depends(require_cashflow_pin)])
def get_cashflow_dashboard(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        summary = runtime.get_cashflow_summary_from_sheet(force_refresh=force_refresh)
        expense_summary = runtime.get_cashflow_expense_records(force_refresh=force_refresh)
        weekly_allowance = summary.get('weekly_allowance') or runtime.get_weekly_allowance_from_sheet(force_refresh=force_refresh)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {
        'summary': summary,
        'weekly_allowance': weekly_allowance,
        'expenses': expense_summary.get('items', []),
        'expense_source': expense_summary.get('source', 'database'),
        'expense_sheet_title': expense_summary.get('sheet_title', 'CASH FLOW'),
    }


@router.get('/weekly-allowance', dependencies=[Depends(require_admin), Depends(require_cashflow_pin)])
def get_weekly_allowance(runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        summary = runtime.financial_data_service.get_weekly_allowance_summary(
            actor_role=str((current_user or {}).get('role') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return summary


@router.post('/cashflow/rebuild-week', dependencies=[Depends(require_admin), Depends(require_cashflow_pin)])
def rebuild_cashflow_week(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result = runtime.rebuild_cashflow_sheet_for_current_week(force_refresh=force_refresh)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return result


@router.post('/cashflow-pin/change', dependencies=[Depends(require_admin)])
def change_cashflow_pin(payload: CashflowPinChangeRequest, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    expected_pin = _get_cashflow_pin(runtime)
    current_pin = _normalize_cashflow_pin(payload.current_pin)
    new_pin = _normalize_cashflow_pin(payload.new_pin)

    if current_pin != expected_pin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Current cash flow PIN is incorrect.',
        )
    if not new_pin:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='New cash flow PIN must be four digits.',
        )

    try:
        item = runtime.financial_data_service.set_app_config(
            key='cashflow_pin',
            value=new_pin,
            actor_role=str((current_user or {}).get('role') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'item': item}
