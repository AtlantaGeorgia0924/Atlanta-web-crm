import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
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
    allowance_impact: str = 'personal_allowance'


class AppConfigUpsertRequest(BaseModel):
    value: Any


class CashflowPinChangeRequest(BaseModel):
    current_pin: str = Field(min_length=4, max_length=4, pattern=r'^\d{4}$')
    new_pin: str = Field(min_length=4, max_length=4, pattern=r'^\d{4}$')


@router.post('/expenses')
def create_expense(payload: CreateExpenseRequest, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    category = str(payload.category or '').strip()
    allowance_impact = str(payload.allowance_impact or 'personal_allowance').strip().lower()
    if allowance_impact == 'business_only' and category:
        category = f'BUSINESS ONLY: {category}'

    try:
        sheet_item = runtime.append_cashflow_expense_record(
            amount=payload.amount,
            category=category,
            description=payload.description,
            date_text=payload.date,
            created_by=str((current_user or {}).get('username') or ''),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return {'expense': sheet_item, 'sheet_expense': sheet_item}


@router.get('/expenses')
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


@router.get('/cashflow-summary', dependencies=[Depends(require_admin)])
def get_cashflow_summary(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        summary = runtime.get_cashflow_summary_from_sheet(force_refresh=force_refresh)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'summary': summary}


@router.get('/cashflow-dashboard', dependencies=[Depends(require_admin)])
def get_cashflow_dashboard(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    started = time.perf_counter()
    try:
        summary = runtime.get_cashflow_summary_from_sheet(force_refresh=force_refresh)
        expense_summary = runtime.get_cashflow_expense_records(force_refresh=force_refresh)
        capital = runtime.get_phone_capital_outflow(force_refresh=force_refresh)
        weekly_allowance = summary.get('weekly_allowance') or runtime.get_weekly_allowance_from_sheet(force_refresh=force_refresh)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    transactions = runtime.get_cashflow_sheet_records(force_refresh=force_refresh).get('items', [])

    result = {
        'summary': summary,
        'weekly_allowance': weekly_allowance,
        'expenses': expense_summary.get('items', []),
        'expense_source': expense_summary.get('source', 'database'),
        'expense_sheet_title': expense_summary.get('sheet_title', 'CASH FLOW'),
        'transactions': transactions,
        'capital': capital,
    }
    runtime.logger.info(
        'query_timing kind=dashboard_read_cashflow duration_ms=%.2f force_refresh=%s expense_count=%s transaction_count=%s read_mode=%s',
        round((time.perf_counter() - started) * 1000, 2),
        bool(force_refresh),
        len(result.get('expenses') or []),
        len(transactions),
        'postgres_first' if runtime.postgres_ready else 'sheet_only',
    )
    return result


@router.get('/weekly-allowance', dependencies=[Depends(require_admin)])
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


@router.post('/allowance/undo-last', dependencies=[Depends(require_admin)])
def undo_last_weekly_allowance(runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result = runtime.undo_last_weekly_allowance_withdrawal()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result['error'])
    return result


@router.post('/cashflow/rebuild-week', dependencies=[Depends(require_admin)])
def rebuild_cashflow_week(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result = runtime.rebuild_cashflow_sheet_for_current_week(force_refresh=force_refresh)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return result


@router.post('/cashflow/rebuild', dependencies=[Depends(require_admin)])
def rebuild_cashflow(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result = runtime.rebuild_cashflow_sheet(force_refresh=force_refresh, current_week_only=False)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return result
