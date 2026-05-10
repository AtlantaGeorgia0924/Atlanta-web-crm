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


class AllowanceWithdrawalRequest(BaseModel):
    week_start: str = ''
    allowance_amount: float = Field(ge=0)
    withdrawn_by: str = ''


@router.post('/expenses')
def create_expense(payload: CreateExpenseRequest, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    category = str(payload.category or '').strip()
    allowance_impact = str(payload.allowance_impact or 'personal_allowance').strip().lower()
    if allowance_impact == 'business_only' and category:
        category = f'BUSINESS ONLY: {category}'

    try:
        expense = runtime.financial_data_service.create_expense(
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

    return {'expense': expense}


@router.get('/expenses')
def list_expenses(limit: int = 200, offset: int = 0, runtime=Depends(get_runtime)):
    try:
        items = runtime.financial_data_service.list_expenses(limit=limit, offset=offset)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'expenses': items, 'count': len(items), 'source': 'database'}


@router.get('/allowance/withdrawals', dependencies=[Depends(require_admin)])
def list_allowance_withdrawals(limit: int = 200, offset: int = 0, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        items = runtime.financial_data_service.list_allowance_withdrawals(limit=limit, offset=offset)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'withdrawals': items, 'count': len(items)}


@router.post('/allowance/withdraw', dependencies=[Depends(require_admin)])
def create_allowance_withdrawal(
    payload: AllowanceWithdrawalRequest,
    runtime=Depends(get_runtime),
    current_user=Depends(get_current_user),
):
    try:
        result = runtime.financial_data_service.create_allowance_withdrawal(
            week_start=payload.week_start,
            allowance_amount=payload.allowance_amount,
            withdrawn_by=payload.withdrawn_by or str((current_user or {}).get('username') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    if isinstance(result, dict) and result.get('error'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result['error'])
    return {'withdrawal': result}


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
        if force_refresh:
            runtime.financial_data_service.rebuild_cashflow_summary_rows()
        summary_rows = runtime.financial_data_service.get_current_cashflow_summary_rows()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    summary = {str(row.get('period_type') or '').lower(): row for row in summary_rows}
    return {
        'summary': summary,
        'rows': summary_rows,
        'weekly_allowance': summary.get('week') or {},
    }


@router.get('/cashflow-dashboard', dependencies=[Depends(require_admin)])
def get_cashflow_dashboard(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    started = time.perf_counter()
    try:
        if force_refresh:
            runtime.financial_data_service.rebuild_cashflow_summary_rows()
        summary_rows = runtime.financial_data_service.get_current_cashflow_summary_rows()
        expense_items = runtime.financial_data_service.list_expenses(limit=500, offset=0)
        withdrawal_items = runtime.financial_data_service.list_allowance_withdrawals(limit=500, offset=0)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    summary = {str(row.get('period_type') or '').lower(): row for row in summary_rows}
    normalized_transactions = [
        {
            'date': item.get('expense_date'),
            'payment_date': item.get('expense_date'),
            'category': str(item.get('description') or '').split(' - ')[0] if item.get('description') else 'EXPENSE',
            'description': item.get('description') or '',
            'amount': item.get('amount', 0),
            'source': 'expense',
        }
        for item in expense_items
    ] + [
        {
            'date': item.get('withdrawn_date') or item.get('week_start'),
            'payment_date': item.get('withdrawn_date') or item.get('week_start'),
            'category': 'WEEKLY ALLOWANCE',
            'description': f"Weekly allowance for {item.get('week_start')}",
            'amount': item.get('allowance_amount', 0),
            'source': 'expense',
        }
        for item in withdrawal_items
    ]
    result = {
        'summary': summary,
        'weekly_allowance': summary.get('week') or {},
        'monthly_allowance': summary.get('month') or {},
        'expenses': expense_items,
        'expense_source': 'database',
        'expense_sheet_title': 'manual_expenses',
        'withdrawals': withdrawal_items,
        'transactions': normalized_transactions,
        'capital': {'month_total': 0, 'week_total': 0, 'entries': []},
    }
    runtime.logger.info(
        'query_timing kind=dashboard_read_cashflow duration_ms=%.2f force_refresh=%s expense_count=%s transaction_count=%s read_mode=%s',
        round((time.perf_counter() - started) * 1000, 2),
        bool(force_refresh),
        len(result.get('expenses') or []),
        0,
        'postgres_first' if runtime.postgres_ready else 'sheet_only',
    )
    return result


@router.get('/weekly-allowance', dependencies=[Depends(require_admin)])
def get_weekly_allowance(runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        summary_rows = runtime.financial_data_service.get_current_cashflow_summary_rows()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    summary = {str(row.get('period_type') or '').lower(): row for row in summary_rows}
    return summary.get('week') or {}


@router.post('/allowance/undo-last', dependencies=[Depends(require_admin)])
def undo_last_weekly_allowance(runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result = runtime.financial_data_service.undo_last_allowance_withdrawal()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result['error'])
    return {
        'removed_amount': result.get('allowance_amount', 0),
        'withdrawal': result,
    }


@router.post('/cashflow/rebuild-week', dependencies=[Depends(require_admin)])
def rebuild_cashflow_week(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result = runtime.financial_data_service.rebuild_cashflow_summary_rows()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return result


@router.post('/cashflow/rebuild', dependencies=[Depends(require_admin)])
def rebuild_cashflow(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result = runtime.financial_data_service.rebuild_cashflow_summary_rows()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return result
