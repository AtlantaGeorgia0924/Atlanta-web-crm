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


def _summary_row_has_activity(row):
    if not isinstance(row, dict):
        return False
    for key in ('profit_seen', 'expenses_total', 'net_profit', 'allowance_amount', 'profit_left'):
        try:
            if abs(float(row.get(key) or 0)) > 0:
                return True
        except Exception:
            continue
    return False


def _refresh_live_operational_data(runtime):
    if not bool(getattr(runtime, 'postgres_ready', False)):
        return {'attempted': False, 'ok': False, 'error': 'postgres_not_ready'}
    try:
        runtime.pull_once()
        return {'attempted': True, 'ok': True, 'error': ''}
    except Exception as exc:
        runtime.logger.warning('Live pull failed before cashflow rebuild: %s', exc)
        return {'attempted': True, 'ok': False, 'error': str(exc)}


def _rebuild_cashflow_from_live(runtime, force_refresh=False):
    pull_meta = {'attempted': False, 'ok': False, 'error': ''}
    if force_refresh:
        pull_meta = _refresh_live_operational_data(runtime)

    rebuilt = runtime.financial_data_service.rebuild_cashflow_summary_rows()
    week_row = (rebuilt or {}).get('week') or {}
    month_row = (rebuilt or {}).get('month') or {}
    if not _summary_row_has_activity(week_row) and not _summary_row_has_activity(month_row):
        second_pull_meta = _refresh_live_operational_data(runtime)
        if second_pull_meta.get('attempted'):
            pull_meta = second_pull_meta
        rebuilt = runtime.financial_data_service.rebuild_cashflow_summary_rows()

    verification = runtime.financial_data_service.build_cashflow_verification_report()
    return rebuilt, verification, pull_meta


def _build_sheet_cashflow_fallback(runtime, force_refresh: bool = False):
    def _to_number(value):
        try:
            return float(value or 0)
        except Exception:
            return 0.0

    try:
        summary = runtime.get_cashflow_summary_from_sheet(force_refresh=force_refresh) or {}
    except Exception as exc:
        runtime.logger.exception('Sheet fallback summary failed: %s', exc)
        summary = {}

    try:
        expenses_payload = runtime.get_cashflow_expense_records(force_refresh=force_refresh) or {}
    except Exception as exc:
        runtime.logger.exception('Sheet fallback expenses failed: %s', exc)
        expenses_payload = {}

    try:
        capital = runtime.get_phone_capital_outflow(force_refresh=force_refresh) or {'month_total': 0, 'week_total': 0, 'entries': []}
    except Exception as exc:
        runtime.logger.exception('Sheet fallback capital failed: %s', exc)
        capital = {'month_total': 0, 'week_total': 0, 'entries': []}

    weekly_allowance = summary.get('weekly_allowance') or {}
    expense_items = expenses_payload.get('items') or []

    week_allowance_amount = _to_number(
        weekly_allowance.get('suggested_allowance')
        or summary.get('next_week_allowance')
        or 0
    )
    month_allowance_amount = _to_number(summary.get('monthly_allowance_paid') or 0)

    week_row = {
        'period_type': 'week',
        'period_start': summary.get('current_week_start') or '',
        'period_end': summary.get('current_week_end') or '',
        'profit_seen': _to_number(summary.get('current_week_gross_profit') or summary.get('weekly_realized_profit') or 0),
        'expenses_total': _to_number(summary.get('current_week_expenses') or 0),
        'net_profit': _to_number(summary.get('current_week_net_profit') or 0),
        'allowance_amount': week_allowance_amount,
        'profit_left': _to_number(summary.get('current_week_net_profit') or 0) - week_allowance_amount,
        'generated_at': '',
    }
    month_row = {
        'period_type': 'month',
        'period_start': '',
        'period_end': '',
        'profit_seen': _to_number(summary.get('monthly_gross_profit') or summary.get('total_cash_in') or 0),
        'expenses_total': _to_number(summary.get('total_expenses') or 0),
        'net_profit': _to_number(summary.get('monthly_net_profit') or summary.get('net_profit') or 0),
        'allowance_amount': month_allowance_amount,
        'profit_left': _to_number(summary.get('monthly_remaining_profit') or summary.get('month_remainder_profit_after_paid_allowance') or 0),
        'generated_at': '',
    }

    summary_payload = dict(summary)
    summary_payload['week'] = week_row
    summary_payload['month'] = month_row

    normalized_transactions = [
        {
            'date': item.get('payment_date') or item.get('date') or '',
            'payment_date': item.get('payment_date') or item.get('date') or '',
            'category': item.get('category') or 'EXPENSE',
            'description': item.get('description') or '',
            'amount': item.get('amount', 0),
            'source': item.get('source') or 'expense',
        }
        for item in expense_items
    ]

    return {
        'summary': summary_payload,
        'rows': [week_row, month_row],
        'weekly_allowance': weekly_allowance,
        'monthly_allowance': month_row,
        'expenses': expense_items,
        'expense_source': expenses_payload.get('source') or summary.get('expense_source') or 'sheet',
        'expense_sheet_title': expenses_payload.get('sheet_title') or summary.get('expense_sheet_title') or 'CASH FLOW',
        'withdrawals': [],
        'transactions': normalized_transactions,
        'capital': {
            'month_total': _to_number(capital.get('month_total') or 0),
            'week_total': _to_number(capital.get('week_total') or 0),
            'entries': capital.get('entries') or [],
        },
    }


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


@router.post('/expenses/{expense_id}/reverse', dependencies=[Depends(require_admin)])
def reverse_expense(expense_id: str, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    normalized_id = str(expense_id or '').strip()
    if not normalized_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Expense id is required.')

    try:
        current = runtime.financial_data_service.get_expense(normalized_id)
        if not current:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Expense not found.')

        if bool(current.get('is_reversed')):
            return {'expense': current, 'already_reversed': True}

        reversed_expense = runtime.financial_data_service.reverse_manual_expense(normalized_id)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except Exception as exc:
        runtime.logger.exception('Unexpected reverse expense error: %s', exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail='Expense reversal is temporarily unavailable. Please try again shortly.',
        ) from exc

    if not reversed_expense:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Expense not found.')

    return {'expense': reversed_expense, 'already_reversed': False}


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
        rebuilt_rows, verification, pull_meta = _rebuild_cashflow_from_live(runtime, force_refresh=force_refresh)
        summary_rows = runtime.financial_data_service.get_current_cashflow_summary_rows()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        runtime.logger.warning('Cashflow summary DB path unavailable; falling back to sheet summary: %s', exc)
        fallback_payload = _build_sheet_cashflow_fallback(runtime, force_refresh=force_refresh)
        return {
            'summary': fallback_payload.get('summary') or {},
            'rows': fallback_payload.get('rows') or [],
            'weekly_allowance': fallback_payload.get('weekly_allowance') or {},
        }
    except Exception as exc:
        runtime.logger.exception('Unexpected cashflow summary error; using sheet fallback: %s', exc)
        fallback_payload = _build_sheet_cashflow_fallback(runtime, force_refresh=force_refresh)
        return {
            'summary': fallback_payload.get('summary') or {},
            'rows': fallback_payload.get('rows') or [],
            'weekly_allowance': fallback_payload.get('weekly_allowance') or {},
        }

    summary = {str(row.get('period_type') or '').lower(): row for row in summary_rows}
    return {
        'summary': summary,
        'rows': summary_rows,
        'weekly_allowance': summary.get('week') or {},
        'rebuild': rebuilt_rows,
        'verification_report': verification,
        'live_pull': pull_meta,
    }


@router.get('/cashflow-dashboard', dependencies=[Depends(require_admin)])
def get_cashflow_dashboard(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    started = time.perf_counter()
    try:
        rebuilt_rows, verification, pull_meta = _rebuild_cashflow_from_live(runtime, force_refresh=force_refresh)
        summary_rows = runtime.financial_data_service.get_current_cashflow_summary_rows()
        expense_items = runtime.financial_data_service.list_expenses(limit=500, offset=0)
        withdrawal_items = runtime.financial_data_service.list_allowance_withdrawals(limit=500, offset=0)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        runtime.logger.warning('Cashflow dashboard DB path unavailable; falling back to sheet summary: %s', exc)
        fallback_payload = _build_sheet_cashflow_fallback(runtime, force_refresh=force_refresh)
        runtime.logger.info(
            'query_timing kind=dashboard_read_cashflow_fallback duration_ms=%.2f force_refresh=%s expense_count=%s transaction_count=%s read_mode=%s',
            round((time.perf_counter() - started) * 1000, 2),
            bool(force_refresh),
            len(fallback_payload.get('expenses') or []),
            len(fallback_payload.get('transactions') or []),
            'sheet_fallback',
        )
        return fallback_payload
    except Exception as exc:
        runtime.logger.exception('Unexpected cashflow dashboard error; using sheet fallback: %s', exc)
        fallback_payload = _build_sheet_cashflow_fallback(runtime, force_refresh=force_refresh)
        runtime.logger.info(
            'query_timing kind=dashboard_read_cashflow_fallback duration_ms=%.2f force_refresh=%s expense_count=%s transaction_count=%s read_mode=%s',
            round((time.perf_counter() - started) * 1000, 2),
            bool(force_refresh),
            len(fallback_payload.get('expenses') or []),
            len(fallback_payload.get('transactions') or []),
            'sheet_fallback',
        )
        return fallback_payload

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
        'rebuild': rebuilt_rows,
        'verification_report': verification,
        'live_pull': pull_meta,
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
        result, verification, pull_meta = _rebuild_cashflow_from_live(runtime, force_refresh=True)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {
        'rebuild': result,
        'verification_report': verification,
        'live_pull': pull_meta,
    }


@router.post('/cashflow/rebuild', dependencies=[Depends(require_admin)])
def rebuild_cashflow(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        result, verification, pull_meta = _rebuild_cashflow_from_live(runtime, force_refresh=True)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {
        'rebuild': result,
        'verification_report': verification,
        'live_pull': pull_meta,
    }
