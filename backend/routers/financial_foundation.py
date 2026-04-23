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


class AppConfigUpsertRequest(BaseModel):
    value: Any


@router.post('/expenses')
def create_expense(payload: CreateExpenseRequest, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        item = runtime.financial_data_service.create_expense(
            amount=payload.amount,
            category=payload.category,
            description=payload.description,
            date=payload.date,
            created_by=str((current_user or {}).get('username') or ''),
        )
        sheet_item = None
        try:
            sheet_item = runtime.append_cashflow_expense_record(
                amount=payload.amount,
                category=payload.category,
                description=payload.description,
                date_text=payload.date,
                created_by=str((current_user or {}).get('username') or ''),
            )
        except Exception as exc:
            runtime.logger.warning('Cashflow expense sheet append failed: %s', exc)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return {'expense': item, 'sheet_expense': sheet_item}


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
        expense_summary = runtime.get_cashflow_expense_records(force_refresh=force_refresh)
        summary = runtime.financial_data_service.get_cashflow_summary(
            actor_role=str((current_user or {}).get('role') or ''),
            expense_total_override=expense_summary['total'] if expense_summary.get('source') == 'sheet' else None,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {'summary': summary}


@router.get('/cashflow-dashboard', dependencies=[Depends(require_admin)])
def get_cashflow_dashboard(force_refresh: bool = False, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    try:
        expense_summary = runtime.get_cashflow_expense_records(force_refresh=force_refresh)
        summary = runtime.financial_data_service.get_cashflow_summary(
            actor_role=str((current_user or {}).get('role') or ''),
            expense_total_override=expense_summary['total'] if expense_summary.get('source') == 'sheet' else None,
        )
        weekly_allowance = runtime.financial_data_service.get_weekly_allowance_summary(
            actor_role=str((current_user or {}).get('role') or ''),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {
        'summary': summary,
        'weekly_allowance': weekly_allowance,
        'expenses': expense_summary.get('items', []),
        'expense_source': expense_summary.get('source', 'database'),
    }


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
