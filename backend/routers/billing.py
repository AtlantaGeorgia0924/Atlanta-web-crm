from datetime import date
import os
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from backend.dependencies import get_runtime, require_admin
from services.billing_service import (
    build_debtor_send_summary,
    build_payment_plan,
    build_services_today_rows,
    build_unpaid_today_customers,
    compute_debtors,
    compute_sales_snapshot,
    generate_bill_text,
    get_whatsapp_send_entry,
    get_customer_outstanding_items_from_records,
    get_customer_outstanding_items_from_values,
    load_whatsapp_send_history,
    mark_whatsapp_bill_sent,
    parse_sheet_date,
    save_whatsapp_send_history,
)

router = APIRouter(
    prefix='/api/billing',
    tags=['billing'],
    dependencies=[Depends(require_admin)],
)


class OutstandingItemsValuesRequest(BaseModel):
    name_input: str
    values: list[list[Any]]


class OutstandingItemsRecordsRequest(BaseModel):
    name_input: str
    records: list[dict[str, Any]]


class GenerateBillRequest(BaseModel):
    name_input: str
    records: list[dict[str, Any]]
    payment_details: str = ''


class ComputeDebtorsRequest(BaseModel):
    records: list[dict[str, Any]]


class PaymentPlanRequest(BaseModel):
    name_input: str
    payment_amount: int
    values: list[list[Any]]
    manual_service_row_idx: int | None = None


class SalesSnapshotRequest(BaseModel):
    records: list[dict[str, Any]]
    today: date | None = None


class LivePaymentPlanRequest(BaseModel):
    name_input: str
    payment_amount: int
    manual_service_row_idx: int | None = None
    force_refresh: bool = False


class ApplyPaymentRequest(BaseModel):
    name_input: str
    payment_amount: int
    manual_service_row_idx: int | None = None
    force_refresh: bool = False


class PaymentHistoryActionRequest(BaseModel):
    force_refresh: bool = False


class MarkWhatsappSentRequest(BaseModel):
    name_input: str
    source: str = 'single'


class MarkWhatsappSentManyRequest(BaseModel):
    names: list[str]
    source: str = 'bulk'


def _whatsapp_history_file(runtime):
    return os.path.join(getattr(runtime, 'base_dir', os.getcwd()), 'whatsapp_bill_history.json')


@router.post('/outstanding-items/from-values')
def outstanding_items_from_values(payload: OutstandingItemsValuesRequest):
    outstanding_items, total_outstanding, columns = get_customer_outstanding_items_from_values(
        payload.name_input,
        payload.values,
    )
    return {
        'outstanding_items': outstanding_items,
        'total_outstanding': total_outstanding,
        'columns': columns,
    }


@router.post('/outstanding-items/from-records')
def outstanding_items_from_records(payload: OutstandingItemsRecordsRequest):
    outstanding_items, total_outstanding = get_customer_outstanding_items_from_records(
        payload.name_input,
        payload.records,
    )
    return {
        'outstanding_items': outstanding_items,
        'total_outstanding': total_outstanding,
    }


@router.post('/debtors/compute')
def compute_debtors_endpoint(payload: ComputeDebtorsRequest):
    return compute_debtors(payload.records)


@router.post('/payment-plan')
def build_payment_plan_endpoint(payload: PaymentPlanRequest):
    return build_payment_plan(
        payload.name_input,
        payload.payment_amount,
        payload.values,
        manual_service_row_idx=payload.manual_service_row_idx,
    )


@router.post('/bill/generate')
def generate_bill_endpoint(payload: GenerateBillRequest):
    return {
        'bill_text': generate_bill_text(
            payload.name_input,
            payload.records,
            payload.payment_details,
        )
    }


@router.post('/sales-snapshot')
def compute_sales_snapshot_endpoint(payload: SalesSnapshotRequest):
    return compute_sales_snapshot(payload.records, today=payload.today)


@router.get('/debtors/live')
def compute_live_debtors(force_refresh: bool = False, runtime=Depends(get_runtime)):
    return compute_debtors(runtime.get_main_records(force_refresh=force_refresh))


@router.get('/sales-snapshot/live')
def compute_live_sales_snapshot(force_refresh: bool = False, runtime=Depends(get_runtime)):
    return compute_sales_snapshot(runtime.get_main_records(force_refresh=force_refresh))


@router.get('/outstanding-items/live/{name_input}')
def outstanding_items_live(name_input: str, force_refresh: bool = False, runtime=Depends(get_runtime)):
    outstanding_items, total_outstanding = get_customer_outstanding_items_from_records(
        name_input,
        runtime.get_main_records(force_refresh=force_refresh),
    )
    return {
        'outstanding_items': outstanding_items,
        'total_outstanding': total_outstanding,
    }


@router.get('/bill/live/{name_input}')
def generate_live_bill(name_input: str, force_refresh: bool = False, runtime=Depends(get_runtime)):
    return {
        'bill_text': generate_bill_text(
            name_input,
            runtime.get_main_records(force_refresh=force_refresh),
            runtime.config.get('payment_details', ''),
        )
    }


@router.post('/payment-plan/live')
def build_live_payment_plan(payload: LivePaymentPlanRequest, runtime=Depends(get_runtime)):
    values = runtime.get_main_values(force_refresh=payload.force_refresh)
    result = build_payment_plan(
        payload.name_input,
        payload.payment_amount,
        values,
        manual_service_row_idx=payload.manual_service_row_idx,
    )
    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/payments/apply')
def apply_payment_endpoint(payload: ApplyPaymentRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.apply_payment(
            payload.name_input,
            payload.payment_amount,
            manual_service_row_idx=payload.manual_service_row_idx,
            force_refresh=payload.force_refresh,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/payments/undo')
def undo_payment_endpoint(payload: PaymentHistoryActionRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.undo_last_payment()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/payments/redo')
def redo_payment_endpoint(payload: PaymentHistoryActionRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.redo_last_payment()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.get('/whatsapp/history/live')
def whatsapp_history_live(force_refresh: bool = False, runtime=Depends(get_runtime)):
    records = runtime.get_main_records(force_refresh=force_refresh)
    history = load_whatsapp_send_history(_whatsapp_history_file(runtime))
    return {
        'by_name': build_debtor_send_summary(records, history),
    }


@router.post('/whatsapp/history/mark-sent')
def mark_whatsapp_sent(payload: MarkWhatsappSentRequest, runtime=Depends(get_runtime)):
    file_path = _whatsapp_history_file(runtime)
    history = load_whatsapp_send_history(file_path)
    updated = mark_whatsapp_bill_sent(history, [payload.name_input], source=payload.source)
    save_whatsapp_send_history(file_path, history)
    name_key = str(payload.name_input or '').strip().upper()
    return {
        'entry': get_whatsapp_send_entry(history, name_key),
        'updated': updated.get(name_key, {}),
    }


@router.post('/whatsapp/history/mark-many')
def mark_whatsapp_sent_many(payload: MarkWhatsappSentManyRequest, runtime=Depends(get_runtime)):
    file_path = _whatsapp_history_file(runtime)
    history = load_whatsapp_send_history(file_path)
    updated = mark_whatsapp_bill_sent(history, payload.names, source=payload.source)
    save_whatsapp_send_history(file_path, history)

    return {
        'updated_count': len(updated),
        'by_name': {
            name: get_whatsapp_send_entry(history, name)
            for name in updated
        },
    }


@router.get('/unpaid-today/live')
def unpaid_today_live(force_refresh: bool = False, runtime=Depends(get_runtime)):
    records = runtime.get_main_records(force_refresh=force_refresh)
    registry = runtime.get_client_registry(force_reload=False)
    history = load_whatsapp_send_history(_whatsapp_history_file(runtime))

    customers = []
    with_phone_count = 0
    for entry in build_unpaid_today_customers(records):
        name = entry['name']
        phone = str(registry.get(name) or '').strip()
        has_phone = bool(phone)
        if has_phone:
            with_phone_count += 1
        customers.append({
            **entry,
            'has_phone': has_phone,
            'phone': phone,
            'send_stats': get_whatsapp_send_entry(history, name),
        })

    return {
        'customers': customers,
        'count': len(customers),
        'with_phone_count': with_phone_count,
    }


@router.get('/unpaid-today/live-bills')
def unpaid_today_live_bills(force_refresh: bool = False, runtime=Depends(get_runtime)):
    records = runtime.get_main_records(force_refresh=force_refresh)
    registry = runtime.get_client_registry(force_reload=False)
    history = load_whatsapp_send_history(_whatsapp_history_file(runtime))
    payment_details = runtime.config.get('payment_details', '')

    customers = []
    for entry in build_unpaid_today_customers(records):
        name = entry['name']
        phone = str(registry.get(name) or '').strip()
        has_phone = bool(phone)
        customers.append({
            **entry,
            'has_phone': has_phone,
            'phone': phone,
            'bill_text': generate_bill_text(name, records, payment_details),
            'send_stats': get_whatsapp_send_entry(history, name),
        })

    return {
        'customers': customers,
        'count': len(customers),
        'with_phone_count': sum(1 for item in customers if item['has_phone']),
    }

@router.get('/services-today/live')
def services_today_live(force_refresh: bool = False, target_date: str = '', runtime=Depends(get_runtime)):
    records = runtime.get_main_records(force_refresh=force_refresh)
    normalized_target = str(target_date or '').strip()
    parsed_target = parse_sheet_date(normalized_target) if normalized_target else None
    if normalized_target and parsed_target is None:
        raise HTTPException(status_code=400, detail='Invalid target_date. Use YYYY-MM-DD or MM/DD/YYYY.')

    services = build_services_today_rows(records, today=parsed_target)
    return {
        'services': services,
        'count': len(services),
        'target_date': str(parsed_target.isoformat() if parsed_target else ''),
    }
