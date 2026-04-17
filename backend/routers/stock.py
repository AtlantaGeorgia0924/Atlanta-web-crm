from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.dependencies import get_current_user, get_runtime, require_staff
from services.stock_service import (
    build_sale_status_update_values,
    build_stock_form_defaults,
    build_stock_row_values,
    build_stock_view,
    classify_available_series,
    compute_stock_qty_status,
    detect_stock_headers,
    map_sale_status,
    validate_stock_row,
)

router = APIRouter(
    prefix='/api/stock',
    tags=['stock'],
    dependencies=[Depends(get_current_user)],
)


def _normalized_header_name(value: str):
    return ' '.join(str(value or '').strip().upper().replace('_', ' ').split())


def _is_cost_price_header(value: str):
    return _normalized_header_name(value) == 'COST PRICE'


def _is_staff_user(current_user: dict):
    return str((current_user or {}).get('role') or '').strip().lower() == 'staff'


def _cost_price_index(headers=None, headers_upper=None):
    headers = list(headers or [])
    headers_upper = list(headers_upper or [])
    if not headers_upper and headers:
        headers_upper = [_normalized_header_name(header) for header in headers]

    for idx, header in enumerate(headers_upper):
        if _is_cost_price_header(header):
            return idx
    if headers:
        for idx, header in enumerate(headers):
            if _is_cost_price_header(header):
                return idx
    return None


def _strip_cost_price_from_values_by_header(values_by_header: dict[str, Any]):
    filtered = {}
    for key, value in dict(values_by_header or {}).items():
        if _is_cost_price_header(key):
            continue
        filtered[key] = value
    return filtered


def _remove_cost_price_index_from_stock_view(stock_view: dict, cost_idx: int | None):
    if not isinstance(stock_view, dict) or cost_idx is None:
        return stock_view

    sanitized = dict(stock_view)
    rows = []
    for row in (sanitized.get('all_rows_cache') or []):
        next_row = dict(row)
        padded = list(next_row.get('padded') or [])
        if cost_idx < len(padded):
            padded.pop(cost_idx)
        next_row['padded'] = padded
        rows.append(next_row)
    sanitized['all_rows_cache'] = rows

    groups = []
    for group in (sanitized.get('available_series_items') or []):
        next_group = dict(group)
        next_rows = []
        for item in (next_group.get('rows') or []):
            next_item = dict(item)
            values = list(next_item.get('values') or [])
            if cost_idx < len(values):
                values.pop(cost_idx)
            next_item['values'] = values
            next_rows.append(next_item)
        next_group['rows'] = next_rows
        groups.append(next_group)
    sanitized['available_series_items'] = groups
    return sanitized


def _sanitize_stock_view_for_staff(stock_view: dict):
    if not isinstance(stock_view, dict):
        return stock_view

    headers = list(stock_view.get('headers') or [])
    headers_upper = list(stock_view.get('headers_upper') or [])
    cost_idx = _cost_price_index(headers=headers, headers_upper=headers_upper)
    sanitized = _remove_cost_price_index_from_stock_view(stock_view, cost_idx)

    if cost_idx is None:
        return sanitized

    if headers and cost_idx < len(headers):
        next_headers = list(headers)
        next_headers.pop(cost_idx)
        sanitized['headers'] = next_headers

    if headers_upper and cost_idx < len(headers_upper):
        next_headers_upper = list(headers_upper)
        next_headers_upper.pop(cost_idx)
        sanitized['headers_upper'] = next_headers_upper

    return sanitized


def _sanitize_stock_form_for_staff(stock_form: dict):
    if not isinstance(stock_form, dict):
        return stock_form

    sanitized = dict(stock_form)
    visible_headers = list(sanitized.get('visible_headers') or [])
    defaults = dict(sanitized.get('defaults') or {})

    # Keep form metadata aligned with admin and only hide COST PRICE from visible
    # controls/defaults for staff.
    sanitized['visible_headers'] = [header for header in visible_headers if not _is_cost_price_header(header)]
    sanitized['defaults'] = {
        key: value for key, value in defaults.items()
        if not _is_cost_price_header(key)
    }
    return sanitized


def _serialize_stock_view(stock_view):
    breakdown_items = []
    for key, count in (stock_view.get('available_breakdown') or {}).items():
        if isinstance(key, (list, tuple)) and len(key) >= 2:
            brand, series = key[0], key[1]
        else:
            brand, series = 'Other', str(key)
        breakdown_items.append({
            'brand': brand,
            'series': series,
            'count': count,
        })

    series_groups = []
    for key, rows in (stock_view.get('available_series_items') or {}).items():
        if isinstance(key, (list, tuple)) and len(key) >= 2:
            brand, series = key[0], key[1]
        else:
            brand, series = 'Other', str(key)
        series_groups.append({
            'brand': brand,
            'series': series,
            'rows': [
                {'row_num': row_num, 'values': values}
                for row_num, values in rows
            ],
        })

    stock_view['available_breakdown'] = breakdown_items
    stock_view['available_series_items'] = series_groups
    return stock_view


class DetectStockHeadersRequest(BaseModel):
    values: list[list[Any]]


class StockFormDefaultsRequest(BaseModel):
    values: list[list[Any]]
    header_row_idx: int
    headers_upper: list[str]


class StockRowValuesRequest(BaseModel):
    headers: list[str]
    values_by_header: dict[str, Any]


class ValidateStockRowRequest(BaseModel):
    row_values: list[Any]
    headers_upper: list[str]


class ComputeStockQtyStatusRequest(BaseModel):
    current_qty: int
    delta: int


class MapSaleStatusRequest(BaseModel):
    status_choice: str


class SaleStatusUpdatesRequest(BaseModel):
    status_key: str
    qty_col: int | None = None
    status_col: int | None = None
    sold_date_col: int | None = None
    sold_date_value: str = ''


class StockViewRequest(BaseModel):
    values: list[list[Any]]
    headers: list[str]
    headers_upper: list[str]
    header_row_idx: int
    color_status_map: dict[int, str] = Field(default_factory=dict)
    filter_text: str = ''
    filter_mode: str = 'all'


class ClassifySeriesRequest(BaseModel):
    description_text: Any = ''


class StockLiveAddRequest(BaseModel):
    values_by_header: dict[str, Any] = Field(default_factory=dict)
    force_refresh: bool = False


class StockLiveUpdateRowRequest(BaseModel):
    row_num: int
    values_by_header: dict[str, Any] = Field(default_factory=dict)
    force_refresh: bool = False


class StockLiveServiceAddRequest(BaseModel):
    values_by_header: dict[str, Any] = Field(default_factory=dict)
    force_refresh: bool = False


class StockLiveReturnRequest(BaseModel):
    row_num: int
    force_refresh: bool = False


class StockLivePendingPaymentUpdateRequest(BaseModel):
    row_num: int
    payment_status: str
    amount_paid: Any = None
    force_refresh: bool = False


class StockLiveServicePendingRequest(BaseModel):
    force_refresh: bool = False


class StockCartItem(BaseModel):
    stock_row_num: int
    buyer_name: str = ''
    buyer_phone: str = ''
    sale_price: Any = None
    stock_status: str = 'sold'
    inventory_status: str = 'UNPAID'
    availability_value: str = ''


class StockCartCheckoutRequest(BaseModel):
    items: list[StockCartItem] = Field(default_factory=list)
    force_refresh: bool = False


@router.post('/headers/detect')
def detect_stock_headers_endpoint(payload: DetectStockHeadersRequest):
    header_row_idx, headers, headers_upper = detect_stock_headers(payload.values)
    return {
        'header_row_idx': header_row_idx,
        'headers': headers,
        'headers_upper': headers_upper,
    }


@router.post('/form/defaults')
def build_stock_form_defaults_endpoint(payload: StockFormDefaultsRequest):
    return {
        'defaults': build_stock_form_defaults(
            payload.values,
            payload.header_row_idx,
            payload.headers_upper,
        )
    }


@router.post('/row/build')
def build_stock_row_values_endpoint(payload: StockRowValuesRequest, current_user=Depends(get_current_user)):
    values_by_header = payload.values_by_header
    if _is_staff_user(current_user):
        values_by_header = _strip_cost_price_from_values_by_header(values_by_header)
    row_values, non_empty_count = build_stock_row_values(payload.headers, values_by_header)
    return {
        'row_values': row_values,
        'non_empty_count': non_empty_count,
    }


@router.post('/row/validate')
def validate_stock_row_endpoint(payload: ValidateStockRowRequest):
    return {
        'error': validate_stock_row(payload.row_values, payload.headers_upper)
    }


@router.post('/quantity-status')
def compute_stock_qty_status_endpoint(payload: ComputeStockQtyStatusRequest):
    new_qty, status = compute_stock_qty_status(payload.current_qty, payload.delta)
    return {
        'new_qty': new_qty,
        'status': status,
    }


@router.post('/sale-status/map')
def map_sale_status_endpoint(payload: MapSaleStatusRequest):
    status_key, fill_color = map_sale_status(payload.status_choice)
    return {
        'status_key': status_key,
        'fill_color': fill_color,
    }


@router.post('/sale-status/updates')
def build_sale_status_updates_endpoint(payload: SaleStatusUpdatesRequest):
    return {
        'updates': build_sale_status_update_values(
            payload.status_key,
            qty_col=payload.qty_col,
            status_col=payload.status_col,
            sold_date_col=payload.sold_date_col,
            sold_date_value=payload.sold_date_value,
        )
    }


@router.post('/view')
def build_stock_view_endpoint(payload: StockViewRequest, current_user=Depends(get_current_user)):
    stock_view = _serialize_stock_view(build_stock_view(
        payload.values,
        payload.headers,
        payload.headers_upper,
        payload.header_row_idx,
        color_status_map=payload.color_status_map,
        filter_text=payload.filter_text,
        filter_mode=payload.filter_mode,
    ))
    if _is_staff_user(current_user):
        stock_view = _remove_cost_price_index_from_stock_view(
            stock_view,
            _cost_price_index(headers=payload.headers, headers_upper=payload.headers_upper),
        )
    return stock_view


@router.post('/series/classify')
def classify_available_series_endpoint(payload: ClassifySeriesRequest):
    return {
        'series': classify_available_series(payload.description_text)
    }


@router.get('/view/live')
def build_live_stock_view(
    filter_text: str = '',
    filter_mode: str = 'all',
    force_refresh: bool = False,
    runtime=Depends(get_runtime),
    current_user=Depends(get_current_user),
):
    try:
        stock_view = _serialize_stock_view(runtime.get_stock_view_payload(
            filter_text=filter_text,
            filter_mode=filter_mode,
            force_refresh=force_refresh,
        ))
        if _is_staff_user(current_user):
            stock_view = _sanitize_stock_view_for_staff(stock_view)
        return stock_view
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f'Stock view temporarily unavailable: {exc}') from exc


@router.get('/form/live')
def get_live_stock_form(
    force_refresh: bool = False,
    runtime=Depends(get_runtime),
    current_user=Depends(get_current_user),
):
    try:
        stock_form = runtime.get_stock_form_payload(force_refresh=force_refresh)
        if _is_staff_user(current_user):
            stock_form = _sanitize_stock_form_for_staff(stock_form)
        return stock_form
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f'Stock form temporarily unavailable: {exc}') from exc


@router.post('/live/add')
def add_live_stock_record(payload: StockLiveAddRequest, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    values_by_header = payload.values_by_header
    if _is_staff_user(current_user):
        values_by_header = _strip_cost_price_from_values_by_header(values_by_header)
    try:
        result = runtime.add_stock_record(values_by_header, force_refresh=payload.force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/update-row')
def update_live_stock_row(payload: StockLiveUpdateRowRequest, runtime=Depends(get_runtime), current_user=Depends(get_current_user)):
    values_by_header = payload.values_by_header
    if _is_staff_user(current_user):
        values_by_header = _strip_cost_price_from_values_by_header(values_by_header)
    try:
        result = runtime.update_stock_row(
            payload.row_num,
            values_by_header,
            force_refresh=payload.force_refresh,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/service/add')
def add_live_service_record(payload: StockLiveServiceAddRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.add_service_record(payload.values_by_header, force_refresh=payload.force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.get('/live/service/pending')
def list_live_pending_service_records(force_refresh: bool = False, runtime=Depends(get_runtime)):
    return runtime.get_pending_service_deals(force_refresh=force_refresh)


@router.post('/live/service/return')
def return_live_service_record(payload: StockLiveReturnRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.return_service_deal(payload.row_num, force_refresh=payload.force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/service/payment')
def update_live_service_payment(payload: StockLivePendingPaymentUpdateRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.update_service_pending_payment(
            payload.row_num,
            payload.payment_status,
            amount_paid=payload.amount_paid,
            force_refresh=payload.force_refresh,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/return')
def return_live_stock_item(payload: StockLiveReturnRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.return_stock_item(payload.row_num, force_refresh=payload.force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/pending/payment')
def update_live_pending_payment(payload: StockLivePendingPaymentUpdateRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.update_pending_deal_payment(
            payload.row_num,
            payload.payment_status,
            amount_paid=payload.amount_paid,
            force_refresh=payload.force_refresh,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/import-sheet-phones')
def import_live_sheet_phones(force_refresh: bool = False, runtime=Depends(get_runtime)):
    try:
        return runtime.import_sheet_phone_numbers_to_clients(force_refresh=force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post('/live/cart/checkout')
def checkout_live_stock_cart(
    payload: StockCartCheckoutRequest,
    runtime=Depends(get_runtime),
    current_user=Depends(get_current_user),
):
    try:
        result = runtime.checkout_sale_cart(
            [item.model_dump() for item in payload.items],
            force_refresh=payload.force_refresh,
            sold_by=str((current_user or {}).get('username') or ''),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f'Cart checkout temporarily unavailable: {exc}') from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/refresh-workspace', dependencies=[Depends(require_staff)])
def refresh_workspace_endpoint(runtime=Depends(get_runtime), current_user=Depends(require_staff)):
    """Allows staff and admin to refresh the workspace (pull latest from sheets)."""
    try:
        result = runtime._process_client_sheet_sync(force_refresh=True, include_autofill=False)
        return {
            'status': 'success',
            'message': 'Workspace refreshed successfully',
            'refreshed_by': str((current_user or {}).get('username') or 'system'),
            'refresh_result': result,
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f'Workspace refresh failed: {exc}') from exc
