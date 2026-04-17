#!/usr/bin/env python3
import argparse
import hashlib
from datetime import datetime, timezone

from backend.runtime import BackendRuntime
from services.billing_service import clean_amount
from services.stock_service import header_index as stock_header_index
from services.sync_service import detect_sheet_header_row


def _header_index(headers_upper, *names):
    for name in names:
        idx = stock_header_index(headers_upper, name)
        if idx is not None:
            return idx
    return None


def _safe_cell(row, index):
    if index is None:
        return ''
    if index < 0 or index >= len(row):
        return ''
    return row[index]


def _build_fallback_stock_record_id(row_num, row):
    raw_parts = [
        str(_safe_cell(row, 0) or ''),
        str(_safe_cell(row, 1) or ''),
        str(_safe_cell(row, 2) or ''),
        str(_safe_cell(row, 3) or ''),
        str(_safe_cell(row, 4) or ''),
        str(_safe_cell(row, 5) or ''),
        str(_safe_cell(row, 6) or ''),
        str(row_num),
    ]
    digest = hashlib.sha1('|'.join(raw_parts).encode('utf-8')).hexdigest()[:24]
    return f'legacy-{digest}'


def _parse_sale_date(date_text):
    text = str(date_text or '').strip()
    if not text:
        return datetime.now(timezone.utc)

    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y'):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    try:
        if text.endswith('Z'):
            text = text[:-1] + '+00:00'
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return datetime.now(timezone.utc)


def main():
    parser = argparse.ArgumentParser(description='One-time idempotent backfill for sales_ledger.')
    parser.add_argument('--force-refresh', action='store_true', help='Pull latest sheet cache before reading.')
    parser.add_argument('--dry-run', action='store_true', help='Only print what would be inserted.')
    args = parser.parse_args()

    runtime = BackendRuntime()
    runtime._init_postgres_sync()

    if not runtime.postgres_ready:
        raise SystemExit('PostgreSQL foundation storage is not ready. Start backend with sheets environment first.')

    stock_values = runtime.get_stock_values(force_refresh=args.force_refresh)
    if not stock_values:
        raise SystemExit('Stock values are empty; nothing to backfill.')

    main_values = runtime.get_main_values(force_refresh=args.force_refresh)
    if not main_values:
        raise SystemExit('Main values are empty; nothing to backfill.')

    header_row_idx = detect_sheet_header_row(stock_values)
    headers = [str(cell or '').strip() for cell in stock_values[header_row_idx]] if header_row_idx < len(stock_values) else []
    headers_upper = [h.upper() for h in headers]

    date_col = _header_index(headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE', 'DATE')
    status_col = _header_index(headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
    inventory_status_col = _header_index(headers_upper, 'STATUS OF DEVICE', 'STATUS')
    cost_col = _header_index(headers_upper, 'COST PRICE', 'COST', 'BUYING PRICE')
    qty_col = _header_index(headers_upper, 'QTY', 'QUANTITY')
    record_id_col = _header_index(headers_upper, 'RECORD_ID', 'RECORD ID')
    imei_col = _header_index(headers_upper, 'IMEI')

    if status_col is None and inventory_status_col is None:
        raise SystemExit('Could not find stock status columns for SOLD filtering.')
    if cost_col is None:
        raise SystemExit('Could not find COST PRICE column required for cost_price_at_sale mapping.')

    main_header_row_idx = detect_sheet_header_row(main_values)
    main_headers = [str(cell or '').strip() for cell in main_values[main_header_row_idx]] if main_header_row_idx < len(main_values) else []
    main_headers_upper = [h.upper() for h in main_headers]
    main_record_id_col = _header_index(main_headers_upper, 'RECORD_ID', 'RECORD ID')
    main_amount_paid_col = _header_index(main_headers_upper, 'AMOUNT PAID', 'PAID AMOUNT', 'AMOUNT PAID ')
    main_qty_col = _header_index(main_headers_upper, 'QTY', 'QUANTITY')
    main_imei_col = _header_index(main_headers_upper, 'IMEI')

    if main_amount_paid_col is None:
        raise SystemExit('Could not find main Amount paid column for selling_price mapping.')

    main_by_record_id = {}
    main_by_imei = {}
    for main_row_num in range(main_header_row_idx + 2, len(main_values) + 1):
        main_row = list(main_values[main_row_num - 1] if main_row_num - 1 < len(main_values) else [])
        record_key = str(_safe_cell(main_row, main_record_id_col) or '').strip()
        if record_key:
            main_by_record_id[record_key] = main_row
        imei_value = str(_safe_cell(main_row, main_imei_col) or '').strip()
        if imei_value:
            main_by_imei[imei_value] = main_row

    manager = runtime.postgres_sync_manager
    service = runtime.financial_data_service

    scanned = 0
    inserted = 0
    skipped_existing = 0
    skipped_invalid = 0
    skipped_unsold = 0

    for row_num in range(header_row_idx + 2, len(stock_values) + 1):
        row = list(stock_values[row_num - 1] if row_num - 1 < len(stock_values) else [])
        scanned += 1

        stock_status_text = str(_safe_cell(row, status_col) or '').strip().upper()
        inventory_status_text = str(_safe_cell(row, inventory_status_col) or '').strip().upper()
        if stock_status_text != 'SOLD' and inventory_status_text != 'SOLD':
            skipped_unsold += 1
            continue

        # cost_price_at_sale must be a valid non-zero snapshot value.
        cost_price_at_sale = clean_amount(_safe_cell(row, cost_col))
        if cost_price_at_sale <= 0:
            skipped_invalid += 1
            continue

        stock_record_id = str(_safe_cell(row, record_id_col) or '').strip()
        if not stock_record_id:
            stock_record_id = _build_fallback_stock_record_id(row_num, row)

        main_row = None
        if record_id_col is not None:
            raw_stock_record_id = str(_safe_cell(row, record_id_col) or '').strip()
            if raw_stock_record_id:
                main_row = main_by_record_id.get(raw_stock_record_id)
        if main_row is None and imei_col is not None:
            imei_value = str(_safe_cell(row, imei_col) or '').strip()
            if imei_value:
                main_row = main_by_imei.get(imei_value)
        if main_row is None:
            skipped_invalid += 1
            continue

        # selling_price must represent actual customer payment from main sheet.
        selling_price = clean_amount(_safe_cell(main_row, main_amount_paid_col))
        if selling_price <= 0:
            skipped_invalid += 1
            continue

        quantity = int(clean_amount(_safe_cell(row, qty_col)) or clean_amount(_safe_cell(main_row, main_qty_col)) or 1)
        quantity = max(1, quantity)

        existing = manager.fetchone_dict(
            'SELECT id FROM sales_ledger WHERE stock_record_id = %s LIMIT 1',
            (stock_record_id,),
        )
        if existing and existing.get('id'):
            skipped_existing += 1
            continue

        date_text = str(_safe_cell(row, date_col) or '').strip()
        sale_date = _parse_sale_date(date_text)

        if args.dry_run:
            inserted += 1
            continue

        service.create_sale_ledger_entry(
            stock_record_id=stock_record_id,
            stock_row_num=row_num,
            selling_price=selling_price,
            cost_price_at_sale=cost_price_at_sale,
            quantity=quantity,
            date=sale_date,
            sold_by='backfill',
        )
        inserted += 1

    print('Backfill complete')
    print(f'scanned={scanned}')
    print(f'skipped_unsold={skipped_unsold}')
    print(f'inserted={inserted}')
    print(f'skipped_existing={skipped_existing}')
    print(f'skipped_invalid={skipped_invalid}')
    print(f'valid_sales_inserted={inserted}')
    print(f'invalid_data_skipped={skipped_invalid}')
    if args.dry_run:
        print('dry_run=true')


if __name__ == '__main__':
    main()
