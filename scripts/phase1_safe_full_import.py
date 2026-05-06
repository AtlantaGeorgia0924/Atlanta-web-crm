import json
import os
import sys
from dataclasses import dataclass

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.runtime import BackendRuntime
from db_sync import PostgresSyncManager


@dataclass
class ImportStats:
    key: str
    sheet_name: str
    sheet_rows: int
    supabase_rows_before: int
    missing_rows_imported: int
    duplicate_rows_skipped: int
    already_present_skipped: int
    supabase_rows_after: int


def normalize_scalar(value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, float)):
        text = str(value)
        return text[:-2] if text.endswith('.0') else text
    return str(value).strip()


def signature_dict_row(row):
    payload = {
        str(key): normalize_scalar(value)
        for key, value in sorted((row or {}).items(), key=lambda item: str(item[0]))
    }
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def signature_list_row(row):
    payload = [normalize_scalar(value) for value in (row or [])]
    return json.dumps(payload, separators=(',', ':'))


def merge_missing_rows(existing_rows, sheet_rows, signature_builder):
    existing = list(existing_rows or [])
    sheet = list(sheet_rows or [])

    existing_signatures = set()
    for row in existing:
        existing_signatures.add(signature_builder(row))

    sheet_seen = set()
    missing_rows = []
    duplicate_rows_skipped = 0
    already_present_skipped = 0

    for row in sheet:
        row_sig = signature_builder(row)
        if row_sig in sheet_seen:
            duplicate_rows_skipped += 1
            continue
        sheet_seen.add(row_sig)

        if row_sig in existing_signatures:
            already_present_skipped += 1
            continue

        missing_rows.append(row)
        existing_signatures.add(row_sig)

    merged_rows = existing + missing_rows
    return {
        'merged_rows': merged_rows,
        'missing_rows': missing_rows,
        'duplicate_rows_skipped': duplicate_rows_skipped,
        'already_present_skipped': already_present_skipped,
    }


def read_sheet_sources(runtime):
    main_records = runtime.main_sheet.get_all_records()

    stock_worksheet = runtime._resolve_stock_worksheet(runtime._resolve_stock_sheet_id())
    stock_values = stock_worksheet.get_all_values()

    cashflow_worksheet = runtime._resolve_cashflow_expense_worksheet(create_if_missing=False)
    cashflow_values = cashflow_worksheet.get_all_values() if cashflow_worksheet is not None else []

    return {
        'main_records': {
            'sheet_name': runtime.main_sheet.title,
            'rows': main_records,
            'signature': signature_dict_row,
        },
        'stock_values': {
            'sheet_name': stock_worksheet.title,
            'rows': stock_values,
            'signature': signature_list_row,
        },
        'cashflow_expense_values': {
            'sheet_name': cashflow_worksheet.title if cashflow_worksheet is not None else 'CASH FLOW',
            'rows': cashflow_values,
            'signature': signature_list_row,
        },
    }


def run_phase1_import(config_path='config.json'):
    runtime = BackendRuntime(config_path=config_path)

    if not runtime._connect_sheets():
        raise RuntimeError(runtime.sync_state.get('sheet_error') or 'Failed to connect Google Sheets')

    # Prefer POSTGRES_DSN env var (Supabase) over config.json which may hold a local DSN
    dsn = (
        os.getenv('POSTGRES_DSN')
        or os.getenv('APP_AUTH_POSTGRES_DSN')
        or str(runtime.config.get('postgres_dsn') or '')
    ).strip()
    manager = PostgresSyncManager(dsn=dsn, pull_interval_sec=90, logger=runtime.logger)
    if not manager.ready:
        raise RuntimeError('PostgreSQL DSN/driver not ready. Set POSTGRES_DSN to your Supabase connection string.')

    manager.ensure_schema()

    sources = read_sheet_sources(runtime)
    report = []

    for key in ('main_records', 'stock_values', 'cashflow_expense_values'):
        source = sources[key]
        sheet_rows = source['rows']
        signature = source['signature']

        existing_rows = manager.load_cached_rows(key)
        before_count = len(existing_rows)

        merge_result = merge_missing_rows(existing_rows, sheet_rows, signature)
        merged_rows = merge_result['merged_rows']
        missing_rows = merge_result['missing_rows']

        if missing_rows:
            manager.upsert_sheet_cache(key, merged_rows)

        report.append(ImportStats(
            key=key,
            sheet_name=source['sheet_name'],
            sheet_rows=len(sheet_rows),
            supabase_rows_before=before_count,
            missing_rows_imported=len(missing_rows),
            duplicate_rows_skipped=merge_result['duplicate_rows_skipped'],
            already_present_skipped=merge_result['already_present_skipped'],
            supabase_rows_after=len(merged_rows),
        ))

    total_imported = sum(item.missing_rows_imported for item in report)
    total_duplicates_skipped = sum(item.duplicate_rows_skipped for item in report)
    total_present_skipped = sum(item.already_present_skipped for item in report)

    # Refresh operational mirror tables from freshly imported cache
    mirrors_refreshed = False
    mirror_errors = []
    try:
        # Wire the manager into the runtime so _refresh_operational_mirrors can use it
        runtime.postgres_sync_manager = manager
        runtime.sync_state['ready'] = True
        main_records = manager.load_cached_rows('main_records')
        stock_values = manager.load_cached_rows('stock_values')
        cashflow_values = manager.load_cached_rows('cashflow_expense_values')
        runtime._refresh_operational_mirrors(main_records, stock_values, cashflow_values)
        mirrors_refreshed = True
    except Exception as exc:
        mirror_errors.append(str(exc))

    # Verify row counts directly from Supabase
    supabase_verified = {}
    try:
        for sheet_key in ('main_records', 'stock_values', 'cashflow_expense_values'):
            row = manager.fetchone(
                "SELECT row_count FROM sheet_cache WHERE sheet_key = %s", (sheet_key,)
            )
            supabase_verified[sheet_key] = int(row[0]) if row else 0
        for tbl in ('operational_stock_rows', 'operational_billing_rows', 'operational_cashflow_rows'):
            row = manager.fetchone(f"SELECT COUNT(*) FROM {tbl}")
            supabase_verified[tbl] = int(row[0]) if row else 0
    except Exception as exc:
        mirror_errors.append(f'verification_error: {exc}')

    return {
        'status': 'ok',
        'phase': 'phase1_safe_full_import',
        'total_missing_rows_imported': total_imported,
        'total_duplicate_rows_skipped': total_duplicates_skipped,
        'total_already_present_skipped': total_present_skipped,
        'mirrors_refreshed': mirrors_refreshed,
        'mirror_errors': mirror_errors,
        'supabase_row_counts': supabase_verified,
        'tables': [
            {
                'table': item.key,
                'sheet_name': item.sheet_name,
                'sheet_row_count': item.sheet_rows,
                'supabase_row_count_before': item.supabase_rows_before,
                'missing_rows_imported': item.missing_rows_imported,
                'duplicate_rows_skipped': item.duplicate_rows_skipped,
                'already_present_skipped': item.already_present_skipped,
                'supabase_row_count_after': item.supabase_rows_after,
            }
            for item in report
        ],
    }


if __name__ == '__main__':
    result = run_phase1_import()
    print(json.dumps(result, indent=2))
