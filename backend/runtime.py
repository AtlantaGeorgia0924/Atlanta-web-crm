import base64
import glob
import json
import logging
import mimetypes
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build

from db_sync import PSYCOPG2_AVAILABLE, create_postgres_sync_manager
from services.billing_service import build_payment_plan, clean_amount, compute_debtors, parse_sheet_date
from services.client_service import (
    build_client_directory_rows,
    find_existing_client_key,
    import_sheet_phone_numbers_to_registry,
    normalize_client_name,
    normalize_phone_number,
    set_client_phone,
    validate_client_entry,
)
from services.contact_import_service import fetch_google_contacts
from services.financial_foundation_service import FinancialFoundationService
from services.name_fix_service import (
    build_name_fix_all_updates,
    build_name_fix_updates,
    find_name_mismatches,
)
from services.stock_service import (
    build_sale_status_update_values,
    build_stock_form_defaults,
    build_stock_row_values,
    build_stock_view,
    detect_stock_headers,
    find_next_table_write_row,
    get_stock_color_status_map as svc_get_stock_color_status_map,
    header_index as svc_stock_header_index,
    map_sale_status,
    normalize_stock_status_value,
    order_stock_form_headers,
    stock_status_key_to_label,
    validate_stock_row,
)
from services.sync_service import (
    build_client_phone_sheet_updates,
    build_phone_autofill_plan,
    column_index_to_letter,
    detect_sheet_header_row,
    ensure_directory_sheet,
    rollout_record_ids_for_known_sheets,
)


class BackendRuntime:
    def __init__(self, config_path='config.json'):
        self.config_path = config_path
        self.base_dir = os.path.dirname(os.path.abspath(config_path)) or os.getcwd()
        self.clients_file = os.path.join(self.base_dir, 'clients.json')
        self.logger = logging.getLogger(__name__)
        self._sheet_lock = threading.RLock()
        self._clients_lock = threading.RLock()
        self._google_contacts_lock = threading.RLock()
        self._rollout_done = False

        self.config = self._load_config()
        self.creds = None
        self.gspread_client = None
        self.sheets_api_service = None
        self.main_spreadsheet = None
        self.main_sheet = None
        self.postgres_sync_manager = None
        self.financial_data_service = FinancialFoundationService(None, logger=self.logger)
        self._google_contacts_cache = []
        self._google_contacts_synced_at = ''
        self._logo_payload = None
        self.last_payment_action = None
        self.last_undone_payment_action = None

        self.sync_state = {
            'enabled': False,
            'ready': False,
            'last_status': 'disabled',
            'last_error': '',
            'sheets_connected': False,
            'sheet_error': '',
        }

    def _load_config(self):
        defaults = {
            'sheet_id': '',
            'phone_stock_sheet_id': '',
            'credentials_file': 'credentials.json',
            'contacts_oauth_file': '',
            'enable_postgres_cache': True,
            'legacy_sheet_fallback': True,
            'startup_mode': 'cache_then_sync',
            'sync_pull_interval_sec': 90,
            'sync_conflict_policy': 'sheet_wins',
            'record_id_rollout': True,
            'postgres_dsn': '',
            'payment_details': '',
        }

        config = defaults.copy()
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as config_file:
                    loaded = json.load(config_file)
                if isinstance(loaded, dict):
                    config.update(loaded)
            except Exception as exc:
                self.logger.warning('Failed to load %s: %s', self.config_path, exc)

        env_dsn = os.getenv('POSTGRES_DSN') or os.getenv('DATABASE_URL')
        if env_dsn:
            config['postgres_dsn'] = env_dsn

        # Render/production-friendly overrides so secrets can come from env vars.
        env_sheet_id = os.getenv('SHEET_ID') or os.getenv('MAIN_SHEET_ID')
        if env_sheet_id:
            config['sheet_id'] = env_sheet_id

        env_stock_sheet_id = os.getenv('PHONE_STOCK_SHEET_ID') or os.getenv('STOCK_SHEET_ID')
        if env_stock_sheet_id:
            config['phone_stock_sheet_id'] = env_stock_sheet_id

        env_credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE') or os.getenv('CREDENTIALS_FILE')
        if env_credentials_file:
            config['credentials_file'] = env_credentials_file

        env_contacts_oauth_file = os.getenv('GOOGLE_CONTACTS_OAUTH_FILE') or os.getenv('CONTACTS_OAUTH_FILE')
        if env_contacts_oauth_file:
            config['contacts_oauth_file'] = env_contacts_oauth_file

        # Billing/payment account details can be injected via env vars in production.
        env_payment_details = (
            os.getenv('PAYMENT_DETAILS')
            or os.getenv('APP_PAYMENT_DETAILS')
            or os.getenv('BILL_PAYMENT_DETAILS')
            or os.getenv('ACCOUNT_DETAILS')
        )
        if env_payment_details is not None and str(env_payment_details).strip():
            config['payment_details'] = str(env_payment_details).replace('\\n', '\n').strip()
        elif config.get('payment_details'):
            config['payment_details'] = str(config.get('payment_details') or '').replace('\\n', '\n').strip()

        return config

    def _resolve_contacts_oauth_file(self):
        configured_path = str(self.config.get('contacts_oauth_file', '')).strip()
        if configured_path and os.path.exists(configured_path):
            return configured_path

        home_dir = os.path.expanduser('~')
        candidate_patterns = [
            os.path.join(self.base_dir, 'credentials1.json'),
            os.path.join(self.base_dir, 'credentials*.json'),
            os.path.join(self.base_dir, 'client_secret*.json'),
            os.path.join(self.base_dir, '*oauth*.json'),
            os.path.join(home_dir, 'Downloads', 'credentials1.json'),
            os.path.join(home_dir, 'Downloads', 'credentials*.json'),
            os.path.join(home_dir, 'Downloads', 'client_secret*.json'),
            os.path.join(home_dir, 'Downloads', '*oauth*.json'),
        ]

        for pattern in candidate_patterns:
            matches = sorted(path for path in glob.glob(pattern) if os.path.isfile(path))
            if matches:
                resolved = matches[0]
                self.config['contacts_oauth_file'] = resolved
                return resolved

        return ''

    def _load_service_account_credentials(self, scopes):
        raw_json = (os.environ.get('GOOGLE_CREDS_JSON') or '').strip()
        if not raw_json:
            # Fall back to credentials_file from config (e.g. credentials.json)
            creds_file = self.config.get('credentials_file', 'credentials.json')
            if not os.path.isabs(creds_file):
                creds_file = os.path.join(self.base_dir, creds_file)
            if os.path.exists(creds_file):
                try:
                    with open(creds_file, 'r') as f:
                        raw_json = f.read().strip()
                except Exception as exc:
                    raise RuntimeError(f'Could not read credentials file {creds_file}: {exc}') from exc
            else:
                raise RuntimeError('Missing GOOGLE_CREDS_JSON environment variable for Google Sheets authentication')

        try:
            info = json.loads(raw_json)
        except Exception as exc:
            raise RuntimeError(f'Invalid GOOGLE_CREDS_JSON: {exc}') from exc

        try:
            return ServiceAccountCredentials.from_service_account_info(info, scopes=scopes)
        except Exception as exc:
            raise RuntimeError(f'GOOGLE_CREDS_JSON could not initialize credentials: {exc}') from exc

    def _load_clients_from_disk(self):
        if not os.path.exists(self.clients_file):
            return {}

        try:
            with open(self.clients_file, 'r') as clients_file:
                payload = json.load(clients_file)
        except Exception as exc:
            self.logger.warning('Failed to load %s: %s', self.clients_file, exc)
            return {}

        if not isinstance(payload, dict):
            return {}

        normalized = {}
        for key, value in payload.items():
            clean_key = normalize_client_name(key) or str(key or '').strip()
            if not clean_key:
                continue
            normalized[clean_key] = normalize_phone_number(value)

        if normalized != payload:
            try:
                self._save_clients_to_disk(normalized)
            except Exception as exc:
                self.logger.warning('Failed to normalize %s: %s', self.clients_file, exc)

        return normalized

    def _save_clients_to_disk(self, registry):
        normalized = {
            normalize_client_name(key): normalize_phone_number(value)
            for key, value in sorted((registry or {}).items(), key=lambda item: str(item[0]).upper())
            if normalize_client_name(key)
        }

        with open(self.clients_file, 'w') as clients_file:
            json.dump(normalized, clients_file, indent=4)

        return normalized

    @staticmethod
    def _extract_sheet_id(value):
        raw = str(value or '').strip()
        if not raw:
            return ''
        if '/d/' in raw:
            raw = raw.split('/d/', 1)[1]
        raw = raw.split('/', 1)[0]
        raw = raw.split('?', 1)[0]
        raw = raw.split('#', 1)[0]
        return raw.strip()

    @property
    def postgres_ready(self):
        return bool(self.postgres_sync_manager and self.postgres_sync_manager.ready and self.sync_state.get('ready'))

    def _safe_record_sale_ledger_entry(
        self,
        *,
        stock_record_id,
        stock_row_num,
        selling_price,
        cost_price_at_sale,
        quantity,
        date,
        sold_by,
    ):
        stock_record_key = str(stock_record_id or '').strip()
        if not stock_record_key:
            return None

        try:
            profit = (selling_price - cost_price_at_sale) * quantity
            if profit > 0:
                total_cost = cost_price_at_sale * quantity
                self.append_cashflow_income_record(
                    amount=profit,
                    category='PHONE PROFIT',
                    description=f'Item record_id: {stock_record_key}, sale: {selling_price:.2f}, cost: {cost_price_at_sale:.2f}',
                    date_text=datetime.now(timezone.utc).date().isoformat(),
                    created_by=str(sold_by or '').strip(),
                    payment_status='PAID',
                    entry_type='phone',
                    cost_price=total_cost,
                    payment_date_text=datetime.now(timezone.utc).date().isoformat(),
                )
        except Exception as cashflow_exc:
            self.logger.warning('Failed to write phone profit to cashflow sheet: %s', cashflow_exc)

        if not self.financial_data_service or not self.financial_data_service.ready:
            return None

        try:
            existing = self.postgres_sync_manager.fetchone_dict(
                "SELECT id FROM sales_ledger WHERE stock_record_id = %s LIMIT 1",
                (stock_record_key,),
            )
            if existing and existing.get('id'):
                return self.financial_data_service.get_sale_ledger_entry(existing.get('id'))

            result = self.financial_data_service.create_sale_ledger_entry(
                stock_record_id=stock_record_key,
                stock_row_num=stock_row_num,
                selling_price=selling_price,
                cost_price_at_sale=cost_price_at_sale,
                quantity=quantity,
                date=date,
                sold_by=sold_by,
            )
            
            # Log the sale to audit log
            try:
                profit = (selling_price - cost_price_at_sale) * quantity
                self.financial_data_service.log_sale_action(
                    sold_by=sold_by,
                    items_count=quantity,
                    total_amount=selling_price * quantity,
                    description=f"Item record_id: {stock_record_key}, profit: {profit:.2f}",
                )
            except Exception as audit_exc:
                self.logger.warning('Failed to log sale audit entry: %s', audit_exc)
            
            return result
        except Exception as exc:
            self.logger.warning(
                'Non-blocking sales_ledger write failed for stock_record_id=%s: %s',
                stock_record_key,
                exc,
            )
            return None

    def start(self):
        self._connect_sheets()
        self._init_postgres_sync()

    def stop(self):
        try:
            if self.postgres_sync_manager:
                self.postgres_sync_manager.stop()
        except Exception as exc:
            self.logger.warning('Backend runtime shutdown warning: %s', exc)

    def _connect_sheets(self):
        main_sheet_id = self._extract_sheet_id(self.config.get('sheet_id', ''))
        if not main_sheet_id:
            self.sync_state['sheets_connected'] = False
            self.sync_state['sheet_error'] = 'sheet_id is empty (set SHEET_ID or config.json sheet_id)'
            return False

        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            self.creds = self._load_service_account_credentials(scopes)
            with self._sheet_lock:
                self.gspread_client = gspread.authorize(self.creds)
                self.sheets_api_service = build('sheets', 'v4', credentials=self.creds, cache_discovery=False)
                self.main_spreadsheet = self.gspread_client.open_by_key(main_sheet_id)
                self.main_sheet = self.main_spreadsheet.sheet1
            self.sync_state['sheets_connected'] = True
            self.sync_state['sheet_error'] = ''
            return True
        except Exception as exc:
            self.sync_state['sheets_connected'] = False
            self.sync_state['sheet_error'] = str(exc)
            self.logger.warning('Google Sheets connection failed: %s', exc)
            return False

    def _ensure_sheet_connection(self):
        if self.sync_state.get('sheets_connected') and self.main_spreadsheet and self.main_sheet and self.sheets_api_service:
            return True
        return self._connect_sheets()

    def _resolve_stock_sheet_id(self):
        return self._extract_sheet_id(self.config.get('phone_stock_sheet_id', ''))

    def _resolve_stock_worksheet(self, stock_sheet_id=None):
        stock_sheet_id = stock_sheet_id or self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            raise RuntimeError('Stock sheet ID is missing')
        if not self._ensure_sheet_connection():
            raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')
        return self.gspread_client.open_by_key(stock_sheet_id).sheet1

    def _resolve_cashflow_expense_worksheet(self, create_if_missing=True):
        if not self._ensure_sheet_connection():
            raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')

        expected_titles = {'CASH FLOW', 'DATA SHEET 2', 'CASH FLOW EXPENSES'}
        with self._sheet_lock:
            for worksheet in self.main_spreadsheet.worksheets():
                if str(worksheet.title or '').strip().upper() in expected_titles:
                    return worksheet

            if not create_if_missing:
                return None

            worksheet = self.main_spreadsheet.add_worksheet(title='CASH FLOW', rows='1000', cols='10')
            worksheet.update(
                'A1:J1',
                [[
                    'DATE',
                    'CATEGORY',
                    'AMOUNT',
                    'DESCRIPTION',
                    'CREATED BY',
                    'SOURCE',
                    'PAYMENT_STATUS',
                    'TYPE',
                    'COST_PRICE',
                    'PAYMENT_DATE',
                ]],
            )
            return worksheet

    @staticmethod
    def _normalize_cashflow_expense_row(row_values, row_num=None):
        row_values = list(row_values or [])
        if len(row_values) < 10:
            row_values = row_values + [''] * (10 - len(row_values))
        raw_payment_status = str(row_values[6] or '').strip().upper()
        # Backward compat: old income rows without payment_status default to PAID.
        source = str(row_values[5] or '').strip() or 'expense'
        raw_type = str(row_values[7] or '').strip().lower()
        raw_cost_price = str(row_values[8] or '').strip()
        raw_payment_date = str(row_values[9] or '').strip()

        if source == 'income' and raw_payment_status not in ('PAID', 'OWING'):
            raw_payment_status = 'PAID'
        elif source != 'income':
            raw_payment_status = ''

        if source == 'income':
            if raw_type not in ('phone', 'service'):
                category = str(row_values[1] or '').strip().lower()
                raw_type = 'service' if 'service' in category else 'phone'
            if raw_payment_status == 'PAID' and not raw_payment_date:
                raw_payment_date = str(row_values[0] or '').strip()
        else:
            raw_type = 'expense'
            raw_payment_date = ''

        return {
            'row_num': row_num,
            'date': str(row_values[0] or '').strip(),
            'category': str(row_values[1] or '').strip(),
            'amount': str(row_values[2] or '').strip(),
            'description': str(row_values[3] or '').strip(),
            'created_by': str(row_values[4] or '').strip(),
            'source': source,
            'payment_status': raw_payment_status,
            'type': raw_type,
            'cost_price': raw_cost_price,
            'payment_date': raw_payment_date,
        }

    def _append_cashflow_sheet_record(self, *, amount, category='', description='', date_text='', created_by='', source='expense', payment_status='', entry_type='', cost_price='', payment_date_text=''):
        worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=True)
        resolved_source = str(source or '').strip() or 'expense'
        if resolved_source == 'income':
            resolved_payment_status = str(payment_status or '').strip().upper()
            if resolved_payment_status not in ('PAID', 'OWING'):
                resolved_payment_status = 'PAID'
            resolved_type = str(entry_type or '').strip().lower()
            if resolved_type not in ('phone', 'service'):
                category_text = str(category or '').strip().lower()
                resolved_type = 'service' if 'service' in category_text else 'phone'
            resolved_cost_price = str(cost_price or '').strip()
            resolved_payment_date = str(payment_date_text or '').strip()
            if resolved_payment_status == 'PAID' and not resolved_payment_date:
                resolved_payment_date = str(date_text or datetime.now(timezone.utc).date().isoformat()).strip()
        else:
            resolved_payment_status = ''
            resolved_type = 'expense'
            resolved_cost_price = ''
            resolved_payment_date = ''
        row_values = [
            str(date_text or datetime.now(timezone.utc).date().isoformat()).strip(),
            str(category or '').strip(),
            str(amount or '').strip(),
            str(description or '').strip(),
            str(created_by or '').strip(),
            resolved_source,
            resolved_payment_status,
            resolved_type,
            resolved_cost_price,
            resolved_payment_date,
        ]
        with self._sheet_lock:
            worksheet.append_row(row_values, value_input_option='USER_ENTERED')
            if self.postgres_ready:
                try:
                    self.postgres_sync_manager.upsert_sheet_cache('cashflow_expense_values', worksheet.get_all_values())
                except Exception as exc:
                    self.logger.warning('Failed to refresh cashflow sheet cache after append: %s', exc)
        return self._normalize_cashflow_expense_row(row_values)

    @staticmethod
    def _most_recent_saturday(today=None):
        today = today or datetime.now(timezone.utc).date()
        days_since_saturday = (today.weekday() - 5) % 7
        return today - timedelta(days=days_since_saturday)

    def _normalized_reserve_percentage(self):
        raw_value = self.config.get('reserve_percentage', 0.3)
        try:
            reserve = float(raw_value)
        except (TypeError, ValueError):
            reserve = 0.3
        if reserve > 1.0 and reserve <= 100.0:
            reserve /= 100.0
        return max(0.0, min(reserve, 1.0))

    def _normalized_allowance_percentage(self):
        raw_value = self.config.get('allowance_percentage', 0.25)
        try:
            allowance = float(raw_value)
        except (TypeError, ValueError):
            allowance = 0.25
        if allowance > 1.0 and allowance <= 100.0:
            allowance /= 100.0
        return max(0.0, min(allowance, 1.0))

    def _read_numeric_config(self, key, default=0.0):
        raw_value = self.config.get(key, default)
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return float(default)

    def get_cashflow_expense_records(self, force_refresh=False):
        sheet_entries = []
        sheet_title = 'CASH FLOW'

        if not force_refresh:
            cached_values = self._load_cached_rows('cashflow_expense_values')
            if cached_values:
                try:
                    worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=False)
                    if worksheet is not None:
                        sheet_title = worksheet.title or sheet_title
                except Exception:
                    pass
                for row_num, row_values in enumerate(cached_values[1:], start=2):
                    if not row_values or not any(str(cell or '').strip() for cell in row_values):
                        continue
                    normalized = self._normalize_cashflow_expense_row(row_values, row_num=row_num)
                    if str(normalized.get('source') or '').strip().lower() == 'income':
                        continue
                    sheet_entries.append(normalized)
                if sheet_entries:
                    sheet_entries.reverse()
                    total = sum(clean_amount(entry.get('amount')) for entry in sheet_entries)
                    return {
                        'items': sheet_entries,
                        'count': len(sheet_entries),
                        'total': total,
                        'source': 'sheet',
                        'sheet_title': sheet_title,
                    }

        try:
            worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=False)
        except Exception as exc:
            self.logger.warning('Failed to resolve cashflow expense worksheet: %s', exc)
            worksheet = None

        if worksheet is not None:
            sheet_title = worksheet.title or sheet_title
            with self._sheet_lock:
                values = worksheet.get_all_values()
            if self.postgres_ready:
                try:
                    self.postgres_sync_manager.upsert_sheet_cache('cashflow_expense_values', values)
                except Exception as exc:
                    self.logger.warning('Failed to upsert cashflow expense cache: %s', exc)
            for row_num, row_values in enumerate(values[1:], start=2):
                if not row_values or not any(str(cell or '').strip() for cell in row_values):
                    continue
                normalized = self._normalize_cashflow_expense_row(row_values, row_num=row_num)
                if str(normalized.get('source') or '').strip().lower() == 'income':
                    continue
                sheet_entries.append(normalized)

        if sheet_entries:
            sheet_entries.reverse()
            total = sum(clean_amount(entry.get('amount')) for entry in sheet_entries)
            return {
                'items': sheet_entries,
                'count': len(sheet_entries),
                'total': total,
                'source': 'sheet',
                'sheet_title': sheet_title,
            }

        return {
            'items': [],
            'count': 0,
            'total': 0.0,
            'source': 'sheet',
            'sheet_title': sheet_title,
        }

    def append_cashflow_expense_record(self, amount, category='', description='', date_text='', created_by=''):
        return self._append_cashflow_sheet_record(
            amount=amount,
            category=category,
            description=description,
            date_text=date_text,
            created_by=created_by,
            source='expense',
            entry_type='expense',
        )

    def append_cashflow_income_record(self, amount, category='', description='', date_text='', created_by='', payment_status='PAID', entry_type='service', cost_price='', payment_date_text=''):
        return self._append_cashflow_sheet_record(
            amount=amount,
            category=category,
            description=description,
            date_text=date_text,
            created_by=created_by,
            source='income',
            payment_status=payment_status,
            entry_type=entry_type,
            cost_price=cost_price,
            payment_date_text=payment_date_text,
        )

    @staticmethod
    def _cashflow_row_match(left, right):
        return str(left or '').strip().upper() == str(right or '').strip().upper()

    @staticmethod
    def _record_value(record, *aliases):
        payload = record or {}
        for key in aliases:
            if key in payload:
                return payload.get(key)
            upper_key = str(key or '').strip().upper()
            if upper_key in payload:
                return payload.get(upper_key)
        return None

    def mark_cashflow_income_paid(self, *, entry_type, description='', created_by='', payment_date_text='', amount=None, cost_price=None):
        worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=True)
        target_payment_date = str(payment_date_text or datetime.now(timezone.utc).date().isoformat()).strip()
        target_type = str(entry_type or '').strip().lower() or 'service'
        with self._sheet_lock:
            values = worksheet.get_all_values()
            target_row_num = None
            target_values = None
            for row_num in range(len(values), 1, -1):
                row_values = values[row_num - 1] if row_num - 1 < len(values) else []
                normalized = self._normalize_cashflow_expense_row(row_values, row_num=row_num)
                if str(normalized.get('source') or '').strip().lower() != 'income':
                    continue
                if str(normalized.get('payment_status') or '').strip().upper() != 'OWING':
                    continue
                if str(normalized.get('type') or '').strip().lower() != target_type:
                    continue
                if description and not self._cashflow_row_match(normalized.get('description'), description):
                    continue
                if created_by and not self._cashflow_row_match(normalized.get('created_by'), created_by):
                    continue
                target_row_num = row_num
                target_values = list(row_values or [])
                break

            if target_row_num is None:
                return None

            if len(target_values) < 10:
                target_values = target_values + [''] * (10 - len(target_values))
            target_values[0] = target_payment_date
            target_values[6] = 'PAID'
            target_values[7] = target_type
            target_values[9] = target_payment_date
            if amount is not None:
                target_values[2] = str(amount)
            if cost_price is not None:
                target_values[8] = str(cost_price)

            worksheet.update(f'A{target_row_num}:J{target_row_num}', [target_values], value_input_option='USER_ENTERED')

            if self.postgres_ready:
                try:
                    self.postgres_sync_manager.upsert_sheet_cache('cashflow_expense_values', worksheet.get_all_values())
                except Exception as exc:
                    self.logger.warning('Failed to refresh cashflow sheet cache after payment status update: %s', exc)

        return self._normalize_cashflow_expense_row(target_values, row_num=target_row_num)

    def has_cashflow_income_owing_record(self, *, entry_type, description='', created_by=''):
        worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=True)
        target_type = str(entry_type or '').strip().lower() or 'service'
        with self._sheet_lock:
            values = worksheet.get_all_values()
            for row_num in range(len(values), 1, -1):
                row_values = values[row_num - 1] if row_num - 1 < len(values) else []
                normalized = self._normalize_cashflow_expense_row(row_values, row_num=row_num)
                if str(normalized.get('source') or '').strip().lower() != 'income':
                    continue
                if str(normalized.get('payment_status') or '').strip().upper() != 'OWING':
                    continue
                if str(normalized.get('type') or '').strip().lower() != target_type:
                    continue
                if description and not self._cashflow_row_match(normalized.get('description'), description):
                    continue
                if created_by and not self._cashflow_row_match(normalized.get('created_by'), created_by):
                    continue
                return True
        return False

    def has_cashflow_income_paid_record(self, *, entry_type, description='', created_by='', payment_date_text=''):
        worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=True)
        target_type = str(entry_type or '').strip().lower() or 'service'
        target_payment_date = str(payment_date_text or '').strip()
        with self._sheet_lock:
            values = worksheet.get_all_values()
            for row_num in range(len(values), 1, -1):
                row_values = values[row_num - 1] if row_num - 1 < len(values) else []
                normalized = self._normalize_cashflow_expense_row(row_values, row_num=row_num)
                if str(normalized.get('source') or '').strip().lower() != 'income':
                    continue
                if str(normalized.get('payment_status') or '').strip().upper() != 'PAID':
                    continue
                if str(normalized.get('type') or '').strip().lower() != target_type:
                    continue
                if description and not self._cashflow_row_match(normalized.get('description'), description):
                    continue
                if created_by and not self._cashflow_row_match(normalized.get('created_by'), created_by):
                    continue
                if target_payment_date and not self._cashflow_row_match(normalized.get('payment_date'), target_payment_date):
                    continue
                return True
        return False

    def get_cashflow_sheet_records(self, force_refresh=False):
        sheet_title = 'CASH FLOW'
        sheet_entries = []

        if not force_refresh:
            cached_values = self._load_cached_rows('cashflow_expense_values')
            if cached_values:
                try:
                    worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=False)
                    if worksheet is not None:
                        sheet_title = worksheet.title or sheet_title
                except Exception:
                    pass
                for row_num, row_values in enumerate(cached_values[1:], start=2):
                    if not row_values or not any(str(cell or '').strip() for cell in row_values):
                        continue
                    sheet_entries.append(self._normalize_cashflow_expense_row(row_values, row_num=row_num))
                if sheet_entries:
                    sheet_entries.reverse()
                    return {
                        'items': sheet_entries,
                        'count': len(sheet_entries),
                        'source': 'sheet',
                        'sheet_title': sheet_title,
                    }

        try:
            worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=False)
        except Exception as exc:
            self.logger.warning('Failed to resolve cashflow expense worksheet: %s', exc)
            worksheet = None

        if worksheet is not None:
            sheet_title = worksheet.title or sheet_title
            with self._sheet_lock:
                values = worksheet.get_all_values()
            if self.postgres_ready:
                try:
                    self.postgres_sync_manager.upsert_sheet_cache('cashflow_expense_values', values)
                except Exception as exc:
                    self.logger.warning('Failed to upsert cashflow expense cache: %s', exc)
            for row_num, row_values in enumerate(values[1:], start=2):
                if not row_values or not any(str(cell or '').strip() for cell in row_values):
                    continue
                sheet_entries.append(self._normalize_cashflow_expense_row(row_values, row_num=row_num))

        sheet_entries.reverse()
        return {
            'items': sheet_entries,
            'count': len(sheet_entries),
            'source': 'sheet',
            'sheet_title': sheet_title,
        }

    def get_phone_capital_outflow(self, force_refresh=False):
        current_day = datetime.now(timezone.utc).date()
        current_month_start = current_day.replace(day=1)
        current_week_start = current_day - timedelta(days=(current_day.weekday() + 1) % 7)

        month_total = 0.0
        week_total = 0.0
        entries = []

        for record in self.get_main_records(force_refresh=force_refresh):
            imei = str(self._record_value(record, 'IMEI') or '').strip()
            if not imei:
                continue

            cost_price = max(0.0, clean_amount(self._record_value(record, 'COST PRICE', 'COST')))
            if cost_price <= 0:
                continue

            # Capital outflow must be purchase-date based only.
            stocked_date = parse_sheet_date(self._record_value(record, 'DATE BOUGHT'))
            if stocked_date is None:
                continue
            if stocked_date < current_month_start or stocked_date > current_day:
                continue

            amount = round(cost_price, 2)
            month_total += amount
            if current_week_start <= stocked_date <= current_day:
                week_total += amount

            entries.append({
                'date': stocked_date.isoformat(),
                'category': 'PHONE CAPITAL OUTFLOW',
                'amount': amount,
                'description': str(self._record_value(record, 'DESCRIPTION', 'MODEL', 'DEVICE') or '').strip(),
                'created_by': str(self._record_value(record, 'SELLER NAME', 'NAME OF SELLER', 'NAME') or '').strip(),
                'source': 'capital',
                'payment_status': '',
                'type': 'phone_capital',
                'cost_price': amount,
                'payment_date': '',
            })

        entries.sort(key=lambda row: (row.get('date') or '', row.get('description') or ''), reverse=True)
        return {
            'month_total': round(month_total, 2),
            'week_total': round(week_total, 2),
            'entries': entries,
        }

    def get_cashflow_summary_from_sheet(self, force_refresh=False):
        payload = self.get_cashflow_sheet_records(force_refresh=force_refresh)
        items = payload.get('items') or []
        capital = self.get_phone_capital_outflow(force_refresh=force_refresh)

        total_paid_income = 0.0
        total_owing_income = 0.0
        total_expenses = 0.0
        total_phone_realized_profit = 0.0
        total_service_realized_profit = 0.0

        current_day = datetime.now(timezone.utc).date()
        current_month_start = current_day.replace(day=1)
        current_week_start = current_day - timedelta(days=(current_day.weekday() + 1) % 7)
        current_week_end_date = current_day
        current_week_paid_income = 0.0
        current_week_expenses = 0.0
        current_week_phone_profit = 0.0
        current_week_service_profit = 0.0

        for item in items:
            amount = max(0.0, clean_amount(item.get('amount')))
            source = str(item.get('source') or '').strip().lower()
            category = str(item.get('category') or '').strip().lower()
            entry_type = str(item.get('type') or '').strip().lower()
            cost_price = max(0.0, clean_amount(item.get('cost_price')))
            payment_status = str(item.get('payment_status') or '').strip().upper()
            try:
                entry_date = parse_sheet_date(item.get('payment_date') or item.get('date'))
            except Exception:
                entry_date = None

            is_income = source == 'income'
            is_paid = payment_status != 'OWING'  # missing/PAID both count as paid (backward compat)
            in_current_month = entry_date is not None and entry_date >= current_month_start and entry_date <= current_day
            has_phone_cost = cost_price > 0
            allow_phone_profit = entry_type != 'phone' or has_phone_cost

            if in_current_month:
                if is_income:
                    if is_paid:
                        if allow_phone_profit:
                            total_paid_income += amount
                            if entry_type == 'service':
                                realized_profit = amount
                                total_service_realized_profit += realized_profit
                            else:
                                # Phone rows in cashflow store realized profit in amount.
                                realized_profit = amount
                                total_phone_realized_profit += realized_profit
                    else:
                        if allow_phone_profit:
                            total_owing_income += amount
                else:
                    total_expenses += amount

            if entry_date is not None and current_week_start <= entry_date <= current_week_end_date:
                if is_income and is_paid:
                    current_week_paid_income += amount
                    if entry_type == 'service' or ('service' in category and entry_type not in ('phone', 'service')):
                        current_week_service_profit += amount
                    else:
                        current_week_phone_profit += amount
                elif not is_income:
                    current_week_expenses += amount

        total_cash_in = round(total_paid_income, 2)
        expected_income = round(total_owing_income, 2)
        total_expenses = round(total_expenses, 2)
        total_realized_profit = round(total_phone_realized_profit + total_service_realized_profit, 2)
        net_profit = round(total_realized_profit - total_expenses, 2)

        receivables_excluded = max(0.0, self._read_numeric_config('receivables_amount', default=0.0))
        reserve_percentage = self._normalized_reserve_percentage()
        available_cash_before_reserve = total_cash_in - total_expenses - receivables_excluded
        reserve_amount = max(0.0, available_cash_before_reserve) * reserve_percentage
        available_cash_after_reserve = available_cash_before_reserve - reserve_amount

        weekly_realized_profit = round(current_week_phone_profit + current_week_service_profit, 2)
        current_week_net_cash_flow = round(current_week_paid_income - current_week_expenses, 2)
        allowance_percentage = 0.25
        raw_allowance = max(0.0, current_week_net_cash_flow) * allowance_percentage
        # Allowance must not exceed actual available cash after reserve.
        suggested_allowance = round(min(raw_allowance, max(0.0, available_cash_after_reserve)), 2)

        return {
            'total_cash_in': total_cash_in,
            'expected_income': expected_income,
            'total_expenses': total_expenses,
            'database_expenses_total': 0.0,
            'sheet_expenses_total': total_expenses,
            'expense_source': 'sheet',
            'total_cost': 0.0,
            'net_profit': net_profit,
            'receivables_excluded': receivables_excluded,
            'reserve_percentage': reserve_percentage,
            'reserve_amount': reserve_amount,
            'available_cash': available_cash_after_reserve,
            'available_cash_before_reserve': available_cash_before_reserve,
            'current_week_cash_in': round(current_week_paid_income, 2),
            'current_week_expenses': round(current_week_expenses, 2),
            'current_week_phone_profit': round(current_week_phone_profit, 2),
            'current_week_service_profit': round(current_week_service_profit, 2),
            'weekly_realized_profit': weekly_realized_profit,
            'current_week_net_cash_flow': current_week_net_cash_flow,
            'capital_outflow_month': capital.get('month_total', 0.0),
            'capital_outflow_week': capital.get('week_total', 0.0),
            'current_week_start': current_week_start.isoformat(),
            'current_week_end': current_week_end_date.isoformat(),
            'weekly_allowance': {
                'suggested_allowance': suggested_allowance,
                'calculation_date': current_day.isoformat(),
                'previous_week_profit': weekly_realized_profit,
                'allowance_percentage': allowance_percentage,
            },
            'expense_sheet_title': payload.get('sheet_title', 'CASH FLOW'),
        }

    def get_weekly_allowance_from_sheet(self, force_refresh=False):
        summary = self.get_cashflow_summary_from_sheet(force_refresh=force_refresh)
        return summary.get('weekly_allowance') or {
            'suggested_allowance': 0.0,
            'calculation_date': self._most_recent_saturday().isoformat(),
            'previous_week_profit': 0.0,
        }

    @staticmethod
    def _normalize_cashflow_lookup_text(value):
        text = str(value or '').upper().strip()
        if not text:
            return ''
        for token in ('IOS UPDATE', 'UPDATE', 'FIX'):
            text = text.replace(token, ' ')
        return ' '.join(text.split())

    def rebuild_cashflow_sheet(self, force_refresh=False, current_week_only=False):
        today = datetime.now(timezone.utc).date()
        week_start = today - timedelta(days=today.weekday())

        main_records = self.get_main_records(force_refresh=force_refresh)
        stock_values = self.get_stock_values(force_refresh=force_refresh)

        stock_cost_by_desc = {}
        stock_cost_by_imei = {}
        stock_header_row_idx, _, stock_headers_upper = detect_stock_headers(stock_values)
        stock_desc_col = svc_stock_header_index(stock_headers_upper, 'DESCRIPTION', 'MODEL', 'DEVICE', 'DESC')
        stock_imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')
        stock_cost_col = svc_stock_header_index(stock_headers_upper, 'COST PRICE', 'COST')
        for row_values in stock_values[stock_header_row_idx + 1:]:
            row = list(row_values or [])
            description = self._normalize_cashflow_lookup_text(row[stock_desc_col] if stock_desc_col is not None and stock_desc_col < len(row) else '')
            imei_text = str(row[stock_imei_col] if stock_imei_col is not None and stock_imei_col < len(row) else '').strip()
            cost_price = clean_amount(row[stock_cost_col] if stock_cost_col is not None and stock_cost_col < len(row) else '')
            if not cost_price:
                continue
            if imei_text and imei_text not in stock_cost_by_imei:
                stock_cost_by_imei[imei_text] = cost_price
            if description and description not in stock_cost_by_desc:
                stock_cost_by_desc[description] = cost_price

        expense_rows = []
        existing_expenses = self.get_cashflow_expense_records(force_refresh=force_refresh).get('items', [])
        for expense in existing_expenses:
            expense_date = parse_sheet_date(expense.get('date'))
            if expense_date is None:
                continue
            if current_week_only and (expense_date < week_start or expense_date > today):
                continue
            expense_rows.append({
                'date': expense_date.isoformat(),
                'category': str(expense.get('category') or 'EXPENSE').strip() or 'EXPENSE',
                'amount': round(max(0.0, clean_amount(expense.get('amount'))), 2),
                'description': str(expense.get('description') or '').strip(),
                'created_by': str(expense.get('created_by') or '').strip(),
                'source': 'expense',
                'type': 'expense',
                'payment_status': '',
                'cost_price': '',
                'payment_date': '',
            })

        income_rows = []
        phone_profit_total = 0.0
        service_profit_total = 0.0

        for record in main_records:
            status = str(self._record_value(record, 'STATUS') or '').strip().upper()
            if status != 'PAID':
                continue

            payment_date_raw = self._record_value(record, 'PAYMENT DATE', 'PAID DATE', 'DATE')
            payment_date = parse_sheet_date(payment_date_raw)
            if payment_date is None:
                continue
            if current_week_only and (payment_date < week_start or payment_date > today):
                continue

            price = clean_amount(self._record_value(record, 'PRICE', 'AMOUNT SOLD', 'SELLING PRICE'))
            if price <= 0:
                continue

            description = str(self._record_value(record, 'DESCRIPTION', 'MODEL', 'DEVICE') or '').strip()
            normalized_description = self._normalize_cashflow_lookup_text(description)
            imei = str(self._record_value(record, 'IMEI') or '').strip()
            actor = str(self._record_value(record, 'NAME', 'NAME OF BUYER', 'CLIENT NAME') or '').strip()

            if imei:
                cost_price = clean_amount(self._record_value(record, 'COST PRICE', 'COST'))
                if cost_price <= 0:
                    cost_price = stock_cost_by_imei.get(imei, 0)
                if cost_price <= 0:
                    cost_price = stock_cost_by_desc.get(normalized_description, 0)
                if cost_price <= 0:
                    continue
                profit = round(max(0.0, price - cost_price), 2)
                if profit <= 0:
                    continue
                phone_profit_total += profit
                income_rows.append({
                    'date': payment_date.isoformat(),
                    'category': 'PHONE PROFIT',
                    'amount': profit,
                    'description': description,
                    'created_by': actor,
                    'source': 'income',
                    'payment_status': 'PAID',
                    'type': 'phone',
                    'cost_price': round(cost_price, 2),
                    'payment_date': payment_date.isoformat(),
                })
                continue

            # Non-IMEI rows are treated as service transactions.
            service_profit = round(max(0.0, price), 2)
            if service_profit <= 0:
                continue
            service_profit_total += service_profit
            income_rows.append({
                'date': payment_date.isoformat(),
                'category': 'SERVICE PROFIT',
                'amount': service_profit,
                'description': description,
                'created_by': actor,
                'source': 'income',
                'payment_status': 'PAID',
                'type': 'service',
                'cost_price': '',
                'payment_date': payment_date.isoformat(),
            })

            # Rebuild service-related expense rows from the main sheet so
            # expense totals remain accurate even after full rebuilds.
            service_expense = clean_amount(
                self._record_value(record, 'SERVICE EXPENSE', 'EXPENSE', 'SERVICE COST')
            )
            if service_expense > 0:
                expense_rows.append({
                    'date': payment_date.isoformat(),
                    'category': 'SERVICE EXPENSE',
                    'amount': round(service_expense, 2),
                    'description': description,
                    'created_by': actor,
                    'source': 'expense',
                    'type': 'expense',
                    'payment_status': '',
                    'cost_price': '',
                    'payment_date': '',
                })

        sheet_rows = sorted(expense_rows + income_rows, key=lambda row: (row.get('date') or '', row.get('category') or '', row.get('description') or ''))
        sheet_values = [[
            'DATE',
            'CATEGORY',
            'AMOUNT',
            'DESCRIPTION',
            'CREATED BY',
            'SOURCE',
            'PAYMENT_STATUS',
            'TYPE',
            'COST_PRICE',
            'PAYMENT_DATE',
        ]]
        for row in sheet_rows:
            source = row.get('source', 'expense')
            payment_status = row.get('payment_status', 'PAID') if source == 'income' else ''
            entry_type = row.get('type', 'service' if source == 'income' else 'expense')
            row_cost_price = row.get('cost_price', '')
            payment_date = row.get('payment_date', row.get('date', '') if payment_status == 'PAID' else '')
            sheet_values.append([
                row.get('date', ''),
                row.get('category', ''),
                str(row.get('amount', 0)),
                row.get('description', ''),
                row.get('created_by', ''),
                source,
                payment_status,
                entry_type,
                str(row_cost_price or ''),
                payment_date,
            ])

        worksheet = self._resolve_cashflow_expense_worksheet(create_if_missing=True)
        with self._sheet_lock:
            worksheet.clear()
            if len(sheet_values) == 1:
                worksheet.update('A1:J1', sheet_values)
            else:
                end_letter = column_index_to_letter(9)
                worksheet.update(f'A1:{end_letter}{len(sheet_values)}', sheet_values, value_input_option='USER_ENTERED')
            if self.postgres_ready:
                try:
                    self.postgres_sync_manager.upsert_sheet_cache('cashflow_expense_values', worksheet.get_all_values())
                except Exception as exc:
                    self.logger.warning('Failed to refresh cashflow cache after rebuild: %s', exc)

        period_expenses = round(sum(row.get('amount', 0) for row in expense_rows), 2)
        period_profit = round(phone_profit_total + service_profit_total - period_expenses, 2)

        return {
            'mode': 'current_week' if current_week_only else 'full',
            'week_start': week_start.isoformat(),
            'week_end': today.isoformat(),
            'phone_profit_total': round(phone_profit_total, 2),
            'service_profit_total': round(service_profit_total, 2),
            'expense_total': period_expenses,
            'net_profit': period_profit,
            'rows_written': max(0, len(sheet_values) - 1),
            'source': 'sheet',
            'sheet_title': 'CASH FLOW',
        }

    def rebuild_cashflow_sheet_for_current_week(self, force_refresh=False):
        return self.rebuild_cashflow_sheet(force_refresh=force_refresh, current_week_only=True)

    @staticmethod
    def _build_sheet_row_values(headers, values_by_header):
        normalized = {
            str(key or '').strip().upper(): '' if value is None else str(value)
            for key, value in (values_by_header or {}).items()
        }
        row_values = []
        for header in headers or []:
            row_values.append(normalized.get(str(header or '').strip().upper(), ''))
        return row_values

    @staticmethod
    def _build_sheet_record(headers, row_values):
        return {
            str(header): row_values[index] if index < len(row_values) else ''
            for index, header in enumerate(headers or [])
        }

    def _update_cached_stock_status(self, row_num, status_key):
        if not self.postgres_ready:
            return

        payload = self.postgres_sync_manager.load_cache_payload('stock_color_status_map')
        status_map = dict(payload or {})
        status_map[int(row_num)] = status_key
        self.postgres_sync_manager.upsert_cache_payload('stock_color_status_map', status_map)

    def _process_client_sheet_sync(self, force_refresh=False, include_autofill=False):
        result = {
            'directory_result': self.sync_client_directory_sheet(force_reload=True),
            'phone_update_result': self.sync_clients_to_sheet_phone_column(force_refresh=force_refresh),
            'validation_result': self.apply_sheet_name_validation(),
        }

        if include_autofill:
            result['autofill_result'] = self.apply_sheet_phone_autofill_formulas(force_refresh=force_refresh)

        return result

    def _queue_client_sheet_sync(self, force_refresh=False, include_autofill=False):
        if self.postgres_ready:
            queue_id = self._enqueue_db_first_operation(
                'clients',
                'clients_sync_sheet',
                {
                    'kind': 'clients_sync_sheet',
                    'force_refresh': bool(force_refresh),
                    'include_autofill': bool(include_autofill),
                },
            )
            return {
                'mode': 'queued',
                'queued_operation_id': queue_id,
                'include_autofill': bool(include_autofill),
            }

        return self._process_client_sheet_sync(force_refresh=force_refresh, include_autofill=include_autofill)

    def _find_logo_paths(self):
        base_candidates = [
            self.base_dir,
            os.path.expanduser('~/Downloads'),
            os.path.expanduser('~/Pictures'),
            os.path.expanduser('~/Desktop'),
        ]
        name_prefixes = [
            'WhatsApp Image 2026-04-04 at 16.29.12 (1)',
            'WhatsApp Image 2026-04-04 at 16.29.12',
        ]
        extensions = ('.png', '.jpg', '.jpeg', '.gif', '.webp')

        found = []
        for folder in base_candidates:
            if not os.path.isdir(folder):
                continue
            try:
                names = os.listdir(folder)
            except Exception:
                continue

            for prefix in name_prefixes:
                for name in names:
                    if not name.startswith(prefix):
                        continue
                    if not name.lower().endswith(extensions):
                        continue
                    full_path = os.path.join(folder, name)
                    if full_path not in found:
                        found.append(full_path)
            if found:
                break

        return found[:1]

    def get_logo_payload(self):
        if self._logo_payload is not None:
            return dict(self._logo_payload)

        env_logo_data_url = (
            os.environ.get('LOGO_DATA_URL')
            or os.environ.get('DASHBOARD_LOGO_DATA_URL')
            or os.environ.get('LOGO_URL')
            or os.environ.get('DASHBOARD_LOGO_URL')
            or ''
        ).strip()
        if env_logo_data_url:
            normalized_logo_source = env_logo_data_url.replace('\\n', '').strip()
            file_name = ''
            if '/' in normalized_logo_source:
                file_name = normalized_logo_source.split('?', 1)[0].rstrip('/').rsplit('/', 1)[-1]
            self._logo_payload = {
                'data_url': normalized_logo_source,
                'file_name': file_name,
            }
            return dict(self._logo_payload)

        env_logo_base64 = (
            os.environ.get('LOGO_BASE64')
            or os.environ.get('DASHBOARD_LOGO_BASE64')
            or ''
        ).strip()
        if env_logo_base64:
            encoded = env_logo_base64
            if encoded.startswith('data:') and ';base64,' in encoded:
                encoded = encoded.split(';base64,', 1)[1]
            mime_type = (os.environ.get('LOGO_MIME') or os.environ.get('DASHBOARD_LOGO_MIME') or 'image/png').strip()
            self._logo_payload = {
                'data_url': f'data:{mime_type};base64,{encoded}',
                'file_name': 'logo',
            }
            return dict(self._logo_payload)

        for path in self._find_logo_paths():
            try:
                with open(path, 'rb') as logo_file:
                    encoded = base64.b64encode(logo_file.read()).decode('ascii')
                mime_type = mimetypes.guess_type(path)[0] or 'image/jpeg'
                self._logo_payload = {
                    'data_url': f'data:{mime_type};base64,{encoded}',
                    'file_name': os.path.basename(path),
                }
                return dict(self._logo_payload)
            except Exception:
                continue

        self._logo_payload = {
            'data_url': '',
            'file_name': '',
        }
        return dict(self._logo_payload)

    def _ensure_stock_cost_price_column(self, force_refresh=False):
        values = self.get_stock_values(force_refresh=force_refresh)
        if not values:
            header_row_idx, headers, headers_upper = detect_stock_headers(values)
            return values, header_row_idx, headers, headers_upper, False
        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        if 'COST PRICE' in headers_upper:
            return values, header_row_idx, headers, headers_upper, False

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return values, header_row_idx, headers, headers_upper, False

        try:
            worksheet = self._resolve_stock_worksheet(stock_sheet_id)
        except Exception as exc:
            self.logger.warning('Could not resolve stock worksheet while ensuring COST PRICE column: %s', exc)
            return values, header_row_idx, headers, headers_upper, False
        insert_after_idx = svc_stock_header_index(headers_upper, 'IMEI')
        if insert_after_idx is None:
            insert_after_idx = svc_stock_header_index(headers_upper, 'STORAGE')
        if insert_after_idx is None:
            insert_after_idx = len(headers) - 1 if headers else 0

        insert_index = max(0, insert_after_idx + 1)
        request_body = {
            'requests': [{
                'insertDimension': {
                    'range': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': insert_index,
                        'endIndex': insert_index + 1,
                    },
                    'inheritFromBefore': insert_index > 0,
                }
            }]
        }

        try:
            with self._sheet_lock:
                self.sheets_api_service.spreadsheets().batchUpdate(
                    spreadsheetId=stock_sheet_id,
                    body=request_body,
                ).execute()
                worksheet.update_cell(header_row_idx + 1, insert_index + 1, 'COST PRICE')
                values = worksheet.get_all_values()
        except Exception as exc:
            self.logger.warning('Could not insert COST PRICE column due to sheet API error: %s', exc)
            return values, header_row_idx, headers, headers_upper, False

        if self.postgres_ready:
            try:
                self.postgres_sync_manager.upsert_sheet_cache('stock_values', values)
            except Exception as exc:
                self.logger.warning('Failed to cache stock sheet after COST PRICE insert: %s', exc)

        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        return values, header_row_idx, headers, headers_upper, True

    def _ensure_stock_product_status_column(self, force_refresh=False):
        values = self.get_stock_values(force_refresh=force_refresh)
        if not values:
            header_row_idx, headers, headers_upper = detect_stock_headers(values)
            return values, header_row_idx, headers, headers_upper, False
        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        if 'PRODUCT STATUS' in headers_upper or 'STATUS OF DEVICE' in headers_upper:
            return values, header_row_idx, headers, headers_upper, False

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return values, header_row_idx, headers, headers_upper, False

        try:
            worksheet = self._resolve_stock_worksheet(stock_sheet_id)
        except Exception as exc:
            self.logger.warning('Could not resolve stock worksheet while ensuring PRODUCT STATUS column: %s', exc)
            return values, header_row_idx, headers, headers_upper, False
        time_col = svc_stock_header_index(headers_upper, 'TIME')
        desc_col = svc_stock_header_index(headers_upper, 'DESCRIPTION', 'DESC', 'DETAILS', 'MODEL', 'PHONE MODEL')
        date_col = svc_stock_header_index(headers_upper, 'DATE')

        if time_col is not None:
            insert_index = time_col + 1
        elif desc_col is not None:
            insert_index = desc_col
        elif date_col is not None:
            insert_index = date_col + 1
        else:
            insert_index = 0

        request_body = {
            'requests': [{
                'insertDimension': {
                    'range': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': insert_index,
                        'endIndex': insert_index + 1,
                    },
                    'inheritFromBefore': insert_index > 0,
                }
            }]
        }

        try:
            with self._sheet_lock:
                self.sheets_api_service.spreadsheets().batchUpdate(
                    spreadsheetId=stock_sheet_id,
                    body=request_body,
                ).execute()
                worksheet.update_cell(header_row_idx + 1, insert_index + 1, 'PRODUCT STATUS')
                values = worksheet.get_all_values()
        except Exception as exc:
            self.logger.warning('Could not insert PRODUCT STATUS column due to sheet API error: %s', exc)
            return values, header_row_idx, headers, headers_upper, False

        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        status_col = svc_stock_header_index(headers_upper, 'PRODUCT STATUS', 'STATUS OF DEVICE', 'STOCK STATUS', 'ITEM STATUS')
        qty_col = svc_stock_header_index(headers_upper, 'QTY', 'QUANTITY', 'STOCK', 'UNITS')
        desc_col = svc_stock_header_index(headers_upper, 'DESCRIPTION', 'DESC', 'DETAILS', 'MODEL', 'PHONE MODEL')

        fallback_color_map = {}
        if desc_col is not None:
            try:
                fallback_color_map = svc_get_stock_color_status_map(
                    self.sheets_api_service,
                    stock_sheet_id,
                    worksheet.title,
                    desc_col,
                    len(values),
                )
            except Exception as exc:
                self.logger.warning('Could not load fallback stock color map while seeding PRODUCT STATUS: %s', exc)

        updates = []
        data_start = header_row_idx + 2
        if status_col is not None:
            for row_num in range(data_start, len(values) + 1):
                row = values[row_num - 1] if row_num - 1 < len(values) else []
                current_value = str(row[status_col]).strip() if status_col < len(row) else ''
                if normalize_stock_status_value(current_value):
                    continue

                status_key = ''
                if row_num in fallback_color_map:
                    status_key = str(fallback_color_map.get(row_num) or '').strip().lower()

                if status_key not in {'available', 'pending', 'needs_details', 'sold'}:
                    status_key = 'available'

                if qty_col is not None and qty_col < len(row):
                    try:
                        if int(str(row[qty_col]).strip() or '0') <= 0:
                            status_key = 'sold'
                    except Exception:
                        pass

                updates.append({
                    'range': f'{column_index_to_letter(status_col)}{row_num}',
                    'values': [[stock_status_key_to_label(status_key)]],
                })

        if updates:
            with self._sheet_lock:
                worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                values = worksheet.get_all_values()

        if self.postgres_ready:
            try:
                self.postgres_sync_manager.upsert_sheet_cache('stock_values', values)
            except Exception as exc:
                self.logger.warning('Failed to cache stock sheet after PRODUCT STATUS insert: %s', exc)

        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        return values, header_row_idx, headers, headers_upper, True

    def _ensure_stock_required_columns(self, force_refresh=False):
        values, header_row_idx, headers, headers_upper, inserted_cost_price = self._ensure_stock_cost_price_column(force_refresh=force_refresh)
        values, header_row_idx, headers, headers_upper, inserted_product_status = self._ensure_stock_product_status_column(force_refresh=False)
        return values, header_row_idx, headers, headers_upper, inserted_cost_price, inserted_product_status

    @staticmethod
    def _inventory_status_to_stock_fields(inventory_status, fallback_date=''):
        status_text = str(inventory_status or '').strip().upper()
        fallback_date = str(fallback_date or '').strip()
        if status_text == 'PAID':
            return 'SOLD', (fallback_date or datetime.now().strftime('%m/%d/%Y'))
        if status_text in {'UNPAID', 'PART PAYMENT'}:
            return 'PENDING DEAL', 'PENDING DEAL'
        if 'RETURN' in status_text:
            return 'AVAILABLE', ''
        return '', ''

    def _build_inventory_latest_status_lookup(self, force_refresh=False):
        main_values = self.get_main_values(force_refresh=force_refresh)
        if not main_values:
            return {}

        header_row_idx = detect_sheet_header_row(main_values)
        headers = [str(cell or '').strip() for cell in (main_values[header_row_idx] if header_row_idx < len(main_values) else [])]
        headers_upper = [header.upper() for header in headers]

        name_col = svc_stock_header_index(headers_upper, 'NAME', 'CLIENT NAME', 'CUSTOMER NAME')
        imei_col = svc_stock_header_index(headers_upper, 'IMEI')
        status_col = svc_stock_header_index(headers_upper, 'STATUS')
        date_col = svc_stock_header_index(headers_upper, 'DATE')
        paid_col = svc_stock_header_index(headers_upper, 'AMOUNT PAID')
        if status_col is None:
            return {}

        latest_by_pair = {}
        latest_by_imei = {}
        for row_idx in range(header_row_idx + 1, len(main_values)):
            row = main_values[row_idx] if row_idx < len(main_values) else []
            status_value = str(row[status_col] if status_col < len(row) else '').strip().upper()
            if not status_value:
                continue

            imei_value = str(row[imei_col] if imei_col is not None and imei_col < len(row) else '').strip().upper()
            name_value = str(row[name_col] if name_col is not None and name_col < len(row) else '').strip().upper()
            date_value = str(row[date_col] if date_col is not None and date_col < len(row) else '').strip()
            paid_value = str(row[paid_col] if paid_col is not None and paid_col < len(row) else '').strip()
            entry = {
                'status': status_value,
                'date': date_value,
                'amount_paid': paid_value,
                'row_num': row_idx + 1,
            }
            if imei_value and name_value:
                latest_by_pair[(imei_value, name_value)] = entry
            if imei_value:
                latest_by_imei[imei_value] = entry

        return {
            'by_pair': latest_by_pair,
            'by_imei': latest_by_imei,
        }

    def _queue_stock_reconcile_updates(self, updates):
        if not updates:
            return []

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return []

        queue_ids = []
        for update in updates[:120]:
            row = int(update.get('row') or 0)
            col = int(update.get('col') or 0)
            value = update.get('value', '')
            if row <= 0 or col <= 0:
                continue
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'stock_reconcile',
                    'stock_update_cell',
                    {
                        'kind': 'stock_update_cell',
                        'stock_sheet_id': stock_sheet_id,
                        'row': row,
                        'col': col,
                        'value': value,
                    },
                    cache_apply_callable=lambda rn=row, cn=col, nv=value: self.postgres_sync_manager.update_cached_stock_value(rn, cn, nv),
                )
            )
        return queue_ids

    def _apply_inventory_status_to_stock_values(self, values, header_row_idx, headers, headers_upper, inventory_lookup):
        if not values or not inventory_lookup:
            return values, []

        status_col = svc_stock_header_index(headers_upper, 'PRODUCT STATUS', 'STATUS OF DEVICE', 'STOCK STATUS', 'ITEM STATUS')
        availability_col = svc_stock_header_index(headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
        imei_col = svc_stock_header_index(headers_upper, 'IMEI')
        buyer_col = svc_stock_header_index(headers_upper, 'NAME OF BUYER')
        if status_col is None or imei_col is None:
            return values, []

        by_pair = inventory_lookup.get('by_pair') or {}
        by_imei = inventory_lookup.get('by_imei') or {}
        queued_updates = []
        data_start = header_row_idx + 1

        for row_index in range(data_start, len(values)):
            row = list(values[row_index]) if row_index < len(values) else []
            if imei_col >= len(row):
                continue

            imei_value = str(row[imei_col] or '').strip().upper()
            buyer_value = str(row[buyer_col] or '').strip().upper() if buyer_col is not None and buyer_col < len(row) else ''
            if not imei_value:
                continue

            matched_by_pair = False
            inv_entry = by_pair.get((imei_value, buyer_value)) if buyer_value else None
            if inv_entry is not None:
                matched_by_pair = True

            # When a buyer is already set on stock, avoid IMEI-only fallback because
            # it can match an unrelated inventory row and incorrectly revert status.
            if inv_entry is None and not buyer_value:
                inv_entry = by_imei.get(imei_value)
            if inv_entry is None:
                continue

            desired_status_label, desired_availability = self._inventory_status_to_stock_fields(inv_entry.get('status'), inv_entry.get('date'))
            if not desired_status_label:
                continue

            while len(row) < len(headers):
                row.append('')

            current_status = str(row[status_col] or '').strip().upper()
            desired_status = desired_status_label.upper()
            current_status_key = normalize_stock_status_value(current_status)
            desired_status_key = normalize_stock_status_value(desired_status)

            # Never downgrade manually set SOLD/PENDING rows from loose matches.
            if current_status_key == 'sold' and desired_status_key in {'available', 'pending'}:
                continue
            if current_status_key == 'pending' and desired_status_key == 'available':
                continue
            if not matched_by_pair and desired_status_key == 'available':
                continue

            row_changed = False
            if current_status != desired_status:
                row[status_col] = desired_status_label
                queued_updates.append({'row': row_index + 1, 'col': status_col + 1, 'value': desired_status_label})
                row_changed = True

            if availability_col is not None and availability_col < len(row):
                current_availability = str(row[availability_col] or '').strip()
                if current_availability != str(desired_availability):
                    row[availability_col] = desired_availability
                    queued_updates.append({'row': row_index + 1, 'col': availability_col + 1, 'value': desired_availability})
                    row_changed = True

            if row_changed:
                values[row_index] = row

        return values, queued_updates

    def get_stock_view_payload(self, filter_text='', filter_mode='all', force_refresh=False):
        values, header_row_idx, headers, headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)
        inventory_lookup = self._build_inventory_latest_status_lookup(force_refresh=force_refresh)
        values, reconcile_updates = self._apply_inventory_status_to_stock_values(
            values,
            header_row_idx,
            headers,
            headers_upper,
            inventory_lookup,
        )

        if force_refresh and reconcile_updates:
            self._queue_stock_reconcile_updates(reconcile_updates)
            try:
                self.replay_pending_queue_now(limit=200)
            except Exception:
                pass

        stock_view = build_stock_view(
            values,
            headers,
            headers_upper,
            header_row_idx,
            color_status_map=self.get_stock_color_status_map(force_refresh=force_refresh),
            filter_text=filter_text,
            filter_mode=filter_mode,
        )

        imei_col = svc_stock_header_index(headers_upper, 'IMEI')
        buyer_col = svc_stock_header_index(headers_upper, 'NAME OF BUYER')
        by_pair = inventory_lookup.get('by_pair') or {}
        by_imei = inventory_lookup.get('by_imei') or {}
        for row in (stock_view.get('all_rows_cache') or []):
            padded = row.get('padded') or []
            imei_value = str(padded[imei_col] if imei_col is not None and imei_col < len(padded) else '').strip().upper()
            buyer_value = str(padded[buyer_col] if buyer_col is not None and buyer_col < len(padded) else '').strip().upper()
            inv_entry = by_pair.get((imei_value, buyer_value)) if imei_value and buyer_value else None
            if inv_entry is None and imei_value:
                inv_entry = by_imei.get(imei_value)
            if inv_entry:
                row['inventory_status'] = str(inv_entry.get('status') or '').strip().upper()
                row['inventory_amount_paid'] = str(inv_entry.get('amount_paid') or '').strip()
                row['inventory_row_num'] = inv_entry.get('row_num')

        stock_view['headers'] = headers
        stock_view['headers_upper'] = headers_upper
        stock_view['header_row_idx'] = header_row_idx
        return stock_view

    def _load_cached_rows(self, sheet_key):
        if not self.postgres_ready:
            return []
        try:
            rows = self.postgres_sync_manager.load_cached_rows(sheet_key)
        except Exception as exc:
            self.logger.warning('Failed to load cached rows for %s: %s', sheet_key, exc)
            return []
        return rows if isinstance(rows, list) else []

    def _load_cached_payload(self, sheet_key):
        if not self.postgres_ready:
            return None
        try:
            return self.postgres_sync_manager.load_cache_payload(sheet_key)
        except Exception as exc:
            self.logger.warning('Failed to load cached payload for %s: %s', sheet_key, exc)
            return None

    def _get_main_sheet_columns(self, force_refresh=False):
        values = self.get_main_values(force_refresh=force_refresh)
        if not values:
            return [], {}

        header_lookup = {str(col).strip().upper(): idx for idx, col in enumerate(values[0])}

        def pick(*candidates):
            for candidate in candidates:
                idx = header_lookup.get(str(candidate).upper())
                if idx is not None:
                    return idx
            return None

        return values, {
            'name_col': pick('NAME', 'CLIENT NAME', 'CUSTOMER NAME'),
            'phone_col': pick('PHONE NUMBER', 'PHONE', 'WHATSAPP NUMBER', 'WHATSAPP', 'NUMBER'),
            'status_col': pick('STATUS'),
            'price_col': pick('PRICE'),
            'paid_col': pick('AMOUNT PAID'),
        }

    def pull_once(self):
        if not self.postgres_ready:
            raise RuntimeError('PostgreSQL sync manager is not ready')
        if not self._ensure_sheet_connection():
            raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')

        stock_sheet_id = self._resolve_stock_sheet_id()
        with self._sheet_lock:
            if self.config.get('record_id_rollout', True) and not self._rollout_done:
                try:
                    rollout_record_ids_for_known_sheets(self.main_spreadsheet, self.gspread_client, stock_sheet_id)
                    self._rollout_done = True
                except Exception as exc:
                    self.logger.warning('RECORD_ID rollout failed in backend pull cycle: %s', exc)

            main_values = self.main_sheet.get_all_values()
            main_records = self.main_sheet.get_all_records()
            self.postgres_sync_manager.upsert_sheet_cache('main_values', main_values)
            self.postgres_sync_manager.upsert_sheet_cache('main_records', main_records)

            cashflow_expense_values = []
            try:
                cashflow_expense_ws = self._resolve_cashflow_expense_worksheet(create_if_missing=False)
                if cashflow_expense_ws is not None:
                    cashflow_expense_values = cashflow_expense_ws.get_all_values()
                    self.postgres_sync_manager.upsert_sheet_cache('cashflow_expense_values', cashflow_expense_values)
            except Exception as exc:
                self.logger.warning('Failed to pull cashflow expense sheet cache: %s', exc)

            stock_values = []
            stock_color_status_map = {}
            if stock_sheet_id:
                stock_ws = self._resolve_stock_worksheet(stock_sheet_id)
                stock_values = stock_ws.get_all_values()
                self.postgres_sync_manager.upsert_sheet_cache('stock_values', stock_values)

                header_row_idx, _, headers_upper = detect_stock_headers(stock_values)
                desc_col = svc_stock_header_index(headers_upper, 'DESCRIPTION', 'DESC', 'DETAILS', 'MODEL', 'PHONE MODEL')
                if desc_col is not None:
                    stock_color_status_map = svc_get_stock_color_status_map(
                        self.sheets_api_service,
                        stock_sheet_id,
                        stock_ws.title,
                        desc_col,
                        len(stock_values),
                    )

            self.postgres_sync_manager.upsert_cache_payload('stock_color_status_map', stock_color_status_map)
            self.postgres_sync_manager.set_meta('sync_runtime', {
                'mode': 'sheet_wins',
                'startup_mode': self.config.get('startup_mode', 'cache_then_sync'),
                'legacy_sheet_fallback': bool(self.config.get('legacy_sheet_fallback', True)),
                'pull_interval_sec': int(self.config.get('sync_pull_interval_sec', 90) or 90),
                'last_pull_utc': datetime.now(timezone.utc).isoformat(),
            })

        return {
            'main_records': len(main_records),
            'main_values': len(main_values),
            'cashflow_expense_values': len(cashflow_expense_values),
            'stock_values': len(stock_values),
            'stock_color_status_map': len(stock_color_status_map),
        }

    def _seed_once_async(self):
        try:
            self.pull_once()
        except Exception as exc:
            self.logger.warning('Initial backend PostgreSQL pull seed failed: %s', exc)

    def _init_postgres_sync(self):
        self.sync_state['enabled'] = bool(self.config.get('enable_postgres_cache', True))
        self.sync_state['ready'] = False
        self.sync_state['last_status'] = 'disabled'
        self.sync_state['last_error'] = ''

        if not self.sync_state['enabled']:
            return

        if create_postgres_sync_manager is None or not PSYCOPG2_AVAILABLE:
            self.sync_state['last_status'] = 'driver_missing'
            self.sync_state['last_error'] = 'psycopg2 not available in active Python environment'
            return

        self.postgres_sync_manager = create_postgres_sync_manager(self.config, logger=self.logger)
        self.financial_data_service.configure(self.postgres_sync_manager)
        if not self.postgres_sync_manager.ready:
            self.sync_state['last_status'] = 'dsn_missing'
            self.sync_state['last_error'] = 'postgres_dsn is empty'
            return

        try:
            self.postgres_sync_manager.ensure_schema()
            self.financial_data_service.ensure_default_app_config()
            self.sync_state['ready'] = True
            self.sync_state['last_status'] = 'running'
            threading.Thread(target=self._seed_once_async, daemon=True).start()
            self.postgres_sync_manager.start_background_pull(self.pull_once)
            self.postgres_sync_manager.start_background_queue_worker(self._replay_queue_operation, interval_sec=1)
        except Exception as exc:
            self.sync_state['last_status'] = 'error'
            self.sync_state['last_error'] = str(exc)
            self.logger.exception('Failed to initialize backend PostgreSQL sync: %s', exc)

    def _replay_queue_operation(self, item):
        payload = item.get('payload_json') or {}
        kind = payload.get('kind', '')

        if not self._ensure_sheet_connection():
            raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')

        with self._sheet_lock:
            if kind == 'main_write_row':
                row = int(payload.get('row', 0))
                row_values = payload.get('row_values') or []
                if row <= 0 or not row_values:
                    raise RuntimeError('Invalid main_write_row payload')
                end_letter = column_index_to_letter(len(row_values) - 1)
                self.main_sheet.update(
                    f'A{row}:{end_letter}{row}',
                    [row_values],
                    value_input_option='USER_ENTERED',
                )
                return

            if kind == 'main_update_cell':
                row = int(payload.get('row', 0))
                col = int(payload.get('col', 0))
                value = payload.get('value', '')
                if row <= 0 or col <= 0:
                    raise RuntimeError('Invalid main_update_cell payload')
                self.main_sheet.update_cell(row, col, value)
                return

            if kind == 'clients_sync_sheet':
                self._process_client_sheet_sync(
                    force_refresh=bool(payload.get('force_refresh', False)),
                    include_autofill=bool(payload.get('include_autofill', False)),
                )
                return

            if kind == 'stock_update_cell':
                stock_sheet_id = payload.get('stock_sheet_id', '')
                row = int(payload.get('row', 0))
                col = int(payload.get('col', 0))
                value = payload.get('value', '')
                if row <= 0 or col <= 0:
                    raise RuntimeError('Invalid stock_update_cell payload')
                self._resolve_stock_worksheet(stock_sheet_id).update_cell(row, col, value)
                return

            if kind == 'stock_batch_update':
                stock_sheet_id = payload.get('stock_sheet_id', '')
                request_body = payload.get('request_body') or {}
                if not stock_sheet_id or not request_body:
                    raise RuntimeError('Invalid stock_batch_update payload')
                self.sheets_api_service.spreadsheets().batchUpdate(
                    spreadsheetId=stock_sheet_id,
                    body=request_body,
                ).execute()
                return

            if kind == 'stock_write_row':
                stock_sheet_id = payload.get('stock_sheet_id', '')
                row = int(payload.get('row', 0))
                row_values = payload.get('row_values') or []
                if row <= 0 or not stock_sheet_id or not row_values:
                    raise RuntimeError('Invalid stock_write_row payload')
                end_letter = column_index_to_letter(len(row_values) - 1)
                self._resolve_stock_worksheet(stock_sheet_id).update(
                    f'A{row}:{end_letter}{row}',
                    [row_values],
                    value_input_option='USER_ENTERED',
                )
                return

            if kind == 'stock_append_row':
                stock_sheet_id = payload.get('stock_sheet_id', '')
                row_values = payload.get('row_values') or []
                if not stock_sheet_id:
                    raise RuntimeError('Invalid stock_append_row payload')
                self._resolve_stock_worksheet(stock_sheet_id).append_row(row_values, value_input_option='USER_ENTERED')
                return

        raise RuntimeError(f'Unsupported queue operation kind: {kind}')

    def _enqueue_db_first_operation(self, entity_name, operation, payload, cache_apply_callable=None):
        if not self.postgres_ready:
            # Fallback path for deployments without PostgreSQL:
            # execute the write directly against Google Sheets so core workflows
            # (sell, return, service add, payment update) still work.
            if not bool(self.config.get('legacy_sheet_fallback', True)):
                raise RuntimeError('PostgreSQL sync is not ready. Configure postgres_dsn or POSTGRES_DSN before DB-first API writes.')

            if not self._ensure_sheet_connection():
                raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')

            self._replay_queue_operation({'payload_json': payload})
            return 'direct-sheet-write'

        queue_id = self.postgres_sync_manager.enqueue_operation(entity_name, operation, payload)
        if queue_id is None:
            raise RuntimeError('Failed to enqueue background sync operation')

        if cache_apply_callable is not None:
            try:
                cache_apply_callable()
            except Exception as exc:
                try:
                    self.postgres_sync_manager.mark_operation_failed(queue_id, f'Cache apply failed: {exc}')
                except Exception:
                    pass
                raise

        # Background queue worker syncs to Google Sheets within ~20 seconds.
        # Cache is already updated above so the UI sees changes immediately.
        return queue_id

    def replay_pending_queue_now(self, limit=200):
        if not self.postgres_ready:
            raise RuntimeError('PostgreSQL sync manager is not ready')

        items = self.postgres_sync_manager.fetch_pending_operations(limit=max(1, int(limit or 200)))
        processed = 0
        failed = 0

        for item in items:
            try:
                self._replay_queue_operation(item)
                self.postgres_sync_manager.mark_operation_done(item['id'])
                processed += 1
            except Exception as exc:
                self.postgres_sync_manager.mark_operation_failed(item['id'], str(exc))
                failed += 1

        remaining = len(self.postgres_sync_manager.fetch_pending_operations(limit=500))
        return {
            'attempted': len(items),
            'processed': processed,
            'failed': failed,
            'remaining_pending': remaining,
        }

    def get_main_records(self, force_refresh=False):
        if not force_refresh:
            cached = self._load_cached_rows('main_records')
            if cached:
                return cached

        if self.postgres_ready:
            try:
                self.pull_once()
                cached = self._load_cached_rows('main_records')
                if cached:
                    return cached
            except Exception as exc:
                self.logger.warning('Backend main_records pull refresh failed: %s', exc)

        if not self._ensure_sheet_connection():
            return []

        with self._sheet_lock:
            records = self.main_sheet.get_all_records()
        if self.postgres_ready:
            try:
                self.postgres_sync_manager.upsert_sheet_cache('main_records', records)
            except Exception as exc:
                self.logger.warning('Failed to upsert main_records fallback cache: %s', exc)
        return records

    def get_main_values(self, force_refresh=False):
        cached = self._load_cached_rows('main_values')
        if not force_refresh:
            if cached:
                return cached

        if self.postgres_ready:
            try:
                self.pull_once()
                cached = self._load_cached_rows('main_values')
                if cached:
                    return cached
            except Exception as exc:
                self.logger.warning('Backend main_values pull refresh failed: %s', exc)

        if not self._ensure_sheet_connection():
            return cached or []

        try:
            with self._sheet_lock:
                values = self.main_sheet.get_all_values()
        except Exception as exc:
            self.logger.warning('Backend main_values direct sheet read failed: %s', exc)
            return cached or []
        if self.postgres_ready:
            try:
                self.postgres_sync_manager.upsert_sheet_cache('main_values', values)
            except Exception as exc:
                self.logger.warning('Failed to upsert main_values fallback cache: %s', exc)
        return values

    def get_stock_values(self, force_refresh=False):
        cached = self._load_cached_rows('stock_values')
        if not force_refresh:
            if cached:
                return cached

        if self.postgres_ready:
            try:
                self.pull_once()
                cached = self._load_cached_rows('stock_values')
                if cached:
                    return cached
            except Exception as exc:
                self.logger.warning('Backend stock_values pull refresh failed: %s', exc)

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return cached or []

        try:
            with self._sheet_lock:
                values = self._resolve_stock_worksheet(stock_sheet_id).get_all_values()
        except Exception as exc:
            self.logger.warning('Backend stock_values direct sheet read failed: %s', exc)
            return cached or []
        if self.postgres_ready:
            try:
                self.postgres_sync_manager.upsert_sheet_cache('stock_values', values)
            except Exception as exc:
                self.logger.warning('Failed to upsert stock_values fallback cache: %s', exc)
        return values

    def get_stock_color_status_map(self, force_refresh=False):
        if not force_refresh:
            cached = self._load_cached_payload('stock_color_status_map')
            if isinstance(cached, dict) and cached:
                return {int(key): value for key, value in cached.items()}

        if self.postgres_ready:
            try:
                self.pull_once()
                cached = self._load_cached_payload('stock_color_status_map')
                if isinstance(cached, dict):
                    return {int(key): value for key, value in cached.items()}
            except Exception as exc:
                self.logger.warning('Backend stock color status refresh failed: %s', exc)

        return {}

    @staticmethod
    def _clone_payment_action(action):
        if not action:
            return None
        return {
            'customer': action.get('customer', ''),
            'rows': [dict(row) for row in (action.get('rows') or [])],
        }

    @staticmethod
    def _filter_google_contacts(contacts, search=''):
        search = str(search or '').strip().lower()
        if not search:
            return list(contacts or [])

        filtered = []
        for contact in contacts or []:
            haystack = ' '.join(
                str(contact.get(field, '') or '')
                for field in ('name', 'phone', 'label')
            ).lower()
            if search in haystack:
                filtered.append(contact)
        return filtered

    def get_google_contacts_payload(self, search='', force_refresh=False):
        with self._google_contacts_lock:
            if force_refresh or not self._google_contacts_cache:
                oauth_file = self._resolve_contacts_oauth_file()
                if not oauth_file:
                    raise FileNotFoundError(
                        'Google Contacts OAuth client JSON is not configured. '
                        'Set contacts_oauth_file in config.json or GOOGLE_CONTACTS_OAUTH_FILE, '
                        'or place your OAuth client JSON in ~/Downloads as credentials1.json.'
                    )
                token_file = os.path.join(self.base_dir, 'google_contacts_token.json')
                contacts = fetch_google_contacts(
                    oauth_file,
                    token_file,
                    ['https://www.googleapis.com/auth/contacts.readonly'],
                )
                self._google_contacts_cache = contacts
                self._google_contacts_synced_at = datetime.now(timezone.utc).isoformat()

            filtered_contacts = self._filter_google_contacts(self._google_contacts_cache, search=search)

        return {
            'contacts': filtered_contacts,
            'count': len(filtered_contacts),
            'total_cached': len(self._google_contacts_cache),
            'synced_at': self._google_contacts_synced_at,
            'search': str(search or '').strip(),
        }

    def get_stock_form_payload(self, force_refresh=False):
        values, header_row_idx, headers, headers_upper, inserted_cost_price, inserted_product_status = self._ensure_stock_required_columns(force_refresh=force_refresh)
        visible_headers = order_stock_form_headers(
            headers,
            {'NAME OF BUYER', 'PHONE NUMBER OF BUYER', 'RECORD_ID', 'COLUMN 17', 'PRODUCT STATUS'},
            (
                'DESCRIPTION', 'MODEL', 'IMEI', 'COST PRICE', 'S/N', 'COLOUR', 'STORAGE',
                'NAME OF SELLER', 'PHONE NUMBER OF SELLER', 'STATUS OF DEVICE', 'DATE BOUGHT',
            ),
        )
        return {
            'headers': headers,
            'headers_upper': headers_upper,
            'header_row_idx': header_row_idx,
            'visible_headers': visible_headers,
            'defaults': build_stock_form_defaults(values, header_row_idx, headers_upper),
            'dropdown_options': self.get_stock_dropdown_options(force_refresh),
            'cost_price_inserted': inserted_cost_price,
            'product_status_inserted': inserted_product_status,
        }

    def get_stock_dropdown_options(self, force_refresh=False):
        stock_values, _, stock_headers, stock_headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)

        imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')
        device_col = svc_stock_header_index(stock_headers_upper, 'DEVICE')
        storage_col = svc_stock_header_index(stock_headers_upper, 'STORAGE')

        imei_set = set()
        device_set = set()
        storage_set = set()

        for row in stock_values:
            if imei_col is not None and imei_col < len(row):
                imei = str(row[imei_col] or '').strip()
                if imei:
                    imei_set.add(imei)
            if device_col is not None and device_col < len(row):
                device = str(row[device_col] or '').strip()
                if device:
                    device_set.add(device)
            if storage_col is not None and storage_col < len(row):
                storage = str(row[storage_col] or '').strip()
                if storage:
                    storage_set.add(storage)

        return {
            'imei': sorted(list(imei_set)),
            'device': sorted(list(device_set)),
            'storage': sorted(list(storage_set)),
        }

    def add_stock_record(self, values_by_header, force_refresh=False):
        values, header_row_idx, headers, headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)
        normalized_values = dict(values_by_header or {})
        status_col = svc_stock_header_index(headers_upper, 'PRODUCT STATUS', 'STATUS OF DEVICE', 'STOCK STATUS', 'ITEM STATUS')
        if status_col is not None:
            status_header = headers[status_col]
            if not str(normalized_values.get(status_header, '')).strip():
                normalized_values[status_header] = 'AVAILABLE'

        row_values, non_empty_count = build_stock_row_values(headers, normalized_values)
        validation_error = validate_stock_row(row_values, headers_upper)
        if validation_error:
            return {'error': validation_error}

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return {'error': 'Stock sheet ID is missing.'}

        target_row = find_next_table_write_row(values, header_row_idx)

        queue_id = self._enqueue_db_first_operation(
            'stock',
            'stock_write_row',
            {
                'kind': 'stock_write_row',
                'stock_sheet_id': stock_sheet_id,
                'row': target_row,
                'row_values': row_values,
            },
            cache_apply_callable=lambda: self.postgres_sync_manager.replace_cached_table_row('stock_values', target_row, row_values),
        )

        return {
            'queued_operation_id': queue_id,
            'row_values': row_values,
            'non_empty_count': non_empty_count,
            'headers': headers,
            'headers_upper': headers_upper,
            'header_row_idx': header_row_idx,
            'target_row': target_row,
        }

    def add_service_record(self, values_by_header, force_refresh=False):
        main_values = self.get_main_values(force_refresh=force_refresh)
        if not main_values:
            return {'error': 'Main inventory sheet is empty.'}

        header_row_idx = detect_sheet_header_row(main_values)
        headers = [str(cell or '').strip() for cell in (main_values[header_row_idx] if header_row_idx < len(main_values) else [])]
        if not headers:
            return {'error': 'Main inventory headers are missing.'}

        now = datetime.now()
        defaults = {
            'DATE': now.strftime('%m/%d/%Y'),
            'TIME': now.strftime('%H:%M'),
            'STATUS': 'UNPAID',
            'AMOUNT PAID': '0',
            'RECORD_ID': uuid.uuid4().hex,
        }
        merged_values = dict(defaults)
        for key, value in (values_by_header or {}).items():
            merged_values[str(key or '').strip().upper()] = '' if value is None else str(value)

        # Service-specific cost incurred while delivering this service.
        # This is recorded into cashflow expenses, not inventory profit columns.
        service_expense_amount = clean_amount(
            merged_values.get('SERVICE EXPENSE')
            or merged_values.get('EXPENSE')
            or merged_values.get('SERVICE COST')
            or 0
        )

        row_values = self._build_sheet_row_values(headers, merged_values)
        if not any(str(value or '').strip() for value in row_values):
            return {'error': 'Fill at least one service field before saving.'}

        target_row = find_next_table_write_row(main_values, header_row_idx)
        row_record = self._build_sheet_record(headers, row_values)

        queue_id = self._enqueue_db_first_operation(
            'service',
            'main_write_row',
            {
                'kind': 'main_write_row',
                'row': target_row,
                'row_values': row_values,
            },
            cache_apply_callable=lambda row=target_row, values=row_values, record=row_record: (
                self.postgres_sync_manager.replace_cached_table_row('main_values', row, values),
                self.postgres_sync_manager.append_cached_dict_row('main_records', record),
            ),
        )

        # Nudge queue replay immediately in the background so service rows
        # appear in Google Sheets quickly without blocking API response time.
        try:
            try:
                price_amount = clean_amount(merged_values.get('PRICE') or 0)
                paid_amount = clean_amount(merged_values.get('AMOUNT PAID') or 0)
                service_status = str(merged_values.get('STATUS') or '').strip().upper()
                payment_date = str(merged_values.get('DATE') or now.date().isoformat()).strip()
                service_description = str(merged_values.get('DESCRIPTION') or merged_values.get('MODEL') or merged_values.get('DEVICE') or '').strip()
                service_actor = str(merged_values.get('NAME') or merged_values.get('CLIENT NAME') or 'service').strip().upper() or 'service'

                if service_status == 'PAID' and paid_amount <= 0 and price_amount > 0:
                    paid_amount = price_amount

                # Any positive amount paid is realized cash-in and should be reflected
                # in cashflow immediately, regardless of service row status text.
                if paid_amount > 0:
                    self.append_cashflow_income_record(
                        amount=paid_amount,
                        category='SERVICE PROFIT',
                        description=service_description,
                        date_text=payment_date,
                        created_by=service_actor,
                        payment_status='PAID',
                        entry_type='service',
                        payment_date_text=payment_date,
                    )

                # Record expense incurred for this specific service into weekly cashflow expenses.
                if service_expense_amount > 0:
                    self.append_cashflow_expense_record(
                        amount=service_expense_amount,
                        category='SERVICE EXPENSE',
                        description=service_description,
                        date_text=payment_date,
                        created_by=service_actor,
                    )
            except Exception as cashflow_exc:
                self.logger.warning('Failed to write service income to cashflow sheet: %s', cashflow_exc)

            threading.Thread(
                target=lambda: self.replay_pending_queue_now(limit=60),
                name='service-add-immediate-queue-flush',
                daemon=True,
            ).start()
        except Exception:
            pass

        return {
            'queued_operation_id': queue_id,
            'target_row': target_row,
            'row_values': row_values,
        }

    def get_pending_service_deals(self, force_refresh=False):
        main_values = self.get_main_values(force_refresh=force_refresh)
        if not main_values:
            return {
                'items': [],
                'count': 0,
            }

        header_row_idx = detect_sheet_header_row(main_values)
        headers = [str(cell or '').strip() for cell in (main_values[header_row_idx] if header_row_idx < len(main_values) else [])]
        headers_upper = [header.upper() for header in headers]
        if not headers:
            return {
                'items': [],
                'count': 0,
            }

        name_col = svc_stock_header_index(headers_upper, 'NAME', 'CLIENT NAME', 'CUSTOMER NAME')
        phone_col = svc_stock_header_index(headers_upper, 'PHONE NUMBER', 'PHONE', 'WHATSAPP NUMBER', 'WHATSAPP', 'NUMBER')
        description_col = svc_stock_header_index(headers_upper, 'DESCRIPTION', 'MODEL', 'DEVICE')
        status_col = svc_stock_header_index(headers_upper, 'STATUS')
        paid_col = svc_stock_header_index(headers_upper, 'AMOUNT PAID', 'AMOUNT PAID ') 
        price_col = svc_stock_header_index(headers_upper, 'PRICE')
        imei_col = svc_stock_header_index(headers_upper, 'IMEI')
        date_col = svc_stock_header_index(headers_upper, 'DATE')

        if status_col is None:
            return {
                'items': [],
                'count': 0,
            }

        items = []
        for row_index in range(header_row_idx + 1, len(main_values)):
            row = main_values[row_index] if row_index < len(main_values) else []
            status_text = str(row[status_col] if status_col < len(row) else '').strip().upper()
            if status_text not in {'UNPAID', 'PART PAYMENT'}:
                continue

            imei_value = str(row[imei_col] if imei_col is not None and imei_col < len(row) else '').strip()
            if imei_value:
                continue

            price_value = clean_amount(row[price_col] if price_col is not None and price_col < len(row) else '')
            paid_value = clean_amount(row[paid_col] if paid_col is not None and paid_col < len(row) else '')
            if price_value <= 0:
                continue

            items.append({
                'kind': 'service',
                'row_num': row_index + 1,
                'name': str(row[name_col] if name_col is not None and name_col < len(row) else '').strip().upper(),
                'phone': normalize_phone_number(row[phone_col] if phone_col is not None and phone_col < len(row) else ''),
                'description': str(row[description_col] if description_col is not None and description_col < len(row) else '').strip(),
                'status': status_text,
                'amount_paid': str(row[paid_col] if paid_col is not None and paid_col < len(row) else '').strip(),
                'price': str(row[price_col] if price_col is not None and price_col < len(row) else '').strip(),
                'balance': max(0, price_value - paid_value),
                'date': str(row[date_col] if date_col is not None and date_col < len(row) else '').strip(),
            })

        items.sort(key=lambda item: int(item.get('row_num') or 0), reverse=True)
        return {
            'items': items,
            'count': len(items),
        }

    def update_service_pending_payment(self, row_num, payment_status, amount_paid=None, force_refresh=False):
        status_text = str(payment_status or '').strip().upper()
        if status_text in {'PARTIAL PAYMENT', 'PARTIAL'}:
            status_text = 'PART PAYMENT'
        if status_text not in {'PAID', 'PART PAYMENT', 'UNPAID'}:
            return {'error': 'Payment status must be PAID, PART PAYMENT, or UNPAID.'}

        main_values = self.get_main_values(force_refresh=force_refresh)
        if not main_values:
            return {'error': 'Main inventory sheet is empty.'}

        header_row_idx = detect_sheet_header_row(main_values)
        headers = [str(cell or '').strip() for cell in (main_values[header_row_idx] if header_row_idx < len(main_values) else [])]
        headers_upper = [header.upper() for header in headers]
        row_num = int(row_num or 0)
        if row_num <= header_row_idx + 1 or row_num > len(main_values):
            return {'error': f'Inventory row {row_num} is no longer available.'}

        status_col = svc_stock_header_index(headers_upper, 'STATUS')
        paid_col = svc_stock_header_index(headers_upper, 'AMOUNT PAID', 'AMOUNT PAID ')
        price_col = svc_stock_header_index(headers_upper, 'PRICE')
        imei_col = svc_stock_header_index(headers_upper, 'IMEI')
        description_col = svc_stock_header_index(headers_upper, 'DESCRIPTION', 'MODEL', 'DEVICE', 'DESC')
        if status_col is None:
            return {'error': 'STATUS column is missing in the inventory sheet.'}

        row = main_values[row_num - 1] if row_num - 1 < len(main_values) else []
        imei_value = str(row[imei_col] if imei_col is not None and imei_col < len(row) else '').strip()
        if imei_value:
            return {'error': 'This pending row is a stock sale, not a service deal.'}

        current_paid = clean_amount(row[paid_col] if paid_col is not None and paid_col < len(row) else '')
        price_value = clean_amount(row[price_col] if price_col is not None and price_col < len(row) else '')
        explicit_amount = None if amount_paid is None or str(amount_paid).strip() == '' else clean_amount(amount_paid)
        name_col = svc_stock_header_index(headers_upper, 'NAME', 'CLIENT NAME', 'CUSTOMER NAME')
        customer_name = str(row[name_col] if name_col is not None and name_col < len(row) else '').strip().upper()

        if explicit_amount is not None and price_value > 0:
            if explicit_amount > price_value:
                remainder = explicit_amount - price_value
                return {
                    'error': f'Amount paid cannot be greater than sale price. Max allowed is NGN {price_value:,}.',
                    'error_code': 'OVERPAYMENT',
                    'sale_price': price_value,
                    'entered_amount': explicit_amount,
                    'remainder': remainder,
                    'customer_name': customer_name,
                }

            if explicit_amount == 0:
                status_text = 'UNPAID'
            elif explicit_amount < price_value:
                status_text = 'PART PAYMENT'
            else:
                status_text = 'PAID'

        if status_text == 'PAID':
            resolved_amount = explicit_amount if explicit_amount and explicit_amount > 0 else price_value
        elif status_text == 'UNPAID':
            resolved_amount = explicit_amount if explicit_amount is not None else 0
        else:
            resolved_amount = explicit_amount if explicit_amount is not None else current_paid

        # Auto-promote to PAID when the entered amount covers the full price.
        if status_text != 'PAID' and price_value > 0 and resolved_amount >= price_value:
            status_text = 'PAID'
            resolved_amount = price_value

        queued_operation_ids = []
        queue_id = self._enqueue_db_first_operation(
            'service',
            'main_update_status',
            {
                'kind': 'main_update_cell',
                'row': row_num,
                'col': status_col + 1,
                'value': status_text,
            },
            cache_apply_callable=lambda rn=row_num, cn=status_col + 1, nv=status_text: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, nv) if self.postgres_ready else None,
        )
        queued_operation_ids.append(queue_id)

        if paid_col is not None:
            queue_id = self._enqueue_db_first_operation(
                'service',
                'main_update_amount_paid',
                {
                    'kind': 'main_update_cell',
                    'row': row_num,
                    'col': paid_col + 1,
                    'value': resolved_amount,
                },
                cache_apply_callable=lambda rn=row_num, cn=paid_col + 1, nv=resolved_amount: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, nv) if self.postgres_ready else None,
            )
            queued_operation_ids.append(queue_id)

        try:
            self.replay_pending_queue_now(limit=120)
        except Exception:
            pass

        # When a pending service becomes PAID, reflect realized service income in cashflow immediately.
        try:
            if status_text == 'PAID' and resolved_amount > 0:
                payment_date_iso = datetime.now(timezone.utc).date().isoformat()
                service_description = str(row[description_col] if description_col is not None and description_col < len(row) else '').strip()
                if not self.has_cashflow_income_paid_record(
                    entry_type='service',
                    description=service_description,
                    created_by=customer_name,
                    payment_date_text=payment_date_iso,
                ):
                    self.append_cashflow_income_record(
                        amount=resolved_amount,
                        category='SERVICE PROFIT',
                        description=service_description,
                        date_text=payment_date_iso,
                        created_by=customer_name or 'service',
                        payment_status='PAID',
                        entry_type='service',
                        payment_date_text=payment_date_iso,
                    )
        except Exception as cashflow_exc:
            self.logger.warning('Failed to sync service pending payment to cashflow rows: %s', cashflow_exc)

        return {
            'row_num': row_num,
            'payment_status': status_text,
            'queued_operation_ids': queued_operation_ids,
            'message': f'Service deal row #{row_num} updated to {status_text}.',
        }

    def return_service_deal(self, row_num, force_refresh=False):
        main_values = self.get_main_values(force_refresh=force_refresh)
        if not main_values:
            return {'error': 'Main inventory sheet is empty.'}

        header_row_idx = detect_sheet_header_row(main_values)
        headers = [str(cell or '').strip() for cell in (main_values[header_row_idx] if header_row_idx < len(main_values) else [])]
        headers_upper = [header.upper() for header in headers]
        row_num = int(row_num or 0)
        if row_num <= header_row_idx + 1 or row_num > len(main_values):
            return {'error': f'Inventory row {row_num} is no longer available.'}

        status_col = svc_stock_header_index(headers_upper, 'STATUS')
        paid_col = svc_stock_header_index(headers_upper, 'AMOUNT PAID', 'AMOUNT PAID ')
        imei_col = svc_stock_header_index(headers_upper, 'IMEI')
        if status_col is None:
            return {'error': 'STATUS column is missing in the inventory sheet.'}

        row = main_values[row_num - 1] if row_num - 1 < len(main_values) else []
        imei_value = str(row[imei_col] if imei_col is not None and imei_col < len(row) else '').strip()
        if imei_value:
            return {'error': 'This pending row is a stock sale, not a service deal.'}

        queued_operation_ids = []
        queue_id = self._enqueue_db_first_operation(
            'service_return',
            'main_update_status',
            {
                'kind': 'main_update_cell',
                'row': row_num,
                'col': status_col + 1,
                'value': 'RETURNED',
            },
            cache_apply_callable=lambda rn=row_num, cn=status_col + 1: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, 'RETURNED') if self.postgres_ready else None,
        )
        queued_operation_ids.append(queue_id)

        if paid_col is not None:
            queue_id = self._enqueue_db_first_operation(
                'service_return',
                'main_update_amount_paid',
                {
                    'kind': 'main_update_cell',
                    'row': row_num,
                    'col': paid_col + 1,
                    'value': 0,
                },
                cache_apply_callable=lambda rn=row_num, cn=paid_col + 1: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, 0) if self.postgres_ready else None,
            )
            queued_operation_ids.append(queue_id)

        try:
            self.replay_pending_queue_now(limit=120)
        except Exception:
            pass

        return {
            'row_num': row_num,
            'queued_operation_ids': queued_operation_ids,
            'message': f'Service deal row #{row_num} returned/refunded successfully.',
        }

    def return_stock_item(self, row_num, force_refresh=False):
        stock_values, stock_header_row_idx, stock_headers, stock_headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)
        row_num = int(row_num or 0)
        if row_num <= stock_header_row_idx + 1 or row_num > len(stock_values):
            return {'error': f'Stock row {row_num} is no longer available.'}

        stock_row = list(stock_values[row_num - 1])
        padded = stock_row + [''] * max(0, len(stock_headers) - len(stock_row))
        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return {'error': 'Stock sheet ID is missing.'}

        status_col = svc_stock_header_index(stock_headers_upper, 'PRODUCT STATUS', 'STATUS OF DEVICE', 'STOCK STATUS', 'ITEM STATUS', 'STATUS')
        buyer_col = svc_stock_header_index(stock_headers_upper, 'NAME OF BUYER', 'BUYER NAME', 'CUSTOMER NAME')
        phone_col = svc_stock_header_index(stock_headers_upper, 'PHONE NUMBER OF BUYER', 'PHONE OF BUYER', 'BUYER PHONE', 'BUYER PHONE NUMBER')
        sold_amount_col = svc_stock_header_index(stock_headers_upper, 'AMOUNT SOLD', 'SELLING PRICE')
        paid_amount_col = svc_stock_header_index(stock_headers_upper, 'AMOUNT PAID')
        availability_col = svc_stock_header_index(stock_headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
        desc_col = svc_stock_header_index(stock_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
        imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')

        if status_col is None:
            return {'error': 'Could not find a stock status column (PRODUCT STATUS / STATUS OF DEVICE / STATUS).'}

        buyer_name_value = padded[buyer_col] if buyer_col is not None and buyer_col < len(padded) else ''
        buyer_phone_value = padded[phone_col] if phone_col is not None and phone_col < len(padded) else ''
        description_value = padded[desc_col] if desc_col is not None and desc_col < len(padded) else ''
        imei_value = padded[imei_col] if imei_col is not None and imei_col < len(padded) else ''

        main_values = self.get_main_values(force_refresh=False)
        main_header_row_idx = detect_sheet_header_row(main_values)
        main_headers = [str(cell or '').strip() for cell in (main_values[main_header_row_idx] if main_header_row_idx < len(main_values) else [])]
        main_headers_upper = [header.upper() for header in main_headers]
        latest_row_num = None

        updates = []
        if status_col is not None:
            updates.append({'col': status_col + 1, 'value': 'AVAILABLE'})
        if buyer_col is not None:
            updates.append({'col': buyer_col + 1, 'value': ''})
        if phone_col is not None:
            updates.append({'col': phone_col + 1, 'value': ''})
        if sold_amount_col is not None:
            updates.append({'col': sold_amount_col + 1, 'value': ''})
        if paid_amount_col is not None:
            updates.append({'col': paid_amount_col + 1, 'value': ''})
        if availability_col is not None:
            updates.append({'col': availability_col + 1, 'value': ''})

        queue_ids = []
        for update in updates:
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'stock',
                    'stock_update_cell',
                    {
                        'kind': 'stock_update_cell',
                        'stock_sheet_id': stock_sheet_id,
                        'row': row_num,
                        'col': update['col'],
                        'value': update['value'],
                    },
                    cache_apply_callable=lambda rn=row_num, cn=update['col'], nv=update['value']: self.postgres_sync_manager.update_cached_stock_value(rn, cn, nv),
                )
            )

        if desc_col is not None:
            _, fill_color = map_sale_status('available')
            request_body = {
                'requests': [{
                    'repeatCell': {
                        'range': {
                            'sheetId': self._resolve_stock_worksheet(stock_sheet_id).id,
                            'startRowIndex': row_num - 1,
                            'endRowIndex': row_num,
                            'startColumnIndex': desc_col,
                            'endColumnIndex': desc_col + 1,
                        },
                        'cell': {'userEnteredFormat': {'backgroundColor': fill_color}},
                        'fields': 'userEnteredFormat.backgroundColor',
                    }
                }]
            }
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'stock',
                    'stock_batch_update',
                    {
                        'kind': 'stock_batch_update',
                        'stock_sheet_id': stock_sheet_id,
                        'request_body': request_body,
                    },
                    cache_apply_callable=lambda rn=row_num: self._update_cached_stock_status(rn, 'available'),
                )
            )

        if main_headers:
            main_name_col = svc_stock_header_index(main_headers_upper, 'NAME', 'CLIENT NAME', 'CUSTOMER NAME')
            main_phone_col = svc_stock_header_index(main_headers_upper, 'PHONE NUMBER', 'PHONE', 'PHONE NO')
            main_description_col = svc_stock_header_index(main_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
            main_imei_col = svc_stock_header_index(main_headers_upper, 'IMEI')
            main_status_col = svc_stock_header_index(main_headers_upper, 'STATUS')
            main_paid_col = svc_stock_header_index(main_headers_upper, 'AMOUNT PAID', 'AMOUNT PAID ')

            if main_status_col is not None:
                buyer_name_key = str(buyer_name_value or '').strip().upper()
                buyer_phone_key = normalize_phone_number(buyer_phone_value or '')
                imei_key = str(imei_value or '').strip()
                desc_key = str(description_value or '').strip().upper()

                for index in range(len(main_values) - 1, main_header_row_idx, -1):
                    row = main_values[index] if index < len(main_values) else []
                    if not row:
                        continue

                    row_status = str(row[main_status_col] or '').strip().upper() if main_status_col < len(row) else ''
                    if row_status == 'RETURNED':
                        continue

                    row_imei = str(row[main_imei_col] or '').strip() if main_imei_col is not None and main_imei_col < len(row) else ''
                    row_name = str(row[main_name_col] or '').strip().upper() if main_name_col is not None and main_name_col < len(row) else ''
                    row_phone = normalize_phone_number(row[main_phone_col] if main_phone_col is not None and main_phone_col < len(row) else '')
                    row_description = str(row[main_description_col] or '').strip().upper() if main_description_col is not None and main_description_col < len(row) else ''

                    if imei_key and row_imei != imei_key:
                        continue

                    # If IMEI is present, trust it as the strongest identity key and
                    # avoid hard-failing on phone/name drift from manual edits.
                    if not imei_key:
                        if buyer_phone_key and row_phone != buyer_phone_key:
                            continue
                        if buyer_name_key and row_name and row_name != buyer_name_key:
                            continue
                        if desc_key and row_description and row_description != desc_key:
                            continue

                    latest_row_num = index + 1
                    break

            if latest_row_num is not None and main_status_col is not None:
                queue_ids.append(
                    self._enqueue_db_first_operation(
                        'inventory',
                        'main_update_status',
                        {
                            'kind': 'main_update_cell',
                            'row': latest_row_num,
                            'col': main_status_col + 1,
                            'value': 'RETURNED',
                        },
                        cache_apply_callable=lambda rn=latest_row_num, cn=main_status_col + 1: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, 'RETURNED'),
                    )
                )

                if main_paid_col is not None:
                    queue_ids.append(
                        self._enqueue_db_first_operation(
                            'inventory',
                            'main_update_amount_paid',
                            {
                                'kind': 'main_update_cell',
                                'row': latest_row_num,
                                'col': main_paid_col + 1,
                                'value': 0,
                            },
                            cache_apply_callable=lambda rn=latest_row_num, cn=main_paid_col + 1: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, 0),
                        )
                    )

        if main_headers and latest_row_num is None:
            now = datetime.now()
            next_main_row = find_next_table_write_row(main_values, main_header_row_idx)
            next_sun_serial = max(1, next_main_row - (main_header_row_idx + 1))
            returned_values_by_header = {
                'DATE': now.strftime('%m/%d/%Y'),
                'TIME': now.strftime('%H:%M'),
                'NAME': str(buyer_name_value or '').strip().upper(),
                'DESCRIPTION': str(description_value or '').strip(),
                'IMEI': str(imei_value or '').strip(),
                'PHONE NUMBER': normalize_phone_number(buyer_phone_value or ''),
                'PRICE': '0',
                'AMOUNT PAID': '0',
                'STATUS': 'RETURNED',
                'RECORD_ID': uuid.uuid4().hex,
                'SUN S/N': str(next_sun_serial),
            }
            returned_row_values = self._build_sheet_row_values(main_headers, returned_values_by_header)
            returned_record = self._build_sheet_record(main_headers, returned_row_values)
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'returns',
                    'main_write_row',
                    {
                        'kind': 'main_write_row',
                        'row': next_main_row,
                        'row_values': returned_row_values,
                    },
                    cache_apply_callable=lambda row=next_main_row, values=returned_row_values, record=returned_record: (
                        self.postgres_sync_manager.replace_cached_table_row('main_values', row, values),
                        self.postgres_sync_manager.append_cached_dict_row('main_records', record),
                    ),
                )
            )

        # Flush queued return operations immediately so inventory reflects RETURNED entries quickly.
        replay_result = None
        try:
            replay_result = self.replay_pending_queue_now(limit=150)
        except Exception:
            replay_result = None

        if replay_result and replay_result.get('failed', 0) > 0:
            return {
                'error': 'Return was queued but could not be written to spreadsheet right now. Please retry in a moment.',
            }

        return {
            'queued_operation_ids': queue_ids,
            'row_num': row_num,
            'status': 'AVAILABLE',
            'message': f'Stock row #{row_num} returned and reset.',
        }

    def update_pending_deal_payment(self, row_num, payment_status, amount_paid=None, force_refresh=False):
        status_text = str(payment_status or '').strip().upper()
        if status_text in {'PARTIAL PAYMENT', 'PARTIAL'}:
            status_text = 'PART PAYMENT'
        if status_text not in {'PAID', 'PART PAYMENT', 'UNPAID'}:
            return {'error': 'Payment status must be PAID, PART PAYMENT, or UNPAID.'}

        stock_values, stock_header_row_idx, stock_headers, stock_headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)
        row_num = int(row_num or 0)
        if row_num <= stock_header_row_idx + 1 or row_num > len(stock_values):
            return {'error': f'Stock row {row_num} is no longer available.'}

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return {'error': 'Stock sheet ID is missing.'}

        stock_row = list(stock_values[row_num - 1])
        padded_stock_row = stock_row + [''] * max(0, len(stock_headers) - len(stock_row))
        buyer_col = svc_stock_header_index(stock_headers_upper, 'NAME OF BUYER')
        buyer_phone_col = svc_stock_header_index(stock_headers_upper, 'PHONE NUMBER OF BUYER', 'PHONE OF BUYER', 'BUYER PHONE')
        imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')
        product_status_col = svc_stock_header_index(stock_headers_upper, 'PRODUCT STATUS', 'STATUS OF DEVICE', 'STOCK STATUS', 'ITEM STATUS')
        availability_col = svc_stock_header_index(stock_headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
        description_col = svc_stock_header_index(stock_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
        cost_price_col = svc_stock_header_index(stock_headers_upper, 'COST PRICE', 'COST', 'COST_PRICE')

        buyer_name = str(padded_stock_row[buyer_col] if buyer_col is not None and buyer_col < len(padded_stock_row) else '').strip().upper()
        buyer_phone = normalize_phone_number(padded_stock_row[buyer_phone_col] if buyer_phone_col is not None and buyer_phone_col < len(padded_stock_row) else '')
        imei_value = str(padded_stock_row[imei_col] if imei_col is not None and imei_col < len(padded_stock_row) else '').strip()
        description_value = str(padded_stock_row[description_col] if description_col is not None and description_col < len(padded_stock_row) else '').strip().upper()
        cost_price_value = clean_amount(padded_stock_row[cost_price_col]) if cost_price_col is not None and cost_price_col < len(padded_stock_row) else 0.0

        # Pre-scan inventory to find the matching row and price so status can be
        # auto-promoted to PAID when the entered amount covers the full price.
        has_explicit_amount = amount_paid is not None and str(amount_paid).strip() != ''
        explicit_amount = clean_amount(amount_paid) if has_explicit_amount else None
        main_values = self.get_main_values(force_refresh=False)
        main_header_row_idx = detect_sheet_header_row(main_values)
        main_headers = [str(cell or '').strip() for cell in (main_values[main_header_row_idx] if main_header_row_idx < len(main_values) else [])]
        main_headers_upper = [header.upper() for header in main_headers]
        main_name_col = svc_stock_header_index(main_headers_upper, 'NAME')
        main_phone_col = svc_stock_header_index(main_headers_upper, 'PHONE NUMBER', 'PHONE', 'PHONE NO')
        main_imei_col = svc_stock_header_index(main_headers_upper, 'IMEI')
        main_description_col = svc_stock_header_index(main_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
        main_status_col = svc_stock_header_index(main_headers_upper, 'STATUS')
        main_paid_col = svc_stock_header_index(main_headers_upper, 'AMOUNT PAID')
        main_price_col = svc_stock_header_index(main_headers_upper, 'PRICE')

        matched_inventory_row = None
        matched_row_values = []
        if main_values and main_status_col is not None:
            for index in range(len(main_values) - 1, main_header_row_idx, -1):
                inv_row = main_values[index] if index < len(main_values) else []
                if not inv_row:
                    continue
                row_imei = str(inv_row[main_imei_col] or '').strip() if main_imei_col is not None and main_imei_col < len(inv_row) else ''
                row_name = str(inv_row[main_name_col] or '').strip().upper() if main_name_col is not None and main_name_col < len(inv_row) else ''
                row_phone = normalize_phone_number(inv_row[main_phone_col] if main_phone_col is not None and main_phone_col < len(inv_row) else '')
                row_description = str(inv_row[main_description_col] or '').strip().upper() if main_description_col is not None and main_description_col < len(inv_row) else ''
                row_status = str(inv_row[main_status_col] or '').strip().upper() if main_status_col < len(inv_row) else ''
                if row_status == 'RETURNED':
                    continue
                if imei_value and row_imei != imei_value:
                    continue
                if buyer_phone and row_phone != buyer_phone:
                    continue
                if buyer_name and row_name and row_name != buyer_name:
                    continue
                if not imei_value and description_value and row_description and row_description != description_value:
                    continue
                matched_inventory_row = index + 1
                matched_row_values = list(inv_row)
                break

        if explicit_amount and explicit_amount > 0 and matched_row_values and main_price_col is not None and status_text != 'PAID':
            pre_price = clean_amount(matched_row_values[main_price_col]) if main_price_col < len(matched_row_values) else 0
            if pre_price > 0 and explicit_amount >= pre_price:
                status_text = 'PAID'

        comparison_price = 0
        if matched_row_values and main_price_col is not None:
            comparison_price = clean_amount(matched_row_values[main_price_col]) if main_price_col < len(matched_row_values) else 0
        if comparison_price <= 0:
            stock_price_col = svc_stock_header_index(stock_headers_upper, 'AMOUNT SOLD', 'SELLING PRICE', 'PRICE')
            if stock_price_col is not None and stock_price_col < len(padded_stock_row):
                comparison_price = clean_amount(padded_stock_row[stock_price_col])

        if explicit_amount is not None and comparison_price > 0:
            if explicit_amount > comparison_price:
                remainder = explicit_amount - comparison_price
                return {
                    'error': f'Amount paid cannot be greater than sale price. Max allowed is NGN {comparison_price:,}.',
                    'error_code': 'OVERPAYMENT',
                    'sale_price': comparison_price,
                    'entered_amount': explicit_amount,
                    'remainder': remainder,
                    'customer_name': buyer_name,
                }

            if explicit_amount == 0:
                status_text = 'UNPAID'
            elif explicit_amount < comparison_price:
                status_text = 'PART PAYMENT'
            else:
                status_text = 'PAID'

        stock_status_choice = 'Sold' if status_text == 'PAID' else 'Pending Deal'
        stock_status_key, fill_color = map_sale_status(stock_status_choice)
        availability_value = datetime.now().strftime('%m/%d/%Y') if status_text == 'PAID' else 'PENDING DEAL'

        queued_operation_ids = []
        if product_status_col is not None:
            queue_id = self._enqueue_db_first_operation(
                'stock',
                'stock_update_cell',
                {
                    'kind': 'stock_update_cell',
                    'stock_sheet_id': stock_sheet_id,
                    'row': row_num,
                    'col': product_status_col + 1,
                    'value': stock_status_key_to_label(stock_status_key),
                },
                cache_apply_callable=lambda rn=row_num, cn=product_status_col + 1, nv=stock_status_key_to_label(stock_status_key): self.postgres_sync_manager.update_cached_stock_value(rn, cn, nv),
            )
            queued_operation_ids.append(queue_id)

        if availability_col is not None:
            queue_id = self._enqueue_db_first_operation(
                'stock',
                'stock_update_cell',
                {
                    'kind': 'stock_update_cell',
                    'stock_sheet_id': stock_sheet_id,
                    'row': row_num,
                    'col': availability_col + 1,
                    'value': availability_value,
                },
                cache_apply_callable=lambda rn=row_num, cn=availability_col + 1, nv=availability_value: self.postgres_sync_manager.update_cached_stock_value(rn, cn, nv),
            )
            queued_operation_ids.append(queue_id)

        if description_col is not None:
            request_body = {
                'requests': [{
                    'repeatCell': {
                        'range': {
                            'sheetId': self._resolve_stock_worksheet(stock_sheet_id).id,
                            'startRowIndex': row_num - 1,
                            'endRowIndex': row_num,
                            'startColumnIndex': description_col,
                            'endColumnIndex': description_col + 1,
                        },
                        'cell': {'userEnteredFormat': {'backgroundColor': fill_color}},
                        'fields': 'userEnteredFormat.backgroundColor',
                    }
                }]
            }
            queue_id = self._enqueue_db_first_operation(
                'stock',
                'stock_batch_update',
                {
                    'kind': 'stock_batch_update',
                    'stock_sheet_id': stock_sheet_id,
                    'request_body': request_body,
                },
                cache_apply_callable=lambda rn=row_num, sk=stock_status_key: self._update_cached_stock_status(rn, sk),
            )
            queued_operation_ids.append(queue_id)

        if matched_inventory_row is not None and main_status_col is not None:
            queue_id = self._enqueue_db_first_operation(
                'inventory',
                'main_update_status',
                {
                    'kind': 'main_update_cell',
                    'row': matched_inventory_row,
                    'col': main_status_col + 1,
                    'value': status_text,
                },
                cache_apply_callable=lambda rn=matched_inventory_row, cn=main_status_col + 1, nv=status_text: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, nv),
            )
            queued_operation_ids.append(queue_id)

            if main_paid_col is not None:
                existing_paid = clean_amount(matched_row_values[main_paid_col]) if main_paid_col < len(matched_row_values) else 0
                existing_price = clean_amount(matched_row_values[main_price_col]) if main_price_col is not None and main_price_col < len(matched_row_values) else 0

                if status_text == 'PAID':
                    resolved_amount = explicit_amount if explicit_amount and explicit_amount > 0 else (existing_price if existing_price > 0 else existing_paid)
                elif status_text == 'UNPAID':
                    resolved_amount = explicit_amount if explicit_amount is not None else 0
                else:
                    resolved_amount = explicit_amount if explicit_amount is not None else existing_paid

                queue_id = self._enqueue_db_first_operation(
                    'inventory',
                    'main_update_amount_paid',
                    {
                        'kind': 'main_update_cell',
                        'row': matched_inventory_row,
                        'col': main_paid_col + 1,
                        'value': resolved_amount,
                    },
                    cache_apply_callable=lambda rn=matched_inventory_row, cn=main_paid_col + 1, nv=resolved_amount: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, nv),
                )
                queued_operation_ids.append(queue_id)

        # Ensure stock + inventory status changes are visible immediately in Sheets.
        try:
            self.replay_pending_queue_now(limit=120)
        except Exception:
            pass

        # Keep payment-based profit rows aligned: OWING rows become PAID on payment date.
        try:
            payment_date_iso = datetime.now(timezone.utc).date().isoformat()
            cashflow_description = description_value or imei_value
            cashflow_created_by = buyer_name or buyer_phone or 'stock'

            if status_text == 'PAID' and comparison_price > 0:
                phone_profit_amount = max(0.0, comparison_price - max(0.0, cost_price_value))
                updated_row = self.mark_cashflow_income_paid(
                    entry_type='phone',
                    description=cashflow_description,
                    created_by=cashflow_created_by,
                    payment_date_text=payment_date_iso,
                    amount=phone_profit_amount,
                    cost_price=cost_price_value if cost_price_value > 0 else None,
                )
                if updated_row is None and not self.has_cashflow_income_paid_record(
                    entry_type='phone',
                    description=cashflow_description,
                    created_by=cashflow_created_by,
                    payment_date_text=payment_date_iso,
                ):
                    self.append_cashflow_income_record(
                        amount=phone_profit_amount,
                        category='PHONE PROFIT',
                        description=cashflow_description,
                        date_text=payment_date_iso,
                        created_by=cashflow_created_by,
                        payment_status='PAID',
                        entry_type='phone',
                        cost_price=cost_price_value if cost_price_value > 0 else '',
                        payment_date_text=payment_date_iso,
                    )
        except Exception as cashflow_exc:
            self.logger.warning('Failed to sync pending-deal payment to cashflow rows: %s', cashflow_exc)

        return {
            'row_num': row_num,
            'payment_status': status_text,
            'inventory_row_num': matched_inventory_row,
            'queued_operation_ids': queued_operation_ids,
            'message': (
                f'Updated stock row #{row_num} to {status_text}; inventory row #{matched_inventory_row} updated.'
                if matched_inventory_row
                else f'Updated stock row #{row_num} to {status_text}; no matching inventory row was found.'
            ),
        }

    def update_stock_row(self, row_num, values_by_header, force_refresh=False):
        values, header_row_idx, headers, headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)
        row_num = int(row_num or 0)
        first_data_row = header_row_idx + 2
        if row_num < first_data_row or row_num > len(values):
            return {'error': f'Stock row {row_num} is not available.'}

        header_to_col = {str(header or '').strip().upper(): index for index, header in enumerate(headers)}
        current_row = list(values[row_num - 1]) if row_num - 1 < len(values) else []
        padded_current = current_row + [''] * max(0, len(headers) - len(current_row))

        updates = []
        queue_ids = []
        for raw_header, raw_value in (values_by_header or {}).items():
            normalized_header = str(raw_header or '').strip().upper()
            if not normalized_header:
                continue
            col_index = header_to_col.get(normalized_header)
            if col_index is None:
                continue

            next_value = '' if raw_value is None else str(raw_value)
            current_value = str(padded_current[col_index] if col_index < len(padded_current) else '')
            if current_value == next_value:
                continue

            col_number = col_index + 1
            queue_id = self._enqueue_db_first_operation(
                'stock',
                'stock_update_cell',
                {
                    'kind': 'stock_update_cell',
                    'stock_sheet_id': self._resolve_stock_sheet_id(),
                    'row': row_num,
                    'col': col_number,
                    'value': next_value,
                },
                cache_apply_callable=lambda rn=row_num, cn=col_number, nv=next_value: self.postgres_sync_manager.update_cached_stock_value(rn, cn, nv),
            )
            queue_ids.append(queue_id)
            updates.append({
                'header': headers[col_index],
                'col': col_number,
                'old_value': current_value,
                'new_value': next_value,
            })

        if not updates:
            return {
                'updated_count': 0,
                'queued_operation_ids': [],
                'updates': [],
                'row_num': row_num,
            }

        return {
            'updated_count': len(updates),
            'queued_operation_ids': queue_ids,
            'updates': updates,
            'row_num': row_num,
        }

    def get_client_registry(self, force_reload=False):
        with self._clients_lock:
            return dict(self._load_clients_from_disk() if force_reload else self._load_clients_from_disk())

    def get_client_registry_payload(self, force_reload=False):
        registry = self.get_client_registry(force_reload=force_reload)
        entries = [
            {
                'name': name,
                'phone': str(phone or ''),
                'has_phone': bool(str(phone or '').strip()),
            }
            for name, phone in sorted(registry.items(), key=lambda item: str(item[0]).upper())
        ]
        stats = {
            'total_count': len(entries),
            'with_phone_count': sum(1 for entry in entries if entry['has_phone']),
            'without_phone_count': sum(1 for entry in entries if not entry['has_phone']),
        }
        return {
            'registry': registry,
            'entries': entries,
            'directory_rows': build_client_directory_rows(registry),
            'stats': stats,
        }

    def sync_client_directory_sheet(self, force_reload=False):
        if not self._ensure_sheet_connection():
            raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')

        registry = self.get_client_registry(force_reload=force_reload)
        rows = build_client_directory_rows(registry)

        with self._sheet_lock:
            directory_ws = ensure_directory_sheet(self.main_spreadsheet, 'CLIENT DIRECTORY')
            directory_ws.clear()
            directory_ws.update(f'A1:B{len(rows)}', rows)

        return {
            'directory_rows_written': max(0, len(rows) - 1),
            'directory_sheet_title': 'CLIENT DIRECTORY',
        }

    def import_sheet_phone_numbers_to_clients(self, force_refresh=False):
        values, columns = self._get_main_sheet_columns(force_refresh=force_refresh)
        name_col = columns.get('name_col')
        phone_col = columns.get('phone_col')
        if not values or name_col is None or phone_col is None:
            return {
                'added': 0,
                'updated': 0,
                'registry': self.get_client_registry(force_reload=True),
            }

        with self._clients_lock:
            registry = self._load_clients_from_disk()
            added, updated = import_sheet_phone_numbers_to_registry(values, name_col, phone_col, registry)
            if added or updated:
                registry = self._save_clients_to_disk(registry)

        return {
            'added': added,
            'updated': updated,
            'registry': registry,
        }

    def sync_clients_to_sheet_phone_column(self, force_refresh=False):
        values, columns = self._get_main_sheet_columns(force_refresh=force_refresh)
        name_col = columns.get('name_col')
        phone_col = columns.get('phone_col')
        if not values or name_col is None or phone_col is None:
            return {
                'updated_count': 0,
                'updates': [],
            }

        registry = self.get_client_registry(force_reload=True)
        updates = build_client_phone_sheet_updates(values, registry, name_col, phone_col)
        if updates:
            if not self._ensure_sheet_connection():
                raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')
            with self._sheet_lock:
                self.main_sheet.batch_update(updates, value_input_option='USER_ENTERED')

        return {
            'updated_count': len(updates),
            'updates': updates,
        }

    def apply_sheet_name_validation(self, name_list=None):
        if not self._ensure_sheet_connection():
            raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')

        values, columns = self._get_main_sheet_columns(force_refresh=False)
        name_col = columns.get('name_col')
        if not values or name_col is None:
            return {
                'validation_names_count': 0,
            }

        if not name_list:
            registry = self.get_client_registry(force_reload=True)
            name_list = [name for name, phone in registry.items() if str(phone or '').strip()]
            if not name_list:
                name_list = list(registry)
            if not name_list:
                name_list = compute_debtors(self.get_main_records(force_refresh=False)).get('client_names', [])

        if not name_list:
            return {
                'validation_names_count': 0,
            }

        sheet_id = self._extract_sheet_id(self.config.get('sheet_id', ''))
        request_body = {
            'requests': [{
                'setDataValidation': {
                    'range': {
                        'sheetId': self.main_sheet.id,
                        'startRowIndex': 1,
                        'startColumnIndex': name_col,
                        'endColumnIndex': name_col + 1,
                    },
                    'rule': {
                        'condition': {
                            'type': 'ONE_OF_LIST',
                            'values': [
                                {'userEnteredValue': value}
                                for value in sorted({str(item).strip().upper() for item in name_list if str(item).strip()})
                            ],
                        },
                        'showCustomUi': True,
                        'strict': False,
                    },
                }
            }]
        }

        with self._sheet_lock:
            self.sheets_api_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body=request_body,
            ).execute()

        return {
            'validation_names_count': len(request_body['requests'][0]['setDataValidation']['rule']['condition']['values']),
        }

    def apply_sheet_phone_autofill_formulas(self, force_refresh=False):
        values, columns = self._get_main_sheet_columns(force_refresh=force_refresh)
        name_col = columns.get('name_col')
        phone_col = columns.get('phone_col')
        if not values or name_col is None or phone_col is None:
            return {
                'autofill_rows': 0,
                'range': '',
            }

        if not self._ensure_sheet_connection():
            raise RuntimeError(self.sync_state.get('sheet_error') or 'Google Sheets connection unavailable')

        self.sync_client_directory_sheet(force_reload=True)
        formula_plan = build_phone_autofill_plan(
            values,
            name_col,
            phone_col,
            self.main_sheet.row_count,
            'CLIENT DIRECTORY',
        )
        if formula_plan.get('range') and formula_plan.get('values'):
            with self._sheet_lock:
                self.main_sheet.update(
                    formula_plan['range'],
                    formula_plan['values'],
                    value_input_option='USER_ENTERED',
                )

        return {
            'autofill_rows': len(formula_plan.get('values') or []),
            'range': formula_plan.get('range', ''),
        }

    def refresh_workspace(self, force_refresh=False):
        import_result = self.import_sheet_phone_numbers_to_clients(force_refresh=force_refresh)
        directory_result = self.sync_client_directory_sheet(force_reload=True)
        phone_update_result = self.sync_clients_to_sheet_phone_column(force_refresh=force_refresh)
        validation_result = self.apply_sheet_name_validation()
        autofill_result = self.apply_sheet_phone_autofill_formulas(force_refresh=force_refresh)
        pull_result = self.pull_once() if self.postgres_ready else {}

        return {
            'import_result': import_result,
            'directory_result': directory_result,
            'phone_update_result': phone_update_result,
            'validation_result': validation_result,
            'autofill_result': autofill_result,
            'pull_result': pull_result,
        }

    def upsert_client(self, name, phone, sync_sheet=True, force_refresh=False):
        validated = validate_client_entry(name, phone)
        if validated.get('error'):
            return validated

        with self._clients_lock:
            registry = self._load_clients_from_disk()
            added, changed, key = set_client_phone(validated['name'], validated['phone'], registry)
            registry = self._save_clients_to_disk(registry)

        sync_result = None
        if sync_sheet and (added or changed):
            sync_result = self._queue_client_sheet_sync(force_refresh=force_refresh, include_autofill=False)

        return {
            'added': added,
            'changed': changed,
            'key': key,
            'registry': registry,
            'sync_result': sync_result,
        }

    def delete_client(self, name, sync_sheet=True):
        with self._clients_lock:
            registry = self._load_clients_from_disk()
            existing_key = find_existing_client_key(name, registry)
            if not existing_key:
                return {'error': 'Client not found.'}
            registry.pop(existing_key, None)
            registry = self._save_clients_to_disk(registry)

        sync_result = None
        if sync_sheet:
            sync_result = self._queue_client_sheet_sync(force_refresh=False, include_autofill=True)

        return {
            'deleted': True,
            'key': existing_key,
            'registry': registry,
            'sync_result': sync_result,
        }

    def checkout_sale_cart(self, items, force_refresh=False, sold_by=''):
        cart_items = list(items or [])
        if not cart_items:
            return {'error': 'Add at least one phone to the cart before checking out.'}

        queued_operation_ids = []

        stock_values, stock_header_row_idx, stock_headers, stock_headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)
        main_values = self.get_main_values(force_refresh=force_refresh)
        if not main_values:
            return {'error': 'Main inventory sheet is empty.'}

        main_header_row_idx = detect_sheet_header_row(main_values)
        main_headers = [str(cell or '').strip() for cell in (main_values[main_header_row_idx] if main_header_row_idx < len(main_values) else [])]
        main_headers_upper = [header.upper() for header in main_headers]
        if not main_headers:
            return {'error': 'Main inventory headers are missing.'}

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return {'error': 'Stock sheet ID is missing.'}

        today_text = datetime.now().strftime('%m/%d/%Y')
        time_text = datetime.now().strftime('%H:%M')
        next_main_row = find_next_table_write_row(main_values, main_header_row_idx)
        next_sun_serial = max(1, next_main_row - (main_header_row_idx + 1))

        # Check if we need to add separator rows for new day
        last_row_date = None
        date_col = svc_stock_header_index(main_headers_upper, 'DATE')
        if date_col is not None:
            for i in range(len(main_values) - 1, main_header_row_idx, -1):
                row = main_values[i]
                if len(row) > date_col and str(row[date_col] or '').strip():
                    last_row_date = str(row[date_col]).strip()
                    break

        if last_row_date and last_row_date != today_text:
            # Add 3 empty rows as separator
            empty_row_values = [''] * len(main_headers)
            for _ in range(3):
                queue_id = self._enqueue_db_first_operation(
                    'sales',
                    'main_write_row',
                    {
                        'kind': 'main_write_row',
                        'row': next_main_row,
                        'row_values': empty_row_values,
                    },
                    cache_apply_callable=lambda row=next_main_row, values=empty_row_values: self.postgres_sync_manager.replace_cached_table_row('main_values', row, values),
                )
                queued_operation_ids.append(queue_id)
                next_main_row += 1
                next_sun_serial += 1
        item_results = []

        name_of_buyer_col = svc_stock_header_index(stock_headers_upper, 'NAME OF BUYER')
        phone_of_buyer_col = svc_stock_header_index(stock_headers_upper, 'PHONE NUMBER OF BUYER')
        availability_col = svc_stock_header_index(stock_headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
        product_status_col = svc_stock_header_index(stock_headers_upper, 'PRODUCT STATUS', 'STATUS OF DEVICE', 'STOCK STATUS', 'ITEM STATUS')
        description_col = svc_stock_header_index(stock_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
        imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')
        main_name_col = svc_stock_header_index(main_headers_upper, 'NAME')
        main_imei_col = svc_stock_header_index(main_headers_upper, 'IMEI')
        main_status_col = svc_stock_header_index(main_headers_upper, 'STATUS')
        main_paid_col = svc_stock_header_index(main_headers_upper, 'AMOUNT PAID')
        main_price_col = svc_stock_header_index(main_headers_upper, 'PRICE')
        main_record_id_col = svc_stock_header_index(main_headers_upper, 'RECORD_ID', 'RECORD ID')
        cost_price_col = svc_stock_header_index(stock_headers_upper, 'COST PRICE', 'COST', 'BUYING PRICE')
        sold_by_text = str(sold_by or '').strip()

        for item in cart_items:
            row_num = int(item.get('stock_row_num') or 0)
            if row_num <= stock_header_row_idx + 1 or row_num > len(stock_values):
                return {'error': f'Stock row {row_num} is no longer available.'}

            stock_row = list(stock_values[row_num - 1])
            padded_stock_row = stock_row + [''] * max(0, len(stock_headers) - len(stock_row))
            description = padded_stock_row[description_col] if description_col is not None and description_col < len(padded_stock_row) else ''
            imei = padded_stock_row[imei_col] if imei_col is not None and imei_col < len(padded_stock_row) else ''
            cost_price_at_sale = clean_amount(
                padded_stock_row[cost_price_col]
                if cost_price_col is not None and cost_price_col < len(padded_stock_row)
                else 0
            )

            buyer_name = str(item.get('buyer_name') or '').strip().upper()
            buyer_phone = normalize_phone_number(item.get('buyer_phone') or '')
            sale_price = clean_amount(item.get('sale_price'))
            raw_amount_paid = clean_amount(item.get('amount_paid'))
            phone_expense = clean_amount(item.get('phone_expense'))
            stock_status_choice = str(item.get('stock_status') or 'sold').strip()
            inventory_status = str(item.get('inventory_status') or 'UNPAID').strip().upper()
            availability_override = str(item.get('availability_value') or '').strip()

            if not buyer_name:
                return {'error': f'Buyer name is required for stock row {row_num}.'}
            if sale_price <= 0:
                return {'error': f'Enter a valid sale price for stock row {row_num}.'}
            if not description:
                return {'error': f'Stock row {row_num} is missing a description.'}

            # Payment status is derived automatically from paid amount vs sold amount.
            if raw_amount_paid < 0:
                raw_amount_paid = 0
            if raw_amount_paid > sale_price:
                return {'error': f'Amount paid cannot be greater than amount sold for stock row {row_num}.'}
            if raw_amount_paid <= 0:
                inventory_status = 'UNPAID'
            elif raw_amount_paid < sale_price:
                inventory_status = 'PART PAYMENT'
            else:
                inventory_status = 'PAID'

            stock_status_choice = 'Sold' if inventory_status == 'PAID' else 'Pending Deal'

            if inventory_status not in {'PAID', 'UNPAID', 'PART PAYMENT', 'RETURNED'}:
                inventory_status = 'UNPAID'

            effective_stock_status = 'available' if inventory_status == 'RETURNED' else stock_status_choice
            status_key, fill_color = map_sale_status(effective_stock_status)
            availability_value = ''
            if inventory_status == 'RETURNED':
                availability_value = ''
            elif availability_override == '__CLEAR__':
                availability_value = ''
            elif availability_override:
                availability_value = availability_override
            elif status_key == 'sold':
                availability_value = today_text
            elif status_key == 'pending':
                availability_value = 'PENDING DEAL'

            stock_cell_updates = []
            if name_of_buyer_col is not None:
                stock_cell_updates.append({'col': name_of_buyer_col + 1, 'value': '' if inventory_status == 'RETURNED' else buyer_name})
            if phone_of_buyer_col is not None:
                stock_cell_updates.append({'col': phone_of_buyer_col + 1, 'value': '' if inventory_status == 'RETURNED' else buyer_phone})
            if availability_col is not None:
                stock_cell_updates.append({'col': availability_col + 1, 'value': availability_value})
            if product_status_col is not None:
                stock_cell_updates.append({'col': product_status_col + 1, 'value': stock_status_key_to_label(status_key)})

            for update in stock_cell_updates:
                queue_id = self._enqueue_db_first_operation(
                    'stock',
                    'stock_update_cell',
                    {
                        'kind': 'stock_update_cell',
                        'stock_sheet_id': stock_sheet_id,
                        'row': row_num,
                        'col': update['col'],
                        'value': update['value'],
                    },
                    cache_apply_callable=lambda rn=row_num, cn=update['col'], nv=update['value']: self.postgres_sync_manager.update_cached_stock_value(rn, cn, nv),
                )
                queued_operation_ids.append(queue_id)

            if description_col is not None:
                request_body = {
                    'requests': [{
                        'repeatCell': {
                            'range': {
                                'sheetId': self._resolve_stock_worksheet(stock_sheet_id).id,
                                'startRowIndex': row_num - 1,
                                'endRowIndex': row_num,
                                'startColumnIndex': description_col,
                                'endColumnIndex': description_col + 1,
                            },
                            'cell': {'userEnteredFormat': {'backgroundColor': fill_color}},
                            'fields': 'userEnteredFormat.backgroundColor',
                        }
                    }]
                }
                queue_id = self._enqueue_db_first_operation(
                    'stock',
                    'stock_batch_update',
                    {
                        'kind': 'stock_batch_update',
                        'stock_sheet_id': stock_sheet_id,
                        'request_body': request_body,
                    },
                    cache_apply_callable=lambda rn=row_num, sk=status_key: self._update_cached_stock_status(rn, sk),
                )
                queued_operation_ids.append(queue_id)

            updated_existing_row = None
            imei_text = str(imei or '').strip()
            if (
                inventory_status == 'PAID'
                and imei_text
                and main_status_col is not None
                and main_paid_col is not None
                and main_imei_col is not None
                and main_name_col is not None
            ):
                for index in range(len(main_values) - 1, main_header_row_idx, -1):
                    row = main_values[index] if index < len(main_values) else []
                    if main_imei_col >= len(row) or main_name_col >= len(row):
                        continue
                    existing_imei = str(row[main_imei_col] or '').strip()
                    existing_name = str(row[main_name_col] or '').strip().upper()
                    existing_status = str(row[main_status_col] or '').strip().upper() if main_status_col < len(row) else ''
                    if existing_imei == imei_text and existing_name == buyer_name and existing_status in {'UNPAID', 'PART PAYMENT'}:
                        updated_existing_row = index + 1
                        break

            if updated_existing_row is not None:
                existing_record_id = ''
                existing_row = main_values[updated_existing_row - 1] if updated_existing_row - 1 < len(main_values) else []
                if (
                    main_record_id_col is not None
                    and isinstance(existing_row, list)
                    and main_record_id_col < len(existing_row)
                ):
                    existing_record_id = str(existing_row[main_record_id_col] or '').strip()

                queue_status = self._enqueue_db_first_operation(
                    'sales',
                    'main_update_status',
                    {
                        'kind': 'main_update_cell',
                        'row': updated_existing_row,
                        'col': main_status_col + 1,
                        'value': 'PAID',
                    },
                    cache_apply_callable=lambda rn=updated_existing_row, cn=main_status_col + 1: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, 'PAID'),
                )
                queued_operation_ids.append(queue_status)

                queue_paid = self._enqueue_db_first_operation(
                    'sales',
                    'main_update_paid',
                    {
                        'kind': 'main_update_cell',
                        'row': updated_existing_row,
                        'col': main_paid_col + 1,
                        'value': raw_amount_paid,
                    },
                    cache_apply_callable=lambda rn=updated_existing_row, cn=main_paid_col + 1, nv=raw_amount_paid: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, nv),
                )
                queued_operation_ids.append(queue_paid)

                if main_price_col is not None:
                    queue_price = self._enqueue_db_first_operation(
                        'sales',
                        'main_update_price',
                        {
                            'kind': 'main_update_cell',
                            'row': updated_existing_row,
                            'col': main_price_col + 1,
                            'value': sale_price,
                        },
                        cache_apply_callable=lambda rn=updated_existing_row, cn=main_price_col + 1, nv=sale_price: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, nv),
                    )
                    queued_operation_ids.append(queue_price)

                item_results.append({
                    'stock_row_num': row_num,
                    'inventory_row_num': updated_existing_row,
                    'stock_record_id': existing_record_id,
                    'buyer_name': buyer_name,
                    'buyer_phone': buyer_phone,
                    'sale_price': sale_price,
                    'stock_status': status_key.upper(),
                    'inventory_status': 'PAID',
                    'description': description,
                    'imei': imei,
                    'mode': 'updated_existing',
                })

                if status_key == 'sold' and inventory_status != 'RETURNED':
                    fallback_record_id = f'legacy-main-row-{updated_existing_row}'
                    self._safe_record_sale_ledger_entry(
                        stock_record_id=existing_record_id or fallback_record_id,
                        stock_row_num=row_num,
                        selling_price=sale_price,
                        cost_price_at_sale=cost_price_at_sale,
                        quantity=1,
                        date=datetime.now(timezone.utc),
                        sold_by=sold_by_text,
                    )
            else:
                amount_paid = raw_amount_paid
                record_id = uuid.uuid4().hex
                values_by_header = {
                    'DATE': today_text,
                    'TIME': time_text,
                    'NAME': buyer_name,
                    'DESCRIPTION': description,
                    'IMEI': imei,
                    'PHONE NUMBER': buyer_phone,
                    'PRICE': sale_price,
                    'AMOUNT PAID': amount_paid,
                    'STATUS': inventory_status,
                    'RECORD_ID': record_id,
                    'SUN S/N': str(next_sun_serial),
                }
                main_row_values = self._build_sheet_row_values(main_headers, values_by_header)
                main_record = self._build_sheet_record(main_headers, main_row_values)
                queue_id = self._enqueue_db_first_operation(
                    'sales',
                    'main_write_row',
                    {
                        'kind': 'main_write_row',
                        'row': next_main_row,
                        'row_values': main_row_values,
                    },
                    cache_apply_callable=lambda row=next_main_row, values=main_row_values, record=main_record: (
                        self.postgres_sync_manager.replace_cached_table_row('main_values', row, values),
                        self.postgres_sync_manager.append_cached_dict_row('main_records', record),
                    ),
                )
                queued_operation_ids.append(queue_id)

                item_results.append({
                    'stock_row_num': row_num,
                    'inventory_row_num': next_main_row,
                    'stock_record_id': record_id,
                    'buyer_name': buyer_name,
                    'buyer_phone': buyer_phone,
                    'sale_price': sale_price,
                    'stock_status': status_key.upper(),
                    'inventory_status': inventory_status,
                    'description': description,
                    'imei': imei,
                    'mode': 'appended',
                })

                if status_key == 'sold' and inventory_status != 'RETURNED':
                    self._safe_record_sale_ledger_entry(
                        stock_record_id=record_id,
                        stock_row_num=row_num,
                        selling_price=sale_price,
                        cost_price_at_sale=cost_price_at_sale,
                        quantity=1,
                        date=datetime.now(timezone.utc),
                        sold_by=sold_by_text,
                    )

            if phone_expense > 0:
                try:
                    self.append_cashflow_expense_record(
                        amount=phone_expense,
                        category='PHONE SALE EXPENSE',
                        description=str(description or '').strip(),
                        date_text=today_text,
                        created_by=buyer_name,
                    )
                except Exception as cashflow_exc:
                    self.logger.warning('Failed to write phone sale expense to cashflow sheet: %s', cashflow_exc)

                next_main_row += 1
                next_sun_serial += 1

        return {
            'processed_count': len(item_results),
            'items': item_results,
            'queued_operation_ids': queued_operation_ids,
        }

    def get_live_name_mismatches(self, force_refresh=False):
        values = self.get_main_values(force_refresh=force_refresh)
        registry = self.get_client_registry(force_reload=True)
        known_names = list(registry)
        if not known_names:
            known_names = compute_debtors(self.get_main_records(force_refresh=force_refresh)).get('client_names', [])

        mismatches = find_name_mismatches(values, known_names)
        return {
            'mismatches': mismatches,
            'known_names_count': len(known_names),
            'count': len(mismatches),
        }

    def apply_name_fix(self, mismatch_entry, correct_name, force_refresh=False):
        values = self.get_main_values(force_refresh=force_refresh)
        updates = build_name_fix_updates(values, mismatch_entry, correct_name)
        if not updates:
            return {'error': 'No matching rows still need that fix.'}

        header = values[0] if values else []
        queue_ids = []
        for row_number, col_number, value in updates:
            field_name = header[col_number - 1] if col_number - 1 < len(header) else 'NAME'
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'name_fix',
                    'main_update_name',
                    {'kind': 'main_update_cell', 'row': row_number, 'col': col_number, 'value': value},
                    cache_apply_callable=lambda dr=row_number - 1, fn=field_name, r=row_number, c=col_number, nv=value: (
                        self.postgres_sync_manager.update_cached_main_record_field(dr, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', r, c, nv),
                    ),
                )
            )

        return {
            'updated_count': len(updates),
            'queued_operation_ids': queue_ids,
            'updates': updates,
        }

    def apply_name_fix_all(self, mismatch_entries, force_refresh=False):
        values = self.get_main_values(force_refresh=force_refresh)
        updates = build_name_fix_all_updates(values, mismatch_entries)
        if not updates:
            return {'error': 'No automatic name fixes are currently available.'}

        header = values[0] if values else []
        queue_ids = []
        for row_number, col_number, value in updates:
            field_name = header[col_number - 1] if col_number - 1 < len(header) else 'NAME'
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'name_fix',
                    'main_update_name',
                    {'kind': 'main_update_cell', 'row': row_number, 'col': col_number, 'value': value},
                    cache_apply_callable=lambda dr=row_number - 1, fn=field_name, r=row_number, c=col_number, nv=value: (
                        self.postgres_sync_manager.update_cached_main_record_field(dr, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', r, c, nv),
                    ),
                )
            )

        return {
            'updated_count': len(updates),
            'queued_operation_ids': queue_ids,
            'updates': updates,
        }

    def _apply_payment_action_rows(self, action, use_new_values):
        if not action or not action.get('rows'):
            return {'error': 'No payment action available.'}

        values = self.get_main_values(force_refresh=False)
        header = values[0] if values else []
        queue_ids = []

        for row in action.get('rows', []):
            row_idx = int(row.get('row_idx', 0))
            paid_col = int(row.get('paid_col', 0))
            status_col = int(row.get('status_col', 0))
            paid_field_name = header[paid_col] if paid_col < len(header) else 'Amount paid'
            status_field_name = header[status_col] if status_col < len(header) else 'STATUS'
            paid_value = row.get('new_paid') if use_new_values else row.get('old_paid')
            status_value = row.get('new_status') if use_new_values else row.get('old_status', '')

            queue_ids.append(
                self._enqueue_db_first_operation(
                    'payment',
                    'main_update_paid',
                    {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': paid_col + 1, 'value': paid_value},
                    cache_apply_callable=lambda ri=row_idx + 1, fn=paid_field_name, col=paid_col + 1, nv=paid_value: (
                        self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', ri, col, nv),
                    ),
                )
            )

            queue_ids.append(
                self._enqueue_db_first_operation(
                    'payment',
                    'main_update_status',
                    {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': status_col + 1, 'value': status_value},
                    cache_apply_callable=lambda ri=row_idx + 1, fn=status_field_name, col=status_col + 1, nv=status_value: (
                        self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', ri, col, nv),
                    ),
                )
            )

        try:
            self.replay_pending_queue_now(limit=120)
        except Exception:
            pass

        return {
            'queued_operation_ids': queue_ids,
            'updates_count': len(action.get('rows', [])),
            'customer': action.get('customer', ''),
        }

    def apply_payment(self, name_input, payment_amount, manual_service_row_idx=None, force_refresh=False):
        values = self.get_main_values(force_refresh=force_refresh)
        if not values:
            return {'error': 'No data in sheet.'}

        plan = build_payment_plan(
            name_input,
            payment_amount,
            values,
            manual_service_row_idx=manual_service_row_idx,
        )
        if plan.get('error'):
            return plan

        paid_col = plan['columns']['paid_col']
        status_col = plan['columns']['status_col']
        headers_upper = [str(cell or '').strip().upper() for cell in (values[0] if values else [])]
        name_col = svc_stock_header_index(headers_upper, 'NAME', 'CLIENT NAME', 'CUSTOMER NAME')
        description_col = svc_stock_header_index(headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
        imei_col = svc_stock_header_index(headers_upper, 'IMEI')
        price_col = svc_stock_header_index(headers_upper, 'PRICE', 'AMOUNT SOLD', 'SELLING PRICE')
        cost_col = svc_stock_header_index(headers_upper, 'COST PRICE', 'COST')
        paid_field_name = values[0][paid_col] if paid_col < len(values[0]) else 'Amount paid'
        status_field_name = values[0][status_col] if status_col < len(values[0]) else 'STATUS'
        queue_ids = []

        for item in plan['updates']:
            row_idx = item['row_idx']
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'payment',
                    'main_update_paid',
                    {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': paid_col + 1, 'value': item['new_paid']},
                    cache_apply_callable=lambda ri=row_idx + 1, fn=paid_field_name, col=paid_col + 1, nv=item['new_paid']: (
                        self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', ri, col, nv),
                    ),
                )
            )

            if item['new_status']:
                queue_ids.append(
                    self._enqueue_db_first_operation(
                        'payment',
                        'main_update_status',
                        {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': status_col + 1, 'value': item['new_status']},
                        cache_apply_callable=lambda ri=row_idx + 1, fn=status_field_name, col=status_col + 1, nv=item['new_status']: (
                            self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                            self.postgres_sync_manager.update_cached_table_value('main_values', ri, col, nv),
                        ),
                    )
                )

        try:
            self.replay_pending_queue_now(limit=120)
        except Exception:
            pass

        # Mirror payment recognition to cashflow sheet using payment date (today).
        try:
            payment_date_iso = datetime.now(timezone.utc).date().isoformat()
            for item in plan.get('updates', []):
                if str(item.get('new_status') or '').strip().upper() != 'PAID':
                    continue

                row_idx = int(item.get('row_idx'))
                if row_idx < 0 or row_idx >= len(values):
                    continue

                row_values = values[row_idx] if row_idx < len(values) else []
                customer_name = str(row_values[name_col] if name_col is not None and name_col < len(row_values) else name_input or '').strip().upper()
                description_text = str(row_values[description_col] if description_col is not None and description_col < len(row_values) else '').strip().upper()
                imei_value = str(row_values[imei_col] if imei_col is not None and imei_col < len(row_values) else '').strip()
                sale_amount = clean_amount(row_values[price_col]) if price_col is not None and price_col < len(row_values) else clean_amount(item.get('new_paid'))
                cost_amount = clean_amount(row_values[cost_col]) if cost_col is not None and cost_col < len(row_values) else 0

                entry_type = 'phone' if imei_value else 'service'
                cashflow_description = description_text or imei_value
                realized_amount = sale_amount if entry_type == 'service' else max(0.0, sale_amount - max(0.0, cost_amount))
                updated_row = self.mark_cashflow_income_paid(
                    entry_type=entry_type,
                    description=cashflow_description,
                    created_by=customer_name,
                    payment_date_text=payment_date_iso,
                    amount=realized_amount if realized_amount > 0 else None,
                    cost_price=cost_amount if entry_type == 'phone' and cost_amount > 0 else None,
                )
                if updated_row is None and realized_amount > 0 and not self.has_cashflow_income_paid_record(
                    entry_type=entry_type,
                    description=cashflow_description,
                    created_by=customer_name,
                    payment_date_text=payment_date_iso,
                ):
                    self.append_cashflow_income_record(
                        amount=realized_amount,
                        category='PHONE PROFIT' if entry_type == 'phone' else 'SERVICE PROFIT',
                        description=cashflow_description,
                        date_text=payment_date_iso,
                        created_by=customer_name or 'payment',
                        payment_status='PAID',
                        entry_type=entry_type,
                        cost_price=cost_amount if entry_type == 'phone' and cost_amount > 0 else '',
                        payment_date_text=payment_date_iso,
                    )
        except Exception as cashflow_exc:
            self.logger.warning('Failed to sync apply_payment to cashflow rows: %s', cashflow_exc)

        self.last_payment_action = self._clone_payment_action({
            'customer': plan.get('name_input', ''),
            'rows': plan.get('undo_rows', []),
        })
        self.last_undone_payment_action = None

        return {
            'status_text': plan['status_text'],
            'total_applied': plan['total_applied'],
            'queued_operation_ids': queue_ids,
            'updates_count': len(plan['updates']),
            'undo_available': bool(self.last_payment_action and self.last_payment_action.get('rows')),
            'redo_available': False,
        }

    def update_main_record_fields(self, record_idx, updates_by_header, force_refresh=False):
        values = self.get_main_values(force_refresh=force_refresh)
        if not values:
            return {'error': 'No data in sheet.'}

        header_row_idx = detect_sheet_header_row(values)
        if header_row_idx < 0 or header_row_idx >= len(values):
            return {'error': 'Sheet header row not found.'}

        headers = [str(cell or '').strip() for cell in (values[header_row_idx] if header_row_idx < len(values) else [])]
        if not headers:
            return {'error': 'Sheet headers are missing.'}

        try:
            record_idx = int(record_idx)
        except Exception:
            return {'error': 'Invalid record row.'}

        if record_idx <= 0:
            return {'error': 'Invalid record row.'}

        sheet_row = header_row_idx + 1 + record_idx
        queue_ids = []

        for header_name, raw_value in (updates_by_header or {}).items():
            header_text = str(header_name or '').strip()
            if not header_text:
                continue

            matched_col = None
            for index, header in enumerate(headers):
                if str(header or '').strip().upper() == header_text.upper():
                    matched_col = index + 1
                    header_text = header
                    break

            if matched_col is None:
                return {'error': f'Column not found: {header_name}.'}

            value = raw_value
            queue_ids.append(
                self._enqueue_db_first_operation(
                    'billing_service_update',
                    'main_update_cell',
                    {
                        'kind': 'main_update_cell',
                        'row': sheet_row,
                        'col': matched_col,
                        'value': value,
                    },
                    cache_apply_callable=lambda r=sheet_row, fn=header_text, c=matched_col, nv=value: (
                        self.postgres_sync_manager.update_cached_main_record_field(r, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', r, c, nv),
                    ),
                )
            )

        return {
            'queued_operation_ids': queue_ids,
            'updates_count': len(queue_ids),
            'row_num': sheet_row,
        }

    def undo_last_payment(self):
        result = self._apply_payment_action_rows(self.last_payment_action, use_new_values=False)
        if result.get('error'):
            return result

        customer = result.get('customer') or 'customer'
        self.last_undone_payment_action = self._clone_payment_action(self.last_payment_action)
        self.last_payment_action = None
        return {
            'status_text': f'Last payment action undone for {customer}.',
            'queued_operation_ids': result['queued_operation_ids'],
            'updates_count': result['updates_count'],
            'undo_available': False,
            'redo_available': bool(self.last_undone_payment_action and self.last_undone_payment_action.get('rows')),
        }

    def redo_last_payment(self):
        result = self._apply_payment_action_rows(self.last_undone_payment_action, use_new_values=True)
        if result.get('error'):
            return result

        customer = result.get('customer') or 'customer'
        self.last_payment_action = self._clone_payment_action(self.last_undone_payment_action)
        self.last_undone_payment_action = None
        return {
            'status_text': f'Last undone payment reapplied for {customer}.',
            'queued_operation_ids': result['queued_operation_ids'],
            'updates_count': result['updates_count'],
            'undo_available': bool(self.last_payment_action and self.last_payment_action.get('rows')),
            'redo_available': False,
        }

    def get_sync_status(self):
        snapshot = self.postgres_sync_manager.get_sync_snapshot() if self.postgres_ready else {
            'ready': False,
            'pull_interval_sec': int(self.config.get('sync_pull_interval_sec', 90) or 90),
            'cache_counts': {},
            'latest_pull': None,
            'latest_error': None,
        }

        queue_pending = 0
        if self.postgres_ready:
            try:
                queue_pending = len(self.postgres_sync_manager.fetch_pending_operations(limit=200))
            except Exception as exc:
                self.logger.warning('Failed to fetch pending queue operations: %s', exc)

        return {
            'sync_state': dict(self.sync_state),
            'postgres_driver_available': PSYCOPG2_AVAILABLE,
            'sheets_connected': bool(self.sync_state.get('sheets_connected')),
            'sheet_id': self._extract_sheet_id(self.config.get('sheet_id', '')),
            'stock_sheet_id': self._resolve_stock_sheet_id(),
            'postgres_snapshot': snapshot,
            'queue_pending': queue_pending,
        }