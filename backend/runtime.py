import base64
import json
import logging
import mimetypes
import os
import threading
import uuid
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build

from db_sync import PSYCOPG2_AVAILABLE, create_postgres_sync_manager
from services.billing_service import build_payment_plan, clean_amount, compute_debtors
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

        return config

    def _load_service_account_credentials(self, scopes):
        raw_json = (os.environ.get('GOOGLE_CREDS_JSON') or '').strip()
        if not raw_json:
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
        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        if 'COST PRICE' in headers_upper:
            return values, header_row_idx, headers, headers_upper, False

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return values, header_row_idx, headers, headers_upper, False

        worksheet = self._resolve_stock_worksheet(stock_sheet_id)
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

        with self._sheet_lock:
            self.sheets_api_service.spreadsheets().batchUpdate(
                spreadsheetId=stock_sheet_id,
                body=request_body,
            ).execute()
            worksheet.update_cell(header_row_idx + 1, insert_index + 1, 'COST PRICE')
            values = worksheet.get_all_values()

        if self.postgres_ready:
            try:
                self.postgres_sync_manager.upsert_sheet_cache('stock_values', values)
            except Exception as exc:
                self.logger.warning('Failed to cache stock sheet after COST PRICE insert: %s', exc)

        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        return values, header_row_idx, headers, headers_upper, True

    def _ensure_stock_product_status_column(self, force_refresh=False):
        values = self.get_stock_values(force_refresh=force_refresh)
        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        if 'PRODUCT STATUS' in headers_upper:
            return values, header_row_idx, headers, headers_upper, False

        stock_sheet_id = self._resolve_stock_sheet_id()
        if not stock_sheet_id:
            return values, header_row_idx, headers, headers_upper, False

        worksheet = self._resolve_stock_worksheet(stock_sheet_id)
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

        with self._sheet_lock:
            self.sheets_api_service.spreadsheets().batchUpdate(
                spreadsheetId=stock_sheet_id,
                body=request_body,
            ).execute()
            worksheet.update_cell(header_row_idx + 1, insert_index + 1, 'PRODUCT STATUS')
            values = worksheet.get_all_values()

        header_row_idx, headers, headers_upper = detect_stock_headers(values)
        status_col = svc_stock_header_index(headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
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

        status_col = svc_stock_header_index(headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
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

            inv_entry = by_pair.get((imei_value, buyer_value)) if buyer_value else None
            if inv_entry is None:
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
        if not self.postgres_sync_manager.ready:
            self.sync_state['last_status'] = 'dsn_missing'
            self.sync_state['last_error'] = 'postgres_dsn is empty'
            return

        try:
            self.postgres_sync_manager.ensure_schema()
            self.sync_state['ready'] = True
            self.sync_state['last_status'] = 'running'
            threading.Thread(target=self._seed_once_async, daemon=True).start()
            self.postgres_sync_manager.start_background_pull(self.pull_once)
            self.postgres_sync_manager.start_background_queue_worker(self._replay_queue_operation, interval_sec=20)
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
            raise RuntimeError('PostgreSQL sync is not ready. Configure postgres_dsn or POSTGRES_DSN before DB-first API writes.')

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

        # Fast-path replay keeps user-facing writes (like payments) in sync with Sheets
        # even if the background queue worker is delayed.
        try:
            if self._ensure_sheet_connection():
                self._replay_queue_operation({'payload_json': payload})
                self.postgres_sync_manager.mark_operation_done(queue_id)
        except Exception as exc:
            try:
                self.postgres_sync_manager.mark_operation_failed(queue_id, f'Immediate replay failed: {exc}')
            except Exception:
                pass
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
        if not force_refresh:
            cached = self._load_cached_rows('main_values')
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
            return []

        with self._sheet_lock:
            values = self.main_sheet.get_all_values()
        if self.postgres_ready:
            try:
                self.postgres_sync_manager.upsert_sheet_cache('main_values', values)
            except Exception as exc:
                self.logger.warning('Failed to upsert main_values fallback cache: %s', exc)
        return values

    def get_stock_values(self, force_refresh=False):
        if not force_refresh:
            cached = self._load_cached_rows('stock_values')
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
            return []

        with self._sheet_lock:
            values = self._resolve_stock_worksheet(stock_sheet_id).get_all_values()
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
                oauth_file = str(self.config.get('contacts_oauth_file', '')).strip()
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
            'cost_price_inserted': inserted_cost_price,
            'product_status_inserted': inserted_product_status,
        }

    def add_stock_record(self, values_by_header, force_refresh=False):
        values, header_row_idx, headers, headers_upper, _, _ = self._ensure_stock_required_columns(force_refresh=force_refresh)
        normalized_values = dict(values_by_header or {})
        status_col = svc_stock_header_index(headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
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
        # Always refresh before service writes so target_row is computed from the latest inventory state.
        main_values = self.get_main_values(force_refresh=True)
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

        # Push queued writes immediately so the inventory sheet reflects new services without delay.
        try:
            self.replay_pending_queue_now(limit=100)
        except Exception:
            pass

        return {
            'queued_operation_id': queue_id,
            'target_row': target_row,
            'row_values': row_values,
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

        status_col = svc_stock_header_index(stock_headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
        buyer_col = svc_stock_header_index(stock_headers_upper, 'NAME OF BUYER')
        phone_col = svc_stock_header_index(stock_headers_upper, 'PHONE NUMBER OF BUYER')
        availability_col = svc_stock_header_index(stock_headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
        desc_col = svc_stock_header_index(stock_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
        imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')

        buyer_name_value = padded[buyer_col] if buyer_col is not None and buyer_col < len(padded) else ''
        buyer_phone_value = padded[phone_col] if phone_col is not None and phone_col < len(padded) else ''
        description_value = padded[desc_col] if desc_col is not None and desc_col < len(padded) else ''
        imei_value = padded[imei_col] if imei_col is not None and imei_col < len(padded) else ''

        main_values = self.get_main_values(force_refresh=False)
        main_header_row_idx = detect_sheet_header_row(main_values)
        main_headers = [str(cell or '').strip() for cell in (main_values[main_header_row_idx] if main_header_row_idx < len(main_values) else [])]
        main_headers_upper = [header.upper() for header in main_headers]

        updates = []
        if status_col is not None:
            updates.append({'col': status_col + 1, 'value': 'AVAILABLE'})
        if buyer_col is not None:
            updates.append({'col': buyer_col + 1, 'value': ''})
        if phone_col is not None:
            updates.append({'col': phone_col + 1, 'value': ''})
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

        if imei_value and main_headers:
            main_imei_col = svc_stock_header_index(main_headers_upper, 'IMEI')
            main_status_col = svc_stock_header_index(main_headers_upper, 'STATUS')

            if main_imei_col is not None and main_status_col is not None:
                latest_row_num = None
                for index in range(len(main_values) - 1, main_header_row_idx, -1):
                    row = main_values[index] if index < len(main_values) else []
                    if main_imei_col >= len(row):
                        continue
                    if str(row[main_imei_col] or '').strip() != str(imei_value).strip():
                        continue
                    latest_row_num = index + 1
                    break

                if latest_row_num is not None:
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

        if main_headers:
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
        try:
            self.replay_pending_queue_now(limit=150)
        except Exception:
            pass

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
        imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')
        product_status_col = svc_stock_header_index(stock_headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
        availability_col = svc_stock_header_index(stock_headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
        description_col = svc_stock_header_index(stock_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')

        buyer_name = str(padded_stock_row[buyer_col] if buyer_col is not None and buyer_col < len(padded_stock_row) else '').strip().upper()
        imei_value = str(padded_stock_row[imei_col] if imei_col is not None and imei_col < len(padded_stock_row) else '').strip()

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

        main_values = self.get_main_values(force_refresh=False)
        main_header_row_idx = detect_sheet_header_row(main_values)
        main_headers = [str(cell or '').strip() for cell in (main_values[main_header_row_idx] if main_header_row_idx < len(main_values) else [])]
        main_headers_upper = [header.upper() for header in main_headers]
        main_name_col = svc_stock_header_index(main_headers_upper, 'NAME')
        main_imei_col = svc_stock_header_index(main_headers_upper, 'IMEI')
        main_status_col = svc_stock_header_index(main_headers_upper, 'STATUS')
        main_paid_col = svc_stock_header_index(main_headers_upper, 'AMOUNT PAID')
        main_price_col = svc_stock_header_index(main_headers_upper, 'PRICE')

        matched_inventory_row = None
        matched_row_values = []
        if main_values and main_status_col is not None:
            for index in range(len(main_values) - 1, main_header_row_idx, -1):
                row = main_values[index] if index < len(main_values) else []
                if not row:
                    continue
                row_imei = str(row[main_imei_col] or '').strip() if main_imei_col is not None and main_imei_col < len(row) else ''
                row_name = str(row[main_name_col] or '').strip().upper() if main_name_col is not None and main_name_col < len(row) else ''
                row_status = str(row[main_status_col] or '').strip().upper() if main_status_col < len(row) else ''
                if row_status == 'RETURNED':
                    continue
                if imei_value and row_imei and row_imei != imei_value:
                    continue
                if buyer_name and row_name and row_name != buyer_name:
                    continue
                matched_inventory_row = index + 1
                matched_row_values = row
                break

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
                has_explicit_amount = amount_paid is not None and str(amount_paid).strip() != ''
                explicit_amount = clean_amount(amount_paid) if has_explicit_amount else None
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

    def checkout_sale_cart(self, items, force_refresh=False):
        cart_items = list(items or [])
        if not cart_items:
            return {'error': 'Add at least one phone to the cart before checking out.'}

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
        item_results = []
        queued_operation_ids = []

        name_of_buyer_col = svc_stock_header_index(stock_headers_upper, 'NAME OF BUYER')
        phone_of_buyer_col = svc_stock_header_index(stock_headers_upper, 'PHONE NUMBER OF BUYER')
        availability_col = svc_stock_header_index(stock_headers_upper, 'AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
        product_status_col = svc_stock_header_index(stock_headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
        description_col = svc_stock_header_index(stock_headers_upper, 'DESCRIPTION', 'MODEL', 'DESC')
        imei_col = svc_stock_header_index(stock_headers_upper, 'IMEI')
        main_name_col = svc_stock_header_index(main_headers_upper, 'NAME')
        main_imei_col = svc_stock_header_index(main_headers_upper, 'IMEI')
        main_status_col = svc_stock_header_index(main_headers_upper, 'STATUS')
        main_paid_col = svc_stock_header_index(main_headers_upper, 'AMOUNT PAID')
        main_price_col = svc_stock_header_index(main_headers_upper, 'PRICE')

        for item in cart_items:
            row_num = int(item.get('stock_row_num') or 0)
            if row_num <= stock_header_row_idx + 1 or row_num > len(stock_values):
                return {'error': f'Stock row {row_num} is no longer available.'}

            stock_row = list(stock_values[row_num - 1])
            padded_stock_row = stock_row + [''] * max(0, len(stock_headers) - len(stock_row))
            description = padded_stock_row[description_col] if description_col is not None and description_col < len(padded_stock_row) else ''
            imei = padded_stock_row[imei_col] if imei_col is not None and imei_col < len(padded_stock_row) else ''

            buyer_name = str(item.get('buyer_name') or '').strip().upper()
            buyer_phone = normalize_phone_number(item.get('buyer_phone') or '')
            sale_price = clean_amount(item.get('sale_price'))
            stock_status_choice = str(item.get('stock_status') or 'sold').strip()
            inventory_status = str(item.get('inventory_status') or 'UNPAID').strip().upper()
            availability_override = str(item.get('availability_value') or '').strip()

            if not buyer_name:
                return {'error': f'Buyer name is required for stock row {row_num}.'}
            if sale_price <= 0:
                return {'error': f'Enter a valid sale price for stock row {row_num}.'}
            if not description:
                return {'error': f'Stock row {row_num} is missing a description.'}

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
            if inventory_status == 'PAID' and main_status_col is not None and main_paid_col is not None and main_imei_col is not None and main_name_col is not None:
                for index in range(len(main_values) - 1, main_header_row_idx, -1):
                    row = main_values[index] if index < len(main_values) else []
                    if main_imei_col >= len(row) or main_name_col >= len(row):
                        continue
                    existing_imei = str(row[main_imei_col] or '').strip()
                    existing_name = str(row[main_name_col] or '').strip().upper()
                    existing_status = str(row[main_status_col] or '').strip().upper() if main_status_col < len(row) else ''
                    if existing_imei == str(imei or '').strip() and existing_name == buyer_name and existing_status in {'UNPAID', 'PART PAYMENT'}:
                        updated_existing_row = index + 1
                        break

            if updated_existing_row is not None:
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
                        'value': sale_price,
                    },
                    cache_apply_callable=lambda rn=updated_existing_row, cn=main_paid_col + 1, nv=sale_price: self.postgres_sync_manager.update_cached_table_value('main_values', rn, cn, nv),
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
                    'buyer_name': buyer_name,
                    'buyer_phone': buyer_phone,
                    'sale_price': sale_price,
                    'stock_status': status_key.upper(),
                    'inventory_status': 'PAID',
                    'description': description,
                    'imei': imei,
                    'mode': 'updated_existing',
                })
            else:
                amount_paid = sale_price if inventory_status == 'PAID' else 0
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
                    'buyer_name': buyer_name,
                    'buyer_phone': buyer_phone,
                    'sale_price': sale_price,
                    'stock_status': status_key.upper(),
                    'inventory_status': inventory_status,
                    'description': description,
                    'imei': imei,
                    'mode': 'appended',
                })

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
                    cache_apply_callable=lambda ri=row_idx, fn=paid_field_name, col=paid_col + 1, nv=paid_value: (
                        self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', ri + 1, col, nv),
                    ),
                )
            )

            queue_ids.append(
                self._enqueue_db_first_operation(
                    'payment',
                    'main_update_status',
                    {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': status_col + 1, 'value': status_value},
                    cache_apply_callable=lambda ri=row_idx, fn=status_field_name, col=status_col + 1, nv=status_value: (
                        self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', ri + 1, col, nv),
                    ),
                )
            )

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
                    cache_apply_callable=lambda ri=row_idx, fn=paid_field_name, col=paid_col + 1, nv=item['new_paid']: (
                        self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                        self.postgres_sync_manager.update_cached_table_value('main_values', ri + 1, col, nv),
                    ),
                )
            )

            if item['new_status']:
                queue_ids.append(
                    self._enqueue_db_first_operation(
                        'payment',
                        'main_update_status',
                        {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': status_col + 1, 'value': item['new_status']},
                        cache_apply_callable=lambda ri=row_idx, fn=status_field_name, col=status_col + 1, nv=item['new_status']: (
                            self.postgres_sync_manager.update_cached_main_record_field(ri, fn, nv),
                            self.postgres_sync_manager.update_cached_table_value('main_values', ri + 1, col, nv),
                        ),
                    )
                )

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