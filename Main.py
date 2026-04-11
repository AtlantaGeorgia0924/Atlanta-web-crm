import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from datetime import datetime, date, timedelta
import threading
import json
import os
import sys
import atexit
import subprocess
import re
import uuid
import logging
import webbrowser
from urllib.parse import quote as url_quote

from services.billing_service import (
    build_payment_plan as svc_build_payment_plan,
    clean_amount as svc_clean_amount,
    compute_sales_snapshot as svc_compute_sales_snapshot,
    compute_debtors as svc_compute_debtors,
    format_date as svc_format_date,
    format_service_option as svc_format_service_option,
    generate_bill_text as svc_generate_bill_text,
    get_customer_outstanding_items_from_records as svc_get_customer_outstanding_items_from_records,
    get_customer_outstanding_items_from_values as svc_get_customer_outstanding_items_from_values,
    is_returned_status as svc_is_returned_status,
    parse_sheet_date as svc_parse_sheet_date,
)
from services.client_service import (
    build_client_directory_rows as svc_build_client_directory_rows,
    build_matched_contact_updates as svc_build_matched_contact_updates,
    build_selected_contact_updates as svc_build_selected_contact_updates,
    find_existing_client_key as svc_find_existing_client_key,
    import_sheet_phone_numbers_to_registry as svc_import_sheet_phone_numbers_to_registry,
    match_contact_to_client_name as svc_match_contact_to_client_name,
    normalize_client_name as svc_normalize_client_name,
    normalize_phone_number as svc_normalize_phone_number,
    set_client_phone as svc_set_client_phone,
    validate_client_entry as svc_validate_client_entry,
)
from services.contact_import_service import (
    deduplicate_contacts as svc_deduplicate_contacts,
    fetch_google_contacts as svc_fetch_google_contacts,
    load_contacts_file as svc_load_contacts_file,
    parse_contacts_csv as svc_parse_contacts_csv,
    parse_contacts_vcf as svc_parse_contacts_vcf,
)
from services.name_fix_service import (
    build_name_fix_all_updates as svc_build_name_fix_all_updates,
    build_name_fix_summary as svc_build_name_fix_summary,
    build_name_fix_updates as svc_build_name_fix_updates,
    find_name_mismatches as svc_find_name_mismatches,
    fuzzy_score as svc_fuzzy_score,
)
from services.stock_service import (
    build_sale_status_update_values as svc_build_sale_status_update_values,
    build_stock_form_defaults as svc_build_stock_form_defaults,
    build_stock_row_values as svc_build_stock_row_values,
    build_stock_view as svc_build_stock_view,
    classify_available_series as svc_classify_available_series,
    classify_stock_fill_color as svc_classify_stock_fill_color,
    compute_stock_qty_status as svc_compute_stock_qty_status,
    detect_stock_headers as svc_detect_stock_headers,
    get_stock_color_status_map as svc_get_stock_color_status_map,
    header_index as svc_stock_header_index,
    map_sale_status as svc_map_sale_status,
    order_stock_form_headers as svc_order_stock_form_headers,
    suggest_next_serial as svc_suggest_next_serial,
    validate_stock_row as svc_validate_stock_row,
)
from services.sync_service import (
    backfill_record_ids as svc_backfill_record_ids,
    build_client_phone_sheet_updates as svc_build_client_phone_sheet_updates,
    build_phone_autofill_plan as svc_build_phone_autofill_plan,
    detect_sheet_header_row as svc_detect_sheet_header_row,
    ensure_directory_sheet as svc_ensure_directory_sheet,
    ensure_record_id_column as svc_ensure_record_id_column,
    rollout_record_ids_for_known_sheets as svc_rollout_record_ids_for_known_sheets,
)

try:
    from db_sync import create_postgres_sync_manager, PSYCOPG2_AVAILABLE
except Exception:
    create_postgres_sync_manager = None
    PSYCOPG2_AVAILABLE = False

try:
    from PIL import Image, ImageTk
    pillow_available = True
except Exception:
    pillow_available = False

# Logging
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Config persistence
CONFIG_FILE = 'config.json'
CONTACTS_TOKEN_FILE = 'contacts_token.json'
CONTACTS_SCOPES = ['https://www.googleapis.com/auth/contacts.readonly']


def load_config():
    defaults = {
        'sheet_id': '1Krh8uRxfZjdZXkLfXc6ujG53RSjrCa5gyrpAvsXmypg',
        'phone_stock_sheet_id': '',
        'credentials_file': 'credentials.json',
        'contacts_oauth_file': '',
        'company_name': 'ATLANTA GEORGIA_TECH',
        'payment_details': '8168364881\nOPAY (PAYCOM)\nAKINPELUMI GEORGE AYOMIDE',
        'last_client': '',
        'enable_postgres_cache': True,
        'legacy_sheet_fallback': True,
        'startup_mode': 'cache_then_sync',
        'sync_pull_interval_sec': 90,
        'sync_conflict_policy': 'sheet_wins',
        'record_id_rollout': True,
        'postgres_dsn': ''
    }

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
        except Exception as e:
            logging.warning('Failed to parse config.json, reinitializing defaults: %s', e)
            config = defaults.copy()
    else:
        config = defaults.copy()
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)

    # Ensure all keys exist
    updated = False
    for k, v in defaults.items():
        if k not in config:
            config[k] = v
            updated = True
    if updated:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)

    return config


def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)


def extract_sheet_id(value):
    raw = str(value or '').strip()
    if not raw:
        return ''
    if '/d/' in raw:
        raw = raw.split('/d/', 1)[1]
    raw = raw.split('/', 1)[0]
    raw = raw.split('?', 1)[0]
    raw = raw.split('#', 1)[0]
    return raw.strip()


config = load_config()
postgres_sync_manager = None
postgres_sync_state = {
    'enabled': False,
    'ready': False,
    'last_status': 'legacy_only',
    'last_error': ''
}

# 📱 Client phone registry
CLIENTS_FILE = 'clients.json'


def load_clients():
    if os.path.exists(CLIENTS_FILE):
        try:
            with open(CLIENTS_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_clients(clients_dict):
    with open(CLIENTS_FILE, 'w') as f:
        json.dump(clients_dict, f, indent=4)


def get_default_contacts_oauth_file():
    configured_path = str(config.get('contacts_oauth_file', '')).strip()
    candidate_paths = [
        configured_path,
        os.path.join(os.getcwd(), 'contacts_oauth_client.json'),
        os.path.join(os.getcwd(), 'credentials1.json'),
        os.path.expanduser('~/Downloads/credentials1.json')
    ]

    for path in candidate_paths:
        if path and os.path.exists(path):
            return path

    return configured_path


def normalize_phone_number(value):
    return svc_normalize_phone_number(value)


DIRECTORY_SHEET_TITLE = 'CLIENT DIRECTORY'


def normalize_client_name(value):
    return svc_normalize_client_name(value)


def find_existing_client_key(name, registry=None):
    registry = registry or clients
    return svc_find_existing_client_key(name, registry)


def set_client_phone(name, phone, registry=None):
    registry = registry if registry is not None else clients
    return svc_set_client_phone(name, phone, registry)


def get_main_sheet_values():
    values = sheet.sheet1.get_all_values()
    if not values:
        return [], {}, {}

    header = values[0]
    header_lookup = {str(col).strip().upper(): idx for idx, col in enumerate(header)}

    def pick(*candidates):
        for candidate in candidates:
            idx = header_lookup.get(candidate.upper())
            if idx is not None:
                return idx
        return None

    columns = {
        'name_col': pick('NAME', 'CLIENT NAME', 'CUSTOMER NAME'),
        'phone_col': pick('PHONE NUMBER', 'PHONE', 'WHATSAPP NUMBER', 'WHATSAPP', 'NUMBER'),
        'status_col': pick('STATUS'),
        'price_col': pick('PRICE'),
        'paid_col': pick('AMOUNT PAID')
    }
    return values, header_lookup, columns


def column_index_to_letter(index):
    index += 1
    result = ''
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def detect_sheet_header_row(values):
    return svc_detect_sheet_header_row(values)


def ensure_record_id_column(worksheet):
    return svc_ensure_record_id_column(worksheet)


def backfill_record_ids(worksheet):
    return svc_backfill_record_ids(worksheet)


def rollout_record_ids_for_known_sheets(main_spreadsheet, gspread_client, stock_sheet_id):
    return svc_rollout_record_ids_for_known_sheets(main_spreadsheet, gspread_client, stock_sheet_id)


def ensure_client_directory_sheet():
    return svc_ensure_directory_sheet(sheet, DIRECTORY_SHEET_TITLE)


def sync_client_directory_sheet():
    directory_ws = ensure_client_directory_sheet()
    rows = svc_build_client_directory_rows(clients)

    directory_ws.clear()
    directory_ws.update(f'A1:B{len(rows)}', rows)
    logging.info('Client directory sheet synced with %d entries.', len(rows) - 1)


def import_sheet_phone_numbers_to_clients():
    values, _, columns = get_main_sheet_values()
    name_col = columns.get('name_col')
    phone_col = columns.get('phone_col')
    if not values or name_col is None or phone_col is None:
        return 0, 0

    added, updated = svc_import_sheet_phone_numbers_to_registry(values, name_col, phone_col, clients)

    if added or updated:
        save_clients(clients)
    return added, updated


def sync_clients_to_sheet_phone_column():
    values, _, columns = get_main_sheet_values()
    name_col = columns.get('name_col')
    phone_col = columns.get('phone_col')
    if not values or name_col is None or phone_col is None:
        return 0

    updates = svc_build_client_phone_sheet_updates(values, clients, name_col, phone_col)

    if updates:
        sheet.sheet1.batch_update(updates, value_input_option='USER_ENTERED')
        logging.info('Updated %d sheet phone number cell(s) from client registry.', len(updates))
    return len(updates)


def apply_sheet_phone_autofill_formulas():
    values, _, columns = get_main_sheet_values()
    name_col = columns.get('name_col')
    phone_col = columns.get('phone_col')
    if not values or name_col is None or phone_col is None:
        return 0

    sync_client_directory_sheet()

    formula_plan = svc_build_phone_autofill_plan(
        values,
        name_col,
        phone_col,
        sheet.sheet1.row_count,
        DIRECTORY_SHEET_TITLE,
    )

    sheet.sheet1.update(
        formula_plan['range'],
        formula_plan['values'],
        value_input_option='USER_ENTERED'
    )
    logging.info('Applied phone autofill formulas to %d row(s).', len(formula_plan['values']))
    return len(formula_plan['values'])


def deduplicate_contacts(contact_rows):
    return svc_deduplicate_contacts(contact_rows)


def parse_contacts_csv(file_path):
    return svc_parse_contacts_csv(file_path)


def parse_contacts_vcf(file_path):
    return svc_parse_contacts_vcf(file_path)


def match_contact_to_debtor(contact_name, debtor_names):
    return svc_match_contact_to_client_name(contact_name, debtor_names) or None


def load_contacts_file(file_path):
    return svc_load_contacts_file(file_path)


def fetch_google_contacts(oauth_file_path):
    return svc_fetch_google_contacts(oauth_file_path, CONTACTS_TOKEN_FILE, CONTACTS_SCOPES)


clients = load_clients()

# 🔐 Auth
scopes = ['https://www.googleapis.com/auth/spreadsheets']

creds = ServiceAccountCredentials.from_service_account_file(config.get('credentials_file', 'credentials.json'), scopes=scopes)
# Heavy network connections deferred to _init_sheets() so the window opens immediately
client = None
sheets_api_service = None
sheet = None
data = []
debtors = []
merged = {}
sorted_debtors = []
client_names = []
total_debtors_amount = 0


def classify_stock_fill_color(color):
    return svc_classify_stock_fill_color(color)


def get_stock_color_status_map(spreadsheet_id, worksheet_title, description_col_idx, last_row):
    return svc_get_stock_color_status_map(
        sheets_api_service,
        spreadsheet_id,
        worksheet_title,
        description_col_idx,
        last_row,
    )


# 💰 Clean amount safely
def clean_amount(value):
    return svc_clean_amount(value)


def get_customer_outstanding_items(name_input, values=None):
    if values is None:
        values = sheet.sheet1.get_all_values()
    return svc_get_customer_outstanding_items_from_values(name_input, values)


def get_customer_outstanding_items_from_data(name_input, records=None):
    records = records if records is not None else data
    return svc_get_customer_outstanding_items_from_records(name_input, records)


def format_service_option(item):
    return svc_format_service_option(item)


# 🧾 Output moved to GUI show_debtors (console output disabled)
# for name, amount in sorted_debtors:
#     print(f"{name} — NGN {amount:,}")


def generate_bill(name_input):
    bill_text = svc_generate_bill_text(name_input, data, config.get('payment_details', ''))
    if bill_text == "No outstanding bill for this customer.":
        print(bill_text)
        return
    print(bill_text)

def format_date(date_str):
    return svc_format_date(date_str)

def update_payment(name_input, payment_amount, manual_service_row_idx=None):
    name_input = name_input.strip().upper()

    # Get all values including headers
    values = sheet.sheet1.get_all_values()
    if not values:
        return "No data in sheet."

    plan = svc_build_payment_plan(name_input, payment_amount, values, manual_service_row_idx=manual_service_row_idx)
    if plan.get('error'):
        return plan['error']

    paid_col = plan['columns']['paid_col']
    status_col = plan['columns']['status_col']
    paid_field_name = values[0][paid_col] if paid_col < len(values[0]) else 'Amount paid'
    status_field_name = values[0][status_col] if status_col < len(values[0]) else 'STATUS'
    undo_rows = list(plan['undo_rows'])

    for item in plan['updates']:
        row_idx = item['row_idx']
        _queue_then_apply(
            'payment',
            'main_update_paid',
            {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': paid_col + 1, 'value': item['new_paid']},
            lambda ri=row_idx, nv=item['new_paid']: sheet.sheet1.update_cell(ri + 1, paid_col + 1, nv),
            async_only=True,
            cache_apply_callable=lambda ri=row_idx, fn=paid_field_name, nv=item['new_paid']: (
                postgres_sync_manager.update_cached_main_record_field(ri, fn, nv)
                if (postgres_sync_state.get('ready') and postgres_sync_manager) else None
            )
        )
        if item['new_status']:
            _queue_then_apply(
                'payment',
                'main_update_status',
                {'kind': 'main_update_cell', 'row': row_idx + 1, 'col': status_col + 1, 'value': item['new_status']},
                lambda ri=row_idx, nv=item['new_status']: sheet.sheet1.update_cell(ri + 1, status_col + 1, nv),
                async_only=True,
                cache_apply_callable=lambda ri=row_idx, fn=status_field_name, nv=item['new_status']: (
                    postgres_sync_manager.update_cached_main_record_field(ri, fn, nv)
                    if (postgres_sync_state.get('ready') and postgres_sync_manager) else None
                )
            )
    
    # Update debtors summary after payment
    update_debtors_summary()
    # capture undo data
    global last_payment_action
    last_payment_action = {
        'customer': name_input,
        'rows': undo_rows
    }
    logging.info("Payment applied: %s", plan['status_text'])
    return plan['status_text']


last_payment_action = None
last_undone_action = None


def undo_last_payment():
    global last_payment_action
    if not last_payment_action or not last_payment_action.get('rows'):
        return "Nothing to undo."

    try:
        for row in last_payment_action['rows']:
            sheet.sheet1.update_cell(row['row_idx'] + 1, row['paid_col'] + 1, row['old_paid'])
            sheet.sheet1.update_cell(row['row_idx'] + 1, row['status_col'] + 1, row['old_status'])

        update_debtors_summary()
        logging.info("Undo payment for %s", last_payment_action['customer'])
        last_undone_action = last_payment_action.copy()
        last_payment_action = None
        return "Last payment action undone."

    except Exception as e:
        logging.error('Failed to undo payment: %s', e)
        return f"Undo failed: {e}"


def refresh_debtors_data(force_sheet=False):
    global data, debtors, merged, sorted_debtors, client_names, total_debtors_amount
    loaded_from_cache = False

    if not force_sheet and postgres_sync_state.get('ready') and postgres_sync_manager:
        try:
            cached_rows = postgres_sync_manager.load_cached_rows('main_records')
            if isinstance(cached_rows, list) and cached_rows:
                data = cached_rows
                loaded_from_cache = True
        except Exception as e:
            logging.warning('Failed to load main_records from PostgreSQL cache: %s', e)

    if not loaded_from_cache:
        data = sheet.sheet1.get_all_records()
        if postgres_sync_state.get('ready') and postgres_sync_manager:
            try:
                postgres_sync_manager.upsert_sheet_cache('main_records', data)
            except Exception as e:
                logging.warning('Failed to refresh PostgreSQL main_records cache: %s', e)

    summary = svc_compute_debtors(data)
    debtors = summary['debtors']
    merged = summary['merged']
    sorted_debtors = summary['sorted_debtors']
    client_names = summary['client_names']
    total_debtors_amount = summary['total_debtors_amount']


def apply_sheet_name_validation(name_list=None):
    """Push a dropdown Data Validation rule on the NAME column of sheet1
    so reps get auto-suggestions while typing in Google Sheets."""
    try:
        values, _, columns = get_main_sheet_values()
        name_col = columns.get('name_col')
        if name_col is None:
            return

        if not name_list:
            name_list = sorted({normalize_client_name(name) for name in clients if str(clients.get(name, '')).strip()})
            if not name_list:
                name_list = client_names
        if not name_list:
            return

        spreadsheet = sheet.spreadsheet
        sheet1_id = sheet.sheet1.id

        # Build the list of allowed values
        values = [{'userEnteredValue': n} for n in sorted(set(name_list))]

        body = {
            'requests': [{
                'setDataValidation': {
                    'range': {
                        'sheetId': sheet1_id,
                        'startRowIndex': 1,       # skip header row
                        'startColumnIndex': name_col,
                        'endColumnIndex': name_col + 1,
                    },
                    'rule': {
                        'condition': {
                            'type': 'ONE_OF_LIST',
                            'values': values
                        },
                        'showCustomUi': True,      # renders as dropdown
                        'strict': False            # still allow free-text entry
                    }
                }
            }]
        }
        spreadsheet.batch_update(body)
        logging.info("Sheet NAME column validation updated with %d names.", len(name_list))
    except Exception as e:
        logging.warning("Could not apply sheet validation: %s", e)


def fuzzy_score(a, b):
    return svc_fuzzy_score(a, b)


def find_name_mismatches():
    values = sheet.sheet1.get_all_values()
    return svc_find_name_mismatches(values, client_names)


def update_debtors_summary():
    refresh_debtors_data()

    try:
        summary_sheet = sheet.worksheet("Debtors Summary")
    except gspread.WorksheetNotFound:
        summary_sheet = sheet.add_worksheet("Debtors Summary", rows=100, cols=2)

    summary_sheet.clear()
    rows = [['Name', 'Outstanding Amount']] + [[name, f'NGN {amount:,}'] for name, amount in sorted_debtors]
    rows.append(['TOTAL OUTSTANDING', f'NGN {total_debtors_amount:,}'])
    summary_sheet.update('A1', rows)

    logging.info("Debtors Summary updated on the spreadsheet.")


def redo_last_payment():
    global last_undone_action, last_payment_action
    if not last_undone_action or not last_undone_action.get('rows'):
        return "Nothing to redo."

    try:
        for row in last_undone_action['rows']:
            sheet.sheet1.update_cell(row['row_idx'] + 1, row['paid_col'] + 1, row['new_paid'])
            sheet.sheet1.update_cell(row['row_idx'] + 1, row['status_col'] + 1, row['new_status'])

        update_debtors_summary()
        logging.info("Redo payment for %s", last_undone_action['customer'])
        last_payment_action = last_undone_action.copy()
        last_undone_action = None
        return "Last undone payment reapplied."

    except Exception as e:
        logging.error('Failed to redo payment: %s', e)
        return f"Redo failed: {e}"

try:
    from tkinter import Tk, Label, Entry, Button, Text, END, StringVar, BooleanVar, Checkbutton, PhotoImage, Canvas, messagebox, scrolledtext, Frame, ttk, Listbox, SINGLE, Toplevel, filedialog
    import tkinter.font as tkFont
    tkinter_available = True
except ImportError:
    tkinter_available = False


class _AutocompleteEntry:
    """Entry widget with a Listbox popup for suggestions.
    Drop-in for an editable ttk.Combobox — avoids the macOS Tcl modal-loop freeze."""

    def __init__(self, parent, root_win, width=35, font=None):
        self._root = root_win
        self._all_values = []
        self._selected_callback = None
        self._var = StringVar()
        self.entry = Entry(parent, textvariable=self._var, width=width, font=font)
        self._popup = None
        self._listbox = None

    # ---- Combobox-compatible API ----
    def get(self):
        return self._var.get()

    def set(self, value):
        self._var.set(value)
        self._hide_popup()

    def __setitem__(self, key, value):
        if key == 'values':
            self._all_values = list(value) if value else []

    def __getitem__(self, key):
        if key == 'values':
            return tuple(self._all_values)
        return None

    def cget(self, key):
        if key == 'values':
            return tuple(self._all_values)
        return self.entry.cget(key)

    def grid(self, **kwargs):
        self.entry.grid(**kwargs)

    def bind(self, sequence, func, add=None):
        return self.entry.bind(sequence, func, add)

    def icursor(self, pos):
        self.entry.icursor(pos)

    def focus_set(self):
        self.entry.focus_set()

    # ---- Popup management ----
    def _ensure_popup(self):
        if self._popup is not None:
            return
        self._popup = Toplevel(self._root)
        self._popup.overrideredirect(True)
        self._popup.attributes('-topmost', True)
        self._popup.withdraw()
        self._listbox = Listbox(
            self._popup, selectmode='browse', activestyle='dotbox',
            relief='solid', bd=1, highlightthickness=0,
            font=self.entry.cget('font'))
        self._listbox.pack(fill='both', expand=True)
        self._listbox.bind('<ButtonRelease-1>', self._on_select)
        # Hide when listbox loses focus (e.g. user clicks elsewhere)
        self._listbox.bind('<FocusOut>', lambda e: self._root.after(120, self._hide_popup))

    def show_popup(self, values):
        if not values:
            self._hide_popup()
            return
        self._ensure_popup()
        self._listbox.delete(0, END)
        for v in values:
            self._listbox.insert(END, v)
        n = min(8, len(values))
        self._listbox.config(height=n)
        x = self.entry.winfo_rootx()
        y = self.entry.winfo_rooty() + self.entry.winfo_height()
        w = self.entry.winfo_width()
        self._popup.geometry(f'{w}x{n * 22}+{x}+{y}')
        self._popup.deiconify()
        self._popup.lift()

    def _hide_popup(self):
        if self._popup is not None and self._popup.winfo_exists():
            self._popup.withdraw()

    def _on_select(self, event=None):
        if not self._listbox:
            return
        sel = self._listbox.curselection()
        if sel:
            val = self._listbox.get(sel[0])
            self._var.set(val)
            self._hide_popup()
            self.entry.focus_set()
            if self._selected_callback:
                self._selected_callback(val)

    def navigate(self, direction):
        """Move popup highlight. direction: +1 down, -1 up. Returns True if popup was open."""
        if self._popup is None or not self._popup.winfo_viewable():
            return False
        size = self._listbox.size()
        if not size:
            return False
        sel = self._listbox.curselection()
        idx = (sel[0] + direction) if sel else (0 if direction > 0 else size - 1)
        idx = max(0, min(idx, size - 1))
        self._listbox.selection_clear(0, END)
        self._listbox.selection_set(idx)
        self._listbox.see(idx)
        return True

    def confirm_selection(self):
        """Accept highlighted popup item. Returns True if handled."""
        if self._popup is None or not self._popup.winfo_viewable():
            return False
        sel = self._listbox.curselection()
        if sel:
            self._on_select()
            return True
        return False


def main_gui():
    stock_window_mode = '--stock-window' in sys.argv
    service_target_map = {}
    debtors_window = None
    payment_client_filter_job = None
    payment_service_filter_job = None
    payment_preview_job = None
    dashboard_metric_labels = {}
    sensitive_metric_state = {}
    action_buttons = []
    weekly_sales_canvas = {'widget': None}
    daily_sales_canvas = {'widget': None}
    logo_refs = []

    def is_returned_status(status_text):
        return svc_is_returned_status(status_text)

    def find_logo_paths():
        """Find up to 2 logo files matching WhatsApp exported image names."""
        base_candidates = [
            os.getcwd(),
            os.path.expanduser('~/Desktop/PYTHON FOLDER'),
            os.path.expanduser('~/Desktop'),
            os.path.expanduser('~/Downloads')
        ]
        name_prefixes = [
            'WhatsApp Image 2026-04-04 at 16.29.12 (1)',
            'WhatsApp Image 2026-04-04 at 16.29.12'
        ]
        exts = ['.png', '.jpg', '.jpeg', '.gif', '.webp']

        found = []
        for folder in base_candidates:
            if not os.path.isdir(folder):
                continue
            try:
                names = os.listdir(folder)
            except Exception:
                continue

            for prefix in name_prefixes:
                for item in names:
                    item_lower = item.lower()
                    if not item.startswith(prefix):
                        continue
                    if any(item_lower.endswith(ext) for ext in exts):
                        full_path = os.path.join(folder, item)
                        if full_path not in found:
                            found.append(full_path)
            if len(found) >= 2:
                break

        return found[:1]

    def parse_sheet_date(date_value):
        return svc_parse_sheet_date(date_value)

    def compute_sales_snapshot():
        return svc_compute_sales_snapshot(data)

    def render_weekly_sales_graph(week_totals):
        canvas = weekly_sales_canvas.get('widget')
        if not canvas:
            return

        canvas.delete('all')
        canvas.update_idletasks()
        width = max(520, canvas.winfo_width())
        height = max(210, canvas.winfo_height())

        left = 44
        right = width - 16
        top = 24
        bottom = height - 42
        bar_gap = 18
        bars = len(week_totals)
        usable_width = max(100, right - left)
        bar_width = max(22, int((usable_width - bar_gap * (bars - 1)) / bars))
        max_total = max(week_totals) if week_totals else 0

        canvas.create_line(left, top, left, bottom, fill='#7f8c8d', width=1)
        canvas.create_line(left, bottom, right, bottom, fill='#7f8c8d', width=1)

        for i, total in enumerate(week_totals):
            x0 = left + i * (bar_width + bar_gap)
            x1 = x0 + bar_width
            if max_total > 0:
                bar_height = int((total / max_total) * (bottom - top - 16))
            else:
                bar_height = 0
            y0 = bottom - bar_height

            canvas.create_rectangle(x0, y0, x1, bottom, fill='#2f7f72', outline='')
            canvas.create_text((x0 + x1) / 2, bottom + 14, text=f'W{i + 1}', fill='#47575c', font=('Avenir Next', 10, 'bold'))
            canvas.create_text((x0 + x1) / 2, y0 - 10, text=f'{total:,}', fill='#1e363e', font=('Avenir Next', 9))

        canvas.create_text(left, 10, anchor='w', text='Weekly Sales (Current Month)', fill='#1e363e', font=('Avenir Next', 11, 'bold'))

    def render_daily_sales_graph(daily_totals):
        canvas = daily_sales_canvas.get('widget')
        if not canvas:
            return

        canvas.delete('all')
        canvas.update_idletasks()
        width = max(520, canvas.winfo_width())
        height = max(190, canvas.winfo_height())

        left = 44
        right = width - 16
        top = 24
        bottom = height - 34
        bars = len(daily_totals)
        bar_gap = 10
        usable_width = max(100, right - left)
        bar_width = max(18, int((usable_width - bar_gap * (bars - 1)) / bars))
        max_total = max(daily_totals) if daily_totals else 0

        labels = []
        today = date.today()
        for offset in range(6, -1, -1):
            labels.append((today - timedelta(days=offset)).strftime('%a'))

        canvas.create_line(left, top, left, bottom, fill='#7f8c8d', width=1)
        canvas.create_line(left, bottom, right, bottom, fill='#7f8c8d', width=1)

        for i, total in enumerate(daily_totals):
            x0 = left + i * (bar_width + bar_gap)
            x1 = x0 + bar_width
            bar_height = int((total / max_total) * (bottom - top - 16)) if max_total > 0 else 0
            y0 = bottom - bar_height

            canvas.create_rectangle(x0, y0, x1, bottom, fill='#5fa8d3', outline='')
            canvas.create_text((x0 + x1) / 2, bottom + 12, text=labels[i], fill='#47575c', font=('Avenir Next', 9, 'bold'))
            canvas.create_text((x0 + x1) / 2, y0 - 9, text=f'{total:,}', fill='#1e363e', font=('Avenir Next', 8))

        canvas.create_text(left, 10, anchor='w', text='Sales Last 7 Days', fill='#1e363e', font=('Avenir Next', 11, 'bold'))

    def set_metric_value(key, value_text):
        state = sensitive_metric_state.get(key)
        if state:
            state['actual'] = value_text
            if state.get('visible'):
                state['label'].config(text=value_text)
            else:
                state['label'].config(text='***')
            return

        label = dashboard_metric_labels.get(key)
        if label:
            label.config(text=value_text)

    def reveal_metric(key, visible):
        state = sensitive_metric_state.get(key)
        if not state:
            return
        state['visible'] = bool(visible)
        state['label'].config(text=state['actual'] if visible else '***')

    def build_payment_preview(name_input, selected_service='Automatic sequence'):
        name_input = name_input.strip().upper()
        if not name_input:
            return "Select a customer to preview outstanding services and balances."

        try:
            outstanding_items, total_outstanding = get_customer_outstanding_items_from_data(name_input)
        except Exception as e:
            return f"Unable to load payment preview: {e}"

        if not outstanding_items:
            return "No outstanding services found for this customer."

        lines = [
            f"Customer: {name_input}",
            f"Outstanding services: {len(outstanding_items)}",
            f"Total outstanding: NGN {total_outstanding:,}",
            f"Payment order: {selected_service if selected_service and selected_service != 'Automatic sequence' else 'Automatic sequence'}",
            "",
            "Outstanding items:"
        ]

        prioritized_row_idx = service_target_map.get(selected_service)
        if prioritized_row_idx is not None:
            prioritized_items = [item for item in outstanding_items if item['row_idx'] == prioritized_row_idx]
            remaining_items = [item for item in outstanding_items if item['row_idx'] != prioritized_row_idx]
            outstanding_items = prioritized_items + remaining_items

        for index, item in enumerate(outstanding_items, 1):
            marker = "[FIRST] " if index == 1 and prioritized_row_idx is not None else ""
            description = item['description'] or 'UNNAMED SERVICE'
            service_date = format_date(item['date']) if item['date'] else 'No date'
            lines.append(
                f"{index}. {marker}{description} ({service_date}) - Balance NGN {item['balance']:,}"
            )

        return "\n".join(lines)

    def update_payment_preview(selected_name=None):
        selected_name = selected_name if selected_name is not None else payment_client_combo.get()
        try:
            selected_service = payment_service_combo.get().strip()
        except NameError:
            selected_service = 'Automatic sequence'
        preview_text = build_payment_preview(selected_name, selected_service)
        payment_preview_box.config(state='normal')
        payment_preview_box.delete('1.0', END)
        payment_preview_box.insert(END, preview_text)
        payment_preview_box.config(state='disabled')

    def queue_payment_preview(selected_name=None, delay_ms=120):
        nonlocal payment_preview_job
        if payment_preview_job is not None:
            root.after_cancel(payment_preview_job)
        def run_preview():
            nonlocal payment_preview_job
            payment_preview_job = None
            update_payment_preview(selected_name)
        payment_preview_job = root.after(delay_ms, run_preview)

    def open_debtors_window():
        nonlocal debtors_window

        if debtors_window and debtors_window.winfo_exists():
            debtors_window.lift()
            debtors_window.focus_force()
            return

        debtors_window = Toplevel(root)
        debtors_window.title("Debtors List")
        debtors_window.geometry('860x620')
        debtors_window.configure(bg='#eef3f8')

        header = Frame(debtors_window, bg='#eef3f8')
        header.pack(fill='x', padx=12, pady=10)

        Label(header, text="Debtors", font=section_font, bg='#eef3f8', fg='#2c3e50').grid(row=0, column=0, sticky='w')
        debtor_count_label = Label(header, text="", font=label_font, bg='#eef3f8', fg='#2c3e50')
        debtor_count_label.grid(row=1, column=0, sticky='w', pady=(6, 0))
        debtor_total_label = Label(header, text="", font=label_font, bg='#eef3f8', fg='#2c3e50')
        debtor_total_label.grid(row=2, column=0, sticky='w')

        list_frame = Frame(debtors_window, bg='#eef3f8')
        list_frame.pack(fill='both', expand=True, padx=12, pady=8)

        debtor_tree_style = ttk.Style(debtors_window)
        debtor_tree_style.configure('DebtorTreeview.Treeview', font=debtor_list_font, rowheight=34)
        debtor_tree_style.configure('DebtorTreeview.Treeview.Heading', font=('Helvetica', 13, 'bold'))

        debtor_tree = ttk.Treeview(list_frame, columns=('name', 'amount'), show='headings', height=18, style='DebtorTreeview.Treeview')
        debtor_tree.heading('name', text='Customer')
        debtor_tree.heading('amount', text='Outstanding Amount')
        debtor_tree.column('name', width=320, anchor='w')
        debtor_tree.column('amount', width=170, anchor='e')

        scrollbar = ttk.Scrollbar(list_frame, orient='vertical', command=debtor_tree.yview)
        debtor_tree.configure(yscrollcommand=scrollbar.set)

        debtor_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='left', fill='y')

        bill_preview_frame = Frame(list_frame, bg='#eef3f8')
        bill_preview_frame.pack(side='left', fill='both', expand=True, padx=(10, 0))
        preview_header = Frame(bill_preview_frame, bg='#eef3f8')
        preview_header.pack(fill='x')
        Label(preview_header, text="Debtor Bill Preview", font=label_font, bg='#eef3f8', fg='#2c3e50').pack(side='left', anchor='w')
        current_preview_debtor = {'name': ''}

        def copy_current_preview_bill():
            debtor_name = current_preview_debtor['name']
            if not debtor_name:
                messagebox.showwarning("No Selection", "Select a debtor first.")
                return
            copy_bill_for_debtor(debtor_name)

        Button(preview_header, text="Copy Preview", command=copy_current_preview_bill, font=('Helvetica', 11, 'bold'), bg='#8e44ad', fg='black', activeforeground='black', activebackground='#8e44ad', width=12, relief='raised', bd=3).pack(side='right')

        bill_preview_box = scrolledtext.ScrolledText(bill_preview_frame, width=55, height=20, font=math_italic_font, wrap='word')
        bill_preview_box.pack(fill='both', expand=True, pady=(6, 0))
        bill_preview_box.insert(END, "Select a debtor from the list to preview their bill here.")
        bill_preview_box.config(state='disabled')

        action_frame = Frame(debtors_window, bg='#eef3f8')
        action_frame.pack(fill='x', padx=12, pady=(0, 12))

        def get_selected_debtor_name(warn_if_none=True):
            selected = debtor_tree.focus()
            if not selected:
                if warn_if_none:
                    messagebox.showwarning("No Selection", "Select a debtor first.")
                return None
            values = debtor_tree.item(selected, 'values')
            if not values:
                return None
            return values[0]

        def view_selected_debtor_bill(warn_if_none=True):
            debtor_name = get_selected_debtor_name(warn_if_none=warn_if_none)
            if debtor_name:
                bill = capture_generate_bill(debtor_name)
                bill_preview_box.config(state='normal')
                bill_preview_box.delete('1.0', END)
                bill_preview_box.insert(END, bill)
                bill_preview_box.config(state='disabled')
                current_preview_debtor['name'] = debtor_name

        def copy_selected_debtor_bill():
            debtor_name = get_selected_debtor_name(warn_if_none=True)
            if debtor_name:
                copy_bill_for_debtor(debtor_name)

        def refresh_debtors_page():
            refresh_debtors_data()
            populate_debtors_page()

        def populate_debtors_page():
            debtor_tree.delete(*debtor_tree.get_children())
            debtor_count_label.config(text=f"Customers owing: {len(sorted_debtors)}")
            debtor_total_label.config(text=f"Total outstanding: NGN {total_debtors_amount:,}")

            for name, amount in sorted_debtors:
                debtor_tree.insert('', END, values=(name, f"NGN {amount:,}"))

        Button(action_frame, text="Copy Selected Bill", command=copy_selected_debtor_bill, font=button_font, bg='#8e44ad', fg='black', activeforeground='black', activebackground='#8e44ad', width=18, relief='raised', bd=3).grid(row=0, column=0, padx=6)
        Button(action_frame, text="Refresh Debtors", command=refresh_debtors_page, font=button_font, bg='#f39c12', fg='black', activeforeground='black', activebackground='#f39c12', width=16, relief='raised', bd=3).grid(row=0, column=1, padx=6)
        Button(action_frame, text="Close", command=debtors_window.destroy, font=button_font, bg='#95a5a6', fg='black', activeforeground='black', activebackground='#95a5a6', width=10, relief='raised', bd=3).grid(row=0, column=2, padx=6)

        def send_whatsapp_bill():
            global clients
            debtor_name = current_preview_debtor['name']
            if not debtor_name:
                debtor_name = get_selected_debtor_name(warn_if_none=True)
            if not debtor_name:
                return

            phone = clients.get(debtor_name.upper(), '').strip()

            if not phone:
                # Prompt user to enter the number
                phone_win = Toplevel(debtors_window)
                phone_win.title(f"WhatsApp Number — {debtor_name}")
                phone_win.geometry('460x180')
                phone_win.configure(bg='#eef3f8')
                phone_win.grab_set()
                Label(phone_win, text=f"No WhatsApp number registered for {debtor_name}.",
                      font=label_font, bg='#eef3f8', wraplength=420).pack(pady=(18, 6))
                Label(phone_win, text="Enter number with country code (e.g. 2348168364881):",
                      font=label_font, bg='#eef3f8').pack()
                phone_entry = Entry(phone_win, width=32, font=combo_font)
                phone_entry.pack(pady=8)

                def confirm_number():
                    global clients
                    entered = phone_entry.get().strip().lstrip('+')
                    if not entered.isdigit() or len(entered) < 7:
                        messagebox.showerror("Invalid", "Enter a valid phone number with country code.",
                                             parent=phone_win)
                        return
                    clients[debtor_name.upper()] = entered
                    save_clients(clients)
                    phone_win.destroy()
                    _open_whatsapp(debtor_name, entered)

                Button(phone_win, text="Save & Send", command=confirm_number,
                       font=button_font, bg='#25D366', fg='black',
                       activebackground='#25D366', width=14, relief='raised', bd=3).pack(pady=4)

                phone_entry.focus()
                phone_entry.bind('<Return>', lambda e: confirm_number())
                return

            _open_whatsapp(debtor_name, phone)

        def _open_whatsapp(debtor_name, phone):
            bill = capture_generate_bill(debtor_name)
            if not bill or 'No outstanding bill' in bill:
                messagebox.showinfo("No Bill", f"No outstanding bill for {debtor_name}.")
                return
            encoded = url_quote(bill)
            url = f"https://wa.me/{phone}?text={encoded}"
            webbrowser.open(url)

        Button(preview_header, text="Send to WhatsApp", command=send_whatsapp_bill,
               font=('Helvetica', 11, 'bold'), bg='#25D366', fg='black',
               activeforeground='black', activebackground='#25D366',
               width=16, relief='raised', bd=3).pack(side='right', padx=(0, 6))

        debtor_tree.bind('<<TreeviewSelect>>', lambda event: view_selected_debtor_bill(warn_if_none=False))
        debtor_tree.bind('<Double-1>', lambda event: view_selected_debtor_bill(warn_if_none=False))
        populate_debtors_page()

    def copy_bill_for_debtor(debtor_name):
        bill = capture_generate_bill(debtor_name)
        if not bill or "No outstanding bill" in bill:
            messagebox.showinfo("No Bill", f"No outstanding bill for {debtor_name}.")
            return

        try:
            root.clipboard_clear()
            root.clipboard_append(bill)
            messagebox.showinfo("Copied", f"Bill for {debtor_name} copied to clipboard")
        except Exception:
            try:
                import pyperclip
                pyperclip.copy(bill)
                messagebox.showinfo("Copied", f"Bill for {debtor_name} copied to clipboard")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to copy bill: {str(e)}")

    def launch_stock_manager_detached():
        """Open stock manager in a separate process so the main dashboard never freezes."""
        if stock_window_mode:
            open_phone_stock_manager()
            return

        try:
            subprocess.Popen([sys.executable, os.path.abspath(__file__), '--stock-window'])
            update_status('Stock manager launched in separate window')
        except Exception as e:
            messagebox.showerror('Stock Manager', f'Could not launch stock window:\n{e}')

    def set_all_buttons(state):
        # Keep this for optional future per-button control. Avoid full lock to prevent perceived freezing.
        widgets = list(action_buttons)
        try:
            widgets.append(apply_payment_btn)
        except NameError:
            pass

        for b in widgets:
            try:
                b.config(state=state)
            except Exception:
                pass

    def update_status(msg):
        if status_label:
            status_label.config(text=msg)
            status_label.update_idletasks()

    def run_async(worker, on_complete=None):
        update_status("Working... Please wait...")
        progress_bar.start(15)

        def wrapper():
            try:
                result = worker()
                if on_complete:
                    root.after(0, lambda: on_complete(result))
            except Exception as e:
                root.after(0, lambda: messagebox.showerror("Error", f"Unexpected error: {e}"))
            finally:
                root.after(0, lambda: update_status("Ready"))
                root.after(0, progress_bar.stop)

        threading.Thread(target=wrapper, daemon=True).start()

    def update_payment_ui():
        customer = payment_client_combo.get().strip()
        amount_text = amount_entry.get().strip()
        selected_service = payment_service_combo.get().strip()

        if not customer or not amount_text:
            messagebox.showwarning("Input needed", "Select client and enter amount")
            return

        try:
            amount = int(amount_text)
        except ValueError:
            messagebox.showerror("Invalid amount", "Payment amount must be a number")
            return

        manual_service_row_idx = service_target_map.get(selected_service)

        def worker():
            return update_payment(customer, amount, manual_service_row_idx=manual_service_row_idx)

        def finalize(status_text):
            # Refresh UI-bound state on the main thread after background payment writes complete.
            refresh_debtors()
            if status_text:
                messagebox.showinfo("Done", status_text)
            else:
                messagebox.showinfo("Done", "Payment update completed")
            update_payment_preview(customer)

        run_async(worker, on_complete=finalize)

    def refresh_list():
        def worker():
            import_sheet_phone_numbers_to_clients()
            update_debtors_summary()
            # make sure data in GUI is refreshed swiftly
            refresh_debtors()
            sync_client_directory_sheet()
            sync_clients_to_sheet_phone_column()
            apply_sheet_name_validation()
            apply_sheet_phone_autofill_formulas()
            return None

        def finalize(_):
            update_payment_preview()
            messagebox.showinfo("Refreshed", "Debtors list, phone numbers, and sheet suggestions updated")

        run_async(worker, on_complete=finalize)

    def open_client_manager():
        global clients
        win = Toplevel(root)
        win.title("Manage Clients & WhatsApp Numbers")
        win.geometry('760x560')
        win.configure(bg='#eef3f8')
        imported_contacts = []
        imported_contacts_path = {'value': get_default_contacts_oauth_file()}

        Label(win, text="Client WhatsApp Numbers", font=section_font,
              bg='#eef3f8', fg='#2c3e50').pack(pady=(12, 6))
        Label(win, text="Numbers must include country code, no + or spaces (e.g. 2348168364881)",
              font=('Helvetica', 10), bg='#eef3f8', fg='#555').pack()
        contacts_status_label = Label(win, text="Contacts source not loaded", font=('Helvetica', 10),
                                      bg='#eef3f8', fg='#1f4e79')
        contacts_status_label.pack(pady=(4, 0))

        oauth_hint = imported_contacts_path['value']
        if oauth_hint:
            update_text = f"Google Contacts OAuth file ready: {os.path.basename(oauth_hint)}"
            contacts_status_label.config(text=update_text)

        tree_frame = Frame(win, bg='#eef3f8')
        tree_frame.pack(fill='both', expand=True, padx=14, pady=10)

        cm_tree = ttk.Treeview(tree_frame, columns=('name', 'phone'), show='headings', height=12, selectmode='extended')
        cm_tree.heading('name', text='Client Name')
        cm_tree.heading('phone', text='WhatsApp Number')
        cm_tree.column('name', width=310, anchor='w')
        cm_tree.column('phone', width=220, anchor='w')
        cm_sb = ttk.Scrollbar(tree_frame, orient='vertical', command=cm_tree.yview)
        cm_tree.configure(yscrollcommand=cm_sb.set)
        cm_tree.pack(side='left', fill='both', expand=True)
        cm_sb.pack(side='right', fill='y')

        def populate_cm():
            cm_tree.delete(*cm_tree.get_children())
            for name, phone in sorted(clients.items()):
                cm_tree.insert('', END, values=(name, phone))

        def focus_client_row(client_name):
            for item in cm_tree.get_children():
                vals = cm_tree.item(item, 'values')
                if vals and vals[0] == client_name:
                    cm_tree.selection_set(item)
                    cm_tree.focus(item)
                    cm_tree.see(item)
                    on_cm_select()
                    break

        def select_all_clients(event=None):
            cm_tree.selection_set(cm_tree.get_children())
            return 'break'

        def update_contacts_status(message):
            contacts_status_label.config(text=message)

        def choose_google_contacts_api_file():
            file_path = filedialog.askopenfilename(
                parent=win,
                title="Select Google Contacts OAuth JSON",
                initialdir=os.path.expanduser('~/Downloads'),
                filetypes=[('JSON files', '*.json'), ('All files', '*.*')]
            )

            if not file_path:
                return ''

            imported_contacts_path['value'] = file_path
            config['contacts_oauth_file'] = file_path
            save_config(config)
            update_contacts_status(f"Google Contacts OAuth file selected: {os.path.basename(file_path)}")
            return file_path

        form_frame = Frame(win, bg='#eef3f8')
        form_frame.pack(fill='x', padx=14, pady=(0, 6))
        Label(form_frame, text="Name:", font=label_font, bg='#eef3f8').grid(row=0, column=0, sticky='e', padx=4)
        name_entry = Entry(form_frame, width=26, font=combo_font)
        name_entry.grid(row=0, column=1, padx=4, pady=4)
        Label(form_frame, text="Number:", font=label_font, bg='#eef3f8').grid(row=0, column=2, sticky='e', padx=4)
        phone_entry_cm = Entry(form_frame, width=20, font=combo_font)
        phone_entry_cm.grid(row=0, column=3, padx=4, pady=4)
        Button(form_frame, text="🔍 Search Contacts", command=lambda: open_contact_picker(fill_phone_only=True),
               font=('Helvetica', 10, 'bold'), bg='#f39c12', fg='black',
               activebackground='#f39c12', relief='raised', bd=2).grid(row=0, column=4, padx=(6, 4), pady=4)

        # Auto-fill name options from debtor list
        all_known = sorted(set(list(client_names) + list(clients.keys())))
        name_entry_combo = ttk.Combobox(form_frame, width=24, font=combo_font,
                                        values=all_known)
        name_entry_combo.grid(row=0, column=1, padx=4, pady=4)
        name_entry.grid_forget()

        def on_cm_select(event):
            selected_items = cm_tree.selection()
            if selected_items:
                vals = cm_tree.item(selected_items[0], 'values')
                if vals:
                    name_entry_combo.set(vals[0])
                    phone_entry_cm.delete(0, END)
                    phone_entry_cm.insert(0, vals[1])

        cm_tree.bind('<<TreeviewSelect>>', on_cm_select)
        cm_tree.bind('<Delete>', lambda e: delete_entry())
        cm_tree.bind('<Control-a>', select_all_clients)
        cm_tree.bind('<Control-A>', select_all_clients)
        cm_tree.bind('<Command-a>', select_all_clients)
        cm_tree.bind('<Command-A>', select_all_clients)
        win.bind('<Control-a>', select_all_clients)
        win.bind('<Control-A>', select_all_clients)
        win.bind('<Command-a>', select_all_clients)
        win.bind('<Command-A>', select_all_clients)

        def save_entry():
            global clients
            entry_plan = svc_validate_client_entry(name_entry_combo.get(), phone_entry_cm.get())
            if entry_plan.get('error'):
                messagebox.showerror("Invalid", entry_plan['error'], parent=win)
                return
            clients[entry_plan['name']] = entry_plan['phone']
            save_clients(clients)
            sync_client_directory_sheet()
            sync_clients_to_sheet_phone_column()
            apply_sheet_name_validation()
            apply_sheet_phone_autofill_formulas()
            populate_cm()
            name_entry_combo.set('')
            phone_entry_cm.delete(0, END)

        def delete_entry():
            global clients
            selected_items = list(cm_tree.selection())
            if not selected_items:
                messagebox.showwarning("No Selection", "Select one or more clients to delete.", parent=win)
                return

            names_to_delete = []
            for item in selected_items:
                vals = cm_tree.item(item, 'values')
                if vals and vals[0] in clients:
                    names_to_delete.append(vals[0])

            if not names_to_delete:
                messagebox.showwarning("No Selection", "Selected rows have no saved clients.", parent=win)
                return

            if len(names_to_delete) == 1:
                confirm_text = f"Delete {names_to_delete[0]}?"
            else:
                confirm_text = f"Delete {len(names_to_delete)} selected clients?"

            if not messagebox.askyesno("Confirm", confirm_text, parent=win):
                return

            for name in names_to_delete:
                clients.pop(name, None)

            save_clients(clients)
            sync_client_directory_sheet()
            apply_sheet_name_validation()
            populate_cm()
            update_contacts_status(f"Deleted {len(names_to_delete)} client(s)")

        def import_from_sheet():
            """Import names from the sheet, including any phone numbers in the PHONE NUMBER column."""
            global clients
            added_from_phone, updated_from_phone = import_sheet_phone_numbers_to_clients()
            added = 0
            for name in client_names:
                key = name.strip().upper()
                if key not in clients:
                    clients[key] = ''
                    added += 1
            save_clients(clients)
            sync_client_directory_sheet()
            apply_sheet_name_validation()
            apply_sheet_phone_autofill_formulas()
            populate_cm()
            messagebox.showinfo("Imported", f"{added} new debtor name(s) added.\nImported {added_from_phone} new phone contact(s) from the sheet.\nUpdated {updated_from_phone} existing client phone(s) from the sheet.",
                                parent=win)

        def open_contact_picker(fill_phone_only=False):
            if not imported_contacts:
                messagebox.showwarning("No Contacts", "Import a contacts file first.", parent=win)
                return

            selected_client_name = ''
            if fill_phone_only:
                selected_items = list(cm_tree.selection())
                if len(selected_items) != 1:
                    messagebox.showwarning(
                        "Select One Client",
                        "Select exactly one client in the clients list before using Search Contacts.",
                        parent=win
                    )
                    return

                selected_values = cm_tree.item(selected_items[0], 'values')
                selected_client_name = str(selected_values[0]).strip() if selected_values else ''
                if not selected_client_name:
                    messagebox.showwarning("No Selection", "Select a valid client row first.", parent=win)
                    return

            picker = Toplevel(win)
            picker.title("Find Contact Number" if fill_phone_only else "Search Imported Contacts")
            picker.geometry('760x420')
            picker.configure(bg='#f7fbff')
            picker.transient(win)
            picker.grab_set()

            Label(picker, text="Search imported contacts", font=section_font,
                  bg='#f7fbff', fg='#2c3e50').pack(pady=(12, 6))

            search_entry = Entry(picker, width=40, font=combo_font)
            search_entry.pack(padx=14, pady=(0, 10))

            picker_tree_frame = Frame(picker, bg='#f7fbff')
            picker_tree_frame.pack(fill='both', expand=True, padx=14, pady=(0, 12))

            picker_tree = ttk.Treeview(picker_tree_frame, columns=('name', 'phone', 'label'), show='headings', height=12, selectmode='extended')
            picker_tree.heading('name', text='Contact Name')
            picker_tree.heading('phone', text='Phone Number')
            picker_tree.heading('label', text='Source')
            picker_tree.column('name', width=300, anchor='w')
            picker_tree.column('phone', width=180, anchor='w')
            picker_tree.column('label', width=180, anchor='w')
            picker_scroll = ttk.Scrollbar(picker_tree_frame, orient='vertical', command=picker_tree.yview)
            picker_tree.configure(yscrollcommand=picker_scroll.set)
            picker_tree.pack(side='left', fill='both', expand=True)
            picker_scroll.pack(side='right', fill='y')

            sel_count_label = Label(picker, text="0 selected", font=('Helvetica', 10), bg='#f7fbff', fg='#555')
            sel_count_label.pack()
            anchor_item = {'id': None}

            def update_sel_count(event=None):
                n = len(picker_tree.selection())
                sel_count_label.config(text=f"{n} selected")

            def select_all(event=None):
                picker_tree.selection_set(picker_tree.get_children())
                children = picker_tree.get_children()
                if children:
                    anchor_item['id'] = children[0]
                update_sel_count()
                return 'break'

            def on_tree_click(event=None):
                clicked = picker_tree.identify_row(event.y) if event else ''
                if clicked:
                    anchor_item['id'] = clicked
                picker.after(0, update_sel_count)

            def on_shift_click(event):
                clicked = picker_tree.identify_row(event.y)
                if not clicked:
                    return 'break'

                children = list(picker_tree.get_children())
                if not children:
                    return 'break'

                anchor = anchor_item['id'] if anchor_item['id'] in children else clicked
                start = children.index(anchor)
                end = children.index(clicked)
                lo, hi = sorted((start, end))
                picker_tree.selection_set(children[lo:hi + 1])
                picker_tree.focus(clicked)
                picker_tree.see(clicked)
                update_sel_count()
                return 'break'

            def populate_picker(filter_text=''):
                filter_text = filter_text.strip().lower()
                picker_tree.delete(*picker_tree.get_children())

                for contact in imported_contacts:
                    haystack = f"{contact['name']} {contact['phone']} {contact['label']}".lower()
                    if filter_text and filter_text not in haystack:
                        continue
                    picker_tree.insert('', END, values=(contact['name'], contact['phone'], contact['label']))

                update_sel_count()

            def use_selected_contacts(event=None):
                global clients

                selected_items = picker_tree.selection()
                if not selected_items:
                    messagebox.showwarning("No Selection", "Select at least one contact first.", parent=picker)
                    return

                if fill_phone_only:
                    values = picker_tree.item(selected_items[0], 'values')
                    contact_phone = str(values[1]).strip() if values and values[1] else ''
                    entry_plan = svc_validate_client_entry(selected_client_name, contact_phone)
                    if entry_plan.get('error'):
                        messagebox.showwarning("No Number", entry_plan['error'], parent=picker)
                        return

                    clients[entry_plan['name']] = entry_plan['phone']
                    save_clients(clients)
                    sync_client_directory_sheet()
                    sync_clients_to_sheet_phone_column()
                    apply_sheet_name_validation()
                    apply_sheet_phone_autofill_formulas()
                    populate_cm()
                    focus_client_row(selected_client_name)
                    update_contacts_status(f"Updated {selected_client_name} phone number")
                    picker.destroy()
                    return

                selected_contacts = []
                for item in selected_items:
                    values = picker_tree.item(item, 'values')
                    if not values:
                        continue
                    selected_contacts.append({'name': values[0], 'phone': values[1]})

                contact_updates = svc_build_selected_contact_updates(selected_contacts)
                for contact_name, contact_phone in contact_updates.items():
                    clients[contact_name] = contact_phone

                save_clients(clients)
                sync_client_directory_sheet()
                sync_clients_to_sheet_phone_column()
                apply_sheet_name_validation()
                apply_sheet_phone_autofill_formulas()
                populate_cm()
                picker.destroy()
                update_contacts_status(f"Saved {len(contact_updates)} contact(s) to client registry")

            search_entry.bind('<KeyRelease>', lambda event: populate_picker(search_entry.get()))
            search_entry.bind('<Return>', use_selected_contacts)
            picker_tree.bind('<ButtonRelease-1>', update_sel_count)
            picker_tree.bind('<Button-1>', on_tree_click)
            picker_tree.bind('<Shift-Button-1>', on_shift_click)
            picker_tree.bind('<KeyRelease>', update_sel_count)
            picker_tree.bind('<Double-1>', use_selected_contacts)
            picker_tree.bind('<Return>', use_selected_contacts)
            picker_tree.bind('<Control-a>', select_all)
            picker_tree.bind('<Control-A>', select_all)
            picker_tree.bind('<Command-a>', select_all)
            picker_tree.bind('<Command-A>', select_all)
            search_entry.bind('<Control-a>', select_all)
            search_entry.bind('<Control-A>', select_all)
            search_entry.bind('<Command-a>', select_all)
            search_entry.bind('<Command-A>', select_all)
            picker.bind('<Control-a>', select_all)
            picker.bind('<Control-A>', select_all)
            picker.bind('<Command-a>', select_all)
            picker.bind('<Command-A>', select_all)

            action_frame = Frame(picker, bg='#f7fbff')
            action_frame.pack(pady=(0, 12))
            btn_label = "Use Phone Number" if fill_phone_only else "Save Selected to Clients"
            Button(action_frame, text=btn_label, command=use_selected_contacts, font=button_font,
                   bg='#27ae60', fg='black', activebackground='#27ae60', width=20,
                   relief='raised', bd=3).grid(row=0, column=0, padx=6)
            Button(action_frame, text="Select All", command=select_all, font=button_font,
                   bg='#2980b9', fg='black', activebackground='#2980b9', width=12,
                   relief='raised', bd=3).grid(row=0, column=1, padx=6)
            Button(action_frame, text="Close", command=picker.destroy, font=button_font,
                   bg='#95a5a6', fg='black', activebackground='#95a5a6', width=10,
                   relief='raised', bd=3).grid(row=0, column=2, padx=6)

            populate_picker()
            search_entry.focus_set()

        def import_contacts_file():
            nonlocal imported_contacts

            file_path = filedialog.askopenfilename(
                parent=win,
                title="Select Google Contacts Export",
                initialdir=os.path.expanduser('~/Downloads'),
                filetypes=[
                    ('Contacts files', '*.csv *.vcf *.vcard'),
                    ('CSV files', '*.csv'),
                    ('VCF files', '*.vcf *.vcard'),
                    ('All files', '*.*')
                ]
            )

            if not file_path:
                return

            try:
                imported_contacts = load_contacts_file(file_path)
            except Exception as e:
                messagebox.showerror("Import Failed", f"Could not read contacts file:\n{e}", parent=win)
                return

            if not imported_contacts:
                messagebox.showwarning("No Contacts", "No phone numbers were found in that file.", parent=win)
                update_contacts_status("Contacts file loaded, but no phone numbers were found")
                return

            imported_contacts_path['value'] = file_path
            update_contacts_status(
                f"Loaded {len(imported_contacts)} contact number(s) from {os.path.basename(file_path)}"
            )
            open_contact_picker()

        def bulk_import_matched():
            nonlocal imported_contacts

            if not imported_contacts:
                messagebox.showwarning("No Contacts", "Import or sync contacts first.", parent=win)
                return

            match_plan = svc_build_matched_contact_updates(imported_contacts, client_names, clients)

            if not match_plan['matched']:
                messagebox.showinfo("No Matches", "No contacts matched your debtor list.", parent=win)
                return

            for name, phone in match_plan['updates'].items():
                clients[name] = phone

            save_clients(clients)
            sync_client_directory_sheet()
            sync_clients_to_sheet_phone_column()
            apply_sheet_name_validation()
            apply_sheet_phone_autofill_formulas()
            populate_cm()

            summary = f"Imported {len(match_plan['updates'])} matched contact(s)"
            if match_plan['unmatched']:
                summary += f"\nSkipped {len(match_plan['unmatched'])} unmatched contact(s)"

            messagebox.showinfo("Bulk Import", summary, parent=win)
            update_contacts_status(f"Bulk imported {len(match_plan['updates'])} contact number(s)")

        def sync_google_contacts():
            nonlocal imported_contacts

            oauth_file = imported_contacts_path['value']
            if not oauth_file or not os.path.exists(oauth_file):
                oauth_file = choose_google_contacts_api_file()
                if not oauth_file:
                    return

            update_contacts_status("Connecting to Google Contacts. Complete the browser sign-in if prompted...")

            def worker():
                try:
                    return {'contacts': fetch_google_contacts(oauth_file), 'error': ''}
                except Exception as e:
                    return {'contacts': [], 'error': str(e)}

            def finalize(result):
                nonlocal imported_contacts

                if result.get('error'):
                    update_contacts_status("Google Contacts sync failed")
                    messagebox.showerror("Google Contacts", f"Failed to sync contacts:\n{result['error']}", parent=win)
                    return

                imported_contacts = result.get('contacts') or []
                if not imported_contacts:
                    update_contacts_status("Google Contacts synced, but no phone numbers were found")
                    messagebox.showinfo("Google Contacts", "No phone numbers were found in your Google Contacts.", parent=win)
                    return

                update_contacts_status(f"Synced {len(imported_contacts)} Google contact number(s)")
                open_contact_picker()

            run_async(worker, on_complete=finalize)

        btn_row = Frame(win, bg='#eef3f8')
        btn_row.pack(pady=(0, 12))
        Button(btn_row, text="Save / Update", command=save_entry, font=button_font,
               bg='#27ae60', fg='black', activebackground='#27ae60', width=14,
               relief='raised', bd=3).grid(row=0, column=0, padx=6)
        Button(btn_row, text="Delete Selected", command=delete_entry, font=button_font,
               bg='#e74c3c', fg='black', activebackground='#e74c3c', width=14,
               relief='raised', bd=3).grid(row=0, column=1, padx=6)
        Button(btn_row, text="Close", command=win.destroy, font=button_font,
               bg='#95a5a6', fg='black', activebackground='#95a5a6', width=8,
               relief='raised', bd=3).grid(row=0, column=2, padx=6)
        Button(btn_row, text="Import Debtor Names", command=import_from_sheet, font=button_font,
               bg='#3498db', fg='black', activebackground='#3498db', width=18,
               relief='raised', bd=3).grid(row=1, column=0, padx=6, pady=(8, 0))
        Button(btn_row, text="Import Contacts File", command=import_contacts_file, font=button_font,
               bg='#9b59b6', fg='black', activebackground='#9b59b6', width=18,
               relief='raised', bd=3).grid(row=1, column=1, padx=6, pady=(8, 0))
        Button(btn_row, text="Pull Sheet Phones", command=lambda: (
            import_sheet_phone_numbers_to_clients(),
            populate_cm(),
            update_contacts_status("Imported phone numbers from sheet into Manage Clients")
        ), font=button_font, bg='#1abc9c', fg='black', activebackground='#1abc9c', width=18,
               relief='raised', bd=3).grid(row=1, column=2, padx=6, pady=(8, 0))
        Button(btn_row, text="Select API JSON", command=choose_google_contacts_api_file, font=button_font,
               bg='#16a085', fg='black', activebackground='#16a085', width=18,
               relief='raised', bd=3).grid(row=2, column=0, padx=6, pady=(8, 0))
        Button(btn_row, text="Sync Google Contacts", command=sync_google_contacts, font=button_font,
               bg='#2ecc71', fg='black', activebackground='#2ecc71', width=18,
               relief='raised', bd=3).grid(row=2, column=1, padx=6, pady=(8, 0))
        Button(btn_row, text="Bulk Import Matched", command=bulk_import_matched, font=button_font,
               bg='#e67e22', fg='black', activebackground='#e67e22', width=18,
               relief='raised', bd=3).grid(row=2, column=2, padx=6, pady=(8, 0))
        Button(btn_row, text="Push Phones to Sheet", command=lambda: (
            sync_client_directory_sheet(),
            sync_clients_to_sheet_phone_column(),
            update_contacts_status("Updated sheet PHONE NUMBER column from Manage Clients")
        ), font=button_font, bg='#2980b9', fg='black', activebackground='#2980b9', width=18,
               relief='raised', bd=3).grid(row=3, column=0, padx=6, pady=(8, 0))
        Button(btn_row, text="Enable Phone Autofill", command=lambda: (
            sync_client_directory_sheet(),
            apply_sheet_name_validation(),
            apply_sheet_phone_autofill_formulas(),
            update_contacts_status("Enabled sheet name suggestions and phone autofill")
        ), font=button_font, bg='#8e44ad', fg='black', activebackground='#8e44ad', width=18,
               relief='raised', bd=3).grid(row=3, column=1, padx=6, pady=(8, 0))

        populate_cm()

    def open_name_fix_window():
        """Scan sheet for names close-but-not-equal to known clients and let user correct them."""

        fix_win = Toplevel(root)
        fix_win.title("Fix Misspelled Names in Sheet")
        fix_win.geometry('860x560')
        fix_win.configure(bg='#f0f4f8')
        fix_win.transient(root)
        fix_win.grab_set()

        Label(fix_win, text="Fix / Merge Misspelled Names in Google Sheet",
              font=section_font, bg='#f0f4f8', fg='#2c3e50').pack(pady=(12, 4))
        Label(fix_win, text="These names in your sheet do not exactly match any known client. "
              "Select the correct name and click Apply Fix.",
              font=('Helvetica', 10), bg='#f0f4f8', fg='#555', wraplength=820).pack(pady=(0, 8))

        status_lbl = Label(fix_win, text="Scanning sheet…", font=('Helvetica', 10),
                           bg='#f0f4f8', fg='#1f4e79')
        status_lbl.pack()

        pane = Frame(fix_win, bg='#f0f4f8')
        pane.pack(fill='both', expand=True, padx=14, pady=10)

        cols = ('bad_name', 'rows_affected', 'suggested_fix')
        fix_tree = ttk.Treeview(pane, columns=cols, show='headings', height=14, selectmode='browse')
        fix_tree.heading('bad_name',      text='Name in Sheet (incorrect)')
        fix_tree.heading('rows_affected', text='Rows')
        fix_tree.heading('suggested_fix', text='Suggested Correct Name')
        fix_tree.column('bad_name',      width=260, anchor='w')
        fix_tree.column('rows_affected', width=80,  anchor='center')
        fix_tree.column('suggested_fix', width=260, anchor='w')
        fsb = ttk.Scrollbar(pane, orient='vertical', command=fix_tree.yview)
        fix_tree.configure(yscrollcommand=fsb.set)
        fix_tree.pack(side='left', fill='both', expand=True)
        fsb.pack(side='right', fill='y')

        mismatch_data = {}   # bad_name_upper → {raw, rows, candidates}

        def load_mismatches():
            status_lbl.config(text="Scanning sheet…")
            fix_win.update_idletasks()

            def worker():
                return find_name_mismatches()

            def done(results):
                fix_tree.delete(*fix_tree.get_children())
                mismatch_data.clear()
                if not results:
                    status_lbl.config(text="No misspelled names found — sheet looks clean!")
                    return
                for entry in results:
                    key = entry['raw'].upper().strip()
                    mismatch_data[key] = entry
                    top_suggestion = entry['candidates'][0] if entry['candidates'] else '(no match)'
                    fix_tree.insert('', END, iid=key, values=(
                        entry['raw'],
                        len(entry['rows']),
                        top_suggestion
                    ))
                status_lbl.config(text=f"Found {len(results)} name(s) that may be misspelled.")

            run_async(worker, on_complete=done)

        # Bottom edit area
        edit_frame = Frame(fix_win, bg='#f0f4f8')
        edit_frame.pack(fill='x', padx=14, pady=(0, 4))
        Label(edit_frame, text="Replace with:", font=label_font, bg='#f0f4f8').grid(row=0, column=0, sticky='e', padx=6)
        correct_combo = ttk.Combobox(edit_frame, width=34, font=combo_font)
        correct_combo.grid(row=0, column=1, padx=6, pady=4)

        def on_fix_select(event=None):
            sel = fix_tree.selection()
            if not sel:
                return
            key = sel[0]
            entry = mismatch_data.get(key, {})
            candidates = entry.get('candidates', [])
            correct_combo['values'] = candidates
            if candidates:
                correct_combo.set(candidates[0])

        fix_tree.bind('<<TreeviewSelect>>', on_fix_select)

        def apply_fix():
            sel = fix_tree.selection()
            if not sel:
                messagebox.showwarning("Nothing selected", "Select a row to fix.", parent=fix_win)
                return
            key = sel[0]
            entry = mismatch_data.get(key, {})
            bad_raw = entry.get('raw', '')
            row_indices = entry.get('rows', [])
            correct_name = correct_combo.get().strip()
            if not correct_name:
                messagebox.showwarning("No replacement", "Enter or choose the correct name.", parent=fix_win)
                return
            if not messagebox.askyesno(
                    "Confirm Fix",
                    f"Replace '{bad_raw}' → '{correct_name}' in {len(row_indices)} row(s)?",
                    parent=fix_win):
                return

            def worker():
                values = sheet.sheet1.get_all_values()
                updates = svc_build_name_fix_updates(values, entry, correct_name)
                for row_number, col_number, value in updates:
                    sheet.sheet1.update_cell(row_number, col_number, value)
                refresh_debtors_data()
                return len(updates)

            def done(count):
                status_lbl.config(text=f"Fixed '{bad_raw}' → '{correct_name}' in {count} row(s).")
                fix_tree.delete(key)
                mismatch_data.pop(key, None)
                # also auto-refresh sheet validation
                apply_sheet_name_validation()

            run_async(worker, on_complete=done)

        def apply_fix_all():
            """Fix every listed mismatch using its top suggestion in one pass."""
            if not mismatch_data:
                messagebox.showinfo("Nothing to fix", "No mismatches loaded.", parent=fix_win)
                return
            summary = svc_build_name_fix_summary(mismatch_data.values())
            if not messagebox.askyesno("Confirm Fix All",
                                       f"Apply these fixes?\n\n{summary}", parent=fix_win):
                return

            def worker():
                values = sheet.sheet1.get_all_values()
                updates = svc_build_name_fix_all_updates(values, mismatch_data.values())
                for row_number, col_number, value in updates:
                    sheet.sheet1.update_cell(row_number, col_number, value)
                refresh_debtors_data()
                apply_sheet_name_validation()
                return len(updates)

            def done(count):
                status_lbl.config(text=f"Fixed {count} row(s) in total.")
                fix_tree.delete(*fix_tree.get_children())
                mismatch_data.clear()

            run_async(worker, on_complete=done)

        btn_row2 = Frame(fix_win, bg='#f0f4f8')
        btn_row2.pack(pady=(0, 12))
        Button(btn_row2, text="Apply Fix", command=apply_fix, font=button_font,
               bg='#27ae60', fg='black', activebackground='#27ae60', width=14,
               relief='raised', bd=3).grid(row=0, column=0, padx=6)
        Button(btn_row2, text="Fix All (auto)", command=apply_fix_all, font=button_font,
               bg='#e67e22', fg='black', activebackground='#e67e22', width=14,
               relief='raised', bd=3).grid(row=0, column=1, padx=6)
        Button(btn_row2, text="Rescan Sheet", command=load_mismatches, font=button_font,
               bg='#3498db', fg='black', activebackground='#3498db', width=14,
               relief='raised', bd=3).grid(row=0, column=2, padx=6)
        Button(btn_row2, text="Update Sheet Dropdown", command=lambda: (
            apply_sheet_name_validation(),
            status_lbl.config(text="Sheet NAME dropdown updated.")
        ), font=button_font, bg='#8e44ad', fg='black', activebackground='#8e44ad', width=18,
               relief='raised', bd=3).grid(row=0, column=3, padx=6)
        Button(btn_row2, text="Close", command=fix_win.destroy, font=button_font,
               bg='#95a5a6', fg='black', activebackground='#95a5a6', width=10,
               relief='raised', bd=3).grid(row=0, column=4, padx=6)

        load_mismatches()

    def open_phone_stock_manager():
        stock_sheet_id = extract_sheet_id(config.get('phone_stock_sheet_id', ''))
        if not stock_sheet_id:
            messagebox.showwarning(
                'Phone Stock',
                'Set your Phone Stock Sheet ID in Settings first.',
                parent=root
            )
            return

        stock_ws = None

        win = Toplevel(root)
        win.title('Phone Stock Manager')
        win.geometry('1280x760')
        win.configure(bg='#eef6f8')
        if '--stock-window' in sys.argv:
            root.withdraw()
            win.protocol('WM_DELETE_WINDOW', root.destroy)
            win.after(80, lambda: (win.lift(), win.focus_force()))

        Label(win, text='Phone Stock Manager', font=section_font, bg='#eef6f8', fg='#111111').pack(pady=(12, 6))
        status_lbl = Label(win, text='Ready', font=('Avenir Next', 10), bg='#eef6f8', fg='#222222')
        status_lbl.pack()

        stock_filter_mode = {'value': 'all'}
        stock_summary_labels = {}
        wheel_state = {'value': 0.0}

        def bind_smooth_wheel(widget, scroll_target):
            def _on_wheel(event):
                if event.widget.winfo_toplevel() != win:
                    return
                if getattr(event, 'num', None) == 4:
                    scroll_target.yview_scroll(-2, 'units')
                    return 'break'
                if getattr(event, 'num', None) == 5:
                    scroll_target.yview_scroll(2, 'units')
                    return 'break'

                delta = getattr(event, 'delta', 0)
                if delta == 0:
                    return 'break'

                if sys.platform == 'darwin':
                    wheel_state['value'] += (-delta / 12.0)
                    step = int(wheel_state['value'])
                    if step != 0:
                        scroll_target.yview_scroll(step, 'units')
                        wheel_state['value'] -= step
                else:
                    step = -1 * int(delta / 120) if abs(delta) >= 120 else (-1 if delta > 0 else 1)
                    scroll_target.yview_scroll(step, 'units')
                return 'break'

            widget.bind('<MouseWheel>', _on_wheel)
            widget.bind('<Button-4>', _on_wheel)
            widget.bind('<Button-5>', _on_wheel)

        summary_frame = Frame(win, bg='#eef6f8')
        summary_frame.pack(fill='x', padx=12, pady=(6, 0))

        def build_stock_chip(parent, column, title, key, bg):
            chip = Frame(parent, bg=bg, padx=12, pady=8)
            chip.grid(row=0, column=column, padx=5, sticky='w')
            Label(chip, text=title, font=('Avenir Next', 10, 'bold'), bg=bg, fg='#17323b').pack(anchor='w')
            value = Label(chip, text='0', font=('Georgia', 15, 'bold'), bg=bg, fg='#17323b')
            value.pack(anchor='w')
            stock_summary_labels[key] = value

        build_stock_chip(summary_frame, 0, 'Available', 'available', '#d8f3dc')
        build_stock_chip(summary_frame, 1, 'Pending Deal', 'pending', '#ffe08a')
        build_stock_chip(summary_frame, 2, 'Needs Details', 'needs_details', '#9fd3ff')
        build_stock_chip(summary_frame, 3, 'Sold', 'sold', '#f6a6a6')

        search_frame = Frame(win, bg='#eef6f8')
        search_frame.pack(fill='x', padx=12, pady=(8, 0))
        Label(search_frame, text='Search:', font=label_font, bg='#eef6f8').pack(side='left', padx=(0, 6))
        search_entry = Entry(search_frame, width=40, font=combo_font)
        search_entry.pack(side='left')

        filter_frame = Frame(win, bg='#eef6f8')
        filter_frame.pack(fill='x', padx=12, pady=(8, 0))

        search_after_id = {'value': None}
        all_rows_cache = []
        stock_cache = {
            'values': None,
            'color_status_map': {},
            'loading': False,
            'headers_ready': False
        }
        stock_render_token = {'value': 0}

        def open_all_table_page():
            if stock_cache['loading']:
                status_lbl.config(text='Stock data is still loading. Please wait...')
                return
            if stock_cache['values'] is None:
                populate_stock(search_entry.get(), force_refresh=True)
                status_lbl.config(text='Loading stock first, then open All page.')
                return
            if not all_rows_cache:
                render_stock_rows(search_entry.get())

            all_win = Toplevel(win)
            all_win.title('All Phone Stock')
            all_win.geometry('1280x700')
            all_win.configure(bg='#f2f2f2')

            Label(all_win, text='All Phone Stock Records', font=('Avenir Next', 12, 'bold'), bg='#f2f2f2', fg='#111111').pack(anchor='w', padx=12, pady=(10, 6))

            all_frame = Frame(all_win, bg='#f2f2f2')
            all_frame.pack(fill='both', expand=True, padx=12, pady=(0, 12))

            cols = headers + ['APP STATUS']
            all_tree = ttk.Treeview(all_frame, columns=cols, show='headings', style='Stock.Treeview')
            all_ysb = ttk.Scrollbar(all_frame, orient='vertical', command=all_tree.yview)
            all_xsb = ttk.Scrollbar(all_frame, orient='horizontal', command=all_tree.xview)
            all_tree.configure(yscrollcommand=all_ysb.set, xscrollcommand=all_xsb.set)

            all_tree.grid(row=0, column=0, sticky='nsew')
            all_ysb.grid(row=0, column=1, sticky='ns')
            all_xsb.grid(row=1, column=0, sticky='ew')
            all_frame.grid_rowconfigure(0, weight=1)
            all_frame.grid_columnconfigure(0, weight=1)

            for col_idx, h in enumerate(cols):
                all_tree.heading(h, text=h)
                sample_max = len(h)
                for item in all_rows_cache[:200]:
                    text = item['padded'][col_idx] if col_idx < len(headers) else item['label']
                    sample_max = max(sample_max, len(str(text)))
                width = int(sample_max * 8.2) + 24
                width = max(130, min(width, 520))
                upper = h.upper()
                if upper in ('DESCRIPTION', 'NAME OF BUYER', 'NAME OF SELLER'):
                    width = max(width, 260)
                if upper in ('PHONE NUMBER OF BUYER', 'PHONE NUMBER OF SELLER'):
                    width = max(width, 190)
                if upper in ('RECEIPT & EVIDIENCE LINK', 'RECEIPT', 'EVIDENCE LINK'):
                    width = max(width, 320)
                if upper == 'APP STATUS':
                    width = max(width, 150)
                all_tree.column(h, width=width, minwidth=110, stretch=True, anchor='w')

            all_tree.tag_configure('even', background='#fbfdff')
            all_tree.tag_configure('odd', background='#f3f8fc')
            all_tree.tag_configure('pending', background='#fff7cc')
            all_tree.tag_configure('needs_details', background='#e8f4ff')
            all_tree.tag_configure('sold', background='#ffe1e1')

            for item in all_rows_cache:
                row_tags = item['tags']
                all_tree.insert('', END, iid=str(item['row_num']), values=tuple(item['padded'][:len(headers)] + [item['label']]), tags=row_tags)

            status_lbl.config(text=f'Opened All page ({len(all_rows_cache)} rows)')

        def on_search_key_release(event=None):
            if search_after_id['value'] is not None:
                win.after_cancel(search_after_id['value'])
            search_after_id['value'] = win.after(250, lambda: populate_stock(search_entry.get()))

        def set_stock_filter(mode):
            stock_filter_mode['value'] = mode
            populate_stock(search_entry.get())
            if mode == 'all':
                open_all_table_page()

        Button(filter_frame, text='All', command=lambda: set_stock_filter('all'), font=('Avenir Next', 10, 'bold'),
               bg='#dfe7ea', fg='black', activebackground='#dfe7ea', width=10, relief='raised', bd=2).pack(side='left', padx=4)
        Button(filter_frame, text='Available', command=lambda: set_stock_filter('available'), font=('Avenir Next', 10, 'bold'),
               bg='#d8f3dc', fg='black', activebackground='#d8f3dc', width=10, relief='raised', bd=2).pack(side='left', padx=4)
        Button(filter_frame, text='Pending', command=lambda: set_stock_filter('pending'), font=('Avenir Next', 10, 'bold'),
               bg='#ffe08a', fg='black', activebackground='#ffe08a', width=10, relief='raised', bd=2).pack(side='left', padx=4)
        Button(filter_frame, text='Needs Details', command=lambda: set_stock_filter('needs_details'), font=('Avenir Next', 10, 'bold'),
             bg='#2b2b2b', fg='white', activebackground='#2b2b2b', width=12, relief='raised', bd=2).pack(side='left', padx=4)
        Button(filter_frame, text='Sold', command=lambda: set_stock_filter('sold'), font=('Avenir Next', 10, 'bold'),
               bg='#f6a6a6', fg='black', activebackground='#f6a6a6', width=10, relief='raised', bd=2).pack(side='left', padx=4)
        all_table_btn = Button(
            filter_frame,
            text='Open All Page',
            command=open_all_table_page,
            font=('Avenir Next', 10, 'bold'),
            bg='#2b2b2b',
            fg='white',
            activebackground='#2b2b2b',
            width=14,
            relief='raised',
            bd=2
        )
        all_table_btn.pack(side='left', padx=(12, 4))

        breakdown_card = Frame(win, bg='#e7f4ea', bd=1, relief='solid')
        breakdown_card.pack(fill='x', padx=12, pady=(8, 0))
        Label(
            breakdown_card,
            text='Available Stocks Breakdown (Series)',
            font=('Avenir Next', 11, 'bold'),
            bg='#e7f4ea',
            fg='#184d2c'
        ).pack(anchor='w', padx=10, pady=(8, 4))

        breakdown_tree = ttk.Treeview(
            breakdown_card,
            columns=('BRAND', 'SERIES', 'COUNT'),
            show='headings',
            height=5,
            style='Stock.Treeview'
        )
        breakdown_tree.heading('BRAND', text='BRAND')
        breakdown_tree.heading('SERIES', text='SERIES')
        breakdown_tree.heading('COUNT', text='AVAILABLE QTY')
        breakdown_tree.column('BRAND', width=170, minwidth=140, anchor='w')
        breakdown_tree.column('SERIES', width=240, minwidth=180, anchor='w')
        breakdown_tree.column('COUNT', width=140, minwidth=120, anchor='center')
        breakdown_tree.pack(fill='x', padx=10, pady=(0, 8))

        available_series_items = {}

        table_frame = Frame(win, bg='#eef6f8')

        stock_style = ttk.Style(win)
        try:
            stock_style.theme_use('clam')
        except Exception:
            pass
        stock_style.configure('Stock.Treeview',
                              font=('Avenir Next', 11),
                              rowheight=30,
                              background='#ffffff',
                              fieldbackground='#ffffff',
                              foreground='#1e2a32')
        stock_style.configure('Stock.Treeview.Heading',
                              font=('Avenir Next', 11, 'bold'),
                        background='#111111',
                              foreground='white',
                              relief='flat')
        stock_style.map('Stock.Treeview.Heading',
                    background=[('active', '#000000')],
                        foreground=[('active', 'white')])
        stock_style.map('Stock.Treeview',
                    background=[('selected', '#2f2f2f')],
                    foreground=[('selected', '#ffffff')])

        stock_tree = ttk.Treeview(table_frame, show='headings', height=12, style='Stock.Treeview')
        ysb = ttk.Scrollbar(table_frame, orient='vertical', command=stock_tree.yview)
        xsb = ttk.Scrollbar(table_frame, orient='horizontal', command=stock_tree.xview)
        stock_tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        stock_tree.grid(row=0, column=0, sticky='nsew')
        ysb.grid(row=0, column=1, sticky='ns')
        xsb.grid(row=1, column=0, sticky='ew')
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        bind_smooth_wheel(stock_tree, stock_tree)
        bind_smooth_wheel(breakdown_tree, breakdown_tree)

        headers = []
        headers_upper = []
        header_row_idx = 0  # 0-based index of the actual column-header row

        def classify_available_series(description_text):
            return svc_classify_available_series(description_text)

        def refresh_available_breakdown(breakdown_counts):
            breakdown_tree.delete(*breakdown_tree.get_children())

            def sort_key(item):
                brand, series = item[0]
                count = item[1]
                num_match = re.search(r'(\d+)', series)
                num = int(num_match.group(1)) if num_match else 999
                return (brand, num, series, -count)

            for (brand, series), count in sorted(breakdown_counts.items(), key=sort_key):
                item_id = f"{brand}|{series}"
                breakdown_tree.insert('', END, iid=item_id, values=(brand, series, str(count)))

        def open_series_details(event=None):
            selected = breakdown_tree.selection()
            if not selected:
                return

            item_id = selected[0]
            parts = item_id.split('|', 1)
            if len(parts) != 2:
                return
            brand, series = parts
            key = (brand, series)
            items = available_series_items.get(key, [])
            if not items:
                messagebox.showinfo('Series Details', 'No available item details found for this series.', parent=win)
                return

            details_win = Toplevel(win)
            details_win.title(f'{brand} {series} - Available Details')
            details_win.geometry('1220x560')
            details_win.configure(bg='#f2f2f2')

            Label(
                details_win,
                text=f'{brand} {series} Available Items ({len(items)} rows)',
                font=('Avenir Next', 12, 'bold'),
                bg='#f2f2f2',
                fg='#111111'
            ).pack(anchor='w', padx=10, pady=(10, 6))

            details_frame = Frame(details_win, bg='#f2f2f2')
            details_frame.pack(fill='both', expand=True, padx=10, pady=(0, 10))

            detail_cols = ['SHEET ROW'] + headers
            details_tree = ttk.Treeview(details_frame, columns=detail_cols, show='headings', style='Stock.Treeview')
            yscroll = ttk.Scrollbar(details_frame, orient='vertical', command=details_tree.yview)
            xscroll = ttk.Scrollbar(details_frame, orient='horizontal', command=details_tree.xview)
            details_tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

            details_tree.grid(row=0, column=0, sticky='nsew')
            yscroll.grid(row=0, column=1, sticky='ns')
            xscroll.grid(row=1, column=0, sticky='ew')
            details_frame.grid_rowconfigure(0, weight=1)
            details_frame.grid_columnconfigure(0, weight=1)

            for idx, col_name in enumerate(detail_cols):
                details_tree.heading(col_name, text=col_name)
                max_len = len(col_name)
                if idx == 0:
                    max_len = max(max_len, 10)
                else:
                    for row_num, row_values in items[:200]:
                        text = str(row_values[idx - 1]) if (idx - 1) < len(row_values) else ''
                        max_len = max(max_len, len(text))
                width = int(max_len * 8.0) + 22
                width = max(120, min(width, 520))
                if col_name.upper() in ('DESCRIPTION', 'NAME OF BUYER', 'NAME OF SELLER'):
                    width = max(width, 260)
                if col_name.upper() in ('PHONE NUMBER OF BUYER', 'PHONE NUMBER OF SELLER'):
                    width = max(width, 190)
                if col_name.upper() == 'SHEET ROW':
                    width = 110
                details_tree.column(col_name, width=width, minwidth=100, stretch=True, anchor='w')

            for idx, (row_num, row_values) in enumerate(items):
                tag = 'even' if idx % 2 == 0 else 'odd'
                padded = row_values + [''] * (len(headers) - len(row_values))
                details_tree.insert('', END, values=tuple([str(row_num)] + padded[:len(headers)]), tags=(tag,))

            details_tree.tag_configure('even', background='#fbfdff')
            details_tree.tag_configure('odd', background='#f3f8fc')

        breakdown_tree.bind('<Double-1>', open_series_details)
        breakdown_tree.bind('<Return>', open_series_details)

        def header_index(*aliases):
            return svc_stock_header_index(headers_upper, *aliases)

        def ensure_headers(values=None):
            nonlocal headers, headers_upper, header_row_idx
            if values is None:
                if stock_ws is None:
                    return False
                values = stock_ws.get_all_values()
            header_row_idx, headers, headers_upper = svc_detect_stock_headers(values)
            if not values or not any(str(c).strip() for c in values[header_row_idx] if header_row_idx < len(values)):
                if stock_ws is not None:
                    stock_ws.update('A1:H1', [headers])
            return True

        def configure_tree_columns(values):
            display_headers = headers + ['APP STATUS']
            stock_tree['columns'] = display_headers
            for col_idx, h in enumerate(display_headers):
                stock_tree.heading(h, text=h)
                sample_max = len(h)
                for row in values[header_row_idx + 1:header_row_idx + 201]:
                    text = ''
                    if col_idx < len(headers):
                        text = str(row[col_idx]) if col_idx < len(row) else ''
                    elif col_idx == len(headers):
                        text = 'PENDING DEAL'
                    if len(text) > sample_max:
                        sample_max = len(text)

                width = int(sample_max * 8.2) + 24
                width = max(130, min(width, 520))
                upper = h.upper()
                if upper in ('DESCRIPTION', 'NAME OF BUYER', 'NAME OF SELLER'):
                    width = max(width, 260)
                if upper in ('PHONE NUMBER OF BUYER', 'PHONE NUMBER OF SELLER'):
                    width = max(width, 190)
                if upper in ('RECEIPT & EVIDIENCE LINK', 'RECEIPT', 'EVIDENCE LINK'):
                    width = max(width, 320)
                if upper == 'APP STATUS':
                    width = max(width, 150)

                stock_tree.column(h, width=width, minwidth=110, stretch=True, anchor='w')

            stock_tree.tag_configure('even', background='#fbfdff')
            stock_tree.tag_configure('odd', background='#f3f8fc')
            stock_tree.tag_configure('pending', background='#fff7cc')
            stock_tree.tag_configure('needs_details', background='#e8f4ff')
            stock_tree.tag_configure('sold', background='#ffe1e1')

        def render_stock_rows(filter_text=''):
            values = stock_cache.get('values') or []
            if not values:
                stock_tree.delete(*stock_tree.get_children())
                all_rows_cache.clear()
                refresh_available_breakdown({})
                for key, label in stock_summary_labels.items():
                    label.config(text='0')
                status_lbl.config(text='No stock rows found')
                return

            configure_tree_columns(values)
            stock_tree.delete(*stock_tree.get_children())
            all_rows_cache.clear()
            available_series_items.clear()
            stock_render_token['value'] += 1
            render_token = stock_render_token['value']
            view_model = svc_build_stock_view(
                values,
                headers,
                headers_upper,
                header_row_idx,
                color_status_map=stock_cache.get('color_status_map') or {},
                filter_text=filter_text,
                filter_mode=stock_filter_mode['value']
            )
            counts = view_model['counts']
            available_breakdown = view_model['available_breakdown']
            all_rows_cache.extend(view_model['all_rows_cache'])
            available_series_items.update(view_model['available_series_items'])

            def preprocess_chunk(start=0, chunk_size=300):
                if render_token != stock_render_token['value']:
                    return
                end = min(start + chunk_size, len(all_rows_cache))
                if end < len(all_rows_cache):
                    status_lbl.config(text=f'Preparing rows {end}/{len(all_rows_cache)}...')
                    win.after(5, lambda: preprocess_chunk(end, chunk_size))
                    return

                for key, label in stock_summary_labels.items():
                    label.config(text=str(counts.get(key, 0)))

                refresh_available_breakdown(available_breakdown)
                insert_chunk()

            def insert_chunk(start=0, chunk_size=80):
                if render_token != stock_render_token['value']:
                    return

                end = min(start + chunk_size, len(all_rows_cache))
                for item in all_rows_cache[start:end]:
                    stock_tree.insert('', END, iid=str(item['row_num']), values=tuple(item['padded'][:len(headers)] + [item['label']]), tags=item['tags'])

                if end < len(all_rows_cache):
                    status_lbl.config(text=f'Loading rows {end}/{len(all_rows_cache)}...')
                    win.after(5, lambda: insert_chunk(end, chunk_size))
                else:
                    status_lbl.config(text=f'Loaded {len(all_rows_cache)} row(s) from stock sheet')

            preprocess_chunk(chunk_size=80)

        def populate_stock(filter_text='', force_refresh=False):
            nonlocal stock_ws
            filter_text = (filter_text or '').strip()

            if stock_cache['loading']:
                status_lbl.config(text='Loading stock data...')
                return

            if force_refresh:
                stock_cache['values'] = None
                stock_cache['color_status_map'] = {}

            if stock_cache['values'] is not None:
                render_stock_rows(filter_text)
                return

            if not force_refresh and postgres_sync_state.get('ready') and postgres_sync_manager:
                try:
                    cached_values = postgres_sync_manager.load_cached_rows('stock_values')
                    if isinstance(cached_values, list) and cached_values:
                        stock_cache['values'] = cached_values
                        stock_cache['color_status_map'] = {}
                        ensure_headers(cached_values)
                        render_stock_rows(filter_text)
                        # Keep cache-first UX fast, then fetch live sheet data in the background.
                        win.after(30, lambda: populate_stock(filter_text, force_refresh=True))
                        return
                except Exception as e:
                    logging.warning('Failed to load stock_values from PostgreSQL cache: %s', e)

            stock_cache['loading'] = True
            status_lbl.config(text='Connecting to stock sheet...')

            def worker():
                ws = stock_ws if stock_ws is not None else client.open_by_key(stock_sheet_id).sheet1
                values = ws.get_all_values()
                detected_idx = 0
                if values:
                    KEY_HEADERS = {'DESCRIPTION', 'DESC', 'S/N', 'SN', 'IMEI', 'MODEL', 'DATE', 'COLOUR', 'COLOR', 'STORAGE'}
                    for i, row in enumerate(values[:8]):
                        upper_cols = [str(c).strip().upper() for c in row]
                        if any(col in KEY_HEADERS for col in upper_cols):
                            detected_idx = i
                            break

                headers_local = []
                if values and detected_idx < len(values):
                    headers_local = [str(c).strip() or f'COL {i+1}' for i, c in enumerate(values[detected_idx])]

                desc_col = None
                if headers_local:
                    header_lookup = {str(col).strip().upper(): idx for idx, col in enumerate(headers_local)}
                    for key in ('DESCRIPTION', 'DESC', 'DETAILS'):
                        if key in header_lookup:
                            desc_col = header_lookup[key]
                            break

                color_status_map = {}
                if desc_col is not None:
                    try:
                        color_status_map = get_stock_color_status_map(stock_sheet_id, ws.title, desc_col, len(values))
                    except Exception:
                        color_status_map = {}

                return ws, values, color_status_map

            def on_done(result):
                nonlocal stock_ws
                ws, values, color_status_map = result
                stock_ws = ws
                ensure_headers(values)

                stock_cache['values'] = values
                stock_cache['color_status_map'] = color_status_map
                stock_cache['loading'] = False

                if postgres_sync_state.get('ready') and postgres_sync_manager:
                    try:
                        postgres_sync_manager.upsert_sheet_cache('stock_values', values)
                    except Exception as e:
                        logging.warning('Failed to refresh PostgreSQL stock_values cache: %s', e)

                render_stock_rows(filter_text)

            def on_fail(err):
                stock_cache['loading'] = False
                messagebox.showerror('Phone Stock', f'Could not open phone stock sheet:\n{err}', parent=win)
                status_lbl.config(text='Failed to load stock sheet')

            def run_worker():
                try:
                    result = worker()
                    win.after(0, lambda: on_done(result))
                except Exception as e:
                    win.after(0, lambda: on_fail(e))

            threading.Thread(target=run_worker, daemon=True).start()

        form_card = Frame(win, bg='#e6e6e6', bd=1, relief='solid')
        form_card.pack(fill='x', padx=12, pady=(0, 8))
        Label(form_card, text='Add New Stock Record (All Details)',
              bg='#e6e6e6', fg='#111111', font=('Avenir Next', 12, 'bold')).pack(anchor='w', padx=10, pady=(8, 4))

        form_canvas = Canvas(form_card, bg='#f5f5f5', height=150, highlightthickness=0)
        form_scroll = ttk.Scrollbar(form_card, orient='vertical', command=form_canvas.yview)
        form_canvas.configure(yscrollcommand=form_scroll.set)
        form_canvas.pack(side='left', fill='both', expand=True, padx=(8, 0), pady=(0, 8))
        form_scroll.pack(side='right', fill='y', padx=(0, 8), pady=(0, 8))

        form = Frame(form_canvas, bg='#f5f5f5')
        form_window = form_canvas.create_window((0, 0), window=form, anchor='nw')

        def _on_form_configure(event):
            form_canvas.configure(scrollregion=form_canvas.bbox('all'))

        def _on_canvas_configure(event):
            form_canvas.itemconfig(form_window, width=event.width)

        form.bind('<Configure>', _on_form_configure)
        form_canvas.bind('<Configure>', _on_canvas_configure)
        bind_smooth_wheel(form_canvas, form_canvas)

        stock_entries = {}
        essential_aliases = (
            'DESCRIPTION', 'MODEL', 'IMEI', 'S/N', 'COLOUR', 'STORAGE',
            'NAME OF SELLER', 'PHONE NUMBER OF SELLER', 'STATUS OF DEVICE', 'DATE BOUGHT'
        )
        hidden_form_aliases = {
            'NAME OF BUYER',
            'PHONE NUMBER OF BUYER'
        }

        def suggest_next_serial(values):
            return svc_suggest_next_serial(values, header_row_idx, headers_upper)

        def build_stock_form_fields(values):
            for child in form.winfo_children():
                child.destroy()
            stock_entries.clear()

            ordered_headers = svc_order_stock_form_headers(headers, hidden_form_aliases, essential_aliases)

            for idx, head in enumerate(ordered_headers):
                row = idx // 2
                col_group = idx % 2
                label_col = col_group * 2
                entry_col = label_col + 1

                Label(form, text=f'{head}:', bg='#f5f5f5', font=label_font).grid(
                    row=row, column=label_col, padx=(8, 4), pady=4, sticky='e'
                )
                ent = Entry(form, width=34, font=combo_font)
                ent.grid(row=row, column=entry_col, padx=(0, 14), pady=4, sticky='we')
                ent.bind('<Return>', lambda e: add_phone_stock())
                stock_entries[head] = ent

            form.grid_columnconfigure(1, weight=1)
            form.grid_columnconfigure(3, weight=1)

            defaults_map = svc_build_stock_form_defaults(values, header_row_idx, headers_upper)
            for head, ent in stock_entries.items():
                u = head.upper()
                if u in defaults_map:
                    ent.insert(0, defaults_map[u])

        def add_phone_stock(event=None):
            if stock_ws is None or stock_cache['loading']:
                messagebox.showinfo('Phone Stock', 'Stock sheet is still loading. Please wait a moment.', parent=win)
                return

            if not stock_entries:
                ensure_headers(stock_cache.get('values') or stock_ws.get_all_values())
                build_stock_form_fields(stock_cache.get('values') or stock_ws.get_all_values())

            ensure_headers()
            values = stock_ws.get_all_values()

            row_values, _ = svc_build_stock_row_values(
                headers,
                {head: (stock_entries.get(head).get().strip() if stock_entries.get(head) else '') for head in headers}
            )

            validation_error = svc_validate_stock_row(row_values, headers_upper)
            if validation_error:
                title = 'Missing Data' if 'at least one' in validation_error.lower() else 'Missing'
                messagebox.showwarning(title, validation_error, parent=win)
                return

            stock_ws.append_row(row_values, value_input_option='USER_ENTERED')

            for head, ent in stock_entries.items():
                ent.delete(0, END)

            ensure_headers()
            values = stock_ws.get_all_values()
            build_stock_form_fields(values)

            stock_cache['values'] = None
            stock_cache['color_status_map'] = {}
            populate_stock(search_entry.get(), force_refresh=True)
            status_lbl.config(text='Stock record added successfully')

        def adjust_selected_qty(delta):
            if stock_ws is None or stock_cache['loading']:
                messagebox.showinfo('Phone Stock', 'Stock sheet is still loading. Please wait a moment.', parent=win)
                return

            sel = stock_tree.selection()
            if not sel:
                messagebox.showwarning('No Selection', 'Select a row first.', parent=win)
                return

            qty_col = header_index('QTY', 'QUANTITY', 'STOCK', 'UNITS')
            if qty_col is None:
                messagebox.showwarning('Missing Column', 'No quantity column found in stock sheet.', parent=win)
                return

            row_num = int(sel[0])
            row_vals = stock_ws.row_values(row_num)
            current = 0
            if qty_col < len(row_vals):
                try:
                    current = int(str(row_vals[qty_col]).strip() or '0')
                except Exception:
                    current = 0

            new_qty, computed_status = svc_compute_stock_qty_status(current, delta)
            _queue_then_apply(
                'stock',
                'stock_update_qty',
                {
                    'kind': 'stock_update_cell',
                    'stock_sheet_id': stock_sheet_id,
                    'row': row_num,
                    'col': qty_col + 1,
                    'value': new_qty
                },
                lambda: stock_ws.update_cell(row_num, qty_col + 1, new_qty),
                async_only=True,
                cache_apply_callable=lambda rn=row_num, cn=qty_col + 1, nv=new_qty: (
                    postgres_sync_manager.update_cached_stock_value(rn, cn, nv)
                    if (postgres_sync_state.get('ready') and postgres_sync_manager) else None
                )
            )

            status_col = header_index('STATUS')
            if status_col is not None:
                new_status = computed_status
                _queue_then_apply(
                    'stock',
                    'stock_update_status',
                    {
                        'kind': 'stock_update_cell',
                        'stock_sheet_id': stock_sheet_id,
                        'row': row_num,
                        'col': status_col + 1,
                        'value': new_status
                    },
                    lambda: stock_ws.update_cell(row_num, status_col + 1, new_status),
                    async_only=True,
                    cache_apply_callable=lambda rn=row_num, cn=status_col + 1, nv=new_status: (
                        postgres_sync_manager.update_cached_stock_value(rn, cn, nv)
                        if (postgres_sync_state.get('ready') and postgres_sync_manager) else None
                    )
                )

            stock_cache['values'] = None
            stock_cache['color_status_map'] = {}
            populate_stock(search_entry.get(), force_refresh=True)
            status_lbl.config(text=f'Updated quantity to {new_qty}')

        def apply_sale_status_to_selected():
            if stock_ws is None or stock_cache['loading']:
                messagebox.showinfo('Phone Stock', 'Stock sheet is still loading. Please wait a moment.', parent=win)
                return

            sel = stock_tree.selection()
            if not sel:
                messagebox.showwarning('No Selection', 'Select a row first.', parent=win)
                return

            ensure_headers()
            row_num = int(sel[0])
            desc_col = header_index('DESCRIPTION', 'DESC', 'DETAILS', 'MODEL', 'PHONE MODEL')
            if desc_col is None:
                messagebox.showwarning('Missing Column', 'No DESCRIPTION/MODEL column found.', parent=win)
                return

            status_key, fill_color = svc_map_sale_status(sale_status_var.get())

            request_body = {
                'requests': [
                    {
                        'repeatCell': {
                            'range': {
                                'sheetId': stock_ws.id,
                                'startRowIndex': row_num - 1,
                                'endRowIndex': row_num,
                                'startColumnIndex': desc_col,
                                'endColumnIndex': desc_col + 1
                            },
                            'cell': {'userEnteredFormat': {'backgroundColor': fill_color}},
                            'fields': 'userEnteredFormat.backgroundColor'
                        }
                    }
                ]
            }

            _queue_then_apply(
                'stock',
                'stock_update_color',
                {
                    'kind': 'stock_batch_update',
                    'stock_sheet_id': stock_sheet_id,
                    'request_body': request_body
                },
                lambda: sheets_api_service.spreadsheets().batchUpdate(
                    spreadsheetId=stock_sheet_id,
                    body=request_body
                ).execute(),
                async_only=True
            )

            qty_col = header_index('QTY', 'QUANTITY', 'STOCK', 'UNITS')
            status_col = header_index('STATUS')
            sold_date_col = header_index('AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE')
            sold_date_value = datetime.now().strftime('%m/%d/%Y') if status_key == 'sold' else ''
            cell_updates = svc_build_sale_status_update_values(
                status_key,
                qty_col=qty_col,
                status_col=status_col,
                sold_date_col=sold_date_col,
                sold_date_value=sold_date_value
            )
            for update in cell_updates:
                _queue_then_apply(
                    'stock',
                    f"stock_{status_key}_cell_update",
                    {
                        'kind': 'stock_update_cell',
                        'stock_sheet_id': stock_sheet_id,
                        'row': row_num,
                        'col': update['col'],
                        'value': update['value']
                    },
                    lambda col=update['col'], value=update['value']: stock_ws.update_cell(row_num, col, value),
                    async_only=True,
                    cache_apply_callable=lambda rn=row_num, col=update['col'], value=update['value']: (
                        postgres_sync_manager.update_cached_stock_value(rn, col, value)
                        if (postgres_sync_state.get('ready') and postgres_sync_manager) else None
                    )
                )

            stock_cache['values'] = None
            stock_cache['color_status_map'] = {}
            populate_stock(search_entry.get(), force_refresh=True)
            status_lbl.config(text=f'Sale status updated to {status_key.upper()} for row {row_num}')

        controls = Frame(win, bg='#eef6f8')
        controls.pack(fill='x', padx=12, pady=(0, 10))
        Button(controls, text='Add Full Stock', command=add_phone_stock, font=button_font,
               bg='#27ae60', fg='black', activebackground='#27ae60', width=12, relief='raised', bd=3).grid(row=0, column=0, padx=5)
        Button(controls, text='Sell 1 (-1)', command=lambda: adjust_selected_qty(-1), font=button_font,
               bg='#e67e22', fg='black', activebackground='#e67e22', width=12, relief='raised', bd=3).grid(row=0, column=1, padx=5)
        Button(controls, text='Restock +1', command=lambda: adjust_selected_qty(1), font=button_font,
             bg='#2b2b2b', fg='white', activebackground='#2b2b2b', width=12, relief='raised', bd=3).grid(row=0, column=2, padx=5)
        sale_status_var = StringVar(value='Sold')
        sale_status_combo = ttk.Combobox(
            controls,
            textvariable=sale_status_var,
            values=['Sold', 'Pending Deal', 'Needs Details', 'Available'],
            state='readonly',
            width=14,
            font=('Avenir Next', 10)
        )
        sale_status_combo.grid(row=0, column=3, padx=(10, 4))
        Button(controls, text='Apply Sale Status', command=apply_sale_status_to_selected, font=button_font,
               bg='#16a085', fg='black', activebackground='#16a085', width=14, relief='raised', bd=3).grid(row=0, column=4, padx=5)
        Button(controls, text='Refresh', command=lambda: populate_stock(search_entry.get(), force_refresh=True), font=button_font,
               bg='#8e44ad', fg='black', activebackground='#8e44ad', width=10, relief='raised', bd=3).grid(row=0, column=5, padx=5)
        Button(controls, text='Close', command=win.destroy, font=button_font,
               bg='#95a5a6', fg='black', activebackground='#95a5a6', width=10, relief='raised', bd=3).grid(row=0, column=6, padx=5)

        search_entry.bind('<KeyRelease>', on_search_key_release)

        status_lbl.config(text='Opening stock manager...')
        win.after(120, lambda: populate_stock(force_refresh=True))

    def open_settings():
        window = Toplevel(root)
        window.title("Settings")
        window.geometry('760x520')
        window.configure(bg='#e8f4f8')

        Label(window, text="Sheet ID:", bg='#e8f4f8').grid(row=0, column=0, padx=10, pady=10, sticky='e')
        sheet_entry = Entry(window, width=40)
        sheet_entry.grid(row=0, column=1, padx=10, pady=10)
        sheet_entry.insert(0, config.get('sheet_id', ''))

        Label(window, text="Credentials file:", bg='#e8f4f8').grid(row=1, column=0, padx=10, pady=10, sticky='e')
        cred_entry = Entry(window, width=40)
        cred_entry.grid(row=1, column=1, padx=10, pady=10)
        cred_entry.insert(0, config.get('credentials_file', ''))

        Label(window, text="Company name:", bg='#e8f4f8').grid(row=2, column=0, padx=10, pady=10, sticky='e')
        company_entry = Entry(window, width=40)
        company_entry.grid(row=2, column=1, padx=10, pady=10)
        company_entry.insert(0, config.get('company_name', ''))

        Label(window, text="Google Contacts OAuth:", bg='#e8f4f8').grid(row=3, column=0, padx=10, pady=10, sticky='e')
        oauth_entry = Entry(window, width=40)
        oauth_entry.grid(row=3, column=1, padx=10, pady=10)
        oauth_entry.insert(0, config.get('contacts_oauth_file', ''))

        Label(window, text="Phone Stock Sheet ID:", bg='#e8f4f8').grid(row=4, column=0, padx=10, pady=10, sticky='e')
        phone_stock_entry = Entry(window, width=40)
        phone_stock_entry.grid(row=4, column=1, padx=10, pady=10)
        phone_stock_entry.insert(0, config.get('phone_stock_sheet_id', ''))

        Label(window, text="PostgreSQL DSN:", bg='#e8f4f8').grid(row=5, column=0, padx=10, pady=10, sticky='e')
        postgres_dsn_entry = Entry(window, width=62)
        postgres_dsn_entry.grid(row=5, column=1, padx=10, pady=10, sticky='w')
        postgres_dsn_entry.insert(0, config.get('postgres_dsn', ''))

        Label(window, text="Sync pull interval (sec):", bg='#e8f4f8').grid(row=6, column=0, padx=10, pady=10, sticky='e')
        sync_interval_entry = Entry(window, width=12)
        sync_interval_entry.grid(row=6, column=1, padx=10, pady=10, sticky='w')
        sync_interval_entry.insert(0, str(config.get('sync_pull_interval_sec', 90)))

        pg_enabled_var = BooleanVar(value=bool(config.get('enable_postgres_cache', True)))
        fallback_var = BooleanVar(value=bool(config.get('legacy_sheet_fallback', True)))
        record_id_var = BooleanVar(value=bool(config.get('record_id_rollout', True)))

        Checkbutton(window, text='Enable PostgreSQL cache mode', variable=pg_enabled_var, bg='#e8f4f8').grid(row=7, column=1, padx=10, pady=(2, 2), sticky='w')
        Checkbutton(window, text='Keep legacy sheet fallback', variable=fallback_var, bg='#e8f4f8').grid(row=8, column=1, padx=10, pady=(2, 2), sticky='w')
        Checkbutton(window, text='RECORD_ID rollout approved', variable=record_id_var, bg='#e8f4f8').grid(row=9, column=1, padx=10, pady=(2, 8), sticky='w')

        def browse_oauth():
            file_path = filedialog.askopenfilename(
                parent=window,
                title="Select Google Contacts OAuth JSON",
                initialdir=os.path.expanduser('~/Downloads'),
                filetypes=[('JSON files', '*.json'), ('All files', '*.*')]
            )
            if file_path:
                oauth_entry.delete(0, 'end')
                oauth_entry.insert(0, file_path)

        Button(window, text='Browse', command=browse_oauth).grid(row=3, column=2, padx=5)

        def open_sync_diagnostics():
            diag = Toplevel(window)
            diag.title('Sync Diagnostics')
            diag.geometry('680x420')
            diag.configure(bg='#eef6f8')

            Label(diag, text='Sync Diagnostics', font=section_font, bg='#eef6f8', fg='#1b1b1b').pack(anchor='w', padx=14, pady=(14, 8))
            status_box = scrolledtext.ScrolledText(diag, width=88, height=16, font=('Courier', 10), wrap='word')
            status_box.pack(fill='both', expand=True, padx=14, pady=(0, 10))

            def refresh_diag_view():
                lines = [
                    f"postgres_enabled: {config.get('enable_postgres_cache', True)}",
                    f"postgres_ready: {postgres_sync_state.get('ready')}",
                    f"postgres_status: {postgres_sync_state.get('last_status')}",
                    f"postgres_error: {postgres_sync_state.get('last_error')}",
                    f"fallback_enabled: {config.get('legacy_sheet_fallback', True)}",
                    f"startup_mode: {config.get('startup_mode', 'cache_then_sync')}",
                    f"pull_interval_sec: {config.get('sync_pull_interval_sec', 90)}",
                    f"conflict_policy: {config.get('sync_conflict_policy', 'sheet_wins')}",
                    f"record_id_rollout: {config.get('record_id_rollout', True)}",
                ]

                if postgres_sync_state.get('ready') and postgres_sync_manager:
                    try:
                        snapshot = postgres_sync_manager.get_sync_snapshot()
                        lines.append('')
                        lines.append('postgres_snapshot:')
                        lines.append(json.dumps(snapshot, indent=2, default=str))
                    except Exception as e:
                        lines.append('')
                        lines.append(f'postgres_snapshot_error: {e}')

                status_box.config(state='normal')
                status_box.delete('1.0', END)
                status_box.insert(END, '\n'.join(lines))
                status_box.config(state='disabled')

            def run_record_id_rollout_ui():
                def worker():
                    stock_sheet_id = extract_sheet_id(config.get('phone_stock_sheet_id', ''))
                    return rollout_record_ids_for_known_sheets(sheet, client, stock_sheet_id)

                def done(result):
                    main_updated = result.get('main', {}).get('updated', 0)
                    stock_updated = result.get('stock', {}).get('updated', 0)
                    main_error = result.get('main', {}).get('error', '')
                    stock_error = result.get('stock', {}).get('error', '')

                    if main_error or stock_error:
                        messagebox.showwarning(
                            'RECORD_ID Rollout',
                            f"Completed with warnings. Main updated={main_updated}, Stock updated={stock_updated}\n"
                            f"Main error: {main_error or 'None'}\nStock error: {stock_error or 'None'}",
                            parent=diag
                        )
                    else:
                        messagebox.showinfo(
                            'RECORD_ID Rollout',
                            f"Rollout complete. Main updated={main_updated}, Stock updated={stock_updated}",
                            parent=diag
                        )
                    refresh_diag_view()

                run_async(worker, on_complete=done)

            btns = Frame(diag, bg='#eef6f8')
            btns.pack(fill='x', padx=14, pady=(0, 12))
            Button(btns, text='Refresh Status', command=refresh_diag_view, font=button_font,
                   bg='#95a5a6', fg='black', activebackground='#95a5a6', width=14, relief='raised', bd=3).pack(side='left', padx=(0, 8))
            Button(btns, text='Run RECORD_ID Rollout', command=run_record_id_rollout_ui, font=button_font,
                   bg='#16a085', fg='black', activebackground='#16a085', width=22, relief='raised', bd=3).pack(side='left')

            refresh_diag_view()

        def save_settings():
            config['sheet_id'] = extract_sheet_id(sheet_entry.get().strip())
            config['credentials_file'] = cred_entry.get().strip()
            config['company_name'] = company_entry.get().strip()
            config['contacts_oauth_file'] = oauth_entry.get().strip()
            config['phone_stock_sheet_id'] = extract_sheet_id(phone_stock_entry.get().strip())
            config['postgres_dsn'] = postgres_dsn_entry.get().strip()
            try:
                interval = int(sync_interval_entry.get().strip() or '90')
            except ValueError:
                interval = 90
            config['sync_pull_interval_sec'] = max(15, interval)
            config['enable_postgres_cache'] = bool(pg_enabled_var.get())
            config['legacy_sheet_fallback'] = bool(fallback_var.get())
            config['record_id_rollout'] = bool(record_id_var.get())
            config['startup_mode'] = 'cache_then_sync'
            config['sync_conflict_policy'] = 'sheet_wins'
            save_config(config)
            messagebox.showinfo('Settings', 'Settings saved. Restart app to apply new config if needed.')
            window.destroy()

        Button(window, text='Sync Diagnostics', command=open_sync_diagnostics).grid(row=10, column=0, pady=20, padx=10)
        Button(window, text='Save', command=save_settings).grid(row=10, column=1, pady=20, padx=10, sticky='w')

    def undo_payment_ui():
        message = undo_last_payment()
        messagebox.showinfo('Undo', message)
        if 'undone' in message.lower():
            refresh_debtors()

    def redo_payment_ui():
        message = redo_last_payment()
        messagebox.showinfo('Redo', message)
        if 'reapplied' in message.lower():
            refresh_debtors()

    def refresh_payment_service_options(selected_name=None):
        nonlocal service_target_map

        selected_name = (selected_name or payment_client_combo.get()).strip()
        current_selection = payment_service_combo.get().strip()
        service_target_map = {}

        if not selected_name:
            payment_service_combo['values'] = ('Automatic sequence',)
            payment_service_combo.set('Automatic sequence')
            update_payment_preview('')
            return

        try:
            outstanding_items, _ = get_customer_outstanding_items_from_data(selected_name)
        except Exception:
            outstanding_items = []

        options = ['Automatic sequence']
        for item in outstanding_items:
            option_label = format_service_option(item)
            options.append(option_label)
            service_target_map[option_label] = item['row_idx']

        payment_service_combo['values'] = tuple(options)
        if current_selection in service_target_map or current_selection == 'Automatic sequence':
            payment_service_combo.set(current_selection)
        else:
            payment_service_combo.set('Automatic sequence')
        queue_payment_preview(selected_name, delay_ms=60)

    def on_payment_client_keyrelease(event):
        nonlocal payment_client_filter_job

        # Arrow/confirm keys: navigate or confirm the suggestion popup
        if event.keysym == 'Escape':
            payment_client_combo._hide_popup()
            return
        if event.keysym == 'Tab':
            payment_client_combo._hide_popup()
            return
        if event.keysym == 'Return':
            if not payment_client_combo.confirm_selection():
                refresh_payment_service_options()
            return
        if event.keysym == 'Down':
            payment_client_combo.navigate(+1)
            return 'break'
        if event.keysym == 'Up':
            payment_client_combo.navigate(-1)
            return 'break'

        if payment_client_filter_job is not None:
            root.after_cancel(payment_client_filter_job)

        def apply_filter():
            nonlocal payment_client_filter_job
            lookup_value = payment_client_combo.get().strip().lower()
            if not lookup_value:
                payment_client_combo._hide_popup()
            else:
                filtered = [n for n in client_names if lookup_value in n.lower()][:50]
                payment_client_combo['values'] = filtered
                payment_client_combo.show_popup(filtered)
            payment_client_filter_job = None

        payment_client_filter_job = root.after(150, apply_filter)

    def on_service_keyrelease(event):
        nonlocal payment_service_filter_job

        if event.keysym in {'Down', 'Up', 'Return', 'Tab', 'Escape'}:
            return
        if payment_service_filter_job is not None:
            root.after_cancel(payment_service_filter_job)

        def apply_service_filter():
            nonlocal payment_service_filter_job

            value = payment_service_combo.get().strip().lower()
            options = ['Automatic sequence'] + [option for option in service_target_map if value in option.lower()][:50]
            payment_service_combo['values'] = tuple(options)
            queue_payment_preview(delay_ms=100)
            payment_service_filter_job = None

        payment_service_filter_job = root.after(120, apply_service_filter)

    def move_combo_selection(event):
        combo = event.widget
        values = list(combo.cget('values'))
        if not values:
            return 'break'

        current_value = combo.get()
        try:
            current_index = values.index(current_value)
        except ValueError:
            current_index = -1

        if event.keysym == 'Down':
            next_index = min(current_index + 1, len(values) - 1) if current_index >= 0 else 0
        else:
            next_index = max(current_index - 1, 0) if current_index >= 0 else 0

        combo.set(values[next_index])
        combo.icursor(END)
        # Avoid generating synthetic Down-like events here to prevent recursive callbacks.
        if combo == payment_client_combo:
            refresh_payment_service_options(combo.get())
        elif combo == payment_service_combo:
            queue_payment_preview(delay_ms=60)
        return 'break'

    def capture_generate_bill(name):
        return svc_generate_bill_text(name, data, config.get('payment_details', '8168364881\nOPAY (PAYCOM)\nAKINPELUMI GEORGE AYOMIDE'))

    def refresh_debtors():
        refresh_debtors_data()
        payment_client_combo['values'] = client_names
        refresh_payment_service_options()
        update_payment_preview()
        if dashboard_metric_labels:
            sales_snapshot = compute_sales_snapshot()
            set_metric_value('debtors', str(len(sorted_debtors)))
            set_metric_value('clients', str(len(clients)))
            set_metric_value('outstanding', f"NGN {total_debtors_amount:,}")
            set_metric_value('customers_today', str(sales_snapshot['customers_today']))
            set_metric_value('services_today', str(sales_snapshot['services_today']))
            set_metric_value('sales_today', f"NGN {sales_snapshot['sales_today']:,}")
            set_metric_value('sales_month', f"NGN {sales_snapshot['sales_month']:,}")
            render_weekly_sales_graph(sales_snapshot['week_totals'])
            render_daily_sales_graph(sales_snapshot['daily_totals'])

    root = Tk()
    if stock_window_mode:
        # Dedicated stock-window processes should not show the dashboard root.
        root.withdraw()
    root.title("ATLANTA GEORGIA_TECH - Client Billing Manager")
    root.geometry('1320x860')
    # Avoid forcing topmost/focus to keep native macOS window controls stable
    # root.attributes('-topmost', True)
    # root.lift()
    # root.focus_force()
    root.configure(bg='#f1ece3')

    # Professional style preferences
    style = ttk.Style(root)
    # try:
    #     style.theme_use('clam')
    # except Exception:
    #     pass

    root.option_add('*Font', 'Helvetica 11')
    style.configure('TButton', font=('Helvetica', 11, 'bold'), foreground='#FFFFFF', background='#2c3e50', padding=8)
    style.map('TButton', background=[('active', '#1d3557'), ('disabled', '#7f8c8d')], foreground=[('disabled', '#bdc3c7')])
    style.configure('TLabel', background='#f1ece3', foreground='#2d3e50')
    style.configure('TFrame', background='#f1ece3')

    # Fonts
    title_font = tkFont.Font(family='Georgia', size=24, weight='bold')
    section_font = tkFont.Font(family='Avenir Next', size=15, weight='bold')
    label_font = tkFont.Font(family='Avenir Next', size=12)
    button_font = tkFont.Font(family='Avenir Next', size=13, weight='bold')
    combo_font = tkFont.Font(family='Avenir Next', size=12)
    math_italic_font = tkFont.Font(family='Cambria Math', size=12, slant='italic', weight='bold')
    debtor_list_font = tkFont.Font(family='Avenir Next', size=12)

    # Whole-page scroll container (website-style vertical scrolling)
    page_wrap = Frame(root, bg='#f1ece3')
    page_wrap.pack(fill='both', expand=True)
    page_canvas = Canvas(page_wrap, bg='#f1ece3', highlightthickness=0)
    page_scrollbar = ttk.Scrollbar(page_wrap, orient='vertical', command=page_canvas.yview)
    page_canvas.configure(yscrollcommand=page_scrollbar.set)
    page_scrollbar.pack(side='right', fill='y')
    page_canvas.pack(side='left', fill='both', expand=True)

    page_content = Frame(page_canvas, bg='#f1ece3')
    page_canvas_window = page_canvas.create_window((0, 0), window=page_content, anchor='nw')
    wheel_remainder = 0.0

    def _update_page_scrollregion(_event=None):
        page_canvas.configure(scrollregion=page_canvas.bbox('all'))

    def _fit_page_width(event):
        page_canvas.itemconfig(page_canvas_window, width=event.width)

    def _on_page_wheel(event):
        nonlocal wheel_remainder
        # Let popup windows and text-like widgets keep native scrolling behavior.
        if event.widget.winfo_toplevel() != root:
            return
        widget_class = event.widget.winfo_class()
        if widget_class in ('Text', 'Entry', 'TCombobox', 'Listbox'):
            return
        if getattr(event, 'num', None) == 4:
            page_canvas.yview_scroll(-2, 'units')
            return 'break'
        if getattr(event, 'num', None) == 5:
            page_canvas.yview_scroll(2, 'units')
            return 'break'
        delta = getattr(event, 'delta', 0)
        if delta == 0:
            return 'break'

        if sys.platform == 'darwin':
            # macOS trackpads emit many tiny deltas; accumulate for smoother motion.
            wheel_remainder += (-delta / 12.0)
            step = int(wheel_remainder)
            if step != 0:
                page_canvas.yview_scroll(step, 'units')
                wheel_remainder -= step
        else:
            step = -1 * int(delta / 120) if abs(delta) >= 120 else (-1 if delta > 0 else 1)
            page_canvas.yview_scroll(step, 'units')
        return 'break'

    page_content.bind('<Configure>', _update_page_scrollregion)
    page_canvas.bind('<Configure>', _fit_page_width)
    page_canvas.bind_all('<MouseWheel>', _on_page_wheel)
    page_canvas.bind_all('<Button-4>', _on_page_wheel)
    page_canvas.bind_all('<Button-5>', _on_page_wheel)

    hero_frame = Frame(page_content, bg='#0b0b0b', bd=0, highlightthickness=0)
    hero_frame.pack(fill='x', padx=12, pady=(8, 6))

    hero_left = Frame(hero_frame, bg='#0b0b0b')
    hero_left.pack(side='left', fill='x', expand=True, padx=12, pady=10)

    logo_frame = Frame(hero_left, bg='#0b0b0b')
    logo_frame.pack(anchor='w', pady=(0, 4))
    for logo_path in find_logo_paths():
        try:
            # PhotoImage handles png/gif reliably; JPEG uses Pillow when available.
            lower = logo_path.lower()
            if lower.endswith(('.jpg', '.jpeg', '.webp')) and pillow_available:
                pil_img = Image.open(logo_path)
                pil_img.thumbnail((130, 80))
                img = ImageTk.PhotoImage(pil_img)
            else:
                img = PhotoImage(file=logo_path)
                sample_x = max(1, int(img.width() / 130))
                sample_y = max(1, int(img.height() / 80))
                img = img.subsample(sample_x, sample_y)
            logo_refs.append(img)
            Label(logo_frame, image=img, bg='#0b0b0b').pack(side='left', padx=(0, 10))
        except Exception:
            continue

    Label(hero_left, text="Atlanta Georgia_Tech", font=title_font, bg='#0b0b0b', fg='#f7f0df').pack(anchor='w')
    Label(hero_left, text="Billing, stock, and client operations in one workspace.",
          font=label_font, bg='#0b0b0b', fg='#d8e4e1').pack(anchor='w', pady=(4, 8))

    status_chip = Frame(hero_left, bg='#171717', padx=10, pady=6)
    status_chip.pack(anchor='w')
    status_label = Label(status_chip, text="Ready", font=('Avenir Next', 11, 'bold'), bg='#171717', fg='#f8f3e8')
    status_label.pack(anchor='w')

    progress_bar = ttk.Progressbar(hero_left, mode='indeterminate', length=220)
    progress_bar.pack(anchor='w', pady=(6, 0))

    def build_metric_card(parent, row, col, title, key, masked=False):
        card = Frame(parent, bg='#f7f0df', padx=10, pady=8)
        card.grid(row=row, column=col, sticky='ew', pady=5, padx=5)
        Label(card, text=title, font=('Avenir Next', 10, 'bold'), bg='#f7f0df', fg='#6b7b76').pack(anchor='w')

        value_row = Frame(card, bg='#f7f0df')
        value_row.pack(fill='x', pady=(4, 0))
        value_label = Label(card, text='--', font=('Georgia', 14, 'bold'), bg='#f7f0df', fg='#1e363e')
        value_label.pack(in_=value_row, side='left', anchor='w')
        dashboard_metric_labels[key] = value_label

        if masked:
            sensitive_metric_state[key] = {'actual': '--', 'visible': False, 'label': value_label}
            value_label.config(text='***')

            reveal_btn = Button(
                value_row,
                text='👁 Hold',
                font=('Avenir Next', 9, 'bold'),
                bg='#dbe8e5',
                fg='#2f4f4f',
                activebackground='#dbe8e5',
                activeforeground='#2f4f4f',
                relief='flat',
                bd=0,
                cursor='hand2',
                padx=8,
                pady=2
            )
            reveal_btn.pack(side='right')
            reveal_btn.bind('<ButtonPress-1>', lambda _e, k=key: reveal_metric(k, True))
            reveal_btn.bind('<ButtonRelease-1>', lambda _e, k=key: reveal_metric(k, False))
            reveal_btn.bind('<Leave>', lambda _e, k=key: reveal_metric(k, False))

    dashboard_body = Frame(page_content, bg='#f1ece3')
    dashboard_body.pack(fill='both', expand=True, padx=12, pady=0)

    sidebar_frame = Frame(
        dashboard_body,
        bg='#f7f0df',
        bd=1,
        relief='solid',
        highlightbackground='#ddcfb6',
        highlightthickness=1
    )
    sidebar_frame.configure(width=280)
    sidebar_frame.pack_propagate(False)
    sidebar_frame.pack(side='left', fill='both', expand=False, padx=(0, 16))

    Label(sidebar_frame, text="General Actions", font=section_font, bg='#f7f0df', fg='#111111').pack(anchor='w', padx=16, pady=(16, 4))
    Label(sidebar_frame, text="Quick icon grid", font=('Avenir Next', 10), bg='#f7f0df', fg='#6b7b76').pack(anchor='w', padx=16, pady=(0, 10))

    action_grid_frame = Frame(sidebar_frame, bg='#f7f0df')
    action_grid_frame.pack(fill='both', expand=True, padx=10, pady=(0, 12))

    for c in range(3):
        action_grid_frame.grid_columnconfigure(c, weight=1)

    def make_action_tile(parent, row, col, icon, label, command, bg, fg='#1b1b1b'):
        text = f"{icon}\n{label}"
        btn = Button(
            parent,
            text=text,
            command=command,
            font=('Avenir Next', 11, 'bold'),
            bg=bg,
            fg=fg,
            activebackground=bg,
            activeforeground=fg,
            justify='center',
            anchor='center',
            width=7,
            height=3,
            padx=6,
            pady=6,
            relief='flat',
            bd=0,
            cursor='hand2'
        )
        btn.grid(row=row, column=col, sticky='nsew', padx=6, pady=6)
        action_buttons.append(btn)
        return btn

    make_action_tile(action_grid_frame, 0, 0, '📒', 'Debtors', open_debtors_window, '#8bd3dd')
    make_action_tile(action_grid_frame, 0, 1, '🔄', 'Refresh', refresh_list, '#f4b942')
    make_action_tile(action_grid_frame, 0, 2, '↩', 'Undo', undo_payment_ui, '#95d5b2')
    make_action_tile(action_grid_frame, 1, 0, '↪', 'Redo', redo_payment_ui, '#b8f2e6')
    make_action_tile(action_grid_frame, 1, 1, '📦', 'Stock', launch_stock_manager_detached, '#9ad1d4')
    make_action_tile(action_grid_frame, 1, 2, '👥', 'Clients', open_client_manager, '#7bd389')
    make_action_tile(action_grid_frame, 2, 0, '🛠', 'Fix', open_name_fix_window, '#f7a072')
    make_action_tile(action_grid_frame, 2, 1, '⚙', 'Settings', open_settings, '#cab8ff')
    make_action_tile(action_grid_frame, 2, 2, '⏻', 'Exit', root.destroy, '#d8d8d8')

    workspace_frame = Frame(dashboard_body, bg='#f1ece3')
    workspace_frame.pack(side='left', fill='both', expand=True)

    summary_frame = Frame(workspace_frame, bg='#0f0f0f', bd=0, highlightthickness=0)
    summary_frame.pack(fill='x', pady=(0, 10))
    Label(summary_frame, text='Live Summary', font=section_font, bg='#0f0f0f', fg='#f7f0df').pack(anchor='w', padx=12, pady=(10, 2))
    metrics_grid = Frame(summary_frame, bg='#0f0f0f')
    metrics_grid.pack(fill='x', padx=8, pady=(4, 8))

    for c in range(4):
        metrics_grid.grid_columnconfigure(c, weight=1)

    build_metric_card(metrics_grid, 0, 0, 'Customers Owing', 'debtors')
    build_metric_card(metrics_grid, 0, 1, 'Saved Clients', 'clients')
    build_metric_card(metrics_grid, 0, 2, 'Total Outstanding', 'outstanding', masked=True)
    build_metric_card(metrics_grid, 0, 3, 'Customers Today', 'customers_today')
    build_metric_card(metrics_grid, 1, 0, 'Services Today', 'services_today')
    build_metric_card(metrics_grid, 1, 1, 'Sales Today', 'sales_today', masked=True)
    build_metric_card(metrics_grid, 1, 2, 'Sales This Month', 'sales_month', masked=True)

    def _update_dashboard_split(event=None):
        total_width = dashboard_body.winfo_width()
        if total_width <= 0:
            return
        # Keep the left action section at ~20% of dashboard width.
        target = int(total_width * 0.20)
        sidebar_frame.configure(width=max(240, target))

    dashboard_body.bind('<Configure>', _update_dashboard_split)

    payment_row = Frame(workspace_frame, bg='#f1ece3')
    payment_row.pack(fill='both', expand=True)

    payment_frame = Frame(payment_row, bg='#fff8ef', bd=0, highlightbackground='#e8dbc1', highlightthickness=1)
    payment_frame.pack(side='left', fill='both', expand=True, padx=(0, 8))

    payment_header = Frame(payment_frame, bg='#fff8ef')
    payment_header.pack(fill='x', padx=18, pady=(16, 8))
    Label(payment_header, text="Update Client Payment", font=section_font, bg='#fff8ef', fg='#34495e').pack(anchor='w')
    Label(payment_header, text="Choose a client, preview the balance flow, then apply payment with confidence.",
          font=('Avenir Next', 10), bg='#fff8ef', fg='#7a6d5c').pack(anchor='w', pady=(4, 0))

    payment_input_frame = Frame(payment_frame, bg='#fff8ef')
    payment_input_frame.pack(fill='x', padx=18, pady=8)
    payment_input_frame.grid_columnconfigure(0, weight=1)

    Label(payment_input_frame, text="Select Client:", font=label_font, bg='#fff8ef').grid(row=0, column=0, padx=5, pady=(4, 2), sticky='w')
    payment_client_combo = _AutocompleteEntry(payment_input_frame, root, width=52, font=combo_font)
    payment_client_combo.grid(row=1, column=0, padx=5, pady=(0, 6), sticky='ew')
    payment_client_combo._selected_callback = lambda name: refresh_payment_service_options(name)
    payment_client_combo.bind('<KeyRelease>', on_payment_client_keyrelease)
    payment_client_combo.bind('<FocusOut>', lambda event: (
        None if (payment_client_combo._listbox and root.focus_get() == payment_client_combo._listbox)
        else refresh_payment_service_options()
    ))

    Label(payment_input_frame, text="Payment Amount (NGN):", font=label_font, bg='#fff8ef').grid(row=2, column=0, padx=5, pady=(4, 2), sticky='w')
    amount_entry = Entry(payment_input_frame, width=54, font=combo_font)
    amount_entry.grid(row=3, column=0, padx=5, pady=(0, 6), sticky='ew')

    Label(payment_input_frame, text="Service for Partial Payment:", font=label_font, bg='#fff8ef').grid(row=4, column=0, padx=5, pady=(4, 2), sticky='w')
    payment_service_combo = ttk.Combobox(payment_input_frame, width=52, font=combo_font)
    payment_service_combo.grid(row=5, column=0, padx=5, pady=(0, 8), sticky='ew')
    payment_service_combo['values'] = ('Automatic sequence',)
    payment_service_combo.set('Automatic sequence')
    payment_service_combo.bind('<KeyRelease>', on_service_keyrelease)
    payment_service_combo.bind('<<ComboboxSelected>>', lambda event: queue_payment_preview(delay_ms=60))
    payment_service_combo.bind('<Down>', move_combo_selection)
    payment_service_combo.bind('<Up>', move_combo_selection)

    payment_button_frame = Frame(payment_frame, bg='#fff8ef')
    payment_button_frame.pack(padx=18, pady=(0, 18), anchor='w')
    apply_payment_btn = Button(payment_button_frame, text="Apply Payment", command=update_payment_ui, font=button_font, bg='#d46a4c', fg='black', activeforeground='black', activebackground='#d46a4c', width=18, height=1, relief='raised', bd=3)
    apply_payment_btn.grid(row=0, column=0, padx=10)

    payment_preview_frame = Frame(payment_row, bg='#f7f0df', bd=0, highlightbackground='#e0d2b1', highlightthickness=1)
    payment_preview_frame.pack(side='left', fill='both', expand=True, padx=(8, 0))
    preview_title_frame = Frame(payment_preview_frame, bg='#f7f0df')
    preview_title_frame.pack(fill='x', padx=18, pady=(16, 8))
    Label(preview_title_frame, text="Payment Preview", font=section_font, bg='#f7f0df', fg='#34495e').pack(anchor='w')
    Label(preview_title_frame, text="See the order and balances before money is applied.", font=('Avenir Next', 10), bg='#f7f0df', fg='#7a6d5c').pack(anchor='w', pady=(4, 0))
    payment_preview_box = scrolledtext.ScrolledText(payment_preview_frame, width=80, height=16, font=('Courier', 11), wrap='word', bd=0, padx=14, pady=12)
    payment_preview_box.pack(fill='both', expand=True, padx=10, pady=(4, 10))
    payment_preview_box.config(state='disabled')

    def _update_payment_row_split(event=None):
        total_width = payment_row.winfo_width()
        if total_width <= 0:
            return
        left_w = int(total_width * 0.45)
        payment_frame.configure(width=max(420, left_w))

    payment_row.bind('<Configure>', _update_payment_row_split)

    sales_graph_frame = Frame(workspace_frame, bg='#eef7f5', bd=0, highlightbackground='#d2e3df', highlightthickness=1)
    sales_graph_frame.pack(fill='x', pady=(12, 0))
    Label(sales_graph_frame, text='Sales Trend', font=section_font, bg='#eef7f5', fg='#2f565d').pack(anchor='w', padx=16, pady=(12, 2))
    Label(sales_graph_frame, text='Weekly and daily sales statistics (returned goods excluded).', font=('Avenir Next', 10), bg='#eef7f5', fg='#6b7b76').pack(anchor='w', padx=16, pady=(0, 8))
    weekly_sales_canvas['widget'] = Canvas(sales_graph_frame, bg='#eef7f5', height=170, highlightthickness=0)
    weekly_sales_canvas['widget'].pack(fill='x', padx=10, pady=(0, 10))
    daily_sales_canvas['widget'] = Canvas(sales_graph_frame, bg='#eef7f5', height=130, highlightthickness=0)
    daily_sales_canvas['widget'].pack(fill='x', padx=10, pady=(0, 10))

    refresh_debtors()
    update_status("Ready. Use Show Debtors to view debtor details and bill previews.")

    if stock_window_mode:
        # Open stock manager directly in this dedicated process.
        root.after(120, open_phone_stock_manager)

    root.mainloop()


def console_menu():
    while True:
        print("\nOptions:")
        print("1. Generate bill for a client")
        print("2. Update payment for a client")
        print("3. Exit")

        choice = input("Choose an option (1-3): ").strip()

        if choice == "1":
            name = input("Enter customer name: ").strip()
            generate_bill(name)
        elif choice == "2":
            name = input("Enter customer name: ").strip()
            try:
                amount = int(input("Enter payment amount (NGN): ").strip())
                update_payment(name, amount)
            except ValueError:
                print("Invalid amount. Please enter a number.")
        elif choice == "3":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please select 1, 2, or 3.")


def _init_postgres_sync():
    global postgres_sync_manager

    postgres_sync_state['enabled'] = bool(config.get('enable_postgres_cache', True))
    postgres_sync_state['ready'] = False
    postgres_sync_state['last_status'] = 'disabled'
    postgres_sync_state['last_error'] = ''

    if not postgres_sync_state['enabled']:
        logging.info('PostgreSQL cache mode is disabled by config.')
        return

    if create_postgres_sync_manager is None or not PSYCOPG2_AVAILABLE:
        postgres_sync_state['last_status'] = 'driver_missing'
        postgres_sync_state['last_error'] = 'psycopg2 not available in active Python environment'
        logging.warning('PostgreSQL cache enabled but psycopg2 is unavailable; using legacy sheet mode.')
        return

    postgres_sync_manager = create_postgres_sync_manager(config, logger=logging.getLogger(__name__))
    if not postgres_sync_manager.ready:
        postgres_sync_state['last_status'] = 'dsn_missing'
        postgres_sync_state['last_error'] = 'postgres_dsn is empty'
        logging.warning('PostgreSQL cache enabled but postgres_dsn is empty; using legacy sheet mode.')
        return

    try:
        postgres_sync_manager.ensure_schema()
        rollout_done = {'value': False}

        def pull_once():
            if config.get('record_id_rollout', True) and not rollout_done['value']:
                stock_sheet_id = extract_sheet_id(config.get('phone_stock_sheet_id', ''))
                try:
                    rollout_result = rollout_record_ids_for_known_sheets(sheet, client, stock_sheet_id)
                    logging.info(
                        'RECORD_ID rollout pull phase: main_updated=%s stock_updated=%s main_err=%s stock_err=%s',
                        rollout_result.get('main', {}).get('updated', 0),
                        rollout_result.get('stock', {}).get('updated', 0),
                        rollout_result.get('main', {}).get('error', ''),
                        rollout_result.get('stock', {}).get('error', '')
                    )
                    rollout_done['value'] = True
                except Exception as rollout_err:
                    logging.warning('RECORD_ID rollout failed in pull cycle: %s', rollout_err)

            try:
                main_records = sheet.sheet1.get_all_records()
                postgres_sync_manager.upsert_sheet_cache('main_records', main_records)
            except Exception as main_err:
                logging.warning('PostgreSQL main_records pull failed: %s', main_err)

            stock_sheet_id = extract_sheet_id(config.get('phone_stock_sheet_id', ''))
            if stock_sheet_id:
                try:
                    stock_ws = client.open_by_key(stock_sheet_id).sheet1
                    stock_values = stock_ws.get_all_values()
                    postgres_sync_manager.upsert_sheet_cache('stock_values', stock_values)
                except Exception as stock_err:
                    logging.warning('PostgreSQL stock_values pull failed: %s', stock_err)

            postgres_sync_manager.set_meta('sync_runtime', {
                'mode': 'sheet_wins',
                'startup_mode': config.get('startup_mode', 'cache_then_sync'),
                'legacy_sheet_fallback': bool(config.get('legacy_sheet_fallback', True)),
                'pull_interval_sec': int(config.get('sync_pull_interval_sec', 90) or 90),
                'last_pull_utc': datetime.utcnow().isoformat()
            })

        # Seed once asynchronously, then continue in the 90-second background loop.
        def seed_once_async():
            try:
                pull_once()
            except Exception as seed_err:
                logging.warning('Initial PostgreSQL pull seed failed: %s', seed_err)

        threading.Thread(target=seed_once_async, daemon=True).start()
        postgres_sync_manager.start_background_pull(pull_once)
        postgres_sync_manager.start_background_queue_worker(_replay_queue_operation, interval_sec=20)
        postgres_sync_state['ready'] = True
        postgres_sync_state['last_status'] = 'running'
        logging.info('PostgreSQL sync is running with pull interval=%ss', int(config.get('sync_pull_interval_sec', 90) or 90))
    except Exception as e:
        postgres_sync_state['last_status'] = 'error'
        postgres_sync_state['last_error'] = str(e)
        logging.exception('Failed to initialize PostgreSQL sync: %s', e)


def _shutdown_postgres_sync():
    try:
        if postgres_sync_manager:
            postgres_sync_manager.stop()
    except Exception as e:
        logging.warning('PostgreSQL sync shutdown warning: %s', e)


def _resolve_stock_worksheet(stock_sheet_id):
    if not stock_sheet_id:
        raise RuntimeError('Stock sheet ID is missing')
    return client.open_by_key(stock_sheet_id).sheet1


def _replay_queue_operation(item):
    payload = item.get('payload_json') or {}
    kind = payload.get('kind', '')

    if kind == 'main_update_cell':
        row = int(payload.get('row', 0))
        col = int(payload.get('col', 0))
        value = payload.get('value', '')
        if row <= 0 or col <= 0:
            raise RuntimeError('Invalid main_update_cell payload')
        sheet.sheet1.update_cell(row, col, value)
        return

    if kind == 'stock_update_cell':
        stock_sheet_id = payload.get('stock_sheet_id', '')
        row = int(payload.get('row', 0))
        col = int(payload.get('col', 0))
        value = payload.get('value', '')
        if row <= 0 or col <= 0:
            raise RuntimeError('Invalid stock_update_cell payload')
        ws = _resolve_stock_worksheet(stock_sheet_id)
        ws.update_cell(row, col, value)
        return

    if kind == 'stock_batch_update':
        stock_sheet_id = payload.get('stock_sheet_id', '')
        request_body = payload.get('request_body') or {}
        if not stock_sheet_id or not request_body:
            raise RuntimeError('Invalid stock_batch_update payload')
        sheets_api_service.spreadsheets().batchUpdate(
            spreadsheetId=stock_sheet_id,
            body=request_body
        ).execute()
        return

    raise RuntimeError(f'Unsupported queue operation kind: {kind}')


def _queue_then_apply(entity_name, operation, payload, apply_callable, async_only=False, cache_apply_callable=None):
    queue_id = None
    if postgres_sync_state.get('ready') and postgres_sync_manager:
        try:
            queue_id = postgres_sync_manager.enqueue_operation(entity_name, operation, payload)
        except Exception as e:
            logging.warning('Queue enqueue failed (%s/%s): %s', entity_name, operation, e)

    if async_only:
        # DB-first mode: enqueue for background sheet push and update local cache immediately.
        if queue_id is not None:
            if cache_apply_callable is not None:
                try:
                    cache_apply_callable()
                except Exception as e:
                    logging.warning('Cache apply failed (%s/%s): %s', entity_name, operation, e)
            return True

        # If queue unavailable, optionally fall back to direct sheet write.
        if not bool(config.get('legacy_sheet_fallback', True)):
            raise RuntimeError('Queue unavailable and legacy fallback is disabled')

    try:
        result = apply_callable()
        if queue_id is not None and postgres_sync_manager:
            try:
                postgres_sync_manager.mark_operation_done(queue_id)
            except Exception as e:
                logging.warning('Queue mark done failed for id=%s: %s', queue_id, e)
        return result
    except Exception as e:
        if queue_id is not None and postgres_sync_manager:
            try:
                postgres_sync_manager.mark_operation_failed(queue_id, str(e))
            except Exception:
                pass
        raise


def _init_sheets():
    """Connect to Google Sheets. Runs in a background thread so the window opens instantly."""
    global client, sheets_api_service, sheet
    client = gspread.authorize(creds)
    sheets_api_service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    sheet = client.open_by_key(config.get('sheet_id'))
    refresh_debtors_data()
    _init_postgres_sync()


if __name__ == "__main__":
    atexit.register(_shutdown_postgres_sync)
    if tkinter_available:
        print("Tkinter available, launching...")

        # ── Loading splash ─────────────────────────────────────────────────
        _splash = Tk()
        _splash.title("Loading")
        _splash.resizable(False, False)
        _splash.configure(bg='#0b0b0b')
        _splash.overrideredirect(True)
        _splash.update_idletasks()
        _sw, _sh = _splash.winfo_screenwidth(), _splash.winfo_screenheight()
        _splash.geometry(f"360x150+{(_sw-360)//2}+{(_sh-150)//2}")

        from tkinter import Label as _Label
        _Label(_splash, text="ATLANTA GEORGIA_TECH",
               font=('Georgia', 15, 'bold'), bg='#0b0b0b', fg='white').pack(pady=(32, 6))
        _status_lbl = _Label(_splash, text="Connecting to Google Sheets\u2026",
                              font=('Helvetica', 11), bg='#0b0b0b', fg='#aaaaaa')
        _status_lbl.pack()
        _splash.update()

        _init_error = [None]

        def _do_init():
            try:
                _init_sheets()
            except Exception as _e:
                _init_error[0] = _e

        _t = threading.Thread(target=_do_init, daemon=True)
        _t.start()

        def _wait_for_init():
            if _t.is_alive():
                _splash.after(100, _wait_for_init)
                return
            _splash.destroy()
            if _init_error[0]:
                print('Failed to connect to Google Sheets:', _init_error[0])
                import sys as _sys
                _sys.exit(1)
            try:
                main_gui()
            except Exception as e:
                print('main_gui exception:', type(e).__name__, e)

        _splash.after(100, _wait_for_init)
        _splash.mainloop()
    else:
        print("Tkinter is not available in this Python environment. Falling back to console mode.")
        console_menu()