import json
import os
from datetime import date, datetime

from services.sync_service import detect_sheet_header_row


def clean_amount(value):
    try:
        if not value:
            return 0

        value = str(value).replace('₦', '').replace(',', '').replace('.00', '').strip()
        if not value.isdigit():
            return 0
        return int(value)
    except Exception:
        return 0


def format_date(date_str):
    try:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        return dt.strftime("%d %B %Y")
    except Exception:
        return date_str


def _day_suffix(day_value):
    if 11 <= day_value <= 13:
        return 'th'
    return {1: 'st', 2: 'nd', 3: 'rd'}.get(day_value % 10, 'th')


def format_bill_date(date_str):
    parsed = parse_sheet_date(date_str)
    if parsed is None:
        return str(date_str or '').strip()

    weekday = parsed.strftime('%a')
    month = parsed.strftime('%B')
    day_num = parsed.day
    return f"{weekday}. {day_num}{_day_suffix(day_num)} of {month}"


def to_math_italic(text):
    if not text:
        return ''

    out = []
    for char in str(text):
        code = ord(char)
        if 'A' <= char <= 'Z':
            out.append(chr(0x1D468 + (code - ord('A'))))
            continue
        if 'a' <= char <= 'z':
            out.append(chr(0x1D482 + (code - ord('a'))))
            continue
        out.append(char)

    return ''.join(out)


def resolve_day_greeting(now=None):
    current = now or datetime.now()
    hour = int(current.hour)
    if hour < 12:
        return 'Good morning'
    if hour < 17:
        return 'Good afternoon'
    return 'Good evening'


def resolve_salutation(gender=''):
    normalized = str(gender or '').strip().lower()
    if normalized == 'male':
        return 'Sir'
    if normalized == 'female':
        return 'Ma'
    return 'there'


def get_customer_outstanding_items_from_values(name_input, values):
    name_input = str(name_input or '').strip().upper()
    if not values:
        return [], 0, None

    header_row_idx = detect_sheet_header_row(values)
    header = values[header_row_idx] if header_row_idx < len(values) else []
    header_upper = [str(cell or '').strip().upper() for cell in header]

    def find_col(*candidates):
        for candidate in candidates:
            upper = str(candidate or '').strip().upper()
            if upper in header_upper:
                return header_upper.index(upper)
        return None

    name_col = find_col('NAME', 'CLIENT NAME', 'CUSTOMER NAME')
    price_col = find_col('PRICE', 'AMOUNT SOLD', 'SELLING PRICE')
    paid_col = find_col('AMOUNT PAID', 'AMOUNT PAID ')
    status_col = find_col('STATUS')
    if name_col is None or price_col is None or paid_col is None or status_col is None:
        return [], 0, None

    description_col = find_col('DESCRIPTION', 'MODEL', 'DEVICE', 'DESC')
    date_col = find_col('DATE')

    outstanding_items = []
    total_outstanding = 0

    for row_idx in range(header_row_idx + 1, len(values)):
        row = values[row_idx]
        if name_col >= len(row):
            continue

        name = str(row[name_col]).strip().upper()
        if name != name_input:
            continue

        status = str(row[status_col]).strip().lower() if status_col < len(row) else ''
        if status in ['paid', 'returned']:
            continue

        price = clean_amount(row[price_col] if price_col < len(row) else '')
        paid = clean_amount(row[paid_col] if paid_col < len(row) else '')
        balance = price - paid
        if balance <= 0:
            continue

        description = row[description_col].strip() if description_col is not None and description_col < len(row) else ""
        date_value = row[date_col].strip() if date_col is not None and date_col < len(row) else ""

        outstanding_items.append({
            'row_idx': row_idx,
            'description': description,
            'date': date_value,
            'price': price,
            'paid': paid,
            'balance': balance,
        })
        total_outstanding += balance

    columns = {
        'paid_col': paid_col,
        'status_col': status_col
    }
    return outstanding_items, total_outstanding, columns


def get_customer_outstanding_items_from_records(name_input, records):
    name_input = str(name_input or '').strip().upper()
    records = records or []

    outstanding_items = []
    total_outstanding = 0

    for record_idx, row in enumerate(records, start=1):
        row_name = str(row.get("NAME", "")).strip().upper()
        if row_name != name_input:
            continue

        status = str(row.get("STATUS", "")).strip().lower()
        if status in ["paid", "returned"]:
            continue

        price = clean_amount(row.get("PRICE"))
        paid = clean_amount(row.get("Amount paid"))
        balance = price - paid
        if balance <= 0:
            continue

        outstanding_items.append({
            'row_idx': record_idx,
            'description': str(row.get("DESCRIPTION", "")).strip(),
            'date': str(row.get("DATE", "")).strip(),
            'price': price,
            'paid': paid,
            'balance': balance,
            'status': str(row.get("STATUS", "")).strip()
        })
        total_outstanding += balance

    return outstanding_items, total_outstanding


def format_service_option(item):
    description = item.get('description') or 'UNNAMED SERVICE'
    date_value = item.get('date') or 'No date'
    if item.get('date'):
        date_value = format_date(item['date'])
    return f"{description} ({date_value}) - Balance NGN {item['balance']:,}"


def generate_bill_text(name_input, records, payment_details, gender=''):
    name_input = str(name_input or '').strip().upper()
    display_name = str(name_input or '').strip().upper()
    records = records or []

    items = []
    total = 0
    for row in records:
        row_name = str(row.get("NAME", "")).strip().upper()
        status = str(row.get("STATUS", "")).strip().lower()
        if row_name != name_input or status in ["paid", "returned"]:
            continue

        price = clean_amount(row.get("PRICE"))
        paid = clean_amount(row.get("Amount paid"))
        balance = price - paid
        if balance <= 0:
            continue

        items.append({
            'date': row.get("DATE"),
            'desc': row.get("DESCRIPTION"),
            'amount': balance,
            'status': status,
            'paid': paid,
            'price': price,
        })
        total += balance

    if not items:
        return "No outstanding bill for this customer."

    generated_at = datetime.now().strftime('%H:%M')
    generated_day = format_bill_date(datetime.now().strftime('%m/%d/%Y'))

    greeting = resolve_day_greeting()
    salutation = resolve_salutation(gender)
    if salutation == 'there':
        salutation = display_name if display_name else 'there'
    intro_line_1 = to_math_italic(f"{greeting} {salutation}, I trust you're doing well.")
    intro_line_2 = to_math_italic('Here is a quick summary of your outstanding bill for your review:')
    lines = [
        intro_line_1,
        intro_line_2,
        '',
        display_name,
        f"Generated: {generated_day} at {generated_at}",
        f"*Total Outstanding: NGN {total:,}*",
        '',
        f"Breakdown ({len(items)} item(s)):",
        '',
    ]
    for index, item in enumerate(items, 1):
        status_text = item['status'].upper() if item['status'] else 'OPEN'
        balance_line = f"NGN {item['amount']:,}"
        if status_text == 'PART PAYMENT':
            balance_line = f"NGN {item['amount']:,} (paid NGN {item['paid']:,} of NGN {item['price']:,})"
        service_text = to_math_italic(item['desc'])
        lines.append(f"{index}. {service_text}")
        lines.append(f"   Date: {format_bill_date(item['date'])}")
        lines.append(f"   Balance: {balance_line}")
        lines.append(f"   Status: {status_text}")
        lines.append("")

    normalized_payment_details = str(payment_details or '').strip()
    if not normalized_payment_details:
        normalized_payment_details = 'Account details are not configured yet. Please contact admin.'

    lines.append('Payment Details:')
    lines.append(normalized_payment_details)
    lines.append('')
    lines.append(to_math_italic('Please note that this message was generated automatically.'))
    lines.append('')
    lines.append('Please send your payment screenshot after transfer. Thank you.')
    return "\n".join(lines)


def compute_debtors(records):
    records = records or []
    debtors = []
    oldest_unpaid_date_by_name = {}
    for row in records:
        status = str(row.get("STATUS", "")).strip().lower()
        if status in ["paid", "returned"]:
            continue
        price = clean_amount(row.get("PRICE"))
        paid = clean_amount(row.get("Amount paid"))
        balance = price - paid
        if balance > 0:
            name = str(row.get("NAME", "")).strip().upper()
            debtors.append({
                'name': name,
                'amount': balance,
                'date': str(row.get("DATE", "")).strip(),
            })

            parsed_date = parse_sheet_date(row.get("DATE"))
            if parsed_date is not None and name:
                previous = oldest_unpaid_date_by_name.get(name)
                if previous is None or parsed_date < previous:
                    oldest_unpaid_date_by_name[name] = parsed_date

    merged = {}
    for item in debtors:
        merged[item['name']] = merged.get(item['name'], 0) + item['amount']

    sorted_debtors = sorted(merged.items(), key=lambda x: x[1], reverse=True)
    client_names = [name for name, _ in sorted_debtors]
    total_debtors_amount = sum(amount for _, amount in sorted_debtors)
    return {
        'debtors': debtors,
        'merged': merged,
        'sorted_debtors': sorted_debtors,
        'client_names': client_names,
        'total_debtors_amount': total_debtors_amount,
        'oldest_unpaid_date_by_name': {
            key: value.isoformat()
            for key, value in oldest_unpaid_date_by_name.items()
        },
    }


def build_payment_plan(name_input, payment_amount, values, manual_service_row_idx=None):
    name_input = str(name_input or '').strip().upper()
    outstanding_items, total_outstanding, columns = get_customer_outstanding_items_from_values(name_input, values)
    if columns is None:
        return {'error': 'Required columns not found in sheet.'}
    if total_outstanding == 0:
        return {'error': 'No outstanding balance found for this customer.'}

    if manual_service_row_idx is not None:
        prioritized_items = [item for item in outstanding_items if item['row_idx'] == manual_service_row_idx]
        if not prioritized_items:
            return {'error': 'Selected service is no longer outstanding for this customer.'}
        remaining_items = [item for item in outstanding_items if item['row_idx'] != manual_service_row_idx]
        outstanding_items = prioritized_items + remaining_items

    paid_col = columns['paid_col']
    status_col = columns['status_col']
    undo_rows = []
    updates = []
    total_applied = 0
    remaining_payment = int(payment_amount)

    if remaining_payment >= total_outstanding:
        for item in outstanding_items:
            row_idx = item['row_idx']
            row = values[row_idx]
            old_paid = item['paid']
            old_status = row[status_col] if status_col < len(row) else ''
            new_paid = item['price']
            new_status = 'PAID'
            updates.append({
                'row_idx': row_idx,
                'paid_col': paid_col,
                'status_col': status_col,
                'old_paid': old_paid,
                'old_status': old_status,
                'new_paid': new_paid,
                'new_status': new_status
            })
            undo_rows.append(updates[-1].copy())
        total_applied = total_outstanding
        status_text = f"Full payment of NGN {total_applied:,} applied to {name_input}. All statuses set to PAID."
    else:
        for item in outstanding_items:
            if remaining_payment <= 0:
                break
            row_idx = item['row_idx']
            row = values[row_idx]
            balance = item['balance']
            if balance <= 0:
                continue

            if remaining_payment >= balance:
                new_paid = item['paid'] + balance
                remaining_payment -= balance
                new_status = 'PAID'
                total_applied += balance
            else:
                new_paid = item['paid'] + remaining_payment
                total_applied += remaining_payment
                remaining_payment = 0
                new_status = 'PART PAYMENT'

            old_paid = item['paid']
            old_status = row[status_col] if status_col < len(row) else ''
            updates.append({
                'row_idx': row_idx,
                'paid_col': paid_col,
                'status_col': status_col,
                'old_paid': old_paid,
                'old_status': old_status,
                'new_paid': new_paid,
                'new_status': new_status
            })
            undo_rows.append(updates[-1].copy())

        if manual_service_row_idx is not None:
            selected_item = next(item for item in outstanding_items if item['row_idx'] == manual_service_row_idx)
            status_text = f"Partial payment of NGN {total_applied:,} applied to {name_input}. Started with {selected_item['description'] or 'the selected service'}."
        else:
            status_text = f"Partial payment of NGN {total_applied:,} applied to {name_input}."

    return {
        'updates': updates,
        'undo_rows': undo_rows,
        'status_text': status_text,
        'total_applied': total_applied,
        'columns': columns,
        'total_outstanding': total_outstanding,
        'outstanding_items': outstanding_items,
        'name_input': name_input,
    }


def is_returned_status(status_text):
    text = str(status_text or '').strip().lower()
    if not text:
        return False
    returned_tokens = ['returned', 'return', 'returned goods', 'return goods', 'goods returned']
    return any(token in text for token in returned_tokens)


def parse_sheet_date(date_value):
    raw = str(date_value or '').strip()
    if not raw:
        return None

    candidate_formats = [
        '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y',
        '%m/%d/%y', '%d/%m/%y', '%Y/%m/%d'
    ]

    for fmt in candidate_formats:
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue

    return None


def compute_sales_snapshot(records, today=None):
    today = today or date.today()
    month_start = today.replace(day=1)

    customers_today = set()
    services_today = 0
    sales_today = 0
    sales_month = 0
    week_totals = [0, 0, 0, 0, 0]
    daily_totals = [0, 0, 0, 0, 0, 0, 0]

    for row in records or []:
        status = str(row.get('STATUS', '')).strip().lower()
        if is_returned_status(status):
            continue

        row_date = parse_sheet_date(row.get('DATE'))
        if row_date is None:
            continue

        amount = clean_amount(row.get('PRICE'))
        if amount <= 0:
            continue

        row_name = str(row.get('NAME', '')).strip().upper()

        if month_start <= row_date <= today:
            sales_month += amount
            week_index = min(4, (row_date.day - 1) // 7)
            week_totals[week_index] += amount

        if row_date == today:
            services_today += 1
            sales_today += amount
            if row_name:
                customers_today.add(row_name)

        day_gap = (today - row_date).days
        if 0 <= day_gap < 7:
            daily_totals[6 - day_gap] += amount

    return {
        'customers_today': len(customers_today),
        'services_today': services_today,
        'sales_today': sales_today,
        'sales_month': sales_month,
        'week_totals': week_totals,
        'daily_totals': daily_totals,
    }


def normalize_customer_name(name_input):
    return str(name_input or '').strip().upper()


def load_whatsapp_send_history(file_path):
    if not file_path or not os.path.exists(file_path):
        return {}

    try:
        with open(file_path, 'r') as file_obj:
            payload = json.load(file_obj)
    except Exception:
        return {}

    if not isinstance(payload, dict):
        return {}

    normalized = {}
    for raw_name, details in payload.items():
        name = normalize_customer_name(raw_name)
        if not name:
            continue

        details = details if isinstance(details, dict) else {}
        events = details.get('events')
        if not isinstance(events, list):
            events = []

        clean_events = []
        for event in events:
            if not isinstance(event, dict):
                continue
            sent_at = str(event.get('sent_at') or '').strip()
            source = str(event.get('source') or '').strip() or 'single'
            if not sent_at:
                continue
            clean_events.append({'sent_at': sent_at, 'source': source})

        normalized[name] = {
            'send_count': int(details.get('send_count') or len(clean_events) or 0),
            'last_sent_at': str(details.get('last_sent_at') or '').strip(),
            'events': clean_events,
        }

    return normalized


def save_whatsapp_send_history(file_path, payload):
    with open(file_path, 'w') as file_obj:
        json.dump(payload or {}, file_obj, indent=4)


def get_whatsapp_send_entry(history_payload, name_input, today=None):
    today = today or date.today()
    today_key = today.isoformat()
    customer_name = normalize_customer_name(name_input)
    details = (history_payload or {}).get(customer_name) or {}
    events = details.get('events') if isinstance(details.get('events'), list) else []
    today_send_count = sum(1 for event in events if str(event.get('sent_at') or '').startswith(today_key))

    return {
        'name': customer_name,
        'send_count': int(details.get('send_count') or 0),
        'today_send_count': today_send_count,
        'last_sent_at': str(details.get('last_sent_at') or '').strip(),
    }


def mark_whatsapp_bill_sent(history_payload, name_inputs, source='single', now=None):
    now = now or datetime.now()
    sent_at = now.isoformat(timespec='seconds')
    source = str(source or '').strip() or 'single'

    updated = {}
    for raw_name in name_inputs or []:
        customer_name = normalize_customer_name(raw_name)
        if not customer_name:
            continue

        entry = (history_payload or {}).get(customer_name)
        if not isinstance(entry, dict):
            entry = {
                'send_count': 0,
                'last_sent_at': '',
                'events': [],
            }

        events = entry.get('events') if isinstance(entry.get('events'), list) else []
        events.append({'sent_at': sent_at, 'source': source})
        entry['events'] = events
        entry['send_count'] = int(entry.get('send_count') or 0) + 1
        entry['last_sent_at'] = sent_at
        history_payload[customer_name] = entry
        updated[customer_name] = {
            'name': customer_name,
            'send_count': entry['send_count'],
            'last_sent_at': sent_at,
        }

    return updated


def build_unpaid_today_customers(records, today=None):
    today = today or date.today()
    merged = {}

    for row in records or []:
        status = str(row.get('STATUS', '')).strip().lower()
        if status in {'paid', 'returned'}:
            continue

        row_date = parse_sheet_date(row.get('DATE'))
        if row_date != today:
            continue

        price = clean_amount(row.get('PRICE'))
        paid = clean_amount(row.get('Amount paid'))
        balance = price - paid
        if balance <= 0:
            continue

        name = normalize_customer_name(row.get('NAME'))
        if not name:
            continue

        existing = merged.setdefault(name, {
            'name': name,
            'services_today': 0,
            'outstanding_today': 0,
        })
        existing['services_today'] += 1
        existing['outstanding_today'] += balance

    return sorted(
        merged.values(),
        key=lambda entry: (entry.get('outstanding_today', 0), entry.get('name', '')),
        reverse=True,
    )


def build_services_today_rows(records, today=None):
    today = today or date.today()
    rows = []

    for row_idx, row in enumerate(records or [], start=2):
        status_text = str(row.get('STATUS', '')).strip()
        if is_returned_status(status_text):
            continue

        row_date = parse_sheet_date(row.get('DATE'))
        if row_date != today:
            continue

        price = clean_amount(row.get('PRICE'))
        if price <= 0:
            continue

        amount_paid = clean_amount(row.get('AMOUNT PAID') if 'AMOUNT PAID' in row else row.get('Amount paid'))
        balance = max(0, price - amount_paid)
        rows.append({
            'row_num': row_idx,
            'name': normalize_customer_name(row.get('NAME')),
            'description': str(row.get('DESCRIPTION') or row.get('MODEL') or row.get('DEVICE') or '').strip(),
            'imei': str(row.get('IMEI') or '').strip(),
            'date': str(row.get('DATE') or '').strip(),
            'time': str(row.get('TIME') or '').strip(),
            'status': str(status_text).upper(),
            'price': price,
            'amount_paid': amount_paid,
            'balance': balance,
            'deal_location': str(row.get('DEAL LOCATION') or '').strip(),
            'internal_note': str(row.get('INTERNAL NOTE') or row.get('SERVICE NOTE') or row.get('NOTE') or row.get('NOTES') or '').strip(),
        })

    rows.sort(key=lambda entry: int(entry.get('row_num') or 0), reverse=True)
    return rows


def search_services_by_name(records, query):
    """Return all service rows (any date) whose customer name contains the query string."""
    query_upper = str(query or '').strip().upper()
    if not query_upper:
        return []

    rows = []
    for row_idx, row in enumerate(records or [], start=2):
        status_text = str(row.get('STATUS', '')).strip()
        if is_returned_status(status_text):
            continue

        price = clean_amount(row.get('PRICE'))
        if price <= 0:
            continue

        name = normalize_customer_name(row.get('NAME'))
        if query_upper not in name.upper():
            continue

        amount_paid = clean_amount(row.get('AMOUNT PAID') if 'AMOUNT PAID' in row else row.get('Amount paid'))
        balance = max(0, price - amount_paid)
        rows.append({
            'row_num': row_idx,
            'name': name,
            'description': str(row.get('DESCRIPTION') or row.get('MODEL') or row.get('DEVICE') or '').strip(),
            'imei': str(row.get('IMEI') or '').strip(),
            'date': str(row.get('DATE') or '').strip(),
            'time': str(row.get('TIME') or '').strip(),
            'status': str(status_text).upper(),
            'price': price,
            'amount_paid': amount_paid,
            'balance': balance,
            'deal_location': str(row.get('DEAL LOCATION') or '').strip(),
            'internal_note': str(row.get('INTERNAL NOTE') or row.get('SERVICE NOTE') or row.get('NOTE') or row.get('NOTES') or '').strip(),
        })

    rows.sort(key=lambda entry: int(entry.get('row_num') or 0), reverse=True)
    return rows


def build_debtor_send_summary(records, history_payload, today=None):
    today = today or date.today()
    customer_names = compute_debtors(records).get('client_names', [])
    return {
        name: get_whatsapp_send_entry(history_payload, name, today=today)
        for name in customer_names
    }
