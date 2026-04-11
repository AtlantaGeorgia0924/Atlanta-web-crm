import re


def _column_index_to_letter(index):
    index += 1
    result = ''
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def classify_stock_fill_color(color):
    if not color:
        return 'available'

    red = float(color.get('red', 0.0) if color.get('red') is not None else 1.0)
    green = float(color.get('green', 0.0) if color.get('green') is not None else 1.0)
    blue = float(color.get('blue', 0.0) if color.get('blue') is not None else 1.0)

    if red > 0.90 and green > 0.90 and blue > 0.90:
        return 'available'
    if red >= 0.65 and green <= 0.50 and blue <= 0.50:
        return 'sold'
    if green >= 0.58 and green > red and green >= blue:
        return 'sold'
    if red >= 0.70 and green >= 0.50 and red >= green and blue < (green - 0.15):
        return 'pending'
    if blue >= 0.55 and blue > red and blue > green:
        return 'needs_details'
    return 'available'


def normalize_stock_status_value(raw_value):
    text = str(raw_value or '').strip().upper()
    if not text:
        return ''

    if text in {'AVAILABLE', 'IN STOCK', 'AVAIL', 'OPEN'}:
        return 'available'
    if text in {'SOLD', 'PAID', 'CLOSED'}:
        return 'sold'
    if text in {'PENDING', 'PENDING DEAL', 'PENDING SALE'}:
        return 'pending'
    if text in {'NEEDS DETAILS', 'NEEDS DETAIL', 'INCOMPLETE', 'MISSING DETAILS'}:
        return 'needs_details'
    return ''


def stock_status_key_to_label(status_key):
    return {
        'available': 'AVAILABLE',
        'pending': 'PENDING DEAL',
        'needs_details': 'NEEDS DETAILS',
        'sold': 'SOLD',
    }.get(status_key or 'available', 'AVAILABLE')


def detect_stock_headers(values):
    values = values or []
    key_headers = {'DESCRIPTION', 'DESC', 'S/N', 'SN', 'IMEI', 'MODEL', 'DATE', 'COLOUR', 'COLOR', 'STORAGE'}
    detected_idx = None
    for index, row in enumerate(values[:8]):
        upper_cols = [str(c).strip().upper() for c in row]
        if any(col in key_headers for col in upper_cols):
            detected_idx = index
            break

    if detected_idx is not None:
        header_row_idx = detected_idx
        headers = [str(c).strip() or f'COL {i+1}' for i, c in enumerate(values[detected_idx])]
    elif values and any(str(c).strip() for c in values[0]):
        header_row_idx = 0
        headers = [str(c).strip() or f'COL {i+1}' for i, c in enumerate(values[0])]
    else:
        header_row_idx = 0
        headers = ['DATE', 'MODEL', 'IMEI', 'QTY', 'COST PRICE', 'SELL PRICE', 'STATUS', 'NOTE']

    headers_upper = [h.upper() for h in headers]
    return header_row_idx, headers, headers_upper


def header_index(headers_upper, *aliases):
    for alias in aliases:
        alias_u = alias.upper()
        for index, header in enumerate(headers_upper):
            if header == alias_u:
                return index
    return None


def classify_available_series(description_text):
    text = str(description_text or '').upper()
    if not text:
        return None

    if 'IPHONE' in text:
        num_match = re.search(r'IPHONE\s*(\d{1,2})\b', text)
        if num_match:
            return ('iPhone', f"{num_match.group(1)} Series")
        if 'SE' in text:
            return ('iPhone', 'SE Series')
        return ('iPhone', 'Other Series')

    if 'SAMSUNG' in text or 'GALAXY' in text:
        s_match = re.search(r'(?:GALAXY\s*)?S\s?(\d{1,2})\b', text)
        if s_match:
            return ('Samsung', f"S{s_match.group(1)} Series")
        return ('Samsung', 'Other Series')

    if 'IWATCH' in text or 'I WATCH' in text or 'APPLE WATCH' in text:
        w_match = re.search(r'SERIES\s*(\d{1,2})\b', text)
        if w_match:
            return ('iWatch', f"Series {w_match.group(1)}")
        return ('iWatch', 'Other Series')

    return None


def suggest_next_serial(values, header_row_idx, headers_upper):
    serial_col = header_index(headers_upper, 'S/N', 'SN', 'SERIAL NO', 'SERIAL NUMBER')
    if serial_col is None:
        return ''

    max_no = 0
    for row in values[header_row_idx + 1:]:
        if serial_col >= len(row):
            continue
        text = str(row[serial_col]).strip()
        if text.isdigit():
            max_no = max(max_no, int(text))
    return str(max_no + 1) if max_no > 0 else '1'


def order_stock_form_headers(headers, hidden_form_aliases, essential_aliases):
    preferred_headers = []
    remaining_headers = []
    for header in headers:
        upper = header.upper()
        if upper in hidden_form_aliases:
            continue
        if upper in essential_aliases:
            preferred_headers.append(header)
        else:
            remaining_headers.append(header)
    return preferred_headers + remaining_headers


def build_stock_form_defaults(values, header_row_idx, headers_upper):
    today = ''
    current_time = ''
    try:
        from datetime import datetime
        today = datetime.now().strftime('%m/%d/%Y')
        current_time = datetime.now().strftime('%H:%M')
    except Exception:
        today = ''
        current_time = ''

    defaults = {}
    next_sn = suggest_next_serial(values, header_row_idx, headers_upper)
    for header in headers_upper:
        if header in ('DATE', 'DATE BOUGHT') and today:
            defaults[header] = today
        elif header == 'TIME' and current_time:
            defaults[header] = current_time
        elif header in ('STATUS', 'STATUS OF DEVICE'):
            defaults[header] = 'UNLOCKED'
        elif header in ('S/N', 'SN') and next_sn:
            defaults[header] = next_sn
    return defaults


def build_stock_row_values(headers, values_by_header):
    row_values = []
    non_empty_count = 0
    for header in headers:
        value = str((values_by_header or {}).get(header, '') or '').strip()
        row_values.append(value)
        if value:
            non_empty_count += 1
    return row_values, non_empty_count


def find_next_table_write_row(values, header_row_idx):
    values = values or []
    data_start = max(0, int(header_row_idx or 0)) + 1
    last_filled_row = max(1, data_start)

    for row_number, row in enumerate(values[data_start:], start=data_start + 1):
        if any(str(cell or '').strip() for cell in row):
            last_filled_row = row_number

    return last_filled_row + 1


def validate_stock_row(row_values, headers_upper):
    if not row_values or not any(str(v or '').strip() for v in row_values):
        return 'Fill at least one stock field before adding.'

    model_idx = header_index(headers_upper, 'DESCRIPTION', 'MODEL', 'PHONE MODEL', 'PHONE')
    if model_idx is not None and model_idx < len(row_values) and not str(row_values[model_idx]).strip():
        return 'Please fill DESCRIPTION/MODEL before adding.'
    return ''


def compute_stock_qty_status(current_qty, delta):
    new_qty = max(0, int(current_qty) + int(delta))
    status = 'IN STOCK' if new_qty > 0 else 'OUT OF STOCK'
    return new_qty, status


def map_sale_status(status_choice):
    status_key = {
        'sold': 'sold',
        'pending deal': 'pending',
        'needs details': 'needs_details',
        'available': 'available'
    }.get(str(status_choice or '').strip().lower(), 'sold')

    fill_color = {
        'sold': {'red': 0.714, 'green': 0.843, 'blue': 0.659},
        'pending': {'red': 0.984, 'green': 0.737, 'blue': 0.016},
        'needs_details': {'red': 0.624, 'green': 0.769, 'blue': 0.996},
        'available': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
    }[status_key]
    return status_key, fill_color


def build_sale_status_update_values(status_key, qty_col=None, status_col=None, sold_date_col=None, sold_date_value=''):
    updates = []
    if status_key == 'sold':
        if qty_col is not None:
            updates.append({'col': qty_col + 1, 'value': 0})
        if status_col is not None:
            updates.append({'col': status_col + 1, 'value': 'SOLD'})
        if sold_date_col is not None and sold_date_value:
            updates.append({'col': sold_date_col + 1, 'value': sold_date_value})
    elif status_key == 'pending':
        if status_col is not None:
            updates.append({'col': status_col + 1, 'value': 'PENDING DEAL'})
    elif status_key == 'needs_details':
        if status_col is not None:
            updates.append({'col': status_col + 1, 'value': 'NEEDS DETAILS'})
    elif status_key == 'available':
        if status_col is not None:
            updates.append({'col': status_col + 1, 'value': 'IN STOCK'})
    return updates


def build_stock_view(values, headers, headers_upper, header_row_idx, color_status_map=None, filter_text='', filter_mode='all'):
    values = values or []
    qty_col = header_index(headers_upper, 'QTY', 'QUANTITY', 'STOCK', 'UNITS')
    product_status_col = header_index(headers_upper, 'PRODUCT STATUS', 'STOCK STATUS', 'ITEM STATUS')
    desc_col = header_index(headers_upper, 'DESCRIPTION', 'DESC', 'DETAILS')
    model_col = header_index(headers_upper, 'MODEL', 'PHONE MODEL', 'PHONE', 'NAME', 'DEVICE')

    data_rows = values[header_row_idx + 1:]
    data_start_sheet_row = header_row_idx + 2

    counts = {'available': 0, 'pending': 0, 'needs_details': 0, 'sold': 0, 'needs_review': 0}
    available_breakdown = {}
    available_series_items = {}
    all_rows_cache = []
    normalized_filter = str(filter_text or '').strip().lower()

    for idx, row in enumerate(data_rows):
        row_num = data_start_sheet_row + idx
        padded = row + [''] * (len(headers) - len(row))

        row_status = ''
        raw_status_text = ''
        needs_review = False
        if product_status_col is not None and product_status_col < len(padded):
            raw_status_text = str(padded[product_status_col] or '').strip()
            row_status = normalize_stock_status_value(raw_status_text)
            if not row_status:
                needs_review = True

        if row_status not in {'available', 'pending', 'needs_details', 'sold'}:
            row_status = 'needs_review'
            needs_review = True

        counts[row_status] = counts.get(row_status, 0) + 1
        if needs_review:
            counts['needs_review'] = counts.get('needs_review', 0) + 1
        if row_status == 'available':
            desc_text = ''
            if desc_col is not None and desc_col < len(padded):
                desc_text = padded[desc_col]
            elif model_col is not None and model_col < len(padded):
                desc_text = padded[model_col]

            classified = classify_available_series(desc_text)
            if classified:
                qty_for_count = 1
                if qty_col is not None and qty_col < len(padded):
                    try:
                        qty_for_count = max(1, int(str(padded[qty_col]).strip() or '1'))
                    except Exception:
                        qty_for_count = 1
                available_breakdown[classified] = available_breakdown.get(classified, 0) + qty_for_count
                available_series_items.setdefault(classified, []).append((row_num, padded[:len(headers)]))

        haystack = ' '.join(str(item) for item in padded).lower()
        if normalized_filter and normalized_filter not in haystack:
            continue
        if filter_mode == 'needs_review':
            if not needs_review:
                continue
        elif filter_mode != 'all' and row_status != filter_mode:
            continue

        row_label = 'NEEDS STATUS REVIEW' if row_status == 'needs_review' else stock_status_key_to_label(row_status)
        base_tag = 'even' if (row_num % 2 == 0) else 'odd'
        row_tags = (base_tag,)
        if row_status in ('pending', 'needs_details', 'sold', 'needs_review'):
            row_tags = (base_tag, row_status)

        all_rows_cache.append({
            'row_num': row_num,
            'padded': padded[:len(headers)],
            'label': row_label,
            'tags': row_tags,
            'needs_review': needs_review,
            'raw_product_status': raw_status_text,
        })

    return {
        'all_rows_cache': all_rows_cache,
        'counts': counts,
        'available_breakdown': available_breakdown,
        'available_series_items': available_series_items,
    }


def get_stock_color_status_map(sheets_api_service, spreadsheet_id, worksheet_title, description_col_idx, last_row):
    if description_col_idx is None or last_row < 2:
        return {}

    col_letter = _column_index_to_letter(description_col_idx)
    safe_title = worksheet_title.replace("'", "''")
    range_name = f"'{safe_title}'!{col_letter}2:{col_letter}{last_row}"

    response = sheets_api_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[range_name],
        includeGridData=True,
        fields='sheets(data(rowData(values(userEnteredFormat(backgroundColor),effectiveFormat(backgroundColor)))))',
    ).execute()

    status_map = {}
    sheets_data = response.get('sheets', [])
    if not sheets_data:
        return status_map

    row_data = sheets_data[0].get('data', [{}])[0].get('rowData', [])
    for offset, row in enumerate(row_data, start=2):
        values = row.get('values', [])
        if not values:
            continue
        cell = values[0]
        ue_color = cell.get('userEnteredFormat', {}).get('backgroundColor', {})
        ef_color = cell.get('effectiveFormat', {}).get('backgroundColor', {})
        ue_r = float(ue_color.get('red', 0.0) if ue_color.get('red') is not None else 1.0)
        ue_g = float(ue_color.get('green', 0.0) if ue_color.get('green') is not None else 1.0)
        ue_b = float(ue_color.get('blue', 0.0) if ue_color.get('blue') is not None else 1.0)
        color = ef_color if ue_r > 0.90 and ue_g > 0.90 and ue_b > 0.90 else ue_color
        status_map[offset] = classify_stock_fill_color(color)

    return status_map
