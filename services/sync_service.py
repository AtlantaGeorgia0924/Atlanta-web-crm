import uuid

from services.client_service import find_existing_client_key, normalize_phone_number


def column_index_to_letter(index):
    index += 1
    result = ''
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def detect_sheet_header_row(values):
    key_headers = {'DESCRIPTION', 'DESC', 'S/N', 'SN', 'IMEI', 'MODEL', 'DATE', 'COLOUR', 'COLOR', 'STORAGE', 'NAME', 'STATUS'}
    for idx, row in enumerate((values or [])[:8]):
        upper_cols = [str(c).strip().upper() for c in row]
        if any(col in key_headers for col in upper_cols):
            return idx
    return 0


def ensure_record_id_column(worksheet):
    values = worksheet.get_all_values()
    header_row_idx = detect_sheet_header_row(values)

    if not values:
        worksheet.update('A1:B1', [['RECORD_ID', 'NAME']])
        values = worksheet.get_all_values()
        return 0, 0, values

    header_row = values[header_row_idx] if header_row_idx < len(values) else []
    header_upper = [str(c).strip().upper() for c in header_row]

    if 'RECORD_ID' in header_upper:
        return header_upper.index('RECORD_ID'), header_row_idx, values

    new_col_idx_0 = len(header_row)
    required_col_count = new_col_idx_0 + 1
    try:
        current_col_count = int(getattr(worksheet, 'col_count', 0) or 0)
    except Exception:
        current_col_count = 0
    if current_col_count and required_col_count > current_col_count:
        worksheet.add_cols(required_col_count - current_col_count)

    worksheet.update_cell(header_row_idx + 1, new_col_idx_0 + 1, 'RECORD_ID')
    values = worksheet.get_all_values()
    return new_col_idx_0, header_row_idx, values


def backfill_record_ids(worksheet):
    record_col_idx, header_row_idx, values = ensure_record_id_column(worksheet)

    updates = []
    data_start = header_row_idx + 2
    for row_num in range(data_start, len(values) + 1):
        row = values[row_num - 1]
        current_value = ''
        if record_col_idx < len(row):
            current_value = str(row[record_col_idx]).strip()
        if current_value:
            continue

        new_id = uuid.uuid4().hex
        updates.append({
            'range': f"{column_index_to_letter(record_col_idx)}{row_num}",
            'values': [[new_id]]
        })

    if updates:
        chunk_size = 500
        for i in range(0, len(updates), chunk_size):
            worksheet.batch_update(updates[i:i + chunk_size], value_input_option='USER_ENTERED')

    return {
        'updated': len(updates),
        'data_rows': max(0, len(values) - (header_row_idx + 1)),
        'record_col_idx': record_col_idx,
        'header_row_idx': header_row_idx,
    }


def rollout_record_ids_for_known_sheets(main_spreadsheet, gspread_client, stock_sheet_id):
    results = {
        'main': {'updated': 0, 'error': ''},
        'stock': {'updated': 0, 'error': ''},
    }

    try:
        main_result = backfill_record_ids(main_spreadsheet.sheet1)
        results['main']['updated'] = main_result.get('updated', 0)
    except Exception as exc:
        results['main']['error'] = str(exc)

    if stock_sheet_id:
        try:
            stock_ws = gspread_client.open_by_key(stock_sheet_id).sheet1
            stock_result = backfill_record_ids(stock_ws)
            results['stock']['updated'] = stock_result.get('updated', 0)
        except Exception as exc:
            results['stock']['error'] = str(exc)

    return results


def ensure_directory_sheet(spreadsheet, sheet_title):
    try:
        return spreadsheet.worksheet(sheet_title)
    except Exception:
        worksheet = spreadsheet.add_worksheet(title=sheet_title, rows='1000', cols='2')
        worksheet.update('A1:B1', [['NAME', 'PHONE NUMBER']])
        return worksheet


def build_client_phone_sheet_updates(values, clients, name_col, phone_col):
    updates = []
    if not values or name_col is None or phone_col is None:
        return updates

    phone_letter = column_index_to_letter(phone_col)
    for row_idx in range(1, len(values)):
        row = values[row_idx]
        if name_col >= len(row):
            continue

        row_name = str(row[name_col]).strip()
        if not row_name:
            continue

        existing_key = find_existing_client_key(row_name, clients)
        if not existing_key:
            continue

        target_phone = normalize_phone_number((clients or {}).get(existing_key, ''))
        current_phone = normalize_phone_number(row[phone_col] if phone_col < len(row) else '')
        if target_phone and current_phone != target_phone:
            updates.append({
                'range': f'{phone_letter}{row_idx + 1}',
                'values': [[target_phone]],
            })

    return updates


def build_phone_autofill_plan(values, name_col, phone_col, sheet_row_count, directory_sheet_title):
    if not values or name_col is None or phone_col is None:
        return {'range': '', 'values': []}

    name_letter = column_index_to_letter(name_col)
    phone_letter = column_index_to_letter(phone_col)
    row_count = max(int(sheet_row_count or 0), len(values) + 200)
    formulas = []

    for row_num in range(2, row_count + 1):
        current_value = ''
        if row_num - 1 < len(values) and phone_col < len(values[row_num - 1]):
            current_value = str(values[row_num - 1][phone_col]).strip()

        if current_value and not current_value.startswith('='):
            formulas.append([current_value])
            continue

        formulas.append([
            f'=IF(LEN(TRIM({name_letter}{row_num}))=0,"",IFNA(VLOOKUP(UPPER(TRIM({name_letter}{row_num})),\'{directory_sheet_title}\'!A:B,2,FALSE),""))'
        ])

    return {
        'range': f'{phone_letter}2:{phone_letter}{row_count}',
        'values': formulas,
    }
