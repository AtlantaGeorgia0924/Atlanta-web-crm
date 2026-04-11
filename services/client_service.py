def normalize_phone_number(value):
    digits = ''.join(ch for ch in str(value or '') if ch.isdigit())
    if not digits:
        return ''

    if digits.startswith('00') and len(digits) > 2:
        digits = digits[2:]

    if digits.startswith('2340') and len(digits) >= 14:
        digits = '234' + digits[4:]

    if digits.startswith('234') and len(digits) >= 13:
        local_digits = digits[3:]
        if local_digits.startswith('0'):
            local_digits = local_digits[1:]
        if len(local_digits) >= 10:
            return '234' + local_digits
        return digits

    if digits.startswith('0') and len(digits) == 11:
        return '234' + digits[1:]

    if len(digits) == 10 and digits[0] in '789':
        return '234' + digits

    return digits


def normalize_client_name(value):
    return str(value or '').strip().upper()


def find_existing_client_key(name, registry):
    registry = registry or {}
    target = normalize_client_name(name)
    if not target:
        return ''
    for existing_key in registry:
        if normalize_client_name(existing_key) == target:
            return existing_key
    return ''


def set_client_phone(name, phone, registry):
    registry = registry if registry is not None else {}
    clean_name = str(name or '').strip()
    clean_phone = normalize_phone_number(phone)
    if not clean_name or not clean_phone:
        return False, False, ''

    existing_key = find_existing_client_key(clean_name, registry)
    if existing_key:
        changed = str(registry.get(existing_key, '')).strip() != clean_phone
        registry[existing_key] = clean_phone
        return False, changed, existing_key

    normalized_name = normalize_client_name(clean_name)
    registry[normalized_name] = clean_phone
    return True, True, normalized_name


def validate_client_entry(name, phone):
    normalized_name = normalize_client_name(name)
    normalized_phone = normalize_phone_number(phone)
    if not normalized_name:
        return {'error': 'Enter a client name.'}
    if not normalized_phone or len(normalized_phone) < 10:
        return {'error': 'Enter a valid phone number using local or international digits.'}
    return {'name': normalized_name, 'phone': normalized_phone}


def match_contact_to_client_name(contact_name, candidate_names):
    contact_upper = normalize_client_name(contact_name)
    if not contact_upper:
        return ''

    for candidate in candidate_names or []:
        candidate_upper = normalize_client_name(candidate)
        if not candidate_upper:
            continue
        if contact_upper == candidate_upper:
            return candidate
        if contact_upper in candidate_upper or candidate_upper in contact_upper:
            return candidate

    return ''


def build_selected_contact_updates(selected_contacts):
    updates = {}
    for contact in selected_contacts or []:
        contact_name = normalize_client_name(contact.get('name'))
        contact_phone = normalize_phone_number(contact.get('phone'))
        if contact_name and contact_phone:
            updates[contact_name] = contact_phone
    return updates


def build_matched_contact_updates(imported_contacts, candidate_names, registry):
    registry = registry or {}
    matched = []
    unmatched = []
    updates = {}

    for contact in imported_contacts or []:
        debtor = match_contact_to_client_name(contact.get('name'), candidate_names)
        phone = normalize_phone_number(contact.get('phone'))
        if debtor and phone:
            matched.append((normalize_client_name(debtor), phone))
        else:
            unmatched.append(str(contact.get('name', '')).strip())

    for name, phone in matched:
        existing_phone = normalize_phone_number(registry.get(name, ''))
        if name not in registry or not existing_phone:
            updates[name] = phone

    return {
        'matched': matched,
        'updates': updates,
        'unmatched': unmatched,
    }


def build_client_directory_rows(registry):
    rows = [['NAME', 'PHONE NUMBER']]
    for name in sorted((registry or {}), key=lambda item: normalize_client_name(item)):
        phone = normalize_phone_number((registry or {}).get(name, ''))
        if phone:
            rows.append([normalize_client_name(name), phone])

    if len(rows) == 1:
        rows.append(['', ''])
    return rows


def import_sheet_phone_numbers_to_registry(values, name_col, phone_col, registry):
    added = 0
    updated = 0
    if not values or name_col is None or phone_col is None:
        return added, updated

    for row in values[1:]:
        if name_col >= len(row) or phone_col >= len(row):
            continue

        name = str(row[name_col]).strip()
        phone = normalize_phone_number(row[phone_col])
        if not name or not phone:
            continue

        was_added, changed, _ = set_client_phone(name, phone, registry)
        if was_added:
            added += 1
        elif changed:
            updated += 1

    return added, updated
