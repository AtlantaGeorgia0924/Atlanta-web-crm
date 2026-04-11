import re
from difflib import SequenceMatcher


def _normalize_name_text(value):
    text = str(value or '').upper().strip()
    if not text:
        return ''
    text = re.sub(r'[^A-Z0-9]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _collapsed_name_text(value):
    return _normalize_name_text(value).replace(' ', '')


def _dice_coefficient(a, b):
    if len(a) < 2 or len(b) < 2:
        return 1.0 if a == b and a else 0.0

    a_pairs = {a[index:index + 2] for index in range(len(a) - 1)}
    b_pairs = {b[index:index + 2] for index in range(len(b) - 1)}
    if not a_pairs or not b_pairs:
        return 0.0

    overlap = len(a_pairs & b_pairs)
    return (2 * overlap) / (len(a_pairs) + len(b_pairs))


def _token_similarity(a_tokens, b_tokens):
    if not a_tokens or not b_tokens:
        return 0.0

    forward = [max(SequenceMatcher(None, token, other).ratio() for other in b_tokens) for token in a_tokens]
    reverse = [max(SequenceMatcher(None, token, other).ratio() for other in a_tokens) for token in b_tokens]
    return (sum(forward) / len(forward) + sum(reverse) / len(reverse)) / 2


def fuzzy_score(a, b):
    normalized_a = _normalize_name_text(a)
    normalized_b = _normalize_name_text(b)
    if not normalized_a or not normalized_b:
        return 0.0

    collapsed_a = _collapsed_name_text(normalized_a)
    collapsed_b = _collapsed_name_text(normalized_b)
    full_ratio = SequenceMatcher(None, normalized_a, normalized_b).ratio()
    collapsed_ratio = SequenceMatcher(None, collapsed_a, collapsed_b).ratio()
    dice_ratio = _dice_coefficient(collapsed_a, collapsed_b)
    token_ratio = _token_similarity(normalized_a.split(), normalized_b.split())

    return max(
        full_ratio,
        (full_ratio * 0.34) + (collapsed_ratio * 0.34) + (token_ratio * 0.2) + (dice_ratio * 0.12),
    )


def find_name_mismatches(values, client_names):
    known_upper = {_normalize_name_text(name) for name in (client_names or [])}
    if not values:
        return []

    header = values[0]
    try:
        name_col = header.index("NAME")
    except ValueError:
        return []

    seen = {}
    for row_idx in range(1, len(values)):
        row = values[row_idx]
        if name_col >= len(row):
            continue
        raw = row[name_col].strip()
        if not raw:
            continue

        name_upper = _normalize_name_text(raw)
        if name_upper in known_upper:
            continue
        if name_upper in seen:
            seen[name_upper]['rows'].append(row_idx)
            continue

        candidates = sorted(
            [(candidate, fuzzy_score(name_upper, candidate)) for candidate in (client_names or [])],
            key=lambda x: x[1],
            reverse=True
        )
        candidates = [name for name, score in candidates if score >= 0.68][:6]
        if candidates:
            seen[name_upper] = {'raw': raw, 'rows': [row_idx], 'candidates': candidates}

    return list(seen.values())


def build_name_fix_updates(values, mismatch_entry, correct_name):
    if not values:
        return []

    header = values[0]
    try:
        name_col = header.index("NAME")
    except ValueError:
        return []

    bad_upper = str(mismatch_entry.get('raw', '') or '').strip().upper()
    correct_upper = str(correct_name or '').strip().upper()
    if not bad_upper or not correct_upper:
        return []

    updates = []
    for row_idx in mismatch_entry.get('rows', []):
        if row_idx >= len(values):
            continue
        row = values[row_idx]
        current = row[name_col].strip().upper() if name_col < len(row) else ''
        if current == bad_upper:
            updates.append((row_idx + 1, name_col + 1, correct_upper))
    return updates


def build_name_fix_all_updates(values, mismatch_entries):
    updates = []
    for entry in mismatch_entries or []:
        candidates = entry.get('candidates', [])
        if not candidates:
            continue
        updates.extend(build_name_fix_updates(values, entry, candidates[0]))
    return updates


def build_name_fix_summary(mismatch_entries):
    return '\n'.join(
        f"  '{entry['raw']}' -> '{entry['candidates'][0]}' ({len(entry['rows'])} rows)"
        for entry in (mismatch_entries or [])
        if entry.get('candidates')
    )
