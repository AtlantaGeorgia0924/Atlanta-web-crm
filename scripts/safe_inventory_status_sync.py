#!/usr/bin/env python3
"""Safe inventory status sync with backup + dry-run safeguards.

Usage examples:
  # Preview only (no writes)
  /usr/local/bin/python3 scripts/safe_inventory_status_sync.py --dry-run

  # Apply with mandatory confirmation token
  /usr/local/bin/python3 scripts/safe_inventory_status_sync.py --apply --confirm APPLY_STATUS_SYNC
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from services.sync_service import detect_sheet_header_row

CONFIRM_TOKEN = "APPLY_STATUS_SYNC"


def normalize_header(text: Any) -> str:
    return " ".join(str(text or "").strip().upper().replace("_", " ").replace("-", " ").split())


def clean_amount(value: Any) -> float:
    text = str(value or "").strip().replace("\u20a6", "").replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def get_col_indexes(headers_upper: list[str], aliases: list[str]) -> list[int]:
    normalized = [normalize_header(h) for h in headers_upper]
    wanted = {normalize_header(a) for a in aliases}
    return [i for i, h in enumerate(normalized) if h in wanted]


def load_sheet_from_config(base_dir: Path):
    config = json.loads((base_dir / "config.json").read_text())
    creds_file = config.get("credentials_file") or "credentials.json"
    creds_path = (base_dir / creds_file).resolve()
    spreadsheet_id = str(config.get("sheet_id") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing sheet_id in config.json")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(spreadsheet_id).sheet1
    return ws


def compute_status(price: float, paid: float) -> str:
    if paid <= 0:
        return "UNPAID"
    if price > 0 and paid < price:
        return "PART PAYMENT"
    return "PAID"


def main() -> None:
    parser = argparse.ArgumentParser(description="Safe inventory status sync")
    parser.add_argument("--apply", action="store_true", help="Apply updates. Without this flag, script is preview-only.")
    parser.add_argument("--dry-run", action="store_true", help="Preview-only mode.")
    parser.add_argument("--confirm", default="", help=f"Required token for --apply: {CONFIRM_TOKEN}")
    args = parser.parse_args()

    do_apply = bool(args.apply and not args.dry_run)
    if do_apply and args.confirm != CONFIRM_TOKEN:
        raise SystemExit(f"Refusing to apply without --confirm {CONFIRM_TOKEN}")

    base_dir = BASE_DIR
    ws = load_sheet_from_config(base_dir)

    values = ws.get_all_values()
    if not values:
        raise SystemExit("Main sheet is empty")

    header_row_idx = detect_sheet_header_row(values)
    headers = [str(c or "").strip() for c in values[header_row_idx]]
    headers_upper = [h.upper() for h in headers]

    status_cols = get_col_indexes(headers_upper, ["STATUS"])
    paid_cols = get_col_indexes(headers_upper, ["AMOUNT PAID", "AMOUNT PAID "])
    price_cols = get_col_indexes(headers_upper, ["PRICE", "AMOUNT SOLD", "SELLING PRICE"])

    if not status_cols or not paid_cols or not price_cols:
        raise SystemExit(
            f"Missing required columns: status={status_cols}, paid={paid_cols}, price={price_cols}"
        )

    status_col = status_cols[0]
    name_col = get_col_indexes(headers_upper, ["NAME", "CLIENT NAME", "CUSTOMER NAME"])
    name_col = name_col[0] if name_col else None

    updates = []
    audit = []
    checked = 0
    for row_num in range(header_row_idx + 2, len(values) + 1):
        row = values[row_num - 1]
        current_status = str(row[status_col] if status_col < len(row) else "").strip().upper()
        if "RETURN" in current_status:
            continue

        paid = max([clean_amount(row[i] if i < len(row) else "") for i in paid_cols] or [0.0])
        price = max([clean_amount(row[i] if i < len(row) else "") for i in price_cols] or [0.0])
        if paid <= 0 and price <= 0:
            continue

        expected_status = compute_status(price, paid)
        checked += 1
        if expected_status == current_status:
            continue

        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_num, status_col + 1),
            "values": [[expected_status]],
        })
        audit.append({
            "row": row_num,
            "name": str(row[name_col] if name_col is not None and name_col < len(row) else ""),
            "old_status": current_status,
            "new_status": expected_status,
            "price": price,
            "paid": paid,
        })

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = base_dir / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    backup_payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "header_row_idx": header_row_idx + 1,
        "sheet_title": ws.title,
        "status_col": status_col + 1,
        "checked_rows": checked,
        "change_count": len(updates),
        "changes": audit,
    }
    backup_path = backup_dir / f"inventory_status_backup_{timestamp}.json"
    backup_path.write_text(json.dumps(backup_payload, indent=2))

    print(f"Checked rows: {checked}")
    print(f"Planned status updates: {len(updates)}")
    print(f"Backup written: {backup_path}")

    if updates and do_apply:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        print("Applied updates to sheet.")
    elif do_apply:
        print("No changes to apply.")
    else:
        print("Dry-run only. No sheet changes were made.")


if __name__ == "__main__":
    main()
