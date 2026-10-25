#!/usr/bin/env python3
"""Rollback FIFO payment receipt consolidation.

Restores child allocation rows (clears reversed_at) and soft-hides parent
receipt rows (sets reversed_at). Does not call undo_sale_payment / redo —
sale amount_paid was never changed by the consolidate step.

Usage:
  SUPABASE_URL=https://....supabase.co \\
  SUPABASE_SERVICE_ROLE_KEY=... \\
  python3 scripts/ops/fifo_payment_receipt_rollback.py

Optional:
  --audit path/to/fifo_payment_receipt_consolidate_audit.json
  --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def rest(method: str, base: str, key: str, path: str, params=None, data=None):
    q = "?" + urllib.parse.urlencode(params or {}, doseq=True) if params else ""
    body = None if data is None else json.dumps(data).encode()
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    req = urllib.request.Request(base + path + q, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=90) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--audit",
        default=str(Path(__file__).with_name("fifo_payment_receipt_consolidate_audit.json")),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
    if not base or not key:
        print("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        return 2

    audit = json.load(open(args.audit))
    groups = audit.get("groups_consolidated") or []
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    print(f"Rolling back {len(groups)} groups (dry_run={args.dry_run})")

    for g in groups:
        parent_id = g["parent_id"]
        child_ids = g["child_ids"]
        print(f"- {g['prefix']} {g['customer_name']} parent={parent_id} children={len(child_ids)}")
        if args.dry_run:
            continue
        rest(
            "PATCH",
            base,
            key,
            "/rest/v1/payments",
            params={"id": f"in.({','.join(child_ids)})"},
            data={"reversed_at": None},
        )
        rest(
            "PATCH",
            base,
            key,
            "/rest/v1/payments",
            params={"id": f"eq.{parent_id}"},
            data={"reversed_at": now},
        )

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
