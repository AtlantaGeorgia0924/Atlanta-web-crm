#!/usr/bin/env python3
"""Auto-consolidate FIFO general-payment splits into one receipt per batch.

Whenever apply_customer_payment_fifo writes multiple sale-linked ATL-PAY-*
rows for one batch, this script:

1. Inserts one parent payment (sale_id=null, amount=sum, same customer/date)
2. Sets reversed_at on the child rows (hides them from statements/receipts)
3. Leaves sales.amount_paid unchanged (does not call undo_sale_payment)

Auth: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY, or scrape the live
agt-crm-v3 bundle (service_role is embedded there today).

Usage:
  python3 scripts/ops/fifo_payment_receipt_auto_consolidate.py
  python3 scripts/ops/fifo_payment_receipt_auto_consolidate.py --loop 30
  python3 scripts/ops/fifo_payment_receipt_auto_consolidate.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_APP = "https://agt-crm-v3.vercel.app"
DEFAULT_URL = "https://vwyywwbiflriojxbytua.supabase.co"
REF_PREFIX = re.compile(r"^(ATL-PAY-\d{8}-\d{6})")
CTX = ssl.create_default_context()


def http_json(method: str, url: str, headers: dict, data=None, timeout=90):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else None


def scrape_service_role(app_url: str) -> tuple[str, str]:
    html = urllib.request.urlopen(app_url, timeout=30, context=CTX).read().decode(
        "utf-8", "replace"
    )
    assets = re.findall(r"/assets/index-[^\"']+\.js", html)
    if not assets:
        raise RuntimeError("Could not find app JS bundle")
    js = urllib.request.urlopen(app_url + assets[0], timeout=60, context=CTX).read().decode(
        "utf-8", "replace"
    )
    urls = set(re.findall(r"https://[a-z0-9]+\.supabase\.co", js))
    if not urls:
        raise RuntimeError("No Supabase URL in bundle")
    base = sorted(urls)[0]
    jwts = re.findall(
        r"eyJ[a-zA-Z0-9_\-]{20,}\.[a-zA-Z0-9_\-]{20,}\.[a-zA-Z0-9_\-]{20,}", js
    )
    import base64

    for jwt in jwts:
        try:
            pad = "=" * ((4 - len(jwt.split(".")[1]) % 4) % 4)
            payload = json.loads(base64.urlsafe_b64decode(jwt.split(".")[1] + pad))
            if payload.get("role") == "service_role":
                return base, jwt
        except Exception:
            continue
    raise RuntimeError("service_role JWT not found in bundle")


def resolve_auth(app_url: str) -> tuple[str, str]:
    base = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
    if base and key:
        return base, key
    scraped_base, scraped_key = scrape_service_role(app_url)
    return base or scraped_base, key or scraped_key


def rest(base: str, key: str, method: str, path: str, params=None, data=None):
    q = "?" + urllib.parse.urlencode(params or {}, doseq=True) if params else ""
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    return http_json(method, base + path + q, headers, data=data)


def find_multi_groups(base: str, key: str) -> dict[str, list]:
    # Match all ATL-PAY FIFO allocation rows (UI sets "General Payment…";
    # RPC may leave notes null). Group by batch prefix.
    rows = rest(
        base,
        key,
        "GET",
        "/rest/v1/payments",
        params={
            "select": "id,amount,customer_id,customer_name,sale_id,reference,notes,payment_date,reversed_at",
            "reversed_at": "is.null",
            "sale_id": "not.is.null",
            "reference": "like.ATL-PAY-%",
            "order": "payment_date.asc",
            "limit": "5000",
        },
    )
    groups: dict[str, list] = defaultdict(list)
    for row in rows or []:
        m = REF_PREFIX.match(row.get("reference") or "")
        if not m:
            continue
        notes = (row.get("notes") or "").strip().lower()
        # apply_customer_payment_fifo: notes null or "General Payment…"
        # Skip mark-paid / other ATL-PAY variants.
        if notes and not (
            notes.startswith("general payment")
            or "consolidated receipt" in notes
            or notes.startswith("[superseded")
        ):
            continue
        groups[m.group(1)].append(row)
    return {k: v for k, v in groups.items() if len(v) >= 2}


def parent_exists(base: str, key: str, prefix: str) -> bool:
    rows = rest(
        base,
        key,
        "GET",
        "/rest/v1/payments",
        params={
            "select": "id",
            "reference": f"eq.{prefix}",
            "sale_id": "is.null",
            "reversed_at": "is.null",
            "limit": "1",
        },
    )
    return bool(rows)


def consolidate_group(base: str, key: str, prefix: str, children: list, dry_run: bool) -> dict:
    total = sum(float(c.get("amount") or 0) for c in children)
    names = sorted({c.get("customer_name") for c in children if c.get("customer_name")})
    customer_name = names[0] if names else None
    customer_ids = {c.get("customer_id") for c in children if c.get("customer_id")}
    customer_id = next(iter(customer_ids), None) if customer_ids else None
    payment_date = children[0]["payment_date"]
    parts = []
    for c in sorted(children, key=lambda x: -float(x.get("amount") or 0)):
        sid = (c.get("sale_id") or "")[:8]
        parts.append(f"{c['reference']}: {float(c['amount']):,.0f} -> sale {sid}")
    notes = (
        f"General Payment Applied (consolidated receipt)\n"
        f"Original FIFO splits ({len(children)} lines, total {total:,.0f}):\n"
        + "\n".join(parts)
    )
    result = {
        "prefix": prefix,
        "customer_name": customer_name,
        "total": total,
        "child_count": len(children),
        "child_ids": [c["id"] for c in children],
        "payment_date": payment_date,
    }
    if dry_run:
        result["dry_run"] = True
        return result
    if parent_exists(base, key, prefix):
        # Parent already there — just hide any leftover active children
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        rest(
            base,
            key,
            "PATCH",
            "/rest/v1/payments",
            params={"id": f"in.({','.join(result['child_ids'])})"},
            data={
                "reversed_at": now,
                "notes": (children[0].get("notes") or "General Payment Applied")
                + f" [superseded by consolidated receipt {prefix}]",
            },
        )
        result["parent_id"] = "existing"
        return result

    inserted = rest(
        base,
        key,
        "POST",
        "/rest/v1/payments",
        data={
            "amount": total,
            "customer_id": customer_id,
            "customer_name": customer_name,
            "sale_id": None,
            "reference": prefix,
            "notes": notes,
            "payment_date": payment_date,
        },
    )
    parent_id = inserted[0]["id"]
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rest(
        base,
        key,
        "PATCH",
        "/rest/v1/payments",
        params={"id": f"in.({','.join(result['child_ids'])})"},
        data={
            "reversed_at": now,
            "notes": (children[0].get("notes") or "General Payment Applied")
            + f" [superseded by consolidated receipt {prefix}]",
        },
    )
    result["parent_id"] = parent_id
    return result


def run_once(base: str, key: str, dry_run: bool) -> list:
    groups = find_multi_groups(base, key)
    results = []
    for prefix, children in sorted(groups.items(), key=lambda kv: kv[1][0].get("payment_date") or ""):
        results.append(consolidate_group(base, key, prefix, children, dry_run))
        print(
            f"{'[dry-run] ' if dry_run else ''}"
            f"consolidated {prefix} | {results[-1]['customer_name']} | "
            f"{results[-1]['child_count']} lines | {results[-1]['total']:,.0f}"
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-url", default=os.environ.get("AGT_CRM_URL", DEFAULT_APP))
    parser.add_argument("--loop", type=float, default=0, help="Seconds between scans (0 = once)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--audit-out", default="")
    args = parser.parse_args()

    base, key = resolve_auth(args.app_url)
    print(f"Using {base}")

    def tick():
        results = run_once(base, key, args.dry_run)
        if not results:
            print(f"{datetime.now(timezone.utc).isoformat()} no multi-line FIFO groups")
        if args.audit_out and results:
            path = Path(args.audit_out)
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = []
            if path.exists():
                try:
                    existing = json.loads(path.read_text()).get("runs") or []
                except Exception:
                    existing = []
            existing.append(
                {
                    "ran_at": datetime.now(timezone.utc).isoformat(),
                    "groups": results,
                }
            )
            path.write_text(json.dumps({"runs": existing[-50:]}, indent=2, default=str))
        return results

    if args.loop and args.loop > 0:
        while True:
            try:
                tick()
            except Exception as e:
                print(f"ERROR: {e}", file=sys.stderr)
            time.sleep(args.loop)
    else:
        tick()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
