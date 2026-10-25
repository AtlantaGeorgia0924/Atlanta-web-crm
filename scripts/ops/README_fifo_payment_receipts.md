# FIFO payment receipt consolidation (agt-crm-v3)

## Problem

`apply_customer_payment_fifo` writes one `payments` row per allocated sale
(`sale_id` set, shared `ATL-PAY-YYYYMMDD-HHMMSS-*` reference). Statements and
Payment Receipts list each split separately, so a single ₦1,000,000 transfer
(e.g. DENIS4WARD `ATL-PAY-20260928-164034`) did not appear as one receipt.

## Interim data fix (applied 2026-10-25)

For each multi-line FIFO group:

1. Insert one parent payment: `sale_id=null`, same customer/date, amount = sum,
   `reference` = batch prefix (no suffix). Shows under **OTHER / GENERAL PAYMENTS**.
2. Set `reversed_at` on child allocation rows so the UI `QO` filter hides them.
3. **Do not** call `undo_sale_payment` — `sales.amount_paid` stays correct.

Audit: `fifo_payment_receipt_consolidate_audit.json`  
Rollback: `fifo_payment_receipt_rollback.py`

## Ongoing (all clients, new payments)

`fifo_payment_receipt_auto_consolidate.py` scans for new multi-line FIFO
batches and consolidates them (typically within ~20s when the loop is running).

- Local/daemon: `python3 scripts/ops/fifo_payment_receipt_auto_consolidate.py --loop 20`
- GitHub Action: `.github/workflows/fifo-receipt-consolidate.yml` (every 5 minutes)

## Permanent fix (needs DB SQL or agt-crm-v3 source)

Ideal: change `apply_customer_payment_fifo` to insert one parent `sale_id=null`
receipt (and keep/hide allocation rows), or group receipts in the v3 frontend.
