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

## Permanent fix (needs agt-crm-v3 source)

- Change `apply_customer_payment_fifo` to also insert a parent `sale_id=null` row
  (or a `payment_batches` table) when applying a general payment.
- Optionally group Payment Receipts by `ATL-PAY-*` prefix in the frontend.
