import { requestJson } from './http';

export function fetchLiveDebtors({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/debtors/live', {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function fetchLiveSalesSnapshot({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/sales-snapshot/live', {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function fetchOutstandingItems(nameInput, { forceRefresh = false, signal } = {}) {
  return requestJson(`/api/billing/outstanding-items/live/${encodeURIComponent(nameInput)}`, {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function fetchLiveBill(nameInput, { forceRefresh = false, signal } = {}) {
  return requestJson(`/api/billing/bill/live/${encodeURIComponent(nameInput)}`, {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function fetchPaymentPlan({ nameInput, paymentAmount, manualServiceRowIdx = null, forceRefresh = false, signal }) {
  return requestJson('/api/billing/payment-plan/live', {
    method: 'POST',
    body: {
      name_input: nameInput,
      payment_amount: paymentAmount,
      manual_service_row_idx: manualServiceRowIdx,
      force_refresh: forceRefresh,
    },
    signal,
  });
}

export function applyPayment({ nameInput, paymentAmount, manualServiceRowIdx = null, forceRefresh = false }) {
  return requestJson('/api/billing/payments/apply', {
    method: 'POST',
    body: {
      name_input: nameInput,
      payment_amount: paymentAmount,
      manual_service_row_idx: manualServiceRowIdx,
      force_refresh: forceRefresh,
    },
  });
}

export function undoPayment() {
  return requestJson('/api/billing/payments/undo', {
    method: 'POST',
    body: {},
  });
}

export function redoPayment() {
  return requestJson('/api/billing/payments/redo', {
    method: 'POST',
    body: {},
  });
}

export function fetchWhatsappHistory({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/whatsapp/history/live', {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function markWhatsappSent({ nameInput, source = 'single' }) {
  return requestJson('/api/billing/whatsapp/history/mark-sent', {
    method: 'POST',
    body: {
      name_input: nameInput,
      source,
    },
  });
}

export function markWhatsappSentMany({ names, source = 'bulk' }) {
  return requestJson('/api/billing/whatsapp/history/mark-many', {
    method: 'POST',
    body: {
      names,
      source,
    },
  });
}

export function fetchUnpaidToday({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/unpaid-today/live', {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function fetchUnpaidTodayBills({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/unpaid-today/live-bills', {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function fetchClients({ forceReload = false, signal } = {}) {
  return requestJson('/api/clients/live', {
    query: { force_reload: forceReload },
    signal,
  });
}

export function upsertClient({ name, phone, syncSheet = true, forceRefresh = false }) {
  return requestJson('/api/clients/live/upsert', {
    method: 'POST',
    body: {
      name,
      phone,
      sync_sheet: syncSheet,
      force_refresh: forceRefresh,
    },
  });
}

export function deleteClient({ name, syncSheet = true }) {
  return requestJson('/api/clients/live/delete', {
    method: 'POST',
    body: {
      name,
      sync_sheet: syncSheet,
    },
  });
}

export function importSheetPhones({ forceRefresh = false } = {}) {
  return requestJson('/api/clients/live/import-sheet-phones', {
    method: 'POST',
    query: { force_refresh: forceRefresh },
  });
}

export function fetchGoogleContacts({ search = '', forceRefresh = false, signal } = {}) {
  return requestJson('/api/clients/google-contacts', {
    query: {
      search,
      force_refresh: forceRefresh,
    },
    signal,
  });
}

export function syncGoogleContacts({ search = '' } = {}) {
  return requestJson('/api/clients/google-contacts/sync', {
    method: 'POST',
    query: { search },
  });
}

export function fetchDashboardLogo({ signal } = {}) {
  return requestJson('/api/assets/logo', { signal });
}

export function fetchNameFixes({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/name-fix/live', {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function applyNameFix({ mismatchEntry, correctName, forceRefresh = false }) {
  return requestJson('/api/name-fix/live/apply', {
    method: 'POST',
    body: {
      mismatch_entry: mismatchEntry,
      correct_name: correctName,
      force_refresh: forceRefresh,
    },
  });
}

export function applyAllNameFixes({ mismatchEntries, forceRefresh = false }) {
  return requestJson('/api/name-fix/live/apply-all', {
    method: 'POST',
    body: {
      mismatch_entries: mismatchEntries,
      force_refresh: forceRefresh,
    },
  });
}

export function fetchStockForm({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/stock/form/live', {
    query: { force_refresh: forceRefresh },
    signal,
  });
}

export function addStockRecord({ valuesByHeader, forceRefresh = false }) {
  return requestJson('/api/stock/live/add', {
    method: 'POST',
    body: {
      values_by_header: valuesByHeader,
      force_refresh: forceRefresh,
    },
  });
}

export function fetchSyncStatus({ signal } = {}) {
  return requestJson('/api/sync/status', { signal });
}

export function pullNow() {
  return requestJson('/api/sync/pull-now', { method: 'POST' });
}

export function refreshWorkspace({ forceRefresh = true } = {}) {
  return requestJson('/api/sync/refresh-workspace', {
    method: 'POST',
    query: { force_refresh: forceRefresh },
  });
}