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

export function fetchHomeBootstrap({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/home-bootstrap', {
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

export function updateDebtorService({ nameInput, rowIdx, price = null, amountPaid = null, status = '', newName = '', forceRefresh = false }) {
  return requestJson('/api/billing/services/update', {
    method: 'POST',
    body: {
      name_input: nameInput,
      row_idx: rowIdx,
      price,
      amount_paid: amountPaid,
      status,
      new_name: newName,
      force_refresh: forceRefresh,
    },
  });
}

export function updateSalesTodayPayment({ rowNum, paymentStatus, amountPaid = null, forceRefresh = false }) {
  return requestJson('/api/billing/services/payment-update', {
    method: 'POST',
    body: {
      row_num: rowNum,
      payment_status: paymentStatus,
      amount_paid: amountPaid,
      force_refresh: forceRefresh,
    },
  });
}

export function returnDebtorService({ nameInput, rowIdx, forceRefresh = false }) {
  return requestJson('/api/billing/services/return', {
    method: 'POST',
    body: {
      name_input: nameInput,
      row_idx: rowIdx,
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

export function fetchServicesToday({ forceRefresh = false, targetDate = '', signal } = {}) {
  return requestJson('/api/billing/services-today/live', {
    query: {
      force_refresh: forceRefresh,
      target_date: targetDate,
    },
    signal,
  });
}

export function searchServices({ query = '', forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/services/search', {
    query: {
      q: query,
      force_refresh: forceRefresh,
    },
    signal,
  });
}

export function fetchFoundationCashflowSummary({ signal } = {}) {
  return requestJson('/api/foundation/cashflow-summary', { signal });
}

export function fetchFoundationWeeklyAllowance({ signal, cashflowPin = '' } = {}) {
  return requestJson('/api/foundation/weekly-allowance', {
    signal,
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
  });
}

export function fetchFoundationCashflowDashboard({ forceRefresh = false, signal, cashflowPin = '' } = {}) {
  return requestJson('/api/foundation/cashflow-dashboard', {
    query: { force_refresh: forceRefresh },
    signal,
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
  });
}

export function createFoundationExpense({ amount, category = '', description = '', date = '', allowanceImpact = 'personal_allowance', cashflowPin = '' }) {
  return requestJson('/api/foundation/expenses', {
    method: 'POST',
    body: {
      amount,
      category,
      description,
      date,
      allowance_impact: allowanceImpact,
    },
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
  });
}

export function undoLastWeeklyAllowanceWithdrawal({ cashflowPin = '' } = {}) {
  return requestJson('/api/foundation/allowance/undo-last', {
    method: 'POST',
    body: {},
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
  });
}

export function changeCashflowPin({ currentPin, newPin }) {
  return requestJson('/api/foundation/cashflow-pin/change', {
    method: 'POST',
    body: {
      current_pin: currentPin,
      new_pin: newPin,
    },
  });
}

export function fetchClients({ forceReload = false, signal } = {}) {
  return requestJson('/api/clients/live', {
    query: { force_reload: forceReload },
    signal,
  });
}

export function upsertClient({ previousName = null, name, phone, gender = null, syncSheet = true, forceRefresh = false }) {
  return requestJson('/api/clients/live/upsert', {
    method: 'POST',
    body: {
      previous_name: previousName,
      name,
      phone,
      gender,
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
  return requestJson('/api/stock/live/import-sheet-phones', {
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

export function fetchDashboardLogo({ signal, auth = true } = {}) {
  return requestJson('/api/assets/logo', { signal, auth });
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

export function addStockRecord({ valuesByHeader, forceRefresh = false, allowStolenWarningOverride = false }) {
  return requestJson('/api/stock/live/add', {
    method: 'POST',
    timeoutMs: 30000,
    body: {
      values_by_header: valuesByHeader,
      force_refresh: forceRefresh,
      allow_stolen_warning_override: allowStolenWarningOverride,
    },
  });
}

export function checkStolenDeviceImei({ imei }) {
  return requestJson('/api/stock/live/stolen-devices/check', {
    method: 'POST',
    timeoutMs: 8000,
    body: { imei },
  });
}

export function fetchStolenDevices({ includeInactive = false, signal } = {}) {
  return requestJson('/api/stock/live/stolen-devices', {
    query: { include_inactive: includeInactive },
    signal,
  });
}

export function createStolenDevice({ phoneName = '', imeiRaw, note = '', source = '' }) {
  return requestJson('/api/stock/live/stolen-devices', {
    method: 'POST',
    body: {
      phone_name: phoneName,
      imei_raw: imeiRaw,
      note,
      source,
    },
  });
}

export function updateStolenDevice({ recordId, phoneName = null, note = null, source = null, isActive = null, clearedNote = null }) {
  return requestJson(`/api/stock/live/stolen-devices/${encodeURIComponent(recordId)}`, {
    method: 'PATCH',
    body: {
      phone_name: phoneName,
      note,
      source,
      is_active: isActive,
      cleared_note: clearedNote,
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