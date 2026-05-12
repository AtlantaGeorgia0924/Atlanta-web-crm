import { requestJson } from './http';

const SHORT_CACHE_MS = 8_000;
const MEDIUM_CACHE_MS = 20_000;
const LONG_CACHE_MS = 60_000;

export function fetchLiveDebtors({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/debtors/live', {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchLiveSalesSnapshot({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/sales-snapshot/live', {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchHomeBootstrap({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/home-bootstrap', {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchOutstandingItems(nameInput, { forceRefresh = false, signal } = {}) {
  return requestJson(`/api/billing/outstanding-items/live/${encodeURIComponent(nameInput)}`, {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchLiveBill(nameInput, { forceRefresh = false, signal } = {}) {
  return requestJson(`/api/billing/bill/live/${encodeURIComponent(nameInput)}`, {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
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
    writeTable: 'operational_billing_rows',
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
    writeTable: 'operational_billing_rows',
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
    writeTable: 'operational_billing_rows',
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
    writeTable: 'operational_billing_rows',
  });
}

export function undoPayment() {
  return requestJson('/api/billing/payments/undo', {
    method: 'POST',
    body: {},
    writeTable: 'operational_billing_rows',
  });
}

export function redoPayment() {
  return requestJson('/api/billing/payments/redo', {
    method: 'POST',
    body: {},
    writeTable: 'operational_billing_rows',
  });
}

export function fetchWhatsappHistory({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/whatsapp/history/live', {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : MEDIUM_CACHE_MS,
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
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchUnpaidTodayBills({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/unpaid-today/live-bills', {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchServicesToday({ forceRefresh = false, targetDate = '', signal } = {}) {
  return requestJson('/api/billing/services-today/live', {
    query: {
      force_refresh: forceRefresh,
      target_date: targetDate,
    },
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function searchServices({ query = '', forceRefresh = false, signal } = {}) {
  return requestJson('/api/billing/services/search', {
    query: {
      q: query,
      force_refresh: forceRefresh,
    },
    signal,
    cacheTtlMs: forceRefresh ? 0 : 2_000,
  });
}

export function fetchFoundationCashflowSummary({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/foundation/cashflow-summary', {
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchFoundationWeeklyAllowance({ forceRefresh = false, signal, cashflowPin = '' } = {}) {
  return requestJson('/api/foundation/weekly-allowance', {
    signal,
    timeoutMs: 15_000,
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
}

export function fetchFoundationCashflowDashboard({ forceRefresh = false, signal, cashflowPin = '' } = {}) {
  return requestJson('/api/foundation/cashflow-dashboard', {
    query: { force_refresh: forceRefresh },
    signal,
    timeoutMs: forceRefresh ? 35_000 : 20_000,
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
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
    writeTable: 'manual_expenses',
  });
}

export function reverseFoundationExpense(expenseId) {
  return requestJson(`/api/foundation/expenses/${encodeURIComponent(String(expenseId || '').trim())}/reverse`, {
    method: 'POST',
    body: {},
    writeTable: 'manual_expenses',
  });
}

export function undoLastWeeklyAllowanceWithdrawal({ cashflowPin = '' } = {}) {
  return requestJson('/api/foundation/allowance/undo-last', {
    method: 'POST',
    body: {},
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
    writeTable: 'allowance_withdrawals',
  });
}

export function createFoundationAllowanceWithdrawal({ weekStart = '', allowanceAmount = 0, withdrawnBy = '', cashflowPin = '' }) {
  return requestJson('/api/foundation/allowance/withdraw', {
    method: 'POST',
    body: {
      week_start: weekStart,
      allowance_amount: allowanceAmount,
      withdrawn_by: withdrawnBy,
    },
    headers: cashflowPin ? { 'X-Cashflow-PIN': String(cashflowPin) } : {},
    writeTable: 'allowance_withdrawals',
  });
}

export function changeCashflowPin({ currentPin, newPin }) {
  return requestJson('/api/foundation/cashflow-pin/change', {
    method: 'POST',
    body: {
      current_pin: currentPin,
      new_pin: newPin,
    },
    writeTable: 'app_config',
  });
}

export function fetchClients({ forceReload = false, signal } = {}) {
  return requestJson('/api/clients/live', {
    query: { force_reload: forceReload },
    signal,
    cacheTtlMs: forceReload ? 0 : MEDIUM_CACHE_MS,
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
    writeTable: 'clients',
  });
}

export function deleteClient({ name, syncSheet = true }) {
  return requestJson('/api/clients/live/delete', {
    method: 'POST',
    body: {
      name,
      sync_sheet: syncSheet,
    },
    writeTable: 'clients',
  });
}

export function importSheetPhones({ forceRefresh = false } = {}) {
  return requestJson('/api/stock/live/import-sheet-phones', {
    method: 'POST',
    query: { force_refresh: forceRefresh },
  });
}

export function importContactsFromSheet() {
  return requestJson('/api/clients/live/import-contacts-from-sheet', {
    method: 'POST',
  });
}

export function fetchGoogleContacts({ search = '', forceRefresh = false, signal } = {}) {
  return requestJson('/api/clients/google-contacts', {
    query: {
      search,
      force_refresh: forceRefresh,
    },
    signal,
    cacheTtlMs: forceRefresh ? 0 : LONG_CACHE_MS,
  });
}

export function syncGoogleContacts({ search = '' } = {}) {
  return requestJson('/api/clients/google-contacts/sync', {
    method: 'POST',
    query: { search },
  });
}

export function fetchDashboardLogo({ signal, auth = true } = {}) {
  return requestJson('/api/assets/logo', { signal, auth, cacheTtlMs: LONG_CACHE_MS });
}

export function fetchNameFixes({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/name-fix/live', {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : MEDIUM_CACHE_MS,
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
    writeTable: 'operational_billing_rows',
  });
}

export function applyAllNameFixes({ mismatchEntries, forceRefresh = false }) {
  return requestJson('/api/name-fix/live/apply-all', {
    method: 'POST',
    body: {
      mismatch_entries: mismatchEntries,
      force_refresh: forceRefresh,
    },
    writeTable: 'operational_billing_rows',
  });
}

export function fetchStockForm({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/stock/form/live', {
    query: { force_refresh: forceRefresh },
    signal,
    cacheTtlMs: forceRefresh ? 0 : LONG_CACHE_MS,
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
    writeTable: 'operational_stock_rows',
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
    cacheTtlMs: MEDIUM_CACHE_MS,
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
    writeTable: 'stolen_devices',
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
    writeTable: 'stolen_devices',
  });
}

export function fetchSyncStatus({ signal, forceRefresh = false } = {}) {
  return requestJson('/api/sync/status', {
    signal,
    cacheTtlMs: forceRefresh ? 0 : SHORT_CACHE_MS,
  });
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

/**
 * Perform a complete workspace refresh directly from Supabase with timing diagnostics.
 * Fetches all critical data sections and returns timing information for each endpoint.
 * @returns {Promise<Object>} { success, data, errors, timing }
 */
export async function performFullWorkspaceRefresh() {
  const startTime = performance.now();
  const results = {};
  const errors = [];
  const timings = {};

  const withTimeout = async (promise, timeoutMs, label) => {
    if (!timeoutMs || timeoutMs <= 0) {
      return promise;
    }
    return Promise.race([
      promise,
      new Promise((_, reject) => {
        window.setTimeout(() => {
          reject(new Error(`${label} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  };

  const fetchWithTiming = async (label, fetchFn, timeoutMs = 30_000) => {
    const startFetch = performance.now();
    try {
      const result = await withTimeout(fetchFn(), timeoutMs, label);
      const duration = performance.now() - startFetch;
      timings[label] = { duration: Math.round(duration), status: 'success' };
      results[label] = result;
      return { success: true, result };
    } catch (error) {
      const duration = performance.now() - startFetch;
      timings[label] = { duration: Math.round(duration), status: 'error', error: error.message };
      errors.push({ endpoint: label, error: error.message });
      return { success: false, error };
    }
  };

  // Fetch all sections in parallel for maximum performance
  await Promise.all([
    // Billing & Dashboard Data
    fetchWithTiming('home-bootstrap', () =>
      fetchHomeBootstrap({ forceRefresh: true })
    ),
    fetchWithTiming('unpaid-today', () =>
      fetchUnpaidToday({ forceRefresh: true })
    ),
    fetchWithTiming('unpaid-bills', () =>
      fetchUnpaidTodayBills({ forceRefresh: true })
    ),
    fetchWithTiming('whatsapp-history', () =>
      fetchWhatsappHistory({ forceRefresh: true })
    ),
    fetchWithTiming('pending-service-deals', () =>
      fetchPendingServiceDeals({ forceRefresh: true })
    ),

    // Stock Data
    fetchWithTiming('stock-dashboard', () =>
      fetchStockDashboard({ filterMode: 'all', forceRefresh: true })
    ),
    fetchWithTiming('stock-form', () =>
      fetchStockForm({ forceRefresh: true })
    ),

    // Financial & Foundation Data
    fetchWithTiming('cashflow-summary', () =>
      fetchFoundationCashflowSummary({ forceRefresh: true })
    ),
    fetchWithTiming(
      'cashflow-dashboard',
      () => fetchFoundationCashflowDashboard({ forceRefresh: true }),
      40_000,
    ),
    fetchWithTiming('weekly-allowance', () =>
      fetchFoundationWeeklyAllowance({ forceRefresh: true })
    ),

    // Clients & Supporting Data
    fetchWithTiming('clients', () =>
      fetchClients({ forceReload: true })
    ),
    fetchWithTiming('sync-status', () =>
      fetchSyncStatus({ forceRefresh: true })
    ),
  ]);

  const totalDuration = performance.now() - startTime;

  return {
    success: errors.length === 0,
    data: results,
    errors,
    timing: {
      total: Math.round(totalDuration),
      byEndpoint: timings,
      successCount: Object.values(timings).filter(t => t.status === 'success').length,
      errorCount: Object.values(timings).filter(t => t.status === 'error').length,
    },
  };
}

export function syncToGoogleSheets({ limit = 5000 } = {}) {
  return requestJson('/api/sync/push-to-sheets', {
    method: 'POST',
    query: { limit },
  });
}