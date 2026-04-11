import { startTransition, useDeferredValue, useEffect, useState } from 'react';

import { fetchStockDashboard } from './api/stock';
import { getApiLabel } from './api/http';
import {
  applyAllNameFixes,
  applyNameFix,
  applyPayment,
  deleteClient,
  fetchClients,
  fetchLiveBill,
  fetchLiveDebtors,
  fetchLiveSalesSnapshot,
  fetchNameFixes,
  fetchOutstandingItems,
  fetchPaymentPlan,
  fetchSyncStatus,
  importSheetPhones,
  pullNow,
  refreshWorkspace,
  upsertClient,
} from './api/workspace';

const FILTER_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'available', label: 'Available' },
  { value: 'pending', label: 'Pending Deal' },
  { value: 'needs_details', label: 'Needs Details' },
  { value: 'sold', label: 'Sold' },
];

const ACTION_TILES = [
  { icon: '📒', label: 'Debtors', color: '#8bd3dd', view: 'debtors' },
  { icon: '🔄', label: 'Refresh', color: '#f4b942', action: 'refresh' },
  { icon: '↩', label: 'Undo', color: '#95d5b2', action: 'undo', disabled: true },
  { icon: '↪', label: 'Redo', color: '#b8f2e6', action: 'redo', disabled: true },
  { icon: '📦', label: 'Stock', color: '#9ad1d4', view: 'stock' },
  { icon: '👥', label: 'Clients', color: '#7bd389', view: 'clients' },
  { icon: '🛠', label: 'Fix', color: '#f7a072', view: 'fix' },
  { icon: '⚙', label: 'Settings', color: '#cab8ff', view: 'settings' },
  { icon: '⏻', label: 'Exit', color: '#d8d8d8', action: 'exit' },
];

const VIEW_META = {
  debtors: {
    title: 'Debtors And Payment Workspace',
    description: 'Review debtors, preview bills, and apply payments from the live backend.',
  },
  stock: {
    title: 'Phone Stock Dashboard',
    description: 'Search the live inventory feed, inspect statuses, and refresh from the API.',
  },
  clients: {
    title: 'Client Registry',
    description: 'Manage WhatsApp numbers and push the registry back into the shared sheets workflow.',
  },
  fix: {
    title: 'Name Fix Workspace',
    description: 'Scan live rows for mismatches, confirm replacements, and queue fixes back to the source sheet.',
  },
  settings: {
    title: 'Sync And Runtime Settings',
    description: 'Inspect cache state, queue activity, and trigger the same refresh actions the desktop app uses.',
  },
};

const STATUS_CLASS_MAP = {
  AVAILABLE: 'available',
  'PENDING DEAL': 'pending',
  'NEEDS DETAILS': 'needs-details',
  SOLD: 'sold',
};

const numberFormatter = new Intl.NumberFormat('en-US');

function formatCount(value) {
  return numberFormatter.format(Number(value || 0));
}

function formatCurrency(value) {
  return `NGN ${formatCount(value)}`;
}

function formatStamp(dateValue) {
  if (!dateValue) {
    return 'Waiting for first fetch';
  }

  return new Intl.DateTimeFormat('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    day: '2-digit',
    month: 'short',
  }).format(dateValue);
}

function formatRuntimeSnapshot(value, fallback = 'None') {
  if (!value) {
    return fallback;
  }

  if (typeof value === 'string') {
    return value;
  }

  if (typeof value === 'object') {
    const parts = [value.status, value.details, value.finished_at].filter(Boolean);
    return parts.length ? parts.join(' | ') : fallback;
  }

  return String(value);
}

function getCacheRowCount(value) {
  if (typeof value === 'number') {
    return value;
  }

  if (value && typeof value === 'object' && 'row_count' in value) {
    return value.row_count;
  }

  return 0;
}

function getStatusClass(label) {
  return STATUS_CLASS_MAP[label] || 'available';
}

function normalizeSearchValue(value) {
  return String(value || '').trim().toUpperCase();
}

function normalizeDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function buildOutstandingLabel(item) {
  const description = item?.description || 'Unnamed service';
  const dateText = item?.date ? `, ${item.date}` : '';
  return `${description}${dateText} - ${formatCurrency(item?.balance)}`;
}

function buildPaymentPreviewText(selectedDebtor, paymentAmount, paymentPlan, paymentPlanError) {
  if (!selectedDebtor) {
    return 'Select a debtor to preview payment allocation.';
  }

  if (!paymentAmount) {
    return 'Enter a payment amount to see how the system will allocate it.';
  }

  if (paymentPlanError) {
    return paymentPlanError;
  }

  if (!paymentPlan) {
    return 'Preparing payment preview...';
  }

  const lines = [
    `Customer: ${paymentPlan.name_input}`,
    `Total outstanding: ${formatCurrency(paymentPlan.total_outstanding)}`,
    `Applied amount: ${formatCurrency(paymentPlan.total_applied)}`,
    '',
    paymentPlan.status_text,
    '',
    'Updates:',
  ];

  paymentPlan.updates.forEach((update) => {
    const service = (paymentPlan.outstanding_items || []).find((item) => item.row_idx === update.row_idx);
    const description = service?.description || `Row ${update.row_idx}`;
    lines.push(`- ${description}: ${formatCurrency(update.new_paid)} -> ${update.new_status}`);
  });

  return lines.join('\n');
}

async function copyText(text) {
  if (!text) {
    return;
  }

  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textArea = document.createElement('textarea');
  textArea.value = text;
  textArea.setAttribute('readonly', 'readonly');
  textArea.style.position = 'absolute';
  textArea.style.left = '-9999px';
  document.body.appendChild(textArea);
  textArea.select();
  document.execCommand('copy');
  document.body.removeChild(textArea);
}

function BillingView({
  debtorSearch,
  setDebtorSearch,
  debtors,
  selectedDebtor,
  onSelectDebtor,
  isLoading,
  detailLoading,
  billText,
  outstandingItems,
  paymentAmount,
  setPaymentAmount,
  selectedServiceRow,
  setSelectedServiceRow,
  paymentPlan,
  paymentPlanError,
  applyingPayment,
  onCopyBill,
  onSendWhatsapp,
  onApplyPayment,
}) {
  return (
    <>
      <section className="workspace-row">
        <section className="content-panel content-panel--main">
          <div className="panel-header">
            <h3>Debtors List</h3>
            <p>Search the current debtors list and open a live bill preview for any customer owing.</p>
          </div>

          <div className="panel-toolbar">
            <div className="search-group">
              <label htmlFor="debtor-search">Search debtors:</label>
              <input
                id="debtor-search"
                type="search"
                placeholder="Customer name..."
                value={debtorSearch}
                onChange={(event) => setDebtorSearch(event.target.value)}
              />
            </div>

            <div className="toolbar-actions">
              <button type="button" className="secondary-button" onClick={onCopyBill} disabled={!selectedDebtor}>
                Copy Bill
              </button>
              <button type="button" className="primary-button" onClick={onSendWhatsapp} disabled={!selectedDebtor}>
                Send To WhatsApp
              </button>
            </div>
          </div>

          {isLoading ? <div className="notice">Loading debtors workspace...</div> : null}

          <div className="table-wrap">
            <table className="data-table debtors-table">
              <thead>
                <tr>
                  <th>Customer</th>
                  <th>Outstanding Amount</th>
                </tr>
              </thead>
              <tbody>
                {debtors.length ? (
                  debtors.map(([name, amount]) => (
                    <tr
                      key={name}
                      className={name === selectedDebtor ? 'table-row-selected' : ''}
                      onClick={() => onSelectDebtor(name)}
                    >
                      <td>{name}</td>
                      <td className="amount-cell">{formatCurrency(amount)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={2} className="empty-state">
                      No debtors matched the current filter.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <aside className="content-panel content-panel--side content-panel--stacked">
          <div className="subpanel">
            <div className="panel-header">
              <h3>Payment Update</h3>
              <p>Apply money to the selected debtor using the same payment-plan logic from the desktop app.</p>
            </div>

            <div className="form-stack">
              <label className="field-label" htmlFor="selected-debtor">Selected Customer</label>
              <input id="selected-debtor" value={selectedDebtor} readOnly placeholder="Select a debtor from the list" />

              <label className="field-label" htmlFor="payment-amount">Payment Amount</label>
              <input
                id="payment-amount"
                type="text"
                inputMode="numeric"
                placeholder="Amount received"
                value={paymentAmount}
                onChange={(event) => setPaymentAmount(normalizeDigits(event.target.value))}
              />

              <label className="field-label" htmlFor="service-target">Service Target</label>
              <select
                id="service-target"
                value={selectedServiceRow}
                onChange={(event) => setSelectedServiceRow(event.target.value)}
              >
                <option value="automatic">Automatic sequence</option>
                {outstandingItems.map((item) => (
                  <option key={item.row_idx} value={String(item.row_idx)}>
                    {buildOutstandingLabel(item)}
                  </option>
                ))}
              </select>

              <div className="meta-stack meta-stack--tight">
                <div className="meta-row">
                  <span>Outstanding services</span>
                  <strong>{formatCount(outstandingItems.length)}</strong>
                </div>
                <div className="meta-row">
                  <span>Detail status</span>
                  <strong>{detailLoading ? 'Loading...' : 'Ready'}</strong>
                </div>
              </div>

              <button type="button" className="primary-button button-wide" onClick={onApplyPayment} disabled={applyingPayment || !selectedDebtor}>
                {applyingPayment ? 'Applying Payment...' : 'Apply Payment'}
              </button>
            </div>

            <div className="preview-card">
              <h4>Payment Preview</h4>
              <pre>{buildPaymentPreviewText(selectedDebtor, paymentAmount, paymentPlan, paymentPlanError)}</pre>
            </div>
          </div>

          <div className="subpanel subpanel--muted">
            <div className="panel-header">
              <h3>Debtor Bill Preview</h3>
              <p>Live bill text for the selected debtor, ready to copy or send.</p>
            </div>

            <div className="preview-card preview-card--bill">
              <pre>{detailLoading ? 'Loading bill preview...' : billText || 'Select a debtor to preview the bill.'}</pre>
            </div>
          </div>
        </aside>
      </section>
    </>
  );
}

function StockViewSection({
  stockSearchText,
  setStockSearchText,
  filterMode,
  setFilterMode,
  stockView,
  isLoading,
  isRefreshing,
  errorText,
  onRefresh,
  lastLoadedAt,
}) {
  const headers = stockView?.headers || [];
  const rows = stockView?.all_rows_cache || [];
  const counts = stockView?.counts || {};
  const breakdown = [...(stockView?.available_breakdown || [])].sort((left, right) => right.count - left.count);
  const topBreakdown = breakdown.slice(0, 8);
  const chartItems = topBreakdown.length
    ? topBreakdown
    : FILTER_OPTIONS.filter((option) => option.value !== 'all').map((option) => ({
        brand: 'Status',
        series: option.label,
        count: counts[option.value] || 0,
      }));
  const maxChartCount = Math.max(1, ...chartItems.map((item) => Number(item.count || 0)));
  const summaryCards = [
    { label: 'Available', value: formatCount(counts.available), note: 'Ready to sell' },
    { label: 'Pending Deal', value: formatCount(counts.pending), note: 'Awaiting close' },
    { label: 'Needs Details', value: formatCount(counts.needs_details), note: 'Needs cleanup' },
    { label: 'Sold', value: formatCount(counts.sold), note: 'Closed stock' },
    { label: 'Rows In View', value: formatCount(rows.length), note: 'Current filter' },
    { label: 'Series Groups', value: formatCount(breakdown.length), note: 'Available clusters' },
    { label: 'Last Sync', value: formatStamp(lastLoadedAt), note: isRefreshing ? 'Refreshing now' : 'Live cache' },
  ];

  return (
    <>
      <section className="summary-frame">
        <h2>Stock Summary</h2>

        <div className="summary-grid">
          {summaryCards.map((card) => (
            <article key={card.label} className="metric-card">
              <span className="metric-label">{card.label}</span>
              <strong className="metric-value">{card.value}</strong>
              <span className="metric-note">{card.note}</span>
            </article>
          ))}
        </div>
      </section>

      <section className="workspace-row">
        <section className="content-panel content-panel--main">
          <div className="panel-header">
            <h3>Phone Stock Dashboard</h3>
            <p>Search the live inventory feed, inspect statuses, and refresh straight from the API.</p>
          </div>

          <div className="panel-toolbar">
            <div className="search-group">
              <label htmlFor="stock-search">Search stock:</label>
              <input
                id="stock-search"
                type="search"
                placeholder="IMEI, model, serial, status..."
                value={stockSearchText}
                onChange={(event) => setStockSearchText(event.target.value)}
              />
            </div>

            <div className="toolbar-controls">
              <div className="filter-tabs" role="tablist" aria-label="Stock filters">
                {FILTER_OPTIONS.map((option) => (
                  <button
                    key={option.value}
                    type="button"
                    className={option.value === filterMode ? 'filter-tab active' : 'filter-tab'}
                    onClick={() => setFilterMode(option.value)}
                  >
                    {option.label}
                  </button>
                ))}
              </div>

              <button type="button" className="primary-button" onClick={onRefresh}>
                {isRefreshing ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>
          </div>

          {errorText ? <div className="notice notice-error">{errorText}</div> : null}
          {isLoading ? <div className="notice">Loading live stock data...</div> : null}

          <div className="table-wrap">
            <table className="stock-table">
              <thead>
                <tr>
                  <th>Row</th>
                  <th>Status</th>
                  {headers.map((header) => (
                    <th key={header}>{header}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.length ? (
                  rows.map((row) => (
                    <tr key={row.row_num}>
                      <td className="row-number">#{row.row_num}</td>
                      <td>
                        <span className={`status-pill status-pill--${getStatusClass(row.label)}`}>{row.label}</span>
                      </td>
                      {headers.map((header, index) => (
                        <td key={`${row.row_num}-${header}`}>{row.padded?.[index] || '—'}</td>
                      ))}
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={headers.length + 2} className="empty-state">
                      No stock rows matched the current filters.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <aside className="content-panel content-panel--side">
          <div className="panel-header">
            <h3>Available Breakdown</h3>
            <p>See which series are carrying the available stock count right now.</p>
          </div>

          <div className="meta-stack">
            <div className="meta-row">
              <span>Visible rows</span>
              <strong>{formatCount(rows.length)}</strong>
            </div>
            <div className="meta-row">
              <span>Last sync</span>
              <strong>{formatStamp(lastLoadedAt)}</strong>
            </div>
          </div>

          <div className="breakdown-list">
            {topBreakdown.length ? (
              topBreakdown.map((item) => (
                <article key={`${item.brand}-${item.series}`} className="breakdown-card">
                  <div>
                    <span className="breakdown-brand">{item.brand}</span>
                    <strong className="breakdown-series">{item.series}</strong>
                  </div>
                  <span className="breakdown-count">{formatCount(item.count)}</span>
                </article>
              ))
            ) : (
              <div className="notice compact">No available-series breakdown returned yet.</div>
            )}
          </div>
        </aside>
      </section>

      <section className="graph-panel">
        <div className="panel-header graph-header">
          <h3>Stock Distribution</h3>
          <p>Series concentration across the current visible inventory.</p>
        </div>

        <div className="bar-chart">
          {chartItems.slice(0, 7).map((item) => (
            <div key={`${item.brand}-${item.series}`} className="bar-column">
              <div className="bar-value">{formatCount(item.count)}</div>
              <div
                className="bar-fill"
                style={{ height: `${Math.max(14, (Number(item.count || 0) / maxChartCount) * 132)}px` }}
              />
              <div className="bar-label">{item.series}</div>
            </div>
          ))}
        </div>
      </section>
    </>
  );
}

function ClientsView({
  clientSearch,
  setClientSearch,
  clients,
  clientForm,
  setClientForm,
  clientBusy,
  onSelectClient,
  onSaveClient,
  onDeleteClient,
  onImportPhones,
  stats,
}) {
  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Client Directory</h3>
          <p>Keep the local WhatsApp registry clean, searchable, and synced into the shared sheet workflow.</p>
        </div>

        <div className="panel-toolbar">
          <div className="search-group">
            <label htmlFor="client-search">Search clients:</label>
            <input
              id="client-search"
              type="search"
              placeholder="Client name or number..."
              value={clientSearch}
              onChange={(event) => setClientSearch(event.target.value)}
            />
          </div>

          <div className="toolbar-actions">
            <button type="button" className="secondary-button" onClick={onImportPhones} disabled={clientBusy}>
              Import Sheet Phones
            </button>
          </div>
        </div>

        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>WhatsApp Number</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {clients.length ? (
                clients.map((entry) => (
                  <tr key={entry.name} onClick={() => onSelectClient(entry)}>
                    <td>{entry.name}</td>
                    <td>{entry.phone || '—'}</td>
                    <td>{entry.has_phone ? 'Saved' : 'Missing Number'}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={3} className="empty-state">
                    No clients matched the current filter.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <aside className="content-panel content-panel--side content-panel--stacked">
        <div className="subpanel">
          <div className="panel-header">
            <h3>Client Editor</h3>
            <p>Save a client with a WhatsApp number and sync it back into the sheet-backed workspace.</p>
          </div>

          <div className="form-stack">
            <label className="field-label" htmlFor="client-name">Client Name</label>
            <input
              id="client-name"
              type="text"
              value={clientForm.name}
              onChange={(event) => setClientForm((current) => ({ ...current, name: event.target.value }))}
              placeholder="Client name"
            />

            <label className="field-label" htmlFor="client-phone">WhatsApp Number</label>
            <input
              id="client-phone"
              type="text"
              inputMode="numeric"
              value={clientForm.phone}
              onChange={(event) => setClientForm((current) => ({ ...current, phone: normalizeDigits(event.target.value) }))}
              placeholder="2348168364881"
            />

            <div className="button-row">
              <button type="button" className="primary-button" onClick={onSaveClient} disabled={clientBusy}>
                {clientBusy ? 'Saving...' : 'Save Client'}
              </button>
              <button type="button" className="secondary-button" onClick={onDeleteClient} disabled={clientBusy || !clientForm.name}>
                Delete Client
              </button>
            </div>
          </div>
        </div>

        <div className="subpanel subpanel--muted">
          <div className="panel-header">
            <h3>Registry Stats</h3>
            <p>Quick health check for the numbers currently saved in the local registry.</p>
          </div>

          <div className="meta-stack">
            <div className="meta-row">
              <span>Total clients</span>
              <strong>{formatCount(stats?.total_count)}</strong>
            </div>
            <div className="meta-row">
              <span>With number</span>
              <strong>{formatCount(stats?.with_phone_count)}</strong>
            </div>
            <div className="meta-row">
              <span>Missing number</span>
              <strong>{formatCount(stats?.without_phone_count)}</strong>
            </div>
          </div>
        </div>
      </aside>
    </section>
  );
}

function NameFixView({
  mismatches,
  loading,
  selectedMismatch,
  correctName,
  setCorrectName,
  onSelectMismatch,
  onApplyFix,
  onApplyAll,
  onRescan,
  applying,
}) {
  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Name Fix Queue</h3>
          <p>Scan live records for likely misspellings, review suggestions, and apply them back through queued updates.</p>
        </div>

        <div className="panel-toolbar">
          <div className="toolbar-actions toolbar-actions--full">
            <button type="button" className="secondary-button" onClick={onRescan} disabled={loading || applying}>
              {loading ? 'Scanning...' : 'Rescan Sheet'}
            </button>
            <button type="button" className="primary-button" onClick={onApplyAll} disabled={loading || applying || !mismatches.length}>
              {applying ? 'Working...' : 'Fix All'}
            </button>
          </div>
        </div>

        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Sheet Name</th>
                <th>Rows</th>
                <th>Top Suggestion</th>
              </tr>
            </thead>
            <tbody>
              {mismatches.length ? (
                mismatches.map((entry) => (
                  <tr
                    key={entry.raw}
                    className={selectedMismatch?.raw === entry.raw ? 'table-row-selected' : ''}
                    onClick={() => onSelectMismatch(entry)}
                  >
                    <td>{entry.raw}</td>
                    <td>{formatCount(entry.rows?.length)}</td>
                    <td>{entry.candidates?.[0] || 'No suggestion'}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={3} className="empty-state">
                    {loading ? 'Scanning live rows for mismatches...' : 'No live name mismatches were found.'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <aside className="content-panel content-panel--side content-panel--stacked">
        <div className="subpanel">
          <div className="panel-header">
            <h3>Fix Selection</h3>
            <p>Choose the final replacement name before queueing the update back to the sheet.</p>
          </div>

          <div className="form-stack">
            <label className="field-label" htmlFor="bad-name">Selected Sheet Name</label>
            <input id="bad-name" value={selectedMismatch?.raw || ''} readOnly placeholder="Choose a row from the list" />

            <label className="field-label" htmlFor="correct-name">Replace With</label>
            <input
              id="correct-name"
              type="text"
              value={correctName}
              onChange={(event) => setCorrectName(event.target.value.toUpperCase())}
              placeholder="Correct customer name"
            />

            <div className="token-row">
              {(selectedMismatch?.candidates || []).map((candidate) => (
                <button key={candidate} type="button" className="token-button" onClick={() => setCorrectName(candidate)}>
                  {candidate}
                </button>
              ))}
            </div>

            <div className="meta-stack meta-stack--tight">
              <div className="meta-row">
                <span>Rows affected</span>
                <strong>{formatCount(selectedMismatch?.rows?.length)}</strong>
              </div>
              <div className="meta-row">
                <span>Suggestions</span>
                <strong>{formatCount(selectedMismatch?.candidates?.length)}</strong>
              </div>
            </div>

            <button type="button" className="primary-button button-wide" onClick={onApplyFix} disabled={!selectedMismatch || !correctName || applying}>
              {applying ? 'Applying Fix...' : 'Apply Fix'}
            </button>
          </div>
        </div>
      </aside>
    </section>
  );
}

function SettingsView({ syncStatus, syncBusy, onPullNow, onRefreshWorkspace, onReloadStatus }) {
  const syncState = syncStatus?.sync_state || {};
  const postgresSnapshot = syncStatus?.postgres_snapshot || {};
  const cacheCounts = postgresSnapshot?.cache_counts || {};

  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Runtime Status</h3>
          <p>See whether the backend is connected, how many rows are cached, and how much queue work is still pending.</p>
        </div>

        <div className="settings-grid">
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Postgres Ready</span>
            <strong className="metric-value">{postgresSnapshot?.ready ? 'Yes' : 'No'}</strong>
            <span className="metric-note">DB-first reads and queue writes</span>
          </article>
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Sheets Connected</span>
            <strong className="metric-value">{syncStatus?.sheets_connected ? 'Yes' : 'No'}</strong>
            <span className="metric-note">Google Sheets API reachability</span>
          </article>
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Queue Pending</span>
            <strong className="metric-value">{formatCount(syncStatus?.queue_pending)}</strong>
            <span className="metric-note">Operations waiting to replay</span>
          </article>
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Pull Interval</span>
            <strong className="metric-value">{formatCount(postgresSnapshot?.pull_interval_sec)}</strong>
            <span className="metric-note">Seconds between background pulls</span>
          </article>
        </div>

        <div className="table-wrap table-wrap--compact">
          <table className="data-table">
            <thead>
              <tr>
                <th>Cache</th>
                <th>Rows</th>
              </tr>
            </thead>
            <tbody>
              {Object.keys(cacheCounts).length ? (
                Object.entries(cacheCounts).map(([key, value]) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{formatCount(getCacheRowCount(value))}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={2} className="empty-state">No cache counts reported yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <aside className="content-panel content-panel--side content-panel--stacked">
        <div className="subpanel">
          <div className="panel-header">
            <h3>Sync Controls</h3>
            <p>Run a pull, or trigger the full refresh workflow that updates clients, validation, autofill, and cache.</p>
          </div>

          <div className="button-column">
            <button type="button" className="primary-button" onClick={onRefreshWorkspace} disabled={syncBusy}>
              {syncBusy ? 'Refreshing...' : 'Refresh Whole Workspace'}
            </button>
            <button type="button" className="secondary-button" onClick={onPullNow} disabled={syncBusy}>
              Pull Sheets Now
            </button>
            <button type="button" className="secondary-button" onClick={onReloadStatus} disabled={syncBusy}>
              Reload Status
            </button>
          </div>
        </div>

        <div className="subpanel subpanel--muted">
          <div className="panel-header">
            <h3>Backend Snapshot</h3>
            <p>Useful when checking whether the web app is actually using the same runtime flow as the desktop app.</p>
          </div>

          <div className="meta-stack">
            <div className="meta-row">
              <span>Runtime status</span>
              <strong>{syncState?.last_status || 'Unknown'}</strong>
            </div>
            <div className="meta-row">
              <span>Latest pull</span>
              <strong>{formatRuntimeSnapshot(postgresSnapshot?.latest_pull, 'Not yet recorded')}</strong>
            </div>
            <div className="meta-row">
              <span>Latest error</span>
              <strong>{formatRuntimeSnapshot(postgresSnapshot?.latest_error || syncState?.last_error, 'None')}</strong>
            </div>
          </div>
        </div>
      </aside>
    </section>
  );
}

function App() {
  const [activeView, setActiveView] = useState('debtors');
  const [statusText, setStatusText] = useState('Loading workspace...');
  const [workspaceError, setWorkspaceError] = useState('');
  const [lastLoadedAt, setLastLoadedAt] = useState(null);
  const [isCoreLoading, setIsCoreLoading] = useState(true);

  const [debtorsData, setDebtorsData] = useState({ sorted_debtors: [], total_debtors_amount: 0 });
  const [salesSnapshot, setSalesSnapshot] = useState({});
  const [debtorSearch, setDebtorSearch] = useState('');
  const deferredDebtorSearch = useDeferredValue(debtorSearch);
  const [selectedDebtor, setSelectedDebtor] = useState('');
  const [billText, setBillText] = useState('Select a debtor from the list to preview their bill here.');
  const [outstandingItems, setOutstandingItems] = useState([]);
  const [isDebtorDetailLoading, setIsDebtorDetailLoading] = useState(false);
  const [paymentAmount, setPaymentAmount] = useState('');
  const [selectedServiceRow, setSelectedServiceRow] = useState('automatic');
  const [paymentPlan, setPaymentPlan] = useState(null);
  const [paymentPlanError, setPaymentPlanError] = useState('');
  const [isApplyingPayment, setIsApplyingPayment] = useState(false);

  const [stockSearchText, setStockSearchText] = useState('');
  const [stockFilterMode, setStockFilterMode] = useState('all');
  const deferredStockSearchText = useDeferredValue(stockSearchText);
  const [stockView, setStockView] = useState(null);
  const [stockErrorText, setStockErrorText] = useState('');
  const [isStockLoading, setIsStockLoading] = useState(false);
  const [isStockRefreshing, setIsStockRefreshing] = useState(false);

  const [clientsData, setClientsData] = useState({ entries: [], registry: {}, stats: {} });
  const [clientSearch, setClientSearch] = useState('');
  const deferredClientSearch = useDeferredValue(clientSearch);
  const [clientForm, setClientForm] = useState({ name: '', phone: '' });
  const [isClientBusy, setIsClientBusy] = useState(false);

  const [nameFixData, setNameFixData] = useState({ mismatches: [], count: 0 });
  const [selectedMismatchRaw, setSelectedMismatchRaw] = useState('');
  const [correctName, setCorrectName] = useState('');
  const [isNameFixLoading, setIsNameFixLoading] = useState(false);
  const [isNameFixApplying, setIsNameFixApplying] = useState(false);

  const [syncStatus, setSyncStatus] = useState(null);
  const [isSyncBusy, setIsSyncBusy] = useState(false);

  const filteredDebtors = (debtorsData.sorted_debtors || []).filter(([name]) => {
    const query = normalizeSearchValue(deferredDebtorSearch);
    if (!query) {
      return true;
    }
    return normalizeSearchValue(name).includes(query);
  });

  const filteredClients = (clientsData.entries || []).filter((entry) => {
    const query = normalizeSearchValue(deferredClientSearch);
    if (!query) {
      return true;
    }
    return normalizeSearchValue(entry.name).includes(query) || normalizeSearchValue(entry.phone).includes(query);
  });

  const selectedMismatch = (nameFixData.mismatches || []).find(
    (entry) => normalizeSearchValue(entry.raw) === normalizeSearchValue(selectedMismatchRaw)
  );

  const activeMeta = VIEW_META[activeView] || VIEW_META.debtors;
  const summaryCards = [
    {
      label: 'Customers Owing',
      value: formatCount((debtorsData.sorted_debtors || []).length),
      note: 'Live debtor list',
    },
    {
      label: 'Total Outstanding',
      value: formatCurrency(debtorsData.total_debtors_amount),
      note: 'Across all debtors',
    },
    {
      label: 'Sales Today',
      value: formatCurrency(salesSnapshot.sales_today),
      note: `${formatCount(salesSnapshot.services_today)} services`,
    },
    {
      label: 'Clients With Phone',
      value: formatCount(clientsData.stats?.with_phone_count),
      note: `${formatCount(clientsData.stats?.total_count)} total clients`,
    },
    {
      label: 'Name Fixes',
      value: formatCount(nameFixData.count),
      note: 'Current mismatch scan',
    },
    {
      label: 'Queue Pending',
      value: formatCount(syncStatus?.queue_pending),
      note: 'Background sheet writes',
    },
    {
      label: 'Last Loaded',
      value: formatStamp(lastLoadedAt),
      note: syncStatus?.sync_state?.last_status || 'Live runtime',
    },
  ];

  async function loadCoreWorkspace(forceRefresh = false) {
    setIsCoreLoading(true);
    setWorkspaceError('');
    setStatusText(forceRefresh ? 'Refreshing workspace...' : 'Loading workspace...');

    const [debtorsResult, salesResult, clientsResult, nameFixResult, syncResult] = await Promise.allSettled([
      fetchLiveDebtors({ forceRefresh }),
      fetchLiveSalesSnapshot({ forceRefresh }),
      fetchClients({ forceReload: true }),
      fetchNameFixes({ forceRefresh }),
      fetchSyncStatus(),
    ]);

    const failures = [];

    if (debtorsResult.status === 'fulfilled') {
      const nextDebtors = debtorsResult.value;
      setDebtorsData(nextDebtors);
      const availableNames = (nextDebtors.sorted_debtors || []).map(([name]) => name);
      const nextSelected = availableNames.includes(selectedDebtor) ? selectedDebtor : availableNames[0] || '';
      startTransition(() => setSelectedDebtor(nextSelected));
    } else {
      failures.push(debtorsResult.reason?.message || 'Could not load debtors');
    }

    if (salesResult.status === 'fulfilled') {
      setSalesSnapshot(salesResult.value);
    } else {
      failures.push(salesResult.reason?.message || 'Could not load sales snapshot');
    }

    if (clientsResult.status === 'fulfilled') {
      setClientsData(clientsResult.value);
    } else {
      failures.push(clientsResult.reason?.message || 'Could not load clients');
    }

    if (nameFixResult.status === 'fulfilled') {
      setNameFixData(nameFixResult.value);
      const availableMismatch = (nameFixResult.value.mismatches || []).some(
        (entry) => normalizeSearchValue(entry.raw) === normalizeSearchValue(selectedMismatchRaw)
      );
      if (!availableMismatch) {
        const first = nameFixResult.value.mismatches?.[0];
        setSelectedMismatchRaw(first?.raw || '');
        setCorrectName(first?.candidates?.[0] || '');
      }
    } else {
      failures.push(nameFixResult.reason?.message || 'Could not load name-fix data');
    }

    if (syncResult.status === 'fulfilled') {
      setSyncStatus(syncResult.value);
    } else {
      failures.push(syncResult.reason?.message || 'Could not load sync status');
    }

    setLastLoadedAt(new Date());
    setWorkspaceError(failures.join(' | '));
    setStatusText(failures.length ? `Partial load: ${failures[0]}` : forceRefresh ? 'Workspace refreshed.' : 'Ready');
    setIsCoreLoading(false);
  }

  async function loadStock(forceRefresh = false) {
    setStockErrorText('');
    if (stockView) {
      setIsStockRefreshing(true);
    } else {
      setIsStockLoading(true);
    }

    try {
      const result = await fetchStockDashboard({
        filterText: deferredStockSearchText,
        filterMode: stockFilterMode,
        forceRefresh,
      });
      setStockView(result);
      setLastLoadedAt(new Date());
    } catch (error) {
      setStockErrorText(error.message || 'Could not load stock dashboard.');
      setStatusText(error.message || 'Could not load stock dashboard.');
    } finally {
      setIsStockLoading(false);
      setIsStockRefreshing(false);
    }
  }

  async function loadClientsOnly(forceReload = true) {
    const result = await fetchClients({ forceReload });
    setClientsData(result);
    return result;
  }

  async function loadNameFixesOnly(forceRefresh = false) {
    setIsNameFixLoading(true);
    try {
      const result = await fetchNameFixes({ forceRefresh });
      setNameFixData(result);
      const first = result.mismatches?.[0];
      if (first && !result.mismatches.some((entry) => normalizeSearchValue(entry.raw) === normalizeSearchValue(selectedMismatchRaw))) {
        setSelectedMismatchRaw(first.raw);
        setCorrectName(first.candidates?.[0] || '');
      }
      if (!result.mismatches?.length) {
        setSelectedMismatchRaw('');
        setCorrectName('');
      }
      return result;
    } finally {
      setIsNameFixLoading(false);
    }
  }

  async function loadSyncStatusOnly() {
    setIsSyncBusy(true);
    try {
      const result = await fetchSyncStatus();
      setSyncStatus(result);
      return result;
    } finally {
      setIsSyncBusy(false);
    }
  }

  async function loadSelectedDebtorDetails(name, forceRefresh = false) {
    if (!name) {
      setBillText('Select a debtor from the list to preview their bill here.');
      setOutstandingItems([]);
      return;
    }

    setIsDebtorDetailLoading(true);
    try {
      const [billResult, itemsResult] = await Promise.all([
        fetchLiveBill(name, { forceRefresh }),
        fetchOutstandingItems(name, { forceRefresh }),
      ]);
      setBillText(billResult.bill_text || 'No outstanding bill for this customer.');
      setOutstandingItems(itemsResult.outstanding_items || []);
      setSelectedServiceRow((current) => {
        if (current === 'automatic') {
          return current;
        }
        return (itemsResult.outstanding_items || []).some((item) => String(item.row_idx) === String(current)) ? current : 'automatic';
      });
    } catch (error) {
      setBillText(error.message || 'Unable to load bill preview.');
      setOutstandingItems([]);
      setStatusText(error.message || 'Unable to load bill preview.');
    } finally {
      setIsDebtorDetailLoading(false);
    }
  }

  useEffect(() => {
    loadCoreWorkspace(false);
  }, []);

  useEffect(() => {
    const delay = window.setTimeout(() => {
      loadSelectedDebtorDetails(selectedDebtor, false);
    }, 120);

    return () => {
      window.clearTimeout(delay);
    };
  }, [selectedDebtor]);

  useEffect(() => {
    if (activeView !== 'stock') {
      return undefined;
    }

    const delay = window.setTimeout(() => {
      loadStock(false);
    }, 180);

    return () => {
      window.clearTimeout(delay);
    };
  }, [activeView, deferredStockSearchText, stockFilterMode]);

  useEffect(() => {
    const parsedAmount = Number(normalizeDigits(paymentAmount));
    const manualServiceRowIdx = selectedServiceRow === 'automatic' ? null : Number(selectedServiceRow);

    if (!selectedDebtor || !paymentAmount.trim()) {
      setPaymentPlan(null);
      setPaymentPlanError('');
      return undefined;
    }

    if (!Number.isFinite(parsedAmount) || parsedAmount <= 0) {
      setPaymentPlan(null);
      setPaymentPlanError('Enter a valid payment amount.');
      return undefined;
    }

    const abortController = new AbortController();
    const delay = window.setTimeout(async () => {
      try {
        const result = await fetchPaymentPlan({
          nameInput: selectedDebtor,
          paymentAmount: parsedAmount,
          manualServiceRowIdx,
          signal: abortController.signal,
        });
        setPaymentPlan(result);
        setPaymentPlanError('');
      } catch (error) {
        if (error.name !== 'AbortError') {
          setPaymentPlan(null);
          setPaymentPlanError(error.message || 'Could not prepare payment preview.');
        }
      }
    }, 220);

    return () => {
      abortController.abort();
      window.clearTimeout(delay);
    };
  }, [selectedDebtor, paymentAmount, selectedServiceRow]);

  function handleSelectDebtor(name) {
    startTransition(() => setSelectedDebtor(name));
    setPaymentAmount('');
    setPaymentPlan(null);
    setPaymentPlanError('');
    setSelectedServiceRow('automatic');
  }

  async function handleCopyBill() {
    try {
      await copyText(billText);
      setStatusText(`Bill copied for ${selectedDebtor}.`);
    } catch (error) {
      setStatusText(error.message || 'Could not copy the bill text.');
    }
  }

  async function handleSendWhatsapp() {
    const phone = normalizeDigits(clientsData.registry?.[selectedDebtor] || '');
    if (!selectedDebtor) {
      setStatusText('Select a debtor first.');
      return;
    }
    if (!phone) {
      startTransition(() => setActiveView('clients'));
      setClientForm({ name: selectedDebtor, phone: '' });
      setStatusText(`No WhatsApp number saved for ${selectedDebtor}. Add it in Clients.`);
      return;
    }
    const url = `https://wa.me/${phone}?text=${encodeURIComponent(billText)}`;
    window.open(url, '_blank', 'noopener,noreferrer');
    setStatusText(`Opened WhatsApp for ${selectedDebtor}.`);
  }

  async function handleApplyPayment() {
    const parsedAmount = Number(normalizeDigits(paymentAmount));
    if (!selectedDebtor) {
      setStatusText('Select a debtor first.');
      return;
    }
    if (!Number.isFinite(parsedAmount) || parsedAmount <= 0) {
      setStatusText('Enter a valid payment amount.');
      return;
    }

    setIsApplyingPayment(true);
    try {
      const result = await applyPayment({
        nameInput: selectedDebtor,
        paymentAmount: parsedAmount,
        manualServiceRowIdx: selectedServiceRow === 'automatic' ? null : Number(selectedServiceRow),
        forceRefresh: false,
      });
      setStatusText(result.status_text || 'Payment applied.');
      setPaymentAmount('');
      setPaymentPlan(null);
      await loadCoreWorkspace(false);
      await loadSelectedDebtorDetails(selectedDebtor, false);
    } catch (error) {
      setStatusText(error.message || 'Could not apply payment.');
    } finally {
      setIsApplyingPayment(false);
    }
  }

  async function handleSaveClient() {
    setIsClientBusy(true);
    try {
      const result = await upsertClient({
        name: clientForm.name,
        phone: clientForm.phone,
        syncSheet: true,
        forceRefresh: true,
      });
      setStatusText(result.added ? 'Client added and synced.' : 'Client updated and synced.');
      await loadCoreWorkspace(false);
      await loadClientsOnly(true);
    } catch (error) {
      setStatusText(error.message || 'Could not save client.');
    } finally {
      setIsClientBusy(false);
    }
  }

  async function handleDeleteClient() {
    if (!clientForm.name || !window.confirm(`Delete ${clientForm.name} from the registry?`)) {
      return;
    }

    setIsClientBusy(true);
    try {
      await deleteClient({ name: clientForm.name, syncSheet: true });
      setClientForm({ name: '', phone: '' });
      setStatusText('Client deleted and registry synced.');
      await loadCoreWorkspace(false);
      await loadClientsOnly(true);
    } catch (error) {
      setStatusText(error.message || 'Could not delete client.');
    } finally {
      setIsClientBusy(false);
    }
  }

  async function handleImportSheetPhones() {
    setIsClientBusy(true);
    try {
      const result = await importSheetPhones({ forceRefresh: true });
      setStatusText(`Imported phones: ${result.added} added, ${result.updated} updated.`);
      await loadCoreWorkspace(false);
      await loadClientsOnly(true);
    } catch (error) {
      setStatusText(error.message || 'Could not import phone numbers from the sheet.');
    } finally {
      setIsClientBusy(false);
    }
  }

  async function handleApplySingleFix() {
    if (!selectedMismatch || !correctName) {
      setStatusText('Select a mismatch and choose the replacement name.');
      return;
    }

    setIsNameFixApplying(true);
    try {
      const result = await applyNameFix({
        mismatchEntry: selectedMismatch,
        correctName,
        forceRefresh: false,
      });
      setStatusText(`Queued ${result.updated_count} row(s) for name correction.`);
      await loadCoreWorkspace(false);
      await loadNameFixesOnly(false);
      await loadSelectedDebtorDetails(selectedDebtor, false);
    } catch (error) {
      setStatusText(error.message || 'Could not apply the selected name fix.');
    } finally {
      setIsNameFixApplying(false);
    }
  }

  async function handleApplyAllFixes() {
    if (!nameFixData.mismatches?.length) {
      setStatusText('No mismatches are currently loaded.');
      return;
    }

    setIsNameFixApplying(true);
    try {
      const result = await applyAllNameFixes({
        mismatchEntries: nameFixData.mismatches,
        forceRefresh: false,
      });
      setStatusText(`Queued ${result.updated_count} row(s) for automatic name fixes.`);
      await loadCoreWorkspace(false);
      await loadNameFixesOnly(false);
      await loadSelectedDebtorDetails(selectedDebtor, false);
    } catch (error) {
      setStatusText(error.message || 'Could not apply automatic fixes.');
    } finally {
      setIsNameFixApplying(false);
    }
  }

  async function handleRescanFixes() {
    try {
      setStatusText('Scanning live rows for mismatches...');
      await loadNameFixesOnly(true);
      setStatusText('Name-fix scan completed.');
    } catch (error) {
      setStatusText(error.message || 'Could not rescan live rows.');
    }
  }

  async function handlePullNow() {
    setIsSyncBusy(true);
    try {
      await pullNow();
      await loadCoreWorkspace(true);
      if (activeView === 'stock') {
        await loadStock(true);
      }
      setStatusText('Manual sheet pull completed.');
    } catch (error) {
      setStatusText(error.message || 'Could not run a manual pull.');
    } finally {
      setIsSyncBusy(false);
    }
  }

  async function handleWorkspaceRefresh() {
    setIsSyncBusy(true);
    try {
      await refreshWorkspace({ forceRefresh: true });
      await loadCoreWorkspace(true);
      if (activeView === 'stock' || stockView) {
        await loadStock(true);
      }
      await loadSelectedDebtorDetails(selectedDebtor, false);
      setStatusText('Full workspace refresh completed.');
    } catch (error) {
      setStatusText(error.message || 'Could not refresh the full workspace.');
    } finally {
      setIsSyncBusy(false);
    }
  }

  async function handleActionClick(tile) {
    if (tile.view) {
      startTransition(() => setActiveView(tile.view));
      setStatusText(VIEW_META[tile.view]?.title || 'Ready');
      if (tile.view === 'clients') {
        try {
          await loadClientsOnly(true);
        } catch (error) {
          setStatusText(error.message || 'Could not load clients.');
        }
      }
      if (tile.view === 'fix') {
        try {
          await loadNameFixesOnly(false);
        } catch (error) {
          setStatusText(error.message || 'Could not load name-fix data.');
        }
      }
      if (tile.view === 'settings') {
        try {
          await loadSyncStatusOnly();
        } catch (error) {
          setStatusText(error.message || 'Could not load sync status.');
        }
      }
      if (tile.view === 'stock') {
        await loadStock(false);
      }
      return;
    }

    if (tile.action === 'refresh') {
      await handleWorkspaceRefresh();
      return;
    }

    if (tile.action === 'exit') {
      window.close();
      setStatusText('Close request sent to the browser window.');
      return;
    }

    setStatusText(`${tile.label} is still desktop-only for now.`);
  }

  function renderActiveView() {
    if (activeView === 'stock') {
      return (
        <StockViewSection
          stockSearchText={stockSearchText}
          setStockSearchText={setStockSearchText}
          filterMode={stockFilterMode}
          setFilterMode={setStockFilterMode}
          stockView={stockView}
          isLoading={isStockLoading}
          isRefreshing={isStockRefreshing}
          errorText={stockErrorText}
          onRefresh={() => loadStock(true)}
          lastLoadedAt={lastLoadedAt}
        />
      );
    }

    if (activeView === 'clients') {
      return (
        <ClientsView
          clientSearch={clientSearch}
          setClientSearch={setClientSearch}
          clients={filteredClients}
          clientForm={clientForm}
          setClientForm={setClientForm}
          clientBusy={isClientBusy}
          onSelectClient={(entry) => setClientForm({ name: entry.name, phone: entry.phone || '' })}
          onSaveClient={handleSaveClient}
          onDeleteClient={handleDeleteClient}
          onImportPhones={handleImportSheetPhones}
          stats={clientsData.stats}
        />
      );
    }

    if (activeView === 'fix') {
      return (
        <NameFixView
          mismatches={nameFixData.mismatches || []}
          loading={isNameFixLoading}
          selectedMismatch={selectedMismatch}
          correctName={correctName}
          setCorrectName={setCorrectName}
          onSelectMismatch={(entry) => {
            setSelectedMismatchRaw(entry.raw);
            setCorrectName(entry.candidates?.[0] || '');
          }}
          onApplyFix={handleApplySingleFix}
          onApplyAll={handleApplyAllFixes}
          onRescan={handleRescanFixes}
          applying={isNameFixApplying}
        />
      );
    }

    if (activeView === 'settings') {
      return (
        <SettingsView
          syncStatus={syncStatus}
          syncBusy={isSyncBusy}
          onPullNow={handlePullNow}
          onRefreshWorkspace={handleWorkspaceRefresh}
          onReloadStatus={loadSyncStatusOnly}
        />
      );
    }

    return (
      <BillingView
        debtorSearch={debtorSearch}
        setDebtorSearch={setDebtorSearch}
        debtors={filteredDebtors}
        selectedDebtor={selectedDebtor}
        onSelectDebtor={handleSelectDebtor}
        isLoading={isCoreLoading}
        detailLoading={isDebtorDetailLoading}
        billText={billText}
        outstandingItems={outstandingItems}
        paymentAmount={paymentAmount}
        setPaymentAmount={setPaymentAmount}
        selectedServiceRow={selectedServiceRow}
        setSelectedServiceRow={setSelectedServiceRow}
        paymentPlan={paymentPlan}
        paymentPlanError={paymentPlanError}
        applyingPayment={isApplyingPayment}
        onCopyBill={handleCopyBill}
        onSendWhatsapp={handleSendWhatsapp}
        onApplyPayment={handleApplyPayment}
      />
    );
  }

  return (
    <div className="dashboard-shell">
      <main className="page">
        <section className="hero-frame">
          <div className="hero-left">
            <h1>Atlanta Georgia_Tech</h1>
            <p>Billing, stock, client registry, and sheet sync in one web workspace.</p>

            <div className="status-chip">
              <span className="status-chip-label">Status</span>
              <strong>{statusText}</strong>
            </div>
          </div>

          <div className="hero-right">
            <div className="hero-note">
              <span className="hero-note-label">Page</span>
              <strong>{activeMeta.title}</strong>
            </div>
            <div className="hero-note">
              <span className="hero-note-label">API Source</span>
              <strong>{getApiLabel()}</strong>
            </div>
          </div>
        </section>

        <section className="dashboard-body">
          <aside className="sidebar-frame">
            <h2>General Actions</h2>
            <p>Now wired to live sections and runtime actions.</p>

            <div className="action-grid">
              {ACTION_TILES.map((tile) => (
                <button
                  key={tile.label}
                  type="button"
                  className={[
                    'action-tile',
                    tile.view === activeView ? 'action-tile--current' : '',
                    tile.disabled ? 'action-tile--disabled' : '',
                  ].join(' ').trim()}
                  style={{ backgroundColor: tile.color }}
                  onClick={() => handleActionClick(tile)}
                  disabled={tile.disabled}
                >
                  <span className="action-icon">{tile.icon}</span>
                  <span className="action-label">{tile.label}</span>
                </button>
              ))}
            </div>
          </aside>

          <section className="workspace-frame">
            <section className="summary-frame">
              <h2>Live Summary</h2>

              <div className="summary-grid">
                {summaryCards.map((card) => (
                  <article key={card.label} className="metric-card">
                    <span className="metric-label">{card.label}</span>
                    <strong className="metric-value">{card.value}</strong>
                    <span className="metric-note">{card.note}</span>
                  </article>
                ))}
              </div>
            </section>

            <section className="content-panel content-panel--headline">
              <div className="panel-header">
                <h3>{activeMeta.title}</h3>
                <p>{activeMeta.description}</p>
              </div>
              {workspaceError ? <div className="notice notice-error notice-inline">{workspaceError}</div> : null}
            </section>

            {renderActiveView()}
          </section>
        </section>
      </main>
    </div>
  );
}

export default App;
