import React, { startTransition, useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';

import { getApiLabel } from './api/http';
import { addServiceRecord, checkoutSaleCart, fetchPendingServiceDeals, fetchStockDashboard, returnServiceDeal, returnStockItem, updatePendingDealMeta, updatePendingDealPayment, updatePendingServiceMeta, updateServiceDealPayment, updateStockRow } from './api/stock';
import { createUser, fetchUsers, updateUser } from './api/users';
import {
  addStockRecord,
  applyAllNameFixes,
  applyNameFix,
  applyPayment,
  createFoundationExpense,
  createStolenDevice,
  deleteClient,
  fetchDashboardLogo,
  fetchClients,
  fetchFoundationCashflowDashboard,
  fetchGoogleContacts,
  fetchHomeBootstrap,
  fetchLiveBill,
  fetchNameFixes,
  fetchOutstandingItems,
  fetchStolenDevices,
  fetchPaymentPlan,
  fetchServicesToday,
  searchServices,
  fetchStockForm,
  fetchSyncStatus,
  fetchUnpaidToday,
  fetchUnpaidTodayBills,
  fetchWhatsappHistory,
  importSheetPhones,
  markWhatsappSent,
  pullNow,
  redoPayment,
  refreshWorkspace,
  checkStolenDeviceImei,
  syncGoogleContacts,
  undoPayment,
  undoLastWeeklyAllowanceWithdrawal,
  updateStolenDevice,
  updateSalesTodayPayment,
  returnDebtorService,
  updateDebtorService,
  upsertClient,
} from './api/workspace';

const PRODUCT_FILTERS = [
  { value: 'all', label: 'All Products' },
  { value: 'available', label: 'Available' },
  { value: 'needs_review', label: 'Needs Status Review' },
  { value: 'pending', label: 'Pending Deal' },
  { value: 'needs_details', label: 'Needs Details' },
  { value: 'sold', label: 'Sold' },
];

const CART_FILTERS = [
  { value: 'available', label: 'Available Products' },
  { value: 'pending', label: 'Phone Pending Deal (Client Yet To Pay)' },
  { value: 'sold', label: 'Sold Items' },
];

const PRODUCT_SUMMARY_COLUMNS = [
  { key: 'description', label: 'Description', aliases: ['DESCRIPTION', 'MODEL', 'DEVICE'] },
  { key: 'colour', label: 'Colour', aliases: ['COLOUR', 'COLOR'] },
  { key: 'storage', label: 'Storage', aliases: ['STORAGE'] },
  { key: 'imei', label: 'IMEI', aliases: ['IMEI'] },
  { key: 'cost_price', label: 'Cost Price', aliases: ['COST PRICE'] },
  { key: 'selling_price', label: 'Selling Price', aliases: ['AMOUNT SOLD', 'SELLING PRICE', 'PRICE'] },
  { key: 'seller', label: 'Seller', aliases: ['NAME OF SELLER'] },
  { key: 'buyer', label: 'Buyer', aliases: ['NAME OF BUYER'] },
];

const ACTION_ITEMS = [
  {
    key: 'refresh',
    type: 'action',
    title: 'Refresh Workspace',
    description: 'Pull the latest sheet changes and rebuild the local cache.',
  },
  {
    key: 'home',
    type: 'view',
    title: 'Home Page',
    description: 'See live summary cards, statistics, and graphs in one place.',
  },
  {
    key: 'products',
    type: 'view',
    title: 'Products',
    description: 'Browse stock, filter products, and add new items to the sheet.',
  },
  {
    key: 'cart',
    type: 'view',
    title: 'Cart',
    description: 'Select stock items, add buyer details, and sell phones into inventory.',
  },
  {
    key: 'undo',
    type: 'action',
    title: 'Undo',
    description: 'Undo the most recent payment action performed from the website.',
  },
  {
    key: 'redo',
    type: 'action',
    title: 'Redo',
    description: 'Reapply the most recently undone payment action.',
  },
  {
    key: 'import_phones',
    type: 'action',
    title: 'Import Spreadsheet Phones',
    description: 'Import phone numbers from the spreadsheet to speed up autofill in forms.',
  },
  {
    key: 'clients',
    type: 'view',
    title: 'Clients',
    description: 'Manage client phone numbers and work with Google Contacts.',
  },
  {
    key: 'debtors',
    type: 'view',
    title: 'Debtors',
    description: 'Review balances, preview bills, and apply payments.',
  },
  {
    key: 'bill_notifications',
    type: 'action',
    title: 'Bill Notifications',
    description: 'Customers with unpaid balances and no bill sent for more than 4 days.',
  },
  {
    key: 'fix',
    type: 'view',
    title: 'Fix',
    description: 'Scan mismatched names and apply sheet-safe corrections.',
  },
  {
    key: 'settings',
    type: 'view',
    title: 'Settings',
    description: 'Inspect sync status, cache counts, and runtime health.',
  },
  {
    key: 'stolen_devices',
    type: 'view',
    title: 'Stolen Devices',
    description: 'Register flagged IMEIs and block risky stock additions.',
  },
  {
    key: 'users',
    type: 'view',
    title: 'User Management',
    description: 'Create users, assign roles, and control account status.',
  },
  {
    key: 'exit',
    type: 'action',
    title: 'Exit',
    description: 'Close the browser tab for this workspace.',
  },
  {
    key: 'logout',
    type: 'action',
    title: 'Log Out',
    description: 'Sign out of your account and return to the login page.',
  },
];

const VIEW_META = {
  home: {
    title: 'Home Page',
    description: 'Live summary, business statistics, and the graphs you need at a glance.',
  },
  products: {
    title: 'Products',
    description: 'Manage stock records, browse inventory, and add new products to the Google Sheet.',
  },
  cart: {
    title: 'Cart',
    description: 'Sell phones from stock, set buyer details, and append clean inventory rows without overwriting anything.',
  },
  cashflow: {
    title: 'Cash Flow',
    description: 'Simple cash movement view based on live sales and outstanding balances.',
  },
  clients: {
    title: 'Clients',
    description: 'Maintain client phone details, sync Google Contacts, and update customer records.',
  },
  debtors: {
    title: 'Debtors',
    description: 'Preview bills, copy or send them, and apply payments with live data.',
  },
  fix: {
    title: 'Fix',
    description: 'Review likely misspellings and queue safe name corrections back to the sheet.',
  },
  settings: {
    title: 'Settings',
    description: 'Watch the sync runtime, queue state, and cache health.',
  },
  stolen_devices: {
    title: 'Stolen Devices',
    description: 'Admin-only IMEI registry for stolen devices and add-product screening.',
  },
  services_today: {
    title: 'Services Today',
    description: 'All services recorded today with status, amount paid, and balances.',
  },
  bill_notifications: {
    title: 'Bill Notifications',
    description: 'Overdue unpaid customers with bill sends older than 4 days.',
  },
  users: {
    title: 'User Management',
    description: 'Admin-only user provisioning and role/status control.',
  },
};

const STAFF_ALLOWED_VIEWS = new Set(['products', 'cart', 'bill_notifications']);
const STOCK_VIEW_CACHE_KEY = 'atlanta_stock_view_cache_v1';
const STOCK_FORM_CACHE_KEY = 'atlanta_stock_form_cache_v1';
const WORKSPACE_CORE_CACHE_KEY = 'atlanta_workspace_core_cache_v1';

const STATUS_CLASS_MAP = {
  AVAILABLE: 'available',
  'PENDING DEAL': 'pending',
  'NEEDS DETAILS': 'needs-details',
  SOLD: 'sold',
  'NEEDS STATUS REVIEW': 'needs-details',
};

const numberFormatter = new Intl.NumberFormat('en-US');

function formatCount(value) {
  return numberFormatter.format(Number(value || 0));
}

function formatCurrency(value) {
  return `NGN ${formatCount(value)}`;
}

function formatShortStamp(dateValue) {
  if (!dateValue) {
    return 'Waiting';
  }

  const parsedDate = dateValue instanceof Date ? dateValue : new Date(dateValue);
  if (Number.isNaN(parsedDate.getTime())) {
    return 'Waiting';
  }

  return new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
  }).format(parsedDate);
}

function useRenderTiming(label, detailValue) {
  const startedAt = performance.now();

  useEffect(() => {
    const durationMs = performance.now() - startedAt;
    if (durationMs > 12) {
      console.info(`[render-timing] ${label} ${Math.round(durationMs)}ms detail=${detailValue}`);
    }
  }, [label, detailValue, startedAt]);
}

function useDebouncedValue(value, delayMs = 120) {
  const [debouncedValue, setDebouncedValue] = useState(value);

  useEffect(() => {
    const timeoutId = window.setTimeout(() => setDebouncedValue(value), Math.max(0, Number(delayMs) || 0));
    return () => window.clearTimeout(timeoutId);
  }, [value, delayMs]);

  return debouncedValue;
}

function getStockFilterDiagnosticsStore() {
  if (typeof window === 'undefined') {
    return null;
  }
  if (!window.__ATLANTA_STOCK_FILTER_DIAGNOSTICS__) {
    window.__ATLANTA_STOCK_FILTER_DIAGNOSTICS__ = {
      events: [],
      staleDrops: 0,
    };
  }
  return window.__ATLANTA_STOCK_FILTER_DIAGNOSTICS__;
}

function recordStockFilterDiagnostic(event) {
  const store = getStockFilterDiagnosticsStore();
  const payload = {
    ...event,
    at: new Date().toISOString(),
  };

  if (store) {
    store.events = [payload, ...(store.events || [])].slice(0, 120);
    if (payload.outcome === 'stale_drop') {
      store.staleDrops = Number(store.staleDrops || 0) + 1;
    }
  }

  if (payload.outcome === 'stale_drop' || payload.durationMs > 250) {
    console.info(
      `[stock-filter] mode=${payload.filterMode} outcome=${payload.outcome} duration=${Math.round(payload.durationMs || 0)}ms staleDrops=${payload.staleDrops || 0}`
    );
  }
}

function useIsCompactViewport(maxWidth = 768) {
  const [isCompact, setIsCompact] = useState(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
      return false;
    }
    return window.matchMedia(`(max-width: ${maxWidth}px)`).matches;
  });

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
      return undefined;
    }
    const mediaQuery = window.matchMedia(`(max-width: ${maxWidth}px)`);
    const handleChange = (event) => setIsCompact(Boolean(event.matches));
    handleChange(mediaQuery);
    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, [maxWidth]);

  return isCompact;
}

function useWindowedRows(items, { containerRef, enabled, rowHeight = 58, overscan = 6 } = {}) {
  const [scrollState, setScrollState] = useState({ scrollTop: 0, height: 0 });

  useEffect(() => {
    if (!enabled || !containerRef.current) {
      return undefined;
    }

    const node = containerRef.current;
    const updateState = () => {
      setScrollState({
        scrollTop: node.scrollTop,
        height: node.clientHeight,
      });
    };

    updateState();
    node.addEventListener('scroll', updateState, { passive: true });
    window.addEventListener('resize', updateState);
    return () => {
      node.removeEventListener('scroll', updateState);
      window.removeEventListener('resize', updateState);
    };
  }, [containerRef, enabled]);

  if (!enabled) {
    return {
      visibleItems: items,
      topSpacerHeight: 0,
      bottomSpacerHeight: 0,
    };
  }

  const totalItems = items.length;
  const viewportCount = Math.max(1, Math.ceil((scrollState.height || rowHeight * 8) / rowHeight));
  const startIndex = Math.max(0, Math.floor(scrollState.scrollTop / rowHeight) - overscan);
  const endIndex = Math.min(totalItems, startIndex + viewportCount + overscan * 2);
  const topSpacerHeight = startIndex * rowHeight;
  const bottomSpacerHeight = Math.max(0, (totalItems - endIndex) * rowHeight);

  return {
    visibleItems: items.slice(startIndex, endIndex),
    topSpacerHeight,
    bottomSpacerHeight,
  };
}

function formatDateForInput(dateValue = new Date()) {
  const year = dateValue.getFullYear();
  const month = String(dateValue.getMonth() + 1).padStart(2, '0');
  const day = String(dateValue.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
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

function readSessionCache(key) {
  try {
    const cachedText = sessionStorage.getItem(key);
    if (!cachedText) {
      return null;
    }
    return JSON.parse(cachedText);
  } catch {
    return null;
  }
}

function writeSessionCache(key, value) {
  try {
    sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Ignore cache storage failures.
  }
}

function normalizeSearchValue(value) {
  return String(value || '').trim().toUpperCase();
}

function normalizeDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function parseAmountLike(value) {
  const digits = normalizeDigits(value);
  if (!digits) {
    return 0;
  }
  return Number(digits) || 0;
}

function normalizeWhatsappPhone(value) {
  let digits = normalizeDigits(value);
  if (!digits) {
    return '';
  }

  if (digits.startsWith('00')) {
    digits = digits.slice(2);
  }

  if (digits.startsWith('2340')) {
    digits = `234${digits.slice(4)}`;
  }

  if (digits.startsWith('234') && digits.length > 13 && digits[3] === '0') {
    digits = `234${digits.slice(4)}`;
  }

  if (digits.startsWith('0') && digits.length === 11) {
    return `234${digits.slice(1)}`;
  }

  if (digits.length === 10) {
    return `234${digits}`;
  }

  return digits;
}

function extractPhoneFromSuggestionText(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    return '';
  }

  const pieces = raw.split(' - ');
  const candidate = pieces.length > 1 ? pieces[pieces.length - 1] : raw;
  const normalized = normalizeWhatsappPhone(candidate);
  return normalized || raw;
}

function buildPageItems(page, totalPages) {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, index) => index + 1);
  }

  const pageNumbers = new Set([1, totalPages, page - 1, page, page + 1]);

  if (page <= 3) {
    pageNumbers.add(2);
    pageNumbers.add(3);
    pageNumbers.add(4);
  }

  if (page >= totalPages - 2) {
    pageNumbers.add(totalPages - 1);
    pageNumbers.add(totalPages - 2);
    pageNumbers.add(totalPages - 3);
  }

  const sortedPages = Array.from(pageNumbers)
    .filter((pageNumber) => pageNumber >= 1 && pageNumber <= totalPages)
    .sort((left, right) => left - right);

  const items = [];
  sortedPages.forEach((pageNumber, index) => {
    const previous = sortedPages[index - 1];
    if (previous && pageNumber - previous > 1) {
      items.push(`ellipsis-${previous}-${pageNumber}`);
    }
    items.push(pageNumber);
  });

  return items;
}

function getStatusClass(label) {
  return STATUS_CLASS_MAP[label] || 'available';
}

function buildOutstandingLabel(item) {
  const description = item?.description || 'Unnamed service';
  const dateText = item?.date ? `, ${item.date}` : '';
  return `${description}${dateText} - ${formatCurrency(item?.balance)}`;
}

function buildPaymentPreviewText(selectedDebtor, paymentAmount, paymentPlan, paymentPlanError, selectedServiceItem = null, selectedServicePrice = '') {
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

  if (selectedServiceItem) {
    const currentPrice = Number(selectedServiceItem.price || 0);
    const nextPrice = Number(normalizeDigits(selectedServicePrice || ''));
    const hasEditedPrice = Number.isFinite(nextPrice) && nextPrice > 0;
    const effectivePrice = hasEditedPrice ? nextPrice : currentPrice;
    const paidValue = Number(selectedServiceItem.paid || 0);
    const effectiveBalance = Math.max(0, effectivePrice - paidValue);

    lines.push('');
    lines.push('Selected service draft:');
    lines.push(`- ${selectedServiceItem.description || `Row ${selectedServiceItem.row_idx}`}`);
    lines.push(`- Current price: ${formatCurrency(currentPrice)}`);
    if (hasEditedPrice && nextPrice !== currentPrice) {
      lines.push(`- Edited price: ${formatCurrency(nextPrice)}`);
    }
    lines.push(`- Balance after edit: ${formatCurrency(effectiveBalance)}`);
  }

  paymentPlan.updates.forEach((update) => {
    const service = (paymentPlan.outstanding_items || []).find((item) => item.row_idx === update.row_idx);
    const description = service?.description || `Row ${update.row_idx}`;
    lines.push(`- ${description}: ${formatCurrency(update.new_paid)} -> ${update.new_status}`);
  });

  return lines.join('\n');
}

function getRollingWeekLabels() {
  const labels = [];
  const today = new Date();
  for (let offset = 6; offset >= 0; offset -= 1) {
    const date = new Date(today);
    date.setDate(today.getDate() - offset);
    labels.push(date.toLocaleDateString('en-GB', { weekday: 'short' }));
  }
  return labels;
}

async function copyText(text) {
  if (!text) {
    return;
  }

  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
  } catch {
    // Fall through to the legacy copy approach.
  }

  const textArea = document.createElement('textarea');
  textArea.value = text;
  textArea.setAttribute('readonly', 'readonly');
  textArea.style.position = 'absolute';
  textArea.style.left = '-9999px';
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();
  textArea.setSelectionRange(0, textArea.value.length);
  const copied = document.execCommand('copy');
  document.body.removeChild(textArea);

  if (!copied) {
    throw new Error('Could not copy the bill text.');
  }
}

function buildWhatsappUrl(phone, text) {
  return `https://api.whatsapp.com/send?phone=${phone}&text=${encodeURIComponent(text)}`;
}

function buildProductFormValues(formConfig) {
  const defaults = formConfig?.defaults || {};
  const values = {};
  for (const header of formConfig?.visible_headers || []) {
    if (isPlaceholderStockColumnHeader(header)) {
      continue;
    }
    values[header] = defaults[header.toUpperCase()] || '';
  }
  return values;
}

function isPlaceholderStockColumnHeader(header) {
  const text = String(header || '').trim().toUpperCase();
  return /^COLUMN\s*\d+$/.test(text) || /^COL\s*\d+$/.test(text);
}

function normalizeHeaderName(value) {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function getValueByHeaderAliases(valuesByHeader, aliases) {
  if (!valuesByHeader || typeof valuesByHeader !== 'object') {
    return '';
  }

  const normalizedEntries = Object.entries(valuesByHeader || {}).map(([key, value]) => [normalizeHeaderName(key), value]);
  for (const alias of aliases || []) {
    const normalizedAlias = normalizeHeaderName(alias);
    const match = normalizedEntries.find(([key]) => key === normalizedAlias);
    if (match) {
      return String(match[1] || '').trim();
    }
  }
  return '';
}

function formatSwapDeviceLabel(valuesByHeader) {
  const description = getValueByHeaderAliases(valuesByHeader, ['DESCRIPTION', 'MODEL', 'DEVICE']) || 'Phone';
  const color = getValueByHeaderAliases(valuesByHeader, ['COLOUR', 'COLOR']);
  const storage = getValueByHeaderAliases(valuesByHeader, ['STORAGE']);
  const imei = getValueByHeaderAliases(valuesByHeader, ['IMEI']);
  return [description, color, storage, imei ? `IMEI ${imei}` : ''].filter(Boolean).join(' | ');
}

function buildClientsDataFromRegistry(registry) {
  const normalizedRegistry = {};
  const normalizedGenders = {};
  Object.entries(registry || {}).forEach(([name, value]) => {
    if (value && typeof value === 'object') {
      normalizedRegistry[name] = String(value.phone || '');
      normalizedGenders[name] = String(value.gender || '').trim().toLowerCase();
      return;
    }
    normalizedRegistry[name] = String(value || '');
    normalizedGenders[name] = '';
  });
  const entries = Object.entries(normalizedRegistry)
    .sort((left, right) => String(left[0]).localeCompare(String(right[0]), undefined, { sensitivity: 'base' }))
    .map(([name, phone]) => ({
      name,
      phone: String(phone || ''),
      gender: String(normalizedGenders[name] || ''),
      has_phone: Boolean(String(phone || '').trim()),
    }));

  return {
    registry: normalizedRegistry,
    entries,
    stats: {
      total_count: entries.length,
      with_phone_count: entries.filter((entry) => entry.has_phone).length,
      without_phone_count: entries.filter((entry) => !entry.has_phone).length,
      with_gender_count: entries.filter((entry) => Boolean(String(entry.gender || '').trim())).length,
    },
  };
}

function normalizeClientsPayload(payload) {
  if (payload && typeof payload === 'object' && payload.registry && Array.isArray(payload.entries)) {
    const normalizedEntries = payload.entries.map((entry) => ({
      ...entry,
      gender: String(entry?.gender || '').trim().toLowerCase(),
    }));
    const normalizedRegistry = {};
    normalizedEntries.forEach((entry) => {
      normalizedRegistry[entry.name] = String(entry.phone || '');
    });
    return {
      registry: normalizedRegistry,
      entries: normalizedEntries,
      stats: {
        total_count: Number(payload?.stats?.total_count || normalizedEntries.length),
        with_phone_count: Number(payload?.stats?.with_phone_count || normalizedEntries.filter((entry) => entry.has_phone).length),
        without_phone_count: Number(payload?.stats?.without_phone_count || normalizedEntries.filter((entry) => !entry.has_phone).length),
        with_gender_count: Number(payload?.stats?.with_gender_count || normalizedEntries.filter((entry) => Boolean(String(entry.gender || '').trim())).length),
      },
    };
  }
  return buildClientsDataFromRegistry(payload || {});
}

function findHeaderIndex(headers, aliases) {
  const normalizedHeaders = (headers || []).map((header) => String(header || '').trim().toUpperCase());
  for (const alias of aliases || []) {
    const index = normalizedHeaders.indexOf(String(alias || '').trim().toUpperCase());
    if (index >= 0) {
      return index;
    }
  }
  return -1;
}

function getProductCellValue(row, headers, aliases) {
  const index = findHeaderIndex(headers, aliases);
  if (index < 0) {
    return '';
  }
  return String(row?.padded?.[index] || '').trim();
}

function buildCartItemFromProductRow(row, headers) {
  return {
    stock_row_num: row.row_num,
    description: getProductCellValue(row, headers, ['DESCRIPTION', 'MODEL', 'DEVICE']),
    imei: getProductCellValue(row, headers, ['IMEI']),
    colour: getProductCellValue(row, headers, ['COLOUR', 'COLOR']),
    storage: getProductCellValue(row, headers, ['STORAGE']),
    cost_price: getProductCellValue(row, headers, ['COST PRICE']),
    buyer_name: getProductCellValue(row, headers, ['NAME OF BUYER']),
    buyer_phone: getProductCellValue(row, headers, ['PHONE NUMBER OF BUYER']),
    sale_price: '',
    amount_paid: '',
    phone_expense: '',
    payment_method: 'CASH',
    fulfillment_method: 'WALK-IN PICKUP',
    pickup_mode: 'BUYER',
    representative_name: '',
    representative_phone: '',
    deal_location: '',
    internal_note: '',
    is_swap: false,
    swap_type: 'UPGRADE',
    swap_devices: '',
    swap_incoming_devices: [],
    swap_cash_amount: '',
    payment_status: 'UNPAID',
    availability_choice: 'AUTO',
    availability_custom: '',
  };
}

function PageNavigator({ page, totalPages, onChange }) {
  if (totalPages <= 1) {
    return null;
  }

  const pageItems = buildPageItems(page, totalPages);

  return (
    <div className="page-nav">
      <span className="page-nav__summary">Page {page} of {totalPages}</span>
      <button type="button" className="page-nav__button" onClick={() => onChange(page - 1)} disabled={page <= 1}>
        Prev
      </button>
      <div className="page-nav__numbers">
        {pageItems.map((item) => {
          if (typeof item !== 'number') {
            return (
              <span key={item} className="page-nav__ellipsis" aria-hidden="true">
                ...
              </span>
            );
          }

          return (
            <button
              key={item}
              type="button"
              className={item === page ? 'page-nav__number active' : 'page-nav__number'}
              onClick={() => onChange(item)}
            >
              {item}
            </button>
          );
        })}
      </div>
      <button type="button" className="page-nav__button" onClick={() => onChange(page + 1)} disabled={page >= totalPages}>
        Next
      </button>
    </div>
  );
}

class ViewErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      hasError: false,
      message: '',
    };
  }

  static getDerivedStateFromError(error) {
    return {
      hasError: true,
      message: String(error?.message || 'A view rendering error occurred.'),
    };
  }

  componentDidCatch(error) {
    // Keep this visible in browser console for debugging in production.
    // eslint-disable-next-line no-console
    console.error('ViewErrorBoundary caught:', error);
  }

  componentDidUpdate(prevProps) {
    if (prevProps.resetKey !== this.props.resetKey && this.state.hasError) {
      this.setState({ hasError: false, message: '' });
    }
  }

  render() {
    if (this.state.hasError) {
      return (
        <section className="content-panel content-panel--main content-panel--full">
          <div className="panel-header">
            <h3>Section Failed To Render</h3>
            <p>This section crashed while rendering. Switch sections and try again.</p>
          </div>
          <div className="notice notice-error">
            {this.state.message || 'Unknown render error.'}
          </div>
        </section>
      );
    }
    return this.props.children;
  }
}

function MaskedMetricCard({
  label,
  value,
  note,
  revealKey,
  revealedMetric,
  setRevealedMetric,
  className = '',
  onClick,
  onClickMode = 'card',
}) {
  const visible = revealedMetric === revealKey;
  const canViewDetails = typeof onClick === 'function';
  const cardClickable = canViewDetails && onClickMode === 'card';

  return (
    <article
      className={`metric-card metric-card--home ${className}`.trim()}
      style={cardClickable ? { cursor: 'pointer' } : undefined}
      onClick={cardClickable ? onClick : undefined}
    >
      <div className="metric-card__top">
        <span className="metric-label">{label}</span>
        <div className="metric-card__actions">
          {canViewDetails && onClickMode === 'button' ? (
            <button
              type="button"
              className="hold-button"
              onClick={(e) => {
                e.stopPropagation();
                onClick();
              }}
            >
              View
            </button>
          ) : null}
          <button
            type="button"
            className="hold-button"
            onPointerDown={(e) => { e.stopPropagation(); setRevealedMetric(revealKey); }}
            onPointerUp={(e) => { e.stopPropagation(); setRevealedMetric(''); }}
            onPointerLeave={() => setRevealedMetric('')}
            onPointerCancel={() => setRevealedMetric('')}
          >
            Hold to unhide
          </button>
        </div>
      </div>
      <strong className="metric-value">{visible ? value : '***'}</strong>
      <span className="metric-note">{note}</span>
    </article>
  );
}

function SimpleBarGraph({ title, description, items, valueFormatter = formatCount, emptyText = 'No data yet.' }) {
  const maxValue = Math.max(1, ...items.map((item) => Number(item.value || 0)));

  return (
    <section className="graph-panel">
      <div className="panel-header graph-header">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>

      {items.length ? (
        <div className="bar-chart">
          {items.map((item) => (
            <div key={item.label} className="bar-column">
              <div className="bar-value">{valueFormatter(item.value)}</div>
              <div
                className="bar-fill"
                style={{ height: `${Math.max(14, (Number(item.value || 0) / maxValue) * 132)}px` }}
              />
              <div className="bar-label">{item.label}</div>
            </div>
          ))}
        </div>
      ) : (
        <div className="notice compact">{emptyText}</div>
      )}
    </section>
  );
}

function ActionSidebar({ activeView, undoEnabled, redoEnabled, onTrigger, actionItems }) {
  return (
    <aside className="sidebar-frame sidebar-frame--list">
      <h2>General Actions</h2>
      <p>Reordered for the web workspace and expanded with clear descriptions.</p>

      <div className="action-list">
        {(actionItems || []).map((item) => {
          const isView = item.type === 'view';
          const isActive = isView && item.key === activeView;
          const disabled = (item.key === 'undo' && !undoEnabled) || (item.key === 'redo' && !redoEnabled);
          return (
            <button
              key={item.key}
              type="button"
              className={isActive ? 'action-list-item active' : 'action-list-item'}
              onClick={() => onTrigger(item)}
              disabled={disabled}
            >
              <span className="action-list-item__title" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px' }}>
                <span>{item.title}</span>
                {Number(item.badge || 0) > 0 ? (
                  <span className="floating-action-badge" style={{ position: 'static', minWidth: '22px', height: '22px' }}>
                    {formatCount(item.badge)}
                  </span>
                ) : null}
              </span>
              <span className="action-list-item__description">{item.description}</span>
            </button>
          );
        })}
      </div>
    </aside>
  );
}

function HomeView({ debtorsData, salesSnapshot, stockView, nameFixData, syncStatus, lastLoadedAt, revealedMetric, setRevealedMetric, onStatisticClick, onSecretCashflow }) {
  const stockCounts = stockView?.counts || {};
  const [secretTapCount, setSecretTapCount] = useState(0);
  const secretTapResetRef = useRef(null);

  useEffect(() => () => {
    if (secretTapResetRef.current) {
      clearTimeout(secretTapResetRef.current);
    }
  }, []);

  function handleSecretCashflowTap() {
    setSecretTapCount((current) => {
      const next = current + 1;
      if (secretTapResetRef.current) {
        clearTimeout(secretTapResetRef.current);
      }
      secretTapResetRef.current = setTimeout(() => {
        setSecretTapCount(0);
      }, 2500);

      if (next >= 7) {
        clearTimeout(secretTapResetRef.current);
        secretTapResetRef.current = null;
        onSecretCashflow?.();
        return 0;
      }

      return next;
    });
  }
  const homeSummaryCards = [
    {
      type: 'plain',
      label: 'Customers Owing',
      value: formatCount((debtorsData.sorted_debtors || []).length),
      note: 'Live debtor count.',
    },
    {
      type: 'plain',
      label: 'Products Available',
      value: formatCount(stockCounts.available),
      note: 'Current available stock count.',
    },
    {
      type: 'plain',
      label: 'Pending Deals',
      value: formatCount(stockCounts.pending),
      note: 'Deals waiting to close.',
    },
    {
      type: 'plain',
      label: 'Name Fixes',
      value: formatCount(nameFixData.count),
      note: 'Rows waiting for correction.',
    },
  ];

  const statisticsCards = [
    { label: 'Customers Today', value: formatCount(salesSnapshot.customers_today), note: 'Unique buyers today.' },
    { label: 'Services Today', value: formatCount(salesSnapshot.services_today), note: 'Completed transactions today.' },
    { label: 'Products Available', value: formatCount(stockCounts.available), note: 'Items ready to sell.' },
    { label: 'Pending Deals', value: formatCount(stockCounts.pending), note: 'Deals waiting to close.' },
    { label: 'Needs Details', value: formatCount(stockCounts.needs_details), note: 'Products that need cleanup.' },
    { label: 'Name Fixes', value: formatCount(nameFixData.count), note: 'Rows waiting for correction.' },
    { label: 'Queue Pending', value: formatCount(syncStatus?.queue_pending), note: 'Background writes still pending.' },
    { label: 'Last Loaded', value: formatShortStamp(lastLoadedAt), note: syncStatus?.sync_state?.last_status || 'Live runtime' },
  ];

  const dailyLabels = getRollingWeekLabels();
  const dailySalesItems = dailyLabels.map((label, index) => ({
    label,
    value: salesSnapshot.daily_totals?.[index] || 0,
  }));
  const weeklySalesItems = (salesSnapshot.week_totals || []).map((value, index) => ({
    label: `Week ${index + 1}`,
    value,
  }));
  const stockDistributionItems = (stockView?.available_breakdown || [])
    .slice()
    .sort((left, right) => right.count - left.count)
    .slice(0, 7)
    .map((item) => ({
      label: item.series,
      value: item.count,
    }));

  return (
    <div className="workspace-stack">
      <section className="summary-frame">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', gap: '12px' }}>
          <h2>Live Summary</h2>
          <button
            type="button"
            onClick={handleSecretCashflowTap}
            aria-label="Open cash flow"
            title={`Open Cash Flow Dashboard (${Math.max(0, 7 - secretTapCount)} taps left)`}
            style={{
              width: '28px',
              height: '28px',
              borderRadius: '999px',
              border: '2px solid #007bff',
              background: '#e3f0ff',
              opacity: 0.85,
              padding: 0,
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              boxShadow: '0 0 4px #007bff55',
            }}
          >
            <svg width="18" height="18" viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
              <circle cx="10" cy="10" r="9" stroke="#007bff" strokeWidth="2" fill="#fff" />
              <path d="M6 10c0-2.21 1.79-4 4-4s4 1.79 4 4-1.79 4-4 4" stroke="#007bff" strokeWidth="1.5" fill="none" />
              <text x="10" y="14" textAnchor="middle" fontSize="8" fill="#007bff" fontFamily="Arial">₦</text>
            </svg>
          </button>
        </div>
        <div className="summary-grid summary-grid--home">
          {homeSummaryCards.map((card) =>
            card.type === 'masked' ? (
              <MaskedMetricCard
                key={card.label}
                label={card.label}
                value={card.value}
                note={card.note}
                revealKey={card.revealKey}
                revealedMetric={revealedMetric}
                setRevealedMetric={setRevealedMetric}
              />
            ) : (
              <article key={card.label} className="metric-card metric-card--home">
                <span className="metric-label">{card.label}</span>
                <strong className="metric-value">{card.value}</strong>
                <span className="metric-note">{card.note}</span>
              </article>
            )
          )}
        </div>
      </section>

      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Statistics</h3>
          <p>Quick operational numbers pulled from debtors, sales, stock, sync, and fix queues.</p>
        </div>
        <div className="stats-grid">
          {statisticsCards.map((card) => {
            const isClickable = card.label !== 'Queue Pending' && card.label !== 'Last Loaded';
            return (
              <article
                key={card.label}
                className={`stat-card ${isClickable ? 'stat-card--clickable' : ''}`}
                onClick={() => isClickable && onStatisticClick?.(card.label)}
                style={isClickable ? { cursor: 'pointer' } : {}}
              >
                <span className="metric-label">{card.label}</span>
                <strong className="stat-card__value">{card.value}</strong>
                <span className="metric-note">{card.note}</span>
              </article>
            );
          })}
        </div>
      </section>

      <section className="home-graphs">
        <SimpleBarGraph
          title="Daily Sales"
          description="Last seven days of sales totals."
          items={dailySalesItems}
          valueFormatter={formatCurrency}
        />
        <SimpleBarGraph
          title="Weekly Sales"
          description="This month split into week buckets."
          items={weeklySalesItems}
          valueFormatter={formatCurrency}
        />
        <SimpleBarGraph
          title="Available Product Distribution"
          description="Top available product groups by series."
          items={stockDistributionItems}
          valueFormatter={formatCount}
          emptyText="No available stock groups returned yet."
        />
      </section>
    </div>
  );
}

function CashFlowView({
  cashflowSummary,
  weeklyAllowance,
  salesSnapshot,
  expenses,
  transactions,
  capitalFlow,
  debtorsData,
  stockView,
  nameFixData,
  expenseSource,
  expenseSheetTitle,
  loading,
  errorText,
  expenseErrorText,
  expenseBusy,
  lastUpdatedAt,
  onReload,
  onCreateExpense,
  onUndoLastAllowanceWithdrawal,
}) {
  const summary = cashflowSummary || {};
  const allowance = weeklyAllowance || {};
  const sales = salesSnapshot || {};
  const capital = capitalFlow || { month_total: 0, week_total: 0, entries: [] };
  const liveSummaryCards = [
    {
      key: 'sales-month',
      label: 'Sales This Month',
      value: formatCurrency(sales.sales_month || 0),
      note: 'Total value of completed services for this month.',
    },
    {
      key: 'sales-today',
      label: 'Sales Today',
      value: formatCurrency(sales.sales_today || 0),
      note: 'Total value of completed services today.',
    },
    {
      key: 'debt-outstanding',
      label: 'Total Debt Outstanding',
      value: formatCurrency(debtorsData?.total_debtors_amount || 0),
      note: 'Current unpaid customer balance.',
    },
    {
      key: 'expected-income',
      label: 'Expected Income',
      value: formatCurrency(summary.expected_income || 0),
      note: 'Projected incoming cash from unpaid records.',
    },
  ];
  const [revealedMetric, setRevealedMetric] = useState('');
  const [drillDown, setDrillDown] = useState(null); // { title, rows }
  const allTx = Array.isArray(transactions) ? transactions : [];
  const capitalRows = Array.isArray(capital.entries) ? capital.entries : [];

  function openDrillDown(title, filterFn) {
    setDrillDown({ title, rows: allTx.filter(filterFn) });
  }

  function closeDrillDown() {
    setDrillDown(null);
  }

  function openCapitalDrillDown(title, filterFn) {
    setDrillDown({ title, rows: capitalRows.filter(filterFn) });
  }

  function txIsThisWeek(tx, weekStart, weekEnd) {
    try {
      const d = parse_date_approx(tx.date || tx.payment_date || '');
      return d !== null && d >= weekStart && d <= weekEnd;
    } catch {
      return false;
    }
  }

  function parse_date_approx(raw) {
    if (!raw) return null;
    // Try formats dd/mm/yyyy, yyyy-mm-dd, d/m/yyyy, dd-mm-yyyy
    const clean = String(raw).trim();
    let m = clean.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (m) return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
    m = clean.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
    return null;
  }

  function normalizeTxText(value) {
    return String(value || '').trim().toUpperCase().replace(/\s+/g, ' ');
  }

  function txDateKey(tx) {
    return String(tx?.payment_date || tx?.date || '').trim();
  }

  const drillDownDisplay = useMemo(() => {
    if (!drillDown) {
      return { rows: [], total: 0, amountHeader: 'Amount', amountHelpText: '' };
    }

    const titleText = String(drillDown.title || '').toLowerCase();
    const isProfitView = titleText.includes('profit');
    const serviceExpensePool = new Map();

    if (isProfitView) {
      for (const tx of allTx) {
        const source = String(tx?.source || '').toLowerCase();
        const category = String(tx?.category || '').toLowerCase();
        if (source === 'income' || !category.includes('service expense')) {
          continue;
        }

        const expenseAmount = Number(String(tx?.amount || '0').replace(/[^0-9.\-]/g, '')) || 0;
        if (expenseAmount <= 0) {
          continue;
        }

        const key = `${txDateKey(tx)}|${normalizeTxText(tx?.description)}|${normalizeTxText(tx?.created_by)}`;
        serviceExpensePool.set(key, (serviceExpensePool.get(key) || 0) + expenseAmount);
      }
    }

    const rows = (drillDown.rows || []).map((tx) => {
      const txType = String(tx?.type || '').toLowerCase();
      const txSource = String(tx?.source || '').toLowerCase();
      const rawAmount = Number(String(tx?.amount || '0').replace(/[^0-9.\-]/g, '')) || 0;
      let displayAmount = rawAmount;

      if (isProfitView && txSource === 'income') {
        if (txType === 'phone') {
          const costPrice = Number(String(tx?.cost_price || '0').replace(/[^0-9.\-]/g, '')) || 0;
          if (costPrice > 0 && rawAmount > costPrice) {
            displayAmount = rawAmount - costPrice;
          }
        } else if (txType === 'service') {
          const key = `${txDateKey(tx)}|${normalizeTxText(tx?.description)}|${normalizeTxText(tx?.created_by)}`;
          const remainingExpense = serviceExpensePool.get(key) || 0;
          const appliedExpense = Math.min(remainingExpense, Math.max(0, rawAmount));
          displayAmount = rawAmount - appliedExpense;
          if (appliedExpense > 0) {
            serviceExpensePool.set(key, Math.max(0, remainingExpense - appliedExpense));
          }
        }
      }

      return {
        ...tx,
        _displayAmount: Number.isFinite(displayAmount) ? displayAmount : 0,
      };
    });

    const total = rows.reduce((sum, tx) => sum + (Number(tx._displayAmount) || 0), 0);
    return {
      rows,
      total,
      amountHeader: isProfitView ? 'Realized Profit' : 'Amount',
      amountHelpText: isProfitView
        ? 'Phone: realized profit = sale minus cost price. Service: realized profit = service income minus linked service expense.'
        : 'Raw transaction amount.',
    };
  }, [drillDown, allTx]);

  const today = new Date();
  // Backend week starts on Sunday: current_day - (weekday+1)%7
  // JS getDay(): Sun=0 Mon=1 … Sat=6 → subtract getDay() to land on Sunday
  const weekStart = new Date(today); weekStart.setDate(today.getDate() - today.getDay()); weekStart.setHours(0,0,0,0);
  const weekEnd = new Date(today); weekEnd.setHours(23,59,59,999);
  const [allowanceActionBusy, setAllowanceActionBusy] = useState(false);
  const todayKey = formatDateForInput();

  const weeklyAllowanceEntriesThisWeek = useMemo(() => {
    return allTx
      .filter((tx) => {
        if (!txIsThisWeek(tx, weekStart, weekEnd)) {
          return false;
        }
        const source = String(tx?.source || '').trim().toLowerCase();
        const category = String(tx?.category || '').trim().toUpperCase();
        return source !== 'income' && category.includes('WEEKLY ALLOWANCE');
      })
      .map((tx) => {
        const dateValue = parse_date_approx(tx?.payment_date || tx?.date || '');
        return {
          ...tx,
          _dateValue: dateValue,
          _dateKey: dateValue instanceof Date ? dateValue.toISOString().slice(0, 10) : '',
          _amount: Number(String(tx?.amount || '0').replace(/[^0-9.\-]/g, '')) || 0,
        };
      })
      .sort((left, right) => {
        const leftTime = left?._dateValue instanceof Date ? left._dateValue.getTime() : 0;
        const rightTime = right?._dateValue instanceof Date ? right._dateValue.getTime() : 0;
        return rightTime - leftTime;
      });
  }, [allTx, weekStart, weekEnd]);

  const withdrawnAllowanceThisWeek = weeklyAllowanceEntriesThisWeek
    .filter((tx) => (tx._amount || 0) > 0)
    .reduce((sum, tx) => sum + (tx._amount || 0), 0);
  const suggestedAllowanceAmount = Number(allowance?.suggested_allowance || 0);
  const remainingAllowanceToWithdraw = Math.max(0, suggestedAllowanceAmount - withdrawnAllowanceThisWeek);
  const latestWeeklyAllowanceEntry = weeklyAllowanceEntriesThisWeek[0] || null;
  const canUndoLatestWeeklyAllowance = Boolean(
    latestWeeklyAllowanceEntry
    && (latestWeeklyAllowanceEntry._amount || 0) > 0
    && latestWeeklyAllowanceEntry._dateKey === todayKey
  );

  const [expenseDraft, setExpenseDraft] = useState({
    amount: '',
    category: '',
    description: '',
    date: formatDateForInput(),
    allowance_impact: 'personal_allowance',
  });

  const recentExpenses = Array.isArray(expenses) ? expenses.slice(0, 8) : [];

  async function handleSubmitExpense(event) {
    event.preventDefault();
    const saved = await onCreateExpense?.({
      amount: expenseDraft.amount,
      category: expenseDraft.category,
      description: expenseDraft.description,
      date: expenseDraft.date,
      allowance_impact: expenseDraft.allowance_impact,
    });

    if (saved) {
      setExpenseDraft({
        amount: '',
        category: '',
        description: '',
        date: formatDateForInput(),
        allowance_impact: 'personal_allowance',
      });
    }
  }

  async function handleMarkAllowanceWithdrawn() {
    if (remainingAllowanceToWithdraw <= 0) {
      return;
    }

    const weekLabel = `${weekStart.toISOString().slice(0, 10)} to ${weekEnd.toISOString().slice(0, 10)}`;
    const shouldContinue = window.confirm(
      `Record weekly allowance withdrawal of ${formatCurrency(remainingAllowanceToWithdraw)} for ${weekLabel}?`
    );
    if (!shouldContinue) {
      return;
    }

    setAllowanceActionBusy(true);
    try {
      const saved = await onCreateExpense?.({
        amount: remainingAllowanceToWithdraw,
        category: 'WEEKLY ALLOWANCE',
        description: `Allowance withdrawn for week ${weekLabel}`,
        date: todayKey,
        allowance_impact: 'personal_allowance',
      });
      if (saved === false) {
        return;
      }
    } finally {
      setAllowanceActionBusy(false);
    }
  }

  async function handleUndoLastAllowanceWithdrawal() {
    if (!canUndoLatestWeeklyAllowance) {
      return;
    }

    const shouldContinue = window.confirm(
      `Undo the latest weekly allowance withdrawal (${formatCurrency(latestWeeklyAllowanceEntry?._amount || 0)}) recorded today?`
    );
    if (!shouldContinue) {
      return;
    }

    setAllowanceActionBusy(true);
    try {
      await onUndoLastAllowanceWithdrawal?.();
    } finally {
      setAllowanceActionBusy(false);
    }
  }

  const weekGrossProfit = (summary.current_week_phone_profit || 0) + (summary.current_week_service_profit || 0);

  const cards = [
    // ── Monthly overview ────────────────────────────────────────────────────
    {
      key: 'cash-in',
      label: 'Total Cash In (Month)',
      value: formatCurrency(summary.total_cash_in || 0),
      note: 'All paid income received this month (not net profit). Click to view.',
      className: '',
      onClick: () => openDrillDown('Total Cash In (Month) — All Paid Income', (tx) => tx.source === 'income' && tx.payment_status !== 'OWING'),
    },
    {
      key: 'expenses',
      label: 'Total Expenses',
      value: formatCurrency(summary.total_expenses || 0),
      note: 'All tracked expenses from the cash-flow sheet. Click to view.',
      className: '',
      onClick: () => openDrillDown('Total Expenses — All Recorded Expenses', (tx) => tx.source !== 'income'),
    },
    {
      key: 'profit',
      label: 'Net Profit',
      value: formatCurrency(summary.net_profit || 0),
      note: 'Operating profit after expenses.',
      className: 'metric-card--profit',
    },
    {
      key: 'expected-income',
      label: 'Expected Income',
      value: formatCurrency(summary.expected_income || 0),
      note: 'Owing / not yet paid — not included in profit. Click to view.',
      className: '',
      onClick: () => openDrillDown('Expected Income — Unpaid / Owing', (tx) => tx.source === 'income' && tx.payment_status === 'OWING'),
    },
    // ── This week ───────────────────────────────────────────────────────────
    {
      key: 'week-gross',
      label: 'This Week Profit',
      value: formatCurrency(weekGrossProfit),
      note: `Phone ${formatCurrency(summary.current_week_phone_profit || 0)} + services ${formatCurrency(summary.current_week_service_profit || 0)}. Click to view.`,
      className: '',
      onClick: () => openDrillDown('This Week Profit — Phones & Services', (tx) => tx.source === 'income' && tx.payment_status !== 'OWING' && txIsThisWeek(tx, weekStart, weekEnd)),
    },
    {
      key: 'week-expenses',
      label: 'This Week Expenses',
      value: formatCurrency(summary.current_week_expenses || 0),
      note: 'Expenses recorded during this week. Click to view.',
      className: '',
      onClick: () => openDrillDown('This Week Expenses', (tx) => tx.source !== 'income' && txIsThisWeek(tx, weekStart, weekEnd)),
    },
    {
      key: 'week-net',
      label: 'This Week Net Profit',
      value: formatCurrency(summary.current_week_net_profit || 0),
      note: 'Real weekly profit after deducting this week\'s expenses.',
      className: 'metric-card--profit',
    },
    {
      key: 'allowance-base',
      label: 'Allowance Profit Base',
      value: formatCurrency(summary.allowance_base_net_profit || 0),
      note: `Used for allowance only. Starts from realized weekly profit and excludes business-only expenses (${formatCurrency(summary.current_week_business_only_expenses || 0)} this week).`,
      className: 'metric-card--profit',
    },
    {
      key: 'business-week-expenses',
      label: 'Business-Only Expenses (Week)',
      value: formatCurrency(summary.current_week_business_only_expenses || 0),
      note: 'Tracked for visibility; does not reduce your weekly allowance.',
      className: '',
    },
    // ── Cash position ────────────────────────────────────────────────────────
    {
      key: 'available-cash',
      label: 'Available Cash',
      value: formatCurrency(summary.available_cash || 0),
      note: 'After reserves and receivables exclusion.',
      className: '',
    },
    {
      key: 'reserve',
      label: 'Reserve Amount',
      value: formatCurrency(summary.reserve_amount || 0),
      note: 'Protected by reserve percentage.',
      className: '',
    },
    {
      key: 'allowance',
      label: 'Next Week Allowance',
      value: formatCurrency(allowance.suggested_allowance || 0),
      note: `Allowance is 25% of allowance-base net profit, capped by usable cash and a ${allowance.buffer_weeks_threshold || 4}-week buffer policy.`,
      className: 'metric-card--allowance',
    },
  ];

  const cashHealthLabel = String(summary.cash_health_status || 'red').toUpperCase();
  const monthlyRemainder = (summary.monthly_allowance_paid || 0) > 0
    ? (summary.month_remainder_profit_after_paid_allowance || 0)
    : (summary.month_remainder_profit_after_provision || 0);

  const capitalCards = [
    {
      key: 'capital-month',
      label: 'Business Capital Outflow (Month)',
      value: formatCurrency(capital.month_total || 0),
      note: 'Phone restocking cost this month. Kept separate from operating profit. Click to view.',
      onClick: () => openCapitalDrillDown('Business Capital Outflow (Month)', () => true),
    },
    {
      key: 'capital-week',
      label: 'Business Capital Outflow (This Week)',
      value: formatCurrency(capital.week_total || 0),
      note: 'Phone restocking cost this week. Kept separate from allowance and operating profit. Click to view.',
      onClick: () => openCapitalDrillDown('Business Capital Outflow (This Week)', (row) => txIsThisWeek(row, weekStart, weekEnd)),
    },
  ];

  const governanceCards = [
    {
      key: 'monthly-fixed-overhead',
      label: 'Fixed Monthly Overhead',
      value: formatCurrency(summary.monthly_fixed_overhead || 0),
      note: 'Estimated recurring expenses (internet, rent, subscription, wages, etc.).',
    },
    {
      key: 'cash-health',
      label: 'Cash Health Score',
      value: `${cashHealthLabel} (${summary.cash_runway_weeks || 0}w runway)`,
      note: `Buffer check: ${(allowance.cash_buffer_ok ? 'OK' : 'LOW')} (required ${formatCurrency(allowance.required_cash_buffer || 0)}).`,
    },
    {
      key: 'month-remainder',
      label: 'Month Remainder Profit',
      value: formatCurrency(monthlyRemainder),
      note: (summary.monthly_allowance_paid || 0) > 0
        ? `Net profit after recorded weekly allowance payouts (${formatCurrency(summary.monthly_allowance_paid || 0)}).`
        : `Net profit after weekly allowance provision (${formatCurrency(summary.monthly_allowance_provision || 0)}).`,
    },
  ];

  return (
    <div className="workspace-stack">
      <section className="summary-frame">
        <h2>Live Summary</h2>
        <div className="summary-grid summary-grid--home" style={{ marginTop: '10px' }}>
          {liveSummaryCards.map((card) => (
            <MaskedMetricCard
              key={card.key}
              label={card.label}
              value={card.value}
              note={card.note}
              revealKey={`cashflow-${card.key}`}
              revealedMetric={revealedMetric}
              setRevealedMetric={setRevealedMetric}
            />
          ))}
        </div>
      </section>

      <section className="summary-frame">
        <h2>Business Capital</h2>
        <p className="metric-note" style={{ marginTop: '8px' }}>
          Capital tracking is back for visibility, but kept separate from operating profit, expenses, allowance, and health metrics.
        </p>
        <div className="summary-grid summary-grid--home" style={{ marginTop: '10px' }}>
          {capitalCards.map((card) => (
            <MaskedMetricCard
              key={card.key}
              label={card.label}
              value={card.value}
              note={card.note}
              revealKey={`cashflow-${card.key}`}
              revealedMetric={revealedMetric}
              setRevealedMetric={setRevealedMetric}
              onClickMode="button"
              onClick={card.onClick}
            />
          ))}
        </div>
      </section>

      <section className="summary-frame">
        <h2>Cashflow Dashboard</h2>
        <p className="metric-note" style={{ marginTop: '8px' }}>
          Last Updated: {lastUpdatedAt ? formatShortStamp(lastUpdatedAt) : 'Not loaded yet'}
        </p>
        <div className="button-row" style={{ marginTop: '10px' }}>
          <button type="button" className="secondary-button" onClick={() => onReload?.()} disabled={loading}>
            {loading ? 'Loading...' : 'Reload'}
          </button>
          <button
            type="button"
            className="primary-button"
            onClick={handleMarkAllowanceWithdrawn}
            disabled={loading || expenseBusy || allowanceActionBusy || remainingAllowanceToWithdraw <= 0}
          >
            {allowanceActionBusy ? 'Recording...' : 'Mark Weekly Allowance Withdrawn'}
          </button>
          <button
            type="button"
            className="secondary-button"
            onClick={handleUndoLastAllowanceWithdrawal}
            disabled={loading || expenseBusy || allowanceActionBusy || !canUndoLatestWeeklyAllowance}
            title={canUndoLatestWeeklyAllowance ? 'Undo latest weekly allowance withdrawal recorded today' : 'Undo is available only for the latest weekly allowance entry recorded today'}
          >
            {allowanceActionBusy ? 'Working...' : 'Undo Last Allowance Withdrawal'}
          </button>
        </div>
        <div className="notice compact" style={{ marginTop: '10px' }}>
          Suggested: {formatCurrency(suggestedAllowanceAmount)} | Withdrawn this week: {formatCurrency(withdrawnAllowanceThisWeek)} | Remaining: {formatCurrency(remainingAllowanceToWithdraw)}
          {latestWeeklyAllowanceEntry
            ? ` | Last withdrawal: ${latestWeeklyAllowanceEntry.date || latestWeeklyAllowanceEntry.payment_date || 'No date'} (${formatCurrency(latestWeeklyAllowanceEntry._amount || 0)})`
            : ' | No allowance withdrawal recorded yet this week.'}
        </div>
        {errorText ? (
          <div className="notice notice-error" style={{ marginTop: '12px' }}>
            {errorText}
          </div>
        ) : null}
        <div className="summary-grid summary-grid--home">
          {cards.map((card) => {
            if (loading) {
              return (
                <article key={card.key} className={`metric-card metric-card--home ${card.className}`.trim()}>
                  <span className="metric-label">{card.label}</span>
                  <strong className="metric-value">Loading...</strong>
                  <span className="metric-note">{card.note}</span>
                </article>
              );
            }

            return (
              <MaskedMetricCard
                key={card.key}
                label={card.label}
                value={card.value}
                note={card.note}
                revealKey={`cashflow-${card.key}`}
                revealedMetric={revealedMetric}
                setRevealedMetric={setRevealedMetric}
                className={card.className}
                onClickMode="button"
                onClick={card.onClick}
              />
            );
          })}
        </div>
      </section>

      <section className="summary-frame">
        <h2>Governance & Health</h2>
        <p className="metric-note" style={{ marginTop: '8px' }}>
          Controls to keep allowance healthy: buffer guardrail, overhead visibility, and month remainder after expenses and weekly allowance.
        </p>
        <div className="summary-grid summary-grid--home" style={{ marginTop: '10px' }}>
          {governanceCards.map((card) => (
            <article key={card.key} className="metric-card metric-card--home">
              <span className="metric-label">{card.label}</span>
              <strong className="metric-value">{card.value}</strong>
              <span className="metric-note">{card.note}</span>
            </article>
          ))}
        </div>
      </section>

      {drillDown && (
        <div
          role="dialog"
          aria-modal="true"
          aria-label={drillDown.title}
          style={{
            position: 'fixed', inset: 0, zIndex: 9999,
            background: 'rgba(0,0,0,0.55)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            padding: '16px',
          }}
          onClick={(e) => { if (e.target === e.currentTarget) closeDrillDown(); }}
        >
          <div style={{
            background: '#fff', borderRadius: '12px',
            width: '100%', maxWidth: '720px',
            maxHeight: '80vh', display: 'flex', flexDirection: 'column',
            boxShadow: '0 8px 32px rgba(0,0,0,0.22)',
          }}>
            <div style={{ padding: '16px 20px', borderBottom: '1px solid #e5e7eb', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 style={{ margin: 0, fontSize: '1rem' }}>{drillDown.title}</h3>
              <button type="button" onClick={closeDrillDown} style={{ background: 'none', border: 'none', fontSize: '1.4rem', cursor: 'pointer', lineHeight: 1 }}>×</button>
            </div>
            {drillDown.rows.length === 0 ? (
              <p style={{ padding: '24px 20px', color: '#6b7280', textAlign: 'center' }}>No records found.</p>
            ) : (
              <div style={{ overflowY: 'auto', flex: 1 }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
                  <thead>
                    <tr style={{ background: '#f9fafb', position: 'sticky', top: 0 }}>
                      <th style={{ padding: '8px 12px', textAlign: 'left', borderBottom: '1px solid #e5e7eb' }}>Date</th>
                      <th style={{ padding: '8px 12px', textAlign: 'left', borderBottom: '1px solid #e5e7eb' }}>Type</th>
                      <th style={{ padding: '8px 12px', textAlign: 'left', borderBottom: '1px solid #e5e7eb' }}>Category / Item</th>
                      <th style={{ padding: '8px 12px', textAlign: 'left', borderBottom: '1px solid #e5e7eb' }}>Description</th>
                      <th style={{ padding: '8px 12px', textAlign: 'right', borderBottom: '1px solid #e5e7eb' }}>
                        <span>{drillDownDisplay.amountHeader}</span>
                        <span
                          title={drillDownDisplay.amountHelpText}
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            marginLeft: '6px',
                            width: '15px',
                            height: '15px',
                            borderRadius: '999px',
                            border: '1px solid #9ca3af',
                            color: '#6b7280',
                            fontSize: '10px',
                            fontWeight: 700,
                            cursor: 'help',
                            lineHeight: 1,
                          }}
                        >
                          i
                        </span>
                      </th>
                      <th style={{ padding: '8px 12px', textAlign: 'left', borderBottom: '1px solid #e5e7eb' }}>By</th>
                    </tr>
                  </thead>
                  <tbody>
                    {drillDownDisplay.rows.map((tx, idx) => {
                      const txType = String(tx.type || '').toLowerCase();
                      const txSource = String(tx.source || '').toLowerCase();
                      let typeLabel = '—';
                      let typeBadgeStyle = { padding: '2px 7px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 600, background: '#f3f4f6', color: '#374151' };
                      if (txSource === 'income' && txType === 'phone') {
                        typeLabel = 'Phone';
                        typeBadgeStyle = { ...typeBadgeStyle, background: '#dbeafe', color: '#1d4ed8' };
                      } else if (txSource === 'income' && txType === 'service') {
                        typeLabel = 'Service';
                        typeBadgeStyle = { ...typeBadgeStyle, background: '#dcfce7', color: '#15803d' };
                      } else if (txSource === 'capital') {
                        typeLabel = 'Capital';
                        typeBadgeStyle = { ...typeBadgeStyle, background: '#fef3c7', color: '#92400e' };
                      } else if (txSource !== 'income') {
                        typeLabel = 'Expense';
                        typeBadgeStyle = { ...typeBadgeStyle, background: '#fee2e2', color: '#b91c1c' };
                      }
                      return (
                        <tr key={`${tx.row_num ?? idx}`} style={{ borderBottom: '1px solid #f3f4f6' }}>
                          <td style={{ padding: '7px 12px', whiteSpace: 'nowrap' }}>{tx.payment_date || tx.date || '—'}</td>
                          <td style={{ padding: '7px 12px' }}><span style={typeBadgeStyle}>{typeLabel}</span></td>
                          <td style={{ padding: '7px 12px', fontWeight: 500 }}>{tx.category || '—'}</td>
                          <td style={{ padding: '7px 12px', color: '#374151' }}>{tx.description || '—'}</td>
                          <td style={{ padding: '7px 12px', textAlign: 'right', fontWeight: 700 }}>{formatCurrency(tx._displayAmount || 0)}</td>
                          <td style={{ padding: '7px 12px', color: '#6b7280' }}>{tx.created_by || '—'}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
            <div style={{ padding: '10px 20px', borderTop: '1px solid #e5e7eb', display: 'flex', justifyContent: 'space-between', alignItems: 'center', color: '#6b7280', fontSize: '0.8rem' }}>
              <span>{drillDown.rows.length} record{drillDown.rows.length !== 1 ? 's' : ''}</span>
              <strong style={{ color: '#111827', fontSize: '0.9rem' }}>
                Total: {formatCurrency(drillDownDisplay.total)}
              </strong>
            </div>
          </div>
        </div>
      )}

      <section className="summary-frame">
        <h3>Record Expense</h3>
        <p className="metric-note" style={{ marginTop: '8px' }}>
          This writes to the inventory workbook cash-flow sheet and keeps the database mirror in sync.
        </p>
        <form onSubmit={handleSubmitExpense} className="workspace-stack" style={{ marginTop: '12px' }}>
          <div className="summary-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
            <label className="workspace-field">
              <span className="metric-label">Amount</span>
              <input
                type="number"
                min="0"
                step="1"
                value={expenseDraft.amount}
                onChange={(event) => setExpenseDraft((current) => ({ ...current, amount: event.target.value }))}
                placeholder="5000"
              />
            </label>
            <label className="workspace-field">
              <span className="metric-label">Category</span>
              <input
                type="text"
                value={expenseDraft.category}
                onChange={(event) => setExpenseDraft((current) => ({ ...current, category: event.target.value }))}
                placeholder="Transport, fuel, data..."
              />
            </label>
            <label className="workspace-field">
              <span className="metric-label">Date</span>
              <input
                type="date"
                value={expenseDraft.date}
                onChange={(event) => setExpenseDraft((current) => ({ ...current, date: event.target.value }))}
              />
            </label>
            <label className="workspace-field">
              <span className="metric-label">Allowance Impact</span>
              <select
                value={expenseDraft.allowance_impact}
                onChange={(event) => setExpenseDraft((current) => ({ ...current, allowance_impact: event.target.value }))}
              >
                <option value="personal_allowance">Personal Allowance (affects weekly allowance)</option>
                <option value="business_only">Business Only (does not affect weekly allowance)</option>
              </select>
            </label>
          </div>
          <label className="workspace-field">
            <span className="metric-label">Description</span>
            <input
              type="text"
              value={expenseDraft.description}
              onChange={(event) => setExpenseDraft((current) => ({ ...current, description: event.target.value }))}
              placeholder="Short note about the expense"
            />
          </label>
          {expenseErrorText ? (
            <div className="notice notice-error">{expenseErrorText}</div>
          ) : null}
          <div className="button-row">
            <button type="submit" className="secondary-button" disabled={expenseBusy || loading}>
              {expenseBusy ? 'Saving...' : 'Add Expense'}
            </button>
          </div>
        </form>
      </section>

      <section className="summary-frame">
        <h3>Recent Expenses</h3>
        <p className="metric-note" style={{ marginTop: '8px' }}>
          Source: {expenseSource === 'sheet' ? `${expenseSheetTitle || 'CASH FLOW'} tab in the inventory workbook` : 'Database fallback'}.
        </p>
        {recentExpenses.length ? (
          <div className="summary-grid" style={{ marginTop: '12px', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
            {recentExpenses.map((expense, index) => (
              <article key={`${expense.row_num || index}-${expense.date || ''}-${expense.amount || ''}`} className="metric-card metric-card--home">
                <span className="metric-label">{expense.category || 'Expense'}</span>
                <strong className="metric-value">{formatCurrency(expense.amount || 0)}</strong>
                <span className="metric-note">
                  {expense.date || 'No date'}
                  {expense.description ? ` • ${expense.description}` : ''}
                </span>
              </article>
            ))}
          </div>
        ) : (
          <div className="notice" style={{ marginTop: '12px' }}>
            No expenses recorded yet.
          </div>
        )}
      </section>
    </div>
  );
}

function ProductComposerModal({
  stockForm,
  productFormValues,
  isAddingProduct,
  sellerPhoneOptions,
  currentTimeLabel,
  dropdownOptions,
  onClose,
  onSubmitProduct,
  onResetProductForm,
}) {
  const visibleHeaders = (stockForm?.visible_headers || []).filter((header) => !isPlaceholderStockColumnHeader(header));
  const [draftValues, setDraftValues] = useState({});

  useEffect(() => {
    setDraftValues(productFormValues || {});
  }, [productFormValues]);

  const canSubmitProduct = !isAddingProduct;

  function renderProductField(header) {
    const key = String(header || '').toUpperCase();
    const value = draftValues[header] || '';

    if (key === 'TIME') {
      return (
        <input
          type="text"
          value={value || currentTimeLabel}
          onChange={(event) => setDraftValues((current) => ({
            ...current,
            [header]: event.target.value,
          }))}
          placeholder="Auto time"
        />
      );
    }

    if (key === 'PHONE NUMBER OF SELLER') {
      return (
        <>
          <input
            type="text"
            list="seller-phone-options"
            value={value}
            onChange={(event) => setDraftValues((current) => ({
              ...current,
              [header]: extractPhoneFromSuggestionText(event.target.value),
            }))}
            placeholder="Search Google/saved contact numbers"
          />
          <datalist id="seller-phone-options">
            {(sellerPhoneOptions || []).map((option) => (
              <option key={option} value={option} />
            ))}
          </datalist>
        </>
      );
    }

    if (key === 'PRODUCT STATUS' || key === 'STOCK STATUS' || key === 'ITEM STATUS') {
      return (
        <select
          value={value || 'AVAILABLE'}
          onChange={(event) => setDraftValues((current) => ({
            ...current,
            [header]: event.target.value,
          }))}
        >
          <option value="AVAILABLE">AVAILABLE</option>
          <option value="SOLD">SOLD</option>
        </select>
      );
    }

    if (key === 'IMEI') {
      return (
        <>
          <input
            type="text"
            list="imei-options"
            value={value}
            onChange={(event) => setDraftValues((current) => ({
              ...current,
              [header]: event.target.value,
            }))}
            placeholder="Type to search IMEIs"
          />
          <datalist id="imei-options">
            {(dropdownOptions?.imei || []).slice(0, 50).map((option) => (
              <option key={option} value={option} />
            ))}
          </datalist>
        </>
      );
    }

    if (key === 'DEVICE') {
      const deviceOptions = Array.from(new Set([
        ...(dropdownOptions?.device || []),
        value,
      ].map((option) => String(option || '').trim()).filter(Boolean)));
      return (
        <>
          <input
            type="text"
            list="device-options"
            value={value}
            onChange={(event) => setDraftValues((current) => ({
              ...current,
              [header]: event.target.value,
            }))}
            placeholder="Type or pick device"
          />
          <datalist id="device-options">
            {deviceOptions.map((option) => (
              <option key={option} value={option} />
            ))}
          </datalist>
        </>
      );
    }

    if (key === 'STORAGE') {
      const storageOptions = Array.from(new Set([
        ...(dropdownOptions?.storage || []),
        value,
      ].map((option) => String(option || '').trim()).filter(Boolean)));
      return (
        <>
          <input
            type="text"
            list="storage-options"
            value={value}
            onChange={(event) => setDraftValues((current) => ({
              ...current,
              [header]: event.target.value,
            }))}
            placeholder="Type or pick storage"
          />
          <datalist id="storage-options">
            {storageOptions.map((option) => (
              <option key={option} value={option} />
            ))}
          </datalist>
        </>
      );
    }

    return (
      <input
        type="text"
        value={value}
        onChange={(event) => setDraftValues((current) => ({
          ...current,
          [header]: event.target.value,
        }))}
      />
    );
  }

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section
        className="modal-sheet"
        role="dialog"
        aria-modal="true"
        aria-labelledby="product-composer-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="modal-sheet__header">
          <div className="panel-header">
            <h3 id="product-composer-title">Add Product</h3>
            <p>Fill in product and seller details. New items are queued back into the same Google Sheet format used by the desktop workflow.</p>
          </div>

          <button type="button" className="secondary-button" onClick={onClose} disabled={isAddingProduct}>
            Close
          </button>
        </div>

        {visibleHeaders.length ? (
          <form className="form-stack" onSubmit={(event) => onSubmitProduct(event, draftValues)}>
            <div className="form-grid form-grid--modal">
              {visibleHeaders.map((header) => (
                <label key={header} className="field-block">
                  <span className="field-label">{header}</span>
                  {renderProductField(header)}
                </label>
              ))}
            </div>

            <div className="button-row button-row--end">
              <button type="button" className="secondary-button" onClick={onResetProductForm} disabled={isAddingProduct}>
                Reset Form
              </button>
              <button type="submit" className="primary-button" disabled={!canSubmitProduct}>
                {isAddingProduct ? 'Adding Product...' : 'Add Product'}
              </button>
            </div>
          </form>
        ) : (
          <div className="notice">Loading the product form fields...</div>
        )}
      </section>
    </div>
  );
}

function ProductDetailModal({ row, headers, onClose, onSave, saving }) {
  const [isEditing, setIsEditing] = useState(false);
  const [valuesByHeader, setValuesByHeader] = useState({});
  const [productStatus, setProductStatus] = useState('available');
  const localAmountPaidDraftKey = '__LOCAL_BUYER_AMOUNT_PAID__';
  const resolveHeader = (...aliases) => {
    const normalize = (value) => String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    const normalizedAliases = aliases.map(normalize).filter(Boolean);
    if (!normalizedAliases.length) {
      return '';
    }

    const exactMatch = headers.find((header) => normalizedAliases.includes(normalize(header)));
    if (exactMatch) {
      return exactMatch;
    }

    return headers.find((header) => {
      const normalizedHeader = normalize(header);
      return normalizedAliases.some((alias) => normalizedHeader.includes(alias) || alias.includes(normalizedHeader));
    }) || '';
  };

  const buyerNameHeader = resolveHeader('NAME OF BUYER');
  const buyerPhoneHeader = resolveHeader('PHONE NUMBER OF BUYER', 'PHONE OF BUYER', 'BUYER PHONE');
  const costPriceHeader = resolveHeader('COST PRICE');
  const amountPaidHeader = resolveHeader('AMOUNT PAID', 'PAID', 'PAID AMOUNT');
  const productStatusHeader = resolveHeader('PRODUCT STATUS', 'STATUS OF DEVICE', 'STOCK STATUS', 'ITEM STATUS');
  const availabilityHeader = resolveHeader('AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE');

  useEffect(() => {
    if (!row) {
      setValuesByHeader({});
      setIsEditing(false);
      setProductStatus('available');
      return;
    }

    const next = {};
    headers.forEach((header, index) => {
      next[header] = row.padded?.[index] || '';
    });
    setValuesByHeader(next);
    
    // Detect current status from row.label
    const statusMap = {
      'SOLD': 'sold',
      'PENDING DEAL': 'pending',
      'AVAILABLE': 'available',
    };
    setProductStatus(statusMap[String(row.label || '').toUpperCase()] || 'available');
    setIsEditing(false);
  }, [row, headers]);

  if (!row) {
    return null;
  }

  async function handleSave() {
    const statusValue = { 'sold': 'SOLD', 'pending': 'PENDING DEAL', 'available': 'AVAILABLE' }[productStatus] || 'AVAILABLE';

    const updatedValues = { ...valuesByHeader };
    if (productStatusHeader) {
      updatedValues[productStatusHeader] = statusValue;
    }
    if (availabilityHeader) {
      if (productStatus === 'sold') {
        updatedValues[availabilityHeader] = new Date().toLocaleDateString('en-US');
      } else if (productStatus === 'pending') {
        updatedValues[availabilityHeader] = 'PENDING DEAL';
      } else {
        updatedValues[availabilityHeader] = 'AVAILABLE';
      }
    }

    await onSave(row.row_num, updatedValues);
    setIsEditing(false);
  }

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section
        className="modal-sheet modal-sheet--detail"
        role="dialog"
        aria-modal="true"
        aria-labelledby="product-detail-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="modal-sheet__header">
          <div className="panel-header">
            <h3 id="product-detail-title">Product Details</h3>
            <p>Full row details for stock row #{row.row_num}.</p>
          </div>

          <button type="button" className="secondary-button" onClick={onClose} disabled={saving}>
            Close
          </button>
        </div>

        {isEditing ? (
          <div className="form-stack">
            <div className="form-grid form-grid--modal">
              <h4 style={{ gridColumn: '1 / -1', marginBottom: '8px', fontWeight: 600 }}>Buyer Information</h4>
              <label className="field-block">
                <span className="field-label">Buyer Name</span>
                <input
                  type="text"
                  value={buyerNameHeader ? (valuesByHeader[buyerNameHeader] || '') : ''}
                  onChange={(event) => setValuesByHeader((current) => ({
                    ...current,
                    ...(buyerNameHeader ? { [buyerNameHeader]: event.target.value } : {}),
                  }))}
                />
              </label>
              <label className="field-block">
                <span className="field-label">Buyer Phone</span>
                <input
                  type="text"
                  value={buyerPhoneHeader ? (valuesByHeader[buyerPhoneHeader] || '') : ''}
                  onChange={(event) => setValuesByHeader((current) => ({
                    ...current,
                    ...(buyerPhoneHeader ? { [buyerPhoneHeader]: event.target.value } : {}),
                  }))}
                />
              </label>
              <label className="field-block">
                <span className="field-label">Cost Price</span>
                <input
                  type="text"
                  value={costPriceHeader ? (valuesByHeader[costPriceHeader] || '') : ''}
                  onChange={(event) => setValuesByHeader((current) => ({
                    ...current,
                    ...(costPriceHeader ? { [costPriceHeader]: event.target.value } : {}),
                  }))}
                />
              </label>
              <label className="field-block">
                <span className="field-label">Amount Paid</span>
                <input
                  type="text"
                  value={amountPaidHeader ? (valuesByHeader[amountPaidHeader] || '') : (valuesByHeader[localAmountPaidDraftKey] || '')}
                  onChange={(event) => setValuesByHeader((current) => ({
                    ...current,
                    ...(amountPaidHeader ? { [amountPaidHeader]: event.target.value } : { [localAmountPaidDraftKey]: event.target.value }),
                  }))}
                />
              </label>
              <label className="field-block">
                <span className="field-label">Product Status</span>
                <select
                  value={productStatus}
                  onChange={(event) => setProductStatus(event.target.value)}
                >
                  <option value="available">Available</option>
                  <option value="sold">Sold</option>
                  <option value="pending">Pending Deal</option>
                </select>
              </label>
            </div>

            <div style={{ marginTop: '20px', paddingTop: '12px', borderTop: '1px solid rgba(85, 60, 30, 0.1)' }}>
              <h4 style={{ marginBottom: '12px', fontWeight: 600 }}>Product Details</h4>
              <div className="form-grid form-grid--modal">
                {headers.filter((h) => ![
                  buyerNameHeader,
                  buyerPhoneHeader,
                  productStatusHeader,
                  costPriceHeader,
                  amountPaidHeader,
                  availabilityHeader,
                ].includes(h)).map((header) => (
                  <label key={`${row.row_num}-${header}`} className="field-block">
                    <span className="field-label">{header}</span>
                    <input
                      type="text"
                      value={valuesByHeader[header] || ''}
                      onChange={(event) => setValuesByHeader((current) => ({
                        ...current,
                        [header]: event.target.value,
                      }))}
                    />
                  </label>
                ))}
              </div>
            </div>

            <div className="button-row button-row--end">
              <button type="button" className="secondary-button" onClick={() => setIsEditing(false)} disabled={saving}>
                Cancel
              </button>
              <button type="button" className="primary-button" onClick={handleSave} disabled={saving}>
                {saving ? 'Saving...' : 'Save Changes'}
              </button>
            </div>
          </div>
        ) : (
          <>
            <div className="detail-grid">
              <article className="detail-pair">
                <span className="detail-label">Row</span>
                <strong>#{row.row_num}</strong>
              </article>
              <article className="detail-pair">
                <span className="detail-label">Status</span>
                <strong>{row.label}</strong>
              </article>
              {headers.map((header, index) => (
                <article key={`${row.row_num}-${header}`} className="detail-pair">
                  <span className="detail-label">{header}</span>
                  <strong>{row.padded?.[index] || '—'}</strong>
                </article>
              ))}
            </div>

            <div className="button-row button-row--end" style={{ marginTop: '12px' }}>
              <button type="button" className="primary-button" onClick={() => setIsEditing(true)}>
                Edit Details
              </button>
            </div>
          </>
        )}
      </section>
    </div>
  );
}

function ProductSummaryTable({
  headers,
  rows,
  emptyText,
  summaryColumns = PRODUCT_SUMMARY_COLUMNS,
  onOpenDetails,
  onAddToCart,
  showAmountPaidColumn = false,
  cartRowNumbers = [],
  onFulfillPendingDeal,
  onReturnPendingDeal,
  updatingPendingKey = '',
  returningPendingKey = '',
  isOverlayLoading = false,
}) {
  const cartRowSet = new Set(cartRowNumbers);
  const [pendingDrafts, setPendingDrafts] = useState({});
  const tableWrapRef = useRef(null);
  const tableScrollRef = useRef({ top: 0, left: 0 });

  useEffect(() => {
    if (!tableWrapRef.current) {
      return;
    }
    if (isOverlayLoading) {
      tableScrollRef.current = {
        top: tableWrapRef.current.scrollTop,
        left: tableWrapRef.current.scrollLeft,
      };
      return;
    }

    tableWrapRef.current.scrollTop = tableScrollRef.current.top || 0;
    tableWrapRef.current.scrollLeft = tableScrollRef.current.left || 0;
  }, [isOverlayLoading, rows.length]);

  function deriveRowPendingStatus(row, amountText, fallbackStatus = 'UNPAID') {
    const normalizedText = String(amountText || '').trim();
    if (!normalizedText) {
      return 'UNPAID';
    }

    const amountValue = parseAmountLike(normalizedText);
    if (amountValue <= 0) {
      return 'UNPAID';
    }

    const salePriceText = getProductCellValue(row, headers, ['AMOUNT SOLD', 'SELLING PRICE', 'PRICE']);
    const salePriceValue = parseAmountLike(salePriceText);
    if (salePriceValue <= 0) {
      return fallbackStatus;
    }
    if (amountValue < salePriceValue) {
      return 'PART PAYMENT';
    }
    if (amountValue === salePriceValue) {
      return 'PAID';
    }

    // Overpayment is validated by the apply handler.
    return fallbackStatus;
  }

  function getPendingRowDraft(row) {
    const key = `stock-${row.row_num}`;
    const baseStatus = String(row?.inventory_status || '').trim().toUpperCase();
    const normalizedBaseStatus = baseStatus === 'PAID' || baseStatus === 'PART PAYMENT' || baseStatus === 'UNPAID'
      ? baseStatus
      : 'UNPAID';
    return pendingDrafts[key] || { status: normalizedBaseStatus, amount_paid: '' };
  }

  function setPendingRowDraft(row, field, value) {
    const key = `stock-${row.row_num}`;
    const baseStatus = String(row?.inventory_status || '').trim().toUpperCase();
    const normalizedBaseStatus = baseStatus === 'PAID' || baseStatus === 'PART PAYMENT' || baseStatus === 'UNPAID'
      ? baseStatus
      : 'UNPAID';
    setPendingDrafts((prev) => ({
      ...prev,
      [key]: { ...(prev[key] || { status: normalizedBaseStatus, amount_paid: '' }), [field]: value },
    }));
  }

  return (
    <div ref={tableWrapRef} className={isOverlayLoading ? 'table-wrap table-wrap--mobile-cards table-wrap--loading' : 'table-wrap table-wrap--mobile-cards'}>
      <table className="data-table data-table--products">
        <thead>
          <tr>
            <th>See More</th>
            <th>Row</th>
            <th>Status</th>
            {summaryColumns.map((column) => (
              <th key={column.key}>{column.label}</th>
            ))}
            {showAmountPaidColumn ? <th>Amount Paid</th> : null}
            {onAddToCart ? <th>Cart</th> : null}
          </tr>
        </thead>
        <tbody>
          {rows.length ? (
            rows.map((row) => (
              <tr key={row.row_num}>
                <td data-label="See More">
                  <button type="button" className="table-action-button" onClick={() => onOpenDetails(row)}>
                    See More
                  </button>
                </td>
                <td className="row-number" data-label="Row">#{row.row_num}</td>
                <td data-label="Status">
                  <span className={`status-pill status-pill--${getStatusClass(row.label)}`}>{row.label}</span>
                </td>
                {summaryColumns.map((column) => (
                  <td key={`${row.row_num}-${column.key}`} data-label={column.label}>{getProductCellValue(row, headers, column.aliases)}</td>
                ))}
                {showAmountPaidColumn ? (
                  <td data-label="Amount Paid">{row.inventory_amount_paid || getProductCellValue(row, headers, ['AMOUNT PAID']) || '—'}</td>
                ) : null}
                {onAddToCart ? (
                  <td data-label="Cart">
                    {row.label === 'SOLD' ? null
                      : row.label === 'PENDING DEAL' ? (
                        <div className="inline-action-row">
                          <input
                            type="text"
                            inputMode="numeric"
                            value={getPendingRowDraft(row).amount_paid}
                            onChange={(e) => {
                              const nextAmount = normalizeDigits(e.target.value);
                              setPendingRowDraft(row, 'amount_paid', nextAmount);
                            }}
                            placeholder="Amount paid"
                            style={{ width: '100px' }}
                            disabled={updatingPendingKey === `stock-${row.row_num}` || returningPendingKey === `stock-${row.row_num}`}
                          />
                          <button
                            type="button"
                            className="primary-button"
                            onClick={() => onFulfillPendingDeal && onFulfillPendingDeal(
                              { kind: 'stock', row_num: row.row_num },
                              deriveRowPendingStatus(row, getPendingRowDraft(row).amount_paid, getPendingRowDraft(row).status || 'UNPAID'),
                              getPendingRowDraft(row).amount_paid
                            )}
                            disabled={updatingPendingKey === `stock-${row.row_num}` || returningPendingKey === `stock-${row.row_num}`}
                          >
                            {updatingPendingKey === `stock-${row.row_num}` ? 'Updating...' : 'Apply'}
                          </button>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => onReturnPendingDeal && onReturnPendingDeal({ kind: 'stock', row_num: row.row_num })}
                            disabled={updatingPendingKey === `stock-${row.row_num}` || returningPendingKey === `stock-${row.row_num}`}
                          >
                            {returningPendingKey === `stock-${row.row_num}` ? 'Returning...' : 'Return'}
                          </button>
                        </div>
                      ) : (
                        <button
                          type="button"
                          className="table-action-button"
                          onClick={() => onAddToCart(row)}
                          disabled={cartRowSet.has(row.row_num)}
                        >
                          {cartRowSet.has(row.row_num) ? 'Added' : 'Add To Cart'}
                        </button>
                      )
                    }
                  </td>
                ) : null}
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={summaryColumns.length + (showAmountPaidColumn ? 1 : 0) + (onAddToCart ? 4 : 3)} className="empty-state">
                {emptyText}
              </td>
            </tr>
          )}
        </tbody>
      </table>
      {isOverlayLoading ? (
        <div className="table-loading-overlay" role="status" aria-live="polite" aria-label="Loading filtered products">
          <span className="loading-spinner" aria-hidden="true" />
          <span>Updating filter...</span>
        </div>
      ) : null}
    </div>
  );
}

const MemoProductSummaryTable = React.memo(ProductSummaryTable);

function SwapIncomingDevicesModal({
  open,
  cartItem,
  stockForm,
  sellerPhoneOptions,
  currentTimeLabel,
  onClose,
  onSave,
}) {
  const visibleHeaders = stockForm?.visible_headers || [];
  const [entries, setEntries] = useState([]);
  const [activeIndex, setActiveIndex] = useState(0);

  useEffect(() => {
    if (!open || !cartItem) {
      return;
    }
    const existingEntries = Array.isArray(cartItem.swap_incoming_devices) ? cartItem.swap_incoming_devices : [];
    if (existingEntries.length) {
      setEntries(existingEntries.map((entry) => ({ values_by_header: { ...(entry?.values_by_header || {}) } })));
      setActiveIndex(0);
      return;
    }
    setEntries([{ values_by_header: buildProductFormValues(stockForm) }]);
    setActiveIndex(0);
  }, [open, cartItem, stockForm]);

  if (!open || !cartItem) {
    return null;
  }

  const activeEntry = entries[activeIndex] || { values_by_header: {} };

  function updateField(header, value) {
    setEntries((current) => current.map((entry, index) => (
      index === activeIndex
        ? { ...entry, values_by_header: { ...(entry.values_by_header || {}), [header]: value } }
        : entry
    )));
  }

  function addEntry() {
    setEntries((current) => {
      const next = [...current, { values_by_header: buildProductFormValues(stockForm) }];
      setActiveIndex(next.length - 1);
      return next;
    });
  }

  function removeEntry(indexToRemove) {
    setEntries((current) => {
      if (current.length <= 1) {
        return current;
      }
      const next = current.filter((_, index) => index !== indexToRemove);
      setActiveIndex((prev) => Math.max(0, Math.min(prev, next.length - 1)));
      return next;
    });
  }

  function handleSave() {
    const cleaned = (entries || []).map((entry) => {
      const values = {};
      Object.entries(entry?.values_by_header || {}).forEach(([header, value]) => {
        values[header] = String(value ?? '').trim();
      });
      return { values_by_header: values };
    }).filter((entry) => {
      const description = getValueByHeaderAliases(entry.values_by_header, ['DESCRIPTION', 'MODEL', 'DEVICE']);
      const imei = getValueByHeaderAliases(entry.values_by_header, ['IMEI']);
      return Boolean(description || imei);
    });

    onSave(cleaned);
    onClose();
  }

  function renderField(header) {
    const key = String(header || '').toUpperCase();
    const value = activeEntry?.values_by_header?.[header] || '';

    if (key === 'TIME') {
      return (
        <input
          type="text"
          value={value || currentTimeLabel}
          onChange={(event) => updateField(header, event.target.value)}
          placeholder="Auto time"
        />
      );
    }

    if (key === 'PHONE NUMBER OF SELLER') {
      return (
        <>
          <input
            type="text"
            list="swap-seller-phone-options"
            value={value}
            onChange={(event) => updateField(header, extractPhoneFromSuggestionText(event.target.value))}
            placeholder="Search Google/saved contact numbers"
          />
          <datalist id="swap-seller-phone-options">
            {(sellerPhoneOptions || []).map((option) => (
              <option key={option} value={option} />
            ))}
          </datalist>
        </>
      );
    }

    if (key === 'PRODUCT STATUS' || key === 'STOCK STATUS' || key === 'ITEM STATUS') {
      return (
        <select value={value || 'AVAILABLE'} onChange={(event) => updateField(header, event.target.value)}>
          <option value="AVAILABLE">AVAILABLE</option>
          <option value="SOLD">SOLD</option>
        </select>
      );
    }

    return (
      <input
        type="text"
        value={value}
        onChange={(event) => updateField(header, event.target.value)}
      />
    );
  }

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section className="modal-sheet" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
        <div className="modal-sheet__header">
          <div className="panel-header">
            <h3>Incoming Swap Devices</h3>
            <p>Fill full stock details for each incoming swap phone. These entries will be stocked automatically at checkout.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>Close</button>
        </div>

        <div className="panel-toolbar">
          <div className="filter-tabs" role="tablist" aria-label="Incoming swap devices">
            {entries.map((entry, index) => {
              const label = formatSwapDeviceLabel(entry?.values_by_header || {});
              return (
                <button
                  key={`incoming-device-${index}`}
                  type="button"
                  className={index === activeIndex ? 'filter-tab active' : 'filter-tab'}
                  onClick={() => setActiveIndex(index)}
                >
                  {label || `Device ${index + 1}`}
                </button>
              );
            })}
          </div>

          <div className="button-row button-row--end" style={{ margin: 0 }}>
            <button type="button" className="secondary-button" onClick={addEntry}>Add Another</button>
            <button
              type="button"
              className="secondary-button"
              onClick={() => removeEntry(activeIndex)}
              disabled={entries.length <= 1}
            >
              Remove Current
            </button>
          </div>
        </div>

        <div className="form-grid form-grid--modal">
          {visibleHeaders.map((header) => (
            <label key={header} className="field-block">
              <span className="field-label">{header}</span>
              {renderField(header)}
            </label>
          ))}
        </div>

        <div className="button-row button-row--end">
          <button type="button" className="secondary-button" onClick={onClose}>Cancel</button>
          <button type="button" className="primary-button" onClick={handleSave}>Save Incoming Devices</button>
        </div>
      </section>
    </div>
  );
}

function CartView({
  stockView,
  stockViewRaw,
  stockForm,
  productSearchText,
  setProductSearchText,
  filterMode,
  setFilterMode,
  cartPage,
  setCartPage,
  isLoading,
  isRefreshing,
  errorText,
  onRefresh,
  selectedProductDetail,
  onOpenDetails,
  onCloseProductDetails,
  onSaveProductDetails,
  savingProductDetails,
  onAddToCart,
  cartItems,
  onUpdateCartItem,
  onRemoveCartItem,
  onReturnCartItem,
  onCheckoutCart,
  serviceDraft,
  setServiceDraft,
  onSubmitService,
  serviceBusy,
  pendingDealEntries,
  onReturnPendingDeal,
  onUpdatePendingDealPayment,
  onUpdatePendingDealMeta,
  returningPendingKey,
  updatingPendingKey,
  updatingPendingMetaKey,
  clientNameOptions,
  sellerPhoneOptions,
  contactAutofillOptions,
  currentTimeLabel,
  cartBusy,
  summaryColumns,
}) {
  const headers = stockView?.headers || [];
  const rows = stockView?.all_rows_cache || [];
  const rowsPerPage = 12;
  const totalPages = Math.max(1, Math.ceil(rows.length / rowsPerPage));
  const currentPage = Math.min(cartPage, totalPages);
  const pagedRows = rows.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage);
  const [cartModalOpen, setCartModalOpen] = useState(false);
  const [serviceModalOpen, setServiceModalOpen] = useState(false);
  const [pendingModalOpen, setPendingModalOpen] = useState(false);
  const [swapDeviceModalRowNum, setSwapDeviceModalRowNum] = useState(null);
  const [pendingSearchText, setPendingSearchText] = useState('');
  const [pendingPaymentDrafts, setPendingPaymentDrafts] = useState({});

  function resolveContactOption(rawValue) {
    const text = String(rawValue || '').trim();
    return (contactAutofillOptions || []).find((option) => option.label === text) || null;
  }

  function derivePendingDraftFromRow(row) {
    const rawStatus = String(row?.inventory_status || row?.status || '').trim().toUpperCase();
    const status = rawStatus === 'PAID' || rawStatus === 'PART PAYMENT' || rawStatus === 'UNPAID'
      ? rawStatus
      : 'UNPAID';
    const paymentMethod = String(row?.payment_method || '').trim().toUpperCase();
    const fulfillmentMethod = String(row?.fulfillment_method || '').trim().toUpperCase();
    const pickupMode = String(row?.pickup_mode || '').trim().toUpperCase();
    const swapType = String(row?.swap_type || '').trim().toUpperCase();
    return {
      status,
      amount_paid: String(row?.inventory_amount_paid || '').trim(),
      payment_method: paymentMethod || 'CASH',
      fulfillment_method: fulfillmentMethod || 'WALK-IN PICKUP',
      pickup_mode: pickupMode || 'BUYER',
      representative_name: String(row?.representative_name || '').trim(),
      representative_phone: String(row?.representative_phone || '').trim(),
      is_swap: Boolean(String(swapType || '').trim()),
      swap_type: swapType || 'UPGRADE',
      swap_devices: String(row?.swap_detail || '').trim(),
      swap_cash_amount: normalizeDigits(String(row?.swap_cash_amount || '').trim()),
    };
  }

  function getPendingDraft(row) {
    const key = `${row?.kind || 'stock'}-${row?.row_num || ''}`;
    if (!key) {
      return { status: 'UNPAID', amount_paid: '' };
    }
    return pendingPaymentDrafts[key] || derivePendingDraftFromRow(row);
  }

  function updatePendingDraft(rowKey, field, value) {
    const key = String(rowKey || '');
    setPendingPaymentDrafts((current) => ({
      ...current,
      [key]: {
        status: current[key]?.status || 'UNPAID',
        amount_paid: current[key]?.amount_paid || '',
        payment_method: current[key]?.payment_method || 'CASH',
        fulfillment_method: current[key]?.fulfillment_method || 'WALK-IN PICKUP',
        pickup_mode: current[key]?.pickup_mode || 'BUYER',
        representative_name: current[key]?.representative_name || '',
        representative_phone: current[key]?.representative_phone || '',
        is_swap: Boolean(current[key]?.is_swap),
        swap_type: current[key]?.swap_type || 'UPGRADE',
        swap_devices: current[key]?.swap_devices || '',
        swap_cash_amount: current[key]?.swap_cash_amount || '',
        [field]: value,
      },
    }));
  }

  const activeSwapModalItem = (cartItems || []).find((item) => Number(item.stock_row_num) === Number(swapDeviceModalRowNum)) || null;

  function saveIncomingSwapDevices(rowNum, devices) {
    const incomingList = Array.isArray(devices) ? devices : [];
    onUpdateCartItem(rowNum, 'swap_incoming_devices', incomingList);
    onUpdateCartItem(
      rowNum,
      'swap_devices',
      incomingList
        .map((entry) => formatSwapDeviceLabel(entry?.values_by_header || {}))
        .filter(Boolean)
        .join('\n')
    );
  }

  function isPaidPhoneMissingCost(item) {
    if (!item || !item.imei) {
      return false;
    }
    const statusText = String(item.payment_status || '').trim().toUpperCase();
    if (statusText !== 'PAID') {
      return false;
    }
    return parseAmountLike(item.cost_price) <= 0;
  }

  const hasPaidPhoneWithoutCost = (cartItems || []).some((item) => isPaidPhoneMissingCost(item));

  function derivePendingStatusFromAmount(row, amountText, fallbackStatus = 'UNPAID') {
    const normalizedText = String(amountText || '').trim();
    if (!normalizedText) {
      return fallbackStatus;
    }

    const amountValue = parseAmountLike(normalizedText);
    if (amountValue <= 0) {
      return 'UNPAID';
    }

    const salePriceValue = parseAmountLike(row?.price || row?.amount);
    if (salePriceValue <= 0) {
      return fallbackStatus;
    }
    if (amountValue < salePriceValue) {
      return 'PART PAYMENT';
    }
    if (amountValue === salePriceValue) {
      return 'PAID';
    }

    // Keep current status for overpayment; Apply handler will validate and prompt redirect.
    return fallbackStatus;
  }

  const filteredPendingDeals = useMemo(() => {
    const query = normalizeSearchValue(pendingSearchText);
    if (!query) {
      return pendingDealEntries || [];
    }
    return (pendingDealEntries || []).filter((row) => {
      const haystack = [
        row.kind,
        row.description,
        row.date,
        row.buyer_name,
        row.name,
        row.buyer_phone,
        row.phone,
        row.imei,
        row.payment_method,
        row.fulfillment_method,
        row.pickup_mode,
        row.representative_name,
        row.representative_phone,
        row.swap_type,
        row.swap_detail,
        row.row_num,
      ].map((value) => String(value || '')).join(' ');
      return normalizeSearchValue(haystack).includes(query);
    });
  }, [pendingDealEntries, pendingSearchText]);

  const [swapHistoryTypeFilter, setSwapHistoryTypeFilter] = React.useState('ALL');
  const [swapHistoryDateFrom, setSwapHistoryDateFrom] = React.useState('');
  const [swapHistoryDateTo, setSwapHistoryDateTo] = React.useState('');

  const swapHistoryEntries = useMemo(() => {
    const sourceHeaders = stockViewRaw?.headers || headers;
    const sourceRows = stockViewRaw?.all_rows_cache || rows;
    const entries = (sourceRows || []).map((row) => {
      const swapType = String(getProductCellValue(row, sourceHeaders, ['SWAP TYPE']) || '').trim().toUpperCase();
      const swapDetail = String(getProductCellValue(row, sourceHeaders, ['SWAP DETAIL', 'SWAP DETAILS']) || '').trim();
      const swapCash = String(getProductCellValue(row, sourceHeaders, ['SWAP CASH AMOUNT', 'SWAP CASH']) || '').trim();
      if (!swapType && !swapDetail && !swapCash) {
        return null;
      }

      return {
        row_num: row.row_num,
        status: String(row.label || '').trim() || '—',
        date: getProductCellValue(row, sourceHeaders, ['AVAILABILITY/DATE SOLD', 'DATE SOLD', 'SOLD DATE']) || '—',
        buyer_name: getProductCellValue(row, sourceHeaders, ['NAME OF BUYER']) || '—',
        description: getProductCellValue(row, sourceHeaders, ['DESCRIPTION', 'MODEL', 'DEVICE']) || '—',
        imei: getProductCellValue(row, sourceHeaders, ['IMEI']) || '—',
        swap_type: swapType || '—',
        swap_detail: swapDetail || '—',
        swap_cash: swapCash || '—',
      };
    }).filter(Boolean);

    let filtered = entries.sort((left, right) => Number(right.row_num || 0) - Number(left.row_num || 0));

    if (swapHistoryTypeFilter !== 'ALL') {
      filtered = filtered.filter((e) => e.swap_type === swapHistoryTypeFilter);
    }

    if (swapHistoryDateFrom) {
      filtered = filtered.filter((e) => {
        const d = String(e.date || '');
        return d >= swapHistoryDateFrom;
      });
    }

    if (swapHistoryDateTo) {
      filtered = filtered.filter((e) => {
        const d = String(e.date || '');
        return d <= swapHistoryDateTo;
      });
    }

    return filtered.slice(0, 40);
  }, [headers, rows, stockViewRaw?.all_rows_cache, stockViewRaw?.headers, swapHistoryTypeFilter, swapHistoryDateFrom, swapHistoryDateTo]);

  return (
    <div className="workspace-stack">
      <section className="content-panel content-panel--main content-panel--full">
        <div className="panel-header">
          <h3>Sell From Stock</h3>
          <p>Pick phones from stock, set buyer details, and queue each sale cleanly into both stock and inventory sheets. Time is auto-captured from your device clock ({currentTimeLabel}).</p>
        </div>

        <div className="panel-toolbar">
          <div className="search-group">
            <label htmlFor="cart-search">Search stock for cart:</label>
            <input
              id="cart-search"
              type="search"
              placeholder="IMEI, model, seller, buyer..."
              value={productSearchText}
              onChange={(event) => setProductSearchText(event.target.value)}
            />
          </div>

          <div className="toolbar-controls">
            <div className="filter-tabs" role="tablist" aria-label="Cart stock filters">
              {CART_FILTERS.map((option) => (
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
        {isLoading ? <div className="notice">Loading stock for the cart...</div> : null}

        <MemoProductSummaryTable
          headers={headers}
          rows={pagedRows}
          emptyText="No stock items are ready for the current cart filter."
          summaryColumns={summaryColumns}
          onOpenDetails={onOpenDetails}
          onAddToCart={onAddToCart}
          showAmountPaidColumn={filterMode === 'pending'}
          cartRowNumbers={cartItems.map((item) => item.stock_row_num)}
          onFulfillPendingDeal={onUpdatePendingDealPayment}
          onReturnPendingDeal={onReturnPendingDeal}
          updatingPendingKey={updatingPendingKey}
          returningPendingKey={returningPendingKey}
          isOverlayLoading={isRefreshing}
        />

        <div className="page-nav-wrap">
          <PageNavigator page={currentPage} totalPages={totalPages} onChange={setCartPage} />
        </div>
      </section>

      <section className="content-panel content-panel--main content-panel--full">
        <div className="panel-header">
          <h3>Swap History</h3>
          <p>Live history of swap deals showing source-to-target details, IMEIs, and any cash adjustment.</p>
        </div>

        <div className="panel-toolbar">
          <div className="filter-tabs" role="tablist" aria-label="Swap type filter">
            {['ALL', 'UPGRADE', 'DOWNGRADE'].map((tab) => (
              <button
                key={tab}
                type="button"
                className={swapHistoryTypeFilter === tab ? 'filter-tab active' : 'filter-tab'}
                onClick={() => setSwapHistoryTypeFilter(tab)}
              >
                {tab === 'ALL' ? 'All' : tab.charAt(0) + tab.slice(1).toLowerCase()}
              </button>
            ))}
          </div>

          <div className="search-group" style={{ flexDirection: 'row', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
            <label style={{ whiteSpace: 'nowrap' }}>From:</label>
            <input
              type="date"
              value={swapHistoryDateFrom}
              onChange={(e) => setSwapHistoryDateFrom(e.target.value)}
              style={{ width: 'auto' }}
            />
            <label style={{ whiteSpace: 'nowrap' }}>To:</label>
            <input
              type="date"
              value={swapHistoryDateTo}
              onChange={(e) => setSwapHistoryDateTo(e.target.value)}
              style={{ width: 'auto' }}
            />
            {(swapHistoryDateFrom || swapHistoryDateTo) && (
              <button
                type="button"
                className="secondary-button"
                onClick={() => { setSwapHistoryDateFrom(''); setSwapHistoryDateTo(''); }}
              >
                Clear
              </button>
            )}
          </div>
        </div>

        <div className="table-wrap table-wrap--mobile-cards">
          <table className="data-table">
            <thead>
              <tr>
                <th>Row</th>
                <th>Status</th>
                <th>Date</th>
                <th>Buyer</th>
                <th>Sold Device</th>
                <th>IMEI</th>
                <th>Swap Type</th>
                <th>Swap Cash</th>
                <th>Swap Detail</th>
              </tr>
            </thead>
            <tbody>
              {swapHistoryEntries.length ? swapHistoryEntries.map((entry) => (
                <tr key={`swap-history-${entry.row_num}`}>
                  <td data-label="Row">#{entry.row_num}</td>
                  <td data-label="Status">{entry.status}</td>
                  <td data-label="Date">{entry.date}</td>
                  <td data-label="Buyer">{entry.buyer_name}</td>
                  <td data-label="Sold Device">{entry.description}</td>
                  <td data-label="IMEI">{entry.imei}</td>
                  <td data-label="Swap Type">{entry.swap_type}</td>
                  <td data-label="Swap Cash">{entry.swap_cash}</td>
                  <td data-label="Swap Detail">{entry.swap_detail}</td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={9} className="empty-state">No swap history found yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {selectedProductDetail ? (
        <ProductDetailModal
          row={selectedProductDetail}
          headers={headers}
          onClose={onCloseProductDetails}
          onSave={onSaveProductDetails}
          saving={savingProductDetails}
        />
      ) : null}

      <datalist id="buyer-name-options">
        {(clientNameOptions || []).map((name) => (
          <option key={name} value={name} />
        ))}
      </datalist>
      <datalist id="buyer-phone-options">
        {(sellerPhoneOptions || []).map((phoneLabel) => (
          <option key={phoneLabel} value={phoneLabel} />
        ))}
      </datalist>
      <datalist id="contact-autofill-options">
        {(contactAutofillOptions || []).map((option) => (
          <option key={option.label} value={option.label} />
        ))}
      </datalist>

      <div className="floating-action-stack">
        <button
          type="button"
          className="floating-action-button"
          onClick={() => setCartModalOpen(true)}
          aria-label="Open sales cart"
          title="Sales cart"
        >
          🧾
          <span className="floating-action-badge">{formatCount(cartItems.length)}</span>
        </button>
        <button
          type="button"
          className="floating-action-button"
          onClick={() => setPendingModalOpen(true)}
          aria-label="Open pending deals - phone and service"
          title="Service Pending Deal"
        >
          ⏳
          <span className="floating-action-badge">{formatCount((pendingDealEntries || []).length)}</span>
        </button>
        <button
          type="button"
          className="floating-action-button floating-action-button--service"
          onClick={() => setServiceModalOpen(true)}
          aria-label="Open add service"
          title="Add service"
        >
          🛠
        </button>
      </div>

      {cartModalOpen ? (
        <div className="modal-backdrop" role="presentation" onClick={() => setCartModalOpen(false)}>
          <section className="modal-sheet modal-sheet--detail" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="modal-sheet__header">
              <div className="panel-header">
                <h3>Sales Cart</h3>
                <p>Each cart line writes buyer details to stock and appends a new inventory row without overwriting existing text.</p>
              </div>
              <button type="button" className="secondary-button" onClick={() => setCartModalOpen(false)}>Close</button>
            </div>

            <div className="cart-items">
              {cartItems.length ? (
                cartItems.map((item) => {
                  const paidPhoneMissingCost = isPaidPhoneMissingCost(item);
                  return (
                  <article key={item.stock_row_num} className="cart-item">
                    <div className="cart-item__header">
                      <div>
                        <strong>#{item.stock_row_num} {item.description || 'Selected phone'}</strong>
                        <span className="cart-item__meta">IMEI: {item.imei || '—'} | Selling Price: {item.sale_price || item.cost_price || '—'} | Amount Paid: {item.amount_paid || '—'}</span>
                        {paidPhoneMissingCost ? (
                          <p className="notice compact notice-error" style={{ marginTop: '8px' }}>
                            COST PRICE is missing. This phone cannot be sold as PAID until cost price is added.
                          </p>
                        ) : null}
                      </div>

                      <button type="button" className="secondary-button" onClick={() => onRemoveCartItem(item.stock_row_num)} disabled={cartBusy}>
                        Remove
                      </button>
                      <button type="button" className="secondary-button" onClick={() => onReturnCartItem(item.stock_row_num)} disabled={cartBusy}>
                        Return
                      </button>
                    </div>

                    <div className="cart-item__grid">
                      <label className="field-block field-block--wide">
                        <span className="field-label">Contact Autofill</span>
                        <input
                          type="text"
                          list="contact-autofill-options"
                          defaultValue=""
                          onChange={(event) => {
                            const match = resolveContactOption(event.target.value);
                            if (!match) {
                              return;
                            }
                            onUpdateCartItem(item.stock_row_num, 'buyer_name', match.name);
                            onUpdateCartItem(item.stock_row_num, 'buyer_phone', match.phone);
                          }}
                          placeholder="Pick Google/client contact to autofill"
                        />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Buyer Name</span>
                        <input
                          type="text"
                          list="buyer-name-options"
                          value={item.buyer_name}
                          onChange={(event) => onUpdateCartItem(item.stock_row_num, 'buyer_name', event.target.value)}
                          placeholder="Buyer name"
                        />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Buyer Phone</span>
                        <input
                          type="text"
                          inputMode="tel"
                          list="buyer-phone-options"
                          value={item.buyer_phone}
                          onChange={(event) => onUpdateCartItem(item.stock_row_num, 'buyer_phone', extractPhoneFromSuggestionText(event.target.value))}
                          placeholder="090..., 23490..., or +23490..."
                        />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Amount Sold</span>
                        <input
                          type="text"
                          inputMode="numeric"
                          value={item.sale_price}
                          onChange={(event) => onUpdateCartItem(item.stock_row_num, 'sale_price', normalizeDigits(event.target.value))}
                          placeholder="Selling price"
                        />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Amount Paid</span>
                        <input
                          type="text"
                          inputMode="numeric"
                          value={item.amount_paid}
                          onChange={(event) => onUpdateCartItem(item.stock_row_num, 'amount_paid', normalizeDigits(event.target.value))}
                          placeholder="Amount received"
                        />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Phone Expense (Optional)</span>
                        <input
                          type="text"
                          inputMode="numeric"
                          value={item.phone_expense}
                          onChange={(event) => onUpdateCartItem(item.stock_row_num, 'phone_expense', normalizeDigits(event.target.value))}
                          placeholder="Cost incurred for this sale"
                        />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Payment Method</span>
                        <select value={item.payment_method || 'CASH'} onChange={(event) => onUpdateCartItem(item.stock_row_num, 'payment_method', event.target.value)}>
                          <option value="CASH">Cash</option>
                          <option value="TRANSFER">Transfer</option>
                        </select>
                      </label>

                      <label className="field-block">
                        <span className="field-label">Fulfillment Method</span>
                        <select value={item.fulfillment_method || 'WALK-IN PICKUP'} onChange={(event) => onUpdateCartItem(item.stock_row_num, 'fulfillment_method', event.target.value)}>
                          <option value="WALK-IN PICKUP">Walk-in Pickup</option>
                          <option value="WAYBILL">Waybill</option>
                          <option value="IN OFFICE">In Office</option>
                          <option value="OFF OFFICE">Off Office</option>
                        </select>
                      </label>

                      {String(item.fulfillment_method || '').toUpperCase() === 'OFF OFFICE' ? (
                        <label className="field-block">
                          <span className="field-label">Deal Location</span>
                          <input
                            type="text"
                            value={item.deal_location || ''}
                            onChange={(event) => onUpdateCartItem(item.stock_row_num, 'deal_location', event.target.value)}
                            placeholder="Where was the deal done?"
                          />
                        </label>
                      ) : null}

                      <label className="field-block field-block--wide">
                        <span className="field-label">Internal Note</span>
                        <input
                          type="text"
                          value={item.internal_note || ''}
                          onChange={(event) => onUpdateCartItem(item.stock_row_num, 'internal_note', event.target.value)}
                          placeholder="Optional internal note"
                        />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Pickup By</span>
                        <select value={item.pickup_mode || 'BUYER'} onChange={(event) => onUpdateCartItem(item.stock_row_num, 'pickup_mode', event.target.value)}>
                          <option value="BUYER">Buyer</option>
                          <option value="REPRESENTATIVE">Representative</option>
                        </select>
                      </label>

                      {String(item.pickup_mode || '').toUpperCase() === 'REPRESENTATIVE' ? (
                        <>
                          <label className="field-block">
                            <span className="field-label">Representative Name</span>
                            <input
                              type="text"
                              value={item.representative_name || ''}
                              onChange={(event) => onUpdateCartItem(item.stock_row_num, 'representative_name', event.target.value)}
                              placeholder="Person sent to pick up"
                            />
                          </label>

                          <label className="field-block">
                            <span className="field-label">Representative Phone</span>
                            <input
                              type="text"
                              inputMode="tel"
                              value={item.representative_phone || ''}
                              onChange={(event) => onUpdateCartItem(item.stock_row_num, 'representative_phone', event.target.value)}
                              placeholder="080..., 234..., +234..."
                            />
                          </label>
                        </>
                      ) : null}

                      <label className="field-block field-block--wide">
                        <span className="field-label">Swap Deal</span>
                        <select value={item.is_swap ? 'YES' : 'NO'} onChange={(event) => onUpdateCartItem(item.stock_row_num, 'is_swap', event.target.value === 'YES')}>
                          <option value="NO">No</option>
                          <option value="YES">Yes</option>
                        </select>
                      </label>

                      {item.is_swap ? (
                        <>
                          <label className="field-block">
                            <span className="field-label">Swap Type</span>
                            <select value={item.swap_type || 'UPGRADE'} onChange={(event) => onUpdateCartItem(item.stock_row_num, 'swap_type', event.target.value)}>
                              <option value="UPGRADE">Upgrade</option>
                              <option value="DOWNGRADE">Downgrade</option>
                            </select>
                          </label>

                          <label className="field-block">
                            <span className="field-label">Swap Cash Amount</span>
                            <input
                              type="text"
                              inputMode="numeric"
                              value={item.swap_cash_amount || ''}
                              onChange={(event) => onUpdateCartItem(item.stock_row_num, 'swap_cash_amount', normalizeDigits(event.target.value))}
                              placeholder={item.swap_type === 'DOWNGRADE' ? 'Cash given to customer' : 'Cash added by customer'}
                            />
                          </label>

                          <div className="field-block field-block--wide" style={{ alignSelf: 'stretch' }}>
                            <span className="field-label">Incoming Swap Device(s)</span>
                            <div className="button-row" style={{ marginTop: '8px' }}>
                              <button
                                type="button"
                                className="secondary-button"
                                onClick={() => setSwapDeviceModalRowNum(item.stock_row_num)}
                              >
                                Manage Incoming Swap Devices
                              </button>
                              <span className="metric-note">
                                {Array.isArray(item.swap_incoming_devices) && item.swap_incoming_devices.length
                                  ? `${item.swap_incoming_devices.length} device(s) configured`
                                  : 'No incoming device configured yet'}
                              </span>
                            </div>
                          </div>
                        </>
                      ) : null}

                      <label className="field-block field-block--wide">
                        <span className="field-label">Payment Status (Auto)</span>
                        <input type="text" value={item.payment_status || 'UNPAID'} readOnly />
                      </label>

                      <label className="field-block">
                        <span className="field-label">Availability</span>
                        <select value={item.availability_choice || 'AUTO'} onChange={(event) => onUpdateCartItem(item.stock_row_num, 'availability_choice', event.target.value)}>
                          <option value="AUTO">Auto</option>
                          <option value="TODAY">Today</option>
                          <option value="PENDING">Pending Deal</option>
                          <option value="CLEAR">Clear</option>
                          <option value="CUSTOM">Custom</option>
                        </select>
                      </label>

                      {item.availability_choice === 'CUSTOM' ? (
                        <label className="field-block field-block--wide">
                          <span className="field-label">Custom Availability Text</span>
                          <input
                            type="text"
                            value={item.availability_custom || ''}
                            onChange={(event) => onUpdateCartItem(item.stock_row_num, 'availability_custom', event.target.value)}
                            placeholder="e.g. 03/08/2026 or RESERVED"
                          />
                        </label>
                      ) : null}
                    </div>
                  </article>
                );
                })
              ) : (
                <div className="notice">Add phones from the stock list to start the cart.</div>
              )}
            </div>

            <div className="button-row button-row--end">
              <button type="button" className="primary-button" onClick={onCheckoutCart} disabled={cartBusy || !cartItems.length || hasPaidPhoneWithoutCost}>
                {cartBusy ? 'Selling Phones...' : 'Sell Out Cart'}
              </button>
            </div>
            {hasPaidPhoneWithoutCost ? (
              <p className="notice compact notice-error" style={{ margin: '10px 0 0' }}>
                Add COST PRICE to every PAID phone in cart before checkout.
              </p>
            ) : null}
          </section>
        </div>
      ) : null}

      {serviceModalOpen ? (
        <div className="modal-backdrop" role="presentation" onClick={() => setServiceModalOpen(false)}>
          <section className="modal-sheet" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="modal-sheet__header">
              <div className="panel-header">
                <h3>Add Service</h3>
                <p>Create a non-stock service row directly in inventory.</p>
              </div>
              <button type="button" className="secondary-button" onClick={() => setServiceModalOpen(false)}>Close</button>
            </div>

            <div className="form-grid form-grid--modal">
              <label className="field-block">
                <span className="field-label">Contact Autofill</span>
                <input
                  type="text"
                  list="contact-autofill-options"
                  defaultValue=""
                  onChange={(event) => {
                    const match = resolveContactOption(event.target.value);
                    if (!match) {
                      return;
                    }
                    setServiceDraft((current) => ({ ...current, name: match.name, phone: match.phone }));
                  }}
                  placeholder="Pick Google/client contact to autofill"
                />
              </label>

              <label className="field-block">
                <span className="field-label">Customer Name</span>
                <input
                  type="text"
                  list="buyer-name-options"
                  value={serviceDraft.name}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, name: event.target.value }))}
                  placeholder="Customer name"
                />
              </label>

              <label className="field-block">
                <span className="field-label">Phone Number</span>
                <input
                  type="text"
                  inputMode="tel"
                  list="buyer-phone-options"
                  value={serviceDraft.phone}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, phone: extractPhoneFromSuggestionText(event.target.value) }))}
                  placeholder="080..., 234..., +234..."
                />
              </label>

              <label className="field-block field-block--wide">
                <span className="field-label">Description</span>
                <input
                  type="text"
                  value={serviceDraft.description}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, description: event.target.value }))}
                  placeholder="Service description"
                />
              </label>

              <label className="field-block">
                <span className="field-label">Fulfillment Method</span>
                <select
                  value={serviceDraft.fulfillment_method || 'WALK-IN PICKUP'}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, fulfillment_method: event.target.value, deal_location: event.target.value !== 'OFF OFFICE' ? '' : current.deal_location }))}
                >
                  <option value="WALK-IN PICKUP">Walk-in Pickup</option>
                  <option value="WAYBILL">Waybill</option>
                  <option value="IN OFFICE">In Office</option>
                  <option value="OFF OFFICE">Off Office</option>
                </select>
              </label>

              {String(serviceDraft.fulfillment_method || '').toUpperCase() === 'OFF OFFICE' ? (
                <label className="field-block field-block--wide">
                  <span className="field-label">Deal Location</span>
                  <input
                    type="text"
                    value={serviceDraft.deal_location || ''}
                    onChange={(event) => setServiceDraft((current) => ({ ...current, deal_location: event.target.value }))}
                    placeholder="Where was the deal done?"
                  />
                </label>
              ) : null}

              <label className="field-block field-block--wide">
                <span className="field-label">Internal Note <span style={{fontWeight:'normal',fontSize:'0.85em'}}>(not included in bills)</span></span>
                <input
                  type="text"
                  value={serviceDraft.internal_note || ''}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, internal_note: event.target.value }))}
                  placeholder="Internal note — not sent to customer"
                />
              </label>

              <label className="field-block">
                <span className="field-label">Price</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={serviceDraft.price}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, price: normalizeDigits(event.target.value) }))}
                  placeholder="Amount charged"
                />
              </label>

              <label className="field-block">
                <span className="field-label">Service Expense</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={serviceDraft.service_expense}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, service_expense: normalizeDigits(event.target.value) }))}
                  placeholder="Cost incurred for this service"
                />
              </label>

              <label className="field-block">
                <span className="field-label">Amount Paid</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={serviceDraft.amount_paid}
                  onChange={(event) => setServiceDraft((current) => ({ ...current, amount_paid: normalizeDigits(event.target.value) }))}
                  placeholder="Amount received"
                />
              </label>

              <label className="field-block">
                <span className="field-label">Payment Method</span>
                <select value={serviceDraft.payment_method || 'CASH'} onChange={(event) => setServiceDraft((current) => ({ ...current, payment_method: event.target.value }))}>
                  <option value="CASH">Cash</option>
                  <option value="TRANSFER">Transfer</option>
                </select>
              </label>

              <label className="field-block">
                <span className="field-label">Pickup By</span>
                <select
                  value={serviceDraft.pickup_mode || 'BUYER'}
                  onChange={(event) => setServiceDraft((current) => ({
                    ...current,
                    pickup_mode: event.target.value,
                    representative_name: event.target.value === 'REPRESENTATIVE' ? current.representative_name : '',
                    representative_phone: event.target.value === 'REPRESENTATIVE' ? current.representative_phone : '',
                  }))}
                >
                  <option value="BUYER">Buyer</option>
                  <option value="REPRESENTATIVE">Representative</option>
                </select>
              </label>

              {String(serviceDraft.pickup_mode || '').toUpperCase() === 'REPRESENTATIVE' ? (
                <>
                  <label className="field-block">
                    <span className="field-label">Representative Name</span>
                    <input
                      type="text"
                      value={serviceDraft.representative_name || ''}
                      onChange={(event) => setServiceDraft((current) => ({ ...current, representative_name: event.target.value }))}
                      placeholder="Person sent by customer"
                    />
                  </label>

                  <label className="field-block">
                    <span className="field-label">Representative Phone</span>
                    <input
                      type="text"
                      inputMode="tel"
                      value={serviceDraft.representative_phone || ''}
                      onChange={(event) => setServiceDraft((current) => ({ ...current, representative_phone: extractPhoneFromSuggestionText(event.target.value) }))}
                      placeholder="080..., 234..., +234..."
                    />
                  </label>
                </>
              ) : null}

              <label className="field-block">
                <span className="field-label">Status</span>
                <input
                  type="text"
                  readOnly
                  value={(() => {
                    const priceValue = Number.parseInt(serviceDraft.price || '0', 10) || 0;
                    const paidValue = Number.parseInt(serviceDraft.amount_paid || '0', 10) || 0;
                    if (paidValue <= 0) return 'UNPAID';
                    if (priceValue > 0 && paidValue < priceValue) return 'PART PAYMENT';
                    return 'PAID';
                  })()}
                />
              </label>
            </div>

            <div className="button-row button-row--end">
              <button type="button" className="primary-button" onClick={onSubmitService} disabled={serviceBusy}>
                {serviceBusy ? 'Adding Service...' : 'Add Service'}
              </button>
            </div>
          </section>
        </div>
      ) : null}

      {pendingModalOpen ? (
        <div className="modal-backdrop" role="presentation" onClick={() => setPendingModalOpen(false)}>
          <section className="modal-sheet" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="modal-sheet__header">
              <div className="panel-header">
                <h3>Phone & Service Pending Deals</h3>
                <p>Manage both phone pending deals and service pending deals here. Search, update payment fulfillment, or return/refund unsuccessful deals.</p>
              </div>
              <button type="button" className="secondary-button" onClick={() => setPendingModalOpen(false)}>Close</button>
            </div>
            <div className="search-group pending-search-group">
              <label htmlFor="pending-deal-search">Search pending deals:</label>
              <input
                id="pending-deal-search"
                type="search"
                placeholder="Search by type, customer, phone, description, IMEI..."
                value={pendingSearchText}
                onChange={(event) => setPendingSearchText(event.target.value)}
              />
            </div>
            <div className="table-wrap table-wrap--mobile-cards">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Type</th>
                    <th>Row</th>
                    <th>Description</th>
                    <th>Customer</th>
                    <th>Date</th>
                    <th>Sale Price</th>
                    <th>Amount Paid</th>
                    <th>Amount Entry</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredPendingDeals.length ? filteredPendingDeals.map((row) => {
                    const draft = getPendingDraft(row);
                    const rowKey = `${row.kind || 'stock'}-${row.row_num}`;
                    const rowBusy = returningPendingKey === rowKey || updatingPendingKey === rowKey || updatingPendingMetaKey === rowKey;
                    return (
                      <tr key={`cart-pending-${rowKey}`}>
                        <td data-label="Type">{row.kind === 'service' ? 'Service' : 'Stock'}</td>
                        <td data-label="Row">#{row.row_num}</td>
                        <td data-label="Description">{row.description || getProductCellValue(row, headers, ['DESCRIPTION', 'MODEL', 'DEVICE']) || '—'}</td>
                        <td data-label="Customer">{row.buyer_name || row.name || getProductCellValue(row, headers, ['NAME OF BUYER']) || '—'}</td>
                        <td data-label="Date">{row.date || getProductCellValue(row, headers, ['DATE', 'DATE BOUGHT', 'AVAILABILITY/DATE SOLD']) || '—'}</td>
                        <td data-label="Sale Price">{row.price || row.amount || '—'}</td>
                        <td data-label="Amount Paid">{row.amount_paid || row.inventory_amount_paid || '—'}</td>
                        <td data-label="Amount Entry">
                          <div className="inline-action-row">
                            <input
                              type="text"
                              inputMode="numeric"
                              value={draft.amount_paid}
                              onChange={(event) => {
                                const nextAmount = normalizeDigits(event.target.value);
                                updatePendingDraft(rowKey, 'amount_paid', nextAmount);
                              }}
                              placeholder="Amount paid"
                              disabled={rowBusy}
                            />
                          </div>
                        </td>
                        <td data-label="Action">
                          <div className="inline-action-row">
                            <button
                              type="button"
                              className="primary-button"
                              onClick={() => onUpdatePendingDealPayment(
                                row,
                                derivePendingStatusFromAmount(row, draft.amount_paid, draft.status),
                                draft.amount_paid
                              )}
                              disabled={rowBusy}
                            >
                              {updatingPendingKey === rowKey ? 'Updating...' : 'Apply'}
                            </button>
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => onReturnPendingDeal(row)}
                              disabled={rowBusy}
                            >
                              {returningPendingKey === rowKey ? 'Returning...' : 'Return'}
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  }) : (
                    <tr>
                      <td colSpan={8} className="empty-state">No pending deals matched the current filter.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      ) : null}

      <SwapIncomingDevicesModal
        open={Boolean(activeSwapModalItem)}
        cartItem={activeSwapModalItem}
        stockForm={stockForm}
        sellerPhoneOptions={sellerPhoneOptions}
        currentTimeLabel={currentTimeLabel}
        onClose={() => setSwapDeviceModalRowNum(null)}
        onSave={(devices) => {
          if (!activeSwapModalItem) {
            return;
          }
          saveIncomingSwapDevices(activeSwapModalItem.stock_row_num, devices);
        }}
      />
    </div>
  );
}

function ProductsView({
  stockView,
  stockForm,
  productFormValues,
  setProductFormValues,
  productSearchText,
  setProductSearchText,
  filterMode,
  setFilterMode,
  stockPage,
  setStockPage,
  isLoading,
  isRefreshing,
  isAddingProduct,
  isProductComposerOpen,
  errorText,
  onRefresh,
  selectedProductDetail,
  onOpenProductDetails,
  onCloseProductDetails,
  onSaveProductDetails,
  savingProductDetails,
  onOpenProductComposer,
  onCloseProductComposer,
  onSubmitProduct,
  onResetProductForm,
  sellerPhoneOptions,
  sellerNameOptions,
  sellerPhoneByName,
  onCheckStolenImei,
  currentTimeLabel,
  summaryColumns,
}) {
  const headers = stockView?.headers || [];
  const rows = stockView?.all_rows_cache || [];
  const counts = stockView?.counts || {};
  const rowsPerPage = 20;
  const totalPages = Math.max(1, Math.ceil(rows.length / rowsPerPage));
  const currentPage = Math.min(stockPage, totalPages);
  const pagedRows = rows.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage);

  return (
    <div className="workspace-stack">
      <section className="summary-frame">
        <h2>Product Statistics</h2>
        <div className="summary-grid">
          <article className="metric-card">
            <span className="metric-label">Available</span>
            <strong className="metric-value">{formatCount(counts.available)}</strong>
            <span className="metric-note">Ready to sell.</span>
          </article>
          <article className="metric-card">
            <span className="metric-label">Pending Deal</span>
            <strong className="metric-value">{formatCount(counts.pending)}</strong>
            <span className="metric-note">Still open.</span>
          </article>
          <article className="metric-card">
            <span className="metric-label">Needs Details</span>
            <strong className="metric-value">{formatCount(counts.needs_details)}</strong>
            <span className="metric-note">Requires cleanup.</span>
          </article>
          <article className="metric-card">
            <span className="metric-label">Sold</span>
            <strong className="metric-value">{formatCount(counts.sold)}</strong>
            <span className="metric-note">Closed stock.</span>
          </article>
        </div>
      </section>

      <section className="content-panel content-panel--main content-panel--full">
        <div className="panel-header">
          <h3>Product List</h3>
          <p>Search the stock sheet, switch filters, and browse products twenty items at a time across the full workspace width.</p>
        </div>

        <div className="panel-toolbar">
          <div className="search-group">
            <label htmlFor="product-search">Search products:</label>
            <input
              id="product-search"
              type="search"
              placeholder="IMEI, model, seller, buyer..."
              value={productSearchText}
              onChange={(event) => setProductSearchText(event.target.value)}
            />
          </div>

          <div className="toolbar-controls">
            <div className="filter-tabs" role="tablist" aria-label="Product filters">
              {PRODUCT_FILTERS.map((option) => (
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
        {isLoading ? <div className="notice">Loading products...</div> : null}

        <MemoProductSummaryTable
          headers={headers}
          rows={pagedRows}
          emptyText="No products matched the current filters."
          summaryColumns={summaryColumns}
          onOpenDetails={onOpenProductDetails}
          isOverlayLoading={isRefreshing}
        />

        <div className="page-nav-wrap">
          <PageNavigator page={currentPage} totalPages={totalPages} onChange={setStockPage} />
        </div>
      </section>

      <button
        type="button"
        className="floating-action-button floating-action-button--add"
        onClick={onOpenProductComposer}
        aria-label="Open add product form"
        title="Add product"
      >
        +
      </button>

      {isProductComposerOpen ? (
        <ProductComposerModal
          stockForm={stockForm}
          productFormValues={productFormValues}
          isAddingProduct={isAddingProduct}
          onClose={onCloseProductComposer}
          onSubmitProduct={onSubmitProduct}
          onResetProductForm={onResetProductForm}
          sellerPhoneOptions={sellerPhoneOptions}
          sellerNameOptions={sellerNameOptions}
          sellerPhoneByName={sellerPhoneByName}
          currentTimeLabel={currentTimeLabel}
          dropdownOptions={stockForm?.dropdown_options}
          onCheckStolenImei={onCheckStolenImei}
        />
      ) : null}

      {selectedProductDetail ? (
        <ProductDetailModal
          row={selectedProductDetail}
          headers={headers}
          onClose={onCloseProductDetails}
          onSave={onSaveProductDetails}
          saving={savingProductDetails}
        />
      ) : null}
    </div>
  );
}

function DebtorsView({
  debtors,
  debtorPage,
  setDebtorPage,
  debtorSearch,
  setDebtorSearch,
  selectedDebtor,
  onSelectDebtor,
  billText,
  outstandingItems,
  paymentAmount,
  setPaymentAmount,
  selectedServiceRow,
  setSelectedServiceRow,
  paymentPlan,
  paymentPlanError,
  detailLoading,
  applyingPayment,
  onCopyBill,
  onQuickCopyBill,
  onSendWhatsapp,
  onRefreshTodayUnpaid,
  onSendTodayUnpaidCustomer,
  onRefreshDebtorsSection,
  sendingTodayBills,
  refreshingDebtorsSection,
  unpaidTodaySummary,
  whatsappHistoryByName,
  onApplyPayment,
  onApplyFullPayment,
  serviceActionBusy,
  onUpdateServiceRow,
  onReturnServiceRow,
}) {
  const rowsPerPage = 10;
  const totalPages = Math.max(1, Math.ceil(debtors.length / rowsPerPage));
  const currentPage = Math.min(debtorPage, totalPages);
  const pagedDebtors = debtors.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage);
  const [servicePriceDraft, setServicePriceDraft] = useState('');

  const selectedSendStats = selectedDebtor ? whatsappHistoryByName?.[selectedDebtor] : null;
  const selectedServiceItem = useMemo(() => {
    if (selectedServiceRow === 'automatic') {
      return null;
    }

    return (outstandingItems || []).find((item) => String(item.row_idx) === String(selectedServiceRow)) || null;
  }, [outstandingItems, selectedServiceRow]);

  useEffect(() => {
    if (selectedServiceItem) {
      setServicePriceDraft(String(selectedServiceItem.price ?? ''));
      return;
    }

    setServicePriceDraft('');
  }, [selectedServiceItem, selectedDebtor]);

  const servicePricePreview = Number(normalizeDigits(servicePriceDraft));
  const servicePaidValue = Number(selectedServiceItem?.paid || 0);
  const serviceBalancePreview = Number.isFinite(servicePricePreview) && servicePricePreview > 0
    ? Math.max(0, servicePricePreview - servicePaidValue)
    : Number(selectedServiceItem?.balance || 0);

  async function handleUpdateServicePrice() {
    if (!selectedDebtor || !selectedServiceItem) {
      return;
    }

    const nextPrice = Number(normalizeDigits(servicePriceDraft));
    if (!Number.isFinite(nextPrice) || nextPrice <= 0) {
      return;
    }

    await onUpdateServiceRow?.({
      rowIdx: selectedServiceItem.row_idx,
      price: nextPrice,
    });
    // Update the service price
    // If the payment amount is set, immediately apply the payment to this service
    if (paymentAmount && Number(paymentAmount) > 0) {
      await onApplyPayment?.();
    }
  }

  async function handleReturnService() {
    if (!selectedDebtor || !selectedServiceItem) {
      return;
    }

    await onReturnServiceRow?.({
      rowIdx: selectedServiceItem.row_idx,
    });
  }

  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Debtors List</h3>
          <p>Search your debtors, copy or send the current bill, and move through the list one page at a time.</p>
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
            <button type="button" className="secondary-button" onClick={onCopyBill} disabled={!selectedDebtor || detailLoading}>
              Copy Bill To Clipboard
            </button>
            <button type="button" className="primary-button" onClick={onSendWhatsapp} disabled={!selectedDebtor || detailLoading}>
              Send To WhatsApp
            </button>
            <button
              type="button"
              className="secondary-button"
              onClick={onRefreshDebtorsSection}
              disabled={refreshingDebtorsSection || sendingTodayBills}
            >
              {refreshingDebtorsSection ? 'Refreshing Debtors...' : 'Refresh Debtors Section'}
            </button>
            <button type="button" className="secondary-button" onClick={onRefreshTodayUnpaid} disabled={sendingTodayBills || refreshingDebtorsSection}>
              {sendingTodayBills ? 'Loading Today List...' : 'Refresh Today Unpaid List'}
            </button>
          </div>
        </div>

        <div className="notice compact">
          Today unpaid customers: {formatCount(unpaidTodaySummary?.count || 0)} | With phone: {formatCount(unpaidTodaySummary?.with_phone_count || 0)}
        </div>

        <div className="table-wrap table-wrap--mobile-cards">
          <table className="data-table">
            <thead>
              <tr>
                <th>Customer</th>
                <th>Outstanding Amount</th>
                <th>WhatsApp Sends</th>
                <th>Last Sent</th>
                <th>Quick Copy</th>
              </tr>
            </thead>
            <tbody>
              {pagedDebtors.length ? (
                pagedDebtors.map(([name, amount]) => (
                  (() => {
                    const sendStats = whatsappHistoryByName?.[name] || {};
                    return (
                  <tr
                    key={name}
                    className={name === selectedDebtor ? 'table-row-selected' : ''}
                    onClick={() => onSelectDebtor(name)}
                  >
                    <td data-label="Customer">{name}</td>
                    <td className="amount-cell" data-label="Outstanding Amount">{formatCurrency(amount)}</td>
                    <td data-label="WhatsApp Sends">{formatCount(sendStats.send_count || 0)}</td>
                    <td data-label="Last Sent">{sendStats.last_sent_at ? sendStats.last_sent_at.replace('T', ' ').slice(0, 16) : 'Never'}</td>
                    <td className="row-actions-cell" data-label="Quick Copy">
                      <button
                        type="button"
                        className="table-action-button"
                        onClick={(event) => {
                          event.stopPropagation();
                          onQuickCopyBill(name);
                        }}
                      >
                        Copy
                      </button>
                    </td>
                  </tr>
                    );
                  })()
                ))
              ) : (
                <tr>
                  <td colSpan={5} className="empty-state">No debtors matched the current filter.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="page-nav-wrap">
          <PageNavigator page={currentPage} totalPages={totalPages} onChange={setDebtorPage} />
        </div>

        <div className="panel-header" style={{ marginTop: '16px' }}>
          <h3>Today Unpaid Customers</h3>
          <p>Customers serviced today who have not fully paid. Send each bill one by one at end of day.</p>
        </div>

        <div className="table-wrap table-wrap--mobile-cards">
          <table className="data-table">
            <thead>
              <tr>
                <th>Customer</th>
                <th>Services Today</th>
                <th>Outstanding Today</th>
                <th>Sends</th>
                <th>Last Sent</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {(unpaidTodaySummary?.customers || []).length ? (
                (unpaidTodaySummary?.customers || []).map((entry) => {
                  const tracked = whatsappHistoryByName?.[entry.name] || entry.send_stats || {};
                  return (
                    <tr key={`today-${entry.name}`}>
                      <td data-label="Customer">{entry.name}</td>
                      <td data-label="Services Today">{formatCount(entry.services_today || 0)}</td>
                      <td className="amount-cell" data-label="Outstanding Today">{formatCurrency(entry.outstanding_today || 0)}</td>
                      <td data-label="Sends">{formatCount(tracked.send_count || 0)}</td>
                      <td data-label="Last Sent">{tracked.last_sent_at ? tracked.last_sent_at.replace('T', ' ').slice(0, 16) : 'Never'}</td>
                      <td className="row-actions-cell" data-label="Action">
                        <button
                          type="button"
                          className="table-action-button"
                          onClick={() => onSendTodayUnpaidCustomer(entry)}
                          disabled={!entry.has_phone || sendingTodayBills}
                          title={entry.has_phone ? 'Open WhatsApp bill for this customer' : 'No phone saved for this customer'}
                        >
                          {entry.has_phone ? 'Send Bill' : 'No Phone'}
                        </button>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={6} className="empty-state">No unpaid customers found for today.</td>
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
            <p>Choose the debtor, enter the amount received, and target a specific service if needed.</p>
          </div>

          <div className="form-stack">
            <label className="field-block">
              <span className="field-label">Selected Customer</span>
              <input type="text" readOnly value={selectedDebtor} placeholder="Select a debtor from the list" />
            </label>

            <label className="field-block">
              <span className="field-label">Payment Amount</span>
              <input
                type="text"
                inputMode="numeric"
                value={paymentAmount}
                onChange={(event) => setPaymentAmount(normalizeDigits(event.target.value))}
                placeholder="Amount received"
              />
            </label>

            <label className="field-block">
              <span className="field-label">Service Target</span>
              <select value={selectedServiceRow} onChange={(event) => setSelectedServiceRow(event.target.value)}>
                <option value="automatic">Automatic sequence</option>
                {outstandingItems.map((item) => (
                  <option key={item.row_idx} value={String(item.row_idx)}>
                    {buildOutstandingLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            {selectedServiceItem ? (
              <div className="preview-card preview-card--bill">
                <h4>Selected Service</h4>
                <div className="meta-stack meta-stack--tight">
                  <div className="meta-row">
                    <span>Description</span>
                    <strong>{selectedServiceItem.description || 'UNNAMED SERVICE'}</strong>
                  </div>
                  <div className="meta-row">
                    <span>Date</span>
                    <strong>{selectedServiceItem.date || 'No date'}</strong>
                  </div>
                  <div className="meta-row">
                    <span>Current Price</span>
                    <strong>{formatCurrency(selectedServiceItem.price || 0)}</strong>
                  </div>
                  <div className="meta-row">
                    <span>Paid</span>
                    <strong>{formatCurrency(selectedServiceItem.paid || 0)}</strong>
                  </div>
                  <div className="meta-row">
                    <span>Balance After Edit</span>
                    <strong>{formatCurrency(serviceBalancePreview)}</strong>
                  </div>
                </div>

                <label className="field-block" style={{ marginTop: '12px' }}>
                  <span className="field-label">Edited Price</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={servicePriceDraft}
                    onChange={(event) => setServicePriceDraft(normalizeDigits(event.target.value))}
                    placeholder="Enter new price"
                  />
                </label>

                <div className="button-row button-row--end">
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={handleReturnService}
                    disabled={serviceActionBusy}
                  >
                    {serviceActionBusy ? 'Working...' : 'Return Service'}
                  </button>
                  <button
                    type="button"
                    className="primary-button"
                    onClick={handleUpdateServicePrice}
                    disabled={serviceActionBusy}
                  >
                    {serviceActionBusy ? 'Working...' : 'Update Price'}
                  </button>
                </div>
              </div>
            ) : null}

            <button type="button" className="primary-button button-wide" onClick={onApplyPayment} disabled={applyingPayment || !selectedDebtor}>
              {applyingPayment ? 'Applying Payment...' : 'Apply Payment'}
            </button>
            <button type="button" className="secondary-button button-wide" onClick={onApplyFullPayment} disabled={applyingPayment || !selectedDebtor}>
              {applyingPayment ? 'Applying Payment...' : 'Mark Fully Paid'}
            </button>
          </div>

          <div className="preview-card">
            <h4>Payment Preview</h4>
            <pre>{buildPaymentPreviewText(selectedDebtor, paymentAmount, paymentPlan, paymentPlanError, selectedServiceItem, servicePriceDraft)}</pre>
          </div>
        </div>

        <div className="subpanel subpanel--muted">
          <div className="panel-header">
            <h3>Bill Preview</h3>
            <p>Live bill text for the selected debtor.</p>
          </div>

          <div className="preview-card preview-card--bill">
            <pre>{detailLoading ? 'Loading bill preview...' : billText || 'Select a debtor to preview the bill.'}</pre>
          </div>

          <div className="meta-stack">
            <div className="meta-row">
              <span>WhatsApp sends for selected</span>
              <strong>{formatCount(selectedSendStats?.send_count || 0)}</strong>
            </div>
            <div className="meta-row">
              <span>Last sent</span>
              <strong>{selectedSendStats?.last_sent_at ? selectedSendStats.last_sent_at.replace('T', ' ').slice(0, 16) : 'Never'}</strong>
            </div>
          </div>
        </div>
      </aside>
    </section>
  );
}

function ClientsView({
  clients,
  clientPage,
  setClientPage,
  clientSearch,
  setClientSearch,
  clientForm,
  setClientForm,
  clientsBusy,
  onSelectClient,
  onSaveClient,
  onDeleteClient,
  onImportPhones,
  googleContacts,
  googleSearch,
  setGoogleSearch,
  googleContactPage,
  setGoogleContactPage,
  selectedGoogleContact,
  onSelectGoogleContact,
  onSyncGoogleContacts,
  onApplyGoogleContact,
  googleContactsBusy,
  googleContactsError,
  stats,
}) {
  const rowsPerPage = 10;
  const totalPages = Math.max(1, Math.ceil(clients.length / rowsPerPage));
  const currentPage = Math.min(clientPage, totalPages);
  const pagedClients = clients.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage);

  const contactsPerPage = 10;
  const totalContactPages = Math.max(1, Math.ceil((googleContacts.contacts || []).length / contactsPerPage));
  const currentContactPage = Math.min(googleContactPage, totalContactPages);
  const pagedContacts = (googleContacts.contacts || []).slice((currentContactPage - 1) * contactsPerPage, currentContactPage * contactsPerPage);

  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Client List</h3>
          <p>Ten client records per page, with search and page navigation underneath.</p>
        </div>

        <div className="panel-toolbar">
          <div className="search-group">
            <label htmlFor="client-search">Search clients:</label>
            <input
              id="client-search"
              type="search"
              placeholder="Client name or phone number..."
              value={clientSearch}
              onChange={(event) => setClientSearch(event.target.value)}
            />
          </div>

          <div className="toolbar-actions">
            <button type="button" className="secondary-button" onClick={onImportPhones} disabled={clientsBusy}>
              Import Sheet Phones
            </button>
          </div>
        </div>

        <div className="table-wrap table-wrap--mobile-cards">
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>WhatsApp Number</th>
                <th>Gender</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {pagedClients.length ? (
                pagedClients.map((entry) => (
                  <tr key={entry.name} onClick={() => onSelectClient(entry)}>
                    <td data-label="Name">{entry.name}</td>
                    <td data-label="WhatsApp Number">{entry.phone || '—'}</td>
                    <td data-label="Gender">{entry.gender ? `${entry.gender.charAt(0).toUpperCase()}${entry.gender.slice(1)}` : '—'}</td>
                    <td data-label="Status">{entry.has_phone ? 'Saved' : 'Missing Number'}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={4} className="empty-state">No clients matched the current filter.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="page-nav-wrap">
          <PageNavigator page={currentPage} totalPages={totalPages} onChange={setClientPage} />
        </div>
      </section>

      <aside className="content-panel content-panel--side content-panel--stacked">
        <div className="subpanel">
          <div className="panel-header">
            <h3>Client Editor</h3>
            <p>Save a number in either local or international format and it will be normalized automatically.</p>
          </div>

          <div className="form-stack">
            <label className="field-block">
              <span className="field-label">Client Name</span>
              <input
                type="text"
                value={clientForm.name}
                onChange={(event) => setClientForm((current) => ({ ...current, name: event.target.value }))}
                placeholder="Client name"
              />
            </label>

            <label className="field-block">
              <span className="field-label">WhatsApp Number</span>
              <input
                type="text"
                inputMode="tel"
                value={clientForm.phone}
                onChange={(event) => setClientForm((current) => ({ ...current, phone: event.target.value }))}
                placeholder="090..., 23490..., or +23490..."
              />
            </label>

            <label className="field-block">
              <span className="field-label">Gender (Optional)</span>
              <select
                value={clientForm.gender || ''}
                onChange={(event) => setClientForm((current) => ({ ...current, gender: event.target.value }))}
              >
                <option value="">Not set</option>
                <option value="male">Male</option>
                <option value="female">Female</option>
              </select>
            </label>

            <div className="button-row">
              <button type="button" className="primary-button" onClick={onSaveClient} disabled={clientsBusy}>
                {clientsBusy ? 'Saving...' : 'Save Client'}
              </button>
              <button type="button" className="secondary-button" onClick={onDeleteClient} disabled={clientsBusy || !clientForm.name}>
                Delete Client
              </button>
            </div>
          </div>
        </div>

        <div className="subpanel subpanel--muted">
          <div className="panel-header">
            <h3>Client Statistics</h3>
            <p>Phone coverage now lives here instead of the main dashboard.</p>
          </div>

          <div className="meta-stack">
            <div className="meta-row">
              <span>Total clients</span>
              <strong>{formatCount(stats.total_count)}</strong>
            </div>
            <div className="meta-row">
              <span>Clients with phone</span>
              <strong>{formatCount(stats.with_phone_count)}</strong>
            </div>
            <div className="meta-row">
              <span>Clients missing phone</span>
              <strong>{formatCount(stats.without_phone_count)}</strong>
            </div>
            <div className="meta-row">
              <span>Clients with gender</span>
              <strong>{formatCount(stats.with_gender_count || 0)}</strong>
            </div>
          </div>
        </div>

        <div className="subpanel">
          <div className="panel-header">
            <h3>Google Contacts</h3>
            <p>Load your cached Google contacts, search them, and click a contact row to fill the current client phone number.</p>
          </div>

          <div className="form-stack">
            <div className="button-row">
              <button type="button" className="primary-button" onClick={onSyncGoogleContacts} disabled={googleContactsBusy}>
                {googleContactsBusy ? 'Syncing Contacts...' : 'Sync Google Contacts'}
              </button>
              <button type="button" className="secondary-button" onClick={onApplyGoogleContact} disabled={!selectedGoogleContact || clientsBusy}>
                Use Selected Contact
              </button>
            </div>

            <label className="field-block">
              <span className="field-label">Search Synced Contacts</span>
              <input
                type="search"
                value={googleSearch}
                onChange={(event) => setGoogleSearch(event.target.value)}
                placeholder="Contact name or number..."
              />
            </label>
          </div>

          {googleContactsError ? <div className="notice notice-error">{googleContactsError}</div> : null}

          <div className="contacts-meta">
            <span>Loaded: {formatCount(googleContacts.total_cached)}</span>
            <span>Last sync: {googleContacts.synced_at || 'Not synced yet'}</span>
          </div>

          <div className="table-wrap table-wrap--narrow table-wrap--mobile-cards">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Phone</th>
                </tr>
              </thead>
              <tbody>
                {pagedContacts.length ? (
                  pagedContacts.map((contact) => {
                    const key = `${contact.name}-${contact.phone}`;
                    return (
                      <tr
                        key={key}
                        className={selectedGoogleContact?.phone === contact.phone && selectedGoogleContact?.name === contact.name ? 'table-row-selected' : ''}
                        onClick={() => onSelectGoogleContact(contact)}
                      >
                        <td data-label="Name">{contact.name}</td>
                        <td data-label="Phone">{contact.phone}</td>
                      </tr>
                    );
                  })
                ) : (
                  <tr>
                    <td colSpan={2} className="empty-state">Sync Google Contacts to search and use contact numbers.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="page-nav-wrap page-nav-wrap--tight">
            <PageNavigator page={currentContactPage} totalPages={totalContactPages} onChange={setGoogleContactPage} />
          </div>
        </div>
      </aside>
    </section>
  );
}

function FixView({ mismatches, selectedMismatch, correctName, setCorrectName, onSelectMismatch, onApplyFix, onApplyAll, onRescan, loading, applying }) {
  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Fix Queue</h3>
          <p>Rescan live records, inspect the best suggestions, and apply one or all fixes.</p>
        </div>

        <div className="panel-toolbar">
          <div className="toolbar-actions toolbar-actions--full">
            <button type="button" className="secondary-button" onClick={onRescan} disabled={loading || applying}>
              {loading ? 'Scanning...' : 'Rescan Sheet'}
            </button>
            <button type="button" className="primary-button" onClick={onApplyAll} disabled={!mismatches.length || loading || applying}>
              {applying ? 'Working...' : 'Fix All'}
            </button>
          </div>
        </div>

        <div className="table-wrap table-wrap--mobile-cards">
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
                    <td data-label="Sheet Name">{entry.raw}</td>
                    <td data-label="Rows">{formatCount(entry.rows?.length)}</td>
                    <td data-label="Top Suggestion">{entry.candidates?.[0] || 'No suggestion'}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={3} className="empty-state">{loading ? 'Scanning live rows...' : 'No live name mismatches found.'}</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <aside className="content-panel content-panel--side content-panel--stacked">
        <div className="subpanel">
          <div className="panel-header">
            <h3>Selected Fix</h3>
            <p>Pick the right replacement and apply it back to the source sheet.</p>
          </div>

          <div className="form-stack">
            <label className="field-block">
              <span className="field-label">Selected Sheet Name</span>
              <input type="text" readOnly value={selectedMismatch?.raw || ''} placeholder="Choose a mismatch" />
            </label>

            <label className="field-block">
              <span className="field-label">Replace With</span>
              <input type="text" value={correctName} onChange={(event) => setCorrectName(event.target.value.toUpperCase())} placeholder="Correct customer name" />
            </label>

            <div className="token-row">
              {(selectedMismatch?.candidates || []).map((candidate) => (
                <button key={candidate} type="button" className="token-button" onClick={() => setCorrectName(candidate)}>
                  {candidate}
                </button>
              ))}
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

function ServicesTodayView({ servicesTodayData, servicesTodayDate, servicesTodayBusy, onChangeDate, onLoadDate, onUpdateServiceEntry, onUpdateServicePayment }) {
  useRenderTiming('ServicesTodayView', `${servicesTodayData?.count || 0}:${servicesTodayBusy ? 'busy' : 'idle'}`);
  const items = servicesTodayData?.services || [];
  const [editingRowNum, setEditingRowNum] = useState(null);
  const [editingName, setEditingName] = useState('');
  const [savingRowNum, setSavingRowNum] = useState(null);
  const [expandedRowNum, setExpandedRowNum] = useState(null);
  const [paymentEdits, setPaymentEdits] = useState({});
  const [savingPaymentRowNum, setSavingPaymentRowNum] = useState(null);
  const [searchMode, setSearchMode] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searchBusy, setSearchBusy] = useState(false);
  const [searchCount, setSearchCount] = useState(null);
  const searchAbortRef = React.useRef(null);
  const tableContainerRef = useRef(null);
  const isCompactViewport = useIsCompactViewport();

  function beginEdit(entry) {
    setEditingRowNum(entry?.row_num || null);
    setEditingName(String(entry?.name || '').trim());
  }

  function cancelEdit() {
    setEditingRowNum(null);
    setEditingName('');
    setSavingRowNum(null);
  }

  async function saveEdit(entry) {
    const nextName = String(editingName || '').trim().toUpperCase();
    const currentName = String(entry?.name || '').trim().toUpperCase();
    const rowNum = Number(entry?.row_num || 0);
    if (!rowNum || !currentName || !nextName) {
      return;
    }

    if (nextName === currentName) {
      cancelEdit();
      return;
    }

    setSavingRowNum(rowNum);
    try {
      await onUpdateServiceEntry?.({ rowNum, currentName, nextName });
      cancelEdit();
    } finally {
      setSavingRowNum(null);
    }
  }

  function toggleExpand(rowNum) {
    setExpandedRowNum((prev) => (prev === rowNum ? null : rowNum));
  }

  function getPaymentEdit(entry) {
    const rn = entry.row_num;
    return paymentEdits[rn] || { amountPaid: String(entry.amount_paid || '0') };
  }

  function setPaymentEdit(rowNum, key, value) {
    setPaymentEdits((prev) => ({ ...prev, [rowNum]: { ...getPaymentEditByRowNum(prev, rowNum), [key]: value } }));
  }

  function getPaymentEditByRowNum(edits, rowNum) {
    return edits[rowNum] || {};
  }

  function derivePaymentStatusFromAmount(priceValueRaw, amountPaidRaw) {
    const normalizedAmount = String(amountPaidRaw ?? '').replace(/[^0-9.]/g, '');
    const amountPaid = Number.parseFloat(normalizedAmount) || 0;
    const priceValue = Number.parseFloat(String(priceValueRaw ?? '0')) || 0;
    if (amountPaid <= 0) {
      return 'UNPAID';
    }
    if (priceValue > 0 && amountPaid < priceValue) {
      return 'PART PAYMENT';
    }
    return 'PAID';
  }

  async function savePaymentEdit(entry) {
    const rn = Number(entry.row_num);
    if (!rn) return;
    const edit = paymentEdits[rn] || {};
    const amountPaid = String(edit.amountPaid ?? entry.amount_paid ?? '0').trim();
    const paymentStatus = derivePaymentStatusFromAmount(entry.price, amountPaid);
    setSavingPaymentRowNum(rn);
    try {
      await onUpdateServicePayment?.({ rowNum: rn, paymentStatus, amountPaid });
    } finally {
      setSavingPaymentRowNum(null);
    }
  }

  async function runSearch(q) {
    if (!String(q || '').trim()) {
      setSearchResults([]);
      setSearchCount(null);
      return;
    }
    if (searchAbortRef.current) {
      searchAbortRef.current.abort();
    }
    const controller = new AbortController();
    searchAbortRef.current = controller;
    setSearchBusy(true);
    try {
      const result = await searchServices({ query: q, signal: controller.signal });
      setSearchResults(result.services || []);
      setSearchCount(result.count ?? 0);
    } catch (err) {
      if (err?.name !== 'AbortError') {
        setSearchResults([]);
        setSearchCount(null);
      }
    } finally {
      setSearchBusy(false);
    }
  }

  function handleSearchInput(value) {
    setSearchQuery(value);
  }

  useEffect(() => {
    if (!searchMode) {
      return undefined;
    }
    const delayId = window.setTimeout(() => {
      runSearch(searchQuery);
    }, 220);
    return () => {
      window.clearTimeout(delayId);
    };
  }, [searchMode, searchQuery]);

  const displayItems = searchMode ? searchResults : items;
  const shouldWindowRows = !isCompactViewport && !expandedRowNum && !editingRowNum && displayItems.length > 40;
  const { visibleItems, topSpacerHeight, bottomSpacerHeight } = useWindowedRows(displayItems, {
    containerRef: tableContainerRef,
    enabled: shouldWindowRows,
    rowHeight: 58,
    overscan: 8,
  });

  return (
    <section className="workspace-stack">
      <section className="content-panel content-panel--main content-panel--full">
        <div className="panel-header">
          <h3>Services Done Today</h3>
          <p>View services recorded for any selected day, or search all services by customer name.</p>
        </div>

        <div className="panel-toolbar" style={{ flexWrap: 'wrap', gap: '10px' }}>
          <div className="tab-toggle" style={{ display: 'flex', gap: '6px' }}>
            <button
              type="button"
              className={searchMode ? 'secondary-button' : 'primary-button'}
              onClick={() => setSearchMode(false)}
            >
              By Date
            </button>
            <button
              type="button"
              className={searchMode ? 'primary-button' : 'secondary-button'}
              onClick={() => setSearchMode(true)}
            >
              Search
            </button>
          </div>

          {!searchMode ? (
            <>
              <div className="search-group" style={{ maxWidth: '260px' }}>
                <label htmlFor="services-day-picker">Select date:</label>
                <input
                  id="services-day-picker"
                  type="date"
                  value={servicesTodayDate}
                  onChange={(event) => onChangeDate?.(event.target.value)}
                />
              </div>
              <div className="toolbar-actions">
                <button type="button" className="primary-button" onClick={() => onLoadDate?.(servicesTodayDate, true)} disabled={servicesTodayBusy || savingRowNum !== null}>
                  {servicesTodayBusy ? 'Loading...' : 'Load Sales'}
                </button>
              </div>
            </>
          ) : (
            <div className="search-group" style={{ flex: 1, maxWidth: '400px' }}>
              <label htmlFor="services-name-search">Customer name:</label>
              <input
                id="services-name-search"
                type="text"
                value={searchQuery}
                onChange={(event) => handleSearchInput(event.target.value)}
                placeholder="Search by customer name…"
                autoFocus
              />
            </div>
          )}
        </div>

        <div className="notice compact">
          {searchMode
            ? (searchBusy ? 'Searching…' : searchCount !== null ? `Found ${formatCount(searchCount)} service(s) for "${searchQuery}"` : 'Enter a name to search all services.')
            : `Total services for ${servicesTodayDate || 'selected date'}: ${formatCount(servicesTodayData?.count || 0)}`}
        </div>

        <div ref={tableContainerRef} className={shouldWindowRows ? 'table-wrap table-wrap--mobile-cards table-wrap--windowed' : 'table-wrap table-wrap--mobile-cards'}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Row</th>
                <th>Date</th>
                <th>Time</th>
                <th>Customer</th>
                <th>Description</th>
                <th>IMEI</th>
                <th>Status</th>
                <th>Price</th>
                <th>Amount Paid</th>
                <th>Balance</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {displayItems.length ? (
                <>
                {shouldWindowRows && topSpacerHeight > 0 ? (
                  <tr className="table-spacer-row" aria-hidden="true">
                    <td colSpan={11} style={{ height: `${topSpacerHeight}px` }} />
                  </tr>
                ) : null}
                {visibleItems.map((entry) => {
                  const isEditing = Number(editingRowNum) === Number(entry.row_num);
                  const isSaving = Number(savingRowNum) === Number(entry.row_num);
                  const isExpanded = Number(expandedRowNum) === Number(entry.row_num);
                  const isSavingPayment = Number(savingPaymentRowNum) === Number(entry.row_num);
                  const payEdit = getPaymentEdit(entry);
                  const computedStatus = derivePaymentStatusFromAmount(entry.price, payEdit.amountPaid);
                  return (
                  <React.Fragment key={`service-today-${entry.row_num}`}>
                  <tr>
                    <td className="row-number" data-label="Row">#{entry.row_num}</td>
                    <td data-label="Date">{entry.date || '—'}</td>
                    <td data-label="Time">{entry.time || '—'}</td>
                    <td data-label="Customer">
                      {isEditing ? (
                        <input
                          type="text"
                          value={editingName}
                          onChange={(event) => setEditingName(event.target.value.toUpperCase())}
                          placeholder="Customer paying"
                          style={{ width: '100%', minWidth: '160px' }}
                          disabled={isSaving}
                        />
                      ) : (entry.name || '—')}
                    </td>
                    <td data-label="Description">{entry.description || '—'}</td>
                    <td data-label="IMEI">{entry.imei || '—'}</td>
                    <td data-label="Status">{computedStatus}</td>
                    <td className="amount-cell" data-label="Price">{formatCurrency(entry.price || 0)}</td>
                    <td className="amount-cell" data-label="Amount Paid">
                      <input
                        type="text"
                        inputMode="numeric"
                        value={payEdit.amountPaid}
                        onChange={(e) => setPaymentEdit(entry.row_num, 'amountPaid', e.target.value.replace(/[^0-9.]/g, ''))}
                        disabled={isSavingPayment}
                        style={{ width: '80px', textAlign: 'right' }}
                      />
                    </td>
                    <td className="amount-cell" data-label="Balance">{formatCurrency(entry.balance || 0)}</td>
                    <td className="row-actions-cell" data-label="Action">
                      <div className="button-row button-row--end" style={{ gap: '4px', flexWrap: 'wrap' }}>
                        {isEditing ? (
                          <>
                            <button type="button" className="table-action-button" onClick={cancelEdit} disabled={isSaving}>Cancel</button>
                            <button type="button" className="table-action-button" onClick={() => saveEdit(entry)} disabled={isSaving || !String(editingName || '').trim()}>
                              {isSaving ? 'Saving...' : 'Save Name'}
                            </button>
                          </>
                        ) : (
                          <button type="button" className="table-action-button" onClick={() => beginEdit(entry)} disabled={savingRowNum !== null || (searchMode ? searchBusy : servicesTodayBusy)}>
                            Edit Name
                          </button>
                        )}
                        <button type="button" className="table-action-button" onClick={() => savePaymentEdit(entry)} disabled={isSavingPayment || isSaving}>
                          {isSavingPayment ? 'Applying...' : 'Apply Payment'}
                        </button>
                        <button type="button" className="table-action-button" onClick={() => toggleExpand(entry.row_num)}>
                          {isExpanded ? 'Less' : 'More'}
                        </button>
                      </div>
                    </td>
                  </tr>
                  {isExpanded ? (
                    <tr>
                      <td colSpan={11} style={{ background: 'var(--surface-2, #f8f9fa)', padding: '12px 16px' }}>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '16px', fontSize: '0.9em' }}>
                          <div><strong>Fulfillment Method:</strong> {entry.fulfillment_method || '—'}</div>
                          <div><strong>Deal Location:</strong> {entry.deal_location || '—'}</div>
                          <div><strong>Internal Note:</strong> {entry.internal_note || '—'}</div>
                          <div><strong>Payment Method:</strong> {entry.payment_method || '—'}</div>
                          <div><strong>Pickup Mode:</strong> {entry.pickup_mode || '—'}</div>
                          {entry.representative_name ? <div><strong>Representative:</strong> {entry.representative_name} {entry.representative_phone ? `(${entry.representative_phone})` : ''}</div> : null}
                        </div>
                      </td>
                    </tr>
                  ) : null}
                  </React.Fragment>
                );
                })
                }
                {shouldWindowRows && bottomSpacerHeight > 0 ? (
                  <tr className="table-spacer-row" aria-hidden="true">
                    <td colSpan={11} style={{ height: `${bottomSpacerHeight}px` }} />
                  </tr>
                ) : null}
                </>
              ) : (
                <tr>
                  <td colSpan={11} className="empty-state">
                    {searchMode
                      ? (searchBusy ? 'Searching…' : searchQuery ? 'No services found for this name.' : 'Enter a customer name above to search.')
                      : 'No services were recorded for this date.'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
}

const MemoServicesTodayView = React.memo(ServicesTodayView);

function StolenDevicesView({ data, busy, form, onFormChange, onLoad, onCreate, onToggleActive }) {
  const items = data?.items || [];

  React.useEffect(() => {
    if (!data) {
      onLoad?.();
    }
  }, []);

  return (
    <section className="workspace-stack">
      <section className="content-panel content-panel--main content-panel--full">
        <div className="panel-header">
          <h3>Stolen Device Registry</h3>
          <p>Manage the IMEI registry used to block sales of reported stolen devices.</p>
        </div>

        <div className="panel-toolbar">
          <button type="button" className="primary-button" onClick={onLoad} disabled={busy}>
            {busy ? 'Loading...' : 'Refresh'}
          </button>
        </div>

        <form className="modal-form" onSubmit={onCreate} style={{ marginBottom: '20px' }}>
          <h4 style={{ marginBottom: '10px' }}>Add Stolen Device</h4>
          <div className="form-grid">
            <label className="field-block">
              <span className="field-label">Phone Name</span>
              <input type="text" value={form?.phone_name || ''} onChange={(e) => onFormChange?.('phone_name', e.target.value)} placeholder="e.g. iPhone 14 Pro" required />
            </label>
            <label className="field-block">
              <span className="field-label">IMEI</span>
              <input type="text" value={form?.imei_raw || ''} onChange={(e) => onFormChange?.('imei_raw', e.target.value)} placeholder="IMEI number" required />
            </label>
            <label className="field-block">
              <span className="field-label">Note</span>
              <input type="text" value={form?.note || ''} onChange={(e) => onFormChange?.('note', e.target.value)} placeholder="Optional note" />
            </label>
            <label className="field-block">
              <span className="field-label">Source</span>
              <input type="text" value={form?.source || ''} onChange={(e) => onFormChange?.('source', e.target.value)} placeholder="Who reported this?" />
            </label>
          </div>
          <div className="button-row" style={{ marginTop: '10px' }}>
            <button type="submit" className="primary-button" disabled={busy}>
              {busy ? 'Saving...' : 'Add to Registry'}
            </button>
          </div>
        </form>

        <div className="notice compact">Total records: {formatCount(items.length)}</div>

        <div className="table-wrap table-wrap--mobile-cards">
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Phone Name</th>
                <th>IMEI</th>
                <th>Status</th>
                <th>Note</th>
                <th>Source</th>
                <th>Added</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {items.length ? items.map((item) => (
                <tr key={`stolen-${item.id}`} style={{ opacity: item.is_active ? 1 : 0.5 }}>
                  <td data-label="ID">#{item.id}</td>
                  <td data-label="Phone Name">{item.phone_name || '—'}</td>
                  <td data-label="IMEI"><code>{item.imei_raw || '—'}</code></td>
                  <td data-label="Status">{item.is_active ? 'Active' : 'Cleared'}</td>
                  <td data-label="Note">{item.note || '—'}</td>
                  <td data-label="Source">{item.source || '—'}</td>
                  <td data-label="Added">{item.created_at ? new Date(item.created_at).toLocaleDateString() : '—'}</td>
                  <td data-label="Action">
                    <button type="button" className="table-action-button" onClick={() => onToggleActive?.(item)} disabled={busy}>
                      {item.is_active ? 'Clear' : 'Reactivate'}
                    </button>
                  </td>
                </tr>
              )) : (
                <tr><td colSpan={8} className="empty-state">No stolen device records found.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
}

function BillNotificationsView({ entries, onOpenDebtors, onSendEntry, sendingKey = '' }) {
  const rows = Array.isArray(entries) ? entries : [];

  return (
    <section className="workspace-stack">
      <section className="content-panel content-panel--main content-panel--full">
        <div className="panel-header">
          <h3>Bill Notifications</h3>
          <p>Customers with unpaid balances whose last bill send is more than 4 days ago, sorted highest overdue to lowest.</p>
        </div>

        <div className="notice compact">
          Total overdue customers: {formatCount(rows.length)}
        </div>

        <div className="button-row" style={{ marginBottom: '10px' }}>
          <button type="button" className="secondary-button" onClick={onOpenDebtors}>
            Open Debtors
          </button>
        </div>

        <div className="table-wrap table-wrap--mobile-cards">
          <table className="data-table">
            <thead>
              <tr>
                <th>Customer</th>
                <th>Outstanding</th>
                <th>Days Since Last Bill</th>
                <th>Last Bill Sent</th>
                <th>Total Sends</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {rows.length ? (
                rows.map((entry) => (
                  <tr key={`bill-notify-${entry.name}`}>
                    <td data-label="Customer">{entry.name}</td>
                    <td className="amount-cell" data-label="Outstanding">{formatCurrency(entry.outstanding || 0)}</td>
                    <td data-label="Days Since Last Bill">{formatCount(entry.days_since_last_bill || 0)}</td>
                    <td data-label="Last Bill Sent">{entry.last_sent_at ? String(entry.last_sent_at).replace('T', ' ').slice(0, 16) : 'Never'}</td>
                    <td data-label="Total Sends">{formatCount(entry.send_count || 0)}</td>
                    <td className="row-actions-cell" data-label="Action">
                      <button
                        type="button"
                        className="table-action-button"
                        onClick={() => onSendEntry?.(entry)}
                        disabled={Boolean(sendingKey) || !entry.has_phone}
                        title={entry.has_phone ? 'Open WhatsApp bill for this customer' : 'No phone saved for this customer'}
                      >
                        {String(sendingKey || '') === String(entry.name || '') ? 'Sending...' : (entry.has_phone ? 'Send Bill' : 'No Phone')}
                      </button>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6} className="empty-state">No overdue bill notifications right now.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
}

function SettingsView({ syncStatus, syncBusy, onPullNow, onRefreshWorkspace, onReloadStatus }) {
  const syncState = syncStatus?.sync_state || {};
  const postgresSnapshot = syncStatus?.postgres_snapshot || {};
  const cacheCounts = postgresSnapshot?.cache_counts || {};
  const latestErrorText = formatRuntimeSnapshot(postgresSnapshot.latest_error || syncState.last_error, 'None');
  const quotaWarning = /quota exceeded/i.test(latestErrorText) && /read requests/i.test(latestErrorText);

  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>Runtime Status</h3>
          <p>Inspect sync state, queue depth, and cached sheet row counts.</p>
        </div>

        <div className="settings-grid">
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Postgres Ready</span>
            <strong className="metric-value">{postgresSnapshot.ready ? 'Yes' : 'No'}</strong>
            <span className="metric-note">DB-first reads and queue writes.</span>
          </article>
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Sheets Connected</span>
            <strong className="metric-value">{syncStatus?.sheets_connected ? 'Yes' : 'No'}</strong>
            <span className="metric-note">Google Sheets API availability.</span>
          </article>
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Queue Pending</span>
            <strong className="metric-value">{formatCount(syncStatus?.queue_pending)}</strong>
            <span className="metric-note">Operations still waiting to replay.</span>
          </article>
          <article className="metric-card metric-card--soft">
            <span className="metric-label">Pull Interval</span>
            <strong className="metric-value">{formatCount(postgresSnapshot.pull_interval_sec)}</strong>
            <span className="metric-note">Seconds between background pulls.</span>
          </article>
        </div>

        <div className="table-wrap table-wrap--compact table-wrap--mobile-cards">
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
                    <td data-label="Cache">{key}</td>
                    <td data-label="Rows">{formatCount(getCacheRowCount(value))}</td>
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
            <p>Run a full refresh or trigger a manual pull from the sheet.</p>
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
            <p>Formatted directly from the runtime metadata returned by the API.</p>
          </div>

          <div className="meta-stack">
            <div className="meta-row">
              <span>Runtime status</span>
              <strong>{syncState.last_status || 'Unknown'}</strong>
            </div>
            <div className="meta-row">
              <span>Latest pull</span>
              <strong>{formatRuntimeSnapshot(postgresSnapshot.latest_pull, 'Not yet recorded')}</strong>
            </div>
            <div className="meta-row">
              <span>Latest error</span>
              <strong>{latestErrorText}</strong>
            </div>
            <div className="meta-row">
              <span>API Source</span>
              <strong>{getApiLabel()}</strong>
            </div>
          </div>

          {quotaWarning ? (
            <div className="notice compact">
              Google Sheets read quota was hit. The workspace now avoids loading Fix data during unrelated refreshes, so wait briefly and then try Reload Status or Pull Sheets Now again.
            </div>
          ) : null}
        </div>
      </aside>
    </section>
  );
}

const MemoSettingsView = React.memo(SettingsView);

function UsersView({
  users,
  usersLoading,
  usersBusy,
  userForm,
  setUserForm,
  onCreateUser,
  onRefreshUsers,
  onUpdateUserRole,
  onToggleUserStatus,
}) {
  return (
    <section className="workspace-row">
      <section className="content-panel content-panel--main">
        <div className="panel-header">
          <h3>User Management</h3>
          <p>Create user accounts and manage role/access state. This section is admin-only.</p>
        </div>

        <div className="table-wrap table-wrap--mobile-cards">
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Username</th>
                <th>Role</th>
                <th>Status</th>
                <th>Created</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {users.length ? (
                users.map((entry) => (
                  <tr key={entry.id}>
                    <td data-label="ID">{entry.id}</td>
                    <td data-label="Username">{entry.username}</td>
                    <td data-label="Role">
                      <select
                        value={entry.role}
                        onChange={(event) => onUpdateUserRole(entry.id, event.target.value)}
                        disabled={usersBusy}
                      >
                        <option value="admin">admin</option>
                        <option value="staff">staff</option>
                      </select>
                    </td>
                    <td data-label="Status">
                      <span className={entry.is_active ? 'status-pill status-pill--available' : 'status-pill status-pill--needs-details'}>
                        {entry.is_active ? 'ACTIVE' : 'DISABLED'}
                      </span>
                    </td>
                    <td data-label="Created">{formatShortStamp(entry.created_at)}</td>
                    <td data-label="Action">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => onToggleUserStatus(entry.id, !entry.is_active)}
                        disabled={usersBusy}
                      >
                        {entry.is_active ? 'Disable' : 'Enable'}
                      </button>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6} className="empty-state">{usersLoading ? 'Loading users...' : 'No users found.'}</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <aside className="content-panel content-panel--side content-panel--stacked">
        <div className="subpanel">
          <div className="panel-header">
            <h3>Create User</h3>
            <p>Add a new account and assign role immediately.</p>
          </div>

          <form className="form-stack" onSubmit={onCreateUser}>
            <label className="field-block">
              <span className="field-label">Username</span>
              <input
                type="text"
                value={userForm.username}
                onChange={(event) => setUserForm((current) => ({ ...current, username: event.target.value }))}
                required
              />
            </label>

            <label className="field-block">
              <span className="field-label">Password</span>
              <input
                type="password"
                value={userForm.password}
                onChange={(event) => setUserForm((current) => ({ ...current, password: event.target.value }))}
                required
              />
            </label>

            <label className="field-block">
              <span className="field-label">Role</span>
              <select
                value={userForm.role}
                onChange={(event) => setUserForm((current) => ({ ...current, role: event.target.value }))}
              >
                <option value="staff">staff</option>
                <option value="admin">admin</option>
              </select>
            </label>

            <label className="field-block">
              <span className="field-label">Active</span>
              <select
                value={userForm.is_active ? 'true' : 'false'}
                onChange={(event) => setUserForm((current) => ({ ...current, is_active: event.target.value === 'true' }))}
              >
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            </label>

            <div className="button-row button-row--end">
              <button type="submit" className="primary-button" disabled={usersBusy}>
                {usersBusy ? 'Saving...' : 'Create User'}
              </button>
              <button type="button" className="secondary-button" onClick={onRefreshUsers} disabled={usersBusy}>
                Refresh
              </button>
            </div>
          </form>
        </div>
      </aside>
    </section>
  );
}

const MemoClientsView = React.memo(ClientsView);

function WorkspaceApp({ currentUser, onLogout, userLoading = false }) {
  const isAdmin = String(currentUser?.role || '').toLowerCase() === 'admin';
  const [activeView, setActiveView] = useState('products');
  const [statusText, setStatusText] = useState('Ready');
  const [workspaceError, setWorkspaceError] = useState('');
  const [lastLoadedAt, setLastLoadedAt] = useState(null);
  const [revealedMetric, setRevealedMetric] = useState('');

  const [debtorsData, setDebtorsData] = useState({ sorted_debtors: [], total_debtors_amount: 0 });
  const [salesSnapshot, setSalesSnapshot] = useState({ daily_totals: [], week_totals: [] });
  const [selectedDebtor, setSelectedDebtor] = useState('');
  const [debtorSearch, setDebtorSearch] = useState('');
  const deferredDebtorSearch = useDeferredValue(debtorSearch);
  const [debtorPage, setDebtorPage] = useState(1);
  const [billText, setBillText] = useState('Select a debtor from the list to preview the bill.');
  const [outstandingItems, setOutstandingItems] = useState([]);
  const [paymentAmount, setPaymentAmount] = useState('');
  const [selectedServiceRow, setSelectedServiceRow] = useState('automatic');
  const [serviceActionBusy, setServiceActionBusy] = useState(false);
  const [paymentPlan, setPaymentPlan] = useState(null);
  const [paymentPlanError, setPaymentPlanError] = useState('');
  const [isDebtorDetailLoading, setIsDebtorDetailLoading] = useState(false);
  const [isApplyingPayment, setIsApplyingPayment] = useState(false);
  const [undoEnabled, setUndoEnabled] = useState(false);
  const [redoEnabled, setRedoEnabled] = useState(false);
  const [whatsappHistoryByName, setWhatsappHistoryByName] = useState({});
  const stockPreloadStartedRef = useRef(false);
  const stockFormPreloadStartedRef = useRef(false);
  const stockRequestSeqRef = useRef(0);
  const stockAbortControllerRef = useRef(null);
  const stockStaleDropCountRef = useRef(0);
  const [unpaidTodaySummary, setUnpaidTodaySummary] = useState({ count: 0, with_phone_count: 0, customers: [] });
  const [servicesTodayData, setServicesTodayData] = useState({ services: [], count: 0 });
  const [servicesTodayDate, setServicesTodayDate] = useState(formatDateForInput());
  const [servicesTodayBusy, setServicesTodayBusy] = useState(false);
  const [sendingTodayBills, setSendingTodayBills] = useState(false);
  const [refreshingDebtorsSection, setRefreshingDebtorsSection] = useState(false);
  const [sendingBillNotificationKey, setSendingBillNotificationKey] = useState('');
  const [stolenDevicesData, setStolenDevicesData] = useState({ items: [], count: 0 });
  const [stolenDevicesBusy, setStolenDevicesBusy] = useState(false);
  const [stolenDeviceForm, setStolenDeviceForm] = useState({ phone_name: '', imei_raw: '', note: '', source: '' });

  const [stockSearchText, setStockSearchText] = useState('');
  const deferredStockSearchText = useDeferredValue(stockSearchText);
  const [productFilterMode, setProductFilterMode] = useState('available');
  const [cartFilterMode, setCartFilterMode] = useState('available');
  const debouncedProductFilterMode = useDebouncedValue(productFilterMode, 140);
  const debouncedCartFilterMode = useDebouncedValue(cartFilterMode, 140);
  const [stockView, setStockView] = useState(null);
  const [stockForm, setStockForm] = useState({ visible_headers: [], defaults: {} });
  const [productFormValues, setProductFormValues] = useState({});
  const [stockPage, setStockPage] = useState(1);
  const [cartPage, setCartPage] = useState(1);
  const [isStockLoading, setIsStockLoading] = useState(false);
  const [isStockRefreshing, setIsStockRefreshing] = useState(false);
  const [isAddingProduct, setIsAddingProduct] = useState(false);
  const [isProductComposerOpen, setIsProductComposerOpen] = useState(false);
  const [stockErrorText, setStockErrorText] = useState('');
  const [selectedProductDetail, setSelectedProductDetail] = useState(null);
  const [isSavingProductDetail, setIsSavingProductDetail] = useState(false);
  const [saleCartItems, setSaleCartItems] = useState([]);
  const [cartBusy, setCartBusy] = useState(false);
  const [serviceDraft, setServiceDraft] = useState({
    name: '',
    phone: '',
    description: '',
    internal_note: '',
    deal_location: '',
    price: '',
    service_expense: '',
    amount_paid: '',
    payment_method: 'CASH',
    fulfillment_method: 'WALK-IN PICKUP',
    pickup_mode: 'BUYER',
    representative_name: '',
    representative_phone: '',
    status: 'UNPAID',
  });
  const [serviceBusy, setServiceBusy] = useState(false);
  const [servicePendingDeals, setServicePendingDeals] = useState({ items: [], count: 0 });
  const [returningPendingKey, setReturningPendingKey] = useState('');
  const [updatingPendingKey, setUpdatingPendingKey] = useState('');
  const [updatingPendingMetaKey, setUpdatingPendingMetaKey] = useState('');
  const [currentTimeLabel, setCurrentTimeLabel] = useState(new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }));

  const [clientsData, setClientsData] = useState({ entries: [], registry: {}, stats: {} });
  const [clientSearch, setClientSearch] = useState('');
  const deferredClientSearch = useDeferredValue(clientSearch);
  const [clientPage, setClientPage] = useState(1);
  const [clientForm, setClientForm] = useState({ name: '', phone: '', gender: '' });
  const [clientOriginalName, setClientOriginalName] = useState('');
  const [clientsBusy, setClientsBusy] = useState(false);

  const [googleContactsData, setGoogleContactsData] = useState({ contacts: [], total_cached: 0, synced_at: '' });
  const [googleSearch, setGoogleSearch] = useState('');
  const deferredGoogleSearch = useDeferredValue(googleSearch);
  const [googleContactPage, setGoogleContactPage] = useState(1);
  const [selectedGoogleContact, setSelectedGoogleContact] = useState(null);
  const [googleContactsBusy, setGoogleContactsBusy] = useState(false);
  const [googleContactsError, setGoogleContactsError] = useState('');
  const [googleContactsLoadAttempted, setGoogleContactsLoadAttempted] = useState(false);

  const [nameFixData, setNameFixData] = useState({ mismatches: [], count: 0 });
  const [selectedMismatchRaw, setSelectedMismatchRaw] = useState('');
  const [correctName, setCorrectName] = useState('');
  const [isNameFixLoading, setIsNameFixLoading] = useState(false);
  const [isNameFixApplying, setIsNameFixApplying] = useState(false);
  const [nameFixLoadAttempted, setNameFixLoadAttempted] = useState(false);

  const [syncStatus, setSyncStatus] = useState(null);
  const [syncBusy, setSyncBusy] = useState(false);
  const [cashflowSummary, setCashflowSummary] = useState(null);
  const [weeklyAllowance, setWeeklyAllowance] = useState(null);
  const [cashflowExpenses, setCashflowExpenses] = useState([]);
  const [cashflowTransactions, setCashflowTransactions] = useState([]);
  const [cashflowCapital, setCashflowCapital] = useState({ month_total: 0, week_total: 0, entries: [] });
  const [cashflowExpenseSource, setCashflowExpenseSource] = useState('database');
  const [cashflowExpenseSheetTitle, setCashflowExpenseSheetTitle] = useState('CASH FLOW');
  const [cashflowLoading, setCashflowLoading] = useState(false);
  const [cashflowError, setCashflowError] = useState('');
  const [cashflowExpenseBusy, setCashflowExpenseBusy] = useState(false);
  const [cashflowExpenseError, setCashflowExpenseError] = useState('');
  const [cashflowUpdatedAt, setCashflowUpdatedAt] = useState(null);
  const [coreLoading, setCoreLoading] = useState(false);
  const [logoData, setLogoData] = useState({ data_url: '', file_name: '' });
  const [usersData, setUsersData] = useState([]);
  const [usersLoading, setUsersLoading] = useState(false);
  const [usersBusy, setUsersBusy] = useState(false);
  const [userForm, setUserForm] = useState({
    username: '',
    password: '',
    role: 'staff',
    is_active: true,
  });

  useRenderTiming('WorkspaceApp', `${activeView}:${coreLoading ? 'core' : userLoading ? 'loading' : 'ready'}`);

  const allowedViews = useMemo(() => {
    if (isAdmin) {
      return new Set(Object.keys(VIEW_META));
    }
    return new Set(STAFF_ALLOWED_VIEWS);
  }, [isAdmin]);

  const billNotificationEntries = useMemo(() => {
    const now = Date.now();
    const fourDaysMs = 4 * 24 * 60 * 60 * 1000;
    const oldestUnpaidByName = debtorsData?.oldest_unpaid_date_by_name || {};
    const rows = (debtorsData.sorted_debtors || []).map(([name, amount]) => {
      const balance = Number(amount || 0);
      if (!(balance > 0)) {
        return null;
      }
      const stats = whatsappHistoryByName?.[name] || {};
      const lastSent = stats.last_sent_at ? new Date(stats.last_sent_at).getTime() : 0;
      const oldestUnpaidDateRaw = oldestUnpaidByName?.[name] || '';
      const oldestUnpaidTs = oldestUnpaidDateRaw ? new Date(oldestUnpaidDateRaw).getTime() : 0;
      const baselineTs = (!lastSent || Number.isNaN(lastSent))
        ? (oldestUnpaidTs && !Number.isNaN(oldestUnpaidTs) ? oldestUnpaidTs : now)
        : lastSent;
      const elapsedMs = now - baselineTs;
      if (!(elapsedMs > fourDaysMs)) {
        return null;
      }

      const daysSince = Math.max(0, Math.floor(elapsedMs / (24 * 60 * 60 * 1000)));
      const phone = normalizeWhatsappPhone(clientsData.registry?.[name] || '');

      return {
        name,
        outstanding: balance,
        days_since_last_bill: daysSince,
        last_sent_at: stats.last_sent_at || '',
        send_count: Number(stats.send_count || 0),
        has_phone: Boolean(phone),
        phone,
      };
    }).filter(Boolean);

    rows.sort((a, b) => {
      if ((b.days_since_last_bill || 0) !== (a.days_since_last_bill || 0)) {
        return (b.days_since_last_bill || 0) - (a.days_since_last_bill || 0);
      }
      return (b.outstanding || 0) - (a.outstanding || 0);
    });

    return rows;
  }, [debtorsData.sorted_debtors, debtorsData.oldest_unpaid_date_by_name, whatsappHistoryByName, clientsData.registry]);

  const billNotificationCount = billNotificationEntries.length;

  const visibleActionItems = useMemo(() => (
    ACTION_ITEMS.filter((item) => {
      if (item.key === 'exit') {
        return true;
      }
      if (item.type === 'action') {
        return isAdmin || item.key === 'import_phones' || item.key === 'logout' || item.key === 'refresh' || item.key === 'bill_notifications';
      }
      if (item.type === 'view') {
        return isAdmin || STAFF_ALLOWED_VIEWS.has(item.key);
      }
      return false;
    }).map((item) => {
      if (item.key === 'bill_notifications') {
        return { ...item, badge: billNotificationCount };
      }
      return item;
    })
  ), [billNotificationCount, isAdmin]);

  const productSummaryColumns = useMemo(() => {
    const baseKeys = ['description', 'colour', 'storage', 'imei', 'seller'];
    const optionalKeys = [];

    if (productFilterMode === 'pending') {
      optionalKeys.push('selling_price', 'buyer');
    }

    if (isAdmin) {
      optionalKeys.splice(0, 0, 'cost_price');
    }

    const visibleKeys = new Set([...baseKeys, ...optionalKeys]);
    return PRODUCT_SUMMARY_COLUMNS.filter((column) => visibleKeys.has(column.key));
  }, [isAdmin, productFilterMode]);

  const cartSummaryColumns = useMemo(() => {
    const baseKeys = ['description', 'colour', 'storage', 'imei', 'seller'];
    const optionalKeys = [];

    if (cartFilterMode === 'pending') {
      optionalKeys.push('selling_price', 'buyer');
    }

    if (isAdmin) {
      optionalKeys.splice(0, 0, 'cost_price');
    }

    const visibleKeys = new Set([...baseKeys, ...optionalKeys]);
    return PRODUCT_SUMMARY_COLUMNS.filter((column) => visibleKeys.has(column.key));
  }, [isAdmin, cartFilterMode]);

  const coreViews = useMemo(() => new Set(['home', 'cashflow', 'debtors', 'clients', 'fix', 'settings']), []);
  const [hasCoreLoaded, setHasCoreLoaded] = useState(false);

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

  const filteredGoogleContacts = (googleContactsData.contacts || []).filter((contact) => {
    const query = normalizeSearchValue(deferredGoogleSearch);
    if (!query) {
      return true;
    }
    return normalizeSearchValue(contact.name).includes(query) || normalizeSearchValue(contact.phone).includes(query);
  });

  const googleContactsView = {
    ...googleContactsData,
    contacts: filteredGoogleContacts,
  };

  const normalizedStockSearchQuery = useMemo(
    () => normalizeSearchValue(deferredStockSearchText),
    [deferredStockSearchText]
  );

  const stockRowsForSearch = useMemo(
    () => (Array.isArray(stockView?.all_rows_cache) ? stockView.all_rows_cache : []),
    [stockView?.all_rows_cache]
  );

  const stockRowSearchIndex = useMemo(
    () => stockRowsForSearch.map((row) => {
      const paddedPreview = Array.isArray(row?.padded) ? row.padded.slice(0, 24) : [];
      return normalizeSearchValue([
        row?.row_num,
        row?.label,
        row?.inventory_status,
        row?.inventory_amount_paid,
        row?.description,
        row?.buyer_name,
        row?.buyer_phone,
        row?.imei,
        row?.date,
        ...paddedPreview,
      ].join(' '));
    }),
    [stockRowsForSearch]
  );

  const filteredStockRows = useMemo(() => {
    if (!normalizedStockSearchQuery) {
      return stockRowsForSearch;
    }

    return stockRowsForSearch.filter((_, index) => (
      (stockRowSearchIndex[index] || '').includes(normalizedStockSearchQuery)
    ));
  }, [normalizedStockSearchQuery, stockRowsForSearch, stockRowSearchIndex]);

  const selectedMismatch = (nameFixData.mismatches || []).find(
    (entry) => normalizeSearchValue(entry.raw) === normalizeSearchValue(selectedMismatchRaw)
  );

  const activeMeta = VIEW_META[activeView] || (isAdmin ? VIEW_META.home : VIEW_META.products);
  const clientNameOptions = useMemo(() => (
    Array.from(new Set([
      ...Object.keys(clientsData.registry || {}),
    ]))
      .filter((name) => String(name || '').trim())
      .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }))
      .slice(0, 500)
  ), [clientsData.registry]);
  const sellerPhoneOptions = useMemo(() => (
    Array.from(new Set([
      ...(clientsData.entries || []).map((entry) => {
        const name = String(entry.name || '').trim();
        const phone = normalizeWhatsappPhone(entry.phone || '');
        return name && phone ? `${name} - ${phone}` : '';
      }).filter(Boolean),
    ])).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' })).slice(0, 600)
  ), [clientsData.entries]);
  const sellerPhoneByName = useMemo(() => {
    const mapping = {};
    (googleContactsData.contacts || []).forEach((entry) => {
      const name = String(entry.name || '').trim().toUpperCase();
      const phone = normalizeWhatsappPhone(entry.phone || '');
      if (name && phone && !mapping[name]) {
        mapping[name] = phone;
      }
    });
    (clientsData.entries || []).forEach((entry) => {
      const name = String(entry.name || '').trim().toUpperCase();
      const phone = normalizeWhatsappPhone(entry.phone || '');
      if (name && phone) {
        mapping[name] = phone;
      }
    });
    return mapping;
  }, [clientsData.entries, googleContactsData.contacts]);
  const sellerNameOptions = useMemo(
    () => Object.keys(sellerPhoneByName).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' })),
    [sellerPhoneByName]
  );
  const contactAutofillOptions = useMemo(() => {
    const seen = new Set();
    const options = [];
    [...(googleContactsData.contacts || []), ...(clientsData.entries || [])].forEach((entry) => {
      const name = String(entry.name || '').trim();
      const phone = normalizeWhatsappPhone(entry.phone || '');
      if (!name || !phone) {
        return;
      }
      const key = `${name.toUpperCase()}|${phone}`;
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      options.push({ label: `${name} - ${phone}`, name, phone });
    });
    return options.sort((left, right) => left.name.localeCompare(right.name, undefined, { sensitivity: 'base' }));
  }, [clientsData.entries, googleContactsData.contacts]);
  const soldUnpaidStockRows = useMemo(() => {
    // Find SOLD items with unpaid or partial payments
    return (stockView?.all_rows_cache || []).filter((row) => {
      const label = String(row.label || '').toUpperCase();
      if (label !== 'SOLD') return false;
      const invStatus = String(row.inventory_status || '').toUpperCase();
      return invStatus === 'UNPAID' || invStatus === 'PART PAYMENT';
    });
  }, [stockView?.all_rows_cache]);

  const pendingDealEntries = useMemo(() => {
    // Only include service entries in floating pending deals
    const serviceEntries = (servicePendingDeals.items || []).map((row) => ({
      ...row,
      inventory_status: row.status,
      inventory_amount_paid: row.amount_paid,
    }));
    // Add SOLD items with unpaid/partial payments to pending deals
    const unaidSoldStockEntries = (soldUnpaidStockRows || []).map((row) => ({
      kind: 'stock',
      row_num: row.row_num,
      description: getProductCellValue(row, stockView?.headers || [], ['DESCRIPTION', 'MODEL', 'DEVICE']) || '',
      buyer_name: getProductCellValue(row, stockView?.headers || [], ['NAME OF BUYER']) || '',
      buyer_phone: getProductCellValue(row, stockView?.headers || [], ['PHONE NUMBER OF BUYER']) || '',
      date: getProductCellValue(row, stockView?.headers || [], ['DATE', 'DATE BOUGHT', 'AVAILABILITY/DATE SOLD']) || '',
      imei: getProductCellValue(row, stockView?.headers || [], ['IMEI']) || '',
      price: getProductCellValue(row, stockView?.headers || [], ['PRICE', 'AMOUNT SOLD', 'SELLING PRICE']) || '',
      amount_paid: getProductCellValue(row, stockView?.headers || [], ['AMOUNT PAID']) || '',
      payment_method: getProductCellValue(row, stockView?.headers || [], ['PAYMENT METHOD']) || '',
      fulfillment_method: getProductCellValue(row, stockView?.headers || [], ['FULFILLMENT METHOD', 'DELIVERY METHOD']) || '',
      pickup_mode: getProductCellValue(row, stockView?.headers || [], ['PICKUP MODE', 'PICKUP TYPE']) || '',
      representative_name: getProductCellValue(row, stockView?.headers || [], ['REPRESENTATIVE NAME', 'PICKUP REPRESENTATIVE NAME']) || '',
      representative_phone: getProductCellValue(row, stockView?.headers || [], ['REPRESENTATIVE PHONE', 'PICKUP REPRESENTATIVE PHONE']) || '',
      swap_type: getProductCellValue(row, stockView?.headers || [], ['SWAP TYPE']) || '',
      swap_detail: getProductCellValue(row, stockView?.headers || [], ['SWAP DETAIL', 'SWAP DETAILS']) || '',
      swap_cash_amount: getProductCellValue(row, stockView?.headers || [], ['SWAP CASH AMOUNT', 'SWAP CASH']) || '',
      inventory_status: row.inventory_status,
      inventory_amount_paid: row.inventory_amount_paid,
    }));
    return [...serviceEntries, ...unaidSoldStockEntries].sort((left, right) => Number(right.row_num || 0) - Number(left.row_num || 0));
  }, [soldUnpaidStockRows, servicePendingDeals.items, stockView?.headers]);

  function applyClientRegistryToState(registry) {
    const nextData = normalizeClientsPayload(registry);
    setClientsData(nextData);
    return nextData;
  }

  async function loadCoreWorkspace(forceRefresh = false) {
    setCoreLoading(true);
    setWorkspaceError('');

    if (!isAdmin) {
      const pendingServicesResult = await Promise.allSettled([
        fetchPendingServiceDeals({ forceRefresh }),
      ]);

      const failures = [];
      const pendingResult = pendingServicesResult[0];
      if (pendingResult.status === 'fulfilled') {
        setServicePendingDeals(pendingResult.value || { items: [], count: 0 });
      } else {
        failures.push(pendingResult.reason?.message || 'Could not load pending service deals.');
      }

      const uniqueFailures = Array.from(new Set(failures.filter(Boolean).map((item) => String(item).trim()).filter(Boolean)));
      setWorkspaceError(uniqueFailures.join(' | '));
      setStatusText(uniqueFailures.length ? uniqueFailures[0] : forceRefresh ? 'Workspace refreshed.' : 'Ready');
      setLastLoadedAt(new Date());
      setHasCoreLoaded(true);
      setCoreLoading(false);
      return;
    }

    const bootstrapResult = await fetchHomeBootstrap({ forceRefresh });

    const failures = [];

    if (bootstrapResult?.debtors) {
      const nextDebtors = bootstrapResult.debtors;
      setDebtorsData(nextDebtors);
      const availableNames = (nextDebtors.sorted_debtors || []).map(([name]) => name);
      const nextSelected = availableNames.includes(selectedDebtor) ? selectedDebtor : availableNames[0] || '';
      startTransition(() => setSelectedDebtor(nextSelected));
    } else {
      failures.push('Could not load debtors.');
    }

    if (bootstrapResult?.sales_snapshot) {
      setSalesSnapshot(bootstrapResult.sales_snapshot);
    } else {
      failures.push('Could not load sales snapshot.');
    }

    if (bootstrapResult?.sync_status) {
      setSyncStatus(bootstrapResult.sync_status);
    } else {
      failures.push('Could not load sync status.');
    }

    const uniqueFailures = Array.from(new Set(failures.filter(Boolean).map((item) => String(item).trim()).filter(Boolean)));

    writeSessionCache(WORKSPACE_CORE_CACHE_KEY, {
      debtorsData: bootstrapResult?.debtors || { sorted_debtors: [], total_debtors_amount: 0 },
      salesSnapshot: bootstrapResult?.sales_snapshot || { daily_totals: [], week_totals: [] },
      syncStatus: bootstrapResult?.sync_status || null,
      cached_at: new Date().toISOString(),
    });

    setLastLoadedAt(new Date());
    setWorkspaceError(uniqueFailures.join(' | '));
    setStatusText(uniqueFailures.length ? uniqueFailures[0] : forceRefresh ? 'Workspace refreshed.' : 'Ready');
    setHasCoreLoaded(true);
    setCoreLoading(false);

    // Keep first paint fast: hydrate clients/whatsapp/unpaid and the secondary workspace data in the background.
    window.setTimeout(() => {
      Promise.allSettled([
        fetchPendingServiceDeals({ forceRefresh }),
        fetchClients({ forceReload: forceRefresh }),
        fetchWhatsappHistory({ forceRefresh }),
        fetchUnpaidToday({ forceRefresh }),
      ]).then(([
      pendingServicesResult,
      clientsResult,
      whatsappHistoryResult,
      unpaidTodayResult,
    ]) => {
      if (pendingServicesResult.status === 'fulfilled') {
        setServicePendingDeals(pendingServicesResult.value || { items: [], count: 0 });
      }
      if (clientsResult.status === 'fulfilled') {
        setClientsData(normalizeClientsPayload(clientsResult.value));
      }
      if (whatsappHistoryResult.status === 'fulfilled') {
        setWhatsappHistoryByName(whatsappHistoryResult.value?.by_name || {});
      }
      if (unpaidTodayResult.status === 'fulfilled') {
        setUnpaidTodaySummary(unpaidTodayResult.value || { count: 0, with_phone_count: 0, customers: [] });
      }
      });
    }, 180);
  }

  async function loadNameFixes({ forceRefresh = false, silent = false } = {}) {
    setIsNameFixLoading(true);
    setNameFixLoadAttempted(true);

    try {
      const result = await fetchNameFixes({ forceRefresh });
      setNameFixData(result);

      const nextSelectedRaw = result.mismatches?.some(
        (entry) => normalizeSearchValue(entry.raw) === normalizeSearchValue(selectedMismatchRaw)
      )
        ? selectedMismatchRaw
        : result.mismatches?.[0]?.raw || '';
      const selectedEntry = result.mismatches?.find(
        (entry) => normalizeSearchValue(entry.raw) === normalizeSearchValue(nextSelectedRaw)
      );

      setSelectedMismatchRaw(nextSelectedRaw);
      setCorrectName(selectedEntry?.candidates?.[0] || '');
      return result;
    } catch (error) {
      if (!silent) {
        setStatusText(error.message || 'Could not load name-fix data.');
      }
      return null;
    } finally {
      setIsNameFixLoading(false);
    }
  }

  async function loadCashflowDashboard(forceRefresh = false) {
    if (!isAdmin) {
      return;
    }

    const defaultSummary = {
      total_cash_in: 0,
      expected_income: 0,
      total_expenses: 0,
      total_cost: 0,
      net_profit: 0,
      receivables_excluded: 0,
      reserve_percentage: 0,
      reserve_amount: 0,
      available_cash: 0,
      available_cash_before_reserve: 0,
      current_week_cash_in: 0,
      current_week_expenses: 0,
      current_week_phone_profit: 0,
      current_week_service_profit: 0,
      current_week_net_cash_flow: 0,
      current_week_net_profit: 0,
      allowance_base_net_profit: 0,
      current_week_allowance_expenses: 0,
      current_week_business_only_expenses: 0,
      monthly_fixed_overhead: 0,
      monthly_allowance_paid: 0,
      monthly_allowance_provision: 0,
      month_remainder_profit_after_paid_allowance: 0,
      month_remainder_profit_after_provision: 0,
      cash_runway_weeks: 0,
      cash_health_status: 'red',
      capital_outflow_month: 0,
      capital_outflow_week: 0,
      current_week_start: '',
      current_week_end: '',
    };
    const defaultAllowance = {
      suggested_allowance: 0,
      calculation_date: '',
      previous_week_profit: 0,
    };

    setCashflowLoading(true);
    setCashflowError('');
    try {
      const result = await fetchFoundationCashflowDashboard({ forceRefresh });

      const nextSummary = result?.summary || defaultSummary;
      const nextAllowance = result?.weekly_allowance || defaultAllowance;

      setCashflowSummary(nextSummary);
      setWeeklyAllowance(nextAllowance);
      setCashflowExpenses(Array.isArray(result?.expenses) ? result.expenses : []);
      setCashflowTransactions(Array.isArray(result?.transactions) ? result.transactions : []);
      setCashflowCapital(result?.capital || { month_total: 0, week_total: 0, entries: [] });
      setCashflowExpenseSource(result?.expense_source || nextSummary.expense_source || 'database');
      setCashflowExpenseSheetTitle(result?.expense_sheet_title || 'CASH FLOW');
      setCashflowExpenseError('');
      setCashflowUpdatedAt(new Date());

      setCashflowError('');
    } catch (error) {
      const message = error?.message || 'Could not load cashflow dashboard.';
      setCashflowError(message);
      setStatusText(message);
    } finally {
      setCashflowLoading(false);
    }
  }

  async function handleCreateCashflowExpense(expenseDraft) {
    setCashflowExpenseBusy(true);
    setCashflowExpenseError('');

    try {
      await createFoundationExpense({
        amount: Number(expenseDraft.amount || 0),
        category: expenseDraft.category,
        description: expenseDraft.description,
        date: expenseDraft.date,
        allowanceImpact: expenseDraft.allowance_impact,
      });
      await loadCashflowDashboard(true);
      return true;
    } catch (error) {
      const message = error?.message || 'Could not save expense.';
      setCashflowExpenseError(message);
      setStatusText(message);
      return false;
    } finally {
      setCashflowExpenseBusy(false);
    }
  }

  async function handleUndoLastAllowanceWithdrawal() {
    setCashflowExpenseBusy(true);
    setCashflowExpenseError('');
    try {
      const result = await undoLastWeeklyAllowanceWithdrawal();
      await loadCashflowDashboard(true);
      setStatusText(
        result?.removed_amount
          ? `Undid latest weekly allowance withdrawal: ${formatCurrency(result.removed_amount)}.`
          : 'Undid latest weekly allowance withdrawal.'
      );
      return true;
    } catch (error) {
      const message = error?.message || 'Could not undo the latest allowance withdrawal.';
      setCashflowExpenseError(message);
      setStatusText(message);
      return false;
    } finally {
      setCashflowExpenseBusy(false);
    }
  }

  async function loadServicesTodayForDate(dateText, forceRefresh = false) {
    const nextDate = String(dateText || '').trim() || formatDateForInput();
    setServicesTodayBusy(true);
    try {
      const result = await fetchServicesToday({ forceRefresh, targetDate: nextDate });
      setServicesTodayData(result || { services: [], count: 0 });
      setServicesTodayDate(nextDate);
      setStatusText(`Loaded ${formatCount(result?.count || 0)} service(s) for ${nextDate}.`);
    } catch (error) {
      setStatusText(error.message || 'Could not load services for the selected date.');
    } finally {
      setServicesTodayBusy(false);
    }
  }

  async function loadStolenDevices(includeInactive = true) {
    if (!isAdmin) {
      return;
    }
    setStolenDevicesBusy(true);
    try {
      const result = await fetchStolenDevices({ includeInactive });
      setStolenDevicesData(result || { items: [], count: 0 });
    } catch (error) {
      setStatusText(error.message || 'Could not load stolen device registry.');
    } finally {
      setStolenDevicesBusy(false);
    }
  }

  async function handleCheckStolenImei(imei) {
    const result = await checkStolenDeviceImei({ imei });
    return result;
  }

  async function handleUpdateServicesTodayEntry({ rowNum, currentName, nextName }) {
    if (!rowNum || !currentName || !nextName) {
      return;
    }

    setServicesTodayBusy(true);
    try {
      await updateDebtorService({
        nameInput: currentName,
        rowIdx: rowNum - 1,
        newName: nextName,
        forceRefresh: true,
      });
      await loadServicesTodayForDate(servicesTodayDate, true);
      await loadCoreWorkspace(true);
      if (selectedDebtor && String(selectedDebtor).trim().toUpperCase() === String(currentName).trim().toUpperCase()) {
        setSelectedDebtor(String(nextName).trim().toUpperCase());
      }
      setStatusText(`Updated service row #${rowNum} customer to ${String(nextName).trim().toUpperCase()}.`);
    } catch (error) {
      setStatusText(error.message || 'Could not update the service customer.');
      throw error;
    } finally {
      setServicesTodayBusy(false);
    }
  }

  async function handleUpdateServicesTodayPayment({ rowNum, paymentStatus, amountPaid }) {
    if (!rowNum) {
      return;
    }
    setServicesTodayBusy(true);
    try {
      await updateSalesTodayPayment({
        rowNum,
        paymentStatus,
        amountPaid,
        forceRefresh: true,
      });
      await loadServicesTodayForDate(servicesTodayDate, true);
      await loadCoreWorkspace(true);
      if (selectedDebtor) {
        await loadSelectedDebtorDetails(selectedDebtor, true);
      }
      setStatusText(`Applied payment for sales row #${rowNum}.`);
    } catch (error) {
      setStatusText(error.message || 'Could not apply payment for this sales row.');
      throw error;
    } finally {
      setServicesTodayBusy(false);
    }
  }

  async function handleCreateStolenDevice(event) {
    event.preventDefault();
    setStolenDevicesBusy(true);
    try {
      await createStolenDevice({
        phoneName: stolenDeviceForm.phone_name,
        imeiRaw: stolenDeviceForm.imei_raw,
        note: stolenDeviceForm.note,
        source: stolenDeviceForm.source,
      });
      setStolenDeviceForm({ phone_name: '', imei_raw: '', note: '', source: '' });
      await loadStolenDevices(true);
      setStatusText('Stolen device record added.');
    } catch (error) {
      setStatusText(error.message || 'Could not add stolen device record.');
    } finally {
      setStolenDevicesBusy(false);
    }
  }

  async function handleToggleStolenDevice(record) {
    if (!record?.id) {
      return;
    }
    setStolenDevicesBusy(true);
    try {
      await updateStolenDevice({
        recordId: record.id,
        isActive: !Boolean(record.is_active),
        clearedNote: !Boolean(record.is_active) ? '' : 'Cleared from registry',
      });
      await loadStolenDevices(true);
      setStatusText(`Stolen device record ${record.is_active ? 'cleared' : 'reactivated'}.`);
    } catch (error) {
      setStatusText(error.message || 'Could not update stolen device record.');
    } finally {
      setStolenDevicesBusy(false);
    }
  }

  async function loadGoogleContacts({ forceRefresh = false, silent = false } = {}) {
    setGoogleContactsBusy(true);
    setGoogleContactsError('');
    setGoogleContactsLoadAttempted(true);

    try {
      const result = forceRefresh
        ? await syncGoogleContacts()
        : await fetchGoogleContacts();

      setGoogleContactsData(result);
      setSelectedGoogleContact((current) => {
        if (!current) {
          return null;
        }

        return (result.contacts || []).some(
          (contact) => contact.name === current.name && contact.phone === current.phone
        )
          ? current
          : null;
      });

      return result;
    } catch (error) {
      const message = error.message || (forceRefresh ? 'Could not sync Google Contacts.' : 'Could not load Google Contacts.');
      setGoogleContactsError(message);
      if (!silent) {
        setStatusText(message);
      }
      return null;
    } finally {
      setGoogleContactsBusy(false);
    }
  }

  async function loadStock(forceRefresh = false, showLoader = true, explicitFilterMode = '') {
    const activeFilterMode = explicitFilterMode || (activeView === 'cart' ? cartFilterMode : productFilterMode);
    const requestSeq = stockRequestSeqRef.current + 1;
    stockRequestSeqRef.current = requestSeq;
    if (stockAbortControllerRef.current) {
      stockAbortControllerRef.current.abort();
    }
    const abortController = new AbortController();
    stockAbortControllerRef.current = abortController;
    const startedAt = performance.now();

    setStockErrorText('');
    if (showLoader) {
      if (stockView) {
        setIsStockRefreshing(true);
      } else {
        setIsStockLoading(true);
      }
    }

    try {
      const result = await fetchStockDashboard({
        // Text search is filtered locally to keep typing instant.
        filterText: '',
        filterMode: activeFilterMode,
        forceRefresh,
        signal: abortController.signal,
      });
      if (requestSeq !== stockRequestSeqRef.current) {
        stockStaleDropCountRef.current += 1;
        recordStockFilterDiagnostic({
          filterMode: activeFilterMode,
          durationMs: performance.now() - startedAt,
          outcome: 'stale_drop',
          staleDrops: stockStaleDropCountRef.current,
        });
        return;
      }
      setStockView(result);
      try {
        sessionStorage.setItem(STOCK_VIEW_CACHE_KEY, JSON.stringify(result));
      } catch {
        // Ignore cache storage failures.
      }
      setLastLoadedAt(new Date());
      if (forceRefresh) {
        setStatusText('Products refreshed.');
      }
      recordStockFilterDiagnostic({
        filterMode: activeFilterMode,
        durationMs: performance.now() - startedAt,
        outcome: 'success',
        staleDrops: stockStaleDropCountRef.current,
      });
    } catch (error) {
      if (requestSeq !== stockRequestSeqRef.current) {
        stockStaleDropCountRef.current += 1;
        recordStockFilterDiagnostic({
          filterMode: activeFilterMode,
          durationMs: performance.now() - startedAt,
          outcome: 'stale_drop',
          staleDrops: stockStaleDropCountRef.current,
        });
        return;
      }

      const aborted = error?.name === 'AbortError' || String(error?.message || '').toLowerCase().includes('abort');
      if (aborted) {
        recordStockFilterDiagnostic({
          filterMode: activeFilterMode,
          durationMs: performance.now() - startedAt,
          outcome: 'aborted',
          staleDrops: stockStaleDropCountRef.current,
        });
        return;
      }

      const message = error.message || 'Could not load products.';
      if (stockView?.all_rows_cache?.length) {
        // Keep last loaded data visible during transient API/quota failures.
        setStockErrorText('');
        setStatusText(`${message} Showing last loaded stock data.`);
      } else {
        setStockErrorText(message);
        setStatusText(message);
      }
      recordStockFilterDiagnostic({
        filterMode: activeFilterMode,
        durationMs: performance.now() - startedAt,
        outcome: 'error',
        staleDrops: stockStaleDropCountRef.current,
      });
    } finally {
      if (stockAbortControllerRef.current === abortController) {
        stockAbortControllerRef.current = null;
      }
      if (requestSeq === stockRequestSeqRef.current) {
        setIsStockLoading(false);
        setIsStockRefreshing(false);
      }
    }
  }

  async function loadStockForm(forceRefresh = false, resetForm = false) {
    try {
      const result = await fetchStockForm({ forceRefresh });
      setStockForm(result);
      try {
        sessionStorage.setItem(STOCK_FORM_CACHE_KEY, JSON.stringify(result));
      } catch {
        // Ignore cache storage failures.
      }
      setProductFormValues((current) => {
        if (!resetForm && Object.keys(current).length) {
          return current;
        }
        return buildProductFormValues(result);
      });
      if (result.cost_price_inserted || ((stockView?.headers_upper || []).indexOf('COST PRICE') === -1 && (result.headers_upper || []).includes('COST PRICE'))) {
        await loadStock(true);
      }
    } catch (error) {
      setStatusText(error.message || 'Could not load the product form.');
    }
  }

  async function loadSelectedDebtorDetails(name, forceRefresh = false) {
    if (!name) {
      setBillText('Select a debtor from the list to preview the bill.');
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
      setBillText(error.message || 'Could not load the bill preview.');
      setOutstandingItems([]);
    } finally {
      setIsDebtorDetailLoading(false);
    }
  }

  async function handleFullRefresh() {
    setSyncBusy(true);
    try {
      await refreshWorkspace({ forceRefresh: true });
      await loadCoreWorkspace(true);
      await loadStock(true);
      await loadStockForm(true, true);
      await loadSelectedDebtorDetails(selectedDebtor, true);
      const nameFixResult = activeView === 'home' || activeView === 'fix'
        ? await loadNameFixes({ forceRefresh: true, silent: true })
        : true;
      setStatusText(
        nameFixResult === null
          ? 'Workspace refreshed, but the Fix scan is unavailable right now.'
          : 'Workspace refreshed from sheet and cache.'
      );
    } catch (error) {
      setStatusText(error.message || 'Could not refresh the workspace.');
    } finally {
      setSyncBusy(false);
    }
  }

  useEffect(() => {
    setHasCoreLoaded(false);
    setCoreLoading(false);
    setWorkspaceError('');
    setStatusText('Ready');
  }, [isAdmin]);

  useEffect(() => {
    if (!isAdmin) {
      setLogoData({ data_url: '', file_name: '' });
      return undefined;
    }

    let active = true;

    fetchDashboardLogo()
      .then((result) => {
        if (active) {
          setLogoData(result || { data_url: '', file_name: '' });
        }
      })
      .catch(() => {
        if (active) {
          setLogoData({ data_url: '', file_name: '' });
        }
      });

    return () => {
      active = false;
    };
  }, [isAdmin]);

  useEffect(() => {
    if (allowedViews.has(activeView)) {
      return;
    }

    startTransition(() => setActiveView(isAdmin ? 'home' : 'products'));
  }, [activeView, allowedViews, isAdmin]);

  useEffect(() => {
    if (activeView !== 'home' && activeView !== 'fix') {
      return;
    }
    if (nameFixLoadAttempted) {
      return;
    }

    loadNameFixes({ forceRefresh: false, silent: true });
  }, [activeView, nameFixLoadAttempted]);

  useEffect(() => {
    try {
      const cachedCore = readSessionCache(WORKSPACE_CORE_CACHE_KEY);
      if (cachedCore && typeof cachedCore === 'object') {
        if (cachedCore.debtorsData) {
          setDebtorsData(cachedCore.debtorsData);
        }
        if (cachedCore.salesSnapshot) {
          setSalesSnapshot(cachedCore.salesSnapshot);
        }
        if (cachedCore.syncStatus) {
          setSyncStatus(cachedCore.syncStatus);
        }
        if (cachedCore.cached_at) {
          setLastLoadedAt(new Date(cachedCore.cached_at));
        }
      }

      const cachedStockViewText = sessionStorage.getItem(STOCK_VIEW_CACHE_KEY);
      if (cachedStockViewText) {
        const cachedStockView = JSON.parse(cachedStockViewText);
        if (cachedStockView && typeof cachedStockView === 'object') {
          setStockView(cachedStockView);
        }
      }

      const cachedStockFormText = sessionStorage.getItem(STOCK_FORM_CACHE_KEY);
      if (cachedStockFormText) {
        const cachedStockForm = JSON.parse(cachedStockFormText);
        if (cachedStockForm && typeof cachedStockForm === 'object') {
          setStockForm(cachedStockForm);
          setProductFormValues((current) => (
            Object.keys(current || {}).length ? current : buildProductFormValues(cachedStockForm)
          ));
        }
      }
    } catch {
      // Ignore malformed cache values.
    }
  }, []);

  useEffect(() => {
    if (activeView !== 'products' && activeView !== 'cart') {
      return undefined;
    }

    const requestedFilterMode = activeView === 'cart' ? debouncedCartFilterMode : debouncedProductFilterMode;
    const delay = window.setTimeout(() => {
      loadStock(false, true, requestedFilterMode);
    }, 40);

    return () => {
      window.clearTimeout(delay);
    };
  }, [activeView, debouncedProductFilterMode, debouncedCartFilterMode]);

  useEffect(() => {
    if (stockPreloadStartedRef.current) {
      return;
    }
    if (stockView?.all_rows_cache?.length) {
      stockPreloadStartedRef.current = true;
      return;
    }

    stockPreloadStartedRef.current = true;
    loadStock(false, false);
  }, [stockView]);

  useEffect(() => {
    if (!isAdmin) {
      return;
    }
    if (!coreViews.has(activeView)) {
      return;
    }
    if (hasCoreLoaded) {
      return;
    }

    loadCoreWorkspace(false);
  }, [activeView, isAdmin, hasCoreLoaded, coreViews]);

  useEffect(() => {
    if (activeView !== 'debtors') {
      return;
    }

    handleRefreshTodayUnpaidList(false);
  }, [activeView]);

  useEffect(() => {
    if (activeView !== 'products') {
      return;
    }

    loadStockForm(false, false);
  }, [activeView]);

  useEffect(() => {
    if (stockFormPreloadStartedRef.current) {
      return;
    }
    if (stockForm && typeof stockForm === 'object') {
      stockFormPreloadStartedRef.current = true;
      return;
    }

    stockFormPreloadStartedRef.current = true;
    loadStockForm(false, false);
  }, [stockForm]);

  useEffect(() => {
    if (activeView !== 'clients' && activeView !== 'cart') {
      return;
    }
    if (googleContactsLoadAttempted) {
      return;
    }

    loadGoogleContacts({ forceRefresh: false, silent: true });
  }, [activeView, googleContactsLoadAttempted]);

  useEffect(() => {
    if (activeView !== 'users' || !isAdmin) {
      return;
    }

    loadUsers();
  }, [activeView, isAdmin]);

  useEffect(() => {
    if (activeView !== 'cashflow' || !isAdmin) {
      return;
    }

    loadCashflowDashboard(false);
  }, [activeView, isAdmin]);

  useEffect(() => {
    if (activeView !== 'services_today') {
      return;
    }

    if (servicesTodayData?.count || servicesTodayBusy) {
      return;
    }

    loadServicesTodayForDate(servicesTodayDate, false);
  }, [activeView]);

  useEffect(() => {
    if (activeView === 'products') {
      return;
    }

    setIsProductComposerOpen(false);
  }, [activeView]);

  useEffect(() => {
    if (activeView === 'products' || activeView === 'cart') {
      return;
    }

    setSelectedProductDetail(null);
  }, [activeView]);

  useEffect(() => {
    const update = () => setCurrentTimeLabel(new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }));
    update();
    const timerId = window.setInterval(update, 30_000);
    return () => window.clearInterval(timerId);
  }, []);

  useEffect(() => {
    if (!selectedDebtor) {
      return undefined;
    }

    const delay = window.setTimeout(() => {
      loadSelectedDebtorDetails(selectedDebtor, false);
    }, 140);

    return () => {
      window.clearTimeout(delay);
    };
  }, [selectedDebtor]);

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
          setPaymentPlanError(error.message || 'Could not prepare the payment preview.');
        }
      }
    }, 220);

    return () => {
      abortController.abort();
      window.clearTimeout(delay);
    };
  }, [selectedDebtor, paymentAmount, selectedServiceRow]);

  useEffect(() => {
    setClientPage(1);
  }, [deferredClientSearch]);

  useEffect(() => {
    setDebtorPage(1);
  }, [deferredDebtorSearch]);

  useEffect(() => {
    setStockPage(1);
  }, [deferredStockSearchText, productFilterMode]);

  useEffect(() => {
    setCartPage(1);
  }, [deferredStockSearchText, cartFilterMode]);

  useEffect(() => {
    setGoogleContactPage(1);
  }, [deferredGoogleSearch]);

  function handleSelectDebtor(name) {
    startTransition(() => setSelectedDebtor(name));
    setPaymentAmount('');
    setPaymentPlan(null);
    setPaymentPlanError('');
    setSelectedServiceRow('automatic');
  }

  async function handleCopyBill() {
    if (!selectedDebtor) {
      setStatusText('Select a debtor first.');
      return;
    }

    await handleCopyBillForDebtor(selectedDebtor);
  }

  async function handleCopyBillForDebtor(name) {
    const debtorName = String(name || '').trim();
    if (!debtorName) {
      setStatusText('Select a debtor first.');
      return;
    }

    try {
      let nextBillText = billText;
      if (debtorName !== selectedDebtor || !billText || billText.startsWith('Select a debtor')) {
        const result = await fetchLiveBill(debtorName, { forceRefresh: false });
        nextBillText = result.bill_text || 'No outstanding bill for this customer.';
        startTransition(() => setSelectedDebtor(debtorName));
        setBillText(nextBillText);
      }

      if (!nextBillText || nextBillText.includes('No outstanding bill')) {
        setStatusText(`No outstanding bill found for ${debtorName}.`);
        return;
      }

      await copyText(nextBillText);
      setStatusText(`Bill copied for ${debtorName}.`);
    } catch (error) {
      setStatusText(error.message || 'Could not copy the bill.');
    }
  }

  async function handleSendWhatsapp() {
    const phone = normalizeWhatsappPhone(clientsData.registry?.[selectedDebtor] || '');
    if (!selectedDebtor) {
      setStatusText('Select a debtor first.');
      return;
    }
    if (!phone) {
      startTransition(() => setActiveView('clients'));
      setClientForm((current) => ({ ...current, name: selectedDebtor, phone: '', gender: '' }));
      setClientOriginalName(selectedDebtor);
      setGoogleSearch(selectedDebtor);
      setSelectedGoogleContact(null);
      setStatusText(`No client phone is saved for ${selectedDebtor}. Add or sync it from Clients.`);
      return;
    }

    window.open(buildWhatsappUrl(phone, billText), '_blank', 'noopener,noreferrer');
    try {
      const result = await markWhatsappSent({ nameInput: selectedDebtor, source: 'single' });
      setWhatsappHistoryByName((current) => ({
        ...current,
        [selectedDebtor]: result.entry || current[selectedDebtor] || {},
      }));
      setStatusText(`Opened WhatsApp for ${selectedDebtor}. Total sends: ${formatCount(result.entry?.send_count || 0)}.`);
    } catch {
      setStatusText(`Opened WhatsApp for ${selectedDebtor}.`);
    }
  }

  async function handleRefreshTodayUnpaidList(forceRefresh = true) {
    setSendingTodayBills(true);
    try {
      const result = await fetchUnpaidTodayBills({ forceRefresh });
      setUnpaidTodaySummary(result || { count: 0, with_phone_count: 0, customers: [] });
      setStatusText(`Loaded ${formatCount(result?.count || 0)} unpaid customer(s) for today.`);
    } catch (error) {
      setStatusText(error.message || 'Could not load today unpaid list.');
    } finally {
      setSendingTodayBills(false);
    }
  }

  async function handleRefreshDebtorsSection() {
    if (!isAdmin) {
      setStatusText('Debtor section refresh is available for admin users only.');
      return;
    }

    setRefreshingDebtorsSection(true);
    try {
      const [bootstrapResult, unpaidResult, whatsappResult] = await Promise.all([
        fetchHomeBootstrap({ forceRefresh: true }),
        fetchUnpaidTodayBills({ forceRefresh: true }),
        fetchWhatsappHistory({ forceRefresh: true }),
      ]);

      if (bootstrapResult?.debtors) {
        const nextDebtors = bootstrapResult.debtors;
        setDebtorsData(nextDebtors);
        const availableNames = (nextDebtors.sorted_debtors || []).map(([name]) => name);
        const nextSelected = availableNames.includes(selectedDebtor) ? selectedDebtor : availableNames[0] || '';
        startTransition(() => setSelectedDebtor(nextSelected));
        if (nextSelected) {
          await loadSelectedDebtorDetails(nextSelected, true);
        }
      }

      setUnpaidTodaySummary(unpaidResult || { count: 0, with_phone_count: 0, customers: [] });
      setWhatsappHistoryByName(whatsappResult?.by_name || {});
      setStatusText('Debtors section refreshed.');
    } catch (error) {
      setStatusText(error.message || 'Could not refresh the debtors section.');
    } finally {
      setRefreshingDebtorsSection(false);
    }
  }

  async function handleSendTodayUnpaidCustomer(entry) {
    const name = String(entry?.name || '').trim().toUpperCase();
    if (!name) {
      setStatusText('Invalid customer record selected.');
      return;
    }

    const phone = normalizeWhatsappPhone(entry?.phone || clientsData.registry?.[name] || '');
    if (!phone) {
      setStatusText(`No client phone is saved for ${name}.`);
      return;
    }

    let nextBillText = String(entry?.bill_text || '');
    if (!nextBillText || nextBillText.includes('No outstanding bill for this customer.')) {
      try {
        const billResult = await fetchLiveBill(name, { forceRefresh: true });
        nextBillText = billResult.bill_text || '';
      } catch {
        nextBillText = '';
      }
    }

    if (!nextBillText || nextBillText.includes('No outstanding bill for this customer.')) {
      setStatusText(`No outstanding bill found for ${name}.`);
      return;
    }

    window.open(buildWhatsappUrl(phone, nextBillText), '_blank', 'noopener,noreferrer');

    try {
      const result = await markWhatsappSent({ nameInput: name, source: 'today_list' });
      setWhatsappHistoryByName((current) => ({
        ...current,
        [name]: result.entry || current[name] || {},
      }));
      setUnpaidTodaySummary((current) => ({
        ...(current || {}),
        customers: (current?.customers || []).map((item) => (
          String(item.name || '').trim().toUpperCase() === name
            ? { ...item, send_stats: result.entry || item.send_stats || {} }
            : item
        )),
      }));
      setStatusText(`Opened WhatsApp bill for ${name}. Total sends: ${formatCount(result.entry?.send_count || 0)}.`);
    } catch {
      setStatusText(`Opened WhatsApp bill for ${name}.`);
    }
  }

  async function handleSendBillNotificationCustomer(entry) {
    const name = String(entry?.name || '').trim().toUpperCase();
    if (!name) {
      return;
    }

    const phone = normalizeWhatsappPhone(entry?.phone || clientsData.registry?.[name] || '');
    if (!phone) {
      startTransition(() => setActiveView('clients'));
      setClientForm((current) => ({ ...current, name, phone: '', gender: '' }));
      setClientOriginalName(name);
      setGoogleSearch(name);
      setSelectedGoogleContact(null);
      setStatusText(`No client phone is saved for ${name}. Add or sync it from Clients.`);
      return;
    }

    setSendingBillNotificationKey(name);
    try {
      const billResult = await fetchLiveBill(name, { forceRefresh: true });
      const nextBillText = billResult.bill_text || '';
      if (!nextBillText || nextBillText.includes('No outstanding bill for this customer.')) {
        setStatusText(`No outstanding bill found for ${name}.`);
        return;
      }

      window.open(buildWhatsappUrl(phone, nextBillText), '_blank', 'noopener,noreferrer');

      try {
        const result = await markWhatsappSent({ nameInput: name, source: 'bill_notifications' });
        setWhatsappHistoryByName((current) => ({
          ...current,
          [name]: result.entry || current[name] || {},
        }));
        setStatusText(`Opened WhatsApp bill for ${name}. Total sends: ${formatCount(result.entry?.send_count || 0)}.`);
      } catch {
        setStatusText(`Opened WhatsApp bill for ${name}.`);
      }
    } catch (error) {
      setStatusText(error.message || `Could not send bill to ${name}.`);
    } finally {
      setSendingBillNotificationKey('');
    }
  }

  function getSelectedDebtorOutstandingAmount() {
    const entries = debtorsData?.sorted_debtors || [];
    for (const entry of entries) {
      const name = String(entry?.[0] || '').trim().toUpperCase();
      if (name === String(selectedDebtor || '').trim().toUpperCase()) {
        return Number(entry?.[1] || 0);
      }
    }
    return 0;
  }

  function getTargetOutstandingAmount() {
    if (selectedServiceRow !== 'automatic') {
      const selectedRowValue = Number(selectedServiceRow);
      const matched = (outstandingItems || []).find((item) => Number(item?.row_idx) === selectedRowValue);
      return Number(matched?.balance || 0);
    }
    return getSelectedDebtorOutstandingAmount();
  }

  async function applyDebtorPaymentByAmount(parsedAmount) {
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
      });
      setUndoEnabled(Boolean(result.undo_available));
      setRedoEnabled(Boolean(result.redo_available));
      setPaymentAmount('');
      setPaymentPlan(null);
      await loadCoreWorkspace(true);
      await loadSelectedDebtorDetails(selectedDebtor, true);
      setStatusText(result.status_text || 'Payment applied.');
    } catch (error) {
      setStatusText(error.message || 'Could not apply the payment.');
    } finally {
      setIsApplyingPayment(false);
    }
  }

  async function handleApplyPayment() {
    const parsedAmount = Number(normalizeDigits(paymentAmount));
    await applyDebtorPaymentByAmount(parsedAmount);
  }

  async function handleApplyFullPayment() {
    const targetAmount = Number(getTargetOutstandingAmount());
    if (!Number.isFinite(targetAmount) || targetAmount <= 0) {
      setStatusText('No outstanding amount was found for this payment target.');
      return;
    }

    const roundedAmount = Math.round(targetAmount);
    setPaymentAmount(String(roundedAmount));
    await applyDebtorPaymentByAmount(roundedAmount);
  }

  async function handleUpdateDebtorServiceRow({ rowIdx, price }) {
    if (!selectedDebtor || !rowIdx) {
      return;
    }

    setServiceActionBusy(true);
    try {
      await updateDebtorService({
        nameInput: selectedDebtor,
        rowIdx,
        price,
        forceRefresh: true,
      });
      setPaymentPlan(null);
      setPaymentPlanError('');
      await loadSelectedDebtorDetails(selectedDebtor, true);
      await loadCoreWorkspace(true);
      setStatusText(`Updated ${selectedDebtor}'s service price.`);
    } catch (error) {
      setStatusText(error.message || 'Could not update the service price.');
    } finally {
      setServiceActionBusy(false);
    }
  }

  async function handleReturnDebtorServiceRow({ rowIdx }) {
    if (!selectedDebtor || !rowIdx) {
      return;
    }

    setServiceActionBusy(true);
    try {
      await returnDebtorService({
        nameInput: selectedDebtor,
        rowIdx,
        forceRefresh: true,
      });
      setPaymentPlan(null);
      setPaymentPlanError('');
      await loadSelectedDebtorDetails(selectedDebtor, true);
      await loadCoreWorkspace(true);
      setStatusText(`Returned the selected service for ${selectedDebtor}.`);
    } catch (error) {
      setStatusText(error.message || 'Could not return the selected service.');
    } finally {
      setServiceActionBusy(false);
    }
  }

  async function handleUndo() {
    try {
      const result = await undoPayment();
      setUndoEnabled(Boolean(result.undo_available));
      setRedoEnabled(Boolean(result.redo_available));
      await loadCoreWorkspace(true);
      await loadSelectedDebtorDetails(selectedDebtor, true);
      setStatusText(result.status_text || 'Last payment action undone.');
    } catch (error) {
      setStatusText(error.message || 'Could not undo the last payment.');
    }
  }

  async function handleRedo() {
    try {
      const result = await redoPayment();
      setUndoEnabled(Boolean(result.undo_available));
      setRedoEnabled(Boolean(result.redo_available));
      await loadCoreWorkspace(true);
      await loadSelectedDebtorDetails(selectedDebtor, true);
      setStatusText(result.status_text || 'Last undone payment reapplied.');
    } catch (error) {
      setStatusText(error.message || 'Could not redo the last payment.');
    }
  }

  async function handleSaveClient() {
    setClientsBusy(true);
    try {
      const result = await upsertClient({
        previousName: clientOriginalName || null,
        name: clientForm.name,
        phone: clientForm.phone,
        gender: clientForm.gender || null,
        syncSheet: true,
        forceRefresh: false,
      });
      const nextData = applyClientRegistryToState(result);
      const nextName = result.key || clientForm.name.trim().toUpperCase();
      const selectedEntry = (nextData.entries || []).find((entry) => String(entry.name || '').trim().toUpperCase() === String(nextName || '').trim().toUpperCase());
      setClientForm({
        name: nextName,
        phone: selectedEntry?.phone || result.registry?.[nextName] || '',
        gender: selectedEntry?.gender || result.gender || '',
      });
      setClientOriginalName(nextName);
      setStatusText(
        `${result.added ? 'Client added' : 'Client updated'} and normalized.${result.propagation_result ? ` Updated ${formatCount((result.propagation_result.main_updates || 0) + (result.propagation_result.stock_updates || 0))} transaction cell(s) across sheets.` : ''}${result.sync_result?.mode === 'queued' ? ' Sheet sync queued in background.' : ''}`
      );
    } catch (error) {
      setStatusText(error.message || 'Could not save the client.');
    } finally {
      setClientsBusy(false);
    }
  }

  async function handleDeleteClient() {
    if (!clientForm.name || !window.confirm(`Delete ${clientForm.name} from the client list?`)) {
      return;
    }

    setClientsBusy(true);
    try {
      const result = await deleteClient({ name: clientForm.name, syncSheet: true });
      applyClientRegistryToState(result);
      setClientForm({ name: '', phone: '', gender: '' });
      setClientOriginalName('');
      setStatusText(`Client deleted.${result.sync_result?.mode === 'queued' ? ' Sheet sync queued in background.' : ''}`);
    } catch (error) {
      setStatusText(error.message || 'Could not delete the client.');
    } finally {
      setClientsBusy(false);
    }
  }

  async function handleImportSheetPhones() {
    setClientsBusy(true);
    try {
      const result = await importSheetPhones({ forceRefresh: true });
      applyClientRegistryToState(result);
      setStatusText(`Imported sheet phone numbers: ${result.added} added, ${result.updated} updated.`);
    } catch (error) {
      setStatusText(error.message || 'Could not import phone numbers from the sheet.');
    } finally {
      setClientsBusy(false);
    }
  }

  async function handleSyncGoogleContacts() {
    try {
      const result = await loadGoogleContacts({ forceRefresh: true });
      if (!result) {
        return;
      }
      setStatusText(`Synced ${result.total_cached || result.count || 0} Google contact number(s).`);
    } catch {
      // Errors are handled in loadGoogleContacts.
    }
  }

  async function handleApplyGoogleContact() {
    if (!clientForm.name) {
      setStatusText('Select or enter a client before applying a Google contact.');
      return;
    }
    if (!selectedGoogleContact) {
      setStatusText('Select a Google contact first.');
      return;
    }

    setClientsBusy(true);
    try {
      const result = await upsertClient({
        previousName: clientOriginalName || null,
        name: clientForm.name,
        phone: selectedGoogleContact.phone,
        gender: clientForm.gender || null,
        syncSheet: true,
        forceRefresh: false,
      });
      applyClientRegistryToState(result);
      setClientForm((current) => ({ ...current, phone: selectedGoogleContact.phone }));
      setClientOriginalName(result.key || clientForm.name.trim().toUpperCase());
      setStatusText(`Updated ${clientForm.name} with ${selectedGoogleContact.phone}.${result.sync_result?.mode === 'queued' ? ' Sheet sync queued in background.' : ''}`);
    } catch (error) {
      setStatusText(error.message || 'Could not update the client from Google Contacts.');
    } finally {
      setClientsBusy(false);
    }
  }

  async function handleApplyFix() {
    if (!selectedMismatch || !correctName) {
      setStatusText('Select a mismatch and choose the replacement name.');
      return;
    }

    setIsNameFixApplying(true);
    try {
      const result = await applyNameFix({ mismatchEntry: selectedMismatch, correctName });
      await loadCoreWorkspace(false);
      await loadNameFixes({ forceRefresh: false, silent: true });
      setStatusText(`Queued ${result.updated_count} row(s) for name correction.`);
    } catch (error) {
      setStatusText(error.message || 'Could not apply the selected fix.');
    } finally {
      setIsNameFixApplying(false);
    }
  }

  async function handleApplyAllFixes() {
    if (!nameFixData.mismatches?.length) {
      setStatusText('No name fixes are currently loaded.');
      return;
    }

    setIsNameFixApplying(true);
    try {
      const result = await applyAllNameFixes({ mismatchEntries: nameFixData.mismatches });
      await loadCoreWorkspace(false);
      await loadNameFixes({ forceRefresh: false, silent: true });
      setStatusText(`Queued ${result.updated_count} row(s) for automatic fixes.`);
    } catch (error) {
      setStatusText(error.message || 'Could not apply all fixes.');
    } finally {
      setIsNameFixApplying(false);
    }
  }

  async function handleRescanFixes() {
    try {
      const result = await loadNameFixes({ forceRefresh: true });
      if (!result) {
        return;
      }
      setStatusText('Name-fix scan completed.');
    } catch {
      // Errors are handled in loadNameFixes.
    }
  }

  async function handlePullNow() {
    setSyncBusy(true);
    try {
      await pullNow();
      await loadCoreWorkspace(true);
      await loadStock(true);
      const nameFixResult = activeView === 'home' || activeView === 'fix'
        ? await loadNameFixes({ forceRefresh: true, silent: true })
        : true;
      setStatusText(
        nameFixResult === null
          ? 'Manual pull completed, but the Fix scan is unavailable right now.'
          : 'Manual sheet pull completed.'
      );
    } catch (error) {
      setStatusText(error.message || 'Could not run the manual pull.');
    } finally {
      setSyncBusy(false);
    }
  }

  async function loadUsers() {
    if (!isAdmin) {
      return;
    }

    setUsersLoading(true);
    try {
      const result = await fetchUsers();
      setUsersData(result?.users || []);
    } catch (error) {
      setStatusText(error.message || 'Could not load users.');
    } finally {
      setUsersLoading(false);
    }
  }

  async function handleCreateUser(event) {
    event.preventDefault();
    if (!isAdmin) {
      setStatusText('Only admin users can create accounts.');
      return;
    }

    setUsersBusy(true);
    try {
      await createUser({
        username: userForm.username,
        password: userForm.password,
        role: userForm.role,
        isActive: userForm.is_active,
      });
      setUserForm({ username: '', password: '', role: 'staff', is_active: true });
      await loadUsers();
      setStatusText('User account created successfully.');
    } catch (error) {
      setStatusText(error.message || 'Could not create user.');
    } finally {
      setUsersBusy(false);
    }
  }

  async function handleUpdateUserRole(userId, role) {
    if (!isAdmin) {
      setStatusText('Only admin users can update account roles.');
      return;
    }

    setUsersBusy(true);
    try {
      await updateUser({ userId, role });
      await loadUsers();
      setStatusText('User role updated.');
    } catch (error) {
      setStatusText(error.message || 'Could not update user role.');
    } finally {
      setUsersBusy(false);
    }
  }

  async function handleToggleUserStatus(userId, isActive) {
    if (!isAdmin) {
      setStatusText('Only admin users can update account status.');
      return;
    }

    setUsersBusy(true);
    try {
      await updateUser({ userId, isActive });
      await loadUsers();
      setStatusText('User status updated.');
    } catch (error) {
      setStatusText(error.message || 'Could not update user status.');
    } finally {
      setUsersBusy(false);
    }
  }

  async function handleSubmitProduct(event, submittedValues = null, options = {}) {
    event.preventDefault();
    setIsAddingProduct(true);
    try {
      const now = new Date();
      const sourceValues = submittedValues || productFormValues || {};
      const nextValues = {
        ...sourceValues,
        TIME: sourceValues.TIME || now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }),
        DATE: sourceValues.DATE || now.toLocaleDateString('en-US'),
      };
      if (!nextValues['PRODUCT STATUS']) {
        nextValues['PRODUCT STATUS'] = 'AVAILABLE';
      }
      if (!nextValues['AVAILABILITY/DATE SOLD']) {
        nextValues['AVAILABILITY/DATE SOLD'] = 'AVAILABLE';
      }
      await addStockRecord({
        valuesByHeader: nextValues,
        forceRefresh: false,
        allowStolenWarningOverride: Boolean(options?.allowStolenWarningOverride),
      });
      await loadStock(true);
      await loadStockForm(true, true);
      setIsProductComposerOpen(false);
      setStatusText('Product queued into the stock sheet successfully.');
    } catch (error) {
      setStatusText(error.message || 'Could not add the product.');
    } finally {
      setIsAddingProduct(false);
    }
  }

  function handleResetProductForm() {
    setProductFormValues(buildProductFormValues(stockForm));
    setStatusText('Product form reset to defaults.');
  }

  function handleAddToCart(row) {
    setSaleCartItems((current) => {
      if (current.some((item) => item.stock_row_num === row.row_num)) {
        return current;
      }
      return [...current, buildCartItemFromProductRow(row, stockView?.headers || [])];
    });
    setStatusText(`Added stock row #${row.row_num} to the cart.`);
  }

  function handleUpdateCartItem(stockRowNum, field, value) {
    setSaleCartItems((current) => current.map((item) => {
      if (item.stock_row_num !== stockRowNum) {
        return item;
      }

      if (field === 'buyer_name') {
        const normalized = String(value || '').trim().toUpperCase();
        const knownPhone = clientsData.registry?.[normalized] || '';
        return {
          ...item,
          buyer_name: value,
          buyer_phone: item.buyer_phone || knownPhone,
        };
      }

      const nextItem = { ...item, [field]: value };
      if (field === 'pickup_mode' && String(value || '').toUpperCase() !== 'REPRESENTATIVE') {
        nextItem.representative_name = '';
        nextItem.representative_phone = '';
      }
      if (field === 'fulfillment_method' && String(value || '').toUpperCase() !== 'OFF OFFICE') {
        nextItem.deal_location = '';
      }
      if (field === 'representative_phone') {
        nextItem.representative_phone = extractPhoneFromSuggestionText(value);
      }
      if (field === 'is_swap' && !value) {
        nextItem.swap_incoming_devices = [];
        nextItem.swap_devices = '';
        nextItem.swap_cash_amount = '';
      }
      if (field === 'sale_price' || field === 'amount_paid') {
        const saleValue = parseAmountLike(nextItem.sale_price);
        const paidValue = parseAmountLike(nextItem.amount_paid);
        if (paidValue <= 0) {
          nextItem.payment_status = 'UNPAID';
        } else if (saleValue > 0 && paidValue < saleValue) {
          nextItem.payment_status = 'PART PAYMENT';
        } else if (saleValue > 0 && paidValue >= saleValue) {
          nextItem.payment_status = 'PAID';
        }
      }

      return nextItem;
    }));
  }

  function handleRemoveCartItem(stockRowNum) {
    setSaleCartItems((current) => current.filter((item) => item.stock_row_num !== stockRowNum));
    setStatusText(`Removed stock row #${stockRowNum} from the cart.`);
  }

  async function handleReturnPendingDeal(entry) {
    const pendingKey = `${entry?.kind || 'stock'}-${entry?.row_num || entry}`;
    setReturningPendingKey(pendingKey);
    try {
      if (entry?.kind === 'service') {
        await returnServiceDeal({ rowNum: entry.row_num, forceRefresh: false });
      } else {
        await returnStockItem({ rowNum: entry.row_num || entry, forceRefresh: false });
      }
      await Promise.all([loadStock(true), loadCoreWorkspace(false)]);
      setStatusText(
        entry?.kind === 'service'
          ? `Service deal row #${entry.row_num} returned/refunded successfully.`
          : `Pending deal row #${entry?.row_num || entry} returned and marked available.`
      );
    } catch (error) {
      setStatusText(error.message || 'Could not return this pending deal.');
    } finally {
      setReturningPendingKey('');
    }
  }

  async function handleUpdatePendingDealPayment(entry, paymentStatus, amountPaid) {
    const pendingKey = `${entry?.kind || 'stock'}-${entry?.row_num || entry}`;
    setUpdatingPendingKey(pendingKey);
    try {
      let normalizedStatus = String(paymentStatus || '').trim().toUpperCase();
      const normalizedAmount = String(amountPaid || '').trim();
      const entryPrice = parseAmountLike(entry?.price);
      const amountValue = parseAmountLike(normalizedAmount);

      if (normalizedAmount !== '') {
        if (amountValue <= 0) {
          normalizedStatus = 'UNPAID';
        } else if (entryPrice > 0) {
          if (amountValue > entryPrice) {
            const remainder = amountValue - entryPrice;
            const customerName = String(entry?.name || entry?.buyer_name || '').trim().toUpperCase();
            if (!customerName) {
              throw new Error(`Amount exceeds sale price by NGN ${formatCount(remainder)}. Customer name is missing, so remainder cannot be redirected.`);
            }

            const shouldRedirect = window.confirm(
              `Amount exceeds sale price by NGN ${formatCount(remainder)}.\n\n` +
              `Tap OK to mark this deal as PAID with NGN ${formatCount(entryPrice)} and redirect the NGN ${formatCount(remainder)} remainder to ${customerName}'s other unpaid service(s).\n` +
              `Tap Cancel to edit the amount.`
            );

            if (!shouldRedirect) {
              setStatusText('Update cancelled. Enter an amount that is not greater than sale price.');
              return;
            }

            let settleResult = null;
            if (entry?.kind === 'service') {
              settleResult = await updateServiceDealPayment({
                rowNum: entry.row_num,
                paymentStatus: 'PAID',
                amountPaid: String(entryPrice),
                forceRefresh: false,
              });
            } else {
              settleResult = await updatePendingDealPayment({
                rowNum: entry.row_num || entry,
                paymentStatus: 'PAID',
                amountPaid: String(entryPrice),
                forceRefresh: false,
              });
            }

            let remainderMessage = `Remainder NGN ${formatCount(remainder)} redirected.`;
            try {
              await applyPayment({
                nameInput: customerName,
                paymentAmount: remainder,
                manualServiceRowIdx: null,
                forceRefresh: false,
              });
            } catch (remainderError) {
              remainderMessage = remainderError?.message || 'Main deal was updated, but remainder redirect failed.';
            }

            await Promise.all([loadStock(true), loadCoreWorkspace(false)]);
            const savedStatus = String(settleResult?.payment_status || 'PAID').toUpperCase();
            setStatusText(
              `${entry?.kind === 'service' ? `Service deal row #${entry.row_num}` : `Pending deal row #${entry?.row_num || entry}`} updated to ${savedStatus}. ${remainderMessage}`
            );
            return;
          }

          if (amountValue === entryPrice) {
            normalizedStatus = 'PAID';
          } else {
            normalizedStatus = 'PART PAYMENT';
          }
        }
      }

      let result = null;
      const amountToSend = normalizedAmount === '' ? null : String(amountValue);
      if (entry?.kind === 'service') {
        result = await updateServiceDealPayment({
          rowNum: entry.row_num,
          paymentStatus: normalizedStatus,
          amountPaid: amountToSend,
          forceRefresh: false,
        });
      } else {
        result = await updatePendingDealPayment({
          rowNum: entry.row_num || entry,
          paymentStatus: normalizedStatus,
          amountPaid: amountToSend,
          forceRefresh: false,
        });
      }
      await Promise.all([loadStock(true), loadCoreWorkspace(false)]);
      const savedStatus = String(result?.payment_status || normalizedStatus).toUpperCase();
      setStatusText(
        entry?.kind === 'service'
          ? `Service deal row #${entry.row_num} updated to ${savedStatus}.`
          : `Pending deal row #${entry?.row_num || entry} updated to ${savedStatus}.`
      );
    } catch (error) {
      setStatusText(error.message || 'Could not update this pending deal payment status.');
    } finally {
      setUpdatingPendingKey('');
    }
  }

  async function handleUpdatePendingDealMeta(entry, draft) {
    const pendingKey = `${entry?.kind || 'stock'}-${entry?.row_num || entry}`;
    const pickupMode = String(draft?.pickup_mode || 'BUYER').trim().toUpperCase();
    const representativeName = String(draft?.representative_name || '').trim();
    const representativePhone = extractPhoneFromSuggestionText(draft?.representative_phone || '');
    const isSwap = Boolean(draft?.is_swap);
    const swapType = String(draft?.swap_type || '').trim().toUpperCase();
    const swapDevices = String(draft?.swap_devices || '').trim();

    if (pickupMode === 'REPRESENTATIVE' && (!representativeName || !representativePhone)) {
      setStatusText('Representative name and phone are required when pickup mode is REPRESENTATIVE.');
      return;
    }
    if (isSwap && (!swapType || !swapDevices)) {
      setStatusText('Swap type and swap details are required for swap deals.');
      return;
    }

    setUpdatingPendingMetaKey(pendingKey);
    try {
      const valuesByHeader = {
        'PAYMENT METHOD': String(draft?.payment_method || 'CASH').trim().toUpperCase(),
        'FULFILLMENT METHOD': String(draft?.fulfillment_method || 'WALK-IN PICKUP').trim().toUpperCase(),
        'PICKUP MODE': pickupMode,
        'REPRESENTATIVE NAME': representativeName ? representativeName.toUpperCase() : '',
        'REPRESENTATIVE PHONE': representativePhone,
        'SWAP TYPE': isSwap ? swapType : '',
        'SWAP DETAIL': isSwap ? swapDevices : '',
        'SWAP CASH AMOUNT': isSwap ? String(normalizeDigits(draft?.swap_cash_amount || '')) : '',
      };

      if (entry?.kind === 'service') {
        await updatePendingServiceMeta({
          rowNum: entry.row_num,
          valuesByHeader,
          forceRefresh: false,
        });
      } else {
        await updatePendingDealMeta({
          rowNum: entry.row_num || entry,
          valuesByHeader,
          forceRefresh: false,
        });
      }

      await Promise.all([loadStock(true), loadCoreWorkspace(false)]);
      setStatusText(
        entry?.kind === 'service'
          ? `Service pending row #${entry.row_num} details updated.`
          : `Stock pending row #${entry?.row_num || entry} details updated.`
      );
    } catch (error) {
      setStatusText(error.message || 'Could not update pending-deal details.');
    } finally {
      setUpdatingPendingMetaKey('');
    }
  }

  async function handleOpenProductComposer() {
    setIsProductComposerOpen(true);
    if (!googleContactsLoadAttempted) {
      await loadGoogleContacts({ forceRefresh: false, silent: true });
    }
  }

  async function handleReturnCartItem(stockRowNum) {
    if (!window.confirm(`Return stock row #${stockRowNum} and clear buyer/sold status?`)) {
      return;
    }

    setCartBusy(true);
    try {
      await returnStockItem({ rowNum: stockRowNum, forceRefresh: false });
      setSaleCartItems((current) => current.filter((item) => item.stock_row_num !== stockRowNum));
      await Promise.all([loadStock(true), loadCoreWorkspace(false)]);
      setStatusText(`Stock row #${stockRowNum} returned and marked available.`);
    } catch (error) {
      setStatusText(error.message || 'Could not return the selected stock item.');
    } finally {
      setCartBusy(false);
    }
  }

  async function handleCheckoutCart() {
    if (!saleCartItems.length) {
      setStatusText('Add at least one phone to the cart first.');
      return;
    }

    const invalidRepresentative = saleCartItems.find((item) => (
      String(item.pickup_mode || '').toUpperCase() === 'REPRESENTATIVE'
      && (!String(item.representative_name || '').trim() || !String(item.representative_phone || '').trim())
    ));
    if (invalidRepresentative) {
      setStatusText(`Add representative name and phone for cart row #${invalidRepresentative.stock_row_num}.`);
      return;
    }

    const invalidSwap = saleCartItems.find((item) => (
      item.is_swap
      && (
        !String(item.swap_type || '').trim()
        || !Array.isArray(item.swap_incoming_devices)
        || !item.swap_incoming_devices.length
        || item.swap_incoming_devices.some((device) => {
          const valuesByHeader = device?.values_by_header || {};
          const description = getValueByHeaderAliases(valuesByHeader, ['DESCRIPTION', 'MODEL', 'DEVICE']);
          const imei = getValueByHeaderAliases(valuesByHeader, ['IMEI']);
          return !description || !imei;
        })
      )
    ));
    if (invalidSwap) {
      setStatusText(`Add swap type and full incoming device details (including IMEI) for cart row #${invalidSwap.stock_row_num}.`);
      return;
    }

    setCartBusy(true);
    try {
      const result = await checkoutSaleCart({
        items: saleCartItems.map((item) => ({
          availability_value: (
            item.availability_choice === 'TODAY'
              ? new Date().toLocaleDateString('en-US')
              : item.availability_choice === 'PENDING'
                ? 'PENDING DEAL'
                : item.availability_choice === 'CLEAR'
                  ? '__CLEAR__'
                  : item.availability_choice === 'CUSTOM'
                    ? (item.availability_custom || '').trim()
                    : ''
          ),
          stock_row_num: item.stock_row_num,
          buyer_name: item.buyer_name,
          buyer_phone: item.buyer_phone,
          sale_price: item.sale_price,
          amount_paid: item.amount_paid,
          phone_expense: item.phone_expense,
          payment_method: item.payment_method,
          fulfillment_method: item.fulfillment_method,
          pickup_mode: item.pickup_mode,
          representative_name: item.representative_name,
          representative_phone: item.representative_phone,
          deal_location: item.deal_location,
          internal_note: item.internal_note,
          is_swap: Boolean(item.is_swap),
          swap_type: item.swap_type,
          swap_devices: item.swap_incoming_devices,
          swap_cash_amount: item.swap_cash_amount,
          stock_status: item.payment_status === 'PAID' ? 'Sold' : 'Pending Deal',
          inventory_status: item.payment_status,
        })),
        forceRefresh: false,
      });
      setSaleCartItems([]);
      setSelectedProductDetail(null);
      await Promise.all([loadStock(false), loadCoreWorkspace(false)]);
      setStatusText(`${result.processed_count || 0} phone sale(s) queued into stock and inventory.`);
    } catch (error) {
      setStatusText(error.message || 'Could not sell the current cart.');
    } finally {
      setCartBusy(false);
    }
  }

  async function handleSubmitService() {
    const name = String(serviceDraft.name || '').trim().toUpperCase();
    const phone = normalizeWhatsappPhone(serviceDraft.phone || '');
    const description = String(serviceDraft.description || '').trim();
    const internalNote = String(serviceDraft.internal_note || '').trim();
    const dealLocation = String(serviceDraft.deal_location || '').trim();
    const price = normalizeDigits(serviceDraft.price || '');
    const serviceExpense = normalizeDigits(serviceDraft.service_expense || '');
    let amountPaid = normalizeDigits(serviceDraft.amount_paid || '');
    let status = String(serviceDraft.status || 'UNPAID').trim().toUpperCase();
    const paymentMethod = String(serviceDraft.payment_method || 'CASH').trim().toUpperCase();
    const fulfillmentMethod = String(serviceDraft.fulfillment_method || 'WALK-IN PICKUP').trim().toUpperCase();
    const pickupMode = String(serviceDraft.pickup_mode || 'BUYER').trim().toUpperCase();
    const representativeName = String(serviceDraft.representative_name || '').trim().toUpperCase();
    const representativePhone = normalizeWhatsappPhone(serviceDraft.representative_phone || '');
    const priceAmount = Number.parseInt(price || '0', 10) || 0;

    if (!name) {
      setStatusText('Enter a customer name for the service.');
      return;
    }
    if (!description) {
      setStatusText('Enter a description for the service.');
      return;
    }
    if (!price) {
      setStatusText('Enter a valid service price.');
      return;
    }
    if (fulfillmentMethod === 'OFF OFFICE' && !dealLocation) {
      setStatusText('Enter the deal location for off-office services.');
      return;
    }
    if (!amountPaid) {
      amountPaid = '0';
    }
    const amountPaidValue = Number.parseInt(amountPaid || '0', 10) || 0;
    if (amountPaidValue > priceAmount) {
      setStatusText('Amount paid cannot be greater than amount charged.');
      return;
    }
    if (amountPaidValue <= 0) {
      status = 'UNPAID';
    } else if (amountPaidValue < priceAmount) {
      status = 'PART PAYMENT';
    } else {
      status = 'PAID';
    }
    if (pickupMode === 'REPRESENTATIVE' && (!representativeName || !representativePhone)) {
      setStatusText('Enter representative name and phone for this service pickup.');
      return;
    }

    setServiceBusy(true);
    try {
      await addServiceRecord({
        valuesByHeader: {
          NAME: name,
          'PHONE NUMBER': phone,
          DESCRIPTION: description,
          'INTERNAL NOTE': internalNote,
          'DEAL LOCATION': dealLocation,
          'FULFILLMENT METHOD': fulfillmentMethod,
          PRICE: price,
          'SERVICE EXPENSE': serviceExpense || '0',
          'AMOUNT PAID': amountPaid,
          STATUS: status,
          'PAYMENT METHOD': paymentMethod,
          'PICKUP MODE': pickupMode,
          'REPRESENTATIVE NAME': representativeName,
          'REPRESENTATIVE PHONE': representativePhone,
        },
        forceRefresh: true,
      });
      setServiceDraft({
        name: '',
        phone: '',
        description: '',
        internal_note: '',
        deal_location: '',
        price: '',
        service_expense: '',
        amount_paid: '',
        payment_method: 'CASH',
        fulfillment_method: 'WALK-IN PICKUP',
        pickup_mode: 'BUYER',
        representative_name: '',
        representative_phone: '',
        status: 'UNPAID',
      });
      await loadCoreWorkspace(false);
      // Pull latest cashflow sheet values so new service income/expense reflects immediately.
      if (isAdmin) {
        await loadCashflowDashboard(true);
      }
      setStatusText('Service row queued into inventory successfully.');
    } catch (error) {
      setStatusText(error.message || 'Could not add the service row.');
    } finally {
      setServiceBusy(false);
    }
  }

  async function handleSaveProductDetails(rowNum, valuesByHeader) {
    setIsSavingProductDetail(true);
    try {
      const result = await updateStockRow({
        rowNum,
        valuesByHeader,
        forceRefresh: false,
      });
      loadStock(false);
      setSelectedProductDetail(null);
      setStatusText(
        result.updated_count
          ? `Saved ${result.updated_count} product field(s) for row #${rowNum}.`
          : `No product field changes were detected for row #${rowNum}.`
      );
    } catch (error) {
      setStatusText(error.message || 'Could not save product details.');
    } finally {
      setIsSavingProductDetail(false);
    }
  }

  async function handleAction(item) {
    if (item.type === 'view') {
      if (!allowedViews.has(item.key)) {
        setStatusText('You do not have permission to access this section.');
        return;
      }
      if (item.key === 'products') {
        setProductFilterMode('available');
      }
      if (item.key === 'cart') {
        setCartFilterMode('available');
      }
      startTransition(() => setActiveView(item.key));
      setStatusText(VIEW_META[item.key]?.title || 'Ready');
      return;
    }

    if (item.key === 'refresh') {
      await handleFullRefresh();
      return;
    }

    if (item.key === 'undo') {
      await handleUndo();
      return;
    }

    if (item.key === 'redo') {
      await handleRedo();
      return;
    }

    if (item.key === 'import_phones') {
      await handleImportSheetPhones();
      return;
    }

    if (item.key === 'bill_notifications') {
      startTransition(() => setActiveView('bill_notifications'));
      setStatusText(
        billNotificationCount > 0
          ? `${formatCount(billNotificationCount)} customer(s) need bill follow-up after 4+ days.`
          : 'No overdue bill notifications right now.'
      );
      return;
    }

    if (item.key === 'exit') {
      window.close();
      setStatusText('Close request sent to the browser window.');
    }

    if (item.key === 'logout') {
      onLogout();
    }
  }

  function renderActiveView() {
    if (!allowedViews.has(activeView)) {
      return (
        <ProductsView
          stockView={stockView}
          stockForm={stockForm}
          productFormValues={productFormValues}
          setProductFormValues={setProductFormValues}
          productSearchText={stockSearchText}
          setProductSearchText={setStockSearchText}
          filterMode={productFilterMode}
          setFilterMode={setProductFilterMode}
          stockPage={stockPage}
          setStockPage={setStockPage}
          isLoading={isStockLoading}
          isRefreshing={isStockRefreshing}
          isAddingProduct={isAddingProduct}
          isProductComposerOpen={isProductComposerOpen}
          errorText={stockErrorText}
          onRefresh={() => loadStock(true)}
          selectedProductDetail={selectedProductDetail}
          onOpenProductDetails={setSelectedProductDetail}
          onCloseProductDetails={() => setSelectedProductDetail(null)}
          onSaveProductDetails={handleSaveProductDetails}
          savingProductDetails={isSavingProductDetail}
          onOpenProductComposer={handleOpenProductComposer}
          onCloseProductComposer={() => setIsProductComposerOpen(false)}
          onSubmitProduct={handleSubmitProduct}
          onResetProductForm={handleResetProductForm}
          sellerPhoneOptions={sellerPhoneOptions}
          sellerNameOptions={sellerNameOptions}
          sellerPhoneByName={sellerPhoneByName}
          onCheckStolenImei={handleCheckStolenImei}
          currentTimeLabel={currentTimeLabel}
          summaryColumns={productSummaryColumns}
        />
      );
    }

    if (activeView === 'products') {
      return (
        <ProductsView
          stockView={stockView}
          stockForm={stockForm}
          productFormValues={productFormValues}
          productSearchText={stockSearchText}
          setProductSearchText={setStockSearchText}
          filterMode={productFilterMode}
          setFilterMode={setProductFilterMode}
          stockPage={stockPage}
          setStockPage={setStockPage}
          isLoading={isStockLoading}
          isRefreshing={isStockRefreshing}
          isAddingProduct={isAddingProduct}
          isProductComposerOpen={isProductComposerOpen}
          errorText={stockErrorText}
          onRefresh={() => loadStock(true)}
          selectedProductDetail={selectedProductDetail}
          onOpenProductDetails={setSelectedProductDetail}
          onCloseProductDetails={() => setSelectedProductDetail(null)}
          onSaveProductDetails={handleSaveProductDetails}
          savingProductDetails={isSavingProductDetail}
          onOpenProductComposer={handleOpenProductComposer}
          onCloseProductComposer={() => setIsProductComposerOpen(false)}
          onSubmitProduct={handleSubmitProduct}
          onResetProductForm={handleResetProductForm}
          sellerPhoneOptions={sellerPhoneOptions}
          sellerNameOptions={sellerNameOptions}
          sellerPhoneByName={sellerPhoneByName}
          onCheckStolenImei={handleCheckStolenImei}
          currentTimeLabel={currentTimeLabel}
          summaryColumns={productSummaryColumns}
        />
      );
    }

    if (activeView === 'cashflow') {
      return (
        <CashFlowView
          cashflowSummary={cashflowSummary}
          weeklyAllowance={weeklyAllowance}
          salesSnapshot={salesSnapshot}
          expenses={cashflowExpenses}
          transactions={cashflowTransactions}
          capitalFlow={cashflowCapital}
          debtorsData={debtorsData}
          stockView={stockView}
          nameFixData={nameFixData}
          expenseSource={cashflowExpenseSource}
          expenseSheetTitle={cashflowExpenseSheetTitle}
          loading={cashflowLoading}
          errorText={cashflowError}
          expenseErrorText={cashflowExpenseError}
          expenseBusy={cashflowExpenseBusy}
          lastUpdatedAt={cashflowUpdatedAt}
          onReload={loadCashflowDashboard}
          onCreateExpense={handleCreateCashflowExpense}
          onUndoLastAllowanceWithdrawal={handleUndoLastAllowanceWithdrawal}
        />
      );
    }

    if (activeView === 'cart') {
      const stockViewForDisplay = stockView
        ? { ...stockView, all_rows_cache: Array.isArray(filteredStockRows) ? filteredStockRows : [] }
        : stockView;

      return (
        <CartView
          stockView={stockViewForDisplay}
          stockViewRaw={stockView}
          stockForm={stockForm}
          productSearchText={stockSearchText}
          setProductSearchText={setStockSearchText}
          filterMode={cartFilterMode}
          setFilterMode={setCartFilterMode}
          cartPage={cartPage}
          setCartPage={setCartPage}
          isLoading={isStockLoading}
          isRefreshing={isStockRefreshing}
          errorText={stockErrorText}
          onRefresh={() => loadStock(true)}
          selectedProductDetail={selectedProductDetail}
          onOpenDetails={setSelectedProductDetail}
          onCloseProductDetails={() => setSelectedProductDetail(null)}
          onSaveProductDetails={handleSaveProductDetails}
          savingProductDetails={isSavingProductDetail}
          onAddToCart={handleAddToCart}
          cartItems={saleCartItems}
          onUpdateCartItem={handleUpdateCartItem}
          onRemoveCartItem={handleRemoveCartItem}
          onReturnCartItem={handleReturnCartItem}
          onCheckoutCart={handleCheckoutCart}
          serviceDraft={serviceDraft}
          setServiceDraft={setServiceDraft}
          onSubmitService={handleSubmitService}
          serviceBusy={serviceBusy}
          pendingDealEntries={pendingDealEntries}
          onReturnPendingDeal={handleReturnPendingDeal}
          onUpdatePendingDealPayment={handleUpdatePendingDealPayment}
          onUpdatePendingDealMeta={handleUpdatePendingDealMeta}
          returningPendingKey={returningPendingKey}
          updatingPendingKey={updatingPendingKey}
          updatingPendingMetaKey={updatingPendingMetaKey}
          clientNameOptions={clientNameOptions}
          sellerPhoneOptions={sellerPhoneOptions}
          contactAutofillOptions={contactAutofillOptions}
          currentTimeLabel={currentTimeLabel}
          cartBusy={cartBusy}
          summaryColumns={cartSummaryColumns}
        />
      );
    }

    if (activeView === 'clients') {
      return (
        <MemoClientsView
          clients={filteredClients}
          clientPage={clientPage}
          setClientPage={setClientPage}
          clientSearch={clientSearch}
          setClientSearch={setClientSearch}
          clientForm={clientForm}
          setClientForm={setClientForm}
          clientsBusy={clientsBusy}
          onSelectClient={(entry) => {
            setClientForm({ name: entry.name, phone: entry.phone || '', gender: entry.gender || '' });
            setClientOriginalName(entry.name);
            setGoogleSearch(entry.name);
            setSelectedGoogleContact(null);
          }}
          onSaveClient={handleSaveClient}
          onDeleteClient={handleDeleteClient}
          onImportPhones={handleImportSheetPhones}
          googleContacts={googleContactsView}
          googleSearch={googleSearch}
          setGoogleSearch={setGoogleSearch}
          googleContactPage={googleContactPage}
          setGoogleContactPage={setGoogleContactPage}
          selectedGoogleContact={selectedGoogleContact}
          onSelectGoogleContact={(contact) => {
            setSelectedGoogleContact(contact);
            setClientForm((current) => ({
              name: current.name || contact.name,
              phone: contact.phone,
              gender: current.gender || '',
            }));
          }}
          onSyncGoogleContacts={handleSyncGoogleContacts}
          onApplyGoogleContact={handleApplyGoogleContact}
          googleContactsBusy={googleContactsBusy}
          googleContactsError={googleContactsError}
          stats={clientsData.stats || {}}
        />
      );
    }

    if (activeView === 'debtors') {
      return (
        <DebtorsView
          debtors={filteredDebtors}
          debtorPage={debtorPage}
          setDebtorPage={setDebtorPage}
          debtorSearch={debtorSearch}
          setDebtorSearch={setDebtorSearch}
          selectedDebtor={selectedDebtor}
          onSelectDebtor={handleSelectDebtor}
          billText={billText}
          outstandingItems={outstandingItems}
          paymentAmount={paymentAmount}
          setPaymentAmount={setPaymentAmount}
          selectedServiceRow={selectedServiceRow}
          setSelectedServiceRow={setSelectedServiceRow}
          paymentPlan={paymentPlan}
          paymentPlanError={paymentPlanError}
          detailLoading={isDebtorDetailLoading}
          applyingPayment={isApplyingPayment}
          serviceActionBusy={serviceActionBusy}
          onCopyBill={handleCopyBill}
          onQuickCopyBill={handleCopyBillForDebtor}
          onSendWhatsapp={handleSendWhatsapp}
          onRefreshTodayUnpaid={handleRefreshTodayUnpaidList}
          onSendTodayUnpaidCustomer={handleSendTodayUnpaidCustomer}
          onRefreshDebtorsSection={handleRefreshDebtorsSection}
          sendingTodayBills={sendingTodayBills}
          refreshingDebtorsSection={refreshingDebtorsSection}
          unpaidTodaySummary={unpaidTodaySummary}
          whatsappHistoryByName={whatsappHistoryByName}
          onApplyPayment={handleApplyPayment}
          onApplyFullPayment={handleApplyFullPayment}
          onUpdateServiceRow={handleUpdateDebtorServiceRow}
          onReturnServiceRow={handleReturnDebtorServiceRow}
        />
      );
    }

    if (activeView === 'services_today') {
      return (
        <MemoServicesTodayView
          servicesTodayData={servicesTodayData}
          servicesTodayDate={servicesTodayDate}
          servicesTodayBusy={servicesTodayBusy}
          onChangeDate={setServicesTodayDate}
          onLoadDate={loadServicesTodayForDate}
          onUpdateServiceEntry={handleUpdateServicesTodayEntry}
          onUpdateServicePayment={handleUpdateServicesTodayPayment}
        />
      );
    }

    if (activeView === 'stolen_devices') {
      return (
        <StolenDevicesView
          data={stolenDevicesData}
          busy={stolenDevicesBusy}
          form={stolenDeviceForm}
          onFormChange={(key, value) => setStolenDeviceForm((prev) => ({ ...prev, [key]: value }))}
          onLoad={() => loadStolenDevices(true)}
          onCreate={handleCreateStolenDevice}
          onToggleActive={handleToggleStolenDevice}
        />
      );
    }

    if (activeView === 'bill_notifications') {
      return (
        <BillNotificationsView
          entries={billNotificationEntries}
          onOpenDebtors={() => startTransition(() => setActiveView('debtors'))}
          onSendEntry={handleSendBillNotificationCustomer}
          sendingKey={sendingBillNotificationKey}
        />
      );
    }

    if (activeView === 'fix') {
      return (
        <FixView
          mismatches={nameFixData.mismatches || []}
          selectedMismatch={selectedMismatch}
          correctName={correctName}
          setCorrectName={setCorrectName}
          onSelectMismatch={(entry) => {
            setSelectedMismatchRaw(entry.raw);
            setCorrectName(entry.candidates?.[0] || '');
          }}
          onApplyFix={handleApplyFix}
          onApplyAll={handleApplyAllFixes}
          onRescan={handleRescanFixes}
          loading={isNameFixLoading}
          applying={isNameFixApplying}
        />
      );
    }

    if (activeView === 'settings') {
      return (
        <MemoSettingsView
          syncStatus={syncStatus}
          syncBusy={syncBusy}
          onPullNow={handlePullNow}
          onRefreshWorkspace={handleFullRefresh}
          onReloadStatus={() => loadCoreWorkspace(false)}
        />
      );
    }

    if (activeView === 'users') {
      return (
        <UsersView
          users={usersData}
          usersLoading={usersLoading}
          usersBusy={usersBusy}
          userForm={userForm}
          setUserForm={setUserForm}
          onCreateUser={handleCreateUser}
          onRefreshUsers={loadUsers}
          onUpdateUserRole={handleUpdateUserRole}
          onToggleUserStatus={handleToggleUserStatus}
        />
      );
    }

    return (
      <HomeView
        debtorsData={debtorsData}
        salesSnapshot={salesSnapshot}
        stockView={stockView}
        nameFixData={nameFixData}
        syncStatus={syncStatus}
        lastLoadedAt={lastLoadedAt}
        revealedMetric={revealedMetric}
        setRevealedMetric={setRevealedMetric}
        onSecretCashflow={() => {
          startTransition(() => setActiveView('cashflow'));
        }}
        onStatisticClick={(label) => {
          switch (label) {
            case 'Customers Today':
              startTransition(() => setActiveView('debtors'));
              break;
            case 'Services Today':
              startTransition(() => setActiveView('services_today'));
              break;
            case 'Products Available':
              startTransition(() => {
                setActiveView('products');
                setProductFilterMode('available');
              });
              break;
            case 'Pending Deals':
              startTransition(() => {
                setActiveView('cart');
                setCartFilterMode('pending');
              });
              break;
            case 'Needs Details':
              startTransition(() => {
                setActiveView('products');
                setProductFilterMode('needs_details');
              });
              break;
            case 'Name Fixes':
              startTransition(() => setActiveView('fix'));
              break;
            default:
              break;
          }
        }}
      />
    );
  }

  return (
    <div className="workspace-shell">
      <main className="workspace-page">
        <section className="hero-frame">
          <div className="hero-left">
            <div className="hero-brand">
              {logoData.data_url ? <img className="hero-logo" src={logoData.data_url} alt={logoData.file_name || 'Atlanta logo'} /> : null}
              <div className="hero-brand__copy">
                <h1>Atlanta Georgia_Tech</h1>
                <p>Website workspace for sales, products, clients, debtors, fixes, sync tools, and the new sales cart.</p>
              </div>
            </div>
            <div className="status-chip">
              <span className="status-chip-label">Status</span>
              <strong>{statusText}</strong>
            </div>
            <div className="status-chip">
              <span className="status-chip-label">Signed In</span>
              <strong>{currentUser?.username || 'Unknown'} ({String(currentUser?.role || '').toUpperCase() || 'N/A'})</strong>
            </div>
            {userLoading ? (
              <div className="status-chip">
                <span className="status-chip-label">Account</span>
                <strong>Refreshing user info...</strong>
              </div>
            ) : null}
          </div>

          <div className="hero-right">
            <div className="hero-note">
              <span className="hero-note-label">Current Section</span>
              <strong>{activeMeta.title}</strong>
            </div>
          </div>
        </section>

        <section className="workspace-body">
          <ActionSidebar
            activeView={activeView}
            undoEnabled={undoEnabled}
            redoEnabled={redoEnabled}
            onTrigger={handleAction}
            actionItems={visibleActionItems}
          />

          <section className="workspace-main">
            <section className="content-panel content-panel--headline">
              <div className="panel-header">
                <h3>{activeMeta.title}</h3>
                <p>{activeMeta.description}</p>
              </div>
              {workspaceError ? <div className="notice notice-error notice-inline">{workspaceError}</div> : null}
              {coreLoading ? <div className="notice notice-inline">Loading workspace data...</div> : null}
            </section>

            <ViewErrorBoundary resetKey={`${activeView}-${isAdmin ? 'admin' : 'staff'}`}>
              {renderActiveView()}
            </ViewErrorBoundary>
          </section>
        </section>
      </main>
    </div>
  );
}

export default WorkspaceApp;
