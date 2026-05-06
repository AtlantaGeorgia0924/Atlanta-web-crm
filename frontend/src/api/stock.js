import { getApiLabel, requestJson } from './http';

export async function fetchStockDashboard({ filterText = '', filterMode = 'all', forceRefresh = false, signal } = {}) {
  return requestJson('/api/stock/view/live', {
    query: {
      filter_text: filterText,
      filter_mode: filterMode,
      force_refresh: forceRefresh,
    }
    ,
    signal,
    cacheTtlMs: forceRefresh ? 0 : 15_000,
  });
}

export async function checkoutSaleCart({ items, forceRefresh = false }) {
  return requestJson('/api/stock/live/cart/checkout', {
    method: 'POST',
    body: {
      items,
      force_refresh: forceRefresh,
    },
  });
}

export async function updateStockRow({ rowNum, valuesByHeader, forceRefresh = false }) {
  return requestJson('/api/stock/live/update-row', {
    method: 'POST',
    body: {
      row_num: rowNum,
      values_by_header: valuesByHeader,
      force_refresh: forceRefresh,
    },
  });
}

export async function addServiceRecord({ valuesByHeader, forceRefresh = false }) {
  return requestJson('/api/stock/live/service/add', {
    method: 'POST',
    body: {
      values_by_header: valuesByHeader,
      force_refresh: forceRefresh,
    },
  });
}

export async function fetchPendingServiceDeals({ forceRefresh = false, signal } = {}) {
  return requestJson('/api/stock/live/service/pending', {
    query: {
      force_refresh: forceRefresh,
    },
    signal,
    cacheTtlMs: forceRefresh ? 0 : 10_000,
  });
}

export async function returnServiceDeal({ rowNum, forceRefresh = false }) {
  return requestJson('/api/stock/live/service/return', {
    method: 'POST',
    body: {
      row_num: rowNum,
      force_refresh: forceRefresh,
    },
  });
}

export async function updateServiceDealPayment({ rowNum, paymentStatus, amountPaid = null, forceRefresh = false }) {
  return requestJson('/api/stock/live/service/payment', {
    method: 'POST',
    body: {
      row_num: rowNum,
      payment_status: paymentStatus,
      amount_paid: amountPaid,
      force_refresh: forceRefresh,
    },
  });
}

export async function returnStockItem({ rowNum, forceRefresh = false }) {
  return requestJson('/api/stock/live/return', {
    method: 'POST',
    body: {
      row_num: rowNum,
      force_refresh: forceRefresh,
    },
  });
}

export async function updatePendingDealPayment({ rowNum, paymentStatus, amountPaid = null, forceRefresh = false }) {
  return requestJson('/api/stock/live/pending/payment', {
    method: 'POST',
    body: {
      row_num: rowNum,
      payment_status: paymentStatus,
      amount_paid: amountPaid,
      force_refresh: forceRefresh,
    },
  });
}

export async function updatePendingDealMeta({ rowNum, valuesByHeader, forceRefresh = false }) {
  return requestJson('/api/stock/live/pending/meta', {
    method: 'POST',
    body: {
      row_num: rowNum,
      values_by_header: valuesByHeader,
      force_refresh: forceRefresh,
    },
  });
}

export async function updatePendingServiceMeta({ rowNum, valuesByHeader, forceRefresh = false }) {
  return requestJson('/api/stock/live/service/meta', {
    method: 'POST',
    body: {
      row_num: rowNum,
      values_by_header: valuesByHeader,
      force_refresh: forceRefresh,
    },
  });
}