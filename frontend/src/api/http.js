const API_BASE = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '');

export function buildUrl(path, query = {}) {
  const base = API_BASE || window.location.origin;
  const url = new URL(path, base);

  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') {
      return;
    }
    url.searchParams.set(key, String(value));
  });

  return API_BASE ? url.toString() : `${url.pathname}${url.search}`;
}

export async function requestJson(path, { method = 'GET', query, body, headers, signal } = {}) {
  const response = await fetch(buildUrl(path, query), {
    method,
    headers: {
      Accept: 'application/json',
      ...(body ? { 'Content-Type': 'application/json' } : {}),
      ...(headers || {}),
    },
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });

  if (!response.ok) {
    let message = `Request failed with status ${response.status}`;
    try {
      const errorPayload = await response.json();
      if (typeof errorPayload?.detail === 'string') {
        message = errorPayload.detail;
      }
    } catch {
      // Preserve the fallback message when the response is not JSON.
    }
    throw new Error(message);
  }

  return response.json();
}

export function getApiLabel() {
  return API_BASE || 'Vite proxy -> /api';
}