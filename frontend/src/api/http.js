const API_BASE = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '');

const RETRYABLE_STATUS = new Set([408, 429, 500, 502, 503, 504, 522, 524]);

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

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
  const upperMethod = String(method || 'GET').toUpperCase();
  const maxAttempts = upperMethod === 'GET' ? 3 : 1;
  let response = null;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      response = await fetch(buildUrl(path, query), {
        method: upperMethod,
        headers: {
          Accept: 'application/json',
          ...(body ? { 'Content-Type': 'application/json' } : {}),
          ...(headers || {}),
        },
        body: body ? JSON.stringify(body) : undefined,
        signal,
      });
    } catch (error) {
      lastError = error;
      if (attempt < maxAttempts) {
        await sleep(250 * attempt);
        continue;
      }
      throw error;
    }

    if (response.ok || !RETRYABLE_STATUS.has(response.status) || attempt >= maxAttempts) {
      break;
    }

    await sleep(250 * attempt);
  }

  if (!response && lastError) {
    throw lastError;
  }

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