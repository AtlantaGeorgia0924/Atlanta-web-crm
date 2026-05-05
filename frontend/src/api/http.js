let authToken = '';
let unauthorizedHandler = null;
let refreshInFlight = null;

function getApiBaseUrl() {
	const base = String(import.meta.env.VITE_API_BASE_URL || '').trim();
	return base.replace(/\/$/, '');
}

export function getApiLabel() {
	const base = getApiBaseUrl();
	if (base) {
		return base;
	}
	return 'Same-origin API proxy';
}

export function setAuthToken(token) {
	authToken = String(token || '').trim();
}

export function setUnauthorizedHandler(handler) {
	unauthorizedHandler = typeof handler === 'function' ? handler : null;
}

async function runUnauthorizedHandlerOnce() {
	if (!unauthorizedHandler) {
		return false;
	}
	if (refreshInFlight) {
		return refreshInFlight;
	}
	refreshInFlight = Promise.resolve()
		.then(() => unauthorizedHandler())
		.then((result) => Boolean(result))
		.catch(() => false)
		.finally(() => {
			refreshInFlight = null;
		});
	return refreshInFlight;
}

function buildUrl(path, query = null) {
	const normalizedPath = String(path || '').startsWith('/') ? String(path || '') : `/${String(path || '')}`;
	let base = getApiBaseUrl();
	if (base && /\/api$/i.test(base) && /^\/api(\/|$)/i.test(normalizedPath)) {
		base = base.replace(/\/api$/i, '');
	}
	const origin = typeof window !== 'undefined' ? window.location.origin : 'http://localhost';
	const url = new URL(base ? `${base}${normalizedPath}` : normalizedPath, origin);

	if (query && typeof query === 'object') {
		Object.entries(query).forEach(([key, value]) => {
			if (value === undefined || value === null || value === '') {
				return;
			}
			url.searchParams.set(key, String(value));
		});
	}

	if (!base && typeof window !== 'undefined') {
		return `${url.pathname}${url.search}`;
	}
	return url.toString();
}

export async function requestJson(path, {
	method = 'GET',
	query = null,
	body = null,
	headers = {},
	signal,
	timeoutMs = 0,
	auth = true,
	skipUnauthorizedHandler = false,
	retryOnUnauthorized = true,
} = {}) {
	const nextHeaders = {
		Accept: 'application/json',
		...headers,
	};

	if (body !== null && body !== undefined) {
		nextHeaders['Content-Type'] = 'application/json';
	}

	if (auth && authToken) {
		nextHeaders.Authorization = `Bearer ${authToken}`;
	}

	const timeout = Number(timeoutMs || 0);
	const hasTimeout = Number.isFinite(timeout) && timeout > 0;
	const timeoutController = hasTimeout ? new AbortController() : null;
	const timeoutId = hasTimeout
		? globalThis.setTimeout(() => {
			timeoutController.abort('timeout');
		}, timeout)
		: null;
	let abortFromCaller = null;

	const combinedSignal = timeoutController
		? (() => {
			if (!signal) {
				return timeoutController.signal;
			}
			if (signal.aborted) {
				timeoutController.abort(signal.reason);
				return timeoutController.signal;
			}
			abortFromCaller = () => timeoutController.abort(signal.reason);
			signal.addEventListener('abort', abortFromCaller, { once: true });
			return timeoutController.signal;
		})()
		: signal;

	let response;
	try {
		response = await fetch(buildUrl(path, query), {
			method,
			headers: nextHeaders,
			body: body !== null && body !== undefined ? JSON.stringify(body) : undefined,
			signal: combinedSignal,
		});
	} catch (error) {
		if (timeoutController?.signal?.aborted && timeoutController.signal.reason === 'timeout') {
			throw new Error('Request timed out while contacting the API. Please try again.');
		}
		if (signal?.aborted) {
			throw error;
		}
		throw new Error('Could not reach the API. Make sure the backend is running, then refresh and try again.');
	} finally {
		if (timeoutId) {
			globalThis.clearTimeout(timeoutId);
		}
		if (abortFromCaller && signal) {
			signal.removeEventListener('abort', abortFromCaller);
		}
	}

	const contentType = String(response.headers.get('content-type') || '').toLowerCase();
	const isJson = contentType.includes('application/json');
	const payload = isJson ? await response.json().catch(() => ({})) : await response.text().catch(() => '');

	if (!response.ok) {
		if (!skipUnauthorizedHandler && response.status === 401 && retryOnUnauthorized && unauthorizedHandler) {
			const refreshed = await runUnauthorizedHandlerOnce();
			if (refreshed) {
				return requestJson(path, {
					method,
					query,
					body,
					headers,
					signal,
					timeoutMs,
					auth,
					skipUnauthorizedHandler,
					retryOnUnauthorized: false,
				});
			}
		}

		const detail = isJson
			? payload?.detail || payload?.message || response.statusText
			: payload || response.statusText;
		throw new Error(String(detail || `Request failed (${response.status})`));
	}

	return payload;
}
