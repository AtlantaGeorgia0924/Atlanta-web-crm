let authToken = '';
let unauthorizedHandler = null;
let refreshInFlight = null;
const responseCache = new Map();
const inFlightRequests = new Map();

function getApiDiagnosticsStore() {
	if (typeof window === 'undefined') {
		return null;
	}
	if (!window.__ATLANTA_API_DIAGNOSTICS__) {
		window.__ATLANTA_API_DIAGNOSTICS__ = {
			requests: [],
			largestPayloads: [],
			slowRequests: [],
		};
	}
	return window.__ATLANTA_API_DIAGNOSTICS__;
}

function stableSerialize(value) {
	if (value === null || value === undefined) {
		return '';
	}
	if (Array.isArray(value)) {
		return `[${value.map((item) => stableSerialize(item)).join(',')}]`;
	}
	if (typeof value === 'object') {
		return `{${Object.keys(value).sort().map((key) => `${key}:${stableSerialize(value[key])}`).join(',')}}`;
	}
	return String(value);
}

function clonePayload(payload) {
	if (typeof structuredClone === 'function') {
		return structuredClone(payload);
	}
	return JSON.parse(JSON.stringify(payload));
}

function makeRequestKey({ method, path, query, body, auth }) {
	return [method, path, stableSerialize(query), stableSerialize(body), auth ? 'auth' : 'public'].join('::');
}

function clearApiCache(prefix = '') {
	if (!prefix) {
		responseCache.clear();
		return;
	}
	for (const key of responseCache.keys()) {
		if (key.includes(prefix)) {
			responseCache.delete(key);
		}
	}
}

function recordApiTiming({ method, path, durationMs, payloadSize, status, source }) {
	const store = getApiDiagnosticsStore();
	const entry = {
		method,
		path,
		durationMs: Math.round(durationMs * 100) / 100,
		payloadSize,
		status,
		source,
		at: new Date().toISOString(),
	};

	if (store) {
		store.requests = [entry, ...(store.requests || [])].slice(0, 100);
		store.largestPayloads = [entry, ...(store.largestPayloads || [])]
			.sort((left, right) => (right.payloadSize || 0) - (left.payloadSize || 0))
			.slice(0, 20);
		if (durationMs > 300) {
			store.slowRequests = [entry, ...(store.slowRequests || [])].slice(0, 50);
		}
	}

	if (durationMs > 300 || payloadSize > 50_000) {
		console.info(
			`[api-timing] ${method} ${path} ${Math.round(durationMs)}ms payload=${payloadSize}B source=${source} status=${status}`,
		);
	}
}

export function invalidateApiCache(prefix = '') {
	clearApiCache(prefix);
}

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
	cacheTtlMs = 0,
	cacheKey = '',
	dedupeKey = '',
} = {}) {
	const methodUpper = String(method || 'GET').toUpperCase();
	const requestKey = cacheKey || dedupeKey || makeRequestKey({ method: methodUpper, path, query, body, auth });
	const now = Date.now();

	if (methodUpper === 'GET' && cacheTtlMs > 0) {
		const cached = responseCache.get(requestKey);
		if (cached && cached.expiresAt > now) {
			recordApiTiming({
				method: methodUpper,
				path,
				durationMs: 0,
				payloadSize: cached.payloadSize || 0,
				status: 200,
				source: 'memory_cache',
			});
			return clonePayload(cached.payload);
		}
	}

	if (inFlightRequests.has(requestKey)) {
		return inFlightRequests.get(requestKey);
	}

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

	const requestUrl = buildUrl(path, query);
	const startedAt = performance.now();

	const executeRequest = (async () => {
	let response;
	try {
		response = await fetch(requestUrl, {
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
		const hostname = typeof window !== 'undefined' ? String(window.location.hostname || '').trim().toLowerCase() : '';
		const isLocalHost = hostname === 'localhost' || hostname === '127.0.0.1';
		if (!getApiBaseUrl() && hostname && !isLocalHost) {
			throw new Error('Could not reach the API. Set VITE_API_BASE_URL in your frontend deployment to your backend URL, then redeploy.');
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
	const payloadSize = (() => {
		if (typeof payload === 'string') {
			return payload.length;
		}
		try {
			return JSON.stringify(payload).length;
		} catch {
			return 0;
		}
	})();

	if (!response.ok) {
		recordApiTiming({
			method: methodUpper,
			path,
			durationMs: performance.now() - startedAt,
			payloadSize,
			status: response.status,
			source: 'network_error',
		});
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

	recordApiTiming({
		method: methodUpper,
		path,
		durationMs: performance.now() - startedAt,
		payloadSize,
		status: response.status,
		source: 'network',
	});

	if (methodUpper === 'GET' && cacheTtlMs > 0) {
		responseCache.set(requestKey, {
			expiresAt: now + cacheTtlMs,
			payload,
			payloadSize,
		});
	} else if (methodUpper !== 'GET') {
		clearApiCache();
	}

	return payload;
	})();

	inFlightRequests.set(requestKey, executeRequest);
	try {
		return await executeRequest;
	} finally {
		inFlightRequests.delete(requestKey);
	}
}
