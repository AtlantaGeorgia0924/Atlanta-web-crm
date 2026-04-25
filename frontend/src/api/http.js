let authToken = '';
let unauthorizedHandler = null;

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
	auth = true,
	skipUnauthorizedHandler = false,
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

	const response = await fetch(buildUrl(path, query), {
		method,
		headers: nextHeaders,
		body: body !== null && body !== undefined ? JSON.stringify(body) : undefined,
		signal,
	});

	const contentType = String(response.headers.get('content-type') || '').toLowerCase();
	const isJson = contentType.includes('application/json');
	const payload = isJson ? await response.json().catch(() => ({})) : await response.text().catch(() => '');

	if (!response.ok) {
		if (!skipUnauthorizedHandler && response.status === 401 && unauthorizedHandler) {
			unauthorizedHandler();
		}

		const detail = isJson
			? payload?.detail || payload?.message || response.statusText
			: payload || response.statusText;
		throw new Error(String(detail || `Request failed (${response.status})`));
	}

	return payload;
}
