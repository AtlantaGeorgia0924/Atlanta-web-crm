import { requestJson } from './http';

export function loginRequest({ username, password }) {
  return requestJson('/api/auth/login', {
    method: 'POST',
    body: {
      username,
      password,
    },
    auth: false,
  });
}

export function refreshRequest({ refreshToken }) {
  return requestJson('/api/auth/refresh', {
    method: 'POST',
    body: {
      refresh_token: refreshToken,
    },
    auth: false,
    skipUnauthorizedHandler: true,
  });
}

export function fetchCurrentUser({ signal } = {}) {
  return requestJson('/api/auth/me', {
    signal,
  });
}