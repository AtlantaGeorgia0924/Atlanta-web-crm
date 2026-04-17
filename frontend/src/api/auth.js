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

export function fetchCurrentUser({ signal } = {}) {
  return requestJson('/api/auth/me', {
    signal,
  });
}