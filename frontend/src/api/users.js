import { requestJson } from './http';

export function fetchUsers({ signal } = {}) {
  return requestJson('/api/users', { signal });
}

export function createUser({ username, password, role = 'staff', isActive = true }) {
  return requestJson('/api/users', {
    method: 'POST',
    body: {
      username,
      password,
      role,
      is_active: isActive,
    },
  });
}

export function updateUser({ userId, role, isActive }) {
  const body = {};
  if (role !== undefined) {
    body.role = role;
  }
  if (isActive !== undefined) {
    body.is_active = isActive;
  }

  return requestJson(`/api/users/${encodeURIComponent(String(userId))}`, {
    method: 'PATCH',
    body,
  });
}
