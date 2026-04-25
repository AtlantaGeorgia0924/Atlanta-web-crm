import { createContext, useContext, useEffect, useMemo, useState } from 'react';

import { fetchCurrentUser, loginRequest, refreshRequest } from '../api/auth';
import { setAuthToken, setUnauthorizedHandler } from '../api/http';

const TOKEN_STORAGE_KEY = 'atlanta_auth_token';
const REFRESH_TOKEN_STORAGE_KEY = 'atlanta_refresh_token';
const INITIAL_TOKEN = readStoredToken();
const INITIAL_REFRESH_TOKEN = readStoredRefreshToken();

setAuthToken(INITIAL_TOKEN);

const AuthContext = createContext(null);

function readStoredToken() {
  try {
    return localStorage.getItem(TOKEN_STORAGE_KEY) || '';
  } catch {
    return '';
  }
}

function writeStoredToken(token) {
  try {
    if (token) {
      localStorage.setItem(TOKEN_STORAGE_KEY, token);
    } else {
      localStorage.removeItem(TOKEN_STORAGE_KEY);
    }
  } catch {
    // Ignore storage failures in restricted browser modes.
  }
}

function readStoredRefreshToken() {
  try {
    return localStorage.getItem(REFRESH_TOKEN_STORAGE_KEY) || '';
  } catch {
    return '';
  }
}

function writeStoredRefreshToken(token) {
  try {
    if (token) {
      localStorage.setItem(REFRESH_TOKEN_STORAGE_KEY, token);
    } else {
      localStorage.removeItem(REFRESH_TOKEN_STORAGE_KEY);
    }
  } catch {
    // Ignore storage failures in restricted browser modes.
  }
}

function decodeTokenUser(token) {
  const raw = String(token || '').trim();
  if (!raw) {
    return null;
  }

  try {
    const parts = raw.split('.');
    if (parts.length < 2) {
      return null;
    }
    const payloadPart = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const jsonText = atob(payloadPart.padEnd(Math.ceil(payloadPart.length / 4) * 4, '='));
    const payload = JSON.parse(jsonText);
    if (!payload || !payload.sub) {
      return null;
    }

    return {
      id: Number(payload.sub),
      username: String(payload.username || ''),
      role: String(payload.role || ''),
      is_active: true,
      created_at: '',
      updated_at: '',
    };
  } catch {
    return null;
  }
}

export function AuthProvider({ children }) {
  const [token, setToken] = useState(() => INITIAL_TOKEN);
  const [refreshToken, setRefreshToken] = useState(() => INITIAL_REFRESH_TOKEN);
  const [user, setUser] = useState(() => decodeTokenUser(INITIAL_TOKEN));
  const [isUserLoading, setIsUserLoading] = useState(Boolean(INITIAL_TOKEN || INITIAL_REFRESH_TOKEN));

  async function clearAuth() {
    setAuthToken('');
    setToken('');
    setRefreshToken('');
    setUser(null);
    setIsUserLoading(false);
    writeStoredToken('');
    writeStoredRefreshToken('');
  }

  async function refreshSession() {
    if (!refreshToken) {
      await clearAuth();
      return false;
    }

    try {
      const result = await refreshRequest({ refreshToken });
      const nextAccessToken = String(result?.access_token || '').trim();
      const nextRefreshToken = String(result?.refresh_token || '').trim();
      const nextUser = result?.user || null;
      if (!nextAccessToken || !nextRefreshToken || !nextUser) {
        throw new Error('Refresh succeeded but auth payload is incomplete.');
      }

      setAuthToken(nextAccessToken);
      setToken(nextAccessToken);
      setRefreshToken(nextRefreshToken);
      setUser(nextUser);
      setIsUserLoading(false);
      writeStoredToken(nextAccessToken);
      writeStoredRefreshToken(nextRefreshToken);
      return true;
    } catch {
      await clearAuth();
      return false;
    }
  }

  useEffect(() => {
    setAuthToken(token || '');
  }, [token]);

  useEffect(() => {
    if (token || refreshToken) {
      return;
    }
    setIsUserLoading(false);
  }, [token, refreshToken]);

  useEffect(() => {
    setUnauthorizedHandler(() => {
      void refreshSession();
    });

    return () => {
      setUnauthorizedHandler(null);
    };
  }, [refreshToken]);

  useEffect(() => {
    let active = true;

    async function hydrateUser() {
      if (!token) {
        if (refreshToken) {
          await refreshSession();
          return;
        }

        if (active) {
          setUser(null);
          setIsUserLoading(false);
        }
        return;
      }

      if (active) {
        setUser((current) => current || decodeTokenUser(token));
        setIsUserLoading(true);
      }

      try {
        const currentUser = await fetchCurrentUser();
        if (active) {
          setUser(currentUser);
        }
      } catch {
        if (active) {
          setIsUserLoading(false);
        }
      } finally {
        if (active) {
          setIsUserLoading(false);
        }
      }
    }

    hydrateUser();

    return () => {
      active = false;
    };
  }, [token]);

  async function login(username, password) {
    const result = await loginRequest({ username, password });
    const nextToken = String(result?.access_token || '').trim();
    const nextRefreshToken = String(result?.refresh_token || '').trim();
    const nextUser = result?.user || null;
    if (!nextToken || !nextRefreshToken || !nextUser) {
      throw new Error('Login succeeded but auth payload is incomplete.');
    }

    // Set HTTP auth header source immediately to avoid first-render 401 races.
    setAuthToken(nextToken);
    setToken(nextToken);
    setRefreshToken(nextRefreshToken);
    setUser(nextUser);
    setIsUserLoading(false);
    writeStoredToken(nextToken);
    writeStoredRefreshToken(nextRefreshToken);
    return nextUser;
  }

  function logout() {
    void clearAuth();
  }

  const value = useMemo(() => ({
    token,
    user,
    isAuthenticated: Boolean(token),
    isUserLoading,
    login,
    logout,
  }), [token, user, isUserLoading]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider.');
  }
  return context;
}

export function ProtectedRoute({ children, fallback = null }) {
  const { isAuthenticated } = useAuth();
  if (!isAuthenticated) {
    return fallback;
  }
  return children;
}