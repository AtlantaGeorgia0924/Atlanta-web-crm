import { createContext, useContext, useEffect, useMemo, useState } from 'react';

import { fetchCurrentUser, loginRequest } from '../api/auth';
import { setAuthToken, setUnauthorizedHandler } from '../api/http';

const TOKEN_STORAGE_KEY = 'atlanta_auth_token';
const INITIAL_TOKEN = readStoredToken();

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
  const [user, setUser] = useState(() => decodeTokenUser(INITIAL_TOKEN));
  const [isUserLoading, setIsUserLoading] = useState(Boolean(INITIAL_TOKEN));

  useEffect(() => {
    setAuthToken(token || '');
  }, [token]);

  useEffect(() => {
    setUnauthorizedHandler(() => {
      setAuthToken('');
      setToken('');
      setUser(null);
      setIsUserLoading(false);
      writeStoredToken('');
    });

    return () => {
      setUnauthorizedHandler(null);
    };
  }, []);

  useEffect(() => {
    let active = true;

    async function hydrateUser() {
      if (!token) {
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
          setToken('');
          setUser(null);
          setIsUserLoading(false);
          writeStoredToken('');
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
    const nextUser = result?.user || null;
    if (!nextToken || !nextUser) {
      throw new Error('Login succeeded but auth payload is incomplete.');
    }

    // Set HTTP auth header source immediately to avoid first-render 401 races.
    setAuthToken(nextToken);
    setToken(nextToken);
    setUser(nextUser);
    setIsUserLoading(false);
    writeStoredToken(nextToken);
    return nextUser;
  }

  function logout() {
    setAuthToken('');
    setToken('');
    setUser(null);
    setIsUserLoading(false);
    writeStoredToken('');
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