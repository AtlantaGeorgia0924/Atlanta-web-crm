import { useEffect, useState } from 'react';

import { fetchDashboardLogo } from './api/workspace';
import { useAuth } from './auth/AuthContext';

function LoginPage() {
  const { login, isInitializing } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [errorText, setErrorText] = useState('');
  const [logoUrl, setLogoUrl] = useState('');

  useEffect(() => {
    let active = true;

    async function loadLogo() {
      try {
        const result = await fetchDashboardLogo({ auth: false });
        if (active) {
          setLogoUrl(String(result?.data_url || '').trim());
        }
      } catch {
        if (active) {
          setLogoUrl('');
        }
      }
    }

    loadLogo();
    return () => {
      active = false;
    };
  }, []);

  async function handleSubmit(event) {
    event.preventDefault();
    setSubmitting(true);
    setErrorText('');

    try {
      await login(username.trim(), password);
    } catch (error) {
      setErrorText(error?.message || 'Login failed. Check your credentials and try again.');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div
      className="workspace-shell login-shell"
      style={{ '--login-logo-url': logoUrl ? `url(${logoUrl})` : 'none' }}
    >
      <main className="workspace-page login-page">
        <section className="content-panel login-card">
          <div className="panel-header">
            <h3>Sign In</h3>
            <p>Enter your username and password to access Atlanta Georgia_Tech workspace.</p>
          </div>

          <form className="form-stack" onSubmit={handleSubmit}>
            <label className="field-block">
              <span className="field-label">Username</span>
              <input
                type="text"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                autoComplete="username"
                required
              />
            </label>

            <label className="field-block">
              <span className="field-label">Password</span>
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                autoComplete="current-password"
                required
              />
            </label>

            {errorText ? <div className="notice notice-error">{errorText}</div> : null}

            <div className="button-row button-row--end">
              <button type="submit" className="primary-button" disabled={submitting || isInitializing}>
                {submitting ? 'Signing In...' : 'Sign In'}
              </button>
            </div>
          </form>
        </section>
      </main>
    </div>
  );
}

export default LoginPage;