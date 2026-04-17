import React from 'react';
import ReactDOM from 'react-dom/client';

import { AuthProvider, ProtectedRoute, useAuth } from './auth/AuthContext';
import LoginPage from './LoginPage';
import WorkspaceApp from './WorkspaceApp';
import './workspace.css';

function AppRouter() {
  const { user, logout, isUserLoading } = useAuth();

  return (
    <ProtectedRoute fallback={<LoginPage />}>
      <WorkspaceApp currentUser={user} onLogout={logout} userLoading={isUserLoading} />
    </ProtectedRoute>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <AuthProvider>
    <AppRouter />
  </AuthProvider>
);