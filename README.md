# Atlanta Stock Dashboard

## Production fix for disappearing staff users

The app now supports persistent auth storage when the backend is given a Postgres DSN.
If you deploy the frontend separately, you must also point it at a real backend API host.

Set these environment variables on the backend deployment:

- `APP_AUTH_POSTGRES_DSN` or `AUTH_POSTGRES_DSN` or `POSTGRES_DSN` or `DATABASE_URL`
- `APP_DEFAULT_ADMIN_USERNAME`
- `APP_DEFAULT_ADMIN_PASSWORD`

Set this on the frontend deployment:

- `VITE_API_BASE_URL=https://your-backend-host.example.com`

If `VITE_API_BASE_URL` is not set, the frontend uses same-origin or the Vite proxy in local development.
If no persistent auth DSN is set, the backend falls back to local SQLite and created users can disappear when the process or container is replaced.

## Local development

Frontend:

- `cd frontend`
- `npm install`
- `npm run dev`

Backend:

- `python3 -m uvicorn backend.main:app --host 127.0.0.1 --port 8000`
