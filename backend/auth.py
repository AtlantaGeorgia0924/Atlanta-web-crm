import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt


class AuthError(RuntimeError):
    pass


class InvalidCredentialsError(AuthError):
    pass


class InactiveUserError(AuthError):
    pass


class TokenValidationError(AuthError):
    pass


class UserExistsError(AuthError):
    pass


class UserNotFoundError(AuthError):
    pass


@dataclass(frozen=True)
class AuthSettings:
    db_path: str
    jwt_secret: str
    jwt_algorithm: str
    jwt_expiration_minutes: int
    default_admin_username: str
    default_admin_password: str

    @classmethod
    def from_base_dir(cls, base_dir: str):
        db_path = os.getenv('APP_AUTH_DB_PATH') or os.path.join(base_dir, 'auth.db')
        jwt_secret = os.getenv('APP_JWT_SECRET') or 'change-this-jwt-secret-in-production'
        jwt_algorithm = os.getenv('APP_JWT_ALGORITHM') or 'HS256'
        expiration_text = os.getenv('APP_JWT_EXPIRATION_MINUTES') or '480'
        try:
            jwt_expiration_minutes = max(1, int(expiration_text))
        except ValueError:
            jwt_expiration_minutes = 480

        return cls(
            db_path=db_path,
            jwt_secret=jwt_secret,
            jwt_algorithm=jwt_algorithm,
            jwt_expiration_minutes=jwt_expiration_minutes,
            default_admin_username=(os.getenv('APP_DEFAULT_ADMIN_USERNAME') or '').strip(),
            default_admin_password=os.getenv('APP_DEFAULT_ADMIN_PASSWORD') or '',
        )


class AuthService:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.settings = AuthSettings.from_base_dir(base_dir)
        self._connection = None
        self._connection_lock = threading.RLock()
        self._initialized = False

    def initialize(self):
        with self._connection_lock:
            if self._initialized and self._connection is not None:
                return

            db_dir = os.path.dirname(self.settings.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

            self._connection = sqlite3.connect(self.settings.db_path, check_same_thread=False)
            self._connection.row_factory = sqlite3.Row

            self._connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('admin', 'staff')),
                    is_active INTEGER NOT NULL DEFAULT 1,
                    logo_url TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                '''
            )
            self._connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)'
            )
            self._connection.execute(
                'CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users(lower(username))'
            )
            
            # Add logo_url column if it doesn't exist (migration for existing databases)
            try:
                self._connection.execute('ALTER TABLE users ADD COLUMN logo_url TEXT')
            except Exception:
                pass  # Column already exists
            
            self._connection.commit()
            self._initialized = True

    def close(self):
        with self._connection_lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._initialized = False

    def _get_connection(self):
        if self._connection is None:
            raise RuntimeError('AuthService is not initialized. Call initialize() at startup.')
        return self._connection

    def _execute(self, query: str, params=()):
        with self._connection_lock:
            connection = self._get_connection()
            cursor = connection.execute(query, params)
            connection.commit()
            return cursor

    def _fetchone(self, query: str, params=()):
        with self._connection_lock:
            connection = self._get_connection()
            return connection.execute(query, params).fetchone()

    def _fetchall(self, query: str, params=()):
        with self._connection_lock:
            connection = self._get_connection()
            return connection.execute(query, params).fetchall()

    def ensure_default_admin(self):
        existing = self._fetchone("SELECT id FROM users WHERE role = 'admin' LIMIT 1")
        if existing:
            return False

        if not self.settings.default_admin_username or not self.settings.default_admin_password:
            raise RuntimeError(
                'APP_DEFAULT_ADMIN_USERNAME and APP_DEFAULT_ADMIN_PASSWORD must be set before first startup.'
            )

        timestamp = self._utc_now_iso()
        self._execute(
            '''
            INSERT INTO users (username, password_hash, role, is_active, created_at, updated_at)
            VALUES (?, ?, 'admin', 1, ?, ?)
            ''',
            (
                self.settings.default_admin_username,
                self.hash_password(self.settings.default_admin_password),
                timestamp,
                timestamp,
            ),
        )
        return True

    def hash_password(self, password: str):
        password_bytes = str(password or '').encode('utf-8')
        return bcrypt.hashpw(password_bytes, bcrypt.gensalt()).decode('utf-8')

    def verify_password(self, password: str, password_hash: str):
        password_bytes = str(password or '').encode('utf-8')
        password_hash_bytes = str(password_hash or '').encode('utf-8')
        try:
            return bcrypt.checkpw(password_bytes, password_hash_bytes)
        except ValueError:
            return False

    def authenticate_user(self, username: str, password: str):
        user = self.get_user_by_username(username)
        if user is None or not self.verify_password(password, user.get('password_hash', '')):
            raise InvalidCredentialsError('Invalid username or password.')
        if not user.get('is_active'):
            raise InactiveUserError('User account is inactive.')
        return self.public_user(user)

    def create_access_token(self, user: dict):
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=self.settings.jwt_expiration_minutes)
        payload = {
            'sub': str(user['id']),
            'username': user['username'],
            'role': user['role'],
            'type': 'access',
            'iat': int(now.timestamp()),
            'exp': int(expires_at.timestamp()),
        }
        return jwt.encode(payload, self.settings.jwt_secret, algorithm=self.settings.jwt_algorithm)

    def validate_access_token(self, token: str):
        try:
            payload = jwt.decode(
                token,
                self.settings.jwt_secret,
                algorithms=[self.settings.jwt_algorithm],
            )
        except jwt.ExpiredSignatureError as exc:
            raise TokenValidationError('Access token has expired.') from exc
        except jwt.InvalidTokenError as exc:
            raise TokenValidationError('Access token is invalid.') from exc

        token_type = str(payload.get('type') or '').strip().lower()
        subject = str(payload.get('sub') or '').strip()
        if token_type != 'access' or not subject:
            raise TokenValidationError('Access token payload is invalid.')
        return payload

    def get_user_by_id(self, user_id):
        try:
            normalized_user_id = int(user_id)
        except (TypeError, ValueError):
            return None
        row = self._fetchone(
            '''
            SELECT id, username, password_hash, role, is_active, created_at, updated_at
            FROM users
            WHERE id = ?
            LIMIT 1
            ''',
            (normalized_user_id,),
        )
        return self._row_to_user(row)

    def get_user_by_username(self, username: str):
        normalized = str(username or '').strip()
        if not normalized:
            return None
        row = self._fetchone(
            '''
            SELECT id, username, password_hash, role, is_active, created_at, updated_at
            FROM users
            WHERE lower(username) = lower(?)
            LIMIT 1
            ''',
            (normalized,),
        )
        return self._row_to_user(row)

    def public_user(self, user: dict):
        return {
            'id': user['id'],
            'username': user['username'],
            'role': user['role'],
            'is_active': bool(user['is_active']),
            'created_at': user['created_at'],
            'updated_at': user['updated_at'],
        }

    def list_users(self):
        rows = self._fetchall(
            '''
            SELECT id, username, password_hash, role, is_active, created_at, updated_at
            FROM users
            ORDER BY lower(username) ASC
            '''
        )
        return [self.public_user(self._row_to_user(row)) for row in rows]

    def create_user(self, username: str, password: str, role: str = 'staff', is_active: bool = True):
        normalized_username = str(username or '').strip()
        normalized_password = str(password or '')
        normalized_role = str(role or '').strip().lower()

        if not normalized_username:
            raise ValueError('Username is required.')
        if len(normalized_username) < 3:
            raise ValueError('Username must be at least 3 characters long.')
        if len(normalized_password) < 6:
            raise ValueError('Password must be at least 6 characters long.')
        if normalized_role not in {'admin', 'staff'}:
            raise ValueError('Role must be either admin or staff.')

        existing = self.get_user_by_username(normalized_username)
        if existing is not None:
            raise UserExistsError('A user with this username already exists.')

        timestamp = self._utc_now_iso()
        cursor = self._execute(
            '''
            INSERT INTO users (username, password_hash, role, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                normalized_username,
                self.hash_password(normalized_password),
                normalized_role,
                1 if bool(is_active) else 0,
                timestamp,
                timestamp,
            ),
        )

        created = self.get_user_by_id(cursor.lastrowid)
        return self.public_user(created)

    def update_user(self, user_id, role: str | None = None, is_active: bool | None = None, logo_url: str | None = None):
        user = self.get_user_by_id(user_id)
        if user is None:
            raise UserNotFoundError('User not found.')

        next_role = user.get('role')
        if role is not None:
            normalized_role = str(role or '').strip().lower()
            if normalized_role not in {'admin', 'staff'}:
                raise ValueError('Role must be either admin or staff.')
            next_role = normalized_role

        next_active = user.get('is_active')
        if is_active is not None:
            next_active = bool(is_active)

        next_logo_url = user.get('logo_url')
        if logo_url is not None:
            next_logo_url = str(logo_url or '').strip() or None

        timestamp = self._utc_now_iso()
        self._execute(
            '''
            UPDATE users
            SET role = ?, is_active = ?, logo_url = ?, updated_at = ?
            WHERE id = ?
            ''',
            (
                next_role,
                1 if next_active else 0,
                next_logo_url,
                timestamp,
                int(user_id),
            ),
        )

        updated = self.get_user_by_id(user_id)
        return self.public_user(updated)

    def _row_to_user(self, row):
        if row is None:
            return None
        user = dict(row)
        user['is_active'] = bool(user.get('is_active'))
        return user

    def _utc_now_iso(self):
        return datetime.now(timezone.utc).isoformat()