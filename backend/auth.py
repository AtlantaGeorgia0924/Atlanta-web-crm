import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_AVAILABLE = True
except Exception:
    psycopg2 = None
    RealDictCursor = None
    PSYCOPG2_AVAILABLE = False


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
    postgres_dsn: str
    jwt_secret: str
    jwt_algorithm: str
    jwt_expiration_minutes: int
    default_admin_username: str
    default_admin_password: str

    @classmethod
    def from_base_dir(cls, base_dir: str):
        db_path = os.getenv('APP_AUTH_DB_PATH') or os.path.join(base_dir, 'auth.db')
        postgres_dsn = (
            os.getenv('APP_AUTH_POSTGRES_DSN')
            or os.getenv('AUTH_POSTGRES_DSN')
            or os.getenv('POSTGRES_DSN')
            or os.getenv('DATABASE_URL')
            or ''
        ).strip()
        jwt_secret = os.getenv('APP_JWT_SECRET') or 'change-this-jwt-secret-in-production'
        jwt_algorithm = os.getenv('APP_JWT_ALGORITHM') or 'HS256'
        expiration_text = os.getenv('APP_JWT_EXPIRATION_MINUTES') or '480'
        try:
            jwt_expiration_minutes = max(1, int(expiration_text))
        except ValueError:
            jwt_expiration_minutes = 480

        return cls(
            db_path=db_path,
            postgres_dsn=postgres_dsn,
            jwt_secret=jwt_secret,
            jwt_algorithm=jwt_algorithm,
            jwt_expiration_minutes=jwt_expiration_minutes,
            default_admin_username=(os.getenv('APP_DEFAULT_ADMIN_USERNAME') or 'admin').strip(),
            default_admin_password=os.getenv('APP_DEFAULT_ADMIN_PASSWORD') or 'Atlanta',
        )


class AuthService:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.settings = AuthSettings.from_base_dir(base_dir)
        self._connection = None
        self._pg_connection = None
        self._connection_lock = threading.RLock()
        self._initialized = False
        self._storage_mode = 'sqlite'

    def initialize(self):
        with self._connection_lock:
            if self._initialized:
                return

            if self._initialize_postgres_storage():
                self._storage_mode = 'postgres'
                self._initialized = True
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
            self._connection.execute('CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)')
            self._connection.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users(lower(username))')
            try:
                self._connection.execute('ALTER TABLE users ADD COLUMN logo_url TEXT')
            except Exception:
                pass

            self._connection.commit()
            self._storage_mode = 'sqlite'
            self._initialized = True

    def _initialize_postgres_storage(self):
        if not self.settings.postgres_dsn or not PSYCOPG2_AVAILABLE:
            return False

        try:
            self._pg_connection = psycopg2.connect(self.settings.postgres_dsn, connect_timeout=5)
            self._pg_connection.autocommit = True
            self._ensure_postgres_schema()
            self._maybe_migrate_sqlite_users_to_postgres()
            return True
        except Exception:
            if self._pg_connection is not None:
                try:
                    self._pg_connection.close()
                except Exception:
                    pass
            self._pg_connection = None
            return False

    def _ensure_postgres_schema(self):
        connection = self._get_postgres_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS auth_users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('admin', 'staff')),
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    logo_url TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                '''
            )
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_users_username_lower ON auth_users(lower(username))')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_auth_users_role ON auth_users(role)')

    def _maybe_migrate_sqlite_users_to_postgres(self):
        if not os.path.exists(self.settings.db_path):
            return

        sqlite_connection = None
        try:
            sqlite_connection = sqlite3.connect(self.settings.db_path)
            sqlite_connection.row_factory = sqlite3.Row
            rows = sqlite_connection.execute(
                '''
                SELECT id, username, password_hash, role, is_active, logo_url, created_at, updated_at
                FROM users
                ORDER BY id ASC
                '''
            ).fetchall()
        except Exception:
            return
        finally:
            if sqlite_connection is not None:
                try:
                    sqlite_connection.close()
                except Exception:
                    pass

        if not rows:
            return

        connection = self._get_postgres_connection()
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM auth_users')
            existing_count = int((cursor.fetchone() or [0])[0] or 0)
            if existing_count:
                return

            for row in rows:
                cursor.execute(
                    '''
                    INSERT INTO auth_users (username, password_hash, role, is_active, logo_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (username) DO NOTHING
                    ''',
                    (
                        row['username'],
                        row['password_hash'],
                        row['role'],
                        bool(row['is_active']),
                        row['logo_url'],
                        row['created_at'],
                        row['updated_at'],
                    ),
                )

    def close(self):
        with self._connection_lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            if self._pg_connection is not None:
                try:
                    self._pg_connection.close()
                except Exception:
                    pass
                self._pg_connection = None
            self._initialized = False
            self._storage_mode = 'sqlite'

    def _get_connection(self):
        if self._connection is None:
            raise RuntimeError('AuthService is not initialized. Call initialize() at startup.')
        return self._connection

    def _get_postgres_connection(self):
        if self._pg_connection is None:
            raise RuntimeError('AuthService PostgreSQL connection is not initialized.')
        if getattr(self._pg_connection, 'closed', 1):
            self._pg_connection = psycopg2.connect(self.settings.postgres_dsn, connect_timeout=5)
            self._pg_connection.autocommit = True
        return self._pg_connection

    def _is_postgres_storage(self):
        return self._storage_mode == 'postgres'

    def _execute(self, query: str, params=()):
        with self._connection_lock:
            if self._is_postgres_storage():
                connection = self._get_postgres_connection()
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor

            connection = self._get_connection()
            cursor = connection.execute(query, params)
            connection.commit()
            return cursor

    def _fetchone(self, query: str, params=()):
        with self._connection_lock:
            if self._is_postgres_storage():
                connection = self._get_postgres_connection()
                with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchone()

            connection = self._get_connection()
            return connection.execute(query, params).fetchone()

    def _fetchall(self, query: str, params=()):
        with self._connection_lock:
            if self._is_postgres_storage():
                connection = self._get_postgres_connection()
                with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()

            connection = self._get_connection()
            return connection.execute(query, params).fetchall()

    def ensure_default_admin(self):
        default_admin_username = self.settings.default_admin_username
        default_admin_password = self.settings.default_admin_password

        if not default_admin_username or not default_admin_password:
            if self._is_postgres_storage():
                existing_admin = self._fetchone("SELECT id FROM auth_users WHERE role = 'admin' LIMIT 1")
            else:
                existing_admin = self._fetchone("SELECT id FROM users WHERE role = 'admin' LIMIT 1")
            if existing_admin:
                return False
            raise RuntimeError(
                'APP_DEFAULT_ADMIN_USERNAME and APP_DEFAULT_ADMIN_PASSWORD must be set before first startup.'
            )

        existing_default_admin = self.get_user_by_username(default_admin_username)
        timestamp = self._utc_now_iso()
        desired_password_hash = self.hash_password(default_admin_password)

        if existing_default_admin is None:
            if self._is_postgres_storage():
                self._execute(
                    '''
                    INSERT INTO auth_users (username, password_hash, role, is_active, created_at, updated_at)
                    VALUES (%s, %s, 'admin', TRUE, %s, %s)
                    ON CONFLICT (username) DO NOTHING
                    ''',
                    (
                        default_admin_username,
                        desired_password_hash,
                        timestamp,
                        timestamp,
                    ),
                )
            else:
                self._execute(
                    '''
                    INSERT INTO users (username, password_hash, role, is_active, created_at, updated_at)
                    VALUES (?, ?, 'admin', 1, ?, ?)
                    ''',
                    (
                        default_admin_username,
                        desired_password_hash,
                        timestamp,
                        timestamp,
                    ),
                )
            return True

        password_matches = self.verify_password(default_admin_password, existing_default_admin.get('password_hash', ''))
        role_matches = str(existing_default_admin.get('role') or '').strip().lower() == 'admin'
        active_matches = bool(existing_default_admin.get('is_active'))

        if password_matches and role_matches and active_matches:
            return False

        if self._is_postgres_storage():
            self._execute(
                '''
                UPDATE auth_users
                SET password_hash = %s, role = 'admin', is_active = TRUE, updated_at = %s
                WHERE id = %s
                ''',
                (
                    desired_password_hash,
                    timestamp,
                    int(existing_default_admin['id']),
                ),
            )
        else:
            self._execute(
                '''
                UPDATE users
                SET password_hash = ?, role = 'admin', is_active = 1, updated_at = ?
                WHERE id = ?
                ''',
                (
                    desired_password_hash,
                    timestamp,
                    int(existing_default_admin['id']),
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

        if self._is_postgres_storage():
            row = self._fetchone(
                '''
                SELECT id, username, password_hash, role, is_active, logo_url, created_at, updated_at
                FROM auth_users
                WHERE id = %s
                LIMIT 1
                ''',
                (normalized_user_id,),
            )
        else:
            row = self._fetchone(
                '''
                SELECT id, username, password_hash, role, is_active, logo_url, created_at, updated_at
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

        if self._is_postgres_storage():
            row = self._fetchone(
                '''
                SELECT id, username, password_hash, role, is_active, logo_url, created_at, updated_at
                FROM auth_users
                WHERE lower(username) = lower(%s)
                LIMIT 1
                ''',
                (normalized,),
            )
        else:
            row = self._fetchone(
                '''
                SELECT id, username, password_hash, role, is_active, logo_url, created_at, updated_at
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
        if self._is_postgres_storage():
            rows = self._fetchall(
                '''
                SELECT id, username, password_hash, role, is_active, logo_url, created_at, updated_at
                FROM auth_users
                ORDER BY lower(username) ASC
                '''
            )
        else:
            rows = self._fetchall(
                '''
                SELECT id, username, password_hash, role, is_active, logo_url, created_at, updated_at
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

        if self.get_user_by_username(normalized_username) is not None:
            raise UserExistsError('A user with this username already exists.')

        timestamp = self._utc_now_iso()
        if self._is_postgres_storage():
            self._execute(
                '''
                INSERT INTO auth_users (username, password_hash, role, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ''',
                (
                    normalized_username,
                    self.hash_password(normalized_password),
                    normalized_role,
                    bool(is_active),
                    timestamp,
                    timestamp,
                ),
            )
            created = self.get_user_by_username(normalized_username)
            return self.public_user(created)

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
        if self._is_postgres_storage():
            self._execute(
                '''
                UPDATE auth_users
                SET role = %s, is_active = %s, logo_url = %s, updated_at = %s
                WHERE id = %s
                ''',
                (
                    next_role,
                    bool(next_active),
                    next_logo_url,
                    timestamp,
                    int(user_id),
                ),
            )
        else:
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
