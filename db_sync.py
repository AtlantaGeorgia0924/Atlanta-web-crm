import json
import logging
import threading
import time
from datetime import datetime, timezone

try:
    import psycopg2
    from psycopg2.extras import Json
    PSYCOPG2_AVAILABLE = True
except Exception:
    psycopg2 = None
    Json = None
    PSYCOPG2_AVAILABLE = False


class PostgresSyncManager:
    """Sheet-wins PostgreSQL cache/sync scaffold.

    This class is intentionally lightweight for phased rollout:
    - stores cached sheet snapshots for fast startup/read paths
    - logs sync outcomes
    - runs background pull loop at configured interval
    """

    def __init__(self, dsn, pull_interval_sec=90, logger=None):
        self.dsn = str(dsn or '').strip()
        self.pull_interval_sec = max(15, int(pull_interval_sec or 90))
        self.logger = logger or logging.getLogger(__name__)
        self._db_lock = threading.RLock()
        self._thread = None
        self._queue_thread = None
        self._stop_event = threading.Event()

    @property
    def ready(self):
        return bool(self.dsn) and PSYCOPG2_AVAILABLE

    def _connect(self):
        return psycopg2.connect(self.dsn, connect_timeout=5)

    def execute(self, sql, params=None):
        if not self.ready:
            raise RuntimeError('PostgreSQL sync manager is not ready')
        with self._db_lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    return cur.rowcount

    def fetchone(self, sql, params=None):
        if not self.ready:
            raise RuntimeError('PostgreSQL sync manager is not ready')
        with self._db_lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    return cur.fetchone()

    def fetchall(self, sql, params=None):
        if not self.ready:
            raise RuntimeError('PostgreSQL sync manager is not ready')
        with self._db_lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    return cur.fetchall()

    def fetchone_dict(self, sql, params=None):
        if not self.ready:
            raise RuntimeError('PostgreSQL sync manager is not ready')
        with self._db_lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    fetched = cur.fetchone()
                    if not fetched:
                        return None
                    columns = [desc[0] for desc in cur.description or []]
        return {columns[idx]: fetched[idx] for idx in range(len(columns))}

    def fetchall_dict(self, sql, params=None):
        if not self.ready:
            raise RuntimeError('PostgreSQL sync manager is not ready')
        with self._db_lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description or []]
        return [
            {columns[idx]: row[idx] for idx in range(len(columns))}
            for row in rows
        ]

    def ensure_schema(self):
        if not self.ready:
            return False

        schema_sql = """
        CREATE TABLE IF NOT EXISTS sheet_cache (
            sheet_key TEXT PRIMARY KEY,
            payload_json JSONB NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'sheet',
            pulled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id BIGSERIAL PRIMARY KEY,
            sync_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            details TEXT,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS sync_queue (
            id BIGSERIAL PRIMARY KEY,
            entity_name TEXT NOT NULL,
            operation TEXT NOT NULL,
            record_id TEXT,
            payload_json JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS conflict_log (
            id BIGSERIAL PRIMARY KEY,
            sheet_key TEXT NOT NULL,
            record_id TEXT,
            field_name TEXT,
            db_value TEXT,
            sheet_value TEXT,
            resolution TEXT NOT NULL DEFAULT 'sheet_wins',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS expenses (
            id BIGSERIAL PRIMARY KEY,
            amount NUMERIC(14,2) NOT NULL CHECK (amount >= 0),
            category TEXT,
            description TEXT,
            date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_by TEXT
        );

        CREATE TABLE IF NOT EXISTS sales_ledger (
            id BIGSERIAL PRIMARY KEY,
            stock_record_id TEXT,
            stock_row_num INTEGER,
            selling_price NUMERIC(14,2) NOT NULL,
            cost_price_at_sale NUMERIC(14,2) NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
            date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            sold_by TEXT
        );

        CREATE TABLE IF NOT EXISTS returns_ledger (
            id BIGSERIAL PRIMARY KEY,
            sale_id BIGINT REFERENCES sales_ledger(id),
            refund_amount NUMERIC(14,2) NOT NULL CHECK (refund_amount >= 0),
            date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_by TEXT
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id BIGSERIAL PRIMARY KEY,
            action_type TEXT,
            description TEXT,
            user_id TEXT,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stolen_devices (
            id BIGSERIAL PRIMARY KEY,
            phone_name TEXT NOT NULL DEFAULT '',
            imei_raw TEXT NOT NULL,
            imei_digits TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            cleared_at TIMESTAMPTZ,
            cleared_note TEXT NOT NULL DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date);
        CREATE INDEX IF NOT EXISTS idx_expenses_created_by ON expenses(created_by);
        CREATE INDEX IF NOT EXISTS idx_sales_ledger_date ON sales_ledger(date);
        CREATE INDEX IF NOT EXISTS idx_sales_ledger_stock_record_id ON sales_ledger(stock_record_id);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_sales_ledger_stock_record_id_nonempty
            ON sales_ledger(stock_record_id)
            WHERE stock_record_id <> '';
        CREATE INDEX IF NOT EXISTS idx_returns_ledger_sale_id ON returns_ledger(sale_id);
        CREATE INDEX IF NOT EXISTS idx_returns_ledger_date ON returns_ledger(date);
        CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_log_user_id ON audit_log(user_id);
        CREATE INDEX IF NOT EXISTS idx_stolen_devices_imei_digits ON stolen_devices(imei_digits);
        CREATE INDEX IF NOT EXISTS idx_stolen_devices_active ON stolen_devices(is_active);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_stolen_devices_imei_raw_active
            ON stolen_devices(imei_raw)
            WHERE is_active = TRUE;
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
        return True

    def upsert_cache_payload(self, sheet_key, payload_obj):
        if not self.ready:
            return False

        now_iso = datetime.now(timezone.utc).isoformat()
        if isinstance(payload_obj, (list, dict)):
            row_count = len(payload_obj)
        elif payload_obj is None:
            row_count = 0
        else:
            row_count = 1

        sql = """
        INSERT INTO sheet_cache (sheet_key, payload_json, row_count, source, pulled_at, updated_at)
        VALUES (%s, %s, %s, 'sheet', NOW(), NOW())
        ON CONFLICT (sheet_key) DO UPDATE SET
            payload_json = EXCLUDED.payload_json,
            row_count = EXCLUDED.row_count,
            source = 'sheet',
            pulled_at = NOW(),
            updated_at = NOW();
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (sheet_key, Json({'data': payload_obj, 'cached_at': now_iso}), row_count))
        return True

    def upsert_sheet_cache(self, sheet_key, payload):
        rows = payload if isinstance(payload, list) else []
        return self.upsert_cache_payload(sheet_key, rows)

    def load_cache_payload(self, sheet_key):
        if not self.ready:
            return None

        sql = "SELECT payload_json FROM sheet_cache WHERE sheet_key = %s"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (sheet_key,))
                row = cur.fetchone()
        if not row:
            return None

        payload = row[0] or {}
        if isinstance(payload, dict):
            if 'data' in payload:
                return payload.get('data')
            if 'rows' in payload:
                return payload.get('rows')
        return payload

    def load_cached_rows(self, sheet_key):
        rows = self.load_cache_payload(sheet_key)
        return rows if isinstance(rows, list) else []

    def update_cached_main_record_field(self, data_row_index_1based, field_name, value):
        """Mutate cached main_records row field by data-row index (1-based)."""
        if not self.ready:
            return False

        rows = self.load_cached_rows('main_records')
        idx = int(data_row_index_1based or 0) - 1
        if idx < 0 or idx >= len(rows):
            return False

        if not isinstance(rows[idx], dict):
            return False

        rows[idx][str(field_name)] = value
        return self.upsert_sheet_cache('main_records', rows)

    def update_cached_stock_value(self, row_1based, col_1based, value):
        """Mutate cached stock_values cell by absolute row/col (1-based)."""
        return self.update_cached_table_value('stock_values', row_1based, col_1based, value)

    def update_cached_table_value(self, sheet_key, row_1based, col_1based, value):
        if not self.ready:
            return False

        rows = self.load_cached_rows(sheet_key)
        r = int(row_1based or 0) - 1
        c = int(col_1based or 0) - 1
        if r < 0 or c < 0:
            return False

        while len(rows) <= r:
            rows.append([])

        row = rows[r]
        if not isinstance(row, list):
            row = list(row) if row else []
        while len(row) <= c:
            row.append('')

        row[c] = '' if value is None else str(value)
        rows[r] = row
        return self.upsert_sheet_cache(sheet_key, rows)

    def replace_cached_table_row(self, sheet_key, row_1based, row_values):
        if not self.ready:
            return False

        rows = self.load_cached_rows(sheet_key)
        row_index = int(row_1based or 0) - 1
        if row_index < 0:
            return False

        while len(rows) <= row_index:
            rows.append([])

        rows[row_index] = ['' if value is None else str(value) for value in (row_values or [])]
        return self.upsert_sheet_cache(sheet_key, rows)

    def append_cached_table_row(self, sheet_key, row_values):
        if not self.ready:
            return False

        rows = self.load_cached_rows(sheet_key)
        rows.append(['' if value is None else str(value) for value in (row_values or [])])
        return self.upsert_sheet_cache(sheet_key, rows)

    def append_cached_dict_row(self, sheet_key, row_dict):
        if not self.ready:
            return False

        rows = self.load_cached_rows(sheet_key)
        rows.append(dict(row_dict or {}))
        return self.upsert_sheet_cache(sheet_key, rows)

    def set_meta(self, key, value_obj):
        if not self.ready:
            return False

        sql = """
        INSERT INTO app_meta (key, value_json, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (key) DO UPDATE SET
            value_json = EXCLUDED.value_json,
            updated_at = NOW();
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (key, Json(value_obj)))
        return True

    def write_sync_log(self, sync_kind, status, details, started_at):
        if not self.ready:
            return False

        sql = """
        INSERT INTO sync_log (sync_kind, status, details, started_at, finished_at)
        VALUES (%s, %s, %s, %s, NOW())
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (sync_kind, status, details, started_at))
        return True

    def get_sync_snapshot(self):
        if not self.ready:
            return {
                'ready': False,
                'pull_interval_sec': self.pull_interval_sec,
                'cache_counts': {},
                'latest_pull': None,
                'latest_error': None
            }

        snapshot = {
            'ready': True,
            'pull_interval_sec': self.pull_interval_sec,
            'cache_counts': {},
            'latest_pull': None,
            'latest_error': None
        }

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sheet_key, row_count, updated_at FROM sheet_cache ORDER BY sheet_key")
                for sheet_key, row_count, updated_at in cur.fetchall():
                    snapshot['cache_counts'][sheet_key] = {
                        'row_count': int(row_count or 0),
                        'updated_at': str(updated_at)
                    }

                cur.execute(
                    "SELECT status, details, finished_at FROM sync_log WHERE sync_kind='pull' ORDER BY id DESC LIMIT 1"
                )
                last_pull = cur.fetchone()
                if last_pull:
                    snapshot['latest_pull'] = {
                        'status': last_pull[0],
                        'details': last_pull[1],
                        'finished_at': str(last_pull[2])
                    }

                cur.execute(
                    "SELECT details, finished_at FROM sync_log WHERE sync_kind='pull' AND status='error' ORDER BY id DESC LIMIT 1"
                )
                last_error = cur.fetchone()
                if last_error:
                    snapshot['latest_error'] = {
                        'details': last_error[0],
                        'finished_at': str(last_error[1])
                    }

        return snapshot

    def start_background_pull(self, pull_once_callable):
        if not self.ready:
            return False
        if self._thread and self._thread.is_alive():
            return True

        self._stop_event.clear()

        def loop():
            while not self._stop_event.is_set():
                started_at = datetime.now(timezone.utc)
                try:
                    pull_once_callable()
                    self.write_sync_log('pull', 'ok', 'Sheet pull completed', started_at)
                except Exception as exc:
                    self.logger.exception('Background pull failed: %s', exc)
                    try:
                        self.write_sync_log('pull', 'error', str(exc), started_at)
                    except Exception:
                        pass

                self._stop_event.wait(self.pull_interval_sec)

        self._thread = threading.Thread(target=loop, name='postgres-pull-sync', daemon=True)
        self._thread.start()
        return True

    def enqueue_operation(self, entity_name, operation, payload_obj, record_id=None):
        if not self.ready:
            return None

        sql = """
        INSERT INTO sync_queue (entity_name, operation, record_id, payload_json, status, retry_count, updated_at)
        VALUES (%s, %s, %s, %s, 'pending', 0, NOW())
        RETURNING id
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (entity_name, operation, record_id, Json(payload_obj or {})))
                row = cur.fetchone()
        return int(row[0]) if row else None

    def mark_operation_done(self, queue_id):
        if not self.ready or queue_id is None:
            return False
        sql = "UPDATE sync_queue SET status='done', last_error=NULL, updated_at=NOW() WHERE id=%s"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (queue_id,))
        return True

    def mark_operation_failed(self, queue_id, error_text):
        if not self.ready or queue_id is None:
            return False
        sql = """
        UPDATE sync_queue
        SET status='failed', retry_count=retry_count+1, last_error=%s, updated_at=NOW()
        WHERE id=%s
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(error_text or '')[:2000], queue_id))
        return True

    def fetch_pending_operations(self, limit=50, max_retry=20):
        if not self.ready:
            return []
        sql = """
        SELECT id, entity_name, operation, record_id, payload_json, status, retry_count
        FROM sync_queue
        WHERE status IN ('pending', 'failed')
          AND retry_count < %s
        ORDER BY id ASC
        LIMIT %s
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(max_retry), int(limit)))
                rows = cur.fetchall()

        output = []
        for row in rows:
            output.append({
                'id': int(row[0]),
                'entity_name': row[1],
                'operation': row[2],
                'record_id': row[3],
                'payload_json': row[4] or {},
                'status': row[5],
                'retry_count': int(row[6] or 0)
            })
        return output

    def start_background_queue_worker(self, process_item_callable, interval_sec=3):
        if not self.ready:
            return False
        if self._queue_thread and self._queue_thread.is_alive():
            return True

        idle_wait_sec = max(1, int(interval_sec or 3))

        def loop():
            while not self._stop_event.is_set():
                try:
                    items = self.fetch_pending_operations(limit=100)
                    if items:
                        for item in items:
                            try:
                                process_item_callable(item)
                                self.mark_operation_done(item['id'])
                            except Exception as item_exc:
                                self.mark_operation_failed(item['id'], str(item_exc))
                        # If we got a full batch, loop immediately to drain remaining items
                        if len(items) >= 100:
                            continue
                    else:
                        # Nothing pending — wait before checking again
                        self._stop_event.wait(idle_wait_sec)
                except Exception as exc:
                    self.logger.warning('Queue worker cycle failed: %s', exc)
                    self._stop_event.wait(idle_wait_sec)

        self._queue_thread = threading.Thread(target=loop, name='postgres-queue-sync', daemon=True)
        self._queue_thread.start()
        return True

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.5)
        if self._queue_thread and self._queue_thread.is_alive():
            self._queue_thread.join(timeout=1.5)


def create_postgres_sync_manager(config, logger=None):
    dsn = str(config.get('postgres_dsn', '')).strip()
    interval = int(config.get('sync_pull_interval_sec', 90) or 90)
    manager = PostgresSyncManager(dsn=dsn, pull_interval_sec=interval, logger=logger)
    return manager
