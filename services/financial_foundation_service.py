from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json


class FinancialFoundationService:
    """CRUD service layer for financial foundation tables in PostgreSQL."""

    def __init__(self, postgres_sync_manager=None, logger=None):
        self._manager = postgres_sync_manager
        self.logger = logger

    def configure(self, postgres_sync_manager):
        self._manager = postgres_sync_manager

    @property
    def ready(self):
        return bool(self._manager and self._manager.ready)

    def _require_manager(self):
        if not self.ready:
            raise RuntimeError('Financial foundation storage is not ready.')
        return self._manager

    @staticmethod
    def _normalize_timestamp(value):
        if value in (None, ''):
            return datetime.now(timezone.utc)
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return datetime.now(timezone.utc)
        if text.endswith('Z'):
            text = text[:-1] + '+00:00'
        return datetime.fromisoformat(text)

    @staticmethod
    def _serialize_value(value):
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _serialize_row(self, row):
        if row is None:
            return None
        return {key: self._serialize_value(value) for key, value in row.items()}

    def _serialize_rows(self, rows):
        return [self._serialize_row(row) for row in (rows or [])]

    @staticmethod
    def _to_number(value, fallback=0.0):
        if isinstance(value, Decimal):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    @staticmethod
    def _to_number_with_status(value, fallback=0.0):
        if isinstance(value, Decimal):
            return float(value), False
        try:
            return float(value), False
        except (TypeError, ValueError):
            return float(fallback), True

    def _config_value(self, key, default=None):
        manager = self._require_manager()
        try:
            row = manager.fetchone_dict(
                "SELECT value FROM app_config WHERE key = %s",
                (str(key or '').strip(),),
            )
        except Exception as exc:
            if self.logger:
                self.logger.warning('Failed to read app_config key %s: %s', key, exc)
            return default
        if not row:
            if self.logger:
                self.logger.warning('app_config key %s is missing; using fallback value=%r', key, default)
            return default
        value = row.get('value', default)
        if value is None:
            if self.logger:
                self.logger.warning('app_config key %s is NULL; using fallback value=%r', key, default)
            return default
        return value

    def _safe_json_parse(self, key, value):
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return value
            try:
                return json.loads(text)
            except Exception as exc:
                if self.logger:
                    self.logger.warning(
                        'app_config key %s contains invalid JSON string %r; falling back to raw parsing (%s)',
                        key,
                        value,
                        exc,
                    )
                return value
        return value

    def _extract_numeric_config(self, key, raw_value, default=0.0):
        value = self._safe_json_parse(key, raw_value)
        if isinstance(value, dict):
            for field_name in ('value', 'percentage', 'amount'):
                if field_name in value:
                    return self._extract_numeric_config(key, value.get(field_name), default=default)
            return float(default), True
        if isinstance(value, list):
            if not value:
                return float(default), True
            return self._extract_numeric_config(key, value[0], default=default)
        return self._to_number_with_status(value, fallback=default)

    def _read_numeric_config(self, key, default=0.0):
        raw_value = self._config_value(key, default)
        numeric_value, used_fallback = self._extract_numeric_config(key, raw_value, default=default)
        if used_fallback and self.logger:
            self.logger.warning(
                'app_config key %s used fallback numeric value=%r for raw=%r',
                key,
                default,
                raw_value,
            )
        return numeric_value

    def ensure_default_app_config(self):
        manager = self._require_manager()
        defaults = {
            'allowance_percentage': 0.25,
            'reserve_percentage': 0.3,
        }
        for key, value in defaults.items():
            manager.execute(
                """
                INSERT INTO app_config (key, value)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO NOTHING
                """,
                (str(key), json.dumps(value)),
            )

    def _normalized_reserve_percentage(self):
        reserve = self._read_numeric_config('reserve_percentage', default=0.0)
        # Allow both 0.15 and 15 style inputs.
        if reserve > 1.0 and reserve <= 100.0:
            reserve = reserve / 100.0
        reserve = max(0.0, min(reserve, 1.0))
        return reserve

    def _normalized_allowance_percentage(self):
        allowance = self._read_numeric_config('allowance_percentage', default=0.25)
        # Allow both 0.25 and 25 style inputs.
        if allowance > 1.0 and allowance <= 100.0:
            allowance = allowance / 100.0
        allowance = max(0.0, min(allowance, 1.0))
        return allowance

    @staticmethod
    def _most_recent_saturday(today=None):
        today = today or date.today()
        # Python weekday: Monday=0 ... Saturday=5 ... Sunday=6
        days_since_saturday = (today.weekday() - 5) % 7
        return today - timedelta(days=days_since_saturday)

    @staticmethod
    def _build_update_clause(allowed_fields, updates):
        clauses = []
        values = []
        for field in allowed_fields:
            if field in updates:
                clauses.append(f"{field} = %s")
                values.append(updates[field])
        return clauses, values

    @staticmethod
    def _require_admin(actor_role):
        role = str(actor_role or '').strip().lower()
        if role and role != 'admin':
            raise PermissionError('Admin role is required for this operation.')

    def create_expense(self, amount, category='', description='', date=None, created_by=''):
        manager = self._require_manager()
        sql = """
        INSERT INTO expenses (amount, category, description, date, created_by)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, amount, category, description, date, created_by
        """
        row = manager.fetchone_dict(
            sql,
            (
                amount,
                str(category or '').strip(),
                str(description or '').strip(),
                self._normalize_timestamp(date),
                str(created_by or '').strip(),
            ),
        )
        return self._serialize_row(row)

    def get_expense(self, expense_id):
        manager = self._require_manager()
        row = manager.fetchone_dict(
            """
            SELECT id, amount, category, description, date, created_by
            FROM expenses
            WHERE id = %s
            """,
            (int(expense_id),),
        )
        return self._serialize_row(row)

    def list_expenses(self, limit=200, offset=0):
        manager = self._require_manager()
        rows = manager.fetchall_dict(
            """
            SELECT id, amount, category, description, date, created_by
            FROM expenses
            ORDER BY date DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            (max(1, int(limit or 200)), max(0, int(offset or 0))),
        )
        return self._serialize_rows(rows)

    def update_expense(self, expense_id, updates):
        manager = self._require_manager()
        updates = dict(updates or {})
        if 'date' in updates:
            updates['date'] = self._normalize_timestamp(updates.get('date'))

        fields = ['amount', 'category', 'description', 'date', 'created_by']
        clauses, values = self._build_update_clause(fields, updates)
        if not clauses:
            return self.get_expense(expense_id)

        values.append(int(expense_id))
        row = manager.fetchone_dict(
            f"""
            UPDATE expenses
            SET {', '.join(clauses)}
            WHERE id = %s
            RETURNING id, amount, category, description, date, created_by
            """,
            tuple(values),
        )
        return self._serialize_row(row)

    def delete_expense(self, expense_id):
        manager = self._require_manager()
        deleted = manager.fetchone(
            "DELETE FROM expenses WHERE id = %s RETURNING id",
            (int(expense_id),),
        )
        return bool(deleted)

    def create_sale_ledger_entry(
        self,
        stock_record_id='',
        stock_row_num=None,
        selling_price=0,
        cost_price_at_sale=0,
        quantity=1,
        date=None,
        sold_by='',
    ):
        manager = self._require_manager()
        row = manager.fetchone_dict(
            """
            INSERT INTO sales_ledger (
                stock_record_id,
                stock_row_num,
                selling_price,
                cost_price_at_sale,
                quantity,
                date,
                sold_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, stock_record_id, stock_row_num, selling_price, cost_price_at_sale, quantity, date, sold_by
            """,
            (
                str(stock_record_id or '').strip(),
                int(stock_row_num) if stock_row_num is not None else None,
                selling_price,
                cost_price_at_sale,
                max(1, int(quantity or 1)),
                self._normalize_timestamp(date),
                str(sold_by or '').strip(),
            ),
        )
        return self._serialize_row(row)

    def get_sale_ledger_entry(self, sale_id):
        manager = self._require_manager()
        row = manager.fetchone_dict(
            """
            SELECT id, stock_record_id, stock_row_num, selling_price, cost_price_at_sale, quantity, date, sold_by
            FROM sales_ledger
            WHERE id = %s
            """,
            (int(sale_id),),
        )
        return self._serialize_row(row)

    def list_sales_ledger_entries(self, limit=200, offset=0):
        manager = self._require_manager()
        rows = manager.fetchall_dict(
            """
            SELECT id, stock_record_id, stock_row_num, selling_price, cost_price_at_sale, quantity, date, sold_by
            FROM sales_ledger
            ORDER BY date DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            (max(1, int(limit or 200)), max(0, int(offset or 0))),
        )
        return self._serialize_rows(rows)

    def update_sale_ledger_entry(self, sale_id, updates):
        manager = self._require_manager()
        updates = dict(updates or {})
        if 'date' in updates:
            updates['date'] = self._normalize_timestamp(updates.get('date'))
        if 'stock_row_num' in updates and updates.get('stock_row_num') is not None:
            updates['stock_row_num'] = int(updates.get('stock_row_num'))
        if 'quantity' in updates:
            updates['quantity'] = max(1, int(updates.get('quantity') or 1))

        fields = [
            'stock_record_id',
            'stock_row_num',
            'selling_price',
            'cost_price_at_sale',
            'quantity',
            'date',
            'sold_by',
        ]
        clauses, values = self._build_update_clause(fields, updates)
        if not clauses:
            return self.get_sale_ledger_entry(sale_id)

        values.append(int(sale_id))
        row = manager.fetchone_dict(
            f"""
            UPDATE sales_ledger
            SET {', '.join(clauses)}
            WHERE id = %s
            RETURNING id, stock_record_id, stock_row_num, selling_price, cost_price_at_sale, quantity, date, sold_by
            """,
            tuple(values),
        )
        return self._serialize_row(row)

    def delete_sale_ledger_entry(self, sale_id):
        manager = self._require_manager()
        deleted = manager.fetchone(
            "DELETE FROM sales_ledger WHERE id = %s RETURNING id",
            (int(sale_id),),
        )
        return bool(deleted)

    def create_return_ledger_entry(self, sale_id=None, refund_amount=0, date=None, processed_by=''):
        manager = self._require_manager()
        row = manager.fetchone_dict(
            """
            INSERT INTO returns_ledger (sale_id, refund_amount, date, processed_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, sale_id, refund_amount, date, processed_by
            """,
            (
                int(sale_id) if sale_id is not None else None,
                refund_amount,
                self._normalize_timestamp(date),
                str(processed_by or '').strip(),
            ),
        )
        return self._serialize_row(row)

    def get_return_ledger_entry(self, return_id):
        manager = self._require_manager()
        row = manager.fetchone_dict(
            """
            SELECT id, sale_id, refund_amount, date, processed_by
            FROM returns_ledger
            WHERE id = %s
            """,
            (int(return_id),),
        )
        return self._serialize_row(row)

    def list_return_ledger_entries(self, limit=200, offset=0):
        manager = self._require_manager()
        rows = manager.fetchall_dict(
            """
            SELECT id, sale_id, refund_amount, date, processed_by
            FROM returns_ledger
            ORDER BY date DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            (max(1, int(limit or 200)), max(0, int(offset or 0))),
        )
        return self._serialize_rows(rows)

    def update_return_ledger_entry(self, return_id, updates):
        manager = self._require_manager()
        updates = dict(updates or {})
        if 'date' in updates:
            updates['date'] = self._normalize_timestamp(updates.get('date'))
        if 'sale_id' in updates and updates.get('sale_id') is not None:
            updates['sale_id'] = int(updates.get('sale_id'))

        fields = ['sale_id', 'refund_amount', 'date', 'processed_by']
        clauses, values = self._build_update_clause(fields, updates)
        if not clauses:
            return self.get_return_ledger_entry(return_id)

        values.append(int(return_id))
        row = manager.fetchone_dict(
            f"""
            UPDATE returns_ledger
            SET {', '.join(clauses)}
            WHERE id = %s
            RETURNING id, sale_id, refund_amount, date, processed_by
            """,
            tuple(values),
        )
        return self._serialize_row(row)

    def delete_return_ledger_entry(self, return_id):
        manager = self._require_manager()
        deleted = manager.fetchone(
            "DELETE FROM returns_ledger WHERE id = %s RETURNING id",
            (int(return_id),),
        )
        return bool(deleted)

    def create_audit_log(self, action_type='', description='', user_id='', timestamp=None):
        manager = self._require_manager()
        row = manager.fetchone_dict(
            """
            INSERT INTO audit_log (action_type, description, user_id, timestamp)
            VALUES (%s, %s, %s, %s)
            RETURNING id, action_type, description, user_id, timestamp
            """,
            (
                str(action_type or '').strip(),
                str(description or '').strip(),
                str(user_id or '').strip(),
                self._normalize_timestamp(timestamp),
            ),
        )
        return self._serialize_row(row)

    def get_audit_log(self, audit_id, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        row = manager.fetchone_dict(
            """
            SELECT id, action_type, description, user_id, timestamp
            FROM audit_log
            WHERE id = %s
            """,
            (int(audit_id),),
        )
        return self._serialize_row(row)

    def list_audit_logs(self, limit=200, offset=0, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        rows = manager.fetchall_dict(
            """
            SELECT id, action_type, description, user_id, timestamp
            FROM audit_log
            ORDER BY timestamp DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            (max(1, int(limit or 200)), max(0, int(offset or 0))),
        )
        return self._serialize_rows(rows)

    def update_audit_log(self, audit_id, updates, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        updates = dict(updates or {})
        if 'timestamp' in updates:
            updates['timestamp'] = self._normalize_timestamp(updates.get('timestamp'))

        fields = ['action_type', 'description', 'user_id', 'timestamp']
        clauses, values = self._build_update_clause(fields, updates)
        if not clauses:
            return self.get_audit_log(audit_id, actor_role='admin')

        values.append(int(audit_id))
        row = manager.fetchone_dict(
            f"""
            UPDATE audit_log
            SET {', '.join(clauses)}
            WHERE id = %s
            RETURNING id, action_type, description, user_id, timestamp
            """,
            tuple(values),
        )
        return self._serialize_row(row)

    def delete_audit_log(self, audit_id, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        deleted = manager.fetchone(
            "DELETE FROM audit_log WHERE id = %s RETURNING id",
            (int(audit_id),),
        )
        return bool(deleted)

    def set_app_config(self, key, value, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        payload = json.dumps(value)
        row = manager.fetchone_dict(
            """
            INSERT INTO app_config (key, value)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            RETURNING key, value
            """,
            (str(key or '').strip(), payload),
        )
        return self._serialize_row(row)

    def get_app_config(self, key, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        row = manager.fetchone_dict(
            "SELECT key, value FROM app_config WHERE key = %s",
            (str(key or '').strip(),),
        )
        return self._serialize_row(row)

    def list_app_config(self, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        rows = manager.fetchall_dict(
            "SELECT key, value FROM app_config ORDER BY key ASC"
        )
        return self._serialize_rows(rows)

    def delete_app_config(self, key, actor_role=None):
        self._require_admin(actor_role)
        manager = self._require_manager()
        deleted = manager.fetchone(
            "DELETE FROM app_config WHERE key = %s RETURNING key",
            (str(key or '').strip(),),
        )
        return bool(deleted)

    def log_sale_action(self, sold_by='', items_count=0, total_amount=0.0, description=''):
        """Log a sale action to the audit log.
        
        Args:
            sold_by: Username of the person who made the sale
            items_count: Number of items sold
            total_amount: Total sale amount
            description: Additional description
        """
        action_desc = f"Sale by {sold_by}: {items_count} item(s), amount: {total_amount:.2f}. {description}"
        return self.create_audit_log(
            action_type='SALE',
            description=action_desc,
            user_id=sold_by,
        )

    def log_edit_action(self, edited_by='', item_type='', item_id='', field_name='', description=''):
        """Log an edit action to the audit log.
        
        Args:
            edited_by: Username of the person who made the edit
            item_type: Type of item edited (e.g., 'stock', 'inventory', 'expense')
            item_id: ID of the item that was edited
            field_name: Name of the field that was changed
            description: Additional description of the change
        """
        action_desc = f"{item_type} edit: {item_id}, field: {field_name}. {description}"
        return self.create_audit_log(
            action_type=f'EDIT_{item_type.upper()}',
            description=action_desc,
            user_id=edited_by,
        )

    def get_cashflow_summary(self, actor_role=None, expense_total_override=None):
        self._require_admin(actor_role)
        manager = self._require_manager()

        sales_row = manager.fetchone_dict(
            """
            SELECT
                COALESCE(SUM(selling_price * quantity), 0) AS total_cash_in,
                COALESCE(SUM(cost_price_at_sale * quantity), 0) AS total_cost
            FROM sales_ledger
            """
        ) or {}
        expense_row = manager.fetchone_dict(
            "SELECT COALESCE(SUM(amount), 0) AS total_expenses FROM expenses"
        ) or {}

        total_cash_in = self._to_number(sales_row.get('total_cash_in'))
        total_cost = self._to_number(sales_row.get('total_cost'))
        database_expenses_total = self._to_number(expense_row.get('total_expenses'))
        sheet_expenses_total = None if expense_total_override is None else self._to_number(expense_total_override)
        total_expenses = database_expenses_total if sheet_expenses_total is None else sheet_expenses_total
        net_profit = total_cash_in - total_cost - total_expenses

        # Receivables are explicitly excluded from available cash.
        receivables_excluded = max(0.0, self._read_numeric_config('receivables_amount', default=0.0))

        reserve_percentage = self._normalized_reserve_percentage()
        available_cash_before_reserve = total_cash_in - total_expenses - receivables_excluded
        reserve_amount = max(0.0, available_cash_before_reserve) * reserve_percentage
        available_cash_after_reserve = available_cash_before_reserve - reserve_amount

        return {
            'total_cash_in': total_cash_in,
            'total_expenses': total_expenses,
            'database_expenses_total': database_expenses_total,
            'sheet_expenses_total': 0.0 if sheet_expenses_total is None else sheet_expenses_total,
            'expense_source': 'database' if sheet_expenses_total is None else 'sheet',
            'total_cost': total_cost,
            'net_profit': net_profit,
            'receivables_excluded': receivables_excluded,
            'reserve_percentage': reserve_percentage,
            'reserve_amount': reserve_amount,
            'available_cash': available_cash_after_reserve,
            'available_cash_before_reserve': available_cash_before_reserve,
        }

    def _get_week_profit(self, week_start, week_end):
        """Calculate total profit for a given week: selling_price * qty - cost * qty.
        
        Args:
            week_start: datetime or date for start of week (inclusive)
            week_end: datetime or date for end of week (exclusive)
            
        Returns:
            float: Total profit for the week
        """
        manager = self._require_manager()
        
        profit_row = manager.fetchone_dict(
            """
            SELECT COALESCE(
                SUM((selling_price - cost_price_at_sale) * quantity), 
                0
            ) AS total_profit
            FROM sales_ledger
            WHERE date >= %s AND date < %s
            """,
            (week_start, week_end)
        ) or {}
        
        return self._to_number(profit_row.get('total_profit', 0.0))

    def get_weekly_allowance_summary(self, actor_role=None):
        self._require_admin(actor_role)

        allowance_percentage = self._normalized_allowance_percentage()
        
        # Calculate previous week's profit
        # Current week ends on most_recent_saturday
        current_week_end = self._most_recent_saturday()
        
        # Previous week: starts 14 days before current_week_end, ends 7 days before
        previous_week_end = current_week_end - timedelta(days=7)
        previous_week_start = current_week_end - timedelta(days=14)
        
        # Get profit for previous week
        previous_week_profit = self._get_week_profit(previous_week_start, previous_week_end)
        
        # Suggested allowance is configured percentage of previous week's profit
        suggested_allowance = round(max(0.0, previous_week_profit) * allowance_percentage, 2)
        calculation_date = current_week_end.isoformat()

        return {
            'suggested_allowance': suggested_allowance,
            'calculation_date': calculation_date,
            'previous_week_profit': round(previous_week_profit, 2),
        }
