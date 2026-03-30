import sqlite3
from datetime import date, timedelta
from utils.market_hours import utc_today

DB_PATH = 'database/trading_saas.db'

_SUBSCRIBERS_CANONICAL_COLUMNS = [
    # identity
    ("chat_id", "TEXT PRIMARY KEY"),
    # broker creds / account
    ("email", "TEXT"),
    ("api_password", "TEXT"),
    ("api_key", "TEXT"),
    ("is_demo", "INTEGER DEFAULT 1"),
    # risk controls
    ("risk_percent", "REAL DEFAULT 1.5"),
    ("max_risk_percent", "REAL DEFAULT 2.0"),
    # status flags
    ("is_active", "INTEGER DEFAULT 1"),
    ("kill_switch", "INTEGER DEFAULT 0"),
    ("trading_enabled", "INTEGER DEFAULT 0"),
    # subscription
    ("expiry_date", "TEXT"),
    ("license_key", "TEXT"),
    ("subscription_started_at", "TEXT"),
    # profile / UX
    ("lang", "TEXT DEFAULT 'ar'"),
    ("mode", "TEXT DEFAULT 'AUTO'"),
    ("signal_profile", "TEXT DEFAULT 'FAST'"),
    ("first_name", "TEXT"),
    ("last_name", "TEXT"),
    ("phone", "TEXT"),
    ("payment_proof", "TEXT"),
    ("payment_status", "TEXT DEFAULT 'NONE'"),
    ("preferred_leverage", "INTEGER"),
    # monitoring / heartbeat
    ("last_bot_activity_at", "TEXT"),
    ("last_engine_activity_at", "TEXT"),
]


def _table_columns(cursor: sqlite3.Cursor, table: str) -> list[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in (cursor.fetchall() or [])]


def _rebuild_subscribers_table_without_tier(conn: sqlite3.Connection):
    """
    SQLite cannot DROP COLUMN; we rebuild `subscribers` with canonical columns.
    This physically removes any legacy `tier` column (and keeps all other data).
    """
    c = conn.cursor()
    cols = _table_columns(c, "subscribers")
    if not cols or "tier" not in cols:
        return

    canonical_names = [c0 for (c0, _) in _SUBSCRIBERS_CANONICAL_COLUMNS]
    present = [name for name in canonical_names if name in cols]

    # Build new table
    ddl_cols = ",\n                  ".join([f"{n} {d}" for (n, d) in _SUBSCRIBERS_CANONICAL_COLUMNS])
    c.execute("ALTER TABLE subscribers RENAME TO subscribers__old")
    c.execute(f"""CREATE TABLE subscribers
                 ({ddl_cols})""")

    # Copy intersection columns from old -> new
    col_list = ", ".join(present)
    c.execute(
        f"INSERT INTO subscribers ({col_list}) SELECT {col_list} FROM subscribers__old"
    )

    # Drop old table
    c.execute("DROP TABLE subscribers__old")


def create_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # ── Subscribers ───────────────────────────────────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS subscribers
                 (chat_id          TEXT PRIMARY KEY,
                  email            TEXT,
                  api_password     TEXT,
                  api_key          TEXT,
                  is_demo          INTEGER DEFAULT 1,
                  risk_percent     REAL    DEFAULT 1.5,
                  max_risk_percent REAL    DEFAULT 2.0,
                  is_active        INTEGER DEFAULT 1,
                  kill_switch      INTEGER DEFAULT 0,
                  expiry_date      TEXT,
                  license_key      TEXT,
                  lang             TEXT    DEFAULT 'ar',
                  mode             TEXT    DEFAULT 'AUTO',
                  signal_profile   TEXT    DEFAULT 'FAST',
                  first_name       TEXT,
                  last_name        TEXT,
                  phone            TEXT,
                  payment_proof    TEXT,
                  payment_status   TEXT    DEFAULT 'NONE',
                  trading_enabled  INTEGER DEFAULT 0)''')

    # Migrate existing subscribers table: add new columns if absent
    for col, definition in [
        ('max_risk_percent', 'REAL DEFAULT 2.0'),
        ('kill_switch',      'INTEGER DEFAULT 0'),
        ('first_name',       'TEXT'),
        ('last_name',        'TEXT'),
        ('phone',            'TEXT'),
        ('payment_proof',    'TEXT'),
        ('payment_status',   "TEXT DEFAULT 'NONE'"),
        ('trading_enabled',  'INTEGER DEFAULT 0'),
        ('subscription_started_at', "TEXT"),
        ('preferred_leverage', 'INTEGER'),
        ('signal_profile', "TEXT DEFAULT 'FAST'"),
        ('last_bot_activity_at', "TEXT"),
        ('last_engine_activity_at', "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE subscribers ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # One-plan system migration:
    # Ensure legacy rows remain valid under the single-plan framework.
    try:
        c.execute("UPDATE subscribers SET is_active=1 WHERE is_active IS NULL")
    except Exception:
        pass
    try:
        c.execute(
            "UPDATE subscribers SET signal_profile='FAST' "
            "WHERE signal_profile IS NULL OR TRIM(signal_profile)=''"
        )
    except Exception:
        pass

    # Physically remove legacy `tier` column if it exists.
    try:
        _rebuild_subscribers_table_without_tier(conn)
    except Exception:
        # Never fail boot on migration; engine/bot must still come up.
        pass

    # ── Trade log ─────────────────────────────────────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (trade_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                  chat_id       TEXT,
                  symbol        TEXT,
                  direction     TEXT,
                  entry_price   REAL,
                  size          REAL,
                  deal_id       TEXT,
                  trailing_stop REAL,
                  pnl           REAL,
                  status        TEXT DEFAULT 'OPEN',
                  opened_at     TEXT,
                  closed_at     TEXT)''')

    for col, definition in [
        ('closed_at', 'TEXT'),
        ('opened_at', 'TEXT'),
        ('leg_role', "TEXT"),
        ('parent_session', "TEXT"),
        ('stop_distance', "REAL"),
        # Capital order reference (useful for matching history after close).
        ('deal_reference', "TEXT"),
        # UX: if a position is closed manually on the platform, broker history can lag.
        # We send one "pending sync" notification and then wait for realized P&L.
        ('close_sync_notified', "INTEGER DEFAULT 0"),
        # Sync lifecycle diagnostics for externally closed trades.
        ('close_sync_attempts', "INTEGER DEFAULT 0"),
        ('close_sync_last_try_at', "TEXT"),
        ('close_sync_last_error', "TEXT"),
        ('sync_status', "TEXT"),
        # Final broker-truth fields (synced from Capital.com history after close).
        # `pnl` remains for backward compatibility (reports/analytics) and is set to `actual_pnl`.
        ('actual_pnl', 'REAL'),
        ('exit_price', 'REAL'),
        ('target_reached', "TEXT"),
        ('close_reason', "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE trades ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # ── Session aggregates (TP1+TP2 = one risk/report outcome) ────────────────
    c.execute(
        '''CREATE TABLE IF NOT EXISTS trade_sessions
           (session_id   TEXT PRIMARY KEY,
            chat_id      TEXT NOT NULL,
            symbol       TEXT,
            direction    TEXT,
            opened_at    TEXT,
            closed_at    TEXT,
            total_pnl    REAL,
            outcome      TEXT,
            tp1_hit      INTEGER DEFAULT 0,
            tp2_hit      INTEGER DEFAULT 0,
            leg_count    INTEGER DEFAULT 0)'''
    )

    # ── Per-user per-day risk state ───────────────────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS daily_risk_state
                 (chat_id            TEXT PRIMARY KEY,
                  date               TEXT,
                  consecutive_losses INTEGER DEFAULT 0,
                  state              TEXT    DEFAULT 'NORMAL')''')

    # ── Hybrid mode pending signals ───────────────────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS pending_signals
                 (signal_id  INTEGER PRIMARY KEY AUTOINCREMENT,
                  chat_id    TEXT,
                  symbol     TEXT,
                  action     TEXT,
                  confidence REAL,
                  reason     TEXT,
                  strategy_label TEXT DEFAULT '',
                  stop_loss_pct  REAL,
                  status     TEXT DEFAULT 'PENDING',
                  created_at TEXT)''')

    # Migration for existing DBs (add new columns if absent)
    for col, definition in [
        ('strategy_label', "TEXT DEFAULT ''"),
        ('stop_loss_pct',  'REAL'),
    ]:
        try:
            c.execute(f"ALTER TABLE pending_signals ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # ── Expected rejection audit log (non-user-facing) ───────────────────────
    c.execute(
        '''CREATE TABLE IF NOT EXISTS trade_rejections
           (id         INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            chat_id    TEXT,
            symbol     TEXT,
            action     TEXT,
            stage      TEXT,
            reason     TEXT,
            details    TEXT)'''
    )

    # ── Pending limit orders (Sprint 2) ─────────────────────────────────────
    c.execute(
        '''CREATE TABLE IF NOT EXISTS pending_limit_orders
           (id             INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at     TEXT,
            expires_at     TEXT,
            chat_id        TEXT,
            symbol         TEXT,
            action         TEXT,
            strategy_label TEXT,
            confidence     REAL,
            stop_loss_pct  REAL,
            limit_price    REAL,
            status         TEXT DEFAULT 'PENDING',
            reason         TEXT,
            last_error     TEXT,
            triggered_at   TEXT,
            cancelled_at   TEXT)'''
    )

    # Migration for existing DBs (add new columns if absent)
    for col, definition in [
        ('ai_prob', 'REAL'),
    ]:
        try:
            c.execute(f"ALTER TABLE pending_limit_orders ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # ── Global system settings (key-value store) ──────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS system_settings
                 (key   TEXT PRIMARY KEY,
                  value TEXT)''')

    # Initialise control flags and bank details
    for key, default in [
        ('MAINTENANCE_MODE',     'false'),
        ('MASTER_KILL_SWITCH',   'false'),
        ('BANK_NAME',            'Abu Dhabi Commercial Bank PJSC (ADCB)'),
        ('BANK_ACCOUNT_NAME',    'SAMER I M HAMAD'),
        ('BANK_ACCOUNT_NUMBER',  '13265729810001'),
        ('BANK_IBAN',            'AE040030013265729810001'),
        ('BANK_SWIFT',           'ADCBAEAA'),
        ('BANK_CURRENCY',        'AED'),
        ('BANK_BRANCH',          'IBD - KHALDIYA TOWER BRANCH'),
        ('BANK_INSTRUCTIONS',    'Please include your Telegram username in the payment reference.'),
    ]:
        c.execute(
            "INSERT OR IGNORE INTO system_settings (key, value) VALUES (?, ?)",
            (key, default),
        )

    # Migrate existing rows to ADCB details
    for key, value in [
        ('BANK_NAME',           'Abu Dhabi Commercial Bank PJSC (ADCB)'),
        ('BANK_ACCOUNT_NAME',   'SAMER I M HAMAD'),
        ('BANK_ACCOUNT_NUMBER', '13265729810001'),
        ('BANK_IBAN',           'AE040030013265729810001'),
        ('BANK_SWIFT',          'ADCBAEAA'),
        ('BANK_CURRENCY',       'AED'),
        ('BANK_BRANCH',         'IBD - KHALDIYA TOWER BRANCH'),
    ]:
        c.execute(
            "INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)",
            (key, value),
        )

    conn.commit()
    conn.close()
    print("DB ready.")


# ── Generic settings helper ───────────────────────────────────────────────────

def _get_setting(key: str) -> str:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("SELECT value FROM system_settings WHERE key=?", (key,))
    row  = c.fetchone()
    conn.close()
    return row[0] if row else ''


def _set_setting(key: str, value: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()
    conn.close()


# ── Maintenance mode ──────────────────────────────────────────────────────────

def is_maintenance_mode() -> bool:
    return _get_setting('MAINTENANCE_MODE') == 'true'


def set_maintenance_mode(active: bool):
    _set_setting('MAINTENANCE_MODE', 'true' if active else 'false')


# ── Master kill switch ────────────────────────────────────────────────────────

def is_master_kill_switch() -> bool:
    return _get_setting('MASTER_KILL_SWITCH') == 'true'


def set_master_kill_switch(active: bool):
    _set_setting('MASTER_KILL_SWITCH', 'true' if active else 'false')


# ── Per-user kill switch ──────────────────────────────────────────────────────

def get_user_kill_switch(chat_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("SELECT kill_switch FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row  = c.fetchone()
    conn.close()
    return bool(row[0]) if row else False


def set_user_kill_switch(chat_id: str, active: bool):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET kill_switch=? WHERE chat_id=?",
        (1 if active else 0, str(chat_id)),
    )
    conn.commit()
    conn.close()


# ── Per-user risk settings ────────────────────────────────────────────────────

def get_user_risk_params(chat_id: str) -> tuple:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT risk_percent, max_risk_percent FROM subscribers WHERE chat_id=?",
        (str(chat_id),),
    )
    row  = c.fetchone()
    conn.close()
    if row and row[0] and row[1]:
        return float(row[0]), float(row[1])
    return 1.0, 2.0


# ── Bank details ──────────────────────────────────────────────────────────────

BANK_FIELDS = ['BANK_NAME', 'BANK_ACCOUNT_NAME', 'BANK_ACCOUNT_NUMBER',
               'BANK_IBAN', 'BANK_SWIFT', 'BANK_CURRENCY',
               'BANK_BRANCH', 'BANK_INSTRUCTIONS']


def get_bank_details() -> dict:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT key, value FROM system_settings WHERE key IN ({})".format(
            ','.join('?' * len(BANK_FIELDS))
        ),
        BANK_FIELDS,
    )
    rows = c.fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}


def set_bank_field(field: str, value: str) -> bool:
    if field.upper() not in BANK_FIELDS:
        return False
    _set_setting(field.upper(), value)
    return True


# ── Trading engine on/off per subscriber ──────────────────────────────────────

def get_subscriber_lang(chat_id: str) -> str:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("SELECT lang FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row  = c.fetchone()
    conn.close()
    return (row[0] if row and row[0] else 'ar')


def get_trading_enabled(chat_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("SELECT trading_enabled FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row  = c.fetchone()
    conn.close()
    return bool(row[0]) if row and row[0] is not None else False


def set_trading_enabled(chat_id: str, enabled: bool):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET trading_enabled=? WHERE chat_id=?",
        (1 if enabled else 0, str(chat_id))
    )
    conn.commit()
    conn.close()


def get_user_signal_profile(chat_id: str) -> str:
    """Return per-user signal profile: FAST or GOLDEN."""
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("SELECT signal_profile FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row  = c.fetchone()
    conn.close()
    profile = str(row[0]).strip().upper() if row and row[0] else "FAST"
    return "GOLDEN" if profile == "GOLDEN" else "FAST"


def set_user_signal_profile(chat_id: str, profile: str):
    """Persist per-user signal profile with FAST fallback."""
    val = str(profile or "FAST").strip().upper()
    if val not in ("FAST", "GOLDEN"):
        val = "FAST"
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET signal_profile=? WHERE chat_id=?",
        (val, str(chat_id)),
    )
    conn.commit()
    conn.close()


def _utc_now_iso() -> str:
    """UTC ISO timestamp without microseconds (stable for SQLite text)."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def touch_bot_activity(chat_id: str):
    """Record latest bot interaction time for subscriber."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET last_bot_activity_at=? WHERE chat_id=?",
        (_utc_now_iso(), str(chat_id)),
    )
    conn.commit()
    conn.close()


def touch_engine_activity(chat_id: str):
    """Record latest engine activity time for subscriber."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET last_engine_activity_at=? WHERE chat_id=?",
        (_utc_now_iso(), str(chat_id)),
    )
    conn.commit()
    conn.close()


# ── Subscription period / leverage preferences ────────────────────────────────

SUBSCRIPTION_DAYS_DEFAULT = 30


def get_subscription_started_at(chat_id: str) -> str | None:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("SELECT subscription_started_at FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row  = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def set_subscription_started_today(chat_id: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET subscription_started_at=? WHERE chat_id=?",
        (str(utc_today()), str(chat_id)),
    )
    conn.commit()
    conn.close()


def infer_subscription_start(chat_id: str) -> date | None:
    """First day of current period: stored value, or expiry minus plan length."""
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT subscription_started_at, expiry_date FROM subscribers WHERE chat_id=?",
        (str(chat_id),),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    started, expiry = row[0], row[1]
    if started:
        try:
            return date.fromisoformat(started)
        except ValueError:
            pass
    if expiry:
        try:
            d = int(SUBSCRIPTION_DAYS_DEFAULT)
            return date.fromisoformat(expiry) - timedelta(days=d)
        except ValueError:
            pass
    return None


def get_preferred_leverage(chat_id: str) -> int | None:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("SELECT preferred_leverage FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row = c.fetchone()
    conn.close()
    if not row or row[0] is None:
        return None
    return int(row[0])


def set_preferred_leverage(chat_id: str, value: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET preferred_leverage=? WHERE chat_id=?",
        (int(value), str(chat_id)),
    )
    conn.commit()
    conn.close()


def apply_subscription_cancellation(chat_id: str):
    """Revoke license after user-confirmed cancellation; keeps broker row for renewal flow."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """UPDATE subscribers SET
           license_key=NULL,
           expiry_date=NULL,
           payment_status='CANCELLED',
           subscription_started_at=NULL,
           trading_enabled=0,
           preferred_leverage=NULL
           WHERE chat_id=?""",
        (str(chat_id),),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_db()
