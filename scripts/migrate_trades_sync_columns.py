import sqlite3
from pathlib import Path


DB_PATH = Path("database/trading_saas.db")


def main() -> None:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()

    columns = [
        ("deal_reference", "TEXT"),
        ("actual_pnl", "REAL"),
        ("exit_price", "REAL"),
        ("target_reached", "TEXT"),
        ("close_reason", "TEXT"),
        ("close_sync_notified", "INTEGER DEFAULT 0"),
        ("close_sync_attempts", "INTEGER DEFAULT 0"),
        ("close_sync_last_try_at", "TEXT"),
        ("close_sync_last_error", "TEXT"),
    ]

    added = 0
    for col, definition in columns:
        try:
            c.execute(f"ALTER TABLE trades ADD COLUMN {col} {definition}")
            added += 1
            print(f"added: {col}")
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "duplicate column name" in msg:
                print(f"exists: {col}")
            else:
                raise

    conn.commit()
    conn.close()
    print(f"done. columns added: {added}")


if __name__ == "__main__":
    main()
