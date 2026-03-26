import argparse
import os
import sqlite3
from pathlib import Path

from database.db_manager import create_db, DB_PATH
from bot.notifier import send_telegram_message


REQUIRED_TRADE_COLUMNS = {
    "deal_reference",
    "actual_pnl",
    "exit_price",
    "close_reason",
    "close_sync_notified",
    "close_sync_attempts",
    "close_sync_last_try_at",
    "close_sync_last_error",
    "sync_status",
}


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table_name})")
    rows = c.fetchall() or []
    return {str(r[1]) for r in rows if len(r) > 1}


def _purge_db_files(project_root: Path, include_backups: bool) -> list[Path]:
    removed: list[Path] = []
    patterns = ["**/*.db", "**/*.db-shm", "**/*.db-wal", "**/*.sqlite", "**/*.sqlite3"]
    if include_backups:
        patterns.extend(["**/*.db.enc", "**/*.sqlite.enc"])
    for pat in patterns:
        for p in project_root.glob(pat):
            if not p.is_file():
                continue
            # Keep virtualenv and dependency folders safe.
            skip_parts = {"venv", ".venv", "__pycache__", ".git"}
            if any(part in skip_parts for part in p.parts):
                continue
            try:
                p.unlink()
                removed.append(p)
            except FileNotFoundError:
                pass
    return removed


def _send_init_ping() -> None:
    admin_chat_id = (os.getenv("ADMIN_CHAT_ID") or "").strip()
    if not admin_chat_id:
        print("ADMIN_CHAT_ID not set; init Telegram ping skipped.")
        return
    msg = (
        "✅ System Initialized\n"
        "Database purge completed and latest schema is ready."
    )
    send_telegram_message(admin_chat_id, msg)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hard reset and reinitialize project database.")
    parser.add_argument("--yes", action="store_true", help="Required confirmation flag.")
    parser.add_argument(
        "--keep-backups",
        action="store_true",
        help="Keep encrypted backup files (*.db.enc).",
    )
    args = parser.parse_args()

    if not args.yes:
        raise SystemExit("Refusing destructive reset without --yes")

    project_root = Path(".").resolve()
    removed = _purge_db_files(project_root, include_backups=not args.keep_backups)
    print(f"Purged DB artifacts: {len(removed)}")
    for p in removed[:20]:
        print(f" - {p}")
    if len(removed) > 20:
        print(f" ... and {len(removed) - 20} more")

    # Recreate all tables/columns from latest schema.
    create_db()

    # Safety verification: schema must match expected trade sync fields.
    conn = sqlite3.connect(DB_PATH)
    cols = _table_columns(conn, "trades")
    conn.close()
    missing = sorted(REQUIRED_TRADE_COLUMNS - cols)
    if missing:
        raise RuntimeError(f"Schema mismatch after reset; missing columns: {', '.join(missing)}")

    print("Schema verification passed.")
    _send_init_ping()
    print("Reset complete. System is ready for first clean trade.")


if __name__ == "__main__":
    main()
