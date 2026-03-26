import sqlite3
from pathlib import Path


DB_PATH = Path("database/trading_saas.db")


def _table_columns(cursor: sqlite3.Cursor, table: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    rows = cursor.fetchall() or []
    return {str(r[1]) for r in rows if len(r) > 1}


def main() -> None:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    cols = _table_columns(c, "trades")

    required = {
        "actual_pnl",
        "exit_price",
        "close_reason",
        "deal_reference",
        "close_sync_attempts",
        "close_sync_last_try_at",
        "close_sync_last_error",
    }
    missing = sorted(required - cols)

    print("== Trades table sync audit ==")
    print(f"trades columns count  : {len(cols)}")
    if missing:
        print("missing columns       : " + ", ".join(missing))
        print(
            "action required       : run the app once (create_db migration) "
            "or deploy/restart server to apply ALTER TABLE migrations"
        )
    else:
        c.execute(
            """
            SELECT
                COUNT(*) AS total_closed,
                SUM(CASE WHEN actual_pnl IS NOT NULL THEN 1 ELSE 0 END) AS with_actual_pnl,
                SUM(CASE WHEN exit_price IS NOT NULL THEN 1 ELSE 0 END) AS with_exit_price,
                SUM(CASE WHEN close_reason IS NOT NULL AND TRIM(close_reason) != '' THEN 1 ELSE 0 END) AS with_close_reason,
                SUM(CASE WHEN status='OPEN' AND COALESCE(close_sync_attempts,0) > 0 THEN 1 ELSE 0 END) AS open_pending_sync,
                SUM(CASE WHEN status='CLOSED' AND COALESCE(sync_status,'')='PENDING_FINAL' THEN 1 ELSE 0 END) AS closed_pending_final,
                SUM(CASE WHEN status='CLOSED' AND COALESCE(sync_status,'')='SYNCED' THEN 1 ELSE 0 END) AS closed_synced
            FROM trades
            """
        )
        row = c.fetchone() or (0, 0, 0, 0, 0, 0, 0)
        print(f"closed_total          : {int(row[0] or 0)}")
        print(f"closed_with_actual_pnl: {int(row[1] or 0)}")
        print(f"closed_with_exit_price: {int(row[2] or 0)}")
        print(f"closed_with_reason    : {int(row[3] or 0)}")
        print(f"open_pending_sync     : {int(row[4] or 0)}")
        print(f"closed_pending_final  : {int(row[5] or 0)}")
        print(f"closed_synced         : {int(row[6] or 0)}")

    print("\n== Top pending sync trades (latest) ==")
    if {"deal_reference", "close_sync_attempts", "close_sync_last_try_at", "close_sync_last_error"} <= cols:
        c.execute(
            """
            SELECT
                trade_id, chat_id, symbol, direction, deal_id, deal_reference,
                close_sync_attempts, close_sync_last_try_at, close_sync_last_error
            FROM trades
            WHERE (status='OPEN' AND COALESCE(close_sync_attempts,0) > 0)
               OR (status='CLOSED' AND COALESCE(sync_status,'')='PENDING_FINAL')
            ORDER BY COALESCE(close_sync_attempts,0) DESC, trade_id DESC
            LIMIT 25
            """
        )
        rows = c.fetchall()
        if not rows:
            print("No pending-sync OPEN trades found.")
        else:
            for r in rows:
                print(
                    f"trade_id={r[0]} chat_id={r[1]} {r[2]} {r[3]} "
                    f"deal_id={r[4]} ref={r[5]} attempts={r[6]} "
                    f"last_try={r[7]} err={r[8]}"
                )
    else:
        print("Pending-sync diagnostics unavailable (missing sync columns).")

    conn.close()


if __name__ == "__main__":
    main()
