"""
Force CLOSED + PENDING_FINAL rows to SYNCED when broker history cannot be retrieved.
Sets pnl/actual_pnl to 0.0 and records close_sync_last_error.
Usage: python tools/force_sync_pending_final.py 82 83
"""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "database", "trading_saas.db")


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/force_sync_pending_final.py <trade_id> [...]")
        sys.exit(1)
    tids = [int(x) for x in sys.argv[1:]]
    conn = sqlite3.connect(DB)
    for tid in tids:
        cur = conn.execute(
            "UPDATE trades SET sync_status='SYNCED', pnl=COALESCE(pnl, 0), actual_pnl=COALESCE(actual_pnl, 0), "
            "close_sync_last_error=? WHERE trade_id=? AND status='CLOSED' "
            "AND COALESCE(sync_status,'')='PENDING_FINAL'",
            ("forced_sync_no_broker_history", int(tid)),
        )
        print(f"trade_id={tid} rows_updated={cur.rowcount}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
