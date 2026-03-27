"""Force OPEN rows after false-positive close (admin). Usage: python tools/reopen_trades_by_id.py 78 79 82 83"""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "database", "trading_saas.db")


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/reopen_trades_by_id.py <trade_id> [...]")
        sys.exit(1)
    tids = [int(x) for x in sys.argv[1:]]
    conn = sqlite3.connect(DB)
    for tid in tids:
        cur = conn.execute(
            "UPDATE trades SET status='OPEN', closed_at=NULL, sync_status=NULL, "
            "pnl=NULL, actual_pnl=NULL, exit_price=NULL, "
            "close_sync_last_error='reopened_false_positive_sync', close_reason=NULL, "
            "close_sync_notified=0 WHERE trade_id=?",
            (int(tid),),
        )
        print(f"trade_id={tid} updated={cur.rowcount}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
