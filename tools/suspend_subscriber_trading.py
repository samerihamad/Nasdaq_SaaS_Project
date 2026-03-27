"""
Suspend a subscriber until re-auth: kill_switch + trading_enabled off.
Usage: python tools/suspend_subscriber_trading.py <chat_id>
"""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "database", "trading_saas.db")


def main():
    if len(sys.argv) != 2:
        print("Usage: python tools/suspend_subscriber_trading.py <chat_id>")
        sys.exit(1)
    cid = str(sys.argv[1]).strip()
    conn = sqlite3.connect(DB)
    cur = conn.execute(
        "UPDATE subscribers SET kill_switch=1, trading_enabled=0 WHERE chat_id=?",
        (cid,),
    )
    conn.commit()
    print(f"chat_id={cid} rows_updated={cur.rowcount} (kill_switch=1, trading_enabled=0)")
    conn.close()


if __name__ == "__main__":
    main()
