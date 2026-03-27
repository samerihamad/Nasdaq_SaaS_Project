"""
One-off: reconcile specific trade_ids (DB OPEN, already flat at broker).
Run from project root: python tools/reconcile_trade_ids.py 82 83
"""
import os
import sqlite3
import sys

# Project root
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

DB_PATH = os.path.join(ROOT, "database", "trading_saas.db")


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/reconcile_trade_ids.py <trade_id> [trade_id ...]")
        sys.exit(1)
    tids = [int(x) for x in sys.argv[1:]]
    conn = sqlite3.connect(DB_PATH)
    chat_ids = set()
    for tid in tids:
        row = conn.execute(
            "SELECT trade_id, chat_id, status, deal_id, symbol FROM trades WHERE trade_id=?",
            (tid,),
        ).fetchone()
        print(f"trade_id={tid} row={row}")
        if row and row[2] == "OPEN":
            chat_ids.add(str(row[1]))
    conn.close()

    if not chat_ids:
        print("No OPEN trades to reconcile for given ids.")
        return

    from core.executor import get_user_credentials, get_session
    from core.sync import reconcile

    for cid in sorted(chat_ids):
        creds = get_user_credentials(cid)
        if not creds:
            print(f"[{cid}] skip — no credentials")
            continue
        base_url, headers = get_session(creds)
        if not headers:
            print(f"[{cid}] skip — session failed (re-auth required)")
            continue
        print(f"[{cid}] running reconcile...")
        reconcile(cid, base_url, headers, notify=True)
        print(f"[{cid}] reconcile done.")


if __name__ == "__main__":
    main()
