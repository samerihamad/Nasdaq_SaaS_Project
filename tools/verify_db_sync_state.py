"""Print OPEN / PENDING_FINAL counts for given chat_id(s)."""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "database", "trading_saas.db")


def main():
    uids = sys.argv[1:]
    if not uids:
        print("Usage: python tools/verify_db_sync_state.py <chat_id> [chat_id ...]")
        sys.exit(1)
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    for uid in uids:
        c.execute(
            "SELECT COUNT(*) FROM trades WHERE chat_id=? AND status='OPEN'",
            (uid,),
        )
        open_n = c.fetchone()[0]
        c.execute(
            "SELECT COUNT(*) FROM trades WHERE chat_id=? AND status='CLOSED' "
            "AND COALESCE(sync_status,'')='PENDING_FINAL'",
            (uid,),
        )
        pend = c.fetchone()[0]
        print(f"chat {uid}: OPEN={open_n} CLOSED_pending_final={pend}")
    conn.close()


if __name__ == "__main__":
    main()
