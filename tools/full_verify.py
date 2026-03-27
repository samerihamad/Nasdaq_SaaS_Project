"""Dump trades 82-83, OPEN rows, and sync counts for key chats."""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "database", "trading_saas.db")


def main():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute(
        "SELECT trade_id, chat_id, status, sync_status, pnl, deal_id FROM trades "
        "WHERE trade_id IN (82, 83)"
    )
    print("trades_82_83:", c.fetchall())
    c.execute("SELECT trade_id, chat_id, symbol, status FROM trades WHERE status='OPEN'")
    print("all_OPEN:", c.fetchall())
    for uid in ("6905332685", "1044635944"):
        c.execute(
            "SELECT COUNT(*) FROM trades WHERE chat_id=? AND status='OPEN'", (uid,)
        )
        o = c.fetchone()[0]
        c.execute(
            "SELECT COUNT(*) FROM trades WHERE chat_id=? AND status='CLOSED' "
            "AND COALESCE(sync_status,'')='PENDING_FINAL'",
            (uid,),
        )
        p = c.fetchone()[0]
        print(f"sync_{uid}: OPEN={o} PENDING_FINAL={p}")
    conn.close()


if __name__ == "__main__":
    main()
