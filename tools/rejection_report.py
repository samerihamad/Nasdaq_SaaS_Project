import sqlite3
from database.db_manager import DB_PATH


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA table_info(trade_rejections)")
    tr_cols = [str(r[1]) for r in (c.fetchall() or [])]
    reason_code_sql = ", reason_code" if "reason_code" in tr_cols else ""

    c.execute(
        """
        SELECT stage, COUNT(*) AS n
        FROM trade_rejections
        WHERE created_at >= datetime('now', '-1 day')
        GROUP BY stage
        ORDER BY n DESC
        """
    )
    rows = c.fetchall()
    print("== Rejections last 24h by stage ==")
    if not rows:
        print("No rejections logged.")
    else:
        for stage, n in rows:
            print(f"{stage}: {n}")

    c.execute(
        f"""
        SELECT created_at, chat_id, symbol, action, stage, reason{reason_code_sql}
        FROM trade_rejections
        ORDER BY id DESC
        LIMIT 30
        """
    )
    print("\n== Latest 30 rejections ==")
    for r in c.fetchall():
        print(" | ".join(str(x) for x in r))
    conn.close()


if __name__ == "__main__":
    main()
