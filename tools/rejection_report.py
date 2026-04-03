import sqlite3
from database.db_manager import DB_PATH


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
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
        """
        SELECT created_at, chat_id, symbol, action, stage, reason
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
