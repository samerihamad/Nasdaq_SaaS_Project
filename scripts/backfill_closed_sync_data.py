import sqlite3

from core.executor import get_session
from core.watcher import get_all_active_subscribers
from core.sync import backfill_closed_pnls
from bot.licensing import safe_decrypt


def main() -> None:
    rows = get_all_active_subscribers()
    if not rows:
        print("No active subscribers found.")
        return

    total_updated = 0
    for row in rows:
        chat_id, enc_key, enc_pass, is_demo, enc_email = row
        api_key = safe_decrypt(enc_key)
        password = safe_decrypt(enc_pass)
        email = safe_decrypt(enc_email)
        creds = (api_key, password, is_demo, email)
        base_url, headers = get_session(creds)
        if not headers:
            print(f"skip chat_id={chat_id}: auth failed")
            continue
        try:
            updated = backfill_closed_pnls(str(chat_id), base_url, headers, lookback=2000)
            total_updated += int(updated)
            print(f"chat_id={chat_id} backfilled={updated}")
        except Exception as exc:
            print(f"chat_id={chat_id} backfill error: {exc}")

    print(f"done. total backfilled rows={total_updated}")


if __name__ == "__main__":
    main()
