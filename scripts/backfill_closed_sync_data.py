import sqlite3
import requests

from core.sync import backfill_closed_pnls
from bot.licensing import safe_decrypt

DB_PATH = "database/trading_saas.db"


def _get_all_active_subscribers():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT chat_id, api_key, api_password, is_demo, email "
        "FROM subscribers WHERE is_active=1 AND email IS NOT NULL"
    )
    rows = c.fetchall()
    conn.close()
    return rows


def _auth_session(api_key: str, password: str, is_demo: int, email: str):
    base_url = (
        "https://demo-api-capital.backend-capital.com/api/v1"
        if bool(is_demo)
        else
        "https://api-capital.backend-capital.com/api/v1"
    )
    headers = {
        "X-CAP-API-KEY": str(api_key or "").strip(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        res = requests.post(
            f"{base_url}/session",
            json={"identifier": str(email or "").strip(), "password": str(password or "").strip()},
            headers=headers,
            timeout=20,
        )
    except Exception:
        return None, None
    if res.status_code != 200:
        return None, None
    headers["CST"] = res.headers.get("CST")
    headers["X-SECURITY-TOKEN"] = res.headers.get("X-SECURITY-TOKEN")
    return base_url, headers


def main() -> None:
    rows = _get_all_active_subscribers()
    if not rows:
        print("No active subscribers found.")
        return

    total_updated = 0
    for row in rows:
        chat_id, enc_key, enc_pass, is_demo, enc_email = row
        api_key = safe_decrypt(enc_key)
        password = safe_decrypt(enc_pass)
        email = safe_decrypt(enc_email)
        base_url, headers = _auth_session(api_key, password, is_demo, email)
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
