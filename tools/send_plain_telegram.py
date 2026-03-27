"""Send plain text to a Telegram chat_id (no Markdown). Project root + .env."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

import requests
from dotenv import load_dotenv

load_dotenv()


def main():
    if len(sys.argv) < 3:
        print("Usage: python tools/send_plain_telegram.py <chat_id> <message>")
        sys.exit(1)
    chat_id = str(sys.argv[1]).strip()
    message = " ".join(sys.argv[2:]).strip()
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        print("Missing TELEGRAM_BOT_TOKEN")
        sys.exit(1)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=25)
    print(r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)[:500])


if __name__ == "__main__":
    main()
