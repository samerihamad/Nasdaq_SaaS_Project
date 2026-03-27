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
        print("   or: python tools/send_plain_telegram.py <chat_id> @path/to/file.txt (UTF-8)")
        sys.exit(1)
    chat_id = str(sys.argv[1]).strip()
    rest = sys.argv[2:]
    if rest[0].startswith("@"):
        path = os.path.join(ROOT, rest[0][1:].lstrip("/"))
        with open(path, encoding="utf-8") as f:
            message = f.read().strip()
    else:
        message = " ".join(rest).strip()
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        print("Missing TELEGRAM_BOT_TOKEN")
        sys.exit(1)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=25)
    try:
        body = r.json()
    except Exception:
        body = r.text
    print(r.status_code, str(body)[:500])


if __name__ == "__main__":
    main()
