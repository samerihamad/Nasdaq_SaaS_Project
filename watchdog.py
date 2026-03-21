"""
NATB v2.0 — Heartbeat Watchdog

Run this as a completely separate process alongside main.py and bot/dashboard.py.
It reads heartbeat.json every CHECK_INTERVAL seconds and sends a Telegram alert
to the admin if the primary server has been silent for more than MAX_SILENCE seconds.

Linux deployment (recommended — add to /etc/systemd/system/natb-watchdog.service):
  [Service]
  ExecStart=/usr/bin/python3 /path/to/watchdog.py
  Restart=always

Or a simple background run:
  nohup python watchdog.py &
"""

import os
import sys
import time
import json
import subprocess
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

HEARTBEAT_FILE    = "heartbeat.json"
MAX_SILENCE_SEC   = 90      # alert if no heartbeat for 90 seconds
CHECK_INTERVAL    = 10      # check every 10 seconds
ALERT_COOLDOWN    = 300     # resend alert at most every 5 minutes
AUTO_RESTART      = os.getenv('WATCHDOG_AUTO_RESTART', 'false').lower() == 'true'
ADMIN_CHAT_ID     = os.getenv('ADMIN_CHAT_ID', '')
TELEGRAM_TOKEN    = os.getenv('TELEGRAM_BOT_TOKEN', '')

_last_alert_time  = 0
_server_was_down  = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_heartbeat() -> datetime | None:
    try:
        with open(HEARTBEAT_FILE, 'r') as f:
            data = json.load(f)
        return datetime.fromisoformat(data['timestamp'])
    except Exception:
        return None


def _send_alert(message: str):
    """Send a Telegram message directly via HTTP (no dependency on the bot process)."""
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID:
        print(f"[WATCHDOG] Alert (no Telegram configured): {message}")
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        print(f"[WATCHDOG] Failed to send Telegram alert: {e}")


def _try_restart_main():
    """Attempt to restart main.py as a background process (Linux/Mac only)."""
    try:
        subprocess.Popen(
            [sys.executable, "main.py"],
            stdout=open("main.log", "a"),
            stderr=subprocess.STDOUT,
        )
        print("[WATCHDOG] main.py restart initiated.")
    except Exception as e:
        print(f"[WATCHDOG] Auto-restart failed: {e}")


# ── Main watchdog loop ────────────────────────────────────────────────────────

def run():
    global _last_alert_time, _server_was_down

    print(f"🐕 NATB Watchdog started — threshold: {MAX_SILENCE_SEC}s | check: {CHECK_INTERVAL}s")

    while True:
        try:
            last_hb  = _read_heartbeat()
            now      = datetime.now()
            silence  = (now - last_hb).total_seconds() if last_hb else float('inf')
            now_ts   = time.time()

            if silence > MAX_SILENCE_SEC:
                # Server is down or stale
                if not _server_was_down:
                    _server_was_down = True
                    print(f"[WATCHDOG] 🚨 Server silent for {int(silence)}s — alerting admin")

                if now_ts - _last_alert_time > ALERT_COOLDOWN:
                    last_seen = last_hb.strftime('%H:%M:%S') if last_hb else 'غير متاح'
                    _send_alert(
                        f"🚨 *Server Down! Subscribers at risk*\n\n"
                        f"آخر نشاط مسجّل: `{last_seen}`\n"
                        f"مدة التوقف: *{int(silence)} ثانية*\n"
                        f"الوقت الحالي: `{now.strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                        f"الصفقات المفتوحة ليست تحت مراقبة المحرك."
                    )
                    _last_alert_time = now_ts

                    if AUTO_RESTART:
                        _try_restart_main()

            else:
                # Server is alive
                if _server_was_down:
                    _server_was_down = False
                    _send_alert(
                        f"✅ *الخادم عاد للعمل*\n"
                        f"آخر نبضة: `{last_hb.strftime('%H:%M:%S')}`"
                    )
                    print("[WATCHDOG] ✅ Server recovered.")
                else:
                    print(f"[WATCHDOG] 💚 OK — last heartbeat {int(silence)}s ago")

        except Exception as e:
            print(f"[WATCHDOG] Internal error: {e}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()
