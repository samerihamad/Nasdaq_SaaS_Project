from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytz

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.executor as executor
import main as engine_main
from utils.market_hours import ET, UAE, synchronized_utc_now


@dataclass
class FakeResponse:
    status_code: int
    payload: dict[str, Any] | None = None
    text: str = ""
    headers: dict[str, str] | None = None

    def json(self) -> dict[str, Any]:
        return dict(self.payload or {})


class LoopBreak(Exception):
    pass


def _today_log_file() -> Path:
    root = Path(os.getenv("ENGINE_LOG_ROOT", "logs"))
    day = synchronized_utc_now().strftime("%Y-%m-%d")
    return root / day / "execution_events.txt"


def run_401_recovery_test() -> dict[str, Any]:
    """
    Simulate:
      1) 401 Unauthorized from broker
      2) automatic NTP resync + forced relogin
      3) successful retry
    """
    sent_requests: list[dict[str, Any]] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=20):
        sent_requests.append(
            {
                "method": str(method),
                "url": str(url),
                "headers": dict(headers or {}),
            }
        )
        if len(sent_requests) == 1:
            return FakeResponse(
                401,
                payload={
                    "errorCode": "SESSION_EXPIRED",
                    "message": "Session Expired due to Time Shift",
                },
                text="SESSION_EXPIRED",
            )
        return FakeResponse(200, payload={"ok": True, "dealReference": "DRY-TEST-OK"})

    ntp_diag = {
        "ok": True,
        "server": "pool.ntp.org",
        "offset_seconds": 0.0123,
        "latency_ms": 21.0,
        "synced_at_utc": synchronized_utc_now().isoformat(),
    }

    with (
        patch.object(executor.requests, "request", side_effect=fake_request),
        patch.object(executor, "sync_utc_with_ntp", return_value=ntp_diag),
        patch.object(
            executor,
            "get_session",
            return_value=(
                "https://demo-api-capital.backend-capital.com/api/v1",
                {
                    "X-CAP-API-KEY": "demo-key",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "CST": "demo-cst",
                    "X-SECURITY-TOKEN": "demo-token",
                },
            ),
        ),
    ):
        ok, payload, err, status = executor._broker_request(
            "GET",
            "https://demo-api-capital.backend-capital.com/api/v1/positions",
            headers={
                "X-CAP-API-KEY": "demo-key",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "CST": "stale-cst",
                "X-SECURITY-TOKEN": "stale-token",
            },
            creds=("demo-key", "demo-pass", True, "demo@example.com"),
            chat_id="dry-test-chat",
        )

    log_file = _today_log_file()
    recovery_lines: list[str] = []
    if log_file.exists():
        lines = log_file.read_text(encoding="utf-8").splitlines()
        recovery_lines = [ln for ln in lines if "stage=clock_resync" in ln][-3:]

    return {
        "ok": ok,
        "status": status,
        "error": err,
        "payload": payload,
        "request_count": len(sent_requests),
        "first_request_headers": sent_requests[0]["headers"] if sent_requests else {},
        "second_request_headers": sent_requests[1]["headers"] if len(sent_requests) > 1 else {},
        "recovery_log_lines": recovery_lines,
    }


def run_market_open_dedup_test() -> dict[str, Any]:
    """
    Simulate three consecutive loop passes in NY 09:30-09:35 window.
    Expect exactly one Telegram alert.
    """
    sent_messages: list[str] = []

    # 2026-07-15 is in US DST, so 09:30 ET = 13:30 UTC
    ticks = [
        datetime(2026, 7, 15, 13, 30, 10, tzinfo=timezone.utc),
        datetime(2026, 7, 15, 13, 31, 10, tzinfo=timezone.utc),
        datetime(2026, 7, 15, 13, 34, 50, tzinfo=timezone.utc),
    ]
    tick_state = {"i": 0}

    def fake_now():
        idx = min(tick_state["i"], len(ticks) - 1)
        return ticks[idx]

    def fake_sleep(_seconds):
        tick_state["i"] += 1
        if tick_state["i"] >= len(ticks):
            raise LoopBreak("stop loop after deterministic ticks")

    def fake_send_telegram(chat_id, message):
        sent_messages.append(str(message))
        return {"ok": True, "chat_id": chat_id}

    def fake_scan_market(_symbol, period="5d", interval="1d"):
        if interval == "1d":
            idx = pd.to_datetime(["2026-07-14", "2026-07-15"])
            return pd.DataFrame(
                {"Open": [620.00, 623.00], "Close": [622.50, 624.10]},
                index=idx,
            )
        # 15m premarket bars in UTC (09:00-09:29 ET = 13:00-13:29 UTC during DST)
        idx = pd.to_datetime(
            ["2026-07-15 13:00:00+00:00", "2026-07-15 13:15:00+00:00", "2026-07-15 13:29:00+00:00"]
        )
        return pd.DataFrame(
            {"High": [623.2, 623.5, 623.4], "Low": [622.7, 622.9, 623.0], "Volume": [1000, 1300, 900]},
            index=idx,
        )

    engine_main._market_open_last_alert_date = None

    with (
        patch.object(engine_main, "_load_market_open_last_alert_date", return_value=None),
        patch.object(engine_main, "_save_market_open_last_alert_date", return_value=None),
        patch.object(engine_main, "synchronized_utc_now", side_effect=fake_now),
        patch.object(engine_main.time, "sleep", side_effect=fake_sleep),
        patch.object(engine_main, "send_telegram_message", side_effect=fake_send_telegram),
        patch.object(engine_main, "scan_market", side_effect=fake_scan_market),
    ):
        try:
            engine_main._market_open_alert_loop()
        except LoopBreak:
            pass

    return {
        "ticks_ran": len(ticks),
        "alerts_sent": len(sent_messages),
        "sample_alert": sent_messages[0] if sent_messages else "",
    }


def print_time_matrix() -> dict[str, str]:
    """
    Print synchronized UTC converted to:
      - Dubai time
      - Finland server timezone (Europe/Helsinki)
      - New York market timezone (America/New_York)
    """
    helsinki = pytz.timezone("Europe/Helsinki")
    now_utc = synchronized_utc_now()
    ny = now_utc.astimezone(ET)
    dubai = now_utc.astimezone(UAE)
    fin = now_utc.astimezone(helsinki)

    matrix = {
        "utc": now_utc.strftime("%Y-%m-%d %H:%M:%S %Z%z"),
        "dubai": dubai.strftime("%Y-%m-%d %H:%M:%S %Z%z"),
        "finland": fin.strftime("%Y-%m-%d %H:%M:%S %Z%z"),
        "new_york": ny.strftime("%Y-%m-%d %H:%M:%S %Z%z"),
        "dubai_minus_ny_hours": f"{(dubai.utcoffset() - ny.utcoffset()).total_seconds() / 3600:.0f}",
        "dubai_minus_finland_hours": f"{(dubai.utcoffset() - fin.utcoffset()).total_seconds() / 3600:.0f}",
    }
    return matrix


def main():
    print("=== 401 Recovery Dry Test ===")
    recovery = run_401_recovery_test()
    print(f"broker_request_ok={recovery['ok']} status={recovery['status']} request_count={recovery['request_count']}")
    print("first_request_timestamp_headers=", {
        "X-CAP-TIMESTAMP": recovery["first_request_headers"].get("X-CAP-TIMESTAMP"),
        "X-TIMESTAMP": recovery["first_request_headers"].get("X-TIMESTAMP"),
    })
    print("second_request_timestamp_headers=", {
        "X-CAP-TIMESTAMP": recovery["second_request_headers"].get("X-CAP-TIMESTAMP"),
        "X-TIMESTAMP": recovery["second_request_headers"].get("X-TIMESTAMP"),
    })
    if recovery["recovery_log_lines"]:
        print("clock_resync_log_lines:")
        for ln in recovery["recovery_log_lines"]:
            print("  ", ln)
    else:
        print("clock_resync_log_lines: none found")

    print("\n=== Market Open De-Dupe Dry Test ===")
    dedup = run_market_open_dedup_test()
    print(f"ticks_ran={dedup['ticks_ran']} alerts_sent={dedup['alerts_sent']}")
    if dedup["sample_alert"]:
        print("sample_alert_first_line=", dedup["sample_alert"].splitlines()[0])

    print("\n=== Time Matrix (Synchronized) ===")
    matrix = print_time_matrix()
    print("UTC      :", matrix["utc"])
    print("Dubai    :", matrix["dubai"])
    print("Finland  :", matrix["finland"])
    print("New York :", matrix["new_york"])
    print("Dubai-NY offset (hours):", matrix["dubai_minus_ny_hours"])
    print("Dubai-Finland offset (hours):", matrix["dubai_minus_finland_hours"])


if __name__ == "__main__":
    main()
