from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import (
    AUTOTRAIN_DAILY_UTC_HOUR,
    AUTOTRAIN_DAILY_UTC_MINUTE,
    AUTOTRAIN_WEEKLY_DAY,
    AUTOTRAIN_WEEKLY_UTC_HOUR,
    AUTOTRAIN_WEEKLY_UTC_MINUTE,
    DEEP_DIRECTION_INFERENCE_KIND,
    DEEP_DIRECTION_TIMEFRAME,
)
from utils.autonomous_training import load_autonomous_training_status
from utils.market_hours import (
    ET,
    UAE,
    PRE_MKT_OPEN,
    MARKET_OPEN,
    MARKET_CLOSE,
    AH_CLOSE,
    get_current_timezones,
    session_windows_utc,
    is_within_us_cash_session_utc,
)
from utils.market_scanner import (
    _clean_ohlcv,
    _get_market_data_session,
    _parse_capital_ohlcv,
    _resolve_epic,
)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _print_header(title: str):
    print(f"\n=== {title} ===")


def _to_aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _next_daily_run_utc(now: datetime) -> datetime:
    candidate = now.replace(
        hour=int(AUTOTRAIN_DAILY_UTC_HOUR),
        minute=int(AUTOTRAIN_DAILY_UTC_MINUTE),
        second=0,
        microsecond=0,
    )
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def _next_weekly_run_utc(now: datetime) -> datetime:
    weekday_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    target = weekday_map.get(str(AUTOTRAIN_WEEKLY_DAY).strip().lower(), 6)
    base = now.replace(
        hour=int(AUTOTRAIN_WEEKLY_UTC_HOUR),
        minute=int(AUTOTRAIN_WEEKLY_UTC_MINUTE),
        second=0,
        microsecond=0,
    )
    days_ahead = (target - base.weekday()) % 7
    candidate = base + timedelta(days=days_ahead)
    if candidate <= now:
        candidate = candidate + timedelta(days=7)
    return candidate


def _format_multi_tz(dt_utc: datetime) -> dict:
    aware = _to_aware_utc(dt_utc)
    return {
        "utc": _iso(aware),
        "dubai": _iso(aware.astimezone(UAE)),
        "new_york": _iso(aware.astimezone(ET)),
    }


def _market_windows_report(now_utc: datetime) -> dict:
    windows = session_windows_utc(now_utc)
    ny_date = now_utc.astimezone(ET).date()
    pre_open_et = ET.localize(datetime(ny_date.year, ny_date.month, ny_date.day, PRE_MKT_OPEN.hour, PRE_MKT_OPEN.minute))
    regular_open_et = ET.localize(datetime(ny_date.year, ny_date.month, ny_date.day, MARKET_OPEN.hour, MARKET_OPEN.minute))
    regular_close_et = ET.localize(datetime(ny_date.year, ny_date.month, ny_date.day, MARKET_CLOSE.hour, MARKET_CLOSE.minute))
    ah_close_et = ET.localize(datetime(ny_date.year, ny_date.month, ny_date.day, AH_CLOSE.hour, AH_CLOSE.minute))

    expected = {
        "pre_market_et": (pre_open_et, regular_open_et),
        "regular_et": (regular_open_et, regular_close_et),
        "after_hours_et": (regular_close_et, ah_close_et),
    }
    converted = {
        name: {
            "utc_start": _iso(rng[0].astimezone(timezone.utc)),
            "utc_end": _iso(rng[1].astimezone(timezone.utc)),
            "dubai_start": _iso(rng[0].astimezone(UAE)),
            "dubai_end": _iso(rng[1].astimezone(UAE)),
        }
        for name, rng in expected.items()
    }
    module_regular_open = windows["regular_open_utc"]
    module_regular_close = windows["regular_close_utc"]
    expected_regular_open = expected["regular_et"][0].astimezone(timezone.utc)
    expected_regular_close = expected["regular_et"][1].astimezone(timezone.utc)
    match = (module_regular_open == expected_regular_open) and (module_regular_close == expected_regular_close)
    gate_allowed, gate_reason = is_within_us_cash_session_utc(now_utc)

    return {
        "windows": converted,
        "module_match_expected_regular_session": bool(match),
        "module_regular_open_utc": _iso(module_regular_open),
        "module_regular_close_utc": _iso(module_regular_close),
        "gate_now": {"allowed": bool(gate_allowed), "reason": gate_reason},
    }


def _fetch_candle_alignment(symbol: str = "AAPL") -> dict:
    tf_resolution = {"1d": "DAY", "4h": "HOUR_4", "15m": "MINUTE_15"}
    tf_step = {"1d": 86400, "4h": 14400, "15m": 900}
    base_url, headers = _get_market_data_session()
    if not base_url or not headers:
        return {"error": "Unable to authenticate market data session"}
    epic = _resolve_epic(base_url, headers, symbol)
    if not epic:
        return {"error": f"Unable to resolve epic for {symbol}"}

    out = {"symbol": symbol, "epic": epic, "timeframes": {}}
    for tf in ("1d", "4h", "15m"):
        res = requests.get(
            f"{base_url}/prices/{epic}",
            params={"resolution": tf_resolution[tf], "max": 6},
            headers=headers,
            timeout=20,
        )
        if res.status_code != 200:
            out["timeframes"][tf] = {"error": f"HTTP {res.status_code}"}
            continue
        payload = res.json() or {}
        raw_rows = (payload.get("prices") or payload.get("candles") or payload.get("data") or [])
        raw_latest = raw_rows[-1] if raw_rows else {}
        raw_ts = str(raw_latest.get("snapshotTimeUTC") or raw_latest.get("snapshotTime") or raw_latest.get("time") or "")
        parsed = _parse_capital_ohlcv(payload)
        cleaned = _clean_ohlcv(symbol=epic, timeframe=tf, df=parsed, step_sec=tf_step[tf]) if parsed is not None else None
        if cleaned is None or cleaned.empty:
            out["timeframes"][tf] = {
                "raw_api_timestamp": raw_ts,
                "error": "No cleaned candles returned",
            }
            continue
        latest_idx = cleaned.index[-1]
        idx_is_aware = bool(getattr(latest_idx, "tzinfo", None))
        latest_utc = _to_aware_utc(latest_idx.to_pydatetime())
        out["timeframes"][tf] = {
            "raw_api_timestamp": raw_ts,
            "latest_index_is_tz_aware": idx_is_aware,
            "latest_utc": _iso(latest_utc),
            "latest_dubai": _iso(latest_utc.astimezone(UAE)),
            "latest_new_york": _iso(latest_utc.astimezone(ET)),
            "rows": int(len(cleaned)),
        }
    return out


def _ai_runtime_timestamps_report() -> dict:
    status = load_autonomous_training_status()
    logs_root = Path("logs")
    day_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = logs_root / day_utc
    files = {
        "ai_gatekeeper": day_dir / "ai_telemetry.txt",
        "market_scanner_quality": day_dir / "data_quality.txt",
        "timezone_snapshot": day_dir / "timezone_snapshot.txt",
    }
    tail = {}
    for k, p in files.items():
        if p.exists():
            try:
                lines = [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
                tail[k] = lines[-1] if lines else ""
            except Exception:
                tail[k] = ""
        else:
            tail[k] = ""
    return {
        "autonomous_training_status": status,
        "daily_log_tail": tail,
        "training_timeframe_kind": {
            "deep_direction_timeframe": str(DEEP_DIRECTION_TIMEFRAME),
            "deep_direction_kind": str(DEEP_DIRECTION_INFERENCE_KIND),
        },
    }


def _scan_timezone_code_signals() -> dict:
    # Static verification: these patterns should not appear after fixes.
    targets = list(Path(".").rglob("*.py"))
    bad_patterns = ("datetime.now()", "datetime.utcnow()", "date.today()")
    hits: dict[str, list[str]] = {p: [] for p in bad_patterns}
    for fp in targets:
        try:
            txt = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for pat in bad_patterns:
            if pat in txt:
                hits[pat].append(str(fp))
    return {"forbidden_pattern_hits": hits}


def main():
    now_utc = datetime.now(timezone.utc)
    _print_header("Core Timezones")
    print(json.dumps(get_current_timezones(now_utc), ensure_ascii=False, indent=2))

    _print_header("Market Windows Validation")
    print(json.dumps(_market_windows_report(now_utc), ensure_ascii=False, indent=2))

    _print_header("Candle Alignment Validation (AAPL)")
    print(json.dumps(_fetch_candle_alignment("AAPL"), ensure_ascii=False, indent=2))

    _print_header("Scheduler Validation")
    next_daily = _next_daily_run_utc(now_utc)
    next_weekly = _next_weekly_run_utc(now_utc)
    scheduler = {
        "configured": {
            "AUTOTRAIN_DAILY_UTC_HOUR": AUTOTRAIN_DAILY_UTC_HOUR,
            "AUTOTRAIN_DAILY_UTC_MINUTE": AUTOTRAIN_DAILY_UTC_MINUTE,
            "AUTOTRAIN_WEEKLY_DAY": AUTOTRAIN_WEEKLY_DAY,
            "AUTOTRAIN_WEEKLY_UTC_HOUR": AUTOTRAIN_WEEKLY_UTC_HOUR,
            "AUTOTRAIN_WEEKLY_UTC_MINUTE": AUTOTRAIN_WEEKLY_UTC_MINUTE,
        },
        "next_daily": _format_multi_tz(next_daily),
        "next_weekly": _format_multi_tz(next_weekly),
        "scheduler_internal_basis": "UTC",
    }
    print(json.dumps(scheduler, ensure_ascii=False, indent=2))

    _print_header("AI Runtime Timestamp Sources")
    print(json.dumps(_ai_runtime_timestamps_report(), ensure_ascii=False, indent=2))

    _print_header("Static TZ Pattern Scan")
    print(json.dumps(_scan_timezone_code_signals(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
