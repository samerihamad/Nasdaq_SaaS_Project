import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests

from config import (
    CAPITAL_API_KEY,
    CAPITAL_EMAIL,
    CAPITAL_PASSWORD,
    CAPITAL_IS_DEMO,
    MARKET_DATA_CAPITAL_API_KEY,
    MARKET_DATA_CAPITAL_EMAIL,
    MARKET_DATA_CAPITAL_PASSWORD,
    MARKET_DATA_CAPITAL_IS_DEMO,
    MIN_ANALYSIS_BARS,
    TARGET_ANALYSIS_BARS,
)

_DATA_TIMEOUT_SEC = int(os.getenv("MARKET_DATA_TIMEOUT_SEC", "20"))
_SESSION_TTL_SEC = int(os.getenv("MARKET_DATA_SESSION_TTL_SEC", "45"))
_LOG_ROOT = os.getenv("ENGINE_LOG_ROOT", "logs")
# Capital.com /prices max= upper bound (broker caps vary; stay within common API limits).
_CAPITAL_PRICES_MAX_BARS_CAP = int(os.getenv("CAPITAL_PRICES_MAX_BARS_CAP", "1000"))
# Daily: ask for the deepest history the API allows (capped at 1000).
_DAILY_PRICES_MAX = min(1000, max(50, _CAPITAL_PRICES_MAX_BARS_CAP))

_SESSION_CACHE: dict[str, dict] = {}
_EPIC_CACHE: dict[str, str] = {}
_UNSUPPORTED_CACHE: set[str] = set()

_TF_CONFIG = {
    "1d": {
        "resolutions": ("DAY", "D1", "DAY_1"),
        "max": _DAILY_PRICES_MAX,
        "step_sec": 86400,
        "min_rows": 40,
    },
    "4h": {
        "resolutions": ("HOUR_4", "H4", "HOUR4"),
        "max": 500,
        "step_sec": 14400,
        "min_rows": 40,
    },
    "15m": {
        "resolutions": ("MINUTE_15", "M15", "MINUTE15"),
        "max": 800,
        "step_sec": 900,
        "min_rows": 80,
    },
}


def _max_bar_attempts(base_max: int) -> list[int]:
    """
    Escalate Capital.com historical `max` when the first response has too few bars.
    Tries progressively larger windows up to _CAPITAL_PRICES_MAX_BARS_CAP.
    """
    cap = max(50, min(int(_CAPITAL_PRICES_MAX_BARS_CAP), 1000))
    m = max(1, int(base_max))
    attempts: list[int] = [min(m, cap)]
    for mult in (1.25, 1.5, 2.0, 2.5, 3.0):
        nxt = int(round(m * mult))
        nxt = min(max(nxt, attempts[-1] + 1), cap)
        if nxt not in attempts:
            attempts.append(nxt)
    if cap not in attempts:
        attempts.append(cap)
    return sorted(set(attempts))


def _log_recovered_proceed(symbol: str, bar_count: int, timeframe: str) -> None:
    """Soft band (e.g. 200–219 daily bars): proceed but make it visible in logs."""
    _append_data_quality_log(
        symbol,
        timeframe,
        "OK",
        f"[RECOVERED] Proceeding with {bar_count} bars for {symbol}",
    )


def _smart_retry_daily_fetch(
    base_url: str,
    headers: dict,
    epic: str,
    cfg: dict,
    log_symbol: str,
) -> pd.DataFrame | None:
    """
    After normal escalation fails to reach MIN_ANALYSIS_BARS on daily data, try once more:
    deepest max= with resolutions in reverse order, then a second pass with a slightly
    lower max (covers API quirks without another data source).
    """
    cap = min(1000, max(50, int(_CAPITAL_PRICES_MAX_BARS_CAP)))
    resolutions = list(cfg["resolutions"])
    alt_maxes = (cap, max(MIN_ANALYSIS_BARS, int(round(cap * 0.92))))
    last_err = ""
    for max_try in alt_maxes:
        for res_name in reversed(resolutions):
            try:
                res = requests.get(
                    f"{base_url}/prices/{epic}",
                    params={"resolution": res_name, "max": int(max_try)},
                    headers=headers,
                    timeout=_DATA_TIMEOUT_SEC,
                )
                if res.status_code != 200:
                    last_err = f"smart_retry status={res.status_code} res={res_name} max={max_try}"
                    continue
                df_raw = _parse_capital_ohlcv(res.json() or {})
                if df_raw is None or df_raw.empty:
                    last_err = f"smart_retry empty res={res_name} max={max_try}"
                    continue
                df_clean = _clean_ohlcv(
                    symbol=epic,
                    timeframe="1d",
                    df=df_raw,
                    step_sec=int(cfg["step_sec"]),
                )
                if df_clean is None or df_clean.empty:
                    last_err = f"smart_retry clean failed res={res_name} max={max_try}"
                    continue
                n = len(df_clean)
                if n < int(cfg["min_rows"]):
                    continue
                if n < MIN_ANALYSIS_BARS:
                    last_err = f"smart_retry rows={n} min={MIN_ANALYSIS_BARS} res={res_name} max={max_try}"
                    continue
                _append_data_quality_log(
                    epic,
                    "1d",
                    "OK",
                    f"smart_retry_ok resolution={res_name} max={max_try} rows={n}",
                )
                if MIN_ANALYSIS_BARS <= n < TARGET_ANALYSIS_BARS:
                    _log_recovered_proceed(log_symbol, n, "1d")
                return df_clean
            except Exception as exc:
                last_err = f"smart_retry exception res={res_name} max={max_try}: {exc}"
    _append_data_quality_log(epic, "1d", "WARN", f"smart_retry_exhausted last={last_err}")
    return None


def _append_data_quality_log(symbol: str, timeframe: str, status: str, details: str):
    """Append per-symbol/timeframe data quality logs into daily folder."""
    try:
        day_dir = os.path.join(_LOG_ROOT, datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        os.makedirs(day_dir, exist_ok=True)
        path = os.path.join(day_dir, "data_quality.txt")
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        with open(path, "a", encoding="utf-8") as f:
            f.write(
                f"[{ts}] symbol={symbol} tf={timeframe} status={status} details={details}\n"
            )
    except Exception as exc:
        print(f"[DATA-QUALITY LOG] write failed: {exc}")


def _get_base_url(is_demo: bool) -> str:
    return (
        "https://demo-api-capital.backend-capital.com/api/v1"
        if bool(is_demo)
        else "https://api-capital.backend-capital.com/api/v1"
    )


def _resolve_market_data_identity(session_context: dict | None = None) -> dict:
    """
    Resolve market-data credentials.
    Backward compatible default is the global scanner identity from config.py.

    session_context supported keys:
      - api_key
      - email
      - password
      - is_demo
      - cache_key (optional explicit identity key)
      - label (optional log label)
    """
    ctx = dict(session_context or {})
    default_api_key = str(MARKET_DATA_CAPITAL_API_KEY or CAPITAL_API_KEY or "").strip()
    default_email = str(MARKET_DATA_CAPITAL_EMAIL or CAPITAL_EMAIL or "").strip()
    default_password = str(MARKET_DATA_CAPITAL_PASSWORD or CAPITAL_PASSWORD or "").strip()
    if str(MARKET_DATA_CAPITAL_IS_DEMO or "").strip() in ("true", "false"):
        default_is_demo = str(MARKET_DATA_CAPITAL_IS_DEMO).strip().lower() == "true"
    else:
        default_is_demo = bool(CAPITAL_IS_DEMO)

    api_key = str(ctx.get("api_key") or default_api_key).strip()
    email = str(ctx.get("email") or default_email).strip()
    password = str(ctx.get("password") or default_password).strip()
    is_demo = bool(ctx.get("is_demo")) if "is_demo" in ctx else bool(default_is_demo)
    base_url = _get_base_url(is_demo)
    cache_key = str(
        ctx.get("cache_key")
        or f"{api_key[:8]}|{email}|{int(bool(is_demo))}"
    ).strip()
    label = str(ctx.get("label") or cache_key or "GLOBAL").strip()
    return {
        "api_key": api_key,
        "email": email,
        "password": password,
        "is_demo": is_demo,
        "base_url": base_url,
        "cache_key": cache_key,
        "label": label,
    }


def _get_market_data_session(session_context: dict | None = None) -> tuple[str | None, dict | None]:
    """
    Authenticate once and reuse short-lived Capital headers for scanner calls.
    """
    ident = _resolve_market_data_identity(session_context)
    api_key = str(ident["api_key"])
    email = str(ident["email"])
    password = str(ident["password"])
    base_url = str(ident["base_url"])
    cache_key = str(ident["cache_key"])
    label = str(ident["label"])

    if not api_key or not email or not password:
        _append_data_quality_log(
            label,
            "auth",
            "ERROR",
            "Missing market-data credentials (api_key/email/password)",
        )
        return None, None

    now_ts = datetime.now(timezone.utc).timestamp()
    cached = _SESSION_CACHE.get(cache_key, {})
    cached_headers = cached.get("headers")
    cached_base = cached.get("base_url")
    expires_ts = float(cached.get("expires_ts") or 0.0)
    if cached_headers and cached_base and now_ts < expires_ts:
        return str(cached_base), dict(cached_headers)

    base_headers = {
        "X-CAP-API-KEY": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        res = requests.post(
            f"{base_url}/session",
            json={
                "identifier": email,
                "password": password,
            },
            headers=base_headers,
            timeout=_DATA_TIMEOUT_SEC,
        )
        if res.status_code != 200:
            txt = (res.text or "").strip()
            _append_data_quality_log(
                label,
                "auth",
                "ERROR",
                f"Capital auth failed status={res.status_code} body={txt[:220]}",
            )
            return None, None
        session_headers = {
            **base_headers,
            "CST": res.headers.get("CST", ""),
            "X-SECURITY-TOKEN": res.headers.get("X-SECURITY-TOKEN", ""),
        }
        _SESSION_CACHE[cache_key] = {
            "headers": session_headers,
            "base_url": base_url,
            "expires_ts": now_ts + float(_SESSION_TTL_SEC),
        }
        return base_url, session_headers
    except Exception as exc:
        _append_data_quality_log(label, "auth", "ERROR", f"Auth exception: {exc}")
        return None, None


def _avg_px(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float, np.number)):
        return float(val)
    if isinstance(val, dict):
        bid = val.get("bid")
        ask = val.get("ask")
        if bid is not None and ask is not None:
            try:
                return (float(bid) + float(ask)) / 2.0
            except Exception:
                pass
        for k in ("mid", "value", "price"):
            if val.get(k) is not None:
                try:
                    return float(val.get(k))
                except Exception:
                    pass
    return None


def _parse_capital_ohlcv(payload: dict) -> pd.DataFrame | None:
    prices = (
        (payload or {}).get("prices")
        or (payload or {}).get("candles")
        or (payload or {}).get("data")
        or []
    )
    rows = []
    for p in prices:
        ts = p.get("snapshotTimeUTC") or p.get("snapshotTime") or p.get("time")
        if not ts:
            continue
        o = _avg_px(p.get("openPrice") or p.get("open"))
        h = _avg_px(p.get("highPrice") or p.get("high"))
        l = _avg_px(p.get("lowPrice") or p.get("low"))
        c = _avg_px(p.get("closePrice") or p.get("close"))
        v = p.get("lastTradedVolume", p.get("volume", 0))
        rows.append(
            {
                "Date": ts,
                "Open": o,
                "High": h,
                "Low": l,
                "Close": c,
                "Volume": v if v is not None else 0,
            }
        )
    if not rows:
        return None
    df = pd.DataFrame(rows)
    # Force UTC-aware parsing so downstream frames are timezone-safe.
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df = df.dropna(subset=["Date"])
    if df.empty:
        return None
    df = df.set_index("Date")
    return df


def _clean_ohlcv(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    step_sec: int,
) -> pd.DataFrame | None:
    """
    Normalize and clean data:
    - NaN / invalid OHLC rows
    - duplicate timestamps
    - timezone normalization to UTC-aware
    - basic gap diagnostics
    """
    if df is None or df.empty:
        _append_data_quality_log(symbol, timeframe, "ERROR", "empty dataframe")
        return None

    work = df.copy()
    raw_rows = len(work)
    if not isinstance(work.index, pd.DatetimeIndex):
        work.index = pd.to_datetime(work.index, errors="coerce")
    work = work[~work.index.isna()]
    if work.empty:
        _append_data_quality_log(symbol, timeframe, "ERROR", "all timestamps invalid")
        return None

    if work.index.tz is None:
        work.index = work.index.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT")
    else:
        work.index = work.index.tz_convert("UTC")
    work = work[~work.index.isna()]

    dup_count = int(work.index.duplicated(keep="last").sum())
    if dup_count:
        work = work[~work.index.duplicated(keep="last")]
    work = work.sort_index()

    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col not in work.columns:
            _append_data_quality_log(
                symbol,
                timeframe,
                "ERROR",
                f"missing required column={col}",
            )
            return None

    work["Open"] = pd.to_numeric(work["Open"], errors="coerce")
    work["High"] = pd.to_numeric(work["High"], errors="coerce")
    work["Low"] = pd.to_numeric(work["Low"], errors="coerce")
    work["Close"] = pd.to_numeric(work["Close"], errors="coerce")
    work["Volume"] = pd.to_numeric(work["Volume"], errors="coerce").fillna(0.0)

    before_nan = len(work)
    work = work.dropna(subset=["Open", "High", "Low", "Close"])
    nan_drop = before_nan - len(work)
    work = work[(work["High"] >= work["Low"]) & (work["Open"] > 0) & (work["Close"] > 0)]
    work["Volume"] = work["Volume"].clip(lower=0.0)
    work = work[["Open", "High", "Low", "Close", "Volume"]]

    if work.empty:
        _append_data_quality_log(
            symbol,
            timeframe,
            "ERROR",
            "cleaning removed all rows",
        )
        return None

    gap_count = 0
    if len(work) >= 3 and step_sec > 0:
        delta_s = (
            work.index.to_series().diff().dt.total_seconds().dropna()
        )
        gap_count = int((delta_s > (step_sec * 2.2)).sum())

    _append_data_quality_log(
        symbol,
        timeframe,
        "OK",
        (
            f"rows_raw={raw_rows} rows_clean={len(work)} dropped_nan={nan_drop} "
            f"dup_removed={dup_count} gap_count={gap_count}"
        ),
    )
    return work


def _resolve_epic(base_url: str, headers: dict, symbol: str) -> str | None:
    s = str(symbol or "").strip().upper()
    if not s:
        return None

    # Keep compatibility with historical alias used elsewhere in the project.
    if s == "PAYP":
        s = "PYPL"

    if s in _EPIC_CACHE:
        return _EPIC_CACHE[s]
    if s in _UNSUPPORTED_CACHE:
        return None

    try:
        direct = requests.get(
            f"{base_url}/markets/{s}",
            headers=headers,
            timeout=_DATA_TIMEOUT_SEC,
        )
        if direct.status_code == 200:
            _EPIC_CACHE[s] = s
            return s
    except Exception:
        pass

    candidates = [f"US.{s}.CASH", f"US.{s}.CFD", f"{s}.CASH", f"{s}.CFD"]
    for epic in candidates:
        try:
            res = requests.get(
                f"{base_url}/markets/{epic}",
                headers=headers,
                timeout=_DATA_TIMEOUT_SEC,
            )
            if res.status_code == 200:
                _EPIC_CACHE[s] = epic
                return epic
        except Exception:
            continue

    try:
        res = requests.get(
            f"{base_url}/markets",
            params={"searchTerm": s},
            headers=headers,
            timeout=_DATA_TIMEOUT_SEC,
        )
        if res.status_code == 200:
            markets = (res.json() or {}).get("markets", []) or []
            best_score = -1
            best_epic = None
            for m in markets:
                epic = str(m.get("epic", "")).upper()
                if not epic:
                    continue
                inst = str(m.get("instrumentName", "")).upper()
                country = str(m.get("countryCode", "")).upper()
                currency = str(m.get("currency", "")).upper()
                market_id = str(m.get("marketId", "")).upper()
                score = 0
                if s in epic or s in inst:
                    score += 60
                if country == "US" or currency == "USD" or ".US." in f".{epic}.":
                    score += 20
                if market_id in ("SHARES", "SHARE"):
                    score += 8
                if score > best_score:
                    best_score = score
                    best_epic = epic
            if best_epic:
                _EPIC_CACHE[s] = best_epic
                return best_epic
    except Exception:
        pass

    _UNSUPPORTED_CACHE.add(s)
    return None


def _fetch_bars(
    base_url: str,
    headers: dict,
    epic: str,
    timeframe: str,
    *,
    log_symbol: str | None = None,
) -> pd.DataFrame | None:
    """
    Fetch OHLCV from Capital.com /prices/{epic} only.
    Retries with larger `max` when cleaned row count is below min_rows (indicators need depth).
    For daily (1d), requires at least MIN_ANALYSIS_BARS rows (default 200); soft band to TARGET
    logs [RECOVERED] via _log_recovered_proceed.
    """
    cfg = _TF_CONFIG[timeframe]
    min_rows = int(cfg["min_rows"])
    label = (log_symbol or epic).strip() or epic
    last_err = ""
    for max_try in _max_bar_attempts(int(cfg["max"])):
        for res_name in cfg["resolutions"]:
            try:
                res = requests.get(
                    f"{base_url}/prices/{epic}",
                    params={"resolution": res_name, "max": int(max_try)},
                    headers=headers,
                    timeout=_DATA_TIMEOUT_SEC,
                )
                if res.status_code != 200:
                    last_err = f"status={res.status_code} resolution={res_name} max={max_try}"
                    continue
                df_raw = _parse_capital_ohlcv(res.json() or {})
                if df_raw is None or df_raw.empty:
                    last_err = f"empty payload resolution={res_name} max={max_try}"
                    continue
                df_clean = _clean_ohlcv(
                    symbol=epic,
                    timeframe=timeframe,
                    df=df_raw,
                    step_sec=int(cfg["step_sec"]),
                )
                if df_clean is None or df_clean.empty:
                    last_err = f"clean failed resolution={res_name} max={max_try}"
                    continue
                n = len(df_clean)
                if n < min_rows:
                    last_err = (
                        f"insufficient rows={n} min={min_rows} "
                        f"resolution={res_name} max={max_try}"
                    )
                    continue
                if timeframe == "1d" and n < MIN_ANALYSIS_BARS:
                    last_err = (
                        f"insufficient rows={n} need>={MIN_ANALYSIS_BARS} "
                        f"resolution={res_name} max={max_try}"
                    )
                    continue
                if timeframe == "1d" and MIN_ANALYSIS_BARS <= n < TARGET_ANALYSIS_BARS:
                    _log_recovered_proceed(label, n, timeframe)
                if max_try > int(cfg["max"]):
                    _append_data_quality_log(
                        epic,
                        timeframe,
                        "OK",
                        f"capital_retry_ok max={max_try} resolution={res_name} rows={n}",
                    )
                return df_clean
            except Exception as exc:
                last_err = f"exception resolution={res_name} max={max_try}: {exc}"

    if timeframe == "1d":
        recovered = _smart_retry_daily_fetch(
            base_url, headers, epic, cfg, log_symbol=label
        )
        if recovered is not None:
            return recovered

    _append_data_quality_log(epic, timeframe, "ERROR", last_err or "unknown fetch error")
    return None


def scan_market(ticker_symbol, period="1d", interval="5m", session_context: dict | None = None):
    """
    Fetch OHLCV data for a single symbol. Kept for backward compatibility.
    Capital.com API only (/prices/{epic}); retries with larger max= if history is short.
    """
    interval_map = {
        "15m": ("MINUTE_15", 900),
        "1h": ("HOUR", 3600),
        "4h": ("HOUR_4", 14400),
        "1d": ("DAY", 86400),
        # backward-compat fallback for existing callers
        "5m": ("MINUTE_15", 900),
    }
    resolution, step_sec = interval_map.get(str(interval).lower(), ("MINUTE_15", 900))
    period_to_days = {
        "1d": 1,
        "5d": 5,
        "1mo": 30,
        "12mo": 365,
        "3mo": 90,
        "6mo": 180,
        "1y": 365,
        "2y": 730,
        "3y": 1095,
        "5y": 1825,
    }
    days = int(period_to_days.get(str(period).lower(), 5))
    base_need = max(20, int((days * 86400) / max(step_sec, 1)) + 10)
    max_attempts = _max_bar_attempts(max(20, min(int(_CAPITAL_PRICES_MAX_BARS_CAP), base_need)))

    base_url, headers = _get_market_data_session(session_context=session_context)
    if not base_url or not headers:
        return None
    epic = _resolve_epic(base_url, headers, ticker_symbol)
    if not epic:
        _append_data_quality_log(
            str(ticker_symbol),
            str(interval),
            "ERROR",
            "epic resolution failed",
        )
        return None

    sym = str(ticker_symbol).strip()
    is_daily = str(interval).lower() == "1d"
    daily_resolutions = ("DAY", "D1", "DAY_1")
    resolutions_try = daily_resolutions if is_daily else (resolution,)

    try:
        last_status = 0
        for max_bars in max_attempts:
            for res_one in resolutions_try:
                res = requests.get(
                    f"{base_url}/prices/{epic}",
                    params={"resolution": res_one, "max": int(max_bars)},
                    headers=headers,
                    timeout=_DATA_TIMEOUT_SEC,
                )
                last_status = int(res.status_code)
                if res.status_code != 200:
                    continue
                parsed = _parse_capital_ohlcv(res.json() or {})
                if parsed is None or parsed.empty:
                    continue
                cleaned = _clean_ohlcv(
                    symbol=str(ticker_symbol),
                    timeframe=str(interval),
                    df=parsed,
                    step_sec=step_sec,
                )
                if cleaned is None or cleaned.empty:
                    continue
                n = len(cleaned)
                if is_daily:
                    if n < MIN_ANALYSIS_BARS:
                        continue
                    if MIN_ANALYSIS_BARS <= n < TARGET_ANALYSIS_BARS:
                        _log_recovered_proceed(sym, n, str(interval))
                    if max_bars > base_need:
                        _append_data_quality_log(
                            str(ticker_symbol),
                            str(interval),
                            "OK",
                            f"capital_retry_ok max={max_bars} res={res_one} rows={n}",
                        )
                    return cleaned
                if max_bars > base_need:
                    _append_data_quality_log(
                        str(ticker_symbol),
                        str(interval),
                        "OK",
                        f"capital_retry_ok max={max_bars} rows={n}",
                    )
                return cleaned

        if is_daily:
            cfg_1d = _TF_CONFIG["1d"]
            recovered = _smart_retry_daily_fetch(
                base_url, headers, epic, cfg_1d, log_symbol=sym
            )
            if recovered is not None:
                return recovered

        _append_data_quality_log(
            str(ticker_symbol),
            str(interval),
            "ERROR",
            f"insufficient Capital.com history after retries (last_http={last_status})",
        )
        return None
    except Exception as exc:
        print(f"❌ Scanner error [{ticker_symbol}]: {exc}")
        return None


def scan_multi_timeframe(symbol, session_context: dict | None = None):
    """
    Fetch three native analysis timeframes (no resampling):
      - 1D  : master trend
      - 4H  : structure context
      - 15M : entry signal

    Returns dict {'1d': df, '4h': df, '15m': df} or None on failure.
    """
    try:
        base_url, headers = _get_market_data_session(session_context=session_context)
        if not base_url or not headers:
            _append_data_quality_log(symbol, "all", "ERROR", "session unavailable")
            return None

        epic = _resolve_epic(base_url, headers, symbol)
        if not epic:
            _append_data_quality_log(symbol, "all", "ERROR", "epic not found on Capital")
            return None

        df_1d = _fetch_bars(base_url, headers, epic, "1d", log_symbol=str(symbol))
        df_4h = _fetch_bars(base_url, headers, epic, "4h", log_symbol=str(symbol))
        df_15m = _fetch_bars(base_url, headers, epic, "15m", log_symbol=str(symbol))

        if any(df is None or df.empty for df in (df_1d, df_4h, df_15m)):
            _append_data_quality_log(
                symbol,
                "all",
                "ERROR",
                "one or more timeframes missing after fetch/clean",
            )
            return None

        _append_data_quality_log(
            symbol,
            "all",
            "OK",
            (
                f"epic={epic} rows_1d={len(df_1d)} "
                f"rows_4h={len(df_4h)} rows_15m={len(df_15m)}"
            ),
        )
        return {"1d": df_1d, "4h": df_4h, "15m": df_15m}
    except Exception as exc:
        _append_data_quality_log(symbol, "all", "ERROR", f"scan_multi_timeframe exception: {exc}")
        print(f"❌ خطأ في جلب البيانات متعددة الأطر [{symbol}]: {exc}")
        return None
