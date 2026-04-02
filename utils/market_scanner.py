"""
Capital.com OHLCV via aiohttp. Watchlist Level 2 gap filter calls `scan_market_async`
with a shared session and `CAPITAL_HTTP_CONCURRENCY` (see utils.filters.level2_filter_async).
"""
import os
import time
import asyncio
from datetime import datetime, timezone

import aiohttp
from multidict import CIMultiDictProxy
import numpy as np
import pandas as pd

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
    MIN_15M_BARS,
    TARGET_ANALYSIS_BARS,
)

_DATA_TIMEOUT_SEC = int(os.getenv("MARKET_DATA_TIMEOUT_SEC", "20"))
_SESSION_TTL_SEC = int(os.getenv("MARKET_DATA_SESSION_TTL_SEC", "45"))
_LOG_ROOT = os.getenv("ENGINE_LOG_ROOT", "logs")
_CAPITAL_PRICES_MAX_BARS_CAP = int(os.getenv("CAPITAL_PRICES_MAX_BARS_CAP", "1000"))
_DAILY_PRICES_MAX = min(1000, max(50, _CAPITAL_PRICES_MAX_BARS_CAP))
CAPITAL_HTTP_CONCURRENCY = max(1, int(os.getenv("CAPITAL_HTTP_CONCURRENCY", "3")))
# Pause between per-ticker Capital calls in bulk filters (Level 2/3) to reduce HTTP 429.
BULK_TICKER_REQUEST_SLEEP_SEC = float(os.getenv("BULK_TICKER_REQUEST_SLEEP_SEC", "0.1"))
# Hard cap per HTTP request during bulk NASDAQ filters (Level 2/3) so one slow call cannot stall the batch.
BULK_HTTP_REQUEST_TIMEOUT_SEC = float(os.getenv("BULK_HTTP_REQUEST_TIMEOUT_SEC", "5.0"))
BULK_CAPITAL_CLIENT_TIMEOUT = aiohttp.ClientTimeout(total=BULK_HTTP_REQUEST_TIMEOUT_SEC)
# Upper bound for one Level-2 gap check (fetch + parse), slightly above HTTP timeout.
BULK_GAP_ONE_TIMEOUT_SEC = float(os.getenv("BULK_GAP_ONE_TIMEOUT_SEC", "8.0"))
# Level 2/3: parallel batch size + pause between batches (anti-429).
BULK_PARALLEL_BATCH_SIZE = max(1, int(os.getenv("BULK_PARALLEL_BATCH_SIZE", "5")))
BULK_BATCH_GAP_SLEEP_SEC = float(os.getenv("BULK_BATCH_GAP_SLEEP_SEC", "0.5"))

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
        # Request enough history for 100+ clean bars (RSI, SMA50, volume MA20).
        "max": max(800, int(os.getenv("CAPITAL_15M_MAX_BARS_REQUEST", "1000"))),
        "step_sec": 900,
        "min_rows": max(80, int(MIN_15M_BARS)),
    },
}

_AIO_TIMEOUT = aiohttp.ClientTimeout(total=_DATA_TIMEOUT_SEC)
# Shared aiohttp ClientSession timeout for batch scans (signal_engine).
CAPITAL_CLIENT_TIMEOUT = _AIO_TIMEOUT


def _new_http_semaphore() -> asyncio.Semaphore:
    return asyncio.Semaphore(CAPITAL_HTTP_CONCURRENCY)


async def sleep_between_bulk_tickers() -> None:
    """Throttle bulk NASDAQ filter scans (async)."""
    await asyncio.sleep(BULK_TICKER_REQUEST_SLEEP_SEC)


def sleep_between_bulk_tickers_sync() -> None:
    """Throttle bulk NASDAQ filter scans (sync / thread workers)."""
    time.sleep(BULK_TICKER_REQUEST_SLEEP_SEC)


def clear_local_price_caches() -> None:
    _SESSION_CACHE.clear()
    _EPIC_CACHE.clear()
    _UNSUPPORTED_CACHE.clear()


def _max_bar_attempts(base_max: int) -> list[int]:
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
    _append_data_quality_log(
        symbol,
        timeframe,
        "OK",
        f"[RECOVERED] Proceeding with {bar_count} bars for {symbol}",
    )


def _append_data_quality_log(symbol: str, timeframe: str, status: str, details: str):
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


async def _aio_get(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    *,
    headers: dict,
    params: dict | None = None,
) -> tuple[int, dict | list | None]:
    """GET JSON with semaphore, extra backoff on 429, and a short retry on transient errors."""
    max_attempts = 5
    for attempt in range(max_attempts):
        async with sem:
            try:
                async with session.get(url, headers=headers, params=params) as resp:
                    status = int(resp.status)
                    if status == 200:
                        try:
                            data = await resp.json()
                        except Exception:
                            data = None
                        return status, data
                    if status == 429 and attempt < max_attempts - 1:
                        sleep_s = min(3.0, 0.4 + attempt * 0.35)
                        print(
                            f"[API] Rate limit hit. Sleeping for {sleep_s:.2f} seconds.",
                            flush=True,
                        )
                        await asyncio.sleep(sleep_s)
                        continue
                    if attempt == 0:
                        await asyncio.sleep(0.12)
                        continue
                    return status, None
            except Exception:
                if attempt == 0:
                    await asyncio.sleep(0.12)
                    continue
                return 0, None
    return 0, None


async def _aio_post_json(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    *,
    headers: dict,
    json_body: dict,
) -> tuple[int, CIMultiDictProxy | dict, dict | None]:
    """POST JSON; returns (status, response_headers, body or None)."""
    for attempt in range(2):
        async with sem:
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json=json_body,
                ) as resp:
                    status = int(resp.status)
                    try:
                        data = await resp.json() if status == 200 else None
                    except Exception:
                        data = None
                    return status, resp.headers, data
            except Exception:
                if attempt == 0:
                    await asyncio.sleep(0.12)
                    continue
                return 0, {}, None
    return 0, {}, None


async def _get_market_data_session_aio(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    session_context: dict | None = None,
) -> tuple[str | None, dict | None]:
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
        st, hdrs, _ = await _aio_post_json(
            session,
            sem,
            f"{base_url}/session",
            headers=base_headers,
            json_body={"identifier": email, "password": password},
        )
        if st != 200:
            txt = ""
            _append_data_quality_log(
                label,
                "auth",
                "ERROR",
                f"Capital auth failed status={st} body={txt[:220]}",
            )
            return None, None
        session_headers = {
            **base_headers,
            "CST": hdrs.get("CST", "") if hasattr(hdrs, "get") else "",
            "X-SECURITY-TOKEN": hdrs.get("X-SECURITY-TOKEN", "") if hasattr(hdrs, "get") else "",
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


async def _resolve_epic_aio(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    base_url: str,
    headers: dict,
    symbol: str,
) -> str | None:
    s = str(symbol or "").strip().upper()
    if not s:
        return None
    if s == "PAYP":
        s = "PYPL"

    if s in _EPIC_CACHE:
        return _EPIC_CACHE[s]
    if s in _UNSUPPORTED_CACHE:
        return None

    st, data = await _aio_get(session, sem, f"{base_url}/markets/{s}", headers=headers)
    if st == 200 and data is not None:
        _EPIC_CACHE[s] = s
        return s

    candidates = [f"US.{s}.CASH", f"US.{s}.CFD", f"{s}.CASH", f"{s}.CFD"]
    for epic in candidates:
        st2, data2 = await _aio_get(
            session, sem, f"{base_url}/markets/{epic}", headers=headers
        )
        if st2 == 200 and data2 is not None:
            _EPIC_CACHE[s] = epic
            return epic

    st3, data3 = await _aio_get(
        session,
        sem,
        f"{base_url}/markets",
        headers=headers,
        params={"searchTerm": s},
    )
    if st3 == 200 and isinstance(data3, dict):
        markets = data3.get("markets", []) or []
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

    _UNSUPPORTED_CACHE.add(s)
    return None


async def _smart_retry_daily_fetch_aio(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    base_url: str,
    headers: dict,
    epic: str,
    cfg: dict,
    log_symbol: str,
) -> pd.DataFrame | None:
    cap = min(1000, max(50, int(_CAPITAL_PRICES_MAX_BARS_CAP)))
    resolutions = list(cfg["resolutions"])
    alt_maxes = (cap, max(MIN_ANALYSIS_BARS, int(round(cap * 0.92))))
    last_err = ""
    for max_try in alt_maxes:
        for res_name in reversed(resolutions):
            try:
                st, payload = await _aio_get(
                    session,
                    sem,
                    f"{base_url}/prices/{epic}",
                    headers=headers,
                    params={"resolution": res_name, "max": int(max_try)},
                )
                if st != 200 or not isinstance(payload, dict):
                    last_err = f"smart_retry status={st} res={res_name} max={max_try}"
                    continue
                df_raw = _parse_capital_ohlcv(payload)
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


async def _fetch_bars_aio(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    base_url: str,
    headers: dict,
    epic: str,
    timeframe: str,
    *,
    log_symbol: str | None = None,
) -> pd.DataFrame | None:
    cfg = _TF_CONFIG[timeframe]
    min_rows = int(cfg["min_rows"])
    label = (log_symbol or epic).strip() or epic
    last_err = ""
    for max_try in _max_bar_attempts(int(cfg["max"])):
        for res_name in cfg["resolutions"]:
            try:
                st, payload = await _aio_get(
                    session,
                    sem,
                    f"{base_url}/prices/{epic}",
                    headers=headers,
                    params={"resolution": res_name, "max": int(max_try)},
                )
                if st != 200 or not isinstance(payload, dict):
                    last_err = f"status={st} resolution={res_name} max={max_try}"
                    continue
                df_raw = _parse_capital_ohlcv(payload)
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
        recovered = await _smart_retry_daily_fetch_aio(
            session, sem, base_url, headers, epic, cfg, log_symbol=label
        )
        if recovered is not None:
            return recovered

    _append_data_quality_log(epic, timeframe, "ERROR", last_err or "unknown fetch error")
    return None


async def scan_market_async(
    ticker_symbol,
    period="1d",
    interval="5m",
    session_context: dict | None = None,
    *,
    session: aiohttp.ClientSession | None = None,
    semaphore: asyncio.Semaphore | None = None,
    client_timeout: aiohttp.ClientTimeout | None = None,
) -> pd.DataFrame | None:
    interval_map = {
        "15m": ("MINUTE_15", 900),
        "1h": ("HOUR", 3600),
        "4h": ("HOUR_4", 14400),
        "1d": ("DAY", 86400),
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

    close_local = session is None
    if session is None:
        session = aiohttp.ClientSession(timeout=(client_timeout or _AIO_TIMEOUT))
    if semaphore is None:
        semaphore = _new_http_semaphore()

    try:
        base_url, headers = await _get_market_data_session_aio(session, semaphore, session_context)
        if not base_url or not headers:
            return None
        epic = await _resolve_epic_aio(session, semaphore, base_url, headers, ticker_symbol)
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

        last_status = 0
        for max_bars in max_attempts:
            for res_one in resolutions_try:
                st, payload = await _aio_get(
                    session,
                    semaphore,
                    f"{base_url}/prices/{epic}",
                    headers=headers,
                    params={"resolution": res_one, "max": int(max_bars)},
                )
                last_status = int(st)
                if st != 200 or not isinstance(payload, dict):
                    continue
                parsed = _parse_capital_ohlcv(payload)
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
            recovered = await _smart_retry_daily_fetch_aio(
                session, semaphore, base_url, headers, epic, cfg_1d, log_symbol=sym
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
    finally:
        if close_local:
            await session.close()


def scan_market(
    ticker_symbol,
    period="1d",
    interval="5m",
    session_context: dict | None = None,
    *,
    client_timeout: aiohttp.ClientTimeout | None = None,
):
    """Sync wrapper: async Capital.com OHLCV fetch with aiohttp (concurrency capped internally)."""

    async def _run():
        to = client_timeout if client_timeout is not None else _AIO_TIMEOUT
        async with aiohttp.ClientSession(timeout=to) as sess:
            sem = _new_http_semaphore()
            return await scan_market_async(
                ticker_symbol,
                period,
                interval,
                session_context,
                session=sess,
                semaphore=sem,
            )

    return asyncio.run(_run())


async def scan_multi_timeframe_async(
    symbol,
    session_context: dict | None = None,
    *,
    session: aiohttp.ClientSession | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> dict | None:
    """
    Async multi-timeframe fetch. Pass shared session+semaphore from scan batches for efficiency.
    """
    close_local = session is None
    if session is None:
        session = aiohttp.ClientSession(timeout=_AIO_TIMEOUT)
    if semaphore is None:
        semaphore = _new_http_semaphore()

    try:
        base_url, headers = await _get_market_data_session_aio(session, semaphore, session_context)
        if not base_url or not headers:
            _append_data_quality_log(symbol, "all", "ERROR", "session unavailable")
            return None

        epic = await _resolve_epic_aio(session, semaphore, base_url, headers, symbol)
        if not epic:
            _append_data_quality_log(
                symbol,
                "all",
                "ERROR",
                "mapping_error: epic not found on Capital (symbol not mapped)",
            )
            return None

        raw_1d, raw_4h, raw_15m = await asyncio.gather(
            _fetch_bars_aio(
                session, semaphore, base_url, headers, epic, "1d", log_symbol=str(symbol)
            ),
            _fetch_bars_aio(
                session, semaphore, base_url, headers, epic, "4h", log_symbol=str(symbol)
            ),
            _fetch_bars_aio(
                session, semaphore, base_url, headers, epic, "15m", log_symbol=str(symbol)
            ),
            return_exceptions=True,
        )

        def _unwrap(res: object, tf: str) -> pd.DataFrame | None:
            if isinstance(res, Exception):
                _append_data_quality_log(
                    symbol,
                    tf,
                    "ERROR",
                    f"fetch_exception: {res!s}",
                )
                return None
            return res  # type: ignore[return-value]

        df_1d = _unwrap(raw_1d, "1d")
        df_4h = _unwrap(raw_4h, "4h")
        df_15m = _unwrap(raw_15m, "15m")

        # 15m is mandatory for intraday strategies; allow partial 1d/4h (e.g. < MIN_ANALYSIS_BARS daily history).
        if df_15m is None or df_15m.empty:
            _append_data_quality_log(
                symbol,
                "15m",
                "WARN",
                "15m empty after primary fetch — retry once",
            )
            df_15m = await _fetch_bars_aio(
                session,
                semaphore,
                base_url,
                headers,
                epic,
                "15m",
                log_symbol=str(symbol),
            )

        if df_15m is None or df_15m.empty:
            _append_data_quality_log(
                symbol,
                "all",
                "ERROR",
                "15m_unavailable: no usable 15m data after retry (API limit, no history, or cleaning removed all rows)",
            )
            return None

        if df_1d is None or df_1d.empty:
            df_1d = pd.DataFrame()
            _append_data_quality_log(
                symbol,
                "1d",
                "WARN",
                "1d missing or insufficient — continuing with empty 1d (listing history / MIN_ANALYSIS_BARS)",
            )
        if df_4h is None or df_4h.empty:
            df_4h = pd.DataFrame()
            _append_data_quality_log(
                symbol,
                "4h",
                "WARN",
                "4h missing — continuing with empty 4h",
            )

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
    finally:
        if close_local:
            await session.close()


async def fetch_15m_ohlcv_async(
    symbol: str,
    session_context: dict | None = None,
    *,
    session: aiohttp.ClientSession | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> tuple[pd.DataFrame | None, str | None]:
    """
    Epic resolution + 15m bars only (same depth rules as multi-TF scan).
    For diagnostics and tools that do not need 1d/4h — fewer API calls per symbol.

    Returns (dataframe, None) on success, (None, reason) where reason is one of:
      mapping_error, session_error, 15m_unavailable
    """
    close_local = session is None
    if session is None:
        session = aiohttp.ClientSession(timeout=_AIO_TIMEOUT)
    if semaphore is None:
        semaphore = _new_http_semaphore()
    try:
        base_url, headers = await _get_market_data_session_aio(session, semaphore, session_context)
        if not base_url or not headers:
            return None, "session_error"
        epic = await _resolve_epic_aio(session, semaphore, base_url, headers, symbol)
        if not epic:
            return None, "mapping_error"
        df_15m = await _fetch_bars_aio(
            session, semaphore, base_url, headers, epic, "15m", log_symbol=str(symbol)
        )
        if df_15m is None or df_15m.empty:
            df_15m = await _fetch_bars_aio(
                session, semaphore, base_url, headers, epic, "15m", log_symbol=str(symbol)
            )
        if df_15m is None or df_15m.empty:
            return None, "15m_unavailable"
        return df_15m, None
    except Exception as exc:
        _append_data_quality_log(str(symbol), "15m", "ERROR", f"fetch_15m_ohlcv_async: {exc}")
        return None, "15m_unavailable"
    finally:
        if close_local:
            await session.close()


def scan_multi_timeframe(symbol, session_context: dict | None = None):
    """Sync wrapper around async multi-timeframe fetch."""

    async def _run():
        async with aiohttp.ClientSession(timeout=_AIO_TIMEOUT) as sess:
            sem = _new_http_semaphore()
            return await scan_multi_timeframe_async(
                symbol, session_context, session=sess, semaphore=sem
            )

    return asyncio.run(_run())
