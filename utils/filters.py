import asyncio
import logging
import aiohttp
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.market_scanner import (
    scan_market,
    scan_market_async,
    BULK_BATCH_GAP_SLEEP_SEC,
    BULK_CAPITAL_CLIENT_TIMEOUT,
    BULK_GAP_ONE_TIMEOUT_SEC,
    BULK_PARALLEL_BATCH_SIZE,
    CAPITAL_HTTP_CONCURRENCY,
)
from config import EARNINGS_TIMEOUT_SEC, EARNINGS_CACHE_TTL_SEC, FMP_API_KEY

log = logging.getLogger(__name__)

# --- Thresholds ---
MIN_AVG_VOLUME       = 600_000        # 600k shares/day (40% lower to admit more mid-caps)
MIN_MARKET_CAP       = 300_000_000    # $300M
MAX_GAP_PCT          = 0.02           # 2% max gap
EARNINGS_BUFFER_DAYS = 2              # exclude if earnings within 2 days

# ── Level 3 — Price / Daily Range filter ─────────────────────────────────
# Goal: prefer "cheap" tickers with controlled intraday movement.
# We estimate daily movement using (High - Low) / Close.
MIN_PRICE            = 5.00          # $5 minimum
MIN_DAILY_RANGE_PCT = 0.20          # 0.2%
MAX_DAILY_RANGE_PCT = 5.00          # 5.0%
DAILY_RANGE_LOOKBACK_DAYS = 3         # average over last N sessions

# Module-level cache: populated by get_nasdaq_tickers() when screener API works.
# { symbol: {'avg_volume': float, 'market_cap': float} }
_screener_cache: dict = {}
_earnings_cache: dict = {}
_market_cap_cache: dict = {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_market_cap(raw: str) -> float:
    """Convert screener strings like '$3.40T', '$500.2M', '$1.23B' to float."""
    if not raw or raw in ('', 'NA', 'N/A'):
        return 0.0
    s = raw.replace('$', '').replace(',', '').strip()
    multipliers = {'T': 1e12, 'B': 1e9, 'M': 1e6, 'K': 1e3}
    for suffix, mult in multipliers.items():
        if s.upper().endswith(suffix):
            try:
                return float(s[:-1]) * mult
            except ValueError:
                return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _valid_symbol(sym) -> bool:
    return (
        isinstance(sym, str)
        and sym.replace('-', '').replace('.', '').isalpha()
        and 1 <= len(sym) <= 5
    )


def _safe_volume_mean(df_like) -> float:
    """
    Return a scalar mean volume from a daily OHLCV frame.
    """
    try:
        vol = df_like["Volume"]
    except Exception:
        return 0.0
    try:
        # Series -> scalar
        if isinstance(vol, pd.Series):
            return float(pd.to_numeric(vol, errors="coerce").mean())
        # DataFrame (ambiguous in boolean contexts) -> collapse to scalar
        if isinstance(vol, pd.DataFrame):
            numeric = vol.apply(pd.to_numeric, errors="coerce")
            return float(numeric.to_numpy(dtype=float).mean())
        return float(pd.to_numeric(vol, errors="coerce").mean())
    except Exception:
        return 0.0


def _fetch_daily(symbol: str, days: int = 5) -> pd.DataFrame | None:
    """
    Fetch daily bars from the unified scanner provider (Capital-backed).
    """
    period = "5d" if int(days) <= 5 else "1mo"
    try:
        df = scan_market(
            symbol,
            period=period,
            interval="1d",
            client_timeout=BULK_CAPITAL_CLIENT_TIMEOUT,
        )
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def _daterange(start_day, end_day):
    day = start_day
    while day <= end_day:
        yield day
        day = day + timedelta(days=1)


def _fetch_earnings_symbols_nasdaq(start_day, end_day) -> set[str]:
    """
    Fetch earnings symbols from NASDAQ earnings calendar for a date range.
    """
    symbols: set[str] = set()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nasdaq.com/",
    }
    for day in _daterange(start_day, end_day):
        try:
            res = requests.get(
                "https://api.nasdaq.com/api/calendar/earnings",
                params={"date": str(day)},
                headers=headers,
                timeout=EARNINGS_TIMEOUT_SEC,
            )
            if res.status_code != 200:
                continue
            rows = (res.json() or {}).get("data", {}).get("rows", []) or []
            for row in rows:
                sym = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
                if _valid_symbol(sym):
                    symbols.add(sym)
        except Exception:
            continue
    return symbols


def _fetch_earnings_symbols_fmp(start_day, end_day) -> set[str]:
    """
    Optional fallback provider (FinancialModelingPrep) if FMP_API_KEY exists.
    """
    if not FMP_API_KEY:
        return set()
    try:
        res = requests.get(
            "https://financialmodelingprep.com/api/v3/earning_calendar",
            params={"from": str(start_day), "to": str(end_day), "apikey": FMP_API_KEY},
            timeout=EARNINGS_TIMEOUT_SEC,
        )
        if res.status_code != 200:
            return set()
        rows = res.json() or []
        out: set[str] = set()
        for row in rows:
            sym = str(row.get("symbol") or "").strip().upper()
            if _valid_symbol(sym):
                out.add(sym)
        return out
    except Exception:
        return set()


def _get_near_earnings_symbols(today, earnings_cutoff) -> set[str]:
    """
    Cached list of symbols reporting earnings in [today, cutoff].
    """
    cache_key = f"{today.isoformat()}::{earnings_cutoff.isoformat()}"
    now_ts = datetime.now(timezone.utc).timestamp()
    cached = _earnings_cache.get(cache_key)
    if cached and float(cached.get("expires_at", 0.0)) > now_ts:
        return set(cached.get("symbols") or set())

    symbols = _fetch_earnings_symbols_nasdaq(today, earnings_cutoff)
    if not symbols:
        symbols = _fetch_earnings_symbols_fmp(today, earnings_cutoff)

    _earnings_cache[cache_key] = {
        "expires_at": now_ts + float(EARNINGS_CACHE_TTL_SEC),
        "symbols": set(symbols),
    }
    return symbols


def _extract_market_cap(value) -> float:
    """
    Recursively extract market-cap-like values from nested payloads.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) if float(value) > 0 else 0.0
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return 0.0
        # Supports "$3.2T", "450000000", etc.
        cap = _parse_market_cap(s)
        return cap if cap > 0 else 0.0
    if isinstance(value, list):
        for item in value:
            cap = _extract_market_cap(item)
            if cap > 0:
                return cap
        return 0.0
    if isinstance(value, dict):
        # Prefer explicit keys first.
        for k in ("marketCap", "market_cap", "marketcap", "MarketCap", "value"):
            if k in value:
                cap = _extract_market_cap(value.get(k))
                if cap > 0:
                    return cap
        # Then scan all key/value pairs heuristically.
        for k, v in value.items():
            lk = str(k).lower().replace(" ", "").replace("_", "")
            if "marketcap" in lk or lk == "cap":
                cap = _extract_market_cap(v)
                if cap > 0:
                    return cap
        for _, v in value.items():
            cap = _extract_market_cap(v)
            if cap > 0:
                return cap
    return 0.0


def _fetch_market_cap_nasdaq(symbol: str) -> float:
    """
    Try NASDAQ quote/company endpoints for market cap.
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nasdaq.com/",
    }
    urls = [
        f"https://api.nasdaq.com/api/quote/{symbol}/info?assetclass=stocks",
        f"https://api.nasdaq.com/api/company/{symbol}/company-profile",
    ]
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=EARNINGS_TIMEOUT_SEC)
            if res.status_code != 200:
                continue
            payload = res.json() or {}
            cap = _extract_market_cap(payload)
            if cap > 0:
                return cap
        except Exception:
            continue
    return 0.0


def _fetch_market_cap_fmp(symbol: str) -> float:
    """
    Optional fallback market-cap source via FMP profile endpoint.
    """
    if not FMP_API_KEY:
        return 0.0
    try:
        res = requests.get(
            f"https://financialmodelingprep.com/api/v3/profile/{symbol}",
            params={"apikey": FMP_API_KEY},
            timeout=EARNINGS_TIMEOUT_SEC,
        )
        if res.status_code != 200:
            return 0.0
        rows = res.json() or []
        if not rows:
            return 0.0
        cap = _extract_market_cap(rows[0])
        return cap if cap > 0 else 0.0
    except Exception:
        return 0.0


def _fetch_market_cap(symbol: str) -> float:
    """
    Resolve market cap with caching:
    1) Screener cache (if populated)
    2) NASDAQ quote/company endpoints
    3) Optional FMP profile fallback
    """
    sym = str(symbol or "").upper().strip()
    if not _valid_symbol(sym):
        return 0.0
    if sym in _market_cap_cache:
        return float(_market_cap_cache.get(sym) or 0.0)

    cached = _screener_cache.get(sym, {})
    cap0 = float(cached.get("market_cap", 0.0) or 0.0)
    if cap0 > 0:
        _market_cap_cache[sym] = cap0
        return cap0

    cap = _fetch_market_cap_nasdaq(sym)
    if cap <= 0:
        cap = _fetch_market_cap_fmp(sym)
    _market_cap_cache[sym] = float(cap or 0.0)
    return float(cap or 0.0)


# ── Ticker fetch ──────────────────────────────────────────────────────────────

def get_nasdaq_tickers() -> list[str]:
    """
    Fetch Nasdaq common-stock tickers and populate _screener_cache with
    pre-built volume + market-cap data so Level 1 needs zero extra provider calls.

    Source 1: NASDAQ Screener API  (fast, has volume + cap)
    Source 2: nasdaqtrader.com TXT (fallback, symbols only)
    Source 3: hardcoded top-100    (offline last resort)
    """
    global _screener_cache

    # ── Source 1: NASDAQ Screener API ────────────────────────────────────────
    try:
        res = requests.get(
            "https://api.nasdaq.com/api/screener/stocks",
            params={"tableonly": "true", "exchange": "nasdaq", "download": "true"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
        if res.status_code == 200:
            rows    = res.json().get("data", {}).get("rows", []) or []
            tickers = []
            for r in rows:
                sym = r.get("symbol", "")
                if not _valid_symbol(sym):
                    continue
                volume = 0.0
                try:
                    volume = float(str(r.get("volume", "0")).replace(",", "") or 0)
                except (ValueError, TypeError):
                    pass
                cap = _parse_market_cap(str(r.get("marketCap", "") or ""))
                _screener_cache[sym] = {"avg_volume": volume, "market_cap": cap}
                tickers.append(sym)

            if tickers:
                print(f"📋 جلب {len(tickers)} سهم من NASDAQ API (بيانات السيولة والقيمة السوقية مخزّنة)")
                return tickers
    except Exception as e:
        print(f"⚠️  NASDAQ API: {e}")

    # ── Source 2: nasdaqtrader.com TXT ───────────────────────────────────────
    try:
        res = requests.get(
            "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
            timeout=20,
        )
        res.raise_for_status()
        df      = pd.read_csv(StringIO(res.text), sep="|")
        df      = df[df["Symbol"].apply(_valid_symbol)]
        df      = df[df["ETF"] == "N"]
        tickers = df["Symbol"].tolist()
        if tickers:
            print(f"📋 جلب {len(tickers)} سهم من nasdaqtrader.com (بدون بيانات سيولة مسبقة)")
            return tickers
    except Exception as e:
        print(f"⚠️  nasdaqtrader.com: {e}")

    # ── Source 3: hardcoded fallback ─────────────────────────────────────────
    print("⚠️  استخدام القائمة الاحتياطية المدمجة (100 سهم)")
    return [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
        "NFLX", "AMD", "ADBE", "QCOM", "PEP", "INTC", "INTU", "AMAT", "MU", "PANW",
        "LRCX", "SNPS", "KLAC", "MRVL", "ASML", "CDNS", "REGN", "MDLZ", "GILD", "ADI",
        "PYPL", "ISRG", "VRTX", "ABNB", "CSX", "FTNT", "MELI", "NXPI", "ORLY", "PCAR",
        "CTAS", "DXCM", "MAR", "ADP", "TEAM", "WDAY", "CHTR", "ROST", "KDP", "FAST",
        "ODFL", "VRSK", "BIIB", "MNST", "CPRT", "IDXX", "CRWD", "ANSS", "AEP", "XEL",
        "EXC", "PAYX", "SGEN", "DLTR", "WBA", "EBAY", "SIRI", "NTAP", "SWKS", "XLNX",
        "SPLK", "ZBRA", "TTWO", "WDC", "NTES", "JD", "PDD", "BIDU", "ZM", "DOCU",
        "OKTA", "DDOG", "NET", "SNOW", "PLTR", "RIVN", "LCID", "COIN", "HOOD", "RBLX",
        "U", "AFRM", "UPST", "PATH", "S", "ION", "IONQ", "SMCI", "ARM", "APP",
    ]


# ── Level 1 filter ────────────────────────────────────────────────────────────

def level1_filter(tickers: list[str], top_n: int = 300) -> list[str]:
    """
    Level 1 — Macro filter: volume >= 1M/day AND market cap >= $300M.

    Fast path  : uses data already cached from the screener API response.
    Slow path  : falls back to provider-backed OHLCV volume checks.
    """
    print(f"📊 المستوى 1: تصفية {len(tickers)} سهم بالسيولة والقيمة السوقية...")

    # ── Fast path: screener cache available ───────────────────────────────────
    if _screener_cache:
        qualified       = []
        unknown_volume  = []   # cap OK but screener reported volume=0 (pre-market / missing)

        for sym in tickers:
            cache = _screener_cache.get(sym, {})
            vol   = cache.get("avg_volume", 0)
            cap   = cache.get("market_cap", 0)

            if cap < MIN_MARKET_CAP:
                continue                      # definitively too small
            if vol >= MIN_AVG_VOLUME:
                qualified.append(sym)         # passes both checks
            elif vol == 0:
                unknown_volume.append(sym)    # volume unknown — check via provider

        # For symbols with missing screener volume, check last 5 daily bars.
        if unknown_volume:
            print(f"   ({len(unknown_volume)} سهم بدون بيانات حجم في الـ API — جاري التحقق عبر مزود البيانات...)")
            with ThreadPoolExecutor(max_workers=16) as pool:
                futures = {pool.submit(_fetch_daily, sym, 5): sym for sym in unknown_volume}
                for fut in as_completed(futures):
                    sym = futures[fut]
                    try:
                        hist = fut.result()
                        if hist is not None and _safe_volume_mean(hist) >= MIN_AVG_VOLUME:
                            qualified.append(sym)
                    except Exception:
                        continue

        qualified.sort(
            key=lambda s: float(_screener_cache.get(s, {}).get("avg_volume", 0) or 0),
            reverse=True,
        )
        print(f"✅ المستوى 1: اجتاز {len(qualified)} سهم (من بيانات الـ API — بدون تنزيل إضافي)")
        return qualified[:top_n]

    # ── Slow path: provider-only fallback ─────────────────────────────────────
    print("   (لا توجد بيانات مخزّنة — جاري الفحص عبر مزود البيانات، قد يستغرق وقتاً...)")
    volume_qualified = []
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_fetch_daily, sym, 5): sym for sym in tickers}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                hist = fut.result()
                if hist is not None and _safe_volume_mean(hist) >= MIN_AVG_VOLUME:
                    volume_qualified.append(sym)
            except Exception:
                continue

    # Apply market-cap filter with provider lookup (cached).
    # If market cap cannot be resolved, degrade safely (do not penalize).
    final: list[str] = []
    unresolved = 0
    for sym in list(dict.fromkeys(volume_qualified)):
        try:
            cap = _fetch_market_cap(sym)
            if cap <= 0:
                unresolved += 1
                final.append(sym)
                continue
            if cap >= MIN_MARKET_CAP:
                final.append(sym)
        except Exception:
            unresolved += 1
            final.append(sym)

    if unresolved:
        print(f"   ({unresolved} سهم تعذر جلب القيمة السوقية له — تم تمريره بدون عقوبة)")
    final.sort(
        key=lambda s: float(_screener_cache.get(s, {}).get("avg_volume", 0) or 0),
        reverse=True,
    )
    print(f"✅ المستوى 1: اجتاز {len(final)} سهم")
    return final[:top_n]


# ── Level 2 filter ────────────────────────────────────────────────────────────

async def _fetch_daily_async(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    symbol: str,
    days: int = 5,
) -> pd.DataFrame | None:
    """Capital daily bars via shared aiohttp session (same Semaphore(3) as signal scan)."""
    period = "5d" if int(days) <= 5 else "1mo"
    try:
        return await scan_market_async(
            symbol,
            period=period,
            interval="1d",
            session_context=None,
            session=session,
            semaphore=sem,
        )
    except Exception:
        return None


async def level2_filter_async(tickers: list[str]) -> list[str]:
    """
    Level 2 — Stability filter (async).
    Gap checks use one shared ClientSession + batches of BULK_PARALLEL_BATCH_SIZE with BULK_BATCH_GAP_SLEEP_SEC.
    Earnings remain bulk calendar fetch (not per symbol).
    """
    print(f"📊 المستوى 2: تصفية {len(tickers)} سهم (أخبار وفجوات) [async Capital]...")
    today = datetime.now(timezone.utc).date()
    earnings_cutoff = today + timedelta(days=EARNINGS_BUFFER_DAYS)
    syms = [str(s).strip() for s in (tickers or []) if str(s).strip()]
    n = len(syms)
    if n == 0:
        return []

    gap_ok: set[str] = set()
    # Strictly bound Capital.com HTTP concurrency (thundering-herd protection).
    sem = asyncio.Semaphore(max(1, int(CAPITAL_HTTP_CONCURRENCY)))

    print(f"[LEVEL 2] Processing 0/{n}", flush=True)
    async with aiohttp.ClientSession(timeout=BULK_CAPITAL_CLIENT_TIMEOUT) as session:

        async def _gap_one(sym: str) -> None:
            async def _run_gap() -> None:
                hist = await _fetch_daily_async(session, sem, sym, 5)
                if hist is None or len(hist) < 2:
                    gap_ok.add(sym)
                    return
                prev_close = float(hist["Close"].iloc[-2])
                curr_open = float(hist["Open"].iloc[-1])
                if prev_close > 0 and abs(curr_open - prev_close) / prev_close > MAX_GAP_PCT:
                    return
                gap_ok.add(sym)

            try:
                await asyncio.wait_for(_run_gap(), timeout=BULK_GAP_ONE_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                print(
                    f"[LEVEL 2] Gap check exceeded {BULK_GAP_ONE_TIMEOUT_SEC:.0f}s for {sym} — accepting (degraded)",
                    flush=True,
                )
                gap_ok.add(sym)
            except Exception:
                gap_ok.add(sym)

        for batch_start in range(0, n, BULK_PARALLEL_BATCH_SIZE):
            batch = syms[batch_start : batch_start + BULK_PARALLEL_BATCH_SIZE]
            done = min(batch_start + len(batch), n)
            print(f"[LEVEL 2] Processing {done}/{n}", flush=True)
            await asyncio.gather(*[_gap_one(s) for s in batch])
            if batch_start + BULK_PARALLEL_BATCH_SIZE < n:
                await asyncio.sleep(BULK_BATCH_GAP_SLEEP_SEC)

    candidates = [s for s in syms if s in gap_ok]

    near_earnings_symbols = _get_near_earnings_symbols(today, earnings_cutoff)
    if near_earnings_symbols:
        stable = [s for s in candidates if str(s).upper() not in near_earnings_symbols]
    else:
        stable = candidates

    print(f"✅ المستوى 2: اجتاز {len(stable)} سهم")
    return stable


def level2_filter(tickers: list[str]) -> list[str]:
    """
    Level 2 — Stability filter.
    Excludes tickers with:
      - Earnings announcement within EARNINGS_BUFFER_DAYS days.
      - Price gap (open vs prev close) exceeding MAX_GAP_PCT (2%).

    Gap check uses provider daily bars (async aiohttp + shared Semaphore; see level2_filter_async).
    Earnings check uses NASDAQ earnings calendar (cached, bulk by date range); optional FMP fallback.
    On provider failure, it safely degrades to pass-through.
    """
    return asyncio.run(level2_filter_async(tickers))


def _level3_passes_from_hist(hist: pd.DataFrame | None) -> bool:
    """Daily-range / price gate from a daily OHLCV frame (shared by L3 async + tests)."""
    try:
        if hist is None or hist.empty or len(hist) < DAILY_RANGE_LOOKBACK_DAYS:
            return False
        if not all(col in hist.columns for col in ("High", "Low", "Close")):
            return False
        hist = hist.dropna(subset=["High", "Low", "Close"])
        if hist.empty or len(hist) < DAILY_RANGE_LOOKBACK_DAYS:
            return False

        last = hist.iloc[-1]
        close_last = float(last["Close"]) if last["Close"] is not None else 0.0
        if close_last <= 0:
            return False
        if close_last < MIN_PRICE:
            return False

        tail = hist.tail(DAILY_RANGE_LOOKBACK_DAYS)
        ranges = []
        for _, row in tail.iterrows():
            c = float(row["Close"])
            if c <= 0:
                continue
            r = (float(row["High"]) - float(row["Low"])) / c * 100.0
            if r >= 0:
                ranges.append(r)
        if not ranges:
            return False

        avg_range = sum(ranges) / len(ranges)
        last_range = ranges[-1]
        return (
            MIN_DAILY_RANGE_PCT <= avg_range <= MAX_DAILY_RANGE_PCT
            and MIN_DAILY_RANGE_PCT <= last_range <= MAX_DAILY_RANGE_PCT
        )
    except Exception:
        return False


async def level3_filter_async(tickers: list[str]) -> list[str]:
    """
    Level 3 — async batches of BULK_PARALLEL_BATCH_SIZE with one shared ClientSession.
    """
    print(f"📊 المستوى 3: تصفية {len(tickers)} سهم (نطاق حركة 0.2%-5%)...")
    if not tickers:
        return []

    qualified: list[str] = []
    sem = asyncio.Semaphore(BULK_PARALLEL_BATCH_SIZE)
    n = len(tickers)

    print(f"[LEVEL 3] Processing 0/{n}", flush=True)
    async with aiohttp.ClientSession(timeout=BULK_CAPITAL_CLIENT_TIMEOUT) as session:

        async def _l3_one(sym: str) -> tuple[str, bool]:
            async def _run() -> bool:
                hist = await _fetch_daily_async(session, sem, sym, 5)
                return _level3_passes_from_hist(hist)

            try:
                ok = await asyncio.wait_for(_run(), timeout=BULK_GAP_ONE_TIMEOUT_SEC)
                return sym, bool(ok)
            except asyncio.TimeoutError:
                print(
                    f"[LEVEL 3] Range check exceeded {BULK_GAP_ONE_TIMEOUT_SEC:.0f}s for {sym}",
                    flush=True,
                )
                return sym, False
            except Exception:
                return sym, False

        for batch_start in range(0, n, BULK_PARALLEL_BATCH_SIZE):
            batch = tickers[batch_start : batch_start + BULK_PARALLEL_BATCH_SIZE]
            done = min(batch_start + len(batch), n)
            print(f"[LEVEL 3] Processing {done}/{n}", flush=True)
            pairs = await asyncio.gather(*[_l3_one(s) for s in batch])
            for sym, ok in pairs:
                if ok:
                    qualified.append(sym)
            if batch_start + BULK_PARALLEL_BATCH_SIZE < n:
                await asyncio.sleep(BULK_BATCH_GAP_SLEEP_SEC)

    qualified = list(dict.fromkeys(qualified))
    print(f"✅ المستوى 3: اجتاز {len(qualified)} سهم")
    return qualified


def level3_filter(tickers: list[str]) -> list[str]:
    """
    Level 3 — Price & Daily Range filter.

    Keeps tickers where:
      - Last close price >= MIN_PRICE (no upper cap)
      - Average daily range (High-Low)/Close over last N days in
        [MIN_DAILY_RANGE_PCT, MAX_DAILY_RANGE_PCT]
      - Last day's daily range is also within the same band

    This aims to select stocks that are liquid yet not extremely volatile,
    so targets/SL distances behave more predictably.
    """
    return asyncio.run(level3_filter_async(tickers))
