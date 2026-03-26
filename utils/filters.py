import logging
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Silence yfinance per-ticker timeout noise
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# --- Thresholds ---
MIN_AVG_VOLUME       = 1_000_000      # 1M shares/day
MIN_MARKET_CAP       = 300_000_000    # $300M
MAX_GAP_PCT          = 0.02           # 2% max gap
EARNINGS_BUFFER_DAYS = 2              # exclude if earnings within 2 days

# ── Level 3 — Price / Daily Range filter ─────────────────────────────────
# Goal: prefer "cheap" tickers with controlled intraday movement.
# We estimate daily movement using (High - Low) / Close.
MIN_PRICE            = 0.50          # $0.50 minimum
MAX_PRICE            = 30.0          # $30 maximum (tune as needed)
MIN_DAILY_RANGE_PCT = 0.50          # 0.5%
MAX_DAILY_RANGE_PCT = 2.00          # 2.0%
DAILY_RANGE_LOOKBACK_DAYS = 3         # average over last N sessions

# Module-level cache: populated by get_nasdaq_tickers() when screener API works.
# { symbol: {'avg_volume': float, 'market_cap': float} }
_screener_cache: dict = {}


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
    Return a scalar mean volume even when yfinance returns odd shapes
    (e.g., MultiIndex slices that produce DataFrame/Series ambiguity).
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


# ── Ticker fetch ──────────────────────────────────────────────────────────────

def get_nasdaq_tickers() -> list[str]:
    """
    Fetch Nasdaq common-stock tickers and populate _screener_cache with
    pre-built volume + market-cap data so Level 1 needs zero yfinance calls.

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

    Fast path  : uses data already cached from the screener API response
                 (zero extra network calls, completes in milliseconds).
    Slow path  : falls back to yfinance batch downloads when cache is absent
                 (Sources 2 & 3 don't carry volume/cap data).
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
                unknown_volume.append(sym)    # volume unknown — check via yfinance

        # For stocks where the screener had no volume data (vol==0), fall back
        # to a 5-day yfinance batch download to measure real average volume.
        if unknown_volume:
            print(f"   ({len(unknown_volume)} سهم بدون بيانات حجم في الـ API — جاري التحقق عبر yfinance...)")
            for i in range(0, len(unknown_volume), 50):
                batch = unknown_volume[i:i + 50]
                try:
                    raw = yf.download(
                        batch, period="5d", interval="1d",
                        progress=False, auto_adjust=True,
                        group_by="ticker", threads=True,
                    )
                    for sym in batch:
                        try:
                            sym_df = raw[sym] if len(batch) > 1 else raw
                            if sym_df is None or sym_df.empty:
                                continue
                            if _safe_volume_mean(sym_df) >= MIN_AVG_VOLUME:
                                qualified.append(sym)
                        except Exception:
                            continue
                except Exception:
                    continue

        print(f"✅ المستوى 1: اجتاز {len(qualified)} سهم (من بيانات الـ API — بدون تنزيل إضافي)")
        return qualified[:top_n]

    # ── Slow path: yfinance batch download ────────────────────────────────────
    print("   (لا توجد بيانات مخزّنة — جاري التنزيل عبر yfinance، قد يستغرق وقتاً...)")
    volume_qualified = []

    for i in range(0, len(tickers), 50):
        batch = tickers[i:i + 50]
        try:
            raw = yf.download(
                batch, period="5d", interval="1d",
                progress=False, auto_adjust=True,
                group_by="ticker", threads=True,
            )
            for sym in batch:
                try:
                    sym_df = raw[sym] if len(batch) > 1 else raw
                    if sym_df is None or sym_df.empty:
                        continue
                    if _safe_volume_mean(sym_df) >= MIN_AVG_VOLUME:
                        volume_qualified.append(sym)
                except Exception:
                    continue
        except Exception:
            continue

    final = []
    for sym in volume_qualified:
        try:
            cap = getattr(yf.Ticker(sym).fast_info, "market_cap", 0) or 0
            if cap >= MIN_MARKET_CAP:
                final.append(sym)
        except Exception:
            continue

    print(f"✅ المستوى 1: اجتاز {len(final)} سهم")
    return final[:top_n]


# ── Level 2 filter ────────────────────────────────────────────────────────────

def level2_filter(tickers: list[str]) -> list[str]:
    """
    Level 2 — Stability filter.
    Excludes tickers with:
      - Earnings announcement within EARNINGS_BUFFER_DAYS days.
      - Price gap (open vs prev close) exceeding MAX_GAP_PCT (2%).

    Gap check uses a single batch yf.download for all tickers (fast).
    Earnings check runs in parallel threads with a per-stock timeout.
    """
    print(f"📊 المستوى 2: تصفية {len(tickers)} سهم (أخبار وفجوات)...")
    today           = datetime.now().date()
    earnings_cutoff = today + timedelta(days=EARNINGS_BUFFER_DAYS)

    # ── Step 1: batch gap check (one download for all tickers) ───────────────
    gap_ok: set[str] = set()
    try:
        raw = yf.download(
            tickers, period="2d", interval="1d",
            progress=False, auto_adjust=True,
            group_by="ticker", threads=True,
        )
        for sym in tickers:
            try:
                hist = raw[sym] if len(tickers) > 1 else raw
                if hist is None or len(hist) < 2:
                    gap_ok.add(sym)   # no data → don't penalise
                    continue
                prev_close = float(hist["Close"].iloc[-2])
                curr_open  = float(hist["Open"].iloc[-1])
                if prev_close > 0 and abs(curr_open - prev_close) / prev_close > MAX_GAP_PCT:
                    continue          # gap too large — exclude
                gap_ok.add(sym)
            except Exception:
                gap_ok.add(sym)       # can't check → don't penalise
    except Exception:
        gap_ok = set(tickers)         # batch failed → skip gap filter entirely

    candidates = [s for s in tickers if s in gap_ok]

    # ── Step 2: parallel earnings check ──────────────────────────────────────
    def _has_near_earnings(sym: str) -> bool:
        """Return True if sym has earnings within EARNINGS_BUFFER_DAYS."""
        try:
            cal = yf.Ticker(sym).calendar
            if cal is None:
                return False
            if isinstance(cal, dict):
                for key in ("Earnings Date", "earningsDate"):
                    for d in (cal.get(key) or []):
                        try:
                            if today <= pd.to_datetime(d).date() <= earnings_cutoff:
                                return True
                        except Exception:
                            pass
            elif hasattr(cal, "empty") and not cal.empty:
                for col in cal.columns:
                    try:
                        if today <= pd.to_datetime(col).date() <= earnings_cutoff:
                            return True
                    except Exception:
                        pass
        except Exception:
            pass
        return False

    stable: list[str] = []
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_has_near_earnings, sym): sym for sym in candidates}
        for future in as_completed(futures, timeout=120):
            sym = futures[future]
            try:
                if not future.result():
                    stable.append(sym)
            except Exception:
                stable.append(sym)   # timeout / error → don't penalise

    print(f"✅ المستوى 2: اجتاز {len(stable)} سهم")
    return stable


def level3_filter(tickers: list[str]) -> list[str]:
    """
    Level 3 — Price & Daily Range filter.

    Keeps tickers where:
      - Last close price within [MIN_PRICE, MAX_PRICE]
      - Average daily range (High-Low)/Close over last N days in
        [MIN_DAILY_RANGE_PCT, MAX_DAILY_RANGE_PCT]
      - Last day's daily range is also within the same band

    This aims to select stocks that are liquid yet not extremely volatile,
    so targets/SL distances behave more predictably.
    """
    print(f"📊 المستوى 3: تصفية {len(tickers)} سهم (سعر رخيص + حركة يومية 0.5%-2%)...")
    if not tickers:
        return []

    qualified: list[str] = []

    # Chunk to limit memory and request payload size.
    for i in range(0, len(tickers), 50):
        batch = tickers[i:i + 50]
        try:
            raw = yf.download(
                batch,
                period="5d",
                interval="1d",
                progress=False,
                auto_adjust=True,
                group_by="ticker",
                threads=True,
            )
        except Exception:
            continue

        for sym in batch:
            try:
                hist = raw[sym] if len(batch) > 1 else raw
                if hist is None or hist.empty or len(hist) < DAILY_RANGE_LOOKBACK_DAYS:
                    continue

                # Ensure columns exist
                if not all(col in hist.columns for col in ("High", "Low", "Close")):
                    continue

                hist = hist.dropna(subset=["High", "Low", "Close"])
                if hist.empty or len(hist) < DAILY_RANGE_LOOKBACK_DAYS:
                    continue

                last = hist.iloc[-1]
                close_last = float(last["Close"]) if last["Close"] is not None else 0.0
                if close_last <= 0:
                    continue

                price_ok = (close_last >= MIN_PRICE) and (close_last <= MAX_PRICE)
                if not price_ok:
                    continue

                # Daily range for each of the last N sessions.
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
                    continue

                avg_range = sum(ranges) / len(ranges)

                # Also enforce last-day range band.
                last_range = ranges[-1]
                if (
                    MIN_DAILY_RANGE_PCT <= avg_range <= MAX_DAILY_RANGE_PCT
                    and MIN_DAILY_RANGE_PCT <= last_range <= MAX_DAILY_RANGE_PCT
                ):
                    qualified.append(sym)

            except Exception:
                continue

    # Deduplicate in case symbols appear in multiple batches
    qualified = list(dict.fromkeys(qualified))
    print(f"✅ المستوى 3: اجتاز {len(qualified)} سهم")
    return qualified
