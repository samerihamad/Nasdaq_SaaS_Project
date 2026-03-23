"""
NATB v2.0 — Central Configuration
All tunable hyperparameters live here. Never hardcode these in strategy files.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Ticker Universe ───────────────────────────────────────────────────────────

WATCHLIST = [
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "META", "GOOGL", "AMD",  "NFLX", "ORCL",
    # ETFs (broad market + sector)
    "SPY",  "QQQ",  "IWM",  "XLK",  "XLF",
    # Semi & AI
    "SMCI", "ARM",  "INTC", "AVGO", "QCOM",
    # High-beta & momentum names
    "PLTR", "MSTR", "COIN", "RBLX", "UBER",
    # Healthcare & consumer
    "JNJ",  "PFE",  "WMT",  "COST", "HD",
    # Financials
    "JPM",  "BAC",  "GS",   "MS",   "V",
    # Energy & commodities
    "XOM",  "CVX",  "GLD",  "SLV",  "USO",
]

# ── Mean Reversion Parameters ─────────────────────────────────────────────────

# RSI thresholds for oversold / overbought detection
MR_RSI_OVERSOLD      = 30      # below → look for BUY reversal on 15m
MR_RSI_OVERBOUGHT    = 70      # above → look for SELL reversal on 15m

# Required % deviation from VWAP to confirm price is stretched
MR_VWAP_DEV_PCT      = 1.5

# A gap larger than this % on the entry bar flags a "News Trap" → signal skipped
MR_NEWS_TRAP_GAP_PCT = 3.0

# Bars to look back when detecting a Liquidity Sweep
MR_SWEEP_LOOKBACK    = 10

# Minimum composite score (0–100) to emit a mean-reversion signal
MR_MIN_SCORE         = 40

# ── Momentum Parameters ───────────────────────────────────────────────────────

# ADX minimum for a trending market; signals below this are noise
MOM_ADX_THRESHOLD    = 25
# ADX above this is considered a very strong trend (boosts score)
MOM_ADX_STRONG       = 40

# Current bar volume must be ≥ this multiple of the 20-bar average
MOM_VOL_RATIO        = 1.8

# Minimum gap-up or gap-down % to count as a momentum gap signal
MOM_GAP_PCT          = 1.0

# Require MACD line to cross above/below signal line on entry bar
MOM_MACD_CONFIRM     = True

# Minimum composite score (0–100) to emit a momentum signal
MOM_MIN_SCORE        = 45

# ── ATR / Stop-Loss ───────────────────────────────────────────────────────────

# Initial stop = entry ± (ATR × this multiplier)
ATR_STOP_MULTIPLIER  = 2.0

# ATR period used for stop distance calculation (matches trailing_stop.py)
ATR_PERIOD           = 14

# ── Signal Quality Gate ───────────────────────────────────────────────────────

# Signals with confidence below this are discarded before risk checks
MIN_CONFIDENCE       = 63.0

# Hard cap on new positions per user per day (independent of circuit breaker)
MAX_DAILY_TRADES     = 5

# ── News API (optional — NewsAPI.org) ─────────────────────────────────────────

NEWS_API_KEY         = os.getenv("NEWS_API_KEY", "")
NEWS_LOOKBACK_HOURS  = 12      # how far back to fetch headlines per ticker
NEWS_QUALITY_SCORE   = 15      # bonus score when a high-quality news event exists

# ── Scanning Scheduler ────────────────────────────────────────────────────────

SCAN_INTERVAL_SEC    = 300     # run a full market scan every 5 minutes

# ── Main Engine Runtime ────────────────────────────────────────────────────────

# Live engine cycle interval (seconds)
CHECK_INTERVAL = 60

# Final watchlist size after L1/L2 filters
MAX_WATCHLIST = 180

# Hybrid mode approval TTL (seconds)
HYBRID_SIGNAL_TTL = 600

# Heartbeat write interval (seconds)
HEARTBEAT_INTERVAL = 30

# Cloud backup cycle interval (seconds)
BACKUP_INTERVAL = 3600

# Heartbeat state file written by main engine
HEARTBEAT_FILE = "heartbeat.json"

# Pre-market alert is sent when minutes_to_open <= this value
PREMARKET_ALERT_WINDOW_MIN = 30

# Refresh watchlist while market is open (seconds)
WATCHLIST_REFRESH_SECONDS = 3600

# ── Capital.com Broker ────────────────────────────────────────────────────────

# These are per-user credentials stored in DB; the keys below are for
# any standalone / admin-level calls only.
CAPITAL_API_KEY      = os.getenv("CAPITAL_API_KEY", "")
CAPITAL_EMAIL        = os.getenv("CAPITAL_EMAIL", "")
CAPITAL_PASSWORD     = os.getenv("CAPITAL_PASSWORD", "")
CAPITAL_IS_DEMO      = os.getenv("CAPITAL_IS_DEMO", "true").lower() == "true"
