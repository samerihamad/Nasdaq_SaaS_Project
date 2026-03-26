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
MR_RSI_OVERSOLD      = 30      # institutional strict: deeper oversold
MR_RSI_OVERBOUGHT    = 70      # institutional strict: deeper overbought

# Required % deviation from VWAP to confirm price is stretched
MR_VWAP_DEV_PCT      = 2.2

# A gap larger than this % on the entry bar flags a "News Trap" → signal skipped
MR_NEWS_TRAP_GAP_PCT = 2.0

# Bars to look back when detecting a Liquidity Sweep
MR_SWEEP_LOOKBACK    = 16

# Minimum composite score (0–100) to emit a mean-reversion signal
MR_MIN_SCORE         = 65

# ── Momentum Parameters ───────────────────────────────────────────────────────

# ADX minimum for a trending market; signals below this are noise
MOM_ADX_THRESHOLD    = 28
# ADX above this is considered a very strong trend (boosts score)
MOM_ADX_STRONG       = 45

# Current bar volume must be ≥ this multiple of the 20-bar average
MOM_VOL_RATIO        = 2.0

# Minimum gap-up or gap-down % to count as a momentum gap signal
MOM_GAP_PCT          = 1.5

# Require MACD line to cross above/below signal line on entry bar
MOM_MACD_CONFIRM     = True

# Minimum composite score (0–100) to emit a momentum signal
MOM_MIN_SCORE        = 68

# ── ATR / Stop-Loss ───────────────────────────────────────────────────────────

# Initial stop = entry ± (ATR × this multiplier)
ATR_STOP_MULTIPLIER  = 2.0

# ATR period used for stop distance calculation (matches trailing_stop.py)
ATR_PERIOD           = 14

# ── Fixed Target Distances (entry-relative) ────────────────────────────────
# These targets are used to set TP1/TP2 levels regardless of ATR/stop_distance.
# TP1/T2 are percentages from entry price in the direction of the trade.
TP1_PCT = 0.01   # 1%
TP2_PCT = 0.025  # 2.5%

# ── Multi-leg execution split ────────────────────────────────────────────────
# Capital.com only supports one TP per position, so we split into 2 legs:
# - TP1 leg closes at Target 1
# - TP2 leg closes at Target 2
TP1_SPLIT_PCT = 0.70  # 70% size at TP1
TP2_SPLIT_PCT = 0.30  # 30% size at TP2

# When broker enforces min distance for profit targets, we can widen TP2 slightly.
TP2_MIN_DISTANCE_BUFFER_MULT = 1.05

# After TP1 is reached we "lock" the stop on TP2 leg so that the position
# doesn't fall back to entry. We use a tiny buffer beyond breakeven to
# reduce the chance of an exact-entry stop due to rounding/ticks.
BE_LOCK_BUFFER_PCT = 0.0005  # 0.05% beyond entry (per direction)

# ── Signal Quality Gate ───────────────────────────────────────────────────────

# Signals with confidence below this are discarded before risk checks
MIN_CONFIDENCE       = 67.0

# ── Sprint 1: Market Structure Foundation flags ───────────────────────────────
# Toggle these filters without changing strategy code.
ENABLE_MARKET_STRUCTURE_FILTERS = os.getenv("ENABLE_MARKET_STRUCTURE_FILTERS", "true").lower() == "true"
ENABLE_PREMIUM_DISCOUNT_FILTER = os.getenv("ENABLE_PREMIUM_DISCOUNT_FILTER", "true").lower() == "true"
ENABLE_LIQUIDITY_MAP_FILTER = os.getenv("ENABLE_LIQUIDITY_MAP_FILTER", "true").lower() == "true"

# Reject setups around HTF equilibrium ("no man's land").
MARKET_STRUCTURE_NO_TRADE_ZONE_PCT = float(os.getenv("MARKET_STRUCTURE_NO_TRADE_ZONE_PCT", "0.14"))

# 4H candles used to build HTF range and premium/discount context.
MARKET_STRUCTURE_HTF_LOOKBACK = int(os.getenv("MARKET_STRUCTURE_HTF_LOOKBACK", "80"))

# First N 15m candles used for opening-range liquidity levels.
LIQUIDITY_OPENING_RANGE_BARS = int(os.getenv("LIQUIDITY_OPENING_RANGE_BARS", "6"))

# Send Telegram notice when a setup is rejected by market-structure filters.
ENABLE_STRUCTURAL_REJECTION_NOTIFY = os.getenv("ENABLE_STRUCTURAL_REJECTION_NOTIFY", "true").lower() == "true"
STRUCTURAL_REJECTION_NOTIFY_COOLDOWN_SEC = int(os.getenv("STRUCTURAL_REJECTION_NOTIFY_COOLDOWN_SEC", "1800"))
STRUCTURAL_REJECTION_NOTIFY_MAX_PER_CYCLE = int(os.getenv("STRUCTURAL_REJECTION_NOTIFY_MAX_PER_CYCLE", "5"))

# If broker history never returns final realized PnL, send a terminal close
# notification and stop retrying forever.
FINAL_SYNC_FALLBACK_ENABLED = os.getenv("FINAL_SYNC_FALLBACK_ENABLED", "true").lower() == "true"

# Do not push expected setup/broker-rule rejections to Telegram subscribers.
# These are logged to DB for audit instead.
SUPPRESS_EXPECTED_REJECTION_TELEGRAM = os.getenv("SUPPRESS_EXPECTED_REJECTION_TELEGRAM", "true").lower() == "true"

# ── Sprint 2: Limit order policy ─────────────────────────────────────────────
ENABLE_LIMIT_ORDER_MODE = os.getenv("ENABLE_LIMIT_ORDER_MODE", "true").lower() == "true"
LIMIT_ORDER_TTL_BARS = int(os.getenv("LIMIT_ORDER_TTL_BARS", "4"))
LIMIT_ORDER_BAR_MINUTES = int(os.getenv("LIMIT_ORDER_BAR_MINUTES", "15"))
LIMIT_ORDER_MOMENTUM_RETRACE = float(os.getenv("LIMIT_ORDER_MOMENTUM_RETRACE", "0.618"))
LIMIT_ORDER_MEANREV_ATR_OFFSET = float(os.getenv("LIMIT_ORDER_MEANREV_ATR_OFFSET", "0.20"))
LIMIT_ORDER_ALLOW_MARKET_FALLBACK = os.getenv("LIMIT_ORDER_ALLOW_MARKET_FALLBACK", "false").lower() == "true"

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

# ── AI Gate Tuning (soft-gate mode) ──────────────────────────────────────────
#
# Per-strategy minimum AI probability thresholds (%).
# These apply in dispatch_signal() as an execution gate after strategy confidence.
AI_MIN_PROB_RF = 62.0
AI_MIN_PROB_MOMENTUM = 65.0
AI_MIN_PROB_MEANREV = 64.0

# Soft override:
# Allow high-confidence Momentum/MeanRev signals to pass even if AI probability
# is below the per-strategy threshold (override is intentionally harder to reach).
AI_SOFT_OVERRIDE_CONFIDENCE = 63.0
AI_SOFT_OVERRIDE_MIN_PROB = 40.0
ENABLE_AI_SOFT_OVERRIDE = os.getenv("ENABLE_AI_SOFT_OVERRIDE", "false").lower() == "true"

# Refresh watchlist while market is open (seconds)
WATCHLIST_REFRESH_SECONDS = 3600

# ── Capital.com Broker ────────────────────────────────────────────────────────

# These are per-user credentials stored in DB; the keys below are for
# any standalone / admin-level calls only.
CAPITAL_API_KEY      = os.getenv("CAPITAL_API_KEY", "")
CAPITAL_EMAIL        = os.getenv("CAPITAL_EMAIL", "")
CAPITAL_PASSWORD     = os.getenv("CAPITAL_PASSWORD", "")
CAPITAL_IS_DEMO      = os.getenv("CAPITAL_IS_DEMO", "true").lower() == "true"
