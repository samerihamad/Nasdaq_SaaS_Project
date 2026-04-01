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

# ── Capital.com bar depth (scanner + AI training) ────────────────────────────
# Reject only when cleaned history is strictly below MIN_ANALYSIS_BARS.
# Counts in [MIN_ANALYSIS_BARS, TARGET_ANALYSIS_BARS) still run; scanner logs
# "[RECOVERED] Proceeding with X bars for [Symbol]" for that soft band (daily).
# Defaults 180/200: typical listings (~209 daily bars) pass without recovery.
MIN_ANALYSIS_BARS = int(os.getenv("MIN_ANALYSIS_BARS", "180"))
TARGET_ANALYSIS_BARS = int(os.getenv("TARGET_ANALYSIS_BARS", "200"))

# ── Mean Reversion Parameters ─────────────────────────────────────────────────

# RSI thresholds — tier-based (Fast vs Gold / GOLDEN signal profile).
# Tuple: (oversold_max_for_BUY, overbought_min_for_SELL) on 15m RSI.
FAST_RSI_LIMITS = (40, 60)
GOLD_RSI_LIMITS = (30, 70)

# FAST Mean Reversion: at/ beyond these 15m RSI levels, FAST tier may skip reversal candle / sweep (GOLDEN never).
FAST_MR_RSI_EXTREME_OVERSOLD = float(os.getenv("FAST_MR_RSI_EXTREME_OVERSOLD", "30"))
FAST_MR_RSI_EXTREME_OVERBOUGHT = float(os.getenv("FAST_MR_RSI_EXTREME_OVERBOUGHT", "70"))

# Strategy scan defaults: use Fast limits so the engine emits candidates broadly;
# per-user Gold tier re-validates RSI at execution time in validate_pre_trade().
MR_RSI_OVERSOLD = FAST_RSI_LIMITS[0]
MR_RSI_OVERBOUGHT = FAST_RSI_LIMITS[1]

# Required % deviation from VWAP to confirm price is stretched
MR_VWAP_DEV_PCT      = 1.2

# A gap larger than this % on the entry bar flags a "News Trap" → signal skipped
MR_NEWS_TRAP_GAP_PCT = 2.0

# Bars to look back when detecting a Liquidity Sweep
MR_SWEEP_LOOKBACK    = 16

# Minimum composite score (0–100) to emit a mean-reversion signal
MR_MIN_SCORE         = 65

# ── Momentum Parameters ───────────────────────────────────────────────────────

# ADX minimum for a trending market; signals below this are noise
MOM_ADX_THRESHOLD    = 22
# ADX above this is considered a very strong trend (boosts score)
MOM_ADX_STRONG       = 45

# Current bar volume must be ≥ this multiple of the 20-bar average
MOM_VOL_RATIO        = 1.3

# Minimum gap-up or gap-down % to count as a momentum gap signal
MOM_GAP_PCT          = 1.5

# Require MACD line to cross above/below signal line on entry bar
MOM_MACD_CONFIRM     = True

# Minimum composite score (0–100) to emit a momentum signal
MOM_MIN_SCORE        = 68

# 15m bars: scanner requires at least this many cleaned rows (RSI ~14, SMA50, vol MA20).
MIN_15M_BARS = int(os.getenv("MIN_15M_BARS", "100"))

# Momentum tier gates on 15m (scanner emits FAST-style signals; GOLDEN re-checked in dispatch).
FAST_MOM_VOL_RATIO = float(os.getenv("FAST_MOM_VOL_RATIO", "1.0"))
# When 15m RSI > FAST_MOM_RSI_VOL_TIER_HIGH (BUY), allow volume down to this multiple of MA20.
FAST_MOM_VOL_RATIO_HIGH_RSI = float(os.getenv("FAST_MOM_VOL_RATIO_HIGH_RSI", "0.8"))
FAST_MOM_RSI_VOL_TIER_HIGH = float(os.getenv("FAST_MOM_RSI_VOL_TIER_HIGH", "70"))
GOLDEN_MOM_VOL_RATIO = float(os.getenv("GOLDEN_MOM_VOL_RATIO", str(MOM_VOL_RATIO)))
FAST_MOM_RSI_BUY_MAX = float(os.getenv("FAST_MOM_RSI_BUY_MAX", "77"))
# Minimum AI probability (%) for FAST momentum entries using the high-RSI / relaxed-volume path.
FAST_MOM_LOW_VOL_AI_MIN = float(os.getenv("FAST_MOM_LOW_VOL_AI_MIN", "65"))
GOLDEN_MOM_RSI_BUY_MAX = float(os.getenv("GOLDEN_MOM_RSI_BUY_MAX", "65"))
FAST_MOM_RSI_SELL_MIN = float(os.getenv("FAST_MOM_RSI_SELL_MIN", "25"))
GOLDEN_MOM_RSI_SELL_MIN = float(os.getenv("GOLDEN_MOM_RSI_SELL_MIN", "35"))

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

# Default risk budget per trade in percent (used by high-level risk policies).
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))

# ── Signal Quality Gate ───────────────────────────────────────────────────────

# Signals with confidence below this are discarded before risk checks
MIN_CONFIDENCE       = 67.0

# ── Phase 8: Dual Signal Profiles (Fast vs Golden) ───────────────────────────
# Backward compatibility:
# - Existing MIN_CONFIDENCE / MR_MIN_SCORE / MOM_MIN_SCORE remain valid defaults.
# - New SIGNAL_* values are selected by SIGNAL_PROFILE and can be adopted gradually.
SIGNAL_PROFILE = os.getenv("SIGNAL_PROFILE", "FAST").strip().upper()  # FAST | GOLDEN

FAST_MIN_CONFIDENCE = float(os.getenv("FAST_MIN_CONFIDENCE", "63.0"))
GOLDEN_MIN_CONFIDENCE = float(os.getenv("GOLDEN_MIN_CONFIDENCE", "67.0"))

FAST_MR_MIN_SCORE = int(os.getenv("FAST_MR_MIN_SCORE", "52"))
GOLDEN_MR_MIN_SCORE = int(os.getenv("GOLDEN_MR_MIN_SCORE", "62"))

FAST_MOM_MIN_SCORE = int(os.getenv("FAST_MOM_MIN_SCORE", "55"))
GOLDEN_MOM_MIN_SCORE = int(os.getenv("GOLDEN_MOM_MIN_SCORE", "65"))

if SIGNAL_PROFILE == "GOLDEN":
    SIGNAL_MIN_CONFIDENCE = GOLDEN_MIN_CONFIDENCE
    SIGNAL_MR_MIN_SCORE = GOLDEN_MR_MIN_SCORE
    SIGNAL_MOM_MIN_SCORE = GOLDEN_MOM_MIN_SCORE
else:
    SIGNAL_MIN_CONFIDENCE = FAST_MIN_CONFIDENCE
    SIGNAL_MR_MIN_SCORE = FAST_MR_MIN_SCORE
    SIGNAL_MOM_MIN_SCORE = FAST_MOM_MIN_SCORE

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
ENABLE_STRUCTURAL_REJECTION_NOTIFY = os.getenv("ENABLE_STRUCTURAL_REJECTION_NOTIFY", "false").lower() == "true"
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
MAX_TRADES_PER_DAY_PER_USER = int(os.getenv("MAX_TRADES_PER_DAY_PER_USER", "15"))
MAX_DAILY_TRADES = MAX_TRADES_PER_DAY_PER_USER

# Global cap on concurrent open trades.
GLOBAL_MAX_OPEN_TRADES = int(os.getenv("GLOBAL_MAX_OPEN_TRADES", "7"))

# Hard stop: if realized daily loss exceeds this % of live equity, block entries.
MAX_DAILY_LOSS_PCT = float(os.getenv("MAX_DAILY_LOSS_PCT", "3.0"))

# ── News API (optional — NewsAPI.org) ─────────────────────────────────────────

NEWS_API_KEY         = os.getenv("NEWS_API_KEY", "")
NEWS_LOOKBACK_HOURS  = 12      # how far back to fetch headlines per ticker
NEWS_QUALITY_SCORE   = 15      # bonus score when a high-quality news event exists

# ── Earnings Calendar Provider (scanner filters) ──────────────────────────────
# Primary: NASDAQ calendar endpoint (no key).
# Fallback: FinancialModelingPrep if FMP_API_KEY is provided.
EARNINGS_TIMEOUT_SEC = int(os.getenv("EARNINGS_TIMEOUT_SEC", "15"))
EARNINGS_CACHE_TTL_SEC = int(os.getenv("EARNINGS_CACHE_TTL_SEC", "1800"))
FMP_API_KEY = os.getenv("FMP_API_KEY", "").strip()

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
AI_MIN_PROB_RF = float(os.getenv("AI_MIN_PROB_RF", "62.0"))
AI_MIN_PROB_MOMENTUM = float(os.getenv("AI_MIN_PROB_MOMENTUM", "65.0"))
AI_MIN_PROB_MEANREV = float(os.getenv("AI_MIN_PROB_MEANREV", "64.0"))

# Soft override:
# Allow high-confidence Momentum/MeanRev signals to pass even if AI probability
# is below the per-strategy threshold (override is intentionally harder to reach).
AI_SOFT_OVERRIDE_CONFIDENCE = float(os.getenv("AI_SOFT_OVERRIDE_CONFIDENCE", "63.0"))
AI_SOFT_OVERRIDE_MIN_PROB = float(os.getenv("AI_SOFT_OVERRIDE_MIN_PROB", "40.0"))
ENABLE_AI_SOFT_OVERRIDE = os.getenv("ENABLE_AI_SOFT_OVERRIDE", "true").lower() == "true"

# Optional AI feature: market-structure score blending.
# When enabled, validate_signal() adjusts blended probability by:
#   (ms_score - MS_SCORE_AI_NEUTRAL) * MS_SCORE_AI_SCALE
# and clamps final impact to +/- MS_SCORE_AI_MAX_IMPACT.
ENABLE_MS_SCORE_AI_INTEGRATION = os.getenv("ENABLE_MS_SCORE_AI_INTEGRATION", "true").lower() == "true"
MS_SCORE_AI_NEUTRAL = float(os.getenv("MS_SCORE_AI_NEUTRAL", "50.0"))
MS_SCORE_AI_SCALE = float(os.getenv("MS_SCORE_AI_SCALE", "0.18"))
MS_SCORE_AI_MAX_IMPACT = float(os.getenv("MS_SCORE_AI_MAX_IMPACT", "8.0"))

# Anti-spam: minimum gap between repeated rejection notifications to the same user.
EXECUTION_REJECTION_NOTIFY_COOLDOWN_SEC = int(os.getenv("EXECUTION_REJECTION_NOTIFY_COOLDOWN_SEC", "600"))

# Tier-based max stop-loss relaxation (applied on top of MAX_STOP_LOSS_PCT in risk_manager).
# Fast: 15% wider cap when confidence > 80.
FAST_SL_RELAX_CONFIDENCE_THRESHOLD = float(os.getenv("FAST_SL_RELAX_CONFIDENCE_THRESHOLD", "80.0"))
FAST_SL_RELAX_MULTIPLIER = float(os.getenv("FAST_SL_RELAX_MULTIPLIER", "1.15"))
# Gold (GOLDEN): 10% wider cap only when confidence > 90 (stricter discipline).
GOLD_SL_RELAX_CONFIDENCE_THRESHOLD = float(os.getenv("GOLD_SL_RELAX_CONFIDENCE_THRESHOLD", "90.0"))
GOLD_SL_RELAX_MULTIPLIER = float(os.getenv("GOLD_SL_RELAX_MULTIPLIER", "1.10"))

# ── Phase 7: Deep Direction Model (optional) ─────────────────────────────────
# RF pipeline remains default. These settings configure optional LSTM/GRU/Transformer training.
ENABLE_DEEP_DIRECTION_MODEL = os.getenv("ENABLE_DEEP_DIRECTION_MODEL", "false").lower() == "true"
DEEP_DIRECTION_MODEL_KIND = os.getenv("DEEP_DIRECTION_MODEL_KIND", "lstm").strip().lower()  # lstm|gru|transformer
DEEP_DIRECTION_TIMEFRAME = os.getenv("DEEP_DIRECTION_TIMEFRAME", "15m").strip().lower()      # 1d|4h|15m
DEEP_DIRECTION_SEQ_LEN = int(os.getenv("DEEP_DIRECTION_SEQ_LEN", "64"))
DEEP_DIRECTION_LABEL_HORIZON = int(os.getenv("DEEP_DIRECTION_LABEL_HORIZON", "8"))
DEEP_DIRECTION_LABEL_THRESHOLD = float(os.getenv("DEEP_DIRECTION_LABEL_THRESHOLD", "0.012"))
# Inference path toggle:
# false (default): RF -> rule-based
# true: Deep -> RF -> rule-based
ENABLE_DEEP_DIRECTION_INFERENCE = os.getenv("ENABLE_DEEP_DIRECTION_INFERENCE", "false").lower() == "true"
DEEP_DIRECTION_INFERENCE_KIND = os.getenv("DEEP_DIRECTION_INFERENCE_KIND", DEEP_DIRECTION_MODEL_KIND).strip().lower()

# Refresh watchlist while market is open (seconds)
WATCHLIST_REFRESH_SECONDS = 3600

# ── Capital.com Broker ────────────────────────────────────────────────────────

# These are per-user credentials stored in DB; the keys below are for
# any standalone / admin-level calls only.
CAPITAL_API_KEY      = os.getenv("CAPITAL_API_KEY", "")
CAPITAL_EMAIL        = os.getenv("CAPITAL_EMAIL", "")
CAPITAL_PASSWORD     = os.getenv("CAPITAL_PASSWORD", "")
CAPITAL_IS_DEMO      = os.getenv("CAPITAL_IS_DEMO", "true").lower() == "true"

# Optional dedicated market-data identity (shared scanner service account).
# If unset, scanner falls back to CAPITAL_* values above.
MARKET_DATA_CAPITAL_API_KEY = os.getenv("MARKET_DATA_CAPITAL_API_KEY", "").strip()
MARKET_DATA_CAPITAL_EMAIL = os.getenv("MARKET_DATA_CAPITAL_EMAIL", "").strip()
MARKET_DATA_CAPITAL_PASSWORD = os.getenv("MARKET_DATA_CAPITAL_PASSWORD", "").strip()
MARKET_DATA_CAPITAL_IS_DEMO = os.getenv("MARKET_DATA_CAPITAL_IS_DEMO", "").strip().lower()

# ── Autonomous AI training (Phase AUTONOMY) ───────────────────────────────────
# Master switch: keeps current production behavior unchanged unless enabled.
ENABLE_AUTONOMOUS_TRAINING = os.getenv("ENABLE_AUTONOMOUS_TRAINING", "false").lower() == "true"

# Scheduler settings (UTC): daily + weekly cron-like triggers.
ENABLE_AUTONOMOUS_SCHEDULED_TRAINING = (
    os.getenv("ENABLE_AUTONOMOUS_SCHEDULED_TRAINING", "true").lower() == "true"
)
AUTOTRAIN_SCHEDULER_POLL_SEC = int(os.getenv("AUTOTRAIN_SCHEDULER_POLL_SEC", "60"))
AUTOTRAIN_DAILY_UTC_HOUR = int(os.getenv("AUTOTRAIN_DAILY_UTC_HOUR", "2"))
AUTOTRAIN_DAILY_UTC_MINUTE = int(os.getenv("AUTOTRAIN_DAILY_UTC_MINUTE", "15"))
AUTOTRAIN_WEEKLY_DAY = os.getenv("AUTOTRAIN_WEEKLY_DAY", "sun").strip().lower()  # mon..sun
AUTOTRAIN_WEEKLY_UTC_HOUR = int(os.getenv("AUTOTRAIN_WEEKLY_UTC_HOUR", "3"))
AUTOTRAIN_WEEKLY_UTC_MINUTE = int(os.getenv("AUTOTRAIN_WEEKLY_UTC_MINUTE", "30"))

# Job sizing / cadence.
AUTOTRAIN_MAX_SYMBOLS_PER_RUN = int(os.getenv("AUTOTRAIN_MAX_SYMBOLS_PER_RUN", "20"))
AUTOTRAIN_EPOCHS = int(os.getenv("AUTOTRAIN_EPOCHS", "20"))
AUTOTRAIN_AUTO_MAX_MODEL_AGE_HOURS = float(os.getenv("AUTOTRAIN_AUTO_MAX_MODEL_AGE_HOURS", "168"))

# Self-learning loop from closed trade outcomes.
ENABLE_AUTONOMOUS_SELF_LEARNING = os.getenv("ENABLE_AUTONOMOUS_SELF_LEARNING", "true").lower() == "true"
AUTOTRAIN_SELF_LEARNING_INTERVAL_SEC = int(os.getenv("AUTOTRAIN_SELF_LEARNING_INTERVAL_SEC", "21600"))
AUTOTRAIN_SELF_LEARNING_MIN_SAMPLES = int(os.getenv("AUTOTRAIN_SELF_LEARNING_MIN_SAMPLES", "25"))
AUTOTRAIN_SELF_LEARNING_SYMBOLS_PER_RUN = int(os.getenv("AUTOTRAIN_SELF_LEARNING_SYMBOLS_PER_RUN", "6"))
AUTOTRAIN_SELF_LEARNING_EPOCHS = int(os.getenv("AUTOTRAIN_SELF_LEARNING_EPOCHS", "10"))

# Observability.
AUTOTRAIN_NOTIFY_ADMIN = os.getenv("AUTOTRAIN_NOTIFY_ADMIN", "true").lower() == "true"
AUTOTRAIN_LOG_ROOT = os.getenv("AUTOTRAIN_LOG_ROOT", "logs/ai_training")
