"""
AI Model — NATB v2.0

Role: GATEKEEPER, not observer.

Architecture:
  1. Technical strategies (MeanRev, Momentum, RF) generate a candidate signal.
  2. validate_signal() scores that specific direction using the RF model.
     If probability < AI_PROBABILITY_THRESHOLD (65%), the trade is BLOCKED.
  3. detect_regime() classifies market context (TRENDING / RANGING / VOLATILE).
     A VOLATILE regime applies a probability penalty.
  4. analyze_multi_timeframe() supports optional deep inference fallback chain:
     Deep -> RF -> rule-based (RF path remains default).

Key public API:
  validate_signal(symbol, direction, timeframes) -> (approved, probability, regime)
  detect_regime(df)                              -> dict
  analyze_multi_timeframe(timeframes, symbol)    -> (action, confidence, reason)
  load_or_train_model(symbol, timeframe)         -> (model, scaler)

MODEL_VERSION is embedded in saved .pkl files. A version mismatch triggers
automatic retraining so stale models with different feature sets never silently
produce wrong scores.
"""

import os
import pickle
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from utils.market_scanner import scan_market
from config import (
    ENABLE_DEEP_DIRECTION_INFERENCE,
    DEEP_DIRECTION_INFERENCE_KIND,
    ENABLE_MS_SCORE_AI_INTEGRATION,
    MS_SCORE_AI_NEUTRAL,
    MS_SCORE_AI_SCALE,
    MS_SCORE_AI_MAX_IMPACT,
    MIN_ANALYSIS_BARS,
)

log = logging.getLogger(__name__)

MODEL_DIR             = "models"
MODEL_VERSION         = 4          # bump to force retrain after provider/feature alignment changes
AI_PROBABILITY_THRESHOLD = 60.0    # minimum probability to approve a trade
# Align with config MIN_ANALYSIS_BARS / TARGET_ANALYSIS_BARS (scanner + training gate).
MIN_TRAIN_BARS = MIN_ANALYSIS_BARS
TRAIN_RETRY_HOURS     = 6          # avoid retry spam for symbols with short history

os.makedirs(MODEL_DIR, exist_ok=True)
_train_retry_after: dict[str, datetime] = {}
_model_cache: dict[str, tuple[Any, Any, int]] = {}  # symbol -> (model, scaler, version)

# Label parameters
FUTURE_BARS      = 3       # tighter horizon for less label noise
RETURN_THRESHOLD = 0.02    # 2.0% move to qualify as BUY/SELL

LABEL_BUY  =  1
LABEL_SELL = -1
LABEL_HOLD =  0


# ── Indicator helpers ─────────────────────────────────────────────────────────

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast  = _ema(series, fast)
    ema_slow  = _ema(series, slow)
    macd_line = ema_fast - ema_slow
    sig_line  = _ema(macd_line, signal)
    histogram = macd_line - sig_line
    return macd_line, sig_line, histogram


def _vwap(df: pd.DataFrame) -> pd.Series:
    """Session-reset VWAP for intraday; rolling-20 for daily."""
    df = _flatten(df)
    high = _col_series(df, 'High')
    low = _col_series(df, 'Low')
    close = _col_series(df, 'Close')
    typical = (high + low + close) / 3
    volume = _col_series(df, 'Volume')
    is_intraday = len(df) > 1 and (df.index[1] - df.index[0]).total_seconds() < 86400
    if is_intraday:
        dates  = pd.Series([ts.date() for ts in df.index], index=df.index)
        result = pd.Series(np.nan, index=df.index, dtype=float)
        for d in dates.unique():
            mask = dates == d
            result[mask] = (typical[mask] * volume[mask]).cumsum() / volume[mask].cumsum()
        return result
    return (typical * volume).rolling(20).sum() / volume.rolling(20).sum()


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high  = _col_series(df, 'High')
    low   = _col_series(df, 'Low')
    close = _col_series(df, 'Close')
    prev  = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev).abs(),
        (low  - prev).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    ADX(14) to align ML feature-space with Momentum trend logic.
    """
    high = _col_series(df, "High")
    low = _col_series(df, "Low")
    close = _col_series(df, "Close")
    prev_high = high.shift(1)
    prev_low = low.shift(1)
    prev_close = close.shift(1)

    up_move = high - prev_high
    down_move = prev_low - low
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=high.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=high.index,
    )

    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_s = tr.rolling(period).mean().replace(0, np.nan)
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_s)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_s)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    return dx.rolling(period).mean()


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def _col_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Return a float Series for `col` with `df.index`, even if the input frame
    has a single row (where `.squeeze()` would otherwise return a scalar).
    """
    if df is None or col not in df.columns:
        return pd.Series([], dtype=float)
    s = df[col]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    # Avoid `.squeeze()` — it turns 1-row columns into numpy scalars.
    try:
        s = pd.Series(s.values, index=df.index, dtype=float)
    except Exception:
        s = pd.Series(list(s), index=df.index, dtype=float)
    return s


# ── Feature engineering ───────────────────────────────────────────────────────

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a feature matrix from an OHLCV DataFrame.

    Columns (v4 — MODEL_VERSION = 4):
      Existing core set + ADX/trend-strength and candle-structure
      features used by MeanRev/Momentum logic.
    """
    df = _flatten(df)

    close  = _col_series(df, 'Close')
    high   = _col_series(df, 'High')
    low    = _col_series(df, 'Low')
    volume = _col_series(df, 'Volume')

    ema20  = _ema(close, 20)
    ema50  = _ema(close, 50)
    ema200 = _ema(close, 200)
    rsi    = _rsi(close)
    macd_line, sig_line, hist = _macd(close)
    vwap   = _vwap(df)
    atr_v  = _atr(df)
    adx14  = _adx(df)

    # Bollinger Band width as regime proxy
    bb_mid   = _ema(close, 20)
    bb_std   = close.rolling(20).std()
    bb_width = (2 * bb_std) / bb_mid.replace(0, np.nan) * 100
    open_ = _col_series(df, "Open")
    prev_close = close.shift(1)
    bar_range = (high - low).replace(0, np.nan)
    body = (close - open_).abs()
    upper_wick = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_wick = pd.concat([open_, close], axis=1).min(axis=1) - low

    features = pd.DataFrame({
        # Price / EMA distances (%)
        'price_vs_ema20':    (close - ema20)  / ema20.replace(0, np.nan)  * 100,
        'price_vs_ema50':    (close - ema50)  / ema50.replace(0, np.nan)  * 100,
        'price_vs_ema200':   (close - ema200) / ema200.replace(0, np.nan) * 100,
        # EMA alignment
        'ema20_vs_ema50':    (ema20  - ema50)  / ema50.replace(0, np.nan)   * 100,
        'ema50_vs_ema200':   (ema50  - ema200) / ema200.replace(0, np.nan)  * 100,
        # Momentum
        'rsi':               rsi,
        'rsi_slope':         rsi.diff(3),
        'macd_hist':         hist,
        'macd_hist_slope':   hist.diff(2),
        # VWAP
        'price_vs_vwap':     (close - vwap) / vwap.replace(0, np.nan) * 100,
        # Volume
        'vol_ratio':         volume / volume.rolling(20).mean().replace(0, np.nan),
        # Volatility / regime context (NEW in v2)
        'atr_pct':           atr_v / close.replace(0, np.nan) * 100,
        'bb_width':          bb_width,
        'hl_range_pct':      (high - low) / close.replace(0, np.nan) * 100,
        # Strategy-aligned structure/trend context
        'adx14':             adx14,
        'gap_pct':           (open_ - prev_close) / prev_close.replace(0, np.nan) * 100,
        'candle_body_pct':   body / bar_range * 100,
        'upper_wick_pct':    upper_wick / bar_range * 100,
        'lower_wick_pct':    lower_wick / bar_range * 100,
        'close_pos_in_bar':  (close - low) / bar_range,
        'vwap_dev_abs':      ((close - vwap) / vwap.replace(0, np.nan) * 100).abs(),
        # Short horizon returns to align directional momentum behavior.
        'ret_1':             close.pct_change(1) * 100,
        'ret_3':             close.pct_change(3) * 100,
    }, index=df.index)

    return features


def _make_labels(df: pd.DataFrame) -> pd.Series:
    """Label each bar BUY / SELL / HOLD based on future price movement."""
    close         = _col_series(df, 'Close')
    future_return = close.shift(-FUTURE_BARS) / close - 1
    labels        = pd.Series(LABEL_HOLD, index=df.index)
    labels[future_return >  RETURN_THRESHOLD] = LABEL_BUY
    labels[future_return < -RETURN_THRESHOLD] = LABEL_SELL
    return labels


# ── Model persistence ─────────────────────────────────────────────────────────

def _model_path_tf(symbol: str, tf: str) -> str:
    safe = symbol.replace("/", "-").replace("=", "").replace("^", "")
    tf_safe = str(tf or "").strip().lower().replace("/", "-")
    return os.path.join(MODEL_DIR, f"{safe}_{tf_safe}_v{MODEL_VERSION}.pkl")


def train_model(symbol: str, timeframe: str = "1d"):
    """
    Train a Random Forest classifier on market data for a specific timeframe.

    - 1d: 5 years daily bars (Capital.com provider)
    - 4h: 12 months 4-hour bars (native 4h, no resampling)
    - 15m: 90 days 15-minute bars

    Saves model + scaler to disk. Returns (model, scaler) or (None, None).
    """
    now = datetime.now(timezone.utc)
    tf = str(timeframe or "1d").strip().lower()
    if tf in ("15m", "15min", "15"):
        period, interval, label = "90d", "15m", "90d 15m"
        tf = "15m"
    elif tf in ("4h", "240m", "240min"):
        period, interval, label = "12mo", "4h", "12mo 4h"
        tf = "4h"
    else:
        period, interval, label = "5y", "1d", "5y daily"
        tf = "1d"

    retry_key = f"{symbol.upper()}|{tf}"
    retry_at = _train_retry_after.get(retry_key)
    if retry_at and now < retry_at:
        return None, None

    log.info("Training RF model for %s (%s)...", symbol, label)
    df = scan_market(symbol, period=period, interval=interval)
    bars = len(df) if df is not None else 0
    if df is None or bars < MIN_ANALYSIS_BARS:
        # Retry later instead of re-attempting every scan cycle.
        _train_retry_after[retry_key] = now + timedelta(hours=TRAIN_RETRY_HOURS)
        log.warning(
            "Insufficient data for %s (%s, %s bars, need >= %s). Retry after %sh.",
            symbol, tf, bars, MIN_ANALYSIS_BARS, TRAIN_RETRY_HOURS
        )
        return None, None

    df = _flatten(df)
    features = build_features(df)
    labels   = _make_labels(df)

    combined = features.join(labels.rename('label')).dropna()
    if len(combined) < 100:
        log.warning("Too few clean samples for %s", symbol)
        return None, None

    X = combined.drop(columns='label').values
    y = combined['label'].values

    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_leaf=10,
        class_weight='balanced',
        random_state=42,
        n_jobs=1,          # single-threaded: avoids ResourceWarning on predict (1-row)
    )
    model.fit(X_scaled, y)

    with open(_model_path_tf(symbol, tf), 'wb') as f:
        pickle.dump({'model': model, 'scaler': scaler, 'version': MODEL_VERSION}, f)

    log.info("Model saved for %s (%s) — %d training samples", symbol, tf, len(combined))
    return model, scaler


def load_or_train_model(symbol: str, timeframe: str = "1d"):
    """Load a cached model or train a new one if not found or version mismatch.

    Uses an in-memory cache to avoid repeated disk IO during scan loops.
    """
    sym = str(symbol or "").strip().upper()
    tf = str(timeframe or "1d").strip().lower()
    if tf not in ("1d", "4h", "15m"):
        tf = "1d"

    cache_key = f"{sym}|{tf}" if sym else tf
    if cache_key in _model_cache:
        m, s, v = _model_cache[cache_key]
        if v == MODEL_VERSION:
            return m, s

    path = _model_path_tf(symbol, tf)
    if os.path.exists(path):
        try:
            with open(path, 'rb') as f:
                saved = pickle.load(f)
            if saved.get('version') == MODEL_VERSION:
                model, scaler = saved['model'], saved['scaler']
                if sym:
                    _model_cache[cache_key] = (model, scaler, MODEL_VERSION)
                return model, scaler
            log.info("Model version mismatch for %s (%s) — retraining", symbol, tf)
        except Exception as exc:
            log.warning("Failed to load model for %s (%s): %s — retraining", symbol, tf, exc)
    model, scaler = train_model(symbol, timeframe=tf)
    if sym and model and scaler:
        _model_cache[cache_key] = (model, scaler, MODEL_VERSION)
    return model, scaler


# ── Regime detection ──────────────────────────────────────────────────────────

def detect_regime(df: pd.DataFrame) -> dict:
    """
    Classify market regime from OHLCV data.

    Returns:
      {
        'type':    'TRENDING' | 'RANGING' | 'VOLATILE',
        'atr_pct': float,        # ATR as % of price
        'bb_width': float,       # Bollinger Band width %
        'trend_strength': float, # |EMA20 - EMA50| / EMA50 %
      }

    Rules:
      VOLATILE  — ATR% > 3.0 OR BB width > 8%
      TRENDING  — trend_strength > 2%  (clear EMA separation)
      RANGING   — everything else
    """
    df    = _flatten(df)
    close = df['Close'].squeeze().astype(float)
    high  = df['High'].squeeze().astype(float)
    low   = df['Low'].squeeze().astype(float)

    ema20 = _ema(close, 20)
    ema50 = _ema(close, 50)
    atr_v = _atr(df)
    bb_std = close.rolling(20).std()

    atr_pct        = float(atr_v.iloc[-1] / close.iloc[-1] * 100) if close.iloc[-1] > 0 else 0.0
    bb_w           = float(2 * bb_std.iloc[-1] / ema20.iloc[-1] * 100) if ema20.iloc[-1] > 0 else 0.0
    trend_strength = abs(float(ema20.iloc[-1] - ema50.iloc[-1]) / ema50.iloc[-1] * 100) if ema50.iloc[-1] > 0 else 0.0

    if atr_pct > 3.0 or bb_w > 8.0:
        regime_type = 'VOLATILE'
    elif trend_strength > 2.0:
        regime_type = 'TRENDING'
    else:
        regime_type = 'RANGING'

    return {
        'type':           regime_type,
        'atr_pct':        round(atr_pct, 2),
        'bb_width':       round(bb_w, 2),
        'trend_strength': round(trend_strength, 2),
    }


# ── Core gatekeeper ───────────────────────────────────────────────────────────

def validate_signal(
    symbol: str,
    direction: str,
    timeframes: dict,
    *,
    min_probability: float | None = None,
    ms_score: float | None = None,
) -> tuple:
    """
    AI Gatekeeper — the single mandatory checkpoint before every trade.

    Scores the PROPOSED direction (not generating its own) using the RF model.
    Applies a volatility penalty in VOLATILE regime.

    Parameters
    ----------
    symbol     : ticker string
    direction  : 'BUY' or 'SELL'
    timeframes : dict with '1d', '4h', '15m' DataFrames

    Returns
    -------
    (approved: bool, probability: float, regime: str)
      approved is True ONLY if probability >= AI_PROBABILITY_THRESHOLD.
    """
    df_15m = timeframes.get("15m")
    df_4h = timeframes.get("4h")
    df_1d = timeframes.get("1d")

    if not any(df is not None and not df.empty for df in (df_15m, df_4h, df_1d)):
        log.warning("[AI Gate %s] No data for validation — blocking", symbol)
        return False, 0.0, 'UNKNOWN'

    # Regime from fastest available timeframe.
    regime_df = df_15m if (df_15m is not None and not df_15m.empty) else (df_4h if (df_4h is not None and not df_4h.empty) else df_1d)
    regime = detect_regime(_flatten(regime_df))

    # Probability blend by timeframe model to match strategy inputs.
    probs: dict[str, float] = {}
    weights = {"1d": 0.20, "4h": 0.35, "15m": 0.45}
    for tf, df in (("1d", df_1d), ("4h", df_4h), ("15m", df_15m)):
        if df is None or df.empty:
            continue
        frame = _flatten(df)
        model, scaler = load_or_train_model(symbol, timeframe=tf)
        if model and scaler:
            probs[tf] = _direction_probability(frame, model, scaler, direction)
        else:
            probs[tf] = _rule_based_probability(frame, direction)

    if not probs:
        return False, 0.0, regime["type"]

    weight_sum = sum(weights[tf] for tf in probs.keys())
    probability = sum(probs[tf] * weights[tf] for tf in probs.keys()) / max(weight_sum, 1e-9)
    probability = round(float(probability), 1)

    # Optional market-structure context adjustment (soft, bounded).
    if ENABLE_MS_SCORE_AI_INTEGRATION and ms_score is not None:
        try:
            ms_val = float(ms_score)
            if np.isfinite(ms_val):
                delta = (ms_val - float(MS_SCORE_AI_NEUTRAL)) * float(MS_SCORE_AI_SCALE)
                cap = abs(float(MS_SCORE_AI_MAX_IMPACT))
                delta = max(-cap, min(cap, delta))
                probability = probability + delta
        except Exception:
            pass

    # Regime penalty: high volatility = uncertain environment
    if regime['type'] == 'VOLATILE':
        probability = round(probability * 0.90, 1)   # reduced penalty in volatile regimes
        log.info(
            "[AI Gate %s] VOLATILE regime penalty applied → probability=%.1f%%",
            symbol, probability,
        )
    else:
        probability = round(float(probability), 1)

    min_prob = AI_PROBABILITY_THRESHOLD if min_probability is None else float(min_probability)
    approved = probability >= min_prob

    log.info(
        "[AI Gate %s] direction=%s | probability=%.1f%% | regime=%s | %s",
        symbol, direction, probability, regime['type'],
        "APPROVED" if approved else "BLOCKED",
    )

    return approved, probability, regime['type']


def _direction_probability(df: pd.DataFrame, model, scaler, direction: str) -> float:
    """
    Extract the RF model's probability for a SPECIFIC direction.
    This is different from predict() which returns the winning direction.
    """
    features = build_features(df).dropna()
    if features.empty:
        return 0.0

    X     = scaler.transform(features.values[-1:])
    proba = model.predict_proba(X)[0]
    classes = list(model.classes_)

    target_label = LABEL_BUY if direction == 'BUY' else LABEL_SELL
    if target_label not in classes:
        return 0.0

    raw_prob = proba[classes.index(target_label)]
    return round(raw_prob * 100, 1)


def _rule_based_probability(df: pd.DataFrame, direction: str) -> float:
    """
    Rule-based probability estimate when no trained model is available.
    Checks EMA alignment, RSI, MACD direction alignment with proposed direction.
    Returns 0–100.
    """
    try:
        df = _flatten(df)
        close = _col_series(df, 'Close')
        ema20  = _ema(close, 20)
        ema50  = _ema(close, 50)
        ema200 = _ema(close, 200)
        rsi    = _rsi(close)
        _, _, hist = _macd(close)

        c   = float(close.iloc[-1])
        r   = float(rsi.iloc[-1])
        h   = float(hist.iloc[-1])
        e20 = float(ema20.iloc[-1])
        e50 = float(ema50.iloc[-1])
        e200 = float(ema200.iloc[-1])

        if direction == 'BUY':
            checks = [c > e20, c > e50, c > e200, r > 45, h > 0]
        else:
            checks = [c < e20, c < e50, c < e200, r < 55, h < 0]

        hit_rate = sum(checks) / len(checks)
        prob     = round(30.0 + hit_rate * 60.0, 1)    # range 30–90
        return prob

    except Exception:
        return 0.0


# ── Internal prediction (direction generator) ─────────────────────────────────

def _predict(df: pd.DataFrame, model, scaler) -> tuple:
    """
    Run the RF model on the most recent bar.
    Returns (action, confidence) — action is 'BUY', 'SELL', or None.
    """
    features = build_features(df).dropna()
    if features.empty:
        return None, 0.0

    X     = scaler.transform(features.values[-1:])
    proba = model.predict_proba(X)[0]
    classes = list(model.classes_)

    buy_prob  = proba[classes.index(LABEL_BUY)]  if LABEL_BUY  in classes else 0.0
    sell_prob = proba[classes.index(LABEL_SELL)] if LABEL_SELL in classes else 0.0

    if buy_prob >= sell_prob:
        return 'BUY',  round(buy_prob  * 100, 1)
    else:
        return 'SELL', round(sell_prob * 100, 1)


_DEEP_MS_FEATURE_COLUMNS = (
    "ms_score",
    "ms_regime_code",
    "ms_sweep_flag",
    "ms_liq_dist_pct",
    "ms_zone_bias_code",
)


def _enrich_deep_features_with_market_structure(
    features: pd.DataFrame,
    *,
    timeframe: str,
    timeframes: dict | None,
) -> pd.DataFrame:
    """
    Inference-only compatibility bridge for deep models trained with
    market-structure columns. Keeps training code unchanged.
    """
    if features is None or features.empty:
        return features
    if not isinstance(timeframes, dict) or not timeframes:
        return features
    if all(col in features.columns for col in _DEEP_MS_FEATURE_COLUMNS):
        return features

    try:
        from core.market_structure import apply_market_structure_policy
    except Exception:
        return features

    use_tf = str(timeframe or "").strip().lower()
    base_1d = _flatten(timeframes.get("1d")) if isinstance(timeframes.get("1d"), pd.DataFrame) else pd.DataFrame()
    base_4h = _flatten(timeframes.get("4h")) if isinstance(timeframes.get("4h"), pd.DataFrame) else pd.DataFrame()
    base_15m = _flatten(timeframes.get("15m")) if isinstance(timeframes.get("15m"), pd.DataFrame) else pd.DataFrame()
    target = _flatten(timeframes.get(use_tf)) if isinstance(timeframes.get(use_tf), pd.DataFrame) else pd.DataFrame()
    if target.empty or "Close" not in target.columns:
        return features

    out = features.copy()
    for col in _DEEP_MS_FEATURE_COLUMNS:
        if col not in out.columns:
            out[col] = np.nan

    regime_map = {"volatile": 0.0, "ranging": 1.0, "trending": 2.0}
    zone_map = {"premium": -1.0, "equilibrium": 0.0, "discount": 1.0}
    bias_map = {"bearish": -1.0, "neutral": 0.0, "bullish": 1.0}

    for ts in out.index:
        try:
            t_slice = target.loc[:ts]
            if t_slice.empty:
                continue
            close_px = float(t_slice["Close"].iloc[-1])
            s1 = base_1d.loc[:ts] if not base_1d.empty else base_1d
            s4 = base_4h.loc[:ts] if not base_4h.empty else base_4h
            s15 = base_15m.loc[:ts] if not base_15m.empty else base_15m
            ctx = apply_market_structure_policy(
                direction="BUY",
                close_price=close_px,
                df_1d=s1,
                df_4h=s4,
                df_15m=s15,
            )

            liq_dist_pct = 0.0
            if ctx.liq is not None and close_px > 0:
                levels = []
                for lvl in (ctx.liq.pdh, ctx.liq.pdl, ctx.liq.orh, ctx.liq.orl):
                    if lvl is not None:
                        try:
                            lv = float(lvl)
                            if np.isfinite(lv):
                                levels.append(lv)
                        except Exception:
                            pass
                if levels:
                    nearest = min(levels, key=lambda lv: abs(close_px - lv))
                    liq_dist_pct = ((close_px - nearest) / close_px) * 100.0

            out.at[ts, "ms_score"] = float(ctx.ms_score)
            out.at[ts, "ms_regime_code"] = float(regime_map.get(str(ctx.regime).lower(), 1.0))
            out.at[ts, "ms_sweep_flag"] = 1.0 if bool(ctx.sweep_detected) else 0.0
            out.at[ts, "ms_liq_dist_pct"] = float(liq_dist_pct)
            out.at[ts, "ms_zone_bias_code"] = float(
                zone_map.get(str(ctx.zone).lower(), 0.0) + bias_map.get(str(ctx.htf_bias).lower(), 0.0)
            )
        except Exception:
            continue

    # Forward-fill gaps so latest sequence can still be formed.
    out[list(_DEEP_MS_FEATURE_COLUMNS)] = out[list(_DEEP_MS_FEATURE_COLUMNS)].ffill()
    return out


def _predict_deep(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    *,
    timeframes: dict | None = None,
) -> tuple[str | None, float]:
    """
    Optional deep inference path.
    Returns (action, confidence) or (None, 0.0) when unavailable.
    """
    try:
        from utils.ml_direction.infer import (
            load_direction_bundle,
            predict_direction_from_features,
        )
    except Exception:
        return None, 0.0
    if not symbol:
        return None, 0.0
    bundle = load_direction_bundle(
        symbol=symbol,
        timeframe=str(timeframe).lower(),
        kind=DEEP_DIRECTION_INFERENCE_KIND,
    )
    if not bundle:
        return None, 0.0
    feats = build_features(df).dropna()
    if feats.empty:
        return None, 0.0
    feats = _enrich_deep_features_with_market_structure(
        feats,
        timeframe=str(timeframe).lower(),
        timeframes=timeframes,
    )
    action, conf = predict_direction_from_features(feats, bundle)
    if action in ("BUY", "SELL"):
        return action, conf

    # Compatibility fallback for single-class HOLD bundles:
    # keep deep path active by deriving direction from short-horizon momentum.
    try:
        inv_labels = set(int(v) for v in dict(bundle.get("inv_class_map", {})).values())
        if inv_labels == {0}:
            r3 = float(feats["ret_3"].iloc[-1]) if "ret_3" in feats.columns else 0.0
            r1 = float(feats["ret_1"].iloc[-1]) if "ret_1" in feats.columns else 0.0
            momentum = r3 if np.isfinite(r3) else r1
            side = "BUY" if momentum >= 0 else "SELL"
            proxy_conf = float(conf) if np.isfinite(conf) else 50.0
            proxy_conf = max(50.0, min(99.0, proxy_conf))
            return side, round(proxy_conf, 1)
    except Exception:
        pass

    return None, conf


# ── Public API ────────────────────────────────────────────────────────────────

def analyze_multi_timeframe(timeframes: dict, symbol: str = None) -> tuple:
    """
    RF-based multi-timeframe signal generator.

    Requires full directional alignment: 1D + 4H + 15M must all agree.
    Weighted confidence: 1D=40%, 4H=35%, 15M=25%.

    Returns (action, confidence, reason).
      action — 'BUY', 'SELL', or None if no alignment.

    NOTE: This generates a CANDIDATE signal. The caller must pass it through
    validate_signal() before executing any trade.
    """
    try:
        # Use native timeframe-specific models for full alignment with scanner/strategies.
        # Inference order:
        # - default: RF -> rule-based
        # - when ENABLE_DEEP_DIRECTION_INFERENCE=true: Deep -> RF -> rule-based
        models: dict[str, tuple[Any, Any]] = {}
        if symbol:
            models["1d"] = load_or_train_model(symbol, timeframe="1d")
            models["4h"] = load_or_train_model(symbol, timeframe="4h")
            models["15m"] = load_or_train_model(symbol, timeframe="15m")

        results = {}
        path_info: dict[str, str] = {}
        for tf in ("1d", "4h", "15m"):
            df = timeframes.get(tf)
            if df is None or df.empty:
                continue
            df = _flatten(df)
            use_tf = str(tf).lower()
            action = None
            conf = 0.0
            source = "rule"

            if ENABLE_DEEP_DIRECTION_INFERENCE and symbol:
                d_action, d_conf = _predict_deep(
                    symbol=symbol,
                    timeframe=use_tf,
                    df=df,
                    timeframes=timeframes,
                )
                if d_action in ("BUY", "SELL"):
                    action, conf, source = d_action, float(d_conf), "deep"

            if action is None:
                model, scaler = models.get(use_tf, (None, None))
                if model and scaler:
                    rf_action, rf_conf = _predict(df, model, scaler)
                    if rf_action in ("BUY", "SELL"):
                        action, conf, source = rf_action, float(rf_conf), "rf"

            if action is None:
                prob = _rule_based_probability(df, 'BUY')
                bullish = prob >= 50
                action = 'BUY' if bullish else 'SELL'
                conf = prob if bullish else round(100 - prob, 1)
                source = "rule"

            bullish = action == 'BUY'
            results[tf] = {'action': action, 'confidence': conf, 'bullish': bullish}
            path_info[tf] = source

        # Require the core strategy timeframes to be present for strict alignment.
        if any(tf not in results for tf in ("1d", "4h", "15m")):
            present = ",".join(sorted(results.keys())) if results else "none"
            return None, 0.0, f"Incomplete timeframe set for RF alignment ({present})"

        daily_bull = results["1d"]["bullish"]
        h4_bull = results["4h"]["bullish"]
        m15_bull = results["15m"]["bullish"]

        all_bullish = daily_bull and h4_bull and m15_bull
        all_bearish = not daily_bull and not h4_bull and not m15_bull

        if not all_bullish and not all_bearish:
            avg_conf = round(sum(r['confidence'] for r in results.values()) / 3, 1)
            reason   = (
                f"No alignment: 1D={'UP' if daily_bull else 'DN'} | "
                f"4H={'UP' if h4_bull else 'DN'} | "
                f"15M={'UP' if m15_bull else 'DN'}"
            )
            return None, avg_conf, reason

        direction     = 'BUY' if all_bullish else 'SELL'
        combined_conf = round(
            results['1d']['confidence']  * 0.20 +
            results['4h']['confidence']  * 0.35 +
            results['15m']['confidence'] * 0.45,
            1,
        )
        reason = (
            f"Aligned {'bullish' if all_bullish else 'bearish'}: "
            f"1D={results['1d']['confidence']}% | "
            f"4H={results['4h']['confidence']}% | "
            f"15M={results['15m']['confidence']}% | "
            f"path[1D={path_info.get('1d','?')},4H={path_info.get('4h','?')},15M={path_info.get('15m','?')}]"
        )
        return direction, combined_conf, reason

    except Exception as exc:
        log.error("analyze_multi_timeframe error: %s", exc)
        return None, 0.0, str(exc)


def train_deep_direction_model(
    symbol: str,
    *,
    timeframe: str = "15m",
    model_kind: str = "lstm",
    seq_len: int = 64,
    label_horizon: int = 8,
    label_threshold: float = 0.012,
    epochs: int = 25,
) -> dict:
    """
    Optional Phase 7 trainer bridge (LSTM/GRU/Transformer).
    Keeps RF path fully backward-compatible.
    """
    try:
        from utils.ml_direction.trainer import train_direction_for_symbol
    except Exception as exc:
        raise RuntimeError(
            "Deep direction model training unavailable. Install torch to enable Phase 7."
        ) from exc

    res = train_direction_for_symbol(
        symbol=symbol,
        timeframe=timeframe,
        model_kind=model_kind,
        seq_len=seq_len,
        label_horizon=label_horizon,
        label_threshold=label_threshold,
        epochs=epochs,
    )
    return {
        "model_path": res.model_path,
        "model_kind": res.model_kind,
        "best_val_loss": res.best_val_loss,
        "best_val_acc": res.best_val_acc,
        "epochs": res.epochs,
        "n_train": res.n_train,
        "n_val": res.n_val,
        "num_features": res.num_features,
        "num_classes": res.num_classes,
    }
