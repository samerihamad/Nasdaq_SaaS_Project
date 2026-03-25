"""
AI Model — NATB v2.0

Role: GATEKEEPER, not observer.

Architecture:
  1. Technical strategies (MeanRev, Momentum, RF) generate a candidate signal.
  2. validate_signal() scores that specific direction using the RF model.
     If probability < AI_PROBABILITY_THRESHOLD (65%), the trade is BLOCKED.
  3. detect_regime() classifies market context (TRENDING / RANGING / VOLATILE).
     A VOLATILE regime applies a probability penalty.

Key public API:
  validate_signal(symbol, direction, timeframes) -> (approved, probability, regime)
  detect_regime(df)                              -> dict
  analyze_multi_timeframe(timeframes, symbol)    -> (action, confidence, reason)
  load_or_train_model(symbol)                    -> (model, scaler)

MODEL_VERSION is embedded in saved .pkl files. A version mismatch triggers
automatic retraining so stale models with different feature sets never silently
produce wrong scores.
"""

import os
import pickle
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import yfinance as yf

log = logging.getLogger(__name__)

MODEL_DIR             = "models"
MODEL_VERSION         = 2          # bump whenever build_features() columns change
AI_PROBABILITY_THRESHOLD = 60.0    # minimum probability to approve a trade
MIN_TRAIN_BARS        = 220        # supports EMA200 + labeling while allowing newer listings
TRAIN_RETRY_HOURS     = 6          # avoid retry spam for symbols with short history

os.makedirs(MODEL_DIR, exist_ok=True)
_train_retry_after: dict[str, datetime] = {}
_model_cache: dict[str, tuple[Any, Any, int]] = {}  # symbol -> (model, scaler, version)

# Label parameters
FUTURE_BARS      = 5       # look-ahead bars for labeling
RETURN_THRESHOLD = 0.015   # 1.5% move to qualify as BUY/SELL

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
    typical = (df['High'] + df['Low'] + df['Close']).squeeze() / 3
    volume  = df['Volume'].squeeze()
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
    high  = df['High'].squeeze().astype(float)
    low   = df['Low'].squeeze().astype(float)
    close = df['Close'].squeeze().astype(float)
    prev  = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev).abs(),
        (low  - prev).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


# ── Feature engineering ───────────────────────────────────────────────────────

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a feature matrix from an OHLCV DataFrame.

    Columns (v2 — MODEL_VERSION = 2):
      Price / EMA distances, RSI, MACD, VWAP, volume ratio,
      ATR% (volatility), BB width (regime), high-low range%
    """
    df = _flatten(df)

    close  = df['Close'].squeeze().astype(float)
    high   = df['High'].squeeze().astype(float)
    low    = df['Low'].squeeze().astype(float)
    volume = df['Volume'].squeeze().astype(float)

    ema20  = _ema(close, 20)
    ema50  = _ema(close, 50)
    ema200 = _ema(close, 200)
    rsi    = _rsi(close)
    macd_line, sig_line, hist = _macd(close)
    vwap   = _vwap(df)
    atr_v  = _atr(df)

    # Bollinger Band width as regime proxy
    bb_mid   = _ema(close, 20)
    bb_std   = close.rolling(20).std()
    bb_width = (2 * bb_std) / bb_mid.replace(0, np.nan) * 100

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
    }, index=df.index)

    return features


def _make_labels(df: pd.DataFrame) -> pd.Series:
    """Label each bar BUY / SELL / HOLD based on future price movement."""
    close         = df['Close'].squeeze().astype(float)
    future_return = close.shift(-FUTURE_BARS) / close - 1
    labels        = pd.Series(LABEL_HOLD, index=df.index)
    labels[future_return >  RETURN_THRESHOLD] = LABEL_BUY
    labels[future_return < -RETURN_THRESHOLD] = LABEL_SELL
    return labels


# ── Model persistence ─────────────────────────────────────────────────────────

def _model_path(symbol: str) -> str:
    safe = symbol.replace("/", "-").replace("=", "").replace("^", "")
    return os.path.join(MODEL_DIR, f"{safe}_v{MODEL_VERSION}.pkl")


def train_model(symbol: str):
    """
    Train a Random Forest classifier on 5 years of daily data.
    Saves model + scaler to disk. Returns (model, scaler) or (None, None).
    """
    now = datetime.now(timezone.utc)
    retry_at = _train_retry_after.get(symbol)
    if retry_at and now < retry_at:
        return None, None

    log.info("Training RF model for %s (5y daily)...", symbol)
    df = yf.download(symbol, period="5y", interval="1d",
                     progress=False, auto_adjust=True)
    bars = len(df) if df is not None else 0
    if df is None or bars < MIN_TRAIN_BARS:
        # Retry later instead of re-attempting every scan cycle.
        _train_retry_after[symbol] = now + timedelta(hours=TRAIN_RETRY_HOURS)
        log.warning(
            "Insufficient data for %s (%s bars, need >= %s). Retry after %sh.",
            symbol, bars, MIN_TRAIN_BARS, TRAIN_RETRY_HOURS
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

    with open(_model_path(symbol), 'wb') as f:
        pickle.dump({'model': model, 'scaler': scaler, 'version': MODEL_VERSION}, f)

    log.info("Model saved for %s — %d training samples", symbol, len(combined))
    return model, scaler


def load_or_train_model(symbol: str):
    """Load a cached model or train a new one if not found or version mismatch.

    Uses an in-memory cache to avoid repeated disk IO during scan loops.
    """
    sym = str(symbol or "").strip().upper()
    if sym and sym in _model_cache:
        m, s, v = _model_cache[sym]
        if v == MODEL_VERSION:
            return m, s
    path = _model_path(symbol)
    if os.path.exists(path):
        try:
            with open(path, 'rb') as f:
                saved = pickle.load(f)
            if saved.get('version') == MODEL_VERSION:
                model, scaler = saved['model'], saved['scaler']
                if sym:
                    _model_cache[sym] = (model, scaler, MODEL_VERSION)
                return model, scaler
            log.info("Model version mismatch for %s — retraining", symbol)
        except Exception as exc:
            log.warning("Failed to load model for %s: %s — retraining", symbol, exc)
    model, scaler = train_model(symbol)
    if sym and model and scaler:
        _model_cache[sym] = (model, scaler, MODEL_VERSION)
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
    df_15m = timeframes.get('15m')
    df_1d  = timeframes.get('1d')

    # Avoid boolean evaluation on DataFrames (ambiguous truth value).
    if df_15m is not None and not df_15m.empty:
        df_primary = df_15m
    elif df_1d is not None and not df_1d.empty:
        df_primary = df_1d
    else:
        df_primary = None

    if df_primary is None:
        log.warning("[AI Gate %s] No data for validation — blocking", symbol)
        return False, 0.0, 'UNKNOWN'

    df_primary = _flatten(df_primary)

    # Regime check
    regime = detect_regime(df_primary)

    # Load model and compute direction probability
    model, scaler = load_or_train_model(symbol)

    if model and scaler:
        probability = _direction_probability(df_primary, model, scaler, direction)
    else:
        # Fallback: rule-based probability estimate
        probability = _rule_based_probability(df_primary, direction)

    # Regime penalty: high volatility = uncertain environment
    if regime['type'] == 'VOLATILE':
        probability = round(probability * 0.95, 1)   # -5% penalty
        log.info(
            "[AI Gate %s] VOLATILE regime penalty applied → probability=%.1f%%",
            symbol, probability,
        )

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
        close = df['Close'].squeeze().astype(float)
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
        prob     = round(40.0 + hit_rate * 50.0, 1)    # range 40–90
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
        model, scaler = (None, None)
        if symbol:
            model, scaler = load_or_train_model(symbol)

        results = {}
        for tf, df in timeframes.items():
            df = _flatten(df)
            if model and scaler:
                action, conf = _predict(df, model, scaler)
                bullish = action == 'BUY'
            else:
                prob    = _rule_based_probability(df, 'BUY')
                bullish = prob >= 50
                action  = 'BUY' if bullish else 'SELL'
                conf    = prob if bullish else round(100 - prob, 1)
            results[tf] = {'action': action, 'confidence': conf, 'bullish': bullish}

        daily_bull = results['1d']['bullish']
        h4_bull    = results['4h']['bullish']
        m15_bull   = results['15m']['bullish']

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
            results['1d']['confidence']  * 0.40 +
            results['4h']['confidence']  * 0.35 +
            results['15m']['confidence'] * 0.25,
            1,
        )
        reason = (
            f"RF aligned {'bullish' if all_bullish else 'bearish'}: "
            f"1D={results['1d']['confidence']}% | "
            f"4H={results['4h']['confidence']}% | "
            f"15M={results['15m']['confidence']}%"
        )
        return direction, combined_conf, reason

    except Exception as exc:
        log.error("analyze_multi_timeframe error: %s", exc)
        return None, 0.0, str(exc)
