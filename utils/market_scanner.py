import yfinance as yf
import pandas as pd
import numpy as np


def scan_market(ticker_symbol, period="1d", interval="5m"):
    """Fetch OHLCV data for a single symbol. Kept for backward compatibility."""
    try:
        df = yf.download(ticker_symbol, period=period, interval=interval,
                         progress=False, auto_adjust=True, timeout=10)
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        print(f"❌ عطل في السكارنر [{ticker_symbol}]: {e}")
        return None


def scan_multi_timeframe(symbol):
    """
    Fetch data for all three analysis timeframes:
      - 1D  : master trend  (3 months of daily bars)
      - 4H  : zones         (1 month of 1H bars resampled to 4H)
      - 15M : entry signal  (5 days of 15-minute bars)

    Returns dict {'1d': df, '4h': df, '15m': df} or None on failure.
    """
    try:
        df_1d  = yf.download(symbol, period="3mo", interval="1d",  progress=False, auto_adjust=True)
        df_1h  = yf.download(symbol, period="1mo", interval="1h",  progress=False, auto_adjust=True)
        df_15m = yf.download(symbol, period="5d",  interval="15m", progress=False, auto_adjust=True)

        if any(df is None or df.empty for df in [df_1d, df_1h, df_15m]):
            return None

        # Flatten MultiIndex columns that yfinance sometimes produces
        for df in [df_1d, df_1h, df_15m]:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

        # Resample 1H → 4H (defensive against odd yfinance shapes / indices)
        if not isinstance(df_1h, pd.DataFrame):
            return None
        required = {"Open", "High", "Low", "Close", "Volume"}
        if not required.issubset(set(df_1h.columns)):
            return None

        df_1h = df_1h.copy()
        if not isinstance(df_1h.index, pd.DatetimeIndex):
            df_1h.index = pd.to_datetime(df_1h.index, errors="coerce")
            df_1h = df_1h.dropna(subset=[] if df_1h.empty else None)
        df_1h = df_1h.sort_index()
        df_1h = df_1h[~df_1h.index.duplicated(keep="last")]

        def _safe_first(s):
            return s.iloc[0] if len(s) else np.nan

        def _safe_last(s):
            return s.iloc[-1] if len(s) else np.nan

        df_4h = (
            df_1h.resample("4h")
            .agg(
                Open=("Open", _safe_first),
                High=("High", "max"),
                Low=("Low", "min"),
                Close=("Close", _safe_last),
                Volume=("Volume", "sum"),
            )
            .dropna()
        )

        return {"1d": df_1d, "4h": df_4h, "15m": df_15m}

    except Exception as e:
        print(f"❌ خطأ في جلب البيانات متعددة الأطر [{symbol}]: {e}")
        return None
