from __future__ import annotations

import numpy as np
import pandas as pd

LABEL_SELL = -1
LABEL_HOLD = 0
LABEL_BUY = 1


def generate_direction_labels(
    close: pd.Series,
    *,
    horizon_bars: int = 8,
    return_threshold: float = 0.012,
) -> pd.Series:
    """
    Generate tri-class direction labels from future returns.

    - BUY  (+1): future return > +threshold
    - SELL (-1): future return < -threshold
    - HOLD (0): otherwise
    """
    if close is None or len(close) == 0:
        return pd.Series([], dtype=int)

    c = pd.to_numeric(close, errors="coerce")
    future_ret = c.shift(-int(horizon_bars)) / c - 1.0
    y = pd.Series(np.full(len(c), LABEL_HOLD, dtype=int), index=c.index)
    y[future_ret > float(return_threshold)] = LABEL_BUY
    y[future_ret < -float(return_threshold)] = LABEL_SELL
    return y
