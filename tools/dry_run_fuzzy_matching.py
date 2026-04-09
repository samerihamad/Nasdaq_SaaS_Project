"""
Dry-run harness for Jules fuzzy matching logic in core.sync.fetch_closed_deal_final_data.

Runs without live Capital.com calls by stubbing requests.get.

Usage:
  python tools/dry_run_fuzzy_matching.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

# Ensure repo root on sys.path when running from tools/
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class _StubResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = int(status_code)
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


def _run_case(*, name: str, symbol: str, direction: str, size: float, closed_at: str, txs_window: list[dict]):
    import requests
    from core.sync import fetch_closed_deal_final_data

    base_url = "https://stub-capital/api/v1"
    headers = {"X-CAP-API-KEY": "stub", "CST": "stub", "X-SECURITY-TOKEN": "stub"}

    calls: list[tuple[str, dict]] = []

    def _stub_get(url, params=None, headers=None, timeout=None):
        calls.append((str(url), dict(params or {})))
        # Primary search by dealId returns empty -> triggers fuzzy window.
        if str(url).endswith("/history/transactions") and (params or {}).get("dealId") == "DID-123":
            return _StubResponse(200, {"transactions": []})
        # Fuzzy time window call returns provided txs.
        if str(url).endswith("/history/transactions") and ("from" in (params or {}) and "to" in (params or {})):
            return _StubResponse(200, {"transactions": list(txs_window)})
        # Other calls: return empty.
        return _StubResponse(200, {"transactions": []})

    real_get = requests.get
    requests.get = _stub_get
    try:
        out = fetch_closed_deal_final_data(
            base_url,
            headers,
            "DID-123",
            symbol=str(symbol),
            direction=str(direction),
            size=float(size),
            closed_at=str(closed_at),
            wait_for_realized=True,
            quiet=False,
            max_wall_sec=2.0,
            lookback_max=200,
        )
    finally:
        requests.get = real_get

    print(f"\n=== {name} ===")
    print("calls:")
    for u, p in calls:
        print(f"  GET {u} params={p}")
    print("result:", out)


def main():
    closed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # Case 1: BUY closed by SELL, exact symbol match, size within tolerance, valid rpl.
    _run_case(
        name="BUY close -> SELL tx fuzzy match",
        symbol="PTON",
        direction="BUY",
        size=10.0,
        closed_at=closed_at,
        txs_window=[
            {"instrumentName": "PTON", "direction": "SELL", "size": 10.0, "rpl": 3.21, "level": 14.55},
        ],
    )

    # Case 2: SELL closed by BUY, epic match, size within tolerance, valid rpl.
    _run_case(
        name="SELL close -> BUY tx fuzzy match (epic)",
        symbol="AAPL",
        direction="SELL",
        size=5.0,
        closed_at=closed_at,
        txs_window=[
            {"epic": "AAPL", "direction": "BUY", "size": 5.0, "rpl": -1.05, "level": 210.10},
        ],
    )

    # Case 3: Wrong close-side should NOT match.
    _run_case(
        name="Wrong side should not match",
        symbol="PTON",
        direction="BUY",
        size=10.0,
        closed_at=closed_at,
        txs_window=[
            {"instrumentName": "PTON", "direction": "BUY", "size": 10.0, "rpl": 2.0, "level": 14.55},
        ],
    )

    # Case 4: Missing rpl should NOT match.
    _run_case(
        name="Missing rpl should not match",
        symbol="PTON",
        direction="BUY",
        size=10.0,
        closed_at=closed_at,
        txs_window=[
            {"instrumentName": "PTON", "direction": "SELL", "size": 10.0, "level": 14.55},
        ],
    )

    # Case 5: Size outside tolerance should NOT match.
    _run_case(
        name="Size mismatch should not match",
        symbol="PTON",
        direction="BUY",
        size=10.0,
        closed_at=closed_at,
        txs_window=[
            {"instrumentName": "PTON", "direction": "SELL", "size": 10.1, "rpl": 1.0, "level": 14.55},
        ],
    )


if __name__ == "__main__":
    main()

