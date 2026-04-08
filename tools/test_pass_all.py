#!/usr/bin/env python3
"""
Execution smoke test — bypasses signal engine, strategies, and AI gatekeeper.

What this does NOT bypass (by design — core/executor.py):
  - Maintenance mode, NYSE calendar day, duplicate OPEN/PENDING trades,
    risk sizing, R:R, slippage guard, broker min size, session auth.

Usage (DEMO strongly recommended):
  set EXEC_SMOKE_TEST=1
  set I_ACCEPT_EXECUTION_SMOKE_TEST_RISK=YES
  python tools/test_pass_all.py --symbol AAPL --action BUY

Dry run (no order):
  python tools/test_pass_all.py --symbol AAPL --action BUY --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys

# Project root on path
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test Capital.com execution path (no AI/strategies).")
    parser.add_argument("--symbol", default=os.getenv("SMOKE_SYMBOL", "AAPL"), help="Ticker (e.g. AAPL)")
    parser.add_argument("--action", choices=("BUY", "SELL"), default=os.getenv("SMOKE_ACTION", "BUY"))
    parser.add_argument(
        "--chat-id",
        default=os.getenv("ADMIN_CHAT_ID") or os.getenv("SMOKE_CHAT_ID"),
        help="Subscriber chat_id with broker creds in DB (defaults to ADMIN_CHAT_ID)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only validate env and imports; do not call broker")
    args = parser.parse_args()

    if not args.chat_id:
        print("ERROR: Set --chat-id or ADMIN_CHAT_ID / SMOKE_CHAT_ID in environment.")
        return 2

    from dotenv import load_dotenv

    load_dotenv(os.path.join(_ROOT, ".env"))

    if args.dry_run:
        print("[DRY-RUN] Would call place_trade_for_user with:")
        print(f"  chat_id={args.chat_id} symbol={args.symbol} action={args.action}")
        print("  confidence=95.0 stop_loss_pct=0.02 strategy_label=SMOKE_TEST")
        print("No broker request sent.")
        return 0

    if os.getenv("EXEC_SMOKE_TEST", "").strip() != "1":
        print("Refusing: set EXEC_SMOKE_TEST=1 to enable live smoke path.")
        return 3
    if os.getenv("I_ACCEPT_EXECUTION_SMOKE_TEST_RISK", "").strip().upper() != "YES":
        print('Refusing: set I_ACCEPT_EXECUTION_SMOKE_TEST_RISK=YES (you accept real/demo orders).')
        return 4

    # Late import so --dry-run works without full stack
    from core.executor import place_trade_for_user

    result = place_trade_for_user(
        str(args.chat_id),
        str(args.symbol).upper(),
        str(args.action),
        confidence=95.0,
        stop_loss_pct=0.02,
        strategy_label="SMOKE_TEST",
        force_market=False,
        ai_prob=None,
        signal_price=None,
    )
    print("place_trade_for_user returned:")
    print(result)
    return 0 if (isinstance(result, str) and result.strip().startswith("✅")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
