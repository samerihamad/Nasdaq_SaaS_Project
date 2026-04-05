# Execution Pipeline Trace Map

This document maps the live trade execution flow in `core/executor.py` for operations and incident response.

## Primary Orchestrator

- Entry point: `place_trade_for_user(...)`
- Role: orchestration only; delegates work to private pipeline layers and aborts fast on stage failure.

Execution order:

1. `_validate_execution_gate()`
2. `_generate_protection_levels()`
3. `_calculate_position_sizing()`
4. `_check_slippage_safety()`
5. `_execute_broker_order()` (inside per-leg execution loop)

---

## Stage 1 — `_validate_execution_gate`

Purpose:

- Block unsafe or invalid entries before any expensive execution logic.

Checks included:

- maintenance mode
- NYSE trading day guard
- local duplicate guard (`OPEN` / `PENDING`)
- session window guard
- circuit breaker / risk open gate
- user credentials and Capital session
- daily drawdown limit
- broker symbol support (EPIC resolution)
- broker live duplicate position check
- correlation/sector concentration guard (local DB notional only)

Inputs:

- `chat_id`, `symbol`, `action`

Outputs:

- `ok` (bool), `message` (str), `context` (dict)
- `context` includes: `lang`, `creds`, `base_url`, `headers`, `balance`, `free_margin`, `order_epic`, `scanner_ctx`

Failure behavior:

- Immediate abort with reason string.
- Telemetry increments:
  - `weekend_blocks`
  - `local_guard_blocks`

Correlation guard details:

- Correlated group example: `TECH = {AAPL, MSFT, GOOGL, NVDA, QQQ}`
- If group exposure exceeds 20% of account equity, new entries in that group are rejected.
- Guard uses local DB (`trades`) exposure snapshot and does not issue additional broker API calls for this check.

---

## Stage 2 — `_generate_protection_levels`

Purpose:

- Build trade protection profile using current market + institutional SL flow.

What it computes:

- `entry_price`
- weighted ATR path (15m/1h integration via existing execution logic)
- effective SL %
- institutional SL (`generate_institutional_stop_loss`)
- market tradeability check
- RR gate validation
- regime label (`VOLATILE` / `TRENDING` / `RANGING`) for execution pricing hints

Inputs:

- gate context (`base_url`, `headers`, `order_epic`, `scanner_ctx`)
- trade intent (`chat_id`, `symbol`, `action`, `confidence`, `stop_loss_pct`, `strategy_label`)

Outputs:

- `ok`, `message`, `context`
- `context` includes: `entry_price`, `atr`, `effective_sl_pct`, `stop_price`, `min_dist`, `max_dist`, `rsi_15m_gate`, `rr_ratio`

Failure behavior:

- Immediate abort if invalid entry price, market not tradeable, or RR failure.

---

## Stage 3 — `_calculate_position_sizing`

Purpose:

- Produce final position size under leverage/margin/exposure controls.

What it computes:

- effective leverage
- exposure snapshot
- pre-trade validation result
- final size (`pretrade_size` or fallback sizing model)

Inputs:

- `chat_id`, `symbol`, `action`, `confidence`, `strategy_label`
- financial context: `balance`, `free_margin`
- protection context: `entry_price`, `stop_price`, `effective_sl_pct`, `rsi_15m_gate`

Outputs:

- `ok`, `message`, `context`
- `context` includes: `size`, `pretrade_size`, `pre_details`, `leverage`

Failure behavior:

- Immediate abort with pre-trade rejection reason.

---

## Stage 4 — `_check_slippage_safety`

Purpose:

- Block stale signals when market moved too far from signal reference.

Math:

- `calculated_slippage_pct = abs(latest - signal_price) / signal_price`
- threshold: `MAX_SLIPPAGE_PCT` (default `0.003` = 0.3%)

Inputs:

- `chat_id`, `symbol`, `action`
- `signal_price`
- broker session context (`base_url`, `headers`, `order_epic`)

Outputs:

- `ok`, `message`, `calculated_slippage_pct`

Failure behavior:

- Immediate abort with `[SLIPPAGE]` message.
- Telemetry increments `slippage_aborts`.

---

## Stage 5 — `_execute_broker_order`

Purpose:

- The only stage that performs final broker order open calls.

What it does:

- executes open order per leg through `_open_position_with_protection`
- confirms deal visibility
- appends leg metadata (`deal_id`, `deal_reference`, `capital_order_id`)
- returns structured result to orchestrator

Inputs:

- broker context + trade protection + leg data

Outputs:

- `(ok, error_message, effective_target)`

Failure behavior:

- Immediate leg failure returned to orchestrator.
- orchestrator handles rollback of any already-opened legs.

Volatility-aware limit pricing:

- In `VOLATILE` regime, limit entry is tightened toward current market using a small ATR fraction.
- Purpose: improve fill probability during fast moves while preserving core protection math.

---

## Post-Execution Actions (Orchestrator)

After successful leg execution:

- sync TP/SL to broker for each leg
- persist open legs in local DB
- send Telegram trade message
- write audit event (`order_opened`)
- write execution log:
  - `[TRADE_EXECUTION] ... calculated_slippage=... multi_tf_atr_value=...`

---

## Telemetry Counters (Daily Reset)

Stored in executor in-memory counters:

- `slippage_aborts`
- `local_guard_blocks`
- `weekend_blocks`

Reset behavior:

- auto-reset when UTC date changes on the next counter bump.

Log markers:

- `[EXEC_TELEMETRY] day=... slippage_aborts=... local_guard_blocks=... weekend_blocks=...`

---

## Monitor Cycle State Guard (Optional Safety)

In `monitor_and_close(...)`:

- local-open-trades hash and broker-positions hash are compared.
- on mismatch:
  - audit stage `state_hash_mismatch`
  - close cycle is skipped for safety (no close calls issued in that cycle)

Adaptive trailing intelligence:

- Trailing ATR multiplier is ADX-aware:
  - `ADX > 25`: tighter trail (faster lock-in)
  - `ADX < 20`: looser trail (noise tolerance)
  - otherwise: base multiplier

---

## Operations Quick Triage

If a trade did not open:

1. Search `execution_events.txt` for latest `stage=...` row by `chat_id/symbol`.
2. Identify failing stage:
   - `execution_guard_closed_day`
   - `local_execution_guard_duplicate`
   - `slippage_guard_aborted`
   - `pre_trade_validation_reject`
   - `order_leg_failed`
3. Correlate with `[TRADE_EXECUTION]` and `[EXEC_TELEMETRY]` in app logs.

If closes are unexpectedly skipped:

- Check for `stage=state_hash_mismatch` in `execution_events.txt`.

