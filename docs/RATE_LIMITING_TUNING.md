# Rate Limiting Configuration Tuning Guide

**Phase 4 — Apr 28, 2026**

This document explains the rate limiting configuration parameters added to fix HTTP 429 errors from Capital.com API.

---

## Overview

The trading engine now implements a multi-layered rate limiting strategy:

1. **Global Rate Limiter** (`global_rate_limiter`): Single token bucket shared across ALL users
2. **Per-User Rate Limiter** (`rate_limiter`): Isolated token bucket per user session
3. **Signal Dispatch Delay**: Fixed delay between signals to prevent burst
4. **Trade Execution Delay**: Fixed delay between subscriber executions
5. **Exponential Backoff with Jitter**: Retry strategy for transient failures (429, 503)

---

## Configuration Parameters

### Primary Rate Limiting

| Parameter | Default | Purpose | Tuning Guidance |
|-----------|---------|---------|-----------------|
| `CAPITAL_REQUEST_RATE_PER_SEC` | 2.0 | Global request rate (req/sec) | Start at 2.0. Increase to 3.0-4.0 if no 429s. |
| `CAPITAL_BURST_CAPACITY` | 5 | Max burst requests | Keep at 5. Increase to 8-10 if frequent bursts. |
| `SIGNAL_DISPATCH_DELAY_SECONDS` | 0.5 | Delay between signals | Reduce to 0.2s if latency is critical. |
| `TRADE_EXECUTION_DELAY_SECONDS` | 0.1 | Delay between trades per signal | Keep at 0.1s. Increase to 0.3s if 429s persist. |

### Retry & Backoff

| Parameter | Default | Purpose | Tuning Guidance |
|-----------|---------|---------|-----------------|
| `EXPONENTIAL_BACKOFF_BASE` | 1.0 | Base wait time for retries (seconds) | Keep at 1.0s. Increase to 2.0s if 429s recur. |
| `EXPONENTIAL_BACKOFF_MAX` | 60.0 | Max backoff duration (seconds) | Keep at 60s. Increase to 120s for extreme throttling. |
| `MAX_TRADE_RETRIES` | 5 | Max retry attempts per trade | Keep at 5. Reduce to 3 if queue depth grows. |
| `TRADE_EXECUTION_TIMEOUT` | 60.0 | Timeout per trade (seconds) | Keep at 60s. Increase to 120s for slow networks. |

### Queue Configuration

| Parameter | Default | Purpose | Tuning Guidance |
|-----------|---------|---------|-----------------|
| `TRADE_QUEUE_MAX_WORKERS` | 1 | Sequential (1) vs parallel (2+) | Keep at 1 for rate limiting. Increase to 2+ for throughput. |
| `TRADE_QUEUE_ENABLED` | true | Enable queue-based execution | Keep true. Set false for direct execution (not recommended). |

---

## Tuning Workflow

### Step 1: Baseline Testing

Run the engine with default settings for 3 trading days. Collect metrics:

```bash
# Check 429 rate
grep "429" logs/2026-04-28/rate_limit_events.txt | wc -l

# Check success rate
grep "status=FILLED" logs/2026-04-28/execution_summary.txt | wc -l
grep "status=FAILED" logs/2026-04-28/execution_summary.txt | wc -l
```

**Target**: 0 429s, 95%+ success rate.

### Step 2: If 429s Present

Reduce rate limit parameters:

```env
CAPITAL_REQUEST_RATE_PER_SEC=1.0  # Down from 2.0
CAPITAL_BURST_CAPACITY=3           # Down from 5
SIGNAL_DISPATCH_DELAY_SECONDS=0.8   # Up from 0.5
TRADE_EXECUTION_DELAY_SECONDS=0.3   # Up from 0.1
```

Restart engine and retest.

### Step 3: If No 429s but High Latency

Increase rate limit parameters:

```env
CAPITAL_REQUEST_RATE_PER_SEC=3.0  # Up from 2.0
CAPITAL_BURST_CAPACITY=8           # Up from 5
SIGNAL_DISPATCH_DELAY_SECONDS=0.2   # Down from 0.5
```

Monitor for 429s over next 2 trading days.

### Step 4: Load Testing

Simulate high load (50 signals × 3 users = 150 trades):

```python
# In tests/test_trade_queue.py, add load test
def test_load_150_trades():
    q = TradeQueue(executor_fn=mock_executor)
    for i in range(150):
        q.enqueue(chat_id=f"user_{i%3}", symbol=f"SYM{i}", action="BUY", confidence=80.0)
    q.start()
    time.sleep(60)  # allow processing
    q.stop()
    stats = q.stats()
    assert stats["success_rate"] >= 0.85  # 85%+ under load
```

**Target**: 85%+ success rate, no deadlocks, stable memory.

---

## Monitoring

### Key Metrics

| Metric | Source | Healthy Range |
|--------|--------|---------------|
| 429 Count | `rate_limit_events.txt` | 0 |
| Success Rate | `execution_summary.txt` | 95%+ |
| Avg Latency | `execution_summary.txt` | < 2s |
| Queue Depth | `trade_queue.stats()` | < 10 |
| Global Tokens | `global_rate_limiter.available` | > 0 |

### Console Output

At startup, config is logged:

```
[CONFIG] Capital.com rate limiting: 2.0 req/sec, burst capacity: 5
[CONFIG] Signal dispatch delay: 0.5s, trade execution delay: 0.1s
[CONFIG] Exponential backoff: base=1.0s max=60.0s, max retries: 5
```

At cycle end, health report is logged:

```
[EXEC MONITOR] Trades: 8/10 filled (80.0%) | 429s: 0 | avg_latency: 1.2s | avg_attempts: 1.1 | cycle: 15.3s
```

---

## Troubleshooting

### Issue: Still Getting HTTP 429

**Diagnosis**: Check `rate_limit_events.txt` for frequency.

**Solution**:
```env
CAPITAL_REQUEST_RATE_PER_SEC=1.0
CAPITAL_BURST_CAPACITY=3
```

### Issue: Trades Queued But Never Execute

**Diagnosis**: Check `trade_queue.stats()` for depth > 0.

**Solution**: Verify `trade_queue.start()` was called. Check worker logs for errors.

### Issue: High Latency (> 5s)

**Diagnosis**: Check `execution_summary.txt` for elapsed times.

**Solution**:
```env
CAPITAL_REQUEST_RATE_PER_SEC=3.0
SIGNAL_DISPATCH_DELAY_SECONDS=0.2
```

### Issue: Memory Leak / Queue Growing

**Diagnosis**: Check `trade_queue.stats()["peak_queue_depth"]` increasing.

**Solution**: Verify `trade_queue.stop()` called on shutdown. Check for exceptions in worker threads.

---

## Rollback Plan

If rate limiting causes issues, disable queue and revert to direct execution:

```env
TRADE_QUEUE_ENABLED=false
```

This bypasses the queue but keeps the global rate limiter active.

---

## Contact

For issues or questions, check:
- `logs/system_errors.log` for broker errors
- `logs/2026-MM-DD/execution_details.txt` for per-attempt details
- `logs/2026-MM-DD/rate_limit_events.txt` for 429 events
