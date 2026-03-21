# NATB v2.0 — Nasdaq Algorithmic Trading Bot

A production-ready, multi-strategy SaaS trading bot built on Capital.com.
Every trade is gated by a circuit-breaker risk engine before execution.

---

## Architecture Overview

```
main.py
 ├── Heartbeat thread          (watchdog.py monitors this)
 ├── Backup thread             (hourly encrypted cloud backup)
 └── Trading loop
      ├── market hours check   (pre-market / open / closed)
      ├── daily scan           (Nasdaq universe → Level 1/2/3 filters)
      ├── RF model analysis    (utils/ai_model.py)
      ├── Mean Reversion       (core/strategy_meanrev.py)
      ├── Momentum             (core/strategy_momentum.py)
      └── dispatch_signal()
           └── place_trade_for_user()
                ├── can_open_trade()     ← RISK GATE (required)
                ├── duplicate check
                ├── calculate_position_size()
                └── Capital.com order

Position monitoring (core/executor.py → monitor_and_close)
 └── ATR trailing stop → auto-close → record_trade_result()
      └── updates Circuit Breaker state machine
```

---

## Strategy Signals

Three independent analyzers run on every ticker each cycle.
The **highest-confidence** signal that passes its threshold is dispatched.

### 1. RF Model (`utils/ai_model.py`)
- Random Forest trained on 5 years of daily bars per ticker.
- Features: EMA20/50/200 distances, RSI, MACD, VWAP, volume ratio.
- Three-timeframe alignment required: 1D + 4H + 15M must all agree.
- Threshold: `MIN_CONFIDENCE = 70%` (set in `main.py`).

### 2. Mean Reversion (`core/strategy_meanrev.py`)
- Entry: RSI < 30 (BUY) or RSI > 70 (SELL) on the 15m chart.
- **News Trap filter**: skips signal if entry bar gapped > 3% (news-driven move).
- **Reversal Candle required**: Hammer / Bullish Engulfing for BUY,
  Shooting Star / Bearish Engulfing for SELL.
- **Liquidity Sweep detector**: price swept a swing extreme then closed back inside.
- 4H VWAP deviation zone and 1D alignment add score.
- Threshold: composite score >= 55 (maps to ~65–95% confidence).

### 3. Momentum (`core/strategy_momentum.py`)
- ADX > 25 required to confirm a real trend (not chop).
- MACD fresh crossover on 15m.
- Volume spike: current bar >= 1.8× 20-bar average.
- 1D EMA20/50 alignment + RSI > 50 (BUY) or < 50 (SELL).
- Optional gap confirmation (> 1%) and NewsAPI headline boost.
- Threshold: composite score >= 60.

---

## Risk Engine (core/risk_manager.py)

The risk engine is the heart of the system. **No trade is placed without
passing `can_open_trade()`.**

| State            | Meaning                                         |
|------------------|-------------------------------------------------|
| `NORMAL`         | Trading allowed                                 |
| `CIRCUIT_BREAKER`| 2 consecutive losses — new entries blocked      |
| `MANUAL_OVERRIDE`| User approved one extra trade via /override     |
| `HARD_BLOCK`     | Override trade was a loss — locked until next day |

**Position sizing** is dynamic and confidence-based:
- 70% confidence → 1.5% of balance at risk
- 100% confidence → 3.0% of balance at risk
- Stop distance = ATR × 2.0 (entry ± stop defines the dollar risk)

**Open positions are never touched by the circuit breaker** — they stay
active until the ATR trailing stop or hard 3% portfolio stop is hit.

---

## Setup

### 1. Install dependencies
```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### 2. Configure environment variables
Create a `.env` file in the project root:
```env
TELEGRAM_BOT_TOKEN=your_bot_token
ADMIN_CHAT_ID=your_telegram_chat_id
ENCRYPTION_KEY=your_fernet_key          # generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Optional — enables momentum news quality boost
NEWS_API_KEY=your_newsapi_org_key

# Optional — enables auto-restart on crash
WATCHDOG_AUTO_RESTART=true
```

### 3. Initialise the database
```bash
python -c "from database.db_manager import create_db; create_db()"
```

### 4. Run the bot
```bash
# Primary trading engine (main thread = Telegram bot)
python main.py

# Watchdog — run as a separate process
python watchdog.py
```

---

## Hyperparameters (`config.py`)

All strategy thresholds live in `config.py`. Key ones to tune:

| Variable              | Default | Description                              |
|-----------------------|---------|------------------------------------------|
| `WATCHLIST`           | 15 tickers | Symbols to scan every cycle           |
| `MR_RSI_OVERSOLD`     | 30      | RSI buy-reversal threshold               |
| `MR_RSI_OVERBOUGHT`   | 70      | RSI sell-reversal threshold              |
| `MR_VWAP_DEV_PCT`     | 1.5     | Minimum % VWAP deviation for mean-rev    |
| `MR_NEWS_TRAP_GAP_PCT`| 3.0     | Gap % that triggers news-trap skip       |
| `MOM_ADX_THRESHOLD`   | 25      | Minimum ADX for momentum entries         |
| `MOM_VOL_RATIO`       | 1.8     | Volume spike multiplier                  |
| `MOM_GAP_PCT`         | 1.0     | Minimum gap % for gap-momentum boost     |
| `ATR_STOP_MULTIPLIER` | 2.0     | Stop = entry ± ATR × this               |
| `SCAN_INTERVAL_SEC`   | 300     | Seconds between full market scans        |
| `MAX_DAILY_TRADES`    | 5       | Per-user daily position cap              |

---

## File Structure

```
Nasdaq_SaaS_Project/
├── main.py                    # Entry point — trading engine + bot launcher
├── watchdog.py                # Separate process — server health monitor
├── config.py                  # All hyperparameters
│
├── core/
│   ├── risk_manager.py        # Circuit Breaker state machine + position sizing
│   ├── executor.py            # Order placement + position monitoring
│   ├── trailing_stop.py       # ATR trailing stop logic
│   ├── sync.py                # DB ↔ broker reconciliation
│   ├── watcher.py             # Multi-user position watcher
│   ├── signal_engine.py       # Multi-user scan loop (all subscribers)
│   ├── strategy_meanrev.py    # Mean Reversion strategy
│   └── strategy_momentum.py   # Momentum strategy
│
├── utils/
│   ├── market_scanner.py      # Multi-timeframe yfinance data fetcher
│   ├── ai_model.py            # Random Forest model + rule-based fallback
│   ├── filters.py             # Nasdaq universe Level 1/2 filters
│   └── market_hours.py        # Market open/closed/pre-market detection
│
├── bot/
│   ├── dashboard.py           # Telegram bot commands
│   ├── notifier.py            # Telegram message sender
│   ├── registration.py        # User onboarding
│   ├── licensing.py           # License key validation + encryption
│   ├── admin.py               # Admin commands
│   └── i18n.py                # Arabic / English translations
│
└── database/
    └── db_manager.py          # SQLite schema + CRUD helpers
```

---

## Telegram Commands

| Command         | Description                                      |
|-----------------|--------------------------------------------------|
| `/start`        | Register / show dashboard                        |
| `/override`     | Allow one extra trade after Circuit Breaker      |
| `/stop_today`   | Lock all new entries until tomorrow              |
| `/mode`         | Toggle AUTO / HYBRID signal approval             |

---

## Deployment (Linux / VPS)

```ini
# /etc/systemd/system/natb-engine.service
[Unit]
Description=NATB Trading Engine
After=network.target

[Service]
WorkingDirectory=/path/to/Nasdaq_SaaS_Project
ExecStart=/path/to/.venv/bin/python main.py
Restart=always
RestartSec=10
StandardOutput=append:/path/to/main.log
StandardError=append:/path/to/main.log

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/natb-watchdog.service
[Unit]
Description=NATB Watchdog
After=natb-engine.service

[Service]
WorkingDirectory=/path/to/Nasdaq_SaaS_Project
ExecStart=/path/to/.venv/bin/python watchdog.py
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable natb-engine natb-watchdog
sudo systemctl start  natb-engine natb-watchdog
sudo journalctl -fu natb-engine   # live logs
```
