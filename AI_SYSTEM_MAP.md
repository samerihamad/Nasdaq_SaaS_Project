# AI System Map (Full Discovery)

This document inventories all AI/ML logic in the project and how each part behaves in production.

## Runtime Decision Flow

1. `core/signal_engine.py::scan_watchlist_parallel()` scans symbols and builds candidate signals.
2. Candidate sources:
   - RF multi-timeframe (`utils/ai_model.py::analyze_multi_timeframe`)
   - Mean Reversion (`core/strategy_meanrev.py::analyze`)
   - Momentum (`core/strategy_momentum.py::analyze`)
3. Best candidate per symbol is forwarded to `main.py::dispatch_signal()`.
4. `dispatch_signal()` applies **AI Gatekeeper** (`utils/ai_model.py::validate_signal`).
5. If approved (or soft override), per-user dispatch runs and order execution continues in `core/executor.py`.

---

## AI Components Inventory

| System | File(s) | Purpose | Inputs | Outputs | Type | Retraining | .env deps |
|---|---|---|---|---|---|---|---|
| RF multi-timeframe model | `utils/ai_model.py` | Direction candidate and probability scoring across 1D/4H/15m | OHLCV frames + symbol | action/confidence + direction probability | ML (RandomForest) | On-demand/version-based (`MODEL_VERSION`) | `SIGNAL_*`, `AI_MIN_PROB_RF` |
| Momentum model | `core/strategy_momentum.py` | Trend continuation signal with composite score | 15m/4h/1d data, optional news | signal dict with score/confidence | Rule-based | No | `MOM_*`, `SIGNAL_MOM_MIN_SCORE`, `NEWS_*` |
| Mean Reversion model | `core/strategy_meanrev.py` | Reversal setup signal with score | 15m/4h/1d data | signal dict with score/confidence | Rule-based | No | `MR_*`, `SIGNAL_MR_MIN_SCORE` |
| Market Structure AI context | `core/market_structure.py` (+ enrich in `utils/ai_model.py`) | Adds structure context (ms_score, regime, sweep, liquidity distance, zone bias) | HTF/LTF OHLCV | context fields and optional score modifiers | Rule-derived context | No | `ENABLE_MARKET_STRUCTURE_FILTERS`, `MARKET_STRUCTURE_*`, `LIQUIDITY_*` |
| AI Gatekeeper | `utils/ai_model.py::validate_signal` | Final mandatory pre-trade approval | symbol, direction, timeframes, ms_score | `(approved, probability, regime)` | ML + rules | Depends on RF/deep artifacts | `AI_MIN_PROB_*`, `ENABLE_MS_SCORE_AI_INTEGRATION`, `MS_SCORE_AI_*` |
| Deep Direction model | `utils/ml_direction/*`, `tools/train_direction_model.py` | Sequence model inference and training | feature sequences from OHLCV | BUY/SELL confidence, saved `.pt` bundle | Deep learning (PyTorch) | Yes (periodic recommended) | `ENABLE_DEEP_DIRECTION_INFERENCE`, `DEEP_DIRECTION_*` |
| Scanner AI Orchestration | `core/signal_engine.py` | Selects and ranks candidates per symbol | watchlist + multi-timeframe scans | best signal rows | Hybrid orchestrator | No | `SIGNAL_MIN_CONFIDENCE` |
| Admin/API AI telemetry | `api/app.py`, `bot/admin.py` | Monitoring and runtime visibility | daily log files + DB | runtime status payloads | Observability | No | `ENGINE_LOG_ROOT`, AI toggles |

---

## Training Requirements Analysis

### Needs auto/scheduled/background training

- **Deep Direction model**: yes (fresh market regimes demand periodic refresh).
- **RF models**: currently retrain on demand/version mismatch; still benefits from proactive warming.
- **Rule-based systems** (Momentum/MeanRev/Market Structure): no model retraining.

### Continuous improvement candidates (self-learning)

- Closed trade outcomes (`trades` table) can be harvested into reinforcement-like dataset.
- Symbol-level fine-tune cadence can prioritize symbols with enough recent outcomes.

---

## Data / Dependency Map

- **Capital.com data**: via `utils/market_scanner.py` and scanner identity (`MARKET_DATA_*` fallback to `CAPITAL_*`).
- **FMP data**: used by earnings/news-related filters and scanner support.
- **Feature engineering**: `utils/ai_model.py::build_features`.
- **Labeling logic**: `utils/ml_direction/labels.py`.
- **Deep architecture**: `utils/ml_direction/models.py`.
- **Inference compatibility**:
  - RF: `MODEL_VERSION`-checked pickle.
  - Deep: `DIRECTION_MODEL_VERSION` + legacy v2 compatibility path in `utils/ml_direction/infer.py`.
