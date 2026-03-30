# Autonomous Training Framework

This project now includes an autonomous AI training framework implemented in:

- `utils/autonomous_training.py`
- Integrated startup in `main.py`
- Stable-model loading support in `utils/ml_direction/infer.py`

## What It Adds

## 1) Auto-training module

- Trains deep direction models using fresh scanner data through existing pipeline:
  - feature engineering -> labels -> training -> validation
  - powered by `train_deep_direction_model()` bridge
- Persists a versioned artifact on each successful run:
  - `models/stable/<SYMBOL>_dir_<TF>_<KIND>_vN.pt`
- Promotes successful artifacts into stable slots under `models/stable/`.
- Validates promoted model by loading it for inference before marking it stable.
- Updates registry metadata: `models/deep_model_registry.json`.

## 2) Scheduled training (cron-like)

- UTC daily schedule: `AUTOTRAIN_DAILY_UTC_HOUR`, `AUTOTRAIN_DAILY_UTC_MINUTE`
- UTC weekly schedule: `AUTOTRAIN_WEEKLY_DAY`, `AUTOTRAIN_WEEKLY_UTC_HOUR`, `AUTOTRAIN_WEEKLY_UTC_MINUTE`
- Scheduler loop polling: `AUTOTRAIN_SCHEDULER_POLL_SEC`
- Trigger mode labels: `scheduled_daily`, `scheduled_weekly`

## 3) Background training

- Runs on a dedicated single-worker `ThreadPoolExecutor`.
- Does not block trading loop or dispatch pipeline.
- Queue protection prevents concurrent overlapping training jobs.

## 4) Self-learning loop

- Harvests closed trades from DB into:
  - `logs/ai_training/reinforcement_dataset.jsonl`
- Tracks cursor (`last_trade_id`) and sample counters in:
  - `logs/ai_training/autonomous_training_status.json`
- Periodically fine-tunes top symbols when enough new outcomes exist.

## 5) Safety, fallback, and monitoring

- Atomic writes for status/registry (`.tmp` + `os.replace`).
- Stable artifact promotion only after load validation.
- Inference now prefers:
  1) Registry path
  2) Stable model path
  3) Canonical versioned path
  4) Legacy compat bundle
- If stable load fails, inference falls back to next candidate path.
- Admin notifications on completed jobs (optional).

---

## Runtime Integration

- `main.py::_start_background_threads()` starts the autonomous scheduler.
- `main.py::pretrain_models()` now warms RF models for `1d`, `4h`, `15m`.
- Watchlist updates are passed into training manager for symbol selection.
- API runtime endpoint now exposes autonomous training status:
  - `GET /admin/ai-runtime` -> `autonomous_training`

---

## Environment Variables

See `.env.example` for all added keys under **Autonomous AI Training**.

Minimal enablement:

```env
ENABLE_AUTONOMOUS_TRAINING=true
ENABLE_DEEP_DIRECTION_INFERENCE=true
```

Recommended with schedule:

```env
ENABLE_AUTONOMOUS_TRAINING=true
ENABLE_AUTONOMOUS_SCHEDULED_TRAINING=true
AUTOTRAIN_DAILY_UTC_HOUR=2
AUTOTRAIN_DAILY_UTC_MINUTE=15
AUTOTRAIN_WEEKLY_DAY=sun
AUTOTRAIN_WEEKLY_UTC_HOUR=3
AUTOTRAIN_WEEKLY_UTC_MINUTE=30
```

---

## Output Artifacts

- `logs/ai_training/training_runs.jsonl` (history)
- `logs/ai_training/autonomous_training_status.json` (latest status)
- `logs/ai_training/reinforcement_dataset.jsonl` (self-learning dataset)
- `models/stable/*_stable.pt` + scaler
- `models/stable/*_vN.pt` + scaler (version history)
- `models/deep_model_registry.json` (stable model registry)
