from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable, Optional

from config import (
    WATCHLIST,
    DEEP_DIRECTION_INFERENCE_KIND,
    DEEP_DIRECTION_LABEL_HORIZON,
    DEEP_DIRECTION_LABEL_THRESHOLD,
    DEEP_DIRECTION_SEQ_LEN,
    DEEP_DIRECTION_TIMEFRAME,
    ENABLE_AUTONOMOUS_TRAINING,
    ENABLE_AUTONOMOUS_SCHEDULED_TRAINING,
    ENABLE_AUTONOMOUS_SELF_LEARNING,
    AUTOTRAIN_LOG_ROOT,
    AUTOTRAIN_MAX_SYMBOLS_PER_RUN,
    AUTOTRAIN_EPOCHS,
    AUTOTRAIN_SELF_LEARNING_EPOCHS,
    AUTOTRAIN_SCHEDULER_POLL_SEC,
    AUTOTRAIN_DAILY_UTC_HOUR,
    AUTOTRAIN_DAILY_UTC_MINUTE,
    AUTOTRAIN_WEEKLY_DAY,
    AUTOTRAIN_WEEKLY_UTC_HOUR,
    AUTOTRAIN_WEEKLY_UTC_MINUTE,
    AUTOTRAIN_NOTIFY_ADMIN,
    AUTOTRAIN_AUTO_MAX_MODEL_AGE_HOURS,
    AUTOTRAIN_SELF_LEARNING_INTERVAL_SEC,
    AUTOTRAIN_SELF_LEARNING_MIN_SAMPLES,
    AUTOTRAIN_SELF_LEARNING_SYMBOLS_PER_RUN,
)
from database.db_manager import DB_PATH
from utils.ai_model import train_deep_direction_model
from utils.ml_direction.infer import invalidate_direction_bundle_cache, load_direction_bundle
from utils.market_scanner import RateLimitError
from utils.market_hours import is_market_open

import time
import html
import logging

log = logging.getLogger(__name__)

MODELS_DIR = "models"
STABLE_DIR = os.path.join(MODELS_DIR, "stable")
REGISTRY_PATH = os.path.join(MODELS_DIR, "deep_model_registry.json")
STATUS_PATH = os.path.join(AUTOTRAIN_LOG_ROOT, "autonomous_training_status.json")
RUNS_PATH = os.path.join(AUTOTRAIN_LOG_ROOT, "training_runs.jsonl")
SELF_LEARNING_DATASET_PATH = os.path.join(AUTOTRAIN_LOG_ROOT, "reinforcement_dataset.jsonl")

# Persistent cooldown storage - survives server restarts
# Use absolute path based on project root to ensure file is created in correct location
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)
COOLDOWN_FILE_PATH = os.path.join(DATA_DIR, "training_cooldown.json")

# Ensure logs/ai_training directory exists (prevents FileNotFoundError crashes)
os.makedirs(AUTOTRAIN_LOG_ROOT, exist_ok=True)

# DEBUG: Print the exact path where cooldown file will be stored
print(f"DEBUG: Cooldown file path = {COOLDOWN_FILE_PATH}")

_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _read_cooldown_file() -> dict:
    """Read persistent cooldown data from file."""
    try:
        if not os.path.exists(COOLDOWN_FILE_PATH):
            return {}
        with open(COOLDOWN_FILE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_cooldown_file(data: dict):
    """Write cooldown data to persistent file."""
    try:
        tmp = f"{COOLDOWN_FILE_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, indent=2)
        os.replace(tmp, COOLDOWN_FILE_PATH)
    except Exception:
        pass


def _escape_telegram_markdown(text: str) -> str:
    """
    Escape special characters for Telegram MarkdownV2.
    Characters that must be escaped: _ * [ ] ( ) ~ ` > # + - = | { } . !
    """
    if not text:
        return text
    # Characters that need escaping in MarkdownV2
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text


# Cooldown duration after training failure (6 hours = 21600 seconds)
TRAINING_FAILURE_COOLDOWN_SEC = 21600


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(ts: Optional[datetime] = None) -> str:
    t = ts or _utc_now()
    return t.replace(microsecond=0).isoformat()


def _parse_iso_utc(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _load_json(path: str, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json_atomic(path: str, payload):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)
    os.replace(tmp, path)


def _append_jsonl(path: str, payload):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _sanitize_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    return s.replace("/", "-").replace("=", "").replace("^", "")


def _stable_model_path(symbol: str, timeframe: str, kind: str) -> str:
    safe = _sanitize_symbol(symbol)
    tf = str(timeframe).strip().lower()
    k = str(kind).strip().lower()
    return os.path.join(STABLE_DIR, f"{safe}_dir_{tf}_{k}_stable.pt")


def _versioned_model_path(symbol: str, timeframe: str, kind: str, version: int) -> str:
    safe = _sanitize_symbol(symbol)
    tf = str(timeframe).strip().lower()
    k = str(kind).strip().lower()
    return os.path.join(STABLE_DIR, f"{safe}_dir_{tf}_{k}_v{int(version)}.pt")


def _read_registry() -> dict:
    reg = _load_json(REGISTRY_PATH, {"models": {}})
    if not isinstance(reg, dict):
        reg = {"models": {}}
    reg.setdefault("models", {})
    return reg


def _read_status() -> dict:
    status = _load_json(STATUS_PATH, {})
    if not isinstance(status, dict):
        status = {}
    return status


def load_autonomous_training_status() -> dict:
    """
    Read-only helper for API/monitoring.
    """
    st = _read_status()
    if not st:
        return {
            "enabled": bool(ENABLE_AUTONOMOUS_TRAINING),
            "status": "idle",
            "updated_at": "",
            "last_run": None,
            "registry_models": 0,
        }
    reg = _read_registry()
    return {
        "enabled": bool(ENABLE_AUTONOMOUS_TRAINING),
        "status": str(st.get("status", "idle")),
        "updated_at": str(st.get("updated_at", "")),
        "last_run": st.get("last_run"),
        "last_error": st.get("last_error"),
        "registry_models": len((reg.get("models") or {})),
        "self_learning": st.get("self_learning", {}),
    }


class AutonomousTrainingManager:
    """
    Background autonomous training manager.
    - scheduled training (daily/weekly)
    - background execution (single-worker queue)
    - self-learning data collection/fine-tune triggers
    """

    def __init__(self, *, admin_chat_id: str = "", watchlist_provider: Optional[Callable[[], list[str]]] = None):
        self.admin_chat_id = str(admin_chat_id or "").strip()
        self.watchlist_provider = watchlist_provider
        self.stop_event = threading.Event()
        self.scheduler_thread: Optional[threading.Thread] = None
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="autonomous-trainer")
        self.state_lock = threading.Lock()
        self.active_future: Optional[Future] = None
        self.cached_watchlist: list[str] = []
        self.weekly_day = _WEEKDAY_MAP.get(str(AUTOTRAIN_WEEKLY_DAY).strip().lower(), 6)

        os.makedirs(AUTOTRAIN_LOG_ROOT, exist_ok=True)
        os.makedirs(STABLE_DIR, exist_ok=True)
        os.makedirs(MODELS_DIR, exist_ok=True)

    def start(self):
        if not ENABLE_AUTONOMOUS_TRAINING:
            return
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            return
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, name="autonomous-training-scheduler", daemon=True)
        self.scheduler_thread.start()
        self._update_status({"status": "idle", "updated_at": _utc_iso()})

    def stop(self):
        self.stop_event.set()
        try:
            self.executor.shutdown(wait=False, cancel_futures=False)
        except Exception:
            pass

    def update_watchlist(self, watchlist: list[str]):
        cleaned: list[str] = []
        seen = set()
        for sym in watchlist or []:
            s = _sanitize_symbol(sym)
            if not s or s in seen:
                continue
            seen.add(s)
            cleaned.append(s)
        self.cached_watchlist = cleaned

    def request_training(self, *, reason: str, mode: str = "manual", symbols: Optional[list[str]] = None, force: bool = False) -> bool:
        if not ENABLE_AUTONOMOUS_TRAINING:
            return False
        with self.state_lock:
            if self.active_future and not self.active_future.done() and not force:
                return False
            payload_symbols = self._resolve_symbols(symbols)
            self.active_future = self.executor.submit(self._run_training_job, reason, mode, payload_symbols)
        return True

    def _is_in_failure_cooldown(self) -> bool:
        """
        Check if we're within the 6-hour cooldown period after a training failure.
        PERSISTENT: Reads from file to survive server restarts.
        """
        # Try persistent file first (survives restarts)
        cd_data = _read_cooldown_file()
        last_fail_ts = cd_data.get("last_failure_at")
        
        # Fallback to status file for backwards compatibility
        if not last_fail_ts:
            status = _read_status()
            last_fail_ts = status.get("last_failure_at")
        
        if not last_fail_ts:
            return False
        try:
            last_fail_dt = _parse_iso_utc(str(last_fail_ts))
            if last_fail_dt is None:
                return False
            elapsed_sec = (_utc_now() - last_fail_dt).total_seconds()
            return elapsed_sec < TRAINING_FAILURE_COOLDOWN_SEC
        except Exception:
            return False

    def _scheduler_loop(self):
        while not self.stop_event.is_set():
            try:
                # ── MARKET-CLOSED CHECK ────────────────────────────────────────────────
                # If market is closed, sleep for 1 hour and skip all training logic
                if not is_market_open():
                    log.info("Market is closed. Training scheduler is going to sleep.")
                    self._update_status({
                        "status": "market_closed",
                        "updated_at": _utc_iso(),
                        "last_error": "Market closed - sleeping for 1 hour",
                    })
                    self.stop_event.wait(3600)  # Sleep 1 hour
                    continue

                # ── HARD COOLDOWN CHECK ────────────────────────────────────────────────
                # Read directly from persistent file at the START of every loop iteration.
                # This ensures cooldown is enforced even if _is_in_failure_cooldown() has bugs.
                print(f"DEBUG: Checking cooldown file at {COOLDOWN_FILE_PATH}")
                cd_data = _read_cooldown_file()
                last_fail_ts = cd_data.get("last_failure_at")
                if last_fail_ts:
                    try:
                        last_fail_dt = _parse_iso_utc(str(last_fail_ts))
                        if last_fail_dt:
                            elapsed_sec = (_utc_now() - last_fail_dt).total_seconds()
                            if elapsed_sec < TRAINING_FAILURE_COOLDOWN_SEC:
                                # FORCE COOLDOWN: Sleep and continue without any API calls
                                self._update_status({
                                    "status": "cooldown",
                                    "updated_at": _utc_iso(),
                                    "last_error": f"HARD COOLDOWN: {int((TRAINING_FAILURE_COOLDOWN_SEC - elapsed_sec)/60)} min remaining",
                                })
                                self.stop_event.wait(max(60, int(AUTOTRAIN_SCHEDULER_POLL_SEC)))
                                continue
                    except Exception:
                        pass  # If parsing fails, proceed with normal checks
                
                # Check 6-hour cooldown after training failures to prevent "Loop of Death"
                if self._is_in_failure_cooldown():
                    # Read from persistent cooldown file (survives restarts)
                    cd_data = _read_cooldown_file()
                    last_fail = cd_data.get("last_failure_at", "unknown")
                    self._update_status({
                        "status": "cooldown",
                        "updated_at": _utc_iso(),
                        "last_error": f"Training cooldown active (6h) since {last_fail}",
                    })
                    self.stop_event.wait(max(60, int(AUTOTRAIN_SCHEDULER_POLL_SEC)))
                    continue

                if ENABLE_AUTONOMOUS_SELF_LEARNING:
                    self._collect_self_learning_samples()
                    self._maybe_schedule_self_learning_finetune()
                self._maybe_schedule_auto_training()
                if ENABLE_AUTONOMOUS_SCHEDULED_TRAINING:
                    self._maybe_schedule_daily_weekly()
            except Exception as exc:
                self._update_status({"last_error": f"scheduler: {exc}", "updated_at": _utc_iso()})
            self.stop_event.wait(max(10, int(AUTOTRAIN_SCHEDULER_POLL_SEC)))

    def _resolve_symbols(self, symbols: Optional[list[str]]) -> list[str]:
        src = symbols or []
        if not src:
            if callable(self.watchlist_provider):
                try:
                    src = list(self.watchlist_provider() or [])
                except Exception:
                    src = []
        if not src:
            src = self.cached_watchlist or list(WATCHLIST)
        out: list[str] = []
        seen = set()
        for sym in src:
            s = _sanitize_symbol(sym)
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
            if len(out) >= int(AUTOTRAIN_MAX_SYMBOLS_PER_RUN):
                break
        return out

    def _run_training_job(self, reason: str, mode: str, symbols: list[str]):
        started_at = _utc_now()
        started_iso = _utc_iso(started_at)
        tf = str(DEEP_DIRECTION_TIMEFRAME).strip().lower()
        kind = str(DEEP_DIRECTION_INFERENCE_KIND).strip().lower()
        epochs = int(AUTOTRAIN_SELF_LEARNING_EPOCHS if mode == "self_learning" else AUTOTRAIN_EPOCHS)
        self._update_status(
            {
                "status": "running",
                "updated_at": started_iso,
                "running": {
                    "started_at": started_iso,
                    "reason": str(reason),
                    "mode": str(mode),
                    "timeframe": tf,
                    "kind": kind,
                    "epochs": int(epochs),
                    "symbols": symbols,
                },
            }
        )

        ok = 0
        failed = 0
        rows: list[dict] = []
        rate_limited = False
        
        for idx, symbol in enumerate(symbols):
            row = {
                "symbol": symbol,
                "ok": False,
                "error": "",
                "model_path": "",
                "stable_path": "",
                "best_val_acc": None,
                "best_val_loss": None,
            }
            
            # ── Smart Pacing: 3 second delay between symbols to respect Capital.com rate limits ──
            if idx > 0:
                time.sleep(3.0)
            
            # ── Exponential Backoff for retries ──
            max_retries = 2
            retry_delay = 2.0  # Start with 2 seconds
            
            for attempt in range(max_retries):
                try:
                    res = train_deep_direction_model(
                        symbol=symbol,
                        timeframe=tf,
                        model_kind=kind,
                        seq_len=int(DEEP_DIRECTION_SEQ_LEN),
                        label_horizon=int(DEEP_DIRECTION_LABEL_HORIZON),
                        label_threshold=float(DEEP_DIRECTION_LABEL_THRESHOLD),
                        epochs=int(epochs),
                    )
                    model_path = str(res.get("model_path") or "").strip()
                    if not model_path:
                        raise RuntimeError("training returned empty model_path")
                    stable_model_path = self._promote_model(symbol=symbol, model_path=model_path, timeframe=tf, kind=kind)
                    row["ok"] = True
                    row["model_path"] = model_path
                    row["stable_path"] = stable_model_path
                    row["best_val_acc"] = _safe_float(res.get("best_val_acc"), 0.0)
                    row["best_val_loss"] = _safe_float(res.get("best_val_loss"), 0.0)
                    ok += 1
                    break  # Success - exit retry loop
                    
                except RateLimitError as rle:
                    # ── 429 DETECTED: Abort training immediately and trigger cooldown ──
                    rate_limited = True
                    row["error"] = f"RATE_LIMITED: {str(rle)}"
                    failed += 1
                    log_msg = f"[TRAINING ABORTED] HTTP 429 Rate Limit hit for {symbol}. Aborting training session."
                    print(log_msg, flush=True)
                    self._update_status({
                        "status": "aborted",
                        "updated_at": _utc_iso(),
                        "last_error": log_msg,
                    })
                    # Break out of retry loop AND symbol loop
                    break
                    
                except Exception as exc:
                    if attempt < max_retries - 1:
                        # ── Exponential Backoff: 2s, 4s, 8s ──
                        print(
                            f"[TRAINING RETRY] {symbol} attempt {attempt + 1}/{max_retries} failed: {exc}. "
                            f"Retrying in {retry_delay:.0f}s...",
                            flush=True,
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2.0  # Double the delay for next attempt
                        continue
                    else:
                        # Final attempt failed
                        row["error"] = str(exc)
                        failed += 1
                        break
            
            rows.append(row)
            
            # ── If we hit rate limit, abort the entire training session ──
            if rate_limited:
                break

        ended_at = _utc_now()
        run = {
            "started_at": started_iso,
            "ended_at": _utc_iso(ended_at),
            "duration_sec": round((ended_at - started_at).total_seconds(), 1),
            "reason": str(reason),
            "mode": str(mode),
            "timeframe": tf,
            "kind": kind,
            "epochs": int(epochs),
            "symbols_total": len(symbols),
            "symbols_ok": ok,
            "symbols_failed": failed,
            "results": rows,
        }
        _append_jsonl(RUNS_PATH, run)

        # If training failed OR rate limited, set the failure timestamp to trigger 6-hour cooldown
        # PERSISTENT: Write to both status file and dedicated cooldown file
        if failed > 0 or rate_limited:
            fail_ts = _utc_iso()
            self._update_status({
                "last_failure_at": fail_ts,
                "consecutive_failures": _safe_int(_read_status().get("consecutive_failures"), 0) + 1,
            })
            # Write to persistent cooldown file (survives restarts)
            _write_cooldown_file({
                "last_failure_at": fail_ts,
                "consecutive_failures": _safe_int(_read_cooldown_file().get("consecutive_failures"), 0) + 1,
            })
        else:
            # Reset consecutive failures on success
            self._update_status({
                "consecutive_failures": 0,
            })
            _write_cooldown_file({
                "consecutive_failures": 0,
            })

        # ── Set final status: idle (success), failed (some errors), or aborted (429) ──
        final_status = "aborted" if rate_limited else ("failed" if failed > 0 else "idle")
        final_error = ""
        if rate_limited:
            final_error = f"ABORTED due to HTTP 429 Rate Limit at symbol {symbol}"
        elif failed > 0:
            final_error = f"{failed} symbol(s) failed"
        
        self._update_status(
            {
                "status": final_status,
                "updated_at": _utc_iso(),
                "last_run": run,
                "running": None,
                "last_error": final_error,
            }
        )
        self._notify_admin(run)

    def _promote_model(self, *, symbol: str, model_path: str, timeframe: str, kind: str) -> str:
        scaler_src = model_path + ".scaler.pkl"
        if not os.path.exists(model_path) or not os.path.exists(scaler_src):
            raise RuntimeError("trained model artifacts are missing")

        key = f"{symbol}|{timeframe}|{kind}"
        reg = _read_registry()
        prev_entry = (reg.get("models") or {}).get(key) or {}
        next_version = _safe_int(prev_entry.get("version"), 0) + 1

        versioned_model = _versioned_model_path(symbol, timeframe, kind, next_version)
        versioned_scaler = versioned_model + ".scaler.pkl"
        os.makedirs(os.path.dirname(versioned_model), exist_ok=True)
        shutil.copy2(model_path, versioned_model)
        shutil.copy2(scaler_src, versioned_scaler)

        stable_model = _stable_model_path(symbol, timeframe, kind)
        stable_scaler = stable_model + ".scaler.pkl"
        tmp_model = stable_model + ".tmp"
        tmp_scaler = stable_scaler + ".tmp"

        shutil.copy2(versioned_model, tmp_model)
        shutil.copy2(versioned_scaler, tmp_scaler)
        os.replace(tmp_model, stable_model)
        os.replace(tmp_scaler, stable_scaler)

        # Validate promoted artifacts before registry update.
        invalidate_direction_bundle_cache(symbol, timeframe, kind)
        bundle = load_direction_bundle(symbol, timeframe, kind)
        if not bundle:
            raise RuntimeError("promoted model validation failed; fallback preserved")

        reg["updated_at"] = _utc_iso()
        reg["models"][key] = {
            "model_path": stable_model,
            "scaler_path": stable_scaler,
            "versioned_model_path": versioned_model,
            "versioned_scaler_path": versioned_scaler,
            "version": int(next_version),
            "symbol": symbol,
            "timeframe": timeframe,
            "kind": kind,
            "promoted_at": _utc_iso(),
        }
        _save_json_atomic(REGISTRY_PATH, reg)
        return stable_model

    def _collect_self_learning_samples(self):
        status = _read_status()
        sl = status.get("self_learning") or {}
        last_trade_id = _safe_int(sl.get("last_trade_id"), 0)
        rows = []
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT trade_id, symbol, direction, pnl, status, opened_at, closed_at, close_reason "
                "FROM trades WHERE trade_id > ? AND status='CLOSED' ORDER BY trade_id ASC LIMIT 2000",
                (last_trade_id,),
            )
            rows = c.fetchall() or []

        if not rows:
            return

        last_seen = last_trade_id
        added = 0
        for trade_id, symbol, direction, pnl, status_txt, opened_at, closed_at, close_reason in rows:
            last_seen = max(last_seen, _safe_int(trade_id, last_seen))
            pnl_f = _safe_float(pnl, 0.0)
            sample = {
                "trade_id": _safe_int(trade_id, 0),
                "symbol": _sanitize_symbol(symbol),
                "direction": str(direction or "").upper(),
                "pnl": pnl_f,
                "reward": 1 if pnl_f > 0 else (-1 if pnl_f < 0 else 0),
                "status": str(status_txt or ""),
                "opened_at": str(opened_at or ""),
                "closed_at": str(closed_at or ""),
                "close_reason": str(close_reason or ""),
                "collected_at": _utc_iso(),
            }
            _append_jsonl(SELF_LEARNING_DATASET_PATH, sample)
            added += 1

        sl["last_trade_id"] = int(last_seen)
        sl["dataset_samples_total"] = _safe_int(sl.get("dataset_samples_total"), 0) + int(added)
        sl["new_samples_since_finetune"] = _safe_int(sl.get("new_samples_since_finetune"), 0) + int(added)
        sl["last_collection_at"] = _utc_iso()
        status["self_learning"] = sl
        status["updated_at"] = _utc_iso()
        _save_json_atomic(STATUS_PATH, status)

    def _maybe_schedule_self_learning_finetune(self):
        status = _read_status()
        sl = status.get("self_learning") or {}
        last_ts = str(sl.get("last_finetune_at") or "")
        due = True
        if last_ts:
            try:
                last_dt = _parse_iso_utc(last_ts)
                if last_dt is None:
                    raise ValueError("invalid last_finetune_at")
                due = (_utc_now() - last_dt).total_seconds() >= int(AUTOTRAIN_SELF_LEARNING_INTERVAL_SEC)
            except Exception:
                due = True
        if not due:
            return
        new_samples = _safe_int(sl.get("new_samples_since_finetune"), 0)
        if new_samples < int(AUTOTRAIN_SELF_LEARNING_MIN_SAMPLES):
            return

        symbols = self._top_symbols_from_reinforcement_dataset(limit=int(AUTOTRAIN_SELF_LEARNING_SYMBOLS_PER_RUN))
        if not symbols:
            return
        queued = self.request_training(reason="self_learning_finetune", mode="self_learning", symbols=symbols)
        if queued:
            sl["last_finetune_at"] = _utc_iso()
            sl["new_samples_since_finetune"] = 0
            sl["last_finetune_symbols"] = symbols
            status["self_learning"] = sl
            status["updated_at"] = _utc_iso()
            _save_json_atomic(STATUS_PATH, status)

    def _top_symbols_from_reinforcement_dataset(self, limit: int) -> list[str]:
        if not os.path.exists(SELF_LEARNING_DATASET_PATH):
            return []
        scores: dict[str, int] = {}
        try:
            with open(SELF_LEARNING_DATASET_PATH, "r", encoding="utf-8") as f:
                lines = f.readlines()[-3000:]
            for line in lines:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                sym = _sanitize_symbol(row.get("symbol"))
                if not sym:
                    continue
                scores[sym] = scores.get(sym, 0) + 1
        except Exception:
            return []
        ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        return [k for k, _ in ordered[: max(1, int(limit))]]

    def _maybe_schedule_auto_training(self):
        reg = _read_registry()
        models = reg.get("models") or {}
        if not models:
            self.request_training(reason="auto_bootstrap_no_registry", mode="auto")
            return

        oldest_hours = 0.0
        now = _utc_now()
        for entry in models.values():
            ts = str((entry or {}).get("promoted_at") or "")
            if not ts:
                oldest_hours = float(AUTOTRAIN_AUTO_MAX_MODEL_AGE_HOURS) + 1.0
                break
            try:
                dt = _parse_iso_utc(ts)
                if dt is None:
                    raise ValueError("invalid promoted_at")
                age_h = (now - dt).total_seconds() / 3600.0
                oldest_hours = max(oldest_hours, age_h)
            except Exception:
                oldest_hours = float(AUTOTRAIN_AUTO_MAX_MODEL_AGE_HOURS) + 1.0
                break
        if oldest_hours >= float(AUTOTRAIN_AUTO_MAX_MODEL_AGE_HOURS):
            self.request_training(reason=f"auto_refresh_age_{oldest_hours:.1f}h", mode="auto")

    def _maybe_schedule_daily_weekly(self):
        now = _utc_now()
        status = _read_status()
        scheduler = status.get("scheduler") or {}

        daily_key = now.strftime("%Y-%m-%d")
        if now.hour == int(AUTOTRAIN_DAILY_UTC_HOUR) and now.minute == int(AUTOTRAIN_DAILY_UTC_MINUTE):
            if scheduler.get("last_daily_key") != daily_key:
                if self.request_training(reason="scheduled_daily", mode="scheduled"):
                    scheduler["last_daily_key"] = daily_key

        weekly_key = f"{now.strftime('%Y-%m-%d')}-w{now.weekday()}"
        if (
            now.weekday() == int(self.weekly_day)
            and now.hour == int(AUTOTRAIN_WEEKLY_UTC_HOUR)
            and now.minute == int(AUTOTRAIN_WEEKLY_UTC_MINUTE)
        ):
            if scheduler.get("last_weekly_key") != weekly_key:
                if self.request_training(reason="scheduled_weekly", mode="scheduled"):
                    scheduler["last_weekly_key"] = weekly_key

        status["scheduler"] = scheduler
        status["updated_at"] = _utc_iso()
        _save_json_atomic(STATUS_PATH, status)

    def _update_status(self, patch: dict):
        """Update status file with error handling to prevent thread crashes."""
        try:
            status = _read_status()
            status.update(patch or {})
            _save_json_atomic(STATUS_PATH, status)
        except Exception as exc:
            # Log error but never crash the scheduler thread
            log.error(f"Failed to update status file: {exc}")

    def _notify_admin(self, run: dict):
        if not (AUTOTRAIN_NOTIFY_ADMIN and self.admin_chat_id):
            return
        try:
            from bot.notifier import send_telegram_message
            
            # HTML escape all dynamic values to prevent Telegram parsing errors
            # Using HTML parse_mode is much safer than MarkdownV2 for variable content
            mode = html.escape(str(run.get('mode', 'unknown')))
            reason = html.escape(str(run.get('reason', 'unknown')))
            timeframe = html.escape(str(run.get('timeframe', 'unknown')))
            kind = html.escape(str(run.get('kind', 'unknown')))
            
            # Build HTML message - no special characters to break parsing
            msg = (
                "🤖 Autonomous training finished<br>"
                f"mode={mode} reason={reason}<br>"
                f"ok={run.get('symbols_ok')}/{run.get('symbols_total')} "
                f"failed={run.get('symbols_failed')}<br>"
                f"timeframe={timeframe} kind={kind} epochs={run.get('epochs')}"
            )
            # Use parse_mode='HTML' for safer formatting with dynamic content
            send_telegram_message(self.admin_chat_id, msg, parse_mode='HTML')
        except Exception:
            # Never fail main engine flow on notifier errors.
            self._update_status({"last_error": "admin notify failed", "updated_at": _utc_iso()})


def ai_system_inventory() -> list[dict]:
    """
    Discovery metadata used by docs/reporting tools.
    """
    return [
        {
            "name": "RF Multi-timeframe Model",
            "file": "utils/ai_model.py",
            "entrypoints": ["analyze_multi_timeframe", "load_or_train_model", "train_model"],
            "category": "ml",
            "retrain": "on-demand/version-based",
            "env": ["AI_MIN_PROB_RF", "SIGNAL_PROFILE", "FAST_*", "GOLDEN_*"],
        },
        {
            "name": "AI Gatekeeper",
            "file": "utils/ai_model.py",
            "entrypoints": ["validate_signal", "detect_regime"],
            "category": "ml+rules",
            "retrain": "uses RF/deep artifacts",
            "env": ["AI_MIN_PROB_*", "ENABLE_MS_SCORE_AI_INTEGRATION", "MS_SCORE_AI_*"],
        },
        {
            "name": "Mean Reversion Scorer",
            "file": "core/strategy_meanrev.py",
            "entrypoints": ["analyze"],
            "category": "rule-based",
            "retrain": "no",
            "env": ["SIGNAL_MR_MIN_SCORE", "MR_*"],
        },
        {
            "name": "Momentum Scorer",
            "file": "core/strategy_momentum.py",
            "entrypoints": ["analyze"],
            "category": "rule-based",
            "retrain": "no",
            "env": ["SIGNAL_MOM_MIN_SCORE", "MOM_*", "NEWS_*"],
        },
        {
            "name": "Market Structure AI Context",
            "file": "core/market_structure.py",
            "entrypoints": ["apply_market_structure_policy"],
            "category": "rule-derived context",
            "retrain": "no",
            "env": ["ENABLE_MARKET_STRUCTURE_FILTERS", "MARKET_STRUCTURE_*", "LIQUIDITY_*"],
        },
        {
            "name": "Deep Direction Model (LSTM/GRU/Transformer)",
            "file": "utils/ml_direction/*",
            "entrypoints": ["train_direction_for_symbol", "load_direction_bundle", "predict_direction_from_features"],
            "category": "deep-learning",
            "retrain": "periodic recommended",
            "env": ["ENABLE_DEEP_DIRECTION_INFERENCE", "DEEP_DIRECTION_*"],
        },
    ]
