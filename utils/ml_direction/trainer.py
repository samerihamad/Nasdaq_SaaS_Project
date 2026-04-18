from __future__ import annotations

import os
from dataclasses import dataclass
import pickle

import numpy as np
import torch
from torch.utils.data import DataLoader, TensorDataset

from utils.market_scanner import scan_market, RateLimitError
from .dataset import build_sequence_dataset
from .models import create_direction_model


MODEL_DIR = "models"
DIRECTION_MODEL_VERSION = 1
os.makedirs(MODEL_DIR, exist_ok=True)


@dataclass
class DirectionTrainResult:
    model_path: str
    model_kind: str
    best_val_loss: float
    best_val_acc: float
    epochs: int
    n_train: int
    n_val: int
    num_features: int
    num_classes: int


def _direction_model_path(symbol: str, timeframe: str, kind: str) -> str:
    safe = str(symbol).replace("/", "-").replace("=", "").replace("^", "")
    tf = str(timeframe).strip().lower()
    k = str(kind).strip().lower()
    return os.path.join(MODEL_DIR, f"{safe}_dir_{tf}_{k}_v{DIRECTION_MODEL_VERSION}.pt")


def _accuracy(logits: torch.Tensor, y: torch.Tensor) -> float:
    pred = torch.argmax(logits, dim=1)
    return float((pred == y).float().mean().item())


def train_direction_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    *,
    model_kind: str = "lstm",
    epochs: int = 25,
    batch_size: int = 128,
    lr: float = 1e-3,
    weight_decay: float = 1e-5,
    hidden_dim: int = 128,
    num_layers: int = 2,
    dropout: float = 0.2,
    device: str | None = None,
) -> tuple[torch.nn.Module, dict]:
    """
    Train LSTM/GRU/Transformer direction model on sequence tensors.
    """
    dev = torch.device(device or ("cuda" if torch.cuda.is_available() else "cpu"))
    xtr = torch.tensor(X_train, dtype=torch.float32)
    ytr = torch.tensor(y_train, dtype=torch.long)
    xva = torch.tensor(X_val, dtype=torch.float32)
    yva = torch.tensor(y_val, dtype=torch.long)

    model = create_direction_model(
        model_kind,
        input_dim=int(X_train.shape[2]),
        num_classes=int(len(np.unique(y_train))),
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        dropout=dropout,
    ).to(dev)

    train_loader = DataLoader(TensorDataset(xtr, ytr), batch_size=int(batch_size), shuffle=True)
    val_loader = DataLoader(TensorDataset(xva, yva), batch_size=int(batch_size), shuffle=False)

    opt = torch.optim.AdamW(model.parameters(), lr=float(lr), weight_decay=float(weight_decay))
    loss_fn = torch.nn.CrossEntropyLoss()

    best = {
        "val_loss": float("inf"),
        "val_acc": 0.0,
        "state": None,
        "epoch": 0,
    }
    patience = 6
    stale = 0

    for ep in range(1, int(epochs) + 1):
        model.train()
        for xb, yb in train_loader:
            xb = xb.to(dev)
            yb = yb.to(dev)
            opt.zero_grad(set_to_none=True)
            logits = model(xb)
            loss = loss_fn(logits, yb)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            opt.step()

        model.eval()
        val_losses: list[float] = []
        val_accs: list[float] = []
        with torch.no_grad():
            for xb, yb in val_loader:
                xb = xb.to(dev)
                yb = yb.to(dev)
                logits = model(xb)
                val_losses.append(float(loss_fn(logits, yb).item()))
                val_accs.append(_accuracy(logits, yb))
        val_loss = float(np.mean(val_losses)) if val_losses else float("inf")
        val_acc = float(np.mean(val_accs)) if val_accs else 0.0

        if val_loss < best["val_loss"]:
            best["val_loss"] = val_loss
            best["val_acc"] = val_acc
            best["state"] = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            best["epoch"] = ep
            stale = 0
        else:
            stale += 1
            if stale >= patience:
                break

    if best["state"] is not None:
        model.load_state_dict(best["state"])
    return model, {"best_val_loss": best["val_loss"], "best_val_acc": best["val_acc"], "best_epoch": best["epoch"]}


def train_direction_for_symbol(
    symbol: str,
    *,
    timeframe: str = "15m",
    model_kind: str = "lstm",
    seq_len: int = 64,
    label_horizon: int = 8,
    label_threshold: float = 0.012,
    epochs: int = 25,
) -> DirectionTrainResult:
    """
    End-to-end training utility:
    1) fetch bars from scanner provider
    2) build aligned sequence dataset
    3) train and save model bundle
    """
    tf = str(timeframe).strip().lower()
    # ── Data Minimization: Only fetch minimum required for LSTM training ──
    # Requirements:
    #   - LSTM needs max(200, seq_len * 3) clean rows minimum (dataset.py)
    #   - With 20% validation split, need ~250 rows before split
    #   - Feature engineering (EMA200, RSI14, MACD, ADX14) needs ~200 bars for rolling calcs
    #   - Add 50% buffer for weekends/holidays/bad data gaps
    #
    # Raw bars needed: ~400 minimum
    #   15m: 400 bars / (6.5 hrs * 4 bars/hr) = ~15 trading days = 3 weeks
    #   4h:  400 bars / (6.5 hrs / 4 hrs per bar) = ~246 trading days = 12 months
    #   Daily/Weekly: Use 1 year (reduced from 5 years)
    if tf == "15m":
        period = "3wk"   # ~15 trading days * 26 bars/day = ~390 bars (was "3mo")
    elif tf == "4h":
        period = "12mo"  # ~250 trading days * 1.625 bars/day = ~406 bars (was "12mo", kept)
    else:
        period = "1y"    # ~250 trading days (was "5y")

    try:
        bars = scan_market(symbol, period=period, interval=tf)
    except RateLimitError as rle:
        # Re-raise RateLimitError so training loop can detect 429 and trigger cooldown
        raise rle
    if bars is None or bars.empty:
        raise RuntimeError(f"No data returned for {symbol} {tf}")

    bundle = build_sequence_dataset(
        bars,
        seq_len=int(seq_len),
        label_horizon=int(label_horizon),
        label_threshold=float(label_threshold),
        cache_key=(
            f"{str(symbol).upper()}|{tf}|seq={int(seq_len)}|"
            f"h={int(label_horizon)}|thr={float(label_threshold):.6f}"
        ),
        use_cache=True,
    )
    model, hist = train_direction_model(
        bundle.X_train,
        bundle.y_train,
        bundle.X_val,
        bundle.y_val,
        model_kind=model_kind,
        epochs=int(epochs),
    )

    path = _direction_model_path(symbol, tf, model_kind)
    torch.save(
        {
            "state_dict": model.state_dict(),
            "model_kind": str(model_kind).lower(),
            "timeframe": tf,
            "symbol": str(symbol).upper(),
            "seq_len": int(seq_len),
            "feature_names": bundle.feature_names,
            "class_map": bundle.class_map,
            "inv_class_map": bundle.inv_class_map,
            "version": DIRECTION_MODEL_VERSION,
            "metrics": hist,
        },
        path,
    )

    # scaler stored separately (sklearn object)
    scaler_path = path + ".scaler.pkl"
    with open(scaler_path, "wb") as f:
        pickle.dump(bundle.scaler, f)

    return DirectionTrainResult(
        model_path=path,
        model_kind=str(model_kind).lower(),
        best_val_loss=float(hist["best_val_loss"]),
        best_val_acc=float(hist["best_val_acc"]),
        epochs=int(hist["best_epoch"]),
        n_train=int(len(bundle.X_train)),
        n_val=int(len(bundle.X_val)),
        num_features=int(bundle.X_train.shape[2]),
        num_classes=int(len(bundle.class_map)),
    )
