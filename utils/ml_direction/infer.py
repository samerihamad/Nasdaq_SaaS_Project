from __future__ import annotations

import os
import pickle
from typing import Any

import numpy as np
import pandas as pd

from .models import create_direction_model
from .trainer import DIRECTION_MODEL_VERSION


MODEL_DIR = "models"
_CACHE: dict[str, dict[str, Any]] = {}


def _model_path(symbol: str, timeframe: str, kind: str) -> str:
    safe = str(symbol).replace("/", "-").replace("=", "").replace("^", "")
    tf = str(timeframe).strip().lower()
    k = str(kind).strip().lower()
    return os.path.join(MODEL_DIR, f"{safe}_dir_{tf}_{k}_v{DIRECTION_MODEL_VERSION}.pt")


def load_direction_bundle(symbol: str, timeframe: str, kind: str = "lstm") -> dict[str, Any] | None:
    """
    Load deep direction model + scaler bundle for inference.
    Returns None if unavailable or torch is not installed.
    """
    key = f"{str(symbol).upper()}|{str(timeframe).lower()}|{str(kind).lower()}"
    if key in _CACHE:
        return _CACHE[key]

    try:
        import torch
    except Exception:
        return None

    path = _model_path(symbol, timeframe, kind)
    scaler_path = path + ".scaler.pkl"
    if not os.path.exists(path) or not os.path.exists(scaler_path):
        return None

    try:
        payload = torch.load(path, map_location="cpu")
        if int(payload.get("version", -1)) != int(DIRECTION_MODEL_VERSION):
            return None
        feature_names = list(payload.get("feature_names", []) or [])
        class_map = payload.get("class_map", {}) or {}
        inv_class_map = payload.get("inv_class_map", {}) or {}
        if not feature_names or not class_map:
            return None
        with open(scaler_path, "rb") as f:
            scaler = pickle.load(f)

        model = create_direction_model(
            payload.get("model_kind", kind),
            input_dim=int(len(feature_names)),
            num_classes=int(len(class_map)),
            hidden_dim=128,
            num_layers=2,
            dropout=0.2,
        )
        model.load_state_dict(payload["state_dict"])
        model.eval()

        bundle = {
            "model": model,
            "scaler": scaler,
            "seq_len": int(payload.get("seq_len", 64)),
            "feature_names": feature_names,
            "class_map": {int(k): int(v) for k, v in class_map.items()},
            "inv_class_map": {int(k): int(v) for k, v in inv_class_map.items()},
        }
        _CACHE[key] = bundle
        return bundle
    except Exception:
        return None


def predict_direction_from_features(
    features: pd.DataFrame,
    bundle: dict[str, Any],
) -> tuple[str | None, float]:
    """
    Predict BUY/SELL from most recent sequence window.
    Returns (action, confidence_percent). HOLD maps to (None, confidence).
    """
    try:
        import torch
    except Exception:
        return None, 0.0

    if features is None or features.empty:
        return None, 0.0
    if not bundle:
        return None, 0.0

    cols = bundle["feature_names"]
    seq_len = int(bundle["seq_len"])
    if len(features) < seq_len + 1:
        return None, 0.0

    missing = [c for c in cols if c not in features.columns]
    if missing:
        return None, 0.0

    f = features[cols].dropna()
    if len(f) < seq_len + 1:
        return None, 0.0
    arr = f.to_numpy(dtype=np.float32)
    scaled = bundle["scaler"].transform(arr)
    x = scaled[-seq_len:, :]
    x = np.expand_dims(x, axis=0)

    with torch.no_grad():
        logits = bundle["model"](torch.tensor(x, dtype=torch.float32))
        probs = torch.softmax(logits, dim=1).cpu().numpy()[0]
    pred_idx = int(np.argmax(probs))
    conf = float(probs[pred_idx] * 100.0)
    mapped = int(bundle["inv_class_map"].get(pred_idx, 0))
    if mapped == 1:
        return "BUY", round(conf, 1)
    if mapped == -1:
        return "SELL", round(conf, 1)
    return None, round(conf, 1)
