from __future__ import annotations

import os
import pickle
import json
from typing import Any

import numpy as np
import pandas as pd

from .models import create_direction_model
from .trainer import DIRECTION_MODEL_VERSION


MODEL_DIR = "models"
_CACHE: dict[str, dict[str, Any]] = {}
_V2_COMPAT_MODEL_NAME = "direction_model_v2.pt"
_STABLE_DIR = os.path.join(MODEL_DIR, "stable")
_REGISTRY_PATH = os.path.join(MODEL_DIR, "deep_model_registry.json")


def _model_path(symbol: str, timeframe: str, kind: str) -> str:
    safe = str(symbol).replace("/", "-").replace("=", "").replace("^", "")
    tf = str(timeframe).strip().lower()
    k = str(kind).strip().lower()
    return os.path.join(MODEL_DIR, f"{safe}_dir_{tf}_{k}_v{DIRECTION_MODEL_VERSION}.pt")


def _stable_model_path(symbol: str, timeframe: str, kind: str) -> str:
    safe = str(symbol).replace("/", "-").replace("=", "").replace("^", "")
    tf = str(timeframe).strip().lower()
    k = str(kind).strip().lower()
    return os.path.join(_STABLE_DIR, f"{safe}_dir_{tf}_{k}_stable.pt")


def _registry_model_path(symbol: str, timeframe: str, kind: str) -> tuple[str, str] | None:
    key = f"{str(symbol).upper()}|{str(timeframe).lower()}|{str(kind).lower()}"
    try:
        if not os.path.exists(_REGISTRY_PATH):
            return None
        with open(_REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        entry = (data.get("models") or {}).get(key) or {}
        model_path = str(entry.get("model_path") or "").strip()
        scaler_path = str(entry.get("scaler_path") or "").strip()
        if model_path and scaler_path and os.path.exists(model_path) and os.path.exists(scaler_path):
            return model_path, scaler_path
    except Exception:
        return None
    return None


def _candidate_paths(symbol: str, timeframe: str, kind: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    reg = _registry_model_path(symbol, timeframe, kind)
    if reg is not None:
        out.append(reg)

    stable = _stable_model_path(symbol, timeframe, kind)
    stable_scaler = stable + ".scaler.pkl"
    if os.path.exists(stable) and os.path.exists(stable_scaler):
        out.append((stable, stable_scaler))

    canonical = _model_path(symbol, timeframe, kind)
    canonical_scaler = canonical + ".scaler.pkl"
    if os.path.exists(canonical) and os.path.exists(canonical_scaler):
        out.append((canonical, canonical_scaler))

    compat_path = os.path.join(MODEL_DIR, _V2_COMPAT_MODEL_NAME)
    compat_scaler = compat_path + ".scaler.pkl"
    if os.path.exists(compat_path) and os.path.exists(compat_scaler):
        out.append((compat_path, compat_scaler))

    return out


def invalidate_direction_bundle_cache(symbol: str | None = None, timeframe: str | None = None, kind: str | None = None):
    """
    Clear deep inference cache.
    - No args: clear all cached bundles.
    - With args: clear one logical key.
    """
    if symbol is None and timeframe is None and kind is None:
        _CACHE.clear()
        return
    s = str(symbol or "").upper()
    tf = str(timeframe or "").lower()
    k = str(kind or "").lower()
    key = f"{s}|{tf}|{k}"
    _CACHE.pop(key, None)


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

    for path, scaler_path in _candidate_paths(symbol, timeframe, kind):
        try:
            payload = torch.load(path, map_location="cpu")
            payload_version = int(payload.get("version", -1))
            is_v2_compat = (
                os.path.basename(path).lower() == _V2_COMPAT_MODEL_NAME
                and payload_version == 2
            )
            if payload_version != int(DIRECTION_MODEL_VERSION) and not is_v2_compat:
                continue
            feature_names = list(payload.get("feature_names", []) or [])
            class_map = payload.get("class_map", {}) or {}
            inv_class_map = payload.get("inv_class_map", {}) or {}
            if not feature_names or not class_map:
                continue
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
            continue
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
