from __future__ import annotations

import hashlib
import os
import pickle
from dataclasses import dataclass
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from utils.ai_model import build_features
from .labels import generate_direction_labels

DATASET_CACHE_DIR = os.path.join("utils", "ml_direction", "dataset_cache")
os.makedirs(DATASET_CACHE_DIR, exist_ok=True)


@dataclass
class SequenceDatasetBundle:
    X_train: np.ndarray
    y_train: np.ndarray
    X_val: np.ndarray
    y_val: np.ndarray
    scaler: StandardScaler
    feature_names: list[str]
    class_map: dict[int, int]
    inv_class_map: dict[int, int]


def _window_stack(X: np.ndarray, y: np.ndarray, seq_len: int) -> tuple[np.ndarray, np.ndarray]:
    n = len(X)
    if n <= int(seq_len):
        return np.empty((0, seq_len, X.shape[1]), dtype=np.float32), np.empty((0,), dtype=np.int64)
    xs: list[np.ndarray] = []
    ys: list[int] = []
    for i in range(int(seq_len), n):
        xs.append(X[i - seq_len:i, :])
        ys.append(int(y[i]))
    return np.asarray(xs, dtype=np.float32), np.asarray(ys, dtype=np.int64)


def _cache_paths(cache_key: str) -> tuple[str, str]:
    safe = hashlib.sha256(str(cache_key).encode("utf-8")).hexdigest()[:32]
    base = os.path.join(DATASET_CACHE_DIR, f"{safe}")
    return base + ".npz", base + ".meta.pkl"


def _load_cached_bundle(cache_key: str) -> SequenceDatasetBundle | None:
    npz_path, meta_path = _cache_paths(cache_key)
    if not (os.path.exists(npz_path) and os.path.exists(meta_path)):
        return None
    try:
        arr = np.load(npz_path, allow_pickle=False)
        with open(meta_path, "rb") as f:
            meta = pickle.load(f)
        return SequenceDatasetBundle(
            X_train=arr["X_train"],
            y_train=arr["y_train"],
            X_val=arr["X_val"],
            y_val=arr["y_val"],
            scaler=meta["scaler"],
            feature_names=list(meta["feature_names"]),
            class_map={int(k): int(v) for k, v in dict(meta["class_map"]).items()},
            inv_class_map={int(k): int(v) for k, v in dict(meta["inv_class_map"]).items()},
        )
    except Exception:
        return None


def _save_cached_bundle(cache_key: str, bundle: SequenceDatasetBundle) -> None:
    npz_path, meta_path = _cache_paths(cache_key)
    try:
        np.savez_compressed(
            npz_path,
            X_train=bundle.X_train.astype(np.float32, copy=False),
            y_train=bundle.y_train.astype(np.int64, copy=False),
            X_val=bundle.X_val.astype(np.float32, copy=False),
            y_val=bundle.y_val.astype(np.int64, copy=False),
        )
        with open(meta_path, "wb") as f:
            pickle.dump(
                {
                    "scaler": bundle.scaler,
                    "feature_names": list(bundle.feature_names),
                    "class_map": dict(bundle.class_map),
                    "inv_class_map": dict(bundle.inv_class_map),
                },
                f,
            )
    except Exception:
        pass


def build_sequence_dataset(
    df: pd.DataFrame,
    *,
    seq_len: int = 64,
    label_horizon: int = 8,
    label_threshold: float = 0.012,
    val_ratio: float = 0.2,
    cache_key: str | None = None,
    use_cache: bool = True,
) -> SequenceDatasetBundle:
    """
    Build deep-learning sequence dataset from OHLCV bars.

    Feature generation intentionally reuses `utils.ai_model.build_features()`
    so AI training and live gatekeeper remain aligned on core inputs.
    """
    if cache_key and use_cache:
        cached = _load_cached_bundle(str(cache_key))
        if cached is not None:
            return cached

    if df is None or df.empty:
        raise ValueError("Empty dataframe passed to build_sequence_dataset")

    frame = df.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col not in frame.columns:
            raise ValueError(f"Missing required column: {col}")

    features = build_features(frame)
    labels = generate_direction_labels(
        pd.to_numeric(frame["Close"], errors="coerce"),
        horizon_bars=label_horizon,
        return_threshold=label_threshold,
    )

    data = features.join(labels.rename("label")).dropna()
    if len(data) < max(200, seq_len * 3):
        raise ValueError(f"Too few clean rows for sequence dataset: {len(data)}")

    X_raw = data.drop(columns=["label"]).to_numpy(dtype=np.float32)
    y_raw = data["label"].astype(int).to_numpy(dtype=np.int64)
    feature_names = list(data.drop(columns=["label"]).columns)

    split_idx = int(len(X_raw) * (1.0 - float(val_ratio)))
    split_idx = max(seq_len + 1, min(split_idx, len(X_raw) - 1))

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_raw[:split_idx])
    X_val_scaled = scaler.transform(X_raw[split_idx:])

    X_train_seq, y_train_seq = _window_stack(X_train_scaled, y_raw[:split_idx], seq_len)
    X_val_seq, y_val_seq = _window_stack(X_val_scaled, y_raw[split_idx:], seq_len)
    if len(X_train_seq) == 0 or len(X_val_seq) == 0:
        raise ValueError("Sequence windows are empty. Increase history or reduce seq_len.")

    # Map labels from {-1,0,1} to contiguous class IDs for CE loss.
    uniq = sorted({int(v) for v in np.unique(np.concatenate([y_train_seq, y_val_seq]))})
    class_map = {lbl: i for i, lbl in enumerate(uniq)}
    inv_class_map = {v: k for k, v in class_map.items()}
    y_train_ids = np.asarray([class_map[int(v)] for v in y_train_seq], dtype=np.int64)
    y_val_ids = np.asarray([class_map[int(v)] for v in y_val_seq], dtype=np.int64)

    out = SequenceDatasetBundle(
        X_train=X_train_seq,
        y_train=y_train_ids,
        X_val=X_val_seq,
        y_val=y_val_ids,
        scaler=scaler,
        feature_names=feature_names,
        class_map=class_map,
        inv_class_map=inv_class_map,
    )
    if cache_key and use_cache:
        _save_cached_bundle(str(cache_key), out)
    return out
