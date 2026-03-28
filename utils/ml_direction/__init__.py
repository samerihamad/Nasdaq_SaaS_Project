"""
Deep direction-model toolkit (Phase 7).

This package adds optional sequence models (LSTM/GRU/Transformer) without
disrupting the existing RF gatekeeper flow.
"""

from .labels import generate_direction_labels
from .dataset import build_sequence_dataset
from .models import create_direction_model
from .infer import load_direction_bundle, predict_direction_from_features

try:
    from .trainer import train_direction_model, train_direction_for_symbol
except Exception:  # pragma: no cover - optional runtime dependency (torch)
    train_direction_model = None
    train_direction_for_symbol = None

__all__ = [
    "generate_direction_labels",
    "build_sequence_dataset",
    "create_direction_model",
    "load_direction_bundle",
    "predict_direction_from_features",
    "train_direction_model",
    "train_direction_for_symbol",
]
