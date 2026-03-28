from __future__ import annotations

import torch
import torch.nn as nn


class DirectionLSTM(nn.Module):
    def __init__(self, input_dim: int, num_classes: int, hidden_dim: int = 128, num_layers: int = 2, dropout: float = 0.2):
        super().__init__()
        self.rnn = nn.LSTM(
            input_size=input_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.LayerNorm(hidden_dim),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, num_classes),
        )

    def forward(self, x):
        out, _ = self.rnn(x)
        return self.head(out[:, -1, :])


class DirectionGRU(nn.Module):
    def __init__(self, input_dim: int, num_classes: int, hidden_dim: int = 128, num_layers: int = 2, dropout: float = 0.2):
        super().__init__()
        self.rnn = nn.GRU(
            input_size=input_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.LayerNorm(hidden_dim),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, num_classes),
        )

    def forward(self, x):
        out, _ = self.rnn(x)
        return self.head(out[:, -1, :])


class DirectionTransformer(nn.Module):
    def __init__(
        self,
        input_dim: int,
        num_classes: int,
        d_model: int = 128,
        nhead: int = 4,
        num_layers: int = 3,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.input_proj = nn.Linear(input_dim, d_model)
        enc_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers=num_layers)
        self.head = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Dropout(dropout),
            nn.Linear(d_model, num_classes),
        )

    def forward(self, x):
        z = self.input_proj(x)
        z = self.encoder(z)
        return self.head(z[:, -1, :])


def create_direction_model(
    kind: str,
    *,
    input_dim: int,
    num_classes: int,
    hidden_dim: int = 128,
    num_layers: int = 2,
    dropout: float = 0.2,
):
    k = str(kind or "lstm").strip().lower()
    if k == "lstm":
        return DirectionLSTM(
            input_dim=input_dim,
            num_classes=num_classes,
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            dropout=dropout,
        )
    if k == "gru":
        return DirectionGRU(
            input_dim=input_dim,
            num_classes=num_classes,
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            dropout=dropout,
        )
    if k in ("transformer", "xformer"):
        return DirectionTransformer(
            input_dim=input_dim,
            num_classes=num_classes,
            d_model=hidden_dim,
            nhead=4,
            num_layers=max(2, int(num_layers)),
            dropout=dropout,
        )
    raise ValueError(f"Unknown direction model kind: {kind!r}")
