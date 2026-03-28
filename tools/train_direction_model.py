import argparse
import json
import logging

from utils.ai_model import train_deep_direction_model


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Train deep direction model (Phase 7)")
    p.add_argument("symbol", type=str, help="Ticker symbol (e.g. AAPL)")
    p.add_argument("--timeframe", type=str, default="15m", choices=["1d", "4h", "15m"])
    p.add_argument("--kind", type=str, default="lstm", choices=["lstm", "gru", "transformer"])
    p.add_argument("--seq-len", type=int, default=64)
    p.add_argument("--horizon", type=int, default=8)
    p.add_argument("--threshold", type=float, default=0.012)
    p.add_argument("--epochs", type=int, default=25)
    args = p.parse_args()

    result = train_deep_direction_model(
        symbol=args.symbol.upper(),
        timeframe=args.timeframe,
        model_kind=args.kind,
        seq_len=args.seq_len,
        label_horizon=args.horizon,
        label_threshold=args.threshold,
        epochs=args.epochs,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
