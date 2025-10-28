# src/app/run_plot_trades.py
from __future__ import annotations

import sys
import numpy as np
import pandas as pd

# backend headless para salvar PNG sem abrir janela
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.markers import MarkerStyle

from src.config import settings


def _to_naive_np_datetime64(obj: pd.Index | pd.Series) -> np.ndarray:
    """
    Converte Index/Series de datetimes (com ou sem tz) para
    np.ndarray dtype=datetime64[ns], sem timezone.
    """
    idx = pd.DatetimeIndex(obj)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    return idx.to_numpy(dtype="datetime64[ns]")


def main():
    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else settings.DEFAULT_SYMBOL
    trades_csv = sys.argv[2] if len(sys.argv) > 2 else f"{settings.OUTPUT_DIR}/{symbol}_trades.csv"
    annotated = f"{settings.OUTPUT_DIR}/{symbol}_annotated.csv"

    # -------- Série H1 (close) --------
    df = pd.read_csv(annotated)
    df["dt"] = pd.to_datetime(df["t_close_h1"], unit="ms", utc=True)
    df.set_index("dt", inplace=True)

    close = df["h1_close"].astype(float)
    x_price = _to_naive_np_datetime64(close.index)  # ndarray[datetime64[ns]]
    y_price = close.to_numpy(dtype=float)           # ndarray[float]

    # -------- Trades --------
    tr = pd.read_csv(trades_csv)
    if tr.empty:
        print("Sem trades para plotar.")
        return

    tr["dt_entry"] = pd.to_datetime(tr["t_entry"], unit="ms", utc=True)
    tr["dt_exit"]  = pd.to_datetime(tr["t_exit"],  unit="ms", utc=True)

    x_entry = _to_naive_np_datetime64(tr["dt_entry"])
    y_entry = tr["entry"].astype(float).to_numpy()

    # -------- Plot --------
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x_price, y_price, linewidth=1.0, label="H1 close")

    ms_entry  = MarkerStyle("^")
    ms_target = MarkerStyle("o")
    ms_stop   = MarkerStyle("x")
    ms_eod    = MarkerStyle("s")

    ax.scatter(x_entry, y_entry, marker=ms_entry, s=60, label="entry")

    # Saídas por razão
    t = tr[tr["exit_reason"] == "target"]
    if not t.empty:
        ax.scatter(
            _to_naive_np_datetime64(t["dt_exit"]),
            t["exit_price"].astype(float).to_numpy(),
            marker=ms_target, s=40, label="target",
        )

    s = tr[tr["exit_reason"] == "stop"]
    if not s.empty:
        ax.scatter(
            _to_naive_np_datetime64(s["dt_exit"]),
            s["exit_price"].astype(float).to_numpy(),
            marker=ms_stop, s=40, label="stop",
        )

    e = tr[tr["exit_reason"] == "eod"]
    if not e.empty:
        ax.scatter(
            _to_naive_np_datetime64(e["dt_exit"]),
            e["exit_price"].astype(float).to_numpy(),
            marker=ms_eod, s=40, label="eod",
        )

    ax.legend(loc="best")
    ax.set_title(f"{symbol} — trades (backtest v1)")
    ax.set_xlabel("time")
    ax.set_ylabel("price")

    fig.tight_layout()
    out = f"{settings.OUTPUT_DIR}/{symbol}_trades_plot.png"
    fig.savefig(out, dpi=150)
    print(f"OK plot → {out}")


if __name__ == "__main__":
    main()
