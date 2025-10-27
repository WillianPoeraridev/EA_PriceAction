# src/app/run_snapshot.py
from __future__ import annotations

import json
import sys
from collections import Counter
from typing import Dict, List

import numpy as np
import pandas as pd

from src.config import settings


def _pct(x: float) -> float:
    return float(round(100.0 * x, 2))


def _dt_range_ms(ms: pd.Series) -> Dict[str, str]:
    if ms.empty:
        return {"start": "", "end": ""}
    t = pd.to_datetime(ms, unit="ms", utc=True)
    return {"start": t.min().isoformat(), "end": t.max().isoformat()}


def _safe_mean(s: pd.Series) -> float:
    return float(s.mean()) if len(s) else float("nan")


def _ema_stats(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    cols = [
        "h1_ema20", "h1_ema50",
        "h4_ema20", "h4_ema50",
        "d_ema20",  "d_ema50",
        "w_ema20",  "w_ema50",
    ]
    out: Dict[str, Dict[str, float]] = {}
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce").dropna()
            if len(s):
                out[c] = {
                    "mean": float(s.mean()),
                    "min": float(s.min()),
                    "max": float(s.max()),
                }
    return out


def _value_counts(series: pd.Series) -> Dict[str, int]:
    return dict(Counter([str(x) for x in series.dropna().tolist()]))


def main():
    """
    Uso:
      python -m src.app.run_snapshot BTCUSDT
      # opcional: in/out
      python -m src.app.run_snapshot BTCUSDT data/BTCUSDT_annotated.csv data/BTCUSDT_snapshot.json
    """
    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else settings.DEFAULT_SYMBOL
    in_csv = sys.argv[2] if len(sys.argv) > 2 else f"{settings.OUTPUT_DIR}/{symbol}_annotated.csv"
    out_json = sys.argv[3] if len(sys.argv) > 3 else f"{settings.OUTPUT_DIR}/{symbol}_snapshot.json"

    df = pd.read_csv(in_csv)

    # --- QA básico ---
    qc = {
        "rows": int(len(df)),
        "has_t_close_h1": "t_close_h1" in df.columns,
        "date_range": _dt_range_ms(df["t_close_h1"]) if "t_close_h1" in df.columns else {"start": "", "end": ""},
        "duplicated_timestamps": int(df["t_close_h1"].duplicated().sum()) if "t_close_h1" in df.columns else -1,
        "null_ratio_overall_pct": _pct(float(df.isna().sum().sum()) / (len(df) * max(1, df.shape[1])) if len(df) else 0.0),
    }

    # --- Métricas pedidas ---
    # bar_type
    bar_type_counts = _value_counts(df["bar_type"]) if "bar_type" in df.columns else {}

    # tight range %
    tight_pct = _pct(df["is_tight_range"].mean()) if "is_tight_range" in df.columns else float("nan")

    # breakout dir
    bo_counts = _value_counts(df["bo_dir"]) if "bo_dir" in df.columns else {}

    # reversals
    rev_bull = _pct(df["reversal_bar_bull"].mean()) if "reversal_bar_bull" in df.columns else float("nan")
    rev_bear = _pct(df["reversal_bar_bear"].mean()) if "reversal_bar_bear" in df.columns else float("nan")

    # EMA stats
    ema_stats = _ema_stats(df)

    snapshot = {
        "symbol": symbol,
        "qa": qc,
        "counts": {
            "bar_type": bar_type_counts,
            "bo_dir": bo_counts,
        },
        "percentages": {
            "tight_range_pct": tight_pct,
            "reversal_bar_bull_pct": rev_bull,
            "reversal_bar_bear_pct": rev_bear,
        },
        "ema_stats": ema_stats,
    }

    # salvar
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    # imprimir resumo legível
    print(f"OK snapshot → {out_json}")
    print(f"Rows: {qc['rows']}   Range: {qc['date_range']['start']} → {qc['date_range']['end']}")
    if bar_type_counts:
        print("bar_type:", bar_type_counts)
    if bo_counts:
        print("bo_dir:", bo_counts)
    print("tight_range %:", tight_pct)
    print("rev_bull %:", rev_bull, " | rev_bear %:", rev_bear)
    if ema_stats:
        ex = ', '.join([f"{k}: μ={v['mean']:.2f}" for k, v in list(ema_stats.items())[:4]])
        print("ema_stats (amostra):", ex)


if __name__ == "__main__":
    main()
