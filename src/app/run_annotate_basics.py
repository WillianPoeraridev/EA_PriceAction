from __future__ import annotations
import os, sys
from typing import List, Dict
from src.config import settings

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("Este script requer pandas e numpy. Instale com: pip install pandas numpy")
    sys.exit(1)

def compute_body_ratio(df, body_ratio_min):
    rng = (df["h1_high"] - df["h1_low"]).replace(0, np.nan)
    body = (df["h1_close"] - df["h1_open"]).abs()
    df["body_ratio"] = (body / rng).clip(upper=10).fillna(0)
    cond_bull = (df["h1_close"] > df["h1_open"]) & (df["body_ratio"] >= body_ratio_min)
    cond_bear = (df["h1_close"] < df["h1_open"]) & (df["body_ratio"] >= body_ratio_min)
    df["bar_type"] = np.where(cond_bull, "trend_bull", np.where(cond_bear, "trend_bear", "doji"))
    return df

def rolling_overlap_flags(df, N, body_ratio_max, range_factor_max, overlap_min):
    highs = df["h1_high"].values
    lows  = df["h1_low"].values
    br    = df["body_ratio"].values
    flags = np.zeros(len(df), dtype=bool)
    for i in range(len(df)):
        if i+1 < N:
            flags[i] = False
            continue
        lo = i+1-N; hi = i+1
        win_high = highs[lo:hi]; win_low = lows[lo:hi]
        avg_range = np.mean(win_high - win_low)
        big_span  = np.max(win_high) - np.min(win_low)
        prop_small_body = np.mean(br[lo:hi] < body_ratio_max)
        overlaps = 0; denom = N-1
        for j in range(lo+1, hi):
            lowA, highA = lows[j-1], highs[j-1]
            lowB, highB = lows[j], highs[j]
            inter = min(highA, highB) - max(lowA, lowB)
            if inter > 0: overlaps += 1
        prop_overlap = overlaps/denom if denom>0 else 0.0
        flags[i] = (avg_range>0) and (big_span/avg_range < range_factor_max) and (prop_overlap >= overlap_min) and (prop_small_body >= 0.5)
    df["is_tight_range"] = flags
    return df

def breakout_labels(df, M, delta_pct):
    roll_max = df["h1_high"].shift(1).rolling(M, min_periods=1).max()
    roll_min = df["h1_low"].shift(1).rolling(M, min_periods=1).min()
    thr_up = roll_max * (1 + delta_pct)
    thr_dn = roll_min * (1 - delta_pct)
    bo_up = df["h1_high"] > thr_up
    bo_dn = df["h1_low"]  < thr_dn
    df["bo_dir"] = np.where(bo_up, "up", np.where(bo_dn, "down", "none"))
    return df

def reversal_in_range(df, window, wick_ratio_min, pos_low_thresh, pos_high_thresh):
    min_low  = df["h1_low"].rolling(window, min_periods=1).min()
    max_high = df["h1_high"].rolling(window, min_periods=1).max()
    rng = (df["h1_high"] - df["h1_low"]).replace(0, np.nan)
    lower_wick = (np.minimum(df["h1_open"], df["h1_close"]) - df["h1_low"]) / rng
    upper_wick = (df["h1_high"] - np.maximum(df["h1_open"], df["h1_close"])) / rng
    pos = (df["h1_close"] - min_low) / (max_high - min_low).replace(0, np.nan)
    df["reversal_bar_bull"] = (lower_wick >= wick_ratio_min) & (pos <= pos_low_thresh)
    df["reversal_bar_bear"] = (upper_wick >= wick_ratio_min) & (pos >= pos_high_thresh)
    df[["reversal_bar_bull","reversal_bar_bear"]] = df[["reversal_bar_bull","reversal_bar_bear"]].fillna(False)
    return df

def gap20_context(df, window, gap_min_ratio):
    # requer h1_ema20 presente (do CSV mesclado)
    if "h1_ema20" not in df.columns:
        df["is_20_gap_context"] = False
        return df
    dist = (df["h1_close"] - df["h1_ema20"]) / df["h1_ema20"]
    def all_same_sign(x): 
        return 1.0 if (np.all(x>0) or np.all(x<0)) else 0.0
    cons = dist.rolling(window, min_periods=window).apply(all_same_sign, raw=True).fillna(0) > 0.5
    far  = dist.abs().rolling(window, min_periods=1).mean() >= gap_min_ratio
    df["is_20_gap_context"] = cons & far
    return df

def main():
    if len(sys.argv) < 2:
        print("Uso: python -m src.app.run_annotate_basics BTCUSDT [input_csv] [output_csv]")
        sys.exit(1)
    symbol = sys.argv[1].upper()
    in_csv  = sys.argv[2] if len(sys.argv) > 2 else f"{settings.OUTPUT_DIR}/{symbol}_mtf_with_ema.csv"
    out_csv = sys.argv[3] if len(sys.argv) > 3 else f"{settings.OUTPUT_DIR}/{symbol}_annotated.csv"


    # carregar merged + H1 OHLC (para calcular métricas de candle)
    merged = pd.read_csv(in_csv)
    h1 = pd.read_csv(f"{settings.OUTPUT_DIR}/{symbol}_1h.csv")
    # trazer OHLC do H1 para o merged (chave: close_time_ms do H1 == t_close_h1 no merged)
    h1_ren = h1.rename(columns={
        "open":"h1_open","high":"h1_high","low":"h1_low",
        "close_time_ms":"t_close_h1"
    })[["t_close_h1","h1_open","h1_high","h1_low"]]
    df = pd.merge(merged, h1_ren, on="t_close_h1", how="left")
    
    # --- Sanity check (COLE AQUI) ---
    required = {"h1_close", "h1_open", "h1_high", "h1_low"}
    missing = sorted(list(required - set(df.columns)))
    assert not missing, f"Faltando colunas: {missing}. Disponíveis: {list(df.columns)}"
    # -------------------------------

    # parâmetros v0 (poderão vir de YAML futuramente)
    body_ratio_min = 0.3
    df = compute_body_ratio(df, body_ratio_min)

    # R-0002 tight range
    df = rolling_overlap_flags(df, N=10, body_ratio_max=0.3, range_factor_max=2.0, overlap_min=0.6)

    # R-0003 breakout
    df = breakout_labels(df, M=20, delta_pct=0.0)

    # R-0004 reversal em range
    df = reversal_in_range(df, window=20, wick_ratio_min=0.4, pos_low_thresh=0.25, pos_high_thresh=0.75)

    # R-0006 20-gap-bar (EMA20)
    df = gap20_context(df, window=10, gap_min_ratio=0.01)

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"OK: {out_csv} (linhas={len(df)})")

if __name__ == "__main__":
    main()
