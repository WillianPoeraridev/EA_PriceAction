# src/app/run_generate_signals.py
from __future__ import annotations

import sys
import pandas as pd
from src.config import settings
import argparse

"""
Gera sinais long-only (Spot) usando o annotated CSV.

Regras v0 (default):
- Regime: D.ema20 > D.ema50  (fallback ignora W se faltar histórico)
- Gatilho H1: bar_type=="trend_bull" AND (NOT is_tight_range) AND (bo_dir=="up" OR reversal_bar_bull)

CLI:
  python -m src.app.run_generate_signals BTCUSDT [in_csv] [out_csv] [REGIME] [--debug]

REGIME pode ser:
  D_ONLY     -> exige apenas D20>D50   (DEFAULT)
  D_AND_W    -> exige D20>D50 E W20>W50 (se W faltar, ninguém passa)
  H4_AND_D   -> exige H4_20>50 E D20>50
  NONE       -> sem filtro de regime (para testar o fluxo)

Exemplos:
  python -m src.app.run_generate_signals BTCUSDT
  python -m src.app.run_generate_signals BTCUSDT data/BTCUSDT_annotated.csv data/BTCUSDT_signals.csv H4_AND_D --debug
"""

REQUIRED = {
    "t_close_h1",
    "bar_type",
    "is_tight_range",
    "bo_dir",
    "reversal_bar_bull",
    "h1_close",
    "d_ema20", "d_ema50",
    "h4_ema20", "h4_ema50",
    "w_ema20", "w_ema50",
}

RULE_ID = "SIG-V0-TREND"


def _ensure_columns(df: pd.DataFrame) -> None:
    missing = sorted(list(REQUIRED - set(df.columns)))
    if missing:
        raise KeyError(f"Faltando colunas no annotated: {missing}")


def _regime_mask(df: pd.DataFrame, mode: str) -> pd.Series:
    mode = (mode or "D_ONLY").upper()
    d_ok  = (df["d_ema20"]  > df["d_ema50"]).fillna(False)
    h4_ok = (df["h4_ema20"] > df["h4_ema50"]).fillna(False)

    # semanal pode não ter 50 candles suficientes neste recorte
    have_weekly = df["w_ema50"].notna().sum() >= 5
    if have_weekly:
        w_ok = (df["w_ema20"] > df["w_ema50"]).fillna(False)
    else:
        w_ok = pd.Series(False, index=df.index)

    if mode == "D_ONLY":
        return d_ok
    elif mode == "D_AND_W":
        return d_ok & w_ok
    elif mode == "H4_AND_D":
        return h4_ok & d_ok
    elif mode == "NONE":
        return pd.Series(True, index=df.index)
    else:
        raise ValueError(f"REGIME desconhecido: {mode}")


# ... topos do arquivo inalterados ...

def main():
    parser = argparse.ArgumentParser(description="Gerar sinais long-only (Spot)")
    parser.add_argument("symbol", nargs="?", default=settings.DEFAULT_SYMBOL, help="Símbolo (ex: BTCUSDT)")
    parser.add_argument("in_csv", nargs="?", default=None, help="Caminho do annotated.csv (opcional)")
    parser.add_argument("out_csv", nargs="?", default=None, help="Caminho de saída (opcional)")
    parser.add_argument("--regime", choices=["D_ONLY","D_AND_W","H4_AND_D","NONE"], default="D_ONLY")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    symbol = args.symbol.upper()
    in_csv  = args.in_csv  or f"{settings.OUTPUT_DIR}/{symbol}_annotated.csv"
    out_csv = args.out_csv or f"{settings.OUTPUT_DIR}/{symbol}_signals.csv"
    regime  = args.regime
    debug   = args.debug

    df = pd.read_csv(in_csv)
    _ensure_columns(df)

    # Filtros (funil)
    m_regime = _regime_mask(df, regime)
    m_bar    = (df["bar_type"] == "trend_bull")
    m_tight  = (~df["is_tight_range"].fillna(False))
    m_trig   = (df["bo_dir"].fillna("none").eq("up")) | (df["reversal_bar_bull"].fillna(False))

    entry = (m_regime & m_bar & m_tight & m_trig).astype(int)

    out_cols = [
        "t_close_h1", "h1_close",
        "bar_type", "is_tight_range", "bo_dir", "reversal_bar_bull",
        "h4_ema20", "h4_ema50",
        "d_ema20", "d_ema50",
        "w_ema20", "w_ema50",
    ]

    sig = df.loc[:, out_cols].copy()
    sig["regime_mode"] = regime
    sig["regime_ok"]   = m_regime
    sig["bar_ok"]      = m_bar
    sig["tight_ok"]    = m_tight
    sig["trig_ok"]     = m_trig
    sig["entry"]       = entry
    sig["rule_id"]     = RULE_ID
    sig["notes"]       = "trend_bull && !tight && (bo_up || rev_bull)"

    sig.to_csv(out_csv, index=False)

    total = int(len(sig))
    n_reg  = int(m_regime.sum())
    n_bar  = int(m_bar.sum())
    n_tght = int(m_tight.sum())
    n_trig = int(m_trig.sum())
    n_ent  = int(sig["entry"].sum())
    print(f"OK: {out_csv} (linhas={total})")
    print(f"[FUNIL] regime:{n_reg}  bar:{n_bar}  tight:{n_tght}  trig:{n_trig}  entries:{n_ent}")

    if debug and n_ent:
        last_rows = sig[sig["entry"] == 1].tail(5)
        print("Últimos 5 sinais:")
        print(last_rows[["t_close_h1", "h1_close", "entry", "rule_id", "regime_mode"]].to_string(index=False))

if __name__ == "__main__":
    main()
