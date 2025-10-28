# src/app/run_backtest_v1.py
from __future__ import annotations

import sys
import math
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Dict
from src.config import settings

"""
Backtest v1 (Spot, long-only) — close-only

Regras:
- Entra no fechamento da barra-sinal (H1).
- Stop: mínima dos ÚLTIMOS N FECHAMENTOS (antes da barra de entrada).
- R = entry - stop; se R<=0, ignora o sinal.
- Target: entry + (R * r_mult)  [default r_mult=1.0].
- Caminha barra a barra nos FECHAMENTOS seguintes:
    close <= stop   -> sai por stop
    close >= target -> sai por alvo
- Fees/slippage: pega do .env (FEES_BPS e SLIPPAGE_BPS), aplica em entry e exit.

Uso:
  python -m src.app.run_backtest_v1 BTCUSDT data/BTCUSDT_signals_NONE.csv 5 1.0
  #                        símbolo          sinais.csv               N  R-mult
"""


@dataclass
class Trade:
    t_entry: int
    t_exit: int
    entry: float
    stop: float
    target: float
    exit_price: float
    exit_reason: str
    r: float
    pnl_r: float
    pnl_quote: float


def _fees_factor(bps: float) -> float:
    """Converte bps (ex.: 8.0) para fator (0.0008)."""
    return float(bps) / 10000.0


def load_series(symbol: str) -> pd.Series:
    """
    Carrega a série de FECHAMENTOS H1 a partir do annotated CSV,
    garantindo tipos corretos, índice único e ordenado.
    """
    annotated = f"{settings.OUTPUT_DIR}/{symbol}_annotated.csv"
    df = pd.read_csv(annotated)

    s = pd.Series(
        df["h1_close"].astype(float).values,
        index=df["t_close_h1"].astype("int64").values,
        name="close",
    )
    s = s.dropna()
    # remove timestamps duplicados (mantém o último)
    s = s[~pd.Index(s.index).duplicated(keep="last")]
    # ordena por tempo
    s = s.sort_index()
    return s


def backtest(
    symbol: str,
    signals_csv: str,
    lookback_n: int = 5,
    r_mult: float = 1.0,
) -> Tuple[List[Trade], Dict]:
    """
    Executa o backtest close-only sobre um CSV de sinais.
    Retorna (lista_de_trades, métricas).
    """
    close = load_series(symbol)

    sig = pd.read_csv(signals_csv)
    required = {"entry", "t_close_h1", "h1_close"}
    if not required.issubset(sig.columns):
        raise ValueError(f"signals.csv precisa conter colunas: {sorted(required)}")

    # apenas sinais ON (1)
    sig_on = sig[sig["entry"] == 1].copy()
    sig_on.sort_values("t_close_h1", inplace=True)

    fee = _fees_factor(settings.FEES_BPS)
    slp = _fees_factor(settings.SLIPPAGE_BPS)

    trades: List[Trade] = []

    for _, row in sig_on.iterrows():
        t = int(row["t_close_h1"])

        # posição do timestamp na série (retorna int; -1 se não achar)
        idx_pos = int(close.index.get_indexer([t])[0])
        if idx_pos == -1:
            # sinal não mapeia para um close conhecido
            continue

        if idx_pos < lookback_n:
            # não há janelas suficientes p/ stop
            continue

        # Preço de entrada com slippage
        entry_price = float(close.iloc[idx_pos]) * (1 + slp)

        # Janela para o stop (mínimo dos últimos N fechamentos anteriores)
        start = max(0, idx_pos - lookback_n)
        prev_window = close.iloc[start: idx_pos]
        if len(prev_window) == 0:
            continue

        stop_price = float(prev_window.min())
        r = entry_price - stop_price
        if r <= 0 or math.isnan(r):
            continue

        target_price = entry_price + r_mult * r

        # Caminha à frente
        exited = False
        exit_price = entry_price
        exit_reason = "none"
        t_exit = t

        for j in range(idx_pos + 1, len(close)):
            p = float(close.iloc[j])
            t_j = int(close.index[j])

            if p <= stop_price:
                exit_price = p * (1 - slp)  # venda com slippage
                exit_reason = "stop"
                t_exit = t_j
                exited = True
                break
            elif p >= target_price:
                exit_price = p * (1 - slp)
                exit_reason = "target"
                t_exit = t_j
                exited = True
                break

        if not exited:
            # Sem saída dentro do histórico -> encerra na última barra
            p = float(close.iloc[-1])
            exit_price = p * (1 - slp)
            exit_reason = "eod"  # end-of-data
            t_exit = int(close.index[-1])

        # fees ida+volta (proporcional ao notional)
        gross = exit_price - entry_price
        fee_cost = (entry_price + exit_price) * fee
        pnl_quote = gross - fee_cost
        pnl_r = pnl_quote / r

        trades.append(
            Trade(
                t_entry=t,
                t_exit=t_exit,
                entry=entry_price,
                stop=stop_price,
                target=target_price,
                exit_price=exit_price,
                exit_reason=exit_reason,
                r=r,
                pnl_r=pnl_r,
                pnl_quote=pnl_quote,
            )
        )

    # Métricas básicas
    n = len(trades)
    wins = sum(1 for tr in trades if tr.exit_reason == "target")
    losses = sum(1 for tr in trades if tr.exit_reason == "stop")
    eod = sum(1 for tr in trades if tr.exit_reason == "eod")
    avg_r = (sum(tr.pnl_r for tr in trades) / n) if n else 0.0
    winrate = (wins / n) if n else 0.0
    expectancy = avg_r  # média em R por trade (já inclui wins/losses/fees/slippage)

    metrics = dict(
        trades=n,
        wins=wins,
        losses=losses,
        eod=eod,
        winrate=winrate,
        avgR=avg_r,
        expectancyR=expectancy,
    )
    return trades, metrics


def main():
    args = sys.argv
    symbol = args[1].upper() if len(args) > 1 else settings.DEFAULT_SYMBOL
    signals_csv = args[2] if len(args) > 2 else f"{settings.OUTPUT_DIR}/{symbol}_signals.csv"
    lookback_n = int(args[3]) if len(args) > 3 else 5
    r_mult = float(args[4]) if len(args) > 4 else 1.0

    trades, metrics = backtest(symbol, signals_csv, lookback_n, r_mult)

    # Salva CSV de trades
    out_csv = f"{settings.OUTPUT_DIR}/{symbol}_trades.csv"
    if trades:
        df = pd.DataFrame([t.__dict__ for t in trades])
        df.to_csv(out_csv, index=False)
    else:
        pd.DataFrame(
            [],
            columns=[
                "t_entry", "t_exit", "entry", "stop", "target",
                "exit_price", "exit_reason", "r", "pnl_r", "pnl_quote",
            ],
        ).to_csv(out_csv, index=False)

    print(f"OK backtest → {out_csv}")
    print(
        f"Métricas: trades={metrics['trades']} wins={metrics['wins']} "
        f"losses={metrics['losses']} eod={metrics['eod']}"
    )
    print(
        f"Winrate={metrics['winrate']:.2%}  AvgR={metrics['avgR']:.3f}  "
        f"ExpectancyR={metrics['expectancyR']:.3f}"
    )


if __name__ == "__main__":
    main()
