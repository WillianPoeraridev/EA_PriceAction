# src/app/run_bt_sweep.py
from __future__ import annotations
import sys, itertools
import pandas as pd
from src.config import settings
from src.app.run_backtest_v1 import backtest

def main():
    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else settings.DEFAULT_SYMBOL
    # por padrão, varre em cima do DONLY (troque se quiser)
    signals_csv = sys.argv[2] if len(sys.argv) > 2 else f"{settings.OUTPUT_DIR}/{symbol}_signals_DONLY.csv"

    Ns = [3, 5, 8, 10, 13]
    Rs = [0.7, 1.0, 1.5, 2.0]

    rows = []
    for n, r in itertools.product(Ns, Rs):
        trades, m = backtest(symbol, signals_csv, lookback_n=n, r_mult=r)
        rows.append(dict(N=n, R=r, **m))

    df = pd.DataFrame(rows).sort_values(["expectancyR", "avgR"], ascending=[False, False])

    out = f"{settings.OUTPUT_DIR}/{symbol}_sweep.csv"
    df.to_csv(out, index=False)

    print("N  R   trades  winrate   AvgR   ExpectR")
    for _, row in df.iterrows():
        print(f"{int(row.N):<2} {row.R:<3} {int(row.trades):<6}  {row.winrate:>7.2%}  {row.avgR:>6.3f}  {row.expectancyR:>7.3f}")
    print(f"OK sweep → {out}")

if __name__ == "__main__":
    main()
