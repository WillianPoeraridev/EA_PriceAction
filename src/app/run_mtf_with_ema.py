"""
run_mtf_with_ema.py — Baixa SPOT, calcula EMA20/EMA50 por TF, alinha tudo pelo H1 e salva CSV.

Como executar:
    python -m src.app.run_mtf_with_ema BTCUSDT 300
"""
from __future__ import annotations
import csv, sys, os
from typing import List, Dict
from src.data.binance import fetch_klines
from src.core.candles import to_csv_rows
from src.core.mtf import align_on_h1
from src.core.ta import ema_map_by_close_time, ema_on_closes
from src.config import settings

def _write_csv(path: str, rows: List[Dict]):
    """Salva lista de dict em CSV (cria a pasta se necessário)."""

    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows: return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def main():
     # 1) Argumentos
    symbol = sys.argv[1] if len(sys.argv) > 1 else settings.DEFAULT_SYMBOL
    limit_h1 = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    OUT = settings.OUTPUT_DIR


    # 2) Baixar SPOT
    print(f"[SPOT] Baixando {limit_h1} candles H1 de {symbol}...")
    h1 = fetch_klines(symbol, "1h", limit_h1)
    print("[SPOT] Baixando H4/D/W...")
    h4 = fetch_klines(symbol, "4h", max(100, limit_h1//4 + 10))
    d  = fetch_klines(symbol, "1d", max(60,  limit_h1//24 + 10))
    w  = fetch_klines(symbol, "1w", max(30,  limit_h1//168 + 10))

    # 3) Salvar CSVs “crus” (opcional; serve para auditoria)
    _write_csv(f"{OUT}/{symbol}_1h.csv", to_csv_rows(h1))
    _write_csv(f"{OUT}/{symbol}_4h.csv", to_csv_rows(h4))
    _write_csv(f"{OUT}/{symbol}_1d.csv", to_csv_rows(d))
    _write_csv(f"{OUT}/{symbol}_1w.csv", to_csv_rows(w))

    # 4) EMAs por timeframe
    h1_e20 = ema_on_closes(h1, 20); h1_e50 = ema_on_closes(h1, 50)
    h4_m20 = ema_map_by_close_time(h4, 20); h4_m50 = ema_map_by_close_time(h4, 50)
    d_m20  = ema_map_by_close_time(d, 20);  d_m50  = ema_map_by_close_time(d, 50)
    w_m20  = ema_map_by_close_time(w, 20);  w_m50  = ema_map_by_close_time(w, 50)

    # 5) Alinhamento MTF
    merged = align_on_h1(h1, h4, d, w)

    # 6) Construir linhas do CSV mesclado
    rows = []
    for i, m in enumerate(merged):
        c1, c4, cd, cw = m["h1"], m["h4"], m["d"], m["w"]
        rows.append({
            "t_close_h1": c1.close_time_ms if c1 else None,
            "h1_close": c1.close if c1 else None,
            "h4_close": c4.close if c4 else None,
            "d_close":  cd.close if cd else None,
            "w_close":  cw.close if cw else None,
            "h1_ema20": h1_e20[i] if i < len(h1_e20) else None,
            "h1_ema50": h1_e50[i] if i < len(h1_e50) else None,
            "h4_ema20": (h4_m20.get(c4.close_time_ms) if c4 else None),
            "h4_ema50": (h4_m50.get(c4.close_time_ms) if c4 else None),
            "d_ema20":  (d_m20.get(cd.close_time_ms) if cd else None),
            "d_ema50":  (d_m50.get(cd.close_time_ms) if cd else None),
            "w_ema20":  (w_m20.get(cw.close_time_ms) if cw else None),
            "w_ema50":  (w_m50.get(cw.close_time_ms) if cw else None),
        })

    # 7) Salvar mesclado
    out = f"{OUT}/{symbol}_mtf_with_ema.csv"
    _write_csv(out, rows)
    print(f"Pronto! Escrevi: {out}")
    print("Exemplo (últimas 3 linhas):")
    for r in rows[-3:]:
        print(r)

if __name__ == "__main__":
    main()
