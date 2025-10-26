"""
mtf.py — Alinhamento Multi-Timeframe (W/D/H4 referenciados pelo H1).

IDEIA:
- A linha do tempo principal é o H1.
- Para cada H1, pegamos o último H4/D/W já fechado (close_time <= close do H1).
- Evita usar “info do futuro” (backtest honesto e execução correta).
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional
from bisect import bisect_right
from src.core.candles import Candle

def _index_by_close_time(candles: List[Candle]) -> List[int]:
    return [c.close_time_ms for c in candles]

def _latest_at_or_before(t: int, candles: List[Candle], closes: List[int]) -> Optional[Candle]:
    if not candles:
        return None
    i = bisect_right(closes, t) - 1
    if i >= 0:
        return candles[i]
    return None

def align_on_h1(h1: List[Candle], h4: List[Candle], d: List[Candle], w: List[Candle]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    idx_h4 = _index_by_close_time(h4)
    idx_d  = _index_by_close_time(d)
    idx_w  = _index_by_close_time(w)
    for c1 in h1:
        t = c1.close_time_ms
        c4 = _latest_at_or_before(t, h4, idx_h4)
        cd = _latest_at_or_before(t, d, idx_d)
        cw = _latest_at_or_before(t, w, idx_w)
        out.append({"t_close_h1": t, "h1": c1, "h4": c4, "d": cd, "w": cw})
    return out
