"""
ta.py — 'cola' indicadores nos candles (EMA dos fechamentos etc.).
"""
from __future__ import annotations
from typing import List, Dict
from src.core.candles import Candle
from src.core.indicators import ema

def ema_on_closes(candles: List[Candle], period: int) -> List[float]:
    """EMA(period) calculada nos fechamentos dos candles."""

    closes = [c.close for c in candles]
    return ema(closes, period)

def ema_map_by_close_time(candles: List[Candle], period: int) -> Dict[int, float]:
    """
    Mapa {close_time_ms: valor_da_ema}, útil para buscar "a EMA do candle superior
    que corresponde a este H1".
    """
    series = ema_on_closes(candles, period)
    return {c.close_time_ms: v for c, v in zip(candles, series)}
