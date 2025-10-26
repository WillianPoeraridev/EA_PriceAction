# src/core/indicators.py
"""
Indicadores básicos (SMA/EMA) — implementação simples e didática.
"""
from __future__ import annotations
from typing import Iterable, List
import math

def _as_list(values: Iterable[float]) -> List[float]:
    """Converte values em lista de floats, garantindo cópia independente."""
    return [float(v) for v in values]

def sma(values: Iterable[float], period: int) -> List[float]:
    if period <= 0:
        raise ValueError("period deve ser > 0")
    xs = _as_list(values)
    n = len(xs)
    out = [math.nan] * n
    if n == 0:
        return out
    window_sum = 0.0
    for i, x in enumerate(xs):
        window_sum += x
        if i >= period:
            window_sum -= xs[i - period]
        if i >= period - 1:
            out[i] = window_sum / period
    return out

def ema(values: Iterable[float], period: int) -> List[float]:
    if period <= 0:
        raise ValueError("period deve ser > 0")
    xs = _as_list(values)
    n = len(xs)
    out = [math.nan] * n
    if n == 0:
        return out
    alpha = 2.0 / (period + 1.0)
    # Inicializa a EMA com a primeira SMA disponível
    if n >= period:
        first_sma = sum(xs[:period]) / period
        out[period - 1] = first_sma
        ema_prev = first_sma
        for i in range(period, n):
            ema_now = alpha * xs[i] + (1 - alpha) * ema_prev
            out[i] = ema_now
            ema_prev = ema_now
    return out
