"""
binance.py — Downloader SPOT (público) para candles (klines) da Binance.

O QUE ELE FAZ?
- Faz uma requisição HTTP para o endpoint público `/api/v3/klines` (sem API key)
- Converte cada linha retornada (kline) em nosso objeto `Candle` (src.core.candles.Candle)

POR QUE EXISTE?
- Para centralizar a lógica de acesso a dados de mercado. Trocar de corretora no futuro
  fica mais fácil quando a coleta está isolada aqui.

COMO USAR?
    from src.data.binance import fetch_klines
    candles_h1 = fetch_klines("BTCUSDT", "1h", 300)

RETORNO:
- Lista de `Candle` com: open_time_ms, open, high, low, close, volume, close_time_ms.
"""

from __future__ import annotations
from typing import List
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json
from src.core.candles import Candle, from_binance_klines_row
BASE = "https://api.binance.com"
def _get(path: str, params: dict) -> list:
    """
    Requisição HTTP simples (GET) que retorna o JSON como lista Python.
    - path: ex. "/api/v3/klines"
    - params: dicionário com query string (ex.: {"symbol":"BTCUSDT","interval":"1h"})
    """
    qs = urlencode(params)
    url = f"{BASE}{path}?{qs}"
    req = Request(url, headers={"User-Agent":"ea-pa-v1/0.2"})
    with urlopen(req, timeout=30) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))
def fetch_klines(symbol: str, interval: str, limit: int = 500) -> List[Candle]:
    """
    Baixa os últimos `limit` candles para `symbol` e `interval`.
    Exemplos de interval: 1h, 4h, 1d, 1w (ver docs Binance para a lista completa).
    """
    raw = _get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    return [from_binance_klines_row(r) for r in raw]
