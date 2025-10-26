"""
candles.py — Modelo de candle e utilidades para serialização.

O QUE É UM CANDLE AQUI?
- Um objeto com: tempo de abertura, OHLC, volume e tempo de fechamento.
- Usamos `*_ms` (milissegundos desde epoch), pois a Binance devolve assim.

POR QUE EXISTE?
- Padronizar dados independente da fonte (API, CSV, etc).
- Facilitar salvar/ler em CSV e manipular no restante do projeto.
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Iterable, Dict, Any
from datetime import datetime, timezone

@dataclass
class Candle:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time_ms: int
    def to_dict(self) -> Dict[str, Any]:
        """Serializa para dict amigável (inclui timestamps ISO)."""
        d = asdict(self)
        d["open_time_iso"] = ms_to_iso(self.open_time_ms)
        d["close_time_iso"] = ms_to_iso(self.close_time_ms)
        return d

def ms_to_iso(ms: int) -> str:
    """Converte ms desde epoch para string ISO (UTC)."""

    return datetime.fromtimestamp(ms/1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

def from_binance_klines_row(row: list) -> Candle:
    """
    Converte uma linha do endpoint /api/v3/klines em Candle.
    Formato (parcial):
    [0 OpenTime, 1 Open, 2 High, 3 Low, 4 Close, 5 Volume, 6 CloseTime, ...]
    """
    return Candle(
        open_time_ms=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        close_time_ms=int(row[6]),
    )

def to_csv_rows(candles: Iterable[Candle]) -> List[Dict[str, Any]]:
    """Gera lista de dicionários (prontos para csv.DictWriter)."""
    return [c.to_dict() for c in candles]
