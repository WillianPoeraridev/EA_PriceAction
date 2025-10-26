# src/config/__init__.py
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Type, TypeVar, overload, cast as type_cast

# Carrega .env se python-dotenv estiver instalado (opcional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

T = TypeVar("T", str, int, float, bool)

@overload
def _get(name: str, default: str, typ: Type[str] = str) -> str: ...
@overload
def _get(name: str, default: int, typ: Type[int]) -> int: ...
@overload
def _get(name: str, default: float, typ: Type[float]) -> float: ...
@overload
def _get(name: str, default: bool, typ: Type[bool]) -> bool: ...

def _get(name: str, default: T, typ: Type[T] = str) -> T:
    """
    Lê variável de ambiente e converte para o tipo indicado. Em erro, retorna `default`.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        if typ is bool:
            return type_cast(T, raw.lower() in ("1", "true", "yes", "on"))
        if typ is int:
            return type_cast(T, int(raw))
        if typ is float:
            return type_cast(T, float(raw))
        return type_cast(T, str(raw))
    except Exception:
        return default

@dataclass(frozen=True)
class Settings:
    # App
    DEFAULT_SYMBOL: str    = _get("DEFAULT_SYMBOL", "BTCUSDT", str)
    OUTPUT_DIR: str        = _get("OUTPUT_DIR", "data", str)
    TIMEZONE: str          = _get("TIMEZONE", "UTC", str)
    HTTP_TIMEOUT_SEC: int  = _get("HTTP_TIMEOUT_SEC", 30, int)
    USER_AGENT: str        = _get("HTTP_USER_AGENT", "ea-pa-v1/0.2", str)

    # Trading
    TRADING_MODE: str      = _get("TRADING_MODE", "paper", str)
    FEES_BPS: float        = _get("FEES_BPS", 10.0, float)
    SLIPPAGE_BPS: float    = _get("SLIPPAGE_BPS", 0.0, float)

    # Chaves (futuro)
    BINANCE_API_KEY: str   = _get("BINANCE_API_KEY", "", str)
    BINANCE_API_SECRET: str= _get("BINANCE_API_SECRET", "", str)

settings = Settings()
