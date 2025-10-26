# src/core/
**Propósito:** “coração” do domínio, sem dependência de serviços externos.  
- `candles.py` – modelo Candle (OHLCV) + utilidades  
- `indicators.py` – SMA/EMA puras sobre listas  
- `mtf.py` – alinhamento multi-timeframe (W/D/H4 referenciados pelo H1)  
- `ta.py` – aplica indicadores aos candles (EMA dos closes, mapas)
