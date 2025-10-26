# data/
**Propósito:** guardar CSVs gerados pelos scripts (histórico baixado e MTF mesclado).  
**Contém:** `*_1h.csv`, `*_4h.csv`, `*_1d.csv`, `*_1w.csv`, `*_mtf_with_ema.csv`.  
**Não colocar:** chaves/segredos, dumps gigantes versionados.  
**Como gerar:** `python -m src.app.run_mtf_with_ema BTCUSDT 300`  
**Notas:** arquivos `.csv` são ignorados no git via `.gitignore` (mantém só este README).
