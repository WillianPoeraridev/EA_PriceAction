# Rode o -> python tools/make_review_zip.py

import zipfile, glob, os
patterns = [
    "src/core/*.py",
    "src/app/run_mtf_with_ema.py",
    "src/app/run_annotate_basics.py",
    "src/data/binance.py",
    "src/config/__init__.py",
    "rules/specs/*.yaml",
    "docs/brooks/Book1_Trends/Ch01.md",
    "README.md",
]
with zipfile.ZipFile("review_pack.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for pat in patterns:
        for p in glob.glob(pat, recursive=True):
            if os.path.isfile(p):
                z.write(p, p)
print("OK: review_pack.zip")
