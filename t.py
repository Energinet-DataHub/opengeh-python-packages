from pathlib import Path
import shutil

for p in Path(".").rglob("__pycache__"):
    shutil.rmtree(p)
