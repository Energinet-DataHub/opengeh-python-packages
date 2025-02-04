import shutil
from pathlib import Path

for p in Path(".").rglob("__pycache__"):
    shutil.rmtree(p)
