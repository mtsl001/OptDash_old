import sys
from pathlib import Path

# Ensure src/pipeline is in path to import config
sys.path.insert(0, str(Path("src/pipeline")))
from config import (
    DATA_DIR, RAW_DIR, PROCESSED_DIR, DB_DIR,
    LOG_DIR, ATM_WINDOWS_DIR
)

dirs = [
    DATA_DIR,
    RAW_DIR / "options",
    RAW_DIR / "futures",
    PROCESSED_DIR,
    ATM_WINDOWS_DIR,
    DB_DIR,
    LOG_DIR,
]

for d in dirs:
    d.mkdir(parents=True, exist_ok=True)
    print(f"  ✓  {d}")

print("\nAll directories created.")
