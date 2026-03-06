import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
TRADE_DATE = "2026-02-27"

# Constants from current processor.py
TIER1_NEAR_MAX_DTE = 15
TIER1_FAR_MIN_DTE  = 16

def _tier(dte_val):
    if pd.isna(dte_val):
        return "UNKNOWN"
    if dte_val <= TIER1_NEAR_MAX_DTE:
        return "TIER1_NEAR"
    if dte_val >= TIER1_FAR_MIN_DTE:
        return "TIER1_FAR"
    return "TIER2_IV_ONLY"

def repair_file(path: Path):
    try:
        df = pq.read_table(str(path)).to_pandas()
        
        if "dte" not in df.columns or "expiry_tier" not in df.columns:
            return

        old_tiers = df["expiry_tier"].copy()
        df["expiry_tier"] = df["dte"].apply(_tier)
        
        changes = (old_tiers != df["expiry_tier"]).sum()
        if changes > 0:
            logger.info(f"Fixed {changes} rows in {path.name}")
            table = pa.Table.from_pandas(df, preserve_index=False)
            # Use a temporary file to avoid corruption if interrupted
            tmp_path = path.with_suffix(".repair_tmp")
            pq.write_table(table, str(tmp_path), compression="snappy", row_group_size=100_000)
            tmp_path.replace(path)
    except Exception as e:
        logger.error(f"Error repairing {path.name}: {e}")

def main():
    logger.info(f"Starting repair loop for {TRADE_DATE}...")
    while True:
        # Repair options
        opt_dir = RAW_DIR / "options" / f"trade_date={TRADE_DATE}"
        if opt_dir.exists():
            for p in opt_dir.glob("*.parquet"):
                repair_file(p)
                
        # Repair futures
        fut_dir = RAW_DIR / "futures" / f"trade_date={TRADE_DATE}"
        if fut_dir.exists():
            for p in fut_dir.glob("*.parquet"):
                repair_file(p)
        
        # Sleep for 30 seconds before next check
        time.sleep(30)

if __name__ == "__main__":
    main()
