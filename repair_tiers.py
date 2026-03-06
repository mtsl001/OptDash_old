import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
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
    if not path.exists():
        return
    
    logger.info(f"Repairing {path}...")
    df = pq.read_table(str(path)).to_pandas()
    
    if "dte" not in df.columns or "expiry_tier" not in df.columns:
        logger.warning(f"Skipping {path.name}: missing dte or expiry_tier columns")
        return

    old_tiers = df["expiry_tier"].copy()
    df["expiry_tier"] = df["dte"].apply(_tier)
    
    changes = (old_tiers != df["expiry_tier"]).sum()
    if changes > 0:
        logger.info(f"Fixed {changes} rows in {path.name}")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(path), compression="snappy", row_group_size=100_000)
    else:
        logger.info(f"No changes needed for {path.name}")

def main():
    # Repair options
    opt_dir = RAW_DIR / "options" / f"trade_date={TRADE_DATE}"
    if opt_dir.exists():
        for p in opt_dir.glob("*.parquet"):
            repair_file(p)
            
    # Repair futures (though usually not filtered by tier, good for consistency)
    fut_dir = RAW_DIR / "futures" / f"trade_date={TRADE_DATE}"
    if fut_dir.exists():
        for p in fut_dir.glob("*.parquet"):
            repair_file(p)

if __name__ == "__main__":
    main()
