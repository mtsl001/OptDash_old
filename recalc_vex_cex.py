import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import sys
import logging

logging.basicConfig(level=logging.INFO)
sys.path.insert(0, "src/pipeline")
from processor import _bs_d1, _bs_vanna, _bs_charm

RAW_DIR = Path("data/raw/options/trade_date=2026-02-27")

for p in RAW_DIR.glob("*.parquet"):
    logging.info(f"Recalculating {p.name}")
    df = pq.read_table(str(p)).to_pandas()
    
    opt_mask = df["instrument_type"] == "OPTIDX"
    
    vex_valid = (
        opt_mask
        & df["iv"].notna()
        & df["dte"].notna()
        & (df["dte"].fillna(0) >= 0)
        & df["underlying_spot"].notna()
        & (df["underlying_spot"].fillna(0) > 0)
        & df["strike_price"].notna()
        & (df["strike_price"].fillna(0) > 0)
        & df["oi"].notna()
        & (df["iv"].fillna(0) > 0)
    )
    
    if vex_valid.any():
        S     = df.loc[vex_valid, "underlying_spot"].astype(float).values
        K     = df.loc[vex_valid, "strike_price"].astype(float).values
        T     = np.maximum(df.loc[vex_valid, "dte"].astype(float).values, 1.0) / 365.0
        r     = 0.065
        sigma = df.loc[vex_valid, "iv"].astype(float).values / 100.0
        oi    = df.loc[vex_valid, "oi"].astype(float).values

        d1 = _bs_d1(S, K, T, r, sigma)
        d2 = d1 - sigma * np.sqrt(T)

        vanna       = _bs_vanna(S, K, T, sigma, d1, d2)
        charm_daily = _bs_charm(T, r, sigma, d1, d2)

        vex_k_raw = -oi * vanna * S * sigma / 1e6
        cex_k_raw = oi * charm_daily / 1e6

        df.loc[vex_valid, "vex_k"] = np.round(vex_k_raw, 4)
        df.loc[vex_valid, "cex_k"] = np.round(cex_k_raw, 4)
        
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(p), compression="snappy", row_group_size=100_000)
    logging.info(f"Done {p.name}")
