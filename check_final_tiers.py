import duckdb
import pandas as pd
path = "data/raw/options/trade_date=2026-02-27/*.parquet"
df = duckdb.query(f"SELECT underlying, snap_time, expiry_tier, count(*) FROM read_parquet('{path}') WHERE snap_time >= '11:30' GROUP BY underlying, snap_time, expiry_tier ORDER BY snap_time, underlying").df()
print(df)
