import duckdb
import pandas as pd
path = "data/raw/options/trade_date=2026-02-27/BANKNIFTY.parquet"
df = duckdb.query(f"SELECT snap_time, count(*) FROM read_parquet('{path}') GROUP BY snap_time ORDER BY snap_time").df()
print(df)
