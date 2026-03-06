import duckdb
import pandas as pd
path = "data/raw/options/trade_date=2026-02-27/BANKNIFTY.parquet"
df = duckdb.query(f"SELECT DISTINCT expiry_date, dte, expiry_tier FROM read_parquet('{path}') ORDER BY dte").df()
print(df)
