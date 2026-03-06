import sys
from pathlib import Path
sys.path.insert(0, str(Path('src/pipeline')))
import duckdb
from config import DUCKDB_PATH

conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
# Check BANKNIFTY rows for today
sql = """
SELECT 
    COUNT(*) as total_rows,
    COUNT(gex_k) as non_null_gex,
    COUNT(gamma) as non_null_gamma,
    AVG(underlying_spot) as avg_spot,
    MAX(snap_time) as max_snap
FROM vw_options 
WHERE trade_date = '2026-02-27' 
  AND underlying = 'BANKNIFTY'
"""
print("--- BANKNIFTY Today Diagnostic ---")
res = conn.execute(sql).fetchone()
print(f"Total Rows:     {res[0]}")
print(f"Non-Null GEX:   {res[1]}")
print(f"Non-Null Gamma: {res[2]}")
print(f"Avg Spot:       {res[3]}")
print(f"Max Snap:       {res[4]}")

# Check ATM Window
sql_atm = "SELECT COUNT(*) FROM vw_atm WHERE trade_date = '2026-02-27' AND underlying = 'BANKNIFTY'"
print(f"ATM Rows:       {conn.execute(sql_atm).fetchone()[0]}")

conn.close()
