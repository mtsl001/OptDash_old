import sys
from pathlib import Path
sys.path.insert(0, str(Path('src/pipeline')))
import duckdb
from config import DUCKDB_PATH

conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
sql = """
SELECT 
    expiry_date, 
    dte, 
    expiry_tier, 
    in_atm_window,
    COUNT(*) 
FROM vw_options 
WHERE trade_date = '2026-02-27' 
  AND underlying = 'NIFTY'
GROUP BY ALL
"""
print("--- NIFTY Contract Tiers Today ---")
for row in conn.execute(sql).fetchall():
    print(row)
conn.close()
