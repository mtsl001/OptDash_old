import sys
from pathlib import Path
sys.path.insert(0, str(Path('src/pipeline')))
import sys
from pathlib import Path
sys.path.insert(0, str(Path('src/pipeline')))
import duckdb
from config import DUCKDB_PATH

conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
sql = """
SELECT 
    underlying, 
    COUNT(*) as rows, 
    MAX(snap_time) as latest_snap 
FROM vw_options 
WHERE trade_date = '2026-02-27' 
GROUP BY underlying
"""
print(f"{'Underlying':<15} | {'Rows':<6} | {'Latest Snap'}")
print("-" * 40)
for row in conn.execute(sql).fetchall():
    print(f"{row[0]:<15} | {row[1]:<6} | {row[2]}")
conn.close()
