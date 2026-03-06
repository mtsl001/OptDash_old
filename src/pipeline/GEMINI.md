# Pipeline Sub-Context — src/pipeline/

> This file is loaded automatically when Gemini CLI scans subdirectories from the project root. It supplements (does not replace) the root GEMINI.md.

## Module Build Order (Dependency Chain)
Build files in this exact order — later modules import from earlier ones:
```
1.  config.py        ← no internal imports
2.  logger.py        ← imports: config
3.  calendar.py      ← imports: config
4.  watermark.py     ← imports: config
5.  bq_client.py     ← imports: config, tenacity
6.  atm.py           ← imports: config
7.  validator.py     ← imports: numpy, pandas
8.  processor.py     ← imports: config, atm, numpy, pandas
9.  writer.py        ← imports: config, pyarrow
10. duckdb_setup.py  ← imports: config, duckdb
11. analytics.py     ← imports: config, duckdb
12. backfill.py      ← imports: config, calendar, bq_client, validator, atm, processor, writer, watermark
13. pipeline.py      ← imports: config, bq_client, validator, atm, processor, writer, duckdb_setup, watermark
14. scheduler.py     ← imports: calendar, pipeline
```

## Common Mistakes to Avoid

**Timezone (most common bug):**
- **DO NOT** call `.dt.tz_convert()` — there is no timezone to convert
- **DO NOT** use `pd.to_datetime(..., utc=True)` — this shifts times by 5:30
- **DO** use `pd.to_datetime(df["record_time"]).dt.tz_localize(None)` to strip the UTC label without any time shift
- Values like "2026-02-20 09:15:00 UTC" should become `Timestamp('2026-02-20 09:15:00')` — same numbers, no shift

**DuckDB on macOS:**
- Glob paths use forward slashes natively — no `.replace("\\", "/")` needed
- Always use `union_by_name=True` in `read_parquet()` to handle schema evolution across files
- Filter `trade_date` first in every query — enables file-level pruning

**LTP null handling:**
- In Python/pandas: `df["ltp"].combine_first(df["close_price"]).combine_first(df["close"])`
- In DuckDB SQL: `COALESCE(ltp, close_price, close)`
- Column `close` and `close_price` are two separate columns — both exist in the schema

**GEX computation:**
- `gex_k` (per-row) is computed in `processor.py` — formula: `gamma × oi × spot² × 0.01 × d_dir`
- Net GEX aggregation (per snapshot) is done in `analytics.py` via DuckDB SQL
- Never aggregate GEX in processor.py

**Watermark format:**
- String: `"2026-02-19 15:30:00"` — no timezone label, no 'Z', no '+05:30'
- BigQuery WHERE clause: `WHERE record_time > TIMESTAMP("2026-02-19 15:30:00")`
- File location: `data/watermark.json`
- Save atomically on macOS: write to `.tmp` then `Path.replace()` (atomic on APFS/HFS+)

**Backfill end date (CRITICAL):**
- `backfill.py` pulls from `BACKFILL_START_DATE` to `BACKFILL_END_DATE` — both constants in `config.py`
- `BACKFILL_END_DATE = "2026-02-20"` — **fixed string**, never compute from today's date
- **Never write:** `end_date = date.today() - timedelta(days=1)` — this is wrong
- **Always write:** `end_date = date.fromisoformat(BACKFILL_END_DATE)`
- After backfill finishes, watermark = last record_time of 2026-02-20 (~15:30 IST)
- Live 5-min scheduler takes over from that watermark forward — no overlap, no gap

## config.py — macOS Paths
```python
BASE_DIR = Path("/Users/apple/Documents/OptDash")
CREDENTIALS_PATH = BASE_DIR / "universal-ion-437606-b7-5629bd4bc421.json"
```

## Holiday Calendar (from TradingHrs.txt)
**NSE TRADING HOLIDAYS 2026** (F&O closed — do NOT pull on these dates):
Jan 15, Jan 26, Mar 3, Mar 26, Mar 31, Apr 3, Apr 14, May 1, May 28, Jun 26, Sep 14, Oct 2, Oct 20, Nov 8, Nov 10, Nov 24, Dec 25

**Settlement holidays** (NSE open for trading — pull normally):
Feb 19, Mar 19, Apr 1, Aug 26

Weekends: `date.weekday() >= 5` → Saturday=5, Sunday=6 → always closed.

## Shell Commands (macOS zsh)
```bash
# Create venv
python3.11 -m venv .venv

# Activate
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run pipeline
python run_pipeline.py

# Run API
python run_api.py
```
