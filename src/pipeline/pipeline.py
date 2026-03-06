"""
pipeline.py — The 5-minute incremental pull cycle.

Called by scheduler.py every 5 minutes during market hours.

Cycle:
  1. Load watermark
  2. Pull incremental rows from BigQuery
  3. Validate
  4. Compute or load ATM windows for today
  5. Compute derived columns
  6. Write to Parquet
  7. Refresh DuckDB views
  8. Update watermark (only if write succeeded)
"""
import logging
from datetime import date

import pandas as pd
from google.cloud import bigquery

from config import (
    RAW_DIR, ATM_WINDOWS_DIR, WATERMARK_PATH
)
from bq_client import get_bq_client, pull_incremental
from validator import validate_dataframe
from atm import compute_atm_windows, save_atm_windows, load_atm_windows
from processor import compute_derived_columns
from writer import write_incremental_parquet
from duckdb_setup import safe_refresh_views
import watermark as wm

logger = logging.getLogger(__name__)

# Module-level BQ client (created once, reused)
_bq_client: bigquery.Client | None = None


def _get_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = get_bq_client()
    return _bq_client


def run_incremental_pull() -> None:
    """
    Execute one full incremental pull cycle.
    """
    client = _get_client()

    # Step 1: Load current watermark
    current_wm = wm.load(WATERMARK_PATH)
    logger.info(f"Watermark: {current_wm}")

    # Step 2: Pull incremental rows
    df = pull_incremental(client, current_wm)
    if df.empty:
        logger.debug("No new rows — snapshot not yet committed to BQ")
        return

    # Step 3: Validate
    df = validate_dataframe(df)

    # Step 4: ATM windows — load existing or compute from today's data
    today = date.today()
    atm_windows = load_atm_windows(today, ATM_WINDOWS_DIR)
    if not atm_windows:
        # First pull of the day — compute and save ATM windows
        logger.info(f"Computing ATM windows for {today}")
        atm_windows = compute_atm_windows(df, today)
        if atm_windows:
            save_atm_windows(atm_windows, today, ATM_WINDOWS_DIR)
        else:
            logger.warning("Could not compute ATM windows — using full strike universe")

    # Step 5: Derived columns
    df = compute_derived_columns(df, atm_windows)

    # Step 6: Write to Parquet
    write_incremental_parquet(df, RAW_DIR)

    # Step 7: Refresh DuckDB views
    safe_refresh_views()

    # Step 8: Update watermark — ONLY after successful write
    max_ts = df["record_time"].max()
    new_wm = wm.from_timestamp(max_ts)
    wm.save(WATERMARK_PATH, new_wm)

    # Summary log
    n_snaps = df["snap_time"].nunique()
    snaps   = sorted(df["snap_time"].unique())
    logger.info(
        f"Incremental complete: {len(df):,} rows, "
        f"{n_snaps} snapshot(s) [{', '.join(snaps)}], "
        f"watermark → {new_wm}"
    )
