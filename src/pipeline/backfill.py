"""
backfill.py — Historical data pull from BACKFILL_START_DATE to BACKFILL_END_DATE (fixed).

Strategy:
  - Pull one full day at a time from BigQuery (partition-pruned by DATE(record_time))
  - Validate → compute derived columns → compute ATM windows → write Parquet
  - Update watermark after each successful day
  - Skip days where Parquet files already exist (idempotent)
  - Continue on per-day errors (log + skip, do not abort entire backfill)
"""
import logging
from datetime import date, timedelta

import pandas as pd
from google.cloud import bigquery

from config import (
    BACKFILL_START_DATE,
    BACKFILL_END_DATE,
    RAW_DIR,
    ATM_WINDOWS_DIR,
    WATERMARK_PATH,
)
from market_calendar import get_trading_days
from bq_client import get_bq_client, pull_full_day
from validator import validate_dataframe
# These will be created in Part 3, but needed for the complete logic
from processor import compute_derived_columns
from atm import compute_atm_windows, save_atm_windows
from writer import write_day_parquet
import watermark as wm

logger = logging.getLogger(__name__)


def _day_complete(trade_date: date) -> bool:
    """
    Consider a day 'complete' if the NIFTY options Parquet file exists.
    Adjust this check if some days have no NIFTY data.
    """
    path = RAW_DIR / "options" / f"trade_date={trade_date}" / "NIFTY.parquet"
    return path.exists()


def run_backfill() -> None:
    """
    Pull all missing trading days from BACKFILL_START_DATE to BACKFILL_END_DATE (inclusive).

    After completion, the watermark is set to the last record_time of the
    most recent day pulled — ready for the live incremental scheduler.
    """
    start = date.fromisoformat(BACKFILL_START_DATE)
    end   = date.fromisoformat(BACKFILL_END_DATE)    # Fixed end — live pulls take over after this

    trading_days = get_trading_days(start, end)
    if not trading_days:
        logger.info("Backfill: no trading days in range — nothing to do")
        return

    logger.info(
        f"Backfill: {len(trading_days)} trading days "
        f"from {start} to {end}"
    )

    client = get_bq_client()
    last_successful_ts: str | None = None

    for trade_date in trading_days:
        ds = str(trade_date)

        if _day_complete(trade_date):
            logger.info(f"[{ds}] Already on disk — skipping")
            # Update watermark if it's the latest day
            # (Simplified: we assume if it's on disk, watermark should be updated or is already ahead)
            continue

        logger.info(f"[{ds}] Pulling from BigQuery…")
        try:
            df = pull_full_day(client, ds)

            if df.empty:
                logger.warning(f"[{ds}] 0 rows — market closed or no data in BQ")
                continue

            # Validate
            df = validate_dataframe(df)

            # ATM windows from first snapshot
            atm_windows = compute_atm_windows(df, trade_date)
            save_atm_windows(atm_windows, trade_date, ATM_WINDOWS_DIR)

            # Derived columns
            df = compute_derived_columns(df, atm_windows)

            # Write to Parquet
            written = write_day_parquet(df, trade_date, RAW_DIR)
            logger.info(f"[{ds}] Written: {written}")

            # Track watermark
            max_ts = df["record_time"].max()
            last_successful_ts = wm.from_timestamp(max_ts)
            wm.save(WATERMARK_PATH, last_successful_ts)
            logger.info(f"[{ds}] Watermark → {last_successful_ts}")

        except Exception as exc:
            logger.error(f"[{ds}] Backfill failed: {exc}", exc_info=True)
            # Continue with next day

    if last_successful_ts:
        logger.info(f"Backfill complete. Final watermark: {last_successful_ts}")
    else:
        logger.info("Backfill complete (all days already on disk or empty)")
