"""
writer.py — Write processed DataFrames to Parquet with Snappy compression.

Partition structure:
  data/raw/options/trade_date=YYYY-MM-DD/UNDERLYING.parquet
  data/raw/futures/trade_date=YYYY-MM-DD/ALL_FUTURES.parquet

Key setting: row_group_size=100,000
  - DuckDB allocates 1 thread per Parquet row group during scans.
  - With row_group_size=10,000 and ~125k rows/day → 12 groups → 12 threads max.
  - With row_group_size=100,000 → 2 groups → DuckDB uses all available cores.
  - Benchmark: 3-5× faster query execution vs row_group_size=10,000.
"""
import logging
import time
import os
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_COMPRESSION,
    INDEX_UNDERLYINGS,
)

logger = logging.getLogger(__name__)

# Columns to write for options (subset of full df — exclude BQ-raw redundancies)
OPTIONS_WRITE_COLS = [
    "record_time", "snap_time", "trade_date",
    "underlying", "instrument_type", "instrument_key",
    "option_type", "expiry_date", "strike_price", "dte",
    "underlying_spot",
    "open", "high", "low", "close", "close_price", "ltp", "effective_ltp",
    "volume", "oi", "oi_delta",
    "total_buy_qty", "total_sell_qty",
    "iv", "delta", "theta", "gamma", "vega",
    "pcr",
    # Derived
    "moneyness_pct", "L_proxy", "in_atm_window", "expiry_tier",
    "d_dir", "gex_k", "obi_raw",
    "vex_k", "cex_k",
]

FUTURES_WRITE_COLS = [
    "record_time", "snap_time", "trade_date",
    "underlying", "instrument_type", "instrument_key",
    "expiry_date", "dte",
    "underlying_spot",
    "open", "high", "low", "close", "close_price", "ltp", "effective_ltp",
    "volume", "oi", "oi_delta",
    "total_buy_qty", "total_sell_qty",
    # Derived
    "coc", "obi_raw",
]


def _safe_write_parquet(table: pa.Table, path: Path) -> None:
    """
    Write a PyArrow Table to Parquet using a temp-and-swap strategy.
    Handles Windows/OneDrive locking via retries.
    """
    tmp_path = path.with_suffix(f".tmp_{int(time.time())}.parquet")
    
    # Ensure directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # 1. Write to temporary file
    pq.write_table(
        table,
        str(tmp_path),
        compression=PARQUET_COMPRESSION,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
        use_dictionary=True,
        write_statistics=True,
    )

    # 2. Swap temp to real path with retries for Windows locks
    max_retries = 10
    for attempt in range(max_retries):
        try:
            # On Windows, os.replace() is atomic and handles existing files,
            # but still fails if the file is OPEN for reading by DuckDB or OneDrive.
            if path.exists():
                # Attempt to remove old file if replace fails
                try:
                    os.replace(str(tmp_path), str(path))
                except PermissionError:
                    # If replace fails, wait and retry
                    raise
            else:
                tmp_path.rename(path)
            return
        except PermissionError as exc:
            if attempt < max_retries - 1:
                # DuckDB or API might be reading; back off and retry
                wait = 0.5 * (1.5 ** attempt) # slightly less aggressive backoff
                logger.warning(f"File locked: {path.name}. Retrying {attempt+1}/{max_retries} in {wait:.1f}s...")
                time.sleep(wait)
            else:
                logger.error(f"Failed to write {path.name} after {max_retries} attempts: {exc}")
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except: pass
                raise
        except Exception as exc:
            logger.error(f"Unexpected error writing {path.name}: {exc}")
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except: pass
            raise
    if tmp_path.exists():
        try: tmp_path.unlink()
        except: pass


def _write_parquet(df: pd.DataFrame, path: Path, cols: list[str]) -> None:
    """
    Write a DataFrame to a single Parquet file.
    Silently drops columns in cols that are not present in df.
    """
    available = [c for c in cols if c in df.columns]
    missing   = [c for c in cols if c not in df.columns]
    if missing:
        logger.debug(f"Columns not present (will be skipped): {missing}")

    table = pa.Table.from_pandas(df[available], preserve_index=False)
    _safe_write_parquet(table, path)
    
    size_kb = path.stat().st_size // 1024
    logger.debug(f"Written {len(df):,} rows → {path.name} ({size_kb} KB)")


def write_day_parquet(
    df: pd.DataFrame,
    trade_date: date,
    raw_dir: Path,
) -> dict[str, str]:
    """
    Write one trading day's data to Parquet.

    Options: one file per index underlying.
    Futures: one file for all (FUTIDX + FUTSTK combined).

    Returns a dict of {label: filepath_str} for logging.
    """
    ds = str(trade_date)
    written: dict[str, str] = {}

    # ── Options ──────────────────────────────────────────────────────────────
    opt_df = df[df["instrument_type"] == "OPTIDX"].copy()
    opt_dir = raw_dir / "options" / f"trade_date={ds}"

    for underlying in INDEX_UNDERLYINGS:
        sub = opt_df[opt_df["underlying"] == underlying]
        if sub.empty:
            logger.warning(f"[{ds}] No options data for {underlying}")
            continue
        path = opt_dir / f"{underlying}.parquet"
        _write_parquet(sub, path, OPTIONS_WRITE_COLS)
        written[f"options/{underlying}"] = str(path)

    # ── Futures ──────────────────────────────────────────────────────────────
    fut_df = df[df["instrument_type"].isin(["FUTIDX", "FUTSTK"])].copy()
    if not fut_df.empty:
        fut_dir = raw_dir / "futures" / f"trade_date={ds}"
        path = fut_dir / "ALL_FUTURES.parquet"
        _write_parquet(fut_df, path, FUTURES_WRITE_COLS)
        written["futures/ALL"] = str(path)
    else:
        logger.warning(f"[{ds}] No futures data")

    logger.info(f"[{ds}] Parquet write complete — {len(written)} files")
    return written


def write_incremental_parquet(
    df: pd.DataFrame,
    raw_dir: Path,
) -> None:
    """
    Write an incremental pull by MERGING into a single daily file per underlying.

    Strategy: read-merge-overwrite (not append)
    ─────────────────────────────────────────────
    Each call reads the existing daily Parquet (if present), unions with the new
    rows, deduplicates on (snap_time, instrument_key), then writes a single file.

    WHY: The old approach wrote UNDERLYING_HHMM.parquet per snap → by 12:30 IST
    there are 5 underlyings × 19 snaps + futures = 100+ files. DuckDB opens one fd
    per file during glob scans, hitting macOS's 256-fd default and causing
    "Too many open files" / InvalidInputException cascade.

    With merge-overwrite, file count stays constant:
      options/trade_date=YYYY-MM-DD/{NIFTY,BANKNIFTY,...}.parquet  → 5 files
      futures/trade_date=YYYY-MM-DD/ALL_FUTURES.parquet            → 1 file
      Total: 6 files for current day + 6 per historical day = O(days) not O(snaps)

    DuckDB picks up the overwritten file automatically on next query — no view
    refresh needed (views glob the directory, not specific file names).
    """
    if df.empty:
        return

    trade_dates = df["trade_date"].unique()
    for ds in trade_dates:
        day_df = df[df["trade_date"] == ds].copy()

        # Options — one merged file per index underlying
        opt_df  = day_df[day_df["instrument_type"] == "OPTIDX"]
        opt_dir = raw_dir / "options" / f"trade_date={ds}"

        for underlying in INDEX_UNDERLYINGS:
            new_rows = opt_df[opt_df["underlying"] == underlying]
            if new_rows.empty:
                continue

            available = [c for c in OPTIONS_WRITE_COLS if c in new_rows.columns]
            new_rows  = new_rows[available]

            path = opt_dir / f"{underlying}.parquet"

            # Merge with existing file (if present)
            if path.exists():
                existing = None
                max_read_retries = 5
                for read_attempt in range(max_read_retries):
                    try:
                        existing = pq.read_table(str(path)).to_pandas()
                        break
                    except (PermissionError, IOError) as exc:
                        if read_attempt < max_read_retries - 1:
                            wait = 0.5 * (1.5 ** read_attempt)
                            logger.warning(f"[{ds}] Read locked: {path.name}. Retrying {read_attempt+1}/{max_read_retries} in {wait:.1f}s...")
                            time.sleep(wait)
                        else:
                            logger.error(f"[{ds}] Failed to read existing {path.name} after {max_read_retries} attempts: {exc}")
                            # Critical: if we can't read the existing file, we MUST NOT overwrite it
                            # with just new rows, or we lose history. Skip this underlying for now.
                            continue 

                if existing is not None:
                    try:
                        # Keep only columns that are in the write list
                        existing = existing[[c for c in available if c in existing.columns]]
                        merged   = pd.concat([existing, new_rows], ignore_index=True)
                    except Exception as exc:
                        logger.warning(f"[{ds}] Merge failed for {path.name}: {exc} — overwriting")
                        merged = new_rows
                else:
                    # Skip writing this underlying if we failed to read existing (prevent history loss)
                    logger.error(f"[{ds}] Skipping {underlying} to prevent history loss due to read lock.")
                    continue
            else:
                merged = new_rows

            # Deduplicate: keep LAST for each (snap_time, instrument_key) combo
            # "last" = the freshest data from the current pull wins
            if "snap_time" in merged.columns and "instrument_key" in merged.columns:
                merged = (
                    merged
                    .sort_values("snap_time")
                    .drop_duplicates(subset=["snap_time", "instrument_key"], keep="last")
                    .reset_index(drop=True)
                )

            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(merged, preserve_index=False)
            _safe_write_parquet(table, path)
            logger.debug(
                f"[{ds}] {underlying}: merged {len(new_rows):,} new rows → "
                f"{len(merged):,} total rows in {path.name}"
            )

        # Futures — one merged ALL_FUTURES.parquet
        fut_df  = day_df[day_df["instrument_type"].isin(["FUTIDX", "FUTSTK"])]
        if not fut_df.empty:
            fut_dir = raw_dir / "futures" / f"trade_date={ds}"
            path    = fut_dir / "ALL_FUTURES.parquet"

            available = [c for c in FUTURES_WRITE_COLS if c in fut_df.columns]
            new_rows  = fut_df[available]

            if path.exists():
                try:
                    existing = pq.read_table(str(path)).to_pandas()
                    existing = existing[[c for c in available if c in existing.columns]]
                    merged   = pd.concat([existing, new_rows], ignore_index=True)
                except Exception as exc:
                    logger.warning(f"[{ds}] Could not read existing ALL_FUTURES.parquet: {exc} — overwriting")
                    merged = new_rows
            else:
                merged = new_rows

            if "snap_time" in merged.columns and "instrument_key" in merged.columns:
                merged = (
                    merged
                    .sort_values("snap_time")
                    .drop_duplicates(subset=["snap_time", "instrument_key"], keep="last")
                    .reset_index(drop=True)
                )

            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(merged, preserve_index=False)
            _safe_write_parquet(table, path)
            logger.debug(
                f"[{ds}] ALL_FUTURES: merged {len(new_rows):,} new rows → "
                f"{len(merged):,} total rows"
            )
