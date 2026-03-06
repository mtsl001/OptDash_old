#!/usr/bin/env python3
"""
compact_intraday.py — One-time migration script.

Consolidates all UNDERLYING_HHMM.parquet files (old format) into a single
UNDERLYING.parquet file per day per underlying (new format).

Run ONCE after upgrading writer.py, while the API is stopped.
Safe to re-run: it skips underlyings that already have no _HHMM files.

Usage (with venv active):
    cd /Users/apple/Documents/OptDash
    source .venv/bin/activate
    python compact_intraday.py
"""
import sys
import glob
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("compact")

# ── Config ───────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))

from config import RAW_DIR, INDEX_UNDERLYINGS, PARQUET_COMPRESSION, PARQUET_ROW_GROUP_SIZE


def compact_directory(day_dir: Path, underlying: str) -> int:
    """
    Merge all UNDERLYING_HHMM.parquet files in day_dir into UNDERLYING.parquet.
    Returns the number of rows in the merged file (0 = nothing to do).
    """
    # Find incremental files: UNDERLYING_HHMM.parquet  (has underscore + 4 digits before .parquet)
    pattern        = str(day_dir / f"{underlying}_*.parquet")
    intraday_files = sorted(glob.glob(pattern))

    if not intraday_files:
        return 0  # nothing to compact

    logger.info(f"  {underlying}: merging {len(intraday_files)} incremental files...")

    frames = []
    for f in intraday_files:
        try:
            frames.append(pq.read_table(f).to_pandas())
        except Exception as exc:
            logger.warning(f"  Skipping {f}: {exc}")

    if not frames:
        return 0

    merged = pd.concat(frames, ignore_index=True)

    # Deduplicate — keep last (freshest) row per (snap_time, instrument_key)
    if "snap_time" in merged.columns and "instrument_key" in merged.columns:
        merged = (
            merged
            .sort_values("snap_time")
            .drop_duplicates(subset=["snap_time", "instrument_key"], keep="last")
            .reset_index(drop=True)
        )

    # Check for existing base file (from backfill — already clean)
    base_path = day_dir / f"{underlying}.parquet"
    if base_path.exists():
        try:
            existing = pq.read_table(str(base_path)).to_pandas()
            # Only union if the existing base file predates intraday (i.e., backfill data)
            # Identify by checking if it has ALL snaps already — if so, just delete intraday
            if "snap_time" in existing.columns:
                existing_snaps = set(existing["snap_time"].unique())
                new_snaps      = set(merged["snap_time"].unique())
                if new_snaps.issubset(existing_snaps):
                    # All new snaps already in existing → just purge _HHMM files
                    logger.info(f"  {underlying}: existing base covers all snaps — purging _HHMM files only")
                    for f in intraday_files:
                        Path(f).unlink()
                    return len(existing)
                # Otherwise merge
                merged = pd.concat([existing, merged], ignore_index=True)
                if "snap_time" in merged.columns and "instrument_key" in merged.columns:
                    merged = (
                        merged
                        .sort_values("snap_time")
                        .drop_duplicates(subset=["snap_time", "instrument_key"], keep="last")
                        .reset_index(drop=True)
                    )
        except Exception as exc:
            logger.warning(f"  Could not read existing {base_path.name}: {exc} — using intraday only")

    # Write merged file
    table = pa.Table.from_pandas(merged, preserve_index=False)
    pq.write_table(
        table,
        str(base_path),
        compression=PARQUET_COMPRESSION,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
        use_dictionary=True,
        write_statistics=True,
    )
    logger.info(f"  {underlying}: wrote {len(merged):,} rows → {base_path.name}")

    # Delete old _HHMM files
    for f in intraday_files:
        Path(f).unlink()
        logger.debug(f"  Deleted {Path(f).name}")

    return len(merged)


def compact_futures(day_dir: Path) -> int:
    """Merge ALL_FUTURES_HHMM.parquet files into ALL_FUTURES.parquet."""
    pattern        = str(day_dir / "ALL_FUTURES_*.parquet")
    intraday_files = sorted(glob.glob(pattern))

    if not intraday_files:
        return 0

    logger.info(f"  ALL_FUTURES: merging {len(intraday_files)} incremental files...")

    frames = []
    for f in intraday_files:
        try:
            frames.append(pq.read_table(f).to_pandas())
        except Exception as exc:
            logger.warning(f"  Skipping {f}: {exc}")

    if not frames:
        return 0

    merged = pd.concat(frames, ignore_index=True)
    if "snap_time" in merged.columns and "instrument_key" in merged.columns:
        merged = (
            merged
            .sort_values("snap_time")
            .drop_duplicates(subset=["snap_time", "instrument_key"], keep="last")
            .reset_index(drop=True)
        )

    base_path = day_dir / "ALL_FUTURES.parquet"
    if base_path.exists():
        try:
            existing = pq.read_table(str(base_path)).to_pandas()
            merged   = pd.concat([existing, merged], ignore_index=True)
            if "snap_time" in merged.columns and "instrument_key" in merged.columns:
                merged = (
                    merged
                    .sort_values("snap_time")
                    .drop_duplicates(subset=["snap_time", "instrument_key"], keep="last")
                    .reset_index(drop=True)
                )
        except Exception as exc:
            logger.warning(f"  Could not read existing ALL_FUTURES.parquet: {exc}")

    table = pa.Table.from_pandas(merged, preserve_index=False)
    pq.write_table(
        table,
        str(base_path),
        compression=PARQUET_COMPRESSION,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
        use_dictionary=True,
        write_statistics=True,
    )
    logger.info(f"  ALL_FUTURES: wrote {len(merged):,} rows → {base_path.name}")

    for f in intraday_files:
        Path(f).unlink()

    return len(merged)


def main() -> None:
    """Compact all trade_date directories that contain _HHMM files."""
    logger.info("=" * 60)
    logger.info("compact_intraday.py — Consolidating intraday Parquet files")
    logger.info("=" * 60)

    # Options
    opt_root = RAW_DIR / "options"
    if opt_root.exists():
        day_dirs = sorted(opt_root.iterdir())
        for day_dir in day_dirs:
            if not day_dir.is_dir():
                continue
            logger.info(f"\nOptions → {day_dir.name}")
            for underlying in INDEX_UNDERLYINGS:
                compact_directory(day_dir, underlying)
    else:
        logger.info("No options directory found — skipping")

    # Futures
    fut_root = RAW_DIR / "futures"
    if fut_root.exists():
        day_dirs = sorted(fut_root.iterdir())
        for day_dir in day_dirs:
            if not day_dir.is_dir():
                continue
            logger.info(f"\nFutures → {day_dir.name}")
            compact_futures(day_dir)
    else:
        logger.info("No futures directory found — skipping")

    logger.info("\n✓ Compaction complete. Restart run_api.py to apply.")


if __name__ == "__main__":
    main()
