"""
duckdb_setup.py — DuckDB database initialisation and view management.

DuckDB reads Parquet files directly via CREATE OR REPLACE VIEW with glob patterns.
No ETL into DuckDB — it queries Parquet on disk.

Views created:
  vw_options  — all OPTIDX Parquet files across all dates and underlyings
  vw_futures  — all FUTIDX/FUTSTK Parquet files across all dates
  vw_atm      — vw_options filtered to in_atm_window=true (fast dashboard queries)
"""
import logging
from pathlib import Path

import duckdb

from config import DUCKDB_PATH, RAW_DIR

logger = logging.getLogger(__name__)


import time

def _get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. Creates the database file if it doesn't exist."""
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(str(DUCKDB_PATH), read_only=read_only)
            # Optimise for read-heavy analytical workloads
            conn.execute("PRAGMA threads=4")           # Use 4 CPU threads
            conn.execute("PRAGMA memory_limit='2GB'")  # Cap RAM usage
            conn.execute("SET enable_progress_bar=false")
            return conn
        except duckdb.IOException as exc:
            if attempt < max_retries - 1:
                wait = 0.5 * (2 ** attempt)
                logger.debug(f"DuckDB locked (attempt {attempt+1}/{max_retries}). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
    raise duckdb.IOException(f"Failed to connect to DuckDB after {max_retries} attempts.")


def initialize_duckdb() -> None:
    """
    Create or refresh all DuckDB views.
    Call this on startup and after every Parquet write to pick up new files.
    """
    import glob
    
    # Simple glob patterns for DuckDB
    options_glob  = (RAW_DIR / "options" / "**" / "*.parquet").as_posix()
    futures_glob  = (RAW_DIR / "futures" / "**" / "*.parquet").as_posix()

    # Check if ANY files exist before creating views. DuckDB read_parquet
    # throws an error if the glob matches zero files.
    options_files = glob.glob(options_glob)
    futures_files = glob.glob(futures_glob)

    if not options_files and not futures_files:
        logger.warning("No Parquet files found. DuckDB views will be created after first backfill.")
        return

    try:
        conn = _get_connection(read_only=False)
    except Exception as exc:
        logger.warning(f"initialize_duckdb() could not get write connection: {exc}. Trying read-only refresh...")
        try:
            conn = _get_connection(read_only=True)
        except:
            logger.error("DuckDB completely locked — skipping view refresh.")
            return

    try:
        # ── Options view ─────────────────────────────────────────────────────────
        if options_files:
            conn.execute(f"""
                CREATE OR REPLACE VIEW vw_options AS
                SELECT *
                FROM read_parquet(
                    '{options_glob}',
                    hive_partitioning = false,
                    union_by_name     = true
                )
            """)
            
            # ── ATM window view (pre-filtered for fast dashboard queries) ─────────────
            conn.execute("""
                CREATE OR REPLACE VIEW vw_atm AS
                SELECT * FROM vw_options
                WHERE in_atm_window = true
                  AND expiry_tier IN ('TIER1_NEAR', 'TIER1_FAR')
            """)

            # ── Daily IV summary view (used for IVR/IVP rolling computation) ─────────
            conn.execute("""
                CREATE OR REPLACE VIEW vw_daily_atm_iv AS
                SELECT
                    trade_date,
                    underlying,
                    AVG(iv)   AS atm_iv,
                    COUNT(*)  AS sample_size
                FROM vw_options
                WHERE snap_time    = '15:30'
                  AND in_atm_window = true
                  AND iv            > 0
                  AND iv IS NOT NULL
                GROUP BY trade_date, underlying
                HAVING COUNT(*) >= 2
            """)

        # ── Futures view ─────────────────────────────────────────────────────────
        if futures_files:
            conn.execute(f"""
                CREATE OR REPLACE VIEW vw_futures AS
                SELECT *
                FROM read_parquet(
                    '{futures_glob}',
                    hive_partitioning = false,
                    union_by_name     = true
                )
            """)

        logger.info("DuckDB views initialised successfully.")
    except Exception as exc:
        logger.error(f"Failed to initialise DuckDB views: {exc}")
    finally:
        conn.close()


def refresh_views() -> None:
    """
    Alias for initialize_duckdb(). Call after every incremental Parquet write.
    """
    initialize_duckdb()


def safe_refresh_views() -> None:
    """
    Non-fatal wrapper around refresh_views().

    DuckDB only allows ONE read-write connection at a time. If the API server
    (run_api.py) or another process holds a lock on the .duckdb file, this
    function logs a warning and returns instead of crashing.

    This is safe because DuckDB views use glob patterns over the Parquet
    directory — new Parquet files are auto-discovered at query time without
    an explicit view refresh.
    """
    try:
        initialize_duckdb()
    except Exception as exc:
        logger.warning(
            f"safe_refresh_views() skipped (DuckDB lock held by another process): {exc}. "
            f"Parquet files are on disk — queries will pick them up automatically."
        )


def get_conn() -> duckdb.DuckDBPyConnection:
    """
    Get a fresh DuckDB connection for use in a single request/query.
    Callers MUST close the connection after use.
    """
    return _get_connection(read_only=True)
