#!/usr/bin/env python3
"""
run_pipeline.py — Option Buying Dashboard Pipeline Entry Point

Startup decision tree (runs every time, handles ALL gap scenarios):

  1. Ensure directories exist
  2. Initialise DuckDB views
  3. Run fixed historical backfill (2026-02-17→2026-02-20, idempotent)
  4. Startup gap recovery — covers ALL restart scenarios:

     ┌─────────────────────────────────────────────────────────────────────┐
     │ SCENARIO                          │ DETECTION         │ ACTION      │
     ├───────────────────────────────────┼───────────────────┼─────────────┤
     │ A. Normal start, no gaps          │ wm == ~now        │ Skip fill   │
     │ B. Mid-session restart            │ wm < now < 15:30  │ Fill gap    │
     │   e.g. ran 09:16→11:10, now 14:20 │ (same day)        │ up to now   │
     │ C. After-hours restart            │ wm < 15:30 today  │ Fill to EOD │
     │   e.g. ran 09:16→11:10, now 17:00 │ after market      │             │
     │ D. Missed entire day(s)           │ wm.date < today   │ Pull each   │
     │   e.g. holiday, forgot to start   │ no parquet files  │ missed day  │
     │ E. Prior-day partial              │ wm.date < today   │ Fill rest   │
     │   e.g. crashed at 13:00 yesterday │ wm_time < 15:30   │ of that day │
     └─────────────────────────────────────────────────────────────────────┘

  5. If market is currently open: run one immediate catch-up pull, then start
     the 5-minute APScheduler loop.
  6. If market is closed (after 15:30 or holiday): log and exit cleanly.
     Re-run tomorrow — the gap fill on next startup will recover today's data.
"""
import sys
import logging
from pathlib import Path
from datetime import date, datetime, time

# ── Resolve project structure ─────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
PIPELINE_SRC = PROJECT_ROOT / "src" / "pipeline"
sys.path.insert(0, str(PIPELINE_SRC))

# ── Load .env before importing config (config reads env vars at import time) ──
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# ── Pipeline imports ──────────────────────────────────────────────────────────
from config import DATA_DIR, RAW_DIR, PROCESSED_DIR, DB_DIR, LOG_DIR, ATM_WINDOWS_DIR
from logger import setup_logging
from duckdb_setup import initialize_duckdb, safe_refresh_views
from backfill import run_backfill
from gap_fill import run_gap_fill, gap_fill_status
from pipeline import run_incremental_pull
from scheduler import start_live_scheduler
from market_calendar import is_trading_day, is_within_market_hours

# ── Constants ─────────────────────────────────────────────────────────────────
MARKET_OPEN  = time(9, 15, 0)
MARKET_CLOSE = time(15, 30, 0)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def ensure_directories() -> None:
    """Create all required data directories if they don't exist."""
    dirs = [
        DATA_DIR,
        RAW_DIR / "options",
        RAW_DIR / "futures",
        PROCESSED_DIR,
        ATM_WINDOWS_DIR,
        DB_DIR,
        LOG_DIR,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def _log_startup_banner(log: logging.Logger) -> None:
    now = datetime.now()
    today = now.date()
    in_market = MARKET_OPEN <= now.time() <= MARKET_CLOSE
    after_mkt = now.time() > MARKET_CLOSE
    trading   = is_trading_day(today)

    log.info("=" * 65)
    log.info("  Option Buying Dashboard — Pipeline v2.0")
    log.info(f"  Start: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
    log.info(f"  Today: {today} | Trading day: {trading}")
    if trading:
        if in_market:
            log.info("  Market: OPEN  ← live scheduler will start")
        elif after_mkt:
            log.info("  Market: CLOSED (post 15:30)  ← gap fill only, then exit")
        else:
            log.info("  Market: PRE-OPEN  ← scheduler will wait for 09:16:30")
    else:
        log.info("  Market: CLOSED (weekend/holiday)  ← gap fill only, then exit")
    log.info("=" * 65)


def _describe_gaps(log: logging.Logger, status: dict) -> None:
    """Log a clear summary of detected gaps."""
    if status["gaps_found"] == 0:
        log.info(f"  Gap check: CLEAN  (watermark={status['watermark']})")
        return
    log.info(f"  Gap check: {status['gaps_found']} gap(s) detected:")
    for g in status["gaps"]:
        log.info(
            f"    • {g['date']}  from {g['from']}  "
            f"→ ~{g['gap_minutes']} min missing"
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    # ── Step 1: Logging ───────────────────────────────────────────────────────
    setup_logging(LOG_DIR)
    log = logging.getLogger("run_pipeline")
    _log_startup_banner(log)

    # ── Step 2: Directories ───────────────────────────────────────────────────
    ensure_directories()
    log.info("[init] Directories ready")

    # ── Step 3: DuckDB ────────────────────────────────────────────────────────
    try:
        initialize_duckdb()
        log.info("[init] DuckDB views initialised")
    except Exception as exc:
        log.warning(
            f"[init] DuckDB views could not be refreshed (lock held?): {exc}. "
            f"Continuing — views from prior run are still valid."
        )

    # ── Step 4: Fixed historical backfill (2026-02-17 → 2026-02-20) ──────────
    # Idempotent — skips days where Parquet files already exist.
    log.info("[backfill] Checking historical backfill (2026-02-17 → 2026-02-20)…")
    try:
        run_backfill()
        log.info("[backfill] Complete")
    except Exception as exc:
        log.error(f"[backfill] FAILED: {exc}", exc_info=True)
        log.warning("[backfill] Continuing despite backfill error — live data unaffected")

    # ── Step 5: Startup gap recovery ──────────────────────────────────────────
    # Handles ALL restart scenarios (see module docstring decision table).
    log.info("[gap_fill] Scanning for missed data windows…")
    try:
        status = gap_fill_status()
        _describe_gaps(log, status)

        if status["gaps_found"] > 0:
            log.info("[gap_fill] Starting recovery pulls…")
            run_gap_fill()
            # Re-initialise DuckDB views to pick up newly written Parquet files
            safe_refresh_views()
            log.info("[gap_fill] Recovery complete")
        else:
            log.info("[gap_fill] No action needed")

    except Exception as exc:
        log.error(f"[gap_fill] FAILED: {exc}", exc_info=True)
        log.warning("[gap_fill] Continuing — live scheduler may start with gaps in data")

    # ── Step 6: Determine whether to start live scheduler ───────────────────
    now   = datetime.now()
    today = now.date()

    if not is_trading_day(today):
        log.info("[scheduler] Today is not a trading day — pipeline done. Re-run on next trading day.")
        return

    if now.time() > MARKET_CLOSE:
        log.info(
            "[scheduler] Market is closed (past 15:30). "
            "Gap fill has recovered all available data. "
            "Pipeline done — re-run tomorrow morning."
        )
        return

    # ── Step 7: Immediate catch-up pull (if mid-session) ─────────────────────
    # If we're mid-session and the gap fill just ran, the watermark is now
    # roughly current. But there may be a small window between the end of
    # gap fill and now. Run one synchronous pull to minimise the gap before
    # handing over to the scheduler.
    if MARKET_OPEN <= now.time() <= MARKET_CLOSE:
        log.info("[catchup] Running immediate incremental pull to sync watermark to now…")
        try:
            run_incremental_pull()
            log.info("[catchup] Catch-up pull complete — scheduler will take over")
        except Exception as exc:
            log.error(f"[catchup] Pull failed: {exc}", exc_info=True)
            log.warning("[catchup] Continuing to scheduler — next 5-min tick will retry")

        # Re-initialise views after the catch-up write
        safe_refresh_views()

    # ── Step 8: Live 5-minute scheduler ──────────────────────────────────────
    # Blocks until Ctrl+C or 15:35.  APScheduler fires run_incremental_pull
    # every 5 minutes aligned to the NSE snapshot grid (09:16:30, 09:21:30, …).
    log.info("[scheduler] Starting live 5-minute scheduler…")
    try:
        start_live_scheduler(run_incremental_pull)
    except Exception as exc:
        log.error(f"[scheduler] Scheduler exited with error: {exc}", exc_info=True)
    finally:
        log.info("[scheduler] Pipeline shut down cleanly")


if __name__ == "__main__":
    main()
