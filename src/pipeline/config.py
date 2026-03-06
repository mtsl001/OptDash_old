"""
config.py — All constants for the Option Buying Dashboard pipeline.
Every other module imports from here. Nothing is hardcoded elsewhere.
"""
from pathlib import Path
from datetime import time as dtime


# ── Root paths ──────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent.parent.parent
SRC_DIR         = BASE_DIR / "src"
DATA_DIR        = BASE_DIR / "data"
RAW_DIR         = DATA_DIR / "raw"
PROCESSED_DIR   = DATA_DIR / "processed"
DB_DIR          = DATA_DIR / "db"
LOG_DIR         = BASE_DIR / "logs"
ATM_WINDOWS_DIR = PROCESSED_DIR / "atm_windows"


# ── BigQuery ─────────────────────────────────────────────────────────────────
BQ_PROJECT   = "universal-ion-437606-b7"
BQ_DATASET   = "bgquery"
BQ_TABLE     = "upxtx"
BQ_TABLE_FQN = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
CREDENTIALS_PATH = BASE_DIR / "universal-ion-437606-b7-5629bd4bc421.json"


# ── Local database ───────────────────────────────────────────────────────────
DUCKDB_PATH   = DB_DIR / "fo_analytics.duckdb"
WATERMARK_PATH = DATA_DIR / "watermark.json"


# ── Schedule timing ──────────────────────────────────────────────────────────
# record_time in BigQuery is formatted "YYYY-MM-DD HH:MM:SS UTC" but the
# actual values ARE IST. No timezone conversion is applied anywhere.
MARKET_OPEN           = dtime(9, 15, 0)
MARKET_CLOSE          = dtime(15, 30, 0)
FIRST_PULL_TIME       = dtime(9, 16, 30)   # 90 sec after open — data is stable
SCHEDULE_INTERVAL_MIN = 5                  # every 5 minutes
TRADING_MINUTES_PER_DAY = 375
SNAPS_PER_DAY         = 75


# ── Backfill ─────────────────────────────────────────────────────────────────
BACKFILL_START_DATE = "2026-02-17"   # First day to backfill
BACKFILL_END_DATE   = "2026-02-20"   # Last day to backfill (fixed — live pulls take over after this)


# ── Underlyings ──────────────────────────────────────────────────────────────
# Strike interval = minimum gap between consecutive strikes on NSE
STRIKE_INTERVALS: dict[str, int] = {
    "NIFTY":      50,
    "BANKNIFTY":  100,
    "FINNIFTY":   50,
    "MIDCPNIFTY": 25,
    "NIFTYNXT50": 50,
}
INDEX_UNDERLYINGS = list(STRIKE_INTERVALS.keys())

# instrument_type values in the BigQuery table
INSTRUMENT_TYPE_OPTION  = "OPTIDX"
INSTRUMENT_TYPE_FUTURE  = "FUTIDX"
INSTRUMENT_TYPE_STFUT   = "FUTSTK"   # stock futures


# ── ATM window ───────────────────────────────────────────────────────────────
# Number of strikes ABOVE and BELOW the day-open ATM to include in dashboard.
# e.g. NIFTY with ATM=25400, N=8, interval=50: include 25000–25800 (17 strikes)
ATM_WINDOW_N = 8


# ── Expiry tiers ──────────────────────────────────────────────────────────────
# Near: DTE ≤ 15 (covers mid-week)
# Far:  DTE ≥ 16 (no gap)
TIER1_NEAR_MAX_DTE = 15
TIER1_FAR_MIN_DTE  = 16


# ── Parquet storage ──────────────────────────────────────────────────────────
# CRITICAL: row_group_size=100,000 → DuckDB uses 2 threads per daily file.
# row_group_size=10,000 limits DuckDB to single-core execution (10× slower).
PARQUET_ROW_GROUP_SIZE = 100_000
PARQUET_COMPRESSION    = "snappy"    # Fast decompression > max ratio


# ── PCR Divergence thresholds (per underlying) ───────────────────────────────
# BULL_TRAP  = retail bought puts (PCR_vol rising) but institutions stay flat → market rips up
# BEAR_TRAP  = retail bought calls (PCR_vol falling) but institutions stay flat → market drops
# Per-underlying because BANKNIFTY divergence is more predictive (tighter); smaller
# indices need more signal to overcome noise (wider).
PCR_DIVERGENCE_THRESHOLDS: dict[str, dict[str, float]] = {
    "NIFTY":      {"BULL_TRAP": 0.28, "BEAR_TRAP": -0.20},
    "BANKNIFTY":  {"BULL_TRAP": 0.22, "BEAR_TRAP": -0.18},
    "FINNIFTY":   {"BULL_TRAP": 0.30, "BEAR_TRAP": -0.22},
    "MIDCPNIFTY": {"BULL_TRAP": 0.35, "BEAR_TRAP": -0.25},
    "NIFTYNXT50": {"BULL_TRAP": 0.38, "BEAR_TRAP": -0.28},
}
# Default fallback if underlying not in map
PCR_DIVERGENCE_DEFAULT   = {"BULL_TRAP": 0.28, "BEAR_TRAP": -0.20}

# ── CoC Velocity thresholds (per underlying) ────────────────────────────────
COC_VELOCITY_THRESHOLDS: dict[str, float] = {
    "NIFTY":      10.0,
    "BANKNIFTY":  25.0,  # Higher price level (~50k) → higher noise floor
    "FINNIFTY":   12.0,
    "MIDCPNIFTY": 8.0,
    "NIFTYNXT50": 10.0,
}
COC_VELOCITY_DEFAULT = 10.0

# ── VEX/CEX (Vanna/Charm) thresholds (per underlying) ──────────────────────
# CEX measured in Millions (M-units)
CEX_THRESHOLDS: dict[str, dict[str, float]] = {
    "NIFTY":      {"STRONG_BID": 50.0, "BID": 20.0, "PRESSURE": -20.0},
    "BANKNIFTY":  {"STRONG_BID": 80.0, "BID": 35.0, "PRESSURE": -35.0},
    "FINNIFTY":   {"STRONG_BID": 40.0, "BID": 15.0, "PRESSURE": -15.0},
    "MIDCPNIFTY": {"STRONG_BID": 30.0, "BID": 12.0, "PRESSURE": -12.0},
    "NIFTYNXT50": {"STRONG_BID": 40.0, "BID": 18.0, "PRESSURE": -18.0},
}
CEX_DEFAULT = {"STRONG_BID": 50.0, "BID": 20.0, "PRESSURE": -20.0}

# Velocity — EMA window and time-of-day multipliers (see analytics.py)
PCR_VELOCITY_EMA_ALPHA   = 2 / (3 + 1)   # 3-period EMA
PCR_VELOCITY_OPEN_MULT   = 1.8            # 09:15–09:45 noisy open
PCR_VELOCITY_CLOSE_MULT  = 1.3            # 14:00–15:00 European clearing
PCR_VELOCITY_BASE_THRESH = 0.05           # baseline velocity threshold


# ── Analytical filters ───────────────────────────────────────────────────────
MONEYNESS_PCT_FILTER   = 0.05         # ±5% from spot (full analysis)
LIQUIDITY_MIN_TURNOVER = 10_000_000   # ₹1 Cr: volume × ltp threshold
V_BENCH_INDEX          = 2_000_000    # Slippage penalty benchmark (index options)
IVP_WINDOW_DAYS        = 90           # Rolling window for IVP/IVR computation


# ── API ──────────────────────────────────────────────────────────────────────
API_HOST = "127.0.0.1"
API_PORT = 8000


# ── BigQuery column list ─────────────────────────────────────────────────────
# Complete list of columns to SELECT in every BQ pull.
# Note: "close" and "close_price" are two separate columns in the table.
# "close"       = OHLC closing price of the instrument
# "close_price" = alternate close feed (use as fallback when close is null)
# Effective LTP: COALESCE(ltp, close_price, close)
BQ_SELECT_COLS = [
    "record_time",
    "underlying",
    "instrument_type",
    "instrument_key",
    "option_type",
    "expiry_date",
    "strike_price",
    "underlying_spot",
    "open",
    "high",
    "low",
    "close",
    "close_price",
    "ltp",
    "volume",
    "oi",
    "total_buy_qty",
    "total_sell_qty",
    "iv",
    "delta",
    "theta",
    "gamma",
    "vega",
    "pcr",
    "last_trade_time",
]


# ── AI Bot ────────────────────────────────────────────────────────────────────
AI_SL_PCT              = 0.40      # 40% max loss on entry premium
AI_TARGET_PCT          = 0.50      # 50% profit target
AI_MIN_CONFIDENCE      = 50        # Don't recommend below this
AI_MIN_GATE_SCORE      = 5         # Hard floor for Gate
AI_MIN_STARS           = 2         # Skip 1-star strikes
AI_EXPIRY_MAX_SNAPS    = 2         # Trade card expires after 2 snaps (10 min)
AI_LOT_SIZES: dict[str, int] = {
    "NIFTY":      75,
    "BANKNIFTY":  30,
    "FINNIFTY":   40,
    "MIDCPNIFTY": 50,
    "NIFTYNXT50": 25,
}
TRADE_JOURNAL_DB       = DB_DIR / "trade_journal.db"
