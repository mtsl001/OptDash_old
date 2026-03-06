"""
processor.py — Compute all derived columns before Parquet write.

Columns added:
  snap_time        : HH:MM string (e.g. "09:20") from record_time
  trade_date       : date string "YYYY-MM-DD"
  dte              : integer days to expiry from trade_date
  moneyness_pct    : (strike - spot) / spot — continuous, works across underlyings
  L_proxy          : volume * effective_ltp — liquidity turnover proxy
  in_atm_window    : bool — strike within ±8 strikes of day-open ATM
  expiry_tier      : "TIER1_NEAR" (DTE≤15), "TIER1_FAR" (DTE≥16), "TIER2_IV_ONLY"
  d_dir            : +1 CE / -1 PE (for GEX formula)
  gex_k            : per-strike GEX contribution (options only)
  vex_k            : per-strike Vanna Exposure (M-units)  — dealer delta change per 1% IV move
  cex_k            : per-strike Charm Exposure (M-units)  — time-driven dealer delta change per day
  oi_delta         : change in OI vs previous snapshot (filled with 0 for first snap)
  coc              : futures - spot (options rows get NaN; populated for futures)
  obi_raw          : (total_buy_qty - total_sell_qty) / (total_buy_qty + total_sell_qty)
                     = raw Order Book Imbalance (OBI) per row
"""
import logging
from datetime import date

import numpy as np
import pandas as pd

from config import (
    INDEX_UNDERLYINGS,
    STRIKE_INTERVALS,
    ATM_WINDOW_N,
    TIER1_NEAR_MAX_DTE,
    TIER1_FAR_MIN_DTE,
)
from atm import is_in_atm_window

logger = logging.getLogger(__name__)


# ── Black-Scholes helpers (pure numpy — no scipy dependency) ─────────────────
# norm.pdf(x) = exp(-x²/2) / sqrt(2π)
_SQRT_2PI = np.sqrt(2.0 * np.pi)


def _bs_norm_pdf(x: np.ndarray) -> np.ndarray:
    """Standard-normal PDF: φ(x) = exp(-x²/2) / √(2π)."""
    return np.exp(-0.5 * x * x) / _SQRT_2PI


def _bs_d1(
    S: np.ndarray,
    K: np.ndarray,
    T: np.ndarray,
    r: float,
    sigma: np.ndarray,
) -> np.ndarray:
    """Black-Scholes d1 term."""
    return (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))


def _bs_vanna(
    S: np.ndarray,
    K: np.ndarray,
    T: np.ndarray,
    sigma: np.ndarray,
    d1: np.ndarray,
    d2: np.ndarray,
) -> np.ndarray:
    """
    Vanna = ∂Delta/∂sigma = -φ(d1) × d2 / sigma   (sigma in decimal)

    Note: in vex_k = OI × vanna × spot × sigma, the sigma in the vex_k
    numerator cancels the sigma in the denominator of vanna. No /100 here —
    adding it would make VEX 100× too small.
    """
    return -_bs_norm_pdf(d1) * d2 / sigma


def _bs_charm(
    T: np.ndarray,
    r: float,
    sigma: np.ndarray,
    d1: np.ndarray,
    d2: np.ndarray,
) -> np.ndarray:
    """
    Charm = -∂Delta/∂t (annual rate).
    Formula: -φ(d1) × (2rT - d2·σ√T) / (2T·σ√T)
    Divide by 365 to convert to delta change per calendar day.
    """
    sqrt_T = np.sqrt(T)
    charm_annual = -_bs_norm_pdf(d1) * (
        2.0 * r * T - d2 * sigma * sqrt_T
    ) / (2.0 * T * sigma * sqrt_T)
    return charm_annual / 365.0



# ── Expiry tier boundaries ────────────────────────────────────────────────────


def compute_derived_columns(
    df: pd.DataFrame,
    atm_windows: dict[str, dict],
) -> pd.DataFrame:
    """
    Add all derived columns to df. Modifies df in-place and returns it.
    Call after validate_dataframe().

    atm_windows: output of atm.compute_atm_windows()
    """
    df = df.copy()

    # ── Time helpers ─────────────────────────────────────────────────────────
    rt = pd.to_datetime(df["record_time"])
    df["snap_time"]  = rt.dt.strftime("%H:%M")
    df["trade_date"] = rt.dt.strftime("%Y-%m-%d")

    # ── Ensure effective_ltp exists on ALL rows (options AND futures) ─────────
    # BigQuery has ltp=None + close_price=None for futures, but close always exists.
    # COALESCE: ltp → close_price → close
    if "effective_ltp" not in df.columns:
        df["effective_ltp"] = (
            df["ltp"]
            .combine_first(df["close_price"])
            .combine_first(df["close"])
        )
    else:
        # The validator may have already set it for options; repair futures rows
        # where effective_ltp ended up NaN because ltp=NaN and close_price=NaN.
        needs_repair = df["effective_ltp"].isna()
        if needs_repair.any():
            df.loc[needs_repair, "effective_ltp"] = (
                df.loc[needs_repair, "ltp"]
                .combine_first(df.loc[needs_repair, "close_price"])
                .combine_first(df.loc[needs_repair, "close"])
            )

    # ── Back-fill underlying_spot for futures rows from options rows ──────────
    # BigQuery only populates `underlying_spot` on OPTIDX rows, not FUTIDX/FUTSTK.
    # Since the batch always contains both instrument types, we extract a spot
    # map from the options rows and broadcast it to the futures rows that share
    # the same underlying + snap_time.
    opt_spot_map = (
        df[df["instrument_type"] == "OPTIDX"]
        .dropna(subset=["underlying_spot"])
        .groupby(["underlying", "snap_time"])["underlying_spot"]
        .mean()  # average across strikes at same snap (should be identical)
    )
    if not opt_spot_map.empty:
        fut_needs_spot = (
            df["instrument_type"].isin(["FUTIDX", "FUTSTK"])
            & df["underlying_spot"].isna()
        )
        if fut_needs_spot.any():
            # Map spot using (underlying, snap_time) multi-index
            mapped_spot = df.loc[fut_needs_spot].set_index(
                ["underlying", "snap_time"]
            ).index.map(opt_spot_map.to_dict())
            df.loc[fut_needs_spot, "underlying_spot"] = mapped_spot
            logger.info(
                f"Back-filled underlying_spot for "
                f"{fut_needs_spot.sum()} futures rows."
            )

    # ── Days to expiry ────────────────────────────────────────────────────────
    df["dte"] = (
        pd.to_datetime(df["expiry_date"]).dt.normalize()
        - rt.dt.normalize()
    ).dt.days.astype("Int64")

    # ── Options-only derivations ──────────────────────────────────────────────
    opt_mask = df["instrument_type"] == "OPTIDX"

    # Moneyness %: (strike - spot) / spot
    # Positive = OTM for CE, negative = OTM for PE (continuous, not step-based)
    df.loc[opt_mask, "moneyness_pct"] = (
        (df.loc[opt_mask, "strike_price"] - df.loc[opt_mask, "underlying_spot"])
        / df.loc[opt_mask, "underlying_spot"]
    ).round(6)

    # Liquidity turnover proxy: volume × effective_ltp
    df["L_proxy"] = (
        df["volume"].astype(float) * df["effective_ltp"]
    ).where(opt_mask)

    # ATM window flag
    df["in_atm_window"] = False
    for underlying, window in atm_windows.items():
        mask = (
            opt_mask
            & (df["underlying"] == underlying)
            & (df["strike_price"] >= window["lower_strike"])
            & (df["strike_price"] <= window["upper_strike"])
        )
        df.loc[mask, "in_atm_window"] = True

    # Expiry tier
    def _tier(dte_val):
        if pd.isna(dte_val):
            return "UNKNOWN"
        if dte_val <= TIER1_NEAR_MAX_DTE:
            return "TIER1_NEAR"
        if dte_val >= TIER1_FAR_MIN_DTE:
            return "TIER1_FAR"
        return "TIER2_IV_ONLY"

    df["expiry_tier"] = df["dte"].apply(_tier)

    # GEX directional multiplier: +1 CE, -1 PE
    df["d_dir"] = np.where(df["option_type"] == "CE", 1.0,
                  np.where(df["option_type"] == "PE", -1.0, np.nan))

    # Per-strike GEX contribution (options only, skip if any component is null)
    gex_valid = (
        opt_mask
        & df["gamma"].notna()
        & df["oi"].notna()
        & df["underlying_spot"].notna()
        & df["d_dir"].notna()
    )
    df.loc[gex_valid, "gex_k"] = (
        df.loc[gex_valid, "gamma"].astype(float)
        * df.loc[gex_valid, "oi"].astype(float)
        * df.loc[gex_valid, "underlying_spot"].astype(float) ** 2
        * 0.01
        * df.loc[gex_valid, "d_dir"]
    )

    # ── Per-strike Vanna/Charm Exposure (VEX_k, CEX_k) ───────────────────────
    # Pre-initialize so the column always exists in the DataFrame (and Parquet)
    # even when no row passes the null guard (e.g. all iv=NULL rows).
    if opt_mask.any():
        df.loc[opt_mask, "vex_k"] = np.nan
        df.loc[opt_mask, "cex_k"] = np.nan
    # Dealer is assumed SHORT the option the retail buyer holds:
    #   CE: d_dir = +1  →  IV↑ forces dealer to sell more futures (negative flow)
    #   PE: d_dir = -1  →  IV↑ forces dealer to buy more futures (positive flow)
    #
    # VEX_k = OI × vanna × spot × (sigma) × d_dir   [stored in M-units / 1e6]
    # CEX_k = OI × charm_daily × d_dir × (-1)        [stored in M-units / 1e6]
    #   The (-1) on CEX: charm is negative for OTM (delta decays toward 0).
    #   Positive CEX_k = dealers must BUY futures = bullish mechanical flow.
    #
    # Null guard: iv, dte, spot, oi all required
    vex_valid = (
        opt_mask
        & df["iv"].notna()
        & df["dte"].notna()
        & (df["dte"].fillna(0) >= 0)   # dte may be 0 on expiry day — floor to 1 below
        & df["underlying_spot"].notna()
        & (df["underlying_spot"].fillna(0) > 0)
        & df["strike_price"].notna()
        & (df["strike_price"].fillna(0) > 0)
        & df["oi"].notna()
        & (df["iv"].fillna(0) > 0)
        & df["d_dir"].notna()
    )

    if vex_valid.any():
        S     = df.loc[vex_valid, "underlying_spot"].astype(float).values
        K     = df.loc[vex_valid, "strike_price"].astype(float).values
        # Floor DTE at 1 day to avoid divide-by-zero on expiry day
        T     = np.maximum(df.loc[vex_valid, "dte"].astype(float).values, 1.0) / 365.0
        r     = 0.065   # RBI repo rate proxy (6.5%)
        sigma = df.loc[vex_valid, "iv"].astype(float).values / 100.0  # % → decimal
        oi    = df.loc[vex_valid, "oi"].astype(float).values
        ddir  = df.loc[vex_valid, "d_dir"].astype(float).values

        d1 = _bs_d1(S, K, T, r, sigma)
        d2 = d1 - sigma * np.sqrt(T)

        vanna       = _bs_vanna(S, K, T, sigma, d1, d2)       # Δdelta / Δ1% IV
        charm_daily = _bs_charm(T, r, sigma, d1, d2)          # Δdelta / day

        # VEX (Dealer Vanna Exposure to IV Drop):
        # Vanna = ∂Delta/∂sigma. For IV drop, change in hedge = Vanna * (-IV_drop).
        # We want positive VEX = bullish flow (dealer buying).
        # OTM Put Vanna is negative -> IV drop causes buying -> needs positive VEX -> use -vanna.
        vex_k_raw = -oi * vanna * S * sigma / 1e6
        
        # CEX (Dealer Charm Exposure to Time Decay):
        # Charm = ∂Delta/∂t. Hedge flow = ∂Delta/∂t = charm_daily.
        # We want positive CEX = bullish flow (dealer buying).
        # OTM Put Charm is positive -> causes buying -> needs positive CEX -> use charm_daily directly.
        cex_k_raw = oi * charm_daily / 1e6

        df.loc[vex_valid, "vex_k"] = np.round(vex_k_raw, 4)
        df.loc[vex_valid, "cex_k"] = np.round(cex_k_raw, 4)

        logger.debug(
            f"VEX/CEX computed: {vex_valid.sum():,} rows, "
            f"net VEX={vex_k_raw.sum():.2f}M, net CEX={cex_k_raw.sum():.2f}M"
        )

    # Raw OBI per row: (TBQ - TSQ) / (TBQ + TSQ)
    tbq = df["total_buy_qty"].astype(float)
    tsq = df["total_sell_qty"].astype(float)
    total_qty = tbq + tsq
    df["obi_raw"] = np.where(total_qty > 0, (tbq - tsq) / total_qty, np.nan)

    # ── Futures-only: Cost of Carry ───────────────────────────────────────────
    # CoC = futures_price - spot. For futures rows, we use effective_ltp as the
    # futures price (or close / close_price if ltp is null).
    fut_mask = df["instrument_type"].isin(["FUTIDX", "FUTSTK"])
    df.loc[fut_mask, "coc"] = (
        df.loc[fut_mask, "effective_ltp"].astype(float)
        - df.loc[fut_mask, "underlying_spot"].astype(float)
    )

    # ── OI delta (change vs previous snapshot for same contract) ─────────────
    # Sort by contract + time, then diff within each group
    sort_cols = ["underlying", "instrument_type", "expiry_date",
                 "strike_price", "option_type", "record_time"]
    group_cols = ["underlying", "instrument_type", "expiry_date",
                  "strike_price", "option_type"]

    df = df.sort_values(sort_cols)
    df["oi_delta"] = (
        df.groupby(group_cols)["oi"]
        .transform(lambda x: x.astype(float).diff())
        .fillna(0.0)
    )

    logger.info(
        f"Derived columns computed: {len(df):,} rows, "
        f"GEX non-null: {df['gex_k'].notna().sum():,}, "
        f"ATM window: {df['in_atm_window'].sum():,}"
    )
    return df
