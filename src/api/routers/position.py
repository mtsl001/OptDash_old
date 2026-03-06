"""
position.py — P&L attribution and theta-adjusted stop loss endpoints.
"""
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from typing import Any, Literal
import duckdb

from config import SNAPS_PER_DAY
from deps import get_duckdb
from analytics import compute_theta_sl, compute_iv_crush

router = APIRouter()

# ── Pydantic Response Models ──────────────────────────────────────────────────

class PnlAttributionRow(BaseModel):
    snap_time:       str
    ltp:             float
    spot:            float
    iv:              float
    vega:            float
    delta_pnl:       float
    gamma_pnl:       float
    vega_pnl:        float
    theta_pnl:       float
    actual_pnl:      float
    theoretical_pnl: float
    unexplained:     float

class ThetaSL(BaseModel):
    entry_premium:  float
    theta_daily:    float
    sl_base:        float
    sl_adjusted:    float
    current_ltp:    float
    unrealised_pnl: float
    pnl_pct:        float
    status:         Literal["IN_TRADE", "STOP_HIT", "PROFIT_ZONE_PARTIAL_EXIT", "GUARANTEED_PROFIT_ZONE"]

class IvCrushResult(BaseModel):
    iv_at_entry:       float
    iv_current:        float
    iv_crush:          float
    delta_pnl:         float
    vega:              float
    cushion_vol_pts:   float
    is_safe:           bool
    warning:           str

# ──────────────────────────────────────────────────────────────────────────────

@router.get("/pnl-attribution", response_model=list[PnlAttributionRow])
def pnl_attribution(
    trade_date:   str   = Query(...),
    underlying:    str   = Query("NIFTY"),
    expiry_date:   str   = Query(..., description="YYYY-MM-DD"),
    option_type:   str   = Query(..., description="CE or PE"),
    strike_price:  float = Query(...),
    entry_snap:    str   = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM entry time"),
    entry_premium: float = Query(..., description="Entry LTP"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """Taylor Series P&L attribution per snapshot from entry to current/close."""
    rows = conn.execute("""
        WITH snaps AS (
            SELECT
                snap_time,
                effective_ltp AS ltp,
                underlying_spot AS spot,
                iv, delta, theta, gamma, vega
            FROM vw_options
            WHERE trade_date    = ?
              AND underlying     = ?
              AND expiry_date    = ?
              AND option_type    = ?
              AND strike_price   = ?
              AND snap_time     >= ?
            ORDER BY snap_time
        ),
        with_prev AS (
            SELECT *,
                LAG(spot) OVER (ORDER BY snap_time) AS prev_spot,
                LAG(iv)   OVER (ORDER BY snap_time) AS prev_iv,
                LAG(ltp)  OVER (ORDER BY snap_time) AS prev_ltp,
                1.0 / ? AS dt_fraction
            FROM snaps
        )
        SELECT
            snap_time,
            ROUND(ltp, 2) AS ltp,
            ROUND(spot, 2) AS spot,
            ROUND(iv, 4)   AS iv,
            ROUND(vega, 4) AS vega,
            ROUND(delta * (spot - COALESCE(prev_spot, spot)), 2) AS delta_pnl,
            ROUND(0.5 * gamma * POWER(spot - COALESCE(prev_spot, spot), 2), 2) AS gamma_pnl,
            ROUND(vega * (iv - COALESCE(prev_iv, iv)), 2) AS vega_pnl,
            ROUND(theta * dt_fraction, 2) AS theta_pnl,
            ROUND(ltp - ?, 2) AS actual_pnl
        FROM with_prev
        ORDER BY snap_time
    """, [trade_date, underlying, expiry_date, option_type, strike_price,
          entry_snap, SNAPS_PER_DAY, entry_premium]).fetchdf().to_dict(orient="records")

    for r in rows:
        r["theoretical_pnl"] = round(
            (r["delta_pnl"] or 0) + (r["gamma_pnl"] or 0)
            + (r["vega_pnl"] or 0) + (r["theta_pnl"] or 0), 2
        )
        r["unexplained"] = round(
            (r["actual_pnl"] or 0) - r["theoretical_pnl"], 2
        )
    return rows


@router.get("/theta-sl", response_model=ThetaSL)
def theta_stop_loss(
    entry_premium: float = Query(...),
    theta_daily:   float = Query(..., description="Negative value"),
    max_loss_pct:  float = Query(0.30),
    entry_snap:    str   = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM"),
    current_snap:  str   = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM"),
    current_ltp:   float = Query(...),
) -> dict[str, Any]:
    """Compute the theta-adjusted trailing stop loss for a live position."""
    return compute_theta_sl(
        entry_premium, theta_daily, max_loss_pct,
        entry_snap, current_snap, current_ltp
    )


@router.get("/theta-sl-series", response_model=list[ThetaSL])
def theta_sl_series(
    trade_date:    str   = Query(...),
    underlying:    str   = Query("NIFTY"),
    expiry_date:   str   = Query(...),
    option_type:   str   = Query(...),
    strike_price:  float = Query(...),
    entry_snap:    str   = Query(..., pattern=r"^\d{2}:\d{2}$"),
    entry_premium: float = Query(...),
    theta_daily:   float = Query(...),
    max_loss_pct:  float = Query(0.30),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """Theta-adjusted SL for every snapshot from entry to close."""
    ltps = conn.execute("""
        SELECT snap_time, effective_ltp AS ltp
        FROM vw_options
        WHERE trade_date=? AND underlying=? AND expiry_date=?
          AND option_type=? AND strike_price=? AND snap_time >= ?
        ORDER BY snap_time
    """, [trade_date, underlying, expiry_date, option_type,
          strike_price, entry_snap]).fetchdf().to_dict(orient="records")

    return [
        compute_theta_sl(
            entry_premium, theta_daily, max_loss_pct,
            entry_snap, row["snap_time"], row["ltp"]
        )
        for row in ltps
        if row["ltp"] is not None
    ]


@router.get("/iv-crush", response_model=IvCrushResult)
def iv_crush_check(
    trade_date:        str   = Query(..., description="YYYY-MM-DD"),
    underlying:        str   = Query("NIFTY"),
    expiry_date:       str   = Query(...),
    option_type:       str   = Query(...),
    strike_price:      float = Query(...),
    entry_snap:        str   = Query(...),
    iv_at_entry:       float = Query(..., description="IV% at trade entry"),
    iv_current:        float = Query(..., description="Current IV%"),
    delta_pnl:         float = Query(..., description="Running delta P&L in ₹"),
    iv_cushion_thresh: float = Query(1.5, description="Warning threshold in vol pts"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> dict:
    """
    IV Crush Guard — uses peak vega since entry for conservative risk assessment.
    """
    # Fetch max vega since entry
    res = conn.execute("""
        SELECT MAX(ABS(vega)) AS max_vega
        FROM vw_options
        WHERE trade_date=? AND underlying=? AND expiry_date=?
          AND option_type=? AND strike_price=? AND snap_time >= ?
    """, [trade_date, underlying, expiry_date, option_type, strike_price, entry_snap]).fetchone()
    
    peak_vega = res[0] if res and res[0] is not None else 0.0

    return compute_iv_crush(iv_at_entry, iv_current, delta_pnl, peak_vega, iv_cushion_thresh)
