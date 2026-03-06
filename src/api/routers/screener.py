"""
screener.py — Option strike ranking, IV percentile, and term structure.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from typing import Any
import duckdb

from deps import get_duckdb
from analytics import get_top_strikes, get_iv_percentile, get_term_structure

router = APIRouter()

# ── Pydantic Response Models ──────────────────────────────────────────────────

class StrikeRow(BaseModel):
    expiry_date:     str
    dte:             int
    option_type:     str
    strike_price:    float
    ltp:             float
    iv:              float
    delta:           float
    theta:           float
    gamma:           float
    vega:            float
    moneyness_pct:   float
    rho:             float
    eff_ratio:       float
    s_score:         float
    liquidity_cr:    float
    stars:           str

class IVPResult(BaseModel):
    underlying:    str
    atm_iv:        float
    ivr:           float
    ivp:           float

class TermStructureRow(BaseModel):
    expiry_date: str
    dte:         int
    expiry_tier: str
    atm_iv:      float
    avg_theta:   float

# ──────────────────────────────────────────────────────────────────────────────

@router.get("/strikes", response_model=list[StrikeRow])
def strike_screener(
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    underlying:  str = Query("NIFTY"),
    snap_time:   str = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM"),
    top_n:       int = Query(20),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """Ranked list of best option strikes to buy based on S_score."""
    return get_top_strikes(conn, trade_date, underlying, snap_time, top_n)


@router.get("/ivp", response_model=IVPResult)
def iv_percentile(
    underlying: str = Query("NIFTY"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> dict:
    """IV Percentile (IVP) and IV Rank (IVR) for an underlying (90-day window)."""
    rows = get_iv_percentile(conn, underlying)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No IV data found for {underlying}")
    return rows[0] # Return latest day


@router.get("/term-structure", response_model=list[TermStructureRow])
def term_structure(
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    underlying:  str = Query("NIFTY"),
    snap_time:   str = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """IV vs DTE term structure across all available expiries."""
    return get_term_structure(conn, trade_date, underlying, snap_time)
