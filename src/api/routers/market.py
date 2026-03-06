"""
market.py — Endpoints for GEX, CoC velocity, spot data, and environment score.
"""
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from typing import Any, Literal, Optional
import duckdb

from deps import get_duckdb
from analytics import (
    get_gex_timeseries,
    get_coc_velocity,
    get_environment_score,
)

router = APIRouter()

# ── Pydantic Response Models ──────────────────────────────────────────────────

class GEXRow(BaseModel):
    snap_time:   str
    spot:        float
    gex_all_B:   float
    gex_near_B:  float
    gex_far_B:   float
    pct_of_peak: float
    regime:      str

class CoCRow(BaseModel):
    snap_time:  str
    fut_price:  float
    spot:       float
    coc:        float
    v_coc_15m:  Optional[float]
    signal:     str

class SpotData(BaseModel):
    snap_time:  str
    spot:       float
    day_open:   float
    day_high:   float
    day_low:    float
    change_pct: float

class EnvironmentScore(BaseModel):
    score:      int
    max_score:  int
    verdict:    Literal["GO", "WAIT", "NO_GO"]
    conditions: dict[str, Any] # Renamed back from breakdown

# ──────────────────────────────────────────────────────────────────────────────

@router.get("/gex", response_model=list[GEXRow])
def gex_timeseries(
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    underlying:  str = Query("NIFTY", description="NIFTY | BANKNIFTY | …"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """Net GEX per snapshot."""
    return get_gex_timeseries(conn, trade_date, underlying)


@router.get("/coc", response_model=list[CoCRow])
def coc_velocity(
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    underlying:  str = Query("NIFTY"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """CoC and 15-min CoC velocity per snapshot."""
    return get_coc_velocity(conn, trade_date, underlying)


@router.get("/environment", response_model=EnvironmentScore)
def environment_score(
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    snap_time:   str = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM e.g. 09:20"),
    underlying:  str = Query("NIFTY"),
    direction:   str = Query("", description="CE or PE — enables VEX alignment gate"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> dict[str, Any]:
    """8-condition environment suitability score."""
    data = get_environment_score(conn, trade_date, snap_time, underlying, direction)
    return {
        "score":     data["score"],
        "max_score": data["max_score"],
        "verdict":   data["verdict"],
        "conditions": data["conditions"],
    }


@router.get("/spot", response_model=SpotData)
def spot_latest(
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    underlying:  str = Query("NIFTY"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> dict:
    """Latest spot price, intraday high/low, and change% for the requested date."""
    rows = conn.execute("""
        WITH per_snap AS (
            SELECT snap_time,
                   ROUND(AVG(underlying_spot), 2) AS spot
            FROM vw_options
            WHERE trade_date = ? AND underlying = ?
            GROUP BY snap_time
        )
        SELECT
            LAST(snap_time  ORDER BY snap_time)  AS snap_time,
            LAST(spot       ORDER BY snap_time)  AS spot,
            FIRST(spot      ORDER BY snap_time)  AS day_open,
            MAX(spot)                            AS day_high,
            MIN(spot)                            AS day_low
        FROM per_snap
    """, [trade_date, underlying]).fetchdf().to_dict(orient="records")

    if not rows or rows[0]["spot"] is None:
        return {}

    r = rows[0]
    day_open = r["day_open"] or 0
    r["change_pct"] = (
        round((r["spot"] - day_open) / day_open * 100, 2) if day_open else 0.0
    )
    return r
