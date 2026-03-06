"""
microstructure.py — PCR divergence, OBI, volume velocity, and rich alert endpoints.
"""
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from typing import Literal, Optional
import duckdb

from config import (
    PCR_DIVERGENCE_THRESHOLDS, PCR_DIVERGENCE_DEFAULT,
    COC_VELOCITY_THRESHOLDS, COC_VELOCITY_DEFAULT
)
from deps import get_duckdb
from analytics import get_pcr_divergence, get_vanna_charm_exposure, get_vex_cex_by_strike

router = APIRouter()

# ── Pydantic Response Models ──────────────────────────────────────────────────

class AlertItem(BaseModel):
    time:      str = Field(..., json_schema_extra={"example": "10:45"})
    type:      str = Field(..., json_schema_extra={"example": "COC_VELOCITY"})
    severity:  Literal["HIGH", "MEDIUM", "LOW"]
    direction: Literal["BULL", "BEAR", "NEUTRAL"]
    headline:  str
    message:   str

class PCRRow(BaseModel):
    snap_time:      str
    pcr_vol:        float
    pcr_oi:         float
    pcr_divergence: float
    smoothed_obi:   float
    signal:         str
    velocity_signal: str
    ema:            Optional[float] = None # Added for frontend compatibility

class VolVelocityRow(BaseModel):
    snap_time:    str
    vol_total:    int
    baseline_vol: int
    volume_ratio: float
    signal:       str

# ──────────────────────────────────────────────────────────────────────────────

@router.get("/pcr", response_model=list[PCRRow])
def pcr_divergence(
    trade_date: str = Query(...),
    underlying:  str = Query("NIFTY"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """Per-snapshot PCR_Vol, PCR_OI, divergence, smoothed OBI, and signal."""
    return get_pcr_divergence(conn, trade_date, underlying)


@router.get("/volume-velocity", response_model=list[VolVelocityRow])
def volume_velocity(
    trade_date: str = Query(...),
    underlying:  str = Query("NIFTY"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """
    Per-snapshot volume vs 5-day trailing morning baseline.
    Trailing average of 09:15–10:15 volume over the last 5 trading days.
    """
    # 1. Compute trailing 5-day morning baseline
    baseline_res = conn.execute("""
        WITH morning_vols AS (
            SELECT trade_date, SUM(volume) AS morning_total
            FROM vw_options
            WHERE underlying=? AND snap_time BETWEEN '09:15' AND '10:15'
              AND trade_date < ?
            GROUP BY trade_date
            ORDER BY trade_date DESC LIMIT 5
        )
        SELECT CAST(AVG(morning_total) / 13.0 AS BIGINT) AS baseline_per_snap 
        FROM morning_vols
    """, [underlying, trade_date]).fetchone()
    
    baseline_vol = baseline_res[0] if baseline_res and baseline_res[0] else None
    
    rows = conn.execute("""
        WITH current_baseline AS (
            SELECT AVG(vol_total) AS bvol
            FROM (
                SELECT snap_time, SUM(volume) AS vol_total
                FROM vw_options
                WHERE trade_date=? AND underlying=? AND snap_time BETWEEN '09:15' AND '10:15'
                GROUP BY snap_time
            )
        ),
        by_snap AS (
            SELECT snap_time, SUM(volume) AS vol_total
            FROM vw_options
            WHERE trade_date=? AND underlying=?
            GROUP BY snap_time
        )
        SELECT
            s.snap_time,
            CAST(s.vol_total AS BIGINT) AS vol_total,
            CAST(ROUND(COALESCE(?, cb.bvol), 0) AS BIGINT) AS baseline_vol,
            ROUND(s.vol_total / NULLIF(COALESCE(?, cb.bvol), 0), 2) AS volume_ratio,
            CASE WHEN s.vol_total / NULLIF(COALESCE(?, cb.bvol), 0) >= 2.0
                 THEN 'SPIKE' ELSE 'NORMAL' END AS signal
        FROM by_snap s CROSS JOIN current_baseline cb
        ORDER BY s.snap_time
    """, [trade_date, underlying, trade_date, underlying, baseline_vol, baseline_vol, baseline_vol]).fetchdf().to_dict(orient="records")
    return rows


@router.get("/alerts", response_model=list[AlertItem])
def live_alerts(
    trade_date: str = Query(...),
    underlying:  str = Query("NIFTY"),
    snap_time:   str = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM — current snapshot"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> list[dict]:
    """Returns all triggered trading alerts up to snap_time."""
    alerts: list[dict] = []
    coc_thresh = COC_VELOCITY_THRESHOLDS.get(underlying, COC_VELOCITY_DEFAULT)

    coc_rows = conn.execute("""
        WITH snap_spot AS (
            SELECT snap_time, AVG(underlying_spot) AS spot FROM vw_options
            WHERE trade_date=? AND underlying=? GROUP BY snap_time
        ),
        near_fut AS (
            SELECT f.snap_time,
                ROUND(f.effective_ltp - COALESCE(f.underlying_spot, s.spot), 2) AS coc,
                ROUND(f.effective_ltp, 2)  AS fut_price,
                ROUND(COALESCE(f.underlying_spot, s.spot), 2) AS spot
            FROM vw_futures f LEFT JOIN snap_spot s USING (snap_time)
            WHERE f.trade_date=? AND f.underlying=? AND f.instrument_type='FUTIDX'
              AND COALESCE(f.underlying_spot, s.spot) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY f.snap_time ORDER BY f.dte ASC) = 1
        ),
        with_vcoc AS (
            SELECT snap_time, coc, fut_price, spot,
                ROUND(coc - LAG(coc, 3) OVER (ORDER BY snap_time), 2) AS v_coc,
                LAG(snap_time, 3) OVER (ORDER BY snap_time) AS prev_snap
            FROM near_fut
        ),
        with_edge AS (
            SELECT *,
                LAG(v_coc) OVER (ORDER BY snap_time) AS prev_v_coc
            FROM with_vcoc
            WHERE prev_snap IS NOT NULL 
              AND DATEDIFF('minute', CAST(prev_snap || ':00' AS TIME), CAST(snap_time || ':00' AS TIME)) = 15
        )
        SELECT snap_time, coc, fut_price, spot, v_coc
        FROM with_edge
        WHERE snap_time <= ?
          AND v_coc IS NOT NULL
          AND (
            (v_coc <  -? AND (prev_v_coc IS NULL OR prev_v_coc >= -?))
            OR
            (v_coc >   ? AND (prev_v_coc IS NULL OR prev_v_coc <=  ?))
          )
        ORDER BY snap_time
    """, [trade_date, underlying, trade_date, underlying, snap_time, coc_thresh, coc_thresh, coc_thresh, coc_thresh]).fetchdf().to_dict(orient="records")

    for row in coc_rows:
        is_bear  = row["v_coc"] < 0
        alerts.append({
            "time":      row["snap_time"],
            "type":      "COC_VELOCITY",
            "severity":  "HIGH",
            "direction": "BEAR" if is_bear else "BULL",
            "headline":  f"Institutional {'BEAR' if is_bear else 'BULL'} flow detected on {underlying} futures",
            "message": f"V_CoC crossed {'−' if is_bear else '+'}{abs(row['v_coc']):.2f} (15-min momentum).",
        })

    gex_rows = conn.execute("""
        WITH g AS (
            SELECT snap_time, SUM(gex_k) AS gex_all,
                MAX(ABS(SUM(gex_k))) OVER (ORDER BY snap_time ROWS UNBOUNDED PRECEDING) AS peak
            FROM vw_options WHERE trade_date=? AND underlying=?
              AND expiry_tier IN ('TIER1_NEAR','TIER1_FAR') AND gex_k IS NOT NULL
            GROUP BY snap_time
        ),
        pct AS (
            SELECT snap_time, ROUND(gex_all / 1e9, 2) AS gex_B,
                ROUND(100.0 * ABS(gex_all) / NULLIF(peak, 0), 1) AS pct,
                ROUND(peak / 1e9, 2) AS peak_B,
                LAG(ROUND(100.0 * ABS(gex_all) / NULLIF(peak, 0), 1)) OVER (ORDER BY snap_time) AS prev_pct
            FROM g
        )
        SELECT snap_time, gex_B, peak_B, pct FROM pct
        WHERE snap_time <= ? AND pct IS NOT NULL
          AND ((pct <= 70 AND (prev_pct IS NULL OR prev_pct > 70)) OR (pct <= 30 AND (prev_pct IS NULL OR prev_pct > 30)))
        ORDER BY snap_time
    """, [trade_date, underlying, snap_time]).fetchdf().to_dict(orient="records")

    for row in gex_rows:
        critical = row["pct"] <= 30
        alerts.append({
            "time":      row["snap_time"],
            "type":      "GEX_DECLINE",
            "severity":  "HIGH" if critical else "MEDIUM",
            "direction": "BEAR",
            "headline":  f"{'CRITICAL: GEX Collapse' if critical else 'GEX Erosion'} — {underlying} losing dealer hedge support",
            "message": f"GEX at {row['pct']:.1f}% of today's peak.",
        })

    pcr_thresh = PCR_DIVERGENCE_THRESHOLDS.get(underlying, PCR_DIVERGENCE_DEFAULT)
    bull_thresh, bear_thresh = pcr_thresh["BULL_TRAP"], pcr_thresh["BEAR_TRAP"]
    pcr_rows = conn.execute("""
        WITH by_snap AS (
            SELECT snap_time,
                NULLIF(SUM(CASE WHEN option_type='PE' THEN volume ELSE 0 END), 0) / NULLIF(SUM(CASE WHEN option_type='CE' THEN volume ELSE 0 END), 0) AS pcr_vol,
                NULLIF(SUM(CASE WHEN option_type='PE' THEN oi ELSE 0 END), 0) / NULLIF(SUM(CASE WHEN option_type='CE' THEN oi ELSE 0 END), 0) AS pcr_oi
            FROM vw_options WHERE trade_date=? AND underlying=? AND expiry_tier IN ('TIER1_NEAR','TIER1_FAR')
            GROUP BY snap_time
        ),
        with_div AS (
            SELECT snap_time, ROUND(pcr_vol, 3) AS pcr_vol, ROUND(pcr_oi, 3) AS pcr_oi,
                ROUND(pcr_vol - pcr_oi, 3) AS div, LAG(ROUND(pcr_vol - pcr_oi, 3)) OVER (ORDER BY snap_time) AS prev_div
            FROM by_snap WHERE pcr_vol IS NOT NULL AND pcr_oi IS NOT NULL
        )
        SELECT snap_time, pcr_vol, pcr_oi, div FROM with_div
        WHERE snap_time <= ? AND ((div > ? AND (prev_div IS NULL OR prev_div <= ?)) OR (div < ? AND (prev_div IS NULL OR prev_div >= ?)))
        ORDER BY snap_time
    """, [trade_date, underlying, snap_time, bull_thresh, bull_thresh, bear_thresh, bear_thresh]).fetchdf().to_dict(orient="records")

    for row in pcr_rows:
        alerts.append({
            "time":      row["snap_time"],
            "type":      "PCR_DIVERGENCE",
            "severity":  "MEDIUM",
            "direction": "BULL" if row["div"] > 0 else "BEAR",
            "headline":  f"{'BULL TRAP' if row['div'] > 0 else 'BEAR TRAP'} detected (div: {row['div']:+.3f})",
            "message": "Retail is positioning against the trend.",
        })

    obi_rows = conn.execute("""
        WITH atm_obi AS (
            SELECT snap_time, AVG(obi_raw) AS raw_obi FROM vw_options
            WHERE trade_date=? AND underlying=? AND in_atm_window=true GROUP BY snap_time
        ),
        smoothed AS (
            SELECT snap_time, ROUND(AVG(raw_obi) OVER (ORDER BY snap_time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 4) AS sobi
            FROM atm_obi
        ),
        with_edge AS (
            SELECT snap_time, sobi, LAG(sobi) OVER (ORDER BY snap_time) AS prev_sobi
            FROM smoothed WHERE sobi IS NOT NULL
        )
        SELECT snap_time, sobi FROM with_edge
        WHERE snap_time <= ? AND ((sobi < -0.10 AND (prev_sobi IS NULL OR prev_sobi >= -0.10)) OR (sobi > 0.10 AND (prev_sobi IS NULL OR prev_sobi <= 0.10)))
        ORDER BY snap_time
    """, [trade_date, underlying, snap_time]).fetchdf().to_dict(orient="records")

    for row in obi_rows:
        alerts.append({
            "time":      row["snap_time"],
            "type":      "OBI_SHIFT",
            "severity":  "MEDIUM",
            "direction": "BEAR" if row["sobi"] < 0 else "BULL",
            "headline":  f"{'Sellers' if row['sobi'] < 0 else 'Buyers'} taking control in ATM options",
            "message":   f"Smoothed ATM OBI = {row['sobi']:+.4f}.",
        })

    baseline_res = conn.execute("""
        WITH morning_vols AS (
            SELECT trade_date, SUM(volume) AS morning_total
            FROM vw_options WHERE underlying=? AND snap_time BETWEEN '09:15' AND '10:15' AND trade_date < ?
            GROUP BY trade_date ORDER BY trade_date DESC LIMIT 5
        )
        SELECT CAST(AVG(morning_total) / 13.0 AS BIGINT) FROM morning_vols
    """, [underlying, trade_date]).fetchone()
    bvol = baseline_res[0] if baseline_res and baseline_res[0] else None

    vol_rows = conn.execute("""
        WITH current_baseline AS (
            SELECT AVG(vol_total) AS bvol FROM (
                SELECT snap_time, SUM(volume) AS vol_total FROM vw_options
                WHERE trade_date=? AND underlying=? AND snap_time BETWEEN '09:15' AND '10:15' GROUP BY snap_time
            )
        ),
        by_snap AS (
            SELECT snap_time, SUM(volume) AS vol_total FROM vw_options
            WHERE trade_date=? AND underlying=? GROUP BY snap_time
        ),
        with_ratio AS (
            SELECT s.snap_time, s.vol_total, ROUND(s.vol_total / NULLIF(COALESCE(?, cb.bvol), 0), 2) AS ratio,
                LAG(ROUND(s.vol_total / NULLIF(COALESCE(?, cb.bvol), 0), 2)) OVER (ORDER BY s.snap_time) AS prev_ratio
            FROM by_snap s CROSS JOIN current_baseline cb
        )
        SELECT snap_time, ratio FROM with_ratio
        WHERE snap_time <= ? AND ratio >= 2.0 AND (prev_ratio IS NULL OR prev_ratio < 2.0)
        ORDER BY snap_time
    """, [trade_date, underlying, trade_date, underlying, bvol, bvol, snap_time]).fetchdf().to_dict(orient="records")

    for row in vol_rows:
        alerts.append({
            "time":      row["snap_time"],
            "type":      "VOLUME_SPIKE",
            "severity":  "HIGH" if row["ratio"] >= 3.0 else "MEDIUM",
            "direction": "NEUTRAL",
            "headline":  f"Volume surge ({row['ratio']:.1f}× baseline)",
            "message":   "Outsized activity detected.",
        })

    gate_rows = conn.execute("""
        WITH snap_scores AS (
            SELECT snap_time,
                CASE WHEN 100.0 * ABS(SUM(gex_k)) / NULLIF(MAX(ABS(SUM(gex_k))) OVER (ORDER BY snap_time ROWS UNBOUNDED PRECEDING), 0) <= 70 THEN 2 ELSE 0 END AS g_pts,
                CASE WHEN MAX(ABS(ROUND(SUM(gex_k) - LAG(SUM(gex_k), 3) OVER (ORDER BY snap_time), 2))) OVER (ORDER BY snap_time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) > ? THEN 2 ELSE 0 END AS c_pts,
                CASE WHEN ABS(AVG(obi_raw)) > 0.1 THEN 1 ELSE 0 END AS o_pts,
                CASE WHEN ABS(NULLIF(SUM(CASE WHEN option_type='PE' THEN volume ELSE 0 END), 0) / NULLIF(SUM(CASE WHEN option_type='CE' THEN volume ELSE 0 END), 0) - NULLIF(SUM(CASE WHEN option_type='PE' THEN oi ELSE 0 END), 0) / NULLIF(SUM(CASE WHEN option_type='CE' THEN oi ELSE 0 END), 0)) > 0.25 THEN 1 ELSE 0 END AS p_pts
            FROM vw_options WHERE trade_date=? AND underlying=? AND expiry_tier IN ('TIER1_NEAR','TIER1_FAR') AND gex_k IS NOT NULL
            GROUP BY snap_time
        ),
        with_edge AS (
            SELECT snap_time, (g_pts + c_pts + o_pts + p_pts) AS score, LAG(g_pts + c_pts + o_pts + p_pts) OVER (ORDER BY snap_time) AS prev_score
            FROM snap_scores
        )
        SELECT snap_time, score FROM with_edge
        WHERE snap_time <= ? AND prev_score IS NOT NULL AND ((score >= 5 AND prev_score < 5) OR (score < 5 AND prev_score >= 5))
        ORDER BY snap_time
    """, [coc_thresh, trade_date, underlying, snap_time]).fetchdf().to_dict(orient="records")

    for row in gate_rows:
        is_go = row["score"] >= 5
        alerts.append({
            "time":      row["snap_time"],
            "type":      "GATE_CHANGE",
            "severity":  "HIGH",
            "direction": "NEUTRAL",
            "headline":  f"Environment Gate {'UPGRADED' if is_go else 'DOWNGRADED'} (Score: {row['score']})",
            "message": f"Environment score: {row['score']}.",
        })

    alerts.sort(key=lambda x: x["time"])
    return alerts

@router.get("/vex-cex")
def vex_cex(
    trade_date: str = Query(...), underlying: str = Query("NIFTY"),
    snap_time:  str = Query("15:30", pattern=r"^\d{2}:\d{2}$"),
    conn:       duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> dict:
    """Vanna/Charm Exposure for the requested day and underlying."""
    series = get_vanna_charm_exposure(conn, trade_date, underlying)
    by_strike = get_vex_cex_by_strike(conn, trade_date, underlying, snap_time)
    current = next((r for r in reversed(series) if r["snap_time"] <= snap_time), series[-1] if series else {})
    h, m = (int(x) for x in snap_time.split(":"))
    return {
        "series": series, "by_strike": by_strike, "current": current,
        "dealer_oclock": 885 <= (h * 60 + m) <= 930, "interpretation": current.get("interpretation", "No data"),
    }
