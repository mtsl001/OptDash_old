"""
analytics.py — All analytical queries powering the dashboard.

All functions accept a DuckDB connection and a trade_date string "YYYY-MM-DD".
They return plain Python dicts/lists suitable for JSON serialisation.
"""
import logging
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


# ─── Shared helpers ────────────────────────────────────────────────────────────

def _q(conn: duckdb.DuckDBPyConnection, sql: str, params=None) -> list[dict]:
    """Execute a query and return list of dicts (JSON-serialisable)."""
    try:
        result = conn.execute(sql, params or []).fetchdf()
        return result.to_dict(orient="records")
    except Exception as exc:
        logger.error(f"Query error: {exc} SQL: {sql[:200]}")
        raise


def _q_safe(conn: duckdb.DuckDBPyConnection, sql: str, params=None) -> list[dict]:
    """Like _q but silently returns [] on error."""
    try:
        result = conn.execute(sql, params or []).fetchdf()
        return result.to_dict(orient="records")
    except Exception:
        return []


# ─── 1. Net GEX ───────────────────────────────────────────────────────────────

def get_gex_timeseries(
    conn: duckdb.DuckDBPyConnection,
    trade_date: str,
    underlying: str = "NIFTY",
) -> list[dict]:
    """
    Returns per-snapshot Net GEX (in billions) for the given day.
    Includes 4-tier regime logic based on % of daily peak.
    """
    sql = """
    WITH gex_by_snap AS (
        SELECT
            snap_time,
            SUM(gex_k)                                                  AS gex_all,
            SUM(CASE WHEN expiry_tier='TIER1_NEAR' THEN gex_k ELSE 0 END) AS gex_near,
            SUM(CASE WHEN expiry_tier='TIER1_FAR'  THEN gex_k ELSE 0 END) AS gex_far,
            AVG(underlying_spot)                                         AS spot
        FROM vw_options
        WHERE trade_date = ?
          AND underlying  = ?
          AND expiry_tier IN ('TIER1_NEAR', 'TIER1_FAR')
          AND gex_k IS NOT NULL
        GROUP BY snap_time
    ),
    with_peak AS (
        SELECT
            snap_time, gex_all, gex_near, gex_far, spot,
            MAX(ABS(gex_all)) OVER (
                ORDER BY snap_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_peak
        FROM gex_by_snap
    )
    SELECT
        snap_time,
        ROUND(spot, 2)                                           AS spot,
        ROUND(gex_all  / 1e9, 2)                                AS gex_all_B,
        ROUND(gex_near / 1e9, 2)                                AS gex_near_B,
        ROUND(gex_far  / 1e9, 2)                                AS gex_far_B,
        ROUND(100.0 * ABS(gex_all) / NULLIF(running_peak, 0), 1) AS pct_of_peak,
        CASE 
            WHEN gex_all < 0 THEN 'NEGATIVE_TREND'
            WHEN (100.0 * ABS(gex_all) / NULLIF(running_peak, 0)) >= 60 THEN 'STRONG_CHOP'
            WHEN (100.0 * ABS(gex_all) / NULLIF(running_peak, 0)) >= 30 THEN 'WEAKENING'
            ELSE 'DEALER_RETREAT'
        END AS regime
    FROM with_peak
    ORDER BY snap_time
    """
    return _q(conn, sql, [trade_date, underlying])


# ─── 2. CoC Velocity ──────────────────────────────────────────────────────────

def get_coc_velocity(
    conn: duckdb.DuckDBPyConnection,
    trade_date: str,
    underlying: str = "NIFTY",
) -> list[dict]:
    """
    Returns per-snapshot CoC and 15-min velocity (V_CoC) for near-expiry futures.
    Uses per-underlying thresholds and validates 15-min time gap.
    """
    from config import COC_VELOCITY_THRESHOLDS, COC_VELOCITY_DEFAULT
    thresh = COC_VELOCITY_THRESHOLDS.get(underlying, COC_VELOCITY_DEFAULT)

    sql = """
    WITH
    snap_spot AS (
        SELECT snap_time,
               AVG(underlying_spot) AS spot
        FROM vw_options
        WHERE trade_date = ? AND underlying = ?
        GROUP BY snap_time
    ),
    near_fut AS (
        SELECT f.snap_time,
               f.effective_ltp                          AS fut_price,
               COALESCE(f.underlying_spot, s.spot)      AS spot
        FROM vw_futures f
        LEFT JOIN snap_spot s USING (snap_time)
        WHERE f.trade_date       = ?
          AND f.underlying       = ?
          AND f.instrument_type  = 'FUTIDX'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.snap_time
            ORDER BY f.dte ASC
        ) = 1
    ),
    with_coc AS (
        SELECT snap_time,
               ROUND(fut_price, 2)          AS fut_price,
               ROUND(spot, 2)               AS spot,
               ROUND(fut_price - spot, 2)   AS coc,
               LAG(snap_time, 3) OVER (ORDER BY snap_time) AS prev_snap_time
        FROM near_fut
        WHERE spot IS NOT NULL
    )
    SELECT
        snap_time,
        fut_price,
        spot,
        coc,
        -- V_CoC only valid if gap is exactly 15 mins (3 snaps)
        CASE 
            WHEN prev_snap_time IS NOT NULL AND DATEDIFF('minute', CAST(prev_snap_time || ':00' AS TIME), CAST(snap_time || ':00' AS TIME)) = 15
            THEN ROUND(coc - LAG(coc, 3) OVER (ORDER BY snap_time), 2)
            ELSE NULL
        END AS v_coc_15m,
        CASE
            WHEN coc < 0 AND (coc - LAG(coc, 3) OVER (ORDER BY snap_time)) < -? THEN 'DISCOUNT_VELOCITY'
            WHEN (coc - LAG(coc, 3) OVER (ORDER BY snap_time)) < -? THEN 'VELOCITY_BEAR'
            WHEN (coc - LAG(coc, 3) OVER (ORDER BY snap_time)) >  ? THEN 'VELOCITY_BULL'
            WHEN coc < 0 THEN 'DISCOUNT'
            ELSE 'NORMAL'
        END AS signal
    FROM with_coc
    ORDER BY snap_time
    """
    return _q(conn, sql, [trade_date, underlying, trade_date, underlying, thresh, thresh, thresh])


# ─── 3. Environment Score (8 conditions) ──────────────────────────────────────

def get_environment_score(
    conn: duckdb.DuckDBPyConnection,
    trade_date: str,
    snap_time: str,
    underlying: str = "NIFTY",
    direction: str = "",
) -> dict[str, Any]:
    """
    Evaluate the 8 environment conditions at a specific snapshot.
    Includes persistence fix for V_CoC and direction-aware OBI scoring.
    """
    from config import COC_VELOCITY_THRESHOLDS, COC_VELOCITY_DEFAULT
    coc_thresh = COC_VELOCITY_THRESHOLDS.get(underlying, COC_VELOCITY_DEFAULT)

    results = {}

    # 1. GEX % of daily peak
    gex_data = _q(conn, """
        WITH gex_ts AS (
            SELECT snap_time,
                SUM(gex_k)                  AS gex_all,
                MAX(ABS(SUM(gex_k))) OVER (ORDER BY snap_time ROWS UNBOUNDED PRECEDING) AS running_peak
            FROM vw_options
            WHERE trade_date=? AND underlying=?
              AND expiry_tier IN ('TIER1_NEAR','TIER1_FAR') AND gex_k IS NOT NULL
            GROUP BY snap_time
        )
        SELECT ROUND(100.0 * ABS(gex_all) / NULLIF(running_peak, 0), 1) AS pct_of_peak
        FROM gex_ts WHERE snap_time <= ? ORDER BY snap_time DESC LIMIT 1
    """, [trade_date, underlying, snap_time])
    pct_of_peak = gex_data[0]["pct_of_peak"] if gex_data else 100.0
    cond1 = pct_of_peak <= 70.0
    results["gex_declining"] = {"met": cond1, "value": pct_of_peak, "points": 2}

    # 2. |V_CoC| > thresh within LAST 6 SNAPS (30 min persistence)
    coc_data = _q(conn, """
        WITH snap_spot AS (
            SELECT snap_time, AVG(underlying_spot) AS spot FROM vw_options
            WHERE trade_date=? AND underlying=? GROUP BY snap_time
        ),
        fut AS (
            SELECT f.snap_time, ROUND(f.effective_ltp - COALESCE(f.underlying_spot, s.spot), 2) AS coc
            FROM vw_futures f LEFT JOIN snap_spot s USING (snap_time)
            WHERE f.trade_date=? AND f.underlying=? AND f.instrument_type='FUTIDX'
              AND COALESCE(f.underlying_spot, s.spot) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY f.snap_time ORDER BY f.dte ASC) = 1
        ),
        with_vcoc AS (
            SELECT snap_time, coc,
                ROUND(coc - LAG(coc, 3) OVER (ORDER BY snap_time), 2) AS v_coc,
                LAG(snap_time, 3) OVER (ORDER BY snap_time) AS prev_snap_time
            FROM fut
        )
        SELECT MAX(ABS(v_coc)) AS max_recent_vcoc, LAST(v_coc ORDER BY snap_time) AS cur_vcoc
        FROM (
            SELECT * FROM with_vcoc 
            WHERE snap_time <= ? 
              AND prev_snap_time IS NOT NULL 
              AND DATEDIFF('minute', CAST(prev_snap_time || ':00' AS TIME), CAST(snap_time || ':00' AS TIME)) = 15
            ORDER BY snap_time DESC LIMIT 6
        )
    """, [trade_date, underlying, trade_date, underlying, snap_time])
    max_recent = coc_data[0]["max_recent_vcoc"] if coc_data else 0
    cur_vcoc = coc_data[0]["cur_vcoc"] if coc_data else 0
    cond2 = (max_recent is not None) and (max_recent > coc_thresh)
    results["vcoc_signal"] = {"met": cond2, "value": round(cur_vcoc, 2), "points": 2}

    # 3. FUTIDX OBI direction-aware scoring
    fut_obi = _q(conn, """
        SELECT AVG(obi_raw) AS avg_obi FROM vw_futures
        WHERE trade_date=? AND underlying=? AND snap_time=? AND instrument_type='FUTIDX'
    """, [trade_date, underlying, snap_time])
    avg_obi = fut_obi[0]["avg_obi"] if fut_obi else 0
    if direction.upper() == "CE":
        cond3 = avg_obi > 0.2
    elif direction.upper() == "PE":
        cond3 = avg_obi < -0.2
    else:
        cond3 = abs(avg_obi) > 0.2
    results["fut_bs_ratio"] = {"met": cond3, "value": round(avg_obi, 4), "points": 1}

    # 4. |PCR divergence| > threshold (direction-blind for gate)
    pcr_data = _q(conn, """
        SELECT
            SUM(CASE WHEN option_type='PE' THEN volume ELSE 0 END)::FLOAT /
            NULLIF(SUM(CASE WHEN option_type='CE' THEN volume ELSE 0 END), 0) AS pcr_vol,
            SUM(CASE WHEN option_type='PE' THEN oi ELSE 0 END)::FLOAT /
            NULLIF(SUM(CASE WHEN option_type='CE' THEN oi ELSE 0 END), 0)     AS pcr_oi
        FROM vw_options WHERE trade_date=? AND underlying=? AND snap_time=?
          AND expiry_tier IN ('TIER1_NEAR', 'TIER1_FAR')
    """, [trade_date, underlying, snap_time])
    div = (pcr_data[0]["pcr_vol"] - pcr_data[0]["pcr_oi"]) if pcr_data and pcr_data[0]["pcr_vol"] else 0
    cond4 = (div > 0.25) or (div < -0.20)
    results["pcr_divergence"] = {"met": cond4, "value": round(div, 3), "points": 1}

    # 5. IVP < 50%
    ivp_data = _q(conn, """
        SELECT ivp FROM (
            SELECT trade_date, PERCENT_RANK() OVER (PARTITION BY underlying ORDER BY atm_iv) * 100 AS ivp
            FROM vw_daily_atm_iv WHERE underlying=?
        ) sub WHERE trade_date=?
    """, [underlying, trade_date])
    ivp_val = ivp_data[0]["ivp"] if ivp_data else 100.0
    cond5 = ivp_val < 50
    results["ivp_cheap"] = {"met": cond5, "value": round(ivp_val, 1), "points": 2}

    # 6. Smoothed ATM OBI
    obi_data = _q(conn, """
        SELECT AVG(obi_raw) AS smoothed_obi FROM (
            SELECT obi_raw FROM vw_atm WHERE trade_date=? AND underlying=? AND snap_time<=?
            ORDER BY snap_time DESC LIMIT 5
        ) s
    """, [trade_date, underlying, snap_time])
    sobi = obi_data[0]["smoothed_obi"] if obi_data else 0
    cond6 = abs(sobi) > 0.10
    results["obi_negative"] = {"met": cond6, "value": round(sobi, 4), "points": 1}

    # 7. VEX direction alignment
    vex_rows = _q_safe(conn, """
        SELECT SUM(vex_k) AS net_vex FROM vw_options
        WHERE trade_date=? AND underlying=? AND snap_time=? AND expiry_tier IN ('TIER1_NEAR','TIER1_FAR')
    """, [trade_date, underlying, snap_time])
    net_vex = float(vex_rows[0].get("net_vex") or 0.0) if vex_rows else 0.0
    if direction.upper() == "CE":
        cond7 = net_vex > 0
    elif direction.upper() == "PE":
        cond7 = net_vex < 0
    else:
        cond7 = False
    results["vex_aligned"] = {"met": cond7, "value": round(net_vex, 2), "points": 1}

    # 8. Not in Dealer O'Clock charm window (extended to 15:30)
    dte_rows = _q(conn, "SELECT MIN(dte) AS min_dte FROM vw_options WHERE trade_date=? AND underlying=? AND snap_time=?", [trade_date, underlying, snap_time])
    min_dte = dte_rows[0].get("min_dte") if dte_rows else 10
    h_snap, m_snap = (int(x) for x in snap_time.split(":"))
    t_snap = h_snap * 60 + m_snap
    dealer_oclock = 885 <= t_snap <= 930
    in_charm = bool(min_dte is not None and min_dte <= 1 and dealer_oclock)
    results["not_charm_distortion"] = {"met": not in_charm, "value": min_dte, "points": 1}

    total = sum(v["points"] for v in results.values() if v["met"])
    verdict = "GO" if total >= 5 else "WAIT" if total >= 3 else "NO_GO"

    return {
        "trade_date": trade_date, "snap_time": snap_time, "underlying": underlying,
        "direction": direction.upper() or "ANY", "score": total, "max_score": 11,
        "verdict": verdict, "conditions": results
    }


# ─── 4. IV Percentile ─────────────────────────────────────────────────────────

def get_iv_percentile(conn: duckdb.DuckDBPyConnection, underlying: str = "NIFTY", lookback_days: int = 90) -> list[dict]:
    """Compute IVR and IVP for all available dates using a rolling window."""
    sql = """
    WITH ranked AS (
        SELECT trade_date, underlying, atm_iv,
            MIN(atm_iv) OVER w AS iv_min, MAX(atm_iv) OVER w AS iv_max, PERCENT_RANK() OVER w AS ivp_raw
        FROM vw_daily_atm_iv WHERE underlying = ?
        WINDOW w AS (PARTITION BY underlying ORDER BY trade_date ROWS BETWEEN ? PRECEDING AND CURRENT ROW)
    )
    SELECT trade_date, underlying, ROUND(atm_iv, 2) AS atm_iv,
        ROUND((atm_iv - iv_min) / NULLIF(iv_max - iv_min, 0) * 100, 1) AS ivr,
        ROUND(ivp_raw * 100, 1) AS ivp
    FROM ranked ORDER BY trade_date DESC
    """
    return _q(conn, sql, [underlying, lookback_days - 1])


# ─── 5. IV Term Structure ─────────────────────────────────────────────────────

def get_term_structure(conn, trade_date, underlying, snap_time) -> list[dict]:
    """Returns ATM IV per expiry for the term structure chart."""
    sql = """
    SELECT expiry_date, dte, expiry_tier, ROUND(AVG(iv), 2) AS atm_iv, ROUND(AVG(theta), 2) AS avg_theta
    FROM vw_options WHERE trade_date=? AND underlying=? AND snap_time=? AND in_atm_window=true AND iv > 0
    GROUP BY expiry_date, dte, expiry_tier ORDER BY expiry_date
    """
    return _q(conn, sql, [trade_date, underlying, snap_time])


# ─── 6. Strike Screener (S_score with log-dampened ltp) ──────────────────────

def get_top_strikes(conn, trade_date, underlying, snap_time, v_bench=2_000_000, top_n=20) -> list[dict]:
    """Rank all ATM-window strikes by slippage-adjusted efficiency score."""
    sql = """
    WITH base AS (
        SELECT expiry_date, dte, expiry_tier, option_type, strike_price, effective_ltp AS ltp,
            ROUND(iv, 2) AS iv, delta, theta, gamma, vega, moneyness_pct, volume, oi,
            total_buy_qty AS tbq, total_sell_qty AS tsq,
            EXP(-(2.0 * total_buy_qty * total_sell_qty) / NULLIF((total_buy_qty + total_sell_qty) * CAST(? AS DOUBLE), 0)) AS rho,
            -- eff_ratio with log10(ltp+1) to dampen expensive strike bias
            ABS(delta) * LOG10(effective_ltp + 1) / NULLIF(ABS(theta), 0) AS eff_ratio
        FROM vw_atm WHERE trade_date=? AND underlying=? AND snap_time=? AND iv>0 AND effective_ltp>0
    )
    SELECT *, ROUND(eff_ratio * (1 - rho), 4) AS s_score FROM base WHERE rho < 0.99
    ORDER BY s_score DESC LIMIT ?
    """
    rows = _q(conn, sql, [v_bench, trade_date, underlying, snap_time, top_n])
    for r in rows:
        s = r["s_score"]
        r["stars"] = "★★★★" if s > 20 else "★★★☆" if s > 10 else "★★☆☆" if s > 5 else "★☆☆☆"
        r["liquidity_cr"] = round((r["tbq"] + r["tsq"]) * r["ltp"] / 1e7, 2) # just a proxy
    return rows


# ─── 7. PCR Divergence ───────────────────────────────────────────────────────

def get_pcr_divergence(conn, trade_date, underlying) -> list[dict]:
    from config import PCR_DIVERGENCE_THRESHOLDS, PCR_DIVERGENCE_DEFAULT, PCR_VELOCITY_EMA_ALPHA, PCR_VELOCITY_BASE_THRESH
    thresh = PCR_DIVERGENCE_THRESHOLDS.get(underlying, PCR_DIVERGENCE_DEFAULT)
    
    sql = """
    WITH raw AS (
        SELECT snap_time,
            ROUND(SUM(CASE WHEN option_type='PE' THEN volume ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN option_type='CE' THEN volume ELSE 0 END), 0), 3) AS pcr_vol,
            ROUND(SUM(CASE WHEN option_type='PE' THEN oi ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN option_type='CE' THEN oi ELSE 0 END), 0), 3) AS pcr_oi,
            AVG(obi_raw) AS obi_avg
        FROM vw_options WHERE trade_date=? AND underlying=? AND expiry_tier IN ('TIER1_NEAR','TIER1_FAR')
        GROUP BY snap_time
    )
    SELECT *,
        ROUND(AVG(obi_avg) OVER (ORDER BY snap_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 4) AS smoothed_obi
    FROM raw ORDER BY snap_time
    """
    rows = _q(conn, sql, [trade_date, underlying])
    if not rows: return []
    
    alpha, ema = PCR_VELOCITY_EMA_ALPHA, rows[0]["pcr_vol"] or 1.0
    for i, r in enumerate(rows):
        if r["pcr_vol"]:
            ema = alpha * r["pcr_vol"] + (1 - alpha) * ema
        r["pcr_divergence"] = round(r["pcr_vol"] - r["pcr_oi"], 3) if r["pcr_vol"] else 0
        v = round(ema - (rows[i-3]["ema"] if i>=3 else ema), 4)
        r["ema"] = ema
        r["velocity_signal"] = "CALL_BUYING_SURGE" if v < -PCR_VELOCITY_BASE_THRESH else "PUT_BUYING_SURGE" if v > PCR_VELOCITY_BASE_THRESH else "NEUTRAL"
        r["signal"] = "BULL_TRAP" if r["pcr_divergence"] > thresh["BULL_TRAP"] else "BEAR_TRAP" if r["pcr_divergence"] < thresh["BEAR_TRAP"] else "BALANCED"
    return rows


# ─── 8. P&L Attribution & Theta SL ───────────────────────────────────────────

def compute_theta_sl(entry_premium, theta_daily, max_loss_pct, entry_snap, current_snap, current_ltp):
    def s2m(s): h, m = map(int, s.split(":")); return h*60+m
    t = max(0, s2m(current_snap) - s2m(entry_snap))
    sl = (entry_premium * (1 - max_loss_pct)) + (theta_daily * t / 375.0)
    pnl = current_ltp - entry_premium
    return {
        "entry_premium": entry_premium, "theta_daily": theta_daily, "sl_base": round(entry_premium * (1-max_loss_pct), 2),
        "sl_adjusted": round(sl, 2), "current_ltp": current_ltp, "unrealised_pnl": round(pnl, 2),
        "pnl_pct": round(pnl/entry_premium*100, 1) if entry_premium > 0 else 0, 
        "status": "STOP_HIT" if current_ltp < sl else "IN_TRADE"
    }


# ─── 9. IV Crush Guard ────────────────────────────────────────────────────────

def compute_iv_crush(iv_at_entry, iv_current, delta_pnl, vega, iv_cushion_thresh=1.5):
    """IV Crush Guard with expanded activation."""
    ive = round(iv_current - iv_at_entry, 3)
    is_high_risk = iv_current > iv_at_entry * 1.1 or ive > 0.5
    abs_v = abs(vega)
    cushion = round(delta_pnl / abs_v, 2) if abs_v > 0 else 0
    warning = is_high_risk and (cushion < iv_cushion_thresh)
    return {
        "iv_at_entry": iv_at_entry, "iv_current": iv_current, "iv_crush": ive, "delta_pnl": delta_pnl, "vega": vega,
        "cushion_vol_pts": cushion, "is_safe": not warning, "warning": "IV CRUSH RISK" if warning else "SAFE"
    }


# ─── 10. Vanna/Charm Exposure (VEX/CEX) ──────────────────────────────────────

def get_vanna_charm_exposure(conn, trade_date, underlying) -> list[dict]:
    from config import CEX_THRESHOLDS, CEX_DEFAULT
    th = CEX_THRESHOLDS.get(underlying, CEX_DEFAULT)
    sql = """
    WITH base AS (
        SELECT snap_time, option_type, underlying_spot, dte, vex_k, cex_k FROM vw_options
        WHERE trade_date=? AND underlying=? AND expiry_date=(SELECT MIN(expiry_date) FROM vw_options WHERE trade_date=? AND underlying=?)
    )
    SELECT snap_time, ROUND(SUM(vex_k), 4) AS vex_total_M, ROUND(SUM(cex_k), 4) AS cex_total_M, AVG(underlying_spot) AS spot, MIN(dte) AS dte
    FROM base GROUP BY snap_time ORDER BY snap_time
    """
    rows = _q_safe(conn, sql, [trade_date, underlying, trade_date, underlying])
    for r in rows:
        c = r["cex_total_M"] or 0
        r["cex_signal"] = "STRONG_CHARM_BID" if c > th["STRONG_BID"] else "CHARM_BID" if c > th["BID"] else "CHARM_PRESSURE" if c < th["PRESSURE"] else "NEUTRAL"
        h, m = map(int, r["snap_time"].split(":"))
        r["dealer_oclock"] = 885 <= (h*60+m) <= 930
        r["interpretation"] = f"CEX: {r['cex_signal']} | VEX: {r['vex_total_M']:+.2f}M"
    return rows

def get_vex_cex_by_strike(conn, trade_date, underlying, snap_time):
    sql = """
    SELECT strike_price, option_type, ROUND(moneyness_pct*100, 2) AS moneyness_pct, ROUND(SUM(vex_k), 4) AS vex_M, ROUND(SUM(cex_k), 4) AS cex_M, SUM(oi) AS oi, ROUND(AVG(iv), 2) AS iv
    FROM vw_options WHERE trade_date=? AND underlying=? AND snap_time=? AND ABS(moneyness_pct)<=0.03
    GROUP BY strike_price, option_type, moneyness_pct ORDER BY strike_price
    """
    return _q_safe(conn, sql, [trade_date, underlying, snap_time])
