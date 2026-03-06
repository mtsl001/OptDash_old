"""
recommender.py — AI recommendation engine.

Reads all live panel data via analytics.py, infers direction,
scores confidence, picks the best strike, and returns a full trade card.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Optional

import sys
from pathlib import Path
_pipeline = Path(__file__).resolve().parents[1] / "pipeline"
sys.path.insert(0, str(_pipeline))

from config import (
    AI_SL_PCT,
    AI_TARGET_PCT,
    AI_MIN_CONFIDENCE,
    AI_MIN_GATE_SCORE,
    AI_MIN_STARS,
    AI_LOT_SIZES,
)
from analytics import (
    get_environment_score,
    get_top_strikes,
    get_coc_velocity,
    get_pcr_divergence,
    compute_theta_sl,
)

from narrative import generate_narrative
from journal import get_trade_history

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _generate_trade_id() -> str:
    """Generate a short UUID for trade identification."""
    return uuid.uuid4().hex[:12]


def _snap_to_minutes(snap: str) -> int:
    """Convert HH:MM to minutes since midnight."""
    parts = snap.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def star_rating(s_score: float) -> int:
    """Convert S_score to a 1–5 star rating.

    S_score is computed as eff_ratio × (1 - rho), which naturally produces
    values in the 0–5 range. Thresholds calibrated to this distribution.
    """
    if s_score >= 4.0:
        return 5
    if s_score >= 3.0:
        return 4
    if s_score >= 2.0:
        return 3
    if s_score >= 1.0:
        return 2
    return 1


# ── Direction Inference ───────────────────────────────────────────────────────

def _get_latest_vcoc(coc_rows: list[dict]) -> Optional[float]:
    """Extract the latest V_CoC value from CoC timeseries."""
    if not coc_rows:
        return None
    last = coc_rows[-1]
    return last.get("v_coc_15m")


def _get_latest_pcr_div(pcr_rows: list[dict]) -> Optional[float]:
    """Extract the latest PCR divergence value."""
    if not pcr_rows:
        return None
    last = pcr_rows[-1]
    return last.get("pcr_divergence")


def _get_net_vex(conn: Any, trade_date: str, snap_time: str, underlying: str) -> float:
    """Query net VEX at a specific snapshot."""
    try:
        rows = conn.execute("""
            SELECT COALESCE(SUM(vex_k), 0.0) AS net_vex
            FROM vw_options
            WHERE trade_date = ? AND underlying = ?
              AND snap_time  = ?
              AND expiry_tier IN ('TIER1_NEAR', 'TIER1_FAR')
        """, [trade_date, underlying, snap_time]).fetchdf().to_dict(orient="records")
        return float(rows[0].get("net_vex") or 0.0) if rows else 0.0
    except Exception:
        return 0.0


def _get_volume_ratio(conn: Any, trade_date: str, snap_time: str, underlying: str) -> float:
    """Get volume ratio at snap_time vs morning baseline."""
    try:
        rows = conn.execute("""
            WITH baseline AS (
                SELECT AVG(vol_total) AS baseline_vol
                FROM (
                    SELECT snap_time, SUM(volume) AS vol_total
                    FROM vw_options
                    WHERE trade_date=? AND underlying=?
                      AND snap_time BETWEEN '09:15' AND '10:15'
                    GROUP BY snap_time
                )
            ),
            current AS (
                SELECT SUM(volume) AS vol_total
                FROM vw_options
                WHERE trade_date=? AND underlying=? AND snap_time=?
            )
            SELECT ROUND(c.vol_total / NULLIF(b.baseline_vol, 0), 2) AS ratio
            FROM current c CROSS JOIN baseline b
        """, [trade_date, underlying, trade_date, underlying, snap_time]).fetchdf().to_dict(orient="records")
        return float(rows[0].get("ratio") or 1.0) if rows else 1.0
    except Exception:
        return 1.0


def _get_spot(conn: Any, trade_date: str, snap_time: str, underlying: str) -> Optional[float]:
    """Get spot price at snap_time."""
    try:
        rows = conn.execute("""
            SELECT ROUND(AVG(underlying_spot), 2) AS spot
            FROM vw_options
            WHERE trade_date=? AND underlying=? AND snap_time=?
        """, [trade_date, underlying, snap_time]).fetchdf().to_dict(orient="records")
        return float(rows[0]["spot"]) if rows and rows[0].get("spot") else None
    except Exception:
        return None


def infer_direction(
    v_coc: Optional[float],
    net_vex: float,
    pcr_div: Optional[float],
) -> Optional[str]:
    """
    Infer CE/PE direction from signal alignment.

    Decision tree:
      Case 1: V_CoC > +10, VEX > 0, div > 0.25  → CE (strong)
      Case 2: V_CoC < -10, VEX < 0, div < -0.20  → PE (strong)
      Case 3: V_CoC > +10, VEX ≤ 0               → CE (moderate)
      Case 4: V_CoC < -10, VEX ≥ 0               → PE (moderate)
      Case 5: |V_CoC| ≤ 10, VEX > 0, div > 0     → CE (weak)
      Case 6: |V_CoC| ≤ 10, VEX < 0, div < 0     → PE (weak)
      Fallback: conflicting signals                → None
    """
    vc = v_coc or 0.0
    div = pcr_div or 0.0

    # Strong signals
    if vc > 10 and net_vex > 0 and div > 0.25:
        return "CE"
    if vc < -10 and net_vex < 0 and div < -0.20:
        return "PE"

    # Moderate signals
    if vc > 10:
        return "CE"
    if vc < -10:
        return "PE"

    # Weak signals
    if abs(vc) <= 10 and net_vex > 0 and div > 0:
        return "CE"
    if abs(vc) <= 10 and net_vex < 0 and div < 0:
        return "PE"

    # Conflicting — no recommendation
    return None


# ── Confidence Scoring ────────────────────────────────────────────────────────

def compute_confidence(
    env: dict[str, Any],
    v_coc: float,
    pcr_div: float,
    net_vex: float,
    volume_ratio: float,
    s_score: float,
    direction: str,
) -> float:
    """
    Compute a 0–100 confidence score from 7 signals.

    | Signal            | Max Pts | Condition                                    |
    |-------------------|---------|----------------------------------------------|
    | Gate score        | 25      | (score / max_score) × 25                     |
    | V_CoC aligned     | 20      | min(abs(v_coc) / 20, 1) × 20                |
    | VEX aligned       | 15      | VEX sign matches direction                   |
    | PCR divergence    | 15      | Div > 0.25 for CE or < -0.2 for PE           |
    | Volume spike      | 10      | min(volume_ratio / 3, 1) × 10                |
    | S_score quality   | 10      | min(s_score / 20, 1) × 10                    |
    | Charm safe        | 5       | Not in Dealer O'Clock window                 |
    """
    score = 0.0

    # 1. Gate score (max 25)
    gate_score = env.get("score", 0)
    max_score = env.get("max_score", 11)
    score += (gate_score / max(max_score, 1)) * 25

    # 2. V_CoC aligned (max 20)
    vcoc_contribution = min(abs(v_coc) / 20, 1.0) * 20
    # Only add if direction matches V_CoC sign
    if (direction == "CE" and v_coc > 0) or (direction == "PE" and v_coc < 0):
        score += vcoc_contribution

    # 3. VEX aligned (max 15)
    if (direction == "CE" and net_vex > 0) or (direction == "PE" and net_vex < 0):
        score += 15

    # 4. PCR divergence (max 15)
    if (direction == "CE" and pcr_div > 0.25) or (direction == "PE" and pcr_div < -0.20):
        score += 15

    # 5. Volume spike (max 10)
    score += min(volume_ratio / 3, 1.0) * 10

    # 6. S_score quality (max 10)
    score += min(s_score / 20, 1.0) * 10

    # 7. Charm safe (max 5)
    conditions = env.get("conditions", {})
    charm_cond = conditions.get("not_charm_distortion", {})
    if charm_cond.get("met", True):
        score += 5

    return round(min(score, 100.0), 1)


def _collect_fired_signals(
    env: dict[str, Any],
    v_coc: float,
    pcr_div: float,
    net_vex: float,
    volume_ratio: float,
    direction: str,
) -> list[str]:
    """Collect list of signal names that fired for this recommendation."""
    signals: list[str] = []

    conditions = env.get("conditions", {})
    if conditions.get("gex_declining", {}).get("met"):
        signals.append("GEX_DECLINE")
    if conditions.get("vcoc_signal", {}).get("met"):
        if v_coc > 0:
            signals.append("VCOC_BULL")
        else:
            signals.append("VCOC_BEAR")
    if conditions.get("pcr_divergence", {}).get("met"):
        signals.append("PCR_DIVERGENCE")
    if (direction == "CE" and net_vex > 0) or (direction == "PE" and net_vex < 0):
        signals.append("VEX_ALIGNED")
    if volume_ratio >= 2.0:
        signals.append("VOLUME_SPIKE")
    if conditions.get("ivp_cheap", {}).get("met"):
        signals.append("IVP_CHEAP")
    if conditions.get("not_charm_distortion", {}).get("met"):
        signals.append("CHARM_SAFE")

    return signals


# ── Main Recommendation Generator ────────────────────────────────────────────

def generate_recommendation(
    conn: Any,
    trade_date: str,
    snap_time: str,
    underlying: str,
    direction: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """
    Generate a full trade recommendation.

    Steps:
      1. Gate check — hard block on NO_GO
      2. Direction inference (if not specified)
      3. Fetch signals: V_CoC, PCR div, VEX
      4. Strike selection from screener
      5. Confidence scoring
      6. Entry / SL / Target computation
      7. Narrative generation
      8. Build trade card

    Returns None if no valid setup exists.
    """
    # ── Step 1: Gate check ────────────────────────────────────────────────────
    env = get_environment_score(conn, trade_date, snap_time, underlying, direction or "")
    if env["verdict"] == "NO_GO":
        logger.info("Gate NO_GO for %s %s %s — skipping", underlying, trade_date, snap_time)
        return None
    if env["score"] < AI_MIN_GATE_SCORE:
        logger.info("Gate score %d < %d — skipping", env["score"], AI_MIN_GATE_SCORE)
        return None

    # ── Step 2: Fetch signals ────────────────────────────────────────────────
    coc_rows = get_coc_velocity(conn, trade_date, underlying)
    pcr_rows = get_pcr_divergence(conn, trade_date, underlying)

    v_coc = _get_latest_vcoc(coc_rows)
    pcr_div = _get_latest_pcr_div(pcr_rows)
    net_vex = _get_net_vex(conn, trade_date, snap_time, underlying)
    volume_ratio = _get_volume_ratio(conn, trade_date, snap_time, underlying)

    # ── Step 3: Direction inference ──────────────────────────────────────────
    if not direction:
        direction = infer_direction(v_coc, net_vex, pcr_div)
        if not direction:
            logger.info("Cannot infer direction — conflicting signals")
            return None

    direction = direction.upper()

    # ── Step 3b: Dedup — one active recommendation per underlying per day ────
    # Block if any GENERATED or ACCEPTED trade exists for this underlying today.
    # A new recommendation is only allowed after the previous one is resolved
    # (REJECTED, EXPIRED, SL_HIT, TARGET_HIT, etc.)
    recent = get_trade_history(trade_date=trade_date, underlying=underlying, limit=10)
    for t in recent:
        if t["status"] in ("GENERATED", "ACCEPTED"):
            logger.info(
                "Dedup — active %s %s trade %s exists (status=%s), skipping",
                t["underlying"], t["direction"], t["trade_id"], t["status"],
            )
            return None

    # ── Step 4: Strike selection ─────────────────────────────────────────────
    strikes = get_top_strikes(conn, trade_date, underlying, snap_time)
    candidates = [s for s in strikes if s.get("option_type") == direction]
    if not candidates:
        logger.info("No matching %s strikes for %s", direction, underlying)
        return None

    # Filter by minimum star rating
    candidates = [s for s in candidates if star_rating(s.get("s_score", 0)) >= AI_MIN_STARS]
    if not candidates:
        logger.info("No strikes with ≥%d stars", AI_MIN_STARS)
        return None

    best = candidates[0]  # Already sorted by s_score DESC

    # ── Step 5: Confidence scoring ───────────────────────────────────────────
    confidence = compute_confidence(
        env,
        v_coc or 0.0,
        pcr_div or 0.0,
        net_vex,
        volume_ratio,
        best.get("s_score", 0),
        direction,
    )
    if confidence < AI_MIN_CONFIDENCE:
        logger.info("Confidence %.1f < %d — skipping", confidence, AI_MIN_CONFIDENCE)
        return None

    # ── Step 6: Entry / SL / Target ──────────────────────────────────────────
    entry_premium = best["ltp"]
    sl = round(entry_premium * (1 - AI_SL_PCT), 2)
    target = round(entry_premium * (1 + AI_TARGET_PCT), 2)

    # Theta-adjusted SL
    theta_val = best.get("theta", 0)
    theta_sl_result = compute_theta_sl(
        entry_premium, theta_val or 0, AI_SL_PCT,
        snap_time, snap_time, entry_premium,
    )
    theta_sl = theta_sl_result.get("sl_adjusted", sl)

    # Spot price
    entry_spot = _get_spot(conn, trade_date, snap_time, underlying)

    # Lot size
    lot_size = AI_LOT_SIZES.get(underlying, 75)

    # ── Step 7: Collect signals ──────────────────────────────────────────────
    signals_fired = _collect_fired_signals(
        env, v_coc or 0.0, pcr_div or 0.0, net_vex, volume_ratio, direction
    )

    # ── Step 8: Build trade card ─────────────────────────────────────────────
    # DuckDB returns pandas Timestamps and numpy numerics — convert to native
    # Python types so SQLite can bind them.
    trade_card: dict[str, Any] = {
        "trade_id":       _generate_trade_id(),
        "timestamp":      snap_time,
        "trade_date":     trade_date,
        "underlying":     underlying,
        "direction":      direction,
        "strike_price":   float(best["strike_price"]),
        "expiry_date":    str(best["expiry_date"])[:10],  # Timestamp → "YYYY-MM-DD"
        "dte":            int(best.get("dte") or 0),
        "entry_premium":  float(entry_premium),
        "entry_spot":     float(entry_spot) if entry_spot else None,
        "sl":             float(sl),
        "theta_sl":       float(theta_sl) if theta_sl else None,
        "target":         float(target),
        "s_score":        float(best.get("s_score") or 0),
        "stars":          star_rating(float(best.get("s_score") or 0)),
        "confidence":     float(confidence),
        "gate_score":     int(env["score"]),
        "gate_verdict":   str(env["verdict"]),
        "signals_fired":  signals_fired,
        "narrative":      "",    # filled below
        "status":         "GENERATED",
        "lot_size":       int(lot_size),
    }

    # ── Step 9: Narrative ────────────────────────────────────────────────────
    coc_context = {"cur_vcoc": v_coc}
    pcr_context = {"pcr_divergence": pcr_div}
    vex_context = {"net_vex": net_vex}

    trade_card["narrative"] = generate_narrative(
        env, coc_context, pcr_context, vex_context,
        best, confidence, trade_card,
    )

    logger.info(
        "Recommendation: %s %s %s @ ₹%.2f | Confidence %.1f | Gate %d/%d %s",
        underlying, int(best["strike_price"]), direction,
        entry_premium, confidence, env["score"], env["max_score"], env["verdict"],
    )

    return trade_card
