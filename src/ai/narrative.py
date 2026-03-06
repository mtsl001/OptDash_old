"""
narrative.py — Template-based narrative engine for trade recommendations.

Phase 1: Pure template strings with number interpolation.
Phase 2 (future): Sonnet 4.6 API calls for contextual reasoning.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Templates ─────────────────────────────────────────────────────────────────

TEMPLATES: dict[str, str] = {
    "strong_ce": (
        "V_CoC surged to {v_coc:+.1f} (bull threshold +10) with institutional futures "
        "buying. VEX is {vex:+.1f}M — IV compression is forcing dealers to buy, creating "
        "invisible support. PCR divergence at {pcr_div:.2f} signals retail trapped in puts. "
        "{underlying} {strike} CE @ ₹{entry:.2f} scores {stars}★ ({s_score:.1f}) with "
        "{confidence:.0f}% confidence. SL: ₹{sl:.2f} | Target: ₹{target:.2f}."
    ),
    "strong_pe": (
        "V_CoC plunged to {v_coc:+.1f} (bear threshold −10) — institutions are aggressively "
        "shorting futures. VEX at {vex:+.1f}M confirms dealer selling pressure from IV expansion. "
        "PCR divergence at {pcr_div:.2f} shows retail trapped in calls. "
        "{underlying} {strike} PE @ ₹{entry:.2f} scores {stars}★ ({s_score:.1f}) with "
        "{confidence:.0f}% confidence. SL: ₹{sl:.2f} | Target: ₹{target:.2f}."
    ),
    "moderate_ce": (
        "V_CoC at {v_coc:+.1f} shows institutional buying momentum building. "
        "VEX sign is {vex_sign} — dealer flow is {vex_desc}. "
        "Gate score {gate_score}/{max_score} ({gate_verdict}). "
        "{underlying} {strike} CE @ ₹{entry:.2f} — moderate confidence at {confidence:.0f}%. "
        "S_score {s_score:.1f} ({stars}★). SL: ₹{sl:.2f} | Target: ₹{target:.2f}."
    ),
    "moderate_pe": (
        "V_CoC at {v_coc:+.1f} shows institutional selling pressure. "
        "VEX sign is {vex_sign} — dealer flow is {vex_desc}. "
        "Gate score {gate_score}/{max_score} ({gate_verdict}). "
        "{underlying} {strike} PE @ ₹{entry:.2f} — moderate confidence at {confidence:.0f}%. "
        "S_score {s_score:.1f} ({stars}★). SL: ₹{sl:.2f} | Target: ₹{target:.2f}."
    ),
    "gex_breakout": (
        "GEX has collapsed to {gex_pct:.0f}% of today's peak — dealers are no longer "
        "pinning. Combined with V_CoC at {v_coc:+.1f}, {direction_word} acceleration is likely. "
        "{underlying} {strike} {direction} scores {stars}★. "
        "Enter at ₹{entry:.2f}, SL ₹{sl:.2f}, Target ₹{target:.2f}."
    ),
}


def _select_template(direction: str, confidence: float, gex_pct: float) -> str:
    """Pick the best narrative template based on signal strength."""
    if gex_pct < 40:
        return "gex_breakout"
    if confidence >= 75:
        return f"strong_{direction.lower()}"
    return f"moderate_{direction.lower()}"


def generate_narrative(
    env: dict[str, Any],
    coc_data: dict[str, Any],
    pcr_data: dict[str, Any],
    vex_data: dict[str, Any],
    best_strike: dict[str, Any],
    confidence: float,
    trade_card: dict[str, Any],
) -> str:
    """
    Generate a template-based narrative for a trade recommendation.

    Parameters
    ----------
    env         : Environment score result (from get_environment_score)
    coc_data    : CoC velocity data — needs cur_vcoc
    pcr_data    : PCR divergence data — needs latest divergence
    vex_data    : VEX/CEX data — needs net_vex
    best_strike : Top strike from screener
    confidence  : 0-100 confidence score
    trade_card  : The built trade card dict
    """
    direction = trade_card["direction"]
    v_coc = coc_data.get("cur_vcoc", 0.0) or 0.0
    vex = vex_data.get("net_vex", 0.0) or 0.0
    pcr_div = pcr_data.get("pcr_divergence", 0.0) or 0.0
    gex_pct = env.get("conditions", {}).get("gex_declining", {}).get("value", 100.0) or 100.0

    template_key = _select_template(direction, confidence, gex_pct)
    template = TEMPLATES.get(template_key, TEMPLATES["moderate_ce"])

    # Build template context
    ctx = {
        "v_coc":          v_coc,
        "vex":            vex,
        "pcr_div":        pcr_div,
        "gex_pct":        gex_pct,
        "underlying":     trade_card["underlying"],
        "strike":         int(trade_card["strike_price"]),
        "direction":      direction,
        "direction_word": "downside" if direction == "PE" else "upside",
        "entry":          trade_card["entry_premium"],
        "sl":             trade_card["sl"],
        "target":         trade_card["target"],
        "s_score":        trade_card["s_score"],
        "stars":          trade_card["stars"],
        "confidence":     confidence,
        "gate_score":     env.get("score", 0),
        "max_score":      env.get("max_score", 11),
        "gate_verdict":   env.get("verdict", "WAIT"),
        "vex_sign":       "positive (bullish)" if vex > 0 else "negative (bearish)",
        "vex_desc":       "supportive" if (direction == "CE" and vex > 0) or (direction == "PE" and vex < 0) else "neutral",
    }

    try:
        return template.format(**ctx)
    except (KeyError, ValueError) as exc:
        logger.warning("Narrative template error: %s — falling back", exc)
        return (
            f"{trade_card['underlying']} {int(trade_card['strike_price'])} {direction} "
            f"@ ₹{trade_card['entry_premium']:.2f} — {confidence:.0f}% confidence. "
            f"SL: ₹{trade_card['sl']:.2f} | Target: ₹{trade_card['target']:.2f}."
        )
