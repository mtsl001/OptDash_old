"""
learning.py — Win rate analysis, regret analysis, and Gate adjustment suggestions.

All queries run against the SQLite trade journal.
Meaningful analysis requires 30+ closed trades.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any, Optional

import sys
from pathlib import Path
_pipeline = Path(__file__).resolve().parents[1] / "pipeline"
sys.path.insert(0, str(_pipeline))

from config import TRADE_JOURNAL_DB

logger = logging.getLogger(__name__)


def _connect() -> sqlite3.Connection:
    """Return a SQLite connection to the trade journal DB."""
    conn = sqlite3.connect(str(TRADE_JOURNAL_DB))
    conn.row_factory = sqlite3.Row
    return conn


# ── Trade Statistics ──────────────────────────────────────────────────────────

def get_trade_stats() -> dict[str, Any]:
    """
    Aggregate statistics across all closed trades.

    Returns: total_trades, closed_trades, wins, losses, win_rate%,
             avg_pnl_pct, total_pnl_rupees, best/worst trade, avg_confidence.
    """
    conn = _connect()
    try:
        row = conn.execute("""
            SELECT
                COUNT(*)                                             AS total_trades,
                SUM(CASE WHEN status IN ('SL_HIT','TARGET_HIT','MANUAL','TIME_EXIT','GATE_EXIT')
                         THEN 1 ELSE 0 END)                         AS closed_trades,
                SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)       AS wins,
                SUM(CASE WHEN pnl_pct <= 0 AND pnl_pct IS NOT NULL
                         THEN 1 ELSE 0 END)                         AS losses,
                ROUND(AVG(CASE WHEN pnl_pct IS NOT NULL
                               THEN pnl_pct END), 2)                AS avg_pnl_pct,
                ROUND(SUM(COALESCE(pnl_rupees, 0)), 2)              AS total_pnl_rupees,
                ROUND(MAX(COALESCE(pnl_pct, 0)), 1)                 AS best_trade_pct,
                ROUND(MIN(COALESCE(pnl_pct, 0)), 1)                 AS worst_trade_pct,
                ROUND(AVG(confidence), 1)                            AS avg_confidence
            FROM trades
        """).fetchone()

        res = dict(row) if row else {}
        # Fill None with 0
        for k in ["total_trades", "closed_trades", "wins", "losses", "avg_pnl_pct", 
                  "total_pnl_rupees", "best_trade_pct", "worst_trade_pct", "avg_confidence"]:
            if res.get(k) is None:
                res[k] = 0
        
        closed = res.get("closed_trades") or 0
        wins = res.get("wins") or 0
        res["win_rate"] = round(100.0 * wins / max(closed, 1), 1)
        return res
    finally:
        conn.close()


def get_cumulative_pnl() -> list[dict[str, Any]]:
    """
    Return time-series data for cumulative PnL tracking.
    Only includes closed trades with realized PnL.
    """
    conn = _connect()
    try:
        rows = conn.execute("""
            SELECT
                created_at,
                snap_time,
                trade_date,
                pnl_rupees,
                SUM(COALESCE(pnl_rupees, 0)) OVER (ORDER BY created_at ASC) AS cum_pnl_rupees,
                SUM(COALESCE(pnl_pct, 0)) OVER (ORDER BY created_at ASC) AS cum_pnl_pct
            FROM trades
            WHERE status IN ('SL_HIT','TARGET_HIT','MANUAL','TIME_EXIT','GATE_EXIT')
            ORDER BY created_at ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Signal Combo Win Rates ────────────────────────────────────────────────────

def get_signal_combo_winrates() -> list[dict[str, Any]]:
    """
    After 30+ closed trades, compute win rates by individual signal.

    Returns a list of {signal, total_trades, wins, avg_pnl_pct, win_rate}.
    """
    conn = _connect()
    try:
        # Check minimum sample size
        count = conn.execute("""
            SELECT COUNT(*) AS n FROM trades
            WHERE status IN ('SL_HIT','TARGET_HIT','MANUAL','TIME_EXIT','GATE_EXIT')
        """).fetchone()
        if not count or count["n"] < 5:
            return []

        rows = conn.execute("""
            WITH closed AS (
                SELECT trade_id, signals_fired, pnl_pct
                FROM trades
                WHERE status IN ('SL_HIT','TARGET_HIT','MANUAL','TIME_EXIT','GATE_EXIT')
                  AND signals_fired IS NOT NULL
            ),
            exploded AS (
                SELECT
                    trade_id,
                    json_each.value AS signal,
                    pnl_pct
                FROM closed, json_each(closed.signals_fired)
            )
            SELECT
                signal,
                COUNT(*) AS total_trades,
                SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
                ROUND(AVG(pnl_pct), 2) AS avg_pnl_pct,
                ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)
                      / COUNT(*), 1) AS win_rate
            FROM exploded
            GROUP BY signal
            HAVING COUNT(*) >= 3
            ORDER BY win_rate DESC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Regret Analysis ───────────────────────────────────────────────────────────

def get_regret_analysis() -> list[dict[str, Any]]:
    """
    Find trades that were REJECTED or EXPIRED but would have been profitable.

    For now, returns basic info about missed trades with high confidence.
    Full hypothetical P&L tracking requires future price data.
    """
    conn = _connect()
    try:
        rows = conn.execute("""
            SELECT
                trade_id, trade_date, underlying, direction, strike_price,
                entry_premium, confidence, gate_score,
                s_score, stars, narrative
            FROM trades
            WHERE status IN ('REJECTED', 'EXPIRED')
              AND confidence >= 65
            ORDER BY confidence DESC
            LIMIT 50
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Gate Adjustment Suggestions ───────────────────────────────────────────────

def suggest_gate_adjustments() -> list[dict[str, Any]]:
    """
    After 30+ closed trades, suggest Gate threshold adjustments
    based on which conditions correlate with winning vs losing trades.

    Returns a list of {parameter, current, suggested, reason, sample_size}.
    """
    conn = _connect()
    try:
        count = conn.execute("""
            SELECT COUNT(*) AS n FROM trades
            WHERE status IN ('SL_HIT','TARGET_HIT','MANUAL','TIME_EXIT','GATE_EXIT')
        """).fetchone()
        if not count or count["n"] < 30:
            return [{
                "parameter": "insufficient_data",
                "current": 0,
                "suggested": 0,
                "reason": f"Need 30+ closed trades for learning. Currently have {count['n'] if count else 0}.",
                "sample_size": count["n"] if count else 0,
            }]

        suggestions: list[dict[str, Any]] = []

        # Confidence threshold analysis
        conf_rows = conn.execute("""
            SELECT
                CASE
                    WHEN confidence >= 75 THEN 'HIGH (75+)'
                    WHEN confidence >= 60 THEN 'MED-HIGH (60-74)'
                    WHEN confidence >= 50 THEN 'MED (50-59)'
                    ELSE 'LOW (<50)'
                END AS conf_tier,
                COUNT(*) AS n,
                ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate,
                ROUND(AVG(pnl_pct), 2) AS avg_pnl
            FROM trades
            WHERE status IN ('SL_HIT','TARGET_HIT','MANUAL','TIME_EXIT','GATE_EXIT')
            GROUP BY conf_tier
            ORDER BY conf_tier
        """).fetchall()

        for row in conf_rows:
            r = dict(row)
            if r["win_rate"] < 40 and "LOW" in r["conf_tier"]:
                suggestions.append({
                    "parameter": "AI_MIN_CONFIDENCE",
                    "current": 50,
                    "suggested": 60,
                    "reason": f"Trades with {r['conf_tier']} confidence have {r['win_rate']}% win rate (avg P&L {r['avg_pnl']}%). Raising floor could improve quality.",
                    "sample_size": r["n"],
                })

        # Gate score analysis
        gate_rows = conn.execute("""
            SELECT
                gate_score,
                COUNT(*) AS n,
                ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate,
                ROUND(AVG(pnl_pct), 2) AS avg_pnl
            FROM trades
            WHERE status IN ('SL_HIT','TARGET_HIT','MANUAL','TIME_EXIT','GATE_EXIT')
              AND gate_score IS NOT NULL
            GROUP BY gate_score
            HAVING COUNT(*) >= 3
            ORDER BY gate_score
        """).fetchall()

        low_gate_losses = [dict(r) for r in gate_rows if r["win_rate"] < 45 and r["gate_score"] <= 5]
        if low_gate_losses:
            worst = low_gate_losses[0]
            suggestions.append({
                "parameter": "AI_MIN_GATE_SCORE",
                "current": 5,
                "suggested": 6,
                "reason": f"Gate score {worst['gate_score']} trades have {worst['win_rate']}% win rate. Consider raising minimum.",
                "sample_size": worst["n"],
            })

        if not suggestions:
            suggestions.append({
                "parameter": "no_changes",
                "current": 0,
                "suggested": 0,
                "reason": "Current thresholds are performing well. No adjustments needed.",
                "sample_size": count["n"],
            })

        return suggestions
    finally:
        conn.close()
