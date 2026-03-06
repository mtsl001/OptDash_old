"""
tracker.py — Live P&L tracker for open AI-recommended trades.

Runs inside the scheduler loop (every 5 min) to:
  1. Track all ACCEPTED positions — query latest LTP, compute theta-SL, record snap
  2. Auto-close positions on SL hit, target hit, Gate NO_GO, or 15:25 time exit
  3. Expire stale GENERATED recommendations (not acted on within 2 snaps / 10 min)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import sys
from pathlib import Path
_pipeline = Path(__file__).resolve().parents[1] / "pipeline"
sys.path.insert(0, str(_pipeline))

from config import AI_SL_PCT, AI_LOT_SIZES, AI_EXPIRY_MAX_SNAPS, SCHEDULE_INTERVAL_MIN
from analytics import compute_theta_sl, get_environment_score

from journal import (
    get_active_trades,
    get_generated_trades,
    insert_position_snap,
    update_status,
)

logger = logging.getLogger(__name__)


def _snap_to_min(snap: str) -> int:
    """Convert HH:MM to minutes since midnight."""
    h, m = map(int, snap.split(":"))
    return h * 60 + m


def _query_latest_ltp(
    conn: Any,
    trade_date: str,
    underlying: str,
    expiry_date: str,
    option_type: str,
    strike_price: float,
    snap_time: str,
) -> dict[str, Any] | None:
    """Query the latest LTP, spot, and IV for a specific strike at snap_time."""
    try:
        rows = conn.execute("""
            SELECT
                effective_ltp AS ltp,
                underlying_spot AS spot,
                iv
            FROM vw_options
            WHERE trade_date  = ?
              AND underlying   = ?
              AND expiry_date  = ?
              AND option_type  = ?
              AND strike_price = ?
              AND snap_time    = ?
            LIMIT 1
        """, [trade_date, underlying, expiry_date, option_type,
              strike_price, snap_time]).fetchdf().to_dict(orient="records")
        return rows[0] if rows else None
    except Exception as exc:
        logger.warning("LTP query failed: %s", exc)
        return None


def track_open_positions(conn: Any, trade_date: str, snap_time: str) -> None:
    """
    Track all ACCEPTED trades at the current snap_time.

    For each open position:
      1. Query latest LTP from DuckDB
      2. Compute theta-adjusted SL
      3. Check exit conditions (SL, target, gate, time)
      4. Insert position_snaps row
      5. Auto-close if exit condition is met
    """
    active = get_active_trades()
    if not active:
        return

    current_min = _snap_to_min(snap_time)

    for trade in active:
        # Only track trades for today
        if trade["trade_date"] != trade_date:
            continue

        market_data = _query_latest_ltp(
            conn,
            trade["trade_date"],
            trade["underlying"],
            trade["expiry_date"],
            trade["direction"],
            trade["strike_price"],
            snap_time,
        )
        if not market_data or market_data.get("ltp") is None:
            continue

        current_ltp = market_data["ltp"]
        # Guard against ₹0 LTP data artifacts — deep ITM options don't go to 0
        if current_ltp <= 0:
            continue
        current_spot = market_data.get("spot")
        current_iv = market_data.get("iv")

        entry_premium = trade["entry_premium"]
        theta_daily = trade.get("theta_sl") or 0

        # Compute theta-adjusted SL
        sl_result = compute_theta_sl(
            entry_premium,
            theta_daily,
            AI_SL_PCT,
            trade["snap_time"],
            snap_time,
            current_ltp,
        )
        sl_adjusted = sl_result["sl_adjusted"]

        unrealised_pnl = round(current_ltp - entry_premium, 2)
        pnl_pct = round((unrealised_pnl / entry_premium * 100) if entry_premium > 0 else 0, 1)

        # Determine snap status
        if current_ltp <= sl_adjusted:
            snap_status = "SL_WARNING"
        elif pnl_pct > 0:
            snap_status = "PROFIT_ZONE"
        else:
            snap_status = "IN_TRADE"

        # ── Record snap ──────────────────────────────────────────────────────
        insert_position_snap({
            "trade_id":       trade["trade_id"],
            "snap_time":      snap_time,
            "current_ltp":    round(current_ltp, 2),
            "current_spot":   round(current_spot, 2) if current_spot else None,
            "current_iv":     round(current_iv, 4) if current_iv else None,
            "sl_adjusted":    round(sl_adjusted, 2),
            "unrealised_pnl": unrealised_pnl,
            "pnl_pct":        pnl_pct,
            "status":         snap_status,
        })

        # ── Check exit conditions ────────────────────────────────────────────
        exit_reason = None
        exit_status = None
        lot_size = trade.get("lot_size") or AI_LOT_SIZES.get(trade["underlying"], 75)

        # 1. SL hit
        if current_ltp <= sl_adjusted:
            exit_reason = "SL_HIT"
            exit_status = "SL_HIT"

        # 2. Target hit
        elif current_ltp >= trade["target"]:
            exit_reason = "TARGET_HIT"
            exit_status = "TARGET_HIT"

        # 3. Time exit (15:25 IST)
        elif current_min >= 925:  # 15:25
            exit_reason = "TIME_EXIT"
            exit_status = "TIME_EXIT"

        # 4. Gate NO_GO
        elif exit_reason is None:
            try:
                gate = get_environment_score(
                    conn, trade_date, snap_time, trade["underlying"], trade["direction"]
                )
                if gate["verdict"] == "NO_GO":
                    exit_reason = "GATE_EXIT"
                    exit_status = "GATE_EXIT"
            except Exception:
                pass  # Gate query failure shouldn't block tracking

        # ── Close position if exit triggered ─────────────────────────────────
        if exit_reason and exit_status:
            pnl_points = round(current_ltp - entry_premium, 2)
            pnl_pct_final = round((pnl_points / entry_premium * 100) if entry_premium > 0 else 0, 1)
            pnl_rupees = round(pnl_points * lot_size, 2)

            update_status(
                trade["trade_id"],
                exit_status,
                exit_premium=round(current_ltp, 2),
                exit_spot=round(current_spot, 2) if current_spot else None,
                exit_time=snap_time,
                exit_reason=exit_reason,
                pnl_points=pnl_points,
                pnl_pct=pnl_pct_final,
                pnl_rupees=pnl_rupees,
            )
            logger.info(
                "Trade %s CLOSED — %s | P&L: %.2f pts (%.1f%%) ₹%.2f",
                trade["trade_id"], exit_reason, pnl_points, pnl_pct_final, pnl_rupees,
            )


def expire_stale_recommendations(trade_date: str, current_snap: str) -> int:
    """
    Expire GENERATED trades that haven't been acted on within AI_EXPIRY_MAX_SNAPS.

    Returns the number of expired trades.
    """
    generated = get_generated_trades(trade_date)
    current_min = _snap_to_min(current_snap)
    expired_count = 0

    for trade in generated:
        trade_min = _snap_to_min(trade["snap_time"])
        elapsed_snaps = (current_min - trade_min) / SCHEDULE_INTERVAL_MIN

        if elapsed_snaps >= AI_EXPIRY_MAX_SNAPS:
            update_status(trade["trade_id"], "EXPIRED")
            expired_count += 1
            logger.info(
                "Trade %s EXPIRED — generated at %s, now %s (%.0f snaps elapsed)",
                trade["trade_id"], trade["snap_time"], current_snap, elapsed_snaps,
            )

    return expired_count
