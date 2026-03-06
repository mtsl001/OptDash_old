"""
journal.py — SQLite CRUD for the AI trade journal.

Creates and manages the `trades` and `position_snaps` tables.
All trade lifecycle operations (insert, accept, reject, close, query) live here.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import sys
_pipeline = Path(__file__).resolve().parents[1] / "pipeline"
sys.path.insert(0, str(_pipeline))

from config import TRADE_JOURNAL_DB

logger = logging.getLogger(__name__)

# ── Schema DDL ────────────────────────────────────────────────────────────────

_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id        TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    trade_date      TEXT NOT NULL,
    snap_time       TEXT NOT NULL,
    underlying      TEXT NOT NULL,
    direction       TEXT NOT NULL,
    strike_price    REAL NOT NULL,
    expiry_date     TEXT NOT NULL,
    dte             INTEGER NOT NULL,

    entry_premium   REAL NOT NULL,
    entry_spot      REAL,
    sl_initial      REAL NOT NULL,
    theta_sl        REAL,
    target          REAL NOT NULL,

    s_score         REAL,
    stars           INTEGER,
    confidence      REAL,
    gate_score      INTEGER,
    gate_verdict    TEXT,

    signals_fired   TEXT,
    narrative       TEXT,

    status          TEXT NOT NULL DEFAULT 'GENERATED',
    accepted_at     TEXT,
    rejected_at     TEXT,

    exit_premium    REAL,
    exit_spot       REAL,
    exit_time       TEXT,
    exit_reason     TEXT,

    pnl_points      REAL,
    pnl_pct         REAL,
    lot_size        INTEGER DEFAULT 75,
    pnl_rupees      REAL,

    delta_pnl       REAL,
    gamma_pnl       REAL,
    vega_pnl        REAL,
    theta_pnl       REAL,
    unexplained_pnl REAL
);
"""

_SNAPS_DDL = """
CREATE TABLE IF NOT EXISTS position_snaps (
    trade_id        TEXT NOT NULL REFERENCES trades(trade_id),
    snap_time       TEXT NOT NULL,
    current_ltp     REAL NOT NULL,
    current_spot    REAL,
    current_iv      REAL,
    sl_adjusted     REAL,
    unrealised_pnl  REAL,
    pnl_pct         REAL,
    status          TEXT,
    PRIMARY KEY (trade_id, snap_time)
);
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);",
    "CREATE INDEX IF NOT EXISTS idx_trades_underlying ON trades(underlying);",
]


# ── Connection helper ─────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    """Return a SQLite connection to the trade journal DB."""
    TRADE_JOURNAL_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(TRADE_JOURNAL_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db() -> None:
    """Create tables and indexes if they don't exist."""
    conn = _connect()
    try:
        conn.execute(_TRADES_DDL)
        conn.execute(_SNAPS_DDL)
        for idx in _INDEXES:
            conn.execute(idx)
        conn.commit()
        logger.info("Trade journal DB initialised at %s", TRADE_JOURNAL_DB)
    finally:
        conn.close()


# ── Trade CRUD ────────────────────────────────────────────────────────────────

def insert_trade(trade: dict) -> None:
    """Insert a new trade recommendation into the journal."""
    conn = _connect()
    try:
        conn.execute("""
            INSERT INTO trades (
                trade_id, created_at, trade_date, snap_time, underlying, direction,
                strike_price, expiry_date, dte, entry_premium, entry_spot,
                sl_initial, theta_sl, target, s_score, stars, confidence,
                gate_score, gate_verdict, signals_fired, narrative, status, lot_size
            ) VALUES (
                :trade_id, :created_at, :trade_date, :snap_time, :underlying, :direction,
                :strike_price, :expiry_date, :dte, :entry_premium, :entry_spot,
                :sl_initial, :theta_sl, :target, :s_score, :stars, :confidence,
                :gate_score, :gate_verdict, :signals_fired, :narrative, :status, :lot_size
            )
        """, {
            "trade_id":      trade["trade_id"],
            "created_at":    trade.get("created_at", datetime.now().isoformat()),
            "trade_date":    trade["trade_date"],
            "snap_time":     trade["timestamp"],
            "underlying":    trade["underlying"],
            "direction":     trade["direction"],
            "strike_price":  trade["strike_price"],
            "expiry_date":   trade["expiry_date"],
            "dte":           trade["dte"],
            "entry_premium": trade["entry_premium"],
            "entry_spot":    trade.get("entry_spot"),
            "sl_initial":    trade["sl"],
            "theta_sl":      trade.get("theta_sl"),
            "target":        trade["target"],
            "s_score":       trade.get("s_score"),
            "stars":         trade.get("stars"),
            "confidence":    trade.get("confidence"),
            "gate_score":    trade.get("gate_score"),
            "gate_verdict":  trade.get("gate_verdict"),
            "signals_fired": json.dumps(trade.get("signals_fired", [])),
            "narrative":     trade.get("narrative", ""),
            "status":        trade.get("status", "GENERATED"),
            "lot_size":      trade.get("lot_size", 75),
        })
        conn.commit()
        logger.info("Trade %s inserted", trade["trade_id"])
    finally:
        conn.close()


def update_status(
    trade_id: str,
    status: str,
    *,
    accepted_at: Optional[str] = None,
    rejected_at: Optional[str] = None,
    exit_premium: Optional[float] = None,
    exit_spot: Optional[float] = None,
    exit_time: Optional[str] = None,
    exit_reason: Optional[str] = None,
    pnl_points: Optional[float] = None,
    pnl_pct: Optional[float] = None,
    pnl_rupees: Optional[float] = None,
) -> None:
    """Update status and optional exit fields on a trade."""
    conn = _connect()
    try:
        conn.execute("""
            UPDATE trades SET
                status       = ?,
                accepted_at  = COALESCE(?, accepted_at),
                rejected_at  = COALESCE(?, rejected_at),
                exit_premium = COALESCE(?, exit_premium),
                exit_spot    = COALESCE(?, exit_spot),
                exit_time    = COALESCE(?, exit_time),
                exit_reason  = COALESCE(?, exit_reason),
                pnl_points   = COALESCE(?, pnl_points),
                pnl_pct      = COALESCE(?, pnl_pct),
                pnl_rupees   = COALESCE(?, pnl_rupees)
            WHERE trade_id = ?
        """, [
            status, accepted_at, rejected_at,
            exit_premium, exit_spot, exit_time, exit_reason,
            pnl_points, pnl_pct, pnl_rupees,
            trade_id,
        ])
        conn.commit()
        logger.info("Trade %s → %s", trade_id, status)
    finally:
        conn.close()


def get_trade(trade_id: str) -> Optional[dict]:
    """Fetch a single trade by ID."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM trades WHERE trade_id = ?", [trade_id]
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_trade_with_snaps(trade_id: str) -> Optional[dict]:
    """Fetch a trade plus its position_snaps history."""
    trade = get_trade(trade_id)
    if not trade:
        return None
    conn = _connect()
    try:
        snaps = conn.execute(
            "SELECT * FROM position_snaps WHERE trade_id = ? ORDER BY snap_time",
            [trade_id],
        ).fetchall()
        trade["snaps"] = [dict(s) for s in snaps]
        return trade
    finally:
        conn.close()


def get_active_trades() -> list[dict]:
    """Return all currently open (ACCEPTED) trades."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT * FROM trades WHERE status = 'ACCEPTED' ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_trade_history(
    trade_date: Optional[str] = None,
    underlying: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """
    Paginated trade journal history.
    For ACCEPTED trades, includes latest unrealized P&L from position_snaps.
    """
    conn = _connect()
    try:
        # Complex query to get latest snap for each trade
        sql = """
            SELECT 
                t.*,
                s.pnl_pct AS live_pnl_pct,
                s.unrealised_pnl AS live_pnl_rupees
            FROM trades t
            LEFT JOIN (
                SELECT trade_id, pnl_pct, unrealised_pnl, snap_time
                FROM position_snaps
                WHERE (trade_id, snap_time) IN (
                    SELECT trade_id, MAX(snap_time)
                    FROM position_snaps
                    GROUP BY trade_id
                )
            ) s ON t.trade_id = s.trade_id
            WHERE 1=1
        """
        params: list = []
        if trade_date:
            sql += " AND t.trade_date = ?"
            params.append(trade_date)
        if underlying:
            sql += " AND t.underlying = ?"
            params.append(underlying)
        
        sql += " ORDER BY t.created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        rows = conn.execute(sql, params).fetchall()
        
        # Merge live PnL into the main PnL fields for open trades
        result = []
        for r in rows:
            d = dict(r)
            if d["status"] == "ACCEPTED":
                d["pnl_pct"] = d.get("live_pnl_pct")
                # Multiply by lot size for rupees if it's unrealized
                if d.get("live_pnl_rupees") is not None:
                    d["pnl_rupees"] = round(d["live_pnl_rupees"] * d["lot_size"], 2)
            result.append(d)
            
        return result
    finally:
        conn.close()


def get_generated_trades(trade_date: str) -> list[dict]:
    """Return all GENERATED (pending) trades for a given date."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT * FROM trades WHERE status = 'GENERATED' AND trade_date = ? ORDER BY created_at DESC",
            [trade_date],
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Position Snap CRUD ────────────────────────────────────────────────────────

def insert_position_snap(snap: dict) -> None:
    """Record a per-snap tracking row for an open trade."""
    conn = _connect()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO position_snaps (
                trade_id, snap_time, current_ltp, current_spot, current_iv,
                sl_adjusted, unrealised_pnl, pnl_pct, status
            ) VALUES (
                :trade_id, :snap_time, :current_ltp, :current_spot, :current_iv,
                :sl_adjusted, :unrealised_pnl, :pnl_pct, :status
            )
        """, snap)
        conn.commit()
    finally:
        conn.close()


def get_snaps_for_trade(trade_id: str) -> list[dict]:
    """Return all position_snaps for a trade, ordered by time."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT * FROM position_snaps WHERE trade_id = ? ORDER BY snap_time",
            [trade_id],
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
