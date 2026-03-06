"""
schemas.py — Pydantic models for the AI Trading Bot layer.

All API responses and internal data structures are typed here.
"""
from __future__ import annotations

import enum
from typing import Optional
from pydantic import BaseModel, Field


class TradeStatus(str, enum.Enum):
    """Lifecycle states for a trade recommendation."""
    GENERATED   = "GENERATED"
    ACCEPTED    = "ACCEPTED"
    REJECTED    = "REJECTED"
    EXPIRED     = "EXPIRED"
    SL_HIT      = "SL_HIT"
    TARGET_HIT  = "TARGET_HIT"
    MANUAL      = "MANUAL"
    TIME_EXIT   = "TIME_EXIT"
    GATE_EXIT   = "GATE_EXIT"


class TradeCard(BaseModel):
    """Full recommendation output — sent to the frontend trade card UI."""
    trade_id:       str
    timestamp:      str
    trade_date:     str
    underlying:     str
    direction:      str            # CE or PE
    strike_price:   float
    expiry_date:    str
    dte:            int
    entry_premium:  float
    entry_spot:     Optional[float] = None
    sl:             float
    theta_sl:       Optional[float] = None
    target:         float
    s_score:        float
    stars:          int
    confidence:     float
    gate_score:     int
    gate_verdict:   str
    signals_fired:  list[str]
    narrative:      str
    status:         str = TradeStatus.GENERATED.value
    lot_size:       int = 75


class AcceptRequest(BaseModel):
    """POST body for accepting a trade."""
    trade_id: str


class RejectRequest(BaseModel):
    """POST body for rejecting a trade."""
    trade_id: str


class PositionSnap(BaseModel):
    """Per-snapshot tracking of an open position."""
    trade_id:       str
    snap_time:      str
    current_ltp:    float
    current_spot:   Optional[float] = None
    current_iv:     Optional[float] = None
    sl_adjusted:    Optional[float] = None
    unrealised_pnl: float
    pnl_pct:        float
    status:         str              # IN_TRADE / PROFIT_ZONE / SL_WARNING


class TradeDetail(BaseModel):
    """Single trade with its snap history."""
    trade_id:       str
    created_at:     str
    trade_date:     str
    snap_time:      str
    underlying:     str
    direction:      str
    strike_price:   float
    expiry_date:    str
    dte:            int
    entry_premium:  float
    entry_spot:     Optional[float] = None
    sl_initial:     float
    theta_sl:       Optional[float] = None
    target:         float
    s_score:        Optional[float] = None
    stars:          Optional[int] = None
    confidence:     Optional[float] = None
    gate_score:     Optional[int] = None
    gate_verdict:   Optional[str] = None
    signals_fired:  Optional[str] = None
    narrative:      Optional[str] = None
    status:         str
    accepted_at:    Optional[str] = None
    rejected_at:    Optional[str] = None
    exit_premium:   Optional[float] = None
    exit_spot:      Optional[float] = None
    exit_time:      Optional[str] = None
    exit_reason:    Optional[str] = None
    pnl_points:     Optional[float] = None
    pnl_pct:        Optional[float] = None
    lot_size:       int = 75
    pnl_rupees:     Optional[float] = None
    snaps:          list[dict] = Field(default_factory=list)


class TradeStats(BaseModel):
    """Aggregate statistics across all closed trades."""
    total_trades:     int = 0
    closed_trades:    int = 0
    wins:             int = 0
    losses:           int = 0
    win_rate:         float = 0.0
    avg_pnl_pct:      float = 0.0
    total_pnl_rupees: float = 0.0
    best_trade_pct:   float = 0.0
    worst_trade_pct:  float = 0.0
    avg_confidence:   float = 0.0
    avg_hold_snaps:   int = 0


class RegretTrade(BaseModel):
    """A trade that was rejected/expired but would have been profitable."""
    trade_id:       str
    trade_date:     str
    underlying:     str
    direction:      str
    strike_price:   float
    entry_premium:  float
    confidence:     float
    gate_score:     Optional[int] = None
    missed_pnl_pct: Optional[float] = None


class LearningInsight(BaseModel):
    """Gate adjustment suggestion based on trade history."""
    parameter:    str
    current:      float
    suggested:    float
    reason:       str
    sample_size:  int
