"""
ai.py — FastAPI router for the AI Trading Bot endpoints.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Optional, Literal
from datetime import datetime
import duckdb
import json

from deps import get_duckdb
from recommender import generate_recommendation
from journal import (
    insert_trade,
    update_status,
    get_trade,
    get_trade_with_snaps,
    get_active_trades,
    get_trade_history,
)
from tracker import expire_stale_recommendations
from learning import (
    get_trade_stats,
    get_regret_analysis,
    suggest_gate_adjustments,
    get_cumulative_pnl,
)
from schemas import (
    TradeCard,
    AcceptRequest,
    RejectRequest,
    TradeDetail,
    TradeStats,
    RegretTrade,
    LearningInsight,
)

router = APIRouter()

# ── Updated Response Model for History ────────────────────────────────────────

class TradeHistoryItem(BaseModel):
    trade_id:       str
    timestamp:      str = Field(..., alias="snap_time") # Map snap_time to timestamp for frontend
    trade_date:     str
    underlying:     str
    direction:      str
    strike_price:   float
    expiry_date:    str
    dte:            int
    entry_premium:  float
    entry_spot:     Optional[float] = None
    sl:             float = Field(..., alias="sl_initial") # Map sl_initial to sl
    theta_sl:       Optional[float] = None
    target:         float
    s_score:        Optional[float] = None
    stars:          Optional[int] = None
    confidence:     Optional[float] = None
    gate_score:     Optional[int] = None
    gate_verdict:   Optional[str] = None
    signals_fired:  list[str] = [] # Will handle string-to-list conversion in code
    narrative:      Optional[str] = None
    status:         str
    lot_size:       int

    class Config:
        populate_by_name = True

# ──────────────────────────────────────────────────────────────────────────────

@router.get("/recommend", response_model=Optional[TradeCard])
def recommend(
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    snap_time: str = Query(..., pattern=r"^\d{2}:\d{2}$", description="HH:MM e.g. 10:00"),
    underlying: str = Query("NIFTY"),
    direction: Optional[str] = Query(None, description="CE or PE — auto-inferred if empty"),
    conn: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> dict[str, Any] | None:
    """
    Generate a trade recommendation.
    """
    expire_stale_recommendations(trade_date, snap_time)
    card = generate_recommendation(conn, trade_date, snap_time, underlying, direction)
    if card is None:
        return None
    insert_trade(card)
    return card


@router.post("/accept-trade")
def accept_trade(body: AcceptRequest):
    trade = get_trade(body.trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    update_status(body.trade_id, "ACCEPTED", accepted_at=datetime.now().isoformat())
    return {"status": "ACCEPTED", "trade_id": body.trade_id}


@router.post("/reject-trade")
def reject_trade(body: RejectRequest):
    trade = get_trade(body.trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    update_status(body.trade_id, "REJECTED", rejected_at=datetime.now().isoformat())
    return {"status": "REJECTED", "trade_id": body.trade_id}


@router.get("/active-trades", response_model=list[TradeCard])
def active_trades() -> list[dict]:
    return get_active_trades()


@router.get("/trade-history", response_model=list[TradeHistoryItem])
def trade_history(
    trade_date: Optional[str] = Query(None),
    underlying: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> list[dict]:
    """Paginated trade journal history with manual field fixes."""
    rows = get_trade_history(trade_date, underlying, limit, offset)
    for r in rows:
        # 1. Convert JSON string signals_fired to list
        if isinstance(r.get("signals_fired"), str):
            try:
                r["signals_fired"] = json.loads(r["signals_fired"])
            except:
                r["signals_fired"] = []
        elif r.get("signals_fired") is None:
            r["signals_fired"] = []
            
    return rows


@router.get("/trade/{trade_id}", response_model=TradeDetail)
def trade_detail(trade_id: str) -> dict:
    trade = get_trade_with_snaps(trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    return trade


@router.get("/stats", response_model=TradeStats)
def stats() -> dict:
    return get_trade_stats()


@router.get("/pnl-series")
def pnl_series() -> list[dict]:
    return get_cumulative_pnl()


@router.get("/learning", response_model=list[LearningInsight])
def learning() -> list[dict]:
    return suggest_gate_adjustments()


@router.get("/regret", response_model=list[RegretTrade])
def regret() -> list[dict]:
    return get_regret_analysis()
