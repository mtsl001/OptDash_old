# Part 6 — AI Trading Bot Layer

> **Status:** Planning · **Phase 1:** Rule-based engine · **Phase 2:** Sonnet 4.6 intelligence  
> **Principle:** The dashboard already computes every signal. This layer is a **synthesiser + explainer + journal** sitting on top.

---

## Architecture Overview

```
                    ┌──────────────────────────────────┐
                    │         Sonnet 4.6 (Phase 2)     │
                    │    Narrative · Pattern Mining     │
                    └──────────┬───────────────────────┘
                               │ optional LLM call
┌──────────────┐     ┌─────────▼──────────┐     ┌──────────────────────┐
│  Live Panel  │────▶│  Recommendation    │────▶│  Trade Card (UI)     │
│  Data Layer  │     │  Engine            │     │  Entry · SL · Target │
│  (existing)  │     │  /ai/recommend     │     │  Signals · Narrative │
└──────────────┘     └─────────┬──────────┘     └──────────┬───────────┘
                               │                           │
                    ┌──────────▼──────────┐     ┌──────────▼───────────┐
                    │  Trade Journal DB   │     │  Position Monitor    │
                    │  SQLite / DuckDB    │     │  (auto-populated)    │
                    │  All trades logged  │     │  Live PnL tracking   │
                    └─────────────────────┘     └──────────────────────┘
```

---

## Component 1 — Recommendation Engine

### Endpoint

```
GET /ai/recommend?trade_date=&snap_time=&underlying=&direction=
```

**Returns:** A single JSON trade card (or `null` if no valid setup exists).

### Decision Tree (Phase 1 — Pure Rules)

```python
def generate_recommendation(conn, trade_date, snap_time, underlying, direction=None):
    """
    Reads all live panel data, scores direction confidence,
    picks the best strike, returns a full trade card.
    """

    # ── Step 1: Gate check ──────────────────────────────────────────────
    env = get_environment_score(conn, trade_date, snap_time, underlying, direction or "")
    if env["verdict"] == "NO_GO":
        return None  # Hard block — never recommend against the Gate

    # ── Step 2: Direction inference (if caller didn't specify) ──────────
    # Uses V_CoC + VEX + PCR divergence to determine CE vs PE
    if not direction:
        direction = infer_direction(conn, trade_date, snap_time, underlying)

    # ── Step 3: Confidence scoring ──────────────────────────────────────
    # Each signal contributes to a 0–100 confidence score
    confidence = compute_confidence(env, coc, pcr, vex, volume)

    # ── Step 4: Strike selection ────────────────────────────────────────
    # Filter screener to matching direction, pick top S_score strike
    strikes = get_strike_screener(conn, trade_date, underlying, snap_time)
    candidates = [s for s in strikes if s["option_type"] == direction]
    if not candidates:
        return None
    best = candidates[0]  # Already sorted by s_score DESC

    # ── Step 5: Entry / SL / Target computation ─────────────────────────
    entry_premium = best["ltp"]
    sl = round(entry_premium * (1 - SL_PCT), 2)         # e.g. 40% max loss
    target = round(entry_premium * (1 + TARGET_PCT), 2)  # e.g. 50% target
    # Theta-adjusted SL from position engine
    theta_sl = compute_theta_adjusted_sl(entry_premium, best["theta"])

    # ── Step 6: Build trade card ────────────────────────────────────────
    return {
        "trade_id":     generate_trade_id(),
        "timestamp":    snap_time,
        "underlying":   underlying,
        "direction":    direction,
        "strike":       best["strike_price"],
        "expiry":       best["expiry_date"],
        "dte":          best["dte"],
        "entry_premium": entry_premium,
        "sl":           sl,
        "theta_sl":     theta_sl,
        "target":       target,
        "s_score":      best["s_score"],
        "stars":        star_rating(best["s_score"]),
        "confidence":   confidence,
        "gate_score":   env["score"],
        "gate_verdict": env["verdict"],
        "signals_fired": collect_fired_signals(env, coc, pcr, vex, volume),
        "narrative":    generate_narrative(env, coc, pcr, vex, best, confidence),
        "status":       "GENERATED",  # → ACCEPTED / REJECTED / EXPIRED
    }
```

### Direction Inference Logic

```
                 V_CoC        VEX         PCR_div       → Direction
                 ─────        ───         ───────         ─────────
Case 1:    V_CoC > +10    VEX > 0     div > 0.25         CE (strong)
Case 2:    V_CoC < -10    VEX < 0     div < -0.20        PE (strong)
Case 3:    V_CoC > +10    VEX ≤ 0     any                CE (moderate)
Case 4:    V_CoC < -10    VEX ≥ 0     any                PE (moderate)
Case 5:    |V_CoC| ≤ 10   VEX > 0     div > 0            CE (weak)
Case 6:    |V_CoC| ≤ 10   VEX < 0     div < 0            PE (weak)
Fallback:  conflicting signals                            null (no reco)
```

### Confidence Score (0–100)

| Signal | Max Points | Condition |
|---|---|---|
| Gate score | 25 | `(score / max_score) × 25` |
| V_CoC aligned | 20 | `min(abs(v_coc) / 20, 1) × 20` |
| VEX aligned | 15 | VEX sign matches direction |
| PCR divergence | 15 | Divergence > 0.25 in CE direction (or < -0.2 for PE) |
| Volume spike | 10 | `min(volume_ratio / 3, 1) × 10` |
| S_score quality | 10 | `min(s_score / 20, 1) × 10` |
| Charm safe | 5 | Not in Dealer O'Clock window |

**Thresholds:**
- ≥ 75 → **HIGH confidence** — full position size
- 50–74 → **MEDIUM** — half size
- < 50 → **LOW** — quarter size or skip

---

## Component 2 — Narrative Engine

### Phase 1: Template-Based

```python
TEMPLATES = {
    "strong_ce": (
        "V_CoC surged to {v_coc:+.1f} (bull threshold +10) with institutional futures "
        "buying. VEX is {vex:+.1f}M — IV compression is forcing dealers to buy, creating "
        "invisible support. PCR divergence at {pcr_div:.2f} signals retail trapped in puts. "
        "{underlying} {strike} CE @ ₹{entry:.2f} scores {stars} ({s_score:.1f}) with "
        "{confidence}% confidence. SL: ₹{sl:.2f} | Target: ₹{target:.2f}."
    ),
    "strong_pe": ...,
    "moderate_ce": ...,
    "moderate_pe": ...,
    "gex_breakout": (
        "GEX has collapsed to {gex_pct:.0f}% of today's peak — dealers are no longer "
        "pinning. Combined with V_CoC at {v_coc:+.1f}, downside acceleration is likely. "
        "{underlying} {strike} PE scores {stars}. Enter at ₹{entry:.2f}, SL ₹{sl:.2f}."
    ),
}
```

### Phase 2: Sonnet 4.6 (Future)

```python
async def generate_ai_narrative(trade_card: dict, market_context: dict) -> str:
    """
    Uses Sonnet 4.6 to generate contextual trade reasoning.
    Prompt includes all live panel data + trade card.
    Constrained to 3-4 sentences, must reference actual numbers.
    """
    prompt = f"""
    You are an expert NSE F&O options trader. Given the current market state:
    - Gate: {market_context['gate_score']}/{market_context['max_score']} ({market_context['verdict']})
    - V_CoC 15m: {market_context['v_coc']}
    - VEX: {market_context['vex']}M, CEX: {market_context['cex']}M
    - GEX: {market_context['gex']}B ({market_context['gex_pct']}% of peak)
    - PCR divergence: {market_context['pcr_div']}
    - Volume ratio: {market_context['vol_ratio']}x

    Explain in 3-4 sentences why buying {trade_card['underlying']}
    {trade_card['strike']} {trade_card['direction']} at ₹{trade_card['entry_premium']}
    makes sense right now. Be specific with numbers. No hedging language.
    """
    return await call_sonnet(prompt)
```

---

## Component 3 — Trade Journal Database

### Schema (SQLite — `data/db/trade_journal.db`)

```sql
CREATE TABLE trades (
    trade_id        TEXT PRIMARY KEY,        -- UUID
    created_at      TEXT NOT NULL,           -- ISO timestamp
    trade_date      TEXT NOT NULL,
    snap_time       TEXT NOT NULL,
    underlying      TEXT NOT NULL,
    direction       TEXT NOT NULL,           -- CE / PE
    strike_price    REAL NOT NULL,
    expiry_date     TEXT NOT NULL,
    dte             INTEGER NOT NULL,

    -- Entry
    entry_premium   REAL NOT NULL,
    entry_spot      REAL,
    sl_initial      REAL NOT NULL,
    theta_sl        REAL,
    target          REAL NOT NULL,

    -- Scoring at entry
    s_score         REAL,
    stars           INTEGER,
    confidence      REAL,
    gate_score      INTEGER,
    gate_verdict    TEXT,

    -- All signals that fired (JSON array)
    signals_fired   TEXT,                   -- '["GEX_DECLINE","VCOC_BULL","VEX_ALIGNED"]'

    -- AI narrative
    narrative       TEXT,

    -- Status lifecycle
    status          TEXT NOT NULL DEFAULT 'GENERATED',
    -- GENERATED → ACCEPTED → CLOSED / SL_HIT / TARGET_HIT / EXPIRED / MANUAL_EXIT
    -- GENERATED → REJECTED
    -- GENERATED → EXPIRED (if not acted on within 2 snaps = 10 min)

    -- User action
    accepted_at     TEXT,
    rejected_at     TEXT,

    -- Exit
    exit_premium    REAL,
    exit_spot       REAL,
    exit_time       TEXT,
    exit_reason     TEXT,                   -- SL_HIT / TARGET_HIT / MANUAL / TIME_EXIT / GATE_EXIT

    -- PnL (filled on exit)
    pnl_points      REAL,                  -- exit_premium - entry_premium
    pnl_pct         REAL,                  -- pnl_points / entry_premium × 100
    lot_size        INTEGER DEFAULT 75,     -- NIFTY=75, BANKNIFTY=30, etc.
    pnl_rupees      REAL,                  -- pnl_points × lot_size

    -- Greek attribution at exit (optional — from PnL attribution engine)
    delta_pnl       REAL,
    gamma_pnl       REAL,
    vega_pnl        REAL,
    theta_pnl       REAL,
    unexplained_pnl REAL
);

CREATE INDEX idx_trades_date ON trades(trade_date);
CREATE INDEX idx_trades_status ON trades(status);
CREATE INDEX idx_trades_underlying ON trades(underlying);

-- Separate table for snap-by-snap tracking of open positions
CREATE TABLE position_snaps (
    trade_id        TEXT NOT NULL REFERENCES trades(trade_id),
    snap_time       TEXT NOT NULL,
    current_ltp     REAL NOT NULL,
    current_spot    REAL,
    current_iv      REAL,
    sl_adjusted     REAL,                  -- theta-adjusted SL at this snap
    unrealised_pnl  REAL,
    pnl_pct         REAL,
    status          TEXT,                  -- IN_TRADE / PROFIT_ZONE / SL_WARNING
    PRIMARY KEY (trade_id, snap_time)
);
```

### PnL Tracking Flow

```
 GENERATED ──accept──▶ ACCEPTED ──track──▶ position_snaps (every 5 min)
     │                     │
     │                     ├── LTP ≤ SL_adjusted   → status=SL_HIT, close
     │                     ├── LTP ≥ target         → status=TARGET_HIT, close
     │                     ├── Gate → NO_GO         → status=GATE_EXIT, close
     │                     ├── 15:25 reached        → status=TIME_EXIT, close
     │                     └── user clicks close    → status=MANUAL, close
     │
     └──ignore──▶ EXPIRED (after 10 min)
     └──reject──▶ REJECTED

 Both ACCEPTED and GENERATED trades track live PnL for backtesting:
   - ACCEPTED: actual PnL (with exit)
   - GENERATED (not accepted): hypothetical PnL (what would have happened)
     → enables "regret analysis" and signal quality measurement
```

---

## Component 4 — Position Monitor (Auto-Populated)

### Current Problem
Position Monitor requires manual entry of: underlying, expiry, strike, option_type, entry_snap, entry_premium. This is friction when the AI Bot already knows all of these.

### Solution: Auto-Link

```
User clicks "Accept" on trade card
    ↓
POST /ai/accept-trade  { trade_id }
    ↓
1. trades.status → ACCEPTED, accepted_at → now
2. Position Monitor auto-populates with trade details
3. Theta-SL series starts tracking from entry snap
4. PnL Attribution starts computing from entry Greeks
5. position_snaps table starts recording per-snap data
    ↓
User sees live SL line + PnL in Position Monitor immediately
    No manual entry needed
```

### UI Flow

```
┌─────────────────────────────────────────────────────────────┐
│  🤖 AI RECOMMENDATION                                 23000 CE  │
│                                                              │
│  ★★★★ S_score 18.4 · Confidence 82% · Gate 8/11 GO         │
│                                                              │
│  V_CoC surged to +14.2 with institutional futures buying.    │
│  VEX is +12.3M — IV compression forcing dealer buying.       │
│  PCR divergence 0.31 signals retail trapped in puts.         │
│  NIFTY 23000 CE @ ₹142.50 · SL ₹85.50 · Target ₹213.75    │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │ ✅ Accept │  │ ❌ Reject │  │ ⏸ Watch  │                   │
│  └──────────┘  └──────────┘  └──────────┘                   │
│                                                              │
│  Signals: ⚡V_CoC  📉GEX  🌊VEX  🔄PCR  💰IVP             │
└─────────────────────────────────────────────────────────────┘
         │ Accept
         ▼
┌─────────────────────────────────────────────────────────────┐
│  Position Monitor — NIFTY 23000 CE · 27-Feb · Entry ₹142.50│
│  [auto-populated, live tracking, theta-SL line, PnL bars]   │
└─────────────────────────────────────────────────────────────┘
```

---

## Component 5 — Learning Engine (Post-30 Trades)

### Win Rate Analysis

```sql
-- After 30+ closed trades, compute signal combo win rates
SELECT
    json_group_array(signal) AS signal_combo,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
    ROUND(AVG(pnl_pct), 2) AS avg_pnl_pct,
    ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate
FROM trades
CROSS JOIN json_each(trades.signals_fired) AS signal
WHERE status IN ('TARGET_HIT', 'SL_HIT', 'MANUAL', 'TIME_EXIT', 'GATE_EXIT')
GROUP BY signal_combo
HAVING COUNT(*) >= 3
ORDER BY win_rate DESC;
```

### Personalised Gate Adjustment

```python
def suggest_gate_adjustments(trades_db) -> dict:
    """
    After 30+ trades, analyse which Gate conditions
    correlate with winning vs losing trades.

    Returns suggested threshold changes.
    """
    # Example output:
    # {
    #   "vcoc_signal":    {"current": 10, "suggested": 12, "reason": "Your CE wins are 78% when V_CoC > 12 vs 52% when 10-12"},
    #   "gex_declining":  {"current": 0.30, "suggested": 0.25, "reason": "70% threshold too aggressive — losses cluster when GEX is 70-75% of peak"},
    #   "confidence_min":  {"current": 50, "suggested": 65, "reason": "Trades with confidence < 65 have 38% win rate"},
    # }
```

### Regret Analysis (Generated But Not Accepted)

```sql
-- What trades did you skip that would have been profitable?
SELECT
    trade_date, underlying, direction, strike_price,
    entry_premium, confidence, gate_score,
    -- Hypothetical PnL: track max price within next 60 min
    max_ltp_60min, ROUND((max_ltp_60min - entry_premium) / entry_premium * 100, 1) AS missed_pnl_pct
FROM trades
WHERE status = 'REJECTED' OR status = 'EXPIRED'
  AND missed_pnl_pct > 20
ORDER BY missed_pnl_pct DESC;
```

---

## API Endpoints (New)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/ai/recommend` | Generate trade recommendation |
| `POST` | `/ai/accept-trade` | Accept a trade → auto-populate Position Monitor |
| `POST` | `/ai/reject-trade` | Reject a trade (logged for regret analysis) |
| `GET` | `/ai/active-trades` | All currently open (ACCEPTED) trades |
| `GET` | `/ai/trade-history` | Paginated journal of all trades |
| `GET` | `/ai/trade/{id}` | Full trade detail + snap history |
| `GET` | `/ai/stats` | Win rate, avg PnL, best signal combos |
| `GET` | `/ai/learning` | Gate adjustment suggestions (after 30+ trades) |
| `GET` | `/ai/regret` | Missed profitable trades analysis |

---

## File Structure

```
src/
├── ai/
│   ├── __init__.py
│   ├── recommender.py          # Direction inference + confidence scoring
│   ├── narrative.py            # Template engine (Phase 1) + Sonnet (Phase 2)
│   ├── journal.py              # SQLite CRUD for trades + position_snaps
│   ├── tracker.py              # Live PnL tracker (hooks into scheduler)
│   ├── learning.py             # Win rate analysis + Gate suggestions
│   └── schemas.py              # Pydantic models for trade cards
├── api/
│   └── routers/
│       └── ai.py               # FastAPI router for /ai/* endpoints
data/
└── db/
    └── trade_journal.db        # SQLite database

dashboard/
└── src/
    ├── components/
    │   └── panels/
    │       ├── AIRecommendPanel.tsx    # Trade card UI
    │       └── TradeJournalPanel.tsx   # History + stats
    └── hooks/
        └── useAIData.ts               # Hooks for /ai/* endpoints
```

---

## Implementation Phases

### Phase 1 — Rule-Based Engine (This Sprint)

| Step | File | Description |
|---|---|---|
| 1 | `src/ai/schemas.py` | Pydantic TradeCard, TradeStatus models |
| 2 | `src/ai/journal.py` | SQLite init, insert, update, query functions |
| 3 | `src/ai/recommender.py` | Direction inference + confidence + trade card builder |
| 4 | `src/ai/narrative.py` | Template-based narrative (5 templates covering CE/PE × strong/moderate + GEX breakout) |
| 5 | `src/ai/tracker.py` | Per-snap PnL updater (runs inside scheduler loop) |
| 6 | `src/ai/learning.py` | Win rate + regret SQL queries |
| 7 | `src/api/routers/ai.py` | All 9 endpoints |
| 8 | `dashboard/.../AIRecommendPanel.tsx` | Trade card UI with Accept/Reject/Watch |
| 9 | `dashboard/.../TradeJournalPanel.tsx` | History table + stats cards |
| 10 | Integrate tracker into `scheduler.py` | Track open positions every 5 min |
| 11 | Auto-populate Position Monitor | On accept, feed trade details to existing panel |

### Phase 2 — Sonnet 4.6 Intelligence (Future)

| Step | Description |
|---|---|
| 1 | Replace template narratives with Sonnet 4.6 API calls |
| 2 | Add pattern recognition: "This setup is similar to Feb 18 12:30 which yielded +32%" |
| 3 | Adaptive confidence weighting based on personal trade history |
| 4 | Pre-market analysis: morning brief with overnight futures + global cues |
| 5 | Exit timing optimisation: "Based on your 30 trades, optimal hold time for this setup is 35 min" |

---

## Configuration (config.py additions)

```python
# ── AI Bot ────────────────────────────────────────────────────
AI_SL_PCT              = 0.40      # 40% max loss on entry premium
AI_TARGET_PCT          = 0.50      # 50% profit target
AI_MIN_CONFIDENCE      = 50       # Don't recommend below this
AI_MIN_GATE_SCORE      = 5        # Hard floor for Gate
AI_MIN_STARS           = 2        # Skip 1-star strikes
AI_EXPIRY_MAX_SNAPS    = 2        # Trade card expires after 2 snaps (10 min)
AI_LOT_SIZES           = {"NIFTY": 75, "BANKNIFTY": 30, "FINNIFTY": 40,
                          "MIDCPNIFTY": 50, "NIFTYNXT50": 25}
TRADE_JOURNAL_DB       = DATA_DIR / "db" / "trade_journal.db"
SONNET_API_KEY         = ""       # Phase 2 — leave empty for Phase 1
SONNET_MODEL           = "claude-sonnet-4-6-20260901"  # Phase 2
```

---

## Dashboard Layout (Updated)

```
┌─────────────────┬──────────────────┬──────────────────┐ Row 1
│ Environment     │ Net GEX          │ CoC Velocity     │
│ Gate (8 conds)  │ Panel            │ Panel            │
├─────────────────┴──────────────────┴──────────────────┤ Row 1.5 (NEW)
│ 🤖 AI Recommendation Card (full-width, collapsible)  │
├─────────────────┬──────────────────┬──────────────────┤ Row 2
│ Strike Screener │ PCR Divergence   │ Alert Feed       │
├──────────────────┬─────────────────┬──────────────────┤ Row 3
│ Volume Velocity  │ IV Term         │ VEX/CEX          │
│ Panel            │ Structure       │ Panel            │
├──────────────────┴─────────────────┴──────────────────┤ Row 4
│ Position Monitor (auto-populated from accepted trade) │
├───────────────────────────────────────────────────────┤ Row 5 (NEW)
│ Trade Journal — History · Stats · Learning Insights   │
└───────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

1. **SQLite not DuckDB for journal** — DuckDB is analytical (read-optimised). Trade journal needs fast single-row inserts, updates, and ACID transactions. SQLite is the right tool.

2. **Both ACCEPTED and GENERATED trades track PnL** — enables regret analysis ("you skipped a +40% trade") and measures signal quality without requiring the user to accept every recommendation.

3. **10-minute expiry on trade cards** — options markets move fast. A recommendation from 15 minutes ago is stale. After 2 snaps (10 min), status auto-transitions to EXPIRED.

4. **Template narratives in Phase 1** — no external API dependency, zero latency, deterministic. Sonnet 4.6 in Phase 2 adds nuance but the system works standalone.

5. **Position Monitor auto-populate, not auto-trade** — the user still decides. "Accept" feeds the trade into Position Monitor. No broker API, no auto-execution. This is a decision-support system, not a trading bot.

6. **Gate is a hard floor** — even if confidence is 95%, a NO_GO Gate = no recommendation. The Gate's whole purpose is to prevent trading in hostile environments.
