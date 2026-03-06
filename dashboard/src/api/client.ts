import axios from "axios";

// Use /api to leverage Vite's proxy (vite.config.ts)
// This avoids CORS issues and URL mangling during local development.
const BASE = "/api";

const http = axios.create({ 
  baseURL: BASE, 
  timeout: 15_000,
  headers: {
    "Content-Type": "application/json",
  }
});

// ── Types ────────────────────────────────────────────────────────────────────

export interface GEXRow {
  snap_time: string;
  spot: number;
  gex_all_B: number;
  gex_near_B: number;
  gex_far_B: number;
  pct_of_peak: number;
  regime: string;
}

export interface CoCRow {
  snap_time: string;
  fut_price: number;
  spot: number;
  coc: number;
  v_coc_15m: number | null;
  signal: string;
}

export interface EnvironmentScore {
  score: number;
  max_score: number;
  verdict: "GO" | "WAIT" | "NO_GO";
  conditions: Record<string, { met: boolean; value: number | null; points: number; note?: string }>;
}

export interface SpotData {
  snap_time: string;
  spot: number;
  day_open: number;
  day_high: number;
  day_low: number;
  change_pct: number;
}


export interface StrikeRow {
  expiry_date: string;
  dte: number;
  expiry_tier: string;
  option_type: "CE" | "PE";
  strike_price: number;
  ltp: number;
  iv: number;
  delta: number;
  theta: number;
  gamma: number;
  vega: number;
  moneyness_pct: number;
  rho: number;
  eff_ratio: number;
  s_score: number;
  liquidity_cr: number;
  stars: string;
}

export interface IVPRow {
  underlying: string;
  atm_iv: number;
  ivr: number;
  ivp: number;
}

export interface TermStructureRow {
  expiry_date: string;
  dte: number;
  expiry_tier: string;
  atm_iv: number;
  avg_theta: number;
}

export interface PCRRow {
  snap_time: string;
  pcr_vol: number;
  pcr_oi: number;
  pcr_divergence: number;
  smoothed_obi: number;
  signal: string;
  velocity_signal: string;
  ema?: number | null;
}

export interface VolumeVelocityRow {
  snap_time: string;
  vol_total: number;
  baseline_vol: number;
  volume_ratio: number;
  signal: string;
}

export interface Alert {
  time: string;
  type: string;
  severity: "HIGH" | "MEDIUM" | "LOW";
  direction?: "BEAR" | "BULL" | "NEUTRAL";
  headline: string;
  message: string;
}

export interface VexCexSeriesRow {
  snap_time: string;
  vex_total_M: number;
  cex_total_M: number;
  spot: number;
  dte: number;
  cex_signal: string;
  dealer_oclock: boolean;
  interpretation: string;
}

export interface VexCexStrikeRow {
  strike_price: number;
  option_type: "CE" | "PE";
  moneyness_pct: number;
  vex_M: number;
  cex_M: number;
  oi: number;
  iv: number;
}

export interface VexCexResponse {
  series: VexCexSeriesRow[];
  by_strike: VexCexStrikeRow[];
  current: VexCexSeriesRow;
  dealer_oclock: boolean;
  interpretation: string;
}

export interface ThetaSL {
  entry_premium: number;
  theta_daily: number;
  sl_base: number;
  sl_adjusted: number;
  current_ltp: number;
  unrealised_pnl: number;
  pnl_pct: number;
  status: "STOP_HIT" | "PROFIT_ZONE_PARTIAL_EXIT" | "GUARANTEED_PROFIT_ZONE" | "IN_TRADE";
}

export interface TradeCard {
  trade_id: string;
  timestamp: string;
  trade_date: string;
  underlying: string;
  direction: "CE" | "PE";
  strike_price: number;
  expiry_date: string;
  dte: number;
  entry_premium: number;
  entry_spot: number | null;
  sl: number;
  theta_sl: number | null;
  target: number;
  s_score: number;
  stars: number;
  confidence: number;
  gate_score: number;
  gate_verdict: "GO" | "WAIT" | "NO_GO";
  signals_fired: string[];
  narrative: string;
  status: string;
  lot_size: number;
}

export interface TradeStats {
  total_trades: number;
  closed_trades: number;
  wins: number;
  losses: number;
  win_rate: number;
  avg_pnl_pct: number;
  total_pnl_rupees: number;
  best_trade_pct: number;
  worst_trade_pct: number;
  avg_confidence: number;
}

export interface RegretTrade {
  trade_id: string;
  trade_date: string;
  underlying: string;
  direction: string;
  strike_price: number;
  entry_premium: number;
  confidence: number;
  gate_score: number | null;
  narrative: string | null;
}

export interface LearningInsight {
  parameter: string;
  current: number;
  suggested: number;
  reason: string;
  sample_size: number;
}

export interface TradeHistoryRow {
  trade_id: string;
  created_at: string;
  trade_date: string;
  snap_time: string;
  underlying: string;
  direction: string;
  strike_price: number;
  expiry_date: string;
  dte: number;
  entry_premium: number;
  sl_initial: number;
  target: number;
  s_score: number | null;
  stars: number | null;
  confidence: number | null;
  gate_score: number | null;
  gate_verdict: string | null;
  signals_fired: string | null;
  narrative: string | null;
  status: string;
  accepted_at: string | null;
  rejected_at: string | null;
  exit_premium: number | null;
  exit_time: string | null;
  exit_reason: string | null;
  pnl_points: number | null;
  pnl_pct: number | null;
  lot_size: number;
  pnl_rupees: number | null;
}

export interface PnLRow {
  snap_time: string;
  ltp: number;
  spot: number;
  iv: number;
  vega: number;   // Greek sensitivity — ₹ per 1 vol-pt move
  delta_pnl: number;
  gamma_pnl: number;
  vega_pnl: number;   // snap P&L from IV change (≠ vega sensitivity)
  theta_pnl: number;
  actual_pnl: number;
  theoretical_pnl: number;
  unexplained: number;
}

export interface PnLSeriesRow {
  created_at: string;
  snap_time: string;
  trade_date: string;
  pnl_rupees: number;
  cum_pnl_rupees: number;
  cum_pnl_pct: number;
}

// ── API functions ────────────────────────────────────────────────────────────

export const api = {
  // Market
  gex: (date: string, underlying = "NIFTY") =>
    http.get<GEXRow[]>("/market/gex", { params: { trade_date: date, underlying } }),

  coc: (date: string, underlying = "NIFTY") =>
    http.get<CoCRow[]>("/market/coc", { params: { trade_date: date, underlying } }),

  environment: (date: string, snapTime: string, underlying = "NIFTY") =>
    http.get<EnvironmentScore>("/market/environment", {
      params: { trade_date: date, snap_time: snapTime, underlying },
    }),

  spot: (date: string, underlying = "NIFTY") =>
    http.get<SpotData>("/market/spot", { params: { trade_date: date, underlying } }),

  alerts: (date: string, snapTime: string, underlying = "NIFTY") =>
    http.get<Alert[]>("/micro/alerts", {
      params: { trade_date: date, snap_time: snapTime, underlying },
    }),

  // Screener
  strikes: (date: string, underlying = "NIFTY", snapTime = "09:20", topN = 20) =>
    http.get<StrikeRow[]>("/screener/strikes", {
      params: { trade_date: date, underlying, snap_time: snapTime, top_n: topN },
    }),

  ivp: (underlying = "NIFTY") =>
    http.get<IVPRow>("/screener/ivp", {
      params: { underlying },
    }),

  termStructure: (date: string, underlying = "NIFTY", snapTime = "15:30") =>
    http.get<TermStructureRow[]>("/screener/term-structure", {
      params: { trade_date: date, underlying, snap_time: snapTime },
    }),

  // Microstructure
  pcr: (date: string, underlying = "NIFTY") =>
    http.get<PCRRow[]>("/micro/pcr", { params: { trade_date: date, underlying } }),

  volumeVelocity: (date: string, underlying = "NIFTY") =>
    http.get<VolumeVelocityRow[]>("/micro/volume-velocity", {
      params: { trade_date: date, underlying },
    }),

  vexCex: (date: string, underlying = "NIFTY", snapTime = "15:30") =>
    http.get<VexCexResponse>("/micro/vex-cex", {
      params: { trade_date: date, underlying, snap_time: snapTime },
    }),

  // Position
  thetaSlSeries: (params: {
    trade_date: string; underlying: string; expiry_date: string;
    option_type: string; strike_price: number; entry_snap: string;
    entry_premium: number; theta_daily: number; max_loss_pct?: number;
  }) => http.get<ThetaSL[]>("/position/theta-sl-series", { params }),

  pnlAttribution: (params: {
    trade_date: string; underlying: string; expiry_date: string;
    option_type: string; strike_price: number;
    entry_snap: string; entry_premium: number;
  }) => http.get<PnLRow[]>("/position/pnl-attribution", { params }),

  // AI Bot
  aiRecommend: (date: string, snapTime: string, underlying = "NIFTY", direction?: string) =>
    http.get<TradeCard | null>("/ai/recommend", {
      params: { trade_date: date, snap_time: snapTime, underlying, ...(direction ? { direction } : {}) },
    }),

  aiAccept: (tradeId: string) =>
    http.post<{ status: string; trade_id: string }>("/ai/accept-trade", { trade_id: tradeId }),

  aiReject: (tradeId: string) =>
    http.post<{ status: string; trade_id: string }>("/ai/reject-trade", { trade_id: tradeId }),

  aiActiveTrades: () =>
    http.get<TradeHistoryRow[]>("/ai/active-trades"),

  aiHistory: (tradeDate?: string, underlying?: string, limit = 50, offset = 0) =>
    http.get<TradeHistoryRow[]>("/ai/trade-history", {
      params: { ...(tradeDate ? { trade_date: tradeDate } : {}), ...(underlying ? { underlying } : {}), limit, offset },
    }),

  aiTrade: (tradeId: string) =>
    http.get<TradeHistoryRow>(`/ai/trade/${tradeId}`),

  aiStats: () =>
    http.get<TradeStats>("/ai/stats"),

  aiPnLSeries: () =>
    http.get<PnLSeriesRow[]>("/ai/pnl-series"),

  aiLearning: () =>
    http.get<LearningInsight[]>("/ai/learning"),

  aiRegret: () =>
    http.get<RegretTrade[]>("/ai/regret"),
};
