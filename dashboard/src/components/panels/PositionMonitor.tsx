import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import {
  ComposedChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine,
} from "recharts";

interface PositionInput {
  trade_date: string;
  underlying: string;
  expiry_date: string;
  option_type: "CE" | "PE";
  strike_price: number;
  entry_snap: string;
  entry_premium: number;
  theta_daily: number;
}

interface SlRow {
  current_snap: string;
  current_ltp: number;
  sl_adjusted: number;
  unrealised_pnl: number;
  pnl_pct: number;
  status: string;
}

interface PnlRow {
  snap_time: string;
  iv: number;
  vega: number;   // ₹ per 1 vol-pt (Greek sensitivity)
  delta_pnl: number;
  vega_pnl: number;
  actual_pnl: number;
}

interface IvCrushResult {
  iv_expansion_pts: number;
  vega_pnl: number;
  iv_cushion_pts: number | null;
  iv_crush_warning: boolean;
  action: string;
  message: string;
}

export function PositionMonitor() {
  const [pos, setPos] = useState<PositionInput | null>(null);
  const [form, setForm] = useState({
    trade_date: "2026-02-23",
    underlying: "NIFTY",
    expiry_date: "2026-02-27",
    option_type: "PE",
    strike_price: "25400",
    entry_snap: "09:20",
    entry_premium: "19.95",
    theta_daily: "-6.63",
    iv_at_entry: "",   // filled automatically from first pnl row
  });

  const slQuery = useQuery({
    queryKey: ["thetaSl", pos],
    queryFn: () =>
      api.thetaSlSeries({ ...pos!, max_loss_pct: 0.30 }).then((r) => r.data as SlRow[]),
    enabled: !!pos,
    refetchInterval: 5_000,
  });

  // PnL attribution for IV data (used by IV Crush Guard)
  const pnlQuery = useQuery({
    queryKey: ["pnlAttr", pos],
    queryFn: () =>
      api.pnlAttribution({
        trade_date: pos!.trade_date,
        underlying: pos!.underlying,
        expiry_date: pos!.expiry_date,
        option_type: pos!.option_type,
        strike_price: pos!.strike_price,
        entry_snap: pos!.entry_snap,
        entry_premium: pos!.entry_premium,
      }).then((r) => r.data as PnlRow[]),
    enabled: !!pos,
    refetchInterval: 5_000,
  });

  function handleSubmit() {
    if (!form.trade_date || !form.entry_premium) return;
    setPos({
      trade_date: form.trade_date,
      underlying: form.underlying,
      expiry_date: form.expiry_date,
      option_type: form.option_type as "CE" | "PE",
      strike_price: parseFloat(form.strike_price),
      entry_snap: form.entry_snap,
      entry_premium: parseFloat(form.entry_premium),
      theta_daily: parseFloat(form.theta_daily),
    });
  }

  const sl = slQuery.data;
  const latest = sl?.[sl.length - 1];
  const pnlRows = pnlQuery.data;
  const firstRow = pnlRows?.[0];   // entry snap IV
  const lastRow = pnlRows?.[pnlRows.length - 1];

  // --- IV Crush Guard ---
  // Reuses pnl-attribution data (already fetched). No extra API call.
  //
  // Cushion formula: delta_pnl / vega = how many vol-pts IV can compress
  // before the position reaches break-even.
  //   vega       = sensitivity, ₹ per 1-vol-pt move (from pnl-attribution)
  //   vega_pnl   = accumulated vega P&L so far (NOT the same thing — don't use this)
  //   delta_pnl  = total unrealised P&L
  let ivCrush: IvCrushResult | null = null;
  if (firstRow && lastRow && latest) {
    const ivEntry = firstRow.iv;
    const ivCurrent = lastRow.iv;
    const deltaPnl = latest.unrealised_pnl;  // running total P&L
    const vegaSens = lastRow.vega;            // ₹ per 1 vol-pt (sensitivity)
    const ivExpansion = ivCurrent - ivEntry;
    const absVega = Math.abs(vegaSens);
    let cushion: number | null = null;
    let warning = false;
    if (absVega > 0 && deltaPnl > 0 && ivExpansion > 1.0) {
      // vol-pts of IV collapse before P&L = 0
      cushion = parseFloat((deltaPnl / absVega).toFixed(2));
      warning = cushion < 1.5;
    }
    const action = warning
      ? (cushion !== null && cushion < 0.5
        ? "EXIT — IV cushion exhausted"
        : "CONSIDER PARTIAL EXIT — IV cushion thin")
      : "HOLD";
    ivCrush = {
      iv_expansion_pts: parseFloat(ivExpansion.toFixed(3)),
      vega_pnl: parseFloat((vegaSens * ivExpansion).toFixed(2)),
      iv_cushion_pts: cushion,
      iv_crush_warning: warning,
      action,
      message: warning
        ? `IV expanded +${ivExpansion.toFixed(1)} pts since entry. Cushion: only ${cushion?.toFixed(1)} vol pts before delta gain is wiped.`
        : `IV expanded ${ivExpansion >= 0 ? "+" : ""}${ivExpansion.toFixed(1)} pts. No immediate IV crush risk.`,
    };
  }

  const statusColor = (s: string) =>
    s === "STOP_HIT" ? "#C0392B"
      : s === "PROFIT_ZONE_PARTIAL_EXIT" ? "#E8A020"
        : "#1E7C44";

  return (
    <div className="panel">
      <div className="panel-header">Position Monitor</div>

      {/* Input grid */}
      <div className="grid grid-cols-4 gap-2 mt-2">
        {([
          ["Date", "trade_date", "text"],
          ["Underlying", "underlying", "text"],
          ["Expiry", "expiry_date", "text"],
          ["Type CE/PE", "option_type", "text"],
          ["Strike", "strike_price", "number"],
          ["Entry Snap", "entry_snap", "text"],
          ["Entry LTP ₹", "entry_premium", "number"],
          ["Theta/day ₹", "theta_daily", "number"],
        ] as [string, string, string][]).map(([label, key]) => (
          <div key={key}>
            <label className="text-xs text-ink-muted block mb-1">{label}</label>
            <input
              type="text"
              value={(form as Record<string, string>)[key]}
              onChange={(e) => setForm((f) => ({ ...f, [key]: e.target.value }))}
              className="w-full bg-surface border border-border rounded px-2 py-1 text-xs text-white"
            />
          </div>
        ))}
      </div>
      <button
        onClick={handleSubmit}
        className="mt-3 px-4 py-1.5 bg-brand rounded text-white text-sm font-medium"
      >
        Track Position
      </button>

      {latest && (
        <div className="mt-4 space-y-3">
          {/* KPI row */}
          <div className="kpi-row">
            <div className="kpi">
              <div className="kpi-label">Entry ₹</div>
              <div className="kpi-value">{pos!.entry_premium}</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Current LTP</div>
              <div className="kpi-value text-bull">{latest.current_ltp}</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Theta SL</div>
              <div
                className="kpi-value"
                style={{ color: latest.sl_adjusted > pos!.entry_premium ? "#1E7C44" : "#E8A020" }}
              >
                {latest.sl_adjusted}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">P&L</div>
              <div
                className="kpi-value"
                style={{ color: latest.unrealised_pnl >= 0 ? "#1E7C44" : "#C0392B" }}
              >
                {latest.unrealised_pnl >= 0 ? "+" : ""}{latest.unrealised_pnl}{" "}
                ({latest.pnl_pct}%)
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Status</div>
              <div
                className="kpi-value text-xs uppercase"
                style={{ color: statusColor(latest.status) }}
              >
                {latest.status.replace(/_/g, " ")}
              </div>
            </div>
          </div>

          {/* ── IV Crush Guard Banner ── */}
          {ivCrush && (
            <div
              className="px-3 py-2 rounded text-sm"
              style={{
                background: ivCrush.iv_crush_warning ? "#C0392B22" : "#1E7C4418",
                border: `1px solid ${ivCrush.iv_crush_warning ? "#C0392B" : "#1E7C44"}`,
              }}
            >
              <div className="flex items-center justify-between mb-1">
                <span
                  className="font-bold text-xs uppercase"
                  style={{ color: ivCrush.iv_crush_warning ? "#C0392B" : "#1E7C44" }}
                >
                  {ivCrush.iv_crush_warning ? "⚠ IV CRUSH GUARD" : "✓ IV Crush Safe"}
                </span>
                <span
                  className="text-xs font-mono px-2 py-0.5 rounded"
                  style={{
                    background: ivCrush.iv_crush_warning ? "#C0392B" : "#1E7C44",
                    color: "#fff",
                  }}
                >
                  {ivCrush.action}
                </span>
              </div>
              <div className="text-xs text-ink-muted">{ivCrush.message}</div>
              <div className="flex gap-4 mt-1 text-xs font-mono">
                <span style={{ color: ivCrush.iv_expansion_pts >= 0 ? "#C0392B" : "#1E7C44" }}>
                  IV Δ: {ivCrush.iv_expansion_pts >= 0 ? "+" : ""}{ivCrush.iv_expansion_pts.toFixed(2)} pts
                </span>
                {ivCrush.iv_cushion_pts !== null && (
                  <span style={{ color: ivCrush.iv_cushion_pts < 1.5 ? "#C0392B" : "#1E7C44" }}>
                    Cushion: {ivCrush.iv_cushion_pts.toFixed(2)} vol pts
                  </span>
                )}
              </div>
            </div>
          )}

          {/* Chart */}
          <ResponsiveContainer width="100%" height={160}>
            <ComposedChart data={sl} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#243040" />
              <XAxis dataKey="current_snap" tick={{ fill: "#808080", fontSize: 11 }}
                interval="preserveStartEnd" />
              <YAxis tick={{ fill: "#808080", fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: "#162030", border: "1px solid #243040" }}
                labelStyle={{ color: "#D6E4F7" }}
              />
              <ReferenceLine
                y={pos!.entry_premium}
                stroke="#808080"
                strokeDasharray="4 2"
                label={{ value: "Entry", fill: "#808080", fontSize: 10 }}
              />
              <Line type="monotone" dataKey="current_ltp" name="LTP"
                stroke="#1E7C44" dot={false} strokeWidth={2} isAnimationActive={false} />
              <Line type="monotone" dataKey="sl_adjusted" name="Theta SL"
                stroke="#C0392B" dot={false} strokeWidth={1.5}
                strokeDasharray="5 3" isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
