import {
  ComposedChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine, Area,
} from "recharts";
import { usePCR } from "@/hooks/useMarketData";

interface Props { date: string; underlying: string; }

type TrapSignal = "BULL_TRAP" | "BEAR_TRAP" | "BULL_TRAP_BUILDING" | "BALANCED";
type VelSignal = "DIVERGENCE_ACCEL_BEARISH" | "DIVERGENCE_ACCEL_BULLISH" | "NEUTRAL";

const TRAP_META: Record<TrapSignal, { label: string; color: string; bg: string }> = {
  BULL_TRAP: { label: "⚡ BULL TRAP", color: "#1E7C44", bg: "#1E7C4433" },
  BEAR_TRAP: { label: "⚡ BEAR TRAP", color: "#C0392B", bg: "#C0392B33" },
  BULL_TRAP_BUILDING: { label: "▲ TRAP BUILDING", color: "#E8A020", bg: "#E8A02033" },
  BALANCED: { label: "", color: "#808080", bg: "transparent" },
};

const VEL_META: Record<VelSignal, { label: string; color: string }> = {
  DIVERGENCE_ACCEL_BEARISH: { label: "↑ Put buying accel", color: "#C0392B" },
  DIVERGENCE_ACCEL_BULLISH: { label: "↓ Put selling accel", color: "#1E7C44" },
  NEUTRAL: { label: "Velocity neutral", color: "#808080" },
};

function fmt2(v: number | null | undefined): string {
  if (v == null) return "—";
  return v.toFixed(2);
}
function fmt4(v: number | null | undefined): string {
  if (v == null) return "—";
  return v.toFixed(4);
}
function fmtSign(v: number | null | undefined): string {
  if (v == null) return "—";
  return (v >= 0 ? "+" : "") + v.toFixed(4);
}

export function PCRDivergencePanel({ date, underlying }: Props) {
  const { data, isLoading, error } = usePCR(date, underlying);

  if (isLoading)
    return (
      <div className="panel flex flex-col" style={{ minHeight: 340 }}>
        <div className="panel-header">PCR Divergence — {underlying}</div>
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">Loading…</div>
      </div>
    );

  if (error || !data?.length)
    return (
      <div className="panel flex flex-col" style={{ minHeight: 340 }}>
        <div className="panel-header">PCR Divergence — {underlying}</div>
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
          No data for {underlying} on {date}
        </div>
      </div>
    );

  const latest = data[data.length - 1];
  const trap = (latest.signal ?? "BALANCED") as TrapSignal;
  const velSig = (latest.velocity_signal ?? "NEUTRAL") as VelSignal;
  const trapMeta = TRAP_META[trap] ?? TRAP_META["BALANCED"];
  const velMeta = VEL_META[velSig];
  const divColor = latest.pcr_divergence > 0 ? "#1E7C44"
    : latest.pcr_divergence < 0 ? "#C0392B"
      : "#808080";

  // Dynamic reference lines from API (per-underlying thresholds)
  const bullLine = latest.bull_trap_thresh ?? 0.28;
  const bearLine = latest.bear_trap_thresh ?? -0.20;

  return (
    <div className="panel flex flex-col" style={{ minHeight: 380 }}>
      <div className="panel-header">
        <span>PCR Divergence — {underlying}</span>
        <div className="flex gap-2">
          {trap !== "BALANCED" && (
            <span
              className="badge text-xs font-bold"
              style={{ background: trapMeta.bg, color: trapMeta.color, border: `1px solid ${trapMeta.color}` }}
            >
              {trapMeta.label}
            </span>
          )}
          <span
            className="badge text-xs"
            style={{ color: velMeta.color, background: "#0F1923", border: "1px solid #243040" }}
          >
            {velMeta.label}
          </span>
        </div>
      </div>

      {/* KPI row */}
      <div className="kpi-row">
        <div className="kpi">
          <div className="kpi-label">PCR Vol (retail)</div>
          <div className="kpi-value font-mono text-brand-light">{fmt2(latest.pcr_vol)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">PCR OI (inst.)</div>
          <div className="kpi-value font-mono text-bear">{fmt2(latest.pcr_oi)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Divergence</div>
          <div className="kpi-value font-mono" style={{ color: divColor }}>
            {latest.pcr_divergence >= 0 ? "+" : ""}{fmt2(latest.pcr_divergence)}
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">EMA Velocity</div>
          <div className="kpi-value font-mono" style={{ color: velMeta.color }}>
            {fmtSign(latest.ema_velocity)}
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Smoothed OBI</div>
          <div
            className="kpi-value font-mono"
            style={{ color: (latest.smoothed_obi ?? 0) < 0 ? "#C0392B" : "#1E7C44" }}
          >
            {fmt4(latest.smoothed_obi)}
          </div>
        </div>
      </div>

      {/* Threshold labels */}
      <div className="flex gap-4 px-3 pb-1 text-xs text-ink-muted">
        <span style={{ color: "#1E7C44" }}>
          BULL TRAP ≥ +{(bullLine ?? 0).toFixed(2)} (retail trapped LONG → mkt rips)
        </span>
        <span style={{ color: "#C0392B" }}>
          BEAR TRAP ≤ {(bearLine ?? 0).toFixed(2)} (retail trapped SHORT → mkt drops)
        </span>
      </div>

      {/* Chart */}
      <div className="flex-1" style={{ minHeight: 200 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 4, right: 12, left: -8, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#243040" />
            <XAxis
              dataKey="snap_time"
              tick={{ fill: "#808080", fontSize: 10 }}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fill: "#808080", fontSize: 10 }}
              tickFormatter={(v) => (v ?? 0).toFixed(2)}
              width={40}
            />
            <Tooltip
              contentStyle={{ background: "#162030", border: "1px solid #243040" }}
              labelStyle={{ color: "#D6E4F7" }}
              formatter={(value: number | undefined, name?: string) => [
                typeof value === "number" ? value.toFixed(4) : "—",
                name ?? "",
              ]}
            />
            {/* Per-underlying trap thresholds */}
            <ReferenceLine
              y={bullLine}
              stroke="#1E7C44"
              strokeDasharray="4 2"
              label={{ value: `BULL TRAP ${(bullLine ?? 0).toFixed(2)}`, fill: "#1E7C44", fontSize: 9 }}
            />
            <ReferenceLine
              y={bearLine}
              stroke="#C0392B"
              strokeDasharray="4 2"
              label={{ value: `BEAR TRAP ${(bearLine ?? 0).toFixed(2)}`, fill: "#C0392B", fontSize: 9, position: "insideTopRight" }}
            />
            <ReferenceLine y={0} stroke="#404040" strokeWidth={1} />
            <Line
              type="monotone"
              dataKey="pcr_vol"
              name="PCR Vol (retail)"
              stroke="#2E75B6"
              dot={false}
              strokeWidth={2}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="pcr_oi"
              name="PCR OI (inst.)"
              stroke="#C0392B"
              dot={false}
              strokeWidth={2}
              isAnimationActive={false}
            />
            <Area
              type="monotone"
              dataKey="pcr_divergence"
              name="Divergence"
              stroke="#1E7C44"
              fill="#1E7C44"
              fillOpacity={0.15}
              dot={false}
              strokeWidth={1.5}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="ema_velocity"
              name="EMA Velocity"
              stroke="#E8A020"
              dot={false}
              strokeWidth={1.5}
              strokeDasharray="3 2"
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
