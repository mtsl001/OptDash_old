import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine,
} from "recharts";
import { useCoC } from "@/hooks/useMarketData";

interface Props { date: string; underlying: string; }

export function CoCVelocityPanel({ date, underlying }: Props) {
  const { data, isLoading, error } = useCoC(date, underlying);

  // Show loading skeleton — same fixed height as sibling panels
  if (isLoading)
    return (
      <div className="panel flex flex-col" style={{ minHeight: 340 }}>
        <div className="panel-header">CoC Velocity — {underlying}</div>
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
          Loading…
        </div>
      </div>
    );

  // Error or no futures data for this underlying/date
  if (error || !data?.length)
    return (
      <div className="panel flex flex-col" style={{ minHeight: 340 }}>
        <div className="panel-header">CoC Velocity — {underlying}</div>
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
          No futures data for {underlying} on {date}
        </div>
      </div>
    );

  const latest = data[data.length - 1];
  const vCoc = latest.v_coc_15m;
  const isBear = vCoc !== null && vCoc < -10;
  const isBull = vCoc !== null && vCoc > 10;

  const fmt = (v: number | null, dp = 2) =>
    v === null ? "—" : v.toFixed(dp);

  return (
    <div className="panel flex flex-col" style={{ minHeight: 340 }}>
      <div className="panel-header">
        <span>CoC Velocity — {underlying}</span>
        {isBear && <span className="badge bear">⚡ INSTITUTIONAL SHORT</span>}
        {isBull && <span className="badge bull">⚡ INSTITUTIONAL LONG</span>}
      </div>

      <div className="kpi-row">
        <div className="kpi">
          <div className="kpi-label">CoC</div>
          <div
            className="kpi-value font-mono"
            style={{ color: latest.coc < 0 ? "#C0392B" : "#1E7C44" }}
          >
            {latest.coc > 0 ? "+" : ""}{fmt(latest.coc)}
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">V_CoC 15m</div>
          <div
            className="kpi-value font-mono"
            style={{ color: isBear ? "#C0392B" : isBull ? "#1E7C44" : "#808080" }}
          >
            {vCoc !== null ? `${vCoc > 0 ? "+" : ""}${fmt(vCoc)}` : "—"}
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Signal</div>
          <div className="kpi-value text-sm">{latest.signal}</div>
        </div>
      </div>

      <div className="flex-1" style={{ minHeight: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 4, right: 12, left: -8, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#243040" />
            <XAxis
              dataKey="snap_time"
              tick={{ fill: "#808080", fontSize: 10 }}
              interval="preserveStartEnd"
            />
            {/* Left axis: CoC bars */}
            <YAxis
              yAxisId="coc"
              tick={{ fill: "#808080", fontSize: 10 }}
              tickFormatter={(v) => v.toFixed(0)}
              width={36}
            />
            {/* Right axis: V_CoC line */}
            <YAxis
              yAxisId="vel"
              orientation="right"
              tick={{ fill: "#808080", fontSize: 10 }}
              tickFormatter={(v) => v.toFixed(0)}
              width={36}
            />
            <Tooltip
              contentStyle={{ background: "#162030", border: "1px solid #243040" }}
              labelStyle={{ color: "#D6E4F7" }}
              formatter={(value: number, name: string) => [
                typeof value === "number" ? value.toFixed(2) : value,
                name,
              ]}
            />
            <ReferenceLine yAxisId="coc" y={0} stroke="#404040" strokeWidth={1} />
            <ReferenceLine
              yAxisId="vel"
              y={-10}
              stroke="#C0392B"
              strokeDasharray="4 2"
              label={{ value: "-10", fill: "#C0392B", fontSize: 10 }}
            />
            <ReferenceLine
              yAxisId="vel"
              y={10}
              stroke="#1E7C44"
              strokeDasharray="4 2"
              label={{ value: "+10", fill: "#1E7C44", fontSize: 10 }}
            />
            <Bar
              yAxisId="coc"
              dataKey="coc"
              name="CoC"
              fill="#2E75B6"
              opacity={0.75}
              isAnimationActive={false}
            />
            <Line
              yAxisId="vel"
              type="monotone"
              dataKey="v_coc_15m"
              name="V_CoC 15m"
              stroke="#E8A020"
              dot={false}
              strokeWidth={2}
              connectNulls={false}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
