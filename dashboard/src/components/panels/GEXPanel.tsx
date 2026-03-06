import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine, Legend,
} from "recharts";
import { useGEX } from "@/hooks/useMarketData";

interface Props {
  date: string;
  underlying: string;
}

export function GEXPanel({ date, underlying }: Props) {
  const { data, isLoading, isError } = useGEX(date, underlying);

  if (isLoading) return <PanelSkeleton title="Net GEX" />;
  if (isError || !data?.length)
    return <PanelError title="Net GEX" message="No data" />;

  const latest = data[data.length - 1];
  const isNeg  = latest.gex_all_B < 0;
  const regimeColor = isNeg ? "#C0392B" : "#1E7C44";

  return (
    <div className="panel">
      <div className="panel-header">
        <span>Net GEX — {underlying}</span>
        <span
          className="badge"
          style={{ background: regimeColor }}
        >
          {latest.regime.replace("_", " ")}
        </span>
      </div>

      <div className="kpi-row">
        <KPI label="GEX" value={`${latest.gex_all_B}B`} color={regimeColor} />
        <KPI label="% of Peak" value={`${latest.pct_of_peak}%`}
             color={latest.pct_of_peak <= 70 ? "#C0392B" : "#808080"} />
        <KPI label="Near" value={`${latest.gex_near_B}B`} color="#2E75B6" />
        <KPI label="Far"  value={`${latest.gex_far_B}B`}  color="#E8A020" />
      </div>

      <ResponsiveContainer width="100%" height={200}>
        <ComposedChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#243040" />
          <XAxis dataKey="snap_time" tick={{ fill: "#808080", fontSize: 11 }}
                 interval="preserveStartEnd" />
          <YAxis yAxisId="gex" tick={{ fill: "#808080", fontSize: 11 }}
                 tickFormatter={(v) => `${v}B`} />
          <YAxis yAxisId="pct" orientation="right"
                 tick={{ fill: "#808080", fontSize: 11 }}
                 tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
          <Tooltip
            contentStyle={{ background: "#162030", border: "1px solid #243040" }}
            labelStyle={{ color: "#D6E4F7" }}
            formatter={(val: number, name: string) =>
              [name.includes("pct") ? `${val}%` : `${val}B`, name]}
          />
          <Legend wrapperStyle={{ color: "#808080", fontSize: 12 }} />
          <ReferenceLine yAxisId="pct" y={70} stroke="#C0392B" strokeDasharray="4 2"
                         label={{ value: "30% decline", fill: "#C0392B", fontSize: 10 }} />
          <Bar yAxisId="gex" dataKey="gex_near_B" name="Near GEX"
               fill="#2E75B6" opacity={0.8} />
          <Bar yAxisId="gex" dataKey="gex_far_B"  name="Far GEX"
               fill="#E8A020" opacity={0.7} />
          <Line yAxisId="pct" type="monotone" dataKey="pct_of_peak"
                name="% of Peak" stroke="#C0392B" dot={false} strokeWidth={2} />
        </ComposedChart>
      </ResponsiveContainer>

      {latest.pct_of_peak <= 70 && (
        <div className="alert-banner bear">
          ⚡ GEX at {latest.pct_of_peak}% of peak — suppression weakening
        </div>
      )}
    </div>
  );
}

function KPI({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div className="kpi">
      <div className="kpi-label">{label}</div>
      <div className="kpi-value" style={{ color }}>{value}</div>
    </div>
  );
}
function PanelSkeleton({ title }: { title: string }) {
  return (
    <div className="panel animate-pulse">
      <div className="panel-header">{title}</div>
      <div className="h-48 bg-surface rounded mt-2" />
    </div>
  );
}
function PanelError({ title, message }: { title: string; message: string }) {
  return (
    <div className="panel">
      <div className="panel-header">{title}</div>
      <div className="text-ink-muted text-sm mt-4">{message}</div>
    </div>
  );
}
