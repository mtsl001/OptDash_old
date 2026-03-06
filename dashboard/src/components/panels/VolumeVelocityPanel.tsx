import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
    ResponsiveContainer, Cell, ReferenceLine,
} from "recharts";
import { useVolumeVelocity } from "@/hooks/useMarketData";

interface Props { date: string; underlying: string; }

// Color scale: normal → elevated → spike
function ratioColor(ratio: number): string {
    if (ratio >= 3.0) return "#C0392B";   // extreme — red
    if (ratio >= 2.0) return "#E8A020";   // spike — amber
    if (ratio >= 1.5) return "#D6E4F7";   // elevated — light blue
    return "#2E75B6";                      // normal — brand blue
}

// Named formatter to avoid Recharts Formatter generic constraint issues
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function volumeFormatter(value: any, name: any): [string, string] {
    const v = (value as number) ?? 0;
    const label = (name as string) === "volume_ratio" ? "Volume Ratio" : (name as string);
    return [`${v.toFixed(2)}× (${((v - 1) * 100).toFixed(0)}% above baseline)`, label];
}

export function VolumeVelocityPanel({ date, underlying }: Props) {
    const { data, isLoading } = useVolumeVelocity(date, underlying);

    if (isLoading)
        return (
            <div className="panel flex flex-col" style={{ minHeight: 340 }}>
                <div className="panel-header">Volume Velocity — {underlying}</div>
                <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">Loading…</div>
            </div>
        );

    if (!data?.length)
        return (
            <div className="panel flex flex-col" style={{ minHeight: 340 }}>
                <div className="panel-header">Volume Velocity — {underlying}</div>
                <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
                    No volume data for {underlying} on {date}
                </div>
            </div>
        );

    const latest = data[data.length - 1];
    const spikeCount = data.filter((r) => (r.volume_ratio ?? 0) >= 2.0).length;
    const peakRatioRow = data.reduce((a, b) => ((a.volume_ratio ?? 0) > (b.volume_ratio ?? 0) ? a : b));

    return (
        <div className="panel flex flex-col" style={{ minHeight: 340 }}>
            <div className="panel-header">
                <span>Volume Velocity — {underlying}</span>
                {spikeCount > 0 && (
                    <span className="badge" style={{ background: "#E8A020" }}>
                        {spikeCount} spike{spikeCount > 1 ? "s" : ""}
                    </span>
                )}
            </div>

            {/* KPI row */}
            <div className="kpi-row">
                <div className="kpi">
                    <div className="kpi-label">Current ratio</div>
                    <div
                        className="kpi-value font-mono"
                        style={{ color: ratioColor(latest.volume_ratio ?? 1) }}
                    >
                        {(latest.volume_ratio ?? 0).toFixed(2)}×
                    </div>
                </div>
                <div className="kpi">
                    <div className="kpi-label">Peak ratio</div>
                    <div
                        className="kpi-value font-mono"
                        style={{ color: ratioColor(peakRatioRow.volume_ratio ?? 1) }}
                    >
                        {(peakRatioRow.volume_ratio ?? 0).toFixed(2)}× @ {peakRatioRow.snap_time}
                    </div>
                </div>
                <div className="kpi">
                    <div className="kpi-label">Baseline vol</div>
                    <div className="kpi-value font-mono text-ink-muted">
                        {latest.baseline_vol ? (latest.baseline_vol / 1000).toFixed(0) + "K" : "—"}
                    </div>
                </div>
            </div>

            {/* Bar chart — one bar per snap, colored by ratio */}
            <div className="flex-1" style={{ minHeight: 180 }}>
                <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={data} margin={{ top: 4, right: 12, left: -8, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#243040" />
                        <XAxis
                            dataKey="snap_time"
                            tick={{ fill: "#808080", fontSize: 9 }}
                            interval={5}
                        />
                        <YAxis
                            tick={{ fill: "#808080", fontSize: 10 }}
                            tickFormatter={(v) => `${v}×`}
                            width={36}
                        />
                        <Tooltip
                            contentStyle={{ background: "#162030", border: "1px solid #243040" }}
                            labelStyle={{ color: "#D6E4F7" }}
                            formatter={volumeFormatter}
                        />
                        {/* Spike threshold line */}

                        <ReferenceLine
                            y={2}
                            stroke="#E8A020"
                            strokeDasharray="4 2"
                            label={{ value: "2× spike", fill: "#E8A020", fontSize: 9, position: "insideTopRight" }}
                        />
                        <Bar dataKey="volume_ratio" name="Volume Ratio" radius={[2, 2, 0, 0]} isAnimationActive={false}>
                            {data.map((entry, i) => (
                                <Cell key={i} fill={ratioColor(entry.volume_ratio)} />
                            ))}
                        </Bar>
                    </BarChart>
                </ResponsiveContainer>
            </div>

            {/* Legend */}
            <div className="flex gap-3 mt-1 text-xs text-ink-muted justify-end pr-1">
                {[
                    { color: "#2E75B6", label: "Normal" },
                    { color: "#D6E4F7", label: "1.5×+" },
                    { color: "#E8A020", label: "2× spike" },
                    { color: "#C0392B", label: "3×+ extreme" },
                ].map(({ color, label }) => (
                    <span key={label} className="flex items-center gap-1">
                        <span className="w-2 h-2 rounded-sm inline-block" style={{ background: color }} />
                        {label}
                    </span>
                ))}
            </div>
        </div>
    );
}
