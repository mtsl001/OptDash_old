import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
    ResponsiveContainer,
} from "recharts";
import { useTermStructure } from "@/hooks/useMarketData";

interface Props { date: string; underlying: string; snapTime: string; }

// Custom dot — amber for near-term, blue for far-term
function CustomDot(props: {
    cx?: number; cy?: number;
    payload?: { atm_iv: number; expiry_tier: string };
}) {
    const { cx = 0, cy = 0, payload } = props;
    if (!payload) return null;
    const isNear = payload.expiry_tier === "TIER1_NEAR";
    return (
        <circle
            cx={cx} cy={cy}
            r={isNear ? 5 : 3}
            fill={isNear ? "#E8A020" : "#2E75B6"}
            stroke="#0F1923" strokeWidth={1}
        />
    );
}

// Named formatter — avoids Recharts Formatter generic constraint issues
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function ivFormatter(v: any, n: any): [string, string] {
    return [`${(v as number).toFixed(2)}%`, n === "atm_iv" ? "ATM IV" : (n as string)];
}

export function TermStructurePanel({ date, underlying, snapTime }: Props) {
    const { data, isLoading } = useTermStructure(date, underlying, snapTime);

    if (isLoading)
        return (
            <div className="panel flex flex-col" style={{ minHeight: 340 }}>
                <div className="panel-header">IV Term Structure — {underlying}</div>
                <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">Loading…</div>
            </div>
        );

    if (!data?.length)
        return (
            <div className="panel flex flex-col" style={{ minHeight: 340 }}>
                <div className="panel-header">IV Term Structure — {underlying}</div>
                <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
                    No IV data for {underlying} on {date}
                </div>
            </div>
        );

    const nearExp = data.find((r) => r.expiry_tier === "TIER1_NEAR");
    const farExp = data.find((r) => r.expiry_tier === "TIER1_FAR");
    const nearIV = nearExp?.atm_iv ?? null;
    const farIV = farExp?.atm_iv ?? null;
    const isInvert = nearIV !== null && farIV !== null && nearIV > farIV;
    const isFlat = nearIV !== null && farIV !== null && Math.abs(nearIV - farIV) < 1;
    const shape = isInvert ? "BACKWARDATION" : isFlat ? "FLAT" : "CONTANGO";
    const shapeColor = isInvert ? "#C0392B" : isFlat ? "#E8A020" : "#1E7C44";
    const interpretation = isInvert
        ? "Near-term IV > Far-term IV — market pricing in immediate fear/event risk. Options are expensive."
        : isFlat
            ? "IV is flat across expiries — no directional event premium. Neutral structure."
            : "Normal contango — far expiries at premium. Vol decaying toward near expiry.";

    const chartData = data.map((r) => ({ ...r, label: `${r.dte}d` }));

    return (
        <div className="panel flex flex-col" style={{ minHeight: 340 }}>
            <div className="panel-header">
                <span>IV Term Structure — {underlying}</span>
                <span className="badge" style={{ background: shapeColor }}>{shape}</span>
            </div>

            {/* KPIs */}
            <div className="kpi-row">
                <div className="kpi">
                    <div className="kpi-label">Near IV</div>
                    <div className="kpi-value font-mono" style={{ color: "#E8A020" }}>
                        {nearIV !== null ? `${nearIV.toFixed(1)}%` : "—"}
                    </div>
                </div>
                <div className="kpi">
                    <div className="kpi-label">Far IV</div>
                    <div className="kpi-value font-mono" style={{ color: "#2E75B6" }}>
                        {farIV !== null ? `${farIV.toFixed(1)}%` : "—"}
                    </div>
                </div>
                <div className="kpi">
                    <div className="kpi-label">Spread</div>
                    <div className="kpi-value font-mono" style={{ color: isInvert ? "#C0392B" : "#1E7C44" }}>
                        {nearIV !== null && farIV !== null ? `${(nearIV - farIV).toFixed(1)}%` : "—"}
                    </div>
                </div>
            </div>

            {/* Interpretation */}
            <div className="text-xs text-ink-muted px-1 mb-1 leading-snug">{interpretation}</div>

            {/* Chart */}
            <div className="flex-1" style={{ minHeight: 160 }}>
                <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData} margin={{ top: 8, right: 20, left: -8, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#243040" />
                        <XAxis dataKey="label" tick={{ fill: "#808080", fontSize: 10 }} />
                        <YAxis
                            tick={{ fill: "#808080", fontSize: 10 }}
                            tickFormatter={(v) => `${v}%`}
                            width={40}
                            domain={["auto", "auto"]}
                        />
                        <Tooltip
                            contentStyle={{ background: "#162030", border: "1px solid #243040" }}
                            labelStyle={{ color: "#D6E4F7" }}
                            formatter={ivFormatter}
                        />
                        <Line
                            type="monotone"
                            dataKey="atm_iv"
                            name="atm_iv"
                            stroke="#2E75B6"
                            strokeWidth={2}
                            dot={<CustomDot />}
                            isAnimationActive={false}
                        />
                    </LineChart>
                </ResponsiveContainer>
            </div>

            {/* Legend */}
            <div className="flex gap-3 mt-1 text-xs text-ink-muted justify-end pr-1">
                <span className="flex items-center gap-1">
                    <span className="w-3 h-3 rounded-full inline-block" style={{ background: "#E8A020" }} /> Near
                </span>
                <span className="flex items-center gap-1">
                    <span className="w-3 h-3 rounded-full inline-block" style={{ background: "#2E75B6" }} /> Far
                </span>
            </div>
        </div>
    );
}
