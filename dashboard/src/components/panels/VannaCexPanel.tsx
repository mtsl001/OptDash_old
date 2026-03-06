/**
 * VannaCexPanel.tsx
 *
 * Displays Vanna Exposure (VEX) and Charm Exposure (CEX) — the two
 * dealer hedging forces that drive mechanical NIFTY flows, especially
 * in the last 45 min before weekly expiry ("Dealer O'Clock").
 *
 * Chart 1 (VEX): Stacked bars — CE vex (blue) + PE vex (red) per snap.
 *                Net VEX as a white line overlay.
 * Chart 2 (CEX): Single bar per snap, green=bid/red=pressure, dashed
 *                reference lines at ±20M threshold.
 */
import {
    ComposedChart, Bar, Line, BarChart, Cell,
    XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine,
    ResponsiveContainer, Legend,
} from "recharts";
import { useVexCex } from "@/hooks/useMarketData";
import type { VexCexSeriesRow } from "@/api/client";

interface Props {
    date: string;
    snapTime: string;
    underlying: string;
}

// ── CEX bar colour — intensity + direction ────────────────────────────────────
function cexColor(val: number): string {
    if (val > 50) return "#16c784";
    if (val > 20) return "#6ee7b7";
    if (val > 0) return "#a7f3d0";
    if (val > -20) return "#fca5a5";
    return "#ef4444";
}

// ── VEX signal badge ──────────────────────────────────────────────────────────
const VEX_META: Record<string, { label: string; color: string }> = {
    VEX_BULLISH: { label: "▲ VEX BULLISH", color: "#1E7C44" },
    VEX_BEARISH: { label: "▼ VEX BEARISH", color: "#C0392B" },
    NEUTRAL: { label: "VEX NEUTRAL", color: "#808080" },
};
const CEX_META: Record<string, { label: string; color: string }> = {
    STRONG_CHARM_BID: { label: "⏫ STRONG CHARM BID", color: "#16c784" },
    CHARM_BID: { label: "↑ CHARM BID", color: "#6ee7b7" },
    CHARM_PRESSURE: { label: "↓ CHARM PRESSURE", color: "#ef4444" },
    NEUTRAL: { label: "CHARM NEUTRAL", color: "#808080" },
};

const TICK_STYLE = { fill: "#808080", fontSize: 10 };
const GRID_STROKE = "#243040";
const TT_STYLE = { background: "#162030", border: "1px solid #243040" };
const TT_LABEL = { color: "#D6E4F7" };

export function VannaCexPanel({ date, snapTime, underlying }: Props) {
    const { data, isLoading, isError } = useVexCex(date, snapTime, underlying);

    const series: VexCexSeriesRow[] = data?.series ?? [];
    const current = data?.current;
    const dealerOClock = data?.dealer_oclock ?? false;
    const interpretation = data?.interpretation ?? "";

    const vexMeta = VEX_META[current?.vex_signal ?? "NEUTRAL"] ?? VEX_META["NEUTRAL"];
    const cexMeta = CEX_META[current?.cex_signal ?? "NEUTRAL"] ?? CEX_META["NEUTRAL"];

    // Colour each bar in the CEX series
    const cexData = series.map((r) => ({
        ...r,
        fill: cexColor(r.cex_total_M),
    }));

    return (
        <div className="panel">
            {/* ── Header ── */}
            <div className="panel-header flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <span className="text-brand-light font-semibold text-sm">
                        Vanna / Charm Exposure
                    </span>
                    {dealerOClock && (
                        <span
                            className="px-2 py-0.5 rounded text-xs font-bold"
                            style={{ background: "#E8A02033", color: "#E8A020", border: "1px solid #E8A020" }}
                        >
                            ⏰ DEALER O'CLOCK
                        </span>
                    )}
                </div>
                <span className="text-ink-muted text-xs">{underlying}</span>
            </div>

            {/* ── KPI row ── */}
            {current && typeof current.vex_total_M === "number" && (
                <div className="flex gap-3 px-3 pb-2 flex-wrap">
                    <div>
                        <div className="text-ink-muted text-xs mb-0.5">VEX</div>
                        <div
                            className="kpi-value font-mono text-sm"
                            style={{ color: vexMeta.color }}
                        >
                            {current.vex_total_M >= 0 ? "+" : ""}{(current.vex_total_M ?? 0).toFixed(1)}M
                        </div>
                    </div>
                    <div>
                        <div className="text-ink-muted text-xs mb-0.5">Signal</div>
                        <span
                            className="px-2 py-0.5 rounded text-xs"
                            style={{ color: vexMeta.color, border: `1px solid ${vexMeta.color}`, background: `${vexMeta.color}22` }}
                        >
                            {vexMeta.label}
                        </span>
                    </div>
                    <div className="ml-4">
                        <div className="text-ink-muted text-xs mb-0.5">CEX</div>
                        <div
                            className="kpi-value font-mono text-sm"
                            style={{ color: cexMeta.color }}
                        >
                            {current.cex_total_M >= 0 ? "+" : ""}{(current.cex_total_M ?? 0).toFixed(1)}M
                        </div>
                    </div>
                    <div>
                        <div className="text-ink-muted text-xs mb-0.5">Signal</div>
                        <span
                            className="px-2 py-0.5 rounded text-xs"
                            style={{ color: cexMeta.color, border: `1px solid ${cexMeta.color}`, background: `${cexMeta.color}22` }}
                        >
                            {cexMeta.label}
                        </span>
                    </div>
                    <div className="ml-auto text-right">
                        <div className="text-ink-muted text-xs mb-0.5">DTE</div>
                        <div className="font-mono text-sm text-white">{current.dte ?? "—"}</div>
                    </div>
                </div>
            )}

            {isLoading && (
                <div className="flex items-center justify-center h-24 text-ink-muted text-sm">
                    Loading…
                </div>
            )}
            {isError && (
                <div className="flex items-center justify-center h-24 text-bear text-sm">
                    Failed to load VEX/CEX data
                </div>
            )}

            {!isLoading && !isError && series.length === 0 && (
                <div className="flex items-center justify-center h-24 text-ink-muted text-sm">
                    No VEX/CEX data — will populate after processor update writes new Parquet files
                </div>
            )}

            {series.length > 0 && (
                <>
                    {/* ── Chart 1: VEX time series ── */}
                    <div className="px-2 pb-1">
                        <div className="text-ink-muted text-xs px-1 mb-1">
                            Vanna Exposure — M units (stacked CE/PE, net line)
                        </div>
                        <ResponsiveContainer width="100%" height={120}>
                            <ComposedChart data={series} margin={{ top: 4, right: 6, left: 0, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
                                <XAxis dataKey="snap_time" tick={TICK_STYLE} interval="preserveStartEnd" />
                                <YAxis tick={TICK_STYLE} width={42} tickFormatter={(v) => `${v.toFixed(0)}M`} />
                                <Tooltip
                                    contentStyle={TT_STYLE}
                                    labelStyle={TT_LABEL}
                                    formatter={(v?: number, name?: string) => [
                                        typeof v === "number" ? `${v >= 0 ? "+" : ""}${v.toFixed(2)}M` : "—",
                                        name ?? "",
                                    ]}
                                />
                                <Legend wrapperStyle={{ fontSize: 10, color: "#808080" }} />
                                <ReferenceLine y={0} stroke="#243040" strokeWidth={1.5} />
                                <Bar dataKey="vex_ce_M" name="VEX CE" stackId="vex" fill="#2E75B6" isAnimationActive={false} />
                                <Bar dataKey="vex_pe_M" name="VEX PE" stackId="vex" fill="#C0392B" isAnimationActive={false} />
                                <Line
                                    type="monotone"
                                    dataKey="vex_total_M"
                                    name="Net VEX"
                                    stroke="#D6E4F7"
                                    dot={false}
                                    strokeWidth={1.5}
                                    isAnimationActive={false}
                                />
                            </ComposedChart>
                        </ResponsiveContainer>
                    </div>

                    {/* ── Chart 2: CEX time series ── */}
                    <div className="px-2 pb-2">
                        <div className="text-ink-muted text-xs px-1 mb-1">
                            Charm Exposure — M units (green = buy pressure, red = sell)
                        </div>
                        <ResponsiveContainer width="100%" height={100}>
                            <BarChart data={cexData} margin={{ top: 4, right: 6, left: 0, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
                                <XAxis dataKey="snap_time" tick={TICK_STYLE} interval="preserveStartEnd" />
                                <YAxis tick={TICK_STYLE} width={42} tickFormatter={(v) => `${v.toFixed(0)}M`} />
                                <Tooltip
                                    contentStyle={TT_STYLE}
                                    labelStyle={TT_LABEL}
                                    formatter={(v?: number, name?: string) => [
                                        typeof v === "number" ? `${v >= 0 ? "+" : ""}${v.toFixed(2)}M` : "—",
                                        name ?? "",
                                    ]}
                                />
                                <ReferenceLine y={0} stroke="#243040" strokeWidth={1.5} />
                                <ReferenceLine y={20} stroke="#6ee7b7" strokeDasharray="4 2" strokeWidth={1} />
                                <ReferenceLine y={-20} stroke="#fca5a5" strokeDasharray="4 2" strokeWidth={1} />
                                {/* Each bar uses its own colour from cexData.fill */}
                                <Bar dataKey="cex_total_M" name="CEX" isAnimationActive={false}>
                                    {cexData.map((entry, i) => (
                                        <Cell key={i} fill={entry.fill} />
                                    ))}
                                </Bar>
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </>
            )}

            {/* ── Interpretation footer ── */}
            {interpretation && (
                <div
                    className="mx-3 mb-3 px-2 py-1.5 rounded text-xs"
                    style={{ background: "#162030", border: "1px solid #243040", color: "#808080", lineHeight: "1.5" }}
                >
                    {interpretation}
                </div>
            )}
        </div>
    );
}
