import { useState } from "react";
import { 
    useTradeHistory, 
    useTradeStats, 
    usePnLSeries,
    useLearning 
} from "@/hooks/useAIData";
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, 
    Tooltip, ResponsiveContainer, ReferenceLine, Area, AreaChart
} from "recharts";

const STATUS_COLOURS: Record<string, string> = {
    GENERATED: "#808080",
    ACCEPTED: "#2E75B6",
    REJECTED: "#C0392B55",
    EXPIRED: "#80808088",
    SL_HIT: "#C0392B",
    TARGET_HIT: "#1E7C44",
    MANUAL: "#E8A020",
    TIME_EXIT: "#808080",
    GATE_EXIT: "#E8A020",
};

/** Safely format a number or return fallback */
function fmt(v: number | null | undefined, decimals = 1, fallback = "—"): string {
    return v != null ? v.toFixed(decimals) : fallback;
}

interface Props {
    date: string;
    underlying: string;
}

export function TradeJournalPanel({ date, underlying }: Props) {
    const [tab, setTab] = useState<"history" | "pnl" | "analysis">("history");
    
    const { data: history } = useTradeHistory(date, underlying);
    const { data: stats } = useTradeStats();
    const { data: pnlSeries } = usePnLSeries();
    const { data: learning } = useLearning();

    // Null-safe stats accessors
    const s = {
        total_trades: stats?.total_trades ?? 0,
        closed_trades: stats?.closed_trades ?? 0,
        win_rate: stats?.win_rate ?? 0,
        avg_pnl_pct: stats?.avg_pnl_pct ?? 0,
        total_pnl_rupees: stats?.total_pnl_rupees ?? 0,
        best_trade_pct: stats?.best_trade_pct ?? 0,
        worst_trade_pct: stats?.worst_trade_pct ?? 0,
        avg_confidence: stats?.avg_confidence ?? 0,
    };

    return (
        <div className="panel flex flex-col" style={{ minHeight: 500 }}>
            <div className="panel-header flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <span>📓</span>
                    <span>Trade Journal</span>
                </div>
                
                {/* Tabs */}
                <div className="flex bg-bg-panel rounded p-0.5 border border-border scale-90 origin-right">
                    {(["history", "pnl", "analysis"] as const).map((t) => (
                        <button
                            key={t}
                            onClick={() => setTab(t)}
                            className={`px-3 py-1 rounded text-[10px] font-bold uppercase transition-colors ${
                                tab === t ? "bg-brand text-brand-muted shadow-sm" : "text-ink-muted hover:text-white"
                            }`}
                        >
                            {t}
                        </button>
                    ))}
                </div>
            </div>

            {/* ── Stats Summary Row (Always visible) ── */}
            {stats && (
                <div className="flex items-center gap-6 px-3 py-2 bg-surface/30 border-b border-border mb-2">
                    <div className="flex flex-col">
                        <span className="text-[10px] text-ink-muted uppercase">Win Rate</span>
                        <span className="font-mono text-sm font-bold" style={{ color: s.win_rate >= 50 ? "#1E7C44" : "#C0392B" }}>
                            {s.win_rate.toFixed(1)}%
                        </span>
                    </div>
                    <div className="flex flex-col">
                        <span className="text-[10px] text-ink-muted uppercase">Total P&L</span>
                        <span className="font-mono text-sm font-bold" style={{ color: s.total_pnl_rupees >= 0 ? "#1E7C44" : "#C0392B" }}>
                            ₹{s.total_pnl_rupees.toLocaleString("en-IN")}
                        </span>
                    </div>
                    <div className="flex flex-col">
                        <span className="text-[10px] text-ink-muted uppercase">Avg %</span>
                        <span className="font-mono text-sm font-bold" style={{ color: s.avg_pnl_pct >= 0 ? "#1E7C44" : "#C0392B" }}>
                            {s.avg_pnl_pct >= 0 ? "+" : ""}{s.avg_pnl_pct.toFixed(1)}%
                        </span>
                    </div>
                    <div className="flex flex-col ml-auto text-right">
                        <span className="text-[10px] text-ink-muted uppercase">Trades</span>
                        <span className="font-mono text-sm text-brand-muted">{s.closed_trades}/{s.total_trades}</span>
                    </div>
                </div>
            )}

            <div className="flex-1 overflow-hidden flex flex-col">
                {/* ── TAB: History ── */}
                {tab === "history" && (
                    <div className="flex-1 overflow-auto">
                        <table className="w-full text-xs">
                            <thead className="sticky top-0 bg-bg-surface z-10 shadow-sm">
                                <tr className="text-left text-ink-muted uppercase" style={{ borderBottom: "1px solid #243040" }}>
                                    <th className="py-2 px-2">Time</th>
                                    <th className="py-2 px-2">Symbol</th>
                                    <th className="py-2 px-2">Type</th>
                                    <th className="py-2 px-2 text-right">Entry</th>
                                    <th className="py-2 px-2 text-right">Target</th>
                                    <th className="py-2 px-2 text-center">Conf</th>
                                    <th className="py-2 px-2 text-center">Status</th>
                                    <th className="py-2 px-2 text-right pr-3">PnL</th>
                                </tr>
                            </thead>
                            <tbody>
                                {history && history.length > 0 ? (
                                    history.map((t) => {
                                        const pnlColour = t.pnl_pct != null ? (t.pnl_pct >= 0 ? "#1E7C44" : "#C0392B") : "#808080";
                                        return (
                                            <tr key={t.trade_id} className="border-b border-border/30 hover:bg-white/5 transition-colors cursor-default group">
                                                <td className="py-2 px-2 font-mono whitespace-nowrap">
                                                    <div className="text-white">{t.snap_time}</div>
                                                    <div className="text-[9px] text-ink-muted">{t.trade_date.slice(5)}</div>
                                                </td>
                                                <td className="py-2 px-2">
                                                    <div className="font-bold">{t.underlying}</div>
                                                    <div className="text-[10px] text-ink-muted font-mono">{t.strike_price}</div>
                                                </td>
                                                <td className={`py-2 px-2 font-bold ${t.direction === 'CE' ? 'text-bull' : 'text-bear'}`}>
                                                    {t.direction}
                                                </td>
                                                <td className="py-2 px-2 font-mono text-right text-brand-muted">
                                                    ₹{fmt(t.entry_premium, 1)}
                                                </td>
                                                <td className="py-2 px-2 font-mono text-right text-bull/70">
                                                    ₹{fmt(t.target, 1)}
                                                </td>
                                                <td className="py-2 px-2 text-center">
                                                    <span className="text-[10px] font-mono px-1 py-0.5 rounded bg-brand/30">
                                                        {t.confidence != null ? `${t.confidence.toFixed(0)}%` : "—"}
                                                    </span>
                                                </td>
                                                <td className="py-2 px-2 text-center">
                                                    <span
                                                        className="px-1.5 py-0.5 rounded text-[9px] font-bold uppercase whitespace-nowrap"
                                                        style={{
                                                            background: (STATUS_COLOURS[t.status] ?? "#808080") + "22",
                                                            color: STATUS_COLOURS[t.status] ?? "#808080",
                                                            border: `1px solid ${STATUS_COLOURS[t.status] ?? "#808080"}`,
                                                        }}
                                                    >
                                                        {t.status.replace(/_/g, " ")}
                                                    </span>
                                                </td>
                                                <td className="py-2 px-2 font-mono font-bold text-right pr-3" style={{ color: pnlColour }}>
                                                    {t.pnl_pct != null ? (
                                                        <div className="flex flex-col items-end">
                                                            <span>{t.pnl_pct >= 0 ? "+" : ""}{t.pnl_pct.toFixed(1)}%</span>
                                                            {t.pnl_rupees != null && (
                                                                <span className="text-[9px] font-normal opacity-70">
                                                                    ₹{t.pnl_rupees.toLocaleString("en-IN")}
                                                                </span>
                                                            )}
                                                        </div>
                                                    ) : (
                                                        <span className="text-ink-muted opacity-50">—</span>
                                                    )}
                                                </td>
                                            </tr>
                                        );
                                    })
                                ) : (
                                    <tr><td colSpan={8} className="py-12 text-center text-ink-muted text-sm italic">Scanning history...</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                )}

                {/* ── TAB: PnL Visualization ── */}
                {tab === "pnl" && (
                    <div className="flex-1 p-3 flex flex-col">
                        <div className="text-xs text-ink-muted mb-4 px-1 flex justify-between">
                            <span>Realized Cumulative P&L (₹)</span>
                            <span className="text-bull">Peak: ₹{Math.max(...(pnlSeries?.map(p => p.cum_pnl_rupees) ?? [0])).toLocaleString()}</span>
                        </div>
                        
                        <div className="flex-1 min-h-[250px]">
                            <ResponsiveContainer width="100%" height="100%">
                                <AreaChart data={pnlSeries} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                                    <defs>
                                        <linearGradient id="colorPnl" x1="0" y1="0" x2="0" y2="1">
                                            <stop offset="5%" stopColor="#1E7C44" stopOpacity={0.3}/>
                                            <stop offset="95%" stopColor="#1E7C44" stopOpacity={0}/>
                                        </linearGradient>
                                        <linearGradient id="colorLoss" x1="0" y1="0" x2="0" y2="1">
                                            <stop offset="5%" stopColor="#C0392B" stopOpacity={0.3}/>
                                            <stop offset="95%" stopColor="#C0392B" stopOpacity={0}/>
                                        </linearGradient>
                                    </defs>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#243040" vertical={false} />
                                    <XAxis 
                                        dataKey="created_at" 
                                        hide 
                                    />
                                    <YAxis 
                                        tick={{ fill: "#808080", fontSize: 10 }}
                                        tickFormatter={(v) => `₹${v >= 1000 ? (v/1000).toFixed(1)+'k' : v}`}
                                        width={45}
                                    />
                                    <Tooltip 
                                        contentStyle={{ background: "#162030", border: "1px solid #243040" }}
                                        labelStyle={{ color: "#D6E4F7" }}
                                        labelFormatter={(v) => `Trade Date: ${pnlSeries?.find(p => p.created_at === v)?.trade_date ?? v}`}
                                        formatter={(v: number) => [`₹${v.toLocaleString()}`, "Cum. P&L"]}
                                    />
                                    <ReferenceLine y={0} stroke="#404040" strokeWidth={1} />
                                    <Area 
                                        type="monotone" 
                                        dataKey="cum_pnl_rupees" 
                                        stroke="#1E7C44" 
                                        fillOpacity={1} 
                                        fill="url(#colorPnl)" 
                                        strokeWidth={2}
                                        isAnimationActive={false}
                                    />
                                </AreaChart>
                            </ResponsiveContainer>
                        </div>
                        
                        <div className="grid grid-cols-2 gap-3 mt-4">
                            <div className="bg-white/5 rounded p-2 border border-white/5">
                                <div className="text-[10px] text-ink-muted uppercase">Best Trade</div>
                                <div className="text-bull font-mono font-bold">+{s.best_trade_pct}%</div>
                            </div>
                            <div className="bg-white/5 rounded p-2 border border-white/5">
                                <div className="text-[10px] text-ink-muted uppercase">Worst Trade</div>
                                <div className="text-bear font-mono font-bold">{s.worst_trade_pct}%</div>
                            </div>
                        </div>
                    </div>
                )}

                {/* ── TAB: Analysis ── */}
                {tab === "analysis" && (
                    <div className="flex-1 p-3 overflow-auto space-y-4">
                        <div className="space-y-2">
                            <h4 className="text-[10px] font-bold text-ink-muted uppercase tracking-wider">Gate Insights & Logic Adjustments</h4>
                            {learning && learning.length > 0 ? (
                                learning.map((insight, idx) => (
                                    <div key={idx} className="bg-brand/10 border border-brand/20 rounded p-2.5">
                                        <div className="flex justify-between items-center mb-1">
                                            <span className="text-xs font-bold text-brand-muted">{insight.parameter.replace(/_/g, ' ')}</span>
                                            <span className="text-[9px] bg-brand/30 px-1.5 py-0.5 rounded text-ink-muted">n={insight.sample_size}</span>
                                        </div>
                                        <div className="text-[11px] text-brand-muted leading-relaxed">
                                            {insight.reason}
                                        </div>
                                        {insight.suggested !== 0 && (
                                            <div className="mt-2 flex items-center gap-2">
                                                <span className="text-[10px] text-ink-muted">Suggested:</span>
                                                <span className="text-[10px] line-through text-ink-muted opacity-50">{insight.current}</span>
                                                <span className="text-[10px] font-bold text-bull">→ {insight.suggested}</span>
                                            </div>
                                        )}
                                    </div>
                                ))
                            ) : (
                                <div className="py-8 text-center text-ink-muted text-xs italic">
                                    Insufficient data for AI learning. Keep trading!
                                </div>
                            )}
                        </div>
                        
                        <div className="p-3 rounded bg-accent/5 border border-accent/10">
                            <h4 className="text-[10px] font-bold text-accent uppercase mb-2">Strategy Note</h4>
                            <p className="text-[11px] text-brand-muted leading-relaxed">
                                The journal analyzes trades with &gt;65% confidence. If your win rate drops below 40% on any specific signal combo (e.g. VEX aligned but CoC conflicting), the AI will suggest raising the entry gate floor.
                            </p>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
