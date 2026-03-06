import { useState } from "react";
import {
    useRecommendation,
    useAcceptTrade,
    useRejectTrade,
} from "@/hooks/useAIData";

// ── Signal badge config ──────────────────────────────────────────────────────
const SIGNAL_ICONS: Record<string, string> = {
    VCOC_BULL: "⚡",
    VCOC_BEAR: "⚡",
    GEX_DECLINE: "📉",
    VEX_ALIGNED: "🌊",
    PCR_DIVERGENCE: "🔄",
    VOLUME_SPIKE: "💰",
    IVP_CHEAP: "📊",
    CHARM_SAFE: "✓",
};

function Stars({ count }: { count: number }) {
    return (
        <span className="text-accent font-mono tracking-widest">
            {"★".repeat(count)}
            {"☆".repeat(Math.max(0, 5 - count))}
        </span>
    );
}

interface Props {
    date: string;
    snapTime: string;
    underlying: string;
}

export function AIRecommendPanel({ date, snapTime, underlying }: Props) {
    const [collapsed, setCollapsed] = useState(false);

    const { data: card, isLoading, error } = useRecommendation(date, snapTime, underlying);
    const acceptMut = useAcceptTrade();
    const rejectMut = useRejectTrade();

    const handleAccept = () => {
        if (card?.trade_id) acceptMut.mutate(card.trade_id);
    };
    const handleReject = () => {
        if (card?.trade_id) rejectMut.mutate(card.trade_id);
    };

    // ── Empty / Loading / Error states ─────────────────────────────────────
    if (isLoading) {
        return (
            <div className="panel" style={{ minHeight: 60 }}>
                <div className="panel-header flex items-center gap-2">
                    <span>🤖</span> AI Recommendation
                    <span className="ml-auto text-ink-muted text-xs animate-pulse">Scanning…</span>
                </div>
            </div>
        );
    }

    if (error || !card) {
        return (
            <div className="panel" style={{ minHeight: 60 }}>
                <div className="panel-header flex items-center gap-2">
                    <span>🤖</span> AI Recommendation
                    <span className="ml-auto text-ink-muted text-xs">No setup found</span>
                </div>
                <div className="text-xs text-ink-muted mt-2 px-1">
                    {error
                        ? "Could not generate recommendation."
                        : "No valid trade setup at this snapshot. Waiting for signal alignment…"}
                </div>
            </div>
        );
    }

    // ── Confidence badge colour ────────────────────────────────────────────
    const confColour =
        card.confidence >= 75 ? "#1E7C44" : card.confidence >= 50 ? "#E8A020" : "#C0392B";
    const confLabel =
        card.confidence >= 75 ? "HIGH" : card.confidence >= 50 ? "MEDIUM" : "LOW";

    // ── Already accepted / rejected ────────────────────────────────────────
    const isActioned = card.status !== "GENERATED";

    return (
        <div
            className="panel"
            style={{
                border: `1px solid ${card.confidence >= 75 ? "#1E7C44" : "#243040"}`,
            }}
        >
            {/* Header */}
            <div
                className="panel-header flex items-center gap-2 cursor-pointer select-none"
                onClick={() => setCollapsed((v) => !v)}
            >
                <span>🤖</span>
                <span className="font-bold">AI RECOMMENDATION</span>
                <span
                    className="ml-2 px-2 py-0.5 rounded text-xs font-mono font-bold"
                    style={{ background: confColour, color: "#fff" }}
                >
                    {confLabel} {(card.confidence ?? 0).toFixed(0)}%
                </span>
                <span className="ml-auto text-sm font-mono">
                    {card.underlying} {card.strike_price} {card.direction}
                </span>
                <span className="text-ink-muted text-xs">{collapsed ? "▸" : "▾"}</span>
            </div>

            {!collapsed && (
                <div className="mt-3 space-y-3">
                    {/* KPI row */}
                    <div className="kpi-row">
                        <div className="kpi">
                            <div className="kpi-label">Stars</div>
                            <div className="kpi-value">
                                <Stars count={card.stars} />
                            </div>
                        </div>
                        <div className="kpi">
                            <div className="kpi-label">S_score</div>
                            <div className="kpi-value font-mono">{(card.s_score ?? 0).toFixed(1)}</div>
                        </div>
                        <div className="kpi">
                            <div className="kpi-label">Gate</div>
                            <div
                                className="kpi-value font-mono"
                                style={{
                                    color: card.gate_verdict === "GO" ? "#1E7C44" : "#E8A020",
                                }}
                            >
                                {card.gate_score ?? 0}/11 {card.gate_verdict ?? "WAIT"}
                            </div>
                        </div>
                        <div className="kpi">
                            <div className="kpi-label">Entry ₹</div>
                            <div className="kpi-value font-mono">{(card.entry_premium ?? 0).toFixed(2)}</div>
                        </div>
                        <div className="kpi">
                            <div className="kpi-label">SL ₹</div>
                            <div className="kpi-value font-mono text-bear">
                                {(card.sl ?? 0).toFixed(2)}
                            </div>
                        </div>
                        <div className="kpi">
                            <div className="kpi-label">Target ₹</div>
                            <div className="kpi-value font-mono text-bull">
                                {(card.target ?? 0).toFixed(2)}
                            </div>
                        </div>
                        <div className="kpi">
                            <div className="kpi-label">DTE</div>
                            <div className="kpi-value font-mono">{card.dte}</div>
                        </div>
                        <div className="kpi">
                            <div className="kpi-label">Lot</div>
                            <div className="kpi-value font-mono">{card.lot_size}</div>
                        </div>
                    </div>

                    {/* Narrative */}
                    <div
                        className="px-3 py-2 rounded text-sm leading-relaxed"
                        style={{ background: "#0F1923", border: "1px solid #243040" }}
                    >
                        {card.narrative}
                    </div>

                    {/* Action buttons */}
                    <div className="flex items-center gap-3">
                        {!isActioned ? (
                            <>
                                <button
                                    onClick={handleAccept}
                                    disabled={acceptMut.isPending}
                                    className="px-5 py-2 rounded font-semibold text-sm transition-colors"
                                    style={{
                                        background: "#1E7C44",
                                        color: "#fff",
                                        opacity: acceptMut.isPending ? 0.5 : 1,
                                    }}
                                >
                                    ✅ Accept
                                </button>
                                <button
                                    onClick={handleReject}
                                    disabled={rejectMut.isPending}
                                    className="px-5 py-2 rounded font-semibold text-sm transition-colors"
                                    style={{
                                        background: "#C0392B",
                                        color: "#fff",
                                        opacity: rejectMut.isPending ? 0.5 : 1,
                                    }}
                                >
                                    ❌ Reject
                                </button>
                                <button
                                    className="px-5 py-2 rounded font-semibold text-sm"
                                    style={{
                                        background: "#243040",
                                        color: "#808080",
                                        border: "1px solid #243040",
                                    }}
                                >
                                    ⏸ Watch
                                </button>
                            </>
                        ) : (
                            <span
                                className="px-4 py-2 rounded text-xs font-bold uppercase"
                                style={{
                                    background: card.status === "ACCEPTED" ? "#1E7C4422" : "#C0392B22",
                                    color: card.status === "ACCEPTED" ? "#1E7C44" : "#C0392B",
                                    border: `1px solid ${card.status === "ACCEPTED" ? "#1E7C44" : "#C0392B"}`,
                                }}
                            >
                                {card.status}
                            </span>
                        )}

                        {/* Mutation success/error feedback */}
                        {acceptMut.isSuccess && (
                            <span className="text-bull text-xs">✓ Accepted — tracking in Position Monitor</span>
                        )}
                        {rejectMut.isSuccess && (
                            <span className="text-bear text-xs">✓ Rejected — logged for regret analysis</span>
                        )}
                    </div>

                    {/* Signal badges */}
                    <div className="flex flex-wrap gap-2 mt-1">
                        {card.signals_fired.map((sig) => (
                            <span
                                key={sig}
                                className="px-2 py-0.5 rounded text-xs font-mono"
                                style={{ background: "#1B3A6B", color: "#D6E4F7" }}
                            >
                                {SIGNAL_ICONS[sig] ?? "•"} {sig.replace(/_/g, " ")}
                            </span>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
