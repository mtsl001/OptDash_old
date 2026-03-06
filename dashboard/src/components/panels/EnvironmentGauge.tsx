import { useEnvironment } from "@/hooks/useMarketData";

interface Props { date: string; snapTime: string; underlying: string; }

/**
 * Labels for the 8 environment conditions.
 * Sync with analytics.py get_environment_score() keys.
 */
const CONDITION_META: Record<string, { short: string; icon: string }> = {
  gex_declining: { short: "GEX ≤70% of peak", icon: "📉" },
  vcoc_signal: { short: "V_CoC > ±10", icon: "⚡" },
  fut_bs_ratio: { short: "Fut OBI < −0.2", icon: "🏛" },
  pcr_divergence: { short: "PCR divergence", icon: "🔄" },
  ivp_cheap: { short: "IVP < 50%", icon: "💰" },
  obi_negative: { short: "ATM OBI > ±0.10", icon: "🧭" },
  vex_aligned: { short: "VEX aligned", icon: "🌊" },
  not_charm_distortion: { short: "No charm noise", icon: "⏰" },
};

const BONUS_KEYS = new Set(["vex_aligned", "not_charm_distortion"]);

function fmtVal(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") return Math.abs(v) >= 100 ? v.toFixed(0) : v.toFixed(2);
  return String(v);
}

/* ── Circular arc gauge ─────────────────────────────────── */
function ScoreRing({ score, maxScore, color }: { score: number; maxScore: number; color: string }) {
  const r = 44, cx = 50, cy = 50, stroke = 7;
  const circ = 2 * Math.PI * r;
  const pct = Math.min(score / maxScore, 1);
  const dash = circ * pct;
  const gap = circ - dash;

  return (
    <svg viewBox="0 0 100 100" width="96" height="96" style={{ transform: "rotate(-90deg)" }}>
      {/* Track */}
      <circle cx={cx} cy={cy} r={r} fill="none" stroke="#243040" strokeWidth={stroke} />
      {/* Fill */}
      <circle
        cx={cx} cy={cy} r={r} fill="none"
        stroke={color} strokeWidth={stroke}
        strokeDasharray={`${dash} ${gap}`}
        strokeLinecap="round"
        style={{ transition: "stroke-dasharray 0.6s ease" }}
      />
      {/* Score text (counter-rotate so it reads upright) */}
      <text
        x={cx} y={cy + 1}
        textAnchor="middle" dominantBaseline="central"
        style={{
          transform: "rotate(90deg)", transformOrigin: "50% 50%",
          fill: color, fontSize: "26px", fontWeight: 700, fontFamily: "monospace"
        }}
      >
        {score}
      </text>
    </svg>
  );
}

/* ── Main component ──────────────────────────────────────── */
export function EnvironmentGauge({ date, snapTime, underlying }: Props) {
  const { data, isLoading } = useEnvironment(date, snapTime, underlying);

  if (isLoading || !data)
    return (
      <div className="panel flex flex-col" style={{ minHeight: 300 }}>
        <div className="panel-header">Environment Gate</div>
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">Loading…</div>
      </div>
    );

  const verdictColor: Record<string, string> = {
    GO: "#1E7C44", WAIT: "#E8A020", NO_GO: "#C0392B"
  };
  const color = verdictColor[data.verdict] ?? "#808080";

  const entries = Object.entries(data.conditions);
  const coreEntries = entries.filter(([k]) => !BONUS_KEYS.has(k));
  const bonusEntries = entries.filter(([k]) => BONUS_KEYS.has(k));

  // Contextual notes
  const notes = entries
    .filter(([, c]) => c.note)
    .map(([k, c]) => `${(CONDITION_META[k]?.icon ?? "")} ${c.note}`);

  return (
    <div className="panel flex flex-col" style={{ minHeight: 300 }}>
      {/* ── Header ──────────────────────────────────────── */}
      <div className="panel-header" style={{ paddingBottom: 4 }}>
        <span style={{ opacity: 0.85, fontSize: 13 }}>Environment Gate — {underlying}</span>
        <span
          style={{
            background: color,
            color: "#fff",
            padding: "2px 10px",
            borderRadius: 4,
            fontWeight: 700,
            fontSize: 12,
            letterSpacing: 1,
          }}
        >
          {data.verdict === "NO_GO" ? "NO GO" : data.verdict}
        </span>
      </div>

      {/* ── Score ring + summary ───────────────────────── */}
      <div style={{ display: "flex", alignItems: "center", gap: 16, padding: "8px 0 4px" }}>
        <ScoreRing score={data.score} maxScore={data.max_score} color={color} />
        <div style={{ flex: 1 }}>
          <div style={{ color: "#D6E4F7", fontSize: 13, marginBottom: 4 }}>
            <span style={{ fontWeight: 600 }}>{data.score}</span>
            <span style={{ opacity: 0.5 }}> / {data.max_score}</span>
            <span style={{ opacity: 0.4, marginLeft: 6, fontSize: 11 }}>pts</span>
          </div>
          {/* Mini bar */}
          <div style={{ height: 4, borderRadius: 2, background: "#243040", overflow: "hidden", marginBottom: 6 }}>
            <div
              style={{
                height: "100%", borderRadius: 2,
                width: `${Math.round((data.score / data.max_score) * 100)}%`,
                background: color,
                transition: "width 0.5s ease",
              }}
            />
          </div>
          <div style={{ color: "#808080", fontSize: 10 }}>
            Trade threshold: ≥ 5 pts
          </div>
        </div>
      </div>

      {/* ── Core conditions (compact grid) ─────────────── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "3px 12px", padding: "6px 0 2px" }}>
        {coreEntries.map(([key, cond]) => {
          const meta = CONDITION_META[key];
          return (
            <div
              key={key}
              style={{
                display: "flex", alignItems: "center", gap: 6,
                padding: "3px 6px", borderRadius: 4,
                background: cond.met ? "rgba(30,124,68,0.08)" : "transparent",
              }}
            >
              <span style={{ fontSize: 11, width: 16, textAlign: "center", flexShrink: 0 }}>
                {meta?.icon ?? "•"}
              </span>
              <span style={{
                flex: 1, fontSize: 11,
                color: cond.met ? "#D6E4F7" : "#606060",
                whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
              }}>
                {meta?.short ?? key}
              </span>
              <span style={{
                fontFamily: "monospace", fontSize: 10, fontWeight: 600,
                color: cond.met ? "#1E7C44" : "#C0392B",
                flexShrink: 0, width: 14, textAlign: "right",
              }}>
                {cond.met ? "✓" : "✗"}
              </span>
            </div>
          );
        })}
      </div>

      {/* ── Bonus conditions (VEX/CEX) ─────────────────── */}
      {bonusEntries.length > 0 && (
        <div style={{ borderTop: "1px solid #243040", marginTop: 4, paddingTop: 4 }}>
          <div style={{ fontSize: 9, color: "#808080", letterSpacing: 0.5, marginBottom: 3, textTransform: "uppercase" }}>
            Bonus
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            {bonusEntries.map(([key, cond]) => {
              const meta = CONDITION_META[key];
              return (
                <div
                  key={key}
                  style={{
                    display: "flex", alignItems: "center", gap: 5,
                    padding: "2px 8px", borderRadius: 4, fontSize: 11,
                    background: cond.met ? "rgba(30,124,68,0.08)" : "rgba(128,128,128,0.05)",
                    color: cond.met ? "#D6E4F7" : "#606060",
                  }}
                >
                  <span>{meta?.icon ?? "★"}</span>
                  <span>{meta?.short ?? key}</span>
                  <span style={{
                    fontFamily: "monospace", fontWeight: 600, fontSize: 10,
                    color: cond.met ? "#1E7C44" : "#808080",
                  }}>
                    {cond.met ? `+${cond.points}` : "—"}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Notes (collapsed, minimal) ──────────────────── */}
      {notes.length > 0 && (
        <div style={{
          borderTop: "1px solid #243040", marginTop: "auto", paddingTop: 4,
          maxHeight: 48, overflowY: "auto",
        }}>
          {notes.map((note, i) => (
            <div key={i} style={{ color: "#808080", fontSize: 10, lineHeight: 1.4 }}>
              {note}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
