import { useStrikes } from "@/hooks/useMarketData";
import type { StrikeRow } from "@/api/client";

interface Props { date: string; underlying: string; snapTime: string; }

function rhoColor(rho: number): string {
  if (rho < 0.10) return "#1E7C44";
  if (rho < 0.30) return "#E8A020";
  return "#C0392B";
}

// s_score = eff_ratio × (1 − rho), where eff_ratio = delta × ltp / |theta| (dimensionless)
// Typical ATM s_score range: 5–25.  ≥20 = excellent, 10–20 = good, 5–10 = fair, <5 = poor.
function scoreStars(score: number): string {
  if (score > 20) return "★★★★";
  if (score > 10) return "★★★☆";
  if (score > 5) return "★★☆☆";
  return "★☆☆☆";
}

export function StrikeScreener({ date, underlying, snapTime }: Props) {
  const { data, isLoading, error } = useStrikes(date, underlying, snapTime);

  if (isLoading)
    return (
      <div className="panel flex flex-col" style={{ minHeight: 340 }}>
        <div className="panel-header">Strike Screener — {underlying}</div>
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">Loading…</div>
      </div>
    );

  if (error || !data?.length)
    return (
      <div className="panel flex flex-col" style={{ minHeight: 340 }}>
        <div className="panel-header">Strike Screener — {underlying}</div>
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
          No strike data for {underlying} @ {snapTime}
        </div>
      </div>
    );

  return (
    <div className="panel flex flex-col" style={{ minHeight: 340 }}>
      <div className="panel-header">
        Strike Screener — Top {data.length} by S_score
        <span className="text-xs text-ink-muted ml-2">@ {snapTime}</span>
      </div>

      {/* Fixed-height scrollable table body so it doesn't bloat the row */}
      <div className="overflow-auto flex-1" style={{ maxHeight: 480 }}>
        <table className="w-full text-xs mt-2" style={{ borderCollapse: "collapse" }}>
          <thead className="sticky top-0 z-10 bg-surface">
            <tr className="text-ink-muted border-b border-border">
              <th className="text-left py-1 pr-2">Expiry</th>
              <th className="text-left pr-2">Type</th>
              <th className="text-right pr-2">Strike</th>
              <th className="text-right pr-2">LTP</th>
              <th className="text-right pr-2">IV%</th>
              <th className="text-right pr-2">Delta</th>
              <th className="text-right pr-2">EffR</th>
              <th className="text-right pr-2">Rho</th>
              <th className="text-right pr-2 font-bold text-accent">S_score</th>
              <th className="text-right">⭐</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row: StrikeRow, i: number) => (
              <tr
                key={`${row.expiry_date}-${row.option_type}-${row.strike_price}`}
                className={i % 2 === 0 ? "bg-surface" : ""}
              >
                <td className="py-1 pr-2 text-ink-muted font-mono">
                  {row.expiry_date.slice(5)}
                  <span className="ml-1 opacity-50">D{row.dte}</span>
                </td>
                <td
                  className="pr-2 font-bold"
                  style={{ color: row.option_type === "CE" ? "#2E75B6" : "#C0392B" }}
                >
                  {row.option_type}
                </td>
                <td className="text-right pr-2 font-mono">{row.strike_price}</td>
                <td className="text-right pr-2 font-mono">{(row.ltp ?? 0).toFixed(2)}</td>
                <td className="text-right pr-2">{(row.iv ?? 0).toFixed(1)}%</td>
                <td className="text-right pr-2 font-mono">{(row.delta ?? 0).toFixed(3)}</td>
                <td className="text-right pr-2 font-mono">{(row.eff_ratio ?? 0).toFixed(2)}</td>
                <td className="text-right pr-2 font-mono" style={{ color: rhoColor(row.rho ?? 0) }}>
                  {(row.rho ?? 0).toFixed(3)}
                </td>
                <td className="text-right pr-2 font-mono font-bold text-accent">
                  {(row.s_score ?? 0).toFixed(2)}
                </td>
                <td className="text-right text-accent">{scoreStars(row.s_score ?? 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
