import { useEffect } from "react";
import { useAlerts } from "@/hooks/useMarketData";
import type { Alert } from "@/api/client";

interface Props {
  date: string;
  snapTime: string;
  underlying: string;
  onAlertsFetched?: (count: number) => void;
}

const SEVERITY_STYLES = {
  HIGH: { border: "#C0392B", badge: "#C0392B22", text: "#C0392B" },
  MEDIUM: { border: "#E8A020", badge: "#E8A02022", text: "#E8A020" },
  LOW: { border: "#808080", badge: "#80808022", text: "#808080" },
};

const TYPE_ICON: Record<string, string> = {
  GEX_DECLINE: "📉",
  COC_VELOCITY: "⚡",
  PCR_DIVERGENCE: "🔄",
  OBI_SHIFT: "🧭",
  VOLUME_SPIKE: "📈",
  GATE_CHANGE: "🚦",
  OI_SURGE: "📊",
  IV_SPIKE: "🔥",
};

const DIRECTION_STYLE: Record<string, { bg: string; color: string }> = {
  BEAR: { bg: "#C0392B22", color: "#C0392B" },
  BULL: { bg: "#1E7C4422", color: "#1E7C44" },
  NEUTRAL: { bg: "#80808022", color: "#808080" },
};

export function AlertFeed({ date, snapTime, underlying, onAlertsFetched }: Props) {
  const { data, isLoading } = useAlerts(date, snapTime, underlying);
  const alerts = data ?? [];

  useEffect(() => {
    if (!isLoading) onAlertsFetched?.(alerts.length);
  }, [alerts.length, isLoading, onAlertsFetched]);

  // Render multi-line alert message — newlines become paragraph breaks
  function renderMessage(text: string) {
    return text.split("\n").map((line, i) => (
      <p key={i} className="text-ink-muted text-xs leading-snug mb-0.5 last:mb-0">
        {line}
      </p>
    ));
  }

  return (
    <div className="panel flex flex-col" style={{ minHeight: 340 }}>
      <div className="panel-header">
        <span>Alert Feed</span>
        {alerts.length > 0 && (
          <span className="badge bear ml-2">{alerts.length}</span>
        )}
      </div>

      {isLoading && (
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
          Loading…
        </div>
      )}

      {!isLoading && alerts.length === 0 && (
        <div className="flex-1 flex items-center justify-center text-ink-muted text-sm">
          No alerts triggered today.
        </div>
      )}

      {!isLoading && alerts.length > 0 && (
        <div className="flex-1 overflow-y-auto mt-2 space-y-2" style={{ maxHeight: 440 }}>
          {[...alerts].reverse().map((alert: Alert, i: number) => {
            const style = SEVERITY_STYLES[alert.severity] ?? SEVERITY_STYLES.LOW;
            const dirStyle = DIRECTION_STYLE[alert.direction ?? "NEUTRAL"] ?? DIRECTION_STYLE.NEUTRAL;
            const icon = TYPE_ICON[alert.type] ?? "🔔";

            return (
              <div
                key={i}
                className="rounded p-2.5"
                style={{
                  borderLeft: `3px solid ${style.border}`,
                  background: style.badge,
                }}
              >
                {/* Header row */}
                <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                  <span className="text-base leading-none flex-shrink-0">{icon}</span>
                  <span className="font-mono text-ink-muted text-xs shrink-0">{alert.time}</span>
                  {alert.direction && alert.direction !== "NEUTRAL" && (
                    <span
                      className="text-xs px-1.5 py-0.5 rounded font-bold"
                      style={{ background: dirStyle.bg, color: dirStyle.color }}
                    >
                      {alert.direction}
                    </span>
                  )}
                  <span
                    className="text-xs px-1.5 py-0.5 rounded uppercase font-mono"
                    style={{ background: style.badge, color: style.text }}
                  >
                    {alert.type.replace(/_/g, " ")}
                  </span>
                  {/* Severity dot */}
                  <span
                    className="w-2 h-2 rounded-full ml-auto flex-shrink-0"
                    style={{ background: style.border }}
                    title={alert.severity}
                  />
                </div>

                {/* Headline — bold, main signal description */}
                <p className="text-xs font-semibold mb-1" style={{ color: style.text }}>
                  {alert.headline}
                </p>

                {/* Narrative message — multi-line: numbers, meaning, action */}
                <div>{renderMessage(alert.message)}</div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
