import { useState, useCallback, useRef } from "react";
import { GEXPanel } from "@/components/panels/GEXPanel";
import { CoCVelocityPanel } from "@/components/panels/CoCVelocityPanel";
import { EnvironmentGauge } from "@/components/panels/EnvironmentGauge";
import { StrikeScreener } from "@/components/panels/StrikeScreener";
import { PCRDivergencePanel } from "@/components/panels/PCRDivergencePanel";
import { AlertFeed } from "@/components/panels/AlertFeed";
import { PositionMonitor } from "@/components/panels/PositionMonitor";
import { VolumeVelocityPanel } from "@/components/panels/VolumeVelocityPanel";
import { TermStructurePanel } from "@/components/panels/TermStructurePanel";
import { VannaCexPanel } from "@/components/panels/VannaCexPanel";
import { AIRecommendPanel } from "@/components/panels/AIRecommendPanel";
import { TradeJournalPanel } from "@/components/panels/TradeJournalPanel";
import { useGEX, useSpot } from "@/hooks/useMarketData";

// ── Snap times grid (09:15–15:30, 5-min steps) ────────────────────────────────
const SNAP_TIMES: string[] = (() => {
  const times: string[] = [];
  for (let h = 9; h <= 15; h++) {
    for (let m = 0; m < 60; m += 5) {
      if (h === 9 && m < 15) continue;
      if (h === 15 && m > 30) break;
      times.push(`${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`);
    }
  }
  return times;
})();

const UNDERLYINGS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"];

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

// Tiny Web Audio API beep — no external library
function beep() {
  try {
    const ctx = new AudioContext();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = 880;
    gain.gain.setValueAtTime(0.15, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3);
    osc.start();
    osc.stop(ctx.currentTime + 0.3);
  } catch { /* AudioContext not supported */ }
}

export function Dashboard() {
  const [date, setDate] = useState(todayStr());
  const [underlying, setUnderlying] = useState("NIFTY");
  const [snapIdx, setSnapIdx] = useState(SNAP_TIMES.length - 1); // default = latest
  const [soundOn, setSoundOn] = useState(false);
  const prevAlertCount = useRef(0);

  const gex = useGEX(date, underlying);
  const spot = useSpot(date, underlying);

  // Derive snap_time: if GEX data is available, use the latest real snap;
  // otherwise use the slider value (for historical scrubbing).
  const latestDataSnap = gex.data?.[gex.data.length - 1]?.snap_time;
  const isLive = snapIdx === SNAP_TIMES.length - 1;
  const snapTime = isLive && latestDataSnap ? latestDataSnap : SNAP_TIMES[snapIdx];

  // Sound alert callback — passed awareness via ref
  const onAlertsFetched = useCallback((count: number) => {
    if (soundOn && count > prevAlertCount.current) beep();
    prevAlertCount.current = count;
  }, [soundOn]);

  return (
    <div className="min-h-screen bg-panel text-white font-sans">

      {/* ── Header ── */}
      <header className="flex items-center gap-3 px-4 py-2 bg-surface border-b border-border flex-wrap">
        <span className="text-brand-light font-bold text-base whitespace-nowrap">
          OptDash v2.0
        </span>

        {/* Spot ticker */}
        {spot.data && (
          <div className="flex items-center gap-2">
            <span className="text-lg font-mono font-bold">
              {underlying} {spot.data.spot}
            </span>
            <span style={{ color: spot.data.change_pct >= 0 ? "#1E7C44" : "#C0392B" }}>
              {spot.data.change_pct >= 0 ? "▲" : "▼"} {Math.abs(spot.data.change_pct)}%
            </span>
            <span className="text-ink-muted text-xs">
              H:{spot.data.day_high} L:{spot.data.day_low}
            </span>
          </div>
        )}

        {/* Spacer */}
        <div className="ml-auto flex items-center gap-2 flex-wrap">

          {/* Date picker */}
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            className="bg-surface border border-border rounded px-2 py-1 text-xs text-white"
          />

          {/* Snap-time slider */}
          <div className="flex items-center gap-1.5">
            <span className="text-ink-muted text-xs hidden sm:block">Snap</span>
            <input
              type="range"
              min={0}
              max={SNAP_TIMES.length - 1}
              value={snapIdx}
              onChange={(e) => setSnapIdx(Number(e.target.value))}
              className="w-28 accent-brand-light"
              title="Scrub historical snapshots"
            />
            <span
              className="font-mono text-xs px-1.5 py-0.5 rounded"
              style={{
                background: "#162030",
                border: "1px solid #243040",
                color: isLive ? "#1E7C44" : "#D6E4F7",
              }}
            >
              {snapTime}
            </span>
            {isLive && (
              <span className="flex items-center gap-1 text-xs text-bull">
                <span className="w-2 h-2 rounded-full bg-bull animate-pulse inline-block" />
                LIVE
              </span>
            )}
          </div>

          {/* Sound toggle */}
          <button
            onClick={() => setSoundOn((v) => !v)}
            title={soundOn ? "Sound alerts ON" : "Sound alerts OFF"}
            className="text-lg leading-none px-1 rounded"
            style={{
              background: soundOn ? "#1B3A6B" : "transparent",
              border: "1px solid #243040",
              color: soundOn ? "#D6E4F7" : "#808080",
            }}
          >
            {soundOn ? "🔔" : "🔕"}
          </button>
        </div>
      </header>

      {/* ── Underlying pill tabs ── */}
      <div className="flex gap-1.5 px-4 pt-2 pb-1">
        {UNDERLYINGS.map((u) => (
          <button
            key={u}
            onClick={() => setUnderlying(u)}
            className="px-3 py-1 rounded-full text-xs font-semibold transition-colors"
            style={{
              background: underlying === u ? "#2E75B6" : "#162030",
              border: `1px solid ${underlying === u ? "#2E75B6" : "#243040"}`,
              color: underlying === u ? "#fff" : "#808080",
            }}
          >
            {u}
          </button>
        ))}
      </div>

      {/* ── Row 1: Environment · GEX · CoC ── */}
      <div className="grid grid-cols-3 gap-3 p-3 items-stretch">
        <EnvironmentGauge date={date} snapTime={snapTime} underlying={underlying} />
        <GEXPanel date={date} underlying={underlying} />
        <CoCVelocityPanel date={date} underlying={underlying} />
      </div>
      {/* ── Row 1.5: AI Recommendation Card (full-width) ── */}
      <div className="px-3 pb-1">
        <AIRecommendPanel date={date} snapTime={snapTime} underlying={underlying} />
      </div>

      {/* ── Row 2: Strike Screener · PCR · Alert Feed ── */}
      <div className="grid grid-cols-3 gap-3 px-3 pb-3 items-stretch">
        <StrikeScreener
          date={date}
          underlying={underlying}
          snapTime={snapTime}
        />
        <PCRDivergencePanel date={date} underlying={underlying} />
        <AlertFeed
          date={date}
          snapTime={snapTime}
          underlying={underlying}
          onAlertsFetched={onAlertsFetched}
        />
      </div>

      {/* ── Row 3: Volume Velocity · Term Structure · VEX/CEX ── */}
      <div className="grid grid-cols-3 gap-3 px-3 pb-3 items-stretch">
        <VolumeVelocityPanel date={date} underlying={underlying} />
        <TermStructurePanel date={date} underlying={underlying} snapTime={snapTime} />
        <VannaCexPanel date={date} snapTime={snapTime} underlying={underlying} />
      </div>

      {/* ── Position Monitor ── */}
      <div className="px-3 pb-3">
        <PositionMonitor />
      </div>

      {/* ── Row 5: Trade Journal (full-width) ── */}
      <div className="px-3 pb-4">
        <TradeJournalPanel date={date} underlying={underlying} />
      </div>
    </div>
  );
}
