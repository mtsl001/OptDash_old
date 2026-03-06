# Dashboard Sub-Context — dashboard/

> Loaded automatically when working inside the `dashboard/` directory. Supplements root GEMINI.md.

## Stack
- **Vite** (v5+) + **React 18** + **TypeScript** (strict mode)
- **TanStack Query v5** for all data fetching and caching
- **Recharts** for all charts (ComposedChart, LineChart, AreaChart, BarChart)
- **Tailwind CSS** for styling (dark trading terminal theme)
- **Axios** for HTTP requests

## API Base URL
```
http://127.0.0.1:8000
```
Set via `VITE_API_BASE_URL` in `.env` at the project root (`/Users/apple/Documents/OptDash/.env`).  
Read in `src/api/client.ts` as `import.meta.env.VITE_API_BASE_URL`.

## Polling Strategy
- Live panels (GEX, CoC, PCR, Spot, Alerts): `refetchInterval: 5_000` (5 seconds)
- Slower panels (Strike Screener, IVP, Term Structure): `refetchInterval: 30_000`
- Historical/static (PnL attribution): no `refetchInterval`

## Color Palette (from tailwind.config.js)
```
bg-panel    = #0F1923   page background
bg-surface  = #162030   card/panel background
border      = #243040
brand       = #1B3A6B   deep blue
brand-light = #2E75B6
brand-muted = #D6E4F7
accent      = #E8A020   orange highlights
bull        = #1E7C44   green — positive
bear        = #C0392B   red — negative
ink         = #404040   body text
ink-muted   = #808080   secondary text
```

## Component Rules
- Every panel wraps in `<div className="panel">` — defined in `src/index.css`
- Every panel's first child is `<div className="panel-header">`
- KPI numbers use `className="kpi-value font-mono"`
- Recharts `contentStyle`: `{{ background: "#162030", border: "1px solid #243040" }}`
- Recharts `labelStyle`: `{{ color: "#D6E4F7" }}`
- All axis ticks: `{{ fill: "#808080", fontSize: 11 }}`
- All grid strokes: `"#243040"`

## File Build Order (Avoids Broken Imports)
```
1.  tailwind.config.js
2.  vite.config.ts
3.  tsconfig.json
4.  package.json          → then run: npm install
5.  index.html
6.  src/index.css
7.  src/api/client.ts
8.  src/hooks/useMarketData.ts
9.  src/components/panels/GEXPanel.tsx
10. src/components/panels/CoCVelocityPanel.tsx
11. src/components/panels/EnvironmentGauge.tsx
12. src/components/panels/StrikeScreener.tsx
13. src/components/panels/PCRDivergencePanel.tsx
14. src/components/panels/AlertFeed.tsx
15. src/components/panels/PositionMonitor.tsx
16. src/pages/Dashboard.tsx
17. src/App.tsx
18. src/main.tsx
```

## macOS Shell Commands
```bash
# Scaffold (run from project root)
npm create vite@latest dashboard -- --template react-ts
cd dashboard
npm install
npm install axios @tanstack/react-query recharts
npm install -D tailwindcss postcss autoprefixer @types/node
npx tailwindcss init -p

# Run dev server
npm run dev

# Build for production
npm run build && npm run preview
```

## Never Do This
- Never use `localStorage` or `sessionStorage` — all data comes from the FastAPI server
- Never hardcode dates — always use a `date` state variable passed down from `Dashboard.tsx`
- Never use Tailwind class names that aren't defined in `tailwind.config.js` or `index.css`
- Never use `any` type — define proper TypeScript interfaces in `src/api/client.ts`
- Never import React explicitly in every file — Vite's React plugin handles the JSX transform
