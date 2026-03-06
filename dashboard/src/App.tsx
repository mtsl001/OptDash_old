import { Component, ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Dashboard } from "@/pages/Dashboard";

// ── Panel-level Error Boundary ────────────────────────────────────────────────
// Catches any runtime error inside a panel so it shows a red card instead
// of crashing the entire dashboard to a blank page.
interface EBState { hasError: boolean; message: string }

class ErrorBoundary extends Component<{ children: ReactNode }, EBState> {
  state: EBState = { hasError: false, message: "" };

  static getDerivedStateFromError(err: Error): EBState {
    return { hasError: true, message: err.message };
  }

  componentDidCatch(err: Error) {
    console.error("[ErrorBoundary]", err);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div
          className="panel flex flex-col items-center justify-center"
          style={{ minHeight: 120, border: "1px solid #C0392B", color: "#C0392B" }}
        >
          <div className="font-semibold text-sm mb-1">⚠ Panel Error</div>
          <div className="text-xs text-ink-muted text-center px-3">{this.state.message}</div>
          <button
            className="mt-2 text-xs underline"
            onClick={() => this.setState({ hasError: false, message: "" })}
          >
            Retry
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

// ── App ───────────────────────────────────────────────────────────────────────
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      retryDelay: 1000,
    },
  },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ErrorBoundary>
        <Dashboard />
      </ErrorBoundary>
    </QueryClientProvider>
  );
}
