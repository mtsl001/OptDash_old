import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";

// Refresh every 5 seconds during market hours (matches pipeline cadence)
const LIVE_STALE = 5_000;
const SLOW_STALE = 30_000;

export function useGEX(date: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["gex", date, underlying],
    queryFn: () => api.gex(date, underlying).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
  });
}

export function useCoC(date: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["coc", date, underlying],
    queryFn: () => api.coc(date, underlying).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
  });
}

export function useEnvironment(date: string, snapTime: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["env", date, snapTime, underlying],
    queryFn: () => api.environment(date, snapTime, underlying).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
    enabled: !!snapTime,
  });
}

export function useSpot(date: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["spot", date, underlying],
    queryFn: () => api.spot(date, underlying).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
  });
}

export function useAlerts(date: string, snapTime: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["alerts", date, snapTime, underlying],
    queryFn: () => api.alerts(date, snapTime, underlying).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
    enabled: !!snapTime,
  });
}

export function useStrikes(date: string, underlying = "NIFTY", snapTime = "09:20") {
  return useQuery({
    queryKey: ["strikes", date, underlying, snapTime],
    queryFn: () => api.strikes(date, underlying, snapTime).then((r) => r.data),
    refetchInterval: SLOW_STALE,
    staleTime: SLOW_STALE,
  });
}

export function useIVP(underlying = "NIFTY") {
  return useQuery({
    queryKey: ["ivp", underlying],
    queryFn: () => api.ivp(underlying).then((r) => r.data),
    staleTime: 60_000,
  });
}

export function useTermStructure(date: string, underlying = "NIFTY", snapTime = "15:30") {
  return useQuery({
    queryKey: ["termStructure", date, underlying, snapTime],
    queryFn: () => api.termStructure(date, underlying, snapTime).then((r) => r.data),
    refetchInterval: SLOW_STALE,
    staleTime: SLOW_STALE,
  });
}

export function usePCR(date: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["pcr", date, underlying],
    queryFn: () => api.pcr(date, underlying).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
  });
}

export function useVolumeVelocity(date: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["volVelocity", date, underlying],
    queryFn: () => api.volumeVelocity(date, underlying).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
  });
}

export function useVexCex(date: string, snapTime: string, underlying = "NIFTY") {
  return useQuery({
    queryKey: ["vexCex", date, snapTime, underlying],
    queryFn: () => api.vexCex(date, underlying, snapTime).then((r) => r.data),
    refetchInterval: LIVE_STALE,
    staleTime: LIVE_STALE,
    enabled: !!snapTime,
  });
}
