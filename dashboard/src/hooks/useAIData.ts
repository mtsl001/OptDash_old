import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/client";

const LIVE_STALE = 5_000;

export function useRecommendation(date: string, snapTime: string, underlying = "NIFTY") {
    return useQuery({
        queryKey: ["aiRecommend", date, snapTime, underlying],
        queryFn: () => api.aiRecommend(date, snapTime, underlying).then((r) => r.data),
        refetchInterval: LIVE_STALE,
        staleTime: LIVE_STALE,
        enabled: !!snapTime,
    });
}

export function useActiveTrades() {
    return useQuery({
        queryKey: ["aiActive"],
        queryFn: () => api.aiActiveTrades().then((r) => r.data),
        refetchInterval: LIVE_STALE,
        staleTime: LIVE_STALE,
    });
}

export function useTradeHistory(tradeDate?: string, underlying?: string) {
    return useQuery({
        queryKey: ["aiHistory", tradeDate, underlying],
        queryFn: () => api.aiHistory(tradeDate, underlying).then((r) => r.data),
        refetchInterval: 30_000,
        staleTime: 30_000,
    });
}

export function useTradeStats() {
    return useQuery({
        queryKey: ["aiStats"],
        queryFn: () => api.aiStats().then((r) => r.data),
        staleTime: 30_000,
    });
}

export function usePnLSeries() {
    return useQuery({
        queryKey: ["aiPnLSeries"],
        queryFn: () => api.aiPnLSeries().then((r) => r.data),
        staleTime: 30_000,
    });
}

export function useLearning() {
    return useQuery({
        queryKey: ["aiLearning"],
        queryFn: () => api.aiLearning().then((r) => r.data),
        staleTime: 60_000,
    });
}

export function useRegret() {
    return useQuery({
        queryKey: ["aiRegret"],
        queryFn: () => api.aiRegret().then((r) => r.data),
        staleTime: 60_000,
    });
}

export function useAcceptTrade() {
    const qc = useQueryClient();
    return useMutation({
        mutationFn: (tradeId: string) => api.aiAccept(tradeId).then((r) => r.data),
        onSuccess: () => {
            qc.invalidateQueries({ queryKey: ["aiRecommend"] });
            qc.invalidateQueries({ queryKey: ["aiActive"] });
            qc.invalidateQueries({ queryKey: ["aiHistory"] });
        },
    });
}

export function useRejectTrade() {
    const qc = useQueryClient();
    return useMutation({
        mutationFn: (tradeId: string) => api.aiReject(tradeId).then((r) => r.data),
        onSuccess: () => {
            qc.invalidateQueries({ queryKey: ["aiRecommend"] });
            qc.invalidateQueries({ queryKey: ["aiHistory"] });
        },
    });
}
