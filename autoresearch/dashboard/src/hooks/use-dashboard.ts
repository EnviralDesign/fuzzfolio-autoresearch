import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { fetchOverview, triggerRefresh, fetchRunDetail, fetchAttemptDetail, calculateBacktest } from "@/lib/api";

/** Dashboard overview — polls every 30s */
export function useDashboard() {
  return useQuery({
    queryKey: ["dashboard"],
    queryFn: fetchOverview,
    refetchInterval: 30_000,
    staleTime: 10_000,
  });
}

/** Full refresh — triggers the Python pipeline to rebuild from source artifacts */
export function useRefresh() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: triggerRefresh,
    onSuccess: (data) => {
      queryClient.setQueryData(["dashboard"], data);
    },
  });
}

/** Run detail — cached per runId */
export function useRunDetail(runId: string | undefined) {
  return useQuery({
    queryKey: ["run", runId],
    queryFn: () => fetchRunDetail(runId!),
    enabled: !!runId,
    staleTime: 60_000,
  });
}

/** Attempt detail — cached per runId + attemptId */
export function useAttemptDetail(runId: string | undefined, attemptId: string | undefined) {
  return useQuery({
    queryKey: ["attempt", runId, attemptId],
    queryFn: () => fetchAttemptDetail(runId!, attemptId!),
    enabled: !!runId && !!attemptId,
    staleTime: 60_000,
  });
}

/** Calculate full 3yr backtest for an attempt */
export function useCalculateBacktest() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ runId, attemptId }: { runId: string; attemptId: string }) =>
      calculateBacktest(runId, attemptId),
    onSuccess: (data) => {
      queryClient.setQueryData(["attempt", data.runId, data.attempt?.attemptId], data);
    },
  });
}
