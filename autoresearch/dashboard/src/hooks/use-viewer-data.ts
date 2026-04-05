import { useQuery } from "@tanstack/react-query";

import {
  fetchAttemptDetail,
  fetchCatalog,
  fetchRunDetail,
  fetchRuns,
  fetchViewerState,
} from "@/lib/api";

export function useViewerState() {
  return useQuery({
    queryKey: ["viewer-state"],
    queryFn: fetchViewerState,
    refetchInterval: 30_000,
    staleTime: 10_000,
  });
}

export function useCatalog() {
  return useQuery({
    queryKey: ["catalog"],
    queryFn: fetchCatalog,
    refetchInterval: 60_000,
    staleTime: 30_000,
  });
}

export function useRuns() {
  return useQuery({
    queryKey: ["runs"],
    queryFn: fetchRuns,
    refetchInterval: 30_000,
    staleTime: 10_000,
  });
}

export function useRunDetail(runId: string | undefined) {
  return useQuery({
    queryKey: ["run-detail", runId],
    queryFn: () => fetchRunDetail(runId!),
    enabled: Boolean(runId),
    refetchInterval: 30_000,
    staleTime: 10_000,
  });
}

export function useAttemptDetail(attemptId: string | undefined) {
  return useQuery({
    queryKey: ["attempt-detail", attemptId],
    queryFn: () => fetchAttemptDetail(attemptId!),
    enabled: Boolean(attemptId),
    staleTime: 30_000,
  });
}
