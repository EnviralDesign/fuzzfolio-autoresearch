import type {
  AttemptDetail,
  CatalogResponse,
  LivePortfolio,
  RunDetail,
  RunsResponse,
  ViewerState,
} from "@/lib/types";

const BASE = "";

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${BASE}${url}`, init);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${text}`);
  }
  return response.json() as Promise<T>;
}

export function fetchViewerState(): Promise<ViewerState> {
  return fetchJson<ViewerState>("/api/state");
}

export function fetchCatalog(): Promise<CatalogResponse> {
  return fetchJson<CatalogResponse>("/api/catalog");
}

export function fetchRuns(): Promise<RunsResponse> {
  return fetchJson<RunsResponse>("/api/runs");
}

export function fetchRunDetail(runId: string): Promise<RunDetail> {
  return fetchJson<RunDetail>(`/api/runs/${encodeURIComponent(runId)}`);
}

export function fetchAttemptDetail(attemptId: string): Promise<AttemptDetail> {
  return fetchJson<AttemptDetail>(`/api/attempts/${encodeURIComponent(attemptId)}`);
}

export function fetchLivePortfolio(): Promise<LivePortfolio> {
  return fetchJson<LivePortfolio>("/api/live-portfolio");
}

export function saveLivePortfolio(selectedAttemptIds: string[]): Promise<LivePortfolio> {
  return fetchJson<LivePortfolio>("/api/live-portfolio", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ selected_attempt_ids: selectedAttemptIds }),
  });
}
