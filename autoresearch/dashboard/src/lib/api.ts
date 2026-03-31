import type { DashboardPayload, RunDetail, AttemptDetail } from "./types";

const BASE = "";

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, init);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json() as Promise<T>;
}

/** Fetch the full dashboard payload from /api/overview */
export function fetchOverview(): Promise<DashboardPayload> {
  return fetchJson<DashboardPayload>("/api/overview");
}

/** Trigger a full refresh (regenerates from source artifacts) */
export function triggerRefresh(): Promise<DashboardPayload> {
  return fetchJson<DashboardPayload>("/api/refresh", { method: "POST" });
}

/** Fetch detail for a specific run */
export function fetchRunDetail(runId: string): Promise<RunDetail> {
  return fetchJson<RunDetail>(`/api/runs/${encodeURIComponent(runId)}`);
}

/** Fetch detail for a specific attempt within a run */
export function fetchAttemptDetail(runId: string, attemptId: string): Promise<AttemptDetail> {
  return fetchJson<AttemptDetail>(
    `/api/runs/${encodeURIComponent(runId)}/attempts/${encodeURIComponent(attemptId)}`
  );
}

/** Calculate full 3yr backtest for an attempt and return updated detail */
export function calculateBacktest(
  runId: string,
  attemptId: string
): Promise<AttemptDetail> {
  return fetchJson<AttemptDetail>(
    `/api/runs/${encodeURIComponent(runId)}/attempts/${encodeURIComponent(attemptId)}/calculate-backtest`,
    { method: "POST" }
  );
}
