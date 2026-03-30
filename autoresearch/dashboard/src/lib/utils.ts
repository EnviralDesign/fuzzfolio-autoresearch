import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

export function formatInt(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString();
}

export function formatTime(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

export function formatTimeAgo(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export function scoreColor(score: number | null | undefined): string {
  if (score === null || score === undefined) return "text-destructive";
  if (score >= 80) return "text-success";
  if (score >= 60) return "text-primary";
  if (score >= 40) return "text-warning";
  return "text-danger";
}

export function scoreBg(score: number | null | undefined): string {
  if (score === null || score === undefined) return "bg-destructive/10 border-destructive/20";
  if (score >= 80) return "bg-success/10 border-success/20";
  if (score >= 60) return "bg-primary/10 border-primary/20";
  if (score >= 40) return "bg-warning/10 border-warning/20";
  return "bg-danger/10 border-danger/20";
}

export function shortRunId(runId: string): string {
  if (!runId) return "—";
  const parts = runId.split("-");
  if (parts.length >= 3) {
    return parts[parts.length - 1];
  }
  return runId.slice(0, 10);
}
