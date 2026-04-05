import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatNumber(value: number | null | undefined, digits = 1) {
  if (value == null || Number.isNaN(value)) {
    return "—"
  }
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value)
}

export function formatPercent(value: number | null | undefined, digits = 1) {
  if (value == null || Number.isNaN(value)) {
    return "—"
  }
  return `${formatNumber(value * 100, digits)}%`
}

export function formatInt(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "—"
  }
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  }).format(value)
}

export function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return "—"
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date)
}

export function compactRunId(runId: string | null | undefined) {
  if (!runId) {
    return "—"
  }
  const parts = runId.split("-")
  return parts.slice(-2).join("-")
}

export function scoreTone(score: number | null | undefined) {
  if (score == null || Number.isNaN(score)) {
    return "text-muted-foreground"
  }
  if (score >= 80) {
    return "text-emerald-300"
  }
  if (score >= 60) {
    return "text-cyan-300"
  }
  if (score >= 40) {
    return "text-amber-300"
  }
  return "text-rose-300"
}
