import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface MetricCardProps {
  label: string;
  value: string;
  secondary?: string;
  icon?: ReactNode;
  className?: string;
}

export function MetricCard({ label, value, secondary, icon, className }: MetricCardProps) {
  return (
    <article
      className={cn(
        "rounded-xl border border-border bg-card/60 p-4 transition-colors hover:bg-card/80",
        className
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
          {label}
        </span>
        {icon && <span className="text-muted-foreground">{icon}</span>}
      </div>
      <div className="mt-2 text-2xl font-bold tracking-tight">{value}</div>
      {secondary && (
        <div className="mt-1 text-xs text-muted-foreground">{secondary}</div>
      )}
    </article>
  );
}
