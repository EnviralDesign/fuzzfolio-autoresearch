import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

interface PanelProps {
  children: ReactNode;
  className?: string;
}

export function Panel({ children, className }: PanelProps) {
  return (
    <section
      className={cn(
        "rounded-xl border border-border bg-card/60 p-5",
        className
      )}
    >
      {children}
    </section>
  );
}

interface PanelHeaderProps {
  eyebrow?: string;
  title: string;
  note?: string;
  actions?: ReactNode;
}

export function PanelHeader({ eyebrow, title, note, actions }: PanelHeaderProps) {
  return (
    <div className="flex items-start justify-between gap-3 mb-4">
      <div>
        {eyebrow && (
          <p className="text-[10px] font-semibold text-primary uppercase tracking-widest mb-0.5">
            {eyebrow}
          </p>
        )}
        <h3 className="text-base font-semibold tracking-tight">{title}</h3>
        {note && <p className="mt-0.5 text-xs text-muted-foreground">{note}</p>}
      </div>
      {actions && <div className="shrink-0">{actions}</div>}
    </div>
  );
}
