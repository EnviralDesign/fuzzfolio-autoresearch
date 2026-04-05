import type { ReactNode } from "react";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

type MetricTileProps = {
  label: string;
  value: string;
  detail?: string;
  icon?: ReactNode;
  className?: string;
};

export function MetricTile({ label, value, detail, icon, className }: MetricTileProps) {
  return (
    <Card className={cn("border-border/60 bg-card/80 shadow-2xl shadow-black/20", className)}>
      <CardHeader className="gap-3">
        <div className="flex items-center justify-between gap-3">
          <CardDescription className="text-[0.7rem] uppercase tracking-[0.18em] text-muted-foreground/80">
            {label}
          </CardDescription>
          {icon ? <div className="text-muted-foreground">{icon}</div> : null}
        </div>
        <CardTitle className="text-3xl font-semibold tracking-tight">{value}</CardTitle>
      </CardHeader>
      {detail ? (
        <CardContent className="pt-0 text-sm text-muted-foreground">{detail}</CardContent>
      ) : null}
    </Card>
  );
}
