import { ExternalLink } from "lucide-react";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { ChartAsset } from "@/lib/types";
import { cn } from "@/lib/utils";

type ChartPanelProps = {
  title: string;
  description: string;
  chart?: ChartAsset | null;
  className?: string;
};

export function ChartPanel({ title, description, chart, className }: ChartPanelProps) {
  return (
    <Card className={cn("border-border/60 bg-card/80 shadow-2xl shadow-black/20", className)}>
      <CardHeader className="gap-2">
        <div className="flex items-start justify-between gap-4">
          <div className="space-y-1">
            <CardTitle className="text-xl tracking-tight">{title}</CardTitle>
            <CardDescription className="max-w-2xl text-sm leading-6">
              {description}
            </CardDescription>
          </div>
          {chart?.url ? (
            <a
              href={chart.url}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-1 text-xs uppercase tracking-[0.16em] text-muted-foreground transition hover:text-foreground"
            >
              Open
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          ) : null}
        </div>
      </CardHeader>
      <CardContent>
        {chart?.url ? (
          <a href={chart.url} target="_blank" rel="noreferrer" className="block overflow-hidden rounded-3xl border border-border/60 bg-background/60">
            <img src={chart.url} alt={title} className="h-auto w-full object-cover" />
          </a>
        ) : (
          <div className="flex min-h-72 items-center justify-center rounded-3xl border border-dashed border-border/60 bg-background/50 text-sm text-muted-foreground">
            Chart not generated yet.
          </div>
        )}
      </CardContent>
    </Card>
  );
}
