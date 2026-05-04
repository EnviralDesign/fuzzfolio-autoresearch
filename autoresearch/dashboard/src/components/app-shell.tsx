import type { ReactNode } from "react";
import { Activity, Database, WalletCards } from "lucide-react";

import { Button } from "@/components/ui/button";
import { useViewerState } from "@/hooks/use-viewer-data";
import { formatDateTime, formatInt } from "@/lib/utils";

type AppShellProps = {
  children: ReactNode;
};

export function AppShell({ children }: AppShellProps) {
  const { data } = useViewerState();

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-30 border-b border-border/60 bg-background/88 backdrop-blur-xl">
        <div className="mx-auto flex w-full max-w-[1880px] items-center justify-between gap-4 px-4 py-3 md:px-6">
          <div className="flex min-w-0 items-center gap-4">
            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg border border-amber-300/35 bg-amber-300/10 text-amber-100">
              <WalletCards className="h-5 w-5" />
            </div>
            <div className="min-w-0">
              <div className="truncate text-sm font-semibold tracking-tight md:text-base">
                Fuzzfolio Portfolio Workbench
              </div>
              <div className="truncate text-xs text-muted-foreground">
                Runs to attempts to a temporary live strategy set
              </div>
            </div>
          </div>

          <div className="hidden items-center gap-2 lg:flex">
            <HeaderPill
              icon={<Database className="h-3.5 w-3.5" />}
              label="Valid 36mo"
              value={formatInt(data?.corpus_summary?.attempts_with_valid_full_backtest_36m)}
            />
            <HeaderPill
              icon={<Activity className="h-3.5 w-3.5" />}
              label="Updated"
              value={formatDateTime(data?.generated_at)}
            />
            <Button asChild variant="outline" size="sm" className="rounded-lg">
              <a href="/api/catalog" target="_blank" rel="noreferrer">
                Catalog JSON
              </a>
            </Button>
          </div>
        </div>
      </header>

      <div className="mx-auto w-full max-w-[1880px] px-4 py-5 md:px-6">
        {children}
      </div>
    </div>
  );
}

function HeaderPill({
  icon,
  label,
  value,
}: {
  icon: ReactNode;
  label: string;
  value: string;
}) {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-border/60 bg-card/70 px-3 py-2 text-xs">
      <span className="text-muted-foreground">{icon}</span>
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium text-foreground">{value}</span>
    </div>
  );
}
