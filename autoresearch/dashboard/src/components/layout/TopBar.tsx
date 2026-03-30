import { RefreshCw, Circle } from "lucide-react";
import { useRefresh, useDashboard } from "@/hooks/use-dashboard";
import { formatTimeAgo } from "@/lib/utils";
import { cn } from "@/lib/utils";

export function TopBar() {
  const { data, isFetching } = useDashboard();
  const refresh = useRefresh();

  return (
    <header className="flex items-center justify-between gap-4 px-6 py-3 border-b border-border bg-card/30">
      <div className="flex items-center gap-3 text-sm text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <Circle
            className={cn(
              "w-2 h-2 fill-current",
              isFetching ? "text-warning animate-pulse" : "text-success"
            )}
          />
          {isFetching ? "Syncing…" : "Live"}
        </span>
        {data?.overview.generatedAt && (
          <span className="text-xs opacity-70">
            Updated {formatTimeAgo(data.overview.generatedAt)}
          </span>
        )}
      </div>

      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground hidden sm:inline">
          Auto-refresh 30s
        </span>
        <button
          onClick={() => refresh.mutate()}
          disabled={refresh.isPending}
          className={cn(
            "inline-flex items-center gap-2 px-3.5 py-1.5 rounded-lg text-sm font-medium transition-all",
            "bg-primary/10 text-primary border border-primary/20",
            "hover:bg-primary/20 hover:border-primary/30",
            "disabled:opacity-50 disabled:cursor-wait"
          )}
        >
          <RefreshCw
            className={cn("w-3.5 h-3.5", refresh.isPending && "animate-spin")}
          />
          {refresh.isPending ? "Rebuilding…" : "Refresh"}
        </button>
      </div>
    </header>
  );
}
