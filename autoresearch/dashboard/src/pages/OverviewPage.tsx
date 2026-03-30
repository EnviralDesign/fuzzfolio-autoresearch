import { useDashboard } from "@/hooks/use-dashboard";
import { PageHeader } from "@/components/ui/PageHeader";
import { MetricCard } from "@/components/ui/MetricCard";
import { Panel, PanelHeader } from "@/components/ui/Panel";
import { formatInt, formatNumber } from "@/lib/utils";
import { Activity, BarChart3, Zap, Target, Layers, TrendingUp } from "lucide-react";
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
} from "recharts";

export function OverviewPage() {
  const { data, isLoading } = useDashboard();

  if (isLoading || !data) {
    return (
      <div className="p-6">
        <PageHeader title="Overview" eyebrow="Dashboard" />
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="h-28 rounded-xl border border-border bg-card/30 animate-pulse" />
          ))}
        </div>
      </div>
    );
  }

  const { overview, leaderboard, runs } = data;

  const scoreDistribution = leaderboard
    .filter((r) => r.composite_score != null)
    .map((r) => ({
      score: r.composite_score,
      trades: r.best_summary?.best_cell_path_metrics?.trade_count ?? 0,
      label: r.candidate_name,
      runId: r.run_id,
    }));

  const recentRuns = runs.slice(0, 8);

  return (
    <div className="p-6 space-y-6">
      <PageHeader
        title="Dashboard Overview"
        eyebrow="Autoresearch"
        description="Runs, models, and profile backtests at a glance."
      />

      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        <MetricCard label="Runs" value={formatInt(overview.runCount)} secondary={`${formatInt(overview.scoredRunCount)} scored`} icon={<Layers className="w-4 h-4" />} />
        <MetricCard label="Attempts" value={formatInt(overview.attemptCount)} secondary="all ledger entries" icon={<Activity className="w-4 h-4" />} />
        <MetricCard label="Best Score" value={formatNumber(overview.bestScore, 2)} secondary="highest quality score" icon={<TrendingUp className="w-4 h-4" />} />
        <MetricCard label="Median Best" value={formatNumber(overview.medianBestScore, 2)} secondary="typical run leader" icon={<Target className="w-4 h-4" />} />
        <MetricCard label="Validated" value={formatInt(overview.validationPointCount)} secondary="12m + 36m tested" icon={<Zap className="w-4 h-4" />} />
        <MetricCard label="Leaderboard" value={formatInt(overview.leaderboardCount)} secondary={`${formatInt(overview.modelBucketCount)} model groups`} icon={<BarChart3 className="w-4 h-4" />} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Panel>
          <PanelHeader eyebrow="Distribution" title="Score vs Trade Count" note="Each point is a leaderboard candidate" />
          {scoreDistribution.length > 0 ? (
            <ResponsiveContainer width="100%" height={320}>
              <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" />
                <XAxis type="number" dataKey="trades" name="Trades" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "Trade Count", position: "bottom", fill: "#99abc4", fontSize: 11, offset: 0 }} />
                <YAxis type="number" dataKey="score" name="Score" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "Quality Score", angle: -90, position: "insideLeft", fill: "#99abc4", fontSize: 11 }} />
                <Tooltip contentStyle={{ backgroundColor: "rgba(7, 12, 22, 0.95)", border: "1px solid rgba(147, 190, 255, 0.2)", borderRadius: "8px", fontSize: "12px", color: "#ebf3ff" }} />
                <Scatter data={scoreDistribution}>
                  {scoreDistribution.map((entry, i) => (
                    <Cell key={i} fill={entry.score >= 80 ? "#60d6c3" : entry.score >= 60 ? "#60d6c3aa" : entry.score >= 40 ? "#ffba6d" : "#ff7f7faa"} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-80 flex items-center justify-center text-muted-foreground text-sm">No leaderboard data yet</div>
          )}
        </Panel>

        <Panel>
          <PanelHeader eyebrow="Activity" title="Recent Runs" note="Latest 8 runs, newest first" />
          <div className="space-y-2">
            {recentRuns.map((run) => (
              <a
                key={run.runId}
                href={`/runs/${encodeURIComponent(run.runId)}`}
                onClick={(e) => { e.preventDefault(); window.location.href = `/runs/${encodeURIComponent(run.runId)}`; }}
                className="flex items-center justify-between gap-3 px-3 py-2.5 rounded-lg border border-border/50 hover:bg-surface-hover hover:border-border transition-colors cursor-pointer"
              >
                <div className="min-w-0">
                  <div className="text-sm font-medium truncate">{run.explorerModel || run.explorerProfile || "unknown"}</div>
                  <div className="text-xs text-muted-foreground truncate">{run.runId.split("-").slice(-1)[0]} · {formatInt(run.attemptCount)} attempts</div>
                </div>
                <div className="text-right shrink-0">
                  {run.bestAttempt?.score != null ? (
                    <span className={`text-sm font-semibold ${run.bestAttempt.score >= 80 ? "text-success" : run.bestAttempt.score >= 60 ? "text-primary" : "text-warning"}`}>
                      {formatNumber(run.bestAttempt.score, 1)}
                    </span>
                  ) : (
                    <span className="text-xs text-muted-foreground">—</span>
                  )}
                </div>
              </a>
            ))}
          </div>
        </Panel>
      </div>
    </div>
  );
}
