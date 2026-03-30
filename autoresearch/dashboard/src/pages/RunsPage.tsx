import { useDashboard } from "@/hooks/use-dashboard";
import { useNavigate } from "react-router-dom";
import { PageHeader } from "@/components/ui/PageHeader";
import { DataTable } from "@/components/ui/DataTable";
import { ScoreBadge } from "@/components/ui/ScoreBadge";
import { formatInt, formatTime, shortRunId } from "@/lib/utils";
import type { RunSummary } from "@/lib/types";

export function RunsPage() {
  const { data, isLoading } = useDashboard();
  const navigate = useNavigate();

  if (isLoading || !data) {
    return (
      <div className="p-6">
        <PageHeader title="All Runs" eyebrow="Runs" />
        <div className="h-96 rounded-xl border border-border bg-card/30 animate-pulse" />
      </div>
    );
  }

  const columns = [
    {
      key: "run",
      label: "Run",
      render: (row: RunSummary) => (
        <div className="min-w-0">
          <div className="text-sm font-medium font-mono">{shortRunId(row.runId)}</div>
          <div className="text-xs text-muted-foreground truncate max-w-52">
            {row.explorerModel || row.explorerProfile || "unknown"}
          </div>
        </div>
      ),
    },
    {
      key: "best",
      label: "Best",
      render: (row: RunSummary) => <ScoreBadge score={row.bestAttempt?.score} />,
    },
    {
      key: "attempts",
      label: "Attempts",
      render: (row: RunSummary) => formatInt(row.attemptCount),
    },
    {
      key: "scored",
      label: "Scored",
      render: (row: RunSummary) => formatInt(row.scoredAttemptCount),
    },
    {
      key: "step",
      label: "Step",
      render: (row: RunSummary) => formatInt(row.latestStep),
    },
    {
      key: "advisors",
      label: "Advisors",
      render: (row: RunSummary) => formatInt(row.advisorGuidanceCount),
    },
    {
      key: "updated",
      label: "Updated",
      className: "text-xs",
      render: (row: RunSummary) =>
        formatTime(row.latestLogTimestamp || row.latestAttemptAt),
    },
  ];

  return (
    <div className="p-6 space-y-6">
      <PageHeader
        title="All Runs"
        eyebrow="Runs"
        description={`${data.runs.length} runs, newest first.`}
      />

      <DataTable
        columns={columns}
        data={data.runs}
        maxHeight="calc(100vh - 200px)"
        onRowClick={(row) => navigate(`/runs/${encodeURIComponent(row.runId)}`)}
      />
    </div>
  );
}
