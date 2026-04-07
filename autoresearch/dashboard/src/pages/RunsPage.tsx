import { Link } from "react-router-dom";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/data-table";
import { useRuns } from "@/hooks/use-viewer-data";
import { compactRunId, formatDateTime, formatInt, formatNumber, scoreTone } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";
import type { RunSummary } from "@/lib/types";

const runsColumns: ColumnDef<RunSummary, unknown>[] = [
  {
    accessorKey: "run_id",
    header: "Run",
    cell: ({ row }) => (
      <Link
        to={`/runs/${encodeURIComponent(row.original.run_id)}`}
        className="font-medium text-foreground transition hover:text-primary"
      >
        {compactRunId(row.original.run_id)}
      </Link>
    ),
    enableSorting: true,
  },
  {
    accessorKey: "explorer_model",
    header: "Explorer",
    cell: ({ row }) =>
      row.original.explorer_model || row.original.explorer_profile || "—",
    enableSorting: true,
  },
  {
    accessorKey: "attempt_count",
    header: "Attempts",
    cell: ({ row }) => formatInt(row.original.attempt_count),
    enableSorting: true,
  },
  {
    accessorKey: "score_36m_count",
    header: "36mo scores",
    cell: ({ row }) => formatInt(row.original.score_36m_count),
    enableSorting: true,
  },
  {
    accessorKey: "full_backtest_36m_count",
    header: "Full 36mo",
    cell: ({ row }) => formatInt(row.original.full_backtest_36m_count),
    enableSorting: true,
  },
  {
    accessorKey: "best_attempt.score_36m",
    header: "Best 36mo",
    cell: ({ row }) => (
      <span className={scoreTone(row.original.best_attempt?.score_36m ?? null)}>
        {formatNumber(row.original.best_attempt?.score_36m ?? null, 2)}
      </span>
    ),
    enableSorting: true,
  },
  {
    accessorKey: "latest_created_at",
    header: "Latest",
    cell: ({ row }) =>
      formatDateTime(row.original.latest_created_at || row.original.created_at),
    enableSorting: true,
  },
];

function RunsTable({ runs }: { runs: RunSummary[] }) {
  return (
    <DataTable
      columns={runsColumns}
      data={runs}
      emptyMessage="No runs found."
    />
  );
}

export function RunsPage() {
  const { data, isLoading, error } = useRuns();

  if (isLoading) {
    return <div className="py-20 text-sm text-muted-foreground">Loading runs…</div>;
  }

  if (!data) {
    return (
      <div className="py-20 text-sm text-destructive">
        {error instanceof Error ? error.message : "Runs failed to load."}
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[1.15fr_1fr]">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-4">
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary">Runs</Badge>
              <Badge variant="outline">{formatInt(data.run_count)} with attempts</Badge>
            </div>
            <div className="space-y-3">
              <CardTitle className="text-4xl leading-tight tracking-tight">
                Runs are provenance, not the primary ranking unit anymore.
              </CardTitle>
              <CardDescription className="max-w-3xl text-base leading-7">
                This page keeps run lineage visible so you can still audit where shortlisted or promoted candidates came from without letting "best per run" dominate the whole corpus story.
              </CardDescription>
            </div>
          </CardHeader>
        </Card>
      </section>

      <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
        <CardHeader>
          <CardTitle className="text-2xl tracking-tight">Run ledger</CardTitle>
          <CardDescription>
            Sorted newest first. Click through to inspect the attempts associated with a run.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <RunsTable runs={data.runs} />
        </CardContent>
      </Card>
    </div>
  );
}