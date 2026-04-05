import { Link } from "react-router-dom";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useRuns } from "@/hooks/use-viewer-data";
import { compactRunId, formatDateTime, formatInt, formatNumber, scoreTone } from "@/lib/utils";

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
                This page keeps run lineage visible so you can still audit where shortlisted or promoted candidates came from without letting “best per run” dominate the whole corpus story.
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
          <div className="overflow-hidden rounded-3xl border border-border/60 bg-background/45">
            <Table>
              <TableHeader>
                <TableRow className="border-border/60">
                  <TableHead>Run</TableHead>
                  <TableHead>Explorer</TableHead>
                  <TableHead>Attempts</TableHead>
                  <TableHead>36mo scores</TableHead>
                  <TableHead>Full 36mo</TableHead>
                  <TableHead>Best 36mo</TableHead>
                  <TableHead>Latest</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.runs.map((run) => (
                  <TableRow key={run.run_id} className="border-border/50">
                    <TableCell>
                      <Link
                        to={`/runs/${encodeURIComponent(run.run_id)}`}
                        className="font-medium text-foreground transition hover:text-primary"
                      >
                        {compactRunId(run.run_id)}
                      </Link>
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {run.explorer_model || run.explorer_profile || "—"}
                    </TableCell>
                    <TableCell>{formatInt(run.attempt_count)}</TableCell>
                    <TableCell>{formatInt(run.score_36m_count)}</TableCell>
                    <TableCell>{formatInt(run.full_backtest_36m_count)}</TableCell>
                    <TableCell className={scoreTone(run.best_attempt?.score_36m ?? null)}>
                      {formatNumber(run.best_attempt?.score_36m ?? null, 2)}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {formatDateTime(run.latest_created_at || run.created_at)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
