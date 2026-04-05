import { useParams } from "react-router-dom";

import { AttemptTable } from "@/components/attempt-table";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useRunDetail } from "@/hooks/use-viewer-data";
import { compactRunId, formatDateTime, formatInt, formatNumber } from "@/lib/utils";

export function RunDetailPage() {
  const { runId } = useParams();
  const { data, isLoading, error } = useRunDetail(runId);

  if (isLoading) {
    return <div className="py-20 text-sm text-muted-foreground">Loading run detail…</div>;
  }

  if (!data?.run) {
    return (
      <div className="py-20 text-sm text-destructive">
        {error instanceof Error ? error.message : "Run detail failed to load."}
      </div>
    );
  }

  const run = data.run;

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[1.2fr_1fr]">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-4">
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary">Run detail</Badge>
              <Badge variant="outline">{compactRunId(run.run_id)}</Badge>
            </div>
            <div className="space-y-3">
              <CardTitle className="text-4xl leading-tight tracking-tight">
                {run.explorer_model || run.explorer_profile || "Unknown explorer"}
              </CardTitle>
              <CardDescription className="max-w-3xl text-base leading-7">
                A provenance view for one run. Useful for understanding how many attempts it produced and what its best 36-month survivor looked like.
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-3">
            <RunFact label="Attempts" value={formatInt(run.attempt_count)} />
            <RunFact label="36mo scores" value={formatInt(run.score_36m_count)} />
            <RunFact label="Best 36mo" value={formatNumber(run.best_attempt?.score_36m ?? null, 2)} />
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader>
            <CardTitle className="text-2xl tracking-tight">Metadata</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-7 text-muted-foreground">
            <div>Created: {formatDateTime(run.created_at)}</div>
            <div>Latest attempt: {formatDateTime(run.latest_created_at)}</div>
            <div>Quality preset: {String(run.quality_score_preset || "—")}</div>
            <div>Supervisor: {String(run.supervisor_model || run.supervisor_profile || "—")}</div>
          </CardContent>
        </Card>
      </section>

      <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
        <CardHeader>
          <CardTitle className="text-2xl tracking-tight">Attempts in run</CardTitle>
          <CardDescription>
            Sorted by 36-month score when present, otherwise by composite score.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <AttemptTable rows={data.attempts} caption="Attempts loaded from the current attempt catalog." />
        </CardContent>
      </Card>
    </div>
  );
}

function RunFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-3xl border border-border/60 bg-background/40 p-4">
      <div className="text-[0.72rem] uppercase tracking-[0.18em] text-muted-foreground">{label}</div>
      <div className="mt-3 text-3xl font-semibold tracking-tight">{value}</div>
    </div>
  );
}
