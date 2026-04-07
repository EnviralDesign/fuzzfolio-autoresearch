import { AlertTriangle, BookMarked, CircleGauge, LibraryBig, ScanSearch } from "lucide-react";

import { ChartPanel } from "@/components/chart-panel";
import { DataTable } from "@/components/data-table";
import { MetricTile } from "@/components/metric-tile";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useViewerState } from "@/hooks/use-viewer-data";
import { formatInt, formatPercent } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";

type CadenceBandRow = {
  band: string;
  count: number;
  mean_score_36m: number | null;
  max_score_36m: number | null;
  mean_drawdown_r_36m: number | null;
};

const cadenceBandColumns: ColumnDef<CadenceBandRow, unknown>[] = [
  {
    accessorKey: "band",
    header: "Band",
    cell: ({ row }) => row.original.band,
    enableSorting: true,
  },
  {
    accessorKey: "count",
    header: "Count",
    cell: ({ row }) => formatInt(row.original.count),
    enableSorting: true,
  },
  {
    accessorKey: "mean_score_36m",
    header: "Mean score",
    cell: ({ row }) =>
      row.original.mean_score_36m != null
        ? row.original.mean_score_36m.toFixed(2)
        : "—",
    enableSorting: true,
  },
  {
    accessorKey: "max_score_36m",
    header: "Best score",
    cell: ({ row }) =>
      row.original.max_score_36m != null
        ? row.original.max_score_36m.toFixed(2)
        : "—",
    enableSorting: true,
  },
  {
    accessorKey: "mean_drawdown_r_36m",
    header: "Mean drawdown",
    cell: ({ row }) =>
      row.original.mean_drawdown_r_36m != null
        ? `${row.original.mean_drawdown_r_36m.toFixed(2)}R`
        : "—",
    enableSorting: true,
  },
];

function CadenceBandTable({ rows }: { rows: CadenceBandRow[] }) {
  return (
    <DataTable
      columns={cadenceBandColumns}
      data={rows}
      emptyMessage="No cadence band data available."
    />
  );
}

export function CorpusPage() {
  const { data, isLoading, error } = useViewerState();

  if (isLoading) {
    return <div className="py-20 text-sm text-muted-foreground">Loading corpus viewer…</div>;
  }

  if (!data) {
    return (
      <div className="py-20 text-sm text-destructive">
        {error instanceof Error ? error.message : "Viewer state failed to load."}
      </div>
    );
  }

  const summary = data.corpus_summary;
  const audit = data.audit;

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[1.4fr_1fr]">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-4">
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary">Corpus</Badge>
              <Badge variant={audit.status === "ready_for_review" ? "default" : "secondary"}>
                {audit.status || "unknown"}
              </Badge>
            </div>
            <div className="space-y-3">
              <CardTitle className="max-w-4xl text-4xl leading-tight tracking-tight">
                The dashboard now starts from evidence coverage, not from whatever UI happened to be built first.
              </CardTitle>
              <CardDescription className="max-w-3xl text-base leading-7">
                This view tells the corpus story in order: how much of the universe is actually validated,
                where the score versus cadence frontier sits, and how much breadth remains after hard
                36-month evidence exists.
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-3">
            <div className="rounded-3xl border border-border/60 bg-background/40 p-4">
              <div className="text-[0.72rem] uppercase tracking-[0.2em] text-muted-foreground">
                Coverage
              </div>
              <div className="mt-2 text-3xl font-semibold">
                {formatPercent(summary.valid_full_backtest_36m_coverage_ratio, 1)}
              </div>
              <p className="mt-2 text-sm leading-6 text-muted-foreground">
                of the total corpus currently has valid local 36-month full-backtests.
              </p>
            </div>
            <div className="rounded-3xl border border-border/60 bg-background/40 p-4">
              <div className="text-[0.72rem] uppercase tracking-[0.2em] text-muted-foreground">
                Serious pool
              </div>
              <div className="mt-2 text-3xl font-semibold">
                {formatInt(summary.score_36m_ge_40)}
              </div>
              <p className="mt-2 text-sm leading-6 text-muted-foreground">
                candidates clear the current 36-month score floor of 40.
              </p>
            </div>
            <div className="rounded-3xl border border-border/60 bg-background/40 p-4">
              <div className="text-[0.72rem] uppercase tracking-[0.2em] text-muted-foreground">
                Strategy breadth
              </div>
              <div className="mt-2 text-3xl font-semibold">
                {formatInt(summary.unique_full_backtest_strategy_count_36m)}
              </div>
              <p className="mt-2 text-sm leading-6 text-muted-foreground">
                distinct strategy keys have real 36-month full-backtest evidence.
              </p>
            </div>
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-3">
            <div className="flex items-center gap-2 text-amber-300">
              <AlertTriangle className="h-4 w-4" />
              <span className="text-xs uppercase tracking-[0.2em]">Read this correctly</span>
            </div>
            <CardTitle className="text-2xl tracking-tight">Selection pressure is not neutral.</CardTitle>
            <CardDescription className="text-sm leading-7">
              The shortlist selector is score-first, then diversity-aware, then drawdown-aware. It does not
              add a separate positive reward for higher trade cadence beyond whatever cadence the score itself
              already credits.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4 text-sm leading-7 text-muted-foreground">
            <p>
              That is why the chosen set tends to hug the left wall when the corpus frontier says the best
              quality still lives at low trade frequency.
            </p>
            <p>
              The cadence tables below are there to keep that bias legible instead of hidden.
            </p>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Attempts"
          value={formatInt(summary.attempt_count)}
          detail="Total attempts in the corpus."
          icon={<LibraryBig className="h-4 w-4" />}
        />
        <MetricTile
          label="Valid 36mo"
          value={formatInt(summary.attempts_with_valid_full_backtest_36m)}
          detail="Portable, validating local 36-month full-backtests."
          icon={<BookMarked className="h-4 w-4" />}
        />
        <MetricTile
          label="Median 36mo score"
          value={summary.median_score_36m != null ? String(summary.median_score_36m.toFixed(1)) : "—"}
          detail="Median across attempts with a parseable 36-month score."
          icon={<CircleGauge className="h-4 w-4" />}
        />
        <MetricTile
          label="Base sensitivity"
          value={formatInt(summary.attempts_with_base_sensitivity)}
          detail="Attempts that at least have base sensitivity artifacts."
          icon={<ScanSearch className="h-4 w-4" />}
        />
      </section>

      <section className="grid gap-6 xl:grid-cols-2">
        <ChartPanel
          title="Corpus score versus trades per month"
          description="This is the main distribution. The upper-right dream quadrant is scarce, so the useful question is where the frontier bends and what still survives there."
          chart={data.charts.corpus_score_vs_trades}
        />
        <ChartPanel
          title="Corpus score versus drawdown"
          description="Use this alongside cadence to separate smooth-but-thin candidates from the genuinely chaotic ones."
          chart={data.charts.corpus_score_vs_drawdown}
        />
      </section>

      <section className="grid gap-6 xl:grid-cols-2">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader>
            <CardTitle className="text-2xl tracking-tight">Cadence bands, all scored rows</CardTitle>
            <CardDescription>
              A fast census of how quality decays as trade cadence rises across the entire scored corpus.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <CadenceBandTable rows={data.cadence_bands_all_scored} />
          </CardContent>
        </Card>
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader>
            <CardTitle className="text-2xl tracking-tight">Cadence bands, score 40+ only</CardTitle>
            <CardDescription>
              The same view restricted to the serious 36-month pool that the shortlist operates on.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <CadenceBandTable rows={data.cadence_bands_score_ge_40} />
          </CardContent>
        </Card>
      </section>
    </div>
  );
}