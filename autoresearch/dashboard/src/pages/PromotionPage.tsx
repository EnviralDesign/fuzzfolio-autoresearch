import type { ReactNode } from "react";
import { GitBranch, ShieldCheck, ShieldX } from "lucide-react";

import { AttemptTable } from "@/components/attempt-table";
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
import { useViewerState } from "@/hooks/use-viewer-data";
import { compactRunId, formatInt, formatNumber } from "@/lib/utils";

export function PromotionPage() {
  const { data, isLoading, error } = useViewerState();

  if (isLoading) {
    return <div className="py-20 text-sm text-muted-foreground">Loading promotion board…</div>;
  }

  if (!data) {
    return (
      <div className="py-20 text-sm text-destructive">
        {error instanceof Error ? error.message : "Promotion board failed to load."}
      </div>
    );
  }

  const board = data.promotion;
  const selected = board.selected || [];
  const alternates = board.alternates || [];
  const reasons = board.provisional_reasons || [];
  const topPairs = (board.top_similarity_pairs || []).slice(0, 12);

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[1.3fr_0.9fr]">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-4">
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary">Promotion</Badge>
              <Badge variant={board.status === "ready_for_review" ? "default" : "secondary"}>
                {board.status || "unknown"}
              </Badge>
              <Badge variant="outline">{formatInt(board.candidate_count)} considered</Badge>
            </div>
            <div className="space-y-3">
              <CardTitle className="text-4xl leading-tight tracking-tight">
                Promotion is the stricter gate after shortlist, not just the same list with a nicer name.
              </CardTitle>
              <CardDescription className="max-w-3xl text-base leading-7">
                This board is where the corpus tries to answer a harder question: which candidates are strong
                enough, distinct enough, and validated enough to justify serious attention for real promotion.
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-3">
            <FactTile
              icon={<GitBranch className="h-4 w-4" />}
              label="Selected"
              value={formatInt(selected.length)}
            />
            <FactTile
              icon={<ShieldCheck className="h-4 w-4" />}
              label="Similarity pairs"
              value={formatInt(board.similarity_pair_count)}
            />
            <FactTile
              icon={board.status === "ready_for_review" ? <ShieldCheck className="h-4 w-4" /> : <ShieldX className="h-4 w-4" />}
              label="Status"
              value={String(board.status || "unknown")}
            />
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader>
            <CardTitle className="text-2xl tracking-tight">Gate readout</CardTitle>
            <CardDescription className="text-sm leading-7">
              When this is provisional, the board is still useful, but it should be read as a moving gate instead of a settled live list.
            </CardDescription>
          </CardHeader>
          <CardContent>
            {reasons.length > 0 ? (
              <ul className="space-y-3 text-sm leading-7 text-muted-foreground">
                {reasons.map((reason) => (
                  <li key={reason} className="rounded-2xl border border-border/60 bg-background/40 px-4 py-3">
                    {reason}
                  </li>
                ))}
              </ul>
            ) : (
              <div className="rounded-2xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
                No provisional flags are currently raised. This board is ready for review.
              </div>
            )}
          </CardContent>
        </Card>
      </section>

      <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
        <CardHeader>
          <CardTitle className="text-2xl tracking-tight">Promotion set</CardTitle>
          <CardDescription>
            The stricter board survivors, shown with the same utility and sameness fields so you can see why they persisted.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <AttemptTable rows={selected} showSelectionFields caption="Promotion-board survivors." />
        </CardContent>
      </Card>

      <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
        <CardHeader>
          <CardTitle className="text-2xl tracking-tight">Alternates</CardTitle>
          <CardDescription>
            Near misses after the current promotion filters and novelty pressure.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <AttemptTable rows={alternates.slice(0, 24)} showSelectionFields caption="Top promotion alternates." />
        </CardContent>
      </Card>

      <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
        <CardHeader>
          <CardTitle className="text-2xl tracking-tight">Highest similarity collisions</CardTitle>
          <CardDescription>
            A quick read on where the board would start to collapse into sameness if the caps loosened.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-hidden rounded-3xl border border-border/60 bg-background/45">
            <Table>
              <TableHeader>
                <TableRow className="border-border/60">
                  <TableHead>Left</TableHead>
                  <TableHead>Right</TableHead>
                  <TableHead>Similarity</TableHead>
                  <TableHead>Correlation</TableHead>
                  <TableHead>Shared active</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {topPairs.map((pair, index) => (
                  <TableRow key={`${pair.left_attempt_id}-${pair.right_attempt_id}-${index}`} className="border-border/50">
                    <TableCell className="text-sm text-muted-foreground">
                      {String(pair.left_candidate_name || compactRunId(String(pair.left_run_id || "")))}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {String(pair.right_candidate_name || compactRunId(String(pair.right_run_id || "")))}
                    </TableCell>
                    <TableCell>{formatNumber(Number(pair.similarity_score ?? 0), 3)}</TableCell>
                    <TableCell>{formatNumber(Number(pair.positive_correlation ?? 0), 3)}</TableCell>
                    <TableCell>{formatNumber(Number(pair.shared_active_ratio ?? 0), 3)}</TableCell>
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

function FactTile({
  icon,
  label,
  value,
}: {
  icon: ReactNode;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-3xl border border-border/60 bg-background/40 p-4">
      <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
        {icon}
        {label}
      </div>
      <div className="mt-3 text-3xl font-semibold tracking-tight">{value}</div>
    </div>
  );
}
