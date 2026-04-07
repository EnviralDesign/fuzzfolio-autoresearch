import { useState } from "react";
import type { ReactNode } from "react";
import { GitBranch, ShieldCheck, ShieldX } from "lucide-react";

import { AttemptTable } from "@/components/attempt-table";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/data-table";
import { ProfileDropModal } from "@/components/profile-drop-modal";
import { useViewerState } from "@/hooks/use-viewer-data";
import { compactRunId, formatInt, formatNumber } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";
import type { AttemptCatalogRow } from "@/lib/types";

type SimilarityPair = Record<string, unknown>;

const similarityColumns: ColumnDef<SimilarityPair, unknown>[] = [
  {
    accessorKey: "left_candidate_name",
    header: "Left",
    cell: ({ row }) =>
      String(row.original.left_candidate_name || compactRunId(String(row.original.left_run_id || ""))),
    enableSorting: true,
  },
  {
    accessorKey: "right_candidate_name",
    header: "Right",
    cell: ({ row }) =>
      String(row.original.right_candidate_name || compactRunId(String(row.original.right_run_id || ""))),
    enableSorting: true,
  },
  {
    accessorKey: "similarity_score",
    header: "Similarity",
    cell: ({ row }) => formatNumber(Number(row.original.similarity_score ?? 0), 3),
    enableSorting: true,
  },
  {
    accessorKey: "positive_correlation",
    header: "Correlation",
    cell: ({ row }) => formatNumber(Number(row.original.positive_correlation ?? 0), 3),
    enableSorting: true,
  },
  {
    accessorKey: "shared_active_ratio",
    header: "Shared active",
    cell: ({ row }) => formatNumber(Number(row.original.shared_active_ratio ?? 0), 3),
    enableSorting: true,
  },
];

function SimilarityPairsTable({ pairs }: { pairs: SimilarityPair[] }) {
  return (
    <DataTable
      columns={similarityColumns}
      data={pairs}
      emptyMessage="No similarity pairs to display."
    />
  );
}

export function PromotionPage() {
  const { data, isLoading, error } = useViewerState();
  const [selectedAttempt, setSelectedAttempt] = useState<AttemptCatalogRow | null>(null);

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
    <>
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
            <AttemptTable
              rows={selected}
              showSelectionFields
              caption="Promotion-board survivors."
              onAttemptClick={setSelectedAttempt}
            />
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
            <AttemptTable
              rows={alternates.slice(0, 24)}
              showSelectionFields
              caption="Top promotion alternates."
              onAttemptClick={setSelectedAttempt}
            />
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
            <SimilarityPairsTable pairs={topPairs} />
          </CardContent>
        </Card>
      </div>
      <ProfileDropModal
        isOpen={selectedAttempt !== null}
        onClose={() => setSelectedAttempt(null)}
        profilePathUrl={selectedAttempt?.profile_path_url ?? null}
        candidateName={selectedAttempt?.candidate_name || selectedAttempt?.attempt_id || ""}
      />
    </>
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