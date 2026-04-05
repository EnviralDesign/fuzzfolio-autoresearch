import type { ReactNode } from "react";
import { Filter, GalleryHorizontal, ScanHeart } from "lucide-react";

import { AttemptTable } from "@/components/attempt-table";
import { ChartPanel } from "@/components/chart-panel";
import { ProfileDropGrid } from "@/components/profile-drop-grid";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useViewerState } from "@/hooks/use-viewer-data";
import { formatInt } from "@/lib/utils";

export function ShortlistPage() {
  const { data, isLoading, error } = useViewerState();

  if (isLoading) {
    return <div className="py-20 text-sm text-muted-foreground">Loading shortlist…</div>;
  }

  if (!data) {
    return (
      <div className="py-20 text-sm text-destructive">
        {error instanceof Error ? error.message : "Shortlist failed to load."}
      </div>
    );
  }

  const shortlist = data.shortlist;
  const selected = shortlist.selected || [];
  const alternates = shortlist.alternates || [];
  const profileDrops = shortlist.profile_drops || [];

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[1.3fr_0.9fr]">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-4">
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary">Shortlist</Badge>
              <Badge variant="outline">{formatInt(shortlist.candidate_count)} qualified</Badge>
              <Badge>{formatInt(shortlist.selected_count)} selected</Badge>
            </div>
            <div className="space-y-3">
              <CardTitle className="text-4xl leading-tight tracking-tight">
                This is the first intentionally diverse cut, not just the highest points on one axis.
              </CardTitle>
              <CardDescription className="max-w-3xl text-base leading-7">
                The shortlist starts from the full 36-month qualified pool, then applies caps and novelty
                pressure so the board does not collapse into a pile of near-identical winners.
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-3">
            <KeyFact
              icon={<Filter className="h-4 w-4" />}
              label="Score floor"
              value={String(shortlist.filters?.min_score_36 ?? "—")}
            />
            <KeyFact
              icon={<ScanHeart className="h-4 w-4" />}
              label="Sameness cap"
              value={String(shortlist.filters?.max_sameness_to_board ?? "—")}
            />
            <KeyFact
              icon={<GalleryHorizontal className="h-4 w-4" />}
              label="Per strategy cap"
              value={String(shortlist.filters?.max_per_strategy_key ?? "—")}
            />
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader>
            <CardTitle className="text-2xl tracking-tight">What to look for</CardTitle>
            <CardDescription className="text-sm leading-7">
              Green points should sit near the upper envelope of the corpus while still spreading across
              different run lineages and strategy keys.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4 text-sm leading-7 text-muted-foreground">
            <p>
              If everything clumps on the extreme left wall, that means the score frontier itself is still
              telling us low cadence dominates.
            </p>
            <p>
              If high-cadence candidates survive, they should do so by staying close enough on score while
              also clearing the sameness gate.
            </p>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-6 xl:grid-cols-2">
        <ChartPanel
          title="Shortlist overlay on the corpus distribution"
          description="Gray is the qualified corpus. Green is the chosen set. This is the quickest way to see where selection pressure actually lands."
          chart={data.charts.shortlist_overlay_score_vs_trades}
        />
        <ChartPanel
          title="Similarity heatmap"
          description="The chosen set should not light this up like a solid block. This is where accidental sameness becomes obvious."
          chart={data.charts.shortlist_similarity_heatmap}
        />
      </section>

      <section>
        <Tabs defaultValue="selected" className="space-y-4">
          <TabsList className="bg-background/50">
            <TabsTrigger value="selected">Selected</TabsTrigger>
            <TabsTrigger value="alternates">Alternates</TabsTrigger>
          </TabsList>
          <TabsContent value="selected" className="space-y-4">
            <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
              <CardHeader>
                <CardTitle className="text-2xl tracking-tight">Selected attempts</CardTitle>
                <CardDescription>
                  Utility is score minus novelty pressure minus drawdown penalty.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <AttemptTable
                  rows={selected}
                  showSelectionFields
                  caption="These are the current chosen attempts from the shortlist builder."
                />
              </CardContent>
            </Card>
          </TabsContent>
          <TabsContent value="alternates" className="space-y-4">
            <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
              <CardHeader>
                <CardTitle className="text-2xl tracking-tight">Alternates</CardTitle>
                <CardDescription>
                  The next closest survivors after the current diversity pressure has been applied.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <AttemptTable
                  rows={alternates.slice(0, 24)}
                  showSelectionFields
                  caption="Top alternates by post-penalty utility."
                />
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </section>

      <section className="space-y-4">
        <div className="space-y-1">
          <div className="text-[0.72rem] uppercase tracking-[0.2em] text-muted-foreground">
            Official drops
          </div>
          <h2 className="text-3xl font-semibold tracking-tight">Rendered profile drops for the chosen set</h2>
        </div>
        <ProfileDropGrid items={profileDrops} />
      </section>
    </div>
  );
}

function KeyFact({
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
