import type { ReactNode } from "react";
import { Filter, GalleryHorizontal, ScanHeart } from "lucide-react";

import { AttemptTable } from "@/components/attempt-table";
import { BasketCurvePanel } from "@/components/basket-curve-panel";
import { ChartPanel } from "@/components/chart-panel";
import { ProfileDropGrid } from "@/components/profile-drop-grid";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useViewerState } from "@/hooks/use-viewer-data";
import { formatInt, formatNumber } from "@/lib/utils";

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
  const basket = shortlist.selected_basket_summary;
  const isPortfolio = shortlist.source_type === "portfolio";
  const primaryLabel = isPortfolio ? "Portfolio" : "Shortlist";
  const candidateLabel = isPortfolio ? "union qualified" : "qualified";
  const warning = shortlist.warning;
  const overlapCount =
    typeof shortlist.selected_overlap_count === "number" ? shortlist.selected_overlap_count : null;

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[1.3fr_0.9fr]">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-4">
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary">{primaryLabel}</Badge>
              <Badge variant="outline">{formatInt(shortlist.candidate_count)} {candidateLabel}</Badge>
              <Badge>{formatInt(shortlist.selected_count)} selected</Badge>
              {isPortfolio && overlapCount !== null ? (
                <Badge variant="outline">{formatInt(overlapCount)} overlap</Badge>
              ) : null}
            </div>
            <div className="space-y-3">
              <CardTitle className="text-4xl leading-tight tracking-tight">
                {isPortfolio
                  ? "This is the multi-sleeve portfolio union, not a single-axis ranking."
                  : "This is the first intentionally diverse cut, not just the highest points on one axis."}
              </CardTitle>
              <CardDescription className="max-w-3xl text-base leading-7">
                {isPortfolio
                  ? "The dashboard now treats the latest portfolio build as the primary shortlist. It unions multiple sleeves, then shows the final selected basket and where it sits inside the qualified corpus."
                  : "The shortlist starts from the full 36-month qualified pool, then applies caps and novelty pressure so the board does not collapse into a pile of near-identical winners."}
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-3">
            <KeyFact
              icon={<Filter className="h-4 w-4" />}
              label={isPortfolio ? "Sleeves" : "Score floor"}
              value={
                isPortfolio
                  ? String((shortlist.sleeves || []).length || "—")
                  : String(shortlist.filters?.min_score_36 ?? "—")
              }
            />
            <KeyFact
              icon={<ScanHeart className="h-4 w-4" />}
              label={isPortfolio ? "Overlap" : "Sameness cap"}
              value={
                isPortfolio
                  ? String(overlapCount ?? "—")
                  : String(shortlist.filters?.max_sameness_to_board ?? "—")
              }
            />
            <KeyFact
              icon={<GalleryHorizontal className="h-4 w-4" />}
              label={isPortfolio ? "Portfolio name" : "Per strategy cap"}
              value={
                isPortfolio
                  ? String(shortlist.portfolio_name ?? "—")
                  : String(shortlist.filters?.max_per_strategy_key ?? "—")
              }
            />
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader>
            <CardTitle className="text-2xl tracking-tight">What to look for</CardTitle>
            <CardDescription className="text-sm leading-7">
              {isPortfolio
                ? "Green points should show where the unioned sleeves actually landed after overlap and diversity pressure."
                : "Green points should sit near the upper envelope of the corpus while still spreading across different run lineages and strategy keys."}
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
          title={isPortfolio ? "Portfolio overlay on the corpus distribution" : "Shortlist overlay on the corpus distribution"}
          description={isPortfolio ? "Gray is the union-qualified pool. Green is the final portfolio set." : "Gray is the qualified corpus. Green is the chosen set. This is the quickest way to see where selection pressure actually lands."}
          chart={data.charts.shortlist_overlay_score_vs_trades}
        />
        <ChartPanel
          title="Similarity heatmap"
          description="The chosen set should not light this up like a solid block. This is where accidental sameness becomes obvious."
          chart={data.charts.shortlist_similarity_heatmap}
        />
      </section>

      {warning ? (
        <Card className="border-amber-500/30 bg-amber-500/10 shadow-2xl shadow-black/20">
          <CardHeader>
            <CardTitle className="text-xl tracking-tight text-amber-200">Artifact warning</CardTitle>
            <CardDescription className="text-amber-100/85">{warning}</CardDescription>
          </CardHeader>
        </Card>
      ) : null}

      <section>
        <div className="space-y-6">
          <BasketCurvePanel
            title="Basket curve of the shortlist"
            description={
              isPortfolio
                ? "This is the aggregate mark-to-market basket path across the selected portfolio names, using their local 36-month full-backtest curves."
                : "This is the aggregate mark-to-market basket path across the selected shortlist names, using their local 36-month full-backtest curves."
            }
            curve={shortlist.selected_basket_curve_36m}
          />
          <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
            <CardHeader>
              <CardTitle className="text-2xl tracking-tight">Basket view of the shortlist</CardTitle>
              <CardDescription>
                {isPortfolio
                  ? "These are basket-level rollups for the current portfolio union, using local 36-month full-backtest curves and metadata already in the corpus."
                  : "These are portfolio-level rollups for the currently selected names, using the local 36-month full-backtest curves and metadata already in the corpus."}
              </CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              <KeyFact
                icon={<GalleryHorizontal className="h-4 w-4" />}
                label="Strategies"
                value={formatInt(basket?.strategy_count)}
              />
              <KeyFact
                icon={<Filter className="h-4 w-4" />}
                label="Total trades / month"
                value={formatNumber(basket?.trades_per_month?.sum, 2)}
              />
              <KeyFact
                icon={<Filter className="h-4 w-4" />}
                label="Avg trades / month"
                value={formatNumber(basket?.trades_per_month?.mean, 2)}
              />
              <KeyFact
                icon={<ScanHeart className="h-4 w-4" />}
                label="Total R / month"
                value={formatNumber(basket?.realized_r_per_month_36m?.sum, 2)}
              />
              <KeyFact
                icon={<ScanHeart className="h-4 w-4" />}
                label="Avg R / month"
                value={formatNumber(basket?.realized_r_per_month_36m?.mean, 2)}
              />
              <KeyFact
                icon={<ScanHeart className="h-4 w-4" />}
                label="Avg max DD (R)"
                value={formatNumber(basket?.max_drawdown_r_36m?.mean, 2)}
              />
              <KeyFact
                icon={<ScanHeart className="h-4 w-4" />}
                label="Avg max DD / month"
                value={formatNumber(basket?.max_drawdown_r_per_month_36m?.mean, 3)}
              />
              <KeyFact
                icon={<GalleryHorizontal className="h-4 w-4" />}
                label="Avg 36m score"
                value={formatNumber(basket?.score_36m?.mean, 2)}
              />
              <KeyFact
                icon={<GalleryHorizontal className="h-4 w-4" />}
                label="Total 36m R"
                value={formatNumber(basket?.realized_r_total_36m?.sum, 2)}
              />
            </CardContent>
          </Card>
        </div>
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
                  {isPortfolio
                    ? "These are the currently selected names from the latest multi-sleeve portfolio build."
                    : "Utility is score minus novelty pressure minus drawdown penalty."}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <AttemptTable
                  rows={selected}
                  showSelectionFields
                  caption={
                    isPortfolio
                      ? "These are the current chosen attempts from the portfolio builder."
                      : "These are the current chosen attempts from the shortlist builder."
                  }
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
