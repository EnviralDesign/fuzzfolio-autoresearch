import { useDeferredValue, useState } from "react";
import { Filter, Search } from "lucide-react";

import { AttemptTable } from "@/components/attempt-table";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useCatalog } from "@/hooks/use-viewer-data";
import type { AttemptCatalogRow } from "@/lib/types";
import { formatInt } from "@/lib/utils";

export function CatalogPage() {
  const { data, isLoading, error } = useCatalog();
  const [query, setQuery] = useState("");
  const [scoreBand, setScoreBand] = useState("all");
  const [validationFilter, setValidationFilter] = useState("all");
  const deferredQuery = useDeferredValue(query.trim().toLowerCase());

  if (isLoading) {
    return <div className="py-20 text-sm text-muted-foreground">Loading catalog…</div>;
  }

  if (!data) {
    return (
      <div className="py-20 text-sm text-destructive">
        {error instanceof Error ? error.message : "Catalog failed to load."}
      </div>
    );
  }

  const filtered = data.rows
    .filter((row) => matchesQuery(row, deferredQuery))
    .filter((row) => matchesScoreBand(row, scoreBand))
    .filter((row) => matchesValidation(row, validationFilter))
    .sort((left, right) => {
      const rightScore = Number(right.score_36m ?? right.composite_score ?? -Infinity);
      const leftScore = Number(left.score_36m ?? left.composite_score ?? -Infinity);
      return rightScore - leftScore;
    });

  const visible = filtered.slice(0, 300);

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[1.2fr_1fr]">
        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader className="gap-4">
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary">Catalog</Badge>
              <Badge variant="outline">{formatInt(data.attempt_count)} attempts</Badge>
            </div>
            <div className="space-y-3">
              <CardTitle className="text-4xl leading-tight tracking-tight">
                This is the full attempt universe, filtered in place instead of reduced to one winner per run.
              </CardTitle>
              <CardDescription className="max-w-3xl text-base leading-7">
                Use this page to inspect the real shape of the corpus and find where score, cadence, drawdown,
                and strategy-key diversity are clustering.
              </CardDescription>
            </div>
          </CardHeader>
        </Card>

        <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
          <CardHeader>
            <CardTitle className="text-2xl tracking-tight">Filters</CardTitle>
            <CardDescription>
              Keep this simple for now. The goal is to narrow quickly without inventing a full analyst workstation yet.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4">
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search candidate, run, strategy key, instrument…"
                className="pl-9"
              />
            </div>
            <div className="grid gap-4 md:grid-cols-2">
              <Select value={scoreBand} onValueChange={setScoreBand}>
                <SelectTrigger>
                  <SelectValue placeholder="Score band" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All scores</SelectItem>
                  <SelectItem value="70plus">70+</SelectItem>
                  <SelectItem value="60plus">60+</SelectItem>
                  <SelectItem value="40plus">40+</SelectItem>
                </SelectContent>
              </Select>
              <Select value={validationFilter} onValueChange={setValidationFilter}>
                <SelectTrigger>
                  <SelectValue placeholder="Validation status" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All validation states</SelectItem>
                  <SelectItem value="valid">Valid full-backtest only</SelectItem>
                  <SelectItem value="missing">Missing full-backtest</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>
      </section>

      <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
        <CardHeader>
          <div className="flex flex-wrap items-center gap-3">
            <Badge variant="outline">
              <Filter className="mr-1 h-3.5 w-3.5" />
              Showing {formatInt(visible.length)} of {formatInt(filtered.length)}
            </Badge>
          </div>
          <CardTitle className="text-2xl tracking-tight">Attempt catalog</CardTitle>
          <CardDescription>
            Rows are sorted by 36-month score first, then composite score. The table is intentionally capped to the top 300 filtered rows for performance.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <AttemptTable rows={visible} caption="Filtered catalog rows." />
        </CardContent>
      </Card>
    </div>
  );
}

function matchesQuery(row: AttemptCatalogRow, query: string) {
  if (!query) {
    return true;
  }
  const haystack = [
    row.attempt_id,
    row.run_id,
    row.candidate_name,
    row.strategy_key_36m,
    row.timeframe_36m,
    ...(row.instruments_36m || []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(query);
}

function matchesScoreBand(row: AttemptCatalogRow, band: string) {
  const score = Number(row.score_36m ?? -Infinity);
  if (band === "70plus") {
    return score >= 70;
  }
  if (band === "60plus") {
    return score >= 60;
  }
  if (band === "40plus") {
    return score >= 40;
  }
  return true;
}

function matchesValidation(row: AttemptCatalogRow, value: string) {
  const isValid = row.full_backtest_validation_status_36m === "valid";
  if (value === "valid") {
    return isValid;
  }
  if (value === "missing") {
    return !row.has_full_backtest_36m;
  }
  return true;
}
