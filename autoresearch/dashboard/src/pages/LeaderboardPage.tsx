import { useDashboard } from "@/hooks/use-dashboard";
import { useNavigate } from "react-router-dom";
import { PageHeader } from "@/components/ui/PageHeader";
import { DataTable } from "@/components/ui/DataTable";
import { ScoreBadge } from "@/components/ui/ScoreBadge";
import { Panel, PanelHeader } from "@/components/ui/Panel";
import { QualityRadar } from "@/components/charts/QualityRadar";
import { formatNumber, formatInt, shortRunId } from "@/lib/utils";
import { useState } from "react";
import type { LeaderboardRow } from "@/lib/types";

export function LeaderboardPage() {
  const { data, isLoading } = useDashboard();
  const navigate = useNavigate();
  const [selected, setSelected] = useState<LeaderboardRow | null>(null);

  if (isLoading || !data) {
    return (
      <div className="p-6">
        <PageHeader title="Leaderboard" eyebrow="Rankings" />
        <div className="h-96 rounded-xl border border-border bg-card/30 animate-pulse" />
      </div>
    );
  }

  const columns = [
    {
      key: "rank",
      label: "#",
      className: "w-10",
      render: (_: LeaderboardRow, i: number) => (
        <span className="text-muted-foreground font-mono text-xs">{i + 1}</span>
      ),
    },
    {
      key: "candidate",
      label: "Candidate",
      render: (row: LeaderboardRow) => (
        <div className="min-w-0">
          <div className="font-medium text-sm truncate max-w-48">{row.candidate_name}</div>
          <div className="text-xs text-muted-foreground truncate">
            {shortRunId(row.run_id)} · {row.run_metadata?.explorer_model || "—"}
          </div>
        </div>
      ),
    },
    {
      key: "score",
      label: "Score",
      render: (row: LeaderboardRow) => <ScoreBadge score={row.composite_score} />,
    },
    {
      key: "pf",
      label: "PF",
      render: (row: LeaderboardRow) =>
        formatNumber(row.best_summary?.best_cell?.profit_factor, 2),
    },
    {
      key: "trades",
      label: "Trades",
      render: (row: LeaderboardRow) =>
        formatInt(row.best_summary?.best_cell_path_metrics?.trade_count),
    },
    {
      key: "dd",
      label: "MaxDD",
      render: (row: LeaderboardRow) => {
        const dd = row.best_summary?.best_cell_path_metrics?.max_drawdown_r;
        return dd != null ? `${formatNumber(dd, 1)}R` : "—";
      },
    },
    {
      key: "expectancy",
      label: "Exp R",
      render: (row: LeaderboardRow) =>
        formatNumber(row.best_summary?.best_cell?.avg_net_r_per_closed_trade, 2),
    },
    {
      key: "selectivity",
      label: "Style",
      render: (row: LeaderboardRow) => (
        <span className="text-xs text-muted-foreground">
          {row.best_summary?.behavior_summary?.signal_selectivity || "—"}
        </span>
      ),
    },
  ].map((col) => {
    const origRender = col.render;
    return {
      ...col,
      render: (row: LeaderboardRow) =>
        origRender(row, data.leaderboard.indexOf(row)),
    };
  });

  return (
    <div className="p-6 space-y-6">
      <PageHeader
        title="Leaderboard"
        eyebrow="Rankings"
        description={`Top ${data.leaderboard.length} best-per-run candidates ranked by quality score.`}
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="xl:col-span-2">
          <DataTable
            columns={columns}
            data={data.leaderboard}
            maxHeight="640px"
            onRowClick={(row) => {
              setSelected(row);
              navigate(`/runs/${encodeURIComponent(row.run_id)}`);
            }}
          />
        </div>

        {/* Quality radar for selected or top candidate */}
        <Panel className="self-start">
          <PanelHeader
            eyebrow="Quality Breakdown"
            title={selected?.candidate_name || data.leaderboard[0]?.candidate_name || "Select a candidate"}
          />
          {(() => {
            const row = selected || data.leaderboard[0];
            const components = row?.best_summary?.quality_score_payload?.components;
            if (!components) {
              return (
                <div className="text-sm text-muted-foreground py-8 text-center">
                  No quality score components available
                </div>
              );
            }
            return (
              <QualityRadar
                components={components}
                score={row.composite_score}
              />
            );
          })()}
          {(() => {
            const row = selected || data.leaderboard[0];
            const inputs = row?.best_summary?.quality_score_payload?.inputs;
            if (!inputs) return null;
            return (
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 mt-3 text-xs">
                {Object.entries(inputs).slice(0, 10).map(([k, v]) => (
                  <div key={k} className="flex justify-between gap-1">
                    <span className="text-muted-foreground truncate">{k.replace(/_/g, " ")}</span>
                    <span className="font-mono">{typeof v === "number" ? formatNumber(v, 2) : "—"}</span>
                  </div>
                ))}
              </div>
            );
          })()}
        </Panel>
      </div>
    </div>
  );
}
