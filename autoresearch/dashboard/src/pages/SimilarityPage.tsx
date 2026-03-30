import { useDashboard } from "@/hooks/use-dashboard";
import { useNavigate } from "react-router-dom";
import { PageHeader } from "@/components/ui/PageHeader";
import { Panel, PanelHeader } from "@/components/ui/Panel";
import { DataTable } from "@/components/ui/DataTable";
import { formatNumber, shortRunId } from "@/lib/utils";
import type { SimilarityPair } from "@/lib/types";
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
} from "recharts";

export function SimilarityPage() {
  const { data, isLoading } = useDashboard();
  const navigate = useNavigate();

  if (isLoading || !data) {
    return (
      <div className="p-6">
        <PageHeader title="Similarity" eyebrow="Diversity" />
        <div className="h-96 rounded-xl border border-border bg-card/30 animate-pulse" />
      </div>
    );
  }

  const scatterData = data.similarity.map((r) => ({
    ...r,
    shortLabel: r.candidate_name?.slice(0, 20) || shortRunId(r.run_id),
  }));

  const pairColumns = [
    {
      key: "pair",
      label: "Pair",
      render: (row: SimilarityPair) => (
        <div className="min-w-0">
          <div className="text-sm font-medium truncate">{shortRunId(row.left_run_id)}</div>
          <div className="text-xs text-muted-foreground truncate">{shortRunId(row.right_run_id)}</div>
        </div>
      ),
    },
    { key: "sameness", label: "Sameness", render: (row: SimilarityPair) => formatNumber(row.similarity_score, 2) },
    { key: "corr", label: "Corr", render: (row: SimilarityPair) => formatNumber(row.positive_correlation, 2) },
    { key: "overlap", label: "Overlap", render: (row: SimilarityPair) => `${Math.round((row.shared_active_ratio || 0) * 100)}%` },
  ];

  return (
    <div className="p-6 space-y-6">
      <PageHeader title="Similarity & Diversity" eyebrow="Diversity" description="High-scoring leaders that aren't clones. Top-left is the sweet spot." />

      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">
        <Panel className="xl:col-span-3">
          <PanelHeader eyebrow="Diversity Map" title="36m Score vs Closest-Match Sameness" note="Top-left: strong and not a clone" />
          {scatterData.length > 0 ? (
            <ResponsiveContainer width="100%" height={380}>
              <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" />
                <XAxis type="number" dataKey="max_sameness" name="Sameness" domain={[0, 1]} tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "Closest-Match Sameness", position: "bottom", fill: "#99abc4", fontSize: 11, offset: 0 }} />
                <YAxis type="number" dataKey="score_36m" name="36m Score" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "36m Quality Score", angle: -90, position: "insideLeft", fill: "#99abc4", fontSize: 11 }} />
                <Tooltip contentStyle={{ backgroundColor: "rgba(7, 12, 22, 0.95)", border: "1px solid rgba(147, 190, 255, 0.2)", borderRadius: "8px", fontSize: "12px", color: "#ebf3ff" }} />
                <Scatter data={scatterData}>
                  {scatterData.map((entry, i) => (
                    <Cell key={i} fill={entry.max_sameness < 0.5 ? "#60d6c3" : "#ffba6d"} cursor="pointer" onClick={() => navigate(`/runs/${encodeURIComponent(entry.run_id)}`)} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-80 flex items-center justify-center text-muted-foreground text-sm">No similarity data yet</div>
          )}
        </Panel>

        <Panel className="xl:col-span-2">
          <PanelHeader eyebrow="Closest Matches" title="Most Similar Validated Pairs" />
          <DataTable columns={pairColumns} data={data.similarityPairs} maxHeight="360px" onRowClick={(row) => navigate(`/runs/${encodeURIComponent(row.left_run_id)}`)} />
        </Panel>
      </div>
    </div>
  );
}
