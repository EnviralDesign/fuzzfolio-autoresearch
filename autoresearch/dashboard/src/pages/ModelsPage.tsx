import { useDashboard } from "@/hooks/use-dashboard";
import { PageHeader } from "@/components/ui/PageHeader";
import { DataTable } from "@/components/ui/DataTable";
import { Panel, PanelHeader } from "@/components/ui/Panel";
import { formatNumber, formatInt } from "@/lib/utils";
import type { ModelConsistencyRow } from "@/lib/types";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
} from "recharts";

export function ModelsPage() {
  const { data, isLoading } = useDashboard();

  if (isLoading || !data) {
    return (
      <div className="p-6">
        <PageHeader title="Model Consistency" eyebrow="Analysis" />
        <div className="h-96 rounded-xl border border-border bg-card/30 animate-pulse" />
      </div>
    );
  }

  const barData = data.modelConsistency
    .filter((r) => r.runCount >= 1)
    .map((r) => ({
      ...r,
      shortLabel: r.modelLabel.split("/").pop()?.replace(/:.*$/, "") || r.modelLabel,
    }));

  const columns = [
    { key: "model", label: "Model", render: (row: ModelConsistencyRow) => <span className="text-sm font-medium truncate max-w-56 block">{row.modelLabel}</span> },
    { key: "runs", label: "Runs", render: (row: ModelConsistencyRow) => formatInt(row.runCount) },
    { key: "avg", label: "Avg", render: (row: ModelConsistencyRow) => formatNumber(row.averageScore, 2) },
    { key: "median", label: "Median", render: (row: ModelConsistencyRow) => formatNumber(row.medianScore, 2) },
    { key: "best", label: "Best", render: (row: ModelConsistencyRow) => formatNumber(row.bestScore, 2) },
    { key: "70plus", label: "≥70", render: (row: ModelConsistencyRow) => `${Math.round((row.score70PlusRate || 0) * 100)}%` },
    { key: "80plus", label: "≥80", render: (row: ModelConsistencyRow) => `${Math.round((row.score80PlusRate || 0) * 100)}%` },
  ];

  return (
    <div className="p-6 space-y-6">
      <PageHeader title="Model Consistency" eyebrow="Analysis" description="Which explorer models convert runs into quality most reliably?" />

      <Panel>
        <PanelHeader eyebrow="Comparison" title="Average Score by Model" />
        {barData.length > 0 ? (
          <ResponsiveContainer width="100%" height={Math.max(260, barData.length * 36 + 60)}>
            <BarChart data={barData} layout="vertical" margin={{ top: 5, right: 30, bottom: 5, left: 120 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" horizontal={false} />
              <XAxis type="number" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} />
              <YAxis type="category" dataKey="shortLabel" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={false} width={110} />
              <Tooltip contentStyle={{ backgroundColor: "rgba(7, 12, 22, 0.95)", border: "1px solid rgba(147, 190, 255, 0.2)", borderRadius: "8px", fontSize: "12px", color: "#ebf3ff" }} />
              <Bar dataKey="averageScore" radius={[0, 6, 6, 0]}>
                {barData.map((entry, i) => (
                  <Cell key={i} fill={entry.averageScore >= 70 ? "#60d6c3" : entry.averageScore >= 50 ? "#ffba6d" : "#ff7f7faa"} fillOpacity={0.85} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-40 flex items-center justify-center text-muted-foreground text-sm">No model data yet</div>
        )}
      </Panel>

      <DataTable columns={columns} data={data.modelConsistency} maxHeight="400px" />
    </div>
  );
}
