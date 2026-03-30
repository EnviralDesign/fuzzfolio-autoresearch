import { useDashboard } from "@/hooks/use-dashboard";
import { useNavigate } from "react-router-dom";
import { PageHeader } from "@/components/ui/PageHeader";
import { Panel, PanelHeader } from "@/components/ui/Panel";
import { shortRunId } from "@/lib/utils";
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
  ReferenceLine,
  BarChart,
  Bar,
} from "recharts";

export function ValidationPage() {
  const { data, isLoading } = useDashboard();
  const navigate = useNavigate();

  if (isLoading || !data) {
    return (
      <div className="p-6">
        <PageHeader title="Validation" eyebrow="Scrutiny" />
        <div className="h-96 rounded-xl border border-border bg-card/30 animate-pulse" />
      </div>
    );
  }

  const { validation } = data;

  const scatterData = validation.map((r) => ({
    ...r,
    shortLabel: shortRunId(r.run_id),
  }));

  const deltaData = validation
    .map((r) => ({
      ...r,
      shortLabel: r.candidate_name?.slice(0, 20) || shortRunId(r.run_id),
      fill: r.score_delta >= 0 ? "#60d6c3" : "#ff9a76",
    }))
    .sort((a, b) => b.score_delta - a.score_delta);

  return (
    <div className="p-6 space-y-6">
      <PageHeader title="Validation" eyebrow="Scrutiny" description="How candidates hold up under 12-month vs 36-month backtests." />

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <Panel>
          <PanelHeader eyebrow="Validation Map" title="12m vs 36m Score" note="Top-right and near the diagonal survives scrutiny" />
          {scatterData.length > 0 ? (
            <ResponsiveContainer width="100%" height={360}>
              <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" />
                <XAxis type="number" dataKey="score_36m" name="36m Score" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "36m Quality Score", position: "bottom", fill: "#99abc4", fontSize: 11, offset: 0 }} />
                <YAxis type="number" dataKey="score_12m" name="12m Score" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "12m Quality Score", angle: -90, position: "insideLeft", fill: "#99abc4", fontSize: 11 }} />
                <ReferenceLine segment={[{ x: 0, y: 0 }, { x: 100, y: 100 }]} stroke="rgba(96, 214, 195, 0.3)" strokeDasharray="4 4" />
                <Tooltip contentStyle={{ backgroundColor: "rgba(7, 12, 22, 0.95)", border: "1px solid rgba(147, 190, 255, 0.2)", borderRadius: "8px", fontSize: "12px", color: "#ebf3ff" }} />
                <Scatter data={scatterData}>
                  {scatterData.map((entry, i) => (
                    <Cell key={i} fill={entry.score_delta >= 0 ? "#60d6c3" : "#ff9a76"} cursor="pointer" onClick={() => navigate(`/runs/${encodeURIComponent(entry.run_id)}`)} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-80 flex items-center justify-center text-muted-foreground text-sm">No validation data yet</div>
          )}
        </Panel>

        <Panel>
          <PanelHeader eyebrow="Scrutiny Delta" title="36m − 12m Score" note="Closer to zero is more stable. Positive is rare and notable." />
          {deltaData.length > 0 ? (
            <ResponsiveContainer width="100%" height={Math.max(280, deltaData.length * 28 + 60)}>
              <BarChart data={deltaData} layout="vertical" margin={{ top: 5, right: 30, bottom: 5, left: 120 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" horizontal={false} />
                <XAxis type="number" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} />
                <YAxis type="category" dataKey="shortLabel" tick={{ fill: "#99abc4", fontSize: 10 }} axisLine={false} width={110} />
                <ReferenceLine x={0} stroke="rgba(216,228,255,0.5)" />
                <Tooltip contentStyle={{ backgroundColor: "rgba(7, 12, 22, 0.95)", border: "1px solid rgba(147, 190, 255, 0.2)", borderRadius: "8px", fontSize: "12px", color: "#ebf3ff" }} />
                <Bar dataKey="score_delta" radius={[0, 4, 4, 0]}>
                  {deltaData.map((entry, i) => (
                    <Cell key={i} fill={entry.fill} fillOpacity={0.85} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-40 flex items-center justify-center text-muted-foreground text-sm">No validation data yet</div>
          )}
        </Panel>
      </div>
    </div>
  );
}
