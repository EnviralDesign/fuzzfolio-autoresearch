import { useDashboard } from "@/hooks/use-dashboard";
import { useNavigate } from "react-router-dom";
import { PageHeader } from "@/components/ui/PageHeader";
import { Panel, PanelHeader } from "@/components/ui/Panel";
import type { TradeoffRow } from "@/lib/types";
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

export function TradeoffPage() {
  const { data, isLoading } = useDashboard();
  const navigate = useNavigate();

  if (isLoading || !data) {
    return (
      <div className="p-6">
        <PageHeader title="Tradeoff Analysis" eyebrow="Tradeoff" />
        <div className="h-96 rounded-xl border border-border bg-card/30 animate-pulse" />
      </div>
    );
  }

  const tradeoffData = data.tradeoff
    .filter((r: TradeoffRow) => r.composite_score >= 15 && r.trades_per_month <= 200);

  const drawdownData = data.scoreVsDrawdown;

  return (
    <div className="p-6 space-y-6">
      <PageHeader title="Tradeoff Analysis" eyebrow="Tradeoff" description="Score vs trade rate and drawdown — finding the sweet spot." />

      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">
        <Panel className="xl:col-span-3">
          <PanelHeader eyebrow="Tradeoff Map" title="Score vs Trades/Month" note="Click a point to jump to the run" />
          {tradeoffData.length > 0 ? (
            <ResponsiveContainer width="100%" height={380}>
              <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" />
                <XAxis type="number" dataKey="trades_per_month" name="Trades/mo" domain={[0, 200]} tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "Trades / Month", position: "bottom", fill: "#99abc4", fontSize: 11, offset: 0 }} />
                <YAxis type="number" dataKey="composite_score" name="Score" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "Quality Score", angle: -90, position: "insideLeft", fill: "#99abc4", fontSize: 11 }} />
                <Tooltip contentStyle={{ backgroundColor: "rgba(7, 12, 22, 0.95)", border: "1px solid rgba(147, 190, 255, 0.2)", borderRadius: "8px", fontSize: "12px", color: "#ebf3ff" }} />
                <Scatter data={tradeoffData}>
                  {tradeoffData.map((entry, i) => (
                    <Cell key={i} fill={entry.is_trade_envelope ? "#ffba6d" : "#60d6c3"} stroke={entry.is_frontier ? "#dff8f4" : "transparent"} strokeWidth={entry.is_frontier ? 2 : 0} cursor="pointer" onClick={() => navigate(`/runs/${encodeURIComponent(entry.run_id)}`)} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-80 flex items-center justify-center text-muted-foreground text-sm">No tradeoff data yet</div>
          )}
        </Panel>

        <Panel className="xl:col-span-2">
          <PanelHeader eyebrow="Drawdown Lens" title="Score vs Max Drawdown" />
          {drawdownData.length > 0 ? (
            <ResponsiveContainer width="100%" height={380}>
              <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" />
                <XAxis type="number" dataKey="maxDrawdownR" name="Max DD (R)" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "Max Drawdown (R)", position: "bottom", fill: "#99abc4", fontSize: 11, offset: 0 }} />
                <YAxis type="number" dataKey="score" name="Score" tick={{ fill: "#99abc4", fontSize: 11 }} axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }} label={{ value: "Quality Score", angle: -90, position: "insideLeft", fill: "#99abc4", fontSize: 11 }} />
                <Tooltip contentStyle={{ backgroundColor: "rgba(7, 12, 22, 0.95)", border: "1px solid rgba(147, 190, 255, 0.2)", borderRadius: "8px", fontSize: "12px", color: "#ebf3ff" }} />
                <Scatter data={drawdownData}>
                  {drawdownData.map((entry, i) => (
                    <Cell key={i} fill={entry.score >= 80 ? "#ffba6d" : "#60d6c3"} cursor="pointer" onClick={() => navigate(`/runs/${encodeURIComponent(entry.runId)}`)} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-80 flex items-center justify-center text-muted-foreground text-sm">No drawdown data yet</div>
          )}
        </Panel>
      </div>
    </div>
  );
}
