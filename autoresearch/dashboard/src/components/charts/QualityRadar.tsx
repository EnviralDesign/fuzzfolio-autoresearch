import {
  Radar,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  ResponsiveContainer,
  Tooltip,
} from "recharts";
import type { QualityScoreComponents } from "@/lib/types";

const LABELS: Record<keyof QualityScoreComponents, string> = {
  belief: "Belief",
  cadence: "Cadence",
  edge_rate: "Edge Rate",
  path_quality: "Path Quality",
  return_quality: "Return Quality",
  robustness: "Robustness",
};

interface QualityRadarProps {
  components: QualityScoreComponents;
  score?: number;
  className?: string;
}

export function QualityRadar({ components, score, className }: QualityRadarProps) {
  const data = Object.entries(LABELS).map(([key, label]) => ({
    metric: label,
    value: (components[key as keyof QualityScoreComponents] ?? 0) * 100,
    fullMark: 100,
  }));

  return (
    <div className={className}>
      {score !== undefined && (
        <div className="text-center mb-1">
          <span className="text-2xl font-bold tracking-tight">{score.toFixed(1)}</span>
          <span className="text-xs text-muted-foreground ml-1">quality</span>
        </div>
      )}
      <ResponsiveContainer width="100%" height={220}>
        <RadarChart cx="50%" cy="50%" outerRadius="72%" data={data}>
          <PolarGrid stroke="rgba(147, 190, 255, 0.12)" />
          <PolarAngleAxis
            dataKey="metric"
            tick={{ fill: "#99abc4", fontSize: 10 }}
          />
          <PolarRadiusAxis
            domain={[0, 100]}
            tick={false}
            axisLine={false}
          />
          <Radar
            dataKey="value"
            stroke="#60d6c3"
            fill="#60d6c3"
            fillOpacity={0.15}
            strokeWidth={2}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "rgba(7, 12, 22, 0.95)",
              border: "1px solid rgba(147, 190, 255, 0.2)",
              borderRadius: "8px",
              fontSize: "12px",
              color: "#ebf3ff",
            }}
          />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}
