import { useParams, useNavigate } from "react-router-dom";
import { useRunDetail, useAttemptDetail } from "@/hooks/use-dashboard";
import { PageHeader } from "@/components/ui/PageHeader";
import { MetricCard } from "@/components/ui/MetricCard";
import { Panel, PanelHeader } from "@/components/ui/Panel";
import { DataTable } from "@/components/ui/DataTable";
import { ScoreBadge } from "@/components/ui/ScoreBadge";
import { QualityRadar } from "@/components/charts/QualityRadar";
import { LightweightChartPlaceholder } from "@/components/charts/LightweightChart";
import { formatNumber, formatInt, formatTime, shortRunId } from "@/lib/utils";
import type { AttemptSummary } from "@/lib/types";
import {
  ResponsiveContainer,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Line,
  ComposedChart,
  Cell,
} from "recharts";
import { useState } from "react";
import { ArrowLeft } from "lucide-react";

export function RunDetailPage() {
  const { runId } = useParams<{ runId: string }>();
  const navigate = useNavigate();
  const { data, isLoading, error } = useRunDetail(runId);
  const [selectedAttemptId, setSelectedAttemptId] = useState<string | null>(null);

  // Attempt detail - defaults to best attempt
  const effectiveAttemptId =
    selectedAttemptId || data?.run.bestAttempt?.attemptId;
  const attemptDetail = useAttemptDetail(runId, effectiveAttemptId);

  if (isLoading) {
    return (
      <div className="p-6">
        <PageHeader title="Loading run…" eyebrow="Run Detail" />
        <div className="space-y-4">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="h-24 rounded-xl border border-border bg-card/30 animate-pulse" />
          ))}
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="p-6">
        <PageHeader title="Run not found" eyebrow="Error" />
        <p className="text-muted-foreground">
          {error instanceof Error ? error.message : "Could not load run detail."}
        </p>
      </div>
    );
  }

  const { run, attempts } = data;

  // Timeline data for the score trace chart
  const timelineData = attempts
    .filter((a) => a.score !== null && a.score !== undefined)
    .sort((a, b) => a.sequence - b.sequence)
    .map((a) => ({
      sequence: a.sequence,
      score: a.score!,
      candidateName: a.candidateName,
      attemptId: a.attemptId,
      isBest: a.attemptId === run.bestAttempt?.attemptId,
    }));

  const attemptColumns = [
    {
      key: "candidate",
      label: "Candidate",
      render: (row: AttemptSummary) => (
        <div className="min-w-0">
          <div className="text-sm font-medium truncate max-w-40">{row.candidateName || "candidate"}</div>
          <div className="text-xs text-muted-foreground">#{row.sequence}</div>
        </div>
      ),
    },
    {
      key: "score",
      label: "Score",
      render: (row: AttemptSummary) => <ScoreBadge score={row.score} />,
    },
    {
      key: "trades",
      label: "Trades/mo",
      render: (row: AttemptSummary) => formatNumber(row.tradesPerMonth, 1),
    },
    {
      key: "dd",
      label: "DD",
      render: (row: AttemptSummary) =>
        row.maxDrawdownR != null ? `${formatNumber(row.maxDrawdownR, 1)}R` : "—",
    },
    {
      key: "pf",
      label: "PF",
      render: (row: AttemptSummary) => formatNumber(row.profitFactor, 2),
    },
    {
      key: "instrument",
      label: "Instrument",
      render: (row: AttemptSummary) => (
        <span className="text-xs text-muted-foreground">{row.instrument || "—"}</span>
      ),
    },
  ];

  // Attempt detail view
  const ad = attemptDetail.data;
  const adAttempt = ad?.attempt;
  const components = adAttempt?.bestSummary?.quality_score_payload?.components;

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate("/runs")}
          className="p-1.5 rounded-lg border border-border hover:bg-surface-hover transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
        </button>
        <PageHeader
          title={shortRunId(run.runId)}
          eyebrow="Run Detail"
          description={`${run.explorerModel || run.explorerProfile || "unknown"} · ${formatTime(run.createdAt)} · ${formatInt(run.attemptCount)} attempts`}
        />
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <MetricCard
          label="Leader"
          value={formatNumber(run.bestAttempt?.score, 2)}
          secondary={run.bestAttempt?.candidateName || "none"}
        />
        <MetricCard
          label="Advisors"
          value={formatInt(run.advisorGuidanceCount)}
          secondary={`step ${formatInt(run.latestStep)}`}
        />
        <MetricCard
          label="Curves"
          value={formatInt(run.curveAttemptCount)}
          secondary="attempts with path detail"
        />
        <MetricCard
          label="Scored"
          value={`${formatInt(run.scoredAttemptCount)} / ${formatInt(run.attemptCount)}`}
          secondary="attempts scored"
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        {/* Score timeline */}
        <Panel className="xl:col-span-2">
          <PanelHeader
            eyebrow="Score Trace"
            title="Quality score over attempts"
          />
          {timelineData.length > 0 ? (
            <ResponsiveContainer width="100%" height={260}>
              <ComposedChart data={timelineData} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(147, 190, 255, 0.08)" />
                <XAxis
                  dataKey="sequence"
                  tick={{ fill: "#99abc4", fontSize: 11 }}
                  axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }}
                />
                <YAxis
                  tick={{ fill: "#99abc4", fontSize: 11 }}
                  axisLine={{ stroke: "rgba(147, 190, 255, 0.12)" }}
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
                <Line
                  type="monotone"
                  dataKey="score"
                  stroke="#60d6c3"
                  strokeWidth={2}
                  dot={false}
                />
                <Scatter dataKey="score">
                  {timelineData.map((entry, i) => (
                    <Cell
                      key={i}
                      fill={entry.isBest ? "#ffba6d" : "#60d6c3"}
                      r={entry.isBest ? 6 : 3}
                      cursor="pointer"
                      onClick={() => setSelectedAttemptId(entry.attemptId)}
                    />
                  ))}
                </Scatter>
              </ComposedChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-60 flex items-center justify-center text-muted-foreground text-sm">
              No scored attempts yet
            </div>
          )}
        </Panel>

        {/* Quality radar for selected attempt */}
        <Panel>
          <PanelHeader
            eyebrow="Quality Breakdown"
            title={adAttempt?.candidateName || "Select an attempt"}
          />
          {components ? (
            <QualityRadar components={components} score={adAttempt?.score ?? undefined} />
          ) : (
            <div className="h-52 flex items-center justify-center text-muted-foreground text-sm">
              {attemptDetail.isLoading ? "Loading…" : "No quality components"}
            </div>
          )}
        </Panel>
      </div>

      {/* Attempts table */}
      <Panel>
        <PanelHeader
          eyebrow="Attempts"
          title="All attempts in this run"
          note="Best scores float to the top. Click to inspect."
        />
        <DataTable
          columns={attemptColumns}
          data={attempts}
          maxHeight="400px"
          onRowClick={(row) => setSelectedAttemptId(row.attemptId)}
        />
      </Panel>

      {/* Attempt detail section */}
      {adAttempt && (
        <Panel>
          <PanelHeader
            eyebrow="Attempt Detail"
            title={adAttempt.candidateName || effectiveAttemptId || ""}
            note={`${formatTime(adAttempt.createdAt)} · ${formatNumber(adAttempt.score, 2)} score · ${formatNumber(adAttempt.tradesPerMonth, 1)} trades/mo`}
          />

          {/* Key metrics row */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
            <MetricCard label="Score" value={formatNumber(adAttempt.score, 2)} secondary={adAttempt.scoreBasis || "quality score"} />
            <MetricCard label="Trades/mo" value={formatNumber(adAttempt.tradesPerMonth, 1)} secondary={`${formatInt(adAttempt.tradeCount)} resolved`} />
            <MetricCard label="Max DD" value={`${formatNumber(adAttempt.maxDrawdownR, 1)}R`} secondary={`${formatNumber(adAttempt.effectiveWindowMonths, 1)} mo window`} />
            <MetricCard label="Expectancy" value={`${formatNumber(adAttempt.expectancyR, 3)}R`} secondary={`PF ${formatNumber(adAttempt.profitFactor, 2)}`} />
          </div>

          {/* Tags */}
          <div className="flex flex-wrap gap-2 mb-4">
            {adAttempt.instrument && (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary/10 border border-primary/20 text-primary">
                {adAttempt.instrument}
              </span>
            )}
            {adAttempt.timeframe && (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-card border border-border text-foreground">
                {adAttempt.timeframe}
              </span>
            )}
            {adAttempt.signalSelectivity && (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-card border border-border text-foreground">
                {adAttempt.signalSelectivity}
              </span>
            )}
          </div>

          {/* Lightweight Charts placeholder */}
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            <div className="xl:col-span-2">
              <LightweightChartPlaceholder height={320} />
            </div>
            <div className="space-y-3">
              {/* Profile drop images */}
              {ad?.profileDrop12PngUrl && (
                <div className="rounded-lg overflow-hidden border border-border/50">
                  <img
                    alt="Profile drop 12mo"
                    src={`${ad.profileDrop12PngUrl}&t=${Date.now()}`}
                    className="w-full"
                  />
                </div>
              )}
              {ad?.profileDrop36PngUrl && (
                <div className="rounded-lg overflow-hidden border border-border/50">
                  <img
                    alt="Profile drop 36mo"
                    src={`${ad.profileDrop36PngUrl}&t=${Date.now()}`}
                    className="w-full"
                  />
                </div>
              )}
              {!ad?.profileDrop12PngUrl && !ad?.profileDrop36PngUrl && (
                <div className="text-xs text-muted-foreground py-4 text-center">
                  No profile drop images
                </div>
              )}
            </div>
          </div>

          {/* Collapsible JSON payloads */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-4">
            <details className="rounded-lg border border-border bg-surface p-3">
              <summary className="cursor-pointer text-sm font-medium">Profile payload</summary>
              <pre className="mt-2 text-xs text-muted-foreground overflow-auto max-h-64 font-mono">
                {JSON.stringify(ad?.profile || {}, null, 2)}
              </pre>
            </details>
            <details className="rounded-lg border border-border bg-surface p-3">
              <summary className="cursor-pointer text-sm font-medium">Deep replay request</summary>
              <pre className="mt-2 text-xs text-muted-foreground overflow-auto max-h-64 font-mono">
                {JSON.stringify(ad?.deepReplayJob || {}, null, 2)}
              </pre>
            </details>
            <details className="rounded-lg border border-border bg-surface p-3">
              <summary className="cursor-pointer text-sm font-medium">Best summary</summary>
              <pre className="mt-2 text-xs text-muted-foreground overflow-auto max-h-64 font-mono">
                {JSON.stringify(adAttempt.bestSummary || {}, null, 2)}
              </pre>
            </details>
          </div>
        </Panel>
      )}
    </div>
  );
}
