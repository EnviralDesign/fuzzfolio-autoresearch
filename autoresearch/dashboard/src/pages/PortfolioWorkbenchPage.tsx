import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useMutation, useQueries, useQueryClient } from "@tanstack/react-query";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceLine,
  XAxis,
  YAxis,
} from "recharts";
import {
  Bot,
  ChevronDown,
  Check,
  Clock3,
  Download,
  Eye,
  Filter,
  Fingerprint,
  Layers3,
  Maximize2,
  Network,
  Plus,
  Play,
  RotateCcw,
  Search,
  SlidersHorizontal,
  StopCircle,
  Trophy,
  Trash2,
  X,
} from "lucide-react";

import { ProfileDropModal } from "@/components/profile-drop-modal";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  ChartContainer,
  ChartTooltip,
  type ChartConfig,
} from "@/components/ui/chart";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useCatalog, useDashboardJob, useLivePortfolio, useRuns } from "@/hooks/use-viewer-data";
import {
  cancelDashboardJob,
  fetchAttemptDetail,
  fetchDashboardPortfolioConfig,
  saveLivePortfolio,
  startBuildPortfolioJob,
  startExportLivePortfolioJob,
  startFinalizeCorpusJob,
} from "@/lib/api";
import type { AttemptCatalogRow, AttemptDetail, DashboardJob, RunSummary } from "@/lib/types";
import {
  compactRunId,
  formatDateTime,
  formatInt,
  formatNumber,
  scoreTone,
} from "@/lib/utils";

type AccountConfig = {
  balanceUsd: number;
  riskPerRPercent: number;
  riskBasis: "initial" | "current";
  minLot: number;
  lotStep: number;
  notionalUsdPerLot: number;
  leverage: number;
  marginCallLevelPercent: number;
  stopOutLevelPercent: number;
  commissionRPerTrade: number;
  spreadRPerTrade: number;
  slippageRPerTrade: number;
};

type BrokerPreset = {
  id: string;
  label: string;
  description: string;
  account: AccountConfig;
};

type NormalizedPoint = {
  time: number;
  date: string;
  equityR: number;
  realizedR: number;
  cumulativeTrades: number;
  openTrades: number;
};

type PortfolioPoint = {
  time: number;
  date: string;
  equity_r: number;
  drawdown_r: number;
  balance_usd: number;
  drawdown_usd: number;
  used_margin_usd: number;
  stop_out_equity_usd: number;
  deposit_load_pct: number;
  margin_level_pct: number | null;
  stop_out_headroom_pct: number | null;
  margin_call_headroom_pct: number | null;
  gross_margin_load_pct: number;
  margin_risk_pct: number;
  realized_r: number;
  closed_trade_count: number;
  open_trade_count: number;
};

type PortfolioMetrics = {
  selectedCount: number;
  loadedCount: number;
  finalEquityR: number | null;
  finalRealizedR: number | null;
  maxDrawdownR: number | null;
  finalBalanceUsd: number | null;
  finalRealizedUsd: number | null;
  minBalanceUsd: number | null;
  maxDrawdownUsd: number | null;
  totalTrades: number;
  tradesPerMonth: number | null;
  blown: boolean;
  riskDollars: number;
  finalRiskDollars: number | null;
  averageCostUsdPerTrade: number | null;
  minLotForcedTrades: number;
  maxUsedMarginUsd: number;
  maxStopOutEquityUsd: number;
  maxGrossMarginLoadPercent: number | null;
  maxDepositLoadPercent: number | null;
  usedMarginAtMaxDepositLoadUsd: number | null;
  openTradesAtMaxDepositLoad: number;
  maxOpenTrades: number;
  minMarginLevelPercent: number | null;
  maxMarginRiskPercent: number | null;
  marginLiquidated: boolean;
  firstLiquidationDate: string | null;
  costRPerTrade: number;
  sampledCurveCount: number;
  sampledEventPointCount: number;
  sampledReturnedPointCount: number;
};

type RunSortMode = "recent" | "score";
type CandidateScope = "promoted" | "all";
type PortfolioChartMode = "equity" | "drawdown" | "margin";
type WorkbenchMode = "manual" | "auto";

type SimilarityPair = {
  leftAttemptId: string;
  rightAttemptId: string;
  leftLabel: string;
  rightLabel: string;
  similarityScore: number;
  correlation: number | null;
  positiveCorrelation: number;
  activeOverlapRatio: number;
  drawdownOverlapRatio: number;
  overlapDays: number;
};

type SimilarityCell = {
  rowAttemptId: string;
  columnAttemptId: string;
  rowLabel: string;
  columnLabel: string;
  value: number;
  pair: SimilarityPair | null;
  diagonal: boolean;
};

type PortfolioSimilarity = {
  selectedCount: number;
  loadedCount: number;
  averageSameness: number | null;
  maxSameness: number | null;
  maxPair: SimilarityPair | null;
  pairs: SimilarityPair[];
  cells: SimilarityCell[][];
};

type PortfolioClusterMember = {
  row: AttemptCatalogRow;
  attemptId: string;
  label: string;
  score: number | null;
  tradesPerMonth: number | null;
  maxDrawdownR: number | null;
};

type PortfolioCluster = {
  id: string;
  label: string;
  members: PortfolioClusterMember[];
  x: number;
  y: number;
  size: number;
  hue: number;
  averageScore: number | null;
  tradesPerMonth: number | null;
  maxDrawdownR: number | null;
  maxSameness: number | null;
  pairCount: number;
  strongestPair: SimilarityPair | null;
};

type ClusterThresholdStats = {
  linkedPairCount: number;
  nearestIncluded: number | null;
  nearestExcluded: number | null;
};

type LotSizing = {
  instrument: string | null;
  stopLossPercent: number | null;
  riskPerLotDollars: number | null;
};

type NormalizedSeries = {
  points: NormalizedPoint[];
  sizing: LotSizing;
  downsampled: boolean;
  eventPointCount: number | null;
  pointCount: number | null;
};

type SimilarityPrepared = {
  row: AttemptCatalogRow;
  attemptId: string;
  label: string;
  dailyChanges: Map<string, number>;
  activeDates: Set<string>;
  drawdownDates: Set<string>;
  instruments: string[];
  instrumentSet: Set<string>;
  timeframe: string;
  strategyKey: string;
  tradesPerMonth: number | null;
  maxDrawdownR: number | null;
};

type AssetExposureRow = {
  instrument: string;
  tradesPerMonth: number;
  share: number;
  strategyCount: number;
};

const DEFAULT_ACCOUNT: AccountConfig = {
  balanceUsd: 1000,
  riskPerRPercent: 0.25,
  riskBasis: "initial",
  minLot: 0.01,
  lotStep: 0.01,
  notionalUsdPerLot: 100000,
  leverage: 500,
  marginCallLevelPercent: 70,
  stopOutLevelPercent: 50,
  commissionRPerTrade: 0.01,
  spreadRPerTrade: 0.02,
  slippageRPerTrade: 0.005,
};

function dashboardPortfolioAccountSpec(account: AccountConfig): Record<string, unknown> {
  return {
    name: "dashboard-manual",
    account_size_usd: account.balanceUsd,
    balance_usd: account.balanceUsd,
    leverage: account.leverage,
    risk_per_trade_pct: account.riskPerRPercent,
    dashboard_risk_basis: account.riskBasis,
    min_lot: account.minLot,
    lot_step: account.lotStep,
    notional_usd_per_lot: account.notionalUsdPerLot,
    margin_call_level_pct: account.marginCallLevelPercent,
    stop_out_level_pct: account.stopOutLevelPercent,
    commission_r_per_trade: account.commissionRPerTrade,
    spread_r_per_trade: account.spreadRPerTrade,
    slippage_r_per_trade: account.slippageRPerTrade,
  };
}

const BROKER_PRESETS: BrokerPreset[] = [
  {
    id: "coinexx-500",
    label: "Coinexx 500:1",
    description: "$100 offshore high-leverage account",
    account: {
      ...DEFAULT_ACCOUNT,
      balanceUsd: 100,
      riskPerRPercent: 1,
    },
  },
  {
    id: "darwinex-zero-cfd",
    label: "Darwinex Zero CFD",
    description: "$100k signal account, forex CFD defaults",
    account: {
      ...DEFAULT_ACCOUNT,
      balanceUsd: 100000,
      riskPerRPercent: 0.1,
      riskBasis: "initial",
      minLot: 0.01,
      lotStep: 0.01,
      notionalUsdPerLot: 100000,
      leverage: 30,
      marginCallLevelPercent: 100,
      stopOutLevelPercent: 50,
    },
  },
];

const equityChartConfig = {
  equity_r: {
    label: "Portfolio R",
    color: "oklch(0.78 0.16 150)",
  },
  balance_usd: {
    label: "USD balance",
    color: "oklch(0.82 0.13 83)",
  },
} satisfies ChartConfig;

const drawdownChartConfig = {
  drawdown_r: {
    label: "Drawdown R",
    color: "oklch(0.72 0.18 28)",
  },
  drawdown_usd: {
    label: "Drawdown USD",
    color: "oklch(0.76 0.13 36)",
  },
} satisfies ChartConfig;

const marginRiskChartConfig = {
  gross_margin_load_pct: {
    label: "Gross margin load",
    color: "oklch(0.86 0.16 82)",
  },
  margin_risk_pct: {
    label: "Stop-out pressure",
    color: "oklch(0.7 0.18 28)",
  },
} satisfies ChartConfig;

const chartCursor = {
  stroke: "hsl(var(--muted-foreground))",
  strokeOpacity: 0.35,
  strokeWidth: 1,
};

const chartActiveDot = {
  r: 3,
  stroke: "hsl(var(--background))",
  strokeWidth: 1,
};

const DEFAULT_CLUSTER_THRESHOLD = 0.25;
const LOOSE_CLUSTER_THRESHOLD = 0.15;
const STRICT_CLUSTER_THRESHOLD = 0.35;

export function PortfolioWorkbenchPage() {
  const queryClient = useQueryClient();
  const { data: catalog, isLoading: catalogLoading, error: catalogError } = useCatalog();
  const { data: runs } = useRuns();
  const { data: livePortfolio } = useLivePortfolio();
  const { data: dashboardJob } = useDashboardJob();
  const [workbenchMode, setWorkbenchMode] = useState<WorkbenchMode>("manual");
  const [activeRunId, setActiveRunId] = useState<string>("all");
  const [query, setQuery] = useState("");
  const [minScore, setMinScore] = useState(60);
  const [validOnly, setValidOnly] = useState(true);
  const [candidateScope, setCandidateScope] = useState<CandidateScope>("promoted");
  const [runSortMode, setRunSortMode] = useState<RunSortMode>("recent");
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [account, setAccount] = useState<AccountConfig>(DEFAULT_ACCOUNT);
  const [preview, setPreview] = useState<AttemptCatalogRow | null>(null);
  const [portfolioViewerOpen, setPortfolioViewerOpen] = useState(false);
  const [clusterThreshold, setClusterThreshold] = useState(DEFAULT_CLUSTER_THRESHOLD);
  const [clusterCollisionEnabled, setClusterCollisionEnabled] = useState(false);
  const [autoConfigText, setAutoConfigText] = useState("");
  const [autoConfigError, setAutoConfigError] = useState<string | null>(null);
  const lastAppliedLiveSelection = useRef<string | null>(null);
  const hasHydratedLiveSelection = useRef(false);

  const livePortfolioMutation = useMutation({
    mutationFn: saveLivePortfolio,
    onMutate: (attemptIds) => {
      queryClient.setQueryData(["live-portfolio"], {
        selected_attempt_ids: attemptIds,
        updated_at: new Date().toISOString(),
      });
    },
    onSuccess: (payload) => {
      queryClient.setQueryData(["live-portfolio"], payload);
      lastAppliedLiveSelection.current = stableSelectionKey(payload.selected_attempt_ids);
    },
  });

  const finalizeMutation = useMutation({
    mutationFn: () => startFinalizeCorpusJob({ scope: "dashboard" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dashboard-job-current"] });
    },
  });

  const buildPortfolioMutation = useMutation({
    mutationFn: (portfolioConfig: Record<string, unknown>) =>
      startBuildPortfolioJob({ portfolio_config: portfolioConfig }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dashboard-job-current"] });
    },
  });

  const exportLivePortfolioMutation = useMutation({
    mutationFn: () =>
      startExportLivePortfolioJob({
        selected_attempt_ids: selectedIds,
        account: dashboardPortfolioAccountSpec(account),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dashboard-job-current"] });
    },
  });

  const cancelJobMutation = useMutation({
    mutationFn: (jobId?: string) => cancelDashboardJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dashboard-job-current"] });
    },
  });

  const rows = useMemo(() => catalog?.rows ?? [], [catalog?.rows]);
  const selectedRows = useMemo(
    () => selectedIds.map((id) => rows.find((row) => row.attempt_id === id)).filter(Boolean) as AttemptCatalogRow[],
    [rows, selectedIds],
  );

  const detailQueries = useQueries({
    queries: selectedIds.map((attemptId) => ({
      queryKey: ["attempt-detail", attemptId],
      queryFn: () => fetchAttemptDetail(attemptId),
      staleTime: 60_000,
    })),
  });

  const details = detailQueries
    .map((item) => item.data)
    .filter(Boolean) as AttemptDetail[];

  const portfolio = useMemo(
    () => buildPortfolioCurve(details, account, selectedIds.length),
    [account, details, selectedIds.length],
  );

  const similarity = useMemo(
    () => buildPortfolioSimilarity(details, selectedRows, selectedIds.length),
    [details, selectedIds.length, selectedRows],
  );

  const clusters = useMemo(
    () => buildPortfolioClusters(selectedRows, similarity, clusterThreshold, clusterCollisionEnabled),
    [clusterCollisionEnabled, clusterThreshold, selectedRows, similarity],
  );

  const preferredByRun = useMemo(() => buildDashboardPreferredAttemptMap(rows), [rows]);

  const filteredAttempts = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return rows
      .filter((row) => activeRunId === "all" || row.run_id === activeRunId)
      .filter((row) => isWorkbenchCandidate(row, { candidateScope, minScore, needle, preferredByRun, validOnly }))
      .sort((a, b) => Number(b.score_36m ?? -Infinity) - Number(a.score_36m ?? -Infinity));
  }, [activeRunId, candidateScope, minScore, preferredByRun, query, rows, validOnly]);

  const runListRunIds = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return new Set(
      rows
        .filter((row) => isWorkbenchCandidate(row, { candidateScope, minScore, needle, preferredByRun, validOnly }))
        .map((row) => String(row.run_id || "").trim())
        .filter(Boolean),
    );
  }, [candidateScope, minScore, preferredByRun, query, rows, validOnly]);

  const sortedRuns = useMemo(
    () => sortRuns(runs?.runs ?? [], runSortMode).filter((run) => runListRunIds.has(run.run_id)),
    [runListRunIds, runSortMode, runs?.runs],
  );

  const visibleAttempts = filteredAttempts.slice(0, 120);
  const loadedCount = detailQueries.filter((item) => item.data).length;
  const loadingSelectionCount = detailQueries.filter((item) => item.isLoading).length;
  const multiMemberClusterCount = clusters.filter((cluster) => cluster.members.length > 1).length;

  useEffect(() => {
    if (!livePortfolio || hasHydratedLiveSelection.current) {
      return;
    }
    hasHydratedLiveSelection.current = true;
    const incomingIds = livePortfolio.selected_attempt_ids ?? [];
    const incomingKey = stableSelectionKey(incomingIds);
    lastAppliedLiveSelection.current = incomingKey;
    setSelectedIds(incomingIds);
  }, [livePortfolio]);

  useEffect(() => {
    let canceled = false;
    fetchDashboardPortfolioConfig()
      .then((payload) => {
        if (!canceled) {
          setAutoConfigText(JSON.stringify(payload, null, 2));
        }
      })
      .catch((error: unknown) => {
        if (!canceled) {
          setAutoConfigError(error instanceof Error ? error.message : "Could not load auto config.");
        }
      });
    return () => {
      canceled = true;
    };
  }, []);

  const persistSelectedIds = (nextIds: string[]) => {
    const normalizedIds = normalizeSelectedIds(nextIds);
    setSelectedIds(normalizedIds);
    lastAppliedLiveSelection.current = stableSelectionKey(normalizedIds);
    livePortfolioMutation.mutate(normalizedIds);
  };

  const toggleAttempt = (attemptId: string) => {
    persistSelectedIds(
      selectedIds.includes(attemptId)
        ? selectedIds.filter((item) => item !== attemptId)
        : [...selectedIds, attemptId],
    );
  };

  const startAutoBuild = () => {
    try {
      const parsed = JSON.parse(autoConfigText || "{}");
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("Auto portfolio config must be a JSON object.");
      }
      setAutoConfigError(null);
      buildPortfolioMutation.mutate(parsed as Record<string, unknown>);
    } catch (error) {
      setAutoConfigError(error instanceof Error ? error.message : "Invalid auto config JSON.");
    }
  };

  const importAutoSelection = () => {
    const selected = rows
      .filter((row) => Number(row.selection_rank ?? Infinity) > 0)
      .sort((a, b) => Number(a.selection_rank ?? Infinity) - Number(b.selection_rank ?? Infinity))
      .map((row) => row.attempt_id);
    if (selected.length) {
      persistSelectedIds(selected);
    }
  };

  if (catalogLoading) {
    return <div className="py-20 text-sm text-muted-foreground">Loading corpus catalog...</div>;
  }

  if (!catalog) {
    return (
      <div className="py-20 text-sm text-destructive">
        {catalogError instanceof Error ? catalogError.message : "Catalog failed to load."}
      </div>
    );
  }

  return (
    <>
      <div className="grid min-h-[calc(100vh-5.5rem)] gap-5 xl:grid-cols-[260px_minmax(0,1fr)]">
        <aside className="space-y-4 xl:sticky xl:top-24 xl:h-[calc(100vh-7rem)] xl:overflow-y-auto">
          <Panel className="p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
                <Filter className="h-3.5 w-3.5" />
                Runs
              </div>
              <div className="flex items-center gap-1">
                <RunSortButton
                  active={runSortMode === "recent"}
                  label="Sort runs by recency"
                  onClick={() => setRunSortMode("recent")}
                >
                  <Clock3 className="h-3.5 w-3.5" />
                </RunSortButton>
                <RunSortButton
                  active={runSortMode === "score"}
                  label="Sort runs by best score"
                  onClick={() => setRunSortMode("score")}
                >
                  <Trophy className="h-3.5 w-3.5" />
                </RunSortButton>
              </div>
            </div>
            <button
              type="button"
              onClick={() => setActiveRunId("all")}
              className={`mt-4 w-full rounded-lg border px-3 py-3 text-left transition ${
                activeRunId === "all"
                  ? "border-amber-300/60 bg-amber-300/10 text-foreground"
                  : "border-border/60 bg-background/35 text-muted-foreground hover:text-foreground"
              }`}
            >
              <div className="flex items-center justify-between gap-3">
                <span className="font-medium">All runs</span>
                <span>{formatInt(sortedRuns.length)}</span>
              </div>
            </button>
            <div className="mt-3 space-y-2">
              {sortedRuns.map((run) => (
                <button
                  key={run.run_id}
                  type="button"
                  onClick={() => setActiveRunId(run.run_id)}
                  className={`w-full rounded-lg border px-3 py-2.5 text-left transition ${
                    activeRunId === run.run_id
                      ? "border-amber-300/60 bg-amber-300/10 text-foreground"
                      : "border-border/50 bg-background/25 text-muted-foreground hover:border-border hover:text-foreground"
                  }`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <span className="truncate text-sm font-medium">{compactRunId(run.run_id)}</span>
                    <span className="text-xs">{formatInt(run.attempt_count)}</span>
                  </div>
                  <div className="mt-1 flex items-center justify-between gap-3 text-xs">
                    <span>{formatDateTime(run.latest_created_at || run.created_at)}</span>
                    <span className={scoreTone(run.best_attempt?.score_36m)}>
                      {formatNumber(run.best_attempt?.score_36m ?? null, 1)}
                    </span>
                  </div>
                </button>
              ))}
            </div>
          </Panel>
        </aside>

        <main className="space-y-5">
          <section className="grid items-start gap-4 min-[1700px]:grid-cols-[minmax(0,1fr)_460px]">
            <div className="space-y-3">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex flex-wrap items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
                  <Layers3 className="h-3.5 w-3.5" />
                  Portfolio workbench
                </div>
                <div className="grid grid-cols-2 gap-1 rounded-lg border border-border/60 bg-background/35 p-1">
                  <ModeButton active={workbenchMode === "manual"} onClick={() => setWorkbenchMode("manual")}>
                    <Layers3 className="h-3.5 w-3.5" />
                    Manual
                  </ModeButton>
                  <ModeButton active={workbenchMode === "auto"} onClick={() => setWorkbenchMode("auto")}>
                    <Bot className="h-3.5 w-3.5" />
                    Auto
                  </ModeButton>
                </div>
              </div>
              <h1 className="max-w-5xl text-3xl font-semibold leading-tight tracking-tight md:text-5xl">
                {workbenchMode === "manual"
                  ? "Select attempts, watch the 36mo basket curve change immediately."
                  : "Run portfolio automation, inspect the log, then import the selected basket."}
              </h1>
            </div>
            <Panel className="flex flex-wrap items-center gap-2 self-start p-2 min-[1700px]:flex-nowrap">
              <WorkbenchSummaryStat label="Attempts" value={formatInt(filteredAttempts.length)} />
              <WorkbenchSummaryStat label="Selected" value={formatInt(selectedIds.length)} />
              <WorkbenchSummaryStat label="Loaded" value={`${formatInt(loadedCount)}/${formatInt(selectedIds.length)}`} />
              <Button
                type="button"
                variant="secondary"
                className="h-10 min-w-40 flex-1 rounded-lg px-3 min-[1700px]:flex-none"
                onClick={() => setPortfolioViewerOpen(true)}
              >
                <Maximize2 className="h-4 w-4" />
                Portfolio viewer
                {multiMemberClusterCount ? (
                  <span className="rounded-md bg-primary-foreground/15 px-1.5 py-0.5 text-[0.68rem]">
                    {formatInt(multiMemberClusterCount)}
                  </span>
                ) : null}
              </Button>
            </Panel>
          </section>

          {workbenchMode === "auto" ? (
            <AutoBuildPanel
              configText={autoConfigText}
              setConfigText={setAutoConfigText}
              configError={autoConfigError}
              job={dashboardJob}
              onFinalize={() => finalizeMutation.mutate()}
              onBuild={startAutoBuild}
              onCancel={() => cancelJobMutation.mutate(dashboardJob?.id)}
              onImport={importAutoSelection}
              isStartingFinalize={finalizeMutation.isPending}
              isStartingBuild={buildPortfolioMutation.isPending}
              isCanceling={cancelJobMutation.isPending}
            />
          ) : null}

          <Panel className="p-3">
            <div className="grid items-end gap-2.5 sm:grid-cols-2 lg:grid-cols-[minmax(220px,1fr)_124px_112px_120px_100px]">
              <label className="grid gap-1 text-xs text-muted-foreground">
                <span>Search</span>
                <span className="relative block">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                    placeholder="Attempts or symbols"
                    className="h-10 rounded-lg pl-9"
                  />
                </span>
              </label>
              <label className="grid gap-1 text-xs text-muted-foreground">
                <span>Scope</span>
                <Select value={candidateScope} onValueChange={(value) => setCandidateScope(value as CandidateScope)}>
                  <SelectTrigger className="h-10 rounded-lg">
                    <SelectValue placeholder="Candidate scope" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="promoted">Chosen</SelectItem>
                    <SelectItem value="all">All attempts</SelectItem>
                  </SelectContent>
                </Select>
              </label>
              <label className="grid gap-1 text-xs text-muted-foreground">
                <span>Min score</span>
                <Input
                  type="number"
                  value={minScore}
                  min={0}
                  max={100}
                  onChange={(event) => setMinScore(toNumber(event.target.value, 0))}
                  className="h-10 rounded-lg"
                />
              </label>
              <button
                type="button"
                onClick={() => setValidOnly((value) => !value)}
                className={`flex h-10 items-center justify-center gap-2 whitespace-nowrap rounded-lg border px-3 text-sm transition ${
                  validOnly
                    ? "border-emerald-300/50 bg-emerald-300/10 text-emerald-100"
                    : "border-border/60 bg-background/30 text-muted-foreground"
                }`}
              >
                <Check className="h-4 w-4" />
                Valid only
              </button>
              <Button
                variant="outline"
                className="h-10 rounded-lg"
                onClick={() => {
                  setQuery("");
                  setMinScore(60);
                  setValidOnly(true);
                  setCandidateScope("promoted");
                }}
              >
                <RotateCcw className="h-4 w-4" />
                Reset
              </Button>
            </div>
          </Panel>

          <section className="grid gap-4 min-[1200px]:grid-cols-2 min-[1500px]:grid-cols-3">
            {visibleAttempts.map((attempt) => (
              <AttemptCard
                key={attempt.attempt_id}
                attempt={attempt}
                selected={selectedIds.includes(attempt.attempt_id)}
                onToggle={() => toggleAttempt(attempt.attempt_id)}
                onPreview={() => setPreview(attempt)}
              />
            ))}
          </section>
          {filteredAttempts.length > visibleAttempts.length ? (
            <div className="rounded-lg border border-border/60 bg-background/35 px-4 py-3 text-sm text-muted-foreground">
              Showing {formatInt(visibleAttempts.length)} of {formatInt(filteredAttempts.length)} matching attempts. Tighten the filters to narrow the list.
            </div>
          ) : null}
        </main>

      </div>

      <PortfolioViewer
        isOpen={portfolioViewerOpen}
        onClose={() => setPortfolioViewerOpen(false)}
        account={account}
        setAccount={setAccount}
        selectedRows={selectedRows}
        selectedIds={selectedIds}
        persistSelectedIds={persistSelectedIds}
        portfolio={portfolio}
        similarity={similarity}
        clusters={clusters}
        clusterThreshold={clusterThreshold}
        setClusterThreshold={setClusterThreshold}
        clusterCollisionEnabled={clusterCollisionEnabled}
        setClusterCollisionEnabled={setClusterCollisionEnabled}
        loadingSelectionCount={loadingSelectionCount}
        isSavingLivePortfolio={livePortfolioMutation.isPending}
        job={dashboardJob}
        onExport={() => exportLivePortfolioMutation.mutate()}
        onCancelJob={() => cancelJobMutation.mutate(dashboardJob?.id)}
        isStartingExport={exportLivePortfolioMutation.isPending}
        isCancelingJob={cancelJobMutation.isPending}
        onPreview={setPreview}
      />

      <ProfileDropModal
        isOpen={preview !== null}
        onClose={() => setPreview(null)}
        profilePathUrl={preview?.profile_drop_36m_png_url ?? null}
        candidateName={preview?.candidate_name || preview?.attempt_id || ""}
      />
    </>
  );
}

function AttemptCard({
  attempt,
  selected,
  onToggle,
  onPreview,
}: {
  attempt: AttemptCatalogRow;
  selected: boolean;
  onToggle: () => void;
  onPreview: () => void;
}) {
  const title = String(attempt.candidate_name || attempt.attempt_id);
  const role = String(attempt.attempt_role || attempt.play_hand_role || "").trim();
  const decision = String(attempt.attempt_decision || "").trim();
  const canonical = Boolean(attempt.is_canonical_attempt || attempt.is_canonical_playhand_attempt);
  return (
    <article
      className={`overflow-hidden rounded-lg border bg-card/78 transition ${
        selected ? "border-amber-300/70 shadow-[0_0_0_1px_oklch(0.82_0.13_83_/_0.35)]" : "border-border/60"
      }`}
    >
      <button type="button" onClick={onPreview} className="block w-full bg-background/35">
        {attempt.profile_drop_36m_png_url ? (
          <img
            src={attempt.profile_drop_36m_png_url}
            alt={title}
            className="h-auto w-full"
            loading="lazy"
          />
        ) : (
          <div className="flex min-h-96 items-center justify-center text-sm text-muted-foreground">
            Profile drop not rendered
          </div>
        )}
      </button>
      <div className="space-y-4 p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="truncate text-base font-semibold tracking-tight">{title}</div>
            <div className="mt-1 truncate text-xs text-muted-foreground">{attempt.strategy_key_36m || attempt.run_id}</div>
          </div>
          <span className={`text-lg font-semibold ${scoreTone(attempt.score_36m)}`}>
            {formatNumber(attempt.score_36m ?? null, 1)}
          </span>
        </div>
        {canonical || role || decision ? (
          <div className="flex flex-wrap gap-1.5">
            {canonical ? (
              <span className="rounded border border-emerald-400/40 bg-emerald-400/10 px-2 py-0.5 text-[0.68rem] uppercase tracking-wide text-emerald-200">
                Canonical
              </span>
            ) : null}
            {role ? (
              <span className="rounded border border-border/70 bg-background/45 px-2 py-0.5 text-[0.68rem] uppercase tracking-wide text-muted-foreground">
                {role.replaceAll("_", " ")}
              </span>
            ) : null}
            {decision && decision !== "canonical" ? (
              <span className="rounded border border-border/70 bg-background/45 px-2 py-0.5 text-[0.68rem] uppercase tracking-wide text-muted-foreground">
                {decision.replaceAll("_", " ")}
              </span>
            ) : null}
          </div>
        ) : null}
        <div className="grid grid-cols-3 gap-2 text-xs">
          <TinyStat label="DD" value={`${formatNumber(attempt.max_drawdown_r_36m ?? null, 2)}R`} />
          <TinyStat label="Trades/mo" value={formatNumber(attempt.trades_per_month_36m ?? null, 1)} />
          <TinyStat
            label="RR"
            value={formatRewardMultiple(attempt.reward_multiple_36m)}
            title={formatSetupTooltip(attempt)}
          />
        </div>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant={selected ? "secondary" : "default"}
            className="flex-1 rounded-lg"
            onClick={onToggle}
          >
            {selected ? <X className="h-4 w-4" /> : <Plus className="h-4 w-4" />}
            {selected ? "Remove" : "Select"}
          </Button>
          <Button type="button" variant="outline" size="icon" className="rounded-lg" onClick={onPreview}>
            <Eye className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </article>
  );
}

function PortfolioPanel({
  account,
  setAccount,
  selectedRows,
  selectedIds,
  persistSelectedIds,
  portfolio,
  similarity,
  loadingSelectionCount,
  isSavingLivePortfolio,
  job,
  onExport,
  onCancelJob,
  isStartingExport,
  isCancelingJob,
}: {
  account: AccountConfig;
  setAccount: (value: AccountConfig) => void;
  selectedRows: AttemptCatalogRow[];
  selectedIds: string[];
  persistSelectedIds: (value: string[]) => void;
  portfolio: { points: PortfolioPoint[]; metrics: PortfolioMetrics };
  similarity: PortfolioSimilarity;
  loadingSelectionCount: number;
  isSavingLivePortfolio: boolean;
  job?: DashboardJob;
  onExport: () => void;
  onCancelJob: () => void;
  isStartingExport: boolean;
  isCancelingJob: boolean;
}) {
  const metrics = portfolio.metrics;
  const isJobRunning = job?.status === "running" || job?.status === "canceling";
  const [hoveredChart, setHoveredChart] = useState<PortfolioChartMode | null>(null);
  const showUsdCharts = account.riskBasis === "current";
  const equityDataKey = showUsdCharts ? "balance_usd" : "equity_r";
  const drawdownDataKey = showUsdCharts ? "drawdown_usd" : "drawdown_r";
  const chartUnitLabel = showUsdCharts ? "account USD" : "portfolio R";
  const loadBasisLabel = showUsdCharts ? "current balance" : "starting balance";
  const tooltipFor = (mode: PortfolioChartMode) => (
    <PortfolioTooltip mode={mode} enabled={hoveredChart === mode} />
  );
  const assetExposure = useMemo(() => buildAssetExposureRows(selectedRows), [selectedRows]);
  return (
    <>
      <Panel className="p-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
              <SlidersHorizontal className="h-3.5 w-3.5" />
              Account sim
            </div>
            <div className="mt-2 text-xl font-semibold tracking-tight">
              {metrics.marginLiquidated ? "Broker stop-out" : metrics.blown ? "Account breached" : "Account intact"}
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <BrokerPresetMenu onSelect={(preset) => setAccount({ ...preset.account })} />
            <div
              className={`rounded-lg border px-3 py-2 text-sm font-semibold ${
                metrics.blown || metrics.marginLiquidated
                  ? "border-rose-300/50 bg-rose-300/10 text-rose-100"
                  : "border-emerald-300/50 bg-emerald-300/10 text-emerald-100"
              }`}
            >
              {metrics.marginLiquidated ? "Stop-out" : metrics.blown ? "Blown" : "Live"}
            </div>
          </div>
        </div>
        <div className="mt-4 space-y-2">
          <AccountInput
            label="Starting balance"
            value={account.balanceUsd}
            prefix="$"
            step={100}
            onChange={(balanceUsd) => setAccount({ ...account, balanceUsd })}
          />
          <AccountInput
            label="Risk per portfolio R"
            value={account.riskPerRPercent}
            suffix="%"
            step={0.05}
            onChange={(riskPerRPercent) => setAccount({ ...account, riskPerRPercent })}
          />
          <AccountRiskBasisControl
            value={account.riskBasis}
            onChange={(riskBasis) => setAccount({ ...account, riskBasis })}
          />
          <AccountInput
            label="Minimum lot"
            value={account.minLot}
            step={0.01}
            onChange={(minLot) => setAccount({ ...account, minLot })}
          />
          <AccountInput
            label="Lot step"
            value={account.lotStep}
            step={0.01}
            onChange={(lotStep) => setAccount({ ...account, lotStep })}
          />
          <AccountInput
            label="Notional per lot"
            value={account.notionalUsdPerLot}
            prefix="$"
            step={1000}
            onChange={(notionalUsdPerLot) => setAccount({ ...account, notionalUsdPerLot })}
          />
          <AccountInput
            label="Broker leverage"
            value={account.leverage}
            suffix=":1"
            step={50}
            onChange={(leverage) => setAccount({ ...account, leverage })}
          />
          <AccountInput
            label="Margin call level"
            value={account.marginCallLevelPercent}
            suffix="%"
            step={5}
            onChange={(marginCallLevelPercent) => setAccount({ ...account, marginCallLevelPercent })}
          />
          <AccountInput
            label="Stop-out level"
            value={account.stopOutLevelPercent}
            suffix="%"
            step={5}
            onChange={(stopOutLevelPercent) => setAccount({ ...account, stopOutLevelPercent })}
          />
          <AccountInput
            label="Commission per trade"
            value={account.commissionRPerTrade}
            step={0.005}
            onChange={(commissionRPerTrade) => setAccount({ ...account, commissionRPerTrade })}
          />
          <AccountInput
            label="Spread per trade"
            value={account.spreadRPerTrade}
            step={0.005}
            onChange={(spreadRPerTrade) => setAccount({ ...account, spreadRPerTrade })}
          />
          <AccountInput
            label="Slippage per trade"
            value={account.slippageRPerTrade}
            step={0.005}
            onChange={(slippageRPerTrade) => setAccount({ ...account, slippageRPerTrade })}
          />
        </div>
        <div className="mt-3 rounded-lg border border-border/60 bg-background/35 p-3 text-xs leading-5 text-muted-foreground">
          Currency is derived from R: one portfolio R starts at {formatCurrency(metrics.riskDollars)}
          {account.riskBasis === "current" ? ` and ends at ${formatCurrency(metrics.finalRiskDollars)}` : ""}.
          Lot sizing floors to {formatNumber(account.lotStep, 2)} lot steps with a {formatNumber(account.minLot, 2)} minimum lot.
          {metrics.minLotForcedTrades ? ` ${formatInt(metrics.minLotForcedTrades)} trades were forced to minimum lot. ` : " "}
          {metrics.maxOpenTrades ? `Peak simultaneous open trades: ${formatInt(metrics.maxOpenTrades)}. ` : ""}
          {metrics.maxUsedMarginUsd ? `Peak used margin: ${formatCurrency(metrics.maxUsedMarginUsd)}. ` : ""}
          {metrics.maxGrossMarginLoadPercent != null ? `Worst load versus ${loadBasisLabel}: ${formatPercentDecimal(metrics.maxGrossMarginLoadPercent)}. ` : ""}
          {metrics.maxDepositLoadPercent != null ? `Worst equity load: ${formatPercentDecimal(metrics.maxDepositLoadPercent)} at ${formatInt(metrics.openTradesAtMaxDepositLoad)} open / ${formatCurrency(metrics.usedMarginAtMaxDepositLoadUsd)}. ` : ""}
          Stop-out pressure uses current equity divided by used margin, so it can improve when a newly added strategy raises equity before the pressure peak.
          {metrics.marginLiquidated ? ` First stop-out ${metrics.firstLiquidationDate}.` : " No stop-out detected."}
          {metrics.sampledCurveCount ? (
            <span className="mt-2 block text-amber-100/85">
              Using sampled profile-drop curves for {formatInt(metrics.sampledCurveCount)} strategy(s)
              {metrics.sampledEventPointCount && metrics.sampledReturnedPointCount
                ? ` (${formatInt(metrics.sampledReturnedPointCount)} displayed points from ${formatInt(metrics.sampledEventPointCount)} source events)`
                : ""}
              ; intraday margin peaks may be understated until the simulator reads event-level trade ledgers.
            </span>
          ) : null}
        </div>
      </Panel>

      <Panel className="p-4">
        <div className="grid grid-cols-2 gap-3">
          <PortfolioMetric
            label="Final"
            primary={formatCurrency(metrics.finalBalanceUsd)}
            secondary={formatSignedR(metrics.finalEquityR)}
          />
          <PortfolioMetric
            label="Realized"
            primary={formatCurrency(metrics.finalRealizedUsd)}
            secondary={formatSignedR(metrics.finalRealizedR)}
          />
          <PortfolioMetric
            label="Max DD"
            primary={formatCurrency(metrics.maxDrawdownUsd)}
            secondary={metrics.maxDrawdownR == null ? "-" : `${formatNumber(metrics.maxDrawdownR, 2)}R`}
          />
          <PortfolioMetric
            label="Min balance"
            primary={formatCurrency(metrics.minBalanceUsd)}
            secondary={metrics.marginLiquidated ? "stop-out" : metrics.blown ? "breached" : "floor"}
          />
          <PortfolioMetric
            label="Basis load"
            primary={formatPercentDecimal(metrics.maxGrossMarginLoadPercent)}
            secondary={`vs ${loadBasisLabel}`}
          />
          <PortfolioMetric
            label="Equity load"
            primary={formatPercentDecimal(metrics.maxDepositLoadPercent)}
            secondary={`${formatInt(metrics.openTradesAtMaxDepositLoad)} open / ${formatCurrency(metrics.usedMarginAtMaxDepositLoadUsd)}`}
          />
          <PortfolioMetric
            label="Min margin"
            primary={formatPercentDecimal(metrics.minMarginLevelPercent)}
            secondary={`max risk ${formatPercentDecimal(metrics.maxMarginRiskPercent)}`}
          />
          <PortfolioMetric
            label="Trades/mo"
            primary={formatNumber(metrics.tradesPerMonth, 1)}
            secondary={`${formatInt(metrics.totalTrades)} total`}
          />
          <PortfolioMetric
            label="Cost/trade"
            primary={formatCurrency(metrics.averageCostUsdPerTrade)}
            secondary={`${formatNumber(metrics.costRPerTrade, 3)}R`}
          />
        </div>
      </Panel>

      <AssetExposurePanel rows={assetExposure} />

      <SimilarityPanel similarity={similarity} loadingSelectionCount={loadingSelectionCount} />

      <Panel className="p-4">
        <div className="mb-3 flex items-center justify-between gap-3">
          <div>
            <div className="text-sm font-semibold">36mo composite</div>
            <div className="text-xs text-muted-foreground">
              {loadingSelectionCount
                ? `${loadingSelectionCount} curve(s) loading`
                : `${metrics.loadedCount} curve(s) loaded / ${chartUnitLabel}`}
            </div>
          </div>
        </div>
        {portfolio.points.length ? (
          <div className="space-y-4">
            <ChartContainer config={equityChartConfig} className="h-56 w-full">
              <AreaChart
                data={portfolio.points}
                margin={{ left: 4, right: 4, top: 8, bottom: 0 }}
                syncId="portfolio-composite"
                syncMethod="value"
                onMouseEnter={() => setHoveredChart("equity")}
                onMouseLeave={() => setHoveredChart(null)}
              >
                <CartesianGrid vertical={false} strokeDasharray="3 3" />
                <XAxis dataKey="time" minTickGap={28} tickFormatter={formatTickDate} />
                <YAxis
                  width={58}
                  tickFormatter={(value) => showUsdCharts
                    ? formatCurrencyTick(Number(value))
                    : `${formatNumber(Number(value), 0)}R`}
                />
                <ReferenceLine y={0} stroke="hsl(var(--border))" strokeDasharray="4 4" />
                <ChartTooltip
                  cursor={chartCursor}
                  content={tooltipFor("equity")}
                />
                <Area type="monotone" dataKey={equityDataKey} stroke={`var(--color-${equityDataKey})`} fill={`var(--color-${equityDataKey})`} fillOpacity={0.16} strokeWidth={2} dot={false} activeDot={chartActiveDot} />
              </AreaChart>
            </ChartContainer>
            <ChartContainer config={drawdownChartConfig} className="h-40 w-full">
              <AreaChart
                data={portfolio.points}
                margin={{ left: 4, right: 4, top: 8, bottom: 0 }}
                syncId="portfolio-composite"
                syncMethod="value"
                onMouseEnter={() => setHoveredChart("drawdown")}
                onMouseLeave={() => setHoveredChart(null)}
              >
                <CartesianGrid vertical={false} strokeDasharray="3 3" />
                <XAxis dataKey="time" minTickGap={28} tickFormatter={formatTickDate} />
                <YAxis
                  width={58}
                  tickFormatter={(value) => showUsdCharts
                    ? formatCurrencyTick(Number(value))
                    : `${formatNumber(Number(value), 0)}R`}
                />
                <ChartTooltip
                  cursor={chartCursor}
                  content={tooltipFor("drawdown")}
                />
                <Area type="monotone" dataKey={drawdownDataKey} stroke={`var(--color-${drawdownDataKey})`} fill={`var(--color-${drawdownDataKey})`} fillOpacity={0.25} strokeWidth={2} dot={false} activeDot={chartActiveDot} />
              </AreaChart>
            </ChartContainer>
            <div className="rounded-lg border border-border/60 bg-background/25 px-2 py-2">
              <div className="mb-1 flex items-center justify-between gap-3 px-1 text-[0.68rem] uppercase tracking-[0.14em] text-muted-foreground">
                <span>Margin exposure</span>
                <span>yellow load vs {loadBasisLabel} / red stop-out pressure</span>
              </div>
              <ChartContainer config={marginRiskChartConfig} className="h-24 w-full">
                <ComposedChart
                  data={portfolio.points}
                  margin={{ left: 4, right: 4, top: 4, bottom: 8 }}
                  syncId="portfolio-composite"
                  syncMethod="value"
                  onMouseEnter={() => setHoveredChart("margin")}
                  onMouseLeave={() => setHoveredChart(null)}
                >
                  <CartesianGrid vertical={false} strokeDasharray="3 3" />
                  <XAxis dataKey="time" hide />
                  <YAxis
                    yAxisId="margin"
                    width={48}
                    domain={[0, 100]}
                    ticks={[0, Math.round(normalizedMarginCallRiskPercent(account)), 100]}
                    interval={0}
                    tickFormatter={(value) => `${formatNumber(Number(value), 0)}%`}
                  />
                  <ReferenceLine
                    yAxisId="margin"
                    y={normalizedMarginCallRiskPercent(account)}
                    stroke="oklch(0.84 0.16 82)"
                    strokeDasharray="3 5"
                  />
                  <ReferenceLine
                    yAxisId="margin"
                    y={100}
                    stroke="oklch(0.7 0.18 28)"
                    strokeDasharray="4 4"
                  />
                  <ChartTooltip
                    cursor={chartCursor}
                    content={tooltipFor("margin")}
                  />
                  <Line
                    yAxisId="margin"
                    type="monotone"
                    dataKey="gross_margin_load_pct"
                    stroke="var(--color-gross_margin_load_pct)"
                    strokeWidth={1.8}
                    dot={false}
                    activeDot={chartActiveDot}
                    connectNulls={false}
                  />
                  <Line
                    yAxisId="margin"
                    type="monotone"
                    dataKey="margin_risk_pct"
                    stroke="var(--color-margin_risk_pct)"
                    strokeDasharray="4 4"
                    strokeWidth={1.5}
                    dot={false}
                    activeDot={chartActiveDot}
                    connectNulls={false}
                  />
              </ComposedChart>
              </ChartContainer>
              <div className="mt-1 flex items-center justify-end gap-3 px-1 text-[0.68rem] uppercase tracking-[0.14em] text-muted-foreground">
                <span className="inline-flex items-center gap-1.5">
                  <span className="h-0.5 w-5 rounded bg-[oklch(0.86_0.16_82)]" />
                  Load
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="h-0.5 w-5 rounded border-t border-dashed border-[oklch(0.7_0.18_28)]" />
                  Pressure
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="h-0.5 w-5 rounded border-t border-dashed border-[oklch(0.84_0.16_82)]" />
                  Call
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="h-0.5 w-5 rounded border-t border-dashed border-[oklch(0.7_0.18_28)]" />
                  Stop
                </span>
              </div>
            </div>
          </div>
        ) : (
          <div className="flex min-h-72 items-center justify-center rounded-lg border border-dashed border-border/70 bg-background/35 p-6 text-center text-sm text-muted-foreground">
            Select attempts with valid 36mo curves to build a live composite.
          </div>
        )}
      </Panel>

      <Panel className="p-4">
        <div className="mb-3 flex items-center justify-between gap-3">
          <div>
            <div className="text-sm font-semibold">Live set</div>
            {isSavingLivePortfolio ? (
              <div className="mt-1 text-xs text-muted-foreground">Saving selection...</div>
            ) : null}
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <Button
              type="button"
              size="sm"
              className="rounded-lg"
              disabled={!selectedIds.length || isJobRunning || isStartingExport}
              onClick={onExport}
            >
              <Download className="h-4 w-4" />
              Export
            </Button>
            {selectedIds.length ? (
              <Button variant="outline" size="sm" className="rounded-lg" onClick={() => persistSelectedIds([])}>
                <Trash2 className="h-4 w-4" />
                Clear
              </Button>
            ) : null}
          </div>
        </div>
        {job?.kind ? (
          <div className="mb-3 rounded-lg border border-border/60 bg-background/35 p-3">
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-xs uppercase tracking-[0.14em] text-muted-foreground">
                  Portfolio job
                </div>
                <div className="mt-1 truncate text-sm font-medium">
                  {job.kind} / {job.status}
                </div>
              </div>
              {isJobRunning ? (
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="rounded-lg"
                  disabled={isCancelingJob}
                  onClick={onCancelJob}
                >
                  <StopCircle className="h-4 w-4" />
                  Cancel
                </Button>
              ) : null}
            </div>
            {job.portfolio_config_path ? (
              <div className="mt-2 truncate text-xs text-muted-foreground" title={job.portfolio_config_path}>
                Config: {job.portfolio_config_path}
              </div>
            ) : null}
          </div>
        ) : null}
        <div className="space-y-2">
          {selectedRows.length ? (
            selectedRows.map((row) => (
              <div key={row.attempt_id} className="rounded-lg border border-border/60 bg-background/35 p-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-medium">{row.candidate_name || row.attempt_id}</div>
                    <div className="mt-1 truncate text-xs text-muted-foreground">{row.strategy_key_36m || compactRunId(row.run_id)}</div>
                  </div>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    className="rounded-lg"
                    onClick={() => persistSelectedIds(selectedIds.filter((id) => id !== row.attempt_id))}
                  >
                    <X className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            ))
          ) : (
            <div className="rounded-lg border border-dashed border-border/70 bg-background/35 p-4 text-sm text-muted-foreground">
              No selected strategies yet.
            </div>
          )}
        </div>
      </Panel>
    </>
  );
}

function PortfolioViewer({
  isOpen,
  onClose,
  account,
  setAccount,
  selectedRows,
  selectedIds,
  persistSelectedIds,
  portfolio,
  similarity,
  clusters,
  clusterThreshold,
  setClusterThreshold,
  clusterCollisionEnabled,
  setClusterCollisionEnabled,
  loadingSelectionCount,
  isSavingLivePortfolio,
  job,
  onExport,
  onCancelJob,
  isStartingExport,
  isCancelingJob,
  onPreview,
}: {
  isOpen: boolean;
  onClose: () => void;
  account: AccountConfig;
  setAccount: (value: AccountConfig) => void;
  selectedRows: AttemptCatalogRow[];
  selectedIds: string[];
  persistSelectedIds: (value: string[]) => void;
  portfolio: { points: PortfolioPoint[]; metrics: PortfolioMetrics };
  similarity: PortfolioSimilarity;
  clusters: PortfolioCluster[];
  clusterThreshold: number;
  setClusterThreshold: (value: number) => void;
  clusterCollisionEnabled: boolean;
  setClusterCollisionEnabled: (value: boolean) => void;
  loadingSelectionCount: number;
  isSavingLivePortfolio: boolean;
  job?: DashboardJob;
  onExport: () => void;
  onCancelJob: () => void;
  isStartingExport: boolean;
  isCancelingJob: boolean;
  onPreview: (attempt: AttemptCatalogRow) => void;
}) {
  const [activeClusterId, setActiveClusterId] = useState<string | null>(null);
  const activeCluster = useMemo(
    () => clusters.find((cluster) => cluster.id === activeClusterId) ?? null,
    [activeClusterId, clusters],
  );
  const thresholdStats = useMemo(
    () => summarizeClusterThreshold(similarity, clusterThreshold),
    [clusterThreshold, similarity],
  );
  const multiMemberClusters = clusters.filter((cluster) => cluster.members.length > 1);
  const singletonClusters = clusters.length - multiMemberClusters.length;

  useEffect(() => {
    if (!isOpen) return;

    const previousOverflow = document.body.style.overflow;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        if (activeClusterId) {
          setActiveClusterId(null);
        } else {
          onClose();
        }
      }
    };

    document.body.style.overflow = "hidden";
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [activeClusterId, isOpen, onClose]);

  useEffect(() => {
    if (!isOpen) return;
    if (activeClusterId && clusters.some((cluster) => cluster.id === activeClusterId)) return;
    setActiveClusterId(null);
  }, [activeClusterId, clusters, isOpen]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-40 bg-background/95 backdrop-blur-xl">
      <div className="mx-auto flex h-screen w-full max-w-[1920px] flex-col gap-4 px-4 py-4 md:px-6">
        <header className="flex shrink-0 flex-wrap items-center justify-between gap-4 border-b border-border/60 pb-4">
          <div className="min-w-0">
            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
              <Network className="h-3.5 w-3.5" />
              Portfolio topology
            </div>
            <h2 className="mt-2 truncate text-2xl font-semibold tracking-tight md:text-4xl">
              Cluster the live set, then prune inside the cluster.
            </h2>
          </div>
          <div className="flex items-center gap-2">
            <div className="hidden grid-cols-4 gap-2 lg:grid">
              <TinyStat label="Selected" value={formatInt(selectedRows.length)} />
              <TinyStat label="Loaded" value={`${formatInt(similarity.loadedCount)}/${formatInt(similarity.selectedCount)}`} />
              <TinyStat label="Clusters" value={formatInt(clusters.length)} />
              <TinyStat label="Multi" value={formatInt(multiMemberClusters.length)} />
            </div>
            <Button type="button" variant="outline" className="rounded-lg" onClick={onClose}>
              <X className="h-4 w-4" />
              Close
            </Button>
          </div>
        </header>

        <div className="grid min-h-0 flex-1 gap-4 xl:grid-cols-[minmax(0,1fr)_430px]">
          <div className="flex min-h-0 flex-col gap-4 overflow-hidden">
            <Panel className="shrink-0 p-4">
              <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px] lg:items-center">
                <div>
                  <div className="flex items-center gap-2 text-sm font-semibold">
                    <Fingerprint className="h-4 w-4 text-muted-foreground" />
                    Similarity clustering
                  </div>
                  <div className="mt-1 text-sm text-muted-foreground">
                    Connected strategies above the behavioral threshold become a cluster. Position is score versus trade cadence; exposure stays separate.
                  </div>
                  <label className="mt-3 inline-flex items-center gap-2 rounded-lg border border-border/50 bg-background/30 px-3 py-2 text-xs text-muted-foreground">
                    <input
                      type="checkbox"
                      checked={clusterCollisionEnabled}
                      onChange={(event) => setClusterCollisionEnabled(event.target.checked)}
                      className="h-4 w-4 accent-primary"
                    />
                    Nudge overlapping markers
                  </label>
                </div>
                <div className="rounded-lg border border-border/60 bg-background/35 p-3">
                  <div className="mb-2 flex items-center justify-between gap-3 text-xs">
                    <span className="uppercase tracking-[0.14em] text-muted-foreground">Cluster threshold</span>
                    <span className="font-semibold tabular-nums">{formatPercent(clusterThreshold)}</span>
                  </div>
                  <input
                    type="range"
                    min={0.05}
                    max={0.95}
                    step={0.01}
                    value={clusterThreshold}
                    onChange={(event) => setClusterThreshold(toNumber(event.target.value, DEFAULT_CLUSTER_THRESHOLD))}
                    className="w-full accent-primary"
                    aria-label="Cluster similarity threshold"
                  />
                  <div className="mt-2 grid grid-cols-3 gap-1">
                    <ThresholdPresetButton value={LOOSE_CLUSTER_THRESHOLD} current={clusterThreshold} onSelect={setClusterThreshold}>
                      Loose
                    </ThresholdPresetButton>
                    <ThresholdPresetButton value={DEFAULT_CLUSTER_THRESHOLD} current={clusterThreshold} onSelect={setClusterThreshold}>
                      Normal
                    </ThresholdPresetButton>
                    <ThresholdPresetButton value={STRICT_CLUSTER_THRESHOLD} current={clusterThreshold} onSelect={setClusterThreshold}>
                      Strict
                    </ThresholdPresetButton>
                  </div>
                  <div className="mt-3 rounded-md border border-border/45 bg-background/30 px-2.5 py-2 text-[0.68rem] leading-4 text-muted-foreground">
                    <span className="font-medium tabular-nums text-foreground">
                      {formatInt(thresholdStats.linkedPairCount)}
                    </span>
                    {" linked pair(s), "}
                    <span className="font-medium tabular-nums text-foreground">{formatInt(clusters.length)}</span>
                    {" clusters / "}
                    <span className="font-medium tabular-nums text-foreground">{formatInt(multiMemberClusters.length)}</span>
                    {" multi. "}
                    {thresholdStats.nearestExcluded == null ? (
                      "No just-below pairs."
                    ) : (
                      <>Next excluded pair is {formatPercent(thresholdStats.nearestExcluded)}.</>
                    )}
                  </div>
                </div>
              </div>
            </Panel>

            <PortfolioClusterMap
              clusters={clusters}
              activeClusterId={activeCluster?.id ?? null}
              onOpenCluster={setActiveClusterId}
              loadingSelectionCount={loadingSelectionCount}
            />
          </div>

          <aside className="min-h-0 overflow-y-auto pr-1">
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-2 lg:hidden">
                <TinyStat label="Clusters" value={formatInt(clusters.length)} />
                <TinyStat label="Singletons" value={formatInt(singletonClusters)} />
              </div>
              <PortfolioPanel
                account={account}
                setAccount={setAccount}
                selectedRows={selectedRows}
                selectedIds={selectedIds}
                persistSelectedIds={persistSelectedIds}
                portfolio={portfolio}
                similarity={similarity}
                loadingSelectionCount={loadingSelectionCount}
                isSavingLivePortfolio={isSavingLivePortfolio}
                job={job}
                onExport={onExport}
                onCancelJob={onCancelJob}
                isStartingExport={isStartingExport}
                isCancelingJob={isCancelingJob}
              />
            </div>
          </aside>
        </div>
      </div>

      <ClusterDetailModal
        cluster={activeCluster}
        selectedIds={selectedIds}
        persistSelectedIds={persistSelectedIds}
        onClose={() => setActiveClusterId(null)}
        onPreview={onPreview}
      />
    </div>
  );
}

function ThresholdPresetButton({
  value,
  current,
  onSelect,
  children,
}: {
  value: number;
  current: number;
  onSelect: (value: number) => void;
  children: ReactNode;
}) {
  const active = Math.abs(value - current) < 0.005;
  return (
    <button
      type="button"
      onClick={() => onSelect(value)}
      className={`h-8 rounded-md border px-2 text-xs font-medium transition ${
        active
          ? "border-amber-300/60 bg-amber-300/12 text-amber-100"
          : "border-border/50 bg-background/25 text-muted-foreground hover:text-foreground"
      }`}
    >
      {children}
    </button>
  );
}

function PortfolioClusterMap({
  clusters,
  activeClusterId,
  onOpenCluster,
  loadingSelectionCount,
}: {
  clusters: PortfolioCluster[];
  activeClusterId: string | null;
  onOpenCluster: (clusterId: string) => void;
  loadingSelectionCount: number;
}) {
  return (
    <Panel className="min-h-[360px] flex-1 overflow-hidden p-4 lg:min-h-0">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold">Cluster map</div>
          <div className="mt-1 text-xs text-muted-foreground">
            Higher score rises vertically. Faster cadence moves right.
          </div>
        </div>
        <div className="text-right text-xs text-muted-foreground">
          {loadingSelectionCount ? `${formatInt(loadingSelectionCount)} loading` : `${formatInt(clusters.length)} cluster(s)`}
        </div>
      </div>

      {clusters.length ? (
        <div
          className="relative h-full min-h-[320px] overflow-visible rounded-lg border border-border/60 bg-background/35"
          style={{
            backgroundImage:
              "linear-gradient(oklch(0.9 0.02 80 / 0.07) 1px, transparent 1px), linear-gradient(90deg, oklch(0.9 0.02 80 / 0.07) 1px, transparent 1px)",
            backgroundSize: "72px 72px",
          }}
        >
          <div className="pointer-events-none absolute left-4 top-4 text-[0.68rem] uppercase tracking-[0.14em] text-muted-foreground">
            Score
          </div>
          <div className="pointer-events-none absolute bottom-4 right-4 text-[0.68rem] uppercase tracking-[0.14em] text-muted-foreground">
            Cadence
          </div>
          <div className="pointer-events-none absolute bottom-8 left-8 right-8 border-t border-dashed border-border/55" />
          <div className="pointer-events-none absolute bottom-8 top-8 left-8 border-l border-dashed border-border/55" />

          {clusters.map((cluster) => {
            const active = cluster.id === activeClusterId;
            const singleton = cluster.members.length === 1;
            const compactWidth = singleton ? 44 : Math.min(58, 44 + cluster.members.length * 5);
            const compactHeight = singleton ? 38 : 42;
            return (
              <button
                key={cluster.id}
                type="button"
                aria-label={`${cluster.label}, ${formatInt(cluster.members.length)} strategy cluster`}
                aria-pressed={active}
                onClick={() => onOpenCluster(cluster.id)}
                className={`group absolute flex -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-xl border text-center transition-[border-color,box-shadow,transform] duration-200 ease-out hover:z-50 hover:scale-105 focus-visible:z-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50 ${
                  active ? "z-30 ring-2 ring-primary/70" : "z-10"
                }`}
                style={{
                  left: `${cluster.x}%`,
                  top: `${cluster.y}%`,
                  width: compactWidth,
                  height: compactHeight,
                  borderColor: `oklch(0.78 0.14 ${cluster.hue} / ${active ? 0.78 : 0.55})`,
                  backgroundColor: `oklch(0.20 0.055 ${cluster.hue} / 0.96)`,
                  boxShadow: active
                    ? `0 0 0 1px oklch(0.78 0.14 ${cluster.hue} / 0.55), 0 18px 44px oklch(0 0 0 / 0.34)`
                    : "0 8px 20px oklch(0 0 0 / 0.24)",
                }}
              >
                <span className="pointer-events-none text-lg font-semibold tabular-nums tracking-tight text-foreground">
                  {formatInt(cluster.members.length)}
                </span>
                <span
                  className="pointer-events-none absolute left-1/2 top-1/2 z-20 grid w-32 -translate-x-1/2 -translate-y-1/2 scale-75 gap-1.5 rounded-xl border bg-popover/98 p-3 text-left opacity-0 shadow-2xl shadow-black/40 transition duration-200 ease-out group-hover:scale-100 group-hover:opacity-100 group-focus-visible:scale-100 group-focus-visible:opacity-100"
                  style={{ borderColor: `oklch(0.78 0.14 ${cluster.hue} / 0.58)` }}
                >
                  <span className="flex items-center justify-between gap-2">
                    <span className="text-[0.62rem] uppercase tracking-[0.12em] text-muted-foreground">
                      {cluster.label}
                    </span>
                    <span className="text-xl font-semibold tabular-nums">{formatInt(cluster.members.length)}</span>
                  </span>
                  <span className="grid grid-cols-2 gap-x-3 gap-y-1 text-[0.68rem]">
                    <span className="text-muted-foreground">Score</span>
                    <span className="text-right font-medium tabular-nums">{formatNumber(cluster.averageScore, 1)}</span>
                    <span className="text-muted-foreground">Cadence</span>
                    <span className="text-right font-medium tabular-nums">{formatNumber(cluster.tradesPerMonth, 1)}</span>
                    <span className="text-muted-foreground">Sameness</span>
                    <span className="text-right font-medium tabular-nums">{formatPercent(cluster.maxSameness)}</span>
                  </span>
                </span>
              </button>
            );
          })}
        </div>
      ) : (
        <div className="flex h-full min-h-[320px] items-center justify-center rounded-lg border border-dashed border-border/70 bg-background/35 p-6 text-center text-sm text-muted-foreground">
          Select strategies from the workbench to create the portfolio map.
        </div>
      )}
    </Panel>
  );
}

function ClusterDetailModal({
  cluster,
  selectedIds,
  persistSelectedIds,
  onClose,
  onPreview,
}: {
  cluster: PortfolioCluster | null;
  selectedIds: string[];
  persistSelectedIds: (value: string[]) => void;
  onClose: () => void;
  onPreview: (attempt: AttemptCatalogRow) => void;
}) {
  if (!cluster) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-50 bg-black/72 p-4 backdrop-blur-sm md:p-6">
      <section className="mx-auto flex h-full max-w-[1840px] flex-col overflow-hidden rounded-lg border border-border/70 bg-background shadow-2xl shadow-black/45">
        <header className="flex shrink-0 flex-wrap items-start justify-between gap-4 border-b border-border/60 px-4 py-4 md:px-6">
          <div>
            <div className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Cluster drill-down</div>
            <h3 className="mt-1 text-2xl font-semibold tracking-tight">
              {cluster.label} / {formatInt(cluster.members.length)} member{cluster.members.length === 1 ? "" : "s"}
            </h3>
            <div className="mt-1 text-sm text-muted-foreground">
              Avg score {formatNumber(cluster.averageScore, 1)} / cadence {formatNumber(cluster.tradesPerMonth, 1)} / max sameness {formatPercent(cluster.maxSameness)}
            </div>
            <ClusterLinkSummary pair={cluster.strongestPair} />
          </div>
          <div className="flex items-center gap-3">
            <div className="hidden grid-cols-3 gap-2 text-right sm:grid">
              <CompactMetric label="Pairs" value={formatInt(cluster.pairCount)} />
              <CompactMetric label="DD" value={cluster.maxDrawdownR == null ? "-" : `${formatNumber(cluster.maxDrawdownR, 2)}R`} />
              <CompactMetric label="Size" value={formatInt(cluster.members.length)} />
            </div>
            <Button type="button" variant="outline" className="rounded-lg" onClick={onClose}>
              <X className="h-4 w-4" />
              Close
            </Button>
          </div>
        </header>
        <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4 md:px-6">
          <div className="grid gap-4 min-[900px]:grid-cols-2 min-[1400px]:grid-cols-3">
            {cluster.members.map((member) => (
              <AttemptCard
                key={member.attemptId}
                attempt={member.row}
                selected
                onToggle={() => persistSelectedIds(selectedIds.filter((id) => id !== member.attemptId))}
                onPreview={() => onPreview(member.row)}
              />
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}

function ClusterLinkSummary({ pair }: { pair: SimilarityPair | null }) {
  if (!pair) {
    return null;
  }

  return (
    <div className="mt-3 flex max-w-4xl flex-wrap items-center gap-2 rounded-lg border border-border/60 bg-background/35 px-3 py-2 text-xs">
      <span className="shrink-0 uppercase tracking-[0.14em] text-muted-foreground">Strongest link</span>
      <span className="min-w-0 flex-1 truncate font-medium" title={`${pair.leftLabel} / ${pair.rightLabel}`}>
        {pair.leftLabel} / {pair.rightLabel}
      </span>
      <span className="rounded-md border border-border/50 bg-background/45 px-2 py-1 tabular-nums">
        {formatPercent(pair.similarityScore)}
      </span>
      <span className="rounded-md border border-border/50 bg-background/45 px-2 py-1 tabular-nums">
        Delta {pair.correlation == null ? "-" : formatNumber(pair.correlation, 2)}
      </span>
      <span className="rounded-md border border-border/50 bg-background/45 px-2 py-1 tabular-nums">
        Active {formatPercent(pair.activeOverlapRatio)}
      </span>
      <span className="rounded-md border border-border/50 bg-background/45 px-2 py-1 tabular-nums">
        DD {formatPercent(pair.drawdownOverlapRatio)}
      </span>
    </div>
  );
}

function BrokerPresetMenu({ onSelect }: { onSelect: (preset: BrokerPreset) => void }) {
  const [open, setOpen] = useState(false);

  return (
    <div className="relative">
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="rounded-lg"
        onClick={() => setOpen((value) => !value)}
      >
        Presets
        <ChevronDown className={`h-4 w-4 transition-transform ${open ? "rotate-180" : ""}`} />
      </Button>
      {open ? (
        <div className="absolute right-0 top-full z-30 mt-2 w-72 overflow-hidden rounded-lg border border-border bg-popover p-1 text-popover-foreground shadow-xl">
          {BROKER_PRESETS.map((preset) => (
            <button
              key={preset.id}
              type="button"
              className="block w-full rounded-md px-3 py-2 text-left transition hover:bg-muted"
              onClick={() => {
                onSelect(preset);
                setOpen(false);
              }}
            >
              <span className="block text-sm font-medium">{preset.label}</span>
              <span className="mt-0.5 block text-xs text-muted-foreground">{preset.description}</span>
              <span className="mt-1 block text-[0.68rem] uppercase tracking-[0.14em] text-muted-foreground">
                ${formatInt(preset.account.balanceUsd)} / {formatNumber(preset.account.riskPerRPercent, 2)}% R / {formatNumber(preset.account.leverage, 0)}:1
              </span>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function SimilarityPanel({
  similarity,
  loadingSelectionCount,
}: {
  similarity: PortfolioSimilarity;
  loadingSelectionCount: number;
}) {
  const hasMatrix = similarity.cells.length > 1;
  const matrixSize = similarity.cells.length;
  const matrixGapPx = matrixSize > 14 ? 2 : matrixSize > 8 ? 3 : 4;
  return (
    <Panel className="p-4">
      <div className="mb-3 flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold">
            <Fingerprint className="h-4 w-4 text-muted-foreground" />
            Behavioral diversity
          </div>
          <div className="mt-1 text-xs text-muted-foreground">
            {loadingSelectionCount
              ? `${loadingSelectionCount} curve(s) loading`
              : `${similarity.loadedCount}/${similarity.selectedCount} curve(s) compared`}
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2 text-right">
          <CompactMetric label="Avg" value={formatPercent(similarity.averageSameness)} />
          <CompactMetric label="Max" value={formatPercent(similarity.maxSameness)} />
        </div>
      </div>

      {hasMatrix ? (
        <>
          <div className="aspect-square w-full rounded-lg border border-border/60 bg-background/30 p-2">
            <div
              className="grid h-full w-full"
              style={{
                gridTemplateColumns: `repeat(${matrixSize}, minmax(0, 1fr))`,
                gridTemplateRows: `repeat(${matrixSize}, minmax(0, 1fr))`,
                gap: matrixGapPx,
              }}
            >
              {similarity.cells.flatMap((row) =>
                row.map((cell) => (
                  <SimilarityCellButton key={`${cell.rowAttemptId}:${cell.columnAttemptId}`} cell={cell} />
                )),
              )}
            </div>
          </div>
          <div className="mt-3 flex items-center justify-between gap-3 text-[0.68rem] uppercase tracking-[0.14em] text-muted-foreground">
            <span>Distinct</span>
            <div className="h-2 min-w-24 flex-1 rounded-full bg-[linear-gradient(90deg,oklch(0.26_0.14_260),oklch(0.56_0.15_215),oklch(0.92_0.17_95))]" />
            <span>Same</span>
          </div>
          {similarity.maxPair ? (
            <div className="mt-3 truncate text-xs text-muted-foreground">
              Closest: {similarity.maxPair.leftLabel} / {similarity.maxPair.rightLabel}
            </div>
          ) : null}
        </>
      ) : (
        <div className="rounded-lg border border-dashed border-border/70 bg-background/35 p-4 text-sm text-muted-foreground">
          Select at least two loaded strategies to see pairwise sameness.
        </div>
      )}
    </Panel>
  );
}

function AssetExposurePanel({ rows }: { rows: AssetExposureRow[] }) {
  const totalTradesPerMonth = rows.reduce((sum, row) => sum + row.tradesPerMonth, 0);
  return (
    <Panel className="p-4">
      <div className="mb-3 flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold">
            <Layers3 className="h-4 w-4 text-muted-foreground" />
            Asset exposure
          </div>
          <div className="mt-1 text-xs text-muted-foreground">
            Trades/mo split across listed instruments
          </div>
        </div>
        <CompactMetric label="Total" value={formatNumber(totalTradesPerMonth, 1)} />
      </div>

      {rows.length ? (
        <div className="space-y-2">
          {rows.slice(0, 8).map((row) => (
            <div key={row.instrument} className="rounded-lg border border-border/55 bg-background/32 p-2.5">
              <div className="mb-2 flex items-center justify-between gap-3">
                <div className="min-w-0 truncate text-sm font-medium">{row.instrument}</div>
                <div className="shrink-0 text-xs font-semibold tabular-nums">
                  {formatNumber(row.tradesPerMonth, 1)} / mo
                </div>
              </div>
              <div className="h-2 overflow-hidden rounded-full bg-muted/35">
                <div
                  className="h-full rounded-full bg-amber-300/80"
                  style={{ width: `${Math.max(3, row.share * 100)}%` }}
                />
              </div>
              <div className="mt-2 flex items-center justify-between gap-3 text-[0.68rem] uppercase tracking-[0.12em] text-muted-foreground">
                <span>{formatInt(row.strategyCount)} {row.strategyCount === 1 ? "strategy" : "strategies"}</span>
                <span className="tabular-nums">{formatPercent(row.share)}</span>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="rounded-lg border border-dashed border-border/70 bg-background/35 p-4 text-sm text-muted-foreground">
          Select strategies with instruments to see exposure.
        </div>
      )}
    </Panel>
  );
}

function SimilarityCellButton({ cell }: { cell: SimilarityCell }) {
  const label = cell.diagonal
    ? `${cell.rowLabel} self match`
    : `${cell.rowLabel} vs ${cell.columnLabel}`;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label={label}
          className={`aspect-square min-h-2 rounded-[3px] border transition hover:scale-110 hover:border-foreground/70 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/40 ${
            cell.diagonal ? "border-foreground/20" : "border-transparent"
          }`}
          style={{ background: similarityHeatColor(cell.value) }}
        />
      </TooltipTrigger>
      <TooltipContent side="left" className="max-w-72 border border-border/70 bg-popover text-popover-foreground shadow-xl shadow-black/30">
        <SimilarityCellTooltip cell={cell} />
      </TooltipContent>
    </Tooltip>
  );
}

function SimilarityCellTooltip({ cell }: { cell: SimilarityCell }) {
  if (cell.diagonal) {
    return (
      <div className="grid gap-1 text-xs">
        <div className="font-medium">{cell.rowLabel}</div>
        <TooltipMetric label="Sameness" value="100%" />
      </div>
    );
  }
  const pair = cell.pair;
  if (!pair) {
    return (
      <div className="grid gap-1 text-xs">
        <div className="font-medium">{cell.rowLabel}</div>
        <div className="text-muted-foreground">vs {cell.columnLabel}</div>
        <TooltipMetric label="Sameness" value={formatPercent(cell.value)} />
      </div>
    );
  }
  return (
    <div className="grid gap-1 text-xs">
      <div className="font-medium">{pair.leftLabel}</div>
      <div className="text-muted-foreground">vs {pair.rightLabel}</div>
      <TooltipMetric label="Sameness" value={formatPercent(pair.similarityScore)} />
      <TooltipMetric label="Delta corr" value={pair.correlation == null ? "-" : formatNumber(pair.correlation, 2)} />
      <TooltipMetric label="Active overlap" value={formatPercent(pair.activeOverlapRatio)} />
      <TooltipMetric label="DD overlap" value={formatPercent(pair.drawdownOverlapRatio)} />
      <TooltipMetric label="Overlap days" value={formatInt(pair.overlapDays)} />
    </div>
  );
}

function buildPortfolioCurve(
  details: AttemptDetail[],
  account: AccountConfig,
  selectedCount: number,
): { points: PortfolioPoint[]; metrics: PortfolioMetrics } {
  const startingBalanceUsd = Math.max(0, account.balanceUsd);
  const riskPercent = Math.max(0, account.riskPerRPercent) / 100;
  const riskDollars = startingBalanceUsd * riskPercent;
  const costRPerTrade = Math.max(
    0,
    account.commissionRPerTrade + account.spreadRPerTrade + account.slippageRPerTrade,
  );
  const series: NormalizedSeries[] = details
    .map((detail) => {
      const points = normalizeCurvePoints(detail.full_backtest_curve);
      const sampling = curveSamplingInfo(detail.full_backtest_curve, points.length);
      return {
        points,
        sizing: buildLotSizing(detail.attempt, account),
        ...sampling,
      };
    })
    .filter((item) => item.points.length > 0);
  const times = [...new Set(series.flatMap((item) => item.points.map((point) => point.time)))].sort((a, b) => a - b);
  const sampledSeries = series.filter((item) => item.downsampled);

  const metrics: PortfolioMetrics = {
    selectedCount,
    loadedCount: series.length,
    finalEquityR: null,
    finalRealizedR: null,
    maxDrawdownR: null,
    finalBalanceUsd: null,
    finalRealizedUsd: null,
    minBalanceUsd: null,
    maxDrawdownUsd: null,
    totalTrades: 0,
    tradesPerMonth: null,
    blown: false,
    riskDollars,
    finalRiskDollars: null,
    averageCostUsdPerTrade: null,
    minLotForcedTrades: 0,
    maxUsedMarginUsd: 0,
    maxStopOutEquityUsd: 0,
    maxGrossMarginLoadPercent: null,
    maxDepositLoadPercent: null,
    usedMarginAtMaxDepositLoadUsd: null,
    openTradesAtMaxDepositLoad: 0,
    maxOpenTrades: 0,
    minMarginLevelPercent: null,
    maxMarginRiskPercent: null,
    marginLiquidated: false,
    firstLiquidationDate: null,
    costRPerTrade,
    sampledCurveCount: sampledSeries.length,
    sampledEventPointCount: sampledSeries.reduce((sum, item) => sum + Math.max(0, item.eventPointCount ?? 0), 0),
    sampledReturnedPointCount: sampledSeries.reduce((sum, item) => sum + Math.max(0, item.pointCount ?? item.points.length), 0),
  };

  if (!times.length) {
    return { points: [], metrics };
  }

  const cursors = series.map(() => 0);
  const states = series.map(() => ({ equityR: 0, realizedR: 0, cumulativeTrades: 0, openTrades: 0, date: "" }));
  const points: PortfolioPoint[] = [];
  let maxEquityR = 0;
  let maxDrawdownR = 0;
  let balanceUsd = startingBalanceUsd;
  let realizedUsd = 0;
  let totalCostUsd = 0;
  let peakBalanceUsd = startingBalanceUsd;
  let minBalanceUsd = Number.POSITIVE_INFINITY;
  let maxDrawdownUsd = 0;
  let minLotForcedTrades = 0;
  let maxUsedMarginUsd = 0;
  let maxStopOutEquityUsd = 0;
  let maxGrossMarginLoadPercent = 0;
  let maxDepositLoadPercent = 0;
  let usedMarginAtMaxDepositLoadUsd = 0;
  let openTradesAtMaxDepositLoad = 0;
  let maxOpenTrades = 0;
  let minMarginLevelPercent = Number.POSITIVE_INFINITY;
  let maxMarginRiskPercent = 0;
  let marginLiquidated = false;
  let firstLiquidationDate: string | null = null;

  for (const time of times) {
    const previousStates = states.map((state) => ({ ...state }));
    series.forEach((seriesItem, seriesIndex) => {
      const pointsForSeries = seriesItem.points;
      while (
        cursors[seriesIndex] < pointsForSeries.length &&
        pointsForSeries[cursors[seriesIndex]].time <= time
      ) {
        const point = pointsForSeries[cursors[seriesIndex]];
        states[seriesIndex] = {
          equityR: point.equityR,
          realizedR: point.realizedR,
          cumulativeTrades: point.cumulativeTrades,
          openTrades: point.openTrades,
          date: point.date,
        };
        cursors[seriesIndex] += 1;
      }
    });

    const grossEquityR = states.reduce((sum, state) => sum + state.equityR, 0);
    const realizedR = states.reduce((sum, state) => sum + state.realizedR, 0);
    const cumulativeTrades = states.reduce((sum, state) => sum + state.cumulativeTrades, 0);
    const costR = cumulativeTrades * costRPerTrade;
    const equityR = round(grossEquityR - costR, 6);
    const netRealizedR = round(realizedR - costR, 6);
    maxEquityR = Math.max(maxEquityR, equityR);
    const drawdownR = round(Math.max(0, maxEquityR - equityR), 6);
    maxDrawdownR = Math.max(maxDrawdownR, drawdownR);

    const targetRiskDollars = account.riskBasis === "current"
      ? Math.max(0, balanceUsd) * riskPercent
      : riskDollars;
    let balanceDeltaUsd = 0;
    let realizedDeltaUsd = 0;
    let costDeltaUsd = 0;
    let usedMarginUsd = 0;
    let openTradeCount = 0;
    series.forEach((seriesItem, seriesIndex) => {
      const previousState = previousStates[seriesIndex];
      const currentState = states[seriesIndex];
      const previousNetEquityR = previousState.equityR - previousState.cumulativeTrades * costRPerTrade;
      const currentNetEquityR = currentState.equityR - currentState.cumulativeTrades * costRPerTrade;
      const previousNetRealizedR = previousState.realizedR - previousState.cumulativeTrades * costRPerTrade;
      const currentNetRealizedR = currentState.realizedR - currentState.cumulativeTrades * costRPerTrade;
      const deltaTrades = Math.max(0, currentState.cumulativeTrades - previousState.cumulativeTrades);
      const sizedRisk = sizeRiskDollars(targetRiskDollars, seriesItem.sizing, account);
      usedMarginUsd += currentState.openTrades * marginRequiredUsd(sizedRisk.lots, account);
      openTradeCount += currentState.openTrades;
      if (!marginLiquidated) {
        balanceDeltaUsd += (currentNetEquityR - previousNetEquityR) * sizedRisk.riskDollars;
        realizedDeltaUsd += (currentNetRealizedR - previousNetRealizedR) * sizedRisk.riskDollars;
        costDeltaUsd += deltaTrades * costRPerTrade * sizedRisk.riskDollars;
        if (deltaTrades > 0 && sizedRisk.forcedMinimumLot) {
          minLotForcedTrades += deltaTrades;
        }
      }
    });
    if (!marginLiquidated) {
      balanceUsd = round(balanceUsd + balanceDeltaUsd, 2);
      realizedUsd = round(realizedUsd + realizedDeltaUsd, 2);
      totalCostUsd = round(totalCostUsd + costDeltaUsd, 2);
    }
    const stopOutEquityUsd = round(usedMarginUsd * Math.max(0, account.stopOutLevelPercent) / 100, 2);
    const marginLoadBasisUsd = account.riskBasis === "current" ? balanceUsd : startingBalanceUsd;
    const grossMarginLoadPercent = marginLoadBasisUsd > 0
      ? round((usedMarginUsd / marginLoadBasisUsd) * 100, 2)
      : Number.POSITIVE_INFINITY;
    const depositLoadPercent = balanceUsd > 0 ? round((usedMarginUsd / balanceUsd) * 100, 2) : Number.POSITIVE_INFINITY;
    const marginLevelPercent = usedMarginUsd > 0 ? round((balanceUsd / usedMarginUsd) * 100, 2) : null;
    const stopOutHeadroomPercent = marginLevelPercent == null
      ? null
      : round(marginLevelPercent - Math.max(0, account.stopOutLevelPercent), 2);
    const marginCallHeadroomPercent = marginLevelPercent == null
      ? null
      : round(marginLevelPercent - Math.max(0, account.marginCallLevelPercent), 2);
    const marginRiskPercent = marginLevelPercent == null || account.stopOutLevelPercent <= 0
      ? 0
      : round(Math.min(100, Math.max(0, (account.stopOutLevelPercent / Math.max(marginLevelPercent, 0.000001)) * 100)), 2);
    if (!marginLiquidated && usedMarginUsd > 0 && balanceUsd <= stopOutEquityUsd) {
      marginLiquidated = true;
      firstLiquidationDate = states.find((state) => state.date)?.date || new Date(time * 1000).toISOString().slice(0, 10);
    }
    maxUsedMarginUsd = Math.max(maxUsedMarginUsd, usedMarginUsd);
    maxStopOutEquityUsd = Math.max(maxStopOutEquityUsd, stopOutEquityUsd);
    if (Number.isFinite(grossMarginLoadPercent)) {
      maxGrossMarginLoadPercent = Math.max(maxGrossMarginLoadPercent, grossMarginLoadPercent);
    }
    maxOpenTrades = Math.max(maxOpenTrades, openTradeCount);
    if (Number.isFinite(depositLoadPercent) && depositLoadPercent > maxDepositLoadPercent) {
      maxDepositLoadPercent = depositLoadPercent;
      usedMarginAtMaxDepositLoadUsd = usedMarginUsd;
      openTradesAtMaxDepositLoad = openTradeCount;
    }
    if (marginLevelPercent != null && Number.isFinite(marginLevelPercent)) {
      minMarginLevelPercent = Math.min(minMarginLevelPercent, marginLevelPercent);
    }
    maxMarginRiskPercent = Math.max(maxMarginRiskPercent, marginRiskPercent);
    peakBalanceUsd = Math.max(peakBalanceUsd, balanceUsd);
    minBalanceUsd = Math.min(minBalanceUsd, balanceUsd);
    const drawdownUsd = round(Math.max(0, peakBalanceUsd - balanceUsd), 2);
    maxDrawdownUsd = Math.max(maxDrawdownUsd, drawdownUsd);
    points.push({
      time,
      date: states.find((state) => state.date)?.date || new Date(time * 1000).toISOString().slice(0, 10),
      equity_r: equityR,
      drawdown_r: drawdownR,
      balance_usd: balanceUsd,
      drawdown_usd: drawdownUsd,
      used_margin_usd: round(usedMarginUsd, 2),
      stop_out_equity_usd: stopOutEquityUsd,
      deposit_load_pct: depositLoadPercent,
      margin_level_pct: marginLevelPercent,
      stop_out_headroom_pct: stopOutHeadroomPercent,
      margin_call_headroom_pct: marginCallHeadroomPercent,
      gross_margin_load_pct: grossMarginLoadPercent,
      margin_risk_pct: marginRiskPercent,
      realized_r: netRealizedR,
      closed_trade_count: cumulativeTrades,
      open_trade_count: openTradeCount,
    });
  }

  const finalPoint = points[points.length - 1];
  metrics.finalEquityR = finalPoint.equity_r;
  metrics.finalRealizedR = finalPoint.realized_r;
  metrics.maxDrawdownR = round(maxDrawdownR, 6);
  metrics.finalBalanceUsd = finalPoint.balance_usd;
  metrics.finalRealizedUsd = realizedUsd;
  metrics.minBalanceUsd = Number.isFinite(minBalanceUsd) ? minBalanceUsd : null;
  metrics.maxDrawdownUsd = round(maxDrawdownUsd, 2);
  metrics.totalTrades = finalPoint.closed_trade_count;
  metrics.tradesPerMonth = calculateTradesPerMonth(points);
  metrics.finalRiskDollars = round(Math.max(0, finalPoint.balance_usd) * riskPercent, 2);
  metrics.averageCostUsdPerTrade = metrics.totalTrades > 0 ? round(totalCostUsd / metrics.totalTrades, 2) : null;
  metrics.minLotForcedTrades = minLotForcedTrades;
  metrics.maxUsedMarginUsd = round(maxUsedMarginUsd, 2);
  metrics.maxStopOutEquityUsd = round(maxStopOutEquityUsd, 2);
  metrics.maxGrossMarginLoadPercent = maxGrossMarginLoadPercent > 0 ? round(maxGrossMarginLoadPercent, 2) : null;
  metrics.maxDepositLoadPercent = maxDepositLoadPercent > 0 ? round(maxDepositLoadPercent, 2) : null;
  metrics.usedMarginAtMaxDepositLoadUsd = maxDepositLoadPercent > 0 ? round(usedMarginAtMaxDepositLoadUsd, 2) : null;
  metrics.openTradesAtMaxDepositLoad = openTradesAtMaxDepositLoad;
  metrics.maxOpenTrades = maxOpenTrades;
  metrics.minMarginLevelPercent = Number.isFinite(minMarginLevelPercent) ? round(minMarginLevelPercent, 2) : null;
  metrics.maxMarginRiskPercent = maxMarginRiskPercent > 0 ? round(maxMarginRiskPercent, 2) : null;
  metrics.marginLiquidated = marginLiquidated;
  metrics.firstLiquidationDate = firstLiquidationDate;
  metrics.blown = marginLiquidated || points.some((point) => point.balance_usd <= 0);
  return { points, metrics };
}

function buildPortfolioSimilarity(
  details: AttemptDetail[],
  selectedRows: AttemptCatalogRow[],
  selectedCount: number,
): PortfolioSimilarity {
  const detailByAttemptId = new Map(details.map((detail) => [detail.attempt.attempt_id, detail]));
  const prepared = selectedRows
    .map((row) => prepareSimilarityRow(row, detailByAttemptId.get(row.attempt_id)))
    .filter((item): item is SimilarityPrepared => item !== null);

  const loadedCount = prepared.length;
  const pairLookup = new Map<string, SimilarityPair>();
  const pairs: SimilarityPair[] = [];
  const pairScores: number[] = [];
  let maxPair: SimilarityPair | null = null;

  for (let leftIndex = 0; leftIndex < prepared.length; leftIndex += 1) {
    const left = prepared[leftIndex];
    for (let rightIndex = leftIndex + 1; rightIndex < prepared.length; rightIndex += 1) {
      const right = prepared[rightIndex];
      const pair = scoreSimilarityPair(left, right);
      pairLookup.set(pairKey(left.attemptId, right.attemptId), pair);
      pairs.push(pair);
      pairScores.push(pair.similarityScore);
      if (!maxPair || pair.similarityScore > maxPair.similarityScore) {
        maxPair = pair;
      }
    }
  }

  const cells = prepared.map((left) =>
    prepared.map((right) => {
      const diagonal = left.attemptId === right.attemptId;
      const pair = diagonal ? null : pairLookup.get(pairKey(left.attemptId, right.attemptId)) ?? null;
      return {
        rowAttemptId: left.attemptId,
        columnAttemptId: right.attemptId,
        rowLabel: left.label,
        columnLabel: right.label,
        value: diagonal ? 1.0 : pair?.similarityScore ?? 0.0,
        pair,
        diagonal,
      };
    }),
  );

  return {
    selectedCount,
    loadedCount,
    averageSameness: pairScores.length ? pairScores.reduce((sum, value) => sum + value, 0) / pairScores.length : null,
    maxSameness: maxPair?.similarityScore ?? null,
    maxPair,
    pairs,
    cells,
  };
}

function buildPortfolioClusters(
  selectedRows: AttemptCatalogRow[],
  similarity: PortfolioSimilarity,
  threshold: number,
  nudgeOverlaps: boolean,
): PortfolioCluster[] {
  const members = selectedRows.map(clusterMemberFromRow);
  if (!members.length) {
    return [];
  }

  const memberByAttemptId = new Map(members.map((member) => [member.attemptId, member]));
  const adjacency = new Map(members.map((member) => [member.attemptId, new Set<string>()]));
  const pairLookup = new Map<string, SimilarityPair>();

  similarity.pairs.forEach((pair) => {
    pairLookup.set(pairKey(pair.leftAttemptId, pair.rightAttemptId), pair);
    if (
      pair.similarityScore >= threshold
      && memberByAttemptId.has(pair.leftAttemptId)
      && memberByAttemptId.has(pair.rightAttemptId)
    ) {
      adjacency.get(pair.leftAttemptId)?.add(pair.rightAttemptId);
      adjacency.get(pair.rightAttemptId)?.add(pair.leftAttemptId);
    }
  });

  const components: PortfolioClusterMember[][] = [];
  const visited = new Set<string>();
  members.forEach((member) => {
    if (visited.has(member.attemptId)) return;
    const queue = [member.attemptId];
    const component: PortfolioClusterMember[] = [];
    visited.add(member.attemptId);
    while (queue.length) {
      const attemptId = queue.shift();
      if (!attemptId) continue;
      const current = memberByAttemptId.get(attemptId);
      if (current) {
        component.push(current);
      }
      adjacency.get(attemptId)?.forEach((neighborId) => {
        if (!visited.has(neighborId)) {
          visited.add(neighborId);
          queue.push(neighborId);
        }
      });
    }
    components.push(component);
  });

  const scoreValues = members.map((member) => member.score).filter((value): value is number => value != null);
  const cadenceValues = members.map((member) => member.tradesPerMonth).filter((value): value is number => value != null);
  const rawClusters = components
    .map((component, index) => {
      const pairScores: number[] = [];
      const componentPairs: SimilarityPair[] = [];
      const linkedPairs: SimilarityPair[] = [];
      for (let leftIndex = 0; leftIndex < component.length; leftIndex += 1) {
        for (let rightIndex = leftIndex + 1; rightIndex < component.length; rightIndex += 1) {
          const left = component[leftIndex];
          const right = component[rightIndex];
          const pair = pairLookup.get(pairKey(left.attemptId, right.attemptId));
          if (pair) {
            componentPairs.push(pair);
            pairScores.push(pair.similarityScore);
            if (pair.similarityScore >= threshold) {
              linkedPairs.push(pair);
            }
          }
        }
      }
      const strongestPair = [...(linkedPairs.length ? linkedPairs : componentPairs)].sort(
        (left, right) => right.similarityScore - left.similarityScore,
      )[0] ?? null;
      const averageScore = averageNullable(component.map((member) => member.score));
      const tradesPerMonth = averageNullable(component.map((member) => member.tradesPerMonth));
      const maxDrawdownValues = component
        .map((member) => member.maxDrawdownR)
        .filter((value): value is number => value != null);
      return {
        id: component.map((member) => member.attemptId).sort().join("|"),
        label: "",
        members: component.sort((left, right) => Number(right.score ?? -Infinity) - Number(left.score ?? -Infinity)),
        x: metricPosition(tradesPerMonth, cadenceValues, true),
        y: 100 - metricPosition(averageScore, scoreValues, false),
        size: component.length === 1
          ? 56
          : Math.min(128, Math.max(78, 58 + Math.sqrt(component.length) * 30)),
        hue: 78 + ((index * 43) % 250),
        averageScore,
        tradesPerMonth,
        maxDrawdownR: maxDrawdownValues.length ? Math.max(...maxDrawdownValues) : null,
        maxSameness: pairScores.length ? Math.max(...pairScores) : null,
        pairCount: pairScores.length,
        strongestPair,
      } satisfies PortfolioCluster;
    })
    .sort((left, right) => {
      const sizeDelta = right.members.length - left.members.length;
      if (sizeDelta) return sizeDelta;
      return Number(right.averageScore ?? -Infinity) - Number(left.averageScore ?? -Infinity);
    })
    .map((cluster, index) => ({
      ...cluster,
      label: `Cluster ${String(index + 1).padStart(2, "0")}`,
      hue: 78 + ((index * 43) % 250),
    }));

  return nudgeOverlaps ? spreadClusterPositions(rawClusters) : rawClusters;
}

function summarizeClusterThreshold(
  similarity: PortfolioSimilarity,
  threshold: number,
): ClusterThresholdStats {
  const included = similarity.pairs
    .map((pair) => pair.similarityScore)
    .filter((score) => score >= threshold)
    .sort((left, right) => left - right);
  const excluded = similarity.pairs
    .map((pair) => pair.similarityScore)
    .filter((score) => score < threshold)
    .sort((left, right) => right - left);
  return {
    linkedPairCount: included.length,
    nearestIncluded: included[0] ?? null,
    nearestExcluded: excluded[0] ?? null,
  };
}

function clusterMemberFromRow(row: AttemptCatalogRow): PortfolioClusterMember {
  return {
    row,
    attemptId: row.attempt_id,
    label: compactAttemptLabel(row),
    score: nullableNumber(row.score_36m),
    tradesPerMonth: nullableNumber(row.trades_per_month_36m),
    maxDrawdownR: nullableNumber(row.max_drawdown_r_36m),
  };
}

function buildAssetExposureRows(rows: AttemptCatalogRow[]): AssetExposureRow[] {
  const exposureByInstrument = new Map<string, { tradesPerMonth: number; attemptIds: Set<string> }>();
  rows.forEach((row) => {
    const instruments = normalizeTokens(row.instruments_36m ?? []);
    if (!instruments.length) {
      return;
    }
    const tradesPerMonth = Math.max(0, nullableNumber(row.trades_per_month_36m) ?? 0);
    const contribution = tradesPerMonth / instruments.length;
    instruments.forEach((instrument) => {
      if (!exposureByInstrument.has(instrument)) {
        exposureByInstrument.set(instrument, { tradesPerMonth: 0, attemptIds: new Set() });
      }
      const bucket = exposureByInstrument.get(instrument);
      if (!bucket) return;
      bucket.tradesPerMonth += contribution;
      bucket.attemptIds.add(row.attempt_id);
    });
  });

  const totalTradesPerMonth = [...exposureByInstrument.values()].reduce(
    (sum, bucket) => sum + bucket.tradesPerMonth,
    0,
  );

  return [...exposureByInstrument.entries()]
    .map(([instrument, bucket]) => ({
      instrument,
      tradesPerMonth: bucket.tradesPerMonth,
      share: totalTradesPerMonth > 0 ? bucket.tradesPerMonth / totalTradesPerMonth : 0,
      strategyCount: bucket.attemptIds.size,
    }))
    .sort((left, right) => right.tradesPerMonth - left.tradesPerMonth || left.instrument.localeCompare(right.instrument));
}

function averageNullable(values: Array<number | null>) {
  const finiteValues = values.filter((value): value is number => value != null && Number.isFinite(value));
  if (!finiteValues.length) return null;
  return finiteValues.reduce((sum, value) => sum + value, 0) / finiteValues.length;
}

function metricPosition(value: number | null, domain: number[], logScale: boolean) {
  const finiteDomain = domain.filter((item) => Number.isFinite(item));
  if (value == null || !Number.isFinite(value) || !finiteDomain.length) {
    return 50;
  }
  const transform = (item: number) => (logScale ? Math.log1p(Math.max(0, item)) : item);
  const transformedValue = transform(value);
  const transformedDomain = finiteDomain.map(transform);
  const minValue = Math.min(...transformedDomain);
  const maxValue = Math.max(...transformedDomain);
  if (Math.abs(maxValue - minValue) < 1e-9) {
    return 50;
  }
  return 12 + ((transformedValue - minValue) / (maxValue - minValue)) * 76;
}

function spreadClusterPositions(clusters: PortfolioCluster[]) {
  const placed: PortfolioCluster[] = [];
  return clusters.map((cluster, index) => {
    let x = cluster.x;
    let y = cluster.y;
    const clusterMinDistance = 11 + Math.min(12, Math.sqrt(cluster.members.length) * 4);
    for (let attempt = 0; attempt < 80; attempt += 1) {
      const collision = placed.find((other) => {
        const otherMinDistance = 9 + Math.min(10, Math.sqrt(other.members.length) * 3);
        return percentDistance(x, y, other.x, other.y) < clusterMinDistance + otherMinDistance;
      });
      if (!collision) break;
      const angle = ((index * 137.5 + attempt * 37) * Math.PI) / 180;
      const push = 3.5 + attempt * 0.32;
      x = Math.max(8, Math.min(92, x + Math.cos(angle) * push));
      y = Math.max(8, Math.min(92, y + Math.sin(angle) * push));
    }
    const next = { ...cluster, x, y };
    placed.push(next);
    return next;
  });
}

function percentDistance(leftX: number, leftY: number, rightX: number, rightY: number) {
  return Math.sqrt((leftX - rightX) ** 2 + (leftY - rightY) ** 2);
}

function prepareSimilarityRow(row: AttemptCatalogRow, detail: AttemptDetail | undefined): SimilarityPrepared | null {
  const curveFeatures = buildBehavioralCurveFeatures(detail?.full_backtest_curve ?? null);
  if (!curveFeatures.dailyChanges.size) {
    return null;
  }
  const instruments = normalizeTokens(row.instruments_36m ?? []);
  return {
    row,
    attemptId: row.attempt_id,
    label: compactAttemptLabel(row),
    dailyChanges: curveFeatures.dailyChanges,
    activeDates: curveFeatures.activeDates,
    drawdownDates: curveFeatures.drawdownDates,
    instruments,
    instrumentSet: new Set(instruments),
    timeframe: String(row.timeframe_36m || "").trim().toUpperCase(),
    strategyKey: String(row.strategy_key_36m || "").trim(),
    tradesPerMonth: nullableNumber(row.trades_per_month_36m),
    maxDrawdownR: nullableNumber(row.max_drawdown_r_36m),
  };
}

function scoreSimilarityPair(
  left: SimilarityPrepared,
  right: SimilarityPrepared,
): SimilarityPair {
  const commonDates = [...left.dailyChanges.keys()].filter((date) => right.dailyChanges.has(date)).sort();
  const leftValues = commonDates.map((date) => left.dailyChanges.get(date) ?? 0);
  const rightValues = commonDates.map((date) => right.dailyChanges.get(date) ?? 0);
  const correlation = commonDates.length >= 5 ? pearsonCorrelation(leftValues, rightValues) : null;
  const positiveCorrelation = Math.max(0, correlation ?? 0);
  const activeOverlapRatio = jaccardSimilarity(left.activeDates, right.activeDates);
  const drawdownOverlapRatio = jaccardSimilarity(left.drawdownDates, right.drawdownDates);
  const similarityScore = commonDates.length < 5
    ? 0
    : clamp01(
        positiveCorrelation * 0.60
        + activeOverlapRatio * 0.25
        + drawdownOverlapRatio * 0.15,
      );

  return {
    leftAttemptId: left.attemptId,
    rightAttemptId: right.attemptId,
    leftLabel: left.label,
    rightLabel: right.label,
    similarityScore,
    correlation,
    positiveCorrelation,
    activeOverlapRatio,
    drawdownOverlapRatio,
    overlapDays: commonDates.length,
  };
}

function PortfolioTooltip({
  active,
  enabled = true,
  payload,
  mode,
}: {
  active?: boolean;
  enabled?: boolean;
  payload?: Array<{ payload?: PortfolioPoint }>;
  mode: PortfolioChartMode;
}) {
  if (!enabled || !active || !payload?.length) {
    return null;
  }
  const point = payload.find((item) => item.payload)?.payload;
  if (!point) {
    return null;
  }

  return (
    <div className="min-w-52 rounded-lg border border-border/70 bg-popover px-3 py-2 text-xs text-popover-foreground shadow-xl shadow-black/30">
      <div className="mb-2 font-medium">{formatTooltipDate(point.time)}</div>
      <div className="grid gap-1.5">
        {mode === "equity" ? (
          <>
            <TooltipMetric label="Equity" value={`${formatSignedR(point.equity_r)} / ${formatCurrency(point.balance_usd)}`} />
            <TooltipMetric label="Drawdown" value={`${formatNumber(point.drawdown_r, 2)}R / ${formatCurrency(point.drawdown_usd)}`} />
            <TooltipMetric label="Realized" value={formatSignedR(point.realized_r)} />
          </>
        ) : mode === "margin" ? (
          <>
            <TooltipMetric label="Gross load" value={formatPercentDecimal(point.gross_margin_load_pct)} />
            <TooltipMetric label="Stop pressure" value={formatPercentDecimal(point.margin_risk_pct)} />
            <TooltipMetric label="Margin level" value={formatPercentDecimal(point.margin_level_pct)} />
            <TooltipMetric label="Equity load" value={formatPercentDecimal(point.deposit_load_pct)} />
            <TooltipMetric label="Used margin" value={formatCurrency(point.used_margin_usd)} />
            <TooltipMetric label="Open trades" value={formatInt(point.open_trade_count)} />
          </>
        ) : (
          <>
            <TooltipMetric label="Drawdown" value={`${formatNumber(point.drawdown_r, 2)}R / ${formatCurrency(point.drawdown_usd)}`} />
            <TooltipMetric label="Equity" value={`${formatSignedR(point.equity_r)} / ${formatCurrency(point.balance_usd)}`} />
            <TooltipMetric label="Open trades" value={formatInt(point.open_trade_count)} />
          </>
        )}
      </div>
    </div>
  );
}

function TooltipMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="text-muted-foreground">{label}</span>
      <span className="text-right font-medium tabular-nums text-foreground">{value}</span>
    </div>
  );
}

function curveSamplingInfo(
  payload: Record<string, unknown> | null,
  normalizedPointCount: number,
): Pick<NormalizedSeries, "downsampled" | "eventPointCount" | "pointCount"> {
  const curve = (payload as { curve?: Record<string, unknown> } | null)?.curve ?? {};
  const pointCount = nullableNumber(curve.point_count) ?? normalizedPointCount;
  const eventPointCount = nullableNumber(
    curve.event_point_count
      ?? curve.source_point_count
      ?? curve.original_point_count,
  );
  const downsampled = Boolean(curve.downsampled)
    || (eventPointCount != null && pointCount > 0 && eventPointCount > pointCount);
  return {
    downsampled,
    eventPointCount,
    pointCount,
  };
}

function normalizeCurvePoints(payload: Record<string, unknown> | null): NormalizedPoint[] {
  const rawPoints = (payload as { curve?: { points?: unknown[] }; points?: unknown[] } | null)?.curve?.points
    ?? (payload as { points?: unknown[] } | null)?.points
    ?? [];
  let cumulativeTrades = 0;
  return rawPoints
    .map((raw) => {
      const point = raw as Record<string, unknown>;
      const time = toNumber(point.time, NaN);
      if (!Number.isFinite(time)) return null;
      cumulativeTrades += Math.max(0, Math.round(toNumber(point.closed_trade_count, 0)));
      const equityR = toNumber(point.equity_r, 0);
      return {
        time,
        date: String(point.date || new Date(time * 1000).toISOString().slice(0, 10)),
        equityR,
        realizedR: toNumber(point.cumulative_realized_r, equityR),
        cumulativeTrades,
        openTrades: Math.max(0, Math.round(toNumber(point.open_trade_count, 0))),
      };
    })
    .filter((point): point is NormalizedPoint => point !== null)
    .sort((a, b) => a.time - b.time);
}

function Panel({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={`rounded-lg border border-border/60 bg-card/80 shadow-xl shadow-black/15 ${className ?? ""}`}>{children}</div>;
}

function ModeButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={`inline-flex h-8 items-center justify-center gap-1.5 rounded-md px-3 text-xs font-medium transition ${
        active
          ? "bg-amber-300/18 text-amber-100"
          : "text-muted-foreground hover:bg-background/50 hover:text-foreground"
      }`}
    >
      {children}
    </button>
  );
}

function AutoBuildPanel({
  configText,
  setConfigText,
  configError,
  job,
  onFinalize,
  onBuild,
  onCancel,
  onImport,
  isStartingFinalize,
  isStartingBuild,
  isCanceling,
}: {
  configText: string;
  setConfigText: (value: string) => void;
  configError: string | null;
  job?: DashboardJob;
  onFinalize: () => void;
  onBuild: () => void;
  onCancel: () => void;
  onImport: () => void;
  isStartingFinalize: boolean;
  isStartingBuild: boolean;
  isCanceling: boolean;
}) {
  const isRunning = job?.status === "running" || job?.status === "canceling";
  return (
    <Panel className="p-4">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
        <div className="space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
                <Bot className="h-3.5 w-3.5" />
                Auto build
              </div>
              <div className="mt-2 text-sm text-muted-foreground">
                Status: <span className="font-medium text-foreground">{job?.status ?? "idle"}</span>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="outline"
                className="rounded-lg"
                disabled={isRunning || isStartingFinalize}
                onClick={onFinalize}
              >
                <Play className="h-4 w-4" />
                Finalize
              </Button>
              <Button
                type="button"
                className="rounded-lg"
                disabled={isRunning || isStartingBuild}
                onClick={onBuild}
              >
                <Play className="h-4 w-4" />
                Build
              </Button>
              <Button
                type="button"
                variant="outline"
                className="rounded-lg"
                disabled={!isRunning || isCanceling}
                onClick={onCancel}
              >
                <StopCircle className="h-4 w-4" />
                Cancel
              </Button>
            </div>
          </div>
          <label className="block">
            <span className="mb-2 block text-xs uppercase tracking-[0.14em] text-muted-foreground">
              Portfolio config
            </span>
            <textarea
              value={configText}
              onChange={(event) => setConfigText(event.target.value)}
              spellCheck={false}
              className="min-h-80 w-full resize-y rounded-lg border border-border/60 bg-background/45 p-3 font-mono text-xs leading-5 outline-none focus:ring-2 focus:ring-ring/25"
            />
          </label>
          {configError ? (
            <div className="rounded-lg border border-rose-300/40 bg-rose-300/10 px-3 py-2 text-sm text-rose-100">
              {configError}
            </div>
          ) : null}
          <Button type="button" variant="secondary" className="rounded-lg" onClick={onImport}>
            <Plus className="h-4 w-4" />
            Import auto selection
          </Button>
        </div>
        <div className="space-y-3">
          <div className="grid grid-cols-3 gap-2">
            <TinyStat label="Job" value={job?.kind ?? "-"} />
            <TinyStat label="Return" value={job?.returncode == null ? "-" : String(job.returncode)} />
            <TinyStat label="Started" value={formatDateTime(job?.started_at ?? null)} />
          </div>
          <pre className="min-h-96 max-h-[38rem] overflow-auto rounded-lg border border-border/60 bg-black/35 p-3 font-mono text-xs leading-5 text-muted-foreground">
            {job?.log_tail?.trim() || "No job log yet."}
          </pre>
        </div>
      </div>
    </Panel>
  );
}

function WorkbenchSummaryStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-20 rounded-lg border border-border/60 bg-background/35 px-3 py-2">
      <div className="text-[0.62rem] uppercase tracking-[0.12em] text-muted-foreground">{label}</div>
      <div className="mt-1 truncate text-lg font-semibold leading-none tabular-nums tracking-tight">{value}</div>
    </div>
  );
}

function PortfolioMetric({
  label,
  primary,
  secondary,
}: {
  label: string;
  primary: string;
  secondary: string;
}) {
  return (
    <div className="rounded-lg border border-border/60 bg-background/35 p-3">
      <div className="text-[0.68rem] uppercase tracking-[0.16em] text-muted-foreground">{label}</div>
      <div className="mt-2 truncate text-xl font-semibold tracking-tight tabular-nums">{primary}</div>
      <div className="mt-1 truncate text-xs font-medium tabular-nums text-muted-foreground">{secondary}</div>
    </div>
  );
}

function CompactMetric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-[0.62rem] uppercase tracking-[0.14em] text-muted-foreground">{label}</div>
      <div className="mt-1 text-sm font-semibold tabular-nums">{value}</div>
    </div>
  );
}

function TinyStat({ label, value, title }: { label: string; value: string; title?: string }) {
  return (
    <div className="rounded-md border border-border/50 bg-background/35 p-2" title={title}>
      <div className="text-[0.62rem] uppercase tracking-[0.14em] text-muted-foreground">{label}</div>
      <div className="mt-1 truncate font-medium">{value}</div>
    </div>
  );
}

function RunSortButton({
  active,
  label,
  onClick,
  children,
}: {
  active: boolean;
  label: string;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label={label}
          aria-pressed={active}
          onClick={onClick}
          className={`flex h-7 w-7 items-center justify-center rounded-md border transition ${
            active
              ? "border-amber-300/60 bg-amber-300/12 text-amber-100"
              : "border-border/55 bg-background/25 text-muted-foreground hover:border-border hover:text-foreground"
          }`}
        >
          {children}
        </button>
      </TooltipTrigger>
      <TooltipContent side="top">{label}</TooltipContent>
    </Tooltip>
  );
}

function AccountInput({
  label,
  value,
  prefix,
  suffix,
  step,
  onChange,
}: {
  label: string;
  value: number;
  prefix?: string;
  suffix?: string;
  step: number;
  onChange: (value: number) => void;
}) {
  return (
    <label className="flex min-w-0 items-center justify-between gap-3 rounded-lg border border-border/50 bg-background/28 px-3 py-2 text-xs text-muted-foreground">
      <span className="min-w-0 truncate">{label}</span>
      <div className="flex h-9 w-36 shrink-0 items-center rounded-md border border-border/60 bg-input/35 px-2 text-foreground focus-within:ring-2 focus-within:ring-ring/25">
        {prefix ? <span className="shrink-0 text-muted-foreground">{prefix}</span> : null}
        <input
          type="number"
          value={value}
          step={step}
          min={0}
          onChange={(event) => onChange(toNumber(event.target.value, 0))}
          className="min-w-0 flex-1 bg-transparent px-1 text-right text-sm tabular-nums outline-none"
        />
        {suffix ? <span className="shrink-0 text-muted-foreground">{suffix}</span> : null}
      </div>
    </label>
  );
}

function AccountRiskBasisControl({
  value,
  onChange,
}: {
  value: AccountConfig["riskBasis"];
  onChange: (value: AccountConfig["riskBasis"]) => void;
}) {
  return (
    <div className="rounded-lg border border-border/50 bg-background/28 px-3 py-2">
      <div className="mb-2 text-xs text-muted-foreground">Risk sizing</div>
      <div className="grid grid-cols-2 gap-1 rounded-md border border-border/60 bg-input/25 p-1">
        <button
          type="button"
          onClick={() => onChange("initial")}
          className={`h-8 rounded-[5px] px-2 text-xs font-medium transition ${
            value === "initial"
              ? "bg-amber-300/18 text-amber-100"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          Fixed
        </button>
        <button
          type="button"
          onClick={() => onChange("current")}
          className={`h-8 rounded-[5px] px-2 text-xs font-medium transition ${
            value === "current"
              ? "bg-amber-300/18 text-amber-100"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          Compound
        </button>
      </div>
    </div>
  );
}

function formatCurrency(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: value >= 1000 ? 0 : 2,
  }).format(value);
}

function formatCurrencyTick(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatSignedR(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return "-";
  return `${value >= 0 ? "+" : "-"}${formatNumber(Math.abs(value), 2)}R`;
}

function formatRewardMultiple(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return "-";
  return `${formatNumber(value, Number.isInteger(value) ? 0 : 1)}R`;
}

function formatSetupTooltip(attempt: AttemptCatalogRow) {
  const reward = formatRewardMultiple(attempt.reward_multiple_36m);
  if (reward === "-") {
    return undefined;
  }
  const stopLoss = nullableNumber(attempt.selected_stop_loss_percent_36m);
  const takeProfit = nullableNumber(attempt.selected_take_profit_percent_36m);
  const basisLabels: Record<string, string> = {
    recommended_cell: "raw recommended cell",
    best_cell: "profile-drop best cell",
    robust_cell: "profile-drop robust cell",
    artifact_cell: "profile-drop artifact cell",
    profile_drop_exit_policy: "profile-drop exit policy",
    curve_cell: "curve cell",
  };
  const basis = attempt.reward_multiple_basis_36m
    ? basisLabels[attempt.reward_multiple_basis_36m] ?? attempt.reward_multiple_basis_36m
    : "curve cell";
  const parts = [`RR ${reward}`, basis];
  if (stopLoss != null) {
    parts.push(`SL ${formatNumber(stopLoss, 2)}%`);
  }
  if (takeProfit != null) {
    parts.push(`TP ${formatNumber(takeProfit, 2)}%`);
  }
  return parts.join(" / ");
}

function formatPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return "-";
  return `${formatNumber(value * 100, value > 0 && value < 0.1 ? 1 : 0)}%`;
}

function formatPercentDecimal(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return "-";
  if (!Number.isFinite(value)) return "inf";
  return `${formatNumber(value, value > 0 && value < 10 ? 1 : 0)}%`;
}

function normalizedMarginCallRiskPercent(account: AccountConfig) {
  const marginCallLevel = Math.max(0, account.marginCallLevelPercent);
  const stopOutLevel = Math.max(0, account.stopOutLevelPercent);
  if (marginCallLevel <= 0) return 100;
  return Math.min(100, Math.max(0, (stopOutLevel / marginCallLevel) * 100));
}

function normalizeSelectedIds(value: string[]) {
  const seen = new Set<string>();
  const normalized: string[] = [];
  value.forEach((item) => {
    const attemptId = String(item || "").trim();
    if (!attemptId || seen.has(attemptId)) {
      return;
    }
    seen.add(attemptId);
    normalized.push(attemptId);
  });
  return normalized;
}

function stableSelectionKey(value: string[]) {
  return normalizeSelectedIds(value).join("\n");
}

function calculateTradesPerMonth(points: PortfolioPoint[]) {
  if (!points.length) {
    return null;
  }
  const first = points[0];
  const last = points[points.length - 1];
  const elapsedDays = Math.max(1, (last.time - first.time) / 86_400);
  const elapsedMonths = Math.max(1, elapsedDays / 30.4375);
  return last.closed_trade_count / elapsedMonths;
}

function buildLotSizing(attempt: AttemptCatalogRow, account: AccountConfig): LotSizing {
  const stopLossPercent = nullableNumber(attempt.selected_stop_loss_percent_36m);
  const instrument = normalizeTokens(attempt.instruments_36m ?? [])[0] ?? null;
  const notionalUsdPerLot = Math.max(0, account.notionalUsdPerLot);
  return {
    instrument,
    stopLossPercent,
    riskPerLotDollars:
      stopLossPercent != null && stopLossPercent > 0 && notionalUsdPerLot > 0
        ? notionalUsdPerLot * (stopLossPercent / 100)
        : null,
  };
}

function sizeRiskDollars(targetRiskDollars: number, sizing: LotSizing, account: AccountConfig) {
  const riskPerLotDollars = sizing.riskPerLotDollars;
  const minLot = Math.max(0, account.minLot);
  const lotStep = Math.max(0.0001, account.lotStep);
  if (targetRiskDollars <= 0 || riskPerLotDollars == null || riskPerLotDollars <= 0) {
    return {
      riskDollars: Math.max(0, targetRiskDollars),
      lots: 0,
      forcedMinimumLot: false,
    };
  }

  const rawLots = targetRiskDollars / riskPerLotDollars;
  const roundedLots = Math.floor(rawLots / lotStep) * lotStep;
  const forcedMinimumLot = minLot > 0 && roundedLots < minLot;
  const lots = forcedMinimumLot ? minLot : roundedLots;
  return {
    riskDollars: round(lots * riskPerLotDollars, 6),
    lots: round(lots, 4),
    forcedMinimumLot,
  };
}

function marginRequiredUsd(lots: number, account: AccountConfig) {
  const leverage = Math.max(1, account.leverage);
  return Math.max(0, lots) * Math.max(0, account.notionalUsdPerLot) / leverage;
}

function buildBehavioralCurveFeatures(payload: Record<string, unknown> | null) {
  const rawPoints = (payload as { curve?: { points?: unknown[] }; points?: unknown[] } | null)?.curve?.points
    ?? (payload as { points?: unknown[] } | null)?.points
    ?? [];

  const dated = new Map<string, { value: number; realized: number | null; drawdown: number; closedTrades: number | null }>();
  rawPoints.forEach((raw) => {
    const point = raw as Record<string, unknown>;
    const dateKey = curveDateKey(point);
    if (!dateKey) {
      return;
    }
    const value = curveValue(point);
    if (value == null) {
      return;
    }
    dated.set(dateKey, {
      value,
      realized: curveRealizedValue(point),
      drawdown: nullableNumber(point.drawdown_r) ?? 0,
      closedTrades: nullableInteger(point.closed_trade_count),
    });
  });

  const orderedDates = [...dated.keys()].sort();
  const maxDrawdown = orderedDates.reduce((maxValue, date) => Math.max(maxValue, dated.get(date)?.drawdown ?? 0), 0);
  const drawdownThreshold = Math.max(0.25, maxDrawdown * 0.25);
  const dailyChanges = new Map<string, number>();
  const activeDates = new Set<string>();
  const drawdownDates = new Set<string>();
  let previousValue: number | null = null;
  let previousRealized: number | null = null;
  let previousClosedTrades: number | null = null;

  orderedDates.forEach((date) => {
    const item = dated.get(date);
    if (!item) return;
    const delta = previousValue == null ? 0 : item.value - previousValue;
    const realizedDelta = previousRealized == null || item.realized == null ? null : item.realized - previousRealized;
    dailyChanges.set(date, delta);
    if (
      Math.abs(delta) > 1e-9
      || (realizedDelta != null && Math.abs(realizedDelta) > 1e-9)
      || (previousClosedTrades != null && item.closedTrades != null && item.closedTrades !== previousClosedTrades)
    ) {
      activeDates.add(date);
    }
    if (maxDrawdown > 0 && item.drawdown >= drawdownThreshold) {
      drawdownDates.add(date);
    }
    previousValue = item.value;
    previousRealized = item.realized;
    previousClosedTrades = item.closedTrades;
  });

  return { dailyChanges, activeDates, drawdownDates };
}

function pearsonCorrelation(left: number[], right: number[]) {
  if (left.length !== right.length || left.length < 3) {
    return null;
  }
  const leftMean = left.reduce((sum, value) => sum + value, 0) / left.length;
  const rightMean = right.reduce((sum, value) => sum + value, 0) / right.length;
  const leftVariance = left.reduce((sum, value) => sum + (value - leftMean) ** 2, 0);
  const rightVariance = right.reduce((sum, value) => sum + (value - rightMean) ** 2, 0);
  if (leftVariance <= 0 || rightVariance <= 0) {
    return null;
  }
  const covariance = left.reduce((sum, value, index) => sum + (value - leftMean) * (right[index] - rightMean), 0);
  return covariance / Math.sqrt(leftVariance * rightVariance);
}

function jaccardSimilarity(left: Set<string>, right: Set<string>) {
  const union = new Set([...left, ...right]);
  if (!union.size) {
    return 0;
  }
  const intersectionCount = [...left].filter((item) => right.has(item)).length;
  return intersectionCount / union.size;
}

function curveDateKey(point: Record<string, unknown>) {
  const rawDate = String(point.date || "").trim();
  if (rawDate) {
    return rawDate.slice(0, 10);
  }
  const timestamp = nullableNumber(point.time);
  if (timestamp == null) {
    return "";
  }
  return new Date(timestamp * 1000).toISOString().slice(0, 10);
}

function curveValue(point: Record<string, unknown>) {
  return firstNullableNumber(point.equity_r, point.realized_r, point.cumulative_realized_r);
}

function curveRealizedValue(point: Record<string, unknown>) {
  return firstNullableNumber(point.realized_r, point.cumulative_realized_r, point.equity_r);
}

function firstNullableNumber(...values: unknown[]) {
  for (const value of values) {
    const numeric = nullableNumber(value);
    if (numeric != null) {
      return numeric;
    }
  }
  return null;
}

function nullableInteger(value: unknown) {
  const numeric = nullableNumber(value);
  return numeric == null ? null : Math.round(numeric);
}

function normalizeTokens(values: unknown[]) {
  return values
    .map((value) => String(value || "").trim().toUpperCase())
    .filter(Boolean);
}

function nullableNumber(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function compactAttemptLabel(row: AttemptCatalogRow) {
  const name = String(row.candidate_name || row.strategy_key_36m || row.attempt_id || "").trim();
  const instrument = normalizeTokens(row.instruments_36m ?? [])[0];
  if (instrument && name && !name.toUpperCase().includes(instrument)) {
    return `${name} ${instrument}`;
  }
  return name || row.attempt_id;
}

function pairKey(left: string, right: string) {
  return [left, right].sort().join("\n");
}

function similarityHeatColor(value: number) {
  const clamped = clamp01(value);
  const lightness = 0.26 + clamped * 0.66;
  const chroma = 0.14 + clamped * 0.04;
  const hue = 260 - clamped * 165;
  return `oklch(${lightness.toFixed(3)} ${chroma.toFixed(3)} ${hue.toFixed(1)})`;
}

function sortRuns(runs: RunSummary[], mode: RunSortMode) {
  return [...runs].sort((a, b) => {
    const recentDelta = runTimestamp(b) - runTimestamp(a);
    if (mode === "score") {
      const scoreDelta = runScore(b) - runScore(a);
      return scoreDelta || recentDelta || a.run_id.localeCompare(b.run_id);
    }
    return recentDelta || runScore(b) - runScore(a) || a.run_id.localeCompare(b.run_id);
  });
}

function runTimestamp(run: RunSummary) {
  const raw = run.latest_created_at || run.created_at;
  const timestamp = raw ? Date.parse(raw) : NaN;
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function runScore(run: RunSummary) {
  const score = Number(run.best_attempt?.score_36m);
  return Number.isFinite(score) ? score : Number.NEGATIVE_INFINITY;
}

function isCanonicalPlayHandAttempt(row: AttemptCatalogRow) {
  if (row.is_canonical_attempt || row.is_canonical_playhand_attempt) return true;
  const attemptId = String(row.attempt_id || "").trim();
  const canonicalAttemptId = String(row.canonical_attempt_id || "").trim();
  return Boolean(attemptId && canonicalAttemptId && attemptId === canonicalAttemptId);
}

function compareDashboardAttemptScore(left: AttemptCatalogRow, right: AttemptCatalogRow) {
  const leftScore36 = Number(left.score_36m);
  const rightScore36 = Number(right.score_36m);
  const leftComposite = Number(left.composite_score);
  const rightComposite = Number(right.composite_score);
  const leftHasScore36 = Number.isFinite(leftScore36);
  const rightHasScore36 = Number.isFinite(rightScore36);
  if (leftHasScore36 !== rightHasScore36) return leftHasScore36 ? -1 : 1;
  if (leftHasScore36 && rightHasScore36 && leftScore36 !== rightScore36) {
    return rightScore36 - leftScore36;
  }
  const leftSecondary = Number.isFinite(leftComposite) ? leftComposite : -Infinity;
  const rightSecondary = Number.isFinite(rightComposite) ? rightComposite : -Infinity;
  if (leftSecondary !== rightSecondary) return rightSecondary - leftSecondary;
  return String(left.attempt_id || "").localeCompare(String(right.attempt_id || ""));
}

function buildDashboardPreferredAttemptMap(rows: AttemptCatalogRow[]) {
  const byRun = new Map<string, AttemptCatalogRow[]>();
  rows.forEach((row) => {
    const runId = String(row.run_id || "").trim();
    if (!runId) return;
    if (!byRun.has(runId)) {
      byRun.set(runId, []);
    }
    byRun.get(runId)?.push(row);
  });
  const preferredByRun = new Map<string, string>();
  byRun.forEach((group, runId) => {
    const canonicalRows = group.filter(isCanonicalPlayHandAttempt);
    const candidates = canonicalRows.length > 0 ? canonicalRows : group;
    const preferred = [...candidates].sort(compareDashboardAttemptScore)[0];
    const attemptId = String(preferred?.attempt_id || "").trim();
    if (attemptId) preferredByRun.set(runId, attemptId);
  });
  return preferredByRun;
}

function isWorkbenchCandidate(
  row: AttemptCatalogRow,
  {
    candidateScope,
    minScore,
    needle,
    preferredByRun,
    validOnly,
  }: {
    candidateScope: CandidateScope;
    minScore: number;
    needle: string;
    preferredByRun: Map<string, string>;
    validOnly: boolean;
  },
) {
  if (candidateScope !== "all" && !isInPromotedCandidateScope(row, preferredByRun)) return false;
  if (validOnly && row.full_backtest_validation_status_36m !== "valid") return false;
  if (Number(row.score_36m ?? -Infinity) < minScore) return false;
  if (!needle) return true;
  return [
    row.candidate_name,
    row.attempt_id,
    row.run_id,
    row.strategy_key_36m,
    row.timeframe_36m,
    ...(row.instruments_36m ?? []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
    .includes(needle);
}

function isInPromotedCandidateScope(row: AttemptCatalogRow, preferredByRun: Map<string, string>) {
  const runId = String(row.run_id || "").trim();
  const preferredAttemptId = preferredByRun.get(runId);
  if (!preferredAttemptId) return true;
  return preferredAttemptId === String(row.attempt_id || "").trim();
}

function formatTickDate(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "";
  return new Intl.DateTimeFormat("en-US", { month: "short", year: "2-digit" }).format(new Date(numeric * 1000));
}

function formatTooltipDate(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(new Date(numeric * 1000));
}

function toNumber(value: unknown, fallback: number) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function clamp01(value: number) {
  return Math.max(0, Math.min(1, value));
}

function round(value: number, digits: number) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}
