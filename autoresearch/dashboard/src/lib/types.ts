/* Types matching the Python backend API payloads */

export interface Overview {
  generatedAt: string;
  repoRoot: string;
  runsRoot: string;
  runCount: number;
  attemptCount: number;
  scoredRunCount: number;
  bestScore: number | null;
  medianBestScore: number | null;
  profileDropCount: number;
  curveCoverageCount: number;
  leaderboardCount: number;
  modelBucketCount: number;
  tradeoffPointCount: number;
  validationPointCount: number;
  similarityLeaderCount: number;
}

export interface Images {
  aggregatePlotUrl: string | null;
  leaderboardPlotUrl: string | null;
  modelLeaderboardPlotUrl: string | null;
  tradeoffPlotUrl: string | null;
  validationScatterPlotUrl: string | null;
  validationDeltaPlotUrl: string | null;
  similarityHeatmapPlotUrl: string | null;
  similarityScatterPlotUrl: string | null;
}

export interface QualityScoreComponents {
  belief: number;
  cadence: number;
  edge_rate: number;
  path_quality: number;
  return_quality: number;
  robustness: number;
}

export interface QualityScorePayload {
  belief_basis: string;
  components: QualityScoreComponents;
  gates: Record<string, boolean>;
  inputs: Record<string, number>;
  preset: string;
  score: number;
  version: string;
}

export interface BestSummary {
  quality_score?: number;
  quality_score_payload?: QualityScorePayload;
  best_cell?: {
    avg_net_r_per_closed_trade: number;
    profit_factor: number;
    resolved_trades: number;
    reward_multiple: number;
    stop_loss_percent: number;
    take_profit_percent: number;
  };
  best_cell_path_metrics?: {
    equity_curve_r_squared: number;
    final_equity_r: number;
    k_ratio: number;
    max_drawdown_r: number;
    peak_equity_r: number;
    psr: number;
    sharpe_r: number;
    time_under_water_ratio: number;
    trade_count: number;
    ulcer_index_r: number;
  };
  behavior_summary?: {
    signal_selectivity: string;
    bars_per_signal: number;
    direction_flip_rate: number;
    signal_coverage_ratio: number;
  };
  matrix_summary?: {
    positive_cell_ratio: number;
    positive_cell_count: number;
    best_cell_positive_neighbor_count: number;
    largest_positive_cluster_size: number;
  };
  instrument?: string;
  timeframe?: string;
  mode?: string;
  signal_count?: number;
  [key: string]: unknown;
}

export interface AttemptSummary {
  attemptId: string;
  sequence: number;
  createdAt: string;
  candidateName: string;
  score: number | null;
  scoreBasis: string;
  metrics: Record<string, number | null>;
  tradeCount: number | null;
  tradesPerMonth: number | null;
  effectiveWindowMonths: number | null;
  maxDrawdownR: number | null;
  positiveCellRatio: number | null;
  expectancyR: number | null;
  profitFactor: number | null;
  signalSelectivity: string | null;
  instrument: string | null;
  timeframe: string | null;
  profileRef: string | null;
  artifactDir: string | null;
  artifactDirUrl: string | null;
  profilePath: string | null;
  profilePathUrl: string | null;
  sensitivityPath: string | null;
  sensitivityPathUrl: string | null;
  curvePath: string | null;
  curvePathUrl: string | null;
  deepReplayJobPath: string | null;
  deepReplayJobPathUrl: string | null;
  bestSummary: BestSummary;
}

export interface RunSummary {
  runId: string;
  createdAt: string;
  explorerProfile: string;
  explorerModel: string;
  supervisorProfile: string;
  supervisorModel: string;
  qualityScorePreset: string;
  attemptCount: number;
  scoredAttemptCount: number;
  curveAttemptCount: number;
  latestAttemptAt: string | null;
  latestStep: number | null;
  latestLogTimestamp: string | null;
  advisorGuidanceCount: number;
  progressPngUrl: string | null;
  profileDrop12PngUrl: string | null;
  profileDrop36PngUrl: string | null;
  bestAttempt: AttemptSummary | null;
}

export interface LeaderboardRow {
  attempt_id: string;
  sequence: number;
  created_at: string;
  run_id: string;
  candidate_name: string;
  composite_score: number;
  score_basis: string;
  metrics: Record<string, number | null>;
  best_summary: BestSummary;
  run_metadata?: {
    explorer_model?: string;
    explorer_profile?: string;
    supervisor_model?: string;
  };
  leaderboard_label: string;
}

export interface TradeoffRow {
  run_id: string;
  attempt_id: string;
  candidate_name: string;
  composite_score: number;
  trades_per_month: number;
  is_trade_envelope?: boolean;
  is_frontier?: boolean;
}

export interface ValidationRow {
  run_id: string;
  attempt_id: string;
  candidate_name: string;
  leaderboard_label: string | null;
  explorer_model: string | null;
  score_12m: number;
  score_36m: number;
  score_delta: number;
  score_retention_ratio: number;
  trades_per_month_12m: number;
  trades_per_month_36m: number;
  trade_count_12m: number;
  trade_count_36m: number;
  max_drawdown_r_12m: number;
  max_drawdown_r_36m: number;
}

export interface SimilarityLeader {
  run_id: string;
  attempt_id: string;
  candidate_name: string;
  score_36m: number;
  max_sameness: number;
  closest_match_label: string;
  trades_per_month_36m: number;
}

export interface SimilarityPair {
  left_run_id: string;
  left_attempt_id: string;
  right_run_id: string;
  right_attempt_id: string;
  similarity_score: number;
  positive_correlation: number;
  shared_active_ratio: number;
}

export interface ModelConsistencyRow {
  modelLabel: string;
  runCount: number;
  averageScore: number;
  medianScore: number;
  bestScore: number;
  score70PlusRate: number;
  score80PlusRate: number;
}

export interface DrawdownRow {
  runId: string;
  attemptId: string;
  label: string;
  score: number;
  maxDrawdownR: number;
  tradesPerMonth: number | null;
  tradeCount: number | null;
}

export interface DashboardPayload {
  overview: Overview;
  images: Images;
  leaderboard: LeaderboardRow[];
  modelAverages: unknown[];
  modelConsistency: ModelConsistencyRow[];
  tradeoff: TradeoffRow[];
  validation: ValidationRow[];
  similarity: SimilarityLeader[];
  similarityPairs: SimilarityPair[];
  scoreVsDrawdown: DrawdownRow[];
  runs: RunSummary[];
  limit: number;
}

export interface RunDetail {
  run: RunSummary;
  attempts: AttemptSummary[];
}

export interface CurvePoint {
  time: number;
  equity_r: number;
  drawdown_r: number;
  date: string;
  closed_trade_count: number;
}

export interface AttemptDetail {
  runId: string;
  attempt: AttemptSummary;
  curve: { curve: { points: CurvePoint[] } } | null;
  sensitivity: unknown;
  deepReplayJob: unknown;
  profile: unknown;
  profileDrop12PngUrl: string | null;
  profileDrop36PngUrl: string | null;
  fullBacktestResult: FullBacktestResult | null;
  fullBacktestCurve: FullBacktestCurve | null;
  hasFullBacktest: boolean;
}

export interface FullBacktestResult {
  data?: {
    aggregate?: {
      quality_score?: {
        score: number;
        components: Record<string, number>;
        inputs?: {
          trades_per_month?: number;
          expectancy_r?: number;
          profit_factor?: number;
          max_drawdown_r?: number;
          effective_window_months?: number;
        };
      };
      best_cell_path_metrics?: Record<string, number>;
      best_cell?: {
        avg_net_r_per_closed_trade?: number;
        profit_factor?: number;
      };
      market_data_window?: { effective_window_months: number };
    };
  };
}

export interface FullBacktestCurve {
  curve?: {
    points: CurvePoint[];
  };
}
