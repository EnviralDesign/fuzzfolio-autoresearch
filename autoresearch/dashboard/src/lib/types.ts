export type AttemptCatalogRow = {
  run_id: string;
  attempt_id: string;
  created_at?: string | null;
  candidate_name?: string | null;
  composite_score?: number | null;
  score_36m?: number | null;
  score_12m?: number | null;
  trades_per_month_36m?: number | null;
  trade_count_36m?: number | null;
  max_drawdown_r_36m?: number | null;
  strategy_key_36m?: string | null;
  timeframe_36m?: string | null;
  instruments_36m?: string[] | null;
  full_backtest_validation_status_36m?: string | null;
  has_full_backtest_36m?: boolean;
  artifact_dir?: string | null;
  artifact_dir_url?: string | null;
  profile_path?: string | null;
  profile_path_url?: string | null;
  full_backtest_result_path_36m?: string | null;
  full_backtest_result_path_36m_url?: string | null;
  full_backtest_curve_path_36m?: string | null;
  full_backtest_curve_path_36m_url?: string | null;
  selection_rank?: number | null;
  selection_utility?: number | null;
  score_component?: number | null;
  drawdown_penalty_component?: number | null;
  max_sameness_to_selected?: number | null;
  max_sameness_to_board?: number | null;
  closest_selected_attempt_id?: string | null;
  [key: string]: unknown;
};

export type ChartAsset = {
  path: string;
  url: string | null;
  exists: boolean;
};

export type CorpusSummary = {
  run_count?: number;
  attempt_count?: number;
  scored_attempt_count?: number;
  unique_base_strategy_count?: number;
  unique_strategy_count_36m?: number;
  unique_full_backtest_strategy_count_36m?: number;
  attempts_with_scrutiny_36m?: number;
  attempts_with_full_backtest_36m?: number;
  attempts_with_valid_full_backtest_36m?: number;
  attempts_with_invalid_full_backtest_36m?: number;
  attempts_with_base_sensitivity?: number;
  scrutiny_36m_coverage_ratio?: number;
  full_backtest_36m_coverage_ratio?: number;
  valid_full_backtest_36m_coverage_ratio?: number;
  full_backtest_36m_vs_scrutiny_coverage_ratio?: number;
  median_score_36m?: number;
  score_36m_ge_40?: number;
  score_36m_ge_60?: number;
  score_36m_ge_70?: number;
  full_backtest_36m_ge_40?: number;
  full_backtest_36m_ge_60?: number;
  full_backtest_36m_ge_70?: number;
  [key: string]: unknown;
};

export type FullBacktestAudit = {
  summary?: CorpusSummary;
  status?: string;
  provisional_reasons?: string[];
  invalid_examples?: AttemptCatalogRow[];
  pending_scrutiny_examples?: AttemptCatalogRow[];
};

export type FilterRejections = Record<string, number>;

export type ShortlistProfileDrop = {
  attempt_id: string;
  run_id: string;
  candidate_name?: string | null;
  status?: string;
  png_path?: string | null;
  png_url?: string | null;
  manifest_path?: string | null;
  manifest_url?: string | null;
  profile_ref?: string | null;
  recreated_profile?: boolean;
};

export type ShortlistReport = {
  generated_at?: string;
  filters?: Record<string, unknown>;
  candidate_count?: number;
  selected_count?: number;
  alternate_count?: number;
  filter_rejections?: FilterRejections;
  selected_by_run?: Record<string, number>;
  selected_by_strategy_key?: Record<string, number>;
  selected?: AttemptCatalogRow[];
  alternates?: AttemptCatalogRow[];
  top_similarity_pairs?: Record<string, unknown>[];
  charts?: Record<string, ChartAsset>;
  profile_drops?: ShortlistProfileDrop[];
};

export type PromotionBoard = {
  generated_at?: string;
  status?: string;
  provisional_reasons?: string[];
  filters?: Record<string, unknown>;
  coverage?: Record<string, unknown>;
  filter_rejections?: FilterRejections;
  candidate_count?: number;
  similarity_pair_count?: number;
  selected?: AttemptCatalogRow[];
  alternates?: AttemptCatalogRow[];
  top_similarity_pairs?: Record<string, unknown>[];
};

export type RunSummary = {
  run_id: string;
  created_at?: string | null;
  latest_created_at?: string | null;
  explorer_model?: string | null;
  explorer_profile?: string | null;
  supervisor_model?: string | null;
  supervisor_profile?: string | null;
  quality_score_preset?: string | null;
  attempt_count: number;
  scored_attempt_count: number;
  full_backtest_36m_count: number;
  score_36m_count: number;
  best_attempt?: AttemptCatalogRow | null;
  progress_png_url?: string | null;
};

export type CadenceBand = {
  band: string;
  count: number;
  mean_score_36m: number | null;
  max_score_36m: number | null;
  mean_drawdown_r_36m: number | null;
};

export type ViewerState = {
  generated_at: string;
  corpus_summary: CorpusSummary;
  audit: FullBacktestAudit;
  shortlist: ShortlistReport;
  promotion: PromotionBoard;
  runs: RunSummary[];
  charts: Record<string, ChartAsset>;
  cadence_bands_all_scored: CadenceBand[];
  cadence_bands_score_ge_40: CadenceBand[];
};

export type CatalogResponse = {
  generated_at: string;
  attempt_count: number;
  rows: AttemptCatalogRow[];
};

export type RunsResponse = {
  generated_at: string;
  run_count: number;
  runs: RunSummary[];
};

export type RunDetail = {
  run: RunSummary | null;
  attempts: AttemptCatalogRow[];
};

export type AttemptDetail = {
  attempt: AttemptCatalogRow;
  full_backtest_result: Record<string, unknown> | null;
  full_backtest_curve: Record<string, unknown> | null;
};
