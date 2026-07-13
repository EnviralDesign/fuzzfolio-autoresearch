use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{Datelike, NaiveDate};
#[cfg(feature = "python-extension")]
use pyo3::{exceptions::PyValueError, prelude::*};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

const FX_CODES: &[&str] = &[
    "AUD", "CAD", "CHF", "CNH", "EUR", "GBP", "HKD", "JPY", "MXN", "NOK", "NZD", "SEK", "SGD",
    "TRY", "USD", "ZAR",
];
const METAL_SYMBOLS: &[&str] = &["XAUUSD", "XAGUSD"];
const INDEX_SYMBOLS: &[&str] = &[
    "AUS200", "DE40", "FCHI40", "GDAXI", "HK50", "JP225", "NDX", "RUSS2000", "SP500", "SPA35",
    "STOXX50E", "UK100", "US30", "US500", "USTECH", "WS30",
];
const COMMODITY_SYMBOLS: &[&str] = &["UKOUSD", "USOUSD", "XBRUSD", "XNGUSD", "XTIUSD"];
const CRYPTO_SYMBOLS: &[&str] = &["BCHUSD", "BTCUSD", "ETHUSD", "LTCUSD", "SOLUSD", "XRPUSD"];

const PARETO_DIMENSIONS: &[(&str, &str)] = &[
    ("final_r", "max"),
    ("maxdd_r", "min"),
    ("neg_months", "min"),
    ("neg_weeks", "min"),
    ("worst_month_r", "max"),
    ("worst_week_r", "max"),
    ("worst_day_r", "max"),
    ("top_day_gain_share", "min"),
    ("max_daily_loss_streak", "min"),
    ("mean_avg_hold_hours", "min"),
    ("avg_open_positions", "min"),
    ("peak_open_positions", "min"),
];

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PortfolioOptimizerSpec {
    #[serde(default = "default_portfolio_name")]
    pub portfolio_name: String,
    #[serde(default = "default_portfolio_size")]
    pub portfolio_size: usize,
    #[serde(default = "default_candidate_limit")]
    pub candidate_limit: i64,
    #[serde(default = "default_swap_candidate_limit")]
    pub swap_candidate_limit: i64,
    #[serde(default = "default_objective_names")]
    pub objective_names: Vec<String>,
    #[serde(default = "default_max_swaps")]
    pub max_swaps: usize,
    #[serde(default = "default_random_starts")]
    pub random_starts: usize,
    #[serde(default = "default_random_seed")]
    pub random_seed: i64,
    #[serde(default = "default_max_per_family")]
    pub max_per_family: usize,
    #[serde(default = "default_max_instrument_share")]
    pub max_instrument_share: f64,
    #[serde(default = "default_min_fx_share")]
    pub min_fx_share: f64,
    #[serde(default = "default_max_metal_share")]
    pub max_metal_share: f64,
    #[serde(default = "default_max_index_share")]
    pub max_index_share: f64,
    #[serde(default = "default_max_avg_open_positions")]
    pub max_avg_open_positions: f64,
    #[serde(default = "default_max_peak_open_positions")]
    pub max_peak_open_positions: f64,
    #[serde(default = "default_target_trades_per_month")]
    pub target_trades_per_month: f64,
    #[serde(default = "default_max_trades_per_month")]
    pub max_trades_per_month: f64,
    #[serde(default)]
    pub correlation_penalty_weight: f64,
    #[serde(default = "default_diversification_mode")]
    pub diversification_mode: String,
    #[serde(default)]
    pub portfolio_sharpe_weight: f64,
    #[serde(default)]
    pub baseline_attempt_ids: Vec<String>,
    #[serde(default)]
    pub required_attempt_ids: Vec<String>,
    #[serde(default)]
    pub account: Map<String, Value>,
}

impl Default for PortfolioOptimizerSpec {
    fn default() -> Self {
        Self {
            portfolio_name: default_portfolio_name(),
            portfolio_size: default_portfolio_size(),
            candidate_limit: default_candidate_limit(),
            swap_candidate_limit: default_swap_candidate_limit(),
            objective_names: default_objective_names(),
            max_swaps: default_max_swaps(),
            random_starts: default_random_starts(),
            random_seed: default_random_seed(),
            max_per_family: default_max_per_family(),
            max_instrument_share: default_max_instrument_share(),
            min_fx_share: default_min_fx_share(),
            max_metal_share: default_max_metal_share(),
            max_index_share: default_max_index_share(),
            max_avg_open_positions: default_max_avg_open_positions(),
            max_peak_open_positions: default_max_peak_open_positions(),
            target_trades_per_month: default_target_trades_per_month(),
            max_trades_per_month: default_max_trades_per_month(),
            correlation_penalty_weight: 0.0,
            diversification_mode: default_diversification_mode(),
            portfolio_sharpe_weight: 0.0,
            baseline_attempt_ids: Vec::new(),
            required_attempt_ids: Vec::new(),
            account: Map::new(),
        }
    }
}

fn default_portfolio_name() -> String {
    "portfolio-optimizer".to_string()
}
fn default_portfolio_size() -> usize {
    20
}
fn default_candidate_limit() -> i64 {
    120
}
fn default_swap_candidate_limit() -> i64 {
    80
}
fn default_objective_names() -> Vec<String> {
    vec![
        "return".to_string(),
        "balanced".to_string(),
        "stability".to_string(),
    ]
}
fn default_max_swaps() -> usize {
    10
}
fn default_random_starts() -> usize {
    3
}
fn default_random_seed() -> i64 {
    17
}
fn default_max_per_family() -> usize {
    1
}
fn default_max_instrument_share() -> f64 {
    4.0
}
fn default_min_fx_share() -> f64 {
    7.0
}
fn default_max_metal_share() -> f64 {
    8.0
}
fn default_max_index_share() -> f64 {
    6.0
}
fn default_max_avg_open_positions() -> f64 {
    7.0
}
fn default_max_peak_open_positions() -> f64 {
    28.0
}
fn default_target_trades_per_month() -> f64 {
    160.0
}
fn default_max_trades_per_month() -> f64 {
    260.0
}
fn default_diversification_mode() -> String {
    "penalty".to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OptimizerCandidateInput {
    pub attempt_id: String,
    #[serde(default)]
    pub candidate_name: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub instruments: Vec<String>,
    #[serde(default)]
    pub family: Option<String>,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub avg_hold_hours: f64,
    #[serde(default)]
    pub p90_hold_hours: Option<f64>,
    #[serde(default)]
    pub max_hold_hours: Option<f64>,
    #[serde(default)]
    pub path_quality: Option<f64>,
    #[serde(default)]
    pub stop_loss_percent: Option<f64>,
    #[serde(default)]
    pub trade_count: i64,
    #[serde(default)]
    pub trades_per_month: f64,
    #[serde(default)]
    pub dates: Vec<String>,
    #[serde(default)]
    pub daily_r: Vec<f64>,
    #[serde(default)]
    pub open_counts: Vec<i64>,
    #[serde(default)]
    pub closed_counts: Vec<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OptimizerInput {
    #[serde(default)]
    pub spec: PortfolioOptimizerSpec,
    #[serde(default)]
    pub candidates: Vec<OptimizerCandidateInput>,
    #[serde(default)]
    pub objectives: BTreeMap<String, BTreeMap<String, f64>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OptimizerOutput {
    pub variants: BTreeMap<String, OptimizerVariant>,
    pub pareto_front: Vec<ArchiveItem>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SimilarityInput {
    pub schema_version: String,
    pub candidates: Vec<SimilarityCandidateInput>,
    pub reference_attempt_ids: Vec<String>,
    pub active_epsilon: f64,
    pub worst_quantile: f64,
    pub min_observations: usize,
    pub behavioral_weights: BehavioralWeights,
    pub cluster_threshold: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SimilarityCandidateInput {
    pub attempt_id: String,
    pub dates: Vec<String>,
    pub daily_r: Vec<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BehavioralWeights {
    pub active_overlap: f64,
    pub return_correlation: f64,
    pub downside_correlation: f64,
    pub worst_decile_correlation: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SimilarityOutput {
    pub schema_version: String,
    pub attempt_ids: Vec<String>,
    pub reference: SimilarityReferenceMetadata,
    pub active_overlap_matrix: Vec<Vec<f64>>,
    pub return_correlation_matrix: Vec<Vec<f64>>,
    pub downside_correlation_matrix: Vec<Vec<f64>>,
    pub worst_decile_correlation_matrix: Vec<Vec<f64>>,
    pub similarity_matrix: Vec<Vec<f64>>,
    pub clusters: Vec<SimilarityCluster>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SimilarityReferenceMetadata {
    pub attempt_ids: Vec<String>,
    pub calendar_dates: Vec<String>,
    pub daily_r: Vec<f64>,
    pub downside_observation_count: usize,
    pub worst_quantile: f64,
    pub worst_cutoff_r: Option<f64>,
    pub worst_observation_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SimilarityCluster {
    pub id: String,
    pub members: Vec<String>,
}

#[derive(Clone, Debug)]
struct SimilarityAlignedCandidate {
    attempt_id: String,
    daily_r: Vec<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OptimizerVariant {
    pub objective_name: String,
    pub objective_score: f64,
    pub start: String,
    pub selected_attempt_ids: Vec<String>,
    pub swaps: Vec<SwapMove>,
    pub diversification: BTreeMap<String, Value>,
    pub metrics: Value,
    pub selected: Vec<BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SwapMove {
    pub removed: String,
    pub added: String,
    pub objective_after: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArchiveItem {
    pub archive_label: String,
    pub objective_name: String,
    pub objective_score: f64,
    pub objective_scores: BTreeMap<String, f64>,
    pub selected_attempt_ids: Vec<String>,
    pub metrics: Value,
}

#[derive(Clone, Debug)]
struct OptimizerCandidate {
    attempt_id: String,
    candidate_name: Option<String>,
    run_id: Option<String>,
    created_at: Option<String>,
    instruments: Vec<String>,
    primary_asset_class: String,
    family: String,
    score: f64,
    avg_hold_hours: f64,
    p90_hold_hours: Option<f64>,
    max_hold_hours: Option<f64>,
    path_quality: Option<f64>,
    stop_loss_percent: Option<f64>,
    trade_count: i64,
    trades_per_month: f64,
    dates: Vec<String>,
    daily_r: Vec<f64>,
    open_counts: Vec<i64>,
    closed_counts: Vec<i64>,
    vector: Vec<f64>,
    open_vector: Vec<i64>,
    closed_vector: Vec<i64>,
    month_vector: Vec<f64>,
    week_vector: Vec<f64>,
}

impl OptimizerCandidate {
    fn final_r(&self) -> f64 {
        self.daily_r.iter().sum()
    }

    fn maxdd_r(&self) -> f64 {
        max_drawdown(&self.daily_r)
    }
}

pub fn optimize_input(input: OptimizerInput) -> OptimizerOutput {
    let objectives = if input.objectives.is_empty() {
        default_objectives()
    } else {
        input.objectives
    };
    let mut search = PortfolioSearch::new(input.candidates, input.spec, objectives);
    let variants = search.optimize();
    let pareto_front = search.pareto_front(50);
    OptimizerOutput {
        variants,
        pareto_front,
    }
}

pub fn optimize_json(input_json: &str) -> Result<String, String> {
    let input: OptimizerInput = serde_json::from_str(input_json)
        .map_err(|error| format!("invalid optimizer input JSON: {error}"))?;
    validate_required_attempt_ids(&input)?;
    let output = optimize_input(input);
    serde_json::to_string(&output)
        .map_err(|error| format!("failed to serialize optimizer output: {error}"))
}

fn validate_required_attempt_ids(input: &OptimizerInput) -> Result<(), String> {
    let required: BTreeSet<&str> = input
        .spec
        .required_attempt_ids
        .iter()
        .map(String::as_str)
        .collect();
    if required.len() > input.spec.portfolio_size {
        return Err("required_attempt_ids exceed portfolio_size".to_string());
    }
    let known: HashSet<&str> = input
        .candidates
        .iter()
        .map(|candidate| candidate.attempt_id.as_str())
        .collect();
    let missing: Vec<&str> = required
        .iter()
        .copied()
        .filter(|attempt_id| !known.contains(attempt_id))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "required_attempt_ids are missing from the optimizer candidate pool: {}",
            missing.join(", ")
        ));
    }
    Ok(())
}

pub fn analyze_similarity_json(input_json: &str) -> Result<String, String> {
    let input: SimilarityInput = serde_json::from_str(input_json)
        .map_err(|error| format!("invalid similarity input JSON: {error}"))?;
    let output = analyze_similarity(input)?;
    serde_json::to_string(&output)
        .map_err(|error| format!("failed to serialize similarity output: {error}"))
}

pub fn analyze_similarity(input: SimilarityInput) -> Result<SimilarityOutput, String> {
    validate_similarity_input(&input)?;

    let mut date_set = BTreeSet::new();
    for candidate in &input.candidates {
        date_set.extend(candidate.dates.iter().cloned());
    }
    let calendar_dates: Vec<String> = date_set.into_iter().collect();
    let date_index: HashMap<&str, usize> = calendar_dates
        .iter()
        .enumerate()
        .map(|(index, date)| (date.as_str(), index))
        .collect();

    let mut candidates: Vec<SimilarityAlignedCandidate> = input
        .candidates
        .into_iter()
        .map(|candidate| {
            let mut daily_r = vec![0.0; calendar_dates.len()];
            for (date, value) in candidate.dates.iter().zip(candidate.daily_r.iter()) {
                daily_r[*date_index
                    .get(date.as_str())
                    .expect("validated dates are present in the union calendar")] = *value;
            }
            SimilarityAlignedCandidate {
                attempt_id: candidate.attempt_id,
                daily_r,
            }
        })
        .collect();
    candidates.sort_by(|left, right| left.attempt_id.cmp(&right.attempt_id));

    let attempt_ids: Vec<String> = candidates
        .iter()
        .map(|candidate| candidate.attempt_id.clone())
        .collect();
    let candidate_index: HashMap<&str, usize> = attempt_ids
        .iter()
        .enumerate()
        .map(|(index, attempt_id)| (attempt_id.as_str(), index))
        .collect();
    let mut reference_attempt_ids = input.reference_attempt_ids;
    reference_attempt_ids.sort();
    let reference_indexes: Vec<usize> = reference_attempt_ids
        .iter()
        .map(|attempt_id| candidate_index[attempt_id.as_str()])
        .collect();
    let reference_daily_r: Vec<f64> = (0..calendar_dates.len())
        .into_par_iter()
        .map(|date_index| {
            reference_indexes
                .iter()
                .map(|candidate_index| candidates[*candidate_index].daily_r[date_index])
                .sum()
        })
        .collect();
    if reference_daily_r.iter().any(|value| !value.is_finite()) {
        return Err("reference daily return sum is not finite".to_string());
    }

    let downside_indexes: Vec<usize> = reference_daily_r
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (*value < 0.0).then_some(index))
        .collect();
    let worst_cutoff_r = interpolated_percentile(&reference_daily_r, input.worst_quantile);
    let worst_indexes: Vec<usize> = worst_cutoff_r
        .map(|cutoff| {
            reference_daily_r
                .iter()
                .enumerate()
                .filter_map(|(index, value)| (*value <= cutoff).then_some(index))
                .collect()
        })
        .unwrap_or_default();

    let active_overlap_matrix = symmetric_similarity_matrix(candidates.len(), |left, right| {
        active_overlap(
            &candidates[left].daily_r,
            &candidates[right].daily_r,
            input.active_epsilon,
        )
    });
    let return_correlation_matrix = symmetric_similarity_matrix(candidates.len(), |left, right| {
        pearson_corr_with_minimum(
            &candidates[left].daily_r,
            &candidates[right].daily_r,
            input.min_observations,
        )
    });
    let downside_correlation_matrix =
        symmetric_similarity_matrix(candidates.len(), |left, right| {
            indexed_pearson_corr(
                &candidates[left].daily_r,
                &candidates[right].daily_r,
                &downside_indexes,
                input.min_observations,
            )
        });
    let worst_decile_correlation_matrix =
        symmetric_similarity_matrix(candidates.len(), |left, right| {
            indexed_pearson_corr(
                &candidates[left].daily_r,
                &candidates[right].daily_r,
                &worst_indexes,
                input.min_observations,
            )
        });
    let total_weight = input.behavioral_weights.active_overlap
        + input.behavioral_weights.return_correlation
        + input.behavioral_weights.downside_correlation
        + input.behavioral_weights.worst_decile_correlation;
    let similarity_matrix = symmetric_similarity_matrix(candidates.len(), |left, right| {
        let similarity = input.behavioral_weights.active_overlap
            * active_overlap_matrix[left][right]
            + input.behavioral_weights.return_correlation
                * return_correlation_matrix[left][right].max(0.0)
            + input.behavioral_weights.downside_correlation
                * downside_correlation_matrix[left][right].max(0.0)
            + input.behavioral_weights.worst_decile_correlation
                * worst_decile_correlation_matrix[left][right].max(0.0);
        (similarity / total_weight).clamp(0.0, 1.0)
    });

    let clusters = similarity_clusters(&attempt_ids, &similarity_matrix, input.cluster_threshold);
    Ok(SimilarityOutput {
        schema_version: input.schema_version,
        attempt_ids,
        reference: SimilarityReferenceMetadata {
            attempt_ids: reference_attempt_ids,
            calendar_dates,
            daily_r: reference_daily_r,
            downside_observation_count: downside_indexes.len(),
            worst_quantile: input.worst_quantile,
            worst_cutoff_r,
            worst_observation_count: worst_indexes.len(),
        },
        active_overlap_matrix,
        return_correlation_matrix,
        downside_correlation_matrix,
        worst_decile_correlation_matrix,
        similarity_matrix,
        clusters,
    })
}

fn validate_similarity_input(input: &SimilarityInput) -> Result<(), String> {
    if input.schema_version.trim().is_empty() {
        return Err("schema_version must be nonempty".to_string());
    }
    if input.candidates.is_empty() {
        return Err("candidates must be nonempty".to_string());
    }
    let mut candidate_ids = HashSet::new();
    for candidate in &input.candidates {
        if candidate.attempt_id.trim().is_empty() {
            return Err("candidate attempt_id must be nonempty".to_string());
        }
        if !candidate_ids.insert(candidate.attempt_id.as_str()) {
            return Err(format!(
                "duplicate candidate attempt_id: {}",
                candidate.attempt_id
            ));
        }
        if candidate.dates.len() != candidate.daily_r.len() {
            return Err(format!(
                "candidate {} has {} dates but {} daily_r values",
                candidate.attempt_id,
                candidate.dates.len(),
                candidate.daily_r.len()
            ));
        }
        let mut dates = HashSet::new();
        for date in &candidate.dates {
            if date.trim().is_empty() {
                return Err(format!(
                    "candidate {} has an empty date",
                    candidate.attempt_id
                ));
            }
            if !dates.insert(date.as_str()) {
                return Err(format!(
                    "candidate {} has duplicate date: {date}",
                    candidate.attempt_id
                ));
            }
        }
        if candidate.daily_r.iter().any(|value| !value.is_finite()) {
            return Err(format!(
                "candidate {} contains a non-finite daily_r value",
                candidate.attempt_id
            ));
        }
    }
    if input.reference_attempt_ids.is_empty() {
        return Err("reference_attempt_ids must be nonempty".to_string());
    }
    let mut reference_ids = HashSet::new();
    for attempt_id in &input.reference_attempt_ids {
        if !reference_ids.insert(attempt_id.as_str()) {
            return Err(format!("duplicate reference_attempt_id: {attempt_id}"));
        }
        if !candidate_ids.contains(attempt_id.as_str()) {
            return Err(format!("unknown reference_attempt_id: {attempt_id}"));
        }
    }
    if !input.active_epsilon.is_finite() || input.active_epsilon < 0.0 {
        return Err("active_epsilon must be finite and nonnegative".to_string());
    }
    if !input.worst_quantile.is_finite() || !(0.0..=1.0).contains(&input.worst_quantile) {
        return Err("worst_quantile must be finite and between 0 and 1".to_string());
    }
    if input.min_observations == 0 {
        return Err("min_observations must be at least 1".to_string());
    }
    let weights = [
        input.behavioral_weights.active_overlap,
        input.behavioral_weights.return_correlation,
        input.behavioral_weights.downside_correlation,
        input.behavioral_weights.worst_decile_correlation,
    ];
    if weights
        .iter()
        .any(|weight| !weight.is_finite() || *weight < 0.0)
    {
        return Err("behavioral_weights must be finite and nonnegative".to_string());
    }
    if weights.iter().sum::<f64>() <= 0.0 {
        return Err("behavioral_weights must have a positive total".to_string());
    }
    if !input.cluster_threshold.is_finite() || !(0.0..=1.0).contains(&input.cluster_threshold) {
        return Err("cluster_threshold must be finite and between 0 and 1".to_string());
    }
    Ok(())
}

fn active_overlap(left: &[f64], right: &[f64], active_epsilon: f64) -> f64 {
    let mut union = 0_usize;
    let mut intersection = 0_usize;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_active = left_value.abs() > active_epsilon;
        let right_active = right_value.abs() > active_epsilon;
        if left_active || right_active {
            union += 1;
        }
        if left_active && right_active {
            intersection += 1;
        }
    }
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn interpolated_percentile(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let position = quantile * (sorted.len() - 1) as f64;
    let lower_index = position.floor() as usize;
    let upper_index = position.ceil() as usize;
    let fraction = position - lower_index as f64;
    Some(sorted[lower_index] + (sorted[upper_index] - sorted[lower_index]) * fraction)
}

fn pearson_corr_with_minimum(left: &[f64], right: &[f64], min_observations: usize) -> f64 {
    let size = left.len().min(right.len());
    if size < min_observations {
        return 0.0;
    }
    let left = &left[..size];
    let right = &right[..size];
    let left_mean = left.iter().sum::<f64>() / size as f64;
    let right_mean = right.iter().sum::<f64>() / size as f64;
    let (covariance, left_sum_squares, right_sum_squares) = left.iter().zip(right.iter()).fold(
        (0.0, 0.0, 0.0),
        |(covariance, left_sum_squares, right_sum_squares), (left_value, right_value)| {
            let left_delta = left_value - left_mean;
            let right_delta = right_value - right_mean;
            (
                covariance + left_delta * right_delta,
                left_sum_squares + left_delta * left_delta,
                right_sum_squares + right_delta * right_delta,
            )
        },
    );
    if left_sum_squares <= f64::EPSILON || right_sum_squares <= f64::EPSILON {
        0.0
    } else {
        (covariance / (left_sum_squares * right_sum_squares).sqrt()).clamp(-1.0, 1.0)
    }
}

fn indexed_pearson_corr(
    left: &[f64],
    right: &[f64],
    indexes: &[usize],
    min_observations: usize,
) -> f64 {
    if indexes.len() < min_observations {
        return 0.0;
    }
    let left_values: Vec<f64> = indexes.iter().map(|index| left[*index]).collect();
    let right_values: Vec<f64> = indexes.iter().map(|index| right[*index]).collect();
    pearson_corr_with_minimum(&left_values, &right_values, min_observations)
}

fn symmetric_similarity_matrix<F>(size: usize, value_at: F) -> Vec<Vec<f64>>
where
    F: Fn(usize, usize) -> f64 + Sync + Send,
{
    let entries: Vec<Vec<(usize, f64)>> = (0..size)
        .into_par_iter()
        .map(|left| {
            (left..size)
                .map(|right| (right, value_at(left, right)))
                .collect()
        })
        .collect();
    let mut matrix = vec![vec![0.0; size]; size];
    for (left, row) in entries.into_iter().enumerate() {
        for (right, value) in row {
            matrix[left][right] = value;
            matrix[right][left] = value;
        }
    }
    matrix
}

fn similarity_clusters(
    attempt_ids: &[String],
    similarity_matrix: &[Vec<f64>],
    threshold: f64,
) -> Vec<SimilarityCluster> {
    let mut parents: Vec<usize> = (0..attempt_ids.len()).collect();
    for left in 0..attempt_ids.len() {
        for right in (left + 1)..attempt_ids.len() {
            if similarity_matrix[left][right] >= threshold {
                union_similarity_nodes(&mut parents, left, right);
            }
        }
    }
    let mut members_by_root: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for (index, attempt_id) in attempt_ids.iter().enumerate() {
        let root = find_similarity_root(&mut parents, index);
        members_by_root
            .entry(root)
            .or_default()
            .push(attempt_id.clone());
    }
    let mut clusters: Vec<SimilarityCluster> = members_by_root
        .into_values()
        .map(|mut members| {
            members.sort();
            SimilarityCluster {
                id: format!("behavior:{}", members[0]),
                members,
            }
        })
        .collect();
    clusters.sort_by(|left, right| left.id.cmp(&right.id));
    clusters
}

fn find_similarity_root(parents: &mut [usize], node: usize) -> usize {
    if parents[node] != node {
        let root = find_similarity_root(parents, parents[node]);
        parents[node] = root;
    }
    parents[node]
}

fn union_similarity_nodes(parents: &mut [usize], left: usize, right: usize) {
    let left_root = find_similarity_root(parents, left);
    let right_root = find_similarity_root(parents, right);
    if left_root != right_root {
        parents[right_root] = left_root.min(right_root);
        parents[left_root] = left_root.min(right_root);
    }
}

#[cfg(feature = "python-extension")]
#[pyfunction(name = "optimize_json")]
fn optimize_json_py(input_json: &str) -> PyResult<String> {
    optimize_json(input_json).map_err(PyValueError::new_err)
}

#[cfg(feature = "python-extension")]
#[pyfunction(name = "analyze_similarity_json")]
fn analyze_similarity_json_py(input_json: &str) -> PyResult<String> {
    analyze_similarity_json(input_json).map_err(PyValueError::new_err)
}

#[cfg(feature = "python-extension")]
#[pymodule]
fn portfolio_optimizer_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(optimize_json_py, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_similarity_json_py, m)?)?;
    Ok(())
}

fn default_objectives() -> BTreeMap<String, BTreeMap<String, f64>> {
    let mut objectives = BTreeMap::new();
    objectives.insert(
        "return".to_string(),
        weights(&[
            ("final_r", 1.0),
            ("maxdd_r", -2.0),
            ("negative_month", -70.0),
            ("negative_week", -0.8),
            ("worst_week_abs", -1.8),
            ("top_day_share", -240.0),
            ("loss_streak", -8.0),
            ("hold_over_24h", -1.0),
            ("avg_open_over_target", -4.0),
            ("peak_open_over_target", -1.5),
            ("trade_over_target_pm", -0.15),
            ("constraint_violation", -240.0),
        ]),
    );
    objectives.insert(
        "balanced".to_string(),
        weights(&[
            ("final_r", 0.65),
            ("maxdd_r", -6.0),
            ("positive_month", 10.0),
            ("negative_month", -210.0),
            ("negative_week", -2.4),
            ("worst_week_abs", -3.4),
            ("worst_day_abs", -1.3),
            ("top_day_share", -520.0),
            ("loss_streak", -18.0),
            ("hold_over_24h", -3.0),
            ("avg_open_over_target", -18.0),
            ("peak_open_over_target", -4.0),
            ("trade_over_target_pm", -0.55),
            ("constraint_violation", -420.0),
        ]),
    );
    objectives.insert(
        "stability".to_string(),
        weights(&[
            ("final_r", 0.4),
            ("maxdd_r", -9.0),
            ("positive_month", 20.0),
            ("negative_month", -360.0),
            ("negative_week", -4.5),
            ("worst_month_abs", -8.0),
            ("worst_week_abs", -7.0),
            ("worst_day_abs", -2.5),
            ("top_day_share", -950.0),
            ("loss_streak", -35.0),
            ("hold_over_24h", -5.0),
            ("avg_open_over_target", -30.0),
            ("peak_open_over_target", -6.0),
            ("trade_over_target_pm", -0.8),
            ("constraint_violation", -650.0),
        ]),
    );
    objectives.insert(
        "deployable".to_string(),
        weights(&[
            ("final_r", 0.52),
            ("maxdd_r", -8.5),
            ("positive_month", 22.0),
            ("negative_month", -420.0),
            ("negative_week", -5.0),
            ("worst_month_abs", -10.0),
            ("worst_week_abs", -8.5),
            ("worst_day_abs", -3.0),
            ("top_day_share", -900.0),
            ("loss_streak", -42.0),
            ("hold_over_24h", -7.0),
            ("avg_open_over_target", -45.0),
            ("peak_open_over_target", -10.0),
            ("trade_over_target_pm", -1.0),
            ("constraint_violation", -850.0),
        ]),
    );
    objectives
}

fn weights(items: &[(&str, f64)]) -> BTreeMap<String, f64> {
    items
        .iter()
        .map(|(key, value)| ((*key).to_string(), *value))
        .collect()
}

struct PortfolioSearch {
    spec: PortfolioOptimizerSpec,
    candidates: Vec<OptimizerCandidate>,
    objectives: BTreeMap<String, BTreeMap<String, f64>>,
    by_id: HashMap<String, usize>,
    dates: Vec<String>,
    month_indexes: Vec<usize>,
    week_indexes: Vec<usize>,
    month_count: usize,
    week_count: usize,
    metrics_cache: HashMap<(String, bool, bool), Value>,
    score_cache: HashMap<(String, String), f64>,
    archive: HashMap<String, ArchiveItem>,
    pair_corr_matrix: Option<Vec<f64>>,
    positive_corr_cache: HashMap<String, f64>,
    sharpe_cache: HashMap<String, f64>,
}

struct TrialBase {
    daily: Vec<f64>,
    open_counts: Vec<i64>,
    closed_counts: Vec<i64>,
    months: Vec<f64>,
    weeks: Vec<f64>,
}

impl PortfolioSearch {
    fn new(
        inputs: Vec<OptimizerCandidateInput>,
        spec: PortfolioOptimizerSpec,
        objectives: BTreeMap<String, BTreeMap<String, f64>>,
    ) -> Self {
        let mut candidates: Vec<OptimizerCandidate> =
            inputs.into_iter().map(candidate_from_input).collect();
        candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    right
                        .final_r()
                        .partial_cmp(&left.final_r())
                        .unwrap_or(Ordering::Equal)
                })
        });
        if spec.candidate_limit > 0 {
            let protected_ids: HashSet<&str> = spec
                .baseline_attempt_ids
                .iter()
                .chain(spec.required_attempt_ids.iter())
                .map(String::as_str)
                .collect();
            let mut limited = Vec::new();
            let mut protected_seen = HashSet::new();
            for candidate in &candidates {
                if protected_ids.contains(candidate.attempt_id.as_str()) {
                    protected_seen.insert(candidate.attempt_id.clone());
                    limited.push(candidate.clone());
                }
            }
            for candidate in &candidates {
                if protected_seen.contains(&candidate.attempt_id) {
                    continue;
                }
                if limited.len() >= protected_seen.len() + spec.candidate_limit as usize {
                    break;
                }
                limited.push(candidate.clone());
            }
            candidates = limited;
        }

        let mut date_set = BTreeSet::new();
        for candidate in &candidates {
            for date in &candidate.dates {
                date_set.insert(date.clone());
            }
        }
        let dates: Vec<String> = date_set.into_iter().collect();
        let date_index: HashMap<String, usize> = dates
            .iter()
            .enumerate()
            .map(|(index, date)| (date.clone(), index))
            .collect();
        let (month_indexes, month_count) = calendar_bucket_indexes(&dates, "month");
        let (week_indexes, week_count) = calendar_bucket_indexes(&dates, "week");
        for candidate in &mut candidates {
            let mut vector = vec![0.0; dates.len()];
            let mut open_vector = vec![0; dates.len()];
            let mut closed_vector = vec![0; dates.len()];
            for index in 0..candidate.dates.len() {
                if let Some(aligned_index) = date_index.get(&candidate.dates[index]) {
                    vector[*aligned_index] += *candidate.daily_r.get(index).unwrap_or(&0.0);
                    open_vector[*aligned_index] += *candidate.open_counts.get(index).unwrap_or(&0);
                    closed_vector[*aligned_index] +=
                        *candidate.closed_counts.get(index).unwrap_or(&0);
                }
            }
            candidate.vector = vector;
            candidate.open_vector = open_vector;
            candidate.closed_vector = closed_vector;
            candidate.month_vector =
                group_by_indexes(&candidate.vector, &month_indexes, month_count);
            candidate.week_vector = group_by_indexes(&candidate.vector, &week_indexes, week_count);
        }
        let by_id: HashMap<String, usize> = candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| (candidate.attempt_id.clone(), index))
            .collect();
        let pair_corr_matrix = if spec.correlation_penalty_weight > 0.0 {
            let candidate_count = candidates.len();
            let mut matrix = vec![0.0; candidate_count * candidate_count];
            matrix
                .par_chunks_mut(candidate_count.max(1))
                .enumerate()
                .for_each(|(left_index, row)| {
                    for right_index in (left_index + 1)..candidate_count {
                        row[right_index] = pearson_corr(
                            &candidates[left_index].vector,
                            &candidates[right_index].vector,
                        );
                    }
                });
            Some(matrix)
        } else {
            None
        };
        Self {
            spec,
            candidates,
            objectives,
            by_id,
            dates,
            month_indexes,
            week_indexes,
            month_count,
            week_count,
            metrics_cache: HashMap::new(),
            score_cache: HashMap::new(),
            archive: HashMap::new(),
            pair_corr_matrix,
            positive_corr_cache: HashMap::new(),
            sharpe_cache: HashMap::new(),
        }
    }

    fn optimize(&mut self) -> BTreeMap<String, OptimizerVariant> {
        let mut rng = PythonRandom::new(self.spec.random_seed);
        let mut variants = BTreeMap::new();
        let baseline_seed = self.ids_for_known_attempts(&self.spec.baseline_attempt_ids.clone());
        for objective_name in self.spec.objective_names.clone() {
            if !self.objectives.contains_key(&objective_name) {
                continue;
            }
            let mut starts: Vec<(String, Vec<String>)> = Vec::new();
            if !baseline_seed.is_empty() {
                starts.push(("baseline".to_string(), baseline_seed.clone()));
            }
            starts.push(("greedy".to_string(), self.greedy_seed(&objective_name)));
            for index in 0..self.spec.random_starts {
                if let Some(seed) = self.random_seed(&mut rng) {
                    starts.push((format!("random_{}", index + 1), seed));
                }
            }

            let mut best_ids = Vec::new();
            let mut best_score = f64::NEG_INFINITY;
            let mut best_swaps = Vec::new();
            let mut best_start = String::new();
            for (start_name, start_ids) in starts {
                let (selected, swaps) =
                    self.improve_by_swaps(start_ids, &objective_name, &start_name);
                let score = self.objective_score(&selected, &objective_name);
                self.record_archive(
                    &selected,
                    &objective_name,
                    &format!("{start_name}:final"),
                    Some(score),
                );
                if score > best_score {
                    best_score = score;
                    best_ids = selected;
                    best_swaps = swaps;
                    best_start = start_name;
                }
            }

            let avg_positive_corr = self.avg_positive_pair_corr(&best_ids);
            let portfolio_sharpe = self.portfolio_sharpe(&best_ids);
            let diversification_mode = self.spec.diversification_mode.to_lowercase();
            let mut diversification = BTreeMap::new();
            diversification.insert("mode".to_string(), json!(diversification_mode));
            diversification.insert(
                "correlation_penalty_weight".to_string(),
                json!(self.spec.correlation_penalty_weight),
            );
            diversification.insert(
                "portfolio_sharpe_weight".to_string(),
                json!(self.spec.portfolio_sharpe_weight),
            );
            diversification.insert(
                "avg_positive_pair_corr".to_string(),
                json!(avg_positive_corr),
            );
            diversification.insert(
                "correlation_penalty".to_string(),
                json!(self.spec.correlation_penalty_weight * avg_positive_corr),
            );
            diversification.insert("portfolio_sharpe".to_string(), json!(portfolio_sharpe));
            diversification.insert(
                "portfolio_sharpe_term".to_string(),
                json!(if diversification_mode == "marginal_sharpe" {
                    self.spec.portfolio_sharpe_weight * portfolio_sharpe
                } else {
                    0.0
                }),
            );
            let selected = best_ids
                .iter()
                .enumerate()
                .map(|(index, attempt_id)| self.candidate_row(attempt_id, Some(index + 1)))
                .collect();
            let metrics = self.metrics(&best_ids, true, true);
            variants.insert(
                objective_name.clone(),
                OptimizerVariant {
                    objective_name,
                    objective_score: best_score,
                    start: best_start,
                    selected_attempt_ids: best_ids,
                    swaps: best_swaps,
                    diversification,
                    metrics,
                    selected,
                },
            );
        }
        variants
    }

    fn ids_for_known_attempts(&self, attempt_ids: &[String]) -> Vec<String> {
        attempt_ids
            .iter()
            .filter(|attempt_id| self.by_id.contains_key(*attempt_id))
            .cloned()
            .collect()
    }

    fn cache_key(&self, selected_ids: &[String]) -> String {
        let mut ids: Vec<&str> = selected_ids
            .iter()
            .filter(|attempt_id| self.by_id.contains_key(*attempt_id))
            .map(String::as_str)
            .collect();
        ids.sort_unstable();
        ids.join("\u{1f}")
    }

    fn combine_vectors(&self, selected_ids: &[String]) -> (Vec<f64>, Vec<i64>, Vec<i64>) {
        let mut daily = vec![0.0; self.dates.len()];
        let mut open_counts = vec![0; self.dates.len()];
        let mut closed_counts = vec![0; self.dates.len()];
        for attempt_id in selected_ids {
            if let Some(candidate) = self.candidate(attempt_id) {
                for index in 0..self.dates.len() {
                    daily[index] += candidate.vector[index];
                    open_counts[index] += candidate.open_vector[index];
                    closed_counts[index] += candidate.closed_vector[index];
                }
            }
        }
        (daily, open_counts, closed_counts)
    }

    fn trial_base(&self, selected_ids: &[String], removed_index: Option<usize>) -> TrialBase {
        let mut daily = vec![0.0; self.dates.len()];
        let mut open_counts = vec![0; self.dates.len()];
        let mut closed_counts = vec![0; self.dates.len()];
        for (position, attempt_id) in selected_ids.iter().enumerate() {
            if removed_index == Some(position) {
                continue;
            }
            if let Some(candidate) = self.candidate(attempt_id) {
                for index in 0..self.dates.len() {
                    daily[index] += candidate.vector[index];
                    open_counts[index] += candidate.open_vector[index];
                    closed_counts[index] += candidate.closed_vector[index];
                }
            }
        }
        let months = group_by_indexes(&daily, &self.month_indexes, self.month_count);
        let weeks = group_by_indexes(&daily, &self.week_indexes, self.week_count);
        TrialBase {
            daily,
            open_counts,
            closed_counts,
            months,
            weeks,
        }
    }

    fn constraint_violation_size_from_stats(
        &self,
        selected_ids: &[&str],
        avg_open_positions: f64,
        peak_open_positions: f64,
        trades_per_month: f64,
    ) -> f64 {
        let mut instrument_counts: HashMap<&str, f64> = HashMap::new();
        let mut family_counts: HashMap<&str, usize> = HashMap::new();
        let mut fx_share = 0.0;
        let mut metal_share = 0.0;
        let mut index_share = 0.0;
        for attempt_id in selected_ids {
            let Some(candidate) = self.candidate(attempt_id) else {
                continue;
            };
            *family_counts.entry(candidate.family.as_str()).or_insert(0) += 1;
            let share = 1.0 / candidate.instruments.len().max(1) as f64;
            for instrument in &candidate.instruments {
                *instrument_counts.entry(instrument.as_str()).or_insert(0.0) += share;
                match instrument_asset_class(instrument).as_str() {
                    "fx" => fx_share += share,
                    "metal" => metal_share += share,
                    "index" => index_share += share,
                    _ => {}
                }
            }
        }
        let mut violation_size = selected_ids.len().abs_diff(self.spec.portfolio_size) as f64;
        let unique_count = selected_ids.iter().copied().collect::<HashSet<_>>().len();
        violation_size += selected_ids.len().saturating_sub(unique_count) as f64;
        violation_size += (instrument_counts.values().copied().fold(0.0, f64::max)
            - self.spec.max_instrument_share)
            .max(0.0);
        violation_size += (family_counts.values().copied().max().unwrap_or(0) as f64
            - self.spec.max_per_family as f64)
            .max(0.0);
        violation_size += (self.spec.min_fx_share - fx_share).max(0.0);
        violation_size += (metal_share - self.spec.max_metal_share).max(0.0);
        violation_size += (index_share - self.spec.max_index_share).max(0.0);
        if self.spec.max_avg_open_positions > 0.0 {
            violation_size += (avg_open_positions - self.spec.max_avg_open_positions).max(0.0);
        }
        if self.spec.max_peak_open_positions > 0.0 {
            violation_size += (peak_open_positions - self.spec.max_peak_open_positions).max(0.0);
        }
        if self.spec.max_trades_per_month > 0.0 {
            violation_size += (trades_per_month - self.spec.max_trades_per_month).max(0.0);
        }
        violation_size
    }

    fn avg_positive_pair_corr_refs(&self, selected_ids: &[&str]) -> f64 {
        let mut ids = selected_ids
            .iter()
            .copied()
            .filter(|attempt_id| self.by_id.contains_key(*attempt_id))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        let mut total = 0.0;
        let mut pairs = 0usize;
        for left_index in 0..ids.len() {
            for right_id in ids.iter().skip(left_index + 1) {
                total += self.pair_corr(ids[left_index], right_id).max(0.0);
                pairs += 1;
            }
        }
        if pairs > 0 { total / pairs as f64 } else { 0.0 }
    }

    fn objective_score_extension(
        &self,
        selected_ids: &[String],
        removed_index: Option<usize>,
        added_id: &str,
        weights: &BTreeMap<String, f64>,
        base: &TrialBase,
    ) -> f64 {
        let Some(added) = self.candidate(added_id) else {
            return f64::NEG_INFINITY;
        };
        let mut trial_ids = Vec::with_capacity(selected_ids.len() + 1);
        for (position, attempt_id) in selected_ids.iter().enumerate() {
            if removed_index != Some(position) {
                trial_ids.push(attempt_id.as_str());
            }
        }
        trial_ids.push(added_id);

        let mut final_r = 0.0;
        let mut sumsq = 0.0;
        let mut equity: f64 = 0.0;
        let mut peak: f64 = 0.0;
        let mut maxdd_r: f64 = 0.0;
        let mut positive_day_gain = 0.0;
        let mut best_day_r = f64::NEG_INFINITY;
        let mut worst_day_r = f64::INFINITY;
        let mut current_loss_streak = 0usize;
        let mut max_loss_streak = 0usize;
        let mut open_total = 0i64;
        let mut peak_open_positions = 0i64;
        let mut total_closed_trades = 0i64;
        for index in 0..self.dates.len() {
            let value = base.daily[index] + added.vector[index];
            final_r += value;
            sumsq += value * value;
            equity += value;
            peak = peak.max(equity);
            maxdd_r = maxdd_r.max(peak - equity);
            positive_day_gain += value.max(0.0);
            best_day_r = best_day_r.max(value);
            worst_day_r = worst_day_r.min(value);
            if value < -1e-9 {
                current_loss_streak += 1;
                max_loss_streak = max_loss_streak.max(current_loss_streak);
            } else {
                current_loss_streak = 0;
            }
            let open_count = base.open_counts[index] + added.open_vector[index];
            open_total += open_count;
            peak_open_positions = peak_open_positions.max(open_count);
            total_closed_trades += base.closed_counts[index] + added.closed_vector[index];
        }
        if self.dates.is_empty() {
            best_day_r = 0.0;
            worst_day_r = 0.0;
        }

        let mut pos_months = 0usize;
        let mut neg_months = 0usize;
        let mut worst_month_r = f64::INFINITY;
        for (index, base_value) in base.months.iter().enumerate() {
            let value = base_value + added.month_vector[index];
            if value > 1e-9 {
                pos_months += 1;
            } else if value < -1e-9 {
                neg_months += 1;
            }
            worst_month_r = worst_month_r.min(value);
        }
        if base.months.is_empty() {
            worst_month_r = 0.0;
        }
        let mut neg_weeks = 0usize;
        let mut worst_week_r = f64::INFINITY;
        for (index, base_value) in base.weeks.iter().enumerate() {
            let value = base_value + added.week_vector[index];
            if value < -1e-9 {
                neg_weeks += 1;
            }
            worst_week_r = worst_week_r.min(value);
        }
        if base.weeks.is_empty() {
            worst_week_r = 0.0;
        }

        let day_count = self.dates.len();
        let avg_open_positions = if day_count > 0 {
            open_total as f64 / day_count as f64
        } else {
            0.0
        };
        let trades_per_month = if self.month_count > 0 {
            total_closed_trades as f64 / self.month_count as f64
        } else {
            0.0
        };
        let mean_avg_hold_hours = if trial_ids.is_empty() {
            0.0
        } else {
            trial_ids
                .iter()
                .filter_map(|attempt_id| self.candidate(attempt_id))
                .map(|candidate| candidate.avg_hold_hours)
                .sum::<f64>()
                / trial_ids.len() as f64
        };
        let violation_size = self.constraint_violation_size_from_stats(
            &trial_ids,
            avg_open_positions,
            peak_open_positions as f64,
            trades_per_month,
        );
        let top_day_gain_share = if positive_day_gain > 0.0 {
            best_day_r / positive_day_gain
        } else {
            1.0
        };

        let mut score = 0.0;
        score += weight(weights, "final_r") * final_r;
        score += weight(weights, "maxdd_r") * maxdd_r;
        score += weight(weights, "positive_month") * pos_months as f64;
        score += weight(weights, "negative_month") * neg_months as f64;
        score += weight(weights, "negative_week") * neg_weeks as f64;
        score += weight(weights, "worst_month_abs") * worst_month_r.min(0.0).abs();
        score += weight(weights, "worst_week_abs") * worst_week_r.min(0.0).abs();
        score += weight(weights, "worst_day_abs") * worst_day_r.min(0.0).abs();
        score += weight(weights, "top_day_share") * top_day_gain_share;
        score += weight(weights, "loss_streak") * max_loss_streak as f64;
        score += weight(weights, "hold_over_24h") * (mean_avg_hold_hours - 24.0).max(0.0);
        score += weight(weights, "avg_open_position") * avg_open_positions;
        score += weight(weights, "peak_open_position") * peak_open_positions as f64;
        score += weight(weights, "avg_open_over_target")
            * (avg_open_positions - self.spec.max_avg_open_positions).max(0.0);
        score += weight(weights, "peak_open_over_target")
            * (peak_open_positions as f64 - self.spec.max_peak_open_positions).max(0.0);
        score += weight(weights, "trade_over_target_pm")
            * (trades_per_month - self.spec.target_trades_per_month).max(0.0);
        score += weight(weights, "constraint_violation") * violation_size;
        if self.spec.correlation_penalty_weight > 0.0 {
            score -=
                self.spec.correlation_penalty_weight * self.avg_positive_pair_corr_refs(&trial_ids);
        }
        if self.spec.diversification_mode.to_lowercase() == "marginal_sharpe"
            && self.spec.portfolio_sharpe_weight > 0.0
            && day_count >= 2
        {
            let mean = final_r / day_count as f64;
            let variance = ((sumsq / day_count as f64) - (mean * mean)).max(0.0);
            let std = variance.sqrt();
            let sharpe = if std > 1e-12 { mean / std } else { 0.0 };
            score += self.spec.portfolio_sharpe_weight * sharpe;
        }
        score
    }

    fn candidate(&self, attempt_id: &str) -> Option<&OptimizerCandidate> {
        self.by_id
            .get(attempt_id)
            .and_then(|index| self.candidates.get(*index))
    }

    fn pair_corr(&self, left_id: &str, right_id: &str) -> f64 {
        if left_id == right_id {
            return 1.0;
        }
        let Some(&left_index) = self.by_id.get(left_id) else {
            return 0.0;
        };
        let Some(&right_index) = self.by_id.get(right_id) else {
            return 0.0;
        };
        let (lower, upper) = if left_index < right_index {
            (left_index, right_index)
        } else {
            (right_index, left_index)
        };
        if let Some(matrix) = &self.pair_corr_matrix {
            return matrix[lower * self.candidates.len() + upper];
        }
        match (self.candidates.get(lower), self.candidates.get(upper)) {
            (Some(left), Some(right)) => pearson_corr(&left.vector, &right.vector),
            _ => 0.0,
        }
    }

    fn avg_positive_pair_corr(&mut self, selected_ids: &[String]) -> f64 {
        let mut ids = unique_known_ids(selected_ids, &self.by_id);
        ids.sort_unstable();
        let key = ids.join("\u{1f}");
        if let Some(value) = self.positive_corr_cache.get(&key) {
            return *value;
        }
        let mut total = 0.0;
        let mut pairs = 0.0;
        for left_index in 0..ids.len() {
            for right_id in ids.iter().skip(left_index + 1) {
                total += self.pair_corr(&ids[left_index], right_id).max(0.0);
                pairs += 1.0;
            }
        }
        let value = if pairs > 0.0 { total / pairs } else { 0.0 };
        self.positive_corr_cache.insert(key, value);
        value
    }

    fn portfolio_sharpe(&mut self, selected_ids: &[String]) -> f64 {
        let mut ids = unique_known_ids(selected_ids, &self.by_id);
        ids.sort_unstable();
        let key = ids.join("\u{1f}");
        if let Some(value) = self.sharpe_cache.get(&key) {
            return *value;
        }
        let value = if ids.is_empty() || self.dates.len() < 2 {
            0.0
        } else {
            let (daily, _, _) = self.combine_vectors(&ids);
            let total: f64 = daily.iter().sum();
            let day_count = self.dates.len() as f64;
            let mean = total / day_count;
            let sumsq: f64 = daily.iter().map(|value| value * value).sum();
            let variance = ((sumsq / day_count) - (mean * mean)).max(0.0);
            let std = variance.sqrt();
            if std > 1e-12 { mean / std } else { 0.0 }
        };
        self.sharpe_cache.insert(key, value);
        value
    }

    fn diversification_adjustment(&mut self, selected_ids: &[String]) -> f64 {
        let mut adjustment = 0.0;
        if self.spec.correlation_penalty_weight > 0.0 {
            adjustment -=
                self.spec.correlation_penalty_weight * self.avg_positive_pair_corr(selected_ids);
        }
        if self.spec.diversification_mode.to_lowercase() == "marginal_sharpe"
            && self.spec.portfolio_sharpe_weight > 0.0
        {
            adjustment += self.spec.portfolio_sharpe_weight * self.portfolio_sharpe(selected_ids);
        }
        adjustment
    }

    fn exposure_counts(
        &self,
        selected_ids: &[String],
    ) -> (
        BTreeMap<String, f64>,
        BTreeMap<String, f64>,
        BTreeMap<String, i64>,
    ) {
        let mut instrument_counts = BTreeMap::new();
        let mut asset_counts = BTreeMap::new();
        let mut family_counts = BTreeMap::new();
        for attempt_id in selected_ids {
            let Some(candidate) = self.candidate(attempt_id) else {
                continue;
            };
            *family_counts.entry(candidate.family.clone()).or_insert(0) += 1;
            let share = 1.0 / candidate.instruments.len().max(1) as f64;
            for instrument in &candidate.instruments {
                *instrument_counts.entry(instrument.clone()).or_insert(0.0) += share;
                *asset_counts
                    .entry(instrument_asset_class(instrument))
                    .or_insert(0.0) += share;
            }
        }
        (instrument_counts, asset_counts, family_counts)
    }

    fn constraint_violations(&self, selected_ids: &[String]) -> BTreeMap<String, f64> {
        let (instrument_counts, asset_counts, family_counts) = self.exposure_counts(selected_ids);
        let (daily, open_counts, closed_counts) = self.combine_vectors(selected_ids);
        let month_count = group_values(&self.dates, &daily, "month").len();
        let trades_per_month = if month_count > 0 {
            closed_counts.iter().sum::<i64>() as f64 / month_count as f64
        } else {
            0.0
        };
        let avg_open_positions = if open_counts.is_empty() {
            0.0
        } else {
            open_counts.iter().sum::<i64>() as f64 / open_counts.len() as f64
        };
        let peak_open_positions = open_counts.iter().max().copied().unwrap_or(0) as f64;
        let mut violations = BTreeMap::new();
        if selected_ids.len() != self.spec.portfolio_size {
            violations.insert(
                "portfolio_size".to_string(),
                selected_ids.len().abs_diff(self.spec.portfolio_size) as f64,
            );
        }
        let duplicate_count = selected_ids.len() - unique_count(selected_ids);
        if duplicate_count > 0 {
            violations.insert("duplicate_attempts".to_string(), duplicate_count as f64);
        }
        let max_instrument = instrument_counts.values().copied().fold(0.0, f64::max);
        if max_instrument > self.spec.max_instrument_share {
            violations.insert(
                "instrument_share".to_string(),
                max_instrument - self.spec.max_instrument_share,
            );
        }
        let max_family = family_counts.values().copied().max().unwrap_or(0) as usize;
        if max_family > self.spec.max_per_family {
            violations.insert(
                "family_cap".to_string(),
                (max_family - self.spec.max_per_family) as f64,
            );
        }
        let fx_share = *asset_counts.get("fx").unwrap_or(&0.0);
        if fx_share < self.spec.min_fx_share {
            violations.insert(
                "min_fx_share".to_string(),
                self.spec.min_fx_share - fx_share,
            );
        }
        let metal_share = *asset_counts.get("metal").unwrap_or(&0.0);
        if metal_share > self.spec.max_metal_share {
            violations.insert(
                "max_metal_share".to_string(),
                metal_share - self.spec.max_metal_share,
            );
        }
        let index_share = *asset_counts.get("index").unwrap_or(&0.0);
        if index_share > self.spec.max_index_share {
            violations.insert(
                "max_index_share".to_string(),
                index_share - self.spec.max_index_share,
            );
        }
        if self.spec.max_avg_open_positions > 0.0
            && avg_open_positions > self.spec.max_avg_open_positions
        {
            violations.insert(
                "max_avg_open_positions".to_string(),
                avg_open_positions - self.spec.max_avg_open_positions,
            );
        }
        if self.spec.max_peak_open_positions > 0.0
            && peak_open_positions > self.spec.max_peak_open_positions
        {
            violations.insert(
                "max_peak_open_positions".to_string(),
                peak_open_positions - self.spec.max_peak_open_positions,
            );
        }
        if self.spec.max_trades_per_month > 0.0 && trades_per_month > self.spec.max_trades_per_month
        {
            violations.insert(
                "max_trades_per_month".to_string(),
                trades_per_month - self.spec.max_trades_per_month,
            );
        }
        violations
    }

    fn metrics(
        &mut self,
        selected_ids: &[String],
        include_correlation: bool,
        include_account: bool,
    ) -> Value {
        let selected_ids: Vec<String> = selected_ids
            .iter()
            .filter(|attempt_id| self.by_id.contains_key(*attempt_id))
            .cloned()
            .collect();
        let cache_key = (
            self.cache_key(&selected_ids),
            include_correlation,
            include_account,
        );
        if let Some(cached) = self.metrics_cache.get(&cache_key) {
            return cached.clone();
        }
        let (daily, open_counts, closed_counts) = self.combine_vectors(&selected_ids);
        let months = group_values(&self.dates, &daily, "month");
        let weeks = group_values(&self.dates, &daily, "week");
        let days: BTreeMap<String, f64> = self
            .dates
            .iter()
            .cloned()
            .zip(daily.iter().copied())
            .collect();
        let (pos_months, neg_months, flat_months) =
            count_positive_negative_flat(months.values().copied());
        let (pos_weeks, neg_weeks, flat_weeks) =
            count_positive_negative_flat(weeks.values().copied());
        let (pos_days, neg_days, flat_days) = count_positive_negative_flat(days.values().copied());
        let best_day = best_pair(&days, true);
        let worst_day = best_pair(&days, false);
        let best_week = best_pair(&weeks, true);
        let worst_week = best_pair(&weeks, false);
        let best_month = best_pair(&months, true);
        let worst_month = best_pair(&months, false);
        let final_r: f64 = daily.iter().sum();
        let maxdd = max_drawdown(&daily);
        let positive_day_gain: f64 = daily.iter().map(|value| value.max(0.0)).sum();
        let (max_loss_streak, avg_loss_streak) = loss_streak(&daily);
        let (instrument_counts, asset_counts, family_counts) = self.exposure_counts(&selected_ids);
        let average_holds: Vec<f64> = selected_ids
            .iter()
            .filter_map(|attempt_id| self.candidate(attempt_id))
            .map(|candidate| candidate.avg_hold_hours)
            .collect();
        let p90_holds: Vec<f64> = selected_ids
            .iter()
            .filter_map(|attempt_id| self.candidate(attempt_id)?.p90_hold_hours)
            .collect();
        let max_holds: Vec<f64> = selected_ids
            .iter()
            .filter_map(|attempt_id| self.candidate(attempt_id)?.max_hold_hours)
            .collect();

        let mut result = Map::new();
        result.insert("count".to_string(), json!(selected_ids.len()));
        result.insert("final_r".to_string(), json!(final_r));
        result.insert("maxdd_r".to_string(), json!(maxdd));
        result.insert(
            "return_to_dd".to_string(),
            if maxdd > 0.0 {
                json!(final_r / maxdd)
            } else {
                Value::Null
            },
        );
        result.insert("month_count".to_string(), json!(months.len()));
        result.insert("week_count".to_string(), json!(weeks.len()));
        result.insert("pos_months".to_string(), json!(pos_months));
        result.insert("neg_months".to_string(), json!(neg_months));
        result.insert("flat_months".to_string(), json!(flat_months));
        result.insert("worst_month".to_string(), json!(worst_month.0));
        result.insert("worst_month_r".to_string(), json!(worst_month.1));
        result.insert("best_month".to_string(), json!(best_month.0));
        result.insert("best_month_r".to_string(), json!(best_month.1));
        result.insert("pos_weeks".to_string(), json!(pos_weeks));
        result.insert("neg_weeks".to_string(), json!(neg_weeks));
        result.insert("flat_weeks".to_string(), json!(flat_weeks));
        result.insert("worst_week".to_string(), json!(worst_week.0));
        result.insert("worst_week_r".to_string(), json!(worst_week.1));
        result.insert("best_week".to_string(), json!(best_week.0));
        result.insert("best_week_r".to_string(), json!(best_week.1));
        result.insert("pos_days".to_string(), json!(pos_days));
        result.insert("neg_days".to_string(), json!(neg_days));
        result.insert("flat_days".to_string(), json!(flat_days));
        result.insert("worst_day".to_string(), json!(worst_day.0));
        result.insert("worst_day_r".to_string(), json!(worst_day.1));
        result.insert("best_day".to_string(), json!(best_day.0));
        result.insert("best_day_r".to_string(), json!(best_day.1));
        result.insert("positive_day_gain_r".to_string(), json!(positive_day_gain));
        result.insert(
            "top_day_gain_share".to_string(),
            json!(if positive_day_gain > 0.0 {
                best_day.1 / positive_day_gain
            } else {
                1.0
            }),
        );
        result.insert("max_daily_loss_streak".to_string(), json!(max_loss_streak));
        result.insert("avg_daily_loss_streak".to_string(), json!(avg_loss_streak));
        result.insert(
            "avg_open_positions".to_string(),
            json!(if open_counts.is_empty() {
                0.0
            } else {
                open_counts.iter().sum::<i64>() as f64 / open_counts.len() as f64
            }),
        );
        result.insert(
            "peak_open_positions".to_string(),
            json!(open_counts.iter().max().copied().unwrap_or(0)),
        );
        result.insert(
            "total_closed_trades".to_string(),
            json!(closed_counts.iter().sum::<i64>()),
        );
        result.insert(
            "trades_per_month".to_string(),
            json!(if months.is_empty() {
                0.0
            } else {
                closed_counts.iter().sum::<i64>() as f64 / months.len() as f64
            }),
        );
        result.insert("instrument_counts".to_string(), json!(instrument_counts));
        result.insert("asset_class_counts".to_string(), json!(asset_counts));
        result.insert("family_counts".to_string(), json!(family_counts));
        result.insert(
            "mean_avg_hold_hours".to_string(),
            json!(mean_or_zero(&average_holds)),
        );
        result.insert(
            "max_avg_hold_hours".to_string(),
            json!(average_holds.iter().copied().fold(0.0, f64::max)),
        );
        result.insert(
            "max_p90_hold_hours".to_string(),
            json!(p90_holds.iter().copied().fold(0.0, f64::max)),
        );
        result.insert(
            "max_single_trade_hold_hours".to_string(),
            json!(max_holds.iter().copied().fold(0.0, f64::max)),
        );
        result.insert(
            "constraint_violations".to_string(),
            json!(self.constraint_violations(&selected_ids)),
        );
        if include_account && !self.spec.account.is_empty() {
            result.insert(
                "account_initial".to_string(),
                json!(self.account_simulation(&selected_ids, "initial", None)),
            );
            result.insert(
                "account_current".to_string(),
                json!(self.account_simulation(&selected_ids, "current", None)),
            );
        }
        if include_correlation {
            let unique_ids = unique_known_ids(&selected_ids, &self.by_id);
            let mut correlations = Vec::new();
            for left_index in 0..unique_ids.len() {
                for right_id in unique_ids.iter().skip(left_index + 1) {
                    correlations.push(self.pair_corr(&unique_ids[left_index], right_id));
                }
            }
            result.insert(
                "avg_pair_corr".to_string(),
                json!(mean_or_zero(&correlations)),
            );
            result.insert(
                "max_pair_corr".to_string(),
                json!(max_or_zero(&correlations)),
            );
            result.insert(
                "avg_positive_pair_corr".to_string(),
                json!(self.avg_positive_pair_corr(&selected_ids)),
            );
            result.insert(
                "portfolio_sharpe".to_string(),
                json!(self.portfolio_sharpe(&selected_ids)),
            );
        }
        let value = Value::Object(result);
        self.metrics_cache.insert(cache_key, value.clone());
        value
    }

    fn objective_score(&mut self, selected_ids: &[String], objective_name: &str) -> f64 {
        let score_key = (objective_name.to_string(), self.cache_key(selected_ids));
        if let Some(cached) = self.score_cache.get(&score_key) {
            return *cached;
        }
        let Some(weights) = self.objectives.get(objective_name).cloned() else {
            return f64::NEG_INFINITY;
        };
        let metrics = self.metrics(selected_ids, false, false);
        let violation_size: f64 = metrics
            .get("constraint_violations")
            .and_then(Value::as_object)
            .map(|items| items.values().filter_map(Value::as_f64).sum())
            .unwrap_or(0.0);
        let mut score = 0.0;
        score += weight(&weights, "final_r") * metric_f64(&metrics, "final_r");
        score += weight(&weights, "maxdd_r") * metric_f64(&metrics, "maxdd_r");
        score += weight(&weights, "positive_month") * metric_f64(&metrics, "pos_months");
        score += weight(&weights, "negative_month") * metric_f64(&metrics, "neg_months");
        score += weight(&weights, "negative_week") * metric_f64(&metrics, "neg_weeks");
        score += weight(&weights, "worst_month_abs")
            * metric_f64(&metrics, "worst_month_r").min(0.0).abs();
        score += weight(&weights, "worst_week_abs")
            * metric_f64(&metrics, "worst_week_r").min(0.0).abs();
        score +=
            weight(&weights, "worst_day_abs") * metric_f64(&metrics, "worst_day_r").min(0.0).abs();
        score += weight(&weights, "top_day_share") * metric_f64(&metrics, "top_day_gain_share");
        score += weight(&weights, "loss_streak") * metric_f64(&metrics, "max_daily_loss_streak");
        score += weight(&weights, "hold_over_24h")
            * (metric_f64(&metrics, "mean_avg_hold_hours") - 24.0).max(0.0);
        score += weight(&weights, "avg_open_position") * metric_f64(&metrics, "avg_open_positions");
        score +=
            weight(&weights, "peak_open_position") * metric_f64(&metrics, "peak_open_positions");
        score += weight(&weights, "avg_open_over_target")
            * (metric_f64(&metrics, "avg_open_positions") - self.spec.max_avg_open_positions)
                .max(0.0);
        score += weight(&weights, "peak_open_over_target")
            * (metric_f64(&metrics, "peak_open_positions") - self.spec.max_peak_open_positions)
                .max(0.0);
        score += weight(&weights, "trade_over_target_pm")
            * (metric_f64(&metrics, "trades_per_month") - self.spec.target_trades_per_month)
                .max(0.0);
        score += weight(&weights, "constraint_violation") * violation_size;
        score += self.diversification_adjustment(selected_ids);
        self.score_cache.insert(score_key, score);
        score
    }

    fn record_archive(
        &mut self,
        selected_ids: &[String],
        objective_name: &str,
        label: &str,
        objective_score: Option<f64>,
    ) {
        let ids: Vec<String> = selected_ids
            .iter()
            .filter(|attempt_id| self.by_id.contains_key(*attempt_id))
            .cloned()
            .collect();
        let key = self.cache_key(&ids);
        if key.is_empty() {
            return;
        }
        let objective_names: Vec<String> = self.objectives.keys().cloned().collect();
        let mut scores = BTreeMap::new();
        for name in objective_names {
            scores.insert(name.clone(), self.objective_score(&ids, &name));
        }
        let row = ArchiveItem {
            archive_label: label.to_string(),
            objective_name: objective_name.to_string(),
            objective_score: objective_score
                .unwrap_or_else(|| *scores.get(objective_name).unwrap_or(&f64::NEG_INFINITY)),
            objective_scores: scores,
            selected_attempt_ids: ids.clone(),
            metrics: self.metrics(&ids, false, true),
        };
        if let Some(existing) = self.archive.get(&key) {
            if row.objective_score <= existing.objective_score {
                return;
            }
        }
        self.archive.insert(key, row);
    }

    fn candidate_row(&self, attempt_id: &str, rank: Option<usize>) -> BTreeMap<String, Value> {
        let Some(candidate) = self.candidate(attempt_id) else {
            return BTreeMap::new();
        };
        let mut row = BTreeMap::new();
        if let Some(rank) = rank {
            row.insert("rank".to_string(), json!(rank));
        }
        row.insert("attempt_id".to_string(), json!(candidate.attempt_id));
        row.insert(
            "candidate_name".to_string(),
            json!(candidate.candidate_name),
        );
        row.insert("run_id".to_string(), json!(candidate.run_id));
        row.insert("created_at".to_string(), json!(candidate.created_at));
        row.insert(
            "instruments".to_string(),
            json!(candidate.instruments.join("|")),
        );
        row.insert(
            "asset_class".to_string(),
            json!(candidate.primary_asset_class),
        );
        row.insert("score".to_string(), json!(round6(candidate.score)));
        row.insert("final_r".to_string(), json!(round6(candidate.final_r())));
        row.insert("maxdd_r".to_string(), json!(round6(candidate.maxdd_r())));
        row.insert(
            "avg_holding_hours".to_string(),
            json!(round6(candidate.avg_hold_hours)),
        );
        row.insert(
            "p90_holding_hours".to_string(),
            json!(candidate.p90_hold_hours.map(round6)),
        );
        row.insert(
            "max_holding_hours".to_string(),
            json!(candidate.max_hold_hours.map(round6)),
        );
        row.insert(
            "trades_per_month".to_string(),
            json!(round6(candidate.trades_per_month)),
        );
        row.insert("trade_count".to_string(), json!(candidate.trade_count));
        row.insert("family".to_string(), json!(candidate.family));
        row.insert("path_quality".to_string(), json!(candidate.path_quality));
        row
    }

    fn greedy_seed(&mut self, objective_name: &str) -> Vec<String> {
        let mut selected = self.ids_for_known_attempts(&self.spec.required_attempt_ids.clone());
        let pool: Vec<String> = self
            .candidates
            .iter()
            .map(|candidate| candidate.attempt_id.clone())
            .collect();
        let Some(weights) = self.objectives.get(objective_name).cloned() else {
            return selected;
        };
        while selected.len() < self.spec.portfolio_size {
            let base = self.trial_base(&selected, None);
            let selected_set: HashSet<&str> = selected.iter().map(String::as_str).collect();
            let scores: Vec<f64> = pool
                .par_iter()
                .map(|attempt_id| {
                    if selected_set.contains(attempt_id.as_str()) {
                        f64::NEG_INFINITY
                    } else {
                        self.objective_score_extension(&selected, None, attempt_id, &weights, &base)
                    }
                })
                .collect();
            let mut best_attempt_id: Option<String> = None;
            let mut best_score = f64::NEG_INFINITY;
            for (attempt_id, score) in pool.iter().zip(scores.iter()) {
                if selected_set.contains(attempt_id.as_str()) {
                    continue;
                }
                if *score > best_score {
                    best_score = *score;
                    best_attempt_id = Some(attempt_id.clone());
                }
            }
            let Some(attempt_id) = best_attempt_id else {
                break;
            };
            selected.push(attempt_id);
        }
        selected
    }

    fn random_seed(&self, rng: &mut PythonRandom) -> Option<Vec<String>> {
        if self.candidates.len() < self.spec.portfolio_size {
            return None;
        }
        let required = self.ids_for_known_attempts(&self.spec.required_attempt_ids.clone());
        if required.len() > self.spec.portfolio_size {
            return None;
        }
        let required_set: HashSet<&str> = required.iter().map(String::as_str).collect();
        let ids: Vec<String> = self
            .candidates
            .iter()
            .filter(|candidate| !required_set.contains(candidate.attempt_id.as_str()))
            .map(|candidate| candidate.attempt_id.clone())
            .collect();
        let sample_size = self.spec.portfolio_size - required.len();
        let mut best = None;
        let mut best_violation = f64::INFINITY;
        for _ in 0..500 {
            let mut sample = required.clone();
            sample.extend(rng.sample(&ids, sample_size));
            let violations = self.constraint_violations(&sample);
            let violation_size: f64 = violations.values().sum();
            if violation_size < best_violation {
                best = Some(sample.clone());
                best_violation = violation_size;
            }
            if violations.is_empty() {
                return Some(sample);
            }
        }
        best
    }

    fn improve_by_swaps(
        &mut self,
        seed_ids: Vec<String>,
        objective_name: &str,
        start_name: &str,
    ) -> (Vec<String>, Vec<SwapMove>) {
        let required = self.ids_for_known_attempts(&self.spec.required_attempt_ids.clone());
        let required_set: HashSet<&str> = required.iter().map(String::as_str).collect();
        let mut selected = required.clone();
        selected.extend(seed_ids);
        let mut selected = unique_preserve(selected);
        if selected.len() > self.spec.portfolio_size {
            selected.truncate(self.spec.portfolio_size);
        }
        if selected.len() < self.spec.portfolio_size {
            for candidate in &self.candidates {
                if !selected.contains(&candidate.attempt_id) {
                    selected.push(candidate.attempt_id.clone());
                }
                if selected.len() >= self.spec.portfolio_size {
                    break;
                }
            }
        }
        let mut best_score = self.objective_score(&selected, objective_name);
        self.record_archive(
            &selected,
            objective_name,
            &format!(
                "{}:seed",
                if start_name.is_empty() {
                    "seed"
                } else {
                    start_name
                }
            ),
            Some(best_score),
        );
        let mut swaps = Vec::new();
        let mut pool: Vec<String> = if self.spec.swap_candidate_limit > 0 {
            self.candidates
                .iter()
                .take(self.spec.swap_candidate_limit as usize)
                .map(|candidate| candidate.attempt_id.clone())
                .collect()
        } else {
            self.candidates
                .iter()
                .map(|candidate| candidate.attempt_id.clone())
                .collect()
        };
        for attempt_id in &selected {
            if !pool.contains(attempt_id) {
                pool.push(attempt_id.clone());
            }
        }
        let Some(weights) = self.objectives.get(objective_name).cloned() else {
            return (selected, swaps);
        };
        for _ in 0..self.spec.max_swaps {
            let selected_set: HashSet<&str> = selected.iter().map(String::as_str).collect();
            let bases: Vec<TrialBase> = (0..selected.len())
                .map(|removed_index| self.trial_base(&selected, Some(removed_index)))
                .collect();
            let trial_count = selected.len() * pool.len();
            let scores: Vec<f64> = (0..trial_count)
                .into_par_iter()
                .map(|flat_index| {
                    let removed_index = flat_index / pool.len();
                    let added = &pool[flat_index % pool.len()];
                    if required_set.contains(selected[removed_index].as_str())
                        || selected_set.contains(added.as_str())
                    {
                        f64::NEG_INFINITY
                    } else {
                        self.objective_score_extension(
                            &selected,
                            Some(removed_index),
                            added,
                            &weights,
                            &bases[removed_index],
                        )
                    }
                })
                .collect();
            let mut best_flat_index = None;
            for (flat_index, score) in scores.iter().enumerate() {
                let added = &pool[flat_index % pool.len()];
                if selected_set.contains(added.as_str()) {
                    continue;
                }
                if *score > best_score + 1e-9 {
                    best_score = *score;
                    best_flat_index = Some(flat_index);
                }
            }
            let Some(flat_index) = best_flat_index else {
                break;
            };
            let removed_index = flat_index / pool.len();
            let removed = selected[removed_index].clone();
            let added = pool[flat_index % pool.len()].clone();
            let mut trial: Vec<String> = selected
                .iter()
                .enumerate()
                .filter(|(position, _)| *position != removed_index)
                .map(|(_, attempt_id)| attempt_id.clone())
                .collect();
            trial.push(added.clone());
            let score = self.objective_score(&trial, objective_name);
            best_score = score;
            swaps.push(SwapMove {
                removed,
                added,
                objective_after: score,
            });
            selected = trial;
            self.record_archive(
                &selected,
                objective_name,
                &format!(
                    "{}:swap_{}",
                    if start_name.is_empty() {
                        "seed"
                    } else {
                        start_name
                    },
                    swaps.len()
                ),
                Some(score),
            );
        }
        (selected, swaps)
    }

    fn pareto_front(&self, limit: usize) -> Vec<ArchiveItem> {
        let archived: Vec<ArchiveItem> = self
            .archive
            .values()
            .filter(|item| metric_f64(&item.metrics, "count") as usize == self.spec.portfolio_size)
            .cloned()
            .collect();
        let mut front = Vec::new();
        for item in &archived {
            if archived
                .iter()
                .any(|other| !std::ptr::eq(other, item) && Self::dominates(other, item))
            {
                continue;
            }
            front.push(item.clone());
        }
        front.sort_by(|left, right| {
            let left_balanced = *left
                .objective_scores
                .get("balanced")
                .unwrap_or(&f64::NEG_INFINITY);
            let right_balanced = *right
                .objective_scores
                .get("balanced")
                .unwrap_or(&f64::NEG_INFINITY);
            right_balanced
                .partial_cmp(&left_balanced)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    metric_f64(&right.metrics, "final_r")
                        .partial_cmp(&metric_f64(&left.metrics, "final_r"))
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| {
                    metric_f64(&left.metrics, "maxdd_r")
                        .partial_cmp(&metric_f64(&right.metrics, "maxdd_r"))
                        .unwrap_or(Ordering::Equal)
                })
        });
        front.truncate(limit.max(1));
        front
    }

    fn dominates(left: &ArchiveItem, right: &ArchiveItem) -> bool {
        let left_violations = has_constraint_violations(&left.metrics);
        let right_violations = has_constraint_violations(&right.metrics);
        if left_violations && !right_violations {
            return false;
        }
        if right_violations && !left_violations {
            return true;
        }
        let mut better = false;
        let epsilon = 1e-9;
        for (key, direction) in PARETO_DIMENSIONS {
            let left_value = metric_or_inf(&left.metrics, key);
            let right_value = metric_or_inf(&right.metrics, key);
            if *direction == "max" {
                if left_value < right_value - epsilon {
                    return false;
                }
                if left_value > right_value + epsilon {
                    better = true;
                }
            } else {
                if left_value > right_value + epsilon {
                    return false;
                }
                if left_value < right_value - epsilon {
                    better = true;
                }
            }
        }
        better
    }

    fn account_simulation(
        &self,
        selected_ids: &[String],
        risk_basis: &str,
        risk_pct: Option<f64>,
    ) -> BTreeMap<String, Value> {
        let account = &self.spec.account;
        let starting_balance = account_value(
            account,
            &[
                "balance_usd",
                "account_size_usd",
                "balance",
                "account_balance",
            ],
            None,
        );
        let configured_risk_pct = risk_pct.or_else(|| {
            account_value(
                account,
                &["risk_per_trade_pct", "risk_per_trade_percent", "risk_pct"],
                None,
            )
        });
        if starting_balance.unwrap_or(0.0) <= 0.0 || configured_risk_pct.unwrap_or(0.0) <= 0.0 {
            return BTreeMap::new();
        }
        let starting_balance = starting_balance.unwrap();
        let configured_risk_pct = configured_risk_pct.unwrap();
        let leverage = account_value(account, &["leverage"], Some(1.0)).unwrap_or(1.0);
        let min_lot = account_value(account, &["min_lot", "minLot"], Some(0.0)).unwrap_or(0.0);
        let lot_step =
            account_value(account, &["lot_step", "lotStep"], Some(0.0001)).unwrap_or(0.0001);
        let notional_per_lot = account_value(
            account,
            &["notional_usd_per_lot", "notionalUsdPerLot"],
            Some(100000.0),
        )
        .unwrap_or(100000.0);
        let margin_call_level_pct = account_value(
            account,
            &["margin_call_level_pct", "marginCallLevelPercent"],
            Some(70.0),
        )
        .unwrap_or(70.0);
        let stop_out_level_pct = account_value(
            account,
            &["stop_out_level_pct", "stopOutLevelPercent"],
            Some(50.0),
        )
        .unwrap_or(50.0);
        let cost_r_per_trade = [
            "commission_r_per_trade",
            "spread_r_per_trade",
            "slippage_r_per_trade",
        ]
        .iter()
        .map(|key| account_value(account, &[*key], Some(0.0)).unwrap_or(0.0))
        .sum::<f64>();
        let mut normalized_basis = risk_basis.to_lowercase();
        if normalized_basis != "initial" && normalized_basis != "current" {
            normalized_basis = "initial".to_string();
        }
        let selected: Vec<String> = selected_ids
            .iter()
            .filter(|attempt_id| self.by_id.contains_key(*attempt_id))
            .cloned()
            .collect();
        let mut balance = starting_balance;
        let mut realized = 0.0;
        let mut peak_balance = balance;
        let mut min_balance = balance;
        let mut max_drawdown_usd = 0.0;
        let mut max_used_margin_usd = 0.0;
        let mut min_margin_level_pct = f64::INFINITY;
        let mut max_margin_risk_pct = 0.0;
        let mut min_lot_forced_trades = 0_i64;
        let mut total_closed_trades = 0_i64;
        let mut risk_variance_weighted = 0.0;
        let mut risk_variance_weight = 0_i64;
        let mut max_actual_risk_pct = 0.0;
        let mut max_actual_risk_multiple = 0.0;
        let mut margin_liquidated = false;
        let mut first_liquidation_date: Option<String> = None;
        let risk_fraction = configured_risk_pct / 100.0;

        for (index, date_text) in self.dates.iter().enumerate() {
            let target_risk = if normalized_basis == "current" {
                balance.max(0.0) * risk_fraction
            } else {
                starting_balance * risk_fraction
            };
            let mut balance_delta = 0.0;
            let mut realized_delta = 0.0;
            let mut used_margin = 0.0;
            let mut closed_trades_for_day = 0_i64;
            for attempt_id in &selected {
                let Some(candidate) = self.candidate(attempt_id) else {
                    continue;
                };
                let daily_r = *candidate.vector.get(index).unwrap_or(&0.0);
                let open_count = *candidate.open_vector.get(index).unwrap_or(&0);
                let closed_count = *candidate.closed_vector.get(index).unwrap_or(&0);
                let stop_loss_percent = candidate.stop_loss_percent;
                let (sized_risk, lots, forced_min_lot) = if target_risk <= 0.0
                    || stop_loss_percent.unwrap_or(0.0) <= 0.0
                    || notional_per_lot <= 0.0
                {
                    (target_risk, 0.0, false)
                } else {
                    let risk_per_lot = notional_per_lot * (stop_loss_percent.unwrap() / 100.0);
                    let raw_lots = if risk_per_lot > 0.0 {
                        target_risk / risk_per_lot
                    } else {
                        0.0
                    };
                    let rounded_lots = if lot_step > 0.0 {
                        ((raw_lots / lot_step) + 1e-9).floor() * lot_step
                    } else {
                        raw_lots
                    };
                    let forced = min_lot > 0.0 && rounded_lots < min_lot;
                    let lots = if forced {
                        min_lot
                    } else {
                        rounded_lots.max(0.0)
                    };
                    let sized_risk = lots * risk_per_lot;
                    if closed_count > 0 && target_risk > 0.0 {
                        let actual_risk_pct = (sized_risk / balance.max(0.000001)) * 100.0;
                        let actual_multiple = sized_risk / target_risk.max(0.000001);
                        let variance = actual_multiple - 1.0;
                        risk_variance_weighted += variance * closed_count as f64;
                        risk_variance_weight += closed_count;
                        if actual_risk_pct > max_actual_risk_pct {
                            max_actual_risk_pct = actual_risk_pct;
                        }
                        if actual_multiple > max_actual_risk_multiple {
                            max_actual_risk_multiple = actual_multiple;
                        }
                    }
                    if forced && closed_count > 0 {
                        min_lot_forced_trades += closed_count;
                    }
                    (sized_risk, lots, forced)
                };
                let _ = forced_min_lot;
                used_margin += open_count as f64 * (lots * notional_per_lot / leverage.max(1.0));
                closed_trades_for_day += closed_count;
                if !margin_liquidated {
                    let net_daily_r = daily_r - (closed_count as f64 * cost_r_per_trade);
                    balance_delta += net_daily_r * sized_risk;
                    realized_delta += net_daily_r * sized_risk;
                }
            }
            if !margin_liquidated {
                balance = round2(balance + balance_delta);
                realized = round2(realized + realized_delta);
            }
            total_closed_trades += closed_trades_for_day;
            let stop_out_equity = used_margin * (stop_out_level_pct / 100.0);
            let margin_level_pct = if used_margin > 0.0 {
                Some(balance / used_margin * 100.0)
            } else {
                None
            };
            let margin_risk_pct = if let Some(level) = margin_level_pct {
                if stop_out_level_pct > 0.0 {
                    (stop_out_level_pct / level.max(0.000001) * 100.0).clamp(0.0, 100.0)
                } else {
                    0.0
                }
            } else {
                0.0
            };
            if !margin_liquidated && used_margin > 0.0 && balance <= stop_out_equity {
                margin_liquidated = true;
                first_liquidation_date = Some(date_text.clone());
            }
            peak_balance = peak_balance.max(balance);
            min_balance = min_balance.min(balance);
            max_drawdown_usd = f64::max(max_drawdown_usd, peak_balance - balance);
            if used_margin > max_used_margin_usd {
                max_used_margin_usd = used_margin;
            }
            if let Some(level) = margin_level_pct {
                if level.is_finite() && level < min_margin_level_pct {
                    min_margin_level_pct = level;
                }
            }
            if margin_risk_pct > max_margin_risk_pct {
                max_margin_risk_pct = margin_risk_pct;
            }
        }
        let final_return_pct = ((balance - starting_balance) / starting_balance) * 100.0;
        let mut result = BTreeMap::new();
        result.insert("risk_basis".to_string(), json!(normalized_basis));
        result.insert("risk_pct".to_string(), json!(configured_risk_pct));
        result.insert(
            "starting_balance".to_string(),
            json!(round2(starting_balance)),
        );
        result.insert("final_balance".to_string(), json!(round2(balance)));
        result.insert("final_realized_usd".to_string(), json!(round2(realized)));
        result.insert(
            "final_return_pct".to_string(),
            json!(round6(final_return_pct)),
        );
        result.insert(
            "max_drawdown_usd".to_string(),
            json!(round2(max_drawdown_usd)),
        );
        result.insert(
            "max_drawdown_pct".to_string(),
            json!(round6((max_drawdown_usd / starting_balance) * 100.0)),
        );
        result.insert("min_balance".to_string(), json!(round2(min_balance)));
        result.insert(
            "blown".to_string(),
            json!(margin_liquidated || balance <= 0.0),
        );
        result.insert("margin_liquidated".to_string(), json!(margin_liquidated));
        result.insert(
            "first_liquidation_date".to_string(),
            json!(first_liquidation_date),
        );
        result.insert(
            "max_used_margin_usd".to_string(),
            json!(round2(max_used_margin_usd)),
        );
        result.insert(
            "min_margin_level_pct".to_string(),
            if min_margin_level_pct.is_finite() {
                json!(round6(min_margin_level_pct))
            } else {
                Value::Null
            },
        );
        result.insert(
            "max_margin_risk_pct".to_string(),
            json!(round6(max_margin_risk_pct)),
        );
        result.insert(
            "margin_call_level_pct".to_string(),
            json!(margin_call_level_pct),
        );
        result.insert("stop_out_level_pct".to_string(), json!(stop_out_level_pct));
        result.insert(
            "min_lot_forced_trades".to_string(),
            json!(min_lot_forced_trades),
        );
        result.insert(
            "min_lot_forced_trade_pct".to_string(),
            json!(if total_closed_trades > 0 {
                round6((min_lot_forced_trades as f64 / total_closed_trades as f64) * 100.0)
            } else {
                0.0
            }),
        );
        result.insert(
            "total_closed_trades".to_string(),
            json!(total_closed_trades),
        );
        result.insert(
            "avg_actual_risk_variance_pct".to_string(),
            json!(if risk_variance_weight > 0 {
                round6((risk_variance_weighted / risk_variance_weight as f64) * 100.0)
            } else {
                0.0
            }),
        );
        result.insert(
            "max_actual_risk_pct".to_string(),
            json!(round6(max_actual_risk_pct)),
        );
        result.insert(
            "max_actual_risk_multiple".to_string(),
            json!(round6(max_actual_risk_multiple)),
        );
        result
    }
}

fn candidate_from_input(input: OptimizerCandidateInput) -> OptimizerCandidate {
    let instruments: Vec<String> = input
        .instruments
        .into_iter()
        .map(|item| item.trim().to_uppercase())
        .filter(|item| !item.is_empty())
        .collect();
    let asset_classes: BTreeSet<String> = if instruments.is_empty() {
        ["other".to_string()].into_iter().collect()
    } else {
        instruments
            .iter()
            .map(|instrument| instrument_asset_class(instrument))
            .collect()
    };
    let primary_asset_class = if asset_classes.len() == 1 {
        asset_classes
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| "other".to_string())
    } else if asset_classes.contains("metal") {
        "metal".to_string()
    } else if asset_classes.contains("index") {
        "index".to_string()
    } else {
        "fx".to_string()
    };
    let family = input.family.unwrap_or_else(|| input.attempt_id.clone());
    OptimizerCandidate {
        attempt_id: input.attempt_id,
        candidate_name: input.candidate_name,
        run_id: input.run_id,
        created_at: input.created_at,
        instruments,
        primary_asset_class,
        family,
        score: input.score,
        avg_hold_hours: input.avg_hold_hours,
        p90_hold_hours: input.p90_hold_hours,
        max_hold_hours: input.max_hold_hours,
        path_quality: input.path_quality,
        stop_loss_percent: input.stop_loss_percent,
        trade_count: input.trade_count,
        trades_per_month: input.trades_per_month,
        dates: input.dates,
        daily_r: input.daily_r,
        open_counts: input.open_counts,
        closed_counts: input.closed_counts,
        vector: Vec::new(),
        open_vector: Vec::new(),
        closed_vector: Vec::new(),
        month_vector: Vec::new(),
        week_vector: Vec::new(),
    }
}

fn instrument_asset_class(symbol: &str) -> String {
    let token = symbol.trim().to_uppercase();
    if METAL_SYMBOLS.contains(&token.as_str()) {
        return "metal".to_string();
    }
    if INDEX_SYMBOLS.contains(&token.as_str()) {
        return "index".to_string();
    }
    if COMMODITY_SYMBOLS.contains(&token.as_str()) {
        return "commodity".to_string();
    }
    if CRYPTO_SYMBOLS.contains(&token.as_str()) {
        return "crypto".to_string();
    }
    if token.len() == 6 && FX_CODES.contains(&&token[0..3]) && FX_CODES.contains(&&token[3..6]) {
        return "fx".to_string();
    }
    "other".to_string()
}

fn max_drawdown(values: &[f64]) -> f64 {
    let mut equity = 0.0;
    let mut peak = 0.0;
    let mut max_dd = 0.0;
    for value in values {
        equity += value;
        if equity > peak {
            peak = equity;
        }
        let drawdown = peak - equity;
        if drawdown > max_dd {
            max_dd = drawdown;
        }
    }
    max_dd
}

fn loss_streak(values: &[f64]) -> (usize, f64) {
    let mut current = 0;
    let mut longest = 0;
    let mut streaks = Vec::new();
    for value in values {
        if *value < -1e-9 {
            current += 1;
            continue;
        }
        if current > 0 {
            streaks.push(current);
        }
        if current > longest {
            longest = current;
        }
        current = 0;
    }
    if current > 0 {
        streaks.push(current);
    }
    if current > longest {
        longest = current;
    }
    let average = if streaks.is_empty() {
        0.0
    } else {
        streaks.iter().sum::<usize>() as f64 / streaks.len() as f64
    };
    (longest, average)
}

fn calendar_bucket_indexes(dates: &[String], mode: &str) -> (Vec<usize>, usize) {
    let mut indexes = Vec::with_capacity(dates.len());
    let mut by_key = HashMap::new();
    for date_text in dates {
        let key = match mode {
            "month" => date_text.chars().take(7).collect::<String>(),
            "week" => week_key(date_text),
            _ => date_text.clone(),
        };
        let next_index = by_key.len();
        let index = *by_key.entry(key).or_insert(next_index);
        indexes.push(index);
    }
    (indexes, by_key.len())
}

fn group_by_indexes(values: &[f64], indexes: &[usize], size: usize) -> Vec<f64> {
    let mut grouped = vec![0.0; size];
    for (value, index) in values.iter().zip(indexes.iter()) {
        grouped[*index] += value;
    }
    grouped
}

fn group_values(dates: &[String], values: &[f64], mode: &str) -> BTreeMap<String, f64> {
    let mut grouped = BTreeMap::new();
    for (date_text, value) in dates.iter().zip(values.iter()) {
        let key = match mode {
            "month" => date_text.chars().take(7).collect::<String>(),
            "week" => week_key(date_text),
            _ => date_text.clone(),
        };
        *grouped.entry(key).or_insert(0.0) += value;
    }
    grouped
}

fn week_key(date_text: &str) -> String {
    if let Ok(date) = NaiveDate::parse_from_str(date_text, "%Y-%m-%d") {
        let iso = date.iso_week();
        format!("{}-W{:02}", iso.year(), iso.week())
    } else {
        date_text.to_string()
    }
}

fn count_positive_negative_flat<I>(values: I) -> (usize, usize, usize)
where
    I: Iterator<Item = f64>,
{
    let mut positive = 0;
    let mut negative = 0;
    let mut flat = 0;
    for value in values {
        if value > 1e-9 {
            positive += 1;
        } else if value < -1e-9 {
            negative += 1;
        } else {
            flat += 1;
        }
    }
    (positive, negative, flat)
}

fn pearson_corr(first: &[f64], second: &[f64]) -> f64 {
    let size = first.len().min(second.len());
    if size < 3 {
        return 0.0;
    }
    let left = &first[..size];
    let right = &second[..size];
    let left_mean = left.iter().sum::<f64>() / size as f64;
    let right_mean = right.iter().sum::<f64>() / size as f64;
    let left_var: f64 = left
        .iter()
        .map(|value| (value - left_mean) * (value - left_mean))
        .sum();
    let right_var: f64 = right
        .iter()
        .map(|value| (value - right_mean) * (value - right_mean))
        .sum();
    if left_var <= 1e-12 || right_var <= 1e-12 {
        return 0.0;
    }
    let covariance: f64 = left
        .iter()
        .zip(right.iter())
        .map(|(left_value, right_value)| (left_value - left_mean) * (right_value - right_mean))
        .sum();
    covariance / (left_var * right_var).sqrt()
}

fn best_pair(values: &BTreeMap<String, f64>, want_max: bool) -> (String, f64) {
    let mut output = ("".to_string(), 0.0);
    let mut initialized = false;
    for (key, value) in values {
        if !initialized || (want_max && *value > output.1) || (!want_max && *value < output.1) {
            output = (key.clone(), *value);
            initialized = true;
        }
    }
    output
}

fn mean_or_zero(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn max_or_zero(values: &[f64]) -> f64 {
    let Some(first) = values.first() else {
        return 0.0;
    };
    values
        .iter()
        .skip(1)
        .fold(*first, |best, value| best.max(*value))
}

fn weight(weights: &BTreeMap<String, f64>, key: &str) -> f64 {
    *weights.get(key).unwrap_or(&0.0)
}

fn metric_f64(metrics: &Value, key: &str) -> f64 {
    metrics.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn metric_or_inf(metrics: &Value, key: &str) -> f64 {
    metrics
        .get(key)
        .and_then(Value::as_f64)
        .unwrap_or(f64::INFINITY)
}

fn has_constraint_violations(metrics: &Value) -> bool {
    metrics
        .get("constraint_violations")
        .and_then(Value::as_object)
        .is_some_and(|items| !items.is_empty())
}

fn account_value(account: &Map<String, Value>, keys: &[&str], default: Option<f64>) -> Option<f64> {
    for key in keys {
        if let Some(value) = account.get(*key).and_then(Value::as_f64) {
            if value.is_finite() {
                return Some(value);
            }
        }
    }
    default
}

fn unique_count(values: &[String]) -> usize {
    values.iter().collect::<HashSet<_>>().len()
}

fn unique_preserve(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            output.push(value);
        }
    }
    output
}

fn unique_known_ids(values: &[String], by_id: &HashMap<String, usize>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for value in values {
        if by_id.contains_key(value) && seen.insert(value.clone()) {
            output.push(value.clone());
        }
    }
    output
}

fn round2(value: f64) -> f64 {
    py_round_digits(value, 2)
}

fn round6(value: f64) -> f64 {
    py_round_digits(value, 6)
}

fn py_round_digits(value: f64, digits: i32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let factor = 10_f64.powi(digits);
    (value * factor).round_ties_even() / factor
}

struct PythonRandom {
    mt: [u32; 624],
    index: usize,
}

impl PythonRandom {
    fn new(seed: i64) -> Self {
        let mut rng = Self {
            mt: [0; 624],
            index: 624,
        };
        let seed_abs = seed.unsigned_abs();
        let mut key = Vec::new();
        let mut value = seed_abs;
        if value == 0 {
            key.push(0);
        } else {
            while value > 0 {
                key.push((value & 0xffff_ffff) as u32);
                value >>= 32;
            }
        }
        rng.init_by_array(&key);
        rng
    }

    fn init_genrand(&mut self, seed: u32) {
        self.mt[0] = seed;
        for index in 1..624 {
            let previous = self.mt[index - 1];
            self.mt[index] = 1812433253_u32
                .wrapping_mul(previous ^ (previous >> 30))
                .wrapping_add(index as u32);
        }
        self.index = 624;
    }

    fn init_by_array(&mut self, key: &[u32]) {
        self.init_genrand(19650218);
        let key_length = key.len().max(1);
        let mut i = 1_usize;
        let mut j = 0_usize;
        let mut k = 624_usize.max(key_length);
        while k > 0 {
            let previous = self.mt[i - 1];
            self.mt[i] = (self.mt[i] ^ ((previous ^ (previous >> 30)).wrapping_mul(1664525)))
                .wrapping_add(key[j])
                .wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= 624 {
                self.mt[0] = self.mt[623];
                i = 1;
            }
            if j >= key_length {
                j = 0;
            }
            k -= 1;
        }
        k = 623;
        while k > 0 {
            let previous = self.mt[i - 1];
            self.mt[i] = (self.mt[i] ^ ((previous ^ (previous >> 30)).wrapping_mul(1566083941)))
                .wrapping_sub(i as u32);
            i += 1;
            if i >= 624 {
                self.mt[0] = self.mt[623];
                i = 1;
            }
            k -= 1;
        }
        self.mt[0] = 0x8000_0000;
    }

    fn twist(&mut self) {
        const N: usize = 624;
        const M: usize = 397;
        const MATRIX_A: u32 = 0x9908_b0df;
        const UPPER_MASK: u32 = 0x8000_0000;
        const LOWER_MASK: u32 = 0x7fff_ffff;
        for kk in 0..(N - M) {
            let y = (self.mt[kk] & UPPER_MASK) | (self.mt[kk + 1] & LOWER_MASK);
            self.mt[kk] = self.mt[kk + M] ^ (y >> 1) ^ if y & 1 != 0 { MATRIX_A } else { 0 };
        }
        for kk in (N - M)..(N - 1) {
            let y = (self.mt[kk] & UPPER_MASK) | (self.mt[kk + 1] & LOWER_MASK);
            self.mt[kk] = self.mt[kk + M - N] ^ (y >> 1) ^ if y & 1 != 0 { MATRIX_A } else { 0 };
        }
        let y = (self.mt[N - 1] & UPPER_MASK) | (self.mt[0] & LOWER_MASK);
        self.mt[N - 1] = self.mt[M - 1] ^ (y >> 1) ^ if y & 1 != 0 { MATRIX_A } else { 0 };
        self.index = 0;
    }

    fn gen_u32(&mut self) -> u32 {
        if self.index >= 624 {
            self.twist();
        }
        let mut y = self.mt[self.index];
        self.index += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^= y >> 18;
        y
    }

    fn getrandbits(&mut self, bits: usize) -> u64 {
        if bits == 0 {
            return 0;
        }
        if bits <= 32 {
            return (self.gen_u32() >> (32 - bits)) as u64;
        }
        let mut remaining = bits;
        let mut output = 0_u64;
        let mut shift = 0_usize;
        while remaining > 0 {
            let take = remaining.min(32);
            let chunk = if take == 32 {
                self.gen_u32() as u64
            } else {
                (self.gen_u32() >> (32 - take)) as u64
            };
            output |= chunk << shift;
            shift += take;
            remaining -= take;
        }
        output
    }

    fn randbelow(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let bits = usize::BITS as usize - n.leading_zeros() as usize;
        let mut value = self.getrandbits(bits) as usize;
        while value >= n {
            value = self.getrandbits(bits) as usize;
        }
        value
    }

    fn sample(&mut self, values: &[String], count: usize) -> Vec<String> {
        let n = values.len();
        let k = count.min(n);
        let mut result = Vec::with_capacity(k);
        let mut setsize = 21_usize;
        if k > 5 {
            let mut power = 1_usize;
            let target = k * 3;
            while power < target {
                power *= 4;
            }
            setsize += power;
        }
        if n <= setsize {
            let mut pool = values.to_vec();
            for index in 0..k {
                let j = self.randbelow(n - index);
                result.push(pool[j].clone());
                pool[j] = pool[n - index - 1].clone();
            }
        } else {
            let mut selected = HashSet::new();
            for _ in 0..k {
                let mut j = self.randbelow(n);
                while selected.contains(&j) {
                    j = self.randbelow(n);
                }
                selected.insert(j);
                result.push(values[j].clone());
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        attempt_id: &str,
        instrument: &str,
        daily_r: Vec<f64>,
        score: f64,
    ) -> OptimizerCandidateInput {
        let dates = (1..=daily_r.len())
            .map(|index| format!("2026-01-{index:02}"))
            .collect::<Vec<_>>();
        let len = daily_r.len();
        OptimizerCandidateInput {
            attempt_id: attempt_id.to_string(),
            candidate_name: Some(attempt_id.to_string()),
            run_id: Some(format!("run-{attempt_id}")),
            created_at: None,
            instruments: vec![instrument.to_string()],
            family: Some(attempt_id.to_string()),
            score,
            avg_hold_hours: 12.0,
            p90_hold_hours: Some(24.0),
            max_hold_hours: Some(48.0),
            path_quality: Some(0.8),
            stop_loss_percent: Some(1.0),
            trade_count: len as i64,
            trades_per_month: 10.0,
            dates,
            daily_r,
            open_counts: vec![1; len],
            closed_counts: vec![1; len],
        }
    }

    fn spec_for_smoke(objective_names: Vec<&str>) -> PortfolioOptimizerSpec {
        PortfolioOptimizerSpec {
            portfolio_size: 2,
            candidate_limit: 3,
            objective_names: objective_names.into_iter().map(str::to_string).collect(),
            random_starts: 0,
            max_swaps: 4,
            max_per_family: 1,
            min_fx_share: 0.0,
            max_metal_share: 2.0,
            max_index_share: 2.0,
            max_instrument_share: 1.0,
            ..PortfolioOptimizerSpec::default()
        }
    }

    #[test]
    fn python_random_matches_cpython_sample_sequences() {
        let ids = (0..10)
            .map(|index| format!("id-{index}"))
            .collect::<Vec<_>>();
        let mut rng = PythonRandom::new(17);
        assert_eq!(rng.getrandbits(1), 1);
        let mut rng = PythonRandom::new(17);
        assert_eq!(rng.getrandbits(7), 66);
        let mut rng = PythonRandom::new(17);
        assert_eq!(rng.getrandbits(32), 2241903809);
        let mut rng = PythonRandom::new(17);
        assert_eq!(
            rng.sample(&ids, 4),
            vec![
                "id-8".to_string(),
                "id-6".to_string(),
                "id-4".to_string(),
                "id-2".to_string()
            ]
        );
        let mut rng = PythonRandom::new(-17);
        assert_eq!(
            rng.sample(&ids, 4),
            vec![
                "id-8".to_string(),
                "id-6".to_string(),
                "id-4".to_string(),
                "id-2".to_string()
            ]
        );
        let broad = (0..120)
            .map(|index| format!("id-{index}"))
            .collect::<Vec<_>>();
        let mut rng = PythonRandom::new(6181);
        assert_eq!(
            rng.sample(&broad, 20),
            vec![
                "id-92".to_string(),
                "id-47".to_string(),
                "id-46".to_string(),
                "id-36".to_string(),
                "id-1".to_string(),
                "id-95".to_string(),
                "id-75".to_string(),
                "id-87".to_string(),
                "id-45".to_string(),
                "id-4".to_string(),
                "id-2".to_string(),
                "id-77".to_string(),
                "id-110".to_string(),
                "id-9".to_string(),
                "id-62".to_string(),
                "id-72".to_string(),
                "id-107".to_string(),
                "id-64".to_string(),
                "id-71".to_string(),
                "id-112".to_string()
            ]
        );
        let ids = (0..30)
            .map(|index| format!("id-{index}"))
            .collect::<Vec<_>>();
        let mut rng = PythonRandom::new(6181);
        assert_eq!(
            rng.sample(&ids, 7),
            vec![
                "id-23".to_string(),
                "id-11".to_string(),
                "id-28".to_string(),
                "id-9".to_string(),
                "id-0".to_string(),
                "id-29".to_string(),
                "id-18".to_string()
            ]
        );
        assert_eq!(
            rng.sample(&ids, 7),
            vec![
                "id-21".to_string(),
                "id-11".to_string(),
                "id-1".to_string(),
                "id-0".to_string(),
                "id-19".to_string(),
                "id-2".to_string(),
                "id-15".to_string()
            ]
        );
        assert_eq!(
            rng.sample(&ids, 7),
            vec![
                "id-0".to_string(),
                "id-18".to_string(),
                "id-26".to_string(),
                "id-16".to_string(),
                "id-17".to_string(),
                "id-21".to_string(),
                "id-4".to_string()
            ]
        );
    }

    #[test]
    fn rounding_uses_python_bankers_ties() {
        assert_eq!(py_round_digits(2.5, 0), 2.0);
        assert_eq!(py_round_digits(3.5, 0), 4.0);
        assert_eq!(py_round_digits(-2.5, 0), -2.0);
        assert_eq!(py_round_digits(-3.5, 0), -4.0);
    }

    #[test]
    fn selects_smooth_stability_portfolio() {
        let input = OptimizerInput {
            spec: spec_for_smoke(vec!["stability"]),
            candidates: vec![
                candidate("smooth-a", "EURUSD", vec![1.0, 1.0, 1.0, 1.0], 65.0),
                candidate("smooth-b", "XAUUSD", vec![0.5, 1.0, 1.0, 1.0], 65.0),
                candidate("lumpy", "USDJPY", vec![8.0, -7.0, 8.0, -7.0], 95.0),
            ],
            objectives: BTreeMap::new(),
        };
        let output = optimize_input(input);
        let selected = &output.variants["stability"].selected_attempt_ids;
        assert_eq!(
            selected,
            &vec!["smooth-a".to_string(), "smooth-b".to_string()]
        );
        assert!(
            output.variants["stability"].metrics["constraint_violations"]
                .as_object()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            output.variants["stability"].metrics["max_daily_loss_streak"],
            json!(0)
        );
    }

    #[test]
    fn required_attempt_survives_all_starts_and_swaps() {
        let mut spec = spec_for_smoke(vec!["stability"]);
        spec.random_starts = 3;
        spec.max_swaps = 8;
        spec.required_attempt_ids = vec!["required-lumpy".to_string()];
        let output = optimize_input(OptimizerInput {
            spec,
            candidates: vec![
                candidate("required-lumpy", "EURUSD", vec![8.0, -7.0, 8.0, -7.0], 50.0),
                candidate("smooth-a", "GBPUSD", vec![1.0, 1.0, 1.0, 1.0], 80.0),
                candidate("smooth-b", "USDJPY", vec![0.5, 1.0, 1.0, 1.0], 75.0),
            ],
            objectives: default_objectives(),
        });

        assert!(
            output.variants["stability"]
                .selected_attempt_ids
                .contains(&"required-lumpy".to_string())
        );
    }

    #[test]
    fn transient_trials_match_full_scores_without_growing_caches() {
        let mut spec = PortfolioOptimizerSpec {
            portfolio_size: 3,
            candidate_limit: 4,
            objective_names: vec!["balanced".to_string()],
            random_starts: 0,
            max_swaps: 2,
            max_per_family: 3,
            min_fx_share: 0.0,
            max_instrument_share: 3.0,
            correlation_penalty_weight: 10.0,
            diversification_mode: "marginal_sharpe".to_string(),
            portfolio_sharpe_weight: 20.0,
            ..PortfolioOptimizerSpec::default()
        };
        spec.max_metal_share = 3.0;
        let candidates = vec![
            candidate("a", "EURUSD", vec![1.0, -0.5, 1.5, 0.2], 80.0),
            candidate("b", "GBPUSD", vec![0.2, 0.8, -0.3, 1.1], 79.0),
            candidate("c", "USDJPY", vec![-0.1, 0.7, 0.4, 0.6], 78.0),
            candidate("d", "AUDUSD", vec![0.5, -0.2, 0.9, 0.3], 77.0),
        ];
        let mut search = PortfolioSearch::new(candidates, spec, default_objectives());
        let selected = vec!["a".to_string(), "b".to_string()];
        let base = search.trial_base(&selected, None);
        let weights = search.objectives["balanced"].clone();
        let transient = search.objective_score_extension(&selected, None, "c", &weights, &base);

        assert!(search.metrics_cache.is_empty());
        assert!(search.score_cache.is_empty());
        assert!(search.positive_corr_cache.is_empty());
        assert!(search.sharpe_cache.is_empty());
        let trial = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let full = search.objective_score(&trial, "balanced");
        assert!((transient - full).abs() < 1e-9);
    }

    #[test]
    fn penalty_mode_prefers_uncorrelated_candidate() {
        let mut objectives = BTreeMap::new();
        objectives.insert(
            "return".to_string(),
            weights(&[("final_r", 1.0), ("maxdd_r", -2.0)]),
        );
        let mut penalty_spec = spec_for_smoke(vec!["return"]);
        penalty_spec.max_swaps = 0;
        penalty_spec.correlation_penalty_weight = 10.0;
        penalty_spec.diversification_mode = "penalty".to_string();
        let input = OptimizerInput {
            spec: penalty_spec,
            candidates: vec![
                candidate(
                    "clone-a",
                    "EURUSD",
                    vec![2.0, -0.5, 2.0, -0.5, 2.0, -0.5],
                    90.0,
                ),
                candidate(
                    "clone-b",
                    "GBPUSD",
                    vec![2.0, -0.5, 2.0, -0.5, 2.0, -0.5],
                    89.0,
                ),
                candidate(
                    "uncorr-c",
                    "USDJPY",
                    vec![-0.2, 0.6, -0.2, 0.6, -0.2, 0.6],
                    60.0,
                ),
            ],
            objectives,
        };
        let output = optimize_input(input);
        let selected: BTreeSet<String> = output.variants["return"]
            .selected_attempt_ids
            .iter()
            .cloned()
            .collect();
        assert!(selected.contains("uncorr-c"));
        assert_ne!(
            selected,
            ["clone-a".to_string(), "clone-b".to_string()]
                .into_iter()
                .collect()
        );
        assert_eq!(
            output.variants["return"].metrics["avg_positive_pair_corr"],
            json!(0.0)
        );
    }

    #[test]
    fn marginal_sharpe_prefers_anticorrelated_candidate() {
        let mut objectives = BTreeMap::new();
        objectives.insert(
            "return".to_string(),
            weights(&[("final_r", 1.0), ("maxdd_r", -2.0)]),
        );
        let mut spec = spec_for_smoke(vec!["return"]);
        spec.max_swaps = 0;
        spec.diversification_mode = "marginal_sharpe".to_string();
        spec.portfolio_sharpe_weight = 5.0;
        let input = OptimizerInput {
            spec,
            candidates: vec![
                candidate(
                    "seed",
                    "EURUSD",
                    vec![2.0, -0.5, 2.0, -0.5, 2.0, -0.5],
                    90.0,
                ),
                candidate(
                    "corr-high-r",
                    "GBPUSD",
                    vec![1.5, -0.3, 1.5, -0.3, 1.5, -0.3],
                    80.0,
                ),
                candidate(
                    "anti-low-r",
                    "USDJPY",
                    vec![-0.2, 0.6, -0.2, 0.6, -0.2, 0.6],
                    60.0,
                ),
            ],
            objectives,
        };
        let output = optimize_input(input);
        let selected: BTreeSet<String> = output.variants["return"]
            .selected_attempt_ids
            .iter()
            .cloned()
            .collect();
        assert_eq!(
            selected,
            ["seed".to_string(), "anti-low-r".to_string()]
                .into_iter()
                .collect()
        );
        let sharpe = output.variants["return"].diversification["portfolio_sharpe"]
            .as_f64()
            .unwrap();
        assert!((sharpe - (0.95 / 0.85)).abs() < 1e-12);
    }

    #[test]
    fn account_metrics_match_lot_floor_and_current_basis() {
        let mut spec = PortfolioOptimizerSpec {
            portfolio_size: 1,
            objective_names: vec!["return".to_string()],
            random_starts: 0,
            max_swaps: 0,
            min_fx_share: 0.0,
            ..PortfolioOptimizerSpec::default()
        };
        spec.account
            .insert("account_size_usd".to_string(), json!(100.0));
        spec.account
            .insert("risk_per_trade_pct".to_string(), json!(0.1));
        spec.account.insert("min_lot".to_string(), json!(0.01));
        spec.account.insert("lot_step".to_string(), json!(0.01));
        spec.account
            .insert("notional_usd_per_lot".to_string(), json!(100000.0));
        spec.account.insert("leverage".to_string(), json!(500.0));
        spec.account
            .insert("stop_out_level_pct".to_string(), json!(50.0));
        spec.account
            .insert("margin_call_level_pct".to_string(), json!(100.0));
        let mut objectives = BTreeMap::new();
        objectives.insert("return".to_string(), weights(&[("final_r", 1.0)]));
        let output = optimize_input(OptimizerInput {
            spec,
            candidates: vec![candidate("tiny-risk", "EURUSD", vec![1.0, -0.5, 1.0], 70.0)],
            objectives,
        });
        let account = &output.variants["return"].metrics["account_initial"];
        assert_eq!(account["starting_balance"], json!(100.0));
        assert_eq!(account["final_balance"], json!(115.0));
        assert_eq!(account["min_lot_forced_trades"], json!(3));
        assert_eq!(account["max_actual_risk_pct"], json!(10.0));

        let mut compound_spec = PortfolioOptimizerSpec {
            portfolio_size: 1,
            objective_names: vec!["return".to_string()],
            random_starts: 0,
            max_swaps: 0,
            min_fx_share: 0.0,
            ..PortfolioOptimizerSpec::default()
        };
        compound_spec
            .account
            .insert("account_size_usd".to_string(), json!(1000.0));
        compound_spec
            .account
            .insert("risk_per_trade_pct".to_string(), json!(1.0));
        compound_spec
            .account
            .insert("min_lot".to_string(), json!(0.0));
        compound_spec
            .account
            .insert("lot_step".to_string(), json!(0.0001));
        compound_spec
            .account
            .insert("notional_usd_per_lot".to_string(), json!(100000.0));
        compound_spec
            .account
            .insert("leverage".to_string(), json!(500.0));
        compound_spec
            .account
            .insert("stop_out_level_pct".to_string(), json!(50.0));
        let mut objectives = BTreeMap::new();
        objectives.insert("return".to_string(), weights(&[("final_r", 1.0)]));
        let compound = optimize_input(OptimizerInput {
            spec: compound_spec,
            candidates: vec![candidate("compound", "EURUSD", vec![1.0, 1.0], 70.0)],
            objectives,
        });
        assert_eq!(
            compound.variants["return"].metrics["account_initial"]["final_balance"],
            json!(1020.0)
        );
        assert_eq!(
            compound.variants["return"].metrics["account_current"]["final_balance"],
            json!(1020.1)
        );
    }

    fn similarity_input(candidates: Vec<SimilarityCandidateInput>) -> SimilarityInput {
        SimilarityInput {
            schema_version: "similarity.v1".to_string(),
            candidates,
            reference_attempt_ids: vec!["alpha".to_string(), "beta".to_string()],
            active_epsilon: 0.0,
            worst_quantile: 0.5,
            min_observations: 2,
            behavioral_weights: BehavioralWeights {
                active_overlap: 1.0,
                return_correlation: 1.0,
                downside_correlation: 1.0,
                worst_decile_correlation: 1.0,
            },
            cluster_threshold: 0.9,
        }
    }

    fn similarity_candidate(
        attempt_id: &str,
        dates: &[&str],
        daily_r: &[f64],
    ) -> SimilarityCandidateInput {
        SimilarityCandidateInput {
            attempt_id: attempt_id.to_string(),
            dates: dates.iter().map(|date| (*date).to_string()).collect(),
            daily_r: daily_r.to_vec(),
        }
    }

    fn assert_symmetric(matrix: &[Vec<f64>]) {
        for (left, row) in matrix.iter().enumerate() {
            assert_eq!(row.len(), matrix.len());
            for right in 0..matrix.len() {
                assert_eq!(row[right], matrix[right][left]);
            }
        }
    }

    #[test]
    fn similarity_analysis_aligns_reference_curves_and_clusters_deterministically() {
        let output = analyze_similarity(similarity_input(vec![
            similarity_candidate("gamma", &["2026-01-01", "2026-01-03"], &[3.0, 1.0]),
            similarity_candidate(
                "beta",
                &["2026-01-01", "2026-01-02", "2026-01-03"],
                &[0.0, 2.0, -1.0],
            ),
            similarity_candidate("alpha", &["2026-01-02", "2026-01-03"], &[2.0, -1.0]),
        ]))
        .unwrap();

        assert_eq!(output.attempt_ids, vec!["alpha", "beta", "gamma"]);
        assert_eq!(output.reference.attempt_ids, vec!["alpha", "beta"]);
        assert_eq!(
            output.reference.calendar_dates,
            vec!["2026-01-01", "2026-01-02", "2026-01-03"]
        );
        assert_eq!(output.reference.daily_r, vec![0.0, 4.0, -2.0]);
        assert_eq!(output.reference.downside_observation_count, 1);
        assert_eq!(output.reference.worst_cutoff_r, Some(0.0));
        assert_eq!(output.reference.worst_observation_count, 2);
        assert_eq!(output.active_overlap_matrix[0][1], 1.0);
        assert_eq!(output.return_correlation_matrix[0][1], 1.0);
        assert_eq!(output.downside_correlation_matrix[0][1], 0.0);
        assert_eq!(output.worst_decile_correlation_matrix[0][1], 1.0);
        assert_eq!(output.similarity_matrix[0][1], 0.75);
        assert_eq!(
            output.clusters,
            vec![
                SimilarityCluster {
                    id: "behavior:alpha".to_string(),
                    members: vec!["alpha".to_string()],
                },
                SimilarityCluster {
                    id: "behavior:beta".to_string(),
                    members: vec!["beta".to_string()],
                },
                SimilarityCluster {
                    id: "behavior:gamma".to_string(),
                    members: vec!["gamma".to_string()],
                },
            ]
        );
        assert_symmetric(&output.active_overlap_matrix);
        assert_symmetric(&output.return_correlation_matrix);
        assert_symmetric(&output.downside_correlation_matrix);
        assert_symmetric(&output.worst_decile_correlation_matrix);
        assert_symmetric(&output.similarity_matrix);
    }

    #[test]
    fn similarity_analysis_uses_positive_correlations_and_normalized_weights() {
        let mut input = similarity_input(vec![
            similarity_candidate(
                "alpha",
                &["2026-01-01", "2026-01-02", "2026-01-03"],
                &[1.0, 2.0, 3.0],
            ),
            similarity_candidate(
                "beta",
                &["2026-01-01", "2026-01-02", "2026-01-03"],
                &[-1.0, -2.0, -3.0],
            ),
        ]);
        input.reference_attempt_ids = vec!["alpha".to_string()];
        input.behavioral_weights = BehavioralWeights {
            active_overlap: 1.0,
            return_correlation: 3.0,
            downside_correlation: 0.0,
            worst_decile_correlation: 0.0,
        };
        let output = analyze_similarity(input).unwrap();
        assert_eq!(output.active_overlap_matrix[0][1], 1.0);
        assert_eq!(output.return_correlation_matrix[0][1], -1.0);
        assert_eq!(output.similarity_matrix[0][1], 0.25);
    }

    #[test]
    fn similarity_analysis_returns_zero_for_constant_or_insufficient_correlations() {
        let mut input = similarity_input(vec![
            similarity_candidate(
                "alpha",
                &["2026-01-01", "2026-01-02", "2026-01-03"],
                &[1.0, 1.0, 1.0],
            ),
            similarity_candidate(
                "beta",
                &["2026-01-01", "2026-01-02", "2026-01-03"],
                &[1.0, 2.0, 3.0],
            ),
        ]);
        input.reference_attempt_ids = vec!["alpha".to_string()];
        let output = analyze_similarity(input).unwrap();
        assert_eq!(output.return_correlation_matrix[0][1], 0.0);
        assert_eq!(output.return_correlation_matrix[0][0], 0.0);
        assert_eq!(output.downside_correlation_matrix[0][1], 0.0);
        assert_eq!(output.worst_decile_correlation_matrix[0][1], 0.0);

        let mut insufficient = similarity_input(vec![
            similarity_candidate("alpha", &["2026-01-01", "2026-01-02"], &[1.0, 2.0]),
            similarity_candidate("beta", &["2026-01-01", "2026-01-02"], &[2.0, 3.0]),
        ]);
        insufficient.min_observations = 3;
        let output = analyze_similarity(insufficient).unwrap();
        assert_eq!(output.return_correlation_matrix[0][1], 0.0);
    }

    #[test]
    fn similarity_analysis_json_is_stable_across_input_order() {
        let mut first = similarity_input(vec![
            similarity_candidate("beta", &["2026-01-01", "2026-01-02"], &[1.0, -1.0]),
            similarity_candidate("alpha", &["2026-01-01", "2026-01-02"], &[1.0, -1.0]),
        ]);
        first.behavioral_weights = BehavioralWeights {
            active_overlap: 1.0,
            return_correlation: 1.0,
            downside_correlation: 0.0,
            worst_decile_correlation: 0.0,
        };
        let mut second = first.clone();
        second.candidates.reverse();
        second.reference_attempt_ids.reverse();
        let first_json = analyze_similarity_json(&serde_json::to_string(&first).unwrap()).unwrap();
        let second_json =
            analyze_similarity_json(&serde_json::to_string(&second).unwrap()).unwrap();
        assert_eq!(first_json, second_json);
        let output: Value = serde_json::from_str(&first_json).unwrap();
        assert_eq!(output["clusters"][0]["id"], json!("behavior:alpha"));
        assert_eq!(output["clusters"][0]["members"], json!(["alpha", "beta"]));
    }

    #[test]
    fn similarity_clusters_are_threshold_connected_components() {
        let ids = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        let matrix = vec![
            vec![1.0, 0.9, 0.1],
            vec![0.9, 1.0, 0.9],
            vec![0.1, 0.9, 1.0],
        ];
        assert_eq!(
            similarity_clusters(&ids, &matrix, 0.9),
            vec![SimilarityCluster {
                id: "behavior:alpha".to_string(),
                members: ids,
            }]
        );
    }

    #[test]
    fn similarity_validation_rejects_invalid_contracts() {
        let valid_candidates = vec![
            similarity_candidate("alpha", &["2026-01-01"], &[1.0]),
            similarity_candidate("beta", &["2026-01-01"], &[2.0]),
        ];

        let mut duplicate_candidate = similarity_input(valid_candidates.clone());
        duplicate_candidate.candidates[1].attempt_id = "alpha".to_string();
        assert!(
            analyze_similarity(duplicate_candidate)
                .unwrap_err()
                .contains("duplicate candidate")
        );

        let mut unequal_vectors = similarity_input(valid_candidates.clone());
        unequal_vectors.candidates[0].daily_r.clear();
        assert!(
            analyze_similarity(unequal_vectors)
                .unwrap_err()
                .contains("dates but")
        );

        let mut duplicate_reference = similarity_input(valid_candidates.clone());
        duplicate_reference.reference_attempt_ids = vec!["alpha".to_string(), "alpha".to_string()];
        assert!(
            analyze_similarity(duplicate_reference)
                .unwrap_err()
                .contains("duplicate reference")
        );

        let mut unknown_reference = similarity_input(valid_candidates.clone());
        unknown_reference.reference_attempt_ids = vec!["missing".to_string()];
        assert!(
            analyze_similarity(unknown_reference)
                .unwrap_err()
                .contains("unknown reference")
        );

        let mut non_finite = similarity_input(valid_candidates);
        non_finite.candidates[0].daily_r[0] = f64::NAN;
        assert!(
            analyze_similarity(non_finite)
                .unwrap_err()
                .contains("non-finite")
        );
    }
}
