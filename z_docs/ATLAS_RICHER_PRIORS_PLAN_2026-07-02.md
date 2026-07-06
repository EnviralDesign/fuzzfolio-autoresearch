# Atlas Richer Priors Plan

Status: implementation checkpoint. This version folds in the review findings from the implementation, robustness, and PlayHand/sweep passes, and now records the first code pass landed on July 2, 2026.

Implemented in this checkpoint:

- Fixed scrutiny label integrity and added bounded high-sample scrutiny fallback rows.
- Added sample-confidence evidence to scrutiny queues, recipe priors, and seed-plan feature payloads.
- Made PlayHand outcome priors explicit opt-in for recipe-prior builds.
- Added anchor-side timing probes and side-aware timing aggregation.
- Preserved canonical pair family ids and multi-horizon validation evidence in richer JSON rows.
- Added `standard` and `rich` Atlas lab profiles, with `rich` broadening the empirical signal role panel beyond triggers.
- Added focused regression tests and validated the full suite.

Second checkpoint, July 2, 2026 afternoon:

- Added bounded build-surface controls to `atlas-lab` for smoke/development validation:
  - `--signal-max-indicators`
  - `--signal-instrument-limit`
  - `--signal-timeframe-limit`
- These controls do not change `standard` or `rich` defaults. They only cap the effective signal build panel for a specific run and are recorded in `effective_build_profile`.
- Validated a live bounded rich smoke through the lab gateway:
  - Run id: `smoke-rich-bounded-20260702T151224`
  - Command shape: `uv run atlas-lab --atlas-profile rich --signal-max-indicators 2 --signal-instrument-limit 1 --signal-timeframe-limit 1 --active-probes 2 --limit 1 --discovery-queue default --json`
  - Gateway workers at launch: 14 slots total, with only 2 local Docker workers.
  - Result: completed successfully in 31 seconds.
  - Exercised and scored `anchor_pair` and `discovery_pair` via the gateway.
  - Tiny smoke surface did not produce validation or scrutiny candidates; this is expected and does not prove the full ladder.
  - The generated PlayHand seed plan carried `feature_schema_version=atlas_feature_vector_v1`.

Remaining proof gate:

- Completed the first full uncapped `rich-roles` proof run:
  - Run id: `20260702T202029258663Z-atlas-lab`.
  - Gateway capacity during the run: 18 workers total after scaling remote pools, with only 2 local Docker workers.
  - Signal surface: 88 indicators, 704 signal simulations, 704 successful, 0 failed.
  - Role coverage: 184 trigger, 296 setup, 160 context, 64 filter signal rows.
  - Anchor-pair stage: 48 selected/completed/scored rows.
  - Discovery-pair stage: 15,247 rows, 2,431 positive-score rows, 139 rows at or above 70.
  - Validation stage: 48 rows, 44 ok, 4 failed, 16 positive, 1 retained discovered recipe.
  - Scrutiny stage: 4 rows, all ok, all marked `failed_retention`; this proves the stage is reachable but did not produce a retained 36-month family in this run.
  - Gateway post-run: 0 queued, 0 live, 0 result backlog, 0 gateway task failures.
  - Published final priors to `runs/derived/recipe-priors` with `feature_schema_version=atlas_feature_vector_v1`.
- Compared against baseline `20260702T013718797753Z-atlas-lab`:
  - Baseline signal surface was trigger-only with 184 signal simulations.
  - `rich-roles` broadened empirical evidence to all role classes on the same market/timeframe panel.
  - Baseline recipe priors had no final scrutiny rows and legacy/null negative categories.
  - `rich-roles` produced 43 scoped negative pairs with reason/stage fields and 4 scrutiny rows.
- Refreshed the stable PlayHand seed plan after tightening maturity-aware source weighting:
  - `pre_36m_retention` now uses guided source mix `discovery_recipe_validation=0.25`, `curated_recipe_prior=0.75`.
  - `limited_36m_retention` uses `0.45 / 0.55`.
  - `broad_36m_retention` keeps `0.60 / 0.40`.
  - This keeps sparse 12-month-only discoveries selectable without letting one unproven discovery recipe dominate PlayHand sampling.
- Fixed and proved the timing stage after the first run exposed a lab-runner bug:
  - The generic gateway runner read only `queue_rows`; timing Atlas writes `timing_queue_rows`.
  - Added a `ProbeRunSpec.queue_key` and set the timing spec to `timing_queue_rows`.
  - Added a regression test that a timing probe spec using `timing_queue_rows` is selected, run, scored, and written.
  - Reran the full timing queue through the lab gateway: 310 selected, 310 completed, 304 ok, 4 skipped existing, 2 recorded failed rows, 0 lost gateway results.
  - Timing variant coverage: 96 trigger-side, 96 anchor-side, 118 both-side rows.
  - Timing outcomes: 16 improved, 10 rescued positive, 6 watch/default categories in final pair priors, and side-specific timing hints flowed into stable recipe priors.
  - Stable `pair-priors.csv` now has timing policies: 26 `allow_variant`, 9 `catalog_default_only`, 8 `catalog_default`, 6 `watch_variant`.
  - Stable `slot-indicator-priors.csv` now has side-specific timing evidence: 46 trigger, 16 anchor, and 16 both-side rows.
- Verified PlayHand consumption:
  - Direct sampler against `runs/derived/recipe-priors/play-hand-seed-plan.json` sees `atlas_feature_vector_v1`.
  - Soft negative pairs are active: 22 soft pair multipliers, 0 hard negative pairs, multiplier range `0.61` to `0.9925`.
  - Selected slots carry `effective_sampling_weight`, `atlas_feature_multiplier`, `negative_pair_multiplier`, `sample_confidence`, and `horizon_stability_bucket`.
  - A 2,000-sample check after the policy refresh produced 326 `discovery_recipe_validation`, 936 `curated_recipe_prior`, and 738 exploration/fallback selections, matching the conservative pre-36m intent.
  - A 1,000-sample check after the timing refresh produced 480 curated, 147 discovery-validation, and 373 exploration/fallback selections; sampled deals included `allow_variant`, `watch_variant`, `catalog_default_only`, trigger-side, anchor-side, and both-side timing hints.
  - Small `play-hand-massive-v2 --dry-run` loaded the refreshed stable seed plan in all 4 lanes and selected both discovered and curated recipes.

Staged proof gates, July 2-3, 2026:

- After measuring the full all-axis `rich` profile, the uncapped build surface is 13,572 local signal simulations before the gateway probe stages begin. That is too large for the first operational proof run and violates the rollout rule below by expanding roles, instruments, and timeframes at once.
- Added staged rich profiles:
  - `rich-roles`: trigger/setup/context/filter on the current instrument and timeframe panel.
  - `rich-timeframes`: role-expanded plus the expanded timeframe panel on the current instrument panel.
  - `rich-markets`: role-expanded plus the representative instrument panel on the current timeframe panel.
  - `rich`: all axes expanded together, reserved for later scale tests.
- Completed the full uncapped `rich-timeframes` proof run:
  - Run id: `20260702T235822738563Z-atlas-lab`.
  - Gateway capacity during the successful resume: 20 workers total after the local Docker pool was raised to 4, with 8 Sager and 8 Mac workers.
  - Signal surface: 88 indicators, 1,408 signal simulations, 1,408 successful, 0 failed.
  - Role coverage: 368 trigger, 592 setup, 320 context, 128 filter signal rows.
  - Timeframe coverage: `M1`, `M5`, `M15`, `H1` on the current market panel `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`.
  - Anchor-pair stage: 48 selected/completed/scored rows.
  - Timing stage: 312 selected/completed/scored rows, 308 ok, 4 recorded failed rows.
  - Discovery-pair stage: 15,247 rows, 1,516 positive-score rows, 140 strong rows, and all 15,247 probes completed through the lab gateway.
  - Discovery clustering: 7 first clusters, 7 second clusters, 48 cluster-pair rows, 24 discovered recipes.
  - Validation stage: 56 rows, producing 2 retained rows, 8 partial-retention rows, and 1 new-positive cluster expansion.
  - Scrutiny stage: 7 queued/completed 36-month rows; 1 came from strict retention and 6 from fallback partial-retention. The final prior build retained 4 discovered validation rows and 2 discovered recipe families.
  - Gateway post-run: 28,712 enqueued, 28,712 claimed, 28,712 completions accepted, 28,712 results acked, 0 failures, 0 duplicate enqueues, 0 dropped results, 0 incompatible claims, 0 stale workers.
  - Published final priors to `runs/derived/recipe-priors` with `feature_schema_version=atlas_feature_vector_v1`.
- Fixed an operational hang discovered during the first `rich-timeframes` attempt:
  - Atlas gateway task ids were not namespaced by run/stage, so three anchor-pair tasks collided with terminal task ids from older runs.
  - The lab gateway also reported `enqueued=len(tasks)` instead of the actual accepted count, allowing the Atlas runner to wait forever on rejected duplicate tasks.
  - Atlas task ids are now namespaced from source directory and probe kind, replay payload `job_id` mirrors the namespaced task id, and the gateway `/tasks` response reports `submitted`, `accepted`, `enqueued`, and `rejected`.
  - The Atlas enqueue helper now fails fast on partial acceptance instead of leaving zombie in-flight probe state.
- Verified PlayHand consumption of the new stable priors:
  - Stable manifest now points to `20260702T235822738563Z-atlas-lab`.
  - Stable seed plan is `atlas_feature_vector_v1`, maturity `limited_36m_retention`, with guided source mix `discovery_recipe_validation=0.45`, `curated_recipe_prior=0.55`.
  - Stable seed plan contains 6 recipes: 4 curated and 2 discovery-validation recipes.
  - A 2,000-sample direct sampler check produced 1,419 seed-plan-guided deals and 581 policy-exploration deals; guided deals split into 616 discovery-validation and 803 curated selections.
  - Sampled deals exercised side-aware timing hints: `allow_variant` with anchor, trigger, and both-side evidence; `catalog_default_only`; and catalog-default rows.
  - A small `play-hand-massive-v2 --dry-run` loaded the stable seed plan and generated 4 PlayHand lanes without runtime errors.
- Completed the full uncapped `rich-markets` proof run:
  - Run id: `20260703T030916507882Z-atlas-lab`.
  - Gateway capacity during the run: 20 workers total, with 4 local Docker workers, 8 Sager workers, and 8 Mac workers.
  - Signal surface: 88 indicators, 6,864 signal simulations, 6,864 successful, 0 failed.
  - Role coverage: 1,794 trigger, 2,886 setup, 1,560 context, 624 filter signal rows.
  - Market coverage: 39 representative instruments across FX majors/minors, metals, energies, core indices, and core crypto on the current `M5`/`M15` timeframe panel.
  - Forward-response local aggregation was the highest local-pressure point observed, temporarily reaching roughly 5.9 GB working set before dropping after the stage completed.
  - Anchor-pair stage: 48 selected/completed/scored rows.
  - Timing stage: 309 selected/completed/scored rows, 305 ok, 4 recorded failed rows.
  - Discovery-pair stage: 15,233 rows, 1,513 positive-score rows, 139 strong rows, and all 15,233 probes completed through the lab gateway.
  - Discovery clustering: 7 first clusters, 6 second clusters, 41 cluster-pair rows, 21 discovered recipes.
  - Validation stage: 48 rows, producing 4 partial-retention rows and no strict 36-month retained family.
  - Scrutiny stage: 4 queued/completed 36-month rows, all fallback partial-retention, all final `failed_retention`.
  - Gateway post-run: 57,457 enqueued, 57,457 claimed, 57,457 completions accepted, 57,457 results acked, 0 failures, 0 duplicate enqueues, 0 dropped results, 0 incompatible claims, 0 stale workers.
  - Published final priors to `runs/derived/recipe-priors` with `feature_schema_version=atlas_feature_vector_v1`.
- Verified PlayHand consumption of the new `rich-markets` stable priors:
  - Stable manifest now points to `20260703T030916507882Z-atlas-lab`.
  - Stable seed plan is `atlas_feature_vector_v1`, maturity `pre_36m_retention`, with guided source mix `discovery_recipe_validation=0.25`, `curated_recipe_prior=0.75`.
  - Stable seed plan contains 5 recipes: 4 curated and 1 discovery-validation recipe.
  - A 2,000-sample direct sampler check produced 1,208 seed-plan-guided deals and 792 policy-exploration deals; guided deals split into 261 discovery-validation and 947 curated selections.
  - Sampled deals exercised side-aware timing hints including anchor, trigger, and both-side variants.
  - A small `play-hand-massive-v2 --dry-run` loaded the stable seed plan and generated 4 PlayHand lanes without runtime errors.
- Interpretation:
  - `rich-timeframes` produced stronger retained discovery material than `rich-markets` in this run.
  - `rich-markets` broadened market evidence substantially, but its latest stable seed plan is intentionally more conservative because it did not retain a 36-month discovered family.
  - This proves the staged architecture and gateway path, but it also shows that "latest published" is not always the best discovery-yield artifact. A future publish policy may need to distinguish newest, best-retention, and explicit override artifacts.
- The next empirical decision point is whether to run the full all-axis `rich` profile, merge staged evidence, or add a stable-selection policy before letting broader runs overwrite PlayHand defaults.
- The fleet process-manager Atlas coordinator should use one staged profile at a time. For another representative-market run:
  `uv run atlas-lab --gateway-url http://127.0.0.1:8799 --atlas-profile rich-markets --active-probes 256 --enqueue-chunk-size 256 --result-batch-size 250 --max-results-per-cycle 1000 --max-drain-seconds 0.5 --poll-interval-seconds 0.25 --discovery-queue full --publish --json`
- The new smoke caps are for testing and diagnosis only; they should not be added to the production Atlas coordinator unless intentionally running a bounded diagnostic.

## Current Baseline

The latest pure-Atlas run was `20260702T013718797753Z-atlas-lab`.

Static catalog coverage is broader than the empirical surface:

- 88 catalog indicators total.
- Signal roles: 23 trigger, 37 setup, 20 context, 8 filter.
- Strategy roles: 30 mean-reversion, 24 trend, 16 confirm, 8 filter, 5 breakout, 5 unspecified.
- Preferred timeframe roles: 25 entry, 37 mid-setup, 26 higher-context.

The trigger-heavy part is specifically the empirical signal/forward-response stack:

- `build_signal_atlas` defaults to `signal_role="trigger"`.
- `run_atlas_lab` calls `build_signal_atlas(...)` with defaults.
- The signal atlas covered 23 trigger indicators only.
- Market panel was `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`.
- Timeframe panel was `M5`, `M15`.
- Forward response then operated on those 184 trigger/instrument/timeframe cells only.

The later pair-discovery stages are broader:

- Discovery pair atlas used 88 eligible indicators.
- It generated 15,248 discovery pair probes.
- It found 1,513 positive 3-month rows, 139 strong 3-month rows, and 21 discovered recipes.
- It promoted 48 rows into 12-month recipe validation.
- It retained 5 validation rows and 3 discovered recipes in recipe priors.
- It produced 0 36-month scrutiny rows.

Plainly: Atlas already searches combinations, but the earliest learned evidence is trigger-heavy, market coverage is narrow, timing evidence is absent in the latest run, and most combination evidence is still based on short 3-month screens.

## Goal

Atlas should be the self-bootstrapping prior builder:

```text
FuzzFolio catalog + replay/scoring infrastructure -> richer Atlas priors -> PlayHand search bias
```

It should not learn from PlayHand outcomes by default. PlayHand consumes Atlas; Atlas should not back-propagate PlayHand's generated corpus unless we explicitly run a separate experiment.

This is a code contract, not just a preference. `atlas_lab.py` calls `build_recipe_priors(..., include_playhand_outcome_priors=False)`, and standalone recipe-prior builds now keep PlayHand outcome priors disabled unless `--include-playhand-outcome-priors` is explicitly supplied. Any Atlas pipeline entry point must keep the one-way boundary explicit.

## First-Class Fixes Before Broader Priors

These are not optional nice-to-haves. They affect whether the richer Atlas work produces trustworthy evidence.

### 1. Fix Scrutiny Cluster Labels

Confirmed bug: `build_retained_scrutiny_queue_rows` currently copies cluster ids into `first_cluster_label` and `second_cluster_label`.

Why it matters:

- It makes scrutiny artifacts misleading.
- It will pollute any label-based diversity, family grouping, or report generation.
- The richer-priors plan depends on cluster labels becoming more important, so the labels must be correct first.

Required work:

- Copy the actual source labels, not the ids.
- Add a regression test that catches id-to-label copy mistakes.

### 2. Make 36-Month Scrutiny Reachable

Current issue: 36-month scrutiny is effectively a dead branch for this run. The current queue only admits strict retained buckets such as `retained` and `retained_strong`, so no scrutiny rows were generated.

Why it matters:

- The plan depends on 36-month evidence as the durability gate.
- A richer prior model will not help if the pipeline almost never exercises the final gate.
- A 12-month validation run can contain useful partial-retention rows that are not currently eligible for scrutiny.

Required work:

- Keep strict retained rows as the first priority.
- Add a small scrutiny fallback quota for high-sample, structurally useful rows such as `partial_retention` and selected `new_positive_cluster_expansion`.
- Make this quota explicit and bounded so scrutiny does not become a brute-force sink.
- Record the selection reason in the scrutiny queue.

### 3. Add Evidence Floors And Shrinkage

Current issue: small-sample winners can look better than durable but less explosive candidates. In the latest validation output, a 10-trade candidate had the top score, while larger partial-retention families had lower scores but more credible sample sizes.

Why it matters:

- Richer priors can amplify lucky sparse winners if sample confidence is not carried forward.
- This directly relates to live-overfit concerns.

Required work:

- Add minimum trade-count and unique-month coverage gates for validation and scrutiny ranking.
- Use shrinkage for low-sample rows instead of treating their score as equally reliable.
- Keep low-sample rows as "uncertain" rather than automatically "bad" if the shape is interesting.
- Carry `sample_confidence`, `trade_count`, and calendar coverage into recipe priors and seed plans.

### 4. Make Timing Evidence Real

Current issue: `build_pair_profile_document` supports both `anchor_lookback_bars` and `trigger_lookback_bars`, but `build_anchor_pair_timing_atlas` currently varies only the trigger side. The latest Atlas run also had 0 timing rows scored.

Why it matters:

- Setup/context timing may matter more than trigger timing in many strategies.
- Timing evidence is currently not a real contributor to recipe priors.
- The downstream timing aggregation currently keys around trigger-side evidence, so anchor timing would be ignored unless aggregation changes too.

Required work:

- Extend timing queues with `variant_side` values: `trigger`, `anchor`, and later `both`.
- Generate anchor-side timing profile docs using the existing profile-builder support.
- Update recipe-prior timing aggregation to preserve side-specific evidence.
- Add tests that anchor-side timing rows survive into the prior evidence layer.

### 5. Preserve Multi-Horizon Evidence

Current issue: recipe-prior evidence can collapse validation rows to the latest row per pair. That is acceptable if 36-month evidence replaces 12-month evidence, but it is not acceptable if we want both horizons as features.

Why it matters:

- A 12-month row and a 36-month row answer different questions.
- PlayHand should be able to see whether an idea improved, decayed, or stayed stable across horizons.

Required work:

- Add a multi-horizon evidence schema for recipe priors.
- Preserve 3-month discovery, 12-month validation, and 36-month scrutiny summaries separately.
- Add derived fields such as `retention_ratio_12m`, `retention_ratio_36m`, and `horizon_stability_bucket`.

### 6. Canonicalize Pair Families

Current issue: reciprocal pairs can appear as separate discovered recipes even when they are the same unordered family.

Why it matters:

- It inflates apparent diversity.
- It can cause Atlas to spend validation budget twice on the same idea.
- It makes family-level pressure weaker.

Required work:

- Add canonical unordered pair family ids.
- Preserve ordered role information separately.
- Use family ids for diversity, dedupe, and validation budget allocation.

## 1. Broaden The Observation Surface

Current problem: Atlas has static metadata for all roles, but empirical signal and forward-response evidence mostly describes trigger indicators. That makes non-trigger setup/context/filter choices more dependent on static priors and pair-level luck.

Plan:

- Add named Atlas profiles, starting with `standard` and `rich`.
- Keep `standard` close to today's cost envelope.
- Make `rich` expand role panels, instrument panels, and timeframe panels in one coordinated config.
- Run role-specific signal/response panels:
  - trigger: event density, direction balance, persistence, and forward response.
  - setup: state persistence, reversal/continuation response, confluence usefulness, and timing tolerance.
  - context/filter: market-regime coverage, selectivity, and whether filtering improves downstream pair quality.
- Expand instruments by representative buckets rather than arbitrary long lists:
  - FX majors for liquidity and spread behavior.
  - FX crosses to avoid learning only USD-driven behavior.
  - metals/energy because gold and oil behave differently from FX.
  - indices/crypto only if data quality and spread assumptions are good enough for research.
- Add timeframes beyond `M5` and `M15`, likely `M1`, `M5`, `M15`, and `H1` for the first rich profile.

Implementation shape:

- Introduce an `AtlasProfile` or equivalent runtime config in `atlas_lab.py`.
- Decide explicitly between:
  - multiple role-specific signal-atlas passes, or
  - widening `build_signal_atlas` to accept multiple roles.
- Store the chosen profile and panels in every summary artifact.
- Preserve backward compatibility by keeping the current default unless `--atlas-profile rich` is selected.
- Do not combine every surface-area expansion at once. First role expansion should stay on the existing market panel; instrument expansion should be a separate measured step.

Pass criteria:

- Empirical signal/forward artifacts include setup/context/filter rows, not only triggers.
- PlayHand seed plan can see role-specific empirical evidence for non-trigger slots.
- The richer profile remains resumable and can run through the lab gateway without changing worker contracts unnecessarily.

## 2. Preserve More Than One Score

Current problem: Atlas currently carries useful scores, but too many decisions collapse into one "good/bad" number. That hides whether a candidate is robust, pathologically bursty, instrument-specific, decaying, or under-sampled.

Plan:

- Carry feature vectors forward alongside scalar scores.
- Keep scalar scores for ranking, but do not make them the only thing downstream stages see.
- Add row-level evidence features at three levels:
  - Indicator cell features: signal density, direction balance, persistence, volatility/regime occurrence, forward-response horizon shape.
  - Pair/recipe features: trade count, score-lab axes, drawdown, profit factor, expectancy, retention ratio, source timeframe, source instrument panel.
  - Behavior/path features: trade burst concentration, temporal concentration, time under water, max drawdown, consecutive loss clusters, month/week stability.
- Start with metrics already available in existing outputs before inventing new replay math.

What this does:

- Sampling can prefer candidates that are not merely high-scoring, but high-scoring for the right reasons.
- Validation can route uncertain but interesting candidates to 12-month checks instead of discarding them.
- Diversity logic can compare strategy shapes, not just indicator names.
- Bad-result handling can become a soft prior with scope and reason, not a blunt blocklist.

Implementation shape:

- Add a `features` object to recipe-prior JSON rows and seed-plan entries first.
- Keep old fields for current consumers.
- Avoid flattening every feature into CSV immediately; the CSV writers are fixed-width and more likely to break downstream readers.
- Add a small feature schema version so PlayHand can ignore fields it does not understand.

Pass criteria:

- Existing PlayHand and portfolio tooling still reads current artifacts.
- New feature fields are present in recipe priors and seed plans.
- No hard behavior change is required until PlayHand explicitly consumes the richer fields.

## 3. Make Validation Less Brittle

Current problem: 3-month discovery is useful as a cheap screen, but it can be deceptive. The current run had 15,248 discovery probes, 48 12-month validation probes, and no 36-month scrutiny probes. That is too little long-horizon evidence for a system intended to bias future discovery.

Plan:

- Keep 3-month discovery as the cheap broad screen.
- Make 12-month validation the real first confidence gate.
- Make 36-month scrutiny a controlled final evidence layer for candidates that look durable or strategically important.
- Promote not only the top scalar scores, but also representative candidates from distinct clusters and pair families.
- Let some near-miss or high-uncertainty candidates through when they are structurally different from the current winners.
- Separate same-timeframe and mixed-timeframe families in validation; those are different risk classes.

Implementation shape:

- Map new CLI/process-manager knobs onto existing builder knobs first:
  - `max_recipes`
  - `max_pairs_per_recipe`
  - `first_member_limit`
  - `second_member_limit`
  - `max_rows`
- Add higher-level aliases only after the internal knobs are wired:
  - `--validation-candidates`
  - `--scrutiny-candidates`
  - `--min-cluster-validation`
  - `--near-miss-validation-share`
- Use random/as-of anchors consistently so the 3-month screen is not always the same recent window.
- Store source anchor date/window in every promoted validation row.
- Add sample floors and shrinkage before ranking validation candidates.

Pass criteria:

- Rich profile produces meaningful 12-month validation volume.
- Scrutiny gets rows when validation has useful candidates, even if strict retained buckets are sparse.
- Validation selection is explainable by score, sample confidence, cluster coverage, novelty, and uncertainty.

## 4. Add Soft Diversity Pressure

Current problem: Avoiding repeated "gold-like" or same-pair ideas is useful, but raw counts are the wrong signal. We should not penalize EURUSD just because there are many EURUSD attempts if none of them are good.

Plan:

- Apply diversity pressure against the inventory of good retained ideas, not against raw attempt counts.
- Compare candidates using a feature vector:
  - instrument bucket.
  - exact instrument.
  - timeframe.
  - same-timeframe versus mixed-timeframe shape.
  - indicator ids and indicator families.
  - canonical unordered pair family id.
  - ordered role mix.
  - cluster ids and cluster labels.
  - regime/context signature.
  - behavior shape: bursty versus smooth, sparse versus dense, trend versus reversal, short-hold versus long-hold.
  - validation strength and sample confidence.
- Use soft sampling multipliers, not hard bans.
- Keep an exploration floor so weird ideas still get a small share of capacity.

What this does:

- If we have many weak EURUSD ideas, Atlas can still explore EURUSD.
- If we have many strong XAUUSD M5 mean-reversion ideas, similar new XAUUSD M5 mean-reversion ideas get less priority.
- Structurally different candidates on an overrepresented instrument can still pass.
- Reciprocal pair duplicates stop pretending to be independent discoveries.

Implementation shape:

- Build a `retained_inventory` summary from current recipe priors and validation rows.
- Compute simple similarity first: weighted exact/cluster/category matches.
- Add canonical family ids before nearest-neighbor style scoring.
- Record `diversity_penalty`, `nearest_retained_similarity`, `diversity_reason`, and `canonical_pair_family_id` in validation queue rows.

Pass criteria:

- Queue composition shows broader instrument/timeframe/family spread without losing all high-score candidates.
- Diversity pressure is inspectable and reversible.
- No candidate is rejected solely because of one overloaded axis.

## 5. Explore Timing And Composition More Honestly

Current problem: `build_pair_profile_document` can vary both anchor and trigger lookback bars, but `build_anchor_pair_timing_atlas` currently varies only the trigger side. The current recipe shape also tends to assume one anchor/setup side and one trigger side, which may be too prescriptive.

Plan:

- Add timing sweeps on both sides of the pair:
  - trigger lookback variants.
  - anchor/setup lookback variants.
  - optionally paired variants for a small top subset.
- Treat setup timing as first-class. A setup indicator can become normal again before the entry trigger fires, so its lookback/persistence may be more important than trigger persistence.
- Loosen composition constraints over time:
  - continue supporting anchor + trigger.
  - allow setup + setup, context + trigger, context + setup, and limited trigger + trigger recipes.
  - avoid pre-penalizing combinations just because they look nontraditional.
- Detect pathological behavior after replay instead:
  - burst entries.
  - excessive same-direction stacking.
  - too many trades opened in one local reversal.
  - win-rate/profit-factor shapes that only work because of clustered lucky exits.

Implementation shape:

- Extend timing queue rows with `variant_side` values `anchor`, `trigger`, and later `both`.
- Add `anchor_lookback_bars` to timing profile generation.
- Update timing evidence aggregation in recipe priors so anchor-side timing is not dropped.
- Carry timing winners into recipe priors as side-specific evidence.
- Add burst/concentration features to validation rows before using them for penalties.

Pass criteria:

- Timing atlas can produce and score anchor-side variants.
- Anchor-side and trigger-side timing rows both survive into recipe-prior evidence.
- PlayHand can prefer a side-specific timing hint when present.
- Bursty lucky strategies are downweighted by measured behavior, not by assumed-bad indicator taxonomy.

## 6. Learn From Bad Results With Scoped Penalties

Current problem: "Bad" is too broad. A bad result might mean the pair is useless, or it might mean the pair was tested on the wrong instrument, wrong timeframe, wrong side, wrong lookback, or too short a sample.

Plan:

- Convert bad results into scoped negative evidence.
- Negative evidence should influence exploration weights and validation routing. It should not become a permanent global blocklist unless the evidence is overwhelming.
- Treat flat/no-event indicators and dead families as quarantine candidates, not as useful expansion targets.

Useful bad-result categories:

- Flat or no-event signal: deprioritize that indicator/timeframe/instrument cell; do not globally kill the indicator.
- Too dense or bursty: require spacing/hold-time safeguards or downweight similar behavior shapes.
- One-sided response: prefer the useful side or pair it with a direction/context filter.
- Instrument-specific failure: penalize that instrument bucket only.
- Timeframe-specific failure: try adjacent timeframes before global downweighting.
- Short-window fragility: route a small sample to alternate anchors or 12-month validation rather than discarding it immediately.
- Low sample confidence: mark as uncertain, not bad.
- Retention failure from 3-month to 12-month: penalize that recipe shape and source window more than the raw indicator pair.
- Validation decay: high 3-month score with weak 12-month retention gets a decay flag, not another round of similar brute-force variants.

How this helps in code:

- The queue builder can use scoped penalties to reduce repeated garbage without losing the whole search family.
- The validation builder can reserve some capacity for uncertain but diverse rows and avoid spending too much on known brittle shapes.
- PlayHand can sample away from bad local neighborhoods while retaining a wild-exploration floor.

Implementation shape:

- Add a `negative_evidence` table or artifact with:
  - scope type: indicator cell, pair cell, recipe, cluster, instrument bucket, timeframe, behavior shape.
  - reason category.
  - evidence strength.
  - sample confidence.
  - source run/stage.
  - expiration or decay policy.
- Merge scoped penalties into recipe-prior weights.
- Keep `negative_pair_priors.csv` for compatibility, but add richer reason fields when the CSV schema is intentionally versioned.

Pass criteria:

- Bad results produce inspectable soft penalties.
- Repeated low-value shapes become less frequent in generated queues.
- The system still preserves explicit wild exploration capacity.

## Implementation Phases

Phase 0 - evidence integrity and safety:

- Fix the discovered scrutiny queue label bug.
- Add tests for scrutiny labels.
- Add tests that Atlas pipeline calls keep `include_playhand_outcome_priors=False`.
- Make the Atlas -> PlayHand one-way boundary explicit in all Atlas entry points.
- Add profile metadata to Atlas run summaries.

Phase 1 - make the ladder usable:

- Add sample floors and shrinkage to validation ranking.
- Add scrutiny fallback quota for partial-retention and high-confidence near-miss candidates.
- Preserve multi-horizon evidence instead of collapsing 12-month and 36-month rows.
- Add canonical pair family ids.

Phase 2 - timing evidence:

- Add anchor-side timing variants.
- Update timing evidence aggregation to preserve variant side.
- Validate timing rows flow into recipe priors and seed plans.

Phase 3 - profiles and broader panels:

- Add `--atlas-profile standard|rich`.
- Implement role panels through multi-pass orchestration or a widened signal-atlas API.
- Keep `standard` as current behavior.
- Make `rich` opt-in until it is proven.
- Expand one dimension at a time: roles first, then instruments/timeframes.

Phase 4 - richer features:

- Add JSON-first feature objects to recipe-prior rows and seed-plan entries.
- Populate from existing probe output first.
- Add behavior features where existing artifacts already expose enough data.

Phase 5 - diversity and negative evidence:

- Add retained-inventory similarity.
- Add scoped negative evidence.
- Feed both into soft queue weights.

Phase 6 - PlayHand consumption and measurement:

- Teach PlayHand v2 to use the richer seed-plan fields as sampling weights.
- Keep fallbacks for old seed plans.
- Compare fixed-budget hit rate before and after.
- Measure promoted runs per hour and promoted runs per dollar separately from raw throughput.

## Practical Rollout Rule

Do not change every lever at once. The first experiments should isolate one major effect at a time:

1. Evidence integrity and ladder reachability.
2. Anchor-side timing.
3. Role-surface expansion on the current market panel.
4. Instrument/timeframe panel expansion.
5. Diversity and negative-evidence weighting.

This keeps us from mistaking "more compute and bigger artifacts" for actual improvement.

## Open Questions

- How wide should the first `rich` instrument panel be before it becomes wasteful?
- Which exact behavior/path metrics are already available cheaply from current outputs?
- Should 36-month scrutiny be a direct Atlas stage every run, or only when 12-month validation has enough retained plus fallback candidates?
- How should diversity pressure decay as the retained corpus ages?
- Which feature fields belong in JSON only, and which deserve intentional CSV schema versions?
- What minimum trade count and unique-month coverage should be used for 12-month and 36-month gates?

## Wide Discovery Probe Experiment - 2026-07-03

Intent:

- Test whether broadening the intermediate discovery-pair probe matrix improves PlayHand-useful priors.
- Keep the existing standard discovery market basket to avoid turning each probe into a much more expensive multi-market replay.
- Keep the upstream evidence surface aligned with the current stable `rich-markets` run so this experiment isolates discovery breadth.
- Broaden discovery probe timeframes from `M5,M15` to `M1,M5,M15,M30,H1,H4,D1`.
- With the current 88 generation-eligible indicators, the full ordered pair/timeframe matrix is `88 * 87 * 7 = 53,592` rows.

Current comparison baselines:

- `20260702T013718797753Z-atlas-lab` old baseline:
  - Signal successful: 184.
  - Forward response event horizons: 382,303.
  - Discovery queue rows: 15,248.
  - Discovery positives/strong/discovered recipes: 1,513 / 139 / 21.
  - Validation queue rows: 48.
  - Final priors: 650 slot rows, 53 pair rows, 3 discovered recipes, 5 retained discovery-validation rows, 39 negative pairs.
- `20260702T235822738563Z-atlas-lab` rich-timeframes:
  - Signal successful: 1,408.
  - Forward response event horizons: 3,255,926.
  - Discovery queue rows: 15,247.
  - Discovery positives/strong/discovered recipes: 1,516 / 140 / 24.
  - Validation queue rows/sample-floor pass: 56 / 15.
  - Scrutiny queue rows: 7, with 1 retained and 6 partial-retention rows.
  - Final priors: 647 slot rows, 52 pair rows, 2 discovered recipes, 4 retained discovery-validation rows, 48 negative pairs.
- `20260703T030916507882Z-atlas-lab` rich-markets, current stable at experiment start:
  - Signal successful: 6,864.
  - Forward response event horizons: 16,153,410.
  - Discovery queue rows: 15,233.
  - Discovery positives/strong/discovered recipes: 1,513 / 139 / 21.
  - Validation queue rows/sample-floor pass: 48 / 12.
  - Scrutiny queue rows: 4, all partial-retention fallback; final retention failed.
  - Final priors: 642 slot rows, 49 pair rows, 1 discovered recipe, 1 retained discovery-validation row, 42 negative pairs.

Next run:

- Profile: `rich-discovery`.
- Publish policy: do not publish first; compare final artifacts to current stable, then publish only if the delta is actually useful.
- Worker target at launch: 24 local/LAN slots, using 8 local Docker, 8 Sager, and 8 Mac workers.
- First attempted `rich-discovery` definition combined both rich markets and rich timeframes; that was stopped during forward-response aggregation after it reached about 32 GB RAM without producing output. The profile was narrowed to rich markets plus wide discovery before the production test run.

Completed run: `20260703T220442317774Z-atlas-lab`.

- Profile: `rich-discovery`.
- Worker slots: 24 local/LAN slots.
- Operational result:
  - Completed successfully.
  - Gateway finished with zero queued tasks, zero live tasks, zero result backlog, zero failed tasks, zero lost completions, and zero incompatible claims.
  - The run used compact probe artifacts and finished at about 7.6 GB on disk.
  - The coordinator memory stayed flat during the resumed run, roughly 43 MB working set.
- Discovery-pair layer:
  - Pair matrix rows: 53,592.
  - Discovery queue rows: 53,514.
  - Positive rows: 5,797.
  - Strong rows: 556.
  - Score distribution: 8,781 rows scored above 0, 5,797 scored at least 50, 3,072 scored at least 60, 556 scored at least 70, 64 scored at least 75, and 5 scored at least 78.
  - Timeframe positives / strong rows:
    - D1: 0 / 0.
    - H1: 1,399 / 133.
    - H4: 1,314 / 132.
    - M30: 1,184 / 123.
    - M15: 864 / 81.
    - M5: 646 / 57.
    - M1: 390 / 30.
  - Lane positives / strong rows:
    - `proven_neighbor`: 1,880 / 209.
    - `wild_diversity`: 1,939 / 176.
    - `plausible_novel`: 1,324 / 107.
    - `under_tested_role_correct`: 654 / 64.
- Discovery-cluster layer:
  - Cluster pair rows: 2.
  - Discovered recipes: 2.
  - This is the main bottleneck exposed by the wider probe pass: raw discovery breadth improved heavily, but the current clustering step condensed the evidence into very few recipe candidates.
- Validation and scrutiny:
  - 12-month validation queue rows: 8.
  - 12-month sample-floor pass rows: 4.
  - 36-month scrutiny queue rows: 3.
  - 36-month scrutiny sample-floor pass rows: 3.
  - Scrutiny selection reasons: 3 strict-retention rows.
- Final recipe priors:
  - Slot rows: 646.
  - Pair rows: 51.
  - Discovered recipes retained: 1.
  - Discovered recipe pair rows: 3.
  - Discovered validation retained rows: 3.
  - Negative pair rows: 5.
  - Retention buckets: 6 `retained_strong`, 1 `failed_retention`, and 4 `new_failed_cluster_expansion` source rows.
- New retained discovered material:
  - `KEY_REVERSAL_SIGNAL -> ULTOSC_TREND` on H1, retained through 12-month validation and 36-month scrutiny.
  - `ULTOSC_TREND -> KEY_REVERSAL_SIGNAL` on H1, retained through 12-month validation and 36-month scrutiny.
  - `CCI_MEAN_REVERSION -> ADOSC_TREND` on M30, retained through 12-month validation and 36-month scrutiny.
- Seed-plan effect:
  - `discovered_recipe_001` was added alongside the four curated recipes.
  - Seed-plan maturity: `limited_36m_retention`.
  - Guided source mix remains 45% discovery recipe validation and 55% curated recipe prior.
- Interpretation:
  - The wider discovery-pair search clearly improved the raw evidence surface versus the 15k-row runs.
  - It did not produce a broader final PlayHand prior set because the cluster-to-recipe selection layer is now the limiting step.
  - D1 was not useful in this 3-month discovery pass and should be removed or handled separately before repeating this exact profile.
  - Do not publish automatically from this run without deciding whether a narrow but 36-month-retained discovered recipe is desirable enough to become the current PlayHand default.

Recipe bottleneck diagnosis on 2026-07-04:

- Root cause: the discovery-cluster default similarity `0.22` was too permissive for the richer 53k-row evidence surface. It collapsed 88 first-side signatures into 1 first cluster and 88 second-side signatures into 2 second clusters, so the recipe layer could only emit 2 cluster-pair recipes despite 5,797 positive discovery rows.
- Validation then added a second choke: the default recipe validation cap was 8 recipes. That was acceptable when the cluster layer emitted only a handful of recipes, but it would hide most recovered recipe candidates once clustering was fixed.
- Code changes:
  - Raised discovery-cluster default similarity to `0.50`.
  - Raised the cluster and validation recipe caps to `128`, treating them as safety guards rather than selection logic. Similarity and confidence should decide quality; the cap should only catch pathological expansion.
  - Added cluster-shape, recipe-candidate, confidence, validation-selection, pair-cap, and catalog-filter diagnostics to stage summaries.
  - Added `pipeline_summaries` to `atlas-lab --json` output.
- No-backend replay against this run's existing discovery-pair results produced:
  - Clusters: first `10`, second `12`.
  - Cluster-pair rows: `118`.
  - Positive cluster-pair rows: `88`.
  - Recipe candidates before cap: `88`.
  - Discovered recipes with a `32` cap: `32`; with a `128` guard: all `88`.
  - High/promising recipes available at `0.50`: `43`.
  - Validation queue rows with the `128` guard: `344`.
  - Catalog-filter drops: `0`.

Follow-up threshold sweep:

- `0.45`: 32 total recipe candidates, 18 high/promising, 144 validation probes. Too coarse: weaker compatibility distribution and only 33 directly evidenced validation rows.
- `0.50`: 88 total recipe candidates, 43 high/promising, 344 validation probes with the 128 guard. Best current balance: richer than 0.45 without a large combinatorial blow-up.
- `0.55`: 256 total recipe candidates, 97 high/promising, 774 validation probes with the 128 guard. Higher-quality top 32, but much more dependent on caps and broader validation spend.
- Recommended next run: do not rerun full Atlas first. Rebuild cluster/validation from the existing rich-discovery pair results using the new defaults, run the widened 12-month validation queue, then run 36-month scrutiny and rebuild recipe priors. Use the survival rate and diversity at the tail to decide whether `0.55` is worth the extra validation budget.
- Downstream guard update: recipe-prior pair retention previously kept only 80 empirical pair rows after mixing curated and discovered pairs. That could become the next silent choke if the widened validation stream retains many discovered pairs. The default pair-prior guard was raised to 256 and the summary now records pair-prior rows before cap, after cap, truncated count, and before/after source counts.

Dial coverage from this run:

- Indicators: all current catalog indicators, `88/88`.
- Discovery pair matrix: full ordered pair matrix excluding self-pairs, across all catalog timeframes.
- Discovery timeframes: `M1,M5,M15,M30,H1,H4,D1`, `7/7`.
- Discovery instruments: `EURUSD,GBPUSD,USDJPY,XAUUSD`, intentionally not maxed.
- Upstream signal instruments: 39 rich-market instruments across FX majors, FX minors, metals, energies, core indices, and core crypto.
- Upstream signal timeframes: `M5,M15`, intentionally not maxed.
- Discovery lookback: 3 months.
- Recipe validation lookback: 12 months.
- Recipe scrutiny lookback: 36 months.

PlayHand impact:

- PlayHand's guided seed-plan path is recipe-shaped.
- When the guided prior branch is selected, PlayHand picks a weighted recipe first, then samples pair evidence from that recipe's `pair_menu`, then fills indicators from that recipe's `slot_menus`.
- Pair priors and slot priors are therefore not an independent top-level guided search pipe today; they mostly become actionable inside recipe objects.
- If the seed plan is missing, skipped by policy exploration, or has no usable weighted recipe, PlayHand falls back to role-balanced exploration.
- This means the current bottleneck matters a lot: the wide discovery pass can find thousands of positive pair/timeframe results, but if clustering collapses them into one narrow discovered recipe, the guided PlayHand search only receives one new high-level learned shape.
- The next Atlas improvement should focus on recipe extraction and diversity from positive pair evidence, not only on increasing pair probe volume.

Staged recipe-stream v2 result on 2026-07-04:

- Run folder: `runs/derived/atlas-runs/20260704T180024472466Z-atlas-recipe-stream-v2`.
- Source evidence: reused `20260703T220442317774Z-atlas-lab/discovery-pair-atlas` and `recipe-priors-layer3`; did not rerun full upstream Atlas and did not publish.
- Gateway state:
  - Validation and scrutiny both ran through the lab gateway.
  - Worker slots: 24.
  - Gateway ended with zero queued tasks, zero result backlog, zero failed gateway tasks, and zero stale workers.
  - Artifact footprint: about 51 MB across 1,150 files because compact probe artifacts were used.
- Rebuilt cluster layer with new defaults:
  - Scored pair rows: 53,514.
  - Positive rows: 5,797.
  - Strong rows: 556.
  - First-side clusters: 10.
  - Second-side clusters: 12.
  - Cluster-pair rows: 118.
  - Discovered recipes: 88.
- 12-month validation:
  - Queue rows: 344.
  - Results: 344 completed/scored; 316 `ok`, 28 `failed`.
  - Retained-for-priors rows before scrutiny: 71.
  - Unique retained recipe ids before scrutiny: 35.
  - Retention buckets: 17 `retained_strong`, 14 `retained`, 23 `partial_retention`, 9 `new_strong_cluster_expansion`, 8 `new_positive_cluster_expansion`, 59 `failed_retention`, 188 `new_failed_cluster_expansion`, and 26 `new_low_cluster_expansion`.
  - Retained timeframes: H4 26, H1 21, M30 12, M15 5, M5 5, M1 2.
- 36-month scrutiny:
  - Queue rows: 39.
  - Results: 39 completed/scored; 39 `ok`.
  - Retained-for-priors rows after scrutiny: 12.
  - Unique retained recipe ids after scrutiny: 10.
  - Retention buckets: 6 `retained_strong`, 4 `retained`, 2 `partial_retention`, and 27 `failed_retention`.
  - Retained timeframes: M30 3, M15 3, H4 2, H1 2, M5 2.
- Final recipe priors:
  - Slot rows: 714, versus 642 current published and 646 from the bottlenecked rich run.
  - Pair rows: 92, versus 49 current published and 51 from the bottlenecked rich run.
  - Discovered validation source rows: 383.
  - Discovered validation retained rows: 44.
  - Discovered recipe count: 27, versus 1 current published and 1 from the bottlenecked rich run.
  - Discovered recipe pair rows: 44, versus 1 current published and 3 from the bottlenecked rich run.
  - Negative pair rows: 272, which is expected from preserving failed recipe evidence instead of silently dropping it.
  - Pair source mix: 44 `discovery_recipe_validation` rows and 48 `anchor_pair_atlas` rows.
- Top discovered 36-month-retained pair priors:
  - `KEY_REVERSAL_SIGNAL -> ULTOSC_TREND` H1, 36m score 72.409, `retained_strong`.
  - `ULTOSC_TREND -> KEY_REVERSAL_SIGNAL` H1, 36m score 72.409, `retained_strong`.
  - `MOM_MEAN_REVERSION -> MFI_TREND` M15, 36m score 73.4958, `retained_strong`.
  - `MFI_TREND -> BOLLINGER_KELTNER_SQUEEZE_FILTER` H4, 36m score 72.5492, `retained_strong`.
  - `CCI_MEAN_REVERSION -> ADOSC_TREND` M30, 36m score 70.6935, `retained_strong`.
  - `STOCHRSI_MEAN_REVERSION -> BUFFERED_RANGE_BREAKOUT_SIGNAL` M15, 36m score 73.3839, `retained_strong`.
  - `CMO_TREND -> BOLLINGER_KELTNER_SQUEEZE_FILTER` H4, 36m score 67.9318, `retained`.
- Interpretation:
  - The cluster/validation choke was the real recipe-yield bottleneck. Loosening it at `0.50` produced materially richer PlayHand priors without rerunning upstream Atlas.
  - The survival funnel is now visible: 88 recipe candidates -> 344 12m probes -> 71 12m usable rows -> 39 36m probes -> 12 36m usable rows -> 27 discovered recipes in final priors because retained 12m-only material is still preserved with lower maturity.
  - This is a stronger candidate for publishing than the previous rich run because it gives PlayHand many more discovered recipe shapes while still carrying 36-month survival evidence for the strongest subset.
  - The next bottleneck is qualitative, not quantity-only: decide whether `0.50` is good enough to publish and test in PlayHand, or run a controlled `0.55` validation experiment to see whether finer clusters improve 36-month survival rate enough to justify about 2.25x validation probes.

Published default and full-upstream-rich controlled-discovery run on 2026-07-06:

- Published current winner:
  - Source run: `runs/derived/atlas-runs/20260704T180024472466Z-atlas-recipe-stream-v2/recipe-priors`.
  - Published target: `runs/derived/recipe-priors`.
  - Publish manifest: `runs/derived/recipe-priors/atlas-lab-publish-manifest.json`.
  - Prior default was backed up before replacement.
- New experiment run:
  - Run folder: `runs/derived/atlas-runs/20260706T232304665612Z-atlas-lab`.
  - Command shape: `atlas-lab --atlas-profile rich --signal-atlas-executor gateway --active-probes 256 --enqueue-chunk-size 256 --result-batch-size 250 --max-results-per-cycle 1000 --max-drain-seconds 0.5 --poll-interval-seconds 0.25 --discovery-queue full --json`.
  - Published default was not overwritten by this run.
- Worker pool:
  - Local desktop Docker workers: 0.
  - LAN workers: 8 `sager-lan`, 8 `mac-lan`.
  - Vast workers: 3 AMD instances, 195 registered `vast-burst` workers.
  - Peak gateway slots observed: 211.
  - Vast hourly rate while active: about `$0.774/hr`; instances were destroyed immediately after run completion.
- Gateway/runtime result:
  - Full run completed in about 26 minutes from Atlas process start to final JSON log write.
  - Gateway task failures: 0.
  - Result backlog stayed small and never entered result backpressure.
  - Large discovery-pair stage sustained roughly 90-96% slot saturation once fully warmed.
- Upstream signal surface:
  - Signal calls: 13,728 successful, 0 failed.
  - Roles: trigger, setup, context, filter.
  - Instruments: 39 rich-market instruments.
  - Timeframes: M1, M5, M15, H1.
- Controlled discovery/downstream surface:
  - Discovery pair queue rows: 15,223.
  - Discovery pair positives: 1,508.
  - Discovery pair strong rows: 138.
  - Available discovered recipes: 128, versus 88 in the published default run.
  - 12-month validation queue rows: 416, versus 344 in the published default run.
  - 36-month scrutiny queue rows: 105, versus 39 in the published default run.
- Final main recipe priors:
  - Slot rows: 787, versus 714 in the published default.
  - Pair rows: 155, versus 92 in the published default.
  - Pair source mix: 48 anchor-pair rows and 107 discovery-recipe-validation rows.
  - Discovered validation rows: 521, versus 383 in the published default.
  - Discovered validation retained rows: 107, versus 44 in the published default.
  - Discovered recipe count: 39, versus 27 in the published default.
  - Negative pair rows: 297, versus 272 in the published default.
  - PlayHand seed-plan recipe objects: 43, versus 31 in the published default.
  - Seed-plan 36-month retained family count: 36, versus 9 in the published default.
- Layer-3 recipe priors:
  - Produced separately under `recipe-priors-layer3`.
  - Anchor-only in this run: 48 pair rows, 4 seed recipes, and no discovered validation rows.
  - Treat the main `recipe-priors` directory as the relevant PlayHand candidate for this experiment.
- Interpretation:
  - Full upstream rich signal evidence increased the amount of useful recipe material even while discovery/downstream stayed controlled.
  - The main new prior set looks materially richer than the published default by count, retention breadth, and discovery-validation contribution.
  - The result is a strong candidate for the next PlayHand A/B, but it should not become the default solely from Atlas richness metrics; the next decision point is whether it improves PlayHand promotion rate and promoted-run quality versus the currently published `recipe-stream-v2` default.
