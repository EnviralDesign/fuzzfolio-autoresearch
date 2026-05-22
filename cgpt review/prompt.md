# CGPT Review Packet

Please review this Fuzzfolio AutoResearch work as a technical/design reviewer. The goal of this work was to move from blind indicator generation toward evidence-guided, recipe-aware Play Hand generation without hard-filtering away odd but potentially useful combinations.

## What Changed

The code now builds and uses a layered indicator/recipe prior system:

1. Static indicator catalog audit.
2. Signal behavior atlas.
3. Forward-response atlas.
4. Curated anchor-pair atlas.
5. Timing/lookahead variant atlas.
6. Broad discovery-pair backend run across ordered indicator pairs.
7. Discovery cluster atlas that groups indicators by empirical pair behavior.
8. Discovery recipe validation atlas that checks the strongest discovered cluster recipes over 12 months.
9. Recipe priors rebuilt from all of the above.
10. Play Hand now automatically reads `runs/derived/recipe-priors/play-hand-seed-plan.json` and samples weighted curated/discovered recipes while preserving role-balanced exploration fallback.

Implementation commit:

- `2742170 Wire discovered recipe priors into play hand`

This review packet commit should follow that one.

## Key Implementation Files

Review these first in the repo root, not inside this copied artifact folder:

- `autoresearch/recipe_priors.py`
- `autoresearch/play_hand.py`
- `autoresearch/__main__.py`
- `tests/test_recipe_priors.py`
- `tests/test_play_hand.py`
- `README.md`
- `cli.md`

## Important Cached Results

The folder contains selected cached artifacts copied from `runs/derived/`. I intentionally omitted very large generated files such as the full 109 MB `discovery-pair-atlas.json`, the 27 MB full discovery run manifest, and bulky per-probe output/profile directories. The included files should be enough to inspect the evidence chain and question the decisions.

Important included paths:

- `indicator-atlas/`
- `signal-atlas/`
- `forward-response-atlas/`
- `anchor-pair-atlas/`
- `anchor-pair-timing-atlas/`
- `recipe-priors/`
- `discovery-pair-atlas/`
- `discovery-cluster-atlas/`
- `discovery-recipe-validation-atlas/`
- `processes/processes.json`

## Current Findings

Broad discovery-pair run:

- Eligible indicators: 87.
- Full queued backend probes: 14,916.
- Completed/scored: 14,916.
- Statuses: 14,765 `ok`, 151 `skipped_existing`.
- Positive discovery rows by lane: 462 `plausible_novel`, 254 `proven_neighbor`, 27 `under_tested_role_correct`, 503 `wild_diversity`.

Discovery clustering:

- Positive pair rows: 1,246.
- Strong pair rows: 184.
- Discovered recipes emitted: 32.

Discovery recipe validation:

- Validation rows: 64.
- Statuses: 64 `ok`.
- Retention buckets: 6 `retained_strong`, 7 `retained`, 2 `partial_retention`, 1 `new_positive_cluster_expansion`, 10 `failed_retention`, 37 `new_failed_cluster_expansion`, 1 `new_low_cluster_expansion`.

Recipe-prior rebuild after validation:

- Indicator rows: 87.
- Slot-prior rows: 668.
- Pair-prior rows: 64.
- Discovered validation rows consumed: 64.
- Retained discovered validation rows promoted: 16.
- Discovered recipe priors created: 7.

Sampling smoke:

- Focused tests passed: `uv run pytest tests/test_recipe_priors.py tests/test_play_hand.py -q`.
- A sampling check over the real indicator atlas showed Play Hand selecting discovered recipes such as:
  - `discovered_recipe_006`: `WILLR_MEAN_REVERSION + RSI_CROSSBACK`
  - `discovered_recipe_002`: `CHANNEL_REENTRY + WAVETREND_CROSSOVER`
  - `discovered_recipe_003`: `MFI_TREND + OBV_MEAN_REVERSION`
  - `discovered_recipe_008`: `VQI_DIRECTIONAL_QUALITY + WILLR_MEAN_REVERSION`

## Things I Want Reviewed

1. Is the discovered recipe promotion threshold too permissive?
   - Current included buckets: `retained_strong`, `retained`, `partial_retention`, `new_strong_cluster_expansion`, `new_positive_cluster_expansion`.
   - I suspect `partial_retention` should maybe stay as lower-weight experimental, not be treated as stable.

2. Are the discovered recipe slot names too generic?
   - Current discovered slots are `context_or_setup_cluster` and `trigger_or_response_cluster`.
   - This intentionally avoids pretending the discovered clusters map cleanly to curated recipe slot semantics, but it may make downstream reasoning weaker.

3. Is the Play Hand integration too eager?
   - It reads the seed plan automatically when present.
   - It uses `guided_prior_fraction` from the seed plan, defaults to 0.80.
   - It keeps `role_balanced_policy_exploration` fallback.

4. Should Play Hand force at least two indicators when a seed plan exists?
   - Current default can still deal one indicator because the existing `--min-indicators` default is 1.
   - Recipe evidence is pair-based, so single-indicator hands are less useful for this new path.

5. Should discovered recipes use profile docs or the actual validated probe profile as a template?
   - Current implementation only uses pair/slot indicator IDs as priors.
   - It does not yet carry exact config, timeframe, lookback, or reward-grid choices from successful validation probes into Play Hand scaffold defaults.

6. Are we losing useful "looked good alone but bad paired" information?
   - Failed cluster expansion rows are currently not promoted.
   - They may still be useful as negative constraints or "needs different partner" clues.

7. What is the right next experimental branch?
   - A. Add 36-month validation for retained discovered recipe priors.
   - B. Carry validated pair config/timeframe/lookback defaults into Play Hand.
   - C. Build a discovered-recipe dashboard/report focused on why each recipe was promoted.
   - D. Broaden the discovery corpus with more timeframes/instruments.
   - E. Add pair-negative evidence so Play Hand avoids combinations that repeatedly fail.

## My Current Opinion

This is useful and worth keeping, but it is still a first-generation prior system. The broad run produced real evidence that some discovered structures retain over 12 months, and Play Hand now has a direct path to sample them. The next highest-leverage improvement is probably to preserve more of the validated probe context: timeframe, lookback bars, and maybe parameter defaults. Right now Play Hand samples the discovered pair but then still scaffolds mostly from catalog defaults and its normal sweep process.

The second highest-leverage improvement is a 36-month validation gate for the promoted discovered priors before we let them dominate generation.

