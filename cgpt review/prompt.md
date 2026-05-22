# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This is the follow-up pass after your prior review. The goal is still to move from blind indicator generation toward evidence-guided, recipe-aware Play Hand generation without hard-filtering away odd but potentially useful combinations.

## What Changed In This Wave

The latest work implements the highest-leverage items from your previous recommendations:

1. Recipe priors now carry validated pair template context into `play-hand-seed-plan.json`.
   - Retained discovered validation rows include `recommended_profile_template`.
   - The template preserves per-indicator `timeframe`, `lookbackBars`, `ranges`, `talibConfig`, `weight`, `isTrendFollowing`, `normalizationMode`, `useFormingBar`, and `scale`.

2. Play Hand now applies those validated pair defaults after scaffold/role-timeframe assignment and before normal defaults/sweeps.
   - The intent is not to freeze the profile forever.
   - The intent is to start from the exact two-indicator context that retained, then let the existing sweep machinery vary around it.

3. Guided seed-plan deals now force at least two indicators.
   - If a seed plan exists, `cmd_play_hand` raises the effective min/max to at least `2`.
   - Metadata now records both requested and effective min/max indicator counts.

4. Recipe priors now emit negative evidence.
   - `pair-negative-priors.csv`
   - `cluster-expansion-negative-priors.csv`
   - `retention-failures.csv`
   - The seed plan includes the top negative pairs, and Play Hand avoids adding slot/fill indicators that form an exact known negative unordered pair with already selected indicators. Positive pair-menu picks are allowed through so validated retained pairs are not accidentally blocked.

5. Promotion is more conservative before 36-month evidence exists.
   - `new_positive_cluster_expansion` was demoted from medium-prior behavior to uncertain-prior behavior.
   - Unknown signal/forward evidence is no longer treated as neutral `50`; it is scored as `42`.
   - The seed plan uses `60/25/15` guided/uncertain/wild until 36-month retained evidence exists, then moves to `80/15/5`.

6. A 36-month scrutiny queue builder was added.
   - New command: `uv run build-discovery-recipe-scrutiny-atlas`
   - It reads retained 12-month validation results and writes a validation-compatible 36-month queue under `runs/derived/discovery-recipe-scrutiny-atlas/`.
   - The existing runner can execute it with:
     `uv run run-discovery-recipe-validation-probes --atlas-dir runs/derived/discovery-recipe-scrutiny-atlas --workers 32`

## Key Implementation Files

Review these first in the repo root:

- `autoresearch/recipe_priors.py`
- `autoresearch/play_hand.py`
- `autoresearch/discovery_recipe_validation.py`
- `autoresearch/__main__.py`
- `tests/test_recipe_priors.py`
- `tests/test_play_hand.py`
- `tests/test_discovery_recipe_validation.py`
- `README.md`
- `cli.md`
- `pyproject.toml`

## Important Cached Results In This Packet

The folder contains selected cached artifacts copied from `runs/derived/`. I intentionally omitted bulky per-probe output/profile directories and huge raw artifacts where possible.

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
- `discovery-recipe-scrutiny-atlas/`
- `processes/processes.json`

## Current Findings After This Wave

Recipe-prior rebuild after the 12-month validation corpus:

- Indicator rows: 87.
- Slot-prior rows: 668.
- Pair-prior rows: 64.
- Discovered validation rows consumed: 64.
- Retained discovered validation rows promoted: 16.
- Discovered recipe priors created: 7.
- Validated template rows carried into pair priors: 16.
- Negative pair rows emitted: 48.
- Cluster negative rows emitted: 16.
- Retention-failure rows emitted: 10.

Retention buckets from validation:

- 6 `retained_strong`
- 7 `retained`
- 2 `partial_retention`
- 1 `new_positive_cluster_expansion`
- 10 `failed_retention`
- 37 `new_failed_cluster_expansion`
- 1 `new_low_cluster_expansion`

36-month scrutiny queue:

- Source validation rows: 64.
- Queued retained scrutiny rows: 13.
- Source buckets: 6 `retained_strong`, 7 `retained`.
- Output: `discovery-recipe-scrutiny-atlas/`

Verification:

- `uv run pytest tests/test_recipe_priors.py tests/test_discovery_recipe_validation.py tests/test_play_hand.py -q`
- Result: 57 passed.
- `uv run play-hand --dry-run --json --min-indicators 1 --max-indicators 1`
- Result: forced effective deal count to 2 while recording requested min/max as 1/1.

## Questions For This Review

1. Is the template carry-forward implemented at the right level?
   - Current approach carries exact pair profile defaults from validation into Play Hand scaffold.
   - It does not yet carry reward matrix or instrument-panel context into Play Hand’s run configuration.
   - Should instrument panel/reward context be carried too, or is that too constraining?

2. Is the negative-pair behavior too blunt?
   - Current Play Hand avoidance uses exact unordered pair keys from failed validation rows.
   - It does not yet downweight broader cluster-family failures during recipe/slot menu construction except by artifact/reporting.
   - Should cluster-level failure rates actively reduce slot or pair sampling weights now?

3. Is the conservative `60/25/15` pre-36m policy reasonable?
   - The seed plan automatically moves to `80/15/5` only if a 36-month retained result is present in the consumed validation/scrutiny rows.
   - Is that too cautious, not cautious enough, or about right?

4. Should 36-month scrutiny results be folded back through the same `discovery-recipe-validation-results.csv` contract?
   - Current scrutiny atlas intentionally writes validation-compatible artifacts so the existing runner works with `--atlas-dir`.
   - `build-recipe-priors` now also looks for `runs/derived/discovery-recipe-scrutiny-atlas/discovery-recipe-validation-results.csv` when present.

5. What should the next branch be?
   - A. Run the 13-row 36-month scrutiny queue now, then rebuild priors.
   - B. Add cluster-level negative downweighting before the 36-month run.
   - C. Add a human-readable recipe report/dashboard explaining promotions, failures, templates, and avoid-pairs.
   - D. Carry instrument/reward context into Play Hand templates.
   - E. Broaden discovery to additional instruments/timeframes only after the above learning loop is stronger.

## My Current Opinion

This wave closes the biggest implementation gap from the first version: Play Hand no longer just receives indicator IDs from retained discovered recipes; it can now start from the validated two-indicator profile context. The system also starts learning from failures instead of merely ignoring them.

The most likely next operational move is to run the 13-row 36-month scrutiny queue:

```powershell
uv run run-discovery-recipe-validation-probes --atlas-dir runs/derived/discovery-recipe-scrutiny-atlas --workers 32
uv run build-recipe-priors
```

After that, I would review whether 36-month retained families should graduate to the mature `80/15/5` sampling policy and whether cluster-level negative evidence is strong enough to actively downweight broader slot menus.
