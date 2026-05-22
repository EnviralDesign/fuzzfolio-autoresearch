# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This is the follow-up pass after your feedback about negative-pair fill bypasses, 36-month scrutiny profile drift, and cluster-negative rates.

## Latest Commit Scope To Review

The latest work does four things:

1. **Fixes negative-pair avoidance in Play Hand fill.**
   - `role_balanced_fill` now goes through the same `add_indicator()` path as slot-menu candidates.
   - It keeps trying alternatives if a candidate would create a severe known negative pair.
   - Only severe `positive_discovery_collapsed` rows with `negative_weight >= 1.5` become hard unordered avoid-pairs.
   - Lower-severity negatives remain evidence artifacts for reporting or future soft downweighting.

2. **Makes 36-month scrutiny profiles copy the retained 12-month source profile.**
   - `build-discovery-recipe-scrutiny-atlas` now copies the exact source validation profile when `source_validation_probe_id` is available.
   - It only rewrites name/description, unless the user explicitly overrides instruments.
   - The latest scrutiny atlas copied 13 of 13 source profiles.

3. **Recomputes cluster-negative evidence over all validation rows.**
   - `cluster-expansion-negative-priors.csv` now includes `tested_count`, `retained_count`, `retained_strong_count`, `partial_count`, failure bucket counts, `failure_rate`, `retained_rate`, median score, and a proposed `soft_penalty_multiplier`.
   - The current code still does not actively downweight menus from cluster-level rates. It only emits a more truthful artifact.

4. **Runs the 13-row 36-month scrutiny queue and rebuilds recipe priors.**
   - Command run:
     `uv run run-discovery-recipe-validation-probes --atlas-dir runs/derived/discovery-recipe-scrutiny-atlas --workers 32`
   - Then:
     `uv run build-recipe-priors`

## Key Implementation Files

Review these first in the repo root:

- `autoresearch/play_hand.py`
- `autoresearch/recipe_priors.py`
- `autoresearch/discovery_recipe_validation.py`
- `tests/test_play_hand.py`
- `tests/test_recipe_priors.py`
- `tests/test_discovery_recipe_validation.py`
- `README.md`
- `cli.md`

## Important Cached Results In This Packet

Important included paths:

- `recipe-priors/`
- `discovery-recipe-scrutiny-atlas/`
- `discovery-recipe-validation-atlas/`
- `discovery-cluster-atlas/`
- `discovery-pair-atlas/`
- `anchor-pair-atlas/`
- `anchor-pair-timing-atlas/`
- `indicator-atlas/`
- `signal-atlas/`
- `forward-response-atlas/`

## 36-Month Scrutiny Results

The 36-month queue had 13 rows:

- Completed: 13.
- Statuses: 13 `ok`.
- Retained: 4.
- Partial retention: 3.
- Failed retention: 6.

Top 36-month retained rows:

- `BBANDS_POSITION_TREND + MA_SPREAD_MEAN_REVERSION`: 68.3204, retained.
- `WILLR_MEAN_REVERSION + RSI_CROSSBACK`: 66.0566, retained.
- `RSI_CROSSBACK + WILLR_MEAN_REVERSION`: 66.0084, retained.
- `MFI_TREND + OBV_MEAN_REVERSION`: 62.6214, retained.

Partial 36-month rows:

- `OBV_MEAN_REVERSION + PLUS_DI_TREND`: 55.8862.
- `CHANNEL_REENTRY + THRUST_BAR_SIGNAL`: 54.3905.
- `THRUST_BAR_SIGNAL + CHANNEL_REENTRY`: 53.8936.

Several 12-month retained rows collapsed to 0.0 at 36 months, including:

- `MACD_CROSSOVER + CHANNEL_REENTRY`
- `CHANNEL_REENTRY + MACD_CROSSOVER`
- `VQI_DIRECTIONAL_QUALITY + WILLR_MEAN_REVERSION`
- `WILLR_MEAN_REVERSION + VQI_DIRECTIONAL_QUALITY`
- `WAVETREND_CROSSOVER + CHANNEL_REENTRY`
- `CHANNEL_REENTRY + WAVETREND_CROSSOVER`

## Recipe Priors After 36-Month Rebuild

After the 36-month run, `build-recipe-priors` now consumes both 12-month validation and 36-month scrutiny results.

Important detail: promotion now uses the latest/highest-lookback row per exact recipe + ordered pair + timeframe. A 12-month retained row is no longer promoted if the same pair failed at 36 months.

Current counts:

- Source validation rows consumed: 77.
- Latest exact pair rows after superseding: 64.
- Promoted retained/partial/latest discovered rows: 10.
- Discovered recipe count: 6.
- Discovered pair-prior rows: 10.
- Validated template rows carried: 10.
- Negative pair rows: 54.
- Cluster-negative rows: 16.
- Retention-failure rows: 16.
- Seed-plan maturity: `has_36m_retention`.
- Sampling policy: `guided=0.80`, `uncertain=0.15`, `wild=0.05`.

## Verification

Commands run:

```powershell
uv run python -m py_compile autoresearch\recipe_priors.py autoresearch\play_hand.py autoresearch\discovery_recipe_validation.py autoresearch\__main__.py
uv run pytest tests\test_recipe_priors.py tests\test_discovery_recipe_validation.py tests\test_play_hand.py -q
uv run build-discovery-recipe-scrutiny-atlas --json
uv run run-discovery-recipe-validation-probes --atlas-dir runs\derived\discovery-recipe-scrutiny-atlas --workers 32
uv run build-recipe-priors --json
```

Targeted tests: 59 passed.

## Questions For This Review

1. Is the latest/highest-lookback superseding policy correct?
   - Current behavior: if a 36m result exists for the same recipe + ordered pair + timeframe, it replaces the 12m row for promotion decisions.
   - This means 12m retained rows that failed at 36m are not promoted, but they remain in negative evidence.

2. Is hard negative-pair avoidance now scoped correctly?
   - Current hard block: only `negative_reason == positive_discovery_collapsed` and `negative_weight >= 1.5`.
   - Hard block is unordered across timeframes.
   - Lower severity negatives are not currently blocking Play Hand.

3. Should cluster-level soft penalties be applied now?
   - The artifact now has real rates over all tested rows.
   - Proposed fields exist, but no active slot/pair downweighting uses them yet.

4. Does 4 retained out of 13 36-month scrutiny rows justify the mature `80/15/5` policy?
   - The seed plan now marks `has_36m_retention`.
   - We might still choose a middle policy, for example `70/20/10`, until more 36m retained families exist.

5. What is the next best branch?
   - A. Add active cluster-level soft downweighting using the new true failure/retained rates.
   - B. Add a recipe report/dashboard explaining promoted, partial, and failed discovered families.
   - C. Add template instrument policy (`off`, `seed_pool`, `initial_basket`).
   - D. Add reward-context metadata/policy.
   - E. Broaden discovery to additional instruments/timeframes now that 36m gate exists.

## Current Opinion

The 36-month scrutiny did exactly what it was supposed to do: it kept a few families alive and exposed several 12-month survivors as fragile. The best retained discovered structures now look meaningfully stronger than before because they survived 3m discovery, 12m validation, and 36m scrutiny.

I would probably review the `80/15/5` promotion threshold next. The code mechanically graduates once any 36m retained result exists. That may be too binary. A more nuanced version could scale policy by number of retained 36m families and retained-rate.
