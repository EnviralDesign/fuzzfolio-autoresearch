# Family-Policy V1 Comparison Index

Generated from `runs/derived/playhand-prior-test-clean-50-family-policy-v1`.

## Batch Identity

- Batch: `playhand-prior-test-clean-50-family-policy-v1`
- Seeds: `151..200`
- Command shape: `uv run play-hand --seed <seed> --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json`
- Status: `50/50` completed, `0` failed

## Headline Result

- Promotions: `27/50` (`54%`)
- Tombstoned: `23/50`
- Exact-template branches materialized: `10`
- Selected branch: `43` mutated, `7` exact-template
- Exact-template impact: `4` rescues, `3` exact-template outscored mutated, `3` mutated improved over exact template
- Source hit rates from report: discovered `100%`, curated `35%`, policy exploration `53%`
- Pair/template concentration: top family share `8%`, unique promoted pair/template families `13`

## Comparison Artifact Names

The report script names comparisons by batch size, so the current family-policy run appears as `Clean 50`. Interpret them as:

- `recipe-performance-comparison-clean50-vs-clean50.md`: original clean-50 versus `clean-50-family-policy-v1`
- `recipe-performance-comparison-clean100-vs-clean50.md`: clean-100 versus `clean-50-family-policy-v1`

## Data Hygiene Notes

- `batch_status.completed == 50`
- `batch_status.failed == 0`
- The process exited; PID `41544` is no longer alive
- The pair-family concentration metrics exclude blank/unknown/policy-exploration rows
- The final report parses full seed summaries and run metadata, not only `batch-progress.jsonl`

## Initial Read

This is a clean validation run, but it is not an obvious promotion-rate improvement over the prior clean batches:

- Original clean-50: `36/50` promoted (`72%`)
- Clean-100: `73/100` promoted (`73%`)
- Family-policy-v1 clean-50: `27/50` promoted (`54%`)

That does not necessarily mean family-policy-v1 is bad. It may mean the policy reduced template replay/concentration and exposed weaker curated/policy-exploration lanes. The Pro review should decide whether to:

- accept this run into outcome priors as a new feedback source,
- treat it as a policy-regression signal and adjust family caps/fill limits,
- or run a narrower diagnostic separating discovered/template-guided hands from curated and policy-exploration hands.
