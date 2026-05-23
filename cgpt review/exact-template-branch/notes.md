# Exact Template Branch Notes

This folder captures smoke artifacts after adding final comparison between the normal mutated Play Hand branch and the exact retained-template branch.

## Dry Smoke

```powershell
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --dry-run --json
```

Observed behavior:

- The deal came from `play_hand_seed_plan`.
- The selected retained pair template was `drs-0008-r003-mfi-trend-obv-mean-reversion-m15`.
- Play Hand applied template defaults and registered `exact_template`.
- Final branch metadata included both `mutated_final_36mo` and `exact_template_36mo`.
- The exact branch used the validation basket: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`.

After the source-profile correction, this dry run also confirms:

- `exact_template_source = template_profile_path`
- `exact_template.json` contains only `MFI_TREND` and `OBV_MEAN_REVERSION`
- the expanded scaffold still contains four dealt indicators

Because this was a dry run, both scores were null and the normal mutated branch won the tie by design. Unit tests cover the score-based branch selector.

## Real Low-Budget Smoke

```powershell
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
```

Run:

```text
runs/20260523T023608430426Z-playhand-v1
```

Result:

- `exact_template_source = template_profile_path`
- exact template profile had 2 indicators: `MFI_TREND`, `OBV_MEAN_REVERSION`
- mutated final profile had 4 indicators: `BBANDS_POSITION_TREND`, `MFI_TREND`, `OBV_MEAN_REVERSION`, `MA_SPREAD_MEAN_REVERSION`
- `mutated_final_36mo` attempt: `20260523T023608430426Z-playhand-v1-attempt-00009`, score `71.1089`
- `exact_template_36mo` attempt: `20260523T023608430426Z-playhand-v1-attempt-00010`, score `62.6208`
- selected branch: `mutated`
- canonical selection reason: `mutated_branch_selected`

Interpretation: the exact retained template survived 36-month scrutiny, and the Play Hand expanded/mutated branch improved it in this smoke.
