# Exact Template Branch Notes

This folder captures the first smoke artifact after adding final comparison between the normal mutated Play Hand branch and the exact retained-template branch.

Smoke command:

```powershell
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --dry-run --json
```

Observed behavior:

- The deal came from `play_hand_seed_plan`.
- The selected retained pair template was `drs-0008-r003-mfi-trend-obv-mean-reversion-m15`.
- Play Hand applied template defaults and registered `exact_template`.
- Final branch metadata included both `mutated_final_36mo` and `exact_template_36mo`.
- The exact branch used the validation basket: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`.

Because this was a dry run, both scores were null and the normal mutated branch won the tie by design. Unit tests cover the score-based branch selector.
