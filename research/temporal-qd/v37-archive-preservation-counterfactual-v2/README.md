# V37 archive-preservation counterfactual v2 — native control recovery

Status: **exact native historical control reproduced**.

The V37 native fast-ephemeral finalizer was reproducible from its retained,
hash-bound executable and frozen G1–G5 `source.json` inputs. Two fresh,
isolated replays reproduced both the cumulative and parent archive bytes for
every generation, including the historical parent-member trajectory
`3 -> 3 -> 0 -> 0 -> 0`.

The V1 Python-preflight blocker remains preserved as a historical record. Its
conclusion is now scoped correctly: it identifies a Python legacy-opening
incompatibility, not missing native archive authority.

Run the native control only with the retained V37 finalizer bound by
`native-finalization-authority.json`:

```powershell
C:\repos\fuzzfolio-autoresearch\.venv\Scripts\python.exe -m `
  autoresearch.temporal_qd_v37_native_finalizer_replay `
  --v37-root C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-4000x1024x5-20260818-v37 `
  --finalizer C:\repos\fuzzfolio-autoresearch\rust\temporal-qd\target\release\temporal-qd-generation-finalizer.exe `
  --output-dir C:\repos\fuzzfolio-autoresearch\.tmp\artifacts\v37-native-finalizer-control-replay-v1\native-control-replays
```

The output root must not exist before the command. It is not restartable by
design; a rerun requires a new ignored output directory.

See [CONTROL-REPLAY-VERDICT.md](CONTROL-REPLAY-VERDICT.md),
[PYTHON-VS-RUST-BOUNDARY.md](PYTHON-VS-RUST-BOUNDARY.md), and
[EXECUTION-AND-VALIDATION.md](EXECUTION-AND-VALIDATION.md).
