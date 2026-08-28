# Execution and validation

Repository default/base branch: `origin/master` at
`51c2f9175f441166e7fc997109e939a9f9103b5d`.

This branch starts at the published V1 head
`9e27146858da398f4041d19c13551163ed2aa888` and uses the isolated worktree
branch `research/v37-archive-preservation-counterfactual-v2`.

The exact historical source worktree was detached at
`5fa623b88c641d4d886411bf195ee3ef386d6446`. Its release finalizer was built
under the ignored artifact root. The rebuilt bytes differ under the current
compiler environment, while the retained release executable exactly matches
the V37 runtime-authority SHA and was used for replay.

Checks run:

```powershell
cargo test --locked -p temporal-qd-tail-reducer
cargo test --locked -p temporal-qd-generation-finalizer
C:\repos\fuzzfolio-autoresearch\.venv\Scripts\python.exe -m pytest `
  tests\test_temporal_qd_v37_native_finalizer_replay.py `
  tests\test_temporal_qd_v37_archive_preservation_counterfactual.py -q
git diff --check
```

Both Rust suites passed when the repository venv supplied `python` for their
fixture scripts. The first tail-reducer invocation without that PATH binding
failed only because Windows resolved `python` to the Store alias; its log is
retained in the ignored audit packet. The targeted Python suite passed
`4 passed`.

All replay inputs, build products, native output trees, logs, negative controls,
and the audit ZIP reside under this repository's ignored `.tmp/artifacts/`.
