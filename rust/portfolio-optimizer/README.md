# Portfolio Optimizer Rust Kernel

This crate is a standalone Rust implementation of the computational portfolio
optimizer. It intentionally does not import or modify `autoresearch/`.

Boundary:

- Python remains responsible for artifact discovery, candidate filtering,
  dashboard/reporting, and export/materialization.
- Rust receives dense candidate inputs with aligned-able daily return curves,
  search constraints, objective weights, and account settings.
- The CLI prints optimized variants and a Pareto archive as JSON.

Run:

```powershell
cargo test --manifest-path rust/portfolio-optimizer/Cargo.toml
cargo run --manifest-path rust/portfolio-optimizer/Cargo.toml -- --input path\to\fixture.json
cargo build --manifest-path rust/portfolio-optimizer/Cargo.toml --features python-extension --lib
```

## Parity Status

The current target is computational parity with Python's `PortfolioSearch` over
the same dense candidate packet. It is not yet wired into `optimize-portfolio`
or the legacy `build-portfolio` materialization/export path.

Covered by `tools/portfolio_optimizer_rust_parity.py`:

- deterministic greedy/swap selection;
- zero-weight diversification compatibility;
- correlation penalty and marginal Sharpe diversification;
- account simulation, including lot-floor and current-balance risk basis;
- baseline candidate preservation outside the normal candidate limit;
- CPython-compatible seeded random starts;
- full variant metric/diversification/swap comparison;
- Pareto front comparison;
- real-corpus dense packets produced by Python candidate filtering.

Known boundary:

- Python still owns raw corpus discovery, candidate rejection/filtering,
  profile/report artifacts, dashboard job handling, and bundle export.
- Rust currently consumes already-normalized dense candidates. That is the
  intended backend boundary for a future `backend = python | rust | auto`
  integration.
- The PyO3 extension exposes the same dense JSON contract through
  `portfolio_optimizer_rs.optimize_json(...)`. It is an in-process call path,
  but still serializes the full packet and result as JSON.

Latest local checks on this branch:

```powershell
cargo test --manifest-path rust/portfolio-optimizer/Cargo.toml
uv run python tools\portfolio_optimizer_rust_parity.py
uv run python tools\portfolio_optimizer_rust_parity.py --pyo3
uv run python tools\portfolio_optimizer_rust_parity.py --release --real-corpus --candidate-limit 80 --portfolio-size 12 --random-starts 1 --max-swaps 2
uv run python tools\portfolio_optimizer_rust_parity.py --release --real-corpus --benchmark --skip-parity --pyo3 --candidate-limit 40 --portfolio-size 8 --random-starts 1 --max-swaps 2 --repeat 3
uv run pytest tests\test_portfolio_optimizer.py -q
```

Observed local performance:

- 40 real candidates, 8-strategy basket, 3 objectives, 1 random start,
  2 swaps: Python median `24.9373s`, Rust release CLI median `2.9346s`,
  `8.50x` speedup.
- 80 real candidates, 12-strategy basket, 3 objectives, 1 random start,
  2 swaps: Python `116.8211s`, Rust release CLI `13.7708s`,
  about `8.48x` faster on that parity run.
- After adding PyO3, 40 real candidates, 8-strategy basket, 3 objectives,
  1 random start, 2 swaps: Python median `31.1229s`, Rust release CLI median
  `2.8722s`, PyO3 median `3.0901s`. That was `10.84x` CLI speedup and
  `10.07x` PyO3 speedup in the local run. PyO3 is not materially faster than
  CLI yet because this first bridge still serializes JSON in and out.
