from __future__ import annotations

import argparse
import importlib.util
import json
import math
import shutil
import statistics
import subprocess
import sys
import sysconfig
import tempfile
import time
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
RUST_MANIFEST = REPO_ROOT / "rust" / "portfolio-optimizer" / "Cargo.toml"
RUST_TARGET = REPO_ROOT / "rust" / "portfolio-optimizer" / "target"
sys.path.insert(0, str(REPO_ROOT))

from autoresearch import portfolio_optimizer as portfolio_optimizer_module  # noqa: E402
from autoresearch.portfolio_optimizer import (  # noqa: E402
    PortfolioOptimizerSpec,
    PortfolioSearch,
    build_optimizer_candidates,
)


TOLERANCE = 1e-8


def write_attempt_artifacts(
    tmp_path: Path,
    name: str,
    daily_equity: list[float],
    *,
    avg_hold: float = 12.0,
    stop_loss_percent: float = 1.0,
) -> tuple[Path, Path]:
    result_path = tmp_path / f"{name}-result.json"
    curve_path = tmp_path / f"{name}-calendar.json"
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "best_cell_path_metrics": {
                            "avg_holding_hours": avg_hold,
                            "p90_holding_hours": avg_hold * 2,
                            "max_holding_hours": avg_hold * 3,
                            "path_quality": 0.8,
                        },
                        "best_cell": {"stop_loss_percent": stop_loss_percent},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    points = []
    start = date(2025, 10, 1)
    for index, equity in enumerate(daily_equity):
        points.append(
            {
                "date": (start + timedelta(days=index)).isoformat(),
                "equity_r": equity,
                "open_trade_count": 1 if index + 1 < len(daily_equity) else 0,
                "closed_trade_count": 1,
            }
        )
    curve_path.write_text(json.dumps({"curve": {"points": points}}), encoding="utf-8")
    return result_path, curve_path


def row(
    tmp_path: Path,
    attempt_id: str,
    instrument: str,
    daily_equity: list[float],
    *,
    score: float = 70.0,
    avg_hold: float = 12.0,
) -> dict[str, Any]:
    result_path, curve_path = write_attempt_artifacts(
        tmp_path,
        attempt_id,
        daily_equity,
        avg_hold=avg_hold,
    )
    return {
        "attempt_id": attempt_id,
        "run_id": f"run-{attempt_id}",
        "candidate_name": attempt_id,
        "score_36m": score,
        "full_backtest_validation_status_36m": "valid",
        "full_backtest_result_path_36m": str(result_path),
        "full_backtest_calendar_curve_path_36m": str(curve_path),
        "instruments_36m": [instrument],
        "trade_count_36m": len(daily_equity),
        "trades_per_month_36m": 10,
        "selected_stop_loss_percent_36m": 1.0,
    }


def equity_from_returns(returns: list[float]) -> list[float]:
    equity: list[float] = []
    total = 0.0
    for value in returns:
        total += value
        equity.append(total)
    return equity


def generated_returns(candidate_index: int, days: int) -> list[float]:
    base = 0.08 + (candidate_index % 7) * 0.012
    amplitude = 0.04 + (candidate_index % 5) * 0.01
    drag_every = 9 + (candidate_index % 6)
    values: list[float] = []
    for day in range(days):
        seasonal = math.sin((candidate_index + 3) * (day + 2) / 13.0) * amplitude
        drift = base + seasonal
        if (day + candidate_index) % drag_every == 0:
            drift -= 0.18 + (candidate_index % 4) * 0.03
        if (day * (candidate_index + 5)) % 41 == 0:
            drift += 0.22
        values.append(round(drift, 6))
    return values


def candidate_payload(candidate: Any) -> dict[str, Any]:
    return {
        "attempt_id": candidate.attempt_id,
        "candidate_name": candidate.row.get("candidate_name"),
        "run_id": candidate.row.get("run_id"),
        "created_at": candidate.created_at,
        "instruments": candidate.instruments,
        "family": candidate.family,
        "score": candidate.score,
        "avg_hold_hours": candidate.avg_hold_hours,
        "p90_hold_hours": candidate.p90_hold_hours,
        "max_hold_hours": candidate.max_hold_hours,
        "path_quality": candidate.path_quality,
        "stop_loss_percent": candidate.stop_loss_percent,
        "trade_count": candidate.trade_count,
        "trades_per_month": candidate.trades_per_month,
        "dates": candidate.dates,
        "daily_r": candidate.daily_r,
        "open_counts": candidate.open_counts,
        "closed_counts": candidate.closed_counts,
    }


def rust_input_payload(
    candidates: list[Any],
    spec: PortfolioOptimizerSpec,
    *,
    objectives: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    return {
        "spec": asdict(spec),
        "candidates": [candidate_payload(candidate) for candidate in candidates],
        "objectives": objectives or {},
    }


def build_candidates(
    rows: list[dict[str, Any]],
    spec: PortfolioOptimizerSpec,
    *,
    objectives: dict[str, dict[str, float]] | None = None,
) -> list[Any]:
    previous = portfolio_optimizer_module.DEFAULT_OBJECTIVES
    if objectives is not None:
        portfolio_optimizer_module.DEFAULT_OBJECTIVES = objectives
    try:
        candidates, rejections = build_optimizer_candidates(rows, spec)
    finally:
        portfolio_optimizer_module.DEFAULT_OBJECTIVES = previous
    if rejections:
        raise AssertionError(f"unexpected Python fixture rejections: {rejections}")
    return candidates


def python_dense_optimize(
    candidates: list[Any],
    spec: PortfolioOptimizerSpec,
    *,
    objectives: dict[str, dict[str, float]] | None = None,
) -> tuple[dict[str, Any], float]:
    previous = portfolio_optimizer_module.DEFAULT_OBJECTIVES
    if objectives is not None:
        portfolio_optimizer_module.DEFAULT_OBJECTIVES = objectives
    try:
        start = time.perf_counter()
        search = PortfolioSearch(candidates, spec)
        variants = search.optimize()
        pareto_front = search.pareto_front(limit=50)
        elapsed = time.perf_counter() - start
    finally:
        portfolio_optimizer_module.DEFAULT_OBJECTIVES = previous
    return {"variants": variants, "pareto_front": pareto_front}, elapsed


def rust_binary_path(*, release: bool) -> Path:
    profile = "release" if release else "debug"
    suffix = ".exe" if sys.platform.startswith("win") else ""
    return RUST_TARGET / profile / f"portfolio-optimizer-rs{suffix}"


def rust_cdylib_path(*, release: bool) -> Path:
    profile = "release" if release else "debug"
    if sys.platform.startswith("win"):
        return RUST_TARGET / profile / "portfolio_optimizer_rs.dll"
    if sys.platform == "darwin":
        return RUST_TARGET / profile / "libportfolio_optimizer_rs.dylib"
    return RUST_TARGET / profile / "libportfolio_optimizer_rs.so"


def ensure_rust_binary(*, release: bool) -> Path:
    command = ["cargo", "build", "--quiet", "--manifest-path", str(RUST_MANIFEST)]
    if release:
        command.insert(2, "--release")
    subprocess.run(command, cwd=REPO_ROOT, check=True)
    return rust_binary_path(release=release)


def ensure_pyo3_extension(*, release: bool) -> Path:
    command = [
        "cargo",
        "build",
        "--quiet",
        "--manifest-path",
        str(RUST_MANIFEST),
        "--features",
        "python-extension",
        "--lib",
    ]
    if release:
        command.insert(2, "--release")
    subprocess.run(command, cwd=REPO_ROOT, check=True)
    return rust_cdylib_path(release=release)


def load_pyo3_module(*, release: bool):
    cdylib_path = ensure_pyo3_extension(release=release)
    extension_suffix = sysconfig.get_config_var("EXT_SUFFIX") or ".pyd"
    extension_root = REPO_ROOT / ".tmp" / "portfolio_optimizer_pyo3"
    extension_root.mkdir(parents=True, exist_ok=True)
    module_dir = Path(tempfile.mkdtemp(prefix="load-", dir=extension_root))
    module_path = module_dir / f"portfolio_optimizer_rs{extension_suffix}"
    shutil.copy2(cdylib_path, module_path)
    spec = importlib.util.spec_from_file_location("portfolio_optimizer_rs", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to create import spec for {module_path}")
    module = importlib.util.module_from_spec(spec)
    previous = sys.modules.pop("portfolio_optimizer_rs", None)
    try:
        sys.modules["portfolio_optimizer_rs"] = module
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop("portfolio_optimizer_rs", None)
        if previous is not None:
            sys.modules["portfolio_optimizer_rs"] = previous
        raise
    module._portfolio_optimizer_rs_module_path = str(module_path)
    module._portfolio_optimizer_rs_previous = previous
    return module


def rust_dense_optimize(
    payload: dict[str, Any],
    *,
    release: bool = False,
    binary: Path | None = None,
) -> tuple[dict[str, Any], float]:
    if binary is None:
        binary = ensure_rust_binary(release=release)
    with tempfile.TemporaryDirectory() as tmp:
        input_path = Path(tmp) / "optimizer-input.json"
        input_path.write_text(json.dumps(payload), encoding="utf-8")
        start = time.perf_counter()
        completed = subprocess.run(
            [str(binary), "--input", str(input_path)],
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        elapsed = time.perf_counter() - start
    return json.loads(completed.stdout), elapsed


def pyo3_dense_optimize(payload: dict[str, Any], module: Any) -> tuple[dict[str, Any], float]:
    input_json = json.dumps(payload)
    start = time.perf_counter()
    output_json = module.optimize_json(input_json)
    elapsed = time.perf_counter() - start
    return json.loads(output_json), elapsed


def assert_json_close(label: str, left: Any, right: Any, *, tolerance: float = TOLERANCE) -> None:
    if isinstance(left, bool) or isinstance(right, bool):
        if left is not right:
            raise AssertionError(f"{label}: {left!r} != {right!r}")
        return
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        left_float = float(left)
        right_float = float(right)
        if math.isnan(left_float) or math.isnan(right_float):
            if math.isnan(left_float) and math.isnan(right_float):
                return
        if abs(left_float - right_float) > tolerance:
            raise AssertionError(f"{label}: {left!r} != {right!r}")
        return
    if left is None or right is None:
        if left is not None or right is not None:
            raise AssertionError(f"{label}: {left!r} != {right!r}")
        return
    if isinstance(left, dict) and isinstance(right, dict):
        left_keys = set(left)
        right_keys = set(right)
        if left_keys != right_keys:
            raise AssertionError(
                f"{label}: key mismatch left-only={sorted(left_keys - right_keys)} "
                f"right-only={sorted(right_keys - left_keys)}"
            )
        for key in sorted(left_keys):
            assert_json_close(f"{label}.{key}", left[key], right[key], tolerance=tolerance)
        return
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            raise AssertionError(f"{label}: length {len(left)} != {len(right)}")
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            assert_json_close(f"{label}[{index}]", left_item, right_item, tolerance=tolerance)
        return
    if left != right:
        raise AssertionError(f"{label}: {left!r} != {right!r}")


def compare_variant(case: str, objective: str, python_variant: dict[str, Any], rust_variant: dict[str, Any]) -> None:
    for key in ("objective_name", "start", "selected_attempt_ids"):
        assert_json_close(f"{case}.{objective}.{key}", python_variant[key], rust_variant[key])
    assert_json_close(
        f"{case}.{objective}.objective_score",
        python_variant["objective_score"],
        rust_variant["objective_score"],
    )
    assert_json_close(f"{case}.{objective}.swaps", python_variant["swaps"], rust_variant["swaps"])
    assert_json_close(
        f"{case}.{objective}.diversification",
        python_variant["diversification"],
        rust_variant["diversification"],
    )
    assert_json_close(f"{case}.{objective}.metrics", python_variant["metrics"], rust_variant["metrics"])


def compare_outputs(case: str, python_output: dict[str, Any], rust_output: dict[str, Any]) -> None:
    python_variants = python_output["variants"]
    rust_variants = rust_output["variants"]
    if set(python_variants) != set(rust_variants):
        raise AssertionError(
            f"{case}: variant keys differ {sorted(python_variants)} != {sorted(rust_variants)}"
        )
    for objective in sorted(python_variants):
        compare_variant(case, objective, python_variants[objective], rust_variants[objective])
    assert_json_close(f"{case}.pareto_front", python_output["pareto_front"], rust_output["pareto_front"])


def run_case(
    case: str,
    rows: list[dict[str, Any]],
    spec: PortfolioOptimizerSpec,
    *,
    objectives: dict[str, dict[str, float]] | None = None,
    release: bool = False,
    pyo3_module: Any | None = None,
) -> tuple[float, float, float | None, int]:
    candidates = build_candidates(rows, spec, objectives=objectives)
    python_output, python_elapsed = python_dense_optimize(candidates, spec, objectives=objectives)
    payload = rust_input_payload(candidates, spec, objectives=objectives)
    rust_output, rust_elapsed = rust_dense_optimize(payload, release=release)
    compare_outputs(case, python_output, rust_output)
    pyo3_elapsed: float | None = None
    if pyo3_module is not None:
        pyo3_output, pyo3_elapsed = pyo3_dense_optimize(payload, pyo3_module)
        compare_outputs(f"{case}.pyo3", python_output, pyo3_output)
    pyo3_text = f", pyo3={pyo3_elapsed:.4f}s" if pyo3_elapsed is not None else ""
    print(
        f"{case} parity ok "
        f"(candidates={len(candidates)}, python={python_elapsed:.4f}s, "
        f"rust_cli={rust_elapsed:.4f}s{pyo3_text})"
    )
    return python_elapsed, rust_elapsed, pyo3_elapsed, len(candidates)


def run_selection_case(
    tmp_path: Path,
    *,
    release: bool,
    pyo3_module: Any | None,
) -> tuple[float, float, float | None, int]:
    rows = [
        row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        row(tmp_path, "lumpy", "USDJPY", [8, 1, 9, 2], score=95),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("stability",),
        random_starts=0,
        max_swaps=4,
        max_per_family=1,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
    )
    return run_case("selection", rows, spec, release=release, pyo3_module=pyo3_module)


def run_zero_diversification_case(
    tmp_path: Path,
    *,
    release: bool,
    pyo3_module: Any | None,
) -> list[tuple[float, float, float | None, int]]:
    rows = [
        row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        row(tmp_path, "lumpy", "USDJPY", [8, 1, 9, 2], score=95),
    ]
    base_kwargs = dict(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("stability",),
        random_starts=0,
        max_swaps=4,
        max_per_family=1,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
    )
    specs = [
        PortfolioOptimizerSpec(**base_kwargs),
        PortfolioOptimizerSpec(
            **base_kwargs,
            correlation_penalty_weight=0.0,
            diversification_mode="penalty",
            portfolio_sharpe_weight=0.0,
        ),
        PortfolioOptimizerSpec(
            **base_kwargs,
            correlation_penalty_weight=0.0,
            diversification_mode="marginal_sharpe",
            portfolio_sharpe_weight=0.0,
        ),
    ]
    return [
        run_case(f"zero-diversification-{index}", rows, spec, release=release)
        if pyo3_module is None
        else run_case(
            f"zero-diversification-{index}",
            rows,
            spec,
            release=release,
            pyo3_module=pyo3_module,
        )
        for index, spec in enumerate(specs)
    ]


def run_penalty_case(
    tmp_path: Path,
    *,
    release: bool,
    pyo3_module: Any | None,
) -> tuple[float, float, float | None, int]:
    objectives = {"return": {"final_r": 1.0, "maxdd_r": -2.0}}
    clone_returns = [2.0, -0.5, 2.0, -0.5, 2.0, -0.5]
    uncorr_returns = [-0.2, 0.6, -0.2, 0.6, -0.2, 0.6]
    rows = [
        row(tmp_path, "clone-a", "EURUSD", equity_from_returns(clone_returns), score=90),
        row(tmp_path, "clone-b", "GBPUSD", equity_from_returns(clone_returns), score=89),
        row(tmp_path, "uncorr-c", "USDJPY", equity_from_returns(uncorr_returns), score=60),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("return",),
        random_starts=0,
        max_swaps=0,
        min_fx_share=0,
        correlation_penalty_weight=10.0,
        diversification_mode="penalty",
    )
    return run_case(
        "penalty-diversification",
        rows,
        spec,
        objectives=objectives,
        release=release,
        pyo3_module=pyo3_module,
    )


def run_sharpe_case(
    tmp_path: Path,
    *,
    release: bool,
    pyo3_module: Any | None,
) -> tuple[float, float, float | None, int]:
    objectives = {"return": {"final_r": 1.0, "maxdd_r": -2.0}}
    seed_returns = [2.0, -0.5, 2.0, -0.5, 2.0, -0.5]
    correlated_returns = [1.5, -0.3, 1.5, -0.3, 1.5, -0.3]
    anti_returns = [-0.2, 0.6, -0.2, 0.6, -0.2, 0.6]
    rows = [
        row(tmp_path, "seed", "EURUSD", equity_from_returns(seed_returns), score=90),
        row(tmp_path, "corr-high-r", "GBPUSD", equity_from_returns(correlated_returns), score=80),
        row(tmp_path, "anti-low-r", "USDJPY", equity_from_returns(anti_returns), score=60),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("return",),
        random_starts=0,
        max_swaps=0,
        min_fx_share=0,
        diversification_mode="marginal_sharpe",
        portfolio_sharpe_weight=5.0,
    )
    return run_case(
        "marginal-sharpe",
        rows,
        spec,
        objectives=objectives,
        release=release,
        pyo3_module=pyo3_module,
    )


def run_account_case(
    tmp_path: Path,
    *,
    release: bool,
    pyo3_module: Any | None,
) -> list[tuple[float, float, float | None, int]]:
    objectives = {"return": {"final_r": 1.0}}
    rows = [row(tmp_path, "tiny-risk", "EURUSD", [1, 0.5, 1.5], score=70)]
    lot_floor_spec = PortfolioOptimizerSpec(
        portfolio_size=1,
        objective_names=("return",),
        random_starts=0,
        max_swaps=0,
        min_fx_share=0,
        account={
            "account_size_usd": 100.0,
            "risk_per_trade_pct": 0.1,
            "min_lot": 0.01,
            "lot_step": 0.01,
            "notional_usd_per_lot": 100000.0,
            "leverage": 500.0,
            "stop_out_level_pct": 50.0,
            "margin_call_level_pct": 100.0,
        },
    )
    timings = [
        run_case(
            "account-lot-floor",
            rows,
            lot_floor_spec,
            objectives=objectives,
            release=release,
            pyo3_module=pyo3_module,
        )
    ]
    rows = [row(tmp_path, "compound", "EURUSD", [1, 2], score=70)]
    compound_spec = PortfolioOptimizerSpec(
        portfolio_size=1,
        objective_names=("return",),
        random_starts=0,
        max_swaps=0,
        min_fx_share=0,
        account={
            "account_size_usd": 1000.0,
            "risk_per_trade_pct": 1.0,
            "min_lot": 0.0,
            "lot_step": 0.0001,
            "notional_usd_per_lot": 100000.0,
            "leverage": 500.0,
            "stop_out_level_pct": 50.0,
        },
    )
    timings.append(
        run_case(
            "account-current-basis",
            rows,
            compound_spec,
            objectives=objectives,
            release=release,
            pyo3_module=pyo3_module,
        )
    )
    return timings


def run_baseline_case(
    tmp_path: Path,
    *,
    release: bool,
    pyo3_module: Any | None,
) -> tuple[float, float, float | None, int]:
    rows = []
    instruments = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "XAUUSD", "US500"]
    for index in range(10):
        returns = generated_returns(index, 45)
        rows.append(
            row(
                tmp_path,
                f"candidate-{index:02}",
                instruments[index % len(instruments)],
                equity_from_returns(returns),
                score=55 + index,
            )
        )
    spec = PortfolioOptimizerSpec(
        portfolio_size=4,
        candidate_limit=5,
        swap_candidate_limit=3,
        objective_names=("balanced",),
        baseline_attempt_ids=("candidate-00", "candidate-01", "missing"),
        random_starts=0,
        max_swaps=3,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=2,
    )
    return run_case(
        "baseline-preservation",
        rows,
        spec,
        release=release,
        pyo3_module=pyo3_module,
    )


def run_random_start_case(
    tmp_path: Path,
    *,
    release: bool,
    pyo3_module: Any | None,
) -> tuple[float, float, float | None, int]:
    rows = []
    instruments = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD", "XAUUSD", "US500", "SP500"]
    for index in range(32):
        returns = generated_returns(index, 90)
        rows.append(
            row(
                tmp_path,
                f"random-candidate-{index:02}",
                instruments[index % len(instruments)],
                equity_from_returns(returns),
                score=45 + ((index * 13) % 40),
                avg_hold=8 + (index % 6) * 3,
            )
        )
    spec = PortfolioOptimizerSpec(
        portfolio_size=7,
        candidate_limit=30,
        swap_candidate_limit=18,
        objective_names=("return", "balanced", "stability"),
        random_starts=4,
        random_seed=6181,
        max_swaps=5,
        max_per_family=2,
        min_fx_share=0,
        max_metal_share=3,
        max_index_share=3,
        max_instrument_share=3,
        correlation_penalty_weight=2.5,
        diversification_mode="marginal_sharpe",
        portfolio_sharpe_weight=1.5,
    )
    return run_case(
        "random-starts-broad",
        rows,
        spec,
        release=release,
        pyo3_module=pyo3_module,
    )


def run_synthetic_parity(
    *,
    release: bool,
    pyo3_module: Any | None,
) -> list[tuple[float, float, float | None, int]]:
    timings: list[tuple[float, float, float | None, int]] = []
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        timings.append(run_selection_case(tmp_path, release=release, pyo3_module=pyo3_module))
        timings.extend(
            run_zero_diversification_case(
                tmp_path,
                release=release,
                pyo3_module=pyo3_module,
            )
        )
        timings.append(run_penalty_case(tmp_path, release=release, pyo3_module=pyo3_module))
        timings.append(run_sharpe_case(tmp_path, release=release, pyo3_module=pyo3_module))
        timings.extend(run_account_case(tmp_path, release=release, pyo3_module=pyo3_module))
        timings.append(run_baseline_case(tmp_path, release=release, pyo3_module=pyo3_module))
        timings.append(run_random_start_case(tmp_path, release=release, pyo3_module=pyo3_module))
    return timings


def real_corpus_candidates(
    *,
    candidate_limit: int,
    portfolio_size: int,
    random_starts: int,
    max_swaps: int,
) -> tuple[list[Any], PortfolioOptimizerSpec]:
    from autoresearch.__main__ import (  # noqa: PLC0415
        _selection_corpus_rows,
        filter_play_hand_candidate_scope,
        load_config,
    )

    config = load_config()
    full_rows, _info = _selection_corpus_rows(
        config,
        run_ids=None,
        label="portfolio-rust-parity",
        as_json=True,
        materialize_full_corpus=False,
    )
    rows, _scope = filter_play_hand_candidate_scope(full_rows, "promoted")
    spec = PortfolioOptimizerSpec(
        portfolio_size=portfolio_size,
        candidate_limit=candidate_limit,
        swap_candidate_limit=max(1, min(candidate_limit, 80)),
        objective_names=("return", "balanced", "stability"),
        random_starts=random_starts,
        random_seed=17,
        max_swaps=max_swaps,
    )
    candidates, rejections = build_optimizer_candidates(rows, spec)
    print(
        f"real corpus packet: rows={len(rows)}, candidates={len(candidates)}, "
        f"rejections={json.dumps(rejections, sort_keys=True)}"
    )
    if len(candidates) < portfolio_size:
        raise RuntimeError(
            f"real corpus has {len(candidates)} candidates, below portfolio_size={portfolio_size}"
        )
    return candidates, spec


def run_real_corpus_parity(
    *,
    candidate_limit: int,
    portfolio_size: int,
    random_starts: int,
    max_swaps: int,
    release: bool,
    pyo3_module: Any | None,
) -> tuple[float, float, float | None, int]:
    candidates, spec = real_corpus_candidates(
        candidate_limit=candidate_limit,
        portfolio_size=portfolio_size,
        random_starts=random_starts,
        max_swaps=max_swaps,
    )
    python_output, python_elapsed = python_dense_optimize(candidates, spec)
    payload = rust_input_payload(candidates, spec)
    rust_output, rust_elapsed = rust_dense_optimize(payload, release=release)
    compare_outputs("real-corpus", python_output, rust_output)
    pyo3_elapsed: float | None = None
    if pyo3_module is not None:
        pyo3_output, pyo3_elapsed = pyo3_dense_optimize(payload, pyo3_module)
        compare_outputs("real-corpus.pyo3", python_output, pyo3_output)
    pyo3_text = f", pyo3={pyo3_elapsed:.4f}s" if pyo3_elapsed is not None else ""
    print(
        f"real-corpus parity ok "
        f"(candidates={len(candidates)}, python={python_elapsed:.4f}s, "
        f"rust_cli={rust_elapsed:.4f}s{pyo3_text})"
    )
    return python_elapsed, rust_elapsed, pyo3_elapsed, len(candidates)


def benchmark_dense(
    *,
    candidates: list[Any],
    spec: PortfolioOptimizerSpec,
    repeat: int,
    include_pyo3: bool,
) -> None:
    binary = ensure_rust_binary(release=True)
    pyo3_module = load_pyo3_module(release=True) if include_pyo3 else None
    payload = rust_input_payload(candidates, spec)
    with tempfile.TemporaryDirectory() as tmp:
        input_path = Path(tmp) / "optimizer-input.json"
        input_path.write_text(json.dumps(payload), encoding="utf-8")
        python_times = []
        rust_times = []
        pyo3_times = []
        reference_python, _ = python_dense_optimize(candidates, spec)
        for _ in range(repeat):
            _, elapsed = python_dense_optimize(candidates, spec)
            python_times.append(elapsed)
        for _ in range(repeat):
            start = time.perf_counter()
            completed = subprocess.run(
                [str(binary), "--input", str(input_path)],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            rust_elapsed = time.perf_counter() - start
            rust_output = json.loads(completed.stdout)
            compare_outputs("benchmark", reference_python, rust_output)
            rust_times.append(rust_elapsed)
        if pyo3_module is not None:
            for _ in range(repeat):
                pyo3_output, pyo3_elapsed = pyo3_dense_optimize(payload, pyo3_module)
                compare_outputs("benchmark.pyo3", reference_python, pyo3_output)
                pyo3_times.append(pyo3_elapsed)
    python_median = statistics.median(python_times)
    rust_median = statistics.median(rust_times)
    pyo3_median = statistics.median(pyo3_times) if pyo3_times else None
    speedup = python_median / rust_median if rust_median > 0 else float("inf")
    print(
        "benchmark dense packet: "
        f"candidates={len(candidates)}, portfolio_size={spec.portfolio_size}, "
        f"objectives={','.join(spec.objective_names)}, random_starts={spec.random_starts}, "
        f"max_swaps={spec.max_swaps}"
    )
    line = (
        f"python median={python_median:.4f}s over {repeat} runs; "
        f"rust release CLI median={rust_median:.4f}s over {repeat} runs; "
        f"CLI speedup={speedup:.2f}x"
    )
    if pyo3_median is not None:
        pyo3_speedup = python_median / pyo3_median if pyo3_median > 0 else float("inf")
        line += f"; PyO3 median={pyo3_median:.4f}s; PyO3 speedup={pyo3_speedup:.2f}x"
    print(line)


def synthetic_benchmark_packet(tmp_path: Path, *, candidate_limit: int, portfolio_size: int) -> tuple[list[Any], PortfolioOptimizerSpec]:
    rows = []
    instruments = [
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "AUDUSD",
        "NZDUSD",
        "USDCAD",
        "XAUUSD",
        "XAGUSD",
        "US500",
        "SP500",
    ]
    for index in range(candidate_limit):
        rows.append(
            row(
                tmp_path,
                f"bench-candidate-{index:03}",
                instruments[index % len(instruments)],
                equity_from_returns(generated_returns(index, 240)),
                score=45 + ((index * 17) % 55),
                avg_hold=6 + (index % 10) * 4,
            )
        )
    spec = PortfolioOptimizerSpec(
        portfolio_size=portfolio_size,
        candidate_limit=candidate_limit,
        swap_candidate_limit=min(candidate_limit, 80),
        objective_names=("return", "balanced", "stability"),
        random_starts=3,
        random_seed=17,
        max_swaps=6,
        max_per_family=2,
        min_fx_share=0,
        max_metal_share=8,
        max_index_share=8,
        max_instrument_share=5,
        correlation_penalty_weight=2.0,
        diversification_mode="marginal_sharpe",
        portfolio_sharpe_weight=1.0,
    )
    return build_candidates(rows, spec), spec


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", action="store_true", help="Use a release Rust binary for parity timing.")
    parser.add_argument("--real-corpus", action="store_true", help="Also compare against the local materialized corpus.")
    parser.add_argument("--benchmark", action="store_true", help="Run an apples-to-apples dense packet benchmark.")
    parser.add_argument("--pyo3", action="store_true", help="Also build/import the PyO3 extension and compare it.")
    parser.add_argument("--skip-parity", action="store_true", help="Skip parity cases and run only the requested benchmark.")
    parser.add_argument("--candidate-limit", type=int, default=80)
    parser.add_argument("--portfolio-size", type=int, default=12)
    parser.add_argument("--random-starts", type=int, default=2)
    parser.add_argument("--max-swaps", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=5)
    args = parser.parse_args()

    if args.release or args.benchmark:
        ensure_rust_binary(release=True)
    pyo3_module = load_pyo3_module(release=bool(args.release)) if args.pyo3 else None

    timings: list[tuple[float, float, float | None, int]] = []
    if not args.skip_parity:
        timings = run_synthetic_parity(
            release=bool(args.release),
            pyo3_module=pyo3_module,
        )
        if args.real_corpus:
            timings.append(
                run_real_corpus_parity(
                    candidate_limit=int(args.candidate_limit),
                    portfolio_size=int(args.portfolio_size),
                    random_starts=int(args.random_starts),
                    max_swaps=int(args.max_swaps),
                    release=bool(args.release),
                    pyo3_module=pyo3_module,
                )
            )
    if timings:
        python_total = sum(item[0] for item in timings)
        rust_total = sum(item[1] for item in timings)
        pyo3_times = [item[2] for item in timings if item[2] is not None]
        pyo3_text = f", pyo3={sum(pyo3_times):.4f}s" if pyo3_times else ""
        print(
            f"parity timing subtotal: python={python_total:.4f}s, "
            f"rust_cli={rust_total:.4f}s{pyo3_text}, cases={len(timings)}"
        )

    if args.benchmark:
        if args.real_corpus:
            candidates, spec = real_corpus_candidates(
                candidate_limit=int(args.candidate_limit),
                portfolio_size=int(args.portfolio_size),
                random_starts=int(args.random_starts),
                max_swaps=int(args.max_swaps),
            )
        else:
            with tempfile.TemporaryDirectory() as tmp:
                candidates, spec = synthetic_benchmark_packet(
                    Path(tmp),
                    candidate_limit=int(args.candidate_limit),
                    portfolio_size=int(args.portfolio_size),
                )
                benchmark_dense(
                    candidates=candidates,
                    spec=spec,
                    repeat=max(1, int(args.repeat)),
                    include_pyo3=bool(args.pyo3),
                )
                return 0
        benchmark_dense(
            candidates=candidates,
            spec=spec,
            repeat=max(1, int(args.repeat)),
            include_pyo3=bool(args.pyo3),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
