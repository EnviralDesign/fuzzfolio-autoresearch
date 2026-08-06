from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

from scripts import benchmark_temporal_qd_optimization as benchmark


def _write_runner(path: Path) -> None:
    path.write_text(
        """
import json
import os
from pathlib import Path
import sys

context = json.loads(Path(os.environ["TEMPORAL_QD_BENCHMARK_CONTEXT_FILE"]).read_text(encoding="utf-8"))
fixture = context["fixture"]
if fixture.get("failImplementation") == context["implementation"]:
    sys.stderr.write("intentional benchmark fixture failure")
    raise SystemExit(23)
artifact_root = Path(os.environ["TEMPORAL_QD_BENCHMARK_ARTIFACT_ROOT"])
payload = (context["implementation"] + ":" + context["phase"] + ":" + str(context["shape"])).encode("utf-8")
payload += b"x" * int(fixture.get("extraArtifactBytesByImplementation", {}).get(context["implementation"], 0))
(artifact_root / "fixture-artifact.bin").write_bytes(payload)
result = {
    "implementation": context["implementation"],
    "phase": context["phase"],
    "runnerPid": os.getpid(),
    "inputRoot": context["inputRoot"],
    "telemetryCpuNs": int(fixture.get("telemetryCpuNs", 17)),
}
Path(os.environ["TEMPORAL_QD_BENCHMARK_RESULT_FILE"]).write_text(
    json.dumps(result, sort_keys=True), encoding="utf-8"
)
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _plan(
    runner: Path,
    *,
    implementations: tuple[str, ...] = ("python_legacy", "python_oracle"),
    fail_implementation: str | None = None,
    extra_artifact_bytes_by_implementation: dict[str, int] | None = None,
    input_clone: dict | None = None,
    thresholds: dict[str, dict[str, float]] | None = None,
) -> dict:
    command = [sys.executable, str(runner)]
    fixture = {
        "fixtureSchema": "tiny-deterministic-temporal-qd-fixture-v1",
        "candidateCount": 64,
        "telemetryCpuNs": 17,
        **(
            {"failImplementation": fail_implementation}
            if fail_implementation is not None
            else {}
        ),
        **(
            {"extraArtifactBytesByImplementation": extra_artifact_bytes_by_implementation}
            if extra_artifact_bytes_by_implementation is not None
            else {}
        ),
        **({"inputClone": input_clone} if input_clone is not None else {}),
    }
    return {
        "schemaVersion": benchmark.PLAN_SCHEMA,
        "fixtures": {
            "64": {
                "proposal_construction": dict(fixture),
                "consolidation": dict(fixture),
            }
        },
        "implementations": {
            implementation: {
                "proposal_construction": {"command": command},
                "consolidation": {"command": command},
            }
            for implementation in implementations
        },
        **(
            {"nonRegressionThresholds": thresholds}
            if thresholds is not None
            else {}
        ),
    }


def _write_plan(path: Path, plan: dict) -> None:
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _measurement_run(
    implementation: str,
    *,
    wall: int,
    cpu: int,
    tree_cpu: int,
    rss: int,
    headroom: int,
    read_bytes: int,
    written_bytes: int,
    artifact_bytes: int,
) -> dict:
    return {
        "implementation": implementation,
        "measurement": {
            "endToEndWallNs": wall,
            "mainThreadCpuNs": cpu,
            "processTreeCpuNs": tree_cpu,
            "peakRecursiveRssBytes": rss,
            "minimumHostAvailableBytes": headroom,
            "processTreeReadBytes": read_bytes,
            "processTreeWrittenBytes": written_bytes,
        },
        "artifactSizes": {"totalBytes": artifact_bytes},
    }


def test_benchmark_schema_and_fresh_processes_with_selectable_python_oracle(
    tmp_path: Path,
) -> None:
    runner = tmp_path / "runner.py"
    _write_runner(runner)
    frozen_input = tmp_path / "frozen-input.json"
    frozen_input.write_text('{"frozen":true}\n', encoding="utf-8")
    plan_path = tmp_path / "plan.json"
    _write_plan(
        plan_path,
        _plan(
            runner,
            input_clone={
                "sourcePath": str(frozen_input),
                "sourceKind": "file",
                "sourceSha256": benchmark._sha256_file(frozen_input),
            },
        ),
    )

    report = benchmark.run_benchmark(
        plan_path=plan_path,
        output_root=tmp_path / "benchmark-output",
        shapes=(64,),
        old_implementation="python_legacy",
        new_implementation="python_oracle",
        monitor_interval_seconds=0.01,
    )

    assert benchmark._parse_shapes("64,128,1024") == (64, 128, 1024)
    assert report["schemaVersion"] == benchmark.REPORT_SCHEMA
    assert report["status"] == "completed"
    assert report["measurementContext"] == "concurrent_load_provisional"
    assert report["selectedShapes"] == [64]
    assert report["repetitionCount"] == 2
    assert [run["phase"] for run in report["runs"]] == list(benchmark.PHASES)
    assert report["benchmarkReportSha256"].startswith("sha256:")

    worker_pids: list[int] = []
    for item in report["runs"]:
        assert item["repetitionCount"] == 2
        assert [
            repetition["executionOrder"] for repetition in item["repetitions"]
        ] == [
            ["python_legacy", "python_oracle"],
            ["python_oracle", "python_legacy"],
        ]
        assert item["aggregate"]["old"]["aggregation"]["sampleCount"] == 2
        assert item["aggregate"]["new"]["aggregation"]["sampleCount"] == 2
        input_roots: set[str] = set()
        for repetition in item["repetitions"]:
            for run in (repetition["old"], repetition["new"]):
                assert run["schemaVersion"] == benchmark.WORKER_SCHEMA
                assert run["isolation"]["mode"] == "fresh_python_worker_process_tree"
                assert run["isolation"]["workerProcessId"] != os.getpid()
                worker_pids.append(run["isolation"]["workerProcessId"])
                measurement = run["measurement"]
                assert measurement["endToEndWallNs"] >= 0
                assert measurement["mainThreadCpuNs"] >= 0
                assert measurement["peakRecursiveRssBytes"] > 0
                assert measurement["minimumHostAvailableBytes"] > 0
                assert "processTreeReadBytes" in measurement
                assert "processTreeWrittenBytes" in measurement
                assert measurement["runnerTelemetryCpuNs"] == 17
                assert run["artifactSizes"]["fileCount"] == 1
                assert run["artifactSizes"]["totalBytes"] > 0
                assert run["inputClone"]["mode"] == "verified_source_copy"
                assert run["inputClone"]["sourceSha256"] == benchmark._sha256_file(
                    frozen_input
                )
                input_roots.add(run["runner"]["result"]["inputRoot"])
        assert len(input_roots) == 4
    assert len(set(worker_pids)) == len(worker_pids)

    report_path = tmp_path / "benchmark-output" / "benchmark-report.json"
    assert json.loads(report_path.read_text(encoding="utf-8")) == report


def test_compare_runs_calculates_directional_deltas() -> None:
    old = _measurement_run(
        "python_legacy",
        wall=100,
        cpu=80,
        tree_cpu=90,
        rss=400,
        headroom=1_000,
        read_bytes=50,
        written_bytes=60,
        artifact_bytes=70,
    )
    new = _measurement_run(
        "candidate",
        wall=80,
        cpu=60,
        tree_cpu=70,
        rss=300,
        headroom=1_100,
        read_bytes=40,
        written_bytes=30,
        artifact_bytes=20,
    )

    comparison = benchmark.compare_runs(old_run=old, new_run=new)

    wall = comparison["deltas"]["endToEndWallNs"]
    assert wall == {
        "old": 100,
        "new": 80,
        "delta": -20,
        "ratio": 0.8,
        "regressionRatio": -0.2,
        "comparable": True,
    }
    headroom = comparison["deltas"]["minimumHostAvailableBytes"]
    assert headroom["delta"] == 100
    assert headroom["regressionRatio"] == -0.1
    assert comparison["nonRegression"] == {
        "enabled": False,
        "passed": None,
        "checks": {},
    }


def test_non_regression_thresholds_reject_speed_and_memory_regressions() -> None:
    old = _measurement_run(
        "python_legacy",
        wall=100,
        cpu=100,
        tree_cpu=100,
        rss=100,
        headroom=100,
        read_bytes=100,
        written_bytes=100,
        artifact_bytes=100,
    )
    new = _measurement_run(
        "candidate",
        wall=110,
        cpu=100,
        tree_cpu=100,
        rss=101,
        headroom=99,
        read_bytes=100,
        written_bytes=100,
        artifact_bytes=100,
    )

    comparison = benchmark.compare_runs(
        old_run=old,
        new_run=new,
        thresholds={
            "maxWallTimeRegressionRatio": 0.05,
            "maxPeakRecursiveRssRegressionRatio": 0.0,
            "maxHostHeadroomRegressionRatio": 0.0,
        },
    )

    gate = comparison["nonRegression"]
    assert gate["enabled"] is True
    assert gate["passed"] is False
    assert gate["checks"]["maxWallTimeRegressionRatio"]["actualRegressionRatio"] == 0.1
    assert gate["checks"]["maxPeakRecursiveRssRegressionRatio"]["passed"] is False
    assert gate["checks"]["maxHostHeadroomRegressionRatio"]["passed"] is False


def test_fresh_worker_failure_propagates_and_writes_machine_readable_failure(
    tmp_path: Path,
) -> None:
    runner = tmp_path / "runner.py"
    _write_runner(runner)
    plan_path = tmp_path / "plan.json"
    _write_plan(
        plan_path,
        _plan(runner, fail_implementation="python_oracle"),
    )
    output_root = tmp_path / "benchmark-output"

    with pytest.raises(benchmark.BenchmarkExecutionError, match="intentional benchmark fixture failure"):
        benchmark.run_benchmark(
            plan_path=plan_path,
            output_root=output_root,
            shapes=(64,),
            old_implementation="python_legacy",
            new_implementation="python_oracle",
            monitor_interval_seconds=0.01,
        )

    failure = json.loads(
        (output_root / "benchmark-failure.json").read_text(encoding="utf-8")
    )
    assert failure["schemaVersion"] == benchmark.FAILURE_SCHEMA
    assert failure["status"] == "failed"
    assert failure["errorType"] == "BenchmarkExecutionError"
    assert failure["completedRunCount"] == 0


def test_enforced_threshold_failure_preserves_completed_report(tmp_path: Path) -> None:
    runner = tmp_path / "runner.py"
    _write_runner(runner)
    plan_path = tmp_path / "plan.json"
    _write_plan(
        plan_path,
        _plan(
            runner,
            extra_artifact_bytes_by_implementation={"python_oracle": 1},
            thresholds={
                "default": {"maxArtifactBytesRegressionRatio": 0.0},
            },
        ),
    )
    output_root = tmp_path / "benchmark-output"

    # The candidate writes one additional artifact byte, so a no-regression
    # artifact gate fails deterministically while preserving the full report.
    with pytest.raises(benchmark.BenchmarkThresholdError):
        benchmark.run_benchmark(
            plan_path=plan_path,
            output_root=output_root,
            shapes=(64,),
            old_implementation="python_legacy",
            new_implementation="python_oracle",
            monitor_interval_seconds=0.01,
            enforce_non_regression=True,
        )

    report = json.loads(
        (output_root / "benchmark-report.json").read_text(encoding="utf-8")
    )
    assert report["status"] == "completed"
    assert report["nonRegression"]["enforced"] is True
    assert report["nonRegression"]["configured"] is True
    assert report["nonRegression"]["thresholdsPassed"] is False
    assert report["nonRegression"]["counterbalance"]["fullyCounterbalanced"] is True
    assert report["nonRegression"]["passed"] is False
    assert report["nonRegression"]["observationalMetricsExcludedFromHardGates"] == list(
        benchmark.OBSERVATIONAL_METRICS
    )


def test_hard_gates_require_counterbalanced_repetitions_and_reject_observational_io(
    tmp_path: Path,
) -> None:
    runner = tmp_path / "runner.py"
    _write_runner(runner)
    plan_path = tmp_path / "plan.json"
    _write_plan(
        plan_path,
        _plan(
            runner,
            thresholds={"default": {"maxArtifactBytesRegressionRatio": 1.0}},
        ),
    )
    output_root = tmp_path / "benchmark-output"

    with pytest.raises(benchmark.BenchmarkThresholdError):
        benchmark.run_benchmark(
            plan_path=plan_path,
            output_root=output_root,
            shapes=(64,),
            old_implementation="python_legacy",
            new_implementation="python_oracle",
            monitor_interval_seconds=0.01,
            repetitions=1,
            enforce_non_regression=True,
        )

    report = json.loads(
        (output_root / "benchmark-report.json").read_text(encoding="utf-8")
    )
    assert report["nonRegression"]["thresholdsPassed"] is True
    assert report["nonRegression"]["counterbalance"]["fullyCounterbalanced"] is False
    assert report["nonRegression"]["passed"] is False

    rejected = _plan(
        runner,
        thresholds={"default": {"maxBytesReadRegressionRatio": 0.0}},
    )
    with pytest.raises(benchmark.BenchmarkContractError, match="unknown threshold"):
        benchmark.validate_benchmark_plan(rejected)
