"""Benchmark legacy and optimized temporal-QD immigrant construction.

The harness deliberately uses a frozen pair authority and creates no market or
economic evidence.  For every requested target it runs the legacy oracle and
the optimized implementation with the same semantic run configuration, then
requires all semantic artifacts to be byte-identical.  Performance artifacts
are intentionally excluded from that comparison because they are observational
and include implementation-specific timing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any, Mapping

import psutil

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_factory import (
    PairAuthorityBundle,
    load_pair_run_config,
    pair_policy_from_config,
)
from autoresearch.temporal_qd_pair_generation import (
    PAIR_GENERATION_IMPLEMENTATION_LEGACY,
    PAIR_GENERATION_IMPLEMENTATION_OPTIMIZED,
    generate_pair_population,
)


BENCHMARK_SCHEMA = "temporal_qd_pair_generation_benchmark_v1"
_IMPLEMENTATIONS = (
    PAIR_GENERATION_IMPLEMENTATION_LEGACY,
    PAIR_GENERATION_IMPLEMENTATION_OPTIMIZED,
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read JSON object: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"JSON root must be an object: {path}")
    return value


def _fresh_external_root(output_root: Path) -> Path:
    root = output_root.resolve()
    repository = Path(__file__).resolve().parents[1]
    try:
        inside_repository = os.path.commonpath((str(root), str(repository))) == str(
            repository
        )
    except ValueError:
        inside_repository = False
    if inside_repository:
        raise TemporalDiscoveryContractError(
            "--output-root must be external to the autoresearch repository"
        )
    if root.exists() and any(root.iterdir()):
        raise TemporalDiscoveryContractError("--output-root must be absent or empty")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _parse_targets(value: str) -> tuple[int, ...]:
    try:
        targets = tuple(int(part.strip()) for part in str(value).split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--targets must be comma-separated positive integers"
        ) from exc
    if not targets or any(target < 1 for target in targets):
        raise argparse.ArgumentTypeError(
            "--targets must be comma-separated positive integers"
        )
    if len(set(targets)) != len(targets):
        raise argparse.ArgumentTypeError("--targets cannot contain duplicates")
    return targets


class _PeakMemoryMonitor:
    """External-to-generation RSS monitor with its own reported telemetry cost."""

    def __init__(self, *, interval_seconds: float = 0.25) -> None:
        self.interval_seconds = float(interval_seconds)
        self._root = psutil.Process(os.getpid())
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._sample_count = 0
        self._peak_tree_rss_bytes = 0
        self._peak_host_available_bytes = 0
        self._minimum_host_available_bytes: int | None = None
        self._thread_cpu_ns = 0
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        processes = [self._root, *self._root.children(recursive=True)]
        unique = {process.pid: process for process in processes}
        tree_rss = 0
        for process in unique.values():
            try:
                tree_rss += int(process.memory_info().rss)
            except (psutil.Error, OSError):
                continue
        host_available = int(psutil.virtual_memory().available)
        with self._lock:
            self._sample_count += 1
            self._peak_tree_rss_bytes = max(self._peak_tree_rss_bytes, tree_rss)
            self._peak_host_available_bytes = max(
                self._peak_host_available_bytes,
                host_available,
            )
            if (
                self._minimum_host_available_bytes is None
                or host_available < self._minimum_host_available_bytes
            ):
                self._minimum_host_available_bytes = host_available

    def _run(self) -> None:
        clock = getattr(time, "thread_time_ns", time.process_time_ns)
        started_cpu_ns = int(clock())
        try:
            self._sample()
            while not self._stop.wait(self.interval_seconds):
                self._sample()
            self._sample()
        finally:
            with self._lock:
                self._thread_cpu_ns += max(0, int(clock()) - started_cpu_ns)

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("peak monitor was already started")
        self._thread = threading.Thread(
            target=self._run,
            name="temporal-qd-benchmark-rss-monitor",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        if self._thread is None:
            raise RuntimeError("peak monitor was not started")
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds * 4.0))
        with self._lock:
            return {
                "monitorIntervalSeconds": self.interval_seconds,
                "sampleCount": self._sample_count,
                "peakTreeRssBytes": self._peak_tree_rss_bytes,
                "minimumHostAvailableBytes": self._minimum_host_available_bytes or 0,
                "monitorThreadCpuNs": self._thread_cpu_ns,
                "measurementScope": "benchmark_coordinator_and_recursive_children",
            }


def _semantic_manifest(root: Path) -> dict[str, Any]:
    paths = [
        root / "pair-config.json",
        root / "population.json",
        root / "generation-journal.json",
        *sorted((root / "proposal-journal").glob("*.json")),
    ]
    missing = [str(path) for path in paths[:3] if not path.is_file()]
    if missing:
        raise TemporalDiscoveryContractError(
            "generation did not produce required semantic artifacts: "
            + ", ".join(missing)
        )
    artifacts = {
        path.relative_to(root).as_posix(): {
            "sha256": _sha256_file(path),
            "bytes": path.stat().st_size,
        }
        for path in paths
    }
    return {
        "artifactCount": len(artifacts),
        "artifactBytes": sum(int(item["bytes"]) for item in artifacts.values()),
        "artifacts": artifacts,
    }


def _run_one(
    *,
    frozen: Mapping[str, Any],
    implementation: str,
    output_root: Path,
    target: int,
    max_proposal_attempts: int,
    run_label: str,
) -> dict[str, Any]:
    setup_started_perf_ns = time.perf_counter_ns()
    setup_started_cpu_ns = int(getattr(time, "thread_time_ns", time.process_time_ns)())
    with PairAuthorityBundle(frozen) as authority:
        setup_wall_ns = time.perf_counter_ns() - setup_started_perf_ns
        setup_cpu_ns = int(getattr(time, "thread_time_ns", time.process_time_ns)()) - setup_started_cpu_ns
        monitor = _PeakMemoryMonitor()
        monitor.start()
        generation_started_perf_ns = time.perf_counter_ns()
        generation_started_cpu_ns = int(
            getattr(time, "thread_time_ns", time.process_time_ns)()
        )
        try:
            result = generate_pair_population(
                output_root=output_root,
                generation_index=0,
                target_unique_candidates=target,
                run_config={
                    "schemaVersion": "temporal_qd_pair_generation_benchmark_run_v1",
                    "mode": "no_market_no_economic_evidence",
                    "pairRunConfigSha256": frozen["pairRunConfigSha256"],
                    "runLabel": run_label,
                    "targetUniqueCandidates": target,
                },
                pair_policy=pair_policy_from_config(frozen),
                pair_factory=authority.factory,
                module_authority=authority.operator,
                native_validator=authority.validator,
                pair_compiler=authority.compiler,
                operator_implementation_identity=frozen["operatorImplementation"],
                max_proposal_attempts=max_proposal_attempts,
                implementation=implementation,
            )
        finally:
            generation_wall_ns = time.perf_counter_ns() - generation_started_perf_ns
            generation_cpu_ns = int(
                getattr(time, "thread_time_ns", time.process_time_ns)()
            ) - generation_started_cpu_ns
            memory = monitor.stop()
    summary_path = output_root / "performance" / "latest-summary.json"
    summary = _read_object(summary_path) if summary_path.is_file() else None
    return {
        "implementation": implementation,
        "targetUniqueCandidates": target,
        "maxProposalAttempts": max_proposal_attempts,
        "authoritySetupWallNs": max(0, setup_wall_ns),
        "authoritySetupMainThreadCpuNs": max(0, setup_cpu_ns),
        "generationWallNs": max(0, generation_wall_ns),
        "generationMainThreadCpuNs": max(0, generation_cpu_ns),
        "harnessRssMonitor": memory,
        "performanceSummary": {
            "coordinatorCpuNs": (summary or {}).get("coordinatorCpuNs"),
            "resources": (summary or {}).get("resources"),
            "instrumentation": (summary or {}).get("instrumentation"),
        },
        "result": result,
        "semanticArtifacts": _semantic_manifest(output_root),
    }


def _compare_runs(
    legacy: Mapping[str, Any],
    optimized: Mapping[str, Any],
) -> dict[str, Any]:
    result_equal = legacy["result"] == optimized["result"]
    manifest_equal = legacy["semanticArtifacts"] == optimized["semanticArtifacts"]
    return {
        "resultExact": result_equal,
        "semanticArtifactsByteExact": manifest_equal,
        "legacyPopulationSha256": legacy["result"].get("populationSha256"),
        "optimizedPopulationSha256": optimized["result"].get("populationSha256"),
        "legacyJournalSha256": legacy["result"].get("journalSha256"),
        "optimizedJournalSha256": optimized["result"].get("journalSha256"),
    }


def _atomic_write(path: Path, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(encoded, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _run_isolated(
    *,
    pair_run_config: Path,
    implementation: str,
    output_root: Path,
    target: int,
    run_label: str,
    max_proposal_attempt_multiplier: int,
) -> dict[str, Any]:
    """Measure one implementation in a fresh interpreter/process tree."""

    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--pair-run-config",
        str(pair_run_config.resolve()),
        "--output-root",
        str(output_root.resolve()),
        "--targets",
        str(target),
        "--run-label",
        run_label,
        "--max-proposal-attempt-multiplier",
        str(max_proposal_attempt_multiplier),
        "--single-implementation",
        implementation,
    ]
    completed = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-4000:]
        raise TemporalDiscoveryContractError(
            f"isolated {implementation} benchmark failed: {detail}"
        )
    payload = _read_object(output_root / "single-run-report.json")
    run = payload.get("run")
    if not isinstance(run, dict):
        raise TemporalDiscoveryContractError(
            "isolated benchmark did not return an exact run object"
        )
    return run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair-run-config", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--targets", default="8,64", type=_parse_targets)
    parser.add_argument("--run-label", default="temporal-qd-generation-benchmark-v1")
    parser.add_argument("--max-proposal-attempt-multiplier", type=int, default=4)
    parser.add_argument(
        "--single-implementation",
        choices=_IMPLEMENTATIONS,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    if args.max_proposal_attempt_multiplier < 1:
        parser.error("--max-proposal-attempt-multiplier must be positive")

    root = _fresh_external_root(args.output_root)
    frozen = load_pair_run_config(_read_object(args.pair_run_config))
    if args.single_implementation is not None:
        if len(args.targets) != 1:
            parser.error("--single-implementation requires exactly one target")
        target = args.targets[0]
        run = _run_one(
            frozen=frozen,
            implementation=args.single_implementation,
            output_root=root,
            target=target,
            max_proposal_attempts=target * args.max_proposal_attempt_multiplier,
            run_label=str(args.run_label),
        )
        _atomic_write(
            root / "single-run-report.json",
            {
                "schemaVersion": "temporal_qd_pair_generation_single_benchmark_v1",
                "run": run,
            },
        )
        print(
            json.dumps(
                {
                    "implementation": args.single_implementation,
                    "targetUniqueCandidates": target,
                    "completed": run["result"].get("completed"),
                    "singleRunReport": str(root / "single-run-report.json"),
                },
                sort_keys=True,
            )
        )
        return
    report: dict[str, Any] = {
        "schemaVersion": BENCHMARK_SCHEMA,
        "mode": "no_market_no_economic_evidence",
        "pairRunConfigSha256": frozen["pairRunConfigSha256"],
        "targets": list(args.targets),
        "implementations": list(_IMPLEMENTATIONS),
        "telemetryDesign": {
            "mainThreadCpu": "time.thread_time_ns where available",
            "rssMonitor": "0.25 second coordinator-and-children RSS samples",
            "measurementIsolation": (
                "one fresh interpreter and process tree per implementation"
            ),
            "semanticComparison": "all semantic JSON artifact bytes; performance excluded",
        },
        "runs": [],
    }
    for target in args.targets:
        legacy = _run_isolated(
            pair_run_config=args.pair_run_config,
            implementation=PAIR_GENERATION_IMPLEMENTATION_LEGACY,
            output_root=root / f"target-{target}" / "legacy",
            target=target,
            run_label=str(args.run_label),
            max_proposal_attempt_multiplier=args.max_proposal_attempt_multiplier,
        )
        optimized = _run_isolated(
            pair_run_config=args.pair_run_config,
            implementation=PAIR_GENERATION_IMPLEMENTATION_OPTIMIZED,
            output_root=root / f"target-{target}" / "optimized",
            target=target,
            run_label=str(args.run_label),
            max_proposal_attempt_multiplier=args.max_proposal_attempt_multiplier,
        )
        comparison = _compare_runs(legacy, optimized)
        report["runs"].append(
            {
                "targetUniqueCandidates": target,
                "legacy": legacy,
                "optimized": optimized,
                "equivalence": comparison,
            }
        )
    report["allSemanticArtifactsByteExact"] = all(
        item["equivalence"]["semanticArtifactsByteExact"] for item in report["runs"]
    )
    report["allResultsExact"] = all(
        item["equivalence"]["resultExact"] for item in report["runs"]
    )
    report["benchmarkSha256"] = "sha256:" + hashlib.sha256(
        _canonical_json(report).encode("utf-8")
    ).hexdigest()
    report_path = root / "benchmark-report.json"
    _atomic_write(report_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["allSemanticArtifactsByteExact"] or not report["allResultsExact"]:
        raise TemporalDiscoveryContractError(
            "optimized benchmark artifacts diverged from the legacy oracle; "
            f"see {report_path}"
        )


if __name__ == "__main__":
    main()
