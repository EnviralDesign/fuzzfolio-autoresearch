from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_evolution import _empty_identity_ledger
from autoresearch.temporal_qd_native import (
    PAIR_GENERATION_RUNTIME_PYTHON,
    PAIR_GENERATION_RUNTIME_RUST,
)
from scripts import temporal_qd_front_half_oracle as oracle
from scripts import temporal_qd_native_admission as admission


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "temporal_qd_runtime_oracle"


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _authority(tmp_path: Path) -> dict[str, Any]:
    manifest = json.loads((FIXTURE_ROOT / "runtime-manifest.json").read_text(encoding="utf-8"))
    archive_path = tmp_path / "initial-archive.json"
    _write(archive_path, manifest["parentArchive"])
    value = {
        "schemaVersion": admission.AUTHORITY_SCHEMA,
        "source": {"kind": "test_fixture"},
        "pairRunConfig": manifest["pairRunConfig"],
        "parentArchivePath": str(archive_path),
        "parentArchiveSha256": manifest["parentArchiveSha256"],
        "parentArchiveMode": admission._ADMISSION_MODE_EMPTY_G0,
        "generationIndex": 1,
        "g0BootstrapEnabled": True,
        "eligibleParentCount": 0,
        "parentSchedule": None,
        "parentScheduleMode": "supervisor_initial_binding",
        "evidenceIdentityContext": manifest["evidenceIdentityContext"],
        "baseParameters": {
            "version": "temporal_qd_evolution_v3",
            "seed": 17,
            "targetUniqueCandidates": 1,
            "immigrantProposalFraction": 0.2,
            "mutationDepthProbabilities": {"1": 0.7, "2": 0.25, "3": 0.05},
            "maxCumulativeStructuralDepth": 16,
            "maxProposalAttempts": 8,
            "minimumTotalTrades": 8,
            "minimumTradesPerWindow": 4,
            "capTrades": 20,
            "cellCapacity": 4,
        },
        "qdPublicationAuthority": {
            "qdVersion": "temporal_qd_evolution_v3",
            "policyName": "stage5e7_v3_robust_quality_archive",
            "policySha256": manifest["parentArchive"]["policySha256"],
            "frozenPolicy": manifest["parentArchive"]["frozenPolicy"],
        },
        "generationFunnelEnabled": False,
        "marketEvidenceRead": False,
        "lakeContacted": False,
        "gatewayContacted": False,
        "economicWorkPerformed": False,
    }
    value["authoritySha256"] = canonical_sha256(value)
    return value


def _write_completed_root(root: Path) -> dict[str, Any]:
    candidate = {
        "candidateId": "qd_fixture",
        "candidateIdentitySha256": "sha256:" + "1" * 64,
        "programSha256": "sha256:" + "2" * 64,
        "sourceProfileSha256": "sha256:" + "3" * 64,
        "profileSnapshotSha256": "sha256:" + "4" * 64,
        "pairProposalSha256": "sha256:" + "5" * 64,
    }
    proposal = {
        "schemaVersion": "fixture_pair_proposal_v1",
        "proposalOrdinal": 0,
        "proposalSha256": "sha256:" + "5" * 64,
    }
    entry = {
        "schemaVersion": "fixture_pair_entry_v1",
        "proposalOrdinal": 0,
        "disposition": "accepted",
        "proposal": proposal,
        "candidate": candidate,
    }
    entry["entrySha256"] = canonical_sha256(entry)
    ledger = _empty_identity_ledger()
    ledger.pop("ledgerSha256")
    ledger["records"] = [
        {
            "candidateIdentitySha256": candidate["candidateIdentitySha256"],
            "programSha256": candidate["programSha256"],
            "sourceProfileSha256": candidate["sourceProfileSha256"],
            "profileSnapshotSha256": candidate["profileSnapshotSha256"],
        }
    ]
    ledger["ledgerSha256"] = canonical_sha256(ledger)
    population = {
        "schemaVersion": "temporal_qd_generation_population_v3",
        "candidateCount": 1,
        "candidates": [candidate],
    }
    population["populationSha256"] = canonical_sha256(population)
    evaluation = {
        "schemaVersion": "temporal_qd_evaluation_population_v1",
        "candidateCount": 1,
        "candidates": [candidate],
    }
    evaluation["evaluationPopulationSha256"] = canonical_sha256(evaluation)
    journal = {
        "schemaVersion": "temporal_qd_generation_journal_v3",
        "proposalCount": 1,
        "candidateCount": 1,
    }
    journal["journalSha256"] = canonical_sha256(journal)
    _write(root / "pair-config.json", {"configSha256": "sha256:" + "a" * 64})
    _write(root / "proposal-journal" / "00000000.json", entry)
    _write(root / "population.json", population)
    _write(root / "evaluation-population.json", evaluation)
    _write(root / "generation-journal.json", journal)
    _write(root / "identity-ledger.json", ledger)
    _write(root / "g0-bootstrap" / "accepted-pool.json", {"acceptedPoolSha256": "sha256:" + "6" * 64})
    _write(root / "g0-bootstrap" / "campaign-construction-ledger.json", {"ledgerSha256": "sha256:" + "7" * 64})
    _write(root / "g0-bootstrap" / "selection.json", {"selectionSha256": "sha256:" + "8" * 64})
    return {
        "completed": True,
        "configSha256": "sha256:" + "a" * 64,
        "generationIndex": 1,
        "proposalCount": 1,
        "candidateCount": 1,
        "targetUniqueCandidates": 1,
        "populationSha256": population["populationSha256"],
        "evaluationPopulationSha256": evaluation["evaluationPopulationSha256"],
        "journalSha256": journal["journalSha256"],
    }


@pytest.mark.skipif(
    os.name != "nt",
    reason="the committed runtime oracle freezes Windows Dashboard authority paths",
)
def test_default_shape_one_uses_injected_native_runner_and_writes_self_hashed_report(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, Path, int | None]] = []

    def fake_runner(**kwargs: Any) -> dict[str, Any]:
        engine = str(kwargs["engine"])
        root = Path(kwargs["output_root"])
        maximum = kwargs["max_new_proposals"]
        calls.append((engine, root, maximum))
        if maximum == 0:
            _write(root / "checkpoint.json", {"completed": False})
            result = {"completed": False, "proposalCount": 0, "candidateCount": 0}
        else:
            result = _write_completed_root(root)
        return {
            "resultIdentity": result,
            "telemetry": {
                "wallSeconds": 0.001,
                "processCpuSeconds": 0.001,
                "peakRssBytes": 1,
                "readBytes": 0,
                "writeBytes": 1,
                "artifactBytes": admission._artifact_bytes(root),
                "sampleCount": 1,
                "measurementScope": "injected_test_runner",
            },
        }

    report = admission.run_admission(
        authority=_authority(tmp_path),
        output_root=tmp_path / "out",
        invocation_runner=fake_runner,
    )

    assert [engine for engine, _, _ in calls] == [
        PAIR_GENERATION_RUNTIME_PYTHON,
        PAIR_GENERATION_RUNTIME_PYTHON,
        PAIR_GENERATION_RUNTIME_PYTHON,
        PAIR_GENERATION_RUNTIME_RUST,
        PAIR_GENERATION_RUNTIME_RUST,
        PAIR_GENERATION_RUNTIME_RUST,
    ]
    assert len({root for _, root, _ in calls}) == 4
    assert [maximum for _, _, maximum in calls] == [None, 0, None, None, 0, None]
    assert report["shapes"] == [1]
    assert report["maximumLocalConcurrency"] == 1
    assert report["allPublicSemanticTreesAndBytesExact"] is True
    assert report["allProposalCandidateLedgerAndG0IdentitiesExact"] is True
    supplied = report["reportSha256"]
    material = dict(report)
    material.pop("reportSha256")
    assert supplied == canonical_sha256(material)
    persisted = json.loads((tmp_path / "out" / "admission-report.json").read_text(encoding="utf-8"))
    assert persisted == report


def test_bounded_oracle_exact_path_never_materializes_artifact_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write(left / "population.json", {"payload": "x" * 100_000})
    _write(right / "population.json", {"payload": "x" * 100_000})

    def forbidden_read_bytes(_path: Path) -> bytes:
        pytest.fail("bounded exact comparison must stream file bytes")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    result = oracle.compare_roots_bounded_exact(left, right, shape=1024)

    assert result["comparisonMode"] == "streaming_public_byte_exact_fail_closed"
    assert result["semanticExact"] is True
    assert result["byteExact"] is True


def test_process_tree_telemetry_attributes_peak_rss_by_stable_execution_role(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeProcess:
        def __init__(
            self,
            pid: int,
            *,
            executable: str,
            command: list[str],
            rss: int,
        ) -> None:
            self.pid = pid
            self._executable = executable
            self._command = command
            self.rss = rss
            self._children: list[FakeProcess] = []

        def children(self, *, recursive: bool) -> list[FakeProcess]:
            assert recursive is True
            return list(self._children)

        def memory_info(self) -> SimpleNamespace:
            return SimpleNamespace(rss=self.rss)

        def cpu_times(self) -> SimpleNamespace:
            return SimpleNamespace(user=1.0, system=0.5)

        def io_counters(self) -> SimpleNamespace:
            return SimpleNamespace(read_bytes=10, write_bytes=20)

        def cmdline(self) -> list[str]:
            return list(self._command)

        def exe(self) -> str:
            return self._executable

        def name(self) -> str:
            return Path(self._executable).name

    validator = tmp_path / "dashboard-validator.py"
    root = FakeProcess(101, executable="C:\\Python\\python.exe", command=["python"], rss=100)
    batch = FakeProcess(
        202,
        executable="C:\\native\\temporal-qd-batch.exe",
        command=["C:\\native\\temporal-qd-batch.exe"],
        rss=200,
    )
    dashboard = FakeProcess(
        303,
        executable="C:\\Python\\python.exe",
        command=["C:\\Python\\python.exe", str(validator)],
        rss=300,
    )
    other = FakeProcess(404, executable="C:\\helper\\helper.exe", command=["helper"], rss=400)
    root._children = [batch, dashboard, other]
    monkeypatch.setattr(admission.psutil, "Process", lambda pid: root)

    telemetry = admission._ProcessTreeTelemetry(
        root.pid, dashboard_validator_script=str(validator)
    )
    telemetry.sample()
    root.rss, batch.rss, dashboard.rss, other.rss = 125, 250, 275, 500
    telemetry.sample()

    assert telemetry.peak_rss == 1_150
    report = telemetry.role_rss_report()
    assert report["processRolePeakRssBytes"] == {
        "admissionPythonWorker": 125,
        "nativeQdBatch": 250,
        "dashboardJsonlAuthority": 300,
        "otherDescendants": 500,
    }
    assert report["executableRolePeakRssBytes"] == {
        "admissionPythonWorker": {"python.exe": 125},
        "nativeQdBatch": {"temporal-qd-batch.exe": 250},
        "dashboardJsonlAuthority": {"python.exe": 300},
        "otherDescendants": {"allOtherDescendants": 500},
    }
    assert report["rssAttributionScope"] == "best_effort_sampled_recursive_process_tree"


@pytest.mark.parametrize("value", ["2", "1,1", "1,8,2"])
def test_shapes_parser_rejects_non_admission_or_duplicate_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        admission._parse_shapes(value)


def test_parent_archive_authority_derives_next_generation_and_keeps_empty_g0_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The supervisor remains authority; the supplied archive selects parent mode."""

    manifest = json.loads((FIXTURE_ROOT / "runtime-manifest.json").read_text(encoding="utf-8"))
    pair_policy = {"schemaVersion": "test_pair_policy", "enabled": True}
    initial_archive = dict(manifest["parentArchive"])
    initial_archive["bidirectionalPairPolicy"] = pair_policy
    parent_archive = {
        "generationIndex": 4,
        "bidirectionalPairPolicy": pair_policy,
        "cells": [{"members": [{"candidate": {}}]}],
    }
    initial_path = tmp_path / "initial.json"
    parent_path = tmp_path / "parent.json"
    _write(initial_path, {"fixture": "initial"})
    _write(parent_path, {"fixture": "parent"})
    config = {
        "qdVersion": initial_archive["qdVersion"],
        "policyName": initial_archive["policyName"],
        "policySha256": initial_archive["policySha256"],
        "frozenPolicy": initial_archive["frozenPolicy"],
        "bidirectionalPairGeneration": {"fixture": "pair"},
        "initialArchive": {"path": str(initial_path), "archiveSha256": "sha256:initial"},
        "evaluation": {"predeclaredEvidenceContext": manifest["evidenceIdentityContext"]},
        "frozenSearchPolicy": _authority(tmp_path)["baseParameters"],
    }
    config["configSha256"] = canonical_sha256(config)
    config_path = tmp_path / "config.json"
    _write(config_path, config)

    import autoresearch.temporal_qd_supervisor as supervisor

    monkeypatch.setattr(supervisor, "_validate_frozen_sources", lambda _config: [])
    monkeypatch.setattr(admission, "load_pair_run_config", lambda value: dict(value))
    monkeypatch.setattr(admission, "pair_policy_from_config", lambda _config: pair_policy)
    monkeypatch.setattr(
        admission,
        "_load_archive",
        lambda path: (
            (initial_archive, "sha256:initial")
            if Path(path).resolve() == initial_path.resolve()
            else (parent_archive, "sha256:parent")
        ),
    )
    monkeypatch.setattr(
        admission,
        "_reproduction_cells",
        lambda _archive, *, allow_empty_quality_bootstrap: [{"members": [{"candidate": {}}]}],
    )
    monkeypatch.setattr(admission, "_rotating_parent_schedule", lambda _archive: None)

    empty = admission.load_admission_authority(
        supervisor_config_path=config_path,
        pair_config_path=None,
        initial_archive_path=None,
    )
    assert empty["parentArchiveMode"] == admission._ADMISSION_MODE_EMPTY_G0
    assert empty["generationIndex"] == 1
    assert empty["g0BootstrapEnabled"] is True

    parent = admission.load_admission_authority(
        supervisor_config_path=config_path,
        pair_config_path=None,
        initial_archive_path=None,
        parent_archive_override_path=parent_path,
    )
    assert parent["parentArchiveMode"] == admission._ADMISSION_MODE_PARENT_ARCHIVE
    assert parent["generationIndex"] == 5
    assert parent["g0BootstrapEnabled"] is False
    assert parent["eligibleParentCount"] == 1
    assert parent["parentSchedule"] is None
    assert parent["parentScheduleMode"] == "production_legacy"


@pytest.mark.parametrize(
    ("archive", "error"),
    [
        (
            {"generationIndex": 1, "bidirectionalPairPolicy": {"wrong": True}, "cells": []},
            "bound to another pair policy",
        ),
        (
            {
                "generationIndex": 0,
                "bidirectionalPairPolicy": {"expected": True},
                "cells": [],
                "candidateCountSeen": 0,
                "occupiedCellCount": 0,
                "memberCount": 0,
                "qualityMemberCount": 0,
                "observationalMemberCount": 0,
                "negativeNoveltyMemberCount": 0,
            },
            "completed nonempty generation",
        ),
    ],
)
def test_parent_archive_authority_rejects_wrong_policy_or_empty_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    archive: dict[str, Any],
    error: str,
) -> None:
    """An override cannot turn the G0 mode into an unverified parent run."""

    manifest = json.loads((FIXTURE_ROOT / "runtime-manifest.json").read_text(encoding="utf-8"))
    expected_policy = {"expected": True}
    initial = dict(manifest["parentArchive"])
    initial["bidirectionalPairPolicy"] = expected_policy
    initial_path = tmp_path / "initial.json"
    parent_path = tmp_path / "parent.json"
    _write(initial_path, {})
    _write(parent_path, {})
    config = {
        "qdVersion": initial["qdVersion"],
        "policyName": initial["policyName"],
        "policySha256": initial["policySha256"],
        "frozenPolicy": initial["frozenPolicy"],
        "bidirectionalPairGeneration": {"fixture": "pair"},
        "initialArchive": {"path": str(initial_path), "archiveSha256": "sha256:initial"},
        "evaluation": {"predeclaredEvidenceContext": manifest["evidenceIdentityContext"]},
        "frozenSearchPolicy": _authority(tmp_path)["baseParameters"],
    }
    config["configSha256"] = canonical_sha256(config)
    config_path = tmp_path / "config.json"
    _write(config_path, config)
    import autoresearch.temporal_qd_supervisor as supervisor

    monkeypatch.setattr(supervisor, "_validate_frozen_sources", lambda _config: [])
    monkeypatch.setattr(admission, "load_pair_run_config", lambda value: dict(value))
    monkeypatch.setattr(admission, "pair_policy_from_config", lambda _config: expected_policy)
    monkeypatch.setattr(
        admission,
        "_load_archive",
        lambda path: (
            (initial, "sha256:initial")
            if Path(path).resolve() == initial_path.resolve()
            else (archive, "sha256:parent")
        ),
    )
    monkeypatch.setattr(admission, "_reproduction_cells", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(admission, "_rotating_parent_schedule", lambda _archive: None)

    with pytest.raises(admission.TemporalDiscoveryContractError, match=error):
        admission.load_admission_authority(
            supervisor_config_path=config_path,
            pair_config_path=None,
            initial_archive_path=None,
            parent_archive_override_path=parent_path,
        )


def test_parent_worker_derives_generation_and_disables_g0_widths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority = _authority(tmp_path)
    authority.update(
        {
            "parentArchiveMode": admission._ADMISSION_MODE_PARENT_ARCHIVE,
            "generationIndex": 7,
            "g0BootstrapEnabled": False,
            "eligibleParentCount": 2,
            "parentScheduleMode": "production_legacy",
        }
    )
    seen: dict[str, Any] = {}
    monkeypatch.setattr(admission, "_validate_authority", lambda value: dict(value))
    monkeypatch.setattr(
        admission,
        "generate_qd_generation",
        lambda **kwargs: seen.update(kwargs) or {"completed": True, "generationIndex": 7},
    )
    result = admission._run_generation_worker(
        {
            "authority": authority,
            "engine": PAIR_GENERATION_RUNTIME_RUST,
            "shape": 8,
            "outputRoot": str(tmp_path / "out"),
            "timeoutSeconds": 60,
            "maxNewProposals": None,
        }
    )
    assert result["resultIdentity"] == {"completed": True, "generationIndex": 7}
    assert seen["generation_index"] == 7
    assert seen["allow_empty_quality_bootstrap"] is False
    assert seen["initial_construction_pool_size"] is None
    assert seen["evaluation_population_size"] is None


def test_empty_worker_retains_generation_one_g0_widths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority = _authority(tmp_path)
    seen: dict[str, Any] = {}
    monkeypatch.setattr(admission, "_validate_authority", lambda value: dict(value))
    monkeypatch.setattr(
        admission,
        "generate_qd_generation",
        lambda **kwargs: seen.update(kwargs) or {"completed": True, "generationIndex": 1},
    )
    admission._run_generation_worker(
        {
            "authority": authority,
            "engine": PAIR_GENERATION_RUNTIME_RUST,
            "shape": 8,
            "outputRoot": str(tmp_path / "out"),
            "timeoutSeconds": 60,
            "maxNewProposals": None,
        }
    )
    assert seen["generation_index"] == 1
    assert seen["allow_empty_quality_bootstrap"] is True
    assert seen["initial_construction_pool_size"] == 8
    assert seen["evaluation_population_size"] == 8


def test_parent_admission_requires_and_reports_structural_offspring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority = _authority(tmp_path)
    authority.update(
        {
            "parentArchiveMode": admission._ADMISSION_MODE_PARENT_ARCHIVE,
            "generationIndex": 2,
            "g0BootstrapEnabled": False,
            "eligibleParentCount": 1,
            "parentScheduleMode": "production_legacy",
        }
    )
    monkeypatch.setattr(admission, "_validate_authority", lambda value: dict(value))

    def fake_runner(**kwargs: Any) -> dict[str, Any]:
        root = Path(kwargs["output_root"])
        maximum = kwargs["max_new_proposals"]
        if maximum == 0:
            _write(root / "checkpoint.json", {"completed": False})
            result = {"completed": False, "proposalCount": 0, "candidateCount": 0}
        else:
            result = _write_completed_root(root)
            entry_path = root / "proposal-journal" / "00000000.json"
            entry = json.loads(entry_path.read_text(encoding="utf-8"))
            entry["originKind"] = "structural_offspring"
            entry["proposal"]["mutationSteps"] = [{"operation": "fixture"}]
            entry["proposal"]["crossoverAudit"] = {"materialized": True}
            entry.pop("entrySha256")
            entry["entrySha256"] = canonical_sha256(entry)
            _write(entry_path, entry)
        return {
            "resultIdentity": result,
            "telemetry": {
                "wallSeconds": 0.001,
                "processCpuSeconds": 0.001,
                "peakRssBytes": 1,
                "readBytes": 0,
                "writeBytes": 1,
                "artifactBytes": admission._artifact_bytes(root),
                "sampleCount": 1,
                "measurementScope": "injected_test_runner",
            },
        }

    report = admission.run_admission(
        authority=authority,
        output_root=tmp_path / "out",
        invocation_runner=fake_runner,
    )
    assert report["parentArchiveMode"] == admission._ADMISSION_MODE_PARENT_ARCHIVE
    assert report["generationIndex"] == 2
    for engine in ("python", "rust"):
        assert report["runs"][0][engine]["parentOriginReport"] == {
            "originProposalCounts": {"structural_offspring": 1},
            "structuralOffspringProposalCount": 1,
            "acceptedStructuralOffspringCount": 1,
            "acceptedMutationOffspringCount": 1,
            "acceptedCrossoverOffspringCount": 1,
        }


@pytest.mark.parametrize(
    ("mutation_steps", "crossover_audit", "message"),
    [
        ([], {"materialized": True}, "accepted mutation offspring"),
        ([{"operation": "fixture"}], None, "accepted crossover offspring"),
    ],
)
def test_parent_origin_report_rejects_missing_materialized_operation_family(
    tmp_path: Path,
    mutation_steps: list[dict[str, Any]],
    crossover_audit: dict[str, Any] | None,
    message: str,
) -> None:
    root = tmp_path / "parent-origin"
    _write_completed_root(root)
    entry_path = root / "proposal-journal" / "00000000.json"
    entry = json.loads(entry_path.read_text(encoding="utf-8"))
    entry["originKind"] = "structural_offspring"
    entry["proposal"]["mutationSteps"] = mutation_steps
    if crossover_audit is None:
        entry["proposal"].pop("crossoverAudit", None)
    else:
        entry["proposal"]["crossoverAudit"] = crossover_audit
    entry.pop("entrySha256")
    entry["entrySha256"] = canonical_sha256(entry)
    _write(entry_path, entry)

    with pytest.raises(TemporalDiscoveryContractError, match=message):
        admission._parent_origin_report(root)
