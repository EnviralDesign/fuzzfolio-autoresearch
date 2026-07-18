from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from autoresearch import level_c_workflow as workflow
from autoresearch.__main__ import build_parser
from autoresearch.config import load_config
from autoresearch.catalog_index import CATALOG_INDEX_SCHEMA_VERSION
from autoresearch.generation_archive import GenerationArchiveService, utc_now
from autoresearch.instrument_universe import universe_provenance
from autoresearch.level_c_workflow import (
    STAGES,
    LevelCWorkflowError,
    _validate_atlas_stage_root,
    _stage_receipt_path,
    audit_level_c,
    bootstrap_level_c,
    run_level_c_cutoff,
)
from autoresearch.level_c_operator import LevelCOperatorError


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _fixed_hash(character: str) -> str:
    return "sha256:" + character * 64


def _nested_report(path: Path) -> Path:
    attempt_ids = [f"attempt-{index:03d}" for index in range(443)]
    geometry = [
        ("2021-06-29", "2024-06-28", "2024-07-14", "2025-01-13"),
        ("2021-12-30", "2024-12-29", "2025-01-14", "2025-07-13"),
        ("2022-06-29", "2025-06-28", "2025-07-14", "2026-01-13"),
        ("2022-12-30", "2025-12-29", "2026-01-14", "2026-07-13"),
    ]
    folds = [
        {
            "fold": {
                "fold_id": f"fold-{index:02d}",
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
                "embargo_days": 15,
            },
            "status": "complete",
            "strategy_count": len(attempt_ids),
            "train_nonviable_count": 0,
            "outer_nonviable_count": 0,
            "outer_failed_count": 0,
            "records": [
                {
                    "attempt_id": attempt_id,
                    "train_validation_status": "valid",
                    "outer_validation_status": "valid",
                }
                for attempt_id in attempt_ids
            ],
        }
        for index, (train_start, train_end, test_start, test_end) in enumerate(
            geometry, start=1
        )
    ]
    path.write_text(
        json.dumps(
            {
                "status": "complete",
                "campaign_id": "archived-completed-nested",
                "evidence_campaign_plan_id": "archived-plan-id",
                "attempt_cohort_manifest_id": _fixed_hash("9"),
                "attempt_count": 443,
                "fold_count": 4,
                "portfolio_result_count": 24,
                "selection_basis": "recommended_cell",
                "fold_results": folds,
            }
        ),
        encoding="utf-8",
    )
    portfolio_path = path.parent / "portfolio-validation" / "nested-temporal-results.json"
    portfolio_path.parent.mkdir(parents=True, exist_ok=True)
    portfolio_path.write_text(
        json.dumps([{"variant": index} for index in range(24)]), encoding="utf-8"
    )
    return path


def _bootstrap_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    active = tmp_path / "runs"
    archive = tmp_path / "archived-runs"
    archive.mkdir(parents=True)
    catalog = archive / "attempt-catalog.sqlite"
    connection = sqlite3.connect(catalog)
    connection.executescript(
        """
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE run_signatures (
            run_id TEXT PRIMARY KEY,
            signature_json TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE attempt_rows (
            run_id TEXT NOT NULL,
            row_key TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            attempt_id TEXT NOT NULL,
            is_tombstoned INTEGER NOT NULL,
            has_full_backtest_36m INTEGER NOT NULL,
            row_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (run_id, row_key)
        );
        """
    )
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES ('schema_version', ?)",
        (str(CATALOG_INDEX_SCHEMA_VERSION),),
    )
    connection.commit()
    connection.close()
    report = _nested_report(archive / "nested-evidence-report.json")
    controls = tmp_path / "legacy-controls.json"
    identity = {
        "control_set_id": "legacy-controls-fixture",
        "schema": "legacy-controls-manifest-v1",
        "created_at_utc": "2026-07-16T12:00:00Z",
        "archive_context": {
            "archive_id": "manual-atomic-archive-001",
            "new_research_generation_id": "level-c-generation-001",
            "pre_cutover_runs_root": str(tmp_path / "old-runs"),
            "projected_archived_runs_root": str(archive.resolve()),
            "archive_relative_base": archive.name,
            "reference_only": True,
        },
        "exclusion_contract": {
            "active_seeding": "excluded",
            "active_candidate_scans": "excluded",
            "active_selection": "excluded",
            "active_optimizer_candidates": "excluded",
            "empirical_priors": "excluded",
            "permitted_use": "post_campaign_comparison_only",
            "copy_profiles": False,
            "edit_active_runs": False,
            "enforcement_note": "fixture",
        },
        "source_artifacts": {
            "exact_catalog_database": {
                "archive_relative_path": catalog.relative_to(archive).as_posix(),
                "projected_archived_path": str(catalog.resolve()),
                "sha256": _sha256(catalog).split(":", 1)[1],
            },
            "nested_evidence_report": {
                "archive_relative_path": report.relative_to(archive).as_posix(),
                "projected_archived_path": str(report.resolve()),
                "sha256": _sha256(report).split(":", 1)[1],
            },
            # The real controls manifest also carries frozen benchmark references.
            # Bootstrap authenticates them through the manifest self-hash while
            # deeply validating only its two explicit archive inputs.
            "legacy_benchmark_reference": {
                "path": str(tmp_path / "external-benchmark.json"),
                "sha256": "f" * 64,
            },
        },
        "categories": {
            "campaign_controls": {
                "nested": {
                    "campaign_id": "archived-completed-nested",
                    "evidence_campaign_plan_id": "archived-plan-id",
                    "attempt_cohort_manifest_id": _fixed_hash("9"),
                    "attempt_count": 443,
                    "fold_count": 4,
                    "portfolio_result_count": 24,
                    "selection_basis": "recommended_cell",
                }
            }
        },
    }
    identity_hash = _fixed_hash("0")
    identity_hash = "sha256:" + hashlib.sha256(
        json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    controls.write_text(
        json.dumps(
            {
                "schema": "legacy-controls-manifest-v1",
                "integrity": {
                    "identity_hash_algorithm": "sha256",
                    "self_hash": identity_hash,
                    "manifest_id": identity_hash,
                },
                "identity": identity,
            }
        ),
        encoding="utf-8",
    )
    base_config = load_config()
    (tmp_path / "portfolio.research-suites.json").write_bytes(
        (base_config.repo_root / "portfolio.research-suites.json").read_bytes()
    )
    config = replace(
        base_config,
        repo_root=tmp_path,
        fuzzfolio=replace(
            base_config.fuzzfolio,
            workspace_root=Path("C:/repos/Trading-Dashboard"),
        ),
    )
    monkeypatch.setattr("autoresearch.level_c_operator.load_config", lambda: config)
    universe = universe_provenance()
    arguments = {
        "config": config,
        "active_runs_root": active,
        "archive_root": archive,
        "archived_attempt_catalog": catalog,
        "archived_attempt_catalog_sha256": _sha256(catalog),
        "legacy_controls": controls,
        "legacy_controls_sha256": _sha256(controls),
        "completed_nested_report": report,
        "completed_nested_report_sha256": _sha256(report),
        "archive_id": "manual-atomic-archive-001",
        "new_generation_id": "level-c-generation-001",
        "lake_semantic_sha256": _fixed_hash("a"),
        "source_snapshot_sha256": _fixed_hash("b"),
        "universe_id": str(universe["universe_id"]),
        "universe_manifest_sha256": str(universe["universe_hash"]),
        "worker_contract_id": "replay-worker-contract-v1",
        "worker_contract_sha256": _fixed_hash("c"),
        "worker_image": "example.invalid/replay-worker:immutable",
        "global_seed": 73,
    }
    result = bootstrap_level_c(**arguments)
    return config, active, archive, arguments, result


def _rewrite_controls(arguments: dict, mutate) -> None:
    path = arguments["legacy_controls"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    mutate(payload["identity"])
    identity_hash = "sha256:" + hashlib.sha256(
        json.dumps(
            payload["identity"], ensure_ascii=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    payload["integrity"]["self_hash"] = identity_hash
    payload["integrity"]["manifest_id"] = identity_hash
    path.write_text(json.dumps(payload), encoding="utf-8")
    arguments["legacy_controls_sha256"] = _sha256(path)


def _archive_generation_cutover(
    service: GenerationArchiveService,
    tmp_path: Path,
    *,
    archive_id: str,
    new_generation_id: str,
    prior_generation_id: str | None = None,
) -> dict[str, object]:
    provenance: dict[str, object] = {"actor": "level-c-workflow-test"}
    if prior_generation_id is not None:
        provenance["prior_generation_id"] = prior_generation_id
    preview = service.dry_run(archive_id, new_generation_id, provenance=provenance)
    marker = tmp_path / f"{archive_id}.quiesced.json"
    marker.write_text(
        json.dumps(
            {
                "schema_name": "autoresearch.generation.quiescence",
                "schema_version": 1,
                "state": "quiesced",
                "writer_scope": "all-writers-stopped",
                "runs_root": str(service.runs_root),
                "archive_root": str(service.archive_root),
                "archive_id": archive_id,
                "new_generation_id": new_generation_id,
                "inventory_identity": preview["inventory_identity"],
                "issuer_id": "level-c-workflow-test",
                "nonce": hashlib.sha256(
                    f"{archive_id}:{new_generation_id}".encode("utf-8")
                ).hexdigest(),
                "issued_at": utc_now(),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    provenance["reviewed_inventory_identity"] = preview["inventory_identity"]
    provenance["cutover_quiescence"] = {
        "marker_path": str(marker),
        "marker_sha256": hashlib.sha256(marker.read_bytes()).hexdigest(),
    }
    return service.cutover(
        archive_id, new_generation_id, provenance=provenance
    )


def test_bootstrap_is_exact_idempotent_and_does_not_inventory_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, archive, arguments, first = _bootstrap_fixture(tmp_path, monkeypatch)
    (archive / "unrelated-71-gib-placeholder.bin").write_bytes(b"not inspected")

    second = bootstrap_level_c(**arguments)

    assert second == first
    assert first["operator_semantics"] == {
        "suite_name": "darwin-master-v1",
        "suite_config_relative_path": "portfolio.research-suites.json",
        "suite_config_sha256": _sha256(tmp_path / "portfolio.research-suites.json"),
        "selection_basis": "recommended_cell",
        "optimizer_backend": "pyo3",
    }
    assert set(first["execution_plans"]) == set("ABCD")
    assert audit_level_c(config=config, active_runs_root=active)["status"] == "valid"


def test_bootstrap_accepts_archive_generation_handoff_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, arguments, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    archive_id = "rejected-level-c-v1-worker-contract-20260717"
    prior_generation_id = str(arguments["new_generation_id"])
    successor_generation_id = "level-c-generation-002"

    _archive_generation_cutover(
        GenerationArchiveService(active),
        tmp_path,
        archive_id=archive_id,
        new_generation_id=successor_generation_id,
        prior_generation_id=prior_generation_id,
    )
    arguments = dict(arguments)
    arguments["new_generation_id"] = successor_generation_id

    handoff_manifest = json.loads(
        (active / "generation-manifest.json").read_text(encoding="utf-8")
    )
    assert handoff_manifest["archive_linkage"]["archive_id"] == archive_id
    assert list((active / "derived").iterdir()) == []

    result = bootstrap_level_c(**arguments)
    generation = json.loads(
        (active / "generation-manifest.json").read_text(encoding="utf-8")
    )

    assert "bootstrap_id" in result
    assert generation["new_generation_id"] == arguments["new_generation_id"]
    assert generation["archive_generation_handoff"]["prior_generation_id"] == prior_generation_id
    assert generation["archive_generation_handoff"]["successor_generation_id"] == successor_generation_id
    assert generation["archive_generation_handoff"]["archive_linkage"]["archive_id"] == archive_id
    assert generation["archive_generation_handoff"]["provenance"]["actor"] == "level-c-workflow-test"
    audit = audit_level_c(config=config, active_runs_root=active)
    assert audit["status"] == "valid"
    assert audit["archive_generation_handoff"]["prior_generation_id"] == prior_generation_id


def test_bootstrap_rejects_invalid_archive_generation_handoff_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, active, _, arguments, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    archive_id = "rejected-level-c-v1-worker-contract-20260717"
    prior_generation_id = str(arguments["new_generation_id"])
    successor_generation_id = "level-c-generation-002"
    _archive_generation_cutover(
        GenerationArchiveService(active),
        tmp_path,
        archive_id=archive_id,
        new_generation_id=successor_generation_id,
        prior_generation_id=prior_generation_id,
    )
    arguments = dict(arguments)
    arguments["new_generation_id"] = successor_generation_id
    generation_path = active / "generation-manifest.json"
    original = json.loads(generation_path.read_text(encoding="utf-8"))

    mismatched = dict(original)
    mismatched["new_generation_id"] = "level-c-wrong"
    generation_path.write_text(json.dumps(mismatched, sort_keys=True), encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="requested generation"):
        bootstrap_level_c(**arguments)

    generation_path.write_text(json.dumps(original, sort_keys=True), encoding="utf-8")
    no_linkage = dict(original)
    no_linkage.pop("archive_linkage")
    generation_path.write_text(json.dumps(no_linkage, sort_keys=True), encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="archive_linkage"):
        bootstrap_level_c(**arguments)

    generation_path.write_text(json.dumps(original, sort_keys=True), encoding="utf-8")
    no_prior = dict(original)
    no_prior["provenance"] = dict(no_prior["provenance"])
    no_prior["provenance"].pop("prior_generation_id")
    generation_path.write_text(json.dumps(no_prior, sort_keys=True), encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="prior_generation_id"):
        bootstrap_level_c(**arguments)

    generation_path.write_text(json.dumps(original, sort_keys=True), encoding="utf-8")
    wrong_prior = dict(original)
    wrong_prior["provenance"] = dict(wrong_prior["provenance"])
    wrong_prior["provenance"]["prior_generation_id"] = "level-c-other"
    generation_path.write_text(json.dumps(wrong_prior, sort_keys=True), encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="prior_generation_id"):
        bootstrap_level_c(**arguments)

    generation_path.write_text(json.dumps(original, sort_keys=True), encoding="utf-8")
    archive_manifest = Path(original["archive_linkage"]["archive_manifest_path"])
    archive_payload = json.loads(archive_manifest.read_text(encoding="utf-8"))
    archive_payload["state"] = "prepared"
    archive_manifest.write_text(json.dumps(archive_payload, sort_keys=True), encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="not complete"):
        bootstrap_level_c(**arguments)

    archive_payload["state"] = "complete"
    archive_manifest.write_text(json.dumps(archive_payload, sort_keys=True), encoding="utf-8")
    protocol = active / "derived" / "level-c" / "control" / "protocol.json"
    protocol.parent.mkdir(parents=True)
    protocol.write_text("{}", encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="ambiguous bootstrap partial state"):
        bootstrap_level_c(**arguments)


def test_bootstrap_rejects_source_drift_and_ambiguous_partial_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, _, arguments, _ = _bootstrap_fixture(tmp_path / "drift", monkeypatch)
    arguments["legacy_controls"].write_text('{"status":"changed"}', encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="hash mismatch"):
        bootstrap_level_c(**arguments)

    partial_repo = tmp_path / "partial"
    partial_repo.mkdir(parents=True)
    (partial_repo / "portfolio.research-suites.json").write_bytes(
        (arguments["config"].repo_root / "portfolio.research-suites.json").read_bytes()
    )
    partial_config = replace(arguments["config"], repo_root=partial_repo)
    monkeypatch.setattr("autoresearch.level_c_operator.load_config", lambda: partial_config)
    active = partial_config.runs_root
    control = active / "derived" / "level-c" / "control"
    control.mkdir(parents=True)
    (control / "protocol.json").write_text("{}", encoding="utf-8")
    partial_args = dict(arguments)
    partial_args["config"] = partial_config
    partial_args["active_runs_root"] = active
    with pytest.raises(LevelCWorkflowError, match="ambiguous bootstrap partial state"):
        bootstrap_level_c(**partial_args)


def test_bootstrap_rejects_semantic_controls_catalog_and_report_mismatches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, _, context_args, _ = _bootstrap_fixture(tmp_path / "context", monkeypatch)
    _rewrite_controls(
        context_args,
        lambda identity: identity["archive_context"].update({"archive_id": "wrong"}),
    )
    with pytest.raises(LevelCWorkflowError, match="archive context"):
        bootstrap_level_c(**context_args)

    _, _, _, generation_args, _ = _bootstrap_fixture(tmp_path / "generation-context", monkeypatch)
    _rewrite_controls(
        generation_args,
        lambda identity: identity["archive_context"].update({"new_research_generation_id": "level-c-v2"}),
    )
    with pytest.raises(LevelCWorkflowError, match="archive context"):
        bootstrap_level_c(**generation_args)

    _, _, _, exclusion_args, _ = _bootstrap_fixture(tmp_path / "exclusion", monkeypatch)
    _rewrite_controls(
        exclusion_args,
        lambda identity: identity["exclusion_contract"].update(
            {"active_selection": "allowed"}
        ),
    )
    with pytest.raises(LevelCWorkflowError, match="exclusion contract"):
        bootstrap_level_c(**exclusion_args)

    _, _, _, source_args, _ = _bootstrap_fixture(tmp_path / "source", monkeypatch)
    _rewrite_controls(
        source_args,
        lambda identity: identity["source_artifacts"]["exact_catalog_database"].update(
            {"sha256": "0" * 64}
        ),
    )
    with pytest.raises(LevelCWorkflowError, match="source artifact differs"):
        bootstrap_level_c(**source_args)

    _, _, _, catalog_args, _ = _bootstrap_fixture(tmp_path / "catalog", monkeypatch)
    catalog_args["archived_attempt_catalog"].write_bytes(b"not sqlite")
    catalog_args["archived_attempt_catalog_sha256"] = _sha256(
        catalog_args["archived_attempt_catalog"]
    )
    _rewrite_controls(
        catalog_args,
        lambda identity: identity["source_artifacts"]["exact_catalog_database"].update(
            {
                "sha256": catalog_args["archived_attempt_catalog_sha256"].split(":", 1)[1]
            }
        ),
    )
    with pytest.raises(LevelCWorkflowError, match="SQLite header"):
        bootstrap_level_c(**catalog_args)

    _, _, _, schema_args, _ = _bootstrap_fixture(tmp_path / "catalog-schema", monkeypatch)
    connection = sqlite3.connect(schema_args["archived_attempt_catalog"])
    connection.execute("DROP TABLE attempt_rows")
    connection.execute(
        "CREATE TABLE attempt_rows (run_id TEXT NOT NULL, attempt_id TEXT NOT NULL)"
    )
    connection.commit()
    connection.close()
    schema_args["archived_attempt_catalog_sha256"] = _sha256(
        schema_args["archived_attempt_catalog"]
    )
    _rewrite_controls(
        schema_args,
        lambda identity: identity["source_artifacts"]["exact_catalog_database"].update(
            {"sha256": schema_args["archived_attempt_catalog_sha256"].split(":", 1)[1]}
        ),
    )
    with pytest.raises(LevelCWorkflowError, match="schema is incompatible"):
        bootstrap_level_c(**schema_args)

    _, _, _, report_args, _ = _bootstrap_fixture(tmp_path / "report", monkeypatch)
    report = json.loads(report_args["completed_nested_report"].read_text(encoding="utf-8"))
    report["campaign_id"] = "wrong-campaign"
    report_args["completed_nested_report"].write_text(json.dumps(report), encoding="utf-8")
    report_args["completed_nested_report_sha256"] = _sha256(
        report_args["completed_nested_report"]
    )
    _rewrite_controls(
        report_args,
        lambda identity: identity["source_artifacts"]["nested_evidence_report"].update(
            {
                "sha256": report_args["completed_nested_report_sha256"].split(":", 1)[1]
            }
        ),
    )
    with pytest.raises(LevelCWorkflowError, match="campaign semantics"):
        bootstrap_level_c(**report_args)

    _, _, _, accounting_args, _ = _bootstrap_fixture(
        tmp_path / "report-accounting", monkeypatch
    )
    report = json.loads(
        accounting_args["completed_nested_report"].read_text(encoding="utf-8")
    )
    report["fold_results"][0]["records"] = []
    accounting_args["completed_nested_report"].write_text(
        json.dumps(report), encoding="utf-8"
    )
    accounting_args["completed_nested_report_sha256"] = _sha256(
        accounting_args["completed_nested_report"]
    )
    _rewrite_controls(
        accounting_args,
        lambda identity: identity["source_artifacts"]["nested_evidence_report"].update(
            {
                "sha256": accounting_args["completed_nested_report_sha256"].split(
                    ":", 1
                )[1]
            }
        ),
    )
    with pytest.raises(LevelCWorkflowError, match="strategy accounting"):
        bootstrap_level_c(**accounting_args)


def test_runner_recovers_exactly_once_across_every_stage_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    attempts = {stage: 0 for stage in STAGES}
    artifacts = active / "derived" / "level-c" / "fixture-artifacts"

    def fault_once(stage: str, **_kwargs):
        artifacts.mkdir(parents=True, exist_ok=True)
        artifact = artifacts / f"{stage}.json"
        content = json.dumps({"stage": stage}, sort_keys=True)
        if artifact.exists():
            assert artifact.read_text(encoding="utf-8") == content
        else:
            artifact.write_text(content, encoding="utf-8")
        attempts[stage] += 1
        if attempts[stage] == 1:
            raise RuntimeError(f"injected crash after {stage} output")
        return "candidates_frozen" if stage == "frozen_cohort" else "complete", [artifact]

    handlers = {stage: fault_once for stage in STAGES}
    for expected_stage in STAGES:
        with pytest.raises(RuntimeError, match=f"after {expected_stage} output"):
            run_level_c_cutoff(
                config=config,
                active_runs_root=active,
                cutoff="A",
                resume=True,
                stage_handlers=handlers,
            )
        assert not _stage_receipt_path(active, "A", expected_stage).exists()

    result = run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers=handlers,
    )

    assert result["status"] == "complete"
    assert result["completed_stages"] == list(STAGES)
    assert attempts == {stage: 2 for stage in STAGES}

    resumed = run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers={stage: lambda **_kwargs: pytest.fail("completed stage reran") for stage in STAGES},
    )
    assert resumed["completed_stages"] == list(STAGES)


def test_resumed_cutoff_creates_never_started_playhand_campaign(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    plan_path = active / "derived" / "level-c" / "control" / "execution-plan-A.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    campaign_root = Path(plan["expected_artifacts"]["playhand_campaign"]["resolved_path"])
    captured: dict[str, object] = {}
    arguments = dict(plan["playhand_arguments"])
    arguments.update(
        {
            "execution_plan_path": str(plan_path),
            "execution_plan_id": plan["plan_id"],
        }
    )

    monkeypatch.setattr(
        workflow,
        "executor_arguments_from_plan",
        lambda *_args, **_kwargs: (arguments, plan),
    )

    def fake_playhand(runtime) -> int:
        captured["resume"] = runtime.resume
        campaign_root.mkdir(parents=True, exist_ok=True)
        (campaign_root / "play-hand-lab-summary.json").write_text("{}", encoding="utf-8")
        return 0

    monkeypatch.setattr(workflow, "cmd_play_hand_lab", fake_playhand)

    outcome, artifacts = workflow._default_stage_handler(
        "playhand",
        config=config,
        active_root=active,
        cutoff="A",
        plan_path=plan_path,
        plan=plan,
        resume=True,
        gateway_url=None,
        gateway_token=None,
        atlas_active_probes=None,
        playhand_active_runs=None,
        nested_max_workers=1,
        trading_dashboard_root=None,
    )

    assert outcome == "complete"
    assert captured["resume"] is False
    assert artifacts == [campaign_root / "play-hand-lab-summary.json"]


def test_level_c_profile_snapshot_resolver_uses_plan_bound_worker_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    plan_path = active / "derived" / "level-c" / "control" / "execution-plan-A.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    arguments = dict(plan["playhand_arguments"])
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        workflow,
        "executor_arguments_from_plan",
        lambda *_args, **_kwargs: (arguments, plan),
    )

    def worker_ready(profile_payload, *, config, runtime):
        captured["payload"] = profile_payload
        captured["config"] = config
        captured["runtime"] = runtime
        return {"resolved": True, "notificationThreshold": 80.0}

    monkeypatch.setattr(workflow, "_worker_ready_profile_snapshot", worker_ready)
    resolver = workflow._level_c_profile_snapshot_resolver(
        config=config,
        plan_path=plan_path,
        plan=plan,
    )

    authoring = {"format": "fuzzfolio.scoring-profile", "profile": {"name": "Bounded"}}
    assert resolver(authoring) == {"resolved": True, "notificationThreshold": 80.0}
    assert captured["payload"] == authoring
    assert captured["config"] is config
    assert getattr(captured["runtime"], "trading_dashboard_root") == Path(
        plan["bound_contract"]["profile_model_source_root"]
    )


def test_playhand_stage_existing_campaign_requires_its_own_strict_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    plan = json.loads(
        (active / "derived" / "level-c" / "control" / "execution-plan-A.json").read_text(
            encoding="utf-8"
        )
    )
    campaign_root = Path(plan["expected_artifacts"]["playhand_campaign"]["resolved_path"])
    campaign_root.mkdir(parents=True)

    with pytest.raises(LevelCWorkflowError, match="missing its durable execution journal"):
        workflow._playhand_stage_resume_mode(campaign_root=campaign_root, cutoff_resume=True)

    journal_path = campaign_root / "play-hand-lab-execution-journal.json"
    journal_path.write_text("{}", encoding="utf-8")
    assert workflow._playhand_stage_resume_mode(
        campaign_root=campaign_root,
        cutoff_resume=True,
    )
    with pytest.raises(LevelCWorkflowError, match="requires Level C cutoff --resume"):
        workflow._playhand_stage_resume_mode(campaign_root=campaign_root, cutoff_resume=False)


def test_runner_fails_closed_on_mutated_stage_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)

    def complete(stage: str, **_kwargs):
        artifact = active / "derived" / "level-c" / f"{stage}.txt"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(stage, encoding="utf-8")
        return "candidates_frozen" if stage == "frozen_cohort" else "complete", [artifact]

    run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers={stage: complete for stage in STAGES},
    )
    (active / "derived" / "level-c" / "atlas.txt").write_text("mutated", encoding="utf-8")

    with pytest.raises(LevelCWorkflowError, match="artifact drift"):
        run_level_c_cutoff(
            config=config,
            active_runs_root=active,
            cutoff="A",
            resume=True,
            stage_handlers={stage: complete for stage in STAGES},
        )

    (active / "derived" / "level-c" / "atlas.txt").write_text("atlas", encoding="utf-8")
    _stage_receipt_path(active, "A", "playhand").unlink()
    with pytest.raises(LevelCWorkflowError, match="missing predecessor"):
        run_level_c_cutoff(
            config=config,
            active_runs_root=active,
            cutoff="A",
            resume=True,
            stage_handlers={stage: complete for stage in STAGES},
        )


def test_atlas_stage_root_requires_the_canonical_summary_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    plan = json.loads(
        (active / "derived" / "level-c" / "control" / "execution-plan-A.json").read_text(
            encoding="utf-8"
        )
    )
    atlas_root = active / "derived" / "atlas-runs" / str(plan["cutoff"]["atlas_run_id"])
    lineage_path = atlas_root / "recipe-priors" / "level-c-lineage.json"
    lineage_path.parent.mkdir(parents=True)
    lineage_path.write_text(
        json.dumps({"historical_lineage": {"execution_plan_id": plan["plan_id"]}}),
        encoding="utf-8",
    )
    summary = atlas_root / "atlas-lab-summary.json"
    summary.write_text("{}", encoding="utf-8")

    _validate_atlas_stage_root(
        plan,
        run_root=atlas_root,
        summary_path=summary,
        receipt={"artifacts": [{"path": str(summary)}]},
    )
    outside = active / "derived" / "atlas-lab-runs" / "wrong" / "atlas-lab-summary.json"
    outside.parent.mkdir(parents=True)
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="outside the authoritative Atlas root"):
        _validate_atlas_stage_root(plan, receipt={"artifacts": [{"path": str(outside)}]})


def test_runner_rejects_changed_portfolio_suite_after_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    suite_path = tmp_path / "portfolio.research-suites.json"
    document = json.loads(suite_path.read_text(encoding="utf-8"))
    document["research_suites"]["darwin-master-v1"]["description"] = "semantic drift"
    suite_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(LevelCOperatorError, match="authoritative sources"):
        run_level_c_cutoff(
            config=config,
            active_runs_root=active,
            cutoff="A",
            resume=True,
            stage_handlers={},
        )


def test_zero_candidate_cutoff_is_cleanly_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    called: list[str] = []

    def handler(stage: str, **_kwargs):
        called.append(stage)
        artifact = active / "derived" / "level-c" / f"zero-{stage}.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text("{}", encoding="utf-8")
        outcome = "no_defensible_candidates" if stage == "frozen_cohort" else "complete"
        return outcome, [artifact]

    result = run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers={stage: handler for stage in STAGES},
    )

    assert result["status"] == "non_promotable"
    assert called == ["atlas", "playhand", "frozen_cohort"]


def test_nested_portfolio_no_candidate_is_cleanly_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    called: list[str] = []

    def handler(stage: str, **_kwargs):
        called.append(stage)
        artifact = active / "derived" / "level-c" / f"nested-{stage}.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text("{}", encoding="utf-8")
        outcome = "no_candidate" if stage == "frozen_portfolio" else "complete"
        return outcome, [artifact]

    result = run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers={stage: handler for stage in STAGES},
    )

    assert result["status"] == "non_promotable"
    assert called == [
        "atlas",
        "playhand",
        "frozen_cohort",
        "training_evidence",
        "frozen_cells",
        "frozen_portfolio",
    ]
    receipt = json.loads(
        _stage_receipt_path(active, "A", "frozen_portfolio").read_text(encoding="utf-8")
    )
    assert receipt["outcome"] == "no_candidate"
    assert not _stage_receipt_path(active, "A", "selected_outer").exists()


def test_audit_is_non_mutating(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in active.rglob("*")
        if path.is_file()
    }

    result = audit_level_c(config=config, active_runs_root=active)

    after = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in active.rglob("*")
        if path.is_file()
    }
    assert result["status"] == "valid"
    assert before == after


@pytest.mark.parametrize(
    ("command", "arguments"),
    [
        ("level-c-bootstrap", ["--help"]),
        ("level-c-run-cutoff", ["--help"]),
        ("level-c-audit", ["--help"]),
    ],
)
def test_level_c_cli_help(command: str, arguments: list[str]) -> None:
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args([command, *arguments])
    assert exc.value.code == 0
