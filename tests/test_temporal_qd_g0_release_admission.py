"""Hermetic release-gate coverage for the direct native v5 G0 harness."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_native as native
import autoresearch.temporal_qd_supervisor as supervisor
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_evolution import (
    DIRECTIONAL_QD_POLICY,
    DIRECTIONAL_QD_POLICY_NAME,
    DIRECTIONAL_QD_POLICY_SHA256,
    QD_VERSION,
    canonical_empty_directional_bidirectional_archive_template,
    directional_qd_archive_policy_authority,
    qd_predeclared_evidence_context,
)
from autoresearch.temporal_qd_native import (
    G0_FINALIZATION_RUNTIME_RUST,
    PAIR_GENERATION_RUNTIME_PYTHON,
    TemporalQDNativeError,
    build_g0_finalization_runtime_config,
    build_pair_generation_runtime_config,
    load_legacy_v5_g0_finalization_runtime,
    validate_pair_generation_runtime_config,
)
from autoresearch.temporal_qd_pair_generation import (
    generate_v5_pair_population_python_oracle,
)
from scripts.admit_temporal_qd_g0_release import run_release_admission
from scripts.temporal_qd_front_half_python_oracle_corpus import (
    UniqueFixtureFactory,
    _arguments,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _materialize_existing_v5_proposal_root(
    tmp_path: Path,
    *,
    construction_width: int = 4,
    evaluation_width: int = 2,
    legacy_runtime: bool = False,
    supervisor_preflight: bool = False,
) -> tuple[Path, Path, dict[str, object]]:
    """Create a real Python-oracle journal under a minimal frozen v5 layout."""

    run_root = tmp_path / "run"
    proposal_root = run_root / "generations" / "generation-0001" / "proposal"
    archive_authority = directional_qd_archive_policy_authority()
    arguments = _arguments(
        generation_index=1,
        target_unique_candidates=construction_width,
        pair_factory=UniqueFixtureFactory(),
        g0_evaluation_width=evaluation_width,
    )
    proposal_ceiling = max(construction_width * 6, 16)
    arguments["run_config"] = {
        "seed": "release-admission-hermetic-fixture",
        "g0Bootstrap": {
            "initialConstructionPoolSize": construction_width,
            "evaluationPopulationSize": evaluation_width,
        },
        # The normal frozen search width remains the selected/evaluation
        # width, while pair config targetUniqueCandidates is construction.
        "parameters": {
            "targetUniqueCandidates": evaluation_width,
            "maxProposalAttempts": proposal_ceiling,
        },
        "archivePolicyAuthority": archive_authority,
    }
    arguments["identity_ledger_path"] = run_root / "identity-ledger.json"
    arguments["evidence_identity_context"] = qd_predeclared_evidence_context({})
    arguments["archive_policy_authority"] = archive_authority
    oracle_result = generate_v5_pair_population_python_oracle(
        output_root=proposal_root, **arguments
    )
    assert oracle_result["completed"] is True
    pair_config = json.loads((proposal_root / "pair-config.json").read_text())
    config: dict[str, object] = {
        "schemaVersion": "temporal_qd_supervisor_fixture_v5_release_v1",
        "qdVersion": QD_VERSION,
        "policyName": DIRECTIONAL_QD_POLICY_NAME,
        "policySha256": DIRECTIONAL_QD_POLICY_SHA256,
        "frozenPolicy": DIRECTIONAL_QD_POLICY,
        "pairGenerationRuntime": build_pair_generation_runtime_config(
            engine=PAIR_GENERATION_RUNTIME_PYTHON,
            execution_timeout_seconds=3600,
        ),
        "g0Bootstrap": {
            "schemaVersion": "temporal_qd_g0_bootstrap_config_v1",
            "activation": "generation_1_pair_random_immigrants_only",
            "initialConstructionPoolSize": construction_width,
            "evaluationPopulationSize": evaluation_width,
        },
        "bidirectionalPairGeneration": {
            "operatorImplementation": pair_config["operatorImplementation"],
        },
        "evolvableModuleAuthority": {
            "archivePolicyAuthority": archive_authority,
        },
        "identityLedger": {
            "schemaVersion": "temporal_qd_identity_ledger_v3",
            "policySha256": DIRECTIONAL_QD_POLICY_SHA256,
        },
    }
    if not legacy_runtime:
        config["g0FinalizationRuntime"] = build_g0_finalization_runtime_config(
            engine=G0_FINALIZATION_RUNTIME_RUST,
            execution_timeout_seconds=3600,
        )
    if supervisor_preflight:
        # Add only the frozen supervisor surfaces that its public restart
        # preflight reopens.  The proposal itself remains a real v5 Python
        # oracle journal and the migration is still sealed by Rust.
        initial_archive = canonical_empty_directional_bidirectional_archive_template()
        initial_archive_path = run_root / "initial-archive.json"
        template_path = run_root / "template.json"
        template = {"schemaVersion": "fixture_legacy_v5_template_v1"}
        _write_json(initial_archive_path, initial_archive)
        _write_json(template_path, template)
        worker_contract_sha256 = "sha256:" + "c" * 64
        evidence_context = qd_predeclared_evidence_context(
            template, worker_contract_sha256=worker_contract_sha256
        )
        config.update(
            {
                "broadAdmission": False,
                "workerContractSha256": worker_contract_sha256,
                "initialArchive": {
                    "path": str(initial_archive_path.resolve()),
                    "archiveSha256": initial_archive["archiveSha256"],
                    "generationIndex": 0,
                    "resultSetSha256": initial_archive["resultSetSha256"],
                },
                "bidirectionalPairSourceAuthority": config[
                    "bidirectionalPairGeneration"
                ],
                "evaluation": {
                    "templatePreparationPath": str(template_path.resolve()),
                    "templatePreparationSha256": canonical_sha256(template),
                    "predeclaredEvidenceContext": evidence_context,
                    "predeclaredEvidenceContextSha256": evidence_context[
                        "predeclaredEvidenceContextSha256"
                    ],
                    "gatewayUrl": "http://127.0.0.1:8799",
                    "timeoutSecondsPerGeneration": 60.0,
                    "enqueueBatchSize": 1,
                    "costViews": {},
                },
                "generationPlan": {
                    "firstGenerationIndex": 1,
                    "generationCount": 1,
                    "lastGenerationIndex": 1,
                    "targetUniqueCandidatesPerGeneration": evaluation_width,
                    "targetUniqueEvaluations": evaluation_width,
                },
                "frozenSearchPolicy": {
                    "targetUniqueCandidates": evaluation_width,
                    "maxProposalAttempts": proposal_ceiling,
                },
            }
        )
    config["configSha256"] = canonical_sha256(config)
    _write_json(run_root / "config.json", config)
    return run_root, proposal_root, config


def test_release_harness_runs_real_native_fresh_and_receipt_adoption(
    tmp_path: Path,
) -> None:
    run_root, proposal_root, _ = _materialize_existing_v5_proposal_root(tmp_path)

    fresh = run_release_admission(
        proposal_root=proposal_root,
        mode="fresh",
        evidence_output=run_root / "release-evidence" / "fresh.json",
    )
    assert fresh["outcome"]["nativeStatus"] == "completed"
    assert fresh["sourceJournal"] == {
        "measurement": "metadata_only_no_journal_content_read_by_harness",
        "fileCount": 4,
        "declaredBytes": fresh["sourceJournal"]["declaredBytes"],
        "contentBytesReadByHarness": 0,
    }
    assert fresh["nativeBridge"]["nativeProcessObserved"] is True
    assert fresh["nativeBridge"]["peakRssBytes"] > 0
    assert fresh["nativeBridge"]["admissionThreadCap"] == native.G0_ADMISSION_THREAD_CAP_DEFAULT
    fresh_diagnostics = fresh["nativeBridge"]["nativeDiagnostics"]
    assert fresh_diagnostics["mode"] == "completed"
    assert fresh_diagnostics["threadCap"] == native.G0_ADMISSION_THREAD_CAP_DEFAULT
    assert fresh_diagnostics["journalAdmission"]["entryCount"] == 4
    assert fresh_diagnostics["journalAdmission"]["acceptedCount"] == 4
    assert fresh_diagnostics["journalAdmission"]["sourceBytesRead"] > 0
    assert fresh_diagnostics["journalAdmission"]["populationSourceBytesRead"] > 0
    assert fresh_diagnostics["journalAdmission"]["sourceBytesRead"] == (
        fresh_diagnostics["journalAdmission"]["admissionSourceBytesRead"]
        + fresh_diagnostics["journalAdmission"]["populationSourceBytesRead"]
    )
    assert (
        1
        <= fresh_diagnostics["journalAdmission"]["workerCount"]
        <= native.G0_ADMISSION_THREAD_CAP_DEFAULT
    )
    assert fresh["publicOutputs"]["selectedArtifactBytes"] > 0
    assert fresh["publicOutputs"]["totalBytes"] > 0
    assert fresh["economicWorkPerformed"] is False
    assert fresh["supervisorContinued"] is False
    assert (proposal_root / "internal" / "g0-funnel" / "receipt.json").is_file()
    assert json.loads(
        (run_root / "release-evidence" / "fresh.json").read_text(encoding="utf-8")
    ) == fresh

    adopted = run_release_admission(
        proposal_root=proposal_root,
        mode="adopt",
        evidence_output=run_root / "release-evidence" / "adopt.json",
    )
    assert adopted["outcome"]["nativeStatus"] == "adopted"
    verification = adopted["outcome"]["adoptionVerification"]
    assert verification["proposalJournalBytesRead"] == 0
    assert verification["outputBytesHashed"] > 0
    adoption_diagnostics = adopted["nativeBridge"]["nativeDiagnostics"]
    assert adoption_diagnostics["mode"] == "adopted"
    assert adoption_diagnostics["journalAdmission"] is None
    assert adoption_diagnostics["outputBytesHashed"] == verification["outputBytesHashed"]
    assert adopted["sourceJournal"]["contentBytesReadByHarness"] == 0


def test_release_harness_supports_receiptless_native_audit(
    tmp_path: Path,
) -> None:
    run_root, proposal_root, _ = _materialize_existing_v5_proposal_root(
        tmp_path, construction_width=1, evaluation_width=1
    )

    audit = run_release_admission(
        proposal_root=proposal_root,
        mode="audit",
        evidence_output=run_root / "release-evidence" / "audit.json",
    )

    assert audit["mode"] == "audit"
    assert audit["outcome"]["nativeStatus"] == "completed"
    assert audit["outcome"]["adoptionVerification"] is None
    assert audit["sourceJournal"]["fileCount"] == 1
    assert audit["nativeBridge"]["receiptlessFullNativeAudit"] is True
    adopted = run_release_admission(
        proposal_root=proposal_root,
        mode="adopt",
        evidence_output=run_root / "release-evidence" / "audit-adopt.json",
    )
    assert adopted["outcome"]["nativeStatus"] == "adopted"


def test_parallel_admission_cap_preserves_asymmetric_4_to_2_public_bytes(
    tmp_path: Path,
) -> None:
    """Ordinal merge makes one and four workers byte-identical at G0 width split."""

    serial_run, serial_proposal, _ = _materialize_existing_v5_proposal_root(
        tmp_path / "serial", construction_width=4, evaluation_width=2
    )
    parallel_run, parallel_proposal, _ = _materialize_existing_v5_proposal_root(
        tmp_path / "parallel", construction_width=4, evaluation_width=2
    )
    serial = run_release_admission(
        proposal_root=serial_proposal,
        mode="fresh",
        evidence_output=serial_run / "evidence" / "fresh.json",
        admission_thread_cap=1,
    )
    parallel = run_release_admission(
        proposal_root=parallel_proposal,
        mode="fresh",
        evidence_output=parallel_run / "evidence" / "fresh.json",
        admission_thread_cap=4,
    )
    assert serial["outcome"]["proposalCount"] == 2
    assert serial["outcome"]["constructionPoolSize"] == 4
    assert parallel["outcome"]["proposalCount"] == 2
    assert parallel["outcome"]["constructionPoolSize"] == 4
    assert serial["nativeBridge"]["nativeDiagnostics"]["threadCap"] == 1
    assert parallel["nativeBridge"]["nativeDiagnostics"]["threadCap"] == 4
    for relative in (
        "g0-bootstrap/accepted-pool.json",
        "g0-bootstrap/selection.json",
        "g0-bootstrap/campaign-construction-ledger.json",
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
    ):
        assert (serial_proposal / relative).read_bytes() == (
            parallel_proposal / relative
        ).read_bytes(), relative


def test_legacy_v5_runtime_migration_is_receipt_bound_and_reopenable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_root, proposal_root, config = _materialize_existing_v5_proposal_root(
        tmp_path, legacy_runtime=True
    )
    # The production singleton is the preserved real frozen config.  Patch the
    # constant only inside this hermetic fixture so the exact migration logic,
    # rather than an open compatibility path, receives full coverage.
    monkeypatch.setattr(
        native,
        "G0_LEGACY_V5_PRE_CUTOVER_CONFIG_SHA256",
        config["configSha256"],
    )
    fresh = run_release_admission(
        proposal_root=proposal_root,
        mode="fresh",
        evidence_output=run_root / "release-evidence" / "legacy-fresh.json",
    )
    migration_path = proposal_root / native.G0_LEGACY_V5_RUNTIME_MIGRATION_PATH
    assert migration_path.is_file()
    assert fresh["outcome"]["legacyRuntimeMigrationSha256"]

    first = load_legacy_v5_g0_finalization_runtime(
        supervisor_config=config, run_root=run_root
    )
    second = load_legacy_v5_g0_finalization_runtime(
        supervisor_config=config, run_root=run_root
    )
    pair_runtime = validate_pair_generation_runtime_config(config["pairGenerationRuntime"])
    reopened = supervisor._resolve_g0_finalization_runtime_for_reopen(
        config=config,
        pair_runtime=pair_runtime,
        run_root=run_root,
    )
    assert first == second == reopened
    assert first["engine"] == G0_FINALIZATION_RUNTIME_RUST

    original_migration = migration_path.read_bytes()
    tampered = json.loads(original_migration)
    tampered["proposalRoot"] = str((run_root / "wrong-proposal-root").resolve())
    tampered["migrationSha256"] = canonical_sha256(
        {key: item for key, item in tampered.items() if key != "migrationSha256"}
    )
    _write_json(migration_path, tampered)
    with pytest.raises(TemporalQDNativeError, match="migration authority drifted"):
        load_legacy_v5_g0_finalization_runtime(
            supervisor_config=config, run_root=run_root
        )
    migration_path.write_bytes(original_migration)

    receipt_path = proposal_root / "internal" / "g0-funnel" / "receipt.json"
    original_receipt = receipt_path.read_bytes()
    receipt_path.unlink()
    with pytest.raises(TemporalQDNativeError, match="native G0 receipt is absent"):
        load_legacy_v5_g0_finalization_runtime(
            supervisor_config=config, run_root=run_root
        )
    receipt_path.write_bytes(original_receipt)

    wrong_config = dict(config)
    wrong_config["policyName"] = "different-policy"
    wrong_config["configSha256"] = canonical_sha256(
        {key: item for key, item in wrong_config.items() if key != "configSha256"}
    )
    with pytest.raises(TemporalQDNativeError, match="not authorized"):
        load_legacy_v5_g0_finalization_runtime(
            supervisor_config=wrong_config, run_root=run_root
        )
    with pytest.raises(TemporalDiscoveryContractError, match="outside immutable proposal"):
        run_release_admission(
            proposal_root=proposal_root,
            mode="adopt",
            evidence_output=proposal_root / "forbidden-evidence.json",
        )


def test_supervisor_reopens_legacy_v5_config_before_rebuilding_or_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The public restart path must reach post-validation without config rewrite.

    Compact JSON bytes intentionally differ from the supervisor's pretty
    writer.  If this path rebuilt the current config (which includes the new
    runtime field), ``_write_once`` would reject it before migration loading.
    """

    run_root, proposal_root, config = _materialize_existing_v5_proposal_root(
        tmp_path,
        legacy_runtime=True,
        supervisor_preflight=True,
    )
    monkeypatch.setattr(
        native,
        "G0_LEGACY_V5_PRE_CUTOVER_CONFIG_SHA256",
        config["configSha256"],
    )
    run_release_admission(
        proposal_root=proposal_root,
        mode="fresh",
        evidence_output=run_root / "release-evidence" / "legacy-preflight.json",
    )
    config_path = run_root / "config.json"
    frozen_config_bytes = config_path.read_bytes()

    class _FakeEvolvable:
        def generation_bindings(self, run_config: object) -> dict[str, object]:
            assert run_config == config["bidirectionalPairGeneration"]
            return {
                "runConfig": run_config,
                "archivePolicyAuthority": config["evolvableModuleAuthority"][
                    "archivePolicyAuthority"
                ],
                "behaviorAttributionRequirement": None,
                "operatorImplementation": config["bidirectionalPairGeneration"][
                    "operatorImplementation"
                ],
                "capacityReceipt": None,
            }

    class _FakePairAuthorityBundle:
        def __init__(self, frozen: object) -> None:
            assert frozen == config["bidirectionalPairSourceAuthority"]

        def __enter__(self) -> "_FakePairAuthorityBundle":
            return self

        def __exit__(self, *_args: object) -> bool:
            return False

        def open_evolvable_module_authority(self, authority: object) -> _FakeEvolvable:
            assert authority == config["evolvableModuleAuthority"]
            return _FakeEvolvable()

    class _RestartPreflightReached(Exception):
        pass

    validated: list[dict[str, object]] = []
    actual_validate = supervisor._validate_frozen_sources

    def capture_validate(
        frozen: object, **kwargs: object
    ) -> list[str]:
        assert isinstance(frozen, dict)
        validated.append(frozen)
        return actual_validate(frozen, **kwargs)

    def stop_after_restart_preflight(**_kwargs: object) -> dict[int, object]:
        raise _RestartPreflightReached()

    monkeypatch.setattr(supervisor, "PairAuthorityBundle", _FakePairAuthorityBundle)
    monkeypatch.setattr(supervisor, "_validate_frozen_sources", capture_validate)
    monkeypatch.setattr(
        supervisor,
        "_validate_completed_generations",
        stop_after_restart_preflight,
    )
    monkeypatch.setattr(
        supervisor,
        "_frozen_config",
        lambda **_kwargs: pytest.fail("legacy restart rebuilt the current config"),
    )
    monkeypatch.setattr(
        supervisor,
        "LabGatewayClient",
        lambda **_kwargs: pytest.fail("legacy restart launched a campaign"),
    )

    with pytest.raises(_RestartPreflightReached):
        supervisor.run_qd_supervisor(
            run_root=run_root,
            # These are intentionally not the frozen source paths.  Once the
            # exact legacy config and receipts authorize reopen, only its
            # persisted bindings may drive the restart preflight.
            initial_archive_path=tmp_path / "caller-archive-must-not-apply.json",
            source_preparation_path=None,
            base_generator_root=None,
            confirmed_entry_admission_root=None,
            template_preparation_path=tmp_path / "caller-template-must-not-apply.json",
            validator_command_file=None,
            parameters={},
            generation_count=99,
            autoresearch_commit="0" * 40,
                execution_engine_commit="1" * 40,
                worker_contract_sha256="sha256:" + "0" * 64,
                gateway_url="http://127.0.0.1:1",
                # This is a pre-cutover artifact intentionally reopened only
                # for historical receipt validation, not fresh construction.
                generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
            )

    assert validated == [config]
    assert config_path.read_bytes() == frozen_config_bytes
    assert json.loads(config_path.read_text(encoding="utf-8")) == config
    assert json.loads((run_root / "state.json").read_text(encoding="utf-8"))["configSha256"] == config[
        "configSha256"
    ]
