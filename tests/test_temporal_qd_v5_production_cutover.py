"""Production v5 cutover tripwires.

These tests keep the old rich-Python construction surfaces deliberately
hostile.  A receipt-shaped native adapter is the only proposal handoff the
supervisor may consume; no factory, compiler, Dashboard authority, or Python
generation/finalizer is allowed to rescue the path.
"""

from __future__ import annotations

import gzip
import hashlib
import inspect
import json
import os
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

import autoresearch.evolvable_module_qd_authority as evolvable_qd_authority
import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_native as native
import autoresearch.temporal_qd_pair_generation as pair_generation
import autoresearch.temporal_qd_supervisor as supervisor
import autoresearch.temporal_qd_v5_control_plane as control
from autoresearch.result_codec import canonical_json_bytes
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from test_temporal_qd_v5_supervisor_fixture import _fresh_v5_fixture


def _sha(value: bytes | str) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else value
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> tuple[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")
    path.write_bytes(encoded)
    return _sha(encoded), len(encoded)


def _sealed_archive_descriptor(
    path: Path, *, semantic_sha256: str, file_sha256: str | None = None, byte_length: int = 0
) -> dict[str, Any]:
    """Model the compact Rust archive transport used by the production seam."""

    return {
        "absolutePath": str(path.resolve()),
        "fileSha256": file_sha256 or _sha(f"{path.name}-raw"),
        "semanticSha256": semantic_sha256,
        "byteLength": byte_length,
    }


def test_current_v5_supervisor_rejects_oversized_invocation_document_before_opening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Receipt-addressed proposal controls have a fixed compact read envelope."""

    proposal_root = (tmp_path / "proposal").resolve()
    relative_path = "native-batch/v5-proposal/test/manifest.json"
    document_path = proposal_root / relative_path
    document_path.parent.mkdir(parents=True)
    document_path.write_bytes(
        b"x" * (supervisor._NATIVE_V5_INVOCATION_MANIFEST_LIMIT_BYTES + 1)
    )
    descriptor = {
        "schemaVersion": supervisor.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
        "documentSchemaVersion": supervisor.V5_PROPOSAL_MANIFEST_SCHEMA,
        "relativePath": relative_path,
        "absolutePath": str(document_path),
        "semanticSha256": _sha("semantic"),
        "fileSha256": _sha("file"),
        "byteLength": document_path.stat().st_size,
    }
    original_open = Path.open

    def no_oversized_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path == document_path:
            pytest.fail("supervisor opened an oversized invocation document")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", no_oversized_open)
    with pytest.raises(TemporalDiscoveryContractError, match="receipt drifted"):
        supervisor._native_v5_invocation_document(
            descriptor=descriptor,
            proposal_root=proposal_root,
            relative_path=relative_path,
            document_schema=supervisor.V5_PROPOSAL_MANIFEST_SCHEMA,
            identity_field="manifestSha256",
            name="oversized manifest",
        )


def test_current_v5_supervisor_accepts_the_bounded_static_authority_manifest(
    tmp_path: Path,
) -> None:
    """Frozen authority may exceed 1 MiB without admitting an unbounded result."""

    proposal_root = (tmp_path / "proposal").resolve()
    relative_path = "native-batch/v5-proposal/test/manifest.json"
    document_path = proposal_root / relative_path
    document_path.parent.mkdir(parents=True)
    manifest = {
        "schemaVersion": supervisor.V5_PROPOSAL_MANIFEST_SCHEMA,
        "staticAuthorityPadding": "x" * 1_100_000,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    raw = canonical_json_bytes(manifest) + b"\n"
    assert len(raw) > 1_100_000
    assert len(raw) <= supervisor._NATIVE_V5_INVOCATION_MANIFEST_LIMIT_BYTES
    document_path.write_bytes(raw)
    descriptor = {
        "schemaVersion": supervisor.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
        "documentSchemaVersion": supervisor.V5_PROPOSAL_MANIFEST_SCHEMA,
        "relativePath": relative_path,
        "absolutePath": str(document_path),
        "semanticSha256": manifest["manifestSha256"],
        "fileSha256": _sha(raw),
        "byteLength": len(raw),
    }
    validated = supervisor._native_v5_invocation_document(
        descriptor=descriptor,
        proposal_root=proposal_root,
        relative_path=relative_path,
        document_schema=supervisor.V5_PROPOSAL_MANIFEST_SCHEMA,
        identity_field="manifestSha256",
        name="large static authority manifest",
    )
    assert validated["document"] == manifest


def _native_adapter_for_call(**kwargs: Any) -> dict[str, Any]:
    """Make actual receipt-addressed files for a tiny native bridge stand-in.

    The stand-in deliberately returns only the published bridge adapter.  It
    has no candidate object, factory, compiler, validator, or Dashboard call.
    """

    proposal_root = Path(kwargs["output_root"]).resolve()
    generation_config = kwargs["generation_config"]
    generation_index = int(generation_config["generationIndex"])
    evaluation_size = int(kwargs["evaluation_population_size"])
    requested = int(generation_config["targetUniqueCandidates"])

    population_file_sha, population_bytes = _write_json(
        proposal_root / "population.json", {"native": "population"}
    )
    evaluation_file_sha, evaluation_bytes = _write_json(
        proposal_root / "evaluation-population.json", {"native": "evaluation"}
    )
    journal_file_sha, journal_bytes = _write_json(
        proposal_root / "generation-journal.json", {"native": "journal"}
    )
    ledger = {
        "schemaVersion": "temporal_qd_v5_test_identity_ledger_v1",
        "generationIndex": generation_index,
        "acceptedCount": requested,
    }
    ledger["identityLedgerSha256"] = canonical_sha256(ledger)
    ledger_file_sha, ledger_bytes = _write_json(
        proposal_root / "v5-native" / "identity-ledger.json", ledger
    )

    adapter: dict[str, Any] = {
        "schemaVersion": supervisor.V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA,
        "operation": supervisor.V5_PROPOSAL_OPERATION,
        "completed": True,
        "generationKind": kwargs["generation_kind"],
        "generationIndex": generation_index,
        "generationConfigSha256": generation_config["configSha256"],
        "authoritySha256": _sha("authority"),
        "attemptCount": requested,
        "acceptedCandidateCount": requested,
        "selectedEvaluationCandidateCount": evaluation_size,
        "publicationPlanSha256": _sha("publication-plan"),
        "publicationRequestSha256": _sha("publication-request"),
        "proposalResultSha256": _sha("proposal-result"),
        "proposalReceiptSha256": _sha("proposal-receipt"),
        "outputInventorySha256": _sha("output-inventory"),
        "population": {
            "relativePath": "population.json",
            "absolutePath": str((proposal_root / "population.json").resolve()),
            "semanticSha256": _sha("population-semantic"),
            "fileSha256": population_file_sha,
            "byteLength": population_bytes,
        },
        "evaluationPopulation": {
            "relativePath": "evaluation-population.json",
            "absolutePath": str(
                (proposal_root / "evaluation-population.json").resolve()
            ),
            "semanticSha256": _sha("evaluation-semantic"),
            "fileSha256": evaluation_file_sha,
            "byteLength": evaluation_bytes,
        },
        "generationJournal": {
            "relativePath": "generation-journal.json",
            "absolutePath": str((proposal_root / "generation-journal.json").resolve()),
            "semanticSha256": _sha("journal-semantic"),
            "fileSha256": journal_file_sha,
            "byteLength": journal_bytes,
        },
        "identityLedger": {
            "relativePath": "v5-native/identity-ledger.json",
            "absolutePath": str(
                (proposal_root / "v5-native" / "identity-ledger.json").resolve()
            ),
            "semanticSha256": ledger["identityLedgerSha256"],
            "fileSha256": ledger_file_sha,
            "byteLength": ledger_bytes,
        },
    }
    if kwargs["generation_kind"] in {
        supervisor.V5_PROPOSAL_GENERATION_G0,
        supervisor.V5_PROPOSAL_GENERATION_EVOLVED,
    }:
        # The supervisor receives only the content-addressed descriptor.  The
        # core funnel object itself remains Rust-owned and is never projected
        # into a Python candidate list.
        is_evolved = (
            kwargs["generation_kind"] == supervisor.V5_PROPOSAL_GENERATION_EVOLVED
        )
        fragment_key = "evolvedPublicationFragments" if is_evolved else "g0FunnelFragments"
        fragment_schema = (
            supervisor.V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA
            if is_evolved
            else supervisor.V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA
        )
        fragment_core_schema = (
            supervisor.V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA
            if is_evolved
            else supervisor.V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA
        )
        result_schema = (
            supervisor.V5_EVOLVED_PROPOSAL_RESULT_SCHEMA
            if is_evolved
            else supervisor.V5_PROPOSAL_RESULT_SCHEMA
        )
        invocation_schema = (
            supervisor.V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA
            if is_evolved
            else supervisor.V5_G0_NATIVE_V5_INVOCATION_SCHEMA
        )
        fragments = {
            "schemaVersion": fragment_core_schema,
            "fixture": "native-evolved-publication-fragments" if is_evolved else "native-g0-funnel-fragments",
            "generationIndex": generation_index,
        }
        fragments_sha256 = canonical_sha256(fragments)
        fragments_file_sha, fragments_bytes = _write_json(
            proposal_root
            / "v5-native"
            / "objects"
            / "sha256"
            / f"{fragments_sha256.removeprefix('sha256:')}.json",
            fragments,
        )
        if is_evolved:
            adapter["schemaVersion"] = (
                supervisor.V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
            )
        adapter[fragment_key] = {
            "schemaVersion": fragment_schema,
            "coreSchemaVersion": fragment_core_schema,
            "relativePath": (
                "v5-native/objects/sha256/"
                + fragments_sha256.removeprefix("sha256:")
                + ".json"
            ),
            "absolutePath": str(
                (
                    proposal_root
                    / "v5-native"
                    / "objects"
                    / "sha256"
                    / f"{fragments_sha256.removeprefix('sha256:')}.json"
                ).resolve()
            ),
            "semanticSha256": fragments_sha256,
            "fileSha256": fragments_file_sha,
            "byteLength": fragments_bytes,
        }
        if not is_evolved:
            # The public G0 stream is deliberately opaque to the supervisor:
            # this fixture only gives it the receipt-addressed bytes and the
            # self-hashed descriptor Rust will later consume.
            stream_path = proposal_root / supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_PATH
            stream_bytes = b'{"fixture":"native-g0-funnel-projection"}\n'
            stream_path.parent.mkdir(parents=True, exist_ok=True)
            stream_path.write_bytes(stream_bytes)
            stream_file_sha = _sha(stream_bytes)
            projection_receipt = {
                "schemaVersion": supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA,
                "relativePath": supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
                "rowSchema": supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA,
                "inputG0FunnelFragmentsSha256": fragments_sha256,
                "inputProposalAttemptFragmentSha256": _sha("proposal-attempt-fragment"),
                "rawSha256": stream_file_sha,
                "sizeBytes": len(stream_bytes),
                "rowCount": 1,
            }
            projection_receipt["projectionStreamReceiptSha256"] = canonical_sha256(
                projection_receipt
            )
            projection_root = projection_receipt["projectionStreamReceiptSha256"]
            # Core's content-addressed object is a strict binding wrapper.
            # The nested self-hashed receipt owns the semantic root while the
            # wrapper fixes its object path, so the supervisor must reject a
            # bare receipt at this address.
            projection_receipt_binding = {
                "g0FunnelProjectionStreamReceiptSha256": projection_root,
                "relativePath": (
                    "v5-native/objects/sha256/"
                    + projection_root.removeprefix("sha256:")
                    + ".json"
                ),
                "value": projection_receipt,
            }
            receipt_file_sha, receipt_bytes = _write_json(
                proposal_root
                / "v5-native"
                / "objects"
                / "sha256"
                / f"{projection_root.removeprefix('sha256:')}.json",
                projection_receipt_binding,
            )
            adapter["g0FunnelProjectionStream"] = {
                "schemaVersion": (
                    supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA
                ),
                "coreReceiptSchemaVersion": (
                    supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA
                ),
                "rowSchemaVersion": supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA,
                "stream": {
                    "relativePath": supervisor.V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
                    "absolutePath": str(stream_path.resolve()),
                    "semanticSha256": projection_root,
                    "fileSha256": stream_file_sha,
                    "byteLength": len(stream_bytes),
                },
                "receiptObject": {
                    "relativePath": (
                        "v5-native/objects/sha256/"
                        + projection_root.removeprefix("sha256:")
                        + ".json"
                    ),
                    "absolutePath": str(
                        (
                            proposal_root
                            / "v5-native"
                            / "objects"
                            / "sha256"
                            / f"{projection_root.removeprefix('sha256:')}.json"
                        ).resolve()
                    ),
                    "semanticSha256": projection_root,
                    "fileSha256": receipt_file_sha,
                    "byteLength": receipt_bytes,
                },
            }
        # v3 adds the only permitted reopening route for the rich evolved
        # attempt stream.  The supervisor does not discover these files: the
        # descriptor fixes both names beneath this invocation root.
        manifest = {
            "schemaVersion": supervisor.V5_PROPOSAL_MANIFEST_SCHEMA,
            "generationKind": kwargs["generation_kind"],
            "outputRoot": str(proposal_root),
            "resultPath": supervisor.V5_PROPOSAL_RESULT_FILENAME,
        }
        manifest["manifestSha256"] = canonical_sha256(manifest)
        invocation_root = (
            proposal_root
            / "native-batch"
            / "v5-proposal"
            / manifest["manifestSha256"].removeprefix("sha256:")
        )
        manifest_file_sha, manifest_bytes = _write_json(
            invocation_root / "manifest.json", manifest
        )
        result = {
            "schemaVersion": result_schema,
            "manifestSha256": manifest["manifestSha256"],
            "receiptSha256": adapter["proposalReceiptSha256"],
            "outputInventorySha256": adapter["outputInventorySha256"],
        }
        if not is_evolved:
            result["g0FunnelFragmentsSha256"] = fragments_sha256
            result["g0FunnelProjectionStreamReceiptSha256"] = (
                adapter["g0FunnelProjectionStream"]["stream"]["semanticSha256"]
            )
        result["resultSha256"] = canonical_sha256(result)
        result_file_sha, result_bytes = _write_json(
            invocation_root / supervisor.V5_PROPOSAL_RESULT_FILENAME, result
        )
        adapter["proposalResultSha256"] = result["resultSha256"]
        adapter["nativeV5Invocation"] = {
            "schemaVersion": invocation_schema,
            "proposalManifest": {
                "schemaVersion": supervisor.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
                "documentSchemaVersion": supervisor.V5_PROPOSAL_MANIFEST_SCHEMA,
                "relativePath": (
                    "native-batch/v5-proposal/"
                    + manifest["manifestSha256"].removeprefix("sha256:")
                    + "/manifest.json"
                ),
                "absolutePath": str((invocation_root / "manifest.json").resolve()),
                "semanticSha256": manifest["manifestSha256"],
                "fileSha256": manifest_file_sha,
                "byteLength": manifest_bytes,
            },
            "proposalResult": {
                "schemaVersion": supervisor.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
                "documentSchemaVersion": result_schema,
                "relativePath": (
                    "native-batch/v5-proposal/"
                    + manifest["manifestSha256"].removeprefix("sha256:")
                    + "/"
                    + supervisor.V5_PROPOSAL_RESULT_FILENAME
                ),
                "absolutePath": str(
                    (invocation_root / supervisor.V5_PROPOSAL_RESULT_FILENAME).resolve()
                ),
                "semanticSha256": result["resultSha256"],
                "fileSha256": result_file_sha,
                "byteLength": result_bytes,
            },
            "proposalReceiptSha256": adapter["proposalReceiptSha256"],
            "outputInventorySha256": adapter["outputInventorySha256"],
        }
    adapter["adapterSha256"] = canonical_sha256(adapter)
    return adapter


def _state(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": supervisor.SUPERVISOR_STATE_SCHEMA,
        "configSha256": config["configSha256"],
        "status": "running",
        "stage": "generating",
        "completedGenerations": [],
    }


def _compact_real_native_v5_config() -> dict[str, Any]:
    """Build the smallest production-shaped v5 supervisor control plane.

    The sealed fixture is deliberately static: it exercises Rust's native
    construction closure without hydrating the retired Dashboard authority.
    ``_run_native_v5_generation`` remains responsible for deriving the final
    production ``build_pair_generation_config`` shape.
    """

    fixture = json.loads(
        gzip.decompress(
            (
                Path(__file__).parent
                / "fixtures"
                / "temporal_qd_v5_shared_authority_oracle.json.gz"
            ).read_bytes()
        )
    )
    authority_inputs = fixture["authorityInputs"]
    pair_source = deepcopy(authority_inputs["pairSourceAuthority"])
    evolvable = deepcopy(authority_inputs["evolvableModuleAuthority"])
    generation_run_config = deepcopy(pair_source)
    generation_run_config.pop("operatorImplementation", None)
    runtime, bindings = supervisor._build_native_v5_proposal_runtime(
        pair_source_authority=pair_source,
        evolvable_module_authority=evolvable,
        generation_run_config=generation_run_config,
        execution_timeout_seconds=120,
    )
    config: dict[str, Any] = {
        "qdVersion": authority_inputs["qdEngineVersion"],
        "evolvableModuleAuthority": evolvable,
        "bidirectionalPairSourceAuthority": pair_source,
        "bidirectionalPairGeneration": bindings["runConfig"],
        "pairGenerationRuntime": supervisor.build_pair_generation_runtime_config(
            engine=supervisor.PAIR_GENERATION_RUNTIME_RUST,
            execution_timeout_seconds=120,
        ),
        "nativeV5ProposalRuntime": runtime,
        "g0Bootstrap": {
            "schemaVersion": "temporal_qd_g0_bootstrap_config_v1",
            "initialConstructionPoolSize": 2,
            "evaluationPopulationSize": 1,
            "activation": "generation_1_pair_random_immigrants_only",
        },
        "frozenSearchPolicy": {
            "targetUniqueCandidates": 1,
            "maxProposalAttempts": 5,
        },
    }
    config["configSha256"] = canonical_sha256(config)
    return config


def test_uncommitted_native_v5_g0_restart_is_not_classified_as_legacy(
    tmp_path: Path,
) -> None:
    config = _compact_real_native_v5_config()
    assert "g0FinalizationRuntime" not in config
    (tmp_path / "config.json").write_bytes(canonical_json_bytes(config) + b"\n")

    assert supervisor._open_legacy_v5_g0_reopen_authority(root=tmp_path) is None


def test_fresh_v5_defaults_to_one_native_transaction_and_never_opens_python_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs, _fixture = _fresh_v5_fixture(tmp_path)
    inputs.pop("pair_generation_engine")
    forbidden = lambda *_args, **_kwargs: pytest.fail("retired Python construction was called")
    monkeypatch.setattr(supervisor, "PairAuthorityBundle", forbidden)
    monkeypatch.setattr(
        evolvable_qd_authority,
        "open_evolvable_module_pair_authority",
        forbidden,
    )
    monkeypatch.setattr(supervisor, "generate_qd_generation", forbidden)
    # The initial archive is a Rust-certified transport for current v5.  A
    # fresh config freeze may bind that compact descriptor, but it must not
    # reopen/decode the archive before qd-batch owns the first transaction.
    initial_archive = Path(inputs["initial_archive_path"]).resolve()
    original_read_bytes = Path.read_bytes

    def no_initial_archive_read(path: Path) -> bytes:
        if path.resolve() == initial_archive:
            pytest.fail("fresh current v5 decoded the certified initial archive")
        return original_read_bytes(path)

    monkeypatch.setattr(supervisor, "_load_archive", forbidden)
    monkeypatch.setattr(Path, "read_bytes", no_initial_archive_read)

    config, warnings = supervisor._frozen_config(**inputs)

    assert warnings == []
    assert config["pairGenerationRuntime"]["engine"] == supervisor.PAIR_GENERATION_RUNTIME_RUST
    assert config["nativeV5ProposalRuntime"]["engine"] == supervisor.NATIVE_V5_PROPOSAL_ENGINE
    assert supervisor._validate_frozen_sources(config) == []


def test_fast_ephemeral_mode_is_frozen_as_a_distinct_current_v5_contract(
    tmp_path: Path,
) -> None:
    inputs, _fixture = _fresh_v5_fixture(tmp_path)
    inputs["native_v5_execution_mode"] = supervisor.V5_EXECUTION_MODE_FAST_EPHEMERAL

    config, warnings = supervisor._frozen_config(**inputs)

    assert warnings == []
    assert (
        config["nativeV5ExecutionMode"]
        == supervisor.V5_EXECUTION_MODE_FAST_EPHEMERAL
    )
    assert supervisor._validate_frozen_sources(config) == []


def test_fresh_v5_refuses_an_explicit_python_fallback_before_any_authority_opens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs, _fixture = _fresh_v5_fixture(tmp_path)
    inputs["pair_generation_engine"] = supervisor.PAIR_GENERATION_RUNTIME_PYTHON
    monkeypatch.setattr(
        supervisor,
        "PairAuthorityBundle",
        lambda *_args, **_kwargs: pytest.fail("v5 fallback attempted to open authority"),
    )

    with pytest.raises(TemporalDiscoveryContractError, match="Rust-native v5 transaction"):
        supervisor._frozen_config(**inputs)


def _minimal_supervisor_call(*, run_root: Path, **kwargs: Any) -> dict[str, Any]:
    """Inputs that must remain unopened by the early finalizer cutover gate."""

    return supervisor.run_qd_supervisor(
        run_root=run_root,
        initial_archive_path=run_root / "never-opened-initial-archive.json",
        source_preparation_path=None,
        base_generator_root=None,
        confirmed_entry_admission_root=None,
        template_preparation_path=run_root / "never-opened-template.json",
        validator_command_file=None,
        parameters={},
        generation_count=1,
        autoresearch_commit="test-autoresearch",
        execution_engine_commit="test-execution",
        worker_contract_sha256=_sha("test-worker-contract"),
        gateway_url="http://127.0.0.1:9",
        **kwargs,
    )


def test_current_v5_requires_rust_finalization_before_config_or_authority_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("fresh/current v5 reached config or authority work")

    monkeypatch.setattr(supervisor, "_frozen_config", forbidden)
    fresh_root = tmp_path / "fresh-v5"
    with pytest.raises(
        TemporalDiscoveryContractError, match="generation_finalization_engine='rust'"
    ):
        _minimal_supervisor_call(
            run_root=fresh_root,
            evolvable_module_authority_config={},
            generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
        )
    assert not fresh_root.exists()

    restart_root = tmp_path / "current-v5-restart"
    restart_root.mkdir()
    _write_json(restart_root / "config.json", {"nativeV5ProposalRuntime": {}})
    with pytest.raises(
        TemporalDiscoveryContractError, match="generation_finalization_engine='rust'"
    ):
        _minimal_supervisor_call(
            run_root=restart_root,
            generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
        )


def test_fast_ephemeral_generation_uses_only_the_direct_rotating_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path.resolve()
    proposal_root = root / "generations" / "generation-0001" / "proposal"
    evaluation_population = proposal_root / "evaluation-population.json"
    identity_ledger = proposal_root / "v5-native" / "identity-ledger.json"
    state: dict[str, Any] = {
        "stage": "generation_proposal",
        "uniqueCandidatesEvaluated": 0,
        "workerTasksCompleted": 0,
        "completedGenerations": [],
    }
    adapter = {
        "executionMode": supervisor.V5_EXECUTION_MODE_FAST_EPHEMERAL,
        "selectedEvaluationCandidateCount": 1,
        "evaluationPopulation": {
            "absolutePath": str(evaluation_population),
            "semanticSha256": _sha("evaluation-semantic"),
            "fileSha256": _sha("evaluation-file"),
            "byteLength": 123,
        },
        "identityLedger": {
            "absolutePath": str(identity_ledger),
            "relativePath": "v5-native/identity-ledger.json",
            "semanticSha256": _sha("ledger-semantic"),
            "fileSha256": _sha("ledger-file"),
            "byteLength": 99,
        },
    }
    runtime = {"authoritySha256": _sha("runtime")}
    panel = {"panelId": "panel-1"}
    rotating = {
        "rotatingEvidenceSha256": _sha("rotating"),
        "panels": [panel],
        "absoluteGenerationMapping": {"cycleLength": 1},
    }
    config = {
        "rotatingEvidence": rotating,
        "evolvableModuleAuthority": {
            "archivePolicyAuthority": {"policyBindingSha256": _sha("policy")}
        },
    }

    monkeypatch.setattr(
        supervisor,
        "_validate_native_v5_construction_adapter",
        lambda **_kwargs: adapter,
    )
    monkeypatch.setattr(
        supervisor,
        "_v5_evolvable_authority",
        lambda _config: config["evolvableModuleAuthority"],
    )
    monkeypatch.setattr(
        supervisor,
        "_native_v5_archive_policy_binding",
        lambda value: dict(value),
    )
    monkeypatch.setattr(
        supervisor,
        "_native_v5_prefinalizer_archive_binding",
        lambda value, **_kwargs: dict(value),
    )
    monkeypatch.setattr(
        supervisor, "_native_runtime_authority_for_generation", lambda **_kwargs: runtime
    )
    monkeypatch.setattr(
        supervisor, "_require_native_v5_control_plane_runtime_authority", lambda _value: None
    )
    monkeypatch.setattr(supervisor, "panel_for_generation", lambda *_args: panel)
    monkeypatch.setattr(
        supervisor,
        "_run_native_v5_campaign_round",
        lambda **_kwargs: {
            "campaignReceipt": {
                "receiptPath": str(root / "campaign-receipt.json"),
                "receipt": {"campaignInput": {"taskCount": 3}},
            }
        },
    )
    monkeypatch.setattr(
        supervisor,
        "build_native_v5_fast_ephemeral_prefinalizer_base_manifest",
        lambda **_kwargs: {"manifestPath": str(root / "prefinalizer-base.json")},
    )
    monkeypatch.setattr(
        supervisor,
        "run_native_v5_rotating_prefinalizer",
        lambda **_kwargs: {
            "receipt": {
                "status": "ready_for_finalizer",
                "roundIndex": 0,
                "finalizerManifest": {"path": str(root / "finalizer" / "manifest.json")},
            },
            "taskSelections": [],
        },
    )
    monkeypatch.setattr(
        supervisor,
        "run_native_v5_fast_ephemeral_generation_finalizer",
        lambda **_kwargs: {
            "result": {
                "parentArchive": {"archiveSha256": _sha("parent")},
                "cumulativeArchive": {"archiveSha256": _sha("cumulative")},
                "occupiedCellCount": 1,
                "newCellCount": 1,
                "memberCount": 1,
                "parentSchedule": {"schemaVersion": "fixture"},
            },
            "artifacts": {},
        },
    )
    monkeypatch.setattr(supervisor, "_save_state", lambda *_args: None)
    forbidden = lambda *_args, **_kwargs: pytest.fail(
        "fast-ephemeral path entered durable generation finalization"
    )
    monkeypatch.setattr(supervisor, "_complete_native_v5_generation", forbidden)
    monkeypatch.setattr(supervisor, "_apply_native_v5_state_application", forbidden)

    result = supervisor._complete_native_v5_generation_fast_ephemeral(
        root=root,
        state=state,
        state_path=root / "state.json",
        config=config,
        generation_index=1,
        generation_result={
            "generationKind": "g0",
            "nativeV5Construction": adapter,
        },
        parent_archive_descriptor=_sealed_archive_descriptor(
            root / "initial-archive.json", semantic_sha256=_sha("initial")
        ),
        previous_cumulative_archive_descriptor=None,
        gateway_token="fixture-token",
    )

    assert result["generationRecord"]["candidateCount"] == 1
    assert result["generationRecord"]["totalGenerationTaskCount"] == 3
    assert state["uniqueCandidatesEvaluated"] == 1
    assert state["workerTasksCompleted"] == 3
    assert state["currentGenerationIndex"] == 2
    assert state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] == {
        "absolutePath": str(identity_ledger),
        "semanticSha256": _sha("ledger-semantic"),
        "fileSha256": _sha("ledger-file"),
        "byteLength": 99,
    }


def test_v5_legacy_construction_apis_reject_before_work_but_named_oracle_survives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority = qd.directional_qd_archive_policy_authority()
    common = {
        "output_root": tmp_path / "never-opened-python-proposal",
        "generation_index": 1,
        "target_unique_candidates": 1,
        "run_config": {"archivePolicyAuthority": authority},
        "pair_policy": {},
        "module_authority": object(),
        "native_validator": object(),
        "pair_compiler": object(),
        "archive_policy_authority": authority,
    }
    with pytest.raises(TemporalDiscoveryContractError, match="Rust-native v5 proposal"):
        pair_generation.generate_pair_population(**common)
    assert not common["output_root"].exists()

    seen: dict[str, Any] = {}
    monkeypatch.setattr(
        pair_generation,
        "_generate_pair_population_impl",
        lambda **kwargs: seen.update(kwargs) or {"completed": True},
    )
    assert pair_generation.generate_v5_pair_population_python_oracle(**common) == {
        "completed": True
    }
    assert seen["archive_policy_authority"] == authority

    def forbidden_native_root(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("generic native bridge reached filesystem/process work")

    monkeypatch.setattr(native, "_ensure_real_directory_tree", forbidden_native_root)
    with pytest.raises(native.TemporalQDNativeError, match="run_native_v5_generation_construction"):
        native.run_native_generation(
            output_root=tmp_path / "never-opened-native-proposal",
            parent_archive_path=tmp_path / "never-opened-parent.json",
            parent_archive_sha256=_sha("parent"),
            runtime_authority={},
            generation_config={"runConfig": {"archivePolicyAuthority": authority}},
            identity_ledger_path=tmp_path / "never-opened-ledger.json",
            max_new_proposals=None,
            native_execution_timeout_seconds=60,
            allow_empty_quality_bootstrap=False,
            g0_evaluation_width=None,
            frozen_construction_catalog=None,
            qd_version=authority["qdVersion"],
            policy_name=authority["policyName"],
            policy_sha256=authority["policySha256"],
            frozen_policy=authority["frozenPolicy"],
        )


def test_generation_finalization_defaults_to_rust() -> None:
    parameter = inspect.signature(supervisor.run_qd_supervisor).parameters[
        "generation_finalization_engine"
    ]
    assert parameter.default == supervisor.GENERATION_FINALIZATION_ENGINE_RUST


def test_current_v5_startup_runs_state_application_recovery_before_any_phase_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The recovery seam is in the real supervisor startup path, not dead code."""

    config = {
        "nativeV5ProposalRuntime": {"fixture": True},
        "rotatingEvidence": {},
    }
    config["configSha256"] = canonical_sha256(config)
    finalizer = tmp_path / "temporal-qd-generation-finalizer.exe"
    finalizer.write_bytes(b"fixture-finalizer")
    calls: list[dict[str, Any]] = []

    class RecoveryReached(Exception):
        pass

    monkeypatch.setattr(supervisor, "_frozen_config", lambda **_kwargs: (config, []))
    monkeypatch.setattr(
        supervisor,
        "_freeze_native_finalization_runtime_authority",
        lambda **_kwargs: {"fixture": "runtime"},
    )
    monkeypatch.setattr(
        supervisor, "_require_native_v5_control_plane_runtime_authority", lambda value: value
    )

    def recover(**kwargs: Any) -> bool:
        calls.append(kwargs)
        raise RecoveryReached

    monkeypatch.setattr(supervisor, "_recover_native_v5_state_application", recover)
    monkeypatch.setattr(
        supervisor,
        "_validate_frozen_sources",
        lambda *_args, **_kwargs: pytest.fail(
            "startup phase validation ran before v5 state-application recovery"
        ),
    )
    with pytest.raises(RecoveryReached):
        _minimal_supervisor_call(
            run_root=tmp_path / "run",
            generation_finalizer_binary=finalizer,
            generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_RUST,
            tail_result_mode=supervisor.TAIL_RESULT_MODE_INDEXED,
        )
    assert len(calls) == 1
    assert calls[0]["runtime_authority"] == {"fixture": "runtime"}


def test_current_v5_never_enters_python_evidence_ladder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later ladder restart cannot revive the retired Python cohort loop."""

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("current v5 attempted to open or construct a Python ladder")

    monkeypatch.setattr(supervisor, "_ladder_cohort", forbidden)
    monkeypatch.setattr(supervisor, "_canonical_file", forbidden)
    with pytest.raises(TemporalDiscoveryContractError, match="Rust ladder selection/reduction"):
        supervisor._run_evidence_ladder(
            root=tmp_path,
            config={"nativeV5ProposalRuntime": {}, "evidenceLadder": {}},
            client=object(),  # type: ignore[arg-type]
            final_archive_path=tmp_path / "must-not-open.json",
        )


def test_native_v5_campaign_round_chains_only_native_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Current v5 campaign orchestration never regains a Python row loop."""

    template = tmp_path / "panel-template.json"
    catalog = tmp_path / "catalog.json"
    evaluation = tmp_path / "evaluation-population.json"
    for path in (template, catalog, evaluation):
        path.write_text("{}\n", encoding="utf-8")
    panel = {"panelId": "panel-2", "windows": []}
    policy = qd.directional_qd_archive_policy_authority()
    rotating = {
        "rotatingEvidenceSha256": _sha("rotating"),
        "panelTemplates": {
            "panel-2": {
                "path": str(template.resolve()),
                "preparationSha256": _sha("template-preparation"),
            }
        },
        "provisionalReduction": {"maxCandidates": 3},
    }
    config = {
        "nativeV5ProposalRuntime": {},
        "rotatingEvidence": rotating,
        "constructionOperatorPolicy": {
            "catalog": {
                "path": str(catalog.resolve()),
                "catalogSha256": _sha("construction-catalog"),
            }
        },
        "repositories": {"executionEngineCommit": "a" * 40},
        "workerContractSha256": _sha("worker"),
        "evaluation": {
            "behaviorAttributionRequirement": {"fixture": "behavior"},
            "gatewayUrl": "http://127.0.0.1:9",
            "timeoutSecondsPerGeneration": 60.0,
            "enqueueBatchSize": 8,
        },
        "frozenSearchPolicy": {
            "minimumTotalTrades": 1,
            "minimumTradesPerWindow": 1,
            "capTrades": 2,
        },
    }
    calls: list[str] = []

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("current v5 campaign reached a retired Python row loop")

    monkeypatch.setattr(supervisor, "panel_for_generation", lambda *_args: panel)
    monkeypatch.setattr(
        supervisor,
        "_v5_evolvable_authority",
        lambda _config: {"archivePolicyAuthority": policy},
    )
    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", forbidden)
    monkeypatch.setattr(supervisor, "run_temporal_search_tasks", forbidden)
    monkeypatch.setattr(supervisor, "build_qd_archive", forbidden)

    def freeze(**kwargs: Any) -> dict[str, Any]:
        calls.append("freeze")
        assert kwargs["evaluation_population_path"] == evaluation
        assert kwargs["evaluation_population_raw_sha256"] == _sha("evaluation")
        assert kwargs["cohort_selection_path"] is None
        return {
            "outputRoot": str(kwargs["output_root"]),
            "checkpointPath": str(
                Path(kwargs["output_root"]) / "campaign-input-checkpoint.json"
            ),
            "cohortPopulationRawSha256": _sha("cohort-population"),
            "cohortPopulationSha256": _sha("cohort-population-semantic"),
        }

    def dispatch(**kwargs: Any) -> dict[str, Any]:
        calls.append("dispatch")
        assert kwargs["mode"] == "fresh"
        assert kwargs["output_root"] == tmp_path / "campaign" / "gateway-dispatch"
        assert kwargs["campaign_input_checkpoint_path"] == (
            tmp_path / "campaign" / "campaign-input-checkpoint.json"
        )
        return {
            "outputRoot": str(kwargs["output_root"]),
            "receiptPath": str(
                kwargs["output_root"]
                / ".native-gateway-dispatch"
                / "execution-receipt.json"
            ),
        }

    def output(**kwargs: Any) -> dict[str, Any]:
        calls.append("output")
        assert kwargs["campaign_input_checkpoint_path"] == (
            tmp_path / "campaign" / "campaign-input-checkpoint.json"
        )
        assert kwargs["gateway_execution_receipt_path"] == (
            tmp_path
            / "campaign"
            / "gateway-dispatch"
            / ".native-gateway-dispatch"
            / "execution-receipt.json"
        )
        assert kwargs["output_root"] == tmp_path / "campaign" / "campaign-output"
        assert kwargs["cohort_source"] == {
            "kind": "proposal_evaluation_population",
            "sourceSemanticSha256": _sha("evaluation"),
            "candidateCount": 1,
            "selectionSha256": None,
        }
        checkpoint_path = kwargs["output_root"] / "campaign-output-checkpoint.json"
        checkpoint = {
            "schemaVersion": control.CAMPAIGN_OUTPUT_CHECKPOINT_SCHEMA,
            "receiptSha256": _sha("campaign-output"),
        }
        return {
            "checkpoint": checkpoint,
            "checkpointPath": str(checkpoint_path),
            "result": {
                "campaignSeal": {"campaignSealSha256": _sha("seal")},
                "directionalTailAuthority": {
                    "tailAuthoritySha256": _sha("directional-tail-authority")
                },
                "tailResultIndex": {"tailResultIndexSha256": _sha("tail-index")},
            },
        }

    monkeypatch.setattr(supervisor, "run_native_v5_campaign_freeze", freeze)
    monkeypatch.setattr(supervisor, "run_native_gateway_dispatch", dispatch)
    monkeypatch.setattr(supervisor, "run_native_campaign_output", output)

    handoff = supervisor._run_native_v5_campaign_round(
        runtime_authority={"opaque": "runtime"},
        config=config,
        generation_index=2,
        evaluation_population_path=evaluation,
        campaign_root=tmp_path / "campaign",
        panel=panel,
        campaign_role="proposal_current_panel",
        evaluation_population_raw_sha256=_sha("evaluation"),
        cohort_source={
            "kind": "proposal_evaluation_population",
            "sourceSemanticSha256": _sha("evaluation"),
            "candidateCount": 1,
            "selectionSha256": None,
        },
        gateway_token=None,
    )

    assert calls == ["freeze", "dispatch", "output"]
    assert handoff["campaignReceipt"]["receiptPath"].endswith(
        "campaign-output-checkpoint.json"
    )
    assert "sourceBuild" not in handoff
    assert "campaignSeal" not in handoff
    assert "panelBundleSidecar" not in handoff


def test_current_v5_postproposal_rejects_missing_native_authority_without_python_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No partial Python campaign may start when a native receipt lacks authority."""

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("current v5 tried to revive a retired Python postproposal loop")

    for name in (
        "_run_native_v5_campaign_round",
        "freeze_qd_screening_campaign",
        "run_temporal_search_tasks",
        "build_qd_archive",
        "generate_qd_generation",
    ):
        monkeypatch.setattr(supervisor, name, forbidden)

    with pytest.raises(TemporalDiscoveryContractError, match="frozen rotating authorities"):
        supervisor._complete_native_v5_generation(
            root=tmp_path,
            state={},
            state_path=tmp_path / "state.json",
            config={"nativeV5ProposalRuntime": {}},
            generation_index=1,
            generation_result={"generationKind": supervisor.V5_PROPOSAL_GENERATION_G0},
            identity_ledger_input=None,
            parent_archive_descriptor={},
            previous_cumulative_archive_descriptor=None,
            gateway_token=None,
        )


@pytest.mark.parametrize(
    ("generation_kind", "input_ledger_sha256", "expected_extractor"),
    (
        (supervisor.V5_PROPOSAL_GENERATION_G0, None, "g0"),
        (
            supervisor.V5_PROPOSAL_GENERATION_EVOLVED,
            _sha("committed-input-ledger"),
            "evolved",
        ),
    ),
)
def test_current_v5_postproposal_chain_uses_only_native_receipts_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    generation_kind: str,
    input_ledger_sha256: str | None,
    expected_extractor: str,
) -> None:
    """Exercise the common Rust-only postproposal splice without row payloads.

    The native subprocess bridges are intentionally mocked at their receipt
    boundaries.  This verifies the supervisor carries the exact compact
    authority, Rust selection descriptor, and resume receipts through the
    chain without reviving any historic Python campaign/archive/finalizer
    loop.
    """

    root = (tmp_path / generation_kind).resolve()
    root.mkdir()
    generation_index = 1 if generation_kind == supervisor.V5_PROPOSAL_GENERATION_G0 else 2
    config_sha256 = _sha(f"{generation_kind}-config")
    panel = {"panelId": "panel-current", "windows": []}
    rotating = {
        "rotatingEvidenceSha256": _sha(f"{generation_kind}-rotating"),
        "panels": [panel],
    }
    config = {
        "nativeV5ProposalRuntime": {},
        "configSha256": config_sha256,
        "rotatingEvidence": rotating,
        "frozenSearchPolicy": {
            "minimumTotalTrades": 1,
            "minimumTradesPerWindow": 1,
        },
    }
    state: dict[str, Any] = {
        "schemaVersion": supervisor.SUPERVISOR_STATE_SCHEMA,
        "configSha256": config_sha256,
        "status": "running",
        "stage": "generation_proposal",
        "currentGenerationIndex": generation_index,
        "uniqueCandidatesEvaluated": 0,
        "workerTasksCompleted": 0,
        "nextImmigrantContinuationOrdinal": 0,
        "uniqueIdentityCounts": {},
        "duplicateCounters": {},
        "proposalSlotCounters": {},
        "completedGenerations": [],
    }
    state_path = root / "state.json"
    supervisor._save_state(state_path, state)
    evaluation_population = root / "proposal" / "evaluation-population.json"
    evaluation_population.parent.mkdir(parents=True)
    evaluation_population.write_text("{}\n", encoding="utf-8")
    parent_archive = root / "parent-archive.json"
    parent_archive.write_text("{}\n", encoding="utf-8")
    selection_path = root / "prefinalizer-selection.selection.json"
    selection_path.write_text('{"native":"selection"}\n', encoding="utf-8")

    output_ledger_sha256 = _sha(f"{generation_kind}-output-ledger")
    adapter = {
        "evaluationPopulation": {
            "absolutePath": str(evaluation_population),
            "semanticSha256": _sha(f"{generation_kind}-evaluation-population"),
            "fileSha256": _sha(f"{generation_kind}-evaluation-population-file"),
        },
        "selectedEvaluationCandidateCount": 1,
        "generationConfigSha256": _sha(f"{generation_kind}-generation-config"),
        "identityLedger": {
            "semanticSha256": output_ledger_sha256,
            "fileSha256": _sha(f"{generation_kind}-output-ledger-file"),
        },
        "nativeV5Invocation": {
            "schemaVersion": (
                supervisor.V5_G0_NATIVE_V5_INVOCATION_SCHEMA
                if generation_kind == supervisor.V5_PROPOSAL_GENERATION_G0
                else supervisor.V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA
            ),
            "proposalManifest": {"semanticSha256": _sha("proposal-manifest")},
            "proposalResult": {"semanticSha256": _sha("proposal-result")},
            "proposalReceiptSha256": _sha("proposal-receipt"),
            "outputInventorySha256": _sha("output-inventory"),
        },
    }
    proposal_state_authority = {
        "generationKind": generation_kind,
        "proposalManifestSha256": _sha("proposal-manifest"),
        "proposalReceiptSha256": _sha("proposal-receipt"),
        "generationJournalSha256": _sha("proposal-journal"),
        "inputIdentityLedgerSha256": input_ledger_sha256,
        "outputIdentityLedgerRelativePath": "proposal/v5-native/identity-ledger.json",
        "outputIdentityLedgerSha256": output_ledger_sha256,
        "outputIdentityLedgerFileSha256": adapter["identityLedger"]["fileSha256"],
    }
    proposal_semantic_roots = {
        "proposalReceiptSha256": proposal_state_authority["proposalReceiptSha256"],
        "generationJournalSha256": proposal_state_authority[
            "generationJournalSha256"
        ],
    }
    calls: list[str] = []
    base_calls: list[dict[str, Any]] = []
    resume_calls: list[dict[str, Any]] = []
    finalizer_calls: list[dict[str, Any]] = []
    applied: list[dict[str, Any]] = []
    campaign_receipt_paths: list[Path] = []

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("current v5 postproposal chain reached a retired Python loop")

    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", forbidden)
    monkeypatch.setattr(supervisor, "run_temporal_search_tasks", forbidden)
    monkeypatch.setattr(supervisor, "build_qd_archive", forbidden)
    monkeypatch.setattr(supervisor, "generate_qd_generation", forbidden)
    monkeypatch.setattr(supervisor, "panel_for_generation", lambda *_args: panel)
    monkeypatch.setattr(
        supervisor,
        "_v5_evolvable_authority",
        lambda _config: {
            "archivePolicyAuthority": qd.directional_qd_archive_policy_authority()
        },
    )
    monkeypatch.setattr(
        supervisor,
        "_native_runtime_authority_for_generation",
        lambda **_kwargs: {"opaque": "runtime"},
    )
    monkeypatch.setattr(
        supervisor, "_require_native_v5_control_plane_runtime_authority", lambda value: value
    )
    monkeypatch.setattr(
        supervisor,
        "_native_v5_proposal_state_authority_for_generation",
        lambda **_kwargs: (adapter, proposal_state_authority, proposal_semantic_roots),
    )
    def campaign(**kwargs: Any) -> dict[str, Any]:
        calls.append("campaign:" + str(kwargs["campaign_role"]))
        if kwargs["campaign_role"] == "proposal_current_panel":
            assert kwargs.get("cohort_selection_path") is None
        else:
            assert kwargs["campaign_role"] == "retained_parent_current_panel"
            assert kwargs["cohort_selection_path"] == selection_path
        receipt_path = root / f"{len(calls)}-campaign-receipt.json"
        campaign_receipt_paths.append(receipt_path)
        return {
            "campaignOutput": {
                "result": {
                    "campaignSeal": {"campaignSealSha256": _sha("campaign-seal")},
                    "directionalTailAuthority": {
                        "receiptPath": str(root / "directional-tail-authority.json"),
                        "receiptSha256": _sha("tail-authority"),
                    },
                    "tailResultIndex": {"tailResultIndexSha256": _sha("tail-index")},
                },
                "checkpointPath": str(receipt_path),
            },
            "campaignReceipt": {"receiptPath": str(receipt_path)},
        }

    def g0_extractor(**kwargs: Any) -> dict[str, Any]:
        calls.append("extract:g0")
        assert kwargs["construction_adapter"] is adapter
        return {"proposalAttemptAuthority": {"kind": "g0"}}

    def evolved_extractor(**kwargs: Any) -> dict[str, Any]:
        calls.append("extract:evolved")
        assert kwargs["construction_adapter"] is adapter
        return {"proposalAttemptAuthority": {"kind": "evolved"}}

    def funnel(**kwargs: Any) -> dict[str, Any]:
        calls.append("funnel")
        assert kwargs["proposal_attempt_authority"] == {"kind": expected_extractor}
        assert kwargs["campaign_seal"] == {"campaignSealSha256": _sha("campaign-seal")}
        return {
            "input": {"inputSha256": _sha("funnel-input")},
            "assemblyReceiptBinding": {
                "schemaVersion": control.FUNNEL_ASSEMBLY_RECEIPT_BINDING_SCHEMA,
                "path": str(root / "funnel-assembly-receipt.json"),
                "rawSha256": _sha("funnel-assembly-receipt-file"),
                "sizeBytes": 1,
                "receiptSha256": _sha("funnel-assembly-receipt"),
            },
        }

    def base(**kwargs: Any) -> dict[str, Any]:
        calls.append("base")
        base_calls.append(kwargs)
        assert Path(kwargs["output_root"]).name == "base-v2"
        assert kwargs["finalizer_output_root"] == supervisor._native_finalization_root(
            root, generation_index
        )
        return {"manifestPath": str(root / "base-manifest.json")}

    def prefinalizer(**_kwargs: Any) -> dict[str, Any]:
        if not any(call.startswith("prefinalizer:") for call in calls):
            calls.append("prefinalizer:awaiting")
            return {
                "receipt": {
                    "status": "awaiting_retained_parent_current_panel",
                    "roundIndex": 0,
                },
                "taskSelections": (
                    {
                        "taskOrdinal": 0,
                        "campaignRole": "retained_parent_current_panel",
                        "panelId": panel["panelId"],
                        "candidateSetSha256": _sha("candidate-set"),
                        "candidateCount": 1,
                        "selectionSha256": _sha("selection"),
                        "selectionPath": str(selection_path),
                    },
                ),
                "resultPath": str(root / "prefinalizer-awaiting-result.json"),
                "outputRoot": str(root / "prefinalizer-awaiting"),
            }
        calls.append("prefinalizer:ready")
        finalizer_manifest_path = str(
            supervisor._native_finalization_root(root, generation_index)
            / "manifest.json"
        )
        if os.name == "nt":
            finalizer_manifest_path = "\\\\?\\" + finalizer_manifest_path
        return {
            "receipt": {
                "status": "ready_for_finalizer",
                "roundIndex": 1,
                "finalizerManifest": {
                    "schemaVersion": "temporal_qd_v5_prefinalizer_finalizer_manifest_descriptor_v1",
                    "path": finalizer_manifest_path,
                    "rawSha256": _sha("finalizer-manifest-file"),
                    "sizeBytes": 1,
                    "manifestSha256": _sha("finalizer-manifest"),
                },
            },
            "taskSelections": (),
            "resultPath": str(root / "prefinalizer-ready-result.json"),
            "outputRoot": str(root / "prefinalizer-ready"),
        }

    def resume(**kwargs: Any) -> dict[str, Any]:
        calls.append("resume")
        resume_calls.append(kwargs)
        return {"manifestPath": str(root / "resume-manifest.json")}

    def finalizer(**kwargs: Any) -> dict[str, Any]:
        calls.append("finalizer")
        finalizer_calls.append(kwargs)
        return {"opaque": "finalization"}

    record = {"schemaVersion": control.GENERATION_RECORD_SCHEMA, "fixture": generation_kind}

    def apply(**kwargs: Any) -> dict[str, Any]:
        calls.append("apply")
        applied.append(kwargs)
        return record

    monkeypatch.setattr(supervisor, "_run_native_v5_campaign_round", campaign)
    monkeypatch.setattr(supervisor, "extract_native_v5_g0_selected_attempts", g0_extractor)
    monkeypatch.setattr(
        supervisor, "extract_native_v5_evolved_attempt_chain", evolved_extractor
    )
    monkeypatch.setattr(supervisor, "assemble_native_v5_funnel_reduction_source", funnel)
    monkeypatch.setattr(supervisor, "build_native_v5_prefinalizer_base_manifest", base)
    monkeypatch.setattr(supervisor, "run_native_v5_rotating_prefinalizer", prefinalizer)
    monkeypatch.setattr(
        supervisor, "build_native_v5_prefinalizer_resume_manifest", resume
    )
    monkeypatch.setattr(supervisor, "run_native_v5_generation_finalizer", finalizer)
    monkeypatch.setattr(supervisor, "_apply_native_v5_state_application", apply)

    result = supervisor._complete_native_v5_generation(
        root=root,
        state=state,
        state_path=state_path,
        config=config,
        generation_index=generation_index,
        generation_result={"generationKind": generation_kind},
        identity_ledger_input=(
            None
            if input_ledger_sha256 is None
            else {"semanticSha256": input_ledger_sha256}
        ),
        parent_archive_descriptor={
            "absolutePath": str(parent_archive.resolve()),
            "fileSha256": _sha("parent-archive-file"),
            "semanticSha256": _sha("parent-archive"),
            "byteLength": parent_archive.stat().st_size,
        },
        previous_cumulative_archive_descriptor=None,
        gateway_token=None,
    )

    assert result["generationRecord"] == record
    assert calls == [
        "campaign:proposal_current_panel",
        f"extract:{expected_extractor}",
        "funnel",
        "base",
        "prefinalizer:awaiting",
        "campaign:retained_parent_current_panel",
        "resume",
        "prefinalizer:ready",
        "finalizer",
        "apply",
    ]
    assert len(base_calls) == 1
    assert base_calls[0]["completed_generation_records"] == []
    assert base_calls[0]["proposal_state_authority"] == proposal_state_authority
    assert base_calls[0]["proposal_semantic_roots"] == proposal_semantic_roots
    assert base_calls[0]["native_v5_invocation"] == adapter["nativeV5Invocation"]
    assert base_calls[0]["funnel_reduction_input"] == {
        "inputSha256": _sha("funnel-input")
    }
    assert base_calls[0]["funnel_assembly_receipt_binding"] == {
        "schemaVersion": control.FUNNEL_ASSEMBLY_RECEIPT_BINDING_SCHEMA,
        "path": str(root / "funnel-assembly-receipt.json"),
        "rawSha256": _sha("funnel-assembly-receipt-file"),
        "sizeBytes": 1,
        "receiptSha256": _sha("funnel-assembly-receipt"),
    }
    assert base_calls[0]["archive_policy_authority"]["schemaVersion"] == (
        "temporal_qd_archive_policy_binding_v1"
    )
    assert base_calls[0]["archive_policy_authority"]["policyBindingSha256"] == (
        canonical_sha256(
            {
                key: value
                for key, value in base_calls[0]["archive_policy_authority"].items()
                if key != "policyBindingSha256"
            }
        )
    )
    assert resume_calls[0]["new_campaign_receipt_paths"] == (
        campaign_receipt_paths[1],
    )
    assert control.native_v5_transport_path_matches(
        str(finalizer_calls[0]["manifest_path"]),
        supervisor._native_finalization_root(root, generation_index) / "manifest.json",
    )
    assert applied[0]["construction_adapter"] is adapter
    assert applied[0]["generation_kind"] == generation_kind


def test_v5_runtime_authority_freezes_every_native_control_plane_role(
    tmp_path: Path,
) -> None:
    """A current v5 run has no implicit binary lookup or Python escape hatch."""

    suffix = ".exe" if supervisor.os.name == "nt" else ""
    binaries = {
        "campaignFreeze": tmp_path / f"temporal-qd-campaign-freeze{suffix}",
        "gatewayDispatch": tmp_path / f"temporal-qd-gateway-dispatch{suffix}",
        "campaignSeal": tmp_path / f"temporal-qd-campaign-seal{suffix}",
        "tailReducer": tmp_path / f"temporal-qd-tail-reducer{suffix}",
        "rotatingPrefinalizer": tmp_path / f"temporal-qd-rotating-prefinalizer{suffix}",
        "generationFinalizer": tmp_path / f"temporal-qd-generation-finalizer{suffix}",
        "archiveReducer": tmp_path / f"temporal-qd-archive-reducer{suffix}",
    }
    for role, binary in binaries.items():
        binary.write_bytes(role.encode("utf-8"))

    authority = supervisor._freeze_native_finalization_runtime_authority(
        root=tmp_path / "run",
        finalizer_binary=binaries["generationFinalizer"],
        state={"completedGenerations": []},
        require_v5_control_plane_roles=True,
    )
    assert set(authority["binaries"]) == supervisor.NATIVE_V5_CONTROL_PLANE_RUNTIME_ROLES
    assert (
        supervisor._require_native_v5_control_plane_runtime_authority(authority)
        == authority
    )
    for role, binary in binaries.items():
        assert (
            supervisor._pinned_native_authority_binary(
                root=tmp_path / "run",
                authority_sha256=authority["authoritySha256"],
                role=role,
            )
            == binary.resolve()
        )

    binaries["gatewayDispatch"].unlink()
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="gatewayDispatch binary is unavailable",
    ):
        supervisor._native_finalization_runtime_authority(
            binaries["generationFinalizer"],
            require_v5_control_plane_roles=True,
        )


def test_legacy_generate_cli_rejects_directional_v5_before_opening_pair_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    parent_archive = tmp_path / "directional-parent.json"
    _write_json(parent_archive, qd.canonical_empty_directional_bidirectional_archive_template())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "temporal_qd_evolution",
            "generate",
            "--parent-archive",
            str(parent_archive),
            "--output-root",
            str(tmp_path / "proposal"),
            "--generation-index",
            "1",
            "--bidirectional-pair-config",
            str(tmp_path / "never-opened-pair-authority.json"),
        ],
    )

    with pytest.raises(SystemExit) as raised:
        qd.main()

    assert raised.value.code == 2
    assert "Rust-native supervisor transaction" in capsys.readouterr().err


def test_native_v5_g0_and_evolved_construction_adopt_without_python_fallbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs, _fixture = _fresh_v5_fixture(tmp_path)
    config, _warnings = supervisor._frozen_config(**inputs)
    root = tmp_path / "run"
    root.mkdir()
    state = _state(config)
    state_path = root / "state.json"
    calls: list[dict[str, Any]] = []

    def native_bridge(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return _native_adapter_for_call(**kwargs)

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("retired Python/Dashboard construction surface was called")

    monkeypatch.setattr(supervisor, "run_native_v5_generation_construction", native_bridge)
    monkeypatch.setattr(supervisor, "PairAuthorityBundle", forbidden)
    monkeypatch.setattr(
        evolvable_qd_authority,
        "open_evolvable_module_pair_authority",
        forbidden,
    )
    monkeypatch.setattr(supervisor, "generate_qd_generation", forbidden)
    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", forbidden)
    monkeypatch.setattr(supervisor, "build_qd_archive", forbidden)

    # G0 has no parent or campaign-ledger input.  The next generation carries
    # its immutable proposal-output descriptor directly; Python neither
    # snapshots nor promotes the candidate-scale ledger bytes.
    g0_input = supervisor._build_native_v5_identity_ledger_input(
        generation_kind=supervisor.V5_PROPOSAL_GENERATION_G0,
        committed_identity_ledger_descriptor=None,
    )
    assert g0_input is None
    g0 = supervisor._run_native_v5_generation(
        root=root,
        config=config,
        generation_index=1,
        parent_archive_descriptor=_sealed_archive_descriptor(
            Path(inputs["initial_archive_path"]),
            semantic_sha256=config["initialArchive"]["archiveSha256"],
        ),
        parent_schedule=None,
        identity_ledger_input=g0_input,
    )
    assert g0["generationKind"] == supervisor.V5_PROPOSAL_GENERATION_G0
    assert calls[-1]["parent_archive_input"] is None
    assert calls[-1]["identity_ledger_input"] is None
    native_generation_config = calls[-1]["generation_config"]
    assert "reproductionAllocation" in native_generation_config
    assert (
        native_generation_config["operatorImplementation"]
        == config["bidirectionalPairGeneration"]["operatorImplementation"]
    )
    assert (
        native_generation_config["runConfig"]["archivePolicyAuthority"]
        == config["evolvableModuleAuthority"]["archivePolicyAuthority"]
    )
    assert (
        native_generation_config["runConfig"]["behaviorAttributionRequirement"]
        == config["evolvableModuleAuthority"]["behaviorAttributionRequirement"]
    )
    assert (
        native_generation_config["runConfig"].get("capacityReceipt")
        == config["bidirectionalPairGeneration"].get("capacityReceipt")
    )
    forged_operation = dict(g0["nativeV5Construction"])
    forged_operation["operation"] = "python_v5_fallback"
    forged_operation["adapterSha256"] = canonical_sha256(
        {
            key: value
            for key, value in forged_operation.items()
            if key != "adapterSha256"
        }
    )
    with pytest.raises(TemporalDiscoveryContractError, match="adapter binding drifted"):
        supervisor._validate_native_v5_construction_adapter(
            value=forged_operation,
            proposal_root=root / "generations" / "generation-0001" / "proposal",
            generation_index=1,
            generation_kind=supervisor.V5_PROPOSAL_GENERATION_G0,
            generation_config_sha256=g0["nativeV5Invocation"]["generationConfigSha256"],
        )
    legacy_adapter = deepcopy(g0["nativeV5Construction"])
    legacy_adapter["schemaVersion"] = (
        "temporal_qd_native_v5_generation_construction_adapter_v2"
    )
    legacy_adapter["adapterSha256"] = canonical_sha256(
        {key: value for key, value in legacy_adapter.items() if key != "adapterSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="adapter binding drifted"):
        supervisor._validate_native_v5_construction_adapter(
            value=legacy_adapter,
            proposal_root=root / "generations" / "generation-0001" / "proposal",
            generation_index=1,
            generation_kind=supervisor.V5_PROPOSAL_GENERATION_G0,
            generation_config_sha256=g0["nativeV5Invocation"]["generationConfigSha256"],
        )
    tampered_projection = deepcopy(g0["nativeV5Construction"])
    tampered_projection["g0FunnelProjectionStream"]["stream"]["relativePath"] = (
        "v5-native/alternate-projections.jsonl"
    )
    tampered_projection["adapterSha256"] = canonical_sha256(
        {
            key: value
            for key, value in tampered_projection.items()
            if key != "adapterSha256"
        }
    )
    with pytest.raises(TemporalDiscoveryContractError, match="G0 funnel projection"):
        supervisor._validate_native_v5_construction_adapter(
            value=tampered_projection,
            proposal_root=root / "generations" / "generation-0001" / "proposal",
            generation_index=1,
            generation_kind=supervisor.V5_PROPOSAL_GENERATION_G0,
            generation_config_sha256=g0["nativeV5Invocation"]["generationConfigSha256"],
        )

    ledger_descriptor = supervisor._native_v5_identity_ledger_descriptor_from_adapter(
        adapter=g0["nativeV5Construction"],
        root=root,
        generation_index=1,
        name="test G0 immutable identity ledger",
    )
    state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] = ledger_descriptor
    g0_record = {
        "generationIndex": 1,
        "nativeV5Construction": g0["nativeV5Construction"],
        "nativeV5Invocation": g0["nativeV5Invocation"],
    }
    state["completedGenerations"] = [g0_record]

    # A restart must call the same native bridge in receipt-adoption mode; it
    # cannot trust a mutable Python reconstruction of G0.
    assert supervisor._reauthenticate_native_v5_supervisor_invocation(
        root=root, config=config, invocation=g0_record["nativeV5Invocation"]
    ) == g0["nativeV5Construction"]
    assert len(calls) == 2

    parent = root / "generations" / "generation-0001" / "archive.json"
    parent.write_text('{"parent":"opaque native input"}\n', encoding="utf-8")
    ledger_path = Path(ledger_descriptor["absolutePath"])
    original_read_bytes = Path.read_bytes

    def no_candidate_ledger_read(path: Path, *args: Any, **kwargs: Any) -> bytes:
        if path == ledger_path:
            pytest.fail("current v5 reopened its committed identity ledger")
        return original_read_bytes(path, *args, **kwargs)

    original_hash = supervisor._native_binary_file_sha256

    def no_candidate_ledger_hash(path: Path) -> str:
        if path == ledger_path:
            pytest.fail("current v5 rehashed its committed identity ledger")
        return original_hash(path)

    monkeypatch.setattr(Path, "read_bytes", no_candidate_ledger_read)
    monkeypatch.setattr(supervisor, "_native_binary_file_sha256", no_candidate_ledger_hash)
    evolved_input = supervisor._build_native_v5_identity_ledger_input(
        generation_kind=supervisor.V5_PROPOSAL_GENERATION_EVOLVED,
        committed_identity_ledger_descriptor=ledger_descriptor,
    )
    assert evolved_input is not None
    evolved = supervisor._run_native_v5_generation(
        root=root,
        config=config,
        generation_index=2,
        parent_archive_descriptor=_sealed_archive_descriptor(
            parent,
            semantic_sha256=_sha("opaque parent archive semantic root"),
            byte_length=parent.stat().st_size,
        ),
        parent_schedule=None,
        identity_ledger_input=evolved_input,
    )
    assert evolved["generationKind"] == supervisor.V5_PROPOSAL_GENERATION_EVOLVED
    assert calls[-1]["parent_archive_input"]["kind"] == "parentArchive"
    assert calls[-1]["identity_ledger_input"]["kind"] == "identityLedger"
    assert calls[-1]["identity_ledger_input"]["absolutePath"] == str(ledger_path)


def test_small_real_native_v5_g0_construction_and_adoption_never_use_python_fallbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run Rust fresh + receipt adoption through the supervisor's v5 seam."""

    config = _compact_real_native_v5_config()
    root = tmp_path / "real-native-run"
    root.mkdir()
    state = _state(config)
    state_path = root / "state.json"

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("retired Python/Dashboard construction or finalizer was called")

    monkeypatch.setattr(supervisor, "PairAuthorityBundle", forbidden)
    monkeypatch.setattr(supervisor, "generate_qd_generation", forbidden)
    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", forbidden)
    monkeypatch.setattr(supervisor, "build_qd_archive", forbidden)
    monkeypatch.setattr(
        supervisor, "_complete_rotating_generation_transaction", forbidden
    )

    g0_input = supervisor._build_native_v5_identity_ledger_input(
        generation_kind=supervisor.V5_PROPOSAL_GENERATION_G0,
        committed_identity_ledger_descriptor=None,
    )
    assert g0_input is None
    fresh = supervisor._run_native_v5_generation(
        root=root,
        config=config,
        generation_index=1,
        parent_archive_descriptor=_sealed_archive_descriptor(
            root / "unused-g0-parent.json",
            semantic_sha256=_sha("unused-g0-parent"),
        ),
        parent_schedule=None,
        identity_ledger_input=None,
    )

    adapter = fresh["nativeV5Construction"]
    assert fresh["generationKind"] == supervisor.V5_PROPOSAL_GENERATION_G0
    assert adapter["attemptCount"] == 2
    assert adapter["acceptedCandidateCount"] == 2
    assert adapter["selectedEvaluationCandidateCount"] == 1
    native_generation_config = fresh["nativeV5Invocation"]["generationConfig"]
    assert "reproductionAllocation" in native_generation_config
    assert native_generation_config["operatorImplementation"] == config[
        "bidirectionalPairGeneration"
    ]["operatorImplementation"]
    assert native_generation_config["runConfig"]["archivePolicyAuthority"] == config[
        "evolvableModuleAuthority"
    ]["archivePolicyAuthority"]
    assert native_generation_config["runConfig"]["behaviorAttributionRequirement"] == config[
        "evolvableModuleAuthority"
    ]["behaviorAttributionRequirement"]
    assert native_generation_config["runConfig"]["capacityReceipt"] == config[
        "bidirectionalPairGeneration"
    ]["capacityReceipt"]
    assert Path(adapter["population"]["absolutePath"]).is_file()

    # The second call asks Rust to authenticate/adopt the receipt-sealed tree;
    # it cannot rebuild through any of the monkeypatched Python surfaces.
    assert supervisor._reauthenticate_native_v5_supervisor_invocation(
        root=root, config=config, invocation=fresh["nativeV5Invocation"]
    ) == adapter

    descriptor = supervisor._native_v5_identity_ledger_descriptor_from_adapter(
        adapter=adapter,
        root=root,
        generation_index=1,
        name="real native v5 G0 ledger descriptor",
    )
    state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] = descriptor
    assert descriptor["semanticSha256"] == adapter["identityLedger"]["semanticSha256"]
    assert not (root / "identity-ledger.json").exists()


@pytest.mark.skipif(
    supervisor.os.name != "nt", reason="Windows archive-certifier transport ABI"
)
def test_real_release_archive_certifier_preserves_windows_extended_transport(
    tmp_path: Path,
) -> None:
    """The checked-in release certifier returns its self-hashed ``\\\\?\\`` path."""

    release_root = (
        Path(__file__).resolve().parents[1] / "rust" / "temporal-qd" / "target" / "release"
    )
    finalizer = release_root / "temporal-qd-generation-finalizer.exe"
    required = (
        "temporal-qd-campaign-freeze.exe",
        "temporal-qd-gateway-dispatch.exe",
        "temporal-qd-campaign-seal.exe",
        "temporal-qd-tail-reducer.exe",
        "temporal-qd-rotating-prefinalizer.exe",
        "temporal-qd-generation-finalizer.exe",
        "temporal-qd-archive-reducer.exe",
    )
    if not all((release_root / name).is_file() for name in required):
        pytest.skip("native v5 release binaries are unavailable")

    runtime = supervisor._native_finalization_runtime_authority(
        finalizer, require_v5_control_plane_roles=True
    )
    archive = (tmp_path / "initial-archive.json").resolve()
    _write_json(archive, qd.canonical_empty_directional_bidirectional_archive_template())
    descriptor = control.certify_native_v5_initial_archive(
        runtime_authority=runtime, archive_path=archive
    )

    assert descriptor["absolutePath"] == "\\\\?\\" + str(archive)
    assert control.native_v5_archive_transport_path_matches(
        descriptor["absolutePath"], archive
    )
    assert descriptor["descriptorSha256"] == canonical_sha256(
        {key: value for key, value in descriptor.items() if key != "descriptorSha256"}
    )


def _direct_v5_state_application_fixture(
    tmp_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, Any], Path, dict[str, Any]]:
    """Build only compact state/ledger receipts; no candidate payload exists."""

    root = (tmp_path / "direct-v5-state").resolve()
    root.mkdir()
    config_sha = _sha("direct-v5-config")
    runtime_sha = _sha("direct-v5-runtime")
    semantic_sha = _sha("direct-v5-semantic")
    state: dict[str, Any] = {
        "schemaVersion": supervisor.SUPERVISOR_STATE_SCHEMA,
        "configSha256": config_sha,
        "status": "running",
        "stage": "finalizing",
        "currentGenerationIndex": 2,
        "uniqueCandidatesEvaluated": 1,
        "workerTasksCompleted": 2,
        "nextImmigrantContinuationOrdinal": 0,
        "uniqueIdentityCounts": {"accepted": 1},
        "duplicateCounters": {"duplicate": 0},
        "proposalSlotCounters": {"attempt": 1},
        "completedGenerations": [],
    }
    config = {"configSha256": config_sha}
    state_basis = supervisor._native_v5_generation_state_basis(
        state=state, config=config, generation_index=2
    )

    input_ledger = {"schemaVersion": "fixture-v5-ledger", "generationIndex": 1}
    input_ledger["identityLedgerSha256"] = canonical_sha256(input_ledger)
    output_ledger = {"schemaVersion": "fixture-v5-ledger", "generationIndex": 2}
    output_ledger["identityLedgerSha256"] = canonical_sha256(output_ledger)
    output_ledger_path = (
        root
        / "generations"
        / "generation-0002"
        / "proposal"
        / "v5-native"
        / "identity-ledger.json"
    )
    output_file_sha, output_bytes = _write_json(output_ledger_path, output_ledger)
    state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] = {
        "absolutePath": str(
            (
                root
                / "generations"
                / "generation-0001"
                / "proposal"
                / "v5-native"
                / "identity-ledger.json"
            ).resolve()
        ),
        "semanticSha256": input_ledger["identityLedgerSha256"],
        "fileSha256": _sha("fixture-input-ledger-file"),
        "byteLength": 0,
    }

    record = {
        "schemaVersion": control.GENERATION_RECORD_SCHEMA,
        "generationIndex": 2,
        "candidateCount": 1,
        "totalGenerationTaskCount": 2,
        "archivePath": "archive.json",
    }
    record["generationRecordSha256"] = canonical_sha256(record)
    patch = {
        "schemaVersion": control.GENERATION_STATE_PATCH_SCHEMA,
        "stateBasisSha256": state_basis["stateBasisSha256"],
        "generationIndex": 2,
        "nextGenerationIndex": 3,
        "nextStage": "generation_proposal",
        "uniqueCandidatesEvaluated": 7,
        "workerTasksCompleted": 11,
        "nextImmigrantContinuationOrdinal": 5,
        "uniqueIdentityCounts": {"accepted": 7},
        "duplicateCounters": {"duplicate": 3},
        "proposalSlotCounters": {"attempt": 9},
        "completedGenerationsSha256": canonical_sha256([record]),
        "generationRecordSha256": record["generationRecordSha256"],
        "generationRecord": record,
        "semanticAuthoritySha256": semantic_sha,
        "runtimeAuthoritySha256": runtime_sha,
    }
    patch["statePatchSha256"] = canonical_sha256(patch)
    source = {"sourceSha256": _sha("direct-v5-source")}
    manifest = {
        "manifestSha256": _sha("direct-v5-manifest"),
        "sourceSha256": source["sourceSha256"],
        "semanticAuthoritySha256": semantic_sha,
        "runtimeAuthoritySha256": runtime_sha,
    }
    commit = {
        "commitSha256": _sha("direct-v5-commit"),
        "sourceSha256": source["sourceSha256"],
    }
    sidecar = {
        "schemaVersion": control.GENERATION_STATE_APPLICATION_SIDECAR_SCHEMA,
        "contractVersion": supervisor.NATIVE_FOUNDATION_CONTRACT_VERSION,
        "generationIndex": 2,
        "generationKind": supervisor.V5_PROPOSAL_GENERATION_EVOLVED,
        "configSha256": config_sha,
        "stateBasisSha256": state_basis["stateBasisSha256"],
        "completedGenerationsBeforeSha256": state_basis[
            "completedGenerationsSha256"
        ],
        "semanticAuthoritySha256": semantic_sha,
        "runtimeAuthoritySha256": runtime_sha,
        "finalization": {
            "sourceSha256": source["sourceSha256"],
            "manifestSha256": manifest["manifestSha256"],
            "commitSha256": commit["commitSha256"],
            "generationRecordSha256": record["generationRecordSha256"],
            "statePatchSha256": patch["statePatchSha256"],
        },
        "proposalStateAuthority": {
            "proposalManifestSha256": _sha("proposal-manifest"),
            "proposalReceiptSha256": _sha("proposal-receipt"),
            "generationJournalSha256": _sha("proposal-journal"),
        },
        "nextState": {
            "stage": "generation_proposal",
            "currentGenerationIndex": 3,
            "uniqueCandidatesEvaluated": 7,
            "workerTasksCompleted": 11,
            "nextImmigrantContinuationOrdinal": 5,
            "uniqueIdentityCounts": {"accepted": 7},
            "duplicateCounters": {"duplicate": 3},
            "proposalSlotCounters": {"attempt": 9},
            "completedGenerationsSha256": patch["completedGenerationsSha256"],
        },
        "identityLedgerPromotion": {
            "inputIdentityLedgerSha256": input_ledger["identityLedgerSha256"],
            "outputRelativePath": "proposal/v5-native/identity-ledger.json",
            "outputIdentityLedgerSha256": output_ledger["identityLedgerSha256"],
            "outputIdentityLedgerFileSha256": output_file_sha,
        },
    }
    sidecar["sidecarSha256"] = canonical_sha256(sidecar)
    finalization = {
        "generationRecord": record,
        "statePatch": patch,
        "stateApplicationSidecar": sidecar,
        "sourceSha256": source["sourceSha256"],
        "manifest": manifest,
        "commit": commit,
        "identityLedgerDescriptor": {
            "absolutePath": str(output_ledger_path.resolve()),
            "semanticSha256": output_ledger["identityLedgerSha256"],
            "fileSha256": output_file_sha,
            "byteLength": output_bytes,
        },
    }
    state_path = root / "state.json"
    supervisor._save_state(state_path, state)
    return root, config, state, state_path, finalization


def _apply_direct_v5_state_fixture(
    *,
    root: Path,
    config: dict[str, Any],
    state: dict[str, Any],
    state_path: Path,
    finalization: dict[str, Any],
) -> dict[str, Any]:
    if supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY not in state:
        sidecar = finalization["stateApplicationSidecar"]
        state[supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY] = {
            "generationIndex": 2,
            "sidecarSha256": sidecar["sidecarSha256"],
            "identityLedger": finalization["identityLedgerDescriptor"],
            "phase": "pending",
        }
        supervisor._save_state(state_path, state)
    return supervisor._apply_native_v5_state_application(
        root=root,
        state=state,
        state_path=state_path,
        config=config,
        generation_index=2,
        generation_kind=supervisor.V5_PROPOSAL_GENERATION_EVOLVED,
        finalization=finalization,
        construction_adapter=None,
    )


def test_direct_v5_state_application_uses_absolute_rust_values_once(
    tmp_path: Path,
) -> None:
    root, config, state, state_path, finalization = _direct_v5_state_application_fixture(
        tmp_path
    )
    record = _apply_direct_v5_state_fixture(
        root=root,
        config=config,
        state=state,
        state_path=state_path,
        finalization=finalization,
    )

    assert state["completedGenerations"] == [record]
    assert record["schemaVersion"] == control.GENERATION_RECORD_SCHEMA
    assert "nativeV5Construction" not in record
    assert "nativeGenerationFinalization" not in record
    assert state["uniqueCandidatesEvaluated"] == 7
    assert state["workerTasksCompleted"] == 11
    assert state["nextImmigrantContinuationOrdinal"] == 5
    assert supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY not in state
    assert "nativeV5IdentityLedgerTransaction" not in state
    assert state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] == finalization[
        "identityLedgerDescriptor"
    ]
    assert not (root / "identity-ledger.json").exists()

    with pytest.raises(TemporalDiscoveryContractError, match="state basis|duplicate"):
        _apply_direct_v5_state_fixture(
            root=root,
            config=config,
            state=state,
            state_path=state_path,
            finalization=finalization,
        )
    assert state["uniqueCandidatesEvaluated"] == 7
    assert state["workerTasksCompleted"] == 11


@pytest.mark.parametrize("phase", ("before_promotion", "after_promotion"))
def test_direct_v5_state_application_recovers_each_pending_ledger_phase(
    tmp_path: Path, phase: str
) -> None:
    root, config, state, state_path, finalization = _direct_v5_state_application_fixture(
        tmp_path
    )
    sidecar = finalization["stateApplicationSidecar"]
    state[supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY] = {
        "generationIndex": 2,
        "sidecarSha256": sidecar["sidecarSha256"],
        "identityLedger": finalization["identityLedgerDescriptor"],
        "phase": "pending" if phase == "before_promotion" else "ledger_promoted",
    }
    if phase == "after_promotion":
        state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] = finalization[
            "identityLedgerDescriptor"
        ]
    supervisor._save_state(state_path, state)

    record = _apply_direct_v5_state_fixture(
        root=root,
        config=config,
        state=state,
        state_path=state_path,
        finalization=finalization,
    )
    assert state["completedGenerations"] == [record]
    assert state["uniqueCandidatesEvaluated"] == 7
    assert supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY not in state


def test_direct_v5_state_application_recovers_an_exact_already_applied_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, config, state, state_path, finalization = _direct_v5_state_application_fixture(
        tmp_path
    )
    record = _apply_direct_v5_state_fixture(
        root=root,
        config=config,
        state=state,
        state_path=state_path,
        finalization=finalization,
    )
    sidecar = finalization["stateApplicationSidecar"]
    state[supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY] = {
        "generationIndex": 2,
        "sidecarSha256": sidecar["sidecarSha256"],
        "identityLedger": finalization["identityLedgerDescriptor"],
        "phase": "ledger_promoted",
    }
    supervisor._save_state(state_path, state)
    replay_calls: list[dict[str, Any]] = []

    def replay_finalizer(**kwargs: Any) -> dict[str, Any]:
        replay_calls.append(kwargs)
        return finalization

    monkeypatch.setattr(
        supervisor, "run_native_v5_generation_finalizer", replay_finalizer
    )

    assert supervisor._recover_native_v5_state_application(
        root=root,
        state=state,
        state_path=state_path,
        config=config,
        runtime_authority={"never": "opened"},
    )
    assert replay_calls[0]["manifest_path"] == (
        supervisor._native_finalization_root(root, 2) / "manifest.json"
    )
    assert state["completedGenerations"] == [record]
    assert supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY not in state


def test_direct_v5_state_application_rejects_pending_marker_ledger_divergence(
    tmp_path: Path,
) -> None:
    root, config, state, state_path, finalization = _direct_v5_state_application_fixture(
        tmp_path
    )
    sidecar = finalization["stateApplicationSidecar"]
    state[supervisor.NATIVE_V5_STATE_APPLICATION_PENDING_KEY] = {
        "generationIndex": 2,
        "sidecarSha256": sidecar["sidecarSha256"],
        "identityLedger": finalization["identityLedgerDescriptor"],
        "phase": "ledger_promoted",
    }
    state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] = {
        **finalization["identityLedgerDescriptor"],
        "semanticSha256": _sha("rogue-ledger-root"),
    }
    supervisor._save_state(state_path, state)

    with pytest.raises(TemporalDiscoveryContractError, match="promoted ledger descriptor drifted"):
        _apply_direct_v5_state_fixture(
            root=root,
            config=config,
            state=state,
            state_path=state_path,
            finalization=finalization,
        )
    assert state["completedGenerations"] == []


def test_direct_v5_state_application_never_opens_hashes_or_copies_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, config, state, state_path, finalization = _direct_v5_state_application_fixture(
        tmp_path
    )
    candidate_ledger = Path(finalization["identityLedgerDescriptor"]["absolutePath"])
    original_read_bytes = Path.read_bytes

    def no_ledger_read(path: Path, *args: Any, **kwargs: Any) -> bytes:
        if path == candidate_ledger:
            pytest.fail("current v5 state application reopened the identity ledger")
        return original_read_bytes(path, *args, **kwargs)

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("current v5 state application touched candidate-scale ledger bytes")

    monkeypatch.setattr(Path, "read_bytes", no_ledger_read)
    monkeypatch.setattr(supervisor, "_native_binary_file_sha256", forbidden)
    monkeypatch.setattr(supervisor, "_publish_committed_file", forbidden)
    monkeypatch.setattr(supervisor, "_validated_native_v5_identity_ledger", forbidden)

    record = _apply_direct_v5_state_fixture(
        root=root,
        config=config,
        state=state,
        state_path=state_path,
        finalization=finalization,
    )
    assert state["completedGenerations"] == [record]
    assert state[supervisor.NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] == finalization[
        "identityLedgerDescriptor"
    ]
