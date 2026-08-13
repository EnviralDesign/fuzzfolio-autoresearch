from __future__ import annotations

import gzip
import json
import os
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch import temporal_qd_native as native
from autoresearch import temporal_qd_v5_native as v5
from autoresearch.temporal_qd_pair_generation import build_pair_generation_config
from autoresearch.result_codec import canonical_json_bytes, sha256


def _authority_fixture() -> dict[str, object]:
    fixture_path = (
        Path(__file__).parent
        / "fixtures"
        / "temporal_qd_v5_shared_authority_oracle.json.gz"
    )
    return json.loads(gzip.decompress(fixture_path.read_bytes()))


def _sha(letter: str) -> str:
    return "sha256:" + letter * 64


def _input_descriptor(path: Path, semantic_sha256: str) -> dict[str, object]:
    raw = path.read_bytes()
    return {
        "absolutePath": str(path.resolve()),
        "fileSha256": sha256(raw),
        "semanticSha256": semantic_sha256,
        "byteLength": len(raw),
    }


def _authority_inputs() -> dict[str, object]:
    return deepcopy(_authority_fixture()["authorityInputs"])


def _generation_config(
    *, generation_index: int = 1, requested_count: int = 2, attempts: int = 5
) -> dict[str, object]:
    inputs = _authority_inputs()
    source = inputs["pairSourceAuthority"]
    assert isinstance(source, dict)
    bindings = v5.build_v5_generation_bindings(
        generation_run_config={
            "runId": "v5-native-bridge-fixture",
            "pairRunConfigSha256": source["pairRunConfigSha256"],
        },
        pair_source_authority=source,
        evolvable_module_authority=inputs["evolvableModuleAuthority"],
    )
    return build_pair_generation_config(
        generation_index=generation_index,
        target_unique_candidates=requested_count,
        max_proposal_attempts=attempts,
        run_config=bindings["runConfig"],
        pair_policy=inputs["bidirectionalPairPolicy"],
        operator_implementation_identity=bindings["operatorImplementation"],
        parent_archive=None,
        immigrant_construction_policy=source["immigrantConstructionPolicy"],
        global_identity_ledger_enabled=generation_index != 1,
    )


def _batch_authority() -> dict[str, str]:
    value: dict[str, str] = {
        "schemaVersion": native.NATIVE_AUTHORITY_SCHEMA,
        "contractVersion": native.NATIVE_CONTRACT_VERSION,
        "crateVersion": "0.1.0",
        "binaryName": native.NATIVE_BINARY_NAME,
        "buildProfile": "release",
        "executableSha256": _sha("a"),
        "sourceSha256": _sha("b"),
    }
    value["authoritySha256"] = sha256(canonical_json_bytes(value))
    return value


def _manifest(
    tmp_path: Path,
    *,
    generation_index: int = 1,
    requested_count: int = 2,
    evaluation_size: int = 1,
    attempts: int = 5,
    generation_kind: str = v5.V5_PROPOSAL_GENERATION_G0,
    parent_archive_input: dict[str, object] | None = None,
    identity_ledger_input: dict[str, object] | None = None,
) -> dict[str, object]:
    inputs = _authority_inputs()
    return v5.build_v5_proposal_manifest(
        output_root=tmp_path / "output",
        generation_config=_generation_config(
            generation_index=generation_index,
            requested_count=requested_count,
            attempts=attempts,
        ),
        pair_source_authority=inputs["pairSourceAuthority"],
        evolvable_module_authority=inputs["evolvableModuleAuthority"],
        bidirectional_pair_policy=inputs["bidirectionalPairPolicy"],
        native_operator_authority=inputs["nativeOperatorAuthority"],
        qd_engine_version=inputs["qdEngineVersion"],
        native_batch_authority=_batch_authority(),
        evaluation_population_size=evaluation_size,
        thread_cap=1,
        generation_kind=generation_kind,
        parent_archive_input=parent_archive_input,
        identity_ledger_input=identity_ledger_input,
    )


def _attempt_journal(*, accepted_count: int = 2) -> dict[str, object]:
    attempts: list[dict[str, object]] = []
    for ordinal, (disposition, reason) in enumerate(
        (
            ("accepted", "accepted"),
            ("global_duplicate", "candidate_identity_seen"),
            ("no_op", "operator_no_effect"),
            ("rejected_validation", "native_validation_failed"),
            ("accepted", "accepted"),
        )
    ):
        row: dict[str, object] = {
            "proposalOrdinal": ordinal,
            "proposalSeed": _sha(f"{ordinal + 1:x}"),
            "disposition": disposition,
            "reason": reason,
        }
        if disposition == "accepted":
            row["birthOrdinal"] = sum(
                item["disposition"] == "accepted" for item in attempts
            )
        attempts.append(row)
    assert sum(row["disposition"] == "accepted" for row in attempts) == accepted_count
    return {
        "schemaVersion": "temporal_qd_v5_attempt_disposition_journal_v1",
        "attempts": attempts,
    }


def _construction_summary(*, accepted_count: int, attempt_count: int) -> dict[str, object]:
    assert accepted_count == 2 and attempt_count == 5
    return {
        "schemaVersion": v5.V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA,
        "bytes": {
            "compactJournalBytes": 100,
            "staticAuthorityBytes": 100,
            "objectStoreBytes": 100,
            "selectedProjectionBytes": 100,
        },
        "attempts": {
            "byDisposition": {
                "accepted": 2,
                "global_duplicate": 1,
                "no_op": 1,
                "rejected_validation": 1,
            },
            "byReason": {
                "accepted": 2,
                "candidate_identity_seen": 1,
                "operator_no_effect": 1,
                "native_validation_failed": 1,
            },
        },
        "uniqueCounts": {
            "candidateCount": accepted_count,
            "programCount": accepted_count,
            "topologyCount": accepted_count,
            "resourceCount": accepted_count,
        },
    }


def _object_store(*, roots: tuple[str, str]) -> dict[str, object]:
    """Build the bounded v2 closure fixture without embedding object rows."""

    descriptor: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA,
        "rowSchemaVersion": v5.V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA,
        "relativePath": v5.V5_PROPOSAL_OBJECT_INVENTORY_PATH,
        "fileSha256": sha256(b"object-inventory-sidecar"),
        "byteLength": 101,
        "objectCount": len(roots),
        "objectByteCount": len(roots),
    }
    descriptor["descriptorSha256"] = sha256(canonical_json_bytes(descriptor))
    root_entries = [
        {
            "role": role,
            "relativePath": f"sha256/{semantic.removeprefix('sha256:')}.json",
            "objectSha256": semantic,
            "fileSha256": sha256(f"object-{role}".encode("utf-8")),
            "byteLength": 1,
        }
        for role, semantic in roots
    ]
    closure: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA,
        "relativeRoot": "v5-native/objects",
        "inventory": descriptor,
        "roots": root_entries,
    }
    closure["objectStoreSha256"] = sha256(canonical_json_bytes(closure))
    return closure


def _output_inventory(
    *,
    output_root: str,
    expected_authority_sha256: str,
    attempt_journal_sha256: str,
    publication_plan_sha256: str,
    g0_funnel_fragments_sha256: str,
    g0_funnel_projection_stream_receipt_sha256: str,
    compact_journal_sha256: str,
    identity_ledger_sha256: str,
    selected_projection_index_sha256: str,
) -> dict[str, object]:
    semantic_roots = {
        "attemptJournal": attempt_journal_sha256,
        "attemptRows": attempt_journal_sha256,
        "compactJournal": compact_journal_sha256,
        "identityLedger": identity_ledger_sha256,
        "selectedProjectionIndex": selected_projection_index_sha256,
        "sharedAuthority": expected_authority_sha256,
        "evaluationPopulation": sha256(b"evaluation population"),
        "generationJournal": sha256(b"generation journal"),
        "g0AcceptedPool": sha256(b"g0 accepted pool"),
        "g0CampaignConstructionLedger": sha256(b"g0 campaign construction ledger"),
        "g0FunnelProjectionStream": g0_funnel_projection_stream_receipt_sha256,
        "g0Selection": sha256(b"g0 selection"),
        "pairConfig": sha256(b"pair config"),
        "population": sha256(b"population"),
    }
    artifacts: list[dict[str, object]] = []
    for kind, path in (
        ("attemptJournal", "v5-native/attempt-journal-root.json"),
        ("attemptRows", "v5-native/attempts.jsonl"),
        ("compactJournal", "v5-native/accepted-records.jsonl"),
        ("evaluationPopulation", "evaluation-population.json"),
        ("g0AcceptedPool", "g0-bootstrap/accepted-pool.json"),
        (
            "g0CampaignConstructionLedger",
            "g0-bootstrap/campaign-construction-ledger.json",
        ),
        (
            "g0FunnelProjectionStream",
            v5.V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
        ),
        ("g0Selection", "g0-bootstrap/selection.json"),
        ("generationJournal", "generation-journal.json"),
        ("identityLedger", "v5-native/identity-ledger.json"),
        ("pairConfig", "pair-config.json"),
        ("population", "population.json"),
        ("selectedProjectionIndex", "v5-native/selected-projections.jsonl"),
        ("sharedAuthority", "v5-native/authority/shared-authority.json"),
    ):
        artifacts.append(
            {
                "kind": kind,
                "relativePath": path,
                "fileSha256": sha256(path.encode("utf-8")),
                "byteLength": len(path),
                "semanticSha256": semantic_roots[kind],
            }
        )
    object_store = _object_store(
        roots=(
            ("g0FunnelFragments", g0_funnel_fragments_sha256),
            (
                "g0FunnelProjectionStreamReceipt",
                g0_funnel_projection_stream_receipt_sha256,
            ),
            ("publicationPlan", publication_plan_sha256),
        )
    )
    inventory: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA,
        "outputRoot": output_root,
        "outputRootSha256": sha256(
            canonical_json_bytes(
                {
                    "schemaVersion": v5.V5_PROPOSAL_OUTPUT_ROOT_SCHEMA,
                    "absolutePath": output_root,
                }
            )
        ),
        "artifacts": artifacts,
        "objectStore": object_store,
    }
    inventory["outputInventorySha256"] = sha256(canonical_json_bytes(inventory))
    return inventory


def _evolved_output_inventory(
    *,
    output_root: str,
    identity_ledger_sha256: str,
    transaction_sha256: str,
    publication_plan_sha256: str,
    publication_receipt_sha256: str,
    publication_fragments_sha256: str,
) -> dict[str, object]:
    semantic_roots = {
        "evaluationPopulation": sha256(b"evolved evaluation population"),
        "generationJournal": sha256(b"evolved generation journal"),
        "identityLedger": identity_ledger_sha256,
        "pairConfig": sha256(b"evolved pair config"),
        "population": sha256(b"evolved population"),
    }
    artifact_paths = (
        ("evaluationPopulation", "evaluation-population.json"),
        ("generationJournal", "generation-journal.json"),
        ("identityLedger", "v5-native/identity-ledger.json"),
        ("pairConfig", "pair-config.json"),
        ("population", "population.json"),
    )
    artifacts = [
        {
            "kind": kind,
            "relativePath": path,
            "fileSha256": sha256(path.encode("utf-8")),
            "byteLength": len(path),
            "semanticSha256": semantic_roots[kind],
        }
        for kind, path in artifact_paths
    ]
    object_store = _object_store(
        roots=(
            ("publicationFragments", publication_fragments_sha256),
            ("publicationPlan", publication_plan_sha256),
            ("publicationReceipt", publication_receipt_sha256),
            ("transaction", transaction_sha256),
        )
    )
    inventory: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA,
        "outputRoot": output_root,
        "outputRootSha256": sha256(
            canonical_json_bytes(
                {
                    "schemaVersion": v5.V5_PROPOSAL_OUTPUT_ROOT_SCHEMA,
                    "absolutePath": output_root,
                }
            )
        ),
        "artifacts": artifacts,
        "objectStore": object_store,
    }
    inventory["outputInventorySha256"] = sha256(canonical_json_bytes(inventory))
    return inventory


def _completed_result(manifest: dict[str, object]) -> dict[str, object]:
    attempt_journal = _attempt_journal()
    attempt_journal_sha256 = sha256(canonical_json_bytes(attempt_journal))
    publication_request_sha256 = sha256(b"cap-free publication request")
    publication_plan_sha256 = sha256(b"sealed publication plan")
    g0_funnel_fragments_sha256 = sha256(b"sealed G0 funnel fragments")
    g0_funnel_projection_stream_receipt_sha256 = sha256(
        b"sealed G0 funnel projection-stream receipt"
    )
    compact_journal_sha256 = sha256(b"compact accepted records")
    identity_ledger_sha256 = sha256(b"identity ledger root")
    selected_projection_index_sha256 = sha256(b"selected projection index")
    output_inventory = _output_inventory(
        output_root=manifest["outputRoot"],
        expected_authority_sha256=manifest["expectedAuthoritySha256"],
        attempt_journal_sha256=attempt_journal_sha256,
        publication_plan_sha256=publication_plan_sha256,
        g0_funnel_fragments_sha256=g0_funnel_fragments_sha256,
        g0_funnel_projection_stream_receipt_sha256=(
            g0_funnel_projection_stream_receipt_sha256
        ),
        compact_journal_sha256=compact_journal_sha256,
        identity_ledger_sha256=identity_ledger_sha256,
        selected_projection_index_sha256=selected_projection_index_sha256,
    )
    receipt: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_RECEIPT_SCHEMA,
        "authoritySha256": manifest["authoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "expectedAuthoritySha256": manifest["expectedAuthoritySha256"],
        "generationConfigSha256": manifest["generationConfigSha256"],
        "generationIndex": manifest["generationIndex"],
        "requestedCount": manifest["requestedCount"],
        "acceptedRecordCount": manifest["requestedCount"],
        "attemptCount": len(attempt_journal["attempts"]),
        "attemptJournalSha256": attempt_journal_sha256,
        "publicationRequestSha256": publication_request_sha256,
        "publicationPlanSha256": publication_plan_sha256,
        "g0FunnelFragmentsSha256": g0_funnel_fragments_sha256,
        "g0FunnelProjectionStreamReceiptSha256": (
            g0_funnel_projection_stream_receipt_sha256
        ),
        "evaluationPopulationSize": manifest["evaluationPopulationSize"],
        "compactJournalSha256": compact_journal_sha256,
        "identityLedgerSha256": identity_ledger_sha256,
        "selectedProjectionIndexSha256": selected_projection_index_sha256,
        "outputInventory": output_inventory,
        "outputInventorySha256": output_inventory["outputInventorySha256"],
        "nativeBatchAuthoritySha256": manifest["executionAuthority"][
            "nativeBatchAuthoritySha256"
        ],
        "threadCap": manifest["threadCap"],
        "constructionSummary": _construction_summary(
            accepted_count=manifest["requestedCount"],
            attempt_count=len(attempt_journal["attempts"]),
        ),
    }
    receipt["receiptSha256"] = sha256(canonical_json_bytes(receipt))
    result: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_RESULT_SCHEMA,
        "contractVersion": native.NATIVE_CONTRACT_VERSION,
        "operation": v5.V5_PROPOSAL_OPERATION,
        "status": "completed",
        "authoritySha256": manifest["authoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "expectedAuthoritySha256": manifest["expectedAuthoritySha256"],
        "generationConfigSha256": manifest["generationConfigSha256"],
        "generationIndex": manifest["generationIndex"],
        "requestedCount": manifest["requestedCount"],
        "acceptedRecordCount": manifest["requestedCount"],
        "attemptCount": receipt["attemptCount"],
        "attemptJournalSha256": receipt["attemptJournalSha256"],
        "publicationRequestSha256": receipt["publicationRequestSha256"],
        "publicationPlanSha256": receipt["publicationPlanSha256"],
        "g0FunnelFragmentsSha256": receipt["g0FunnelFragmentsSha256"],
        "g0FunnelProjectionStreamReceiptSha256": receipt[
            "g0FunnelProjectionStreamReceiptSha256"
        ],
        "evaluationPopulationSize": manifest["evaluationPopulationSize"],
        "compactJournalSha256": receipt["compactJournalSha256"],
        "identityLedgerSha256": receipt["identityLedgerSha256"],
        "selectedProjectionIndexSha256": receipt["selectedProjectionIndexSha256"],
        "outputInventorySha256": receipt["outputInventorySha256"],
        "receipt": receipt,
        "receiptSha256": receipt["receiptSha256"],
    }
    result["resultSha256"] = sha256(canonical_json_bytes(result))
    return result


def _evolved_construction_summary(
    *, accepted_count: int, attempt_count: int
) -> dict[str, object]:
    return {
        "schemaVersion": v5.V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA,
        "bytes": {"durableObjectBytes": 100, "publicArtifactBytes": 100},
        "attempts": {
            "byDisposition": {"accepted": accepted_count, "no_op": attempt_count - accepted_count},
            "byReason": {"accepted": accepted_count, "operator_no_effect": attempt_count - accepted_count},
        },
        "uniqueCounts": {
            "candidateIdentityCount": accepted_count,
            "executableSemanticCount": accepted_count,
            "pairIdentityCount": accepted_count,
        },
    }


def _completed_evolved_result(manifest: dict[str, object]) -> dict[str, object]:
    transaction_sha256 = sha256(b"sealed evolved transaction")
    publication_request_sha256 = sha256(b"cap-free evolved publication request")
    publication_plan_sha256 = sha256(b"sealed evolved publication plan")
    publication_receipt_sha256 = sha256(b"sealed evolved publication receipt")
    publication_fragments_sha256 = sha256(b"sealed evolved publication fragments")
    identity_ledger_sha256 = sha256(b"evolved identity ledger root")
    output_inventory = _evolved_output_inventory(
        output_root=manifest["outputRoot"],
        identity_ledger_sha256=identity_ledger_sha256,
        transaction_sha256=transaction_sha256,
        publication_plan_sha256=publication_plan_sha256,
        publication_receipt_sha256=publication_receipt_sha256,
        publication_fragments_sha256=publication_fragments_sha256,
    )
    inputs = manifest["inputs"]
    receipt: dict[str, object] = {
        "schemaVersion": v5.V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA,
        "authoritySha256": manifest["authoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "expectedAuthoritySha256": manifest["expectedAuthoritySha256"],
        "generationConfigSha256": manifest["generationConfigSha256"],
        "generationIndex": manifest["generationIndex"],
        "requestedCount": manifest["requestedCount"],
        "acceptedRecordCount": manifest["requestedCount"],
        "attemptCount": 5,
        "transactionSha256": transaction_sha256,
        "parentArchiveInputBindingSha256": inputs["parentArchive"]["bindingSha256"],
        "identityLedgerInputBindingSha256": inputs["identityLedger"]["bindingSha256"],
        "publicationRequestSha256": publication_request_sha256,
        "publicationPlanSha256": publication_plan_sha256,
        "publicationReceiptSha256": publication_receipt_sha256,
        "publicationFragmentsSha256": publication_fragments_sha256,
        "evaluationPopulationSize": manifest["evaluationPopulationSize"],
        "identityLedgerSha256": identity_ledger_sha256,
        "outputInventory": output_inventory,
        "outputInventorySha256": output_inventory["outputInventorySha256"],
        "nativeBatchAuthoritySha256": manifest["executionAuthority"][
            "nativeBatchAuthoritySha256"
        ],
        "threadCap": manifest["threadCap"],
        "constructionSummary": _evolved_construction_summary(
            accepted_count=manifest["requestedCount"], attempt_count=5
        ),
    }
    receipt["receiptSha256"] = sha256(canonical_json_bytes(receipt))
    result: dict[str, object] = {
        "schemaVersion": v5.V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
        "contractVersion": native.NATIVE_CONTRACT_VERSION,
        "operation": v5.V5_PROPOSAL_OPERATION,
        "status": "completed",
        "authoritySha256": manifest["authoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "expectedAuthoritySha256": manifest["expectedAuthoritySha256"],
        "generationConfigSha256": manifest["generationConfigSha256"],
        "generationIndex": manifest["generationIndex"],
        "requestedCount": manifest["requestedCount"],
        "acceptedRecordCount": manifest["requestedCount"],
        "attemptCount": receipt["attemptCount"],
        "transactionSha256": receipt["transactionSha256"],
        "parentArchiveInputBindingSha256": receipt["parentArchiveInputBindingSha256"],
        "identityLedgerInputBindingSha256": receipt["identityLedgerInputBindingSha256"],
        "publicationRequestSha256": receipt["publicationRequestSha256"],
        "publicationPlanSha256": receipt["publicationPlanSha256"],
        "publicationReceiptSha256": receipt["publicationReceiptSha256"],
        "publicationFragmentsSha256": receipt["publicationFragmentsSha256"],
        "evaluationPopulationSize": manifest["evaluationPopulationSize"],
        "identityLedgerSha256": receipt["identityLedgerSha256"],
        "outputInventorySha256": receipt["outputInventorySha256"],
        "receipt": receipt,
        "receiptSha256": receipt["receiptSha256"],
    }
    result["resultSha256"] = sha256(canonical_json_bytes(result))
    return result


def _rehash_result_inventory_chain(result: dict[str, object]) -> None:
    inventory = result["receipt"]["outputInventory"]
    inventory["outputInventorySha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in inventory.items()
                if key != "outputInventorySha256"
            }
        )
    )
    result["receipt"]["outputInventorySha256"] = inventory["outputInventorySha256"]
    result["outputInventorySha256"] = inventory["outputInventorySha256"]
    result["receipt"]["receiptSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in result["receipt"].items()
                if key != "receiptSha256"
            }
        )
    )
    result["receiptSha256"] = result["receipt"]["receiptSha256"]
    result["resultSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in result.items() if key != "resultSha256"}
        )
    )


def _rehash_object_store(closure: dict[str, object]) -> None:
    closure["objectStoreSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in closure.items() if key != "objectStoreSha256"}
        )
    )


def _adoption_evidence(
    manifest: dict[str, object], result: dict[str, object]
) -> dict[str, object]:
    telemetry: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        "executionPath": "fresh",
        "validationMode": "balanced",
        "authenticationStrategy": "fresh_publication_proof",
        "phases": {
            "staticAuthorityMilliseconds": 1,
            "constructionMilliseconds": 2,
            "stagingMilliseconds": 3,
            "prepublicationValidationMilliseconds": 4,
            "publicationMilliseconds": 5,
            "outputAuthenticationMilliseconds": 0,
            "totalMilliseconds": 15,
        },
        "processCpuMilliseconds": 10,
        "cpuUtilizationMilliCores": 666,
        "publicArtifactBytesRead": 100,
        "objectStoreBytesRead": 0,
        "authenticatedFileCount": 9,
        "io": {
            "filesReopened": 0,
            "bytesRead": 0,
            "bytesHashed": 0,
            "bytesWritten": 100,
            "jsonRowsParsed": 0,
        },
        "validationPasses": {
            "constructorReplay": 1,
            "redundantFreshReplay": 0,
            "publicationPrepareReplay": 1,
            "stagedSemanticReplay": 1,
            "stagedFinalRehash": 0,
            "receiptBoundContentAuthentication": 0,
            "deepOutputReplay": 0,
        },
        "parallelAuthenticationWorkers": 0,
        "proposalReconstructionCount": 0,
        "legacyRichExpansionCount": 0,
        "processTree": {
            "measurement": "windows_peak_process_memory_v1",
            "peakRssBytes": 1,
            "peakPrivateBytes": 1,
            "pythonChildCount": 0,
            "dashboardChildCount": 0,
        },
        "threadCap": manifest["threadCap"],
        "constructionPrefetchMultiplier": 16,
    }
    batch = manifest["executionAuthority"]["nativeBatchAuthority"]
    evidence: dict[str, object] = {
        "schemaVersion": v5.V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA,
        "operation": v5.V5_PROPOSAL_OPERATION,
        "status": "adopted",
        "authoritySha256": manifest["authoritySha256"],
        "expectedAuthoritySha256": manifest["expectedAuthoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "immutableResultSha256": result["resultSha256"],
        "outputInventorySha256": result["outputInventorySha256"],
        "nativeBatchAuthoritySha256": manifest["executionAuthority"][
            "nativeBatchAuthoritySha256"
        ],
        "nativeExecutableSha256": batch["executableSha256"],
        "nativeSourceSha256": batch["sourceSha256"],
        "telemetry": telemetry,
    }
    evidence["adoptionEvidenceSha256"] = sha256(canonical_json_bytes(evidence))
    return evidence


def _evolved_adoption_evidence(
    manifest: dict[str, object], result: dict[str, object]
) -> dict[str, object]:
    telemetry: dict[str, object] = {
        "schemaVersion": v5.V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        "executionPath": "fresh",
        "validationMode": "balanced",
        "authenticationStrategy": "fresh_publication_proof",
        "phases": {
            "staticAuthorityMilliseconds": 1,
            "constructionMilliseconds": 2,
            "stagingMilliseconds": 3,
            "prepublicationValidationMilliseconds": 4,
            "publicationMilliseconds": 5,
            "outputAuthenticationMilliseconds": 0,
            "totalMilliseconds": 15,
        },
        "processCpuMilliseconds": 10,
        "cpuUtilizationMilliCores": 666,
        "publicArtifactBytesRead": 100,
        "objectStoreBytesRead": 100,
        "authenticatedFileCount": 8,
        "io": {
            "filesReopened": 0,
            "bytesRead": 0,
            "bytesHashed": 0,
            "bytesWritten": 200,
            "jsonRowsParsed": 0,
        },
        "validationPasses": {
            "constructorReplay": 1,
            "redundantFreshReplay": 0,
            "publicationPrepareReplay": 1,
            "stagedSemanticReplay": 1,
            "stagedFinalRehash": 0,
            "receiptBoundContentAuthentication": 0,
            "deepOutputReplay": 0,
        },
        "parallelAuthenticationWorkers": 0,
        "proposalReconstructionCount": 0,
        "legacyRichExpansionCount": 0,
        "processTree": {
            "measurement": "windows_peak_process_memory_v1",
            "peakRssBytes": 1,
            "peakPrivateBytes": 1,
            "pythonChildCount": 0,
            "dashboardChildCount": 0,
        },
        "threadCap": manifest["threadCap"],
        "constructionPrefetchMultiplier": 16,
    }
    batch = manifest["executionAuthority"]["nativeBatchAuthority"]
    evidence: dict[str, object] = {
        "schemaVersion": v5.V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA,
        "operation": v5.V5_PROPOSAL_OPERATION,
        "status": "adopted",
        "authoritySha256": manifest["authoritySha256"],
        "expectedAuthoritySha256": manifest["expectedAuthoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "immutableResultSha256": result["resultSha256"],
        "outputInventorySha256": result["outputInventorySha256"],
        "nativeBatchAuthoritySha256": manifest["executionAuthority"][
            "nativeBatchAuthoritySha256"
        ],
        "nativeExecutableSha256": batch["executableSha256"],
        "nativeSourceSha256": batch["sourceSha256"],
        "telemetry": telemetry,
    }
    evidence["adoptionEvidenceSha256"] = sha256(canonical_json_bytes(evidence))
    return evidence


def _native_run_kwargs(output_root: Path) -> dict[str, object]:
    inputs = _authority_inputs()
    return {
        "output_root": output_root,
        "generation_config": _generation_config(),
        "pair_source_authority": inputs["pairSourceAuthority"],
        "evolvable_module_authority": inputs["evolvableModuleAuthority"],
        "bidirectional_pair_policy": inputs["bidirectionalPairPolicy"],
        "native_operator_authority": inputs["nativeOperatorAuthority"],
        "qd_engine_version": inputs["qdEngineVersion"],
        "evaluation_population_size": 1,
        "execution_timeout_seconds": 60,
        "thread_cap": 1,
    }


def test_real_shared_authority_fixture_seals_and_preserves_public_policy_identity() -> None:
    fixture = _authority_fixture()
    inputs = fixture["authorityInputs"]
    sealed = fixture["sealedAuthority"]
    assert v5.validate_v5_frozen_authority(sealed) == sealed
    rebuilt = v5.build_v5_frozen_authority(
        pair_source_authority=inputs["pairSourceAuthority"],
        evolvable_module_authority=inputs["evolvableModuleAuthority"],
        bidirectional_pair_policy=inputs["bidirectionalPairPolicy"],
        native_operator_authority=inputs["nativeOperatorAuthority"],
        qd_engine_version=inputs["qdEngineVersion"],
    )
    assert rebuilt == sealed
    for side in ("long", "short"):
        assert (
            rebuilt["authority"][side]["policy"]
            == fixture["publicPolicySnapshots"][side]
        )


def test_pure_v5_generation_bindings_match_the_oracle_only_authority_adapter() -> None:
    """The production bridge projects sealed inputs without opening a runtime.

    The live authority is intentionally instantiated only here, in oracle test
    scope, to prove byte-for-byte parity with the historical adapter.  The
    bridge helper itself must not import or create it.
    """

    from autoresearch.temporal_qd_pair_factory import PairAuthorityBundle

    inputs = _authority_inputs()
    source = inputs["pairSourceAuthority"]
    sealed_dashboard_root = Path(
        str(source["nativeJsonlAuthority"]["dashboardSourceRoot"])
    )
    if not sealed_dashboard_root.is_dir():
        pytest.skip(
            "sealed external Dashboard authority checkout is unavailable"
        )
    run_config = {
        "runId": "v5-native-bridge-parity",
        "pairRunConfigSha256": source["pairRunConfigSha256"],
    }
    source_before = deepcopy(source)
    actual = v5.build_v5_generation_bindings(
        generation_run_config=run_config,
        pair_source_authority=source,
        evolvable_module_authority=inputs["evolvableModuleAuthority"],
    )
    with PairAuthorityBundle(deepcopy(source)) as bundle:
        oracle = bundle.open_evolvable_module_authority(
            deepcopy(inputs["evolvableModuleAuthority"])
        ).generation_bindings(deepcopy(run_config))
    assert actual == oracle
    assert source == source_before
    assert actual["runConfig"]["pairRunConfigSha256"] == source[
        "pairRunConfigSha256"
    ]
    assert v5.build_v5_bidirectional_pair_policy(
        pair_source_authority=source
    ) == inputs["bidirectionalPairPolicy"]
    assert v5.build_v5_native_operator_authority(
        pair_source_authority=source,
        evolvable_module_authority=inputs["evolvableModuleAuthority"],
    ) == inputs["nativeOperatorAuthority"]


def test_manifest_requires_the_existing_full_v2_generation_config(tmp_path: Path) -> None:
    """The bridge must never relaunch the old minimal config schema."""

    inputs = _authority_inputs()
    minimal: dict[str, object] = {
        "schemaVersion": native.PAIR_GENERATION_SCHEMA,
        "generationIndex": 1,
        "targetUniqueCandidates": 2,
        "maxProposalAttempts": 5,
    }
    minimal["configSha256"] = sha256(canonical_json_bytes(minimal))
    with pytest.raises(v5.TemporalQDV5NativeError, match="lacks sealed runConfig"):
        v5.build_v5_proposal_manifest(
            output_root=tmp_path / "output",
            generation_config=minimal,
            pair_source_authority=inputs["pairSourceAuthority"],
            evolvable_module_authority=inputs["evolvableModuleAuthority"],
            bidirectional_pair_policy=inputs["bidirectionalPairPolicy"],
            native_operator_authority=inputs["nativeOperatorAuthority"],
            qd_engine_version=inputs["qdEngineVersion"],
            native_batch_authority=_batch_authority(),
            evaluation_population_size=1,
            thread_cap=1,
        )


def test_generation_construction_adapter_is_an_exact_receipt_inventory_projection(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    result = _completed_result(manifest)
    adapter = v5.build_v5_generation_construction_adapter(
        result=result,
        manifest=manifest,
    )
    assert adapter["schemaVersion"] == v5.V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
    assert adapter["completed"] is True
    assert adapter["attemptCount"] == result["attemptCount"]
    assert adapter["acceptedCandidateCount"] == result["acceptedRecordCount"]
    assert adapter["selectedEvaluationCandidateCount"] == result[
        "evaluationPopulationSize"
    ]
    assert adapter["proposalResultSha256"] == result["resultSha256"]
    assert adapter["proposalReceiptSha256"] == result["receiptSha256"]
    assert adapter["population"]["relativePath"] == "population.json"
    assert adapter["evaluationPopulation"]["relativePath"] == "evaluation-population.json"
    assert adapter["generationJournal"]["relativePath"] == "generation-journal.json"
    assert adapter["identityLedger"]["relativePath"] == "v5-native/identity-ledger.json"
    assert v5.validate_v5_generation_construction_adapter(
        adapter,
        result=result,
        manifest=manifest,
    ) == adapter

    drifted = deepcopy(adapter)
    drifted["population"]["semanticSha256"] = _sha("f")
    drifted["adapterSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in drifted.items() if key != "adapterSha256"}
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="adapter binding drifted"):
        v5.validate_v5_generation_construction_adapter(
            drifted,
            result=result,
            manifest=manifest,
        )


def test_evolved_result_uses_its_separate_five_artifact_receipt_family(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "parent-archive.json"
    ledger = tmp_path / "identity-ledger.json"
    parent.write_bytes(canonical_json_bytes({"archive": "fixture"}) + b"\n")
    ledger.write_bytes(canonical_json_bytes({"ledger": "fixture"}) + b"\n")
    manifest = _manifest(
        tmp_path,
        generation_index=2,
        generation_kind=v5.V5_PROPOSAL_GENERATION_EVOLVED,
        parent_archive_input=v5.build_v5_proposal_input_binding(
            kind="parentArchive", sealed_descriptor=_input_descriptor(parent, _sha("c"))
        ),
        identity_ledger_input=v5.build_v5_proposal_input_binding(
            kind="identityLedger", sealed_descriptor=_input_descriptor(ledger, _sha("d"))
        ),
    )
    result = _completed_evolved_result(manifest)
    assert v5.validate_v5_evolved_proposal_result(result, manifest=manifest) == result
    with pytest.raises(v5.TemporalQDV5NativeError, match="non-G0 manifest"):
        v5.validate_v5_proposal_result(result, manifest=manifest)

    adapter = v5.build_v5_generation_construction_adapter(
        result=result, manifest=manifest
    )
    assert (
        adapter["schemaVersion"]
        == v5.V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
    )
    assert adapter["generationKind"] == v5.V5_PROPOSAL_GENERATION_EVOLVED
    assert adapter["identityLedger"]["relativePath"] == "v5-native/identity-ledger.json"
    assert adapter["identityLedger"]["semanticSha256"] == result["identityLedgerSha256"]
    fragments = adapter["evolvedPublicationFragments"]
    assert (
        fragments["schemaVersion"]
        == v5.V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA
    )
    assert (
        fragments["coreSchemaVersion"]
        == v5.V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA
    )
    assert fragments["semanticSha256"] == result["publicationFragmentsSha256"]
    assert fragments["relativePath"] == (
        "v5-native/objects/sha256/"
        + result["publicationFragmentsSha256"].removeprefix("sha256:")
        + ".json"
    )
    assert (
        v5.validate_v5_evolved_publication_fragments_descriptor(
            fragments, result=result, manifest=manifest
        )
        == fragments
    )
    invocation = adapter["nativeV5Invocation"]
    assert (
        invocation["schemaVersion"]
        == v5.V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA
    )
    assert invocation["proposalReceiptSha256"] == result["receiptSha256"]
    assert invocation["outputInventorySha256"] == result["outputInventorySha256"]
    for name, document, schema, semantic_sha256, suffix in (
        (
            "proposalManifest",
            invocation["proposalManifest"],
            v5.V5_PROPOSAL_MANIFEST_SCHEMA,
            manifest["manifestSha256"],
            "manifest.json",
        ),
        (
            "proposalResult",
            invocation["proposalResult"],
            v5.V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
            result["resultSha256"],
            v5.V5_PROPOSAL_RESULT_FILENAME,
        ),
    ):
        assert document["schemaVersion"] == v5.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA
        assert document["documentSchemaVersion"] == schema
        assert document["semanticSha256"] == semantic_sha256
        assert document["relativePath"] == (
            "native-batch/v5-proposal/"
            + manifest["manifestSha256"].removeprefix("sha256:")
            + "/"
            + suffix
        ), name
        assert document["absolutePath"] == str(
            Path(manifest["outputRoot"]) / document["relativePath"]
        )
        assert document["fileSha256"] == sha256(
            canonical_json_bytes(manifest if name == "proposalManifest" else result)
            + b"\n"
        )
        assert document["byteLength"] == len(
            canonical_json_bytes(manifest if name == "proposalManifest" else result)
            + b"\n"
        )
    assert (
        v5.validate_v5_evolved_native_v5_invocation_descriptor(
            invocation, result=result, manifest=manifest
        )
        == invocation
    )
    assert set(
        artifact["kind"] for artifact in result["receipt"]["outputInventory"]["artifacts"]
    ) == {
        "evaluationPopulation",
        "generationJournal",
        "identityLedger",
        "pairConfig",
        "population",
    }
    evidence = _evolved_adoption_evidence(manifest, result)
    assert (
        v5.validate_v5_evolved_proposal_adoption_evidence(
            evidence, manifest=manifest, immutable_result=result
        )
        == evidence
    )

    missing_receipt_object = deepcopy(result)
    object_store = missing_receipt_object["receipt"]["outputInventory"]["objectStore"]
    object_store["roots"] = [
        item
        for item in object_store["roots"]
        if item["role"] != "publicationReceipt"
    ]
    _rehash_object_store(object_store)
    _rehash_result_inventory_chain(missing_receipt_object)
    with pytest.raises(v5.TemporalQDV5NativeError, match="root roles|publication receipt root"):
        v5.validate_v5_evolved_proposal_result(
            missing_receipt_object, manifest=manifest
        )


def test_evolved_funnel_descriptor_is_exact_inventory_projection_and_never_g0(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "parent-archive.json"
    ledger = tmp_path / "identity-ledger.json"
    parent.write_bytes(canonical_json_bytes({"archive": "fixture"}) + b"\n")
    ledger.write_bytes(canonical_json_bytes({"ledger": "fixture"}) + b"\n")
    evolved_manifest = _manifest(
        tmp_path,
        generation_index=2,
        generation_kind=v5.V5_PROPOSAL_GENERATION_EVOLVED,
        parent_archive_input=v5.build_v5_proposal_input_binding(
            kind="parentArchive", sealed_descriptor=_input_descriptor(parent, _sha("c"))
        ),
        identity_ledger_input=v5.build_v5_proposal_input_binding(
            kind="identityLedger", sealed_descriptor=_input_descriptor(ledger, _sha("d"))
        ),
    )
    evolved_result = _completed_evolved_result(evolved_manifest)
    adapter = v5.build_v5_generation_construction_adapter(
        result=evolved_result, manifest=evolved_manifest
    )

    def rehash_adapter(value: dict[str, object]) -> None:
        value["adapterSha256"] = sha256(
            canonical_json_bytes(
                {key: item for key, item in value.items() if key != "adapterSha256"}
            )
        )

    missing = deepcopy(evolved_result)
    store = missing["receipt"]["outputInventory"]["objectStore"]
    store["roots"] = [
        item
        for item in store["roots"]
        if item["role"] != "publicationFragments"
    ]
    _rehash_object_store(store)
    _rehash_result_inventory_chain(missing)
    with pytest.raises(v5.TemporalQDV5NativeError, match="root roles|publication fragments root"):
        v5.validate_v5_evolved_proposal_result(missing, manifest=evolved_manifest)

    missing_descriptor = deepcopy(adapter)
    missing_descriptor.pop("evolvedPublicationFragments")
    rehash_adapter(missing_descriptor)
    with pytest.raises(v5.TemporalQDV5NativeError, match="fields are not exact"):
        v5.validate_v5_generation_construction_adapter(
            missing_descriptor, result=evolved_result, manifest=evolved_manifest
        )

    publication_receipt_object = next(
        item
        for item in evolved_result["receipt"]["outputInventory"]["objectStore"][
            "roots"
        ]
        if item["role"] == "publicationReceipt"
    )
    replaced = deepcopy(adapter)
    descriptor = replaced["evolvedPublicationFragments"]
    descriptor["semanticSha256"] = publication_receipt_object["objectSha256"]
    descriptor["relativePath"] = "v5-native/objects/" + publication_receipt_object[
        "relativePath"
    ]
    descriptor["absolutePath"] = str(
        Path(evolved_manifest["outputRoot"]) / descriptor["relativePath"]
    )
    descriptor["fileSha256"] = publication_receipt_object["fileSha256"]
    descriptor["byteLength"] = publication_receipt_object["byteLength"]
    rehash_adapter(replaced)
    with pytest.raises(v5.TemporalQDV5NativeError, match="adapter binding drifted"):
        v5.validate_v5_generation_construction_adapter(
            replaced, result=evolved_result, manifest=evolved_manifest
        )

    wrong_kind = deepcopy(adapter)
    wrong_kind["evolvedPublicationFragments"]["coreSchemaVersion"] = (
        v5.V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA
    )
    rehash_adapter(wrong_kind)
    with pytest.raises(v5.TemporalQDV5NativeError, match="adapter binding drifted"):
        v5.validate_v5_generation_construction_adapter(
            wrong_kind, result=evolved_result, manifest=evolved_manifest
        )

    rehashed = deepcopy(adapter)
    rehashed["evolvedPublicationFragments"]["fileSha256"] = _sha("f")
    rehash_adapter(rehashed)
    with pytest.raises(v5.TemporalQDV5NativeError, match="adapter binding drifted"):
        v5.validate_v5_generation_construction_adapter(
            rehashed, result=evolved_result, manifest=evolved_manifest
        )

    g0_manifest = _manifest(tmp_path / "g0")
    g0_result = _completed_result(g0_manifest)
    g0_adapter = v5.build_v5_generation_construction_adapter(
        result=g0_result, manifest=g0_manifest
    )
    assert g0_adapter["schemaVersion"] == v5.V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
    assert "evolvedPublicationFragments" not in g0_adapter
    with pytest.raises(v5.TemporalQDV5NativeError, match="incompatible with G0"):
        v5.build_v5_evolved_publication_fragments_descriptor(
            result=g0_result, manifest=g0_manifest
        )


def test_evolved_invocation_descriptor_is_exact_and_never_a_g0_alias(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "parent-archive.json"
    ledger = tmp_path / "identity-ledger.json"
    parent.write_bytes(canonical_json_bytes({"archive": "fixture"}) + b"\n")
    ledger.write_bytes(canonical_json_bytes({"ledger": "fixture"}) + b"\n")
    manifest = _manifest(
        tmp_path,
        generation_index=2,
        generation_kind=v5.V5_PROPOSAL_GENERATION_EVOLVED,
        parent_archive_input=v5.build_v5_proposal_input_binding(
            kind="parentArchive", sealed_descriptor=_input_descriptor(parent, _sha("c"))
        ),
        identity_ledger_input=v5.build_v5_proposal_input_binding(
            kind="identityLedger", sealed_descriptor=_input_descriptor(ledger, _sha("d"))
        ),
    )
    result = _completed_evolved_result(manifest)
    adapter = v5.build_v5_generation_construction_adapter(
        result=result, manifest=manifest
    )

    def rehash_adapter(value: dict[str, object]) -> None:
        value["adapterSha256"] = sha256(
            canonical_json_bytes(
                {key: item for key, item in value.items() if key != "adapterSha256"}
            )
        )

    for mutation in (
        lambda value: value.pop("nativeV5Invocation"),
        lambda value: value["nativeV5Invocation"].__setitem__(
            "proposalResult", deepcopy(value["nativeV5Invocation"]["proposalManifest"])
        ),
        lambda value: value["nativeV5Invocation"]["proposalResult"].__setitem__(
            "documentSchemaVersion", v5.V5_PROPOSAL_RESULT_SCHEMA
        ),
        lambda value: value["nativeV5Invocation"]["proposalManifest"].update(
            {
                "relativePath": "native-batch/v5-proposal/../proposal/manifest.json",
                "absolutePath": str(
                    Path(manifest["outputRoot"])
                    / "native-batch/v5-proposal/../proposal/manifest.json"
                ),
            }
        ),
        lambda value: value["nativeV5Invocation"]["proposalResult"].__setitem__(
            "semanticSha256", _sha("f")
        ),
    ):
        drifted = deepcopy(adapter)
        mutation(drifted)
        rehash_adapter(drifted)
        with pytest.raises(v5.TemporalQDV5NativeError, match="adapter binding drifted|fields are not exact"):
            v5.validate_v5_generation_construction_adapter(
                drifted, result=result, manifest=manifest
            )

    mismatch = deepcopy(result)
    mismatch["manifestSha256"] = _sha("f")
    _rehash_result_inventory_chain(mismatch)
    with pytest.raises(v5.TemporalQDV5NativeError, match="incompatible with its manifest"):
        v5.build_v5_evolved_native_v5_invocation_descriptor(
            result=mismatch, manifest=manifest
        )

    g0_manifest = _manifest(tmp_path / "g0")
    g0_result = _completed_result(g0_manifest)
    g0_adapter = v5.build_v5_generation_construction_adapter(
        result=g0_result, manifest=g0_manifest
    )
    assert g0_adapter["nativeV5Invocation"]["schemaVersion"] == (
        v5.V5_G0_NATIVE_V5_INVOCATION_SCHEMA
    )
    assert "evolvedPublicationFragments" not in g0_adapter
    with pytest.raises(v5.TemporalQDV5NativeError, match="incompatible with G0"):
        v5.build_v5_evolved_native_v5_invocation_descriptor(
            result=g0_result, manifest=g0_manifest
        )


def test_g0_funnel_and_invocation_descriptors_are_exact_and_family_separated(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    result = _completed_result(manifest)
    adapter = v5.build_v5_generation_construction_adapter(
        result=result, manifest=manifest
    )

    def rehash_adapter(value: dict[str, object]) -> None:
        value["adapterSha256"] = sha256(
            canonical_json_bytes(
                {key: item for key, item in value.items() if key != "adapterSha256"}
            )
        )

    assert adapter["schemaVersion"] == v5.V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
    assert "evolvedPublicationFragments" not in adapter
    funnel = adapter["g0FunnelFragments"]
    assert funnel["schemaVersion"] == v5.V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA
    assert funnel["coreSchemaVersion"] == v5.V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA
    assert funnel["semanticSha256"] == result["g0FunnelFragmentsSha256"]
    assert funnel["relativePath"] == (
        "v5-native/objects/sha256/"
        + result["g0FunnelFragmentsSha256"].removeprefix("sha256:")
        + ".json"
    )
    assert (
        v5.validate_v5_g0_funnel_fragments_descriptor(
            funnel, result=result, manifest=manifest
        )
        == funnel
    )
    projection_stream = adapter["g0FunnelProjectionStream"]
    assert projection_stream["schemaVersion"] == (
        v5.V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA
    )
    assert projection_stream["coreReceiptSchemaVersion"] == (
        v5.V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA
    )
    assert projection_stream["rowSchemaVersion"] == (
        v5.V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA
    )
    projection_root = result["g0FunnelProjectionStreamReceiptSha256"]
    assert projection_stream["stream"]["relativePath"] == (
        v5.V5_G0_FUNNEL_PROJECTION_STREAM_PATH
    )
    assert projection_stream["stream"]["semanticSha256"] == projection_root
    assert projection_stream["receiptObject"]["semanticSha256"] == projection_root
    assert projection_stream["receiptObject"]["relativePath"] == (
        "v5-native/objects/sha256/"
        + projection_root.removeprefix("sha256:")
        + ".json"
    )
    assert (
        v5.validate_v5_g0_funnel_projection_stream_descriptor(
            projection_stream, result=result, manifest=manifest
        )
        == projection_stream
    )
    invocation = adapter["nativeV5Invocation"]
    assert invocation["schemaVersion"] == v5.V5_G0_NATIVE_V5_INVOCATION_SCHEMA
    assert invocation["proposalReceiptSha256"] == result["receiptSha256"]
    assert invocation["outputInventorySha256"] == result["outputInventorySha256"]
    assert invocation["proposalManifest"]["documentSchemaVersion"] == (
        v5.V5_PROPOSAL_MANIFEST_SCHEMA
    )
    assert invocation["proposalResult"]["documentSchemaVersion"] == (
        v5.V5_PROPOSAL_RESULT_SCHEMA
    )
    for name, document, source in (
        ("proposalManifest", invocation["proposalManifest"], manifest),
        ("proposalResult", invocation["proposalResult"], result),
    ):
        assert document["schemaVersion"] == v5.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA
        assert document["fileSha256"] == sha256(canonical_json_bytes(source) + b"\n")
        assert document["byteLength"] == len(canonical_json_bytes(source) + b"\n")
        assert document["relativePath"].startswith(
            "native-batch/v5-proposal/"
            + manifest["manifestSha256"].removeprefix("sha256:")
            + "/"
        ), name
    assert (
        v5.validate_v5_g0_native_v5_invocation_descriptor(
            invocation, result=result, manifest=manifest
        )
        == invocation
    )

    missing_root = deepcopy(result)
    store = missing_root["receipt"]["outputInventory"]["objectStore"]
    store["roots"] = [
        item
        for item in store["roots"]
        if item["role"] != "g0FunnelFragments"
    ]
    _rehash_object_store(store)
    _rehash_result_inventory_chain(missing_root)
    with pytest.raises(v5.TemporalQDV5NativeError, match="root roles|G0 funnel fragments root"):
        v5.validate_v5_proposal_result(missing_root, manifest=manifest)

    missing_stream_receipt = deepcopy(result)
    stream_store = missing_stream_receipt["receipt"]["outputInventory"]["objectStore"]
    stream_store["roots"] = [
        item
        for item in stream_store["roots"]
        if item["role"] != "g0FunnelProjectionStreamReceipt"
    ]
    _rehash_object_store(stream_store)
    _rehash_result_inventory_chain(missing_stream_receipt)
    with pytest.raises(
        v5.TemporalQDV5NativeError,
        match="root roles|G0 funnel projection-stream receipt root",
    ):
        v5.validate_v5_proposal_result(missing_stream_receipt, manifest=manifest)

    aliased_stream_path = deepcopy(result)
    next(
        item
        for item in aliased_stream_path["receipt"]["outputInventory"]["artifacts"]
        if item["kind"] == "g0FunnelProjectionStream"
    )["relativePath"] = "v5-native/../g0-funnel-projections.jsonl"
    _rehash_result_inventory_chain(aliased_stream_path)
    with pytest.raises(v5.TemporalQDV5NativeError, match="path|binding drifted"):
        v5.validate_v5_proposal_result(aliased_stream_path, manifest=manifest)

    plan_object = next(
        item
        for item in result["receipt"]["outputInventory"]["objectStore"]["roots"]
        if item["role"] == "publicationPlan"
    )
    for mutation in (
        lambda value: value.pop("g0FunnelFragments"),
        lambda value: value.pop("g0FunnelProjectionStream"),
        lambda value: value["g0FunnelFragments"].update(
            {
                "semanticSha256": plan_object["objectSha256"],
                "relativePath": "v5-native/objects/" + plan_object["relativePath"],
                "absolutePath": str(
                    Path(manifest["outputRoot"])
                    / "v5-native/objects"
                    / plan_object["relativePath"]
                ),
                "fileSha256": plan_object["fileSha256"],
                "byteLength": plan_object["byteLength"],
            }
        ),
        lambda value: value["g0FunnelFragments"].__setitem__(
            "coreSchemaVersion", v5.V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA
        ),
        lambda value: value["g0FunnelFragments"].update(
            {
                "relativePath": "v5-native/objects/sha256/../alias.json",
                "absolutePath": str(
                    Path(manifest["outputRoot"])
                    / "v5-native/objects/sha256/../alias.json"
                ),
            }
        ),
        lambda value: value["g0FunnelFragments"].__setitem__(
            "semanticSha256", _sha("f")
        ),
        lambda value: value["g0FunnelProjectionStream"].__setitem__(
            "coreReceiptSchemaVersion",
            v5.V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA,
        ),
        lambda value: value["g0FunnelProjectionStream"].__setitem__(
            "stream", deepcopy(value["population"])
        ),
        lambda value: value["g0FunnelProjectionStream"]["receiptObject"].update(
            {
                "relativePath": "v5-native/objects/sha256/../alias.json",
                "absolutePath": str(
                    Path(manifest["outputRoot"])
                    / "v5-native/objects/sha256/../alias.json"
                ),
            }
        ),
        lambda value: value["g0FunnelProjectionStream"]["receiptObject"].__setitem__(
            "semanticSha256", _sha("f")
        ),
    ):
        drifted = deepcopy(adapter)
        mutation(drifted)
        rehash_adapter(drifted)
        with pytest.raises(v5.TemporalQDV5NativeError, match="adapter binding drifted|fields are not exact"):
            v5.validate_v5_generation_construction_adapter(
                drifted, result=result, manifest=manifest
            )

    evolved_parent = tmp_path / "evolved-parent.json"
    evolved_ledger = tmp_path / "evolved-ledger.json"
    evolved_parent.write_bytes(canonical_json_bytes({"archive": "fixture"}) + b"\n")
    evolved_ledger.write_bytes(canonical_json_bytes({"ledger": "fixture"}) + b"\n")
    evolved_manifest = _manifest(
        tmp_path / "evolved",
        generation_index=2,
        generation_kind=v5.V5_PROPOSAL_GENERATION_EVOLVED,
        parent_archive_input=v5.build_v5_proposal_input_binding(
            kind="parentArchive",
            sealed_descriptor=_input_descriptor(evolved_parent, _sha("c")),
        ),
        identity_ledger_input=v5.build_v5_proposal_input_binding(
            kind="identityLedger",
            sealed_descriptor=_input_descriptor(evolved_ledger, _sha("d")),
        ),
    )
    evolved_result = _completed_evolved_result(evolved_manifest)
    evolved_adapter = v5.build_v5_generation_construction_adapter(
        result=evolved_result, manifest=evolved_manifest
    )
    assert "g0FunnelFragments" not in evolved_adapter
    assert "g0FunnelProjectionStream" not in evolved_adapter
    with pytest.raises(v5.TemporalQDV5NativeError, match="incompatible with evolved"):
        v5.build_v5_g0_funnel_fragments_descriptor(
            result=evolved_result, manifest=evolved_manifest
        )
    with pytest.raises(v5.TemporalQDV5NativeError, match="incompatible with evolved"):
        v5.build_v5_g0_native_v5_invocation_descriptor(
            result=evolved_result, manifest=evolved_manifest
        )
    with pytest.raises(v5.TemporalQDV5NativeError, match="incompatible with evolved"):
        v5.build_v5_g0_funnel_projection_stream_descriptor(
            result=evolved_result, manifest=evolved_manifest
        )


def test_temporal_domains_are_projected_from_the_public_operator_specification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _authority_inputs()
    native_operator = inputs["nativeOperatorAuthority"]
    specification = native_operator["temporalOperatorSpecification"]
    domains = native_operator["temporalDomains"]
    assert specification["operatorSpecSha256"] == _authority_fixture()[
        "sealedAuthority"
    ]["authority"]["temporalOperatorSpecSha256"]
    assert domains == v5._build_v5_temporal_domains(
        specification,
        name="test temporal operator specification",
    )
    assert domains == {
        "schemaVersion": v5.V5_TEMPORAL_DOMAINS_SCHEMA,
        "eventAges": [0, 1, 2, 3, 5, 8, 13, 21],
        "positionAges": [1, 2, 3, 5, 8, 13, 21, 34],
        "utcSessionWindows": [
            [0, 360],
            [360, 720],
            [420, 960],
            [720, 1080],
            [780, 1260],
            [1080, 1439],
        ],
        "eventAgeWindows": [[0, 1], [0, 3], [1, 1], [1, 3], [2, 5]],
        "consecutiveCounts": [2, 3, 5],
        "cooldownCounts": [1, 3, 5],
        "temporalDomainsSha256": domains["temporalDomainsSha256"],
    }
    drifted_native = deepcopy(native_operator)
    drifted_native["temporalDomains"]["eventAges"][-1] = 34
    drifted_native["temporalDomains"]["temporalDomainsSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in drifted_native["temporalDomains"].items()
                if key != "temporalDomainsSha256"
            }
        )
    )
    drifted_native["nativeOperatorAuthoritySha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in drifted_native.items()
                if key != "nativeOperatorAuthoritySha256"
            }
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="specification/domains drifted"):
        v5.build_v5_frozen_authority(
            pair_source_authority=inputs["pairSourceAuthority"],
            evolvable_module_authority=inputs["evolvableModuleAuthority"],
            bidirectional_pair_policy=inputs["bidirectionalPairPolicy"],
            native_operator_authority=drifted_native,
            qd_engine_version=inputs["qdEngineVersion"],
        )

    missing = deepcopy(domains)
    del missing["cooldownCounts"]
    missing["temporalDomainsSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in missing.items() if key != "temporalDomainsSha256"}
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="fields are not exact"):
        v5._temporal_domains(missing)

    source_drift = deepcopy(specification)
    source_drift["domains"]["eventAges"][-1] = 34
    source_drift["operatorSpecSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in source_drift.items()
                if key != "operatorSpecSha256"
            }
        )
    )

    class _DriftedTemporalOperatorLayer:
        def __init__(self) -> None:
            self.specification = deepcopy(source_drift)

    import autoresearch.evolvable_module_temporal_operators as temporal_operators

    monkeypatch.setattr(
        temporal_operators,
        "GenomeTemporalOperatorLayer",
        _DriftedTemporalOperatorLayer,
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="specification drifted from its source"):
        v5.build_v5_frozen_authority(
            pair_source_authority=inputs["pairSourceAuthority"],
            evolvable_module_authority=inputs["evolvableModuleAuthority"],
            bidirectional_pair_policy=inputs["bidirectionalPairPolicy"],
            native_operator_authority=native_operator,
            qd_engine_version=inputs["qdEngineVersion"],
        )


def test_resource_operator_public_identity_uses_the_sealed_indicator_timeframe_policy() -> None:
    inputs = _authority_inputs()
    module = inputs["pairSourceAuthority"]["longModule"]
    catalog = module["catalog"]
    policy = module["indicatorPolicy"]
    current = v5._resource_operator_spec_sha256(
        catalog,
        indicator_policy=policy,
        name="test source catalog",
    )
    assert current == _authority_fixture()["sealedAuthority"]["authority"]["long"][
        "policy"
    ]["payload"]["resourceOperatorSpecSha256"]

    alternate = deepcopy(policy)
    alternate["timeframePolicy"] = ["M5", "H1"]
    alternate["catalogSha256"] = sha256(
        canonical_json_bytes({"payload": catalog, "timeframePolicy": ["H1", "M5"]})
    )
    alternate["policySha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in alternate.items() if key != "policySha256"}
        )
    )
    assert (
        v5._resource_operator_spec_sha256(
            catalog,
            indicator_policy=alternate,
            name="test alternate catalog",
        )
        != current
    )

    for frames, fragment in (
        ([], "timeframe policy is invalid"),
        (["H1", "H1"], "duplicate frames"),
        (["H1", "UNKNOWN"], "not catalog-backed"),
    ):
        invalid = deepcopy(alternate)
        invalid["timeframePolicy"] = frames
        invalid["policySha256"] = sha256(
            canonical_json_bytes(
                {key: value for key, value in invalid.items() if key != "policySha256"}
            )
        )
        with pytest.raises(v5.TemporalQDV5NativeError, match=fragment):
            v5._resource_operator_spec_sha256(
                catalog,
                indicator_policy=invalid,
                name="test invalid catalog",
            )


def test_sealed_authority_opens_after_original_source_configs_are_deleted(
    tmp_path: Path,
) -> None:
    inputs = _authority_inputs()
    source_paths = [
        tmp_path / "pair-source.json",
        tmp_path / "evolvable-authority.json",
        tmp_path / "native-operator-authority.json",
    ]
    for path, value in zip(
        source_paths,
        (
            inputs["pairSourceAuthority"],
            inputs["evolvableModuleAuthority"],
            inputs["nativeOperatorAuthority"],
        ),
        strict=True,
    ):
        path.write_bytes(canonical_json_bytes(value) + b"\n")
    sealed = v5.build_v5_frozen_authority(
        pair_source_authority=inputs["pairSourceAuthority"],
        evolvable_module_authority=inputs["evolvableModuleAuthority"],
        bidirectional_pair_policy=inputs["bidirectionalPairPolicy"],
        native_operator_authority=inputs["nativeOperatorAuthority"],
        qd_engine_version=inputs["qdEngineVersion"],
    )
    sealed_path = tmp_path / "sealed-authority.json"
    sealed_path.write_bytes(canonical_json_bytes(sealed) + b"\n")
    for path in source_paths:
        path.unlink()
    assert not any(path.exists() for path in source_paths)
    assert v5.validate_v5_frozen_authority(
        json.loads(sealed_path.read_text(encoding="utf-8"))
    ) == sealed


def test_payload_tamper_and_self_rehashed_snapshot_drift_fail_the_immutable_manifest(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    payload_tamper = deepcopy(manifest)
    grammar = payload_tamper["frozenAuthority"]["authority"]["long"][
        "grammarContext"
    ]
    grammar["payload"]["context"]["tampered"] = True
    with pytest.raises(v5.TemporalQDV5NativeError, match="payload identity mismatch"):
        v5.validate_v5_proposal_manifest(payload_tamper)

    self_rehashed = deepcopy(manifest)
    grammar = self_rehashed["frozenAuthority"]["authority"]["long"][
        "grammarContext"
    ]
    grammar["payload"]["context"]["tampered"] = True
    grammar["sha256"] = sha256(canonical_json_bytes(grammar["payload"]))
    frozen = self_rehashed["frozenAuthority"]
    frozen["authoritySha256"] = sha256(canonical_json_bytes(frozen["authority"]))
    # The old execution/expected binding is intentionally not regenerated.
    # A self-rehashed nested snapshot is therefore a new authority, not a
    # valid restart of this immutable manifest.
    self_rehashed["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in self_rehashed.items() if key != "manifestSha256"}
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="expected authority binding"):
        v5.validate_v5_proposal_manifest(self_rehashed)


def test_manifest_is_compact_at_four_thousand_and_contains_no_rich_population(
    tmp_path: Path,
) -> None:
    small = _manifest(tmp_path / "small", requested_count=2, evaluation_size=1)
    broad = _manifest(
        tmp_path / "broad",
        requested_count=4000,
        evaluation_size=1024,
        attempts=20_000,
    )
    assert len(canonical_json_bytes(broad)) - len(canonical_json_bytes(small)) < 256
    payload = canonical_json_bytes(broad)
    assert b"bidirectionalGenome" not in payload
    assert b"proposal-journal" not in payload
    assert b'"selectedProjection":' not in payload


def test_all_attempt_receipt_binds_rejections_noops_duplicates_and_forbids_python_children(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    result = _completed_result(manifest)
    assert v5.validate_v5_proposal_result(result, manifest=manifest) == result
    assert result["attemptCount"] == 5
    assert result["receipt"]["constructionSummary"]["attempts"]["byDisposition"] == {
        "accepted": 2,
        "global_duplicate": 1,
        "no_op": 1,
        "rejected_validation": 1,
    }

    corrupt_summary = deepcopy(result)
    corrupt_summary["receipt"]["constructionSummary"]["threadCap"] = 1
    corrupt_summary["receipt"]["receiptSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in corrupt_summary["receipt"].items()
                if key != "receiptSha256"
            }
        )
    )
    corrupt_summary["receiptSha256"] = corrupt_summary["receipt"]["receiptSha256"]
    corrupt_summary["resultSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in corrupt_summary.items()
                if key != "resultSha256"
            }
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="fields are not exact"):
        v5.validate_v5_proposal_result(corrupt_summary, manifest=manifest)

    corrupt_evidence = _adoption_evidence(manifest, result)
    corrupt_evidence["telemetry"]["processTree"]["pythonChildCount"] = 1
    corrupt_evidence["adoptionEvidenceSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in corrupt_evidence.items()
                if key != "adoptionEvidenceSha256"
            }
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="forbidden nonzero"):
        v5.validate_v5_proposal_adoption_evidence(
            corrupt_evidence,
            manifest=manifest,
            immutable_result=result,
        )


def test_output_inventory_has_fixed_public_paths_and_root_binding(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    result = _completed_result(manifest)
    assert v5.validate_v5_proposal_result(result, manifest=manifest) == result

    relocated = deepcopy(result)
    compact = next(
        artifact
        for artifact in relocated["receipt"]["outputInventory"]["artifacts"]
        if artifact["kind"] == "compactJournal"
    )
    compact["relativePath"] = "supervisor-state.json"
    _rehash_result_inventory_chain(relocated)
    with pytest.raises(v5.TemporalQDV5NativeError, match="compactJournal binding drifted"):
        v5.validate_v5_proposal_result(relocated, manifest=manifest)

    aliased = deepcopy(result)
    compact_path = next(
        artifact["relativePath"]
        for artifact in aliased["receipt"]["outputInventory"]["artifacts"]
        if artifact["kind"] == "compactJournal"
    )
    identity_ledger = next(
        artifact
        for artifact in aliased["receipt"]["outputInventory"]["artifacts"]
        if artifact["kind"] == "identityLedger"
    )
    identity_ledger["relativePath"] = compact_path
    _rehash_result_inventory_chain(aliased)
    with pytest.raises(v5.TemporalQDV5NativeError, match="paths must be unique"):
        v5.validate_v5_proposal_result(aliased, manifest=manifest)

    undeclared = deepcopy(result)
    undeclared["receipt"]["outputInventory"]["artifacts"].append(
        {
            "kind": "zUntrusted",
            "relativePath": "v5-native/untrusted.json",
            "fileSha256": _sha("f"),
            "byteLength": 1,
            "semanticSha256": _sha("e"),
        }
    )
    _rehash_result_inventory_chain(undeclared)
    with pytest.raises(v5.TemporalQDV5NativeError, match="artifact set is not exact"):
        v5.validate_v5_proposal_result(undeclared, manifest=manifest)

    missing_plan_object = deepcopy(result)
    object_store = missing_plan_object["receipt"]["outputInventory"]["objectStore"]
    object_store["roots"] = []
    _rehash_object_store(object_store)
    _rehash_result_inventory_chain(missing_plan_object)
    with pytest.raises(v5.TemporalQDV5NativeError, match="root roles|publication plan root"):
        v5.validate_v5_proposal_result(missing_plan_object, manifest=manifest)

    root_drift = deepcopy(result)
    root_drift["receipt"]["outputInventory"]["outputRoot"] = str(tmp_path / "other")
    _rehash_result_inventory_chain(root_drift)
    with pytest.raises(v5.TemporalQDV5NativeError, match="root binding drifted"):
        v5.validate_v5_proposal_result(root_drift, manifest=manifest)


def test_compact_object_inventory_never_opens_or_enumerates_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The large object closure is Rust-owned; Python sees only its receipt head."""

    manifest = _manifest(tmp_path)
    result = _completed_result(manifest)
    closure = result["receipt"]["outputInventory"]["objectStore"]
    descriptor = closure["inventory"]
    assert isinstance(descriptor, dict)
    assert "objects" not in closure
    descriptor["objectCount"] = 1_000_000
    descriptor["objectByteCount"] = 9_000_000
    descriptor["descriptorSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in descriptor.items() if key != "descriptorSha256"}
        )
    )
    _rehash_object_store(closure)
    _rehash_result_inventory_chain(result)

    sidecar = Path(manifest["outputRoot"]) / v5.V5_PROPOSAL_OBJECT_INVENTORY_PATH
    original_read_bytes = Path.read_bytes

    def no_sidecar_read(path: Path) -> bytes:
        if path == sidecar:
            pytest.fail("Python opened the candidate-scale object inventory sidecar")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", no_sidecar_read)
    assert v5.validate_v5_proposal_result(result, manifest=manifest) == result
    adapter = v5.build_v5_generation_construction_adapter(
        result=result, manifest=manifest
    )
    assert adapter["g0FunnelFragments"]["semanticSha256"] == result[
        "g0FunnelFragmentsSha256"
    ]


def test_restart_adopts_all_attempt_result_with_one_native_zero_reconstruction_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_root = tmp_path / "output"
    batch = _batch_authority()
    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"fixture")
    monkeypatch.setattr(native, "require_native_batch", lambda: (binary, batch))
    monkeypatch.setattr(native, "_sha256_file", lambda _path: batch["executableSha256"])
    monkeypatch.setattr(native, "native_source_sha256", lambda: batch["sourceSha256"])
    manifest = _manifest(output_root.parent)
    invocation = (
        output_root
        / "native-batch"
        / "v5-proposal"
        / manifest["manifestSha256"].removeprefix("sha256:")
    )
    invocation.mkdir(parents=True)
    result = _completed_result(manifest)
    (invocation / v5.V5_PROPOSAL_RESULT_FILENAME).write_bytes(
        canonical_json_bytes(result) + b"\n"
    )
    commands: list[tuple[str, ...]] = []

    def adopt(command: object, **_kwargs: object) -> SimpleNamespace:
        assert isinstance(command, tuple)
        commands.append(tuple(str(part) for part in command))
        assert command[:2] == (str(binary), "--manifest")
        assert "python" not in " ".join(commands[-1]).lower()
        return SimpleNamespace(stdout=canonical_json_bytes(_adoption_evidence(manifest, result)) + b"\n")

    monkeypatch.setattr(native, "_run_checked", adopt)
    assert v5.run_native_v5_proposal_construction(**_native_run_kwargs(output_root)) == result
    assert len(commands) == 1


def test_new_transaction_starts_one_native_batch_and_no_python_candidate_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_root = tmp_path / "output"
    batch = _batch_authority()
    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"fixture")
    monkeypatch.setattr(native, "require_native_batch", lambda: (binary, batch))
    monkeypatch.setattr(native, "_sha256_file", lambda _path: batch["executableSha256"])
    monkeypatch.setattr(native, "native_source_sha256", lambda: batch["sourceSha256"])
    monkeypatch.setattr(
        native,
        "run_native_generation",
        lambda **_kwargs: pytest.fail("v5 bridge must not use legacy generation"),
    )
    commands: list[tuple[str, ...]] = []

    def run_once(command: object, **_kwargs: object) -> SimpleNamespace:
        assert isinstance(command, tuple)
        commands.append(tuple(str(part) for part in command))
        assert command[0] == str(binary)
        assert command[1] == "--manifest"
        assert len(command) == 3
        assert "python" not in " ".join(commands[-1]).lower()
        # Python owns only the private immutable invocation controls.  The
        # public proposal tree is published solely by the Rust receipt-last
        # transaction, never pre-seeded by a bridge-side authority copy.
        assert not (output_root / "v5-native").exists()
        manifest = json.loads(Path(command[2]).read_text(encoding="utf-8"))
        result = _completed_result(manifest)
        (Path(command[2]).parent / v5.V5_PROPOSAL_RESULT_FILENAME).write_bytes(
            canonical_json_bytes(result) + b"\n"
        )
        return SimpleNamespace(
            stdout=canonical_json_bytes(_adoption_evidence(manifest, result)) + b"\n"
        )

    monkeypatch.setattr(native, "_run_checked", run_once)
    result = v5.run_native_v5_proposal_construction(**_native_run_kwargs(output_root))
    assert len(commands) == 1
    assert result["acceptedRecordCount"] == 2
    assert result["attemptCount"] == 5


def test_fast_ephemeral_bridge_is_distinct_minimal_and_non_resumable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_root = tmp_path / "output"
    batch = _batch_authority()
    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"fixture")
    monkeypatch.setattr(native, "require_native_batch", lambda: (binary, batch))
    monkeypatch.setattr(native, "_sha256_file", lambda _path: batch["executableSha256"])
    monkeypatch.setattr(native, "native_source_sha256", lambda: batch["sourceSha256"])
    commands: list[tuple[str, ...]] = []

    def run_once(command: object, **_kwargs: object) -> SimpleNamespace:
        command = tuple(str(part) for part in command)
        commands.append(command)
        assert command[3:] == (
            "--execution-mode",
            v5.V5_EXECUTION_MODE_FAST_EPHEMERAL,
        )
        manifest = json.loads(Path(command[2]).read_text(encoding="utf-8"))
        artifacts = {
            "evaluationPopulation": {
                "relativePath": "evaluation-population.json",
                "semanticSha256": _sha("c"),
                "fileSha256": _sha("d"),
                "byteLength": 123,
            },
            "identityLedger": {
                "relativePath": "identity-ledger.json",
                "semanticSha256": _sha("e"),
                "fileSha256": _sha("f"),
                "byteLength": 45,
            },
        }
        result = {
            "schemaVersion": v5.V5_FAST_EPHEMERAL_RESULT_SCHEMA,
            "executionMode": v5.V5_EXECUTION_MODE_FAST_EPHEMERAL,
            "status": "completed",
            "generationKind": v5.V5_PROPOSAL_GENERATION_G0,
            "generationIndex": manifest["generationIndex"],
            "manifestSha256": manifest["manifestSha256"],
            "generationConfigSha256": manifest["generationConfigSha256"],
            "attemptCount": manifest["requestedCount"],
            "acceptedCandidateCount": manifest["requestedCount"],
            "selectedEvaluationCandidateCount": manifest["evaluationPopulationSize"],
            "artifacts": artifacts,
            "timings": {
                "staticAuthorityMilliseconds": 1,
                "constructionMilliseconds": 2,
                "ephemeralPublicationMilliseconds": 3,
                "totalMilliseconds": 6,
            },
        }
        result["resultSha256"] = sha256(canonical_json_bytes(result))
        result_path = Path(command[2]).parent / v5.V5_PROPOSAL_RESULT_FILENAME
        result_path.write_bytes(canonical_json_bytes(result) + b"\n")
        complete = {
            "schemaVersion": v5.V5_FAST_EPHEMERAL_COMPLETE_SCHEMA,
            "executionMode": v5.V5_EXECUTION_MODE_FAST_EPHEMERAL,
            "generationIndex": manifest["generationIndex"],
            "resultSha256": result["resultSha256"],
            "artifacts": artifacts,
        }
        complete["completeSha256"] = sha256(canonical_json_bytes(complete))
        (output_root / "COMPLETE.json").write_bytes(
            canonical_json_bytes(complete) + b"\n"
        )
        return SimpleNamespace(stdout=canonical_json_bytes(result) + b"\n")

    monkeypatch.setattr(native, "_run_checked", run_once)
    kwargs = _native_run_kwargs(output_root)
    kwargs["execution_mode"] = v5.V5_EXECUTION_MODE_FAST_EPHEMERAL
    adapter = v5.run_native_v5_generation_construction(**kwargs)

    assert adapter["schemaVersion"] == v5.V5_FAST_EPHEMERAL_ADAPTER_SCHEMA
    assert adapter["executionMode"] == v5.V5_EXECUTION_MODE_FAST_EPHEMERAL
    assert set(adapter) == {
        "schemaVersion",
        "executionMode",
        "operation",
        "completed",
        "generationKind",
        "generationIndex",
        "generationConfigSha256",
        "attemptCount",
        "acceptedCandidateCount",
        "selectedEvaluationCandidateCount",
        "proposalResultSha256",
        "evaluationPopulation",
        "identityLedger",
        "adapterSha256",
    }
    assert len(commands) == 1
    with pytest.raises(v5.TemporalQDV5NativeError, match="non-resumable"):
        v5.run_native_v5_generation_construction(**kwargs)
    assert len(commands) == 1


def test_native_v5_rejects_oversized_compact_proposal_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The immutable result is capped before Python attempts to parse it."""

    output_root = tmp_path / "output"
    batch = _batch_authority()
    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"fixture")
    monkeypatch.setattr(native, "require_native_batch", lambda: (binary, batch))
    monkeypatch.setattr(native, "_sha256_file", lambda _path: batch["executableSha256"])
    monkeypatch.setattr(native, "native_source_sha256", lambda: batch["sourceSha256"])

    def oversized_result(command: object, **_kwargs: object) -> SimpleNamespace:
        result_path = Path(str(tuple(command)[2])).parent / v5.V5_PROPOSAL_RESULT_FILENAME
        result_path.write_bytes(
            b"x" * (v5._V5_COMPACT_PROPOSAL_RESULT_LIMIT_BYTES + 1)
        )
        return SimpleNamespace(stdout=b"{}\n")

    monkeypatch.setattr(native, "_run_checked", oversized_result)
    with pytest.raises(
        native.TemporalQDNativeError,
        match="native v5 proposal result exceeded its .* compact-document limit",
    ):
        v5.run_native_v5_proposal_construction(**_native_run_kwargs(output_root))


def test_fresh_and_adopt_callback_exposes_validated_execution_evidence_without_adapter_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_root = tmp_path / "output"
    batch = _batch_authority()
    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"fixture")
    monkeypatch.setattr(native, "require_native_batch", lambda: (binary, batch))
    monkeypatch.setattr(native, "_sha256_file", lambda _path: batch["executableSha256"])
    monkeypatch.setattr(native, "native_source_sha256", lambda: batch["sourceSha256"])

    def run_once(command: object, **_kwargs: object) -> SimpleNamespace:
        assert isinstance(command, tuple)
        manifest = json.loads(Path(command[2]).read_text(encoding="utf-8"))
        result_path = Path(command[2]).parent / v5.V5_PROPOSAL_RESULT_FILENAME
        if not result_path.exists():
            result = _completed_result(manifest)
            result_path.write_bytes(canonical_json_bytes(result) + b"\n")
        else:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        return SimpleNamespace(
            stdout=canonical_json_bytes(_adoption_evidence(manifest, result)) + b"\n"
        )

    monkeypatch.setattr(native, "_run_checked", run_once)
    received: list[dict[str, object]] = []
    kwargs = _native_run_kwargs(output_root)
    kwargs["on_execution_evidence"] = lambda evidence: received.append(
        deepcopy(dict(evidence))
    )
    fresh = v5.run_native_v5_generation_construction(**kwargs)
    adopted = v5.run_native_v5_generation_construction(**kwargs)

    assert fresh == adopted
    assert len(received) == 2
    invocation = next((output_root / "native-batch" / "v5-proposal").iterdir())
    manifest = json.loads((invocation / "manifest.json").read_text(encoding="utf-8"))
    result = json.loads(
        (invocation / v5.V5_PROPOSAL_RESULT_FILENAME).read_text(encoding="utf-8")
    )
    assert fresh["g0FunnelFragments"]["semanticSha256"] == result["g0FunnelFragmentsSha256"]
    assert fresh["g0FunnelProjectionStream"]["stream"]["semanticSha256"] == (
        result["g0FunnelProjectionStreamReceiptSha256"]
    )
    assert fresh["g0FunnelProjectionStream"]["receiptObject"]["semanticSha256"] == (
        result["g0FunnelProjectionStreamReceiptSha256"]
    )
    assert fresh["nativeV5Invocation"]["proposalManifest"]["semanticSha256"] == (
        manifest["manifestSha256"]
    )
    assert fresh["nativeV5Invocation"]["proposalResult"]["semanticSha256"] == (
        result["resultSha256"]
    )
    for evidence in received:
        assert (
            v5.validate_v5_proposal_adoption_evidence(
                evidence,
                manifest=manifest,
                immutable_result=result,
            )
            == evidence
        )
        process_tree = evidence["telemetry"]["processTree"]
        assert process_tree["peakRssBytes"] > 0
        assert process_tree["peakPrivateBytes"] > 0
        assert process_tree["pythonChildCount"] == 0
        assert process_tree["dashboardChildCount"] == 0


def test_evolved_bridge_selects_its_distinct_result_and_evidence_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_root = tmp_path / "output"
    parent = tmp_path / "parent-archive.json"
    ledger = tmp_path / "identity-ledger.json"
    parent.write_bytes(canonical_json_bytes({"archive": "fixture"}) + b"\n")
    ledger.write_bytes(canonical_json_bytes({"ledger": "fixture"}) + b"\n")
    batch = _batch_authority()
    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"fixture")
    monkeypatch.setattr(native, "require_native_batch", lambda: (binary, batch))
    monkeypatch.setattr(native, "_sha256_file", lambda _path: batch["executableSha256"])
    monkeypatch.setattr(native, "native_source_sha256", lambda: batch["sourceSha256"])

    def run_once(command: object, **_kwargs: object) -> SimpleNamespace:
        assert isinstance(command, tuple)
        manifest = json.loads(Path(command[2]).read_text(encoding="utf-8"))
        result_path = Path(command[2]).parent / v5.V5_PROPOSAL_RESULT_FILENAME
        if not result_path.exists():
            result = _completed_evolved_result(manifest)
            result_path.write_bytes(canonical_json_bytes(result) + b"\n")
        else:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        return SimpleNamespace(
            stdout=canonical_json_bytes(_evolved_adoption_evidence(manifest, result))
            + b"\n"
        )

    monkeypatch.setattr(native, "_run_checked", run_once)
    kwargs = _native_run_kwargs(output_root)
    kwargs.update(
        {
            "generation_config": _generation_config(generation_index=2),
            "generation_kind": v5.V5_PROPOSAL_GENERATION_EVOLVED,
            "parent_archive_input": v5.build_v5_proposal_input_binding(
                kind="parentArchive",
                sealed_descriptor=_input_descriptor(parent, _sha("c")),
            ),
            "identity_ledger_input": v5.build_v5_proposal_input_binding(
                kind="identityLedger",
                sealed_descriptor=_input_descriptor(ledger, _sha("d")),
            ),
        }
    )
    evidence: list[dict[str, object]] = []
    kwargs["on_execution_evidence"] = lambda value: evidence.append(dict(value))

    fresh = v5.run_native_v5_generation_construction(**kwargs)
    adopted = v5.run_native_v5_generation_construction(**kwargs)

    assert fresh == adopted
    assert fresh["generationKind"] == v5.V5_PROPOSAL_GENERATION_EVOLVED
    assert fresh["identityLedger"]["relativePath"] == "v5-native/identity-ledger.json"
    assert len(evidence) == 2
    assert {
        item["schemaVersion"] for item in evidence
    } == {v5.V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA}


def test_real_native_v5_g0_fresh_then_adopts_a_receipt_sealed_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the real one-process bridge with every retired Python path hostile.

    This deliberately builds the production-shaped generation config before
    installing tripwires.  The two bridge calls must then be Rust fresh
    construction followed by Rust receipt adoption; neither call may open a
    Python factory, operator, compiler, Dashboard authority, or legacy
    generation entry point.
    """

    # Do one explicit release build before the hostile paths are installed,
    # then pin both invocations to that exact executable.  This keeps the test
    # a real process test while avoiding an accidental second build/fallback.
    monkeypatch.delenv(native.NATIVE_BINARY_ENV, raising=False)
    binary, _authority = native.ensure_native_batch()
    monkeypatch.setenv(native.NATIVE_BINARY_ENV, str(binary))

    output_root = tmp_path / "real-v5-g0"
    kwargs = _native_run_kwargs(output_root)
    execution_evidence: list[dict[str, object]] = []

    def capture_execution_evidence(evidence: Mapping[str, object]) -> None:
        execution_evidence.append(dict(evidence))

    kwargs["on_execution_evidence"] = capture_execution_evidence

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("retired Python factory/operator/compiler/Dashboard path was called")

    import autoresearch.temporal_discovery_validation as discovery_validation
    import autoresearch.temporal_qd_evolution as qd_evolution
    import autoresearch.temporal_qd_pair_factory as pair_factory
    import autoresearch.temporal_qd_pair_generation as pair_generation
    import autoresearch.temporal_typed_motif_grammar as typed_grammar

    monkeypatch.setattr(pair_factory, "PairAuthorityBundle", forbidden)
    monkeypatch.setattr(pair_generation, "TypedGrammarPairOperator", forbidden)
    monkeypatch.setattr(
        discovery_validation, "DashboardBidirectionalPairCompiler", forbidden
    )
    monkeypatch.setattr(typed_grammar, "DashboardNativeAuthority", forbidden)
    monkeypatch.setattr(qd_evolution, "generate_qd_generation", forbidden)
    monkeypatch.setattr(native, "run_native_generation", forbidden)

    fresh = v5.run_native_v5_generation_construction(**kwargs)
    adopted = v5.run_native_v5_generation_construction(**kwargs)

    # The receipt-derived adapter is a stable supervisor handoff; adoption
    # cannot silently rerun proposal construction and produce a new identity.
    assert adopted == fresh
    assert fresh["completed"] is True
    assert fresh["attemptCount"] == 2
    assert fresh["acceptedCandidateCount"] == 2
    assert fresh["selectedEvaluationCandidateCount"] == 1
    assert len(execution_evidence) == 2

    invocation_parent = output_root / "native-batch" / "v5-proposal"
    invocations = [path for path in invocation_parent.iterdir() if path.is_dir()]
    assert len(invocations) == 1
    invocation = invocations[0]
    manifest = json.loads((invocation / "manifest.json").read_text(encoding="utf-8"))
    result_path = invocation / v5.V5_PROPOSAL_RESULT_FILENAME
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert v5.validate_v5_proposal_result(result, manifest=manifest) == result
    assert (
        v5.build_v5_generation_construction_adapter(result=result, manifest=manifest)
        == fresh
    )

    # The output-tree receipt is the seal; the invocation result is the small
    # post-seal pointer/adoption entry point.  Its compact inventory and root
    # projection are the only Python-visible object-store contract.
    receipt_path = output_root / "internal" / "v5-proposal" / "receipt.json"
    assert receipt_path.is_file()
    assert result_path.is_file()
    inventory = result["receipt"]["outputInventory"]
    # The complete candidate-scale closure is Rust-owned JSONL.  The public
    # result carries only its bounded descriptor plus the fixed root set;
    # this test deliberately does not stat, parse, or hash that sidecar.
    assert inventory["schemaVersion"] == v5.V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA
    assert inventory["outputInventorySha256"] == fresh["outputInventorySha256"]
    assert result["outputInventorySha256"] == fresh["outputInventorySha256"]
    assert {item["kind"] for item in inventory["artifacts"]} == {
        kind
        for kind, _relative_path, _semantic_key in v5._v5_required_output_artifacts(
            v5.V5_PROPOSAL_GENERATION_G0
        )
    }
    object_store = inventory["objectStore"]
    assert object_store["schemaVersion"] == v5.V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA
    assert set(object_store["inventory"]) == {
        "schemaVersion",
        "rowSchemaVersion",
        "relativePath",
        "fileSha256",
        "byteLength",
        "objectCount",
        "objectByteCount",
        "descriptorSha256",
    }
    assert object_store["inventory"]["relativePath"] == v5.V5_PROPOSAL_OBJECT_INVENTORY_PATH
    assert {
        root["role"] for root in object_store["roots"]
    } == {
        "g0FunnelFragments",
        "g0FunnelProjectionStreamReceipt",
        "publicationPlan",
    }
    roots = {root["role"]: root for root in object_store["roots"]}
    assert roots["g0FunnelFragments"]["objectSha256"] == fresh[
        "g0FunnelFragments"
    ]["semanticSha256"]
    assert roots["g0FunnelProjectionStreamReceipt"]["objectSha256"] == fresh[
        "g0FunnelProjectionStream"
    ]["receiptObject"]["semanticSha256"]

    # Fragments are private, are deleted before sealing, and must never become
    # recovery input.  The manifest-bound staging directory itself may remain
    # empty: it is intentionally not swept, because restart must not delete an
    # unknown prefix-matching stale entry.  No private file may survive.
    private_stage = output_root / ".temporal-qd-v5-private-stage"
    assert not private_stage.exists() or not any(
        path.is_file() for path in private_stage.rglob("*")
    )

    # The seal deliberately carries only deterministic construction accounting;
    # mutable process observations are authenticated stdout-only evidence.
    assert "telemetry" not in result["receipt"]
    construction_summary = result["receipt"]["constructionSummary"]
    assert construction_summary["schemaVersion"] == v5.V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA
    assert set(construction_summary) == {
        "schemaVersion",
        "bytes",
        "attempts",
        "uniqueCounts",
    }
    for evidence in execution_evidence:
        assert evidence["schemaVersion"] == v5.V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA
        process_tree = evidence["telemetry"]["processTree"]
        assert process_tree["pythonChildCount"] == 0
        assert process_tree["dashboardChildCount"] == 0
        if os.name == "nt":
            assert process_tree["measurement"] == "windows_peak_process_memory_v1"
            assert process_tree["peakRssBytes"] > 0
            assert process_tree["peakPrivateBytes"] > 0
        else:
            assert process_tree == {
                "measurement": "unavailable_non_windows_v1",
                "peakRssBytes": None,
                "peakPrivateBytes": None,
                "pythonChildCount": 0,
                "dashboardChildCount": 0,
            }


def test_evolved_inputs_require_exact_file_and_semantic_bindings(tmp_path: Path) -> None:
    parent = tmp_path / "parent-archive.json"
    ledger = tmp_path / "identity-ledger.json"
    parent.write_bytes(canonical_json_bytes({"archive": "fixture"}) + b"\n")
    ledger.write_bytes(canonical_json_bytes({"ledger": "fixture"}) + b"\n")
    parent_binding = v5.build_v5_proposal_input_binding(
        kind="parentArchive", sealed_descriptor=_input_descriptor(parent, _sha("c"))
    )
    ledger_binding = v5.build_v5_proposal_input_binding(
        kind="identityLedger", sealed_descriptor=_input_descriptor(ledger, _sha("d"))
    )
    manifest = _manifest(
        tmp_path,
        generation_index=2,
        generation_kind=v5.V5_PROPOSAL_GENERATION_EVOLVED,
        parent_archive_input=parent_binding,
        identity_ledger_input=ledger_binding,
    )
    assert v5.validate_v5_proposal_manifest(manifest) == manifest
    unsafe = deepcopy(manifest)
    unsafe["inputs"]["parentArchive"]["absolutePath"] = "..\\parent.json"
    unsafe["inputs"]["parentArchive"]["bindingSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in unsafe["inputs"]["parentArchive"].items()
                if key != "bindingSha256"
            }
        )
    )
    unsafe["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in unsafe.items() if key != "manifestSha256"}
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="safe absolute path"):
        v5.validate_v5_proposal_manifest(unsafe)

    dotted = deepcopy(manifest)
    dotted["inputs"]["identityLedger"]["absolutePath"] = str(
        ledger.parent / "." / ledger.name
    ).replace("\\", "\\.\\", 1)
    dotted["inputs"]["identityLedger"]["bindingSha256"] = sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in dotted["inputs"]["identityLedger"].items()
                if key != "bindingSha256"
            }
        )
    )
    dotted["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: value for key, value in dotted.items() if key != "manifestSha256"}
        )
    )
    with pytest.raises(v5.TemporalQDV5NativeError, match="safe absolute path"):
        v5.validate_v5_proposal_manifest(dotted)
