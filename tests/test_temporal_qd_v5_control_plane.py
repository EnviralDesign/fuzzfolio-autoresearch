"""Focused fail-closed coverage for the thin Rust v5 control-plane bridges."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

import autoresearch.temporal_qd_v5_control_plane as control
from autoresearch.result_codec import canonical_json_bytes
from autoresearch.temporal_discovery_base import canonical_sha256


def _sha(value: bytes | str) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _write(path: Path, value: dict[str, Any]) -> tuple[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_json_bytes(value) + b"\n"
    path.write_bytes(raw)
    return _sha(raw), len(raw)


def _write_pretty(path: Path, value: dict[str, Any]) -> tuple[str, int]:
    """Match Rust's bounded pretty control-document encoding."""

    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    path.write_bytes(raw)
    return _sha(raw), len(raw)


def _self_hashed(value: dict[str, Any], field: str) -> dict[str, Any]:
    output = dict(value)
    output[field] = canonical_sha256(output)
    return output


def _runtime(root: Path) -> dict[str, Any]:
    binaries: dict[str, Any] = {}
    for role in sorted(control._RUNTIME_ROLES):
        path = root / "bin" / f"{role}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(("native-" + role).encode("ascii"))
        binaries[role] = {
            "path": str(path.resolve()),
            "bytes": path.stat().st_size,
            "fileSha256": _sha(path.read_bytes()),
        }
    authority = {
        "schemaVersion": control.RUNTIME_AUTHORITY_SCHEMA,
        "generationFinalizationEngine": "rust",
        "contractVersion": control.CONTRACT_VERSION,
        "binaries": binaries,
    }
    authority["authoritySha256"] = canonical_sha256(authority)
    return authority


def test_compact_control_reader_rejects_an_oversized_document(tmp_path: Path) -> None:
    """A hostile receipt cannot force a whole-file read before the size fence."""

    receipt = tmp_path / "oversized-receipt.json"
    receipt.write_bytes(
        b"{" * (control._CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES + 1)
    )
    with pytest.raises(
        control.TemporalQDV5ControlPlaneError,
        match="oversized receipt exceeds the control-document limit",
    ):
        control._read_bounded_canonical_object(receipt, name="oversized receipt")


def test_pinned_control_plane_rejects_bounded_pipe_overflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every control-plane binary call goes through the owned capped drainer."""

    binary = tmp_path / "gateway.bin"
    binary.write_bytes(b"fixture")
    monkeypatch.setattr(
        control,
        "pinned_runtime_binary",
        lambda **_kwargs: binary,
    )

    def overflow(*_args: object, **kwargs: object) -> object:
        assert kwargs["stdout_limit_bytes"] == control._CURRENT_V5_COMPACT_STDOUT_LIMIT_BYTES
        assert kwargs["stderr_limit_bytes"] == control._CURRENT_V5_COMPACT_STDERR_LIMIT_BYTES
        raise control.native.TemporalQDNativeError(
            "native Temporal QD command stdout exceeded its 1048576 byte capture limit"
        )

    monkeypatch.setattr(control.native, "_run_checked", overflow)
    with pytest.raises(
        control.TemporalQDV5ControlPlaneError,
        match="stdout exceeded its 1048576 byte capture limit",
    ):
        control._run_pinned(
            runtime_authority={},
            role="gatewayDispatch",
            command=[str(binary)],
            timeout_seconds=1,
        )


def _archive_transport_descriptor(
    *, reported_path: str, archive_path: Path
) -> dict[str, Any]:
    descriptor = {
        "schemaVersion": "temporal_qd_archive_transport_descriptor_v1",
        "absolutePath": reported_path,
        "documentSchemaVersion": "temporal_qd_archive_v3",
        "archiveSha256": _sha("archive-semantic-root"),
        "fileSha256": _sha(archive_path.read_bytes()),
        "sizeBytes": archive_path.stat().st_size,
    }
    return _self_hashed(descriptor, "descriptorSha256")


def test_native_v5_initial_archive_certifier_accepts_its_ordinary_path_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Rust transport may use the ordinary path spelling passed to it."""

    runtime = _runtime(tmp_path)
    archive = (tmp_path / "initial-archive.json").resolve()
    archive.write_bytes(b"{}\n")
    descriptor = _archive_transport_descriptor(
        reported_path=str(archive), archive_path=archive
    )

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["role"] == "archiveReducer"
        assert kwargs["command"][-2:] == ["--certify-archive", str(archive)]
        return descriptor

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    assert control.certify_native_v5_initial_archive(
        runtime_authority=runtime, archive_path=archive
    ) == descriptor


@pytest.mark.skipif(os.name != "nt", reason="Windows extended-path ABI")
def test_native_v5_initial_archive_certifier_preserves_only_the_exact_extended_path_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Rust ``\\\\?\\`` spelling is equivalent, but no alias is admitted."""

    runtime = _runtime(tmp_path)
    archive = (tmp_path / "initial-archive.json").resolve()
    archive.write_bytes(b"{}\n")
    extended_path = "\\\\?\\" + str(archive)
    descriptor = _archive_transport_descriptor(
        reported_path=extended_path, archive_path=archive
    )
    monkeypatch.setattr(control, "_run_pinned", lambda **_kwargs: descriptor)

    certified = control.certify_native_v5_initial_archive(
        runtime_authority=runtime, archive_path=archive
    )
    # The self-hashed Rust document stays byte-for-byte intact.  Python only
    # compares its one platform spelling at the transport boundary.
    assert certified == descriptor
    assert certified["absolutePath"] == extended_path

    foreign = _archive_transport_descriptor(
        reported_path="\\\\?\\" + str((tmp_path / "foreign.json").resolve()),
        archive_path=archive,
    )
    monkeypatch.setattr(control, "_run_pinned", lambda **_kwargs: foreign)
    with pytest.raises(
        control.TemporalQDV5ControlPlaneError,
        match="initial archive transport descriptor drifted",
    ):
        control.certify_native_v5_initial_archive(
            runtime_authority=runtime, archive_path=archive
        )

    assert not control.native_v5_archive_transport_path_matches(
        "\\\\?\\UNC\\server\\share\\initial-archive.json", str(archive)
    )


def _descriptor(path: Path, *, semantic_sha256: str | None = None) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "rawSha256": _sha(path.read_bytes()),
        "sizeBytes": path.stat().st_size,
        **({"semanticSha256": semantic_sha256} if semantic_sha256 is not None else {}),
    }


def _evolved_v3_adapter(
    root: Path, *, manifest_padding: int = 0
) -> dict[str, Any]:
    """Build a tiny receipt-addressed v3 chain without candidate material."""

    proposal_root = (root / "proposal").resolve()
    manifest_body = {
        "schemaVersion": control.V5_PROPOSAL_MANIFEST_SCHEMA,
        "generationKind": "evolved",
        "outputRoot": str(proposal_root),
        "resultPath": control.V5_PROPOSAL_RESULT_FILENAME,
    }
    if manifest_padding:
        manifest_body["staticAuthorityPadding"] = "x" * manifest_padding
    manifest = _self_hashed(manifest_body, "manifestSha256")
    invocation_root = proposal_root / "native-batch" / "v5-proposal" / manifest[
        "manifestSha256"
    ].removeprefix("sha256:")
    manifest_path = invocation_root / "manifest.json"
    _write(manifest_path, manifest)
    result = _self_hashed(
        {
            "schemaVersion": control.V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
            "manifestSha256": manifest["manifestSha256"],
            "receiptSha256": _sha("proposal-receipt"),
            "outputInventorySha256": _sha("output-inventory"),
        },
        "resultSha256",
    )
    result_path = invocation_root / control.V5_PROPOSAL_RESULT_FILENAME
    _write(result_path, result)
    fragment = proposal_root / "v5-native" / "objects" / "sha256" / (
        _sha("fragment-semantic").removeprefix("sha256:") + ".json"
    )
    fragment.parent.mkdir(parents=True, exist_ok=True)
    fragment.write_bytes(b"opaque-fragment")
    fragment_semantic = _sha("fragment-semantic")
    return _self_hashed(
        {
            "schemaVersion": control.V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA,
            "operation": control.V5_PROPOSAL_OPERATION,
            "completed": True,
            "generationKind": "evolved",
            "generationIndex": 2,
            "generationConfigSha256": _sha("generation-config"),
            "authoritySha256": _sha("authority"),
            "attemptCount": 1,
            "acceptedCandidateCount": 1,
            "selectedEvaluationCandidateCount": 1,
            "publicationPlanSha256": _sha("plan"),
            "publicationRequestSha256": _sha("request"),
            "proposalResultSha256": result["resultSha256"],
            "proposalReceiptSha256": result["receiptSha256"],
            "outputInventorySha256": result["outputInventorySha256"],
            "population": {},
            "evaluationPopulation": {"semanticSha256": _sha("evaluation-population")},
            "generationJournal": {},
            "identityLedger": {},
            "evolvedPublicationFragments": {
                "schemaVersion": control.V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA,
                "coreSchemaVersion": control.V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA,
                "relativePath": "v5-native/objects/sha256/"
                + fragment_semantic.removeprefix("sha256:")
                + ".json",
                "absolutePath": str(fragment.resolve()),
                "semanticSha256": fragment_semantic,
                "fileSha256": _sha(fragment.read_bytes()),
                "byteLength": fragment.stat().st_size,
            },
            "nativeV5Invocation": {
                "schemaVersion": control.V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA,
                "proposalManifest": {
                    "schemaVersion": control.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
                    "documentSchemaVersion": control.V5_PROPOSAL_MANIFEST_SCHEMA,
                    "relativePath": "native-batch/v5-proposal/"
                    + manifest["manifestSha256"].removeprefix("sha256:")
                    + "/manifest.json",
                    "absolutePath": str(manifest_path.resolve()),
                    "semanticSha256": manifest["manifestSha256"],
                    "fileSha256": _sha(manifest_path.read_bytes()),
                    "byteLength": manifest_path.stat().st_size,
                },
                "proposalResult": {
                    "schemaVersion": control.V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
                    "documentSchemaVersion": control.V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
                    "relativePath": "native-batch/v5-proposal/"
                    + manifest["manifestSha256"].removeprefix("sha256:")
                    + "/v5-proposal-result.json",
                    "absolutePath": str(result_path.resolve()),
                    "semanticSha256": result["resultSha256"],
                    "fileSha256": _sha(result_path.read_bytes()),
                    "byteLength": result_path.stat().st_size,
                },
                "proposalReceiptSha256": result["receiptSha256"],
                "outputInventorySha256": result["outputInventorySha256"],
            },
        },
        "adapterSha256",
    )


def _gateway_receipt(
    *, root: Path, task_manifest: Path, task_count: int = 1
) -> tuple[dict[str, Any], dict[str, Any]]:
    checkpoint = root / "checkpoint.json"
    journal = root / ".native-gateway-dispatch" / "completion-journal.jsonl"
    checkpoint_sha, _ = _write(checkpoint, {"completed": {"task-0": {}}})
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.write_bytes(b'{"taskId":"task-0"}\n')
    journal_sha = _sha(journal.read_bytes())
    runtime_role = canonical_sha256(
        {
            "schemaVersion": "temporal_qd_native_gateway_runtime_role_v1",
            "runtimeEpoch": "temporal_qd_native_gateway_dispatch_epoch_v1",
            "binaryRole": "temporal-qd-gateway-dispatch",
        }
    )
    receipt = {
        "schemaVersion": control.GATEWAY_RECEIPT_SCHEMA,
        "runtimeRoleSha256": runtime_role,
        "authorityId": _sha("gateway-authority"),
        "taskMatrixSha256": _sha("task-matrix"),
        "sourceTaskManifestSha256": _sha(task_manifest.read_bytes()),
        "taskIndexRootSha256": _sha("task-index"),
        "completionJournalSemanticSha256": _sha("journal-semantic"),
        "checkpointSemanticSha256": _sha("checkpoint-semantic"),
        "completionJournalSha256": journal_sha,
        "checkpointSha256": checkpoint_sha,
        "taskCount": task_count,
        "completedTaskCount": task_count,
        "resultInventoryRootSha256": _sha("result-inventory-root"),
        "resultInventorySha256": _sha("result-inventory"),
        "resultInventorySizeBytes": 0,
        "resultInventoryCount": task_count,
    }
    semantic = {
        key: receipt[key]
        for key in receipt
        if key
        not in {
            "completionJournalSha256",
            "checkpointSha256",
            "semanticReceiptSha256",
            "receiptSha256",
        }
    }
    receipt["semanticReceiptSha256"] = canonical_sha256(semantic)
    receipt["receiptSha256"] = canonical_sha256(receipt)
    _write(root / ".native-gateway-dispatch" / "execution-receipt.json", receipt)
    result = {
        "schemaVersion": control.GATEWAY_RESULT_SCHEMA,
        "authorityId": receipt["authorityId"],
        "taskMatrixSha256": receipt["taskMatrixSha256"],
        "taskCount": task_count,
        "completedTaskCount": task_count,
        "taskIndexRootSha256": receipt["taskIndexRootSha256"],
        "checkpointPath": str(checkpoint),
        "sidecarRoot": str(root / ".native-gateway-dispatch"),
        "createdTaskSidecar": False,
        "executionReceiptSha256": receipt["receiptSha256"],
        "semanticExecutionReceiptSha256": receipt["semanticReceiptSha256"],
        "executionReceiptPath": str(
            root / ".native-gateway-dispatch" / "execution-receipt.json"
        ),
        "telemetry": {},
    }
    return receipt, result


def test_gateway_dispatch_requires_a_pinned_binary_and_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(tmp_path)
    task_manifest = tmp_path / "tasks.json"
    _write(task_manifest, {"schemaVersion": "fixture"})
    output_root = tmp_path / "gateway"
    calls: list[tuple[str, ...]] = []

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        calls.append(tuple(kwargs["command"]))
        _receipt, result = _gateway_receipt(
            root=output_root.resolve(), task_manifest=task_manifest.resolve()
        )
        return result

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    result = control.run_native_gateway_dispatch(
        runtime_authority=runtime,
        task_manifest_path=task_manifest.resolve(),
        output_root=output_root.resolve(),
        gateway_url="http://127.0.0.1:47831",
        mode="fresh",
        timeout_seconds=10,
    )
    assert result["receipt"]["completedTaskCount"] == 1
    assert calls and calls[0][0] == runtime["binaries"]["gatewayDispatch"]["path"]

    tampered = dict(runtime)
    tampered["binaries"] = dict(runtime["binaries"])
    tampered["binaries"].pop("archiveReducer")
    tampered["authoritySha256"] = canonical_sha256(
        {key: value for key, value in tampered.items() if key != "authoritySha256"}
    )
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="role set"):
        control.run_native_gateway_dispatch(
            runtime_authority=tampered,
            task_manifest_path=task_manifest.resolve(),
            output_root=(tmp_path / "blocked").resolve(),
            gateway_url="http://127.0.0.1:47831",
            mode="fresh",
            timeout_seconds=10,
        )


def test_evolved_v3_attempt_extraction_requires_the_sealed_invocation_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(tmp_path)
    adapter = _evolved_v3_adapter(tmp_path, manifest_padding=1_100_000)
    calls: list[tuple[str, ...]] = []

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = tuple(kwargs["command"])
        calls.append(command)
        assert command[1] == "extract-evolved-chain"
        chain = control._read_canonical_object(
            Path(command[2]), name="fixture evolved attempt chain"
        )
        assert chain["schemaVersion"] == control.EVOLVED_ATTEMPT_CHAIN_INPUT_SCHEMA
        assert set(chain) == {
            "schemaVersion",
            "contractVersion",
            "manifest",
            "result",
            "adapter",
            "inputSha256",
        }
        attempts = Path(command[3])
        attempts.write_bytes(b'{"opaque":"native-attempt"}\n')
        receipt = _self_hashed(
            {
                "schemaVersion": control.EVOLVED_ATTEMPT_STREAM_RECEIPT_SCHEMA,
                "inputSha256": chain["inputSha256"],
                "proposalResultSha256": adapter["proposalResultSha256"],
                "proposalReceiptSha256": adapter["proposalReceiptSha256"],
                "outputInventorySha256": adapter["outputInventorySha256"],
                "fragmentBundleSha256": _sha("fragment-bundle"),
                "evaluationPopulationSha256": adapter["evaluationPopulation"][
                    "semanticSha256"
                ],
                "attemptStream": {
                    "path": str(attempts),
                    "rawSha256": _sha(attempts.read_bytes()),
                    "sizeBytes": attempts.stat().st_size,
                    "recordCount": 1,
                    "rowSchema": control.EVOLVED_ATTEMPT_ROW_SCHEMA,
                },
                "proposalAccounting": {
                    "proposalAttemptCount": 1,
                    "originProposalCounts": {"random_immigrant": 1},
                    "dispositionCounts": {"accepted": 1},
                },
            },
            "receiptSha256",
        )
        _write(Path(command[4]), receipt)
        return receipt

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    original_raw_file_sha256 = control.raw_file_sha256

    fragment_path = Path(adapter["evolvedPublicationFragments"]["absolutePath"])
    original_path_read_bytes = Path.read_bytes

    def no_fragment_read(path: Path, *args: Any, **kwargs: Any) -> bytes:
        if path.resolve() == fragment_path.resolve():
            pytest.fail(
                "evolved extractor opened its Rust-authenticated publication fragment"
            )
        return original_path_read_bytes(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", no_fragment_read)

    def no_candidate_scale_hash(path: Path | str) -> str:
        if Path(path).resolve() in {
            (tmp_path / "attempts" / "proposal-attempts.jsonl").resolve(),
            fragment_path.resolve(),
        }:
            pytest.fail(
                "evolved extractor hashed a Rust-authenticated candidate-scale artifact"
            )
        return original_raw_file_sha256(path)

    monkeypatch.setattr(control, "raw_file_sha256", no_candidate_scale_hash)
    extracted = control.extract_native_v5_evolved_attempt_chain(
        runtime_authority=runtime,
        construction_adapter=adapter,
        output_root=(tmp_path / "attempts").resolve(),
    )
    assert calls
    assert extracted["proposalAttemptAuthority"] == {
        "kind": "evolved",
        "receiptPath": extracted["receiptPath"],
        "receiptFileSha256": _sha(Path(extracted["receiptPath"]).read_bytes()),
        "receiptSizeBytes": Path(extracted["receiptPath"]).stat().st_size,
        "receiptSha256": extracted["receipt"]["receiptSha256"],
    }
    assert "attemptStream" not in extracted
    assert "proposalAccounting" not in extracted

    old = dict(adapter)
    old["schemaVersion"] = "temporal_qd_native_v5_evolved_generation_construction_adapter_v2"
    old["adapterSha256"] = canonical_sha256(
        {key: value for key, value in old.items() if key != "adapterSha256"}
    )
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="incompatible"):
        control.extract_native_v5_evolved_attempt_chain(
            runtime_authority=runtime,
            construction_adapter=old,
            output_root=(tmp_path / "old").resolve(),
        )
    assert len(calls) == 1

    forged = _evolved_v3_adapter(tmp_path / "forged")
    forged["nativeV5Invocation"] = dict(forged["nativeV5Invocation"])
    forged["nativeV5Invocation"]["proposalResult"] = dict(
        forged["nativeV5Invocation"]["proposalResult"]
    )
    forged["nativeV5Invocation"]["proposalResult"]["relativePath"] = (
        "native-batch/v5-proposal/forged/manifest.json"
    )
    forged["adapterSha256"] = canonical_sha256(
        {key: value for key, value in forged.items() if key != "adapterSha256"}
    )
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="descriptor binding"):
        control.extract_native_v5_evolved_attempt_chain(
            runtime_authority=runtime,
            construction_adapter=forged,
            output_root=(tmp_path / "forged-attempts").resolve(),
        )
    assert len(calls) == 1


def test_g0_selected_attempt_extraction_uses_only_the_v2_receipt_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The deprecated G0 source command never receives a Python stream."""

    runtime = _runtime(tmp_path)
    manifest_path = tmp_path / "proposal" / "manifest.json"
    result_path = tmp_path / "proposal" / "v5-proposal-result.json"
    manifest = _self_hashed(
        {
            "schemaVersion": control.V5_PROPOSAL_MANIFEST_SCHEMA,
            # A real 8-candidate G0 static-authority manifest is already
            # slightly larger than the 1 MiB compact receipt/result budget.
            "staticAuthorityPadding": "x" * 1_100_000,
        },
        "manifestSha256",
    )
    selected_index = _sha("selected-projection-index")
    proposal_result = _self_hashed(
        {
            "schemaVersion": control.V5_PROPOSAL_RESULT_SCHEMA,
            "selectedProjectionIndexSha256": selected_index,
        },
        "resultSha256",
    )
    _write(manifest_path, manifest)
    _write(result_path, proposal_result)
    adapter = {
        "proposalResultSha256": proposal_result["resultSha256"],
        "proposalReceiptSha256": _sha("proposal-receipt"),
        "outputInventorySha256": _sha("output-inventory"),
        "selectedEvaluationCandidateCount": 1,
        "g0FunnelFragments": {"semanticSha256": _sha("g0-fragments")},
        "g0FunnelProjectionStream": {
            "stream": {"semanticSha256": _sha("projection-receipt")}
        },
    }
    manifest_descriptor = {
        "absolutePath": str(manifest_path.resolve()),
        "semanticSha256": manifest["manifestSha256"],
    }
    result_descriptor = {
        "absolutePath": str(result_path.resolve()),
        "semanticSha256": proposal_result["resultSha256"],
    }
    monkeypatch.setattr(
        control,
        "_validated_g0_adapter_chain",
        lambda **_kwargs: (adapter, manifest_descriptor, result_descriptor),
    )
    calls: list[tuple[str, ...]] = []

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = tuple(kwargs["command"])
        calls.append(command)
        assert command[1] == "extract-g0-selected-attempts"
        assert "extract-g0-funnel-source" not in command
        chain = control._read_canonical_object(
            Path(command[2]), name="fixture G0 selected-attempt chain"
        )
        assert chain["schemaVersion"] == control.G0_ATTEMPT_CHAIN_INPUT_SCHEMA
        attempts_path = Path(command[3])
        attempts_path.write_bytes(b'{"opaque":"native-selected-attempt"}\n')
        construction = {
            "proposalAttemptCount": 2,
            "acceptedCount": 1,
            "selectedCount": 1,
            "attemptJournalSha256": _sha("attempt-journal"),
            "acceptedPoolSha256": _sha("accepted-pool"),
            "selectionSha256": _sha("selection"),
            "campaignLedgerSha256": _sha("campaign-ledger"),
            "compactIdentityLedgerSha256": _sha("compact-ledger"),
        }
        receipt = _self_hashed(
            {
                "schemaVersion": control.G0_SELECTED_ATTEMPT_STREAM_RECEIPT_SCHEMA,
                "contractVersion": control.CONTRACT_VERSION,
                "generationIndex": 1,
                "inputSha256": chain["inputSha256"],
                "proposalManifestSha256": manifest["manifestSha256"],
                "proposalResultSha256": adapter["proposalResultSha256"],
                "proposalReceiptSha256": adapter["proposalReceiptSha256"],
                "outputInventorySha256": adapter["outputInventorySha256"],
                "g0FunnelFragmentsSha256": adapter["g0FunnelFragments"][
                    "semanticSha256"
                ],
                "g0FunnelProjectionStreamReceiptSha256": adapter[
                    "g0FunnelProjectionStream"
                ]["stream"]["semanticSha256"],
                "selectedProjectionIndexSha256": selected_index,
                "ordering": "candidate_id_ascending_v1",
                "attemptStream": {
                    "relativePath": "g0-selected-proposal-attempts.jsonl",
                    "rowSchema": control.G0_SELECTED_ATTEMPT_ROW_SCHEMA,
                    "rawSha256": _sha(attempts_path.read_bytes()),
                    "sizeBytes": attempts_path.stat().st_size,
                    "recordCount": 1,
                },
                "proposalAccounting": {
                    "proposalAttemptCount": 1,
                    "dispositionCounts": {"selected": 1},
                    "originProposalCounts": {"g0": 1},
                    "g0ConstructionProposalAccounting": construction,
                },
            },
            "receiptSha256",
        )
        _write(Path(command[4]), receipt)
        return receipt

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    original_raw_file_sha256 = control.raw_file_sha256

    def no_selected_stream_hash(path: Path | str) -> str:
        if Path(path).resolve() == (
            tmp_path / "selected-attempts" / "g0-selected-proposal-attempts.jsonl"
        ).resolve():
            pytest.fail("G0 extractor hashed its Rust-authenticated selected stream")
        return original_raw_file_sha256(path)

    monkeypatch.setattr(control, "raw_file_sha256", no_selected_stream_hash)
    extracted = control.extract_native_v5_g0_selected_attempts(
        runtime_authority=runtime,
        construction_adapter=adapter,
        output_root=(tmp_path / "selected-attempts").resolve(),
    )
    assert calls
    assert extracted["proposalAttemptAuthority"]["kind"] == "g0_selected"
    assert extracted["proposalAttemptAuthority"]["receiptSha256"] == extracted[
        "receipt"
    ]["receiptSha256"]
    assert "attemptStream" not in extracted
    assert "proposalAccounting" not in extracted


def test_funnel_assembly_passes_only_compact_native_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(tmp_path)
    attempt_receipt_path = tmp_path / "proposal-attempts-receipt.json"
    attempt_receipt = _self_hashed(
        {"schemaVersion": control.EVOLVED_ATTEMPT_STREAM_RECEIPT_SCHEMA},
        "receiptSha256",
    )
    _write(attempt_receipt_path, attempt_receipt)
    proposal_attempt_authority = {
        "kind": "evolved",
        "receiptPath": str(attempt_receipt_path.resolve()),
        "receiptFileSha256": _sha(attempt_receipt_path.read_bytes()),
        "receiptSizeBytes": attempt_receipt_path.stat().st_size,
        "receiptSha256": attempt_receipt["receiptSha256"],
    }
    index = tmp_path / "tail-result-index-v4.json"
    index_value = _self_hashed(
        {"schemaVersion": "temporal_qd_tail_result_index_v4"},
        "tailResultIndexSha256",
    )
    index.write_bytes(canonical_json_bytes(index_value))
    tail_authority = control.build_v5_directional_tail_authority(
        runtime_authority_sha256=runtime["authoritySha256"], generation_index=2
    )
    campaign_seal = _self_hashed(
        {
            "schemaVersion": control.CAMPAIGN_SEAL_SCHEMA,
            "tailResultIndex": {"sha256": index_value["tailResultIndexSha256"]},
        },
        "campaignSealSha256",
    )
    calls: list[tuple[str, ...]] = []

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = tuple(kwargs["command"])
        calls.append(command)
        assert command[1] == "assemble-funnel"
        input_value = control._read_canonical_object(
            Path(command[2]), name="fixture funnel input"
        )
        assert set(input_value) == {
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "proposalAttemptAuthority",
            "evaluationPanel",
            "tailAuthority",
            "campaignSeal",
            "tailResultIndex",
            "minimumTotalTrades",
            "minimumTradesPerWindow",
            "inputSha256",
        }
        assert input_value["proposalAttemptAuthority"] == proposal_attempt_authority
        assert "proposalAttemptStream" not in input_value
        assert "proposalAccounting" not in input_value
        source_path = Path(command[3])
        source_path.write_bytes(b"opaque-rust-funnel-source\n")
        input_path = Path(command[2])
        source_sha = _sha("funnel-source")
        receipt = _self_hashed(
            {
                "schemaVersion": control.FUNNEL_ASSEMBLY_RECEIPT_SCHEMA,
                "contractVersion": control.CONTRACT_VERSION,
                "generationIndex": 2,
                "input": {
                    "schemaVersion": "temporal_qd_v5_native_funnel_assembly_input_descriptor_v1",
                    "path": (
                        "\\\\?\\" + str(input_path)
                        if os.name == "nt"
                        else str(input_path)
                    ),
                    "rawSha256": _sha(input_path.read_bytes()),
                    "sizeBytes": input_path.stat().st_size,
                    "inputSha256": input_value["inputSha256"],
                },
                "source": {
                    "schemaVersion": "temporal_qd_v5_native_funnel_source_descriptor_v1",
                    "path": (
                        "\\\\?\\" + str(source_path)
                        if os.name == "nt"
                        else str(source_path)
                    ),
                    "rawSha256": _sha(source_path.read_bytes()),
                    "sizeBytes": source_path.stat().st_size,
                    "funnelSourceSha256": source_sha,
                },
                "proposalAttemptReceiptSha256": proposal_attempt_authority["receiptSha256"],
                "campaignSealSha256": campaign_seal["campaignSealSha256"],
                "tailResultIndexSha256": index_value["tailResultIndexSha256"],
                "tailAuthoritySha256": tail_authority["tailAuthoritySha256"],
            },
            "receiptSha256",
        )
        _write(input_path.parent / "funnel-assembly-receipt.json", receipt)
        return {
            "schemaVersion": control.FUNNEL_ASSEMBLY_EXECUTION_SCHEMA,
            "restart": False,
            "receipt": receipt,
        }

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    built = control.assemble_native_v5_funnel_reduction_source(
        runtime_authority=runtime,
        proposal_attempt_authority=proposal_attempt_authority,
        generation_index=2,
        evaluation_panel={"panelId": "panel-2"},
        campaign_seal=campaign_seal,
        tail_authority=tail_authority,
        tail_result_index={
            "path": str(index.resolve()),
            "relativePath": "tail-result-index-v4.json",
            "rawSha256": _sha(index.read_bytes()),
            "sizeBytes": index.stat().st_size,
            "tailResultIndexSha256": index_value["tailResultIndexSha256"],
        },
        minimum_total_trades=0,
        minimum_trades_per_window=0,
        output_root=(tmp_path / "funnel").resolve(),
    )
    assert calls
    assert built["assemblyReceiptBinding"]["receiptSha256"] == built[
        "assemblyReceipt"
    ]["receiptSha256"]
    assert "source" not in built

    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="schema drifted"):
        control.assemble_native_v5_funnel_reduction_source(
            runtime_authority=runtime,
            proposal_attempt_authority=proposal_attempt_authority,
            generation_index=2,
            evaluation_panel={"panelId": "panel-2"},
            campaign_seal=campaign_seal,
            tail_authority=tail_authority,
            tail_result_index={
                "path": str(index.resolve()),
                "relativePath": "tail-result-index-v3.json",
                "rawSha256": _sha(canonical_json_bytes(index_value)),
                "sizeBytes": len(canonical_json_bytes(index_value)),
                "tailResultIndexSha256": index_value["tailResultIndexSha256"],
            },
            minimum_total_trades=0,
            minimum_trades_per_window=0,
            output_root=(tmp_path / "tampered-funnel").resolve(),
        )
    assert len(calls) == 1


def _legacy_prefinalizer_v1_fixture_is_retained_for_oracle_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Base/resume never receives a Python candidate list or finalizer context."""

    runtime = _runtime(tmp_path)
    config_sha = _sha("supervisor-config")
    generation_sha = _sha("generation-config")
    parent = _self_hashed(
        {"schemaVersion": "fixture-parent-archive", "cells": []}, "archiveSha256"
    )
    parent_path = tmp_path / "parent-archive.json"
    _write(parent_path, parent)
    proposal_receipt = _self_hashed(
        {
            "schemaVersion": control.CAMPAIGN_RECEIPT_SCHEMA,
            "semanticReceiptSha256": _sha("proposal-campaign-semantic"),
            "generationIndex": 2,
            "campaignRole": "proposal_current_panel",
            "panelId": "panel-current",
        },
        "receiptSha256",
    )
    proposal_receipt_path = tmp_path / "proposal-campaign-receipt.json"
    _write(proposal_receipt_path, proposal_receipt)
    rotating = _self_hashed(
        {"schemaVersion": "temporal_qd_rotating_evidence_v1"},
        "rotatingEvidenceSha256",
    )
    policy = _self_hashed(
        {"schemaVersion": "temporal_qd_archive_policy_binding_v1"},
        "policyBindingSha256",
    )
    funnel = _self_hashed(
        {"schemaVersion": control.FUNNEL_REDUCTION_SOURCE_SCHEMA},
        "funnelSourceSha256",
    )
    funnel_input = _self_hashed(
        {
            "schemaVersion": control.FUNNEL_REDUCTION_INPUT_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "generationIndex": 2,
            "proposalAttemptAuthority": {},
            "evaluationPanel": {},
            "tailAuthority": {},
            "campaignSeal": {},
            "tailResultIndex": {},
            "minimumTotalTrades": 0,
            "minimumTradesPerWindow": 0,
        },
        "inputSha256",
    )
    completed_records: list[dict[str, Any]] = []
    adapter = _evolved_v3_adapter(tmp_path)
    native_invocation = adapter["nativeV5Invocation"]
    proposal_manifest_sha = native_invocation["proposalManifest"]["semanticSha256"]
    proposal_receipt_sha = adapter["proposalReceiptSha256"]
    generation_journal_sha = _sha("generation-journal")
    output_ledger_sha = _sha("ledger")
    output_ledger_file_sha = _sha("ledger-file")
    proposal_state_authority = {
        "generationKind": "evolved",
        "proposalManifestSha256": proposal_manifest_sha,
        "proposalReceiptSha256": proposal_receipt_sha,
        "generationJournalSha256": generation_journal_sha,
        "inputIdentityLedgerSha256": _sha("input-ledger"),
        "outputIdentityLedgerRelativePath": "proposal/v5-native/identity-ledger.json",
        "outputIdentityLedgerSha256": output_ledger_sha,
        "outputIdentityLedgerFileSha256": output_ledger_file_sha,
    }
    state_basis = _self_hashed(
        {
            "schemaVersion": "temporal_qd_v5_generation_state_basis_v1",
            "configSha256": config_sha,
            "generationIndex": 2,
            "completedGenerationsSha256": canonical_sha256(completed_records),
            "uniqueCandidatesEvaluated": 1,
            "workerTasksCompleted": 1,
            "nextImmigrantContinuationOrdinal": 0,
            "uniqueIdentityCounts": {},
            "duplicateCounters": {},
            "proposalSlotCounters": {},
        },
        "stateBasisSha256",
    )
    base_kwargs = {
        "runtime_authority": runtime,
        "output_root": (tmp_path / "prefinalizer" / "base").resolve(),
        "generation_index": 2,
        "supervisor_config_sha256": config_sha,
        "generation_config_sha256": generation_sha,
        "state_basis": state_basis,
        "completed_generation_records": completed_records,
        "proposal_state_authority": proposal_state_authority,
        "rotating_evidence": rotating,
        "archive_policy_authority": policy,
        "proposal_semantic_roots": {
            "proposalReceiptSha256": proposal_receipt_sha,
            "generationJournalSha256": generation_journal_sha,
        },
        "identity_ledger_sha256": output_ledger_sha,
        "native_v5_invocation": native_invocation,
        "funnel_reduction_input": funnel_input,
        "funnel_reduction_source": funnel,
        "previous_parent_archive_path": parent_path.resolve(),
        "previous_parent_archive_sha256": parent["archiveSha256"],
        "previous_cumulative_archive_path": None,
        "previous_cumulative_archive_sha256": None,
        "proposal_campaign_receipt_path": proposal_receipt_path.resolve(),
        "finalizer_output_root": (tmp_path / "generation-finalization").resolve(),
    }
    base = control.build_native_v5_prefinalizer_base_manifest(**base_kwargs)
    assert base["manifest"]["proposalConstructionBinding"][
        "funnelReductionInput"
    ] == funnel_input
    assert base["manifest"]["proposalConstructionBinding"][
        "funnelReductionSource"
    ] == funnel

    def copied_invocation() -> dict[str, Any]:
        return {
            **native_invocation,
            "proposalManifest": dict(native_invocation["proposalManifest"]),
            "proposalResult": dict(native_invocation["proposalResult"]),
        }

    # The base builder reopens the two fixed invocation documents itself.  A
    # descriptor may not redirect a Rust transaction through a path alias,
    # stale bytes, a different document schema, or a forged document root.
    aliased_invocation = copied_invocation()
    aliased_invocation["proposalManifest"]["relativePath"] = (
        "native-batch/v5-proposal/../manifest.json"
    )
    aliased_kwargs = dict(base_kwargs)
    aliased_kwargs["native_v5_invocation"] = aliased_invocation
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="relative path"):
        control.build_native_v5_prefinalizer_base_manifest(**aliased_kwargs)

    invocation_manifest_path = Path(
        native_invocation["proposalManifest"]["absolutePath"]
    )
    original_manifest_bytes = invocation_manifest_path.read_bytes()
    invocation_manifest_path.write_bytes(original_manifest_bytes + b" ")
    try:
        with pytest.raises(control.TemporalQDV5ControlPlaneError, match="descriptor drifted"):
            control.build_native_v5_prefinalizer_base_manifest(**base_kwargs)
    finally:
        invocation_manifest_path.write_bytes(original_manifest_bytes)

    wrong_schema_invocation = copied_invocation()
    wrong_schema_invocation["proposalResult"]["documentSchemaVersion"] = "fixture-result"
    wrong_schema_kwargs = dict(base_kwargs)
    wrong_schema_kwargs["native_v5_invocation"] = wrong_schema_invocation
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="binding drifted"):
        control.build_native_v5_prefinalizer_base_manifest(**wrong_schema_kwargs)

    original_manifest = control._read_canonical_object(
        invocation_manifest_path, name="fixture proposal invocation manifest"
    )
    forged_manifest = dict(original_manifest)
    forged_manifest["manifestSha256"] = _sha("forged-manifest-root")
    _write(invocation_manifest_path, forged_manifest)
    forged_invocation = copied_invocation()
    forged_invocation["proposalManifest"]["fileSha256"] = _sha(
        invocation_manifest_path.read_bytes()
    )
    forged_invocation["proposalManifest"]["byteLength"] = invocation_manifest_path.stat().st_size
    forged_kwargs = dict(base_kwargs)
    forged_kwargs["native_v5_invocation"] = forged_invocation
    try:
        with pytest.raises(control.TemporalQDV5ControlPlaneError, match="identity drifted"):
            control.build_native_v5_prefinalizer_base_manifest(**forged_kwargs)
    finally:
        _write(invocation_manifest_path, original_manifest)

    missing_input_kwargs = dict(base_kwargs)
    missing_input_kwargs["funnel_reduction_input"] = {
        key: value for key, value in funnel_input.items() if key != "inputSha256"
    }
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="funnel reduction input"):
        control.build_native_v5_prefinalizer_base_manifest(**missing_input_kwargs)

    replaced_input = dict(funnel_input)
    replaced_input["generationIndex"] = 3
    replaced_input["inputSha256"] = canonical_sha256(
        {key: value for key, value in replaced_input.items() if key != "inputSha256"}
    )
    replaced_input_kwargs = dict(base_kwargs)
    replaced_input_kwargs["funnel_reduction_input"] = replaced_input
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="funnel input schema"):
        control.build_native_v5_prefinalizer_base_manifest(**replaced_input_kwargs)

    missing_source_kwargs = dict(base_kwargs)
    missing_source_kwargs["funnel_reduction_source"] = {}
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="funnel reduction source"):
        control.build_native_v5_prefinalizer_base_manifest(**missing_source_kwargs)

    # Funnel assembly is deliberately Rust-owned.  Python transports the
    # independently self-hashed input and source verbatim, leaving the pinned
    # prefinalizer to reassemble the input and reject this divergent source.
    divergent_funnel = _self_hashed(
        {
            "schemaVersion": control.FUNNEL_REDUCTION_SOURCE_SCHEMA,
            "fixtureDivergence": "must-reach-rust",
        },
        "funnelSourceSha256",
    )
    divergent_kwargs = dict(base_kwargs)
    divergent_kwargs["output_root"] = (tmp_path / "prefinalizer" / "divergent").resolve()
    divergent_kwargs["funnel_reduction_source"] = divergent_funnel
    divergent = control.build_native_v5_prefinalizer_base_manifest(**divergent_kwargs)
    assert divergent["manifest"]["proposalConstructionBinding"][
        "funnelReductionInput"
    ] == funnel_input
    assert divergent["manifest"]["proposalConstructionBinding"][
        "funnelReductionSource"
    ] == divergent_funnel

    calls: list[tuple[str, ...]] = []

    def write_awaiting_result(manifest_path: Path, *, round_index: int) -> dict[str, Any]:
        manifest = control._read_canonical_object(
            manifest_path, name="fixture prefinalizer manifest"
        )
        root = manifest_path.parent
        candidate_rows = root / f"task-candidates/round-{round_index}-task-0.jsonl"
        candidate_rows.parent.mkdir(parents=True, exist_ok=True)
        candidate_rows.write_bytes(
            canonical_json_bytes(
                {
                    "candidateId": "fixture-candidate",
                    "candidateIdentitySha256": _sha("fixture-candidate"),
                }
            )
            + b"\n"
        )
        candidate_set = _sha(f"candidate-set-{round_index}")
        if manifest["schemaVersion"] == control.PREFINALIZER_BASE_MANIFEST_SCHEMA:
            task_authority = manifest["semanticAuthoritySha256"]
        else:
            task_authority = control._read_canonical_object(
                Path(manifest["baseManifestBinding"]["path"]),
                name="fixture task base manifest",
            )["semanticAuthoritySha256"]
        descriptor = _self_hashed(
            {
                "schemaVersion": control.PREFINALIZER_TASK_DESCRIPTOR_SCHEMA,
                "path": f"task-candidates/round-{round_index}-task-0.jsonl",
                "rawSha256": _sha(candidate_rows.read_bytes()),
                "sizeBytes": candidate_rows.stat().st_size,
                "recordCount": 1,
                "rowSchema": "temporal_qd_selected_rich_candidate_v1",
                "candidateSetSha256": candidate_set,
                "inputAuthoritySha256": task_authority,
            },
            "descriptorSha256",
        )
        task = _self_hashed(
            {
                "taskOrdinal": 0,
                "campaignRole": "retained_parent_current_panel",
                "panelId": "panel-current",
                "rotatingEvidenceSha256": rotating["rotatingEvidenceSha256"],
                "cohortSelection": {
                    "schemaVersion": control.PREFINALIZER_TASK_SELECTION_SCHEMA,
                    "candidateSetSha256": candidate_set,
                    "candidateRows": descriptor,
                },
                "candidateCount": 1,
                "candidateSetSha256": candidate_set,
                "sourceAuthority": {"fixture": "retained-parent"},
                "selectionDocumentSchema": (
                    control.PREFINALIZER_TASK_SELECTION_DOCUMENT_SCHEMA
                ),
                "selectionDocumentRelativePath": (
                    f"task-selections/round-{round_index}-task-0.selection.json"
                ),
                "selectionReceiptRelativePath": (
                    f"task-selections/round-{round_index}-task-0.receipt.json"
                ),
            },
            "taskSha256",
        )
        if manifest["schemaVersion"] == control.PREFINALIZER_BASE_MANIFEST_SCHEMA:
            base_manifest_sha = manifest["manifestSha256"]
            previous_result_sha = None
            semantic_authority = manifest["semanticAuthoritySha256"]
            generation = manifest["generationIndex"]
            funnel_source = manifest["proposalConstructionBinding"]["funnelReductionSource"]
        else:
            base_manifest = control._read_canonical_object(
                Path(manifest["baseManifestBinding"]["path"]),
                name="fixture base manifest",
            )
            previous = control._read_canonical_object(
                Path(manifest["previousResultBinding"]["path"]),
                name="fixture prior result",
            )
            base_manifest_sha = base_manifest["manifestSha256"]
            previous_result_sha = previous["resultSha256"]
            semantic_authority = base_manifest["semanticAuthoritySha256"]
            generation = base_manifest["generationIndex"]
            funnel_source = base_manifest["proposalConstructionBinding"][
                "funnelReductionSource"
            ]
        plan = _self_hashed(
            {
                "schemaVersion": control.PREFINALIZER_TASK_PLAN_SCHEMA,
                "contractVersion": control.CONTRACT_VERSION,
                "semanticAuthoritySha256": semantic_authority,
                "generationIndex": generation,
                "roundIndex": round_index,
                "phase": "retained_parent_current_panel",
                "tasks": [task],
                "taskCount": 1,
            },
            "taskPlanSha256",
        )
        result = _self_hashed(
            {
                "schemaVersion": control.PREFINALIZER_RESULT_SCHEMA,
                "contractVersion": control.CONTRACT_VERSION,
                "baseManifestSha256": base_manifest_sha,
                "manifestSha256": manifest["manifestSha256"],
                "semanticAuthoritySha256": semantic_authority,
                "roundIndex": round_index,
                "previousResultSha256": previous_result_sha,
                "generationIndex": generation,
                "status": "awaiting_retained_parent_current_panel",
                "admittedCampaignLedger": {},
                "cohort": {},
                "provisional": {},
                "panelCoverage": {},
                "taskPlan": plan,
                "funnelReductionSource": funnel_source,
                "selectedRichMembers": None,
                "finalizerSource": None,
                "finalizerManifest": None,
            },
            "resultSha256",
        )
        _write(root / "result.json", result)
        selection_document = _self_hashed(
            {
                "schemaVersion": control.PREFINALIZER_TASK_SELECTION_DOCUMENT_SCHEMA,
                "prefinalizerResultSha256": result["resultSha256"],
                "taskPlanSha256": plan["taskPlanSha256"],
                "taskSha256": task["taskSha256"],
                "semanticAuthoritySha256": semantic_authority,
                "generationIndex": generation,
                "roundIndex": round_index,
                "campaignRole": task["campaignRole"],
                "panelId": task["panelId"],
                "rotatingEvidenceSha256": task["rotatingEvidenceSha256"],
                "candidateSetSha256": candidate_set,
                "candidateRows": descriptor,
                "sourceAuthority": task["sourceAuthority"],
                "selectionReceiptRelativePath": task["selectionReceiptRelativePath"],
            },
            "selectionDocumentSha256",
        )
        selection_receipt = _self_hashed(
            {
                "schemaVersion": control.PREFINALIZER_TASK_SELECTION_RECEIPT_SCHEMA,
                "selectionDocumentSha256": selection_document[
                    "selectionDocumentSha256"
                ],
                "prefinalizerResultSha256": result["resultSha256"],
                "taskPlanSha256": plan["taskPlanSha256"],
                "taskSha256": task["taskSha256"],
                "semanticAuthoritySha256": semantic_authority,
                "generationIndex": generation,
                "roundIndex": round_index,
                "campaignRole": task["campaignRole"],
                "panelId": task["panelId"],
                "rotatingEvidenceSha256": task["rotatingEvidenceSha256"],
                "candidateSetSha256": candidate_set,
                "candidateRowsSha256": descriptor["descriptorSha256"],
            },
            "receiptSha256",
        )
        _write(root / task["selectionDocumentRelativePath"], selection_document)
        _write(root / task["selectionReceiptRelativePath"], selection_receipt)
        return result

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = tuple(kwargs["command"])
        calls.append(command)
        assert command[0] == runtime["binaries"]["rotatingPrefinalizer"]["path"]
        assert len(command) == 2
        manifest_path = Path(command[1])
        manifest = control._read_canonical_object(
            manifest_path, name="fixture invoked prefinalizer manifest"
        )
        round_index = 0 if manifest["schemaVersion"] == control.PREFINALIZER_BASE_MANIFEST_SCHEMA else manifest["roundIndex"]
        result = write_awaiting_result(manifest_path, round_index=round_index)
        return {
            "schemaVersion": control.PREFINALIZER_EXECUTION_SCHEMA,
            "restart": False,
            "result": result,
        }

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    first = control.run_native_v5_rotating_prefinalizer(
        runtime_authority=runtime, manifest_path=Path(base["manifestPath"])
    )
    assert first["result"]["roundIndex"] == 0
    assert first["taskSelections"][0]["selectionPath"].endswith(
        "round-0-task-0.selection.json"
    )
    # The control bridge authenticates only compact document/receipt roots;
    # it must never reopen the rich candidate JSONL sidecar that freezer owns.
    def no_candidate_sidecar(*_args: Any, **_kwargs: Any) -> Path:
        pytest.fail("v2 task-selection bridge reopened candidate JSONL")

    original_require_bound_file = control._require_bound_file
    monkeypatch.setattr(control, "_require_bound_file", no_candidate_sidecar)
    base_manifest = control._read_canonical_object(
        Path(base["manifestPath"]), name="fixture prefinalizer base manifest"
    )
    control._validated_native_v5_prefinalizer_result(
        root=Path(base["manifestPath"]).parent,
        value=first["result"],
        manifest=base_manifest,
    )
    selection_path = Path(first["taskSelections"][0]["selectionPath"])
    selection_receipt_path = Path(first["taskSelections"][0]["selectionReceiptPath"])
    selection_document = control._read_canonical_object(
        selection_path, name="fixture v2 selection document"
    )
    bad_document = dict(selection_document)
    bad_document["candidateSetSha256"] = _sha("substituted-selection-set")
    bad_document["selectionDocumentSha256"] = canonical_sha256(
        {
            key: value
            for key, value in bad_document.items()
            if key != "selectionDocumentSha256"
        }
    )
    _write(selection_path, bad_document)
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="document binding"):
        control._validated_native_v5_prefinalizer_result(
            root=Path(base["manifestPath"]).parent,
            value=first["result"],
            manifest=base_manifest,
        )
    _write(selection_path, selection_document)
    selection_receipt = control._read_canonical_object(
        selection_receipt_path, name="fixture v2 selection receipt"
    )
    bad_receipt = dict(selection_receipt)
    bad_receipt["candidateRowsSha256"] = _sha("substituted-selection-rows")
    bad_receipt["receiptSha256"] = canonical_sha256(
        {key: value for key, value in bad_receipt.items() if key != "receiptSha256"}
    )
    _write(selection_receipt_path, bad_receipt)
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="receipt binding"):
        control._validated_native_v5_prefinalizer_result(
            root=Path(base["manifestPath"]).parent,
            value=first["result"],
            manifest=base_manifest,
        )
    _write(selection_receipt_path, selection_receipt)
    monkeypatch.setattr(control, "_require_bound_file", original_require_bound_file)

    # A ready result may name finalizer artifacts only at the separately
    # sealed generation finalization root, never beneath a prefinalizer round.
    finalizer_root = Path(base_manifest["finalizerOutputRoot"])
    finalizer_source = _self_hashed(
        {"schemaVersion": control.FINALIZER_SOURCE_SCHEMA}, "sourceSha256"
    )
    finalizer_manifest = _self_hashed(
        {"schemaVersion": control.FINALIZER_MANIFEST_SCHEMA}, "manifestSha256"
    )
    _write(finalizer_root / "source.json", finalizer_source)
    _write(finalizer_root / "manifest.json", finalizer_manifest)
    ready_plan = _self_hashed(
        {
            "schemaVersion": control.PREFINALIZER_TASK_PLAN_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "semanticAuthoritySha256": base_manifest["semanticAuthoritySha256"],
            "generationIndex": base_manifest["generationIndex"],
            "roundIndex": 0,
            "phase": "ready_for_finalizer",
            "tasks": [],
            "taskCount": 0,
        },
        "taskPlanSha256",
    )
    selected = _self_hashed(
        {
            "schemaVersion": "temporal_qd_selected_rich_members_v1",
            "generationIndex": base_manifest["generationIndex"],
            "cohortSha256": _sha("ready-cohort"),
            "provisionalSha256": _sha("ready-provisional"),
            "members": [],
            "memberCount": 0,
        },
        "selectedRichMembersSha256",
    )
    ready_result = _self_hashed(
        {
            "schemaVersion": control.PREFINALIZER_RESULT_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "baseManifestSha256": base_manifest["manifestSha256"],
            "manifestSha256": base_manifest["manifestSha256"],
            "semanticAuthoritySha256": base_manifest["semanticAuthoritySha256"],
            "roundIndex": 0,
            "previousResultSha256": None,
            "generationIndex": base_manifest["generationIndex"],
            "status": "ready_for_finalizer",
            "admittedCampaignLedger": {},
            "cohort": {},
            "provisional": {},
            "panelCoverage": {},
            "taskPlan": ready_plan,
            "funnelReductionSource": funnel,
            "selectedRichMembers": selected,
            "finalizerSource": {
                "path": str(finalizer_root / "source.json"),
                "sha256": finalizer_source["sourceSha256"],
            },
            "finalizerManifest": {
                "path": str(finalizer_root / "manifest.json"),
                "sha256": finalizer_manifest["manifestSha256"],
            },
        },
        "resultSha256",
    )
    control._validated_native_v5_prefinalizer_result(
        root=Path(base["manifestPath"]).parent,
        value=ready_result,
        manifest=base_manifest,
    )
    bad_ready = dict(ready_result)
    bad_ready["finalizerManifest"] = {
        "path": str(Path(base["manifestPath"]).parent / "finalizer" / "manifest.json"),
        "sha256": finalizer_manifest["manifestSha256"],
    }
    bad_ready["resultSha256"] = canonical_sha256(
        {key: value for key, value in bad_ready.items() if key != "resultSha256"}
    )
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="finalizer manifest binding"):
        control._validated_native_v5_prefinalizer_result(
            root=Path(base["manifestPath"]).parent,
            value=bad_ready,
            manifest=base_manifest,
        )
    resumed_receipt = _self_hashed(
        {
            "schemaVersion": control.CAMPAIGN_RECEIPT_SCHEMA,
            "generationIndex": 2,
            "campaignRole": "retained_parent_current_panel",
            "panelId": "panel-current",
        },
        "receiptSha256",
    )
    resumed_receipt_path = tmp_path / "resumed-campaign-receipt.json"
    _write(resumed_receipt_path, resumed_receipt)
    resume = control.build_native_v5_prefinalizer_resume_manifest(
        runtime_authority=runtime,
        output_root=(tmp_path / "prefinalizer" / "round-0001").resolve(),
        base_manifest_path=Path(base["manifestPath"]),
        previous_result_path=Path(first["resultPath"]),
        new_campaign_receipt_paths=(resumed_receipt_path.resolve(),),
    )
    second = control.run_native_v5_rotating_prefinalizer(
        runtime_authority=runtime, manifest_path=Path(resume["manifestPath"])
    )
    assert second["result"]["previousResultSha256"] == first["result"]["resultSha256"]
    assert second["result"]["roundIndex"] == 1
    assert len(calls) == 2


def test_native_prefinalizer_v2_transports_only_compact_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The current v2 seam never opens a result, selection stream, or source."""

    runtime = _runtime(tmp_path)
    config_sha = _sha("supervisor-config")
    generation_sha = _sha("generation-config")
    records: list[dict[str, Any]] = []
    adapter = _evolved_v3_adapter(tmp_path, manifest_padding=1_100_000)
    invocation = adapter["nativeV5Invocation"]
    proposal_state = {
        "generationKind": "evolved",
        "proposalManifestSha256": invocation["proposalManifest"]["semanticSha256"],
        "proposalReceiptSha256": adapter["proposalReceiptSha256"],
        "generationJournalSha256": _sha("generation-journal"),
        "inputIdentityLedgerSha256": _sha("input-ledger"),
        "outputIdentityLedgerRelativePath": "proposal/v5-native/identity-ledger.json",
        "outputIdentityLedgerSha256": _sha("output-ledger"),
        "outputIdentityLedgerFileSha256": _sha("output-ledger-file"),
    }
    state_basis = _self_hashed(
        {
            "schemaVersion": "temporal_qd_v5_generation_state_basis_v1",
            "configSha256": config_sha,
            "generationIndex": 2,
            "completedGenerationsSha256": canonical_sha256(records),
            "uniqueCandidatesEvaluated": 0,
            "workerTasksCompleted": 0,
            "nextImmigrantContinuationOrdinal": 0,
            "uniqueIdentityCounts": {},
            "duplicateCounters": {},
            "proposalSlotCounters": {},
        },
        "stateBasisSha256",
    )
    proposal_receipt_path = tmp_path / "proposal-campaign-receipt.json"
    proposal_receipt = _self_hashed(
        {
            "schemaVersion": control.CAMPAIGN_RECEIPT_SCHEMA,
            "semanticReceiptSha256": _sha("campaign-semantic"),
        },
        "receiptSha256",
    )
    _write(proposal_receipt_path, proposal_receipt)
    parent_path = tmp_path / "parent-archive.json"
    parent_path.write_bytes(b"opaque-parent-archive")
    funnel_receipt_path = tmp_path / "funnel-assembly-receipt.json"
    funnel_receipt_path.write_bytes(b"opaque-funnel-assembly-receipt")
    funnel_input = _self_hashed(
        {
            "schemaVersion": control.FUNNEL_REDUCTION_INPUT_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "generationIndex": 2,
            "proposalAttemptAuthority": {},
            "evaluationPanel": {},
            "tailAuthority": {},
            "campaignSeal": {},
            "tailResultIndex": {},
            "minimumTotalTrades": 0,
            "minimumTradesPerWindow": 0,
        },
        "inputSha256",
    )
    base = control.build_native_v5_prefinalizer_base_manifest(
        runtime_authority=runtime,
        output_root=(tmp_path / "prefinalizer" / "base").resolve(),
        generation_index=2,
        supervisor_config_sha256=config_sha,
        generation_config_sha256=generation_sha,
        state_basis=state_basis,
        completed_generation_records=records,
        proposal_state_authority=proposal_state,
        rotating_evidence=_self_hashed(
            {"schemaVersion": "temporal_qd_rotating_evidence_v1"},
            "rotatingEvidenceSha256",
        ),
        archive_policy_authority=_self_hashed(
            {"schemaVersion": "temporal_qd_archive_policy_binding_v1"},
            "policyBindingSha256",
        ),
        proposal_semantic_roots={
            "proposalReceiptSha256": proposal_state["proposalReceiptSha256"],
            "generationJournalSha256": proposal_state["generationJournalSha256"],
        },
        identity_ledger_sha256=proposal_state["outputIdentityLedgerSha256"],
        native_v5_invocation=invocation,
        funnel_reduction_input=funnel_input,
        funnel_assembly_receipt_binding={
            "schemaVersion": control.FUNNEL_ASSEMBLY_RECEIPT_BINDING_SCHEMA,
            "path": str(funnel_receipt_path.resolve()),
            "rawSha256": _sha(funnel_receipt_path.read_bytes()),
            "sizeBytes": funnel_receipt_path.stat().st_size,
            "receiptSha256": _sha("funnel-assembly-semantic"),
        },
        previous_parent_archive_binding={
            "path": str(parent_path.resolve()),
            "rawSha256": _sha(parent_path.read_bytes()),
            "sizeBytes": parent_path.stat().st_size,
            "archiveSha256": _sha("parent-archive"),
        },
        previous_cumulative_archive_binding=None,
        proposal_campaign_receipt_path=proposal_receipt_path.resolve(),
        finalizer_output_root=(tmp_path / "finalizer").resolve(),
    )
    construction = base["manifest"]["proposalConstructionBinding"]
    assert construction["funnelReductionInput"] == funnel_input
    assert "funnelReductionSource" not in construction
    assert construction["funnelAssemblyReceiptBinding"]["schemaVersion"] == (
        control.FUNNEL_ASSEMBLY_RECEIPT_BINDING_SCHEMA
    )

    calls: list[tuple[str, ...]] = []

    def descriptor(
        *, schema: str, path: Path, field: str, semantic: str
    ) -> dict[str, Any]:
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"opaque-rust-sidecar")
        resolved = str(path.resolve())
        transport_path = (
            resolved
            if os.name != "nt" or resolved.startswith("\\\\?\\")
            else "\\\\?\\" + resolved
        )
        return {
            "schemaVersion": schema,
            "path": transport_path,
            "rawSha256": _sha(path.read_bytes()),
            "sizeBytes": path.stat().st_size,
            field: semantic,
        }

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = tuple(kwargs["command"])
        calls.append(command)
        manifest_path = Path(command[1])
        manifest = control._read_canonical_object(
            manifest_path, name="fixture v2 prefinalizer manifest"
        )
        root = manifest_path.parent
        is_base = manifest["schemaVersion"] == control.PREFINALIZER_BASE_MANIFEST_SCHEMA
        round_index = 0 if is_base else manifest["roundIndex"]
        status = "awaiting_retained_parent_current_panel" if is_base else "ready_for_finalizer"
        internal_path = root / "result.json"
        internal = descriptor(
            schema="temporal_qd_v5_prefinalizer_internal_result_descriptor_v1",
            path=internal_path,
            field="resultSha256",
            semantic=_sha(f"internal-result-{round_index}"),
        )
        if is_base:
            selection_path = root / "task-selections" / "round-0-task-0.selection.json"
            selection_receipt_path = root / "task-selections" / "round-0-task-0.receipt.json"
            selections = [
                {
                    "taskOrdinal": 0,
                    "campaignRole": "retained_parent_current_panel",
                    "panelId": "panel-current",
                    "candidateCount": 1,
                    "candidateSetSha256": _sha("candidate-set"),
                    "selectionDocument": descriptor(
                        schema="temporal_qd_v5_prefinalizer_selection_document_descriptor_v1",
                        path=selection_path,
                        field="selectionDocumentSha256",
                        semantic=_sha("selection-document"),
                    ),
                    "selectionReceipt": descriptor(
                        schema="temporal_qd_v5_prefinalizer_selection_receipt_descriptor_v1",
                        path=selection_receipt_path,
                        field="receiptSha256",
                        semantic=_sha("selection-receipt"),
                    ),
                }
            ]
            finalizer_source = None
            finalizer_manifest = None
        else:
            selections = []
            finalizer_root = Path(base["manifest"]["finalizerOutputRoot"])
            finalizer_source = descriptor(
                schema="temporal_qd_v5_prefinalizer_finalizer_source_descriptor_v1",
                path=(finalizer_root / "source.json"),
                field="sourceSha256",
                semantic=_sha("finalizer-source"),
            )
            finalizer_manifest = descriptor(
                schema="temporal_qd_v5_prefinalizer_finalizer_manifest_descriptor_v1",
                path=(finalizer_root / "manifest.json"),
                field="manifestSha256",
                semantic=_sha("finalizer-manifest"),
            )
        receipt = _self_hashed(
            {
                "schemaVersion": control.PREFINALIZER_EXECUTION_RECEIPT_SCHEMA,
                "contractVersion": control.CONTRACT_VERSION,
                "inputManifest": descriptor(
                    schema="temporal_qd_v5_prefinalizer_input_manifest_descriptor_v1",
                    path=manifest_path,
                    field="manifestSha256",
                    semantic=manifest["manifestSha256"],
                ),
                "internalResult": internal,
                "status": status,
                "generationIndex": 2,
                "roundIndex": round_index,
                "semanticAuthoritySha256": (
                    manifest["semanticAuthoritySha256"]
                    if is_base
                    else base["manifest"]["semanticAuthoritySha256"]
                ),
                "baseManifestSha256": base["manifest"]["manifestSha256"],
                "previousResultSha256": None if is_base else _sha("internal-result-0"),
                "taskPlanSha256": _sha(f"task-plan-{round_index}"),
                "taskCount": len(selections),
                "taskSelections": selections,
                "finalizerSource": finalizer_source,
                "finalizerManifest": finalizer_manifest,
            },
            "receiptSha256",
        )
        _write(root / "execution-receipt.json", receipt)
        return {
            "schemaVersion": control.PREFINALIZER_EXECUTION_SCHEMA,
            "restart": False,
            "receipt": receipt,
        }

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    first = control.run_native_v5_rotating_prefinalizer(
        runtime_authority=runtime, manifest_path=Path(base["manifestPath"])
    )
    assert first["receipt"]["status"] == "awaiting_retained_parent_current_panel"
    assert first["taskSelections"][0]["selectionPath"].endswith("selection.json")
    # `result.json` is intentionally opaque; resume relies on the receipt's
    # descriptor even after a committed restart has discarded it.
    Path(first["internalResultBinding"]["path"]).unlink()
    campaign_receipt_path = tmp_path / "round-campaign-receipt.json"
    _write(
        campaign_receipt_path,
        _self_hashed(
            {
                "schemaVersion": control.CAMPAIGN_RECEIPT_SCHEMA,
                "generationIndex": 2,
                "campaignRole": "retained_parent_current_panel",
                "panelId": "panel-current",
            },
            "receiptSha256",
        ),
    )
    resume = control.build_native_v5_prefinalizer_resume_manifest(
        runtime_authority=runtime,
        output_root=(tmp_path / "prefinalizer" / "round-0001").resolve(),
        base_manifest_path=Path(base["manifestPath"]),
        previous_execution_receipt=first["receipt"],
        new_campaign_receipt_paths=(campaign_receipt_path.resolve(),),
    )
    second = control.run_native_v5_rotating_prefinalizer(
        runtime_authority=runtime, manifest_path=Path(resume["manifestPath"])
    )
    assert second["receipt"]["status"] == "ready_for_finalizer"
    assert control.native_v5_transport_path_matches(
        second["receipt"]["finalizerManifest"]["path"],
        (tmp_path / "finalizer" / "manifest.json").resolve(),
    )
    assert len(calls) == 2


def test_native_v5_finalizer_reopens_only_commit_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(tmp_path)
    root = (tmp_path / "finalizer").resolve()
    root.mkdir()
    semantic_authority = _sha("semantic-authority")
    config_sha = _sha("supervisor-config")
    completed_before_sha = canonical_sha256([])
    state_basis = _self_hashed(
        {
            "schemaVersion": "temporal_qd_v5_generation_state_basis_v1",
            "configSha256": config_sha,
            "generationIndex": 2,
            "completedGenerationsSha256": completed_before_sha,
            "uniqueCandidatesEvaluated": 1,
            "workerTasksCompleted": 2,
            "nextImmigrantContinuationOrdinal": 0,
            "uniqueIdentityCounts": {},
            "duplicateCounters": {},
            "proposalSlotCounters": {},
        },
        "stateBasisSha256",
    )
    output_ledger = _self_hashed(
        {"schemaVersion": "fixture-v5-ledger", "generationIndex": 2},
        "identityLedgerSha256",
    )
    output_ledger_path = tmp_path / "proposal" / "v5-native" / "identity-ledger.json"
    _write(output_ledger_path, output_ledger)
    output_ledger_file_sha = _sha(output_ledger_path.read_bytes())
    proposal_state_authority = {
        "generationKind": "evolved",
        "proposalManifestSha256": _sha("proposal-manifest"),
        "proposalReceiptSha256": _sha("proposal-receipt"),
        "generationJournalSha256": _sha("proposal-journal"),
        "inputIdentityLedgerSha256": _sha("input-ledger"),
        "outputIdentityLedgerRelativePath": "proposal/v5-native/identity-ledger.json",
        "outputIdentityLedgerSha256": output_ledger["identityLedgerSha256"],
        "outputIdentityLedgerFileSha256": output_ledger_file_sha,
    }
    source = _self_hashed(
        {
            "schemaVersion": control.FINALIZER_SOURCE_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "generationIndex": 2,
            "runtimeAuthoritySha256": runtime["authoritySha256"],
            "semanticAuthoritySha256": semantic_authority,
            "stateBasis": state_basis,
            "proposalStateAuthority": proposal_state_authority,
        },
        "sourceSha256",
    )
    source_path = root / "source.json"
    _write(source_path, source)
    manifest = _self_hashed(
        {
            "schemaVersion": control.FINALIZER_MANIFEST_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "runtimeAuthoritySha256": runtime["authoritySha256"],
            "semanticAuthoritySha256": semantic_authority,
            "sourcePath": str(source_path),
            "sourceSha256": source["sourceSha256"],
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    manifest_path = root / "manifest.json"
    _write(manifest_path, manifest)

    output_specs = {
        "cumulativeArchive": ("evidence/cumulative-archive.json", "archiveSha256"),
        "parentArchive": ("archive.json", "archiveSha256"),
        "generationFunnel": ("generation-funnel.json", "artifactSha256"),
        "generationFunnelSnapshot": (
            "generation-funnel-snapshot.json",
            "snapshotSha256",
        ),
        "checkpoint": ("evidence/checkpoint.json", "checkpointSha256"),
        "ledger": ("evidence/generation-ledger.json", "ledgerSha256"),
        "generationRecord": ("generation-record.json", "generationRecordSha256"),
        "statePatch": ("generation-state-patch.json", "statePatchSha256"),
    }
    payloads: dict[str, dict[str, Any]] = {}
    for name, (relative, semantic_field) in output_specs.items():
        if name == "generationRecord":
            payload = _self_hashed(
                {
                    "schemaVersion": control.GENERATION_RECORD_SCHEMA,
                    "generationIndex": 2,
                },
                semantic_field,
            )
        elif name == "statePatch":
            # Fill this after the compact record identity is known.
            continue
        else:
            payload = _self_hashed({"fixture": name}, semantic_field)
        payloads[name] = payload
        _write(root / relative, payload)
    payloads["statePatch"] = _self_hashed(
        {
            "schemaVersion": control.GENERATION_STATE_PATCH_SCHEMA,
            "stateBasisSha256": state_basis["stateBasisSha256"],
            "generationIndex": 2,
            "nextGenerationIndex": 3,
            "nextStage": "generation_proposal",
            "uniqueCandidatesEvaluated": 2,
            "workerTasksCompleted": 4,
            "nextImmigrantContinuationOrdinal": 0,
            "uniqueIdentityCounts": {"fixture": 2},
            "duplicateCounters": {"fixture": 0},
            "proposalSlotCounters": {"fixture": 2},
            "completedGenerationsSha256": _sha("completed-after"),
            "generationRecordSha256": payloads["generationRecord"][
                "generationRecordSha256"
            ],
            "generationRecord": payloads["generationRecord"],
            "runtimeAuthoritySha256": runtime["authoritySha256"],
            "semanticAuthoritySha256": semantic_authority,
        },
        "statePatchSha256",
    )
    _write(root / output_specs["statePatch"][0], payloads["statePatch"])
    descriptors: dict[str, Any] = {}
    for name, (relative, semantic_field) in output_specs.items():
        path = root / relative
        descriptors[name] = {
            "path": relative,
            semantic_field: payloads[name][semantic_field],
            "bytes": path.stat().st_size,
            "fileSha256": _sha(path.read_bytes()),
        }
    commit = _self_hashed(
        {
            "schemaVersion": control.GENERATION_COMMIT_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "sourceSha256": source["sourceSha256"],
            "manifestSha256": manifest["manifestSha256"],
            "runtimeAuthoritySha256": runtime["authoritySha256"],
            "semanticAuthoritySha256": semantic_authority,
            "generationIndex": 2,
            "auxiliaryPlanSha256": _sha("auxiliary-plan"),
            **descriptors,
            "restartValidation": "compact_commit_and_output_hashes",
            "rawResultReads": 0,
        },
        "commitSha256",
    )
    _write(root / "generation-commit.json", commit)
    sidecar = _self_hashed(
        {
            "schemaVersion": control.GENERATION_STATE_APPLICATION_SIDECAR_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "generationIndex": 2,
            "generationKind": "evolved",
            "configSha256": config_sha,
            "stateBasisSha256": state_basis["stateBasisSha256"],
            "completedGenerationsBeforeSha256": completed_before_sha,
            "semanticAuthoritySha256": semantic_authority,
            "runtimeAuthoritySha256": runtime["authoritySha256"],
            "finalization": {
                "sourceSha256": source["sourceSha256"],
                "manifestSha256": manifest["manifestSha256"],
                "commitSha256": commit["commitSha256"],
                "generationRecordSha256": payloads["generationRecord"][
                    "generationRecordSha256"
                ],
                "statePatchSha256": payloads["statePatch"]["statePatchSha256"],
            },
            "proposalStateAuthority": {
                key: proposal_state_authority[key]
                for key in (
                    "proposalManifestSha256",
                    "proposalReceiptSha256",
                    "generationJournalSha256",
                )
            },
            "nextState": {
                "stage": "generation_proposal",
                "currentGenerationIndex": 3,
                "uniqueCandidatesEvaluated": 2,
                "workerTasksCompleted": 4,
                "nextImmigrantContinuationOrdinal": 0,
                "uniqueIdentityCounts": {"fixture": 2},
                "duplicateCounters": {"fixture": 0},
                "proposalSlotCounters": {"fixture": 2},
                "completedGenerationsSha256": _sha("completed-after"),
            },
            "identityLedgerPromotion": {
                "inputIdentityLedgerSha256": proposal_state_authority[
                    "inputIdentityLedgerSha256"
                ],
                "outputRelativePath": "proposal/v5-native/identity-ledger.json",
                "outputIdentityLedgerSha256": output_ledger["identityLedgerSha256"],
                "outputIdentityLedgerFileSha256": output_ledger_file_sha,
            },
        },
        "sidecarSha256",
    )
    _write(root / control.GENERATION_STATE_APPLICATION_SIDECAR_FILENAME, sidecar)
    calls: list[tuple[str, ...]] = []

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = tuple(kwargs["command"])
        calls.append(command)
        assert command == (
            runtime["binaries"]["generationFinalizer"]["path"],
            str(manifest_path),
        )
        return {
            "schemaVersion": control.FINALIZER_EXECUTION_SCHEMA,
            "status": "committed",
            "sourceSha256": source["sourceSha256"],
            "manifestSha256": manifest["manifestSha256"],
            "generationIndex": 2,
            "auxiliaryPlanSha256": commit["auxiliaryPlanSha256"],
            "commitSha256": commit["commitSha256"],
            "restart": True,
            "restartValidation": "compact_commit_and_output_hashes",
            "rawResultReads": 0,
            "elapsedMilliseconds": 1,
            "commit": commit,
        }

    original_raw_file_sha256 = control.raw_file_sha256
    candidate_scale_paths = {
        (root / relative).resolve()
        for name, (relative, _semantic_field) in output_specs.items()
        if name
        in {
            "cumulativeArchive",
            "parentArchive",
            "generationFunnel",
            "generationFunnelSnapshot",
            "checkpoint",
            "ledger",
        }
    }
    candidate_scale_paths.add(
        (root.parent / "proposal" / "v5-native" / "identity-ledger.json").resolve()
    )

    def no_candidate_scale_hash(path: Path | str) -> str:
        if Path(path).resolve() in candidate_scale_paths:
            pytest.fail("finalizer control bridge hashed a candidate-scale output")
        return original_raw_file_sha256(path)

    original_read_canonical_object = control._read_canonical_object

    def no_finalizer_source_read(path: Path | str, *, name: str) -> dict[str, Any]:
        if Path(path).resolve() == source_path.resolve():
            pytest.fail("finalizer control bridge reopened source.json")
        return original_read_canonical_object(path, name=name)

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    monkeypatch.setattr(control, "raw_file_sha256", no_candidate_scale_hash)
    monkeypatch.setattr(
        control, "_read_canonical_object", no_finalizer_source_read
    )
    result = control.run_native_v5_generation_finalizer(
        runtime_authority=runtime, manifest_path=manifest_path
    )
    assert result["generationRecord"]["generationIndex"] == 2
    assert result["statePatch"]["generationRecord"] == result["generationRecord"]
    assert result["stateApplicationSidecar"] == sidecar
    assert result["identityLedgerPromotion"]["semanticSha256"] == output_ledger[
        "identityLedgerSha256"
    ]
    assert calls

    # A committed restart must not reopen the candidate-scale source.  The
    # Rust commit is sufficient authority after fresh execution.
    source_path.unlink()
    restarted = control.run_native_v5_generation_finalizer(
        runtime_authority=runtime, manifest_path=manifest_path
    )
    assert restarted["sourceSha256"] == source["sourceSha256"]

    def forged_compact_execution(**kwargs: Any) -> dict[str, Any]:
        execution = fake_run(**kwargs)
        execution["commitSha256"] = _sha("forged-commit")
        return execution

    monkeypatch.setattr(control, "_run_pinned", forged_compact_execution)
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="commit binding"):
        control.run_native_v5_generation_finalizer(
            runtime_authority=runtime, manifest_path=manifest_path
        )


def test_campaign_source_builder_uses_fixed_native_receipt_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(tmp_path)
    freezer_root = tmp_path / "freezer"
    gateway_root = tmp_path / "gateway"
    task_manifest = freezer_root / "screening-run" / "task-manifest.json"
    _write(task_manifest, {"tasks": []})
    freezer_receipt = _self_hashed({"schemaVersion": "fixture-freeze"}, "receiptSha256")
    _write(freezer_root / "native-freeze-receipt.json", freezer_receipt)
    gateway_receipt, _gateway_result = _gateway_receipt(
        root=gateway_root.resolve(), task_manifest=task_manifest.resolve(), task_count=0
    )

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = kwargs["command"]
        manifest_path = Path(command[-1])
        manifest = control._read_canonical_object(manifest_path, name="fixture source manifest")
        source_path = Path(manifest["sourcePath"])
        source = _self_hashed(
            {"schemaVersion": "fixture-source", "authorityId": _sha("authority")},
            "sourceSha256",
        )
        _write(source_path, source)
        receipt = _self_hashed(
            {
                "schemaVersion": control.CAMPAIGN_SOURCE_BUILD_RECEIPT_SCHEMA,
                "manifestSha256": manifest["manifestSha256"],
                "freezerReceiptSha256": freezer_receipt["receiptSha256"],
                "gatewayReceiptSha256": gateway_receipt["receiptSha256"],
                "sourceSha256": source["sourceSha256"],
                "authorityId": source["authorityId"],
                "taskMatrixSha256": _sha("task-matrix"),
                "taskCount": 0,
                "sourcePath": str(source_path),
            },
            "receiptSha256",
        )
        _write(source_path.parent / "source-build-receipt.json", receipt)
        return {
            "schemaVersion": control.CAMPAIGN_SOURCE_BUILD_RESULT_SCHEMA,
            "sourcePath": str(source_path),
            "sourceSha256": source["sourceSha256"],
            "receiptPath": str(source_path.parent / "source-build-receipt.json"),
            "authorityId": source["authorityId"],
            "taskMatrixSha256": receipt["taskMatrixSha256"],
            "taskCount": 0,
            "receiptSha256": receipt["receiptSha256"],
        }

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    result = control.build_native_campaign_seal_source(
        runtime_authority=runtime,
        freezer_root=freezer_root.resolve(),
        gateway_output_root=gateway_root.resolve(),
        source_root=(tmp_path / "source").resolve(),
    )
    assert Path(result["sourcePath"]).parent == (tmp_path / "source").resolve()
    assert result["manifest"]["sourcePath"] == result["sourcePath"]

    _write_pretty(freezer_root / "native-freeze-receipt.json", freezer_receipt)
    (tmp_path / "pretty-rewrite-source").mkdir()
    with pytest.raises(
        control.TemporalQDV5ControlPlaneError,
        match="native v5 freezer receipt must be canonical JSON plus LF",
    ):
        control.build_native_campaign_seal_source(
            runtime_authority=runtime,
            freezer_root=freezer_root.resolve(),
            gateway_output_root=gateway_root.resolve(),
            source_root=(tmp_path / "pretty-rewrite-source").resolve(),
        )


def test_rotating_campaign_receipt_binds_only_native_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(tmp_path)
    freeze_root = (tmp_path / "freeze").resolve()
    seal_root = (tmp_path / "seal").resolve()
    gateway_root = (tmp_path / "gateway").resolve()
    sidecar_root = (tmp_path / "sidecar").resolve()
    role = "proposal_current_panel"
    panel = "panel-1"
    rotating = _sha("rotating")
    manifest = {
        "manifestSha256": _sha("freeze-manifest"),
        "campaignRole": role,
        "panelId": panel,
    }
    transaction = _self_hashed(
        {
            "schemaVersion": "temporal_qd_v5_native_campaign_freeze_transaction_v2",
            "manifestSha256": manifest["manifestSha256"],
            "campaignRole": role,
            "cohortPopulationSha256": _sha("cohort"),
            "preparationSha256": _sha("preparation"),
            "authorityId": _sha("authority"),
            "evaluationIdentitySha256": _sha("evaluation"),
            "campaignSha256": _sha("campaign"),
            "taskMatrixSha256": _sha("task-matrix"),
            "candidateCount": 2,
            "windowCount": 1,
            "taskCount": 2,
        },
        "transactionSha256",
    )
    _write(freeze_root / ".native-v5-campaign-freeze-manifest.json", manifest)
    _write(freeze_root / "native-freeze-transaction.json", transaction)
    for relative in control._NATIVE_V5_FREEZE_RECEIPT_INVENTORY_PATHS:
        path = freeze_root / relative
        if relative == "native-freeze-transaction.json":
            # The receipt inventory includes the immutable transaction we
            # already wrote above; do not replace it with a fixture payload.
            continue
        if path.suffix == ".jsonl":
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'{"fixture":"native"}\n')
        else:
            _write(path, {"fixture": relative})
    freeze_inventory = [
        {
            "relativePath": relative,
            "rawSha256": _sha((freeze_root / relative).read_bytes()),
        }
        for relative in control._NATIVE_V5_FREEZE_RECEIPT_INVENTORY_PATHS
    ]
    freeze_receipt = _self_hashed(
        {
            "manifestSha256": manifest["manifestSha256"],
            "transactionSha256": transaction["transactionSha256"],
            "outputInventory": freeze_inventory,
        },
        "receiptSha256",
    )
    _write(freeze_root / "native-freeze-receipt.json", freeze_receipt)
    gateway_receipt, _gateway_result = _gateway_receipt(
        root=gateway_root,
        task_manifest=(freeze_root / "screening-run" / "task-manifest.json"),
        task_count=1,
    )

    tail_index = seal_root / "tail-result-index-v4.json"
    _write(tail_index, {"tail": "index"})
    tail_index_sha = _sha("tail-index-semantic")
    directional_tail_authority = control.build_v5_directional_tail_authority(
        runtime_authority_sha256=runtime["authoritySha256"], generation_index=1
    )
    campaign_seal = _self_hashed(
        {"tailResultIndex": {"sha256": tail_index_sha}}, "campaignSealSha256"
    )
    tail_transaction = _self_hashed({}, "transactionSha256")
    _write(seal_root / "campaign-seal-result.json", campaign_seal)
    _write(seal_root / "generation-tail-transaction-result.json", tail_transaction)
    members = seal_root / "evaluated-members.jsonl"
    members.parent.mkdir(parents=True, exist_ok=True)
    members.write_bytes(b'{"member":"native"}\n')
    evaluated = {
        "path": "evaluated-members.jsonl",
        "rawSha256": _sha(members.read_bytes()),
        "sizeBytes": members.stat().st_size,
        "recordCount": 1,
    }

    bundles_path = sidecar_root / "candidate-panel-bundles.jsonl"
    bundles_path.parent.mkdir(parents=True, exist_ok=True)
    bundles_path.write_bytes(b'{"bundle":"native"}\n')
    bundles = _self_hashed(
        {
            "schemaVersion": control.PANEL_SIDECAR_DESCRIPTOR_SCHEMA,
            "path": str(bundles_path),
            "rawSha256": _sha(bundles_path.read_bytes()),
            "sizeBytes": bundles_path.stat().st_size,
            "recordCount": 1,
            "rowSchema": "temporal_qd_candidate_panel_evidence_bundle_v1",
        },
        "descriptorSha256",
    )
    sidecar_receipt = _self_hashed(
        {
            "schemaVersion": control.PANEL_SIDECAR_RECEIPT_SCHEMA,
            "inputSha256": _sha("panel-input"),
            "panelReceiptSha256": _sha("panel-receipt"),
            "campaignSealSha256": campaign_seal["campaignSealSha256"],
            "tailAuthoritySha256": directional_tail_authority["tailAuthoritySha256"],
            "candidatePanelBundles": bundles,
        },
        "receiptSha256",
    )
    sidecar_result = _self_hashed(
        {
            "schemaVersion": control.PANEL_SIDECAR_RESULT_SCHEMA,
            "inputSha256": sidecar_receipt["inputSha256"],
            "receiptSha256": sidecar_receipt["receiptSha256"],
            "candidatePanelBundles": bundles,
        },
        "resultSha256",
    )
    campaign_seal_descriptor = {
        "path": "campaign-seal-result.json",
        "rawSha256": _sha((seal_root / "campaign-seal-result.json").read_bytes()),
        "sizeBytes": (seal_root / "campaign-seal-result.json").stat().st_size,
        "campaignSealSha256": campaign_seal["campaignSealSha256"],
    }
    tail_transaction_descriptor = {
        "path": "generation-tail-transaction-result.json",
        "rawSha256": _sha(
            (seal_root / "generation-tail-transaction-result.json").read_bytes()
        ),
        "sizeBytes": (
            seal_root / "generation-tail-transaction-result.json"
        ).stat().st_size,
        "transactionSha256": tail_transaction["transactionSha256"],
    }
    campaign_seal_handoff = {
        "outputRoot": str(seal_root),
        "directionalTailAuthority": directional_tail_authority,
        "campaignSeal": campaign_seal,
        "campaignSealDescriptor": campaign_seal_descriptor,
        "generationTailTransaction": tail_transaction_descriptor,
        "tailResultIndex": {
            "path": str(tail_index.resolve()),
            "relativePath": "tail-result-index-v4.json",
            "rawSha256": _sha(tail_index.read_bytes()),
            "sizeBytes": tail_index.stat().st_size,
            "tailResultIndexSha256": tail_index_sha,
        },
        "tailAuthorityReceiptDocument": {"evaluatedMembers": evaluated},
        "sourceBuild": {
            "gatewayOutputRoot": str(gateway_root),
            "gatewayReceipt": gateway_receipt,
        },
    }
    freeze_handoff = {
        "outputRoot": str(freeze_root),
        "manifest": manifest,
        "manifestPath": str(
            (freeze_root / ".native-v5-campaign-freeze-manifest.json").resolve()
        ),
        "transaction": transaction,
        "transactionPath": str((freeze_root / "native-freeze-transaction.json").resolve()),
        "receipt": freeze_receipt,
    }
    sidecar_handoff = {
        "result": sidecar_result,
        "receipt": sidecar_receipt,
        "candidatePanelBundles": bundles,
    }

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        command = kwargs["command"]
        input_value = control._read_canonical_object(
            Path(command[-2]), name="fixture campaign receipt input"
        )
        semantic = {
            "schemaVersion": control.CAMPAIGN_RECEIPT_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            **{
                field: input_value[field]
                for field in (
                    "generationIndex",
                    "campaignRole",
                    "panelId",
                    "rotatingEvidenceSha256",
                    "cohortSource",
                    "campaignFreeze",
                    "campaignSeal",
                    "evaluatedMembers",
                    "candidatePanelBundles",
                )
            },
        }
        receipt = {
            **semantic,
            "semanticReceiptSha256": canonical_sha256(semantic),
            "runtimeAuthoritySha256": input_value["runtimeAuthoritySha256"],
            "executionBindings": input_value["executionBindings"],
        }
        receipt["receiptSha256"] = canonical_sha256(receipt)
        _write(Path(command[-1]), receipt)
        return receipt

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    candidate_scale_paths = {
        members.resolve(),
        bundles_path.resolve(),
        (freeze_root / "screening-run" / "task-manifest.json").resolve(),
        (gateway_root / "checkpoint.json").resolve(),
        (gateway_root / ".native-gateway-dispatch" / "completion-journal.jsonl").resolve(),
    }
    original_raw_file_sha256 = control.raw_file_sha256

    def reject_candidate_scale_hash(path: Path | str) -> str:
        if Path(path).resolve() in candidate_scale_paths:
            pytest.fail(f"campaign receipt rehashed candidate/task payload: {path}")
        return original_raw_file_sha256(path)

    monkeypatch.setattr(control, "raw_file_sha256", reject_candidate_scale_hash)
    built = control.build_native_rotating_campaign_receipt(
        runtime_authority=runtime,
        campaign_freeze=freeze_handoff,
        campaign_seal=campaign_seal_handoff,
        panel_bundle_sidecar=sidecar_handoff,
        output_root=(tmp_path / "receipt").resolve(),
        generation_index=1,
        campaign_role=role,
        panel_id=panel,
        rotating_evidence_sha256=rotating,
        cohort_source={
            "kind": "proposal_evaluation_population",
            "sourceSemanticSha256": _sha("evaluation-population"),
            "candidateCount": 2,
            "selectionSha256": None,
        },
    )
    assert built["receipt"]["candidatePanelBundles"] == {
        key: bundles[key]
        for key in ("rawSha256", "sizeBytes", "recordCount", "rowSchema")
    }
    assert set(built["input"]["candidatePanelBundles"]) == {
        "rawSha256",
        "sizeBytes",
        "recordCount",
        "rowSchema",
    }


def test_native_v3_ladder_freeze_reopens_only_sealed_control_documents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """V3 ladder validation never opens archive or candidate-bearing outputs."""

    runtime = _runtime(tmp_path)
    template = tmp_path / "template.json"
    catalog = tmp_path / "catalog.json"
    _write(template, {})
    _write(catalog, {})
    archive_sha = _sha("finalizer-archive")
    archive_file_sha = _sha("finalizer-archive-file")
    commit = _self_hashed(
        {
            "schemaVersion": control.GENERATION_COMMIT_SCHEMA,
            "generationIndex": 2,
            "parentArchive": {
                "path": "archive.json",
                "archiveSha256": archive_sha,
                "bytes": 17,
                "fileSha256": archive_file_sha,
            },
        },
        "commitSha256",
    )
    commit_path = tmp_path / "finalizer" / "generation-commit.json"
    _write(commit_path, commit)
    archive_authority = {
        "kind": "generation_finalizer_commit",
        "receiptPath": str(commit_path.resolve()),
        "receiptSha256": commit["commitSha256"],
    }
    policy = {"policyBindingSha256": _sha("policy")}
    behavior = {"schemaVersion": "fixture"}
    template_sha = canonical_sha256({})
    catalog_sha = canonical_sha256({})
    stage = {
        "candidateLimit": 1,
        "templatePreparationPath": str(template.resolve()),
        "templatePreparationSha256": template_sha,
        "constructionCatalogPath": str(catalog.resolve()),
        "constructionCatalogSha256": catalog_sha,
        "archivePolicyAuthority": policy,
        "behaviorAttributionRequirement": behavior,
    }
    ladder = _self_hashed(
        {
            "schemaVersion": control.NATIVE_V5_LADDER_AUTHORITY_SCHEMA,
            "stageOrder": ["validation", "scrutiny"],
            "stages": {"validation": stage, "scrutiny": dict(stage)},
        },
        "ladderAuthoritySha256",
    )
    root = (tmp_path / "ladder-v3").resolve()
    tamper: dict[str, str | None] = {"target": None}

    def inventory(paths: tuple[str, ...], *, tag: str) -> list[dict[str, str]]:
        return [
            {"relativePath": path, "rawSha256": _sha(f"{tag}:{path}")}
            for path in paths
        ]

    def fake_freeze(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["native_binary"] == Path(
            runtime["binaries"]["campaignFreeze"]["path"]
        )
        assert kwargs["archive_authority"] == archive_authority
        assert kwargs["ladder_authority"] == ladder
        output_root = Path(kwargs["output_root"])
        native_runtime = {
            "schemaVersion": "temporal_qd_native_campaign_freeze_runtime_authority_v1",
            "runtimeEpoch": "temporal_qd_native_campaign_freeze_epoch_v2",
            "binaryRole": "temporal-qd-campaign-freeze",
            "binarySha256": runtime["binaries"]["campaignFreeze"]["fileSha256"],
        }
        manifest = {
            "schemaVersion": control.NATIVE_V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA,
            "archiveAuthority": archive_authority,
            "ladderStage": "validation",
            "ladderCandidateLimit": 1,
            "ladderAuthority": ladder,
            "templatePreparationPath": str(template.resolve()),
            "templatePreparationSha256": template_sha,
            "constructionCatalogPath": str(catalog.resolve()),
            "constructionCatalogSha256": catalog_sha,
            "outputRoot": str(output_root),
            "executionEngineCommit": "a" * 40,
            "workerContractSha256": _sha("worker-contract"),
            "campaignRole": "retained_parent_current_panel",
            "panelId": "panel-validation",
            "rotatingEvidence": {"rotatingEvidenceSha256": _sha("rotating")},
            "archivePolicyAuthority": policy,
            "behaviorAttributionRequirement": behavior,
            "nativeRuntimeAuthority": native_runtime,
            "nativeRuntimeAuthoritySha256": canonical_sha256(native_runtime),
        }
        manifest["manifestSha256"] = control._native_v5_ladder_manifest_sha256(manifest)
        _write_pretty(
            output_root / ".native-v5-evidence-ladder-freeze-manifest.json", manifest
        )

        native_transaction = _self_hashed(
            {
                "schemaVersion": "temporal_qd_v5_native_campaign_freeze_transaction_v2",
                "manifestSha256": _sha("delegated-freeze-manifest"),
                "nativeRuntimeAuthoritySha256": manifest["nativeRuntimeAuthoritySha256"],
                "evaluationPopulationRawSha256": _sha("ladder-population-raw"),
                "cohortPopulationSha256": _sha("cohort-population"),
                "templatePreparationSha256": template_sha,
                "constructionCatalogSha256": catalog_sha,
                "preparationSha256": _sha("preparation"),
                "authorityId": _sha("campaign-authority"),
                "evaluationIdentitySha256": _sha("evaluation-identity"),
                "campaignSha256": _sha("campaign"),
                "taskMatrixSha256": _sha("task-matrix"),
                "candidateCount": 1,
                "windowCount": 1,
                "taskCount": 1,
                "campaignRole": "retained_parent_current_panel",
                "outputInventory": inventory(
                    control._NATIVE_V5_FREEZE_TRANSACTION_INVENTORY_PATHS,
                    tag="native-transaction",
                ),
            },
            "transactionSha256",
        )
        native_receipt = _self_hashed(
            {
                "schemaVersion": "temporal_qd_v5_native_campaign_freeze_receipt_v1",
                "manifestSha256": native_transaction["manifestSha256"],
                "nativeRuntimeAuthoritySha256": native_transaction[
                    "nativeRuntimeAuthoritySha256"
                ],
                "transactionSha256": native_transaction["transactionSha256"],
                "campaignSha256": native_transaction["campaignSha256"],
                "authorityId": native_transaction["authorityId"],
                "evaluationIdentitySha256": native_transaction[
                    "evaluationIdentitySha256"
                ],
                "taskMatrixSha256": native_transaction["taskMatrixSha256"],
                "taskCount": native_transaction["taskCount"],
                "outputInventory": inventory(
                    control._NATIVE_V5_FREEZE_RECEIPT_INVENTORY_PATHS,
                    tag="native-receipt",
                ),
                "semanticReceiptSha256": _sha("native-semantic-receipt"),
            },
            "receiptSha256",
        )
        if tamper["target"] == "nested_inventory":
            native_receipt["outputInventory"][0]["relativePath"] = "archive.json"
            native_receipt = _self_hashed(
                {
                    key: value
                    for key, value in native_receipt.items()
                    if key != "receiptSha256"
                },
                "receiptSha256",
            )
        _write(output_root / "native-freeze-transaction.json", native_transaction)
        _write(output_root / "native-freeze-receipt.json", native_receipt)

        common = {
            "manifestSha256": manifest["manifestSha256"],
            "archiveAuthorityKind": archive_authority["kind"],
            "archiveAuthorityReceiptSha256": archive_authority["receiptSha256"],
            "archiveSha256": archive_sha,
            "archiveRawSha256": archive_file_sha,
            "archiveSizeBytes": 17,
            "ladderStage": "validation",
            "ladderCandidateLimit": 1,
            "ladderAuthoritySha256": ladder["ladderAuthoritySha256"],
            "selectionSha256": _sha("selection"),
            "projectionRawSha256": _sha("selection-rows"),
            "cohortPopulationSha256": native_transaction["cohortPopulationSha256"],
            "nativeFreezeReceiptSha256": native_receipt["receiptSha256"],
            "campaignSha256": native_transaction["campaignSha256"],
            "authorityId": native_transaction["authorityId"],
            "evaluationIdentitySha256": native_transaction["evaluationIdentitySha256"],
            "taskMatrixSha256": native_transaction["taskMatrixSha256"],
            "taskCount": native_transaction["taskCount"],
        }
        transaction = _self_hashed(
            {
                "schemaVersion": control.NATIVE_V5_LADDER_ARCHIVE_FREEZE_TRANSACTION_SCHEMA,
                **common,
                "outputInventory": inventory(
                    control._NATIVE_V5_LADDER_TRANSACTION_INVENTORY_PATHS,
                    tag="ladder-transaction",
                ),
            },
            "transactionSha256",
        )
        receipt = _self_hashed(
            {
                "schemaVersion": control.NATIVE_V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA,
                "transactionSha256": transaction["transactionSha256"],
                **common,
                "outputInventory": inventory(
                    control._NATIVE_V5_LADDER_RECEIPT_INVENTORY_PATHS,
                    tag="ladder-receipt",
                ),
            },
            "receiptSha256",
        )
        if tamper["target"] == "outer_inventory":
            receipt["outputInventory"][0]["relativePath"] = "archive.json"
            receipt = _self_hashed(
                {key: value for key, value in receipt.items() if key != "receiptSha256"},
                "receiptSha256",
            )
        result = {
            "schemaVersion": control.NATIVE_V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA,
            "manifestSha256": common["manifestSha256"],
            "archiveAuthorityKind": common["archiveAuthorityKind"],
            "archiveAuthorityReceiptSha256": common["archiveAuthorityReceiptSha256"],
            "archiveSha256": common["archiveSha256"],
            "ladderStage": common["ladderStage"],
            "ladderCandidateLimit": common["ladderCandidateLimit"],
            "selectionSha256": common["selectionSha256"],
            "campaignSha256": common["campaignSha256"],
            "authorityId": common["authorityId"],
            "taskMatrixSha256": common["taskMatrixSha256"],
        }
        _write_pretty(output_root / "ladder-freeze-result.json", result)
        _write_pretty(output_root / "ladder-freeze-transaction.json", transaction)
        _write_pretty(output_root / "ladder-freeze-receipt.json", receipt)
        return {
            "result": result,
            "outputRoot": str(output_root),
            "manifest": manifest,
            "manifestPath": str(
                output_root / ".native-v5-evidence-ladder-freeze-manifest.json"
            ),
        }

    monkeypatch.setattr(control, "freeze_qd_v5_evidence_ladder_archive_native", fake_freeze)
    handoff = control.run_native_v5_evidence_ladder_archive_freeze(
        runtime_authority=runtime,
        archive_authority=archive_authority,
        ladder_stage="validation",
        ladder_candidate_limit=1,
        ladder_authority=ladder,
        template_preparation_path=template.resolve(),
        construction_catalog_path=catalog.resolve(),
        output_root=root,
        execution_engine_commit="a" * 40,
        worker_contract_sha256=_sha("worker-contract"),
        rotating_evidence={"rotatingEvidenceSha256": _sha("rotating")},
        archive_policy_authority=policy,
        behavior_attribution_requirement=behavior,
        campaign_role="retained_parent_current_panel",
        panel_id="panel-validation",
    )
    assert handoff["receipt"]["receiptSha256"]
    assert handoff["nativeFreezeReceipt"]["receiptSha256"]
    assert handoff["cohortPopulationRawSha256"] == _sha(
        "native-receipt:cohort-population.json"
    )
    assert not (tmp_path / "finalizer" / "archive.json").exists()
    assert not (root / "cohort-selection.jsonl").exists()

    tamper["target"] = "outer_inventory"
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="inventory path"):
        control.run_native_v5_evidence_ladder_archive_freeze(
            runtime_authority=runtime,
            archive_authority=archive_authority,
            ladder_stage="validation",
            ladder_candidate_limit=1,
            ladder_authority=ladder,
            template_preparation_path=template.resolve(),
            construction_catalog_path=catalog.resolve(),
            output_root=root,
            execution_engine_commit="a" * 40,
            worker_contract_sha256=_sha("worker-contract"),
            rotating_evidence={"rotatingEvidenceSha256": _sha("rotating")},
            archive_policy_authority=policy,
            behavior_attribution_requirement=behavior,
            campaign_role="retained_parent_current_panel",
            panel_id="panel-validation",
        )

    tamper["target"] = "nested_inventory"
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="inventory path"):
        control.run_native_v5_evidence_ladder_archive_freeze(
            runtime_authority=runtime,
            archive_authority=archive_authority,
            ladder_stage="validation",
            ladder_candidate_limit=1,
            ladder_authority=ladder,
            template_preparation_path=template.resolve(),
            construction_catalog_path=catalog.resolve(),
            output_root=root,
            execution_engine_commit="a" * 40,
            worker_contract_sha256=_sha("worker-contract"),
            rotating_evidence={"rotatingEvidenceSha256": _sha("rotating")},
            archive_policy_authority=policy,
            behavior_attribution_requirement=behavior,
            campaign_role="retained_parent_current_panel",
            panel_id="panel-validation",
        )


def test_native_archive_reducer_accepts_only_a_compact_tail_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tail-to-archive carries descriptors only; Python never opens row files."""

    runtime = _runtime(tmp_path)
    tail_root = (tmp_path / "tail").resolve()
    tail_root.mkdir()
    authority = {
        "schemaVersion": control.TAIL_AUTHORITY_RECEIPT_SCHEMA,
        "generationIndex": 2,
        "tailReductionManifestSha256": _sha("tail-manifest"),
        "evaluationPopulationSha256": _sha("cohort-population"),
        "populationSha256": _sha("population"),
        "tailResultIndexSha256": _sha("tail-index"),
        "taskMatrixSha256": _sha("task-matrix"),
        "resultSetSha256": _sha("result-set"),
        "runtimeAuthoritySha256": runtime["authoritySha256"],
        "tailReductionResult": {
            "path": "tail-reduction-result.json",
            "rawSha256": _sha("tail-result-file"),
            "sizeBytes": 17,
            "resultSha256": _sha("tail-result"),
        },
        "evaluatedMembers": {
            "path": "evaluated-members.jsonl",
            "rawSha256": _sha("evaluated-members-file"),
            "sizeBytes": 23,
            "recordCount": 1,
        },
    }
    authority = _self_hashed(authority, "tailAuthoritySha256")
    authority_path = tail_root / "tail-authority.json"
    _write(authority_path, authority)
    output_root = (tmp_path / "archive").resolve()
    output_root.mkdir()
    observed: dict[str, Any] = {}

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["role"] == "archiveReducer"
        manifest_path = Path(kwargs["command"][-1])
        manifest = control._read_canonical_object(
            manifest_path, name="fixture archive-reducer manifest"
        )
        observed["manifest"] = manifest
        assert set(manifest) == {
            "schemaVersion",
            "contractVersion",
            "operation",
            "tailAuthority",
            "cellCapacity",
            "archivePolicy",
            "directionAware",
            "manifestSha256",
        }
        assert manifest["tailAuthority"] == {
            "receiptPath": str(authority_path),
            "receiptSha256": authority["tailAuthoritySha256"],
        }
        # Do not create tail-reduction-result.json, evaluated-members.jsonl,
        # or archive.json.  A successful bridge proves it opened none of them.
        result = _self_hashed(
            {
                "schemaVersion": control.ARCHIVE_REDUCTION_RESULT_SCHEMA,
                "contractVersion": control.CONTRACT_VERSION,
                "operation": control.ARCHIVE_REDUCTION_OPERATION,
                "status": "completed",
                "manifestSha256": manifest["manifestSha256"],
                "tailAuthoritySha256": authority["tailAuthoritySha256"],
                "archiveSha256": _sha("archive"),
                "archiveRawSha256": _sha("archive-file"),
                "archiveSizeBytes": 19,
                "populationSha256": authority["populationSha256"],
                "resultSetSha256": authority["resultSetSha256"],
                "generationIndex": 2,
                "candidateCountSeen": 1,
                "occupiedCellCount": 1,
                "memberCount": 1,
                "qualityMemberCount": 1,
                "observationalMemberCount": 0,
                "negativeNoveltyMemberCount": 0,
                "archivePath": "archive.json",
                "runtimeAuthoritySha256": runtime["authoritySha256"],
            },
            "resultSha256",
        )
        _write(output_root / "archive-reduction-result.json", result)
        return result

    monkeypatch.setattr(control, "_run_pinned", fake_run)
    policy = {
        "qdVersion": "temporal_qd_evolution_v5",
        "policyName": "fixture-policy",
        "policySha256": _sha("policy"),
        "frozenPolicy": {"archive": {}},
    }
    reduced = control.run_native_v5_archive_reducer(
        runtime_authority=runtime,
        tail_authority={
            "receiptPath": str(authority_path),
            "receiptSha256": authority["tailAuthoritySha256"],
        },
        output_root=output_root,
        cell_capacity=1,
        archive_policy_authority=policy,
        direction_aware=True,
    )
    assert observed["manifest"]["tailAuthority"]["receiptSha256"] == authority[
        "tailAuthoritySha256"
    ]
    assert reduced["archiveAuthority"] == {
        "kind": "qd_archive_reducer_result",
        "receiptPath": str(output_root / "archive-reduction-result.json"),
        "receiptSha256": reduced["result"]["resultSha256"],
    }
    assert not (tail_root / "tail-reduction-result.json").exists()
    assert not (tail_root / "evaluated-members.jsonl").exists()
    assert not (output_root / "archive.json").exists()

    tampered = dict(authority)
    tampered["evaluatedMembers"] = dict(authority["evaluatedMembers"])
    tampered["evaluatedMembers"]["path"] = "unbound-members.jsonl"
    tampered = _self_hashed(
        {key: value for key, value in tampered.items() if key != "tailAuthoritySha256"},
        "tailAuthoritySha256",
    )
    _write(authority_path, tampered)
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="evaluated-members"):
        control.run_native_v5_archive_reducer(
            runtime_authority=runtime,
            tail_authority={
                "receiptPath": str(authority_path),
                "receiptSha256": tampered["tailAuthoritySha256"],
            },
            output_root=output_root,
            cell_capacity=1,
            archive_policy_authority=policy,
            direction_aware=True,
        )


def test_native_ladder_reducer_authority_requires_the_exact_v1_result(
    tmp_path: Path,
) -> None:
    """Scrutiny may receive only the complete tail-authority-bound receipt."""

    result = _self_hashed(
        {
            "schemaVersion": control.ARCHIVE_REDUCTION_RESULT_SCHEMA,
            "contractVersion": control.CONTRACT_VERSION,
            "operation": control.ARCHIVE_REDUCTION_OPERATION,
            "status": "completed",
            "manifestSha256": _sha("manifest"),
            "tailAuthoritySha256": _sha("tail-authority"),
            "archiveSha256": _sha("archive"),
            "archiveRawSha256": _sha("archive-file"),
            "archiveSizeBytes": 19,
            "populationSha256": _sha("population"),
            "resultSetSha256": _sha("result-set"),
            "generationIndex": 2,
            "candidateCountSeen": 1,
            "occupiedCellCount": 1,
            "memberCount": 1,
            "qualityMemberCount": 1,
            "observationalMemberCount": 0,
            "negativeNoveltyMemberCount": 0,
            "archivePath": "archive.json",
            "runtimeAuthoritySha256": _sha("runtime"),
        },
        "resultSha256",
    )
    path = tmp_path / "archive-reduction-result.json"
    _write(path, result)
    authority, receipt, generation = control._validated_native_v5_ladder_archive_authority(
        {
            "kind": "qd_archive_reducer_result",
            "receiptPath": str(path.resolve()),
            "receiptSha256": result["resultSha256"],
        }
    )
    assert authority["receiptSha256"] == result["resultSha256"]
    assert receipt["tailAuthoritySha256"] == result["tailAuthoritySha256"]
    assert generation == 2

    replaced = dict(result)
    replaced.pop("tailAuthoritySha256")
    replaced = _self_hashed(
        {key: value for key, value in replaced.items() if key != "resultSha256"},
        "resultSha256",
    )
    _write(path, replaced)
    with pytest.raises(control.TemporalQDV5ControlPlaneError, match="receipt drifted"):
        control._validated_native_v5_ladder_archive_authority(
            {
                "kind": "qd_archive_reducer_result",
                "receiptPath": str(path.resolve()),
                "receiptSha256": replaced["resultSha256"],
            }
        )


def _legacy_native_ladder_freeze_fixture_is_retained_for_oracle_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control plane never opens or hashes an evidence-ladder archive."""

    runtime = _runtime(tmp_path)
    evaluation = tmp_path / "evaluation-population.json"
    template = tmp_path / "template.json"
    catalog = tmp_path / "catalog.json"
    for path in (evaluation, template, catalog):
        path.write_bytes(b"{}\n")
    reducer_root = tmp_path / "native-reducer"
    reducer_root.mkdir()
    reduction_receipt = reducer_root / "archive-reduction-result.json"
    # Deliberately not JSON: the freezer binary, not Python, authenticates the
    # bounded reducer result.  No archive payload exists beside it either.
    reduction_receipt.write_bytes(b"opaque-native-reducer-receipt\n")
    captured: dict[str, Any] = {}

    def fake_freeze(**kwargs: Any) -> dict[str, Any]:
        root = Path(kwargs["output_root"])
        captured["reduction"] = kwargs["final_archive_reduction_result_path"]
        assert not (reducer_root / "archive.json").exists()
        manifest_sha = _sha("freeze-manifest")
        runtime_sha = _sha("freeze-runtime")
        manifest = {
            "manifestSha256": manifest_sha,
            "nativeRuntimeAuthoritySha256": runtime_sha,
            "nativeRuntimeAuthority": {
                "binarySha256": runtime["binaries"]["campaignFreeze"]["fileSha256"]
            },
        }
        transaction = _self_hashed(
            {
                "schemaVersion": "temporal_qd_v5_native_campaign_freeze_transaction_v2",
                "manifestSha256": manifest_sha,
            },
            "transactionSha256",
        )
        receipt = _self_hashed(
            {
                "schemaVersion": "temporal_qd_v5_native_campaign_freeze_receipt_v1",
                "manifestSha256": manifest_sha,
                "transactionSha256": transaction["transactionSha256"],
                "nativeRuntimeAuthoritySha256": runtime_sha,
                "campaignSha256": _sha("campaign"),
                "taskMatrixSha256": _sha("matrix"),
                "taskCount": 1,
            },
            "receiptSha256",
        )
        _write(root / ".native-v5-campaign-freeze-manifest.json", manifest)
        _write(root / "native-freeze-transaction.json", transaction)
        _write(root / "native-freeze-receipt.json", receipt)
        return {
            "schemaVersion": "temporal_qd_v5_native_evidence_ladder_freeze_result_v1",
            "outputRoot": str(root),
            "campaignSha256": receipt["campaignSha256"],
            "taskMatrixSha256": receipt["taskMatrixSha256"],
            "taskCount": receipt["taskCount"],
        }

    monkeypatch.setattr(control, "freeze_qd_v5_campaign_native", fake_freeze)
    handoff = control.run_native_v5_campaign_freeze(
        runtime_authority=runtime,
        evaluation_population_path=evaluation.resolve(),
        evaluation_population_raw_sha256=_sha(evaluation.read_bytes()),
        template_preparation_path=template.resolve(),
        template_preparation_sha256=_sha(template.read_bytes()),
        construction_catalog_path=catalog.resolve(),
        construction_catalog_sha256=_sha(catalog.read_bytes()),
        output_root=(tmp_path / "ladder-freeze").resolve(),
        execution_engine_commit="a" * 40,
        worker_contract_sha256=_sha("worker"),
        rotating_evidence={"rotatingEvidenceSha256": _sha("rotating")},
        archive_policy_authority={"policyBindingSha256": _sha("policy")},
        behavior_attribution_requirement={"schemaVersion": "fixture"},
        campaign_role="proposal_current_panel",
        panel_id="panel-validation",
        final_archive_reduction_result_path=reduction_receipt.resolve(),
        ladder_stage="validation",
        ladder_candidate_limit=1,
        ladder_authority={"ladderAuthoritySha256": _sha("ladder")},
    )
    assert captured["reduction"] == reduction_receipt.resolve()
    assert handoff["receipt"]["campaignSha256"] == _sha("campaign")
