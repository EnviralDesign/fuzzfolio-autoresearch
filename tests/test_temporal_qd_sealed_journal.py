"""Focused admission tests for the isolated sealed Temporal QD journal layer."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

import autoresearch.temporal_qd_sealed_journal as sealed_journal
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_qd_sealed_journal import (
    SealedProposalJournalError,
    measure_replay,
    open_sealed_proposal_journal,
)


def _authority() -> dict[str, object]:
    return {
        "schemaVersion": "test_temporal_qd_authority_v1",
        "authorityId": "sealed-journal-test-authority",
        "authoritySha256": canonical_sha256({"authority": "sealed-journal-test"}),
    }


def _config() -> dict[str, object]:
    return {
        "schemaVersion": "test_temporal_qd_config_v1",
        "generationIndex": 7,
        "seed": "sealed-journal-test-seed",
    }


def _identity(label: str, ordinal: int) -> str:
    return canonical_sha256({"label": label, "ordinal": ordinal})


def _entry(
    ordinal: int,
    *,
    candidate_identity: str | None = None,
    semantic_identity: str | None = None,
    accepted: bool = True,
    origin_kind: str = "random_immigrant",
    disposition: str | None = None,
    trace_bytes: int = 0,
) -> dict[str, object]:
    resolved_disposition = disposition or (
        "accepted" if accepted else "native_validator_rejected"
    )
    result: dict[str, object] = {
        "contentRef": {
            "contentKind": "candidate-program",
            "contentSha256": _identity("content", ordinal),
        },
        "delta": {
            "proposalSeed": ordinal,
            "mutationAudit": {"operation": "test", "trace": "x" * trace_bytes},
        },
        "originKind": origin_kind,
        "disposition": resolved_disposition,
        "legacyEntrySha256": _identity("legacy-entry", ordinal),
        "accepted": accepted,
        "schedulingDelta": {"proposalsObserved": 1, "immigrantAttempts": 1},
    }
    if accepted:
        result["candidateIdentitySha256"] = candidate_identity or _identity(
            "candidate", ordinal
        )
        result["semanticIdentitySha256"] = semantic_identity or _identity(
            "semantic", ordinal
        )
    return result


def _open(root: Path, *, interval: int = 4):
    return open_sealed_proposal_journal(
        root=root,
        authority=_authority(),
        config=_config(),
        checkpoint_interval_entries=interval,
    )


def _files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _symlink_or_skip(link: Path, target: Path, *, directory: bool = False) -> None:
    """Create a symlink, skipping only platforms where the OS forbids it."""

    try:
        link.symlink_to(target, target_is_directory=directory)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symlink adversarial tests unavailable: {exc}")


def test_uninterrupted_and_split_restart_are_byte_deterministic(tmp_path: Path) -> None:
    entries = [_entry(index) for index in range(12)]

    uninterrupted = _open(tmp_path / "uninterrupted", interval=4)
    for start in range(0, len(entries), 4):
        uninterrupted.append_batch(entries[start : start + 4])
    uninterrupted.seal_checkpoint()

    split_root = tmp_path / "split"
    split = _open(split_root, interval=4)
    split.append_batch(entries[:4])
    split = _open(split_root, interval=4)
    for start in range(4, len(entries), 4):
        split.append_batch(entries[start : start + 4])
    split.seal_checkpoint()

    assert split.snapshot() == uninterrupted.snapshot()
    assert _files(split_root) == _files(tmp_path / "uninterrupted")
    assert split.snapshot()["schedulingCounters"] == {
        "immigrantAttempts": 12,
        "proposalsObserved": 12,
    }


def test_checkpoint_tail_replay_matches_legacy_oracle_with_lower_bytes_and_memory(
    tmp_path: Path,
) -> None:
    root = tmp_path / "measurement"
    journal = _open(root, interval=16)
    entries = [_entry(index, trace_bytes=4096) for index in range(55)]
    for start in range(0, 48, 4):
        journal.append_batch(entries[start : start + 4])
    journal.append_batch(entries[48:])

    measurement = measure_replay(
        root=root,
        authority=_authority(),
        config=_config(),
        checkpoint_interval_entries=16,
    )

    assert measurement["stateEquivalent"] is True
    assert measurement["legacyFullHistory"]["entriesReplayed"] == 55
    assert measurement["legacyFullHistory"]["retainedHistoryEntryCount"] == 55
    assert measurement["sealedCheckpointTail"]["entriesReplayed"] == 7
    assert measurement["sealedCheckpointTail"]["bytesRead"] < measurement["legacyFullHistory"]["bytesRead"]
    assert measurement["bytesReadSaved"] > 0
    assert measurement["memoryNonRegression"] is True


def test_explicit_provenance_and_compact_counts_survive_split_reopen(
    tmp_path: Path,
) -> None:
    entries = [
        _entry(0, origin_kind="random_immigrant"),
        _entry(
            1,
            accepted=False,
            origin_kind="structural_offspring",
            disposition="duplicate_pair_genome",
        ),
        _entry(2, origin_kind="structural_offspring"),
        _entry(
            3,
            accepted=False,
            origin_kind="random_immigrant",
            disposition="native_validator_rejected",
        ),
    ]
    uninterrupted_root = tmp_path / "provenance-uninterrupted"
    uninterrupted = _open(uninterrupted_root, interval=2)
    uninterrupted.append_batch(entries[:2])
    uninterrupted.append_batch(entries[2:])

    split_root = tmp_path / "provenance-split"
    split = _open(split_root, interval=2)
    split.append_batch(entries[:2])
    split = _open(split_root, interval=2)
    split.append_batch(entries[2:])

    snapshot = split.snapshot()
    assert snapshot == uninterrupted.snapshot()
    assert snapshot["originProposalCounts"] == {
        "random_immigrant": 2,
        "structural_offspring": 2,
    }
    assert snapshot["originAcceptedCounts"] == {
        "random_immigrant": 1,
        "structural_offspring": 1,
    }
    assert snapshot["dispositionCounts"] == {
        "accepted": 2,
        "duplicate_pair_genome": 1,
        "native_validator_rejected": 1,
    }
    first_ref = snapshot["acceptedRefs"][0]
    assert first_ref["legacyEntrySha256"] == _identity("legacy-entry", 0)
    assert first_ref["originKind"] == "random_immigrant"
    assert first_ref["disposition"] == "accepted"

    segment = json.loads(
        (split_root / "segments" / "000000000000.segment.json").read_text(
            encoding="utf-8"
        )
    )
    assert segment["entries"][1]["originKind"] == "structural_offspring"
    assert segment["entries"][1]["disposition"] == "duplicate_pair_genome"
    assert segment["entries"][1]["legacyEntrySha256"] == _identity("legacy-entry", 1)
    assert "originKind" not in segment["entries"][1]["delta"]
    checkpoint = json.loads(
        (split_root / "checkpoints" / "000000000004.checkpoint.json").read_text(
            encoding="utf-8"
        )
    )
    assert checkpoint["originProposalCounts"] == snapshot["originProposalCounts"]
    assert checkpoint["dispositionCounts"] == snapshot["dispositionCounts"]


def test_explicit_provenance_fields_are_required_and_tamper_validated(
    tmp_path: Path,
) -> None:
    root = tmp_path / "provenance-corruption"
    journal = _open(root, interval=8)
    missing_legacy = _entry(0)
    missing_legacy.pop("legacyEntrySha256")
    with pytest.raises(SealedProposalJournalError, match="lacks required fields"):
        journal.append_batch([missing_legacy])

    mismatched = _entry(0)
    mismatched["disposition"] = "native_validator_rejected"
    with pytest.raises(SealedProposalJournalError, match="accepted state and disposition disagree"):
        journal.append_batch([mismatched])

    journal.append_batch([_entry(0)])
    journal.append_batch([_entry(1)])
    second = root / "segments" / "000000000001.segment.json"
    payload = json.loads(second.read_text(encoding="utf-8"))
    entry = payload["entries"][0]
    entry["originKind"] = "unsupported_origin"
    entry.pop("entrySha256")
    entry["entrySha256"] = canonical_sha256(entry)
    payload["journalHeadEntrySha256"] = entry["entrySha256"]
    payload.pop("segmentSha256")
    payload["segmentSha256"] = canonical_sha256(payload)
    second.write_bytes(
        (
            json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
    )
    with pytest.raises(SealedProposalJournalError, match="sealed proposal originKind"):
        _open(root, interval=8)


def test_rejects_duplicate_candidate_and_semantic_identities_before_publication(
    tmp_path: Path,
) -> None:
    journal = _open(tmp_path / "duplicates", interval=8)
    journal.append_batch([_entry(0)])
    first = journal.snapshot()["acceptedRefs"][0]

    with pytest.raises(
        SealedProposalJournalError, match="duplicate accepted candidate identity"
    ):
        journal.append_batch(
            [
                _entry(
                    1,
                    candidate_identity=first["candidateIdentitySha256"],
                    semantic_identity=_identity("semantic", 1),
                )
            ]
        )
    with pytest.raises(
        SealedProposalJournalError, match="duplicate accepted semantic identity"
    ):
        journal.append_batch(
            [
                _entry(
                    2,
                    candidate_identity=_identity("candidate", 2),
                    semantic_identity=first["semanticIdentitySha256"],
                )
            ]
        )

    assert journal.snapshot()["proposalCount"] == 1
    assert len(list((tmp_path / "duplicates" / "segments").glob("*.json"))) == 1


def test_strict_binding_and_divergent_existing_segment_are_refused(tmp_path: Path) -> None:
    root = tmp_path / "divergent"
    first_writer = _open(root, interval=8)
    stale_writer = _open(root, interval=8)
    first_writer.append_batch([_entry(0)])

    with pytest.raises(SealedProposalJournalError, match="divergent sealed proposal segment"):
        stale_writer.append_batch([_entry(1)])

    changed_config = _config()
    changed_config["seed"] = "different-seed"
    with pytest.raises(SealedProposalJournalError, match="authority/config binding mismatch"):
        open_sealed_proposal_journal(
            root=root,
            authority=_authority(),
            config=changed_config,
            checkpoint_interval_entries=8,
        )
    changed_authority = _authority()
    changed_authority["authorityId"] = "different-authority"
    with pytest.raises(SealedProposalJournalError, match="authority/config binding mismatch"):
        open_sealed_proposal_journal(
            root=root,
            authority=changed_authority,
            config=_config(),
            checkpoint_interval_entries=8,
        )


def test_orphan_partial_write_is_cleaned_but_truncated_final_segment_fails_closed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "partial"
    journal = _open(root, interval=8)
    journal.append_batch([_entry(0)])
    orphan = root / "segments" / ".lost-write.sealed-journal.tmp"
    orphan.write_bytes(b'{"partial":')

    resumed = _open(root, interval=8)
    assert resumed.snapshot()["proposalCount"] == 1
    assert not orphan.exists()

    segment = root / "segments" / "000000000000.segment.json"
    segment.write_bytes(segment.read_bytes()[:31])
    with pytest.raises(SealedProposalJournalError, match="corrupt or partially written"):
        _open(root, interval=8)


def test_reordered_segments_and_stale_checkpoint_fail_closed(tmp_path: Path) -> None:
    reordered_root = tmp_path / "reordered"
    journal = _open(reordered_root, interval=8)
    journal.append_batch([_entry(0)])
    journal.append_batch([_entry(1)])
    first = reordered_root / "segments" / "000000000000.segment.json"
    second = reordered_root / "segments" / "000000000001.segment.json"
    temporary = reordered_root / "segments" / "swap.json"
    first.replace(temporary)
    second.replace(first)
    temporary.replace(second)
    with pytest.raises(SealedProposalJournalError, match="reordered, duplicated, or gapped"):
        _open(reordered_root, interval=8)

    stale_root = tmp_path / "stale-checkpoint"
    journal = _open(stale_root, interval=2)
    journal.append_batch([_entry(0), _entry(1)])
    original = stale_root / "checkpoints" / "000000000002.checkpoint.json"
    stale = stale_root / "checkpoints" / "000000000003.checkpoint.json"
    shutil.copyfile(original, stale)
    with pytest.raises(SealedProposalJournalError, match="checkpoint filename is stale"):
        _open(stale_root, interval=2)


def test_rehashed_segment_with_a_divergent_predecessor_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "chain"
    journal = _open(root, interval=8)
    journal.append_batch([_entry(0)])
    journal.append_batch([_entry(1)])

    second = root / "segments" / "000000000001.segment.json"
    payload = json.loads(second.read_text(encoding="utf-8"))
    payload["priorSegmentSha256"] = _identity("wrong-predecessor", 1)
    payload.pop("segmentSha256")
    payload["segmentSha256"] = canonical_sha256(payload)
    second.write_bytes(
        (
            json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
    )

    with pytest.raises(SealedProposalJournalError, match="segment hash chain diverged"):
        _open(root, interval=8)


def test_compact_object_store_reference_can_cross_the_batch_boundary(tmp_path: Path) -> None:
    journal = _open(tmp_path / "object-ref", interval=8)
    entry = _entry(0)
    entry["contentRef"] = {
        "namespace": {
            "type": "candidate-program",
            "schemaVersion": 1,
            "codec": "canonical-json-v1",
        },
        "sha256": _identity("object", 0),
        "byteLength": 123,
    }
    journal.append_batch([entry])

    reference = journal.snapshot()["acceptedRefs"][0]["contentRef"]
    assert reference["sha256"] == _identity("object", 0)
    assert reference["contentSha256"] == _identity("object", 0)


def test_content_reference_aliases_must_agree(tmp_path: Path) -> None:
    journal = _open(tmp_path / "content-aliases", interval=8)
    first_identity = _identity("content", 0)
    conflicting = _entry(0)
    conflicting["contentRef"] = {
        "contentSha256": first_identity,
        "objectSha256": _identity("other-content", 0),
        "sha256": first_identity,
    }
    with pytest.raises(
        SealedProposalJournalError, match="conflicting content identity aliases"
    ):
        journal.append_batch([conflicting])

    agreeing = _entry(0)
    agreeing["contentRef"] = {
        "contentSha256": first_identity,
        "objectSha256": first_identity,
        "sha256": first_identity,
        "namespace": {"type": "candidate-program", "schemaVersion": 1},
    }
    journal.append_batch([agreeing])
    reference = journal.snapshot()["acceptedRefs"][0]["contentRef"]
    assert reference["contentSha256"] == first_identity
    assert reference["objectSha256"] == first_identity
    assert reference["sha256"] == first_identity


def test_checkpoint_failure_returns_a_committed_segment_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "checkpoint-receipt"
    journal = _open(root, interval=1)
    original_publish = sealed_journal._publish_immutable_json

    def fail_only_checkpoint(
        root: Path, path: Path, payload: object, *, name: str
    ) -> None:
        if name == "proposal checkpoint":
            raise OSError("injected checkpoint persistence fault")
        original_publish(root, path, payload, name=name)

    monkeypatch.setattr(sealed_journal, "_publish_immutable_json", fail_only_checkpoint)
    receipt = journal.append_batch([_entry(0)])

    assert receipt.segment_committed is True
    assert receipt.checkpoint_attempted is True
    assert receipt.checkpoint_written is False
    assert receipt.checkpoint_error == "OSError: injected checkpoint persistence fault"
    assert receipt.proposal_count == 1
    assert journal.snapshot()["proposalCount"] == 1
    assert len(list((root / "segments").glob("*.segment.json"))) == 1
    assert receipt.as_payload()["segmentCommitted"] is True
    assert receipt.as_payload()["checkpointError"] == receipt.checkpoint_error

    # A caller treats the receipt as committed, not as a reason to append the
    # decision again.  The explicit checkpoint retry is immutable/idempotent.
    monkeypatch.setattr(sealed_journal, "_publish_immutable_json", original_publish)
    resumed = _open(root, interval=1)
    assert resumed.snapshot()["proposalCount"] == 1
    resumed.seal_checkpoint()
    resumed.seal_checkpoint()

    reopened = _open(root, interval=1)
    assert reopened.snapshot()["proposalCount"] == 1
    assert len(list((root / "segments").glob("*.segment.json"))) == 1
    second_receipt = reopened.append_batch([_entry(1)])
    assert second_receipt.segment_ordinal == 1
    assert second_receipt.proposal_count == 2
    assert _open(root, interval=1).snapshot()["proposalCount"] == 2


def test_rejects_a_symlinked_journal_root(tmp_path: Path) -> None:
    outside = tmp_path / "outside-root"
    outside.mkdir()
    linked_root = tmp_path / "linked-root"
    _symlink_or_skip(linked_root, outside, directory=True)

    with pytest.raises(SealedProposalJournalError, match="symlink or junction"):
        _open(linked_root)
    assert not list(outside.iterdir())


def test_rejects_a_symlinked_journal_intermediate_directory(tmp_path: Path) -> None:
    root = tmp_path / "intermediate-link"
    root.mkdir()
    outside = tmp_path / "outside-intermediate"
    outside.mkdir()
    _symlink_or_skip(root / "segments", outside, directory=True)

    with pytest.raises(SealedProposalJournalError, match="symlink or junction"):
        _open(root)
    assert not list(outside.iterdir())


def test_rejects_a_symlinked_final_segment_artifact(tmp_path: Path) -> None:
    root = tmp_path / "final-link"
    journal = _open(root, interval=8)
    journal.append_batch([_entry(0)])
    segment = root / "segments" / "000000000000.segment.json"
    outside = tmp_path / "outside-final-segment.json"
    outside.write_bytes(segment.read_bytes())
    segment.unlink()
    _symlink_or_skip(segment, outside)

    with pytest.raises(SealedProposalJournalError, match="symlink or junction"):
        _open(root, interval=8)
    assert outside.read_bytes()


def test_rejects_a_root_path_swap_after_open(tmp_path: Path) -> None:
    root = tmp_path / "swapped-root"
    journal = _open(root, interval=8)
    preserved_root = tmp_path / "preserved-root"
    root.rename(preserved_root)
    outside = tmp_path / "outside-swapped-root"
    outside.mkdir()
    _symlink_or_skip(root, outside, directory=True)

    with pytest.raises(SealedProposalJournalError, match="symlink or junction"):
        journal.append_batch([_entry(0)])
    assert not list(outside.iterdir())


def test_rejects_an_intermediate_directory_swap_after_open(tmp_path: Path) -> None:
    root = tmp_path / "swapped-intermediate"
    journal = _open(root, interval=8)
    segments = root / "segments"
    segments.mkdir()
    outside = tmp_path / "outside-swapped-intermediate"
    outside.mkdir()
    segments.rmdir()
    _symlink_or_skip(segments, outside, directory=True)

    with pytest.raises(SealedProposalJournalError, match="symlink or junction"):
        journal.append_batch([_entry(0)])
    assert not list(outside.iterdir())


def test_rejects_a_final_artifact_swap_between_validation_and_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "final-swap"
    journal = _open(root, interval=8)
    journal.append_batch([_entry(0)])
    segment = root / "segments" / "000000000000.segment.json"
    outside = tmp_path / "outside-final-swap.json"
    outside.write_bytes(segment.read_bytes())
    probe = tmp_path / "symlink-probe"
    _symlink_or_skip(probe, outside)
    probe.unlink()

    original_open = sealed_journal.os.open
    swapped = False

    def swap_then_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal swapped
        if not swapped and Path(os.fspath(path)) == segment:
            segment.unlink()
            segment.symlink_to(outside)
            swapped = True
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(sealed_journal.os, "open", swap_then_open)
    with pytest.raises(SealedProposalJournalError):
        _open(root, interval=8)
    assert swapped is True
