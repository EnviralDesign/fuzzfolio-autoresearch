from __future__ import annotations

import io
import json
import multiprocessing
from pathlib import Path

import pytest

import autoresearch.temporal_qd_object_store as object_store
from autoresearch.result_codec import semantic_sha256
from autoresearch.temporal_qd_object_store import (
    CANONICAL_JSON_CODEC,
    CompressedPackedTemporalQDObjectStore,
    RAW_BYTES_CODEC,
    ObjectNamespace,
    ObjectStoreIntegrityError,
    ObjectStoreNotFoundError,
    ObjectStorePathError,
    PackedTemporalQDObjectStore,
    TemporalQDObjectStore,
    TemporalQDObjectStoreError,
    benchmark_duplicate_writes_vs_cas,
    benchmark_compressed_packed_vs_uncompressed,
    benchmark_loose_vs_packed,
    build_manifest,
    canonical_json_bytes,
    prepare_bytes,
    prepare_canonical_json_bytes,
    prepare_json,
    sha256_bytes,
)


def _overlapping_packed_publish_worker(
    root: str,
    backend: str,
    payloads: tuple[bytes, bytes],
    start: object,
    results: object,
) -> None:
    """Spawn-safe worker used to exercise the real cross-process lock."""

    store_type = (
        PackedTemporalQDObjectStore
        if backend == "packed"
        else CompressedPackedTemporalQDObjectStore
    )
    store = store_type(root)
    namespace = ObjectNamespace("multiprocess_overlap", 1, RAW_BYTES_CODEC)
    prepared = [prepare_bytes(namespace, payload) for payload in payloads]
    assert getattr(start, "wait")(20), "parent did not release publication workers"
    try:
        result = store.put_many(prepared)
    except Exception as exc:  # pragma: no cover - returned to parent assertion.
        getattr(results, "put")(("error", type(exc).__name__, str(exc)))
    else:
        getattr(results, "put")(("ok", result.created_count, result.reused_count))


def test_json_objects_use_the_existing_canonical_bytes_oracle(tmp_path: Path) -> None:
    store = TemporalQDObjectStore(tmp_path / "objects")
    namespace = ObjectNamespace("candidate_genome", 1)
    value = {"z": [3, 2, 1], "a": {"unicode": "é"}}

    ref = store.put_json(namespace, value)

    assert ref.namespace.codec == CANONICAL_JSON_CODEC
    assert ref.sha256 == semantic_sha256(value)
    assert store.path_for(ref).read_bytes() == canonical_json_bytes(value)
    assert store.get_json(ref) == value
    # Object key order has no effect, and the existing exact bytes are accepted.
    assert store.put_json(namespace, {"a": {"unicode": "é"}, "z": [3, 2, 1]}) == ref
    version_two = store.put_json(ObjectNamespace("candidate_genome", 2), value)
    assert version_two.sha256 == ref.sha256
    assert version_two.namespace != ref.namespace
    assert store.path_for(version_two) != store.path_for(ref)

    with pytest.raises(ObjectStoreIntegrityError, match="not exact canonical"):
        prepare_canonical_json_bytes(namespace, b'{"z":[3,2,1],"a":{}}')


def test_streaming_raw_bytes_and_verified_open_api(tmp_path: Path) -> None:
    store = TemporalQDObjectStore(tmp_path / "objects")
    namespace = ObjectNamespace("worker_blob", 1, RAW_BYTES_CODEC)
    payload = (b"temporal-qd-stream-" * 4096) + b"done"

    ref = store.put_stream(
        namespace,
        io.BytesIO(payload),
        expected_byte_length=len(payload),
        chunk_bytes=257,
    )

    with store.open(ref) as reader:
        assert reader.read(101) == payload[:101]
        # Exiting drains the remaining bytes and checks digest + length.
    assert b"".join(store.iter_bytes(ref, chunk_bytes=509)) == payload
    assert store.get_bytes(ref) == payload
    assert list(store.get_many([ref])) == [(ref, payload)]


def test_existing_exact_bytes_are_reused_but_corruption_is_never_overwritten(
    tmp_path: Path,
) -> None:
    store = TemporalQDObjectStore(tmp_path / "objects")
    namespace = ObjectNamespace("program_blob", 1, RAW_BYTES_CODEC)
    prepared = prepare_bytes(namespace, b"exact immutable source")

    assert store.put_prepared(prepared) == prepared.ref
    assert store.put_prepared(prepared) == prepared.ref
    path = store.path_for(prepared.ref)
    path.write_bytes(b"tampered")

    with pytest.raises(ObjectStoreIntegrityError, match="corrupt or divergent"):
        store.put_prepared(prepared)
    with pytest.raises(ObjectStoreIntegrityError, match="immutable reference"):
        store.get_bytes(prepared.ref)
    assert path.read_bytes() == b"tampered"


def test_racing_divergent_publication_is_rejected_without_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = TemporalQDObjectStore(tmp_path / "objects")
    namespace = ObjectNamespace("program_blob", 1, RAW_BYTES_CODEC)
    prepared = prepare_bytes(namespace, b"our immutable payload")
    target = store.path_for(prepared.ref)
    target.parent.mkdir(parents=True, exist_ok=True)

    def competing_link(source: Path, destination: Path) -> None:
        destination.write_bytes(b"competing writer bytes")
        raise FileExistsError(destination)

    monkeypatch.setattr(object_store.os, "link", competing_link)
    with pytest.raises(ObjectStoreIntegrityError, match="corrupt or divergent"):
        store.put_prepared(prepared)
    assert target.read_bytes() == b"competing writer bytes"


def test_path_validation_and_symlink_escape_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(TemporalQDObjectStoreError, match="namespace type"):
        ObjectNamespace("../escape", 1)

    store = TemporalQDObjectStore(tmp_path / "objects")
    namespace = ObjectNamespace("catalog_blob", 1, RAW_BYTES_CODEC)
    prepared = prepare_bytes(namespace, b"safe source")
    target = store.path_for(prepared.ref)
    target.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside data")
    try:
        target.symlink_to(outside)
    except (NotImplementedError, OSError):
        pytest.skip("file symlinks are unavailable in this test environment")

    with pytest.raises(ObjectStorePathError, match="symlink"):
        store.put_prepared(prepared)
    with pytest.raises(ObjectStorePathError, match="symlink"):
        store.get_bytes(prepared.ref)
    assert outside.read_bytes() == b"outside data"


def test_failed_payload_fsync_leaves_only_ignored_temp_for_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = TemporalQDObjectStore(tmp_path / "objects")
    namespace = ObjectNamespace("large_blob", 1, RAW_BYTES_CODEC)
    prepared = prepare_bytes(namespace, b"payload that must not publish after fsync failure")
    # Avoid a directory fsync being the injected failure; the object directory
    # now exists, so the next fsync is for the temporary payload itself.
    store.path_for(prepared.ref).parent.mkdir(parents=True, exist_ok=True)
    real_fsync = object_store.os.fsync
    failed = False

    def fail_first_fsync(descriptor: int) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected payload fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(object_store.os, "fsync", fail_first_fsync)
    with pytest.raises(OSError, match="injected payload"):
        store.put_prepared(prepared)
    monkeypatch.setattr(object_store.os, "fsync", real_fsync)

    assert not store.path_for(prepared.ref).exists()
    assert store.partial_temp_paths()
    with pytest.raises(ObjectStoreNotFoundError):
        store.get_bytes(prepared.ref)
    assert store.put_prepared(prepared) == prepared.ref
    assert store.get_bytes(prepared.ref) == prepared.data


def test_batch_publication_and_manifest_references_are_deterministic(
    tmp_path: Path,
) -> None:
    store = TemporalQDObjectStore(tmp_path / "objects")
    genome_namespace = ObjectNamespace("candidate_genome", 1)
    archive_namespace = ObjectNamespace("archive_manifest", 1)
    first = prepare_json(genome_namespace, {"candidate": "a", "score": 1})
    second = prepare_json(genome_namespace, {"candidate": "b", "score": 2})

    batch = store.put_many([second, first, first])

    assert batch.refs == (second.ref, first.ref, first.ref)
    assert batch.created_count == 2
    assert batch.reused_count == 1
    assert batch.bytes_written == len(first.data) + len(second.data)
    assert dict(store.get_many([first.ref, second.ref])) == {
        first.ref: first.data,
        second.ref: second.data,
    }

    forward = build_manifest(
        manifest_type="generation.archive",
        manifest_version=1,
        refs=[first.ref, second.ref],
        metadata={"z": 2, "a": 1},
    )
    reverse = build_manifest(
        manifest_type="generation.archive",
        manifest_version=1,
        refs=[second.ref, first.ref],
        metadata={"a": 1, "z": 2},
    )
    assert forward == reverse
    manifest_ref = store.put_manifest(
        archive_namespace,
        manifest_type="generation.archive",
        manifest_version=1,
        refs=[second.ref, first.ref],
        metadata={"z": 2, "a": 1},
    )
    assert store.get_manifest(manifest_ref) == forward


def test_bounded_duplicate_write_vs_cas_reuse_benchmark(tmp_path: Path) -> None:
    evidence = benchmark_duplicate_writes_vs_cas(
        tmp_path / "benchmark",
        repetitions=3,
        small_payload_bytes=2 * 1024 * 1024,
        medium_payload_bytes=6 * 1024 * 1024,
    )

    assert evidence["schemaVersion"] == "temporal_qd_object_store_reuse_benchmark_v1"
    for case in evidence["cases"].values():
        assert case["duplicated"]["bytesWritten"] == case["payloadBytes"] * 3
        assert case["casReuse"]["bytesWritten"] == case["payloadBytes"]
        assert case["casReuse"]["createdCount"] == 1
        assert case["casReuse"]["reusedCount"] == 2
        assert case["memoryWithinBound"]


def _only_packed_index(root: Path) -> Path:
    indexes = sorted((root / "indexes").glob("*.index.json"))
    assert len(indexes) == 1
    return indexes[0]


def _rewrite_checked_packed_index(path: Path, payload: dict[str, object]) -> None:
    """Persist a deliberately changed but internally checksummed test index."""

    body = dict(payload)
    body.pop("indexSha256", None)
    body["indexSha256"] = sha256_bytes(canonical_json_bytes(body))
    path.write_bytes(canonical_json_bytes(body))


def test_packed_store_has_deterministic_full_split_and_reopen_parity(tmp_path: Path) -> None:
    namespace = ObjectNamespace("packed_profile", 1)
    prepared = [
        prepare_json(
            namespace,
            {"candidate": candidate, "score": score, "payload": "x" * 31},
        )
        for candidate, score in (("alpha", 1), ("bravo", 2), ("charlie", 3))
    ]
    inputs = [prepared[2], prepared[0], prepared[1], prepared[0]]

    loose = TemporalQDObjectStore(tmp_path / "loose")
    full = PackedTemporalQDObjectStore(tmp_path / "packed-full")
    full_again = PackedTemporalQDObjectStore(tmp_path / "packed-full-again")
    split = PackedTemporalQDObjectStore(tmp_path / "packed-split")

    loose_result = loose.put_many(inputs)
    full_result = full.put_many(inputs)
    full_again.put_many(list(reversed(inputs[:3])))
    split.put_many(prepared[:2])
    split.put_many(prepared[2:])

    assert full_result.refs == loose_result.refs
    assert full_result.created_count == 3
    assert full_result.reused_count == 1
    assert full_result.bytes_written == loose_result.bytes_written
    refs = tuple(item.ref for item in inputs[:3])
    expected = [(ref, loose.get_bytes(ref)) for ref in refs]
    assert list(full.get_many(refs)) == expected
    assert full.last_batch_read_metrics == {
        "objectsRead": 3,
        "objectBytesRead": sum(len(data) for _ref, data in expected),
        "packFileOpens": 1,
    }
    with full.open(prepared[0].ref) as reader:
        assert reader.read(7) + reader.read() == prepared[0].data
    assert [full.get_json(ref) for ref in refs] == [loose.get_json(ref) for ref in refs]
    assert [split.get_json(ref) for ref in refs] == [loose.get_json(ref) for ref in refs]

    # The full-batch output is independent of caller order; splitting changes
    # pack boundaries but never ObjectRef identity or hydrated bytes.
    assert sorted((full.root / "packs").glob("*.pack"))[0].read_bytes() == sorted(
        (full_again.root / "packs").glob("*.pack")
    )[0].read_bytes()
    assert _only_packed_index(full.root).read_bytes() == _only_packed_index(
        full_again.root
    ).read_bytes()
    assert len(list((full.root / "packs").glob("*.pack"))) == 1
    assert len(list((split.root / "packs").glob("*.pack"))) == 2

    reopened = PackedTemporalQDObjectStore(full.root)
    assert [reopened.get_json(ref) for ref in refs] == [loose.get_json(ref) for ref in refs]
    assert reopened.location_for(prepared[1].ref).pack_path == full.path_for(prepared[1].ref)


def test_packed_store_deduplicates_across_packs_and_preserves_streaming_surface(
    tmp_path: Path,
) -> None:
    namespace = ObjectNamespace("packed_blob", 1, RAW_BYTES_CODEC)
    first = prepare_bytes(namespace, b"first packed object")
    second = prepare_bytes(namespace, b"second packed object")
    third = prepare_bytes(namespace, b"third packed object")
    store = PackedTemporalQDObjectStore(tmp_path / "packed")

    initial = store.put_many([first, second])
    follow_up = store.put_many([second, third, second])
    assert initial.created_count == 2
    assert follow_up.created_count == 1
    assert follow_up.reused_count == 2
    assert follow_up.bytes_written == len(third.data)
    assert len(list((store.root / "packs").glob("*.pack"))) == 2

    stream_payload = b"streamed packed bytes" * 4096
    stream_ref = store.put_stream(
        namespace,
        io.BytesIO(stream_payload),
        expected_sha256=sha256_bytes(stream_payload),
        expected_byte_length=len(stream_payload),
        chunk_bytes=257,
    )
    pack_count = len(list((store.root / "packs").glob("*.pack")))
    assert store.put_stream(namespace, io.BytesIO(stream_payload), chunk_bytes=113) == stream_ref
    assert len(list((store.root / "packs").glob("*.pack"))) == pack_count
    assert b"".join(store.iter_bytes(stream_ref, chunk_bytes=509)) == stream_payload
    assert PackedTemporalQDObjectStore(store.root).get_bytes(third.ref) == third.data


def test_packed_reopen_rejects_index_corruption_and_deferred_payload_tampering(
    tmp_path: Path,
) -> None:
    namespace = ObjectNamespace("packed_corruption", 1, RAW_BYTES_CODEC)

    def build_store(name: str) -> tuple[PackedTemporalQDObjectStore, object, object]:
        store = PackedTemporalQDObjectStore(tmp_path / name)
        # Equal-length payloads let the duplicate-reference case reach the
        # explicit duplicate guard rather than failing a length precondition.
        first = prepare_bytes(namespace, b"alpha")
        second = prepare_bytes(namespace, b"bravo")
        store.put_many([first, second])
        return store, first, second

    tampered, first, _second = build_store("tampered-payload")
    location = tampered.location_for(first.ref)
    pack_bytes = bytearray(location.pack_path.read_bytes())
    pack_bytes[location.offset] ^= 0x01
    location.pack_path.write_bytes(pack_bytes)
    reopened_tampered = PackedTemporalQDObjectStore(tampered.root)
    # Reopen validates compact indexes and pack length without reading every
    # payload; the addressed read must still reject a same-length mutation.
    with pytest.raises(ObjectStoreIntegrityError, match="packed object.*immutable reference"):
        reopened_tampered.get_bytes(first.ref)

    overlap, _first, _second = build_store("overlap-index")
    overlap_index = _only_packed_index(overlap.root)
    overlap_payload = json.loads(overlap_index.read_text(encoding="utf-8"))
    overlap_payload["entries"][1]["offset"] = 0
    _rewrite_checked_packed_index(overlap_index, overlap_payload)
    with pytest.raises(ObjectStoreIntegrityError, match="overlapping|non-contiguous"):
        PackedTemporalQDObjectStore(overlap.root)

    duplicate, duplicate_first, _duplicate_second = build_store("duplicate-index")
    duplicate_index = _only_packed_index(duplicate.root)
    duplicate_payload = json.loads(duplicate_index.read_text(encoding="utf-8"))
    duplicate_payload["entries"][1]["ref"] = duplicate_first.ref.as_dict()
    _rewrite_checked_packed_index(duplicate_index, duplicate_payload)
    with pytest.raises(ObjectStoreIntegrityError, match="duplicate object references"):
        PackedTemporalQDObjectStore(duplicate.root)

    divergent, _first, _second = build_store("divergent-index")
    divergent_index = _only_packed_index(divergent.root)
    divergent_payload = json.loads(divergent_index.read_text(encoding="utf-8"))
    divergent_payload["packSha256"] = "sha256:" + "0" * 64
    _rewrite_checked_packed_index(divergent_index, divergent_payload)
    with pytest.raises(ObjectStoreIntegrityError, match="batch identity"):
        PackedTemporalQDObjectStore(divergent.root)


def test_packed_store_rejects_partial_final_traversal_and_symlink_artifacts(
    tmp_path: Path,
) -> None:
    namespace = ObjectNamespace("packed_path", 1, RAW_BYTES_CODEC)
    prepared = prepare_bytes(namespace, b"safe packed payload")

    ignored = PackedTemporalQDObjectStore(tmp_path / "ignored-partials")
    ignored.put_prepared(prepared)
    (ignored.root / "packs" / "interrupted.pack.tmp").write_bytes(b"incomplete")
    (ignored.root / "indexes" / "interrupted.index.json.tmp").write_bytes(b"{")
    reopened_ignored = PackedTemporalQDObjectStore(ignored.root)
    assert reopened_ignored.get_bytes(prepared.ref) == prepared.data
    assert {path.name for path in reopened_ignored.partial_temp_paths()} >= {
        "interrupted.pack.tmp",
        "interrupted.index.json.tmp",
    }

    partial = PackedTemporalQDObjectStore(tmp_path / "partial-final-index")
    partial.put_prepared(prepared)
    (partial.root / "indexes" / ("0" * 64 + ".index.json")).write_bytes(b"{")
    with pytest.raises(ObjectStoreIntegrityError, match="partial or corrupt"):
        PackedTemporalQDObjectStore(partial.root)

    traversal = PackedTemporalQDObjectStore(tmp_path / "traversal-index")
    traversal.put_prepared(prepared)
    traversal_index = _only_packed_index(traversal.root)
    traversal_payload = json.loads(traversal_index.read_text(encoding="utf-8"))
    traversal_payload["packFile"] = "../outside.pack"
    _rewrite_checked_packed_index(traversal_index, traversal_payload)
    with pytest.raises(ObjectStoreIntegrityError, match="pack filename"):
        PackedTemporalQDObjectStore(traversal.root)

    linked = PackedTemporalQDObjectStore(tmp_path / "symlink-pack")
    linked.put_prepared(prepared)
    linked_pack = linked.path_for(prepared.ref)
    outside = tmp_path / "outside.pack"
    outside.write_bytes(linked_pack.read_bytes())
    linked_pack.unlink()
    try:
        linked_pack.symlink_to(outside)
    except (NotImplementedError, OSError):
        pytest.skip("file symlinks are unavailable in this test environment")
    with pytest.raises(ObjectStorePathError, match="symlink"):
        PackedTemporalQDObjectStore(linked.root)


def test_bounded_loose_vs_packed_benchmark_has_file_and_read_amortization(
    tmp_path: Path,
) -> None:
    evidence = benchmark_loose_vs_packed(
        tmp_path / "packed-benchmark",
        object_count=200,
        small_payload_bytes=256,
        coarse_payload_bytes=2048,
    )

    assert evidence["schemaVersion"] == "temporal_qd_packed_object_store_benchmark_v1"
    for case in evidence["cases"].values():
        assert case["loose"]["bytesWritten"] == case["payloadBytes"]
        assert case["packed"]["bytesWritten"] == case["payloadBytes"]
        assert case["packed"]["fileCount"] < case["loose"]["fileCount"]
        assert case["packed"]["readFileOpens"] < case["loose"]["readFileOpens"]
        assert case["allocatedBytesWithinBound"]
        assert case["memoryWithinBound"]


def _only_compressed_packed_index(root: Path) -> Path:
    indexes = sorted((root / "compressed-indexes").glob("*.zindex.json"))
    assert len(indexes) == 1
    return indexes[0]


def test_compressed_packed_store_preserves_exact_refs_and_batch_parity(
    tmp_path: Path,
) -> None:
    namespace = ObjectNamespace("compressed_profile", 1)
    prepared = [
        prepare_json(
            namespace,
            {"candidate": candidate, "payload": "same canonical body " * 512},
        )
        for candidate in ("alpha", "bravo", "charlie")
    ]
    loose = TemporalQDObjectStore(tmp_path / "loose")
    packed = PackedTemporalQDObjectStore(tmp_path / "packed")
    compressed = CompressedPackedTemporalQDObjectStore(tmp_path / "compressed")
    inputs = [prepared[2], prepared[0], prepared[1], prepared[0]]

    loose_result = loose.put_many(inputs)
    packed_result = packed.put_many(inputs)
    compressed_result = compressed.put_many(inputs)

    assert compressed_result.refs == loose_result.refs == packed_result.refs
    assert compressed_result.created_count == 3
    assert compressed_result.reused_count == 1
    assert compressed_result.bytes_written == loose_result.bytes_written
    refs = tuple(item.ref for item in inputs[:3])
    expected = [(ref, loose.get_bytes(ref)) for ref in refs]
    assert list(compressed.get_many(refs)) == expected
    assert compressed.last_batch_read_metrics == {
        "objectsRead": 3,
        "objectBytesRead": sum(len(data) for _ref, data in expected),
        "packFileOpens": 1,
    }
    with compressed.open(prepared[0].ref) as reader:
        assert reader.read(17) + reader.read() == prepared[0].data
    assert [compressed.get_json(ref) for ref in refs] == [loose.get_json(ref) for ref in refs]

    compressed_pack = compressed.path_for(prepared[0].ref)
    packed_pack = packed.path_for(prepared[0].ref)
    assert compressed_pack.stat().st_size < packed_pack.stat().st_size
    assert len(list((compressed.root / "compressed-packs").glob("*.zpack"))) == 1
    assert len(list((compressed.root / "compressed-indexes").glob("*.zindex.json"))) == 1
    reopened = CompressedPackedTemporalQDObjectStore(compressed.root)
    assert list(reopened.get_many(refs)) == expected


def test_compressed_packed_dedup_and_streaming_keep_raw_bytes_exact(tmp_path: Path) -> None:
    namespace = ObjectNamespace("compressed_blob", 1, RAW_BYTES_CODEC)
    first = prepare_bytes(namespace, b"first compressed immutable payload")
    second = prepare_bytes(namespace, b"second compressed immutable payload")
    third = prepare_bytes(namespace, b"third compressed immutable payload")
    store = CompressedPackedTemporalQDObjectStore(tmp_path / "compressed")

    initial = store.put_many([first, second])
    follow_up = store.put_many([second, third, second])
    assert initial.created_count == 2
    assert follow_up.created_count == 1
    assert follow_up.reused_count == 2
    assert follow_up.bytes_written == len(third.data)
    assert len(list((store.root / "compressed-packs").glob("*.zpack"))) == 2

    stream_payload = b"compressed streamed exact bytes" * 4096
    stream_ref = store.put_stream(
        namespace,
        io.BytesIO(stream_payload),
        expected_sha256=sha256_bytes(stream_payload),
        expected_byte_length=len(stream_payload),
        chunk_bytes=313,
    )
    pack_count = len(list((store.root / "compressed-packs").glob("*.zpack")))
    assert store.put_stream(namespace, io.BytesIO(stream_payload), chunk_bytes=101) == stream_ref
    assert len(list((store.root / "compressed-packs").glob("*.zpack"))) == pack_count
    assert b"".join(store.iter_bytes(stream_ref, chunk_bytes=509)) == stream_payload
    assert store.get_bytes(third.ref) == third.data


def test_compressed_packed_store_fails_closed_on_index_and_block_tampering(
    tmp_path: Path,
) -> None:
    namespace = ObjectNamespace("compressed_corruption", 1, RAW_BYTES_CODEC)
    first = prepare_bytes(namespace, b"alpha" * 4096)
    second = prepare_bytes(namespace, b"bravo" * 4096)

    def build_store(name: str) -> CompressedPackedTemporalQDObjectStore:
        store = CompressedPackedTemporalQDObjectStore(tmp_path / name)
        store.put_many([first, second])
        return store

    tampered = build_store("tampered-block")
    pack = tampered.path_for(first.ref)
    pack_bytes = bytearray(pack.read_bytes())
    pack_bytes[len(pack_bytes) // 2] ^= 0x01
    pack.write_bytes(pack_bytes)
    reopened = CompressedPackedTemporalQDObjectStore(tampered.root)
    with pytest.raises(ObjectStoreIntegrityError, match="compressed"):
        reopened.get_bytes(first.ref)

    truncated = build_store("truncated-block")
    truncated_pack = truncated.path_for(first.ref)
    truncated_pack.write_bytes(truncated_pack.read_bytes()[:-1])
    with pytest.raises(ObjectStoreIntegrityError, match="truncated|length-mismatched"):
        CompressedPackedTemporalQDObjectStore(truncated.root)

    noncanonical = build_store("noncanonical-index")
    noncanonical_index = _only_compressed_packed_index(noncanonical.root)
    noncanonical_index.write_bytes(noncanonical_index.read_bytes() + b"\n")
    with pytest.raises(ObjectStoreIntegrityError, match="exact canonical"):
        CompressedPackedTemporalQDObjectStore(noncanonical.root)

    mismatched = build_store("mismatched-raw-length")
    mismatched_index = _only_compressed_packed_index(mismatched.root)
    mismatched_payload = json.loads(mismatched_index.read_text(encoding="utf-8"))
    mismatched_payload["rawPackByteLength"] += 1
    _rewrite_checked_packed_index(mismatched_index, mismatched_payload)
    with pytest.raises(ObjectStoreIntegrityError, match="exactly cover raw pack bytes"):
        CompressedPackedTemporalQDObjectStore(mismatched.root)

    bomb = build_store("declared-bomb")
    bomb_index = _only_compressed_packed_index(bomb.root)
    bomb_payload = json.loads(bomb_index.read_text(encoding="utf-8"))
    bomb_payload["rawPackByteLength"] = object_store.MAX_COMPRESSED_PACK_RAW_BYTES + 1
    _rewrite_checked_packed_index(bomb_index, bomb_payload)
    with pytest.raises(ObjectStoreIntegrityError, match="exceeds safety limit"):
        CompressedPackedTemporalQDObjectStore(bomb.root)


def test_bounded_compressed_packed_benchmark_reports_storage_and_whole_batch_evidence(
    tmp_path: Path,
) -> None:
    evidence = benchmark_compressed_packed_vs_uncompressed(
        tmp_path / "compressed-benchmark",
        repetitive_object_count=8,
        repetitive_total_payload_bytes=2 * 1024 * 1024,
        small_object_count=100,
        small_payload_bytes=128,
    )

    assert evidence["schemaVersion"] == "temporal_qd_compressed_packed_benchmark_v1"
    assert evidence["compressionCodec"] == "zlib-deflate-v1"
    for case in evidence["cases"].values():
        assert case["direct"]["bytesWritten"] == case["payloadBytes"]
        assert case["packed"]["bytesWritten"] == case["payloadBytes"]
        assert case["compressed"]["bytesWritten"] == case["payloadBytes"]
        assert case["compressed"]["packBytes"] < case["packed"]["packBytes"]
        assert case["compressed"]["readFileOpens"] == 1
        assert case["packed"]["readFileOpens"] == 1
        assert case["direct"]["readFileOpens"] == case["objectCount"]
        assert case["storageMateriallyReduced"]


@pytest.mark.parametrize(
    ("backend", "store_type", "pack_directory"),
    [
        ("packed", PackedTemporalQDObjectStore, "packs"),
        ("compressed", CompressedPackedTemporalQDObjectStore, "compressed-packs"),
    ],
)
def test_packed_cross_process_overlap_is_serialized_without_reopen_poisoning(
    tmp_path: Path,
    backend: str,
    store_type: type[TemporalQDObjectStore],
    pack_directory: str,
) -> None:
    """{X,Y} and {X,Z} must become two delta packs, never duplicate X indexes."""

    root = tmp_path / backend
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    common = b"shared immutable X" * 1024
    first = context.Process(
        target=_overlapping_packed_publish_worker,
        args=(str(root), backend, (common, b"unique immutable Y" * 1024), start, results),
    )
    second = context.Process(
        target=_overlapping_packed_publish_worker,
        args=(str(root), backend, (common, b"unique immutable Z" * 1024), start, results),
    )
    first.start()
    second.start()
    start.set()
    for process in (first, second):
        process.join(timeout=30)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
            pytest.fail(f"{backend} publication worker did not finish")
        assert process.exitcode == 0
    worker_results = [results.get(timeout=5), results.get(timeout=5)]
    assert all(result[0] == "ok" for result in worker_results), worker_results
    assert sum(result[1] for result in worker_results) == 3
    assert sum(result[2] for result in worker_results) == 1

    reopened = store_type(root)
    namespace = ObjectNamespace("multiprocess_overlap", 1, RAW_BYTES_CODEC)
    for payload in (common, b"unique immutable Y" * 1024, b"unique immutable Z" * 1024):
        prepared = prepare_bytes(namespace, payload)
        assert reopened.get_bytes(prepared.ref) == payload
    assert len(list((root / pack_directory).glob("*"))) == 2


@pytest.mark.parametrize(
    "store_type",
    [PackedTemporalQDObjectStore, CompressedPackedTemporalQDObjectStore],
)
def test_packed_writer_failures_cleanup_pack_temps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    store_type: type[TemporalQDObjectStore],
) -> None:
    store = store_type(tmp_path / store_type.__name__)
    namespace = ObjectNamespace("pack_temp_cleanup", 1, RAW_BYTES_CODEC)
    prepared = prepare_bytes(namespace, b"durability failure cleanup")
    real_fsync = object_store.os.fsync
    failed = False

    def fail_first_fsync(descriptor: int) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected packed fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(object_store.os, "fsync", fail_first_fsync)
    with pytest.raises(OSError, match="injected packed fsync failure"):
        store.put_prepared(prepared)
    monkeypatch.setattr(object_store.os, "fsync", real_fsync)
    assert not list(store._packs_root.glob("*.tmp"))
    assert not store.partial_temp_paths()


def test_compressed_pack_preflight_rejects_oversized_batch_before_temp_write(
    tmp_path: Path,
) -> None:
    namespace = ObjectNamespace("compressed_preflight", 1, RAW_BYTES_CODEC)
    prepared = prepare_bytes(namespace, b"x" * 1024)
    store = CompressedPackedTemporalQDObjectStore(
        tmp_path / "compressed-preflight",
        max_raw_block_bytes=1023,
    )

    with pytest.raises(ObjectStoreIntegrityError, match="raw-block safety limit"):
        store.put_many([prepared])
    assert not list((store.root / "compressed-packs").glob("*.tmp"))
    assert not list((store.root / "compressed-packs").glob("*.zpack"))
