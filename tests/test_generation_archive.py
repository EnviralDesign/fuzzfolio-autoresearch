from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from autoresearch import generation_archive
from autoresearch.generation_archive import (
    ARCHIVE_MANIFEST_NAME,
    GENERATION_MANIFEST_NAME,
    GenerationArchiveError,
    GenerationArchiveService,
    build_inventory,
    utc_now,
)


def _runs_root(tmp_path: Path) -> Path:
    root = tmp_path / "runs"
    (root / "derived").mkdir(parents=True)
    (root / "run-001" / "artifacts").mkdir(parents=True)
    (root / "run-001" / "attempts.jsonl").write_text('{"attempt": 1}\n', encoding="utf-8")
    (root / "run-001" / "artifacts" / "result.json").write_text("result", encoding="utf-8")
    (root / "derived" / "attempt-catalog.sqlite").write_bytes(b"catalog")
    (root / "derived" / "finalized-corpus.json").write_text("{}", encoding="utf-8")
    (root / "derived" / "corpus-manifest.json").write_text("{}", encoding="utf-8")
    (root / GENERATION_MANIFEST_NAME).write_text('{"old_generation": true}\n', encoding="utf-8")
    return root


def _formal_level_c_root(tmp_path: Path) -> Path:
    root = tmp_path / "runs"
    control = root / "derived" / "level-c" / "control"
    control.mkdir(parents=True)
    (root / GENERATION_MANIFEST_NAME).write_text(
        json.dumps(
            {
                "schema_name": "autoresearch.generation.manifest",
                "schema_version": 1,
                "new_generation_id": "level-c-v1",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    for name in (
        "archive-linkage.json",
        "bootstrap-result.json",
        "protocol.json",
        "protocol-authority.json",
        "execution-plan-A.json",
        "execution-plan-B.json",
        "execution-plan-C.json",
        "execution-plan-D.json",
    ):
        (control / name).write_text(json.dumps({"artifact": name}, sort_keys=True), encoding="utf-8")
    return root


def _service(tmp_path: Path) -> GenerationArchiveService:
    return GenerationArchiveService(_runs_root(tmp_path))


def _apply_provenance(
    service: GenerationArchiveService,
    tmp_path: Path,
    *,
    archive_id: str = "archive-001",
    generation_id: str = "generation-002",
    provenance: dict[str, object] | None = None,
    critical_artifacts: object = None,
) -> dict[str, object]:
    base = dict(provenance or {"actor": "test"})
    preview = service.dry_run(
        archive_id, generation_id, provenance=base, critical_artifacts=critical_artifacts
    )
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
                "new_generation_id": generation_id,
                "inventory_identity": preview["inventory_identity"],
                "issuer_id": "archive-test-supervisor",
                "nonce": hashlib.sha256(f"{archive_id}:{generation_id}".encode()).hexdigest(),
                "issued_at": utc_now(),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    base["reviewed_inventory_identity"] = preview["inventory_identity"]
    base["cutover_quiescence"] = {
        "marker_path": str(marker),
        "marker_sha256": hashlib.sha256(marker.read_bytes()).hexdigest(),
    }
    return base


def _cutover(service: GenerationArchiveService, tmp_path: Path, **kwargs: object) -> dict[str, object]:
    critical = kwargs.pop("critical_artifacts", None)
    provenance = _apply_provenance(service, tmp_path, critical_artifacts=critical, **kwargs)
    return service.cutover("archive-001", "generation-002", provenance=provenance, critical_artifacts=critical)


def _hold_lock(lock_path: str, ready, release) -> None:  # type: ignore[no-untyped-def]
    with generation_archive._exclusive_lock(Path(lock_path)):
        ready.set()
        release.wait(15)


def test_dry_run_is_non_mutating_and_exposes_deterministic_identity(tmp_path: Path) -> None:
    service = _service(tmp_path)
    before = sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*"))
    first = service.dry_run("archive-001", "generation-002", provenance={"actor": "test"})
    second = service.dry_run("archive-001", "generation-002", provenance={"actor": "test"})

    assert first["inventory_identity"] == second["inventory_identity"]
    assert first["inventory"]["metadata_tree_sha256"]
    assert "no full supervisor writer fence" in first["writer_fence_limitations"]
    assert sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*")) == before
    assert not (tmp_path / "runs_archive").exists()


def test_apply_requires_pinned_preview_and_verified_external_quiescence(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with pytest.raises(GenerationArchiveError, match="reviewed_inventory_identity"):
        service.cutover("archive-001", "generation-002", provenance={"actor": "test"})

    provenance = _apply_provenance(service, tmp_path)
    provenance["reviewed_inventory_identity"] = "0" * 64
    with pytest.raises(GenerationArchiveError, match="reviewed_inventory_identity"):
        service.cutover("archive-001", "generation-002", provenance=provenance)

    provenance = _apply_provenance(service, tmp_path)
    marker = Path(str(provenance["cutover_quiescence"]["marker_path"]))  # type: ignore[index]
    marker.write_text("{}", encoding="utf-8")
    with pytest.raises(GenerationArchiveError, match="quiescence marker"):
        service.cutover("archive-001", "generation-002", provenance=provenance)


def test_apply_rejects_write_after_reviewed_preview(tmp_path: Path) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    (service.runs_root / "run-001" / "attempts.jsonl").write_text("changed\n", encoding="utf-8")

    with pytest.raises(GenerationArchiveError, match="pinned reviewed preview"):
        service.cutover("archive-001", "generation-002", provenance=provenance)


def test_apply_rejects_stale_quiescence_marker(tmp_path: Path) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    marker = Path(str(provenance["cutover_quiescence"]["marker_path"]))  # type: ignore[index]
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["issued_at"] = "2020-01-01T00:00:00Z"
    marker.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    provenance["cutover_quiescence"]["marker_sha256"] = hashlib.sha256(marker.read_bytes()).hexdigest()  # type: ignore[index]

    with pytest.raises(GenerationArchiveError, match="freshness window"):
        service.cutover("archive-001", "generation-002", provenance=provenance)


def test_apply_accepts_fresh_admission_marker_after_long_inventory_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeDatetime:
        current = datetime(2026, 1, 1, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            if tz is None:
                return cls.current.replace(tzinfo=None)
            return cls.current.astimezone(tz)

        @classmethod
        def fromisoformat(cls, value: str) -> datetime:
            return datetime.fromisoformat(value)

    service = _service(tmp_path)
    monkeypatch.setattr(generation_archive, "datetime", FakeDatetime)
    provenance = _apply_provenance(service, tmp_path)
    advanced = False

    def advance_after_admission(path: Path, bytes_read: int) -> None:
        nonlocal advanced
        if bytes_read and not advanced and generation_archive._is_within(path, service.runs_root):
            advanced = True
            FakeDatetime.current = FakeDatetime.current + timedelta(minutes=10)

    monkeypatch.setattr(generation_archive, "_inventory_read_hook", advance_after_admission)

    result = service.cutover("archive-001", "generation-002", provenance=provenance)

    assert advanced is True
    assert result["manifest"]["state"] == "complete"


def test_apply_rejects_stale_quiescence_at_operation_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeDatetime:
        current = datetime(2026, 1, 1, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            if tz is None:
                return cls.current.replace(tzinfo=None)
            return cls.current.astimezone(tz)

        @classmethod
        def fromisoformat(cls, value: str) -> datetime:
            return datetime.fromisoformat(value)

    service = _service(tmp_path)
    monkeypatch.setattr(generation_archive, "datetime", FakeDatetime)
    provenance = _apply_provenance(service, tmp_path)
    FakeDatetime.current = FakeDatetime.current + timedelta(minutes=6)

    with pytest.raises(GenerationArchiveError, match="freshness window"):
        service.cutover("archive-001", "generation-002", provenance=provenance)
    assert not (tmp_path / "runs_archive" / "archive-001").exists()


def test_inventory_identity_detects_same_size_same_mtime_substitution(tmp_path: Path) -> None:
    root = _runs_root(tmp_path)
    target = root / "run-001" / "attempts.jsonl"
    before_stat = target.stat()
    before = build_inventory(root)

    target.write_text('{"attempt": 2}\n', encoding="utf-8")
    assert target.stat().st_size == before_stat.st_size
    os.utime(target, ns=(before_stat.st_atime_ns, before_stat.st_mtime_ns))
    after = build_inventory(root)

    assert after["inventory_identity"] != before["inventory_identity"]
    assert after["content_tree_sha256"] != before["content_tree_sha256"]


def test_inventory_rejects_adversarial_concurrent_content_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _runs_root(tmp_path)
    target = root / "run-001" / "artifacts" / "result.json"
    before_stat = target.stat()
    changed = False

    def mutate_during_first_stream(path: Path, bytes_read: int) -> None:
        nonlocal changed
        if path == target and bytes_read and not changed:
            changed = True
            target.write_text("change", encoding="utf-8")
            os.utime(target, ns=(before_stat.st_atime_ns, before_stat.st_mtime_ns))

    monkeypatch.setattr(generation_archive, "_inventory_read_hook", mutate_during_first_stream)
    with pytest.raises(GenerationArchiveError, match="changed while inventorying"):
        build_inventory(root)


def test_successful_cutover_archives_entire_root_and_persists_contract(tmp_path: Path) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    result = service.cutover("archive-001", "generation-002", provenance=provenance)

    archived = tmp_path / "runs_archive" / "archive-001" / "runs"
    active = tmp_path / "runs"
    assert result["manifest"]["state"] == "complete"
    assert (archived / "run-001" / "attempts.jsonl").exists()
    assert (active / "derived").is_dir()
    assert result["manifest"]["quiescence"]["marker"]["writer_scope"] == "all-writers-stopped"
    assert Path(result["manifest"]["quiescence"]["consumption"]["path"]).is_file()
    assert json.loads((active / GENERATION_MANIFEST_NAME).read_text(encoding="utf-8"))["new_generation_id"] == "generation-002"

    Path(str(provenance["cutover_quiescence"]["marker_path"])).unlink()  # type: ignore[index]
    repeated = GenerationArchiveService(active).cutover(
        "archive-001", "generation-002", provenance=provenance
    )
    assert repeated["already_complete"] is True


@pytest.mark.parametrize(
    "seam",
    ["archive_dir_created", "archive_manifest_written", "runs_renamed", "active_root_created", "active_derived_created", "generation_manifest_written", "archive_manifest_completed"],
)
def test_cutover_recovers_after_every_durable_seam(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seam: str) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)

    def crash(name: str) -> None:
        if name == seam:
            raise RuntimeError(f"injected crash at {name}")

    monkeypatch.setattr(generation_archive, "_checkpoint", crash)
    with pytest.raises(RuntimeError, match=seam):
        service.cutover("archive-001", "generation-002", provenance=provenance)
    monkeypatch.setattr(generation_archive, "_checkpoint", lambda name: None)

    resumed = GenerationArchiveService(tmp_path / "runs").cutover(
        "archive-001", "generation-002", provenance=provenance
    )
    assert resumed["manifest"]["state"] == "complete"
    assert (tmp_path / "runs_archive" / "archive-001" / "runs" / "run-001").is_dir()
    assert (tmp_path / "runs" / GENERATION_MANIFEST_NAME).is_file()


def test_truncated_archive_manifest_is_repaired_from_durable_intent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    monkeypatch.setattr(
        generation_archive, "_checkpoint", lambda name: (_ for _ in ()).throw(RuntimeError()) if name == "runs_renamed" else None
    )
    with pytest.raises(RuntimeError):
        service.cutover("archive-001", "generation-002", provenance=provenance)
    manifest = tmp_path / "runs_archive" / "archive-001" / ARCHIVE_MANIFEST_NAME
    manifest.write_text("{", encoding="utf-8")
    monkeypatch.setattr(generation_archive, "_checkpoint", lambda name: None)

    resumed = GenerationArchiveService(tmp_path / "runs").cutover("archive-001", "generation-002", provenance=provenance)
    assert resumed["manifest"]["state"] == "complete"
    assert json.loads(manifest.read_text(encoding="utf-8"))["state"] == "complete"


def test_truncated_generation_manifest_is_repaired_from_archive_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    monkeypatch.setattr(
        generation_archive, "_checkpoint", lambda name: (_ for _ in ()).throw(RuntimeError()) if name == "generation_manifest_written" else None
    )
    with pytest.raises(RuntimeError):
        service.cutover("archive-001", "generation-002", provenance=provenance)
    (tmp_path / "runs" / GENERATION_MANIFEST_NAME).write_text("{", encoding="utf-8")
    monkeypatch.setattr(generation_archive, "_checkpoint", lambda name: None)

    assert GenerationArchiveService(tmp_path / "runs").cutover("archive-001", "generation-002", provenance=provenance)["manifest"]["state"] == "complete"


def test_required_critical_artifacts_never_skip_missing_paths(tmp_path: Path) -> None:
    root = _runs_root(tmp_path)
    (root / "derived" / "attempt-catalog.sqlite").unlink()
    with pytest.raises(GenerationArchiveError, match="required critical artifact is missing"):
        build_inventory(root)
    with pytest.raises(GenerationArchiveError, match="required critical artifact is missing"):
        build_inventory(root, critical_artifacts=["derived/not-there.json"])


def test_formal_level_c_generation_without_attempt_catalog_can_archive(tmp_path: Path) -> None:
    root = _formal_level_c_root(tmp_path)
    service = GenerationArchiveService(root)

    preview = service.dry_run("level-c-v1-old", "level-c-v2", provenance={"actor": "test"})

    assert preview["inventory"]["critical_artifact_source"] == "formal_level_c_defaults"
    assert "derived/attempt-catalog.sqlite" not in preview["inventory"]["critical_artifacts"]
    assert "derived/level-c/control/execution-plan-A.json" in preview["inventory"]["critical_artifacts"]

    provenance = _apply_provenance(
        service,
        tmp_path,
        archive_id="level-c-v1-old",
        generation_id="level-c-v2",
        provenance={"actor": "test"},
    )
    result = service.cutover("level-c-v1-old", "level-c-v2", provenance=provenance)

    archived = tmp_path / "runs_archive" / "level-c-v1-old" / "runs"
    assert result["manifest"]["state"] == "complete"
    assert (archived / "derived" / "level-c" / "control" / "execution-plan-D.json").is_file()
    assert not (archived / "derived" / "attempt-catalog.sqlite").exists()
    assert result["manifest"]["inventory"]["critical_artifact_source"] == "formal_level_c_defaults"


def test_corpus_generation_archive_still_requires_attempt_catalog(tmp_path: Path) -> None:
    root = _runs_root(tmp_path)
    (root / "derived" / "attempt-catalog.sqlite").unlink()
    service = GenerationArchiveService(root)

    with pytest.raises(GenerationArchiveError, match="derived/attempt-catalog.sqlite"):
        service.dry_run("archive-001", "generation-002", provenance={"actor": "test"})


def test_caller_critical_additions_cannot_disable_mandatory_artifacts(tmp_path: Path) -> None:
    root = _runs_root(tmp_path)
    extra = root / "run-001" / "operator-proof.json"
    extra.write_text("proof", encoding="utf-8")
    (root / "derived" / "attempt-catalog.sqlite").unlink()

    with pytest.raises(GenerationArchiveError, match="derived/attempt-catalog.sqlite"):
        build_inventory(root, critical_artifacts=["run-001/operator-proof.json"])


def test_recovery_rejects_unexpected_new_active_files_before_generation_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    monkeypatch.setattr(
        generation_archive, "_checkpoint", lambda name: (_ for _ in ()).throw(RuntimeError()) if name == "runs_renamed" else None
    )
    with pytest.raises(RuntimeError):
        service.cutover("archive-001", "generation-002", provenance=provenance)
    active = tmp_path / "runs"
    (active / "derived").mkdir(parents=True)
    (active / "derived" / "unexpected.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(generation_archive, "_checkpoint", lambda name: None)

    with pytest.raises(GenerationArchiveError, match="derived directory contains unexpected"):
        GenerationArchiveService(active).cutover("archive-001", "generation-002", provenance=provenance)


def test_complete_archive_requires_exact_new_active_state(tmp_path: Path) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    service.cutover("archive-001", "generation-002", provenance=provenance)
    (tmp_path / "runs" / "derived" / "unexpected.json").write_text("{}", encoding="utf-8")

    with pytest.raises(GenerationArchiveError, match="derived directory contains unexpected"):
        GenerationArchiveService(tmp_path / "runs").cutover("archive-001", "generation-002", provenance=provenance)


def test_exclusive_cutover_lock_rejects_concurrent_apply(tmp_path: Path) -> None:
    service = _service(tmp_path)
    provenance = _apply_provenance(service, tmp_path)
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    process = context.Process(target=_hold_lock, args=(str(tmp_path / ".generation-cutover.lock"), ready, release))
    process.start()
    assert ready.wait(10)
    try:
        with pytest.raises(GenerationArchiveError, match="exclusive lock"):
            service.cutover("archive-001", "generation-002", provenance=provenance)
    finally:
        release.set()
        process.join(10)
    assert process.exitcode == 0


def test_restore_plan_is_dry_run_only(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _cutover(service, tmp_path)
    plan = service.restore_plan("archive-001", destination_runs_root=tmp_path / "restore-here")
    assert plan["dry_run"] is True
    assert not (tmp_path / "restore-here").exists()
