from __future__ import annotations

import hashlib
import inspect
import json
import os
import shutil
from pathlib import Path

import pytest

from autoresearch import archive_relocation, generation_archive
from autoresearch.archive_relocation import (
    ArchiveRelocationError,
    preflight_archive_relocation,
    register_archive_relocation,
    resolve_archive_path,
    resolve_archive_relocation,
    resolve_archive_runs_root,
)
from autoresearch.generation_archive import (
    ARCHIVE_SCHEMA_NAME,
    ARCHIVE_SCHEMA_VERSION,
    build_inventory,
)


ARCHIVE_ID = "completed-level-c-v2-20260719"


@pytest.fixture(autouse=True)
def _isolated_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        archive_relocation,
        "DEFAULT_RELOCATION_RECEIPTS_ROOT",
        tmp_path / "archive-relocation-receipts",
    )


def _registry() -> Path:
    return Path(archive_relocation.DEFAULT_RELOCATION_RECEIPTS_ROOT)


def _tree_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _complete_archive(tmp_path: Path) -> tuple[Path, Path, Path]:
    original_root = tmp_path / "original-runs-archive"
    archive_directory = original_root / ARCHIVE_ID
    runs_root = archive_directory / "runs"
    (runs_root / "derived").mkdir(parents=True)
    (runs_root / "derived" / "attempt-catalog.sqlite").write_bytes(b"catalog")
    (runs_root / "run-001" / "empty").mkdir(parents=True)
    (runs_root / "run-001" / "attempts.jsonl").write_text(
        '{"attempt":1}\n', encoding="utf-8"
    )
    inventory = build_inventory(runs_root)
    manifest = {
        "schema_name": ARCHIVE_SCHEMA_NAME,
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "state": "complete",
        "archive_id": ARCHIVE_ID,
        "archive_root": str(original_root.resolve()),
        "destination_runs_root": str(runs_root.resolve()),
        "verified_archived_inventory": inventory,
    }
    (archive_directory / "archive-manifest.json").write_text(
        json.dumps(manifest, sort_keys=True), encoding="utf-8"
    )
    return original_root, archive_directory, runs_root


def _copy_archive(archive_directory: Path, tmp_path: Path) -> tuple[Path, Path]:
    destination_root = tmp_path / "unraid-runs-archive"
    destination_root.mkdir()
    destination_directory = destination_root / ARCHIVE_ID
    shutil.copytree(archive_directory, destination_directory, copy_function=shutil.copy2)
    return destination_root, destination_directory


def _preflight(
    *, original_root: Path, destination_root: Path
) -> tuple[Path, dict[str, object]]:
    relative_report = Path(f"{ARCHIVE_ID}.preflight.json")
    report = preflight_archive_relocation(
        archive_id=ARCHIVE_ID,
        original_archive_root=original_root,
        destination_archive_root=destination_root,
        report_path=relative_report,
    )
    return _registry() / relative_report, report


def _apply(
    *, original_root: Path, archive_directory: Path, destination_root: Path,
    report_path: Path,
) -> dict[str, object]:
    shutil.rmtree(archive_directory)
    return register_archive_relocation(
        archive_id=ARCHIVE_ID,
        original_archive_root=original_root,
        destination_archive_root=destination_root,
        preflight_report=report_path,
    )


def test_cross_filesystem_metadata_difference_applies_and_resolves_without_rescan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_root, archive_directory, original_runs = _complete_archive(tmp_path)
    destination_root, destination_directory = _copy_archive(archive_directory, tmp_path)
    for path in [destination_directory, *destination_directory.rglob("*")]:
        os.utime(path, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    before = _tree_hashes(destination_directory)
    report_path, preflight = _preflight(
        original_root=original_root, destination_root=destination_root
    )

    assert preflight["mode"] == "preflight"
    assert report_path.is_file()
    with pytest.raises(ArchiveRelocationError, match="delete the local copy first"):
        register_archive_relocation(
            archive_id=ARCHIVE_ID,
            original_archive_root=original_root,
            destination_archive_root=destination_root,
            preflight_report=report_path,
        )

    receipt = _apply(
        original_root=original_root,
        archive_directory=archive_directory,
        destination_root=destination_root,
        report_path=report_path,
    )
    monkeypatch.setattr(
        archive_relocation,
        "_build_content_inventory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("routine resolution must not rescan content")
        ),
    )
    monkeypatch.setattr(
        generation_archive,
        "build_inventory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("routine resolution must not use generation inventory")
        ),
    )

    resolved = resolve_archive_relocation(archive_id=ARCHIVE_ID)
    assert resolved is not None
    assert receipt["content_inventory"] == preflight["content_inventory"]
    assert resolve_archive_relocation(archive_id=ARCHIVE_ID) == resolved
    assert resolve_archive_runs_root(original_runs, archive_id=ARCHIVE_ID) == resolved.runs_root
    assert resolve_archive_path(
        original_runs / "derived" / "attempt-catalog.sqlite", archive_id=ARCHIVE_ID
    ) == resolved.runs_root / "derived" / "attempt-catalog.sqlite"
    assert _tree_hashes(destination_directory) == before


@pytest.mark.parametrize("mutation", ["tamper", "extra", "missing"])
def test_preflight_rejects_content_tamper_extra_and_missing(
    tmp_path: Path, mutation: str
) -> None:
    original_root, archive_directory, _original_runs = _complete_archive(tmp_path)
    destination_root, destination_directory = _copy_archive(archive_directory, tmp_path)
    target = destination_directory / "runs" / "run-001" / "attempts.jsonl"
    if mutation == "tamper":
        target.write_text('{"attempt":2}\n', encoding="utf-8")
    elif mutation == "extra":
        (destination_directory / "runs" / "extra.bin").write_bytes(b"extra")
    else:
        target.unlink()

    with pytest.raises(ArchiveRelocationError, match="content-only inventories differ"):
        _preflight(original_root=original_root, destination_root=destination_root)
    assert not (_registry() / f"{ARCHIVE_ID}.preflight.json").exists()


def test_apply_rejects_data_mutation_after_preflight(tmp_path: Path) -> None:
    original_root, archive_directory, _original_runs = _complete_archive(tmp_path)
    destination_root, destination_directory = _copy_archive(archive_directory, tmp_path)
    report_path, _ = _preflight(
        original_root=original_root, destination_root=destination_root
    )
    (destination_directory / "runs" / "run-001" / "attempts.jsonl").write_text(
        '{"attempt":2}\n', encoding="utf-8"
    )
    shutil.rmtree(archive_directory)

    with pytest.raises(ArchiveRelocationError, match="differs from preflight identity"):
        register_archive_relocation(
            archive_id=ARCHIVE_ID,
            original_archive_root=original_root,
            destination_archive_root=destination_root,
            preflight_report=report_path,
        )
    assert not (_registry() / f"{ARCHIVE_ID}.json").exists()


def test_walk_errors_are_fatal_for_same_subtree_on_both_copies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _original_root, archive_directory, _original_runs = _complete_archive(tmp_path)
    _destination_root, destination_directory = _copy_archive(archive_directory, tmp_path)
    walked: list[Path] = []

    def failing_walk(root: Path, *args: object, **kwargs: object):
        root_path = Path(root)
        walked.append(root_path)
        onerror = kwargs["onerror"]
        assert callable(onerror)
        onerror(PermissionError(13, "denied", str(root_path / "runs" / "run-001" / "empty")))
        yield  # pragma: no cover - the fatal callback must prevent traversal

    monkeypatch.setattr(archive_relocation.os, "walk", failing_walk)
    for copy_root in (archive_directory, destination_directory):
        with pytest.raises(ArchiveRelocationError, match="could not traverse"):
            archive_relocation._build_content_inventory(copy_root)
    assert walked == [archive_directory.resolve(), destination_directory.resolve()]


def test_registration_rejects_tampered_preflight_report(tmp_path: Path) -> None:
    original_root, archive_directory, _original_runs = _complete_archive(tmp_path)
    destination_root, _destination_directory = _copy_archive(archive_directory, tmp_path)
    report_path, _ = _preflight(
        original_root=original_root, destination_root=destination_root
    )
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["archive_id"] = "wrong-archive"
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    shutil.rmtree(archive_directory)

    with pytest.raises(ArchiveRelocationError, match="report identity drifted"):
        register_archive_relocation(
            archive_id=ARCHIVE_ID,
            original_archive_root=original_root,
            destination_archive_root=destination_root,
            preflight_report=report_path,
        )


def test_registration_rejects_destination_manifest_drift_after_preflight(
    tmp_path: Path,
) -> None:
    original_root, archive_directory, _original_runs = _complete_archive(tmp_path)
    destination_root, destination_directory = _copy_archive(archive_directory, tmp_path)
    report_path, _ = _preflight(
        original_root=original_root, destination_root=destination_root
    )
    manifest = destination_directory / "archive-manifest.json"
    manifest.write_bytes(manifest.read_bytes() + b" ")
    shutil.rmtree(archive_directory)

    with pytest.raises(ArchiveRelocationError, match="manifest hash drifted"):
        register_archive_relocation(
            archive_id=ARCHIVE_ID,
            original_archive_root=original_root,
            destination_archive_root=destination_root,
            preflight_report=report_path,
        )


def test_relocation_rejects_wrong_root_and_path_escape(tmp_path: Path) -> None:
    original_root, archive_directory, original_runs = _complete_archive(tmp_path)
    destination_root, _destination_directory = _copy_archive(archive_directory, tmp_path)
    with pytest.raises(ArchiveRelocationError, match="wrong original archive root"):
        preflight_archive_relocation(
            archive_id=ARCHIVE_ID,
            original_archive_root=original_root.parent,
            destination_archive_root=destination_root,
            report_path="wrong.preflight.json",
        )
    report_path, _ = _preflight(
        original_root=original_root, destination_root=destination_root
    )
    _apply(
        original_root=original_root,
        archive_directory=archive_directory,
        destination_root=destination_root,
        report_path=report_path,
    )

    with pytest.raises(ArchiveRelocationError, match="conflicts"):
        resolve_archive_runs_root(original_root / "wrong" / "runs", archive_id=ARCHIVE_ID)
    with pytest.raises(ArchiveRelocationError, match="traversal"):
        resolve_archive_path(original_runs / ".." / "outside", archive_id=ARCHIVE_ID)


@pytest.mark.parametrize(
    "report_path",
    [Path("../escaped-preflight.json"), Path("absolute-placeholder")],
)
def test_preflight_report_must_not_escape_default_registry(
    tmp_path: Path, report_path: Path
) -> None:
    original_root, archive_directory, _ = _complete_archive(tmp_path)
    destination_root, _ = _copy_archive(archive_directory, tmp_path)
    supplied = (
        tmp_path / "outside-registry.json"
        if report_path.name == "absolute-placeholder"
        else report_path
    )

    with pytest.raises(ArchiveRelocationError, match="within the relocation registry"):
        preflight_archive_relocation(
            archive_id=ARCHIVE_ID,
            original_archive_root=original_root,
            destination_archive_root=destination_root,
            report_path=supplied,
        )


@pytest.mark.parametrize("component", ["source", "destination", "registry", "report"])
def test_relocation_rejects_symlink_ancestor_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, component: str
) -> None:
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    original_root, archive_directory, _ = _complete_archive(real_parent)
    destination_root, _ = _copy_archive(archive_directory, real_parent)
    link = tmp_path / "ancestor-link"
    try:
        link.symlink_to(real_parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    source_arg: Path = original_root
    destination_arg: Path = destination_root
    report_arg: Path | str = f"{ARCHIVE_ID}.preflight.json"
    if component == "source":
        source_arg = link / original_root.name
    elif component == "destination":
        destination_arg = link / destination_root.name
    elif component == "registry":
        monkeypatch.setattr(
            archive_relocation,
            "DEFAULT_RELOCATION_RECEIPTS_ROOT",
            link / "archive-relocation-receipts",
        )
    else:
        report_link = _registry() / "linked"
        _registry().mkdir(parents=True)
        report_target = tmp_path / "report-target"
        report_target.mkdir()
        report_link.symlink_to(report_target, target_is_directory=True)
        report_arg = report_link / f"{ARCHIVE_ID}.json"

    with pytest.raises(ArchiveRelocationError, match="symlink or reparse-point ancestor"):
        preflight_archive_relocation(
            archive_id=ARCHIVE_ID,
            original_archive_root=source_arg,
            destination_archive_root=destination_arg,
            report_path=report_arg,
        )


def test_custom_registry_api_is_removed() -> None:
    for function in (
        register_archive_relocation,
        resolve_archive_relocation,
        resolve_archive_path,
        resolve_archive_runs_root,
    ):
        assert "registry_root" not in inspect.signature(function).parameters


def test_cli_requires_explicit_preflight_or_apply() -> None:
    from autoresearch.__main__ import build_parser

    common = [
        "register-archive-relocation",
        "--archive-id", ARCHIVE_ID,
        "--archive-root", "C:/original",
        "--destination-archive-root", "Y:/destination",
        "--preflight-report", f"{ARCHIVE_ID}.preflight.json",
    ]
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(common)
    with pytest.raises(SystemExit):
        parser.parse_args([*common, "--preflight", "--apply"])
    with pytest.raises(SystemExit):
        parser.parse_args([*common, "--preflight", "--receipt-root", "C:/custom"])


@pytest.mark.parametrize("mode", ["preflight", "apply"])
def test_cli_dispatches_explicit_relocation_mode(
    monkeypatch: pytest.MonkeyPatch, mode: str, capsys: pytest.CaptureFixture[str]
) -> None:
    from autoresearch import __main__ as cli

    calls: list[str] = []

    def fake_preflight(**_kwargs: object) -> dict[str, object]:
        calls.append("preflight")
        return {"mode": "preflight"}

    def fake_apply(**_kwargs: object) -> dict[str, object]:
        calls.append("apply")
        return {"receipt_path": "receipt.json"}

    monkeypatch.setattr(archive_relocation, "preflight_archive_relocation", fake_preflight)
    monkeypatch.setattr(archive_relocation, "register_archive_relocation", fake_apply)
    result = cli.main([
        "register-archive-relocation",
        "--archive-id", ARCHIVE_ID,
        "--archive-root", "C:/original",
        "--destination-archive-root", "Y:/destination",
        "--preflight-report", f"{ARCHIVE_ID}.preflight.json",
        f"--{mode}",
        "--json",
    ])

    assert result == 0
    assert calls == [mode]
    assert capsys.readouterr().out
