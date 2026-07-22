from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch import __main__ as ar_main
import autoresearch.phase2_atlas_capsule as capsule_module
from autoresearch.durable_execution import DurableExecutionJournal
from autoresearch.evidence_plan import canonical_sha256
from autoresearch.phase2_atlas_capsule import CapsuleError, cleanup_preview, create_capsule_plan, verify_capsule


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _completed_journal(root: Path, *, outcome: str | None = None, malformed_receipt: bool = False) -> dict[str, object]:
    artifact_receipt: dict[str, object] = {
        "root": str(root),
        "files": {"indicator-atlas/indicator-atlas-summary.json": "sha256:" + "a" * 64},
    }
    artifact_receipt["receipt_sha256"] = canonical_sha256(artifact_receipt)
    payload: dict[str, object] = {"stage": "01-indicator-atlas", "artifact_receipt": artifact_receipt}
    if outcome is not None:
        payload["outcome"] = outcome
    terminal_receipt: dict[str, object] = {"payload": payload, "receipt_sha256": canonical_sha256(payload)}
    if malformed_receipt:
        terminal_receipt["receipt_sha256"] = "sha256:" + "b" * 64
    task = {"status": "terminal", "terminal_receipt": terminal_receipt}
    journal: dict[str, object] = {
        "schema_version": "autoresearch-durable-execution-v1",
        "execution_id": "phase2-fixture",
        "lineage": {},
        "tasks": {"stage": task},
    }
    journal["journal_identity"] = canonical_sha256(journal)
    return journal


def _fixture(tmp_path: Path) -> tuple[Path, list[Path], Path, Path]:
    repo = tmp_path / "repo"
    atlas_parent = repo / "runs/derived/atlas-runs"
    roots: list[Path] = []
    for cutoff in "ABCD":
        root = atlas_parent / f"atlas-{cutoff.lower()}"
        root.mkdir(parents=True)
        _write_json(root / "atlas-lab-run.json", {"status": "completed"})
        _write_json(root / "atlas-lab-summary.json", {"status": "completed", "historical_lineage": {"cutoff_key": cutoff}})
        _write_json(root / "execution-journal.json", _completed_journal(root))
        (root / "atlas-lab-events.jsonl").write_text("{}\n", encoding="utf-8")
        for stage in (
            "indicator-atlas", "signal-atlas", "forward-response-atlas", "anchor-pair-atlas",
            "anchor-pair-timing-atlas", "discovery-pair-atlas", "discovery-cluster-atlas",
            "discovery-recipe-validation-atlas", "discovery-recipe-scrutiny-atlas", "recipe-priors",
        ):
            _write_json(root / stage / f"{stage}-summary.json", {"cutoff": cutoff, "stage": stage})
        (root / "raw-worker-task-tree").mkdir()
        (root / "raw-worker-response.bin").write_bytes(b"not retained")
        roots.append(root)
    control = repo / "runs/derived/level-c/control"
    _write_json(control / "protocol.json", {"authority": True})
    _write_json(control / "protocol-authority.json", {"authority": True})
    for cutoff in "ABCD":
        _write_json(control / f"execution-plan-{cutoff}.json", {"cutoff": cutoff})
    forensics = repo / "runs/derived/phase2-atlas-forensics"
    _write_json(forensics / "cross-cutoff-metrics.json", {"ok": True})
    (forensics / "first" / "PHASE-2-ATLAS-FORENSIC-COMPARISON-REPORT.md").parent.mkdir(parents=True)
    (forensics / "first" / "PHASE-2-ATLAS-FORENSIC-COMPARISON-REPORT.md").write_text("report", encoding="utf-8")
    (forensics / "second" / "SECOND-ANALYSIS-ATLAS-FORENSIC-REVIEW-20260721.md").parent.mkdir(parents=True)
    (forensics / "second" / "SECOND-ANALYSIS-ATLAS-FORENSIC-REVIEW-20260721.md").write_text("review", encoding="utf-8")
    plan = repo / "z_docs/PHASE3_RESEARCH_AND_OPERATIONS_MASTER_PLAN_2026-07-21.md"
    plan.parent.mkdir(parents=True)
    plan.write_text("master plan", encoding="utf-8")
    capsule_root = repo / "runs/derived/phase2-atlas-capsules"
    capsule_root.mkdir()
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    return repo, roots, capsule_root, archive_root


def _args(repo: Path, roots: list[Path], capsule_root: Path, *mode_args: str) -> list[str]:
    return [
        "phase2-atlas-capsule", "--repo-root", str(repo), "--capsule-root", str(capsule_root),
        *(item for root in roots for item in ("--atlas-root", str(root))), *mode_args, "--json",
    ]


def test_cli_is_public_and_registered_as_a_direct_script() -> None:
    parser = ar_main.build_parser()
    assert "phase2-atlas-capsule" in parser._subparsers._group_actions[0].choices  # type: ignore[union-attr]
    pyproject = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    assert 'phase2-atlas-capsule = "autoresearch.__main__:main"' in pyproject


def test_dry_run_is_deterministic_and_does_not_write(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    before = sorted(path.relative_to(repo).as_posix() for path in repo.rglob("*"))

    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "dry-run", "--destination", str(capsule_root / "phase2"))) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["dry_run"] is True
    assert payload["ready"] is True
    assert {item["cutoff"] for item in payload["manifest"]["cutoffs"]} == set("ABCD")
    assert sorted(path.relative_to(repo).as_posix() for path in repo.rglob("*")) == before


def test_accepts_v2_jsonl_execution_journal(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    root = roots[0]
    journal_path = root / "execution-journal.json"
    journal_path.unlink()
    journal = DurableExecutionJournal(
        journal_path,
        execution_id="phase2-fixture-v2",
        lineage={"cutoff_key": "A"},
    )
    artifact_receipt: dict[str, object] = {
        "root": str(root),
        "files": {"indicator-atlas/indicator-atlas-summary.json": "sha256:" + "a" * 64},
    }
    artifact_receipt["receipt_sha256"] = canonical_sha256(artifact_receipt)
    terminal_payload = {"stage": "01-indicator-atlas", "artifact_receipt": artifact_receipt}
    journal.register("stage", {"stage": "01-indicator-atlas"})
    journal.complete("stage", terminal_payload)

    assert ar_main.main(
        _args(repo, roots, capsule_root, "--mode", "dry-run", "--destination", str(capsule_root / "phase2-v2"))
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ready"] is True


def test_build_copy_verify_and_cleanup_preview_never_delete(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo, roots, capsule_root, archive_root = _fixture(tmp_path)
    local = capsule_root / "phase2"
    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "build", "--destination", str(local))) == 0
    assert json.loads(capsys.readouterr().out)["verification"]["verified"] is True
    assert verify_capsule(local)["verified"] is True

    archived = archive_root / "phase2"
    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "copy", "--capsule", str(local), "--destination", str(archived), "--archive-root", str(archive_root))) == 0
    capsys.readouterr()
    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "cleanup-preview", "--capsule", str(local), "--archive-root", str(archive_root), "--archive-capsule", str(archived))) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["delete_operation_available"] is False
    assert any(item["raw_path"].endswith("raw-worker-task-tree") for item in payload["candidate_removals"])
    assert all(root.exists() for root in roots)


def test_fails_closed_for_incomplete_journal_and_paths_outside_configured_root(tmp_path: Path) -> None:
    repo, roots, _, _ = _fixture(tmp_path)
    journal_path = roots[0] / "execution-journal.json"
    pending_journal = _completed_journal(roots[0])
    pending_journal["tasks"] = {"stage": {"status": "pending"}}
    pending_journal["journal_identity"] = canonical_sha256({key: value for key, value in pending_journal.items() if key != "journal_identity"})
    _write_json(journal_path, pending_journal)
    with pytest.raises(CapsuleError, match="active or incomplete"):
        create_capsule_plan(repo_root=repo, atlas_roots=roots)

    _write_json(journal_path, _completed_journal(roots[0]))
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(CapsuleError, match="outside its configured root"):
        create_capsule_plan(repo_root=repo, atlas_roots=[*roots[:3], outside])


def test_fails_closed_when_a_required_authority_file_is_a_reparse_point(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, roots, _, _ = _fixture(tmp_path)
    original = capsule_module._is_reparse_point
    monkeypatch.setattr(
        capsule_module,
        "_is_reparse_point",
        lambda path: path.name == "atlas-lab-run.json" or original(path),
    )
    with pytest.raises(CapsuleError, match="symlink or reparse point"):
        create_capsule_plan(repo_root=repo, atlas_roots=roots)


def test_cleanup_preview_rejects_a_capsule_for_different_raw_roots(tmp_path: Path) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    local = capsule_root / "phase2"
    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "build", "--destination", str(local))) == 0
    changed = repo / "runs/derived/atlas-runs/atlas-other-a"
    changed.mkdir()
    _write_json(changed / "atlas-lab-run.json", {"status": "completed"})
    _write_json(changed / "atlas-lab-summary.json", {"status": "completed", "historical_lineage": {"cutoff_key": "A"}})
    _write_json(changed / "execution-journal.json", _completed_journal(changed))
    for stage in (
        "indicator-atlas", "signal-atlas", "forward-response-atlas", "anchor-pair-atlas",
        "anchor-pair-timing-atlas", "discovery-pair-atlas", "discovery-cluster-atlas",
        "discovery-recipe-validation-atlas", "discovery-recipe-scrutiny-atlas", "recipe-priors",
    ):
        _write_json(changed / stage / "summary.json", {})
    with pytest.raises(CapsuleError, match="does not match"):
        cleanup_preview(repo_root=repo, atlas_roots=[changed, *roots[1:]], capsule=local, capsule_root=capsule_root)


def test_cleanup_preview_rehashes_retained_sources_before_listing_raw_roots(tmp_path: Path) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    local = capsule_root / "phase2"
    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "build", "--destination", str(local))) == 0
    (roots[0] / "indicator-atlas" / "indicator-atlas-summary.json").write_text("changed", encoding="utf-8")

    with pytest.raises(CapsuleError, match="retained source file verification failed"):
        cleanup_preview(repo_root=repo, atlas_roots=roots, capsule=local, capsule_root=capsule_root)


def test_cleanup_preview_rejects_a_linked_retained_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    local = capsule_root / "phase2"
    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "build", "--destination", str(local))) == 0
    source = roots[0] / "indicator-atlas" / "indicator-atlas-summary.json"
    original = capsule_module._is_reparse_point
    monkeypatch.setattr(capsule_module, "_is_reparse_point", lambda path: path == source or original(path))

    with pytest.raises(CapsuleError, match="symlink or reparse point"):
        cleanup_preview(repo_root=repo, atlas_roots=roots, capsule=local, capsule_root=capsule_root)


@pytest.mark.parametrize("outcome,malformed_receipt", [("failed", False), (None, True)])
def test_rejects_failed_or_malformed_terminal_receipts(
    tmp_path: Path, outcome: str | None, malformed_receipt: bool
) -> None:
    repo, roots, _, _ = _fixture(tmp_path)
    _write_json(roots[0] / "execution-journal.json", _completed_journal(roots[0], outcome=outcome, malformed_receipt=malformed_receipt))

    with pytest.raises(CapsuleError, match="terminal receipt"):
        create_capsule_plan(repo_root=repo, atlas_roots=roots)


def test_verify_rejects_empty_extra_directories(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    local = capsule_root / "phase2"
    assert ar_main.main(_args(repo, roots, capsule_root, "--mode", "build", "--destination", str(local))) == 0
    capsys.readouterr()
    (local / "unexpected-empty-directory").mkdir()

    with pytest.raises(CapsuleError, match="unexpected_directories"):
        verify_capsule(local)


def test_rejects_reparse_destination_ancestor_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    nested = capsule_root / "nested"
    nested.mkdir()
    original = capsule_module._is_reparse_point
    monkeypatch.setattr(capsule_module, "_is_reparse_point", lambda path: path == nested or original(path))

    with pytest.raises(CapsuleError, match="symlink or reparse point"):
        capsule_module.build_capsule(
            repo_root=repo,
            atlas_roots=roots,
            destination=nested / "phase2",
            capsule_root=capsule_root,
        )
    assert not (nested / "phase2").exists()


@pytest.mark.parametrize("configured_root", ["source", "destination"])
def test_rejects_reparse_ancestors_above_configured_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, configured_root: str
) -> None:
    repo, roots, capsule_root, _ = _fixture(tmp_path)
    protected_ancestor = repo.parent if configured_root == "source" else capsule_root.parent
    original = capsule_module._is_reparse_point
    monkeypatch.setattr(
        capsule_module,
        "_is_reparse_point",
        lambda path: path == protected_ancestor or original(path),
    )

    with pytest.raises(CapsuleError, match="symlink or reparse point"):
        if configured_root == "source":
            create_capsule_plan(repo_root=repo, atlas_roots=roots)
        else:
            capsule_module.build_capsule(
                repo_root=repo,
                atlas_roots=roots,
                destination=capsule_root / "phase2",
                capsule_root=capsule_root,
            )


def test_mapped_drive_candidate_uses_the_same_resolved_namespace_as_unc_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    unc_root = tmp_path / "unc-share" / "runs_archive"
    unc_root.mkdir(parents=True)
    mapped_root = Path("Y:/ED-BEAST/C/repos/fuzzfolio-autoresearch/runs_archive")
    mapped_candidate = mapped_root / "phase2-atlas-authority-capsule-20260721"
    outside_candidate = mapped_root.parent / "outside"

    monkeypatch.setattr(capsule_module, "_absolute_without_resolving", lambda path: Path(path))
    monkeypatch.setattr(capsule_module, "_reject_reparse_ancestors", lambda *_args, **_kwargs: None)

    def normalize(path: Path) -> Path:
        if path == mapped_candidate:
            return unc_root / mapped_candidate.name
        if path == outside_candidate:
            return unc_root.parent / outside_candidate.name
        return path.resolve(strict=False)

    monkeypatch.setattr(capsule_module, "_resolve_for_containment", normalize)

    candidate, configured = capsule_module._validate_existing_ancestors(
        unc_root, mapped_candidate, label="archive destination"
    )
    assert candidate == mapped_candidate
    assert configured == unc_root.resolve()
    with pytest.raises(CapsuleError, match="outside its configured root"):
        capsule_module._validate_existing_ancestors(unc_root, outside_candidate, label="archive destination")
