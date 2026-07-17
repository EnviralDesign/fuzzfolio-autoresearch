from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch import __main__ as ar_main
from autoresearch import generation_cli
from autoresearch.__main__ import build_parser
from autoresearch.generation_archive import GENERATION_MANIFEST_NAME, GenerationArchiveService, utc_now


def _runs_root(tmp_path: Path) -> Path:
    root = tmp_path / "runs"
    (root / "derived").mkdir(parents=True)
    (root / "run-001").mkdir()
    (root / "run-001" / "attempts.jsonl").write_text('{"attempt": 1}\n', encoding="utf-8")
    (root / "derived" / "attempt-catalog.sqlite").write_bytes(b"catalog")
    (root / "derived" / "finalized-corpus.json").write_text("{}", encoding="utf-8")
    (root / "derived" / "corpus-manifest.json").write_text("{}", encoding="utf-8")
    (root / GENERATION_MANIFEST_NAME).write_text("{}", encoding="utf-8")
    return root


def _provenance() -> dict[str, str]:
    return {
        "operator_command": "archive-generation --apply",
        "autoresearch_git_revision": "a" * 40,
        "trading_dashboard_git_revision": "b" * 40,
        "market_data_lake_git_revision": "c" * 40,
        "lake_semantic_sha256": "sha256:" + "d" * 64,
        "source_snapshot_sha256": "sha256:" + "e" * 64,
        "universe_id": "universe-001",
        "universe_manifest_sha256": "sha256:" + "1" * 64,
        "worker_contract_id": "replay-worker-contract-v1",
        "worker_contract_sha256": "sha256:" + "f" * 64,
        "worker_image": "registry.example/worker:1",
        "engine_id": "fuzzfolio-replay-engine-v1",
        "engine_sha256": "sha256:" + "2" * 64,
        "scoring_policy_id": "score-lab-v1",
        "scoring_policy_sha256": "sha256:" + "3" * 64,
        "cost_policy_id": "research-conservative-v1",
        "cost_policy_sha256": "sha256:" + "4" * 64,
    }


def _configure_runs_root(monkeypatch: pytest.MonkeyPatch, runs_root: Path) -> None:
    monkeypatch.setattr(generation_cli, "load_config", lambda: SimpleNamespace(runs_root=runs_root))


def _json_output(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    return json.loads(capsys.readouterr().out)


def _add_quiescence(
    provenance: dict[str, object], preview: dict[str, object], runs_root: Path, tmp_path: Path
) -> dict[str, object]:
    marker = tmp_path / "writers-quiesced.json"
    marker.write_text(
        json.dumps(
            {
                "schema_name": "autoresearch.generation.quiescence", "schema_version": 1,
                "state": "quiesced", "writer_scope": "all-writers-stopped",
                "runs_root": str(runs_root.resolve()), "archive_id": "archive-001",
                "archive_root": str((runs_root.parent / "runs_archive").resolve()),
                "new_generation_id": "generation-002", "inventory_identity": preview["inventory_identity"],
                "issuer_id": "cli-test-supervisor",
                "nonce": hashlib.sha256(b"cli-test-archive-001-generation-002").hexdigest(),
                "issued_at": utc_now(),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    provenance["reviewed_inventory_identity"] = preview["inventory_identity"]
    provenance["cutover_quiescence"] = {"marker_path": str(marker), "marker_sha256": hashlib.sha256(marker.read_bytes()).hexdigest()}
    return provenance


def _invoke(tmp_path: Path, provenance_path: Path, *extra: str) -> list[str]:
    return ["archive-generation", "--archive-id", "archive-001", "--new-generation-id", "generation-002", "--provenance-json", str(provenance_path), *extra, "--json"]


def test_archive_generation_is_registered_as_direct_script() -> None:
    pyproject = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")

    assert 'archive-generation = "autoresearch.__main__:main"' in pyproject


def test_archive_generation_cli_help() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["archive-generation", "--help"])

    assert exc.value.code == 0


def test_archive_generation_defaults_to_non_mutating_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    runs_root = _runs_root(tmp_path)
    _configure_runs_root(monkeypatch, runs_root)
    provenance_path = tmp_path / "provenance.json"
    provenance_path.write_text(json.dumps(_provenance()), encoding="utf-8")
    before = sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*"))

    assert ar_main.main(_invoke(tmp_path, provenance_path)) == 0
    payload = _json_output(capsys)
    assert payload["dry_run"] is True
    assert payload["ready"] is True
    assert isinstance(payload["inventory_identity"], str)
    assert sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*")) == before


def test_cli_apply_requires_then_uses_the_exact_reviewed_preview_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    runs_root = _runs_root(tmp_path)
    _configure_runs_root(monkeypatch, runs_root)
    provenance_path = tmp_path / "provenance.json"
    provenance = _provenance()
    provenance_path.write_text(json.dumps(provenance), encoding="utf-8")

    assert ar_main.main(_invoke(tmp_path, provenance_path)) == 0
    preview = _json_output(capsys)
    assert ar_main.main(_invoke(tmp_path, provenance_path, "--apply")) == 1
    assert "reviewed_inventory_identity" in str(_json_output(capsys)["error"])

    provenance_path.write_text(json.dumps(_add_quiescence(provenance, preview, runs_root, tmp_path)), encoding="utf-8")
    assert ar_main.main(_invoke(tmp_path, provenance_path, "--apply")) == 0
    payload = _json_output(capsys)
    assert payload["dry_run"] is False
    assert (tmp_path / "runs_archive" / "archive-001" / "runs" / "run-001").is_dir()
    assert (runs_root / GENERATION_MANIFEST_NAME).is_file()


def test_archive_generation_rejects_malformed_provenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _configure_runs_root(monkeypatch, _runs_root(tmp_path))
    provenance_path = tmp_path / "provenance.json"
    provenance_path.write_text("{", encoding="utf-8")
    assert ar_main.main(_invoke(tmp_path, provenance_path)) == 1
    assert "invalid provenance JSON" in str(_json_output(capsys)["error"])


@pytest.mark.parametrize("apply", [False, True])
def test_archive_generation_reports_missing_provenance_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], apply: bool) -> None:
    _configure_runs_root(monkeypatch, _runs_root(tmp_path))
    provenance_path = tmp_path / "provenance.json"
    provenance_path.write_text("{}", encoding="utf-8")
    assert ar_main.main(_invoke(tmp_path, provenance_path, *( ["--apply"] if apply else []))) == 1
    payload = _json_output(capsys)
    assert payload["ready"] is False
    assert payload["missing_required_provenance_fields"] == list(generation_cli.REQUIRED_PROVENANCE_FIELDS)


def test_archive_generation_rejects_noncanonical_provenance_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _configure_runs_root(monkeypatch, _runs_root(tmp_path))
    provenance = _provenance()
    provenance["lake_semantic_sha256"] = "not-a-content-identity"
    provenance_path = tmp_path / "provenance.json"
    provenance_path.write_text(json.dumps(provenance), encoding="utf-8")
    assert ar_main.main(_invoke(tmp_path, provenance_path)) == 1
    assert "lake_semantic_sha256" in str(_json_output(capsys)["error"])


def test_restore_generation_plan_is_non_mutating(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    runs_root = _runs_root(tmp_path)
    _configure_runs_root(monkeypatch, runs_root)
    service = GenerationArchiveService(runs_root)
    base = _provenance()
    preview = service.dry_run("archive-001", "generation-002", provenance=base)
    service.cutover("archive-001", "generation-002", provenance=_add_quiescence(base, preview, runs_root, tmp_path))
    destination = tmp_path / "restored-runs"
    assert ar_main.main(["restore-generation-plan", "--archive-id", "archive-001", "--destination-runs-root", str(destination), "--json"]) == 0
    assert _json_output(capsys)["dry_run"] is True
    assert not destination.exists()
