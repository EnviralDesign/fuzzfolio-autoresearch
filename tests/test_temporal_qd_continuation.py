from __future__ import annotations

from pathlib import Path
import json
import sys

import autoresearch.temporal_qd_supervisor as supervisor


def test_four_generation_continuation_is_deterministic_and_never_targets_source_root(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source-1-4"
    source.mkdir()
    sentinel = source / "immutable-source-marker"
    sentinel.write_text("generations 1-4", encoding="utf-8")
    archive = source / "generations" / "generation-0004" / "archive.json"
    archive.parent.mkdir(parents=True)
    archive.write_text("{}", encoding="utf-8")
    binding = {
        "schemaVersion": "temporal_qd_generation_continuation_v1",
        "sourceRunRoot": str(source.resolve()),
        "sourceConfigSha256": "sha256:" + "a" * 64,
        "sourceStateSha256": "sha256:" + "b" * 64,
        "sourceLastGenerationIndex": 4,
        "sourceArchivePath": str(archive.resolve()),
        "sourceArchiveSha256": "sha256:" + "c" * 64,
        "nextImmigrantContinuationOrdinal": 91,
    }
    calls: list[dict] = []

    monkeypatch.setattr(supervisor, "_continuation_binding", lambda value: binding)

    def fake_run(**kwargs):
        calls.append(kwargs)
        return {"config": kwargs["continuation_from"], "runRoot": str(kwargs["run_root"])}

    monkeypatch.setattr(supervisor, "run_qd_supervisor", fake_run)
    common = {
        "source_run_root": source,
        "run_root": tmp_path / "continued-5-8",
        "generation_count": 4,
        "source_preparation_path": tmp_path / "source.json",
        "base_generator_root": tmp_path / "generator",
        "confirmed_entry_admission_root": tmp_path / "admission",
        "template_preparation_path": tmp_path / "template.json",
        "validator_command_file": tmp_path / "validator.json",
        "parameters": {"fixture": True},
        "autoresearch_commit": "a" * 40,
        "execution_engine_commit": "b" * 40,
        "worker_contract_sha256": "sha256:" + "c" * 64,
        "gateway_url": "http://127.0.0.1:8799",
    }
    first = supervisor.run_qd_continuation(**common)
    second = supervisor.run_qd_continuation(**common)
    assert first == second
    assert len(calls) == 2
    for call in calls:
        assert call["initial_archive_path"] == str(archive.resolve())
        assert call["first_generation_index"] == 5
        assert call["initial_immigrant_continuation_ordinal"] == 91
        assert call["continuation_from"] == binding
        assert Path(call["run_root"]).resolve() != source.resolve()
    assert sentinel.read_text(encoding="utf-8") == "generations 1-4"


def test_cli_continue_from_uses_first_class_continuation_surface(tmp_path: Path, monkeypatch) -> None:
    parameters = tmp_path / "parameters.json"
    parameters.write_text(json.dumps({"fixture": True}), encoding="utf-8")
    seen: dict = {}

    def fake_continue(**kwargs):
        seen.update(kwargs)
        return {"status": "continued"}

    monkeypatch.setattr(supervisor, "run_qd_continuation", fake_continue)
    monkeypatch.setattr(supervisor, "load_lab_gateway_token", lambda **_: "token")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "temporal_qd_supervisor",
            "--run-root", str(tmp_path / "next"),
            "--continue-from", str(tmp_path / "source"),
            "--source-preparation", str(tmp_path / "source.json"),
            "--base-generator-root", str(tmp_path / "generator"),
            "--confirmed-entry-admission-root", str(tmp_path / "admission"),
            "--template-preparation", str(tmp_path / "template.json"),
            "--validator-command-file", str(tmp_path / "validator.json"),
            "--parameters", str(parameters),
            "--construction-catalog", str(tmp_path / "catalog.json"),
            "--generation-count", "4",
            "--autoresearch-commit", "a" * 40,
            "--execution-engine-commit", "b" * 40,
            "--worker-contract-sha256", "sha256:" + "c" * 64,
        ],
    )
    supervisor.main()
    assert seen["source_run_root"] == tmp_path / "source"
    assert seen["generation_count"] == 4


def test_continuation_chains_from_any_completed_contiguous_four_generations(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source-5-8"
    source.mkdir()
    sentinel = source / "immutable-source-marker"
    sentinel.write_text("generations 5-8", encoding="utf-8")
    archive = source / "generations" / "generation-0008" / "archive.json"
    archive.parent.mkdir(parents=True)
    archive.write_text("{}", encoding="utf-8")
    prior = {
        "schemaVersion": "temporal_qd_generation_continuation_v1",
        "sourceRunRoot": str((tmp_path / "source-1-4").resolve()),
        "sourceFirstGenerationIndex": 1,
        "sourceLastGenerationIndex": 4,
    }
    binding = {
        "schemaVersion": "temporal_qd_generation_continuation_v1",
        "sourceRunRoot": str(source.resolve()),
        "sourceConfigSha256": "sha256:" + "a" * 64,
        "sourceStateSha256": "sha256:" + "b" * 64,
        "sourceFirstGenerationIndex": 5,
        "sourceLastGenerationIndex": 8,
        "sourceArchivePath": str(archive.resolve()),
        "sourceArchiveSha256": "sha256:" + "c" * 64,
        "nextImmigrantContinuationOrdinal": 193,
        "priorContinuationFrom": prior,
    }
    seen: dict = {}
    monkeypatch.setattr(supervisor, "_continuation_binding", lambda value: binding)
    monkeypatch.setattr(supervisor, "run_qd_supervisor", lambda **kwargs: seen.update(kwargs) or {"status": "started"})

    supervisor.run_qd_continuation(
        source_run_root=source,
        run_root=tmp_path / "continued-9-12",
        generation_count=4,
        source_preparation_path=tmp_path / "source.json",
        base_generator_root=tmp_path / "generator",
        confirmed_entry_admission_root=tmp_path / "admission",
        template_preparation_path=tmp_path / "template.json",
        validator_command_file=tmp_path / "validator.json",
        parameters={"fixture": True},
        autoresearch_commit="a" * 40,
        execution_engine_commit="b" * 40,
        worker_contract_sha256="sha256:" + "c" * 64,
        gateway_url="http://127.0.0.1:8799",
    )
    assert seen["first_generation_index"] == 9
    assert seen["continuation_from"] == binding
    assert seen["continuation_from"]["priorContinuationFrom"] == prior
    assert Path(seen["run_root"]).resolve() != source.resolve()
    assert sentinel.read_text(encoding="utf-8") == "generations 5-8"
