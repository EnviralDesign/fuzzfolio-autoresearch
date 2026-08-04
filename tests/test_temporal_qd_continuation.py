from __future__ import annotations

from pathlib import Path
import json
import sys

import pytest

import autoresearch.temporal_qd_supervisor as supervisor
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)


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


def test_continuation_rejects_fresh_broad_five_generation_count_without_reading_source(
    tmp_path: Path,
) -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="exactly four generations"):
        supervisor.run_qd_continuation(
            source_run_root=tmp_path / "does-not-need-to-exist",
            run_root=tmp_path / "continued",
            generation_count=supervisor.FRESH_BROAD_GENERATION_COUNT,
        )


def test_completed_fresh_five_generation_source_binds_continuation_to_six_through_nine(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "fresh-1-5"
    source.mkdir()
    archive = source / "generations" / "generation-0005" / "archive.json"
    archive.parent.mkdir(parents=True)
    archive.write_text("{}", encoding="utf-8")
    config = {
        "broadAdmission": True,
        "broadAdmissionContract": {
            "schemaVersion": "temporal_qd_broad_admission_contract_v1",
            **supervisor._broad_admission_contract_values(
                supervisor.FRESH_BROAD_GENERATION_COUNT
            ),
        },
        "generationPlan": {
            "firstGenerationIndex": 1,
            "generationCount": supervisor.FRESH_BROAD_GENERATION_COUNT,
        }
    }
    config["configSha256"] = canonical_sha256(config)
    state = {
        "status": "completed",
        "stateSha256": "sha256:" + "b" * 64,
    }
    latest = {
        "archivePath": str(archive.resolve()),
        "archiveSha256": "sha256:" + "c" * 64,
        "nextImmigrantContinuationOrdinal": 123,
    }

    monkeypatch.setattr(supervisor, "_canonical_file", lambda *_args, **_kwargs: config)
    monkeypatch.setattr(supervisor, "_load_state", lambda *_args, **_kwargs: state)
    monkeypatch.setattr(
        supervisor,
        "_validate_completed_generations",
        lambda **_kwargs: {
            index: (
                latest
                if index == supervisor.FRESH_BROAD_GENERATION_COUNT
                else {"generationIndex": index}
            )
            for index in range(1, supervisor.FRESH_BROAD_GENERATION_COUNT + 1)
        },
    )
    monkeypatch.setattr(
        supervisor, "_validate_evidence_ladder_execution", lambda **_kwargs: None
    )

    binding = supervisor._continuation_binding(source)
    assert binding["sourceFirstGenerationIndex"] == 1
    assert binding["sourceLastGenerationIndex"] == 5

    captured: dict = {}
    monkeypatch.setattr(
        supervisor,
        "run_qd_supervisor",
        lambda **kwargs: captured.update(kwargs) or {"status": "started"},
    )
    result = supervisor.run_qd_continuation(
        source_run_root=source,
        run_root=tmp_path / "continued-6-9",
        generation_count=supervisor.LEGACY_CONTINUATION_GENERATION_COUNT,
    )

    assert result == {"status": "started"}
    assert captured["first_generation_index"] == 6
    assert captured["generation_count"] == supervisor.LEGACY_CONTINUATION_GENERATION_COUNT
    assert captured["continuation_from"] == binding

    config["continuationFrom"] = {"sourceRunRoot": str(tmp_path / "forbidden-prior")}
    config["configSha256"] = canonical_sha256(
        {key: value for key, value in config.items() if key != "configSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="cannot be a chained"):
        supervisor._continuation_binding(source)


def test_continuation_binding_rejects_malformed_source_generation_count(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "malformed-source"
    source.mkdir()
    config = {"generationPlan": {"firstGenerationIndex": 1, "generationCount": 6}}
    config["configSha256"] = canonical_sha256({"generationPlan": config["generationPlan"]})
    monkeypatch.setattr(supervisor, "_canonical_file", lambda *_args, **_kwargs: config)

    with pytest.raises(TemporalDiscoveryContractError, match="fresh five-generation"):
        supervisor._continuation_binding(source)


@pytest.mark.parametrize(
    ("broad_admission", "candidate_count", "message"),
    [
        (False, supervisor.FRESH_BROAD_CANDIDATES_PER_GENERATION, "not admitted"),
        (True, supervisor.FRESH_BROAD_CANDIDATES_PER_GENERATION - 1, "does not match"),
    ],
)
def test_continuation_binding_rejects_missing_or_drifted_broad_admission_provenance(
    tmp_path: Path,
    monkeypatch,
    broad_admission: bool,
    candidate_count: int,
    message: str,
) -> None:
    source = tmp_path / "not-an-admitted-source"
    source.mkdir()
    contract = {
        "schemaVersion": "temporal_qd_broad_admission_contract_v1",
        **supervisor._broad_admission_contract_values(
            supervisor.FRESH_BROAD_GENERATION_COUNT
        ),
    }
    contract["candidatesPerGeneration"] = candidate_count
    config = {
        "broadAdmission": broad_admission,
        "broadAdmissionContract": contract,
        "generationPlan": {
            "firstGenerationIndex": 1,
            "generationCount": supervisor.FRESH_BROAD_GENERATION_COUNT,
        },
    }
    config["configSha256"] = canonical_sha256(config)
    monkeypatch.setattr(supervisor, "_canonical_file", lambda *_args, **_kwargs: config)

    with pytest.raises(TemporalDiscoveryContractError, match=message):
        supervisor._continuation_binding(source)


def test_chained_legacy_block_requires_and_proves_immediate_prior_contiguity(
    tmp_path: Path, monkeypatch
) -> None:
    config_by_root: dict[Path, dict] = {}

    def admitted_config(
        *, first_generation: int, continuation_from: dict | None = None
    ) -> dict:
        config = {
            "broadAdmission": True,
            "broadAdmissionContract": {
                "schemaVersion": "temporal_qd_broad_admission_contract_v1",
                **supervisor._broad_admission_contract_values(
                    supervisor.LEGACY_CONTINUATION_GENERATION_COUNT
                ),
            },
            "generationPlan": {
                "firstGenerationIndex": first_generation,
                "generationCount": supervisor.LEGACY_CONTINUATION_GENERATION_COUNT,
            },
            **(
                {"continuationFrom": continuation_from}
                if continuation_from is not None
                else {}
            ),
        }
        config["configSha256"] = canonical_sha256(config)
        return config

    prior_root = tmp_path / "legacy-1-4"
    prior_root.mkdir()
    source_root = tmp_path / "legacy-5-8"
    source_root.mkdir()
    for root, generation in ((prior_root, 4), (source_root, 8)):
        archive = root / "generations" / f"generation-{generation:04d}" / "archive.json"
        archive.parent.mkdir(parents=True)
        archive.write_text("{}", encoding="utf-8")
    (source_root / "generations" / "generation-0009").mkdir(parents=True)
    (source_root / "generations" / "generation-0009" / "archive.json").write_text(
        "{}", encoding="utf-8"
    )

    config_by_root[prior_root.resolve()] = admitted_config(first_generation=1)

    def fake_canonical(path: Path, **_kwargs):
        return config_by_root[path.parent.resolve()]

    def fake_completed(*, root: Path, config: dict, **_kwargs):
        plan = config["generationPlan"]
        first = plan["firstGenerationIndex"]
        count = plan["generationCount"]
        last = first + count - 1
        archive = root / "generations" / f"generation-{last:04d}" / "archive.json"
        return {
            index: (
                {
                    "archivePath": str(archive.resolve()),
                    "archiveSha256": "sha256:" + "c" * 64,
                    "nextImmigrantContinuationOrdinal": last * 10,
                }
                if index == last
                else {"generationIndex": index}
            )
            for index in range(first, last + 1)
        }

    monkeypatch.setattr(supervisor, "_canonical_file", fake_canonical)
    monkeypatch.setattr(
        supervisor,
        "_load_state",
        lambda *_args, **_kwargs: {"status": "completed", "stateSha256": "sha256:" + "b" * 64},
    )
    monkeypatch.setattr(supervisor, "_validate_completed_generations", fake_completed)
    monkeypatch.setattr(
        supervisor, "_validate_evidence_ladder_execution", lambda **_kwargs: None
    )

    prior_binding = supervisor._continuation_binding(prior_root)
    config_by_root[source_root.resolve()] = admitted_config(
        first_generation=5,
        continuation_from=prior_binding,
    )

    binding = supervisor._continuation_binding(source_root)
    assert binding["sourceFirstGenerationIndex"] == 5
    assert binding["sourceLastGenerationIndex"] == 8
    assert binding["priorContinuationFrom"] == prior_binding

    config_by_root[source_root.resolve()] = admitted_config(
        first_generation=6,
        continuation_from=prior_binding,
    )
    with pytest.raises(TemporalDiscoveryContractError, match="immediately after"):
        supervisor._continuation_binding(source_root)

    (source_root / "generations" / "generation-0005").mkdir(parents=True)
    (source_root / "generations" / "generation-0005" / "archive.json").write_text(
        "{}", encoding="utf-8"
    )
    config_by_root[source_root.resolve()] = admitted_config(first_generation=2)
    with pytest.raises(TemporalDiscoveryContractError, match="begin at generation 1"):
        supervisor._continuation_binding(source_root)


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
