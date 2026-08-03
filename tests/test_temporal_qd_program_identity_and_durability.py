from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

import autoresearch.result_codec as result_codec
import autoresearch.temporal_qd_evolution as qd
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_discovery_validation import (
    build_authored_validation_binding,
    validate_authored_validation_binding,
)


def _candidate() -> dict[str, object]:
    return {
        "candidateId": "candidate_a",
        "sourceProfileSha256": canonical_sha256({"raw": "candidate_a"}),
        "profileSnapshotSha256": canonical_sha256({"normalized": "candidate_a"}),
        "programSha256": canonical_sha256({"authored_program": "candidate_a"}),
    }


def _write_population_envelope(path: Path) -> None:
    """Keep archive tests behind the immutable population-envelope boundary."""
    payload: dict[str, object] = {
        "schemaVersion": "temporal_qd_generation_population_v3",
        "qdVersion": qd.QD_VERSION,
        "policyName": qd.QD_POLICY_NAME,
        "policySha256": qd.QD_POLICY_SHA256,
        "generationIndex": 0,
        "candidateCount": 0,
        "candidates": [],
    }
    payload["populationSha256"] = canonical_sha256(payload)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _window(
    candidate: dict[str, object],
    *,
    source: str | None = None,
    resolved_profile: str | None = None,
    resolved_program: str | None = None,
    v3_admissible: bool = True,
) -> dict[str, object]:
    return {
        "sourceProfileSnapshotSha256": source or str(candidate["profileSnapshotSha256"]),
        "resolvedProfileSnapshotSha256": resolved_profile or canonical_sha256({"resolved": "candidate_a"}),
        "resolvedProgramSha256": resolved_program or canonical_sha256({"resolved_program": "candidate_a"}),
        "programSha256": resolved_program or canonical_sha256({"resolved_program": "candidate_a"}),
        "v3Admissible": v3_admissible,
    }


def _authored_binding_candidate() -> dict[str, object]:
    profile = {"kind": "authored fixture"}
    raw_source_sha = canonical_sha256(profile)
    validation = {
        "profileSnapshotSha256": canonical_sha256({"normalized": profile}),
        "programSha256": canonical_sha256({"program": profile}),
        "validationReportSha256": canonical_sha256({"validation": profile}),
    }
    binding = build_authored_validation_binding(
        raw_source_profile_sha256=raw_source_sha,
        validation=validation,
        provenance={
            "schemaVersion": "temporal_authored_validator_provenance_v1",
            "validationContractSha256": canonical_sha256({}),
            "validatorSchema": "fixture_validator_v1",
            "fuzzfolioCommit": None,
            "validatorCommandSha256": None,
            "commandProvenance": "protocol_command_unavailable",
        },
    )
    return {
        "candidateId": "candidate_authored",
        "sourceProfile": profile,
        "sourceProfileSha256": raw_source_sha,
        "profileSnapshotSha256": validation["profileSnapshotSha256"],
        "programSha256": validation["programSha256"],
        "validationReportSha256": validation["validationReportSha256"],
        "authoredValidationBinding": {
            key: value
            for key, value in binding.items()
            if key != "authoredValidationBindingSha256"
        },
        "authoredValidationBindingSha256": binding[
            "authoredValidationBindingSha256"
        ],
    }


def _refresh_authored_binding_sha(candidate: dict[str, object]) -> None:
    candidate["authoredValidationBindingSha256"] = canonical_sha256(
        candidate["authoredValidationBinding"]
    )


@pytest.mark.parametrize(
    ("windows", "match"),
    [
        (
            [{"resolvedProgramSha256": None, "programSha256": None}],
            "result window 0 program SHA-256",
        ),
        (
            [
                {
                    "sourceProfileSnapshotSha256": canonical_sha256({"wrong": "source"}),
                }
            ],
            "source profile snapshot identity does not match",
        ),
        (
            [
                {"resolvedProgramSha256": canonical_sha256({"program": "one"})},
                {"resolvedProgramSha256": canonical_sha256({"program": "drifted"})},
            ],
            "resolved program identity changed",
        ),
    ],
    ids=("missing-program", "wrong-program", "window-program-drift"),
)
def test_qd_archive_rejects_unbound_or_drifted_window_program_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    windows: list[dict[str, object]],
    match: str,
) -> None:
    candidate = _candidate()
    complete_windows = [_window(candidate) for _ in windows]
    for complete, override in zip(complete_windows, windows, strict=True):
        complete.update(override)
    monkeypatch.setattr(qd, "_load_population", lambda _path: ([candidate], "sha256:" + "a" * 64))
    monkeypatch.setattr(
        qd,
        "load_stage_results",
        lambda _root: {"candidate_a": complete_windows},
    )
    _write_population_envelope(tmp_path / "population.json")

    with pytest.raises(TemporalDiscoveryContractError, match=match):
        qd.build_qd_archive(
            population_path=tmp_path / "population.json",
            result_root=tmp_path / "result-root",
            output_path=tmp_path / "archive.json",
            generation_index=0,
        )


def test_qd_archive_keeps_v3_admissibility_gate_after_program_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate()
    monkeypatch.setattr(qd, "_load_population", lambda _path: ([candidate], "sha256:" + "a" * 64))
    monkeypatch.setattr(
        qd,
        "load_stage_results",
        lambda _root: {
            "candidate_a": [
                _window(candidate, v3_admissible=False)
            ]
        },
    )
    _write_population_envelope(tmp_path / "population.json")

    with pytest.raises(TemporalDiscoveryContractError, match="requires terminal-adjusted"):
        qd.build_qd_archive(
            population_path=tmp_path / "population.json",
            result_root=tmp_path / "result-root",
            output_path=tmp_path / "archive.json",
            generation_index=0,
        )


def test_qd_archive_accepts_authored_vs_resolved_execution_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate()
    resolved_profile = canonical_sha256({"resolved": "profile"})
    resolved_program = canonical_sha256({"resolved": "program"})
    windows = [
        _window(
            candidate,
            resolved_profile=resolved_profile,
            resolved_program=resolved_program,
        )
    ]
    monkeypatch.setattr(
        qd, "_load_population", lambda _path: ([candidate], "sha256:" + "a" * 64)
    )
    monkeypatch.setattr(qd, "load_stage_results", lambda _root: {"candidate_a": windows})
    monkeypatch.setattr(
        qd,
        "_aggregate_candidate",
        lambda source, _windows: {
            "v3Admissible": True,
            "totalTrades": 0,
            "authoredProgramSha256": source["programSha256"],
            "sourceProfileSnapshotSha256": source["profileSnapshotSha256"],
            "resolvedProfileSnapshotSha256": resolved_profile,
            "resolvedProgramSha256": resolved_program,
            "programSha256": resolved_program,
        },
    )
    monkeypatch.setattr(qd, "qd_behavior_descriptor", lambda *_args: {})
    monkeypatch.setattr(qd, "_objective_row", lambda *_args: {})
    monkeypatch.setattr(qd, "_finite_data_validity", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        qd,
        "select_qd_archive",
        lambda members, **_kwargs: [{"cellId": "fixture", "members": members}],
    )
    _write_population_envelope(tmp_path / "population.json")

    qd.build_qd_archive(
        population_path=tmp_path / "population.json",
        result_root=tmp_path / "result-root",
        output_path=tmp_path / "archive.json",
        generation_index=0,
    )

    archive = json.loads((tmp_path / "archive.json").read_text())
    aggregate = archive["cells"][0]["members"][0]["aggregate"]
    assert aggregate["authoredProgramSha256"] == candidate["programSha256"]
    assert aggregate["resolvedProfileSnapshotSha256"] == resolved_profile
    assert aggregate["resolvedProgramSha256"] == resolved_program
    assert aggregate["programSha256"] == resolved_program


def test_qd_archive_deduplicates_distinct_authored_candidates_with_one_resolved_program(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _candidate()
    second = {
        **_candidate(),
        "candidateId": "candidate_b",
        "sourceProfileSha256": canonical_sha256({"raw": "candidate_b"}),
        "profileSnapshotSha256": canonical_sha256({"normalized": "candidate_b"}),
        "programSha256": canonical_sha256({"authored_program": "candidate_b"}),
    }
    resolved_profile = canonical_sha256({"resolved": "shared profile"})
    resolved_program = canonical_sha256({"resolved": "shared program"})
    result_set = {
        "candidate_a": [
            _window(
                first,
                resolved_profile=resolved_profile,
                resolved_program=resolved_program,
            )
        ],
        "candidate_b": [
            _window(
                second,
                resolved_profile=resolved_profile,
                resolved_program=resolved_program,
            )
        ],
    }
    monkeypatch.setattr(
        qd, "_load_population", lambda _path: ([first, second], "sha256:" + "a" * 64)
    )
    monkeypatch.setattr(qd, "load_stage_results", lambda _root: result_set)
    monkeypatch.setattr(
        qd,
        "_aggregate_candidate",
        lambda source, _windows: {
            "v3Admissible": True,
            "totalTrades": 10,
            "authoredProgramSha256": source["programSha256"],
            "sourceProfileSnapshotSha256": source["profileSnapshotSha256"],
            "resolvedProfileSnapshotSha256": resolved_profile,
            "resolvedProgramSha256": resolved_program,
            "programSha256": resolved_program,
        },
    )
    monkeypatch.setattr(qd, "qd_behavior_descriptor", lambda *_args: {"cellId": "fixture"})
    monkeypatch.setattr(
        qd,
        "_objective_row",
        lambda candidate, _aggregate: {
            "worstWindowConservativeNetR": 1.0,
            "maximumDrawdownR": 0.5,
            "structuralComplexity": 1.0,
        },
    )
    monkeypatch.setattr(
        qd,
        "_finite_data_validity",
        lambda *_args, **_kwargs: {"validForQuality": True},
    )
    monkeypatch.setattr(
        qd,
        "select_qd_archive",
        lambda members, **_kwargs: [
            {"cellId": "fixture", "members": list(members)}
        ],
    )
    _write_population_envelope(tmp_path / "population.json")

    qd.build_qd_archive(
        population_path=tmp_path / "population.json",
        result_root=tmp_path / "result-root",
        output_path=tmp_path / "archive.json",
        generation_index=0,
    )

    archive = json.loads((tmp_path / "archive.json").read_text(encoding="utf-8"))
    members = archive["cells"][0]["members"]
    assert len(members) == 1
    assert members[0]["candidateId"] == "candidate_a"
    assert archive["authoredProgramCountBeforeResolvedDeduplication"] == 2
    assert archive["resolvedProgramCountBeforeReduction"] == 1
    assert archive["resolvedExecutionDeduplication"]["frozenPolicy"] == (
        qd.QD_POLICY["resolvedExecutionDeduplication"]
    )
    assert archive["resolvedExecutionDeduplication"]["duplicates"] == [
        {
            "discardedCandidateIds": ["candidate_b"],
            "resolvedProgramSha256": resolved_program,
            "retainedCandidateId": "candidate_a",
        }
    ]


def test_qd_population_admission_rejects_falsified_authored_program_binding(
    tmp_path: Path,
) -> None:
    candidate = _authored_binding_candidate()
    population = {
        "candidateCount": 1,
        "candidates": [candidate],
        "authoredValidationBindingRequired": True,
    }
    population["populationSha256"] = canonical_sha256(population)
    population_path = tmp_path / "population.json"
    population_path.write_text(
        json.dumps(population, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    admitted, _identity = qd._load_population(population_path)
    assert admitted[0]["candidateId"] == candidate["candidateId"]

    population["candidates"][0]["programSha256"] = canonical_sha256(
        {"falsified": "program"}
    )
    material = dict(population)
    material.pop("populationSha256")
    population["populationSha256"] = canonical_sha256(material)
    population_path.write_text(
        json.dumps(population, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="authored validation binding diverges from candidate programSha256",
    ):
        qd._load_population(population_path)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda binding: binding.pop("schemaVersion"),
            "binding schema fields are not exact",
        ),
        (
            lambda binding: binding.__setitem__("schemaVersion", "unknown_schema_v1"),
            "binding has an unknown schema",
        ),
    ],
    ids=("missing-schema", "unknown-schema"),
)
def test_authored_validation_binding_rejects_missing_or_unknown_schema(
    mutation: object,
    match: str,
) -> None:
    candidate = _authored_binding_candidate()
    binding = candidate["authoredValidationBinding"]
    assert isinstance(binding, dict)
    mutation(binding)
    _refresh_authored_binding_sha(candidate)

    with pytest.raises(TemporalDiscoveryContractError, match=match):
        validate_authored_validation_binding(candidate)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda provenance: provenance.__setitem__(
                "validationContractSha256", "not-a-canonical-digest"
            ),
            "validation contract SHA-256",
        ),
        (
            lambda provenance: provenance.__setitem__("validatorSchema", ""),
            "validator schema must be a nonempty canonical string",
        ),
        (
            lambda provenance: provenance.__setitem__("fuzzfolioCommit", "A" * 40),
            "fuzzfolio commit must be None or an exact lowercase commit SHA",
        ),
        (
            lambda provenance: provenance.__setitem__(
                "validatorCommandSha256", "not-a-canonical-digest"
            ),
            "command SHA-256",
        ),
        (
            lambda provenance: provenance.__setitem__(
                "commandProvenance", "unknown_command_source"
            ),
            "command provenance is unknown",
        ),
        (
            lambda provenance: provenance.__setitem__(
                "commandProvenance", "declared_subprocess_command"
            ),
            "command presence is inconsistent",
        ),
    ],
    ids=("bad-contract-sha", "empty-validator-schema", "uppercase-commit", "bad-command-sha", "unknown-command-source", "missing-command-sha"),
)
def test_authored_validation_binding_rejects_malformed_validator_provenance(
    mutation: object,
    match: str,
) -> None:
    candidate = copy.deepcopy(_authored_binding_candidate())
    binding = candidate["authoredValidationBinding"]
    assert isinstance(binding, dict)
    provenance = binding["validatorProvenance"]
    assert isinstance(provenance, dict)
    mutation(provenance)
    _refresh_authored_binding_sha(candidate)

    with pytest.raises(TemporalDiscoveryContractError, match=match):
        validate_authored_validation_binding(candidate)


def test_authored_validation_binding_accepts_none_or_exact_commit_provenance() -> None:
    candidate = _authored_binding_candidate()
    validate_authored_validation_binding(candidate)

    binding = candidate["authoredValidationBinding"]
    assert isinstance(binding, dict)
    provenance = binding["validatorProvenance"]
    assert isinstance(provenance, dict)
    provenance["fuzzfolioCommit"] = "a" * 40
    _refresh_authored_binding_sha(candidate)
    validate_authored_validation_binding(candidate)


def test_gzip_publication_fsyncs_payload_before_link_and_directory_after(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    original_fsync = result_codec.os.fsync
    original_link = result_codec.os.link

    def fsync(descriptor: int) -> None:
        events.append("payload-fsync")
        original_fsync(descriptor)

    def link(source: Path, target: Path) -> None:
        events.append("publish-link")
        original_link(source, target)

    monkeypatch.setattr(result_codec.os, "fsync", fsync)
    monkeypatch.setattr(result_codec.os, "link", link)
    monkeypatch.setattr(
        result_codec,
        "fsync_directory",
        lambda _directory: events.append("directory-fsync") or True,
    )

    result_codec.write_gzip_json_once(tmp_path / "candidate.json.gz", {"value": 1})
    assert events == ["payload-fsync", "publish-link", "directory-fsync"]


def test_qd_immutable_and_mutable_publication_have_durable_ordering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    original_fsync = qd.os.fsync
    original_link = qd.os.link
    original_replace = qd.os.replace

    def fsync(descriptor: int) -> None:
        events.append("payload-fsync")
        original_fsync(descriptor)

    def link(source: Path, target: Path) -> None:
        events.append("publish-link")
        original_link(source, target)

    def replace(source: Path, target: Path) -> None:
        events.append("publish-replace")
        original_replace(source, target)

    monkeypatch.setattr(qd.os, "fsync", fsync)
    monkeypatch.setattr(qd.os, "link", link)
    monkeypatch.setattr(qd.os, "replace", replace)
    monkeypatch.setattr(
        qd,
        "fsync_directory",
        lambda _directory: events.append("directory-fsync") or True,
    )

    immutable = tmp_path / "immutable.json"
    qd._write_once(immutable, {"value": 1})
    assert events == ["payload-fsync", "publish-link", "directory-fsync"]
    assert immutable.read_text(encoding="utf-8") == '{\n  "value": 1\n}\n'

    events.clear()
    qd._replace(tmp_path / "checkpoint.json", {"value": 2})
    assert events == ["payload-fsync", "publish-replace", "directory-fsync"]


def test_windows_directory_sync_fallback_is_safe_when_native_open_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A Windows host can support directory FlushFileBuffers (as this machine
    # often does), so force the unavailable-native-API path explicitly.  It
    # must be a safe no-op rather than failing an interrupted publication or
    # pretending the file payload itself was not already durable.
    import ctypes

    def unavailable(*_args: object, **_kwargs: object) -> object:
        raise OSError("native directory handles unavailable")

    monkeypatch.setattr(ctypes, "WinDLL", unavailable)
    assert result_codec._fsync_directory_windows(tmp_path) is False
    assert result_codec._unsupported_windows_directory_sync_error(5) is True
    assert result_codec._unsupported_windows_directory_sync_error(12345) is False
