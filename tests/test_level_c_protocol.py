from __future__ import annotations

import json
import hashlib
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.level_c_protocol import (
    LevelCProtocolError,
    build_initial_four_cutoff_plans,
    create_level_c_protocol,
    create_level_c_protocol_authority,
    load_level_c_protocol,
    load_level_c_protocol_authority,
)


def _hash(character: str) -> str:
    return "sha256:" + character * 64


def _cutoff(key: str, role: str, start: str, outer_start: str, outer_end: str) -> dict:
    selection_end = (
        datetime.fromisoformat(f"{start}T00:00:00+00:00") + timedelta(days=2)
    ).isoformat().replace("+00:00", "Z")
    payload = {
        "cutoff_key": key,
        "role": role,
        "selection_start": f"{start}T00:00:00Z",
        "selection_end": selection_end,
        "training_start": f"{start}T00:00:00Z",
        "training_end": selection_end,
        "embargo_start": selection_end,
        "embargo_end": f"{outer_start}T00:00:00Z",
        "embargo_days": 15,
        "outer_test_start": f"{outer_start}T00:00:00Z",
        "outer_test_end": f"{outer_end}T00:00:00Z",
        "atlas_run_id": f"atlas-{key.lower()}",
        "playhand_campaign_id": f"playhand-{key.lower()}",
        "cohort_id": f"cohort-{key.lower()}",
        "seed": 100 + ord(key),
        "expected_artifact_locations": {
            "result": f"derived/evidence/{key.lower()}/result.json"
        },
    }
    payload["geometry_sha256"] = canonical_sha256(
        {
            field: payload[field]
            for field in (
                "selection_start",
                "selection_end",
                "training_start",
                "training_end",
                "embargo_start",
                "embargo_end",
                "embargo_days",
                "outer_test_start",
                "outer_test_end",
            )
        }
    )
    return payload


def _rehash_cutoff(payload: dict) -> None:
    payload["geometry_sha256"] = canonical_sha256(
        {
            field: payload[field]
            for field in (
                "selection_start",
                "selection_end",
                "training_start",
                "training_end",
                "embargo_start",
                "embargo_end",
                "embargo_days",
                "outer_test_start",
                "outer_test_end",
            )
        }
    )


def _protocol() -> dict:
    return {
        "protocol_name": "level-c-initial",
        "protocol_version": "v1",
        "status": "frozen",
        "research_generation_id": "generation-001",
        "research_generation_manifest_sha256": _hash("a"),
        "lake_semantic_sha256": _hash("b"),
        "source_snapshot_sha256": _hash("c"),
        "source_coverage_end": "2025-12-31T00:00:00Z",
        "universe_id": "fx-major-v1",
        "universe_manifest_sha256": _hash("d"),
        "worker_contract_id": "worker-contract-v1",
        "worker_contract_sha256": _hash("e"),
        "worker_image": "ghcr.io.fuzzfolio.worker-v1",
        "engine_id": "engine-v1",
        "engine_sha256": _hash("f"),
        "scoring_policy_id": "score-v1",
        "scoring_policy_sha256": _hash("1"),
        "cost_policy_id": "cost-v1",
        "cost_policy_sha256": _hash("2"),
        "global_seed": 77,
        "no_global_priors": True,
        "no_outer_feedback": True,
        "cutoff_plans": [
            _cutoff("A", "development", "2025-01-01", "2025-01-18", "2025-01-21"),
            _cutoff("B", "development", "2025-02-01", "2025-02-18", "2025-02-21"),
            _cutoff("C", "validation", "2025-03-01", "2025-03-18", "2025-03-21"),
            _cutoff("D", "validation", "2025-04-01", "2025-04-18", "2025-04-21"),
        ],
    }


def _report(tmp_path: Path) -> Path:
    folds = []
    for index, (train_start, train_end, test_start, test_end) in enumerate(
        [
            ("2021-06-29", "2024-06-28", "2024-07-14", "2025-01-13"),
            ("2021-12-30", "2024-12-29", "2025-01-14", "2025-07-13"),
            ("2022-06-29", "2025-06-28", "2025-07-14", "2026-01-13"),
            ("2022-12-30", "2025-12-29", "2026-01-14", "2026-07-13"),
        ],
        start=1,
    ):
        fold_id = f"fold-{index:02d}"
        folds.append(
            {
                "fold": {
                    "fold_id": fold_id,
                    "train_start": train_start,
                    "train_end": train_end,
                    "test_start": test_start,
                    "test_end": test_end,
                    "embargo_days": 15,
                },
                "state_path": str(tmp_path / fold_id / "nested-state.json"),
                "records": [
                    {
                        "train_result_path": str(tmp_path / fold_id / "train-result.json"),
                        "outer_result_path": str(tmp_path / fold_id / "outer-result.json"),
                    }
                ],
            }
        )
    path = tmp_path / "nested-evidence-report.json"
    path.write_text(json.dumps({"status": "complete", "campaign_id": "nested-campaign", "fold_results": folds}), encoding="utf-8")
    return path


def test_create_and_reload_valid_protocol(tmp_path: Path) -> None:
    path = tmp_path / "protocol.json"
    created = create_level_c_protocol(path, _protocol())

    assert created["protocol_manifest_id"] == canonical_sha256(
        {key: value for key, value in created.items() if key != "protocol_manifest_id"}
    )
    assert load_level_c_protocol(path) == created


def test_initial_cutoffs_are_derived_from_nested_report(tmp_path: Path) -> None:
    plans = build_initial_four_cutoff_plans(_report(tmp_path), global_seed=41)

    assert [plan["cutoff_key"] for plan in plans] == ["A", "B", "C", "D"]
    assert [plan["role"] for plan in plans] == ["development", "development", "validation", "validation"]
    assert plans[0]["selection_end"] == "2024-06-29T00:00:00Z"
    assert plans[0]["embargo_end"] == "2024-07-14T00:00:00Z"
    assert plans[0]["outer_test_end"] == "2025-01-14T00:00:00Z"
    assert [plan["seed"] for plan in plans] == [41, 42, 43, 44]
    assert len({plan["playhand_campaign_id"] for plan in plans}) == 4
    assert set(plans[0]["expected_artifact_locations"]) == {
        "atlas_run",
        "playhand_campaign",
        "frozen_cohort",
        "campaign_receipt",
    }
    assert plans[0]["expected_artifact_locations"]["atlas_run"] == (
        f"derived/atlas-runs/{plans[0]['atlas_run_id']}"
    )
    assert all(
        "nested" not in location
        for plan in plans
        for location in plan["expected_artifact_locations"].values()
    )
    assert all(
        location.startswith("derived/")
        for plan in plans
        for location in plan["expected_artifact_locations"].values()
    )


def test_cutoff_derivation_ignores_legacy_outcomes_but_binds_geometry(tmp_path: Path) -> None:
    report = _report(tmp_path)
    baseline = build_initial_four_cutoff_plans(report, global_seed=41)
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["fold_results"][0]["records"][0]["outer_result_path"] = "C:/changed/result.json"
    payload["fold_results"][0]["records"][0]["score"] = -999
    report.write_text(json.dumps(payload), encoding="utf-8")
    assert build_initial_four_cutoff_plans(report, global_seed=41) == baseline

    payload["fold_results"][0]["fold"]["train_start"] = "2021-06-28"
    report.write_text(json.dumps(payload), encoding="utf-8")
    geometry_changed = build_initial_four_cutoff_plans(report, global_seed=41)
    assert geometry_changed[0]["geometry_sha256"] != baseline[0]["geometry_sha256"]
    assert geometry_changed[0]["atlas_run_id"] != baseline[0]["atlas_run_id"]


def test_rejects_duplicate_cutoffs_and_bad_geometry(tmp_path: Path) -> None:
    duplicate = _protocol()
    duplicate["cutoff_plans"][1]["atlas_run_id"] = "atlas-a"
    with pytest.raises(LevelCProtocolError, match="unique"):
        create_level_c_protocol(tmp_path / "duplicate.json", duplicate)

    overlapping = _protocol()
    overlapping["cutoff_plans"][0]["outer_test_start"] = "2025-01-10T00:00:00Z"
    _rehash_cutoff(overlapping["cutoff_plans"][0])
    with pytest.raises(LevelCProtocolError, match="overlap|unordered"):
        create_level_c_protocol(tmp_path / "overlap.json", overlapping)

    unordered = _protocol()
    unordered["cutoff_plans"][0]["outer_test_start"] = "2025-03-18T00:00:00Z"
    unordered["cutoff_plans"][0]["outer_test_end"] = "2025-03-21T00:00:00Z"
    _rehash_cutoff(unordered["cutoff_plans"][0])
    with pytest.raises(LevelCProtocolError, match="ordered by outer-test"):
        create_level_c_protocol(tmp_path / "unordered.json", unordered)


def test_rejects_role_order_mutable_priors_and_future_coverage(tmp_path: Path) -> None:
    wrong_roles = _protocol()
    wrong_roles["cutoff_plans"][1]["role"] = "validation"
    wrong_roles["cutoff_plans"][2]["role"] = "development"
    with pytest.raises(LevelCProtocolError, match="roles must be"):
        create_level_c_protocol(tmp_path / "roles.json", wrong_roles)

    incomplete = _protocol()
    incomplete["cutoff_plans"] = incomplete["cutoff_plans"][:3]
    with pytest.raises(LevelCProtocolError, match="at least 4|A/B/C/D"):
        create_level_c_protocol(tmp_path / "incomplete.json", incomplete)

    mutable_prior = _protocol()
    mutable_prior["cutoff_plans"][0]["expected_artifact_locations"]["global_prior"] = "C:/priors/latest.json"
    with pytest.raises(LevelCProtocolError, match="mutable/global-prior"):
        create_level_c_protocol(tmp_path / "prior.json", mutable_prior)

    future = _protocol()
    future["source_coverage_end"] = "2025-04-19T00:00:00Z"
    with pytest.raises(LevelCProtocolError, match="source coverage"):
        create_level_c_protocol(tmp_path / "future.json", future)


def test_rejects_self_consistent_tampering_and_is_create_only(tmp_path: Path) -> None:
    path = tmp_path / "protocol.json"
    created = create_level_c_protocol(path, _protocol())
    forged = deepcopy(created)
    forged["no_outer_feedback"] = False
    identity = {key: value for key, value in forged.items() if key != "protocol_manifest_id"}
    forged["protocol_manifest_id"] = canonical_sha256(identity)
    path.write_text(json.dumps(forged), encoding="utf-8")

    with pytest.raises(LevelCProtocolError, match="True|no_outer_feedback"):
        load_level_c_protocol(path)

    with pytest.raises(LevelCProtocolError, match="already exists"):
        create_level_c_protocol(path, _protocol())


def test_external_authority_rejects_schema_valid_rehashed_protocol_mutation(
    tmp_path: Path,
) -> None:
    generation_path = tmp_path / "generation-manifest.json"
    generation_path.write_text(json.dumps({"generation": "generation-001"}), encoding="utf-8")
    protocol_payload = _protocol()
    protocol_payload["research_generation_manifest_sha256"] = (
        "sha256:" + hashlib.sha256(generation_path.read_bytes()).hexdigest()
    )
    protocol_path = tmp_path / "protocol.json"
    authority_path = tmp_path / "protocol-authority.json"
    created = create_level_c_protocol(protocol_path, protocol_payload)
    authority = create_level_c_protocol_authority(
        authority_path,
        generation_manifest_path=generation_path,
        protocol_path=protocol_path,
    )

    forged = deepcopy(created)
    forged["cutoff_plans"][0]["seed"] += 1000
    identity = {key: value for key, value in forged.items() if key != "protocol_manifest_id"}
    forged["protocol_manifest_id"] = canonical_sha256(identity)
    protocol_path.write_text(json.dumps(forged), encoding="utf-8")

    assert load_level_c_protocol(protocol_path) == forged
    with pytest.raises(LevelCProtocolError, match="authority"):
        load_level_c_protocol(
            protocol_path, expected_manifest_id=authority["protocol_manifest_id"]
        )
    with pytest.raises(LevelCProtocolError, match="authority"):
        load_level_c_protocol_authority(
            authority_path,
            generation_manifest_path=generation_path,
            protocol_path=protocol_path,
        )
