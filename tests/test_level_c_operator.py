from __future__ import annotations

import hashlib
import inspect
import json
from copy import deepcopy
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.generation_archive import GENERATION_SCHEMA_NAME, GENERATION_SCHEMA_VERSION
from autoresearch.instrument_universe import universe_provenance
from autoresearch.level_c_operator import (
    LevelCOperatorError,
    build_level_c_execution_plan,
    create_level_c_execution_plan,
    executor_arguments_from_plan,
    load_level_c_execution_plan,
    validate_level_c_execution_plan,
)
from autoresearch.level_c_protocol import (
    LevelCProtocolError,
    create_level_c_protocol,
    create_level_c_protocol_authority,
)
from autoresearch.config import load_config
from autoresearch.runtime_policy_lock import build_runtime_policy_lock, policy_lock_provenance


def _hash(character: str) -> str:
    return "sha256:" + character * 64


def _cutoff(key: str, role: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "cutoff_key": key,
        "role": role,
        "selection_start": f"2025-0{ord(key) - 64}-01T00:00:00Z",
        "selection_end": f"2025-0{ord(key) - 64}-03T00:00:00Z",
        "training_start": f"2025-0{ord(key) - 64}-01T00:00:00Z",
        "training_end": f"2025-0{ord(key) - 64}-03T00:00:00Z",
        "embargo_start": f"2025-0{ord(key) - 64}-03T00:00:00Z",
        "embargo_end": f"2025-0{ord(key) - 64}-18T00:00:00Z",
        "embargo_days": 15,
        "outer_test_start": f"2025-0{ord(key) - 64}-18T00:00:00Z",
        "outer_test_end": f"2025-0{ord(key) - 64}-21T00:00:00Z",
        "atlas_run_id": f"atlas-{key.lower()}",
        "playhand_campaign_id": f"playhand-{key.lower()}",
        "cohort_id": f"cohort-{key.lower()}",
        "seed": 100 + ord(key),
        "expected_artifact_locations": {
            "atlas_run": f"derived/atlas/{key.lower()}",
            "playhand_campaign": f"derived/playhand/{key.lower()}",
        },
    }
    payload["geometry_sha256"] = canonical_sha256(
        {field: payload[field] for field in (
            "selection_start", "selection_end", "training_start", "training_end",
            "embargo_start", "embargo_end", "embargo_days", "outer_test_start", "outer_test_end",
        )}
    )
    return payload


def _generation_payload(root: Path, provenance: dict[str, str]) -> dict[str, object]:
    return {
        "schema_name": GENERATION_SCHEMA_NAME,
        "schema_version": GENERATION_SCHEMA_VERSION,
        "new_generation_id": "generation-001",
        "created_at": "2025-01-01T00:00:00Z",
        "archive_linkage": {
            "archive_id": "archive-001",
            "archive_manifest_path": str(root.parent / "archive-manifest.json"),
            "archived_runs_root": str(root.parent / "archived-runs"),
            "archive_prepared_at": "2025-01-01T00:00:00Z",
        },
        "source_runs_root": str(root.parent / "archived-runs"),
        "destination_runs_root": str(root.resolve()),
        "archived_inventory": {},
        "restore_instructions": [],
        "provenance": provenance,
    }


def _provenance() -> dict[str, str]:
    live = universe_provenance()
    policy = policy_lock_provenance(
        build_runtime_policy_lock(load_config(), worker_contract_sha256=_hash("c"))
    )
    return {
        "lake_semantic_sha256": _hash("a"),
        "source_snapshot_sha256": _hash("b"),
        "universe_id": str(live["universe_id"]),
        "universe_manifest_sha256": str(live["universe_hash"]),
        "worker_contract_id": "worker-contract-v1",
        "worker_contract_sha256": _hash("c"),
        "worker_image": "worker-image-v1",
        **policy,
    }


def _protocol(manifest_sha256: str, provenance: dict[str, str]) -> dict[str, object]:
    return {
        "protocol_name": "level-c-operator-test",
        "protocol_version": "v1",
        "status": "frozen",
        "research_generation_id": "generation-001",
        "research_generation_manifest_sha256": manifest_sha256,
        "source_coverage_end": "2025-12-31T00:00:00Z",
        **provenance,
        "global_seed": 77,
        "no_global_priors": True,
        "no_outer_feedback": True,
        "cutoff_plans": [
            _cutoff("A", "development"), _cutoff("B", "development"),
            _cutoff("C", "validation"), _cutoff("D", "validation"),
        ],
    }


def _bound_sources(tmp_path: Path) -> tuple[Path, Path, Path, dict[str, object]]:
    root = tmp_path / "runs"
    root.mkdir(parents=True)
    generation = _generation_payload(root, _provenance())
    raw = json.dumps(generation, sort_keys=True).encode("utf-8")
    (root / "generation-manifest.json").write_bytes(raw)
    protocol = _protocol("sha256:" + hashlib.sha256(raw).hexdigest(), _provenance())
    protocol_path = tmp_path / "level-c-protocol.json"
    create_level_c_protocol(protocol_path, protocol)
    authority_path = tmp_path / "level-c-protocol-authority.json"
    create_level_c_protocol_authority(
        authority_path,
        generation_manifest_path=root / "generation-manifest.json",
        protocol_path=protocol_path,
    )
    return root, protocol_path, authority_path, generation


def _rewrite_bound_protocol(
    root: Path,
    path: Path,
    authority_path: Path,
    generation: dict[str, object],
    mutate,
) -> None:
    raw = (root / "generation-manifest.json").read_bytes()
    protocol = _protocol("sha256:" + hashlib.sha256(raw).hexdigest(), dict(generation["provenance"]))
    mutate(protocol)
    path.unlink()
    create_level_c_protocol(path, protocol)
    authority_path.unlink()
    create_level_c_protocol_authority(
        authority_path,
        generation_manifest_path=root / "generation-manifest.json",
        protocol_path=path,
    )


def test_builds_exact_declarative_arguments_from_one_cutoff(tmp_path: Path) -> None:
    root, protocol_path, authority_path, _ = _bound_sources(tmp_path)
    plan = build_level_c_execution_plan(root, protocol_path, authority_path, "C")

    assert plan["execution_mode"] == "declarative-only"
    assert plan["cutoff"]["atlas_run_id"] == "atlas-c"
    assert plan["cutoff"]["playhand_campaign_id"] == "playhand-c"
    for arguments, identifier, value in (
        (plan["atlas_arguments"], "run_id", "atlas-c"),
        (plan["playhand_arguments"], "campaign_id", "playhand-c"),
    ):
        assert arguments["as_of_date"] == "2025-03-03T00:00:00Z"
        assert arguments[identifier] == value
        assert arguments["research_generation_id"] == "generation-001"
        assert arguments["cutoff_key"] == "C"
        assert arguments["lake_manifest_sha256"] == _hash("a")
        assert arguments["worker_contract_hash"] == _hash("c")
    assert plan["playhand_arguments"]["seed"] == 167
    assert plan["playhand_arguments"]["campaign_mode"] == "finite"
    assert plan["playhand_arguments"]["strict_scoring"] is True
    assert plan["atlas_arguments"]["signal_atlas_executor"] == "gateway"
    assert plan["atlas_arguments"]["publish"] is False
    for arguments in (plan["atlas_arguments"], plan["playhand_arguments"]):
        assert arguments["source_snapshot_sha256"] == _hash("b")
        assert arguments["universe_id"] == universe_provenance()["universe_id"]
        assert arguments["universe_manifest_sha256"] == universe_provenance()["universe_hash"]
    assert plan["bound_contract"]["runtime_policy_lock"]["policy_lock_sha256"].startswith(
        "sha256:"
    )


def test_one_authoritative_plan_drives_atlas_and_playhand_executors(tmp_path: Path) -> None:
    root, protocol_path, authority_path, _ = _bound_sources(tmp_path)
    plan = build_level_c_execution_plan(root, protocol_path, authority_path, "C")
    plan_path = tmp_path / "execution-plan.json"
    create_level_c_execution_plan(plan_path, plan)

    atlas_args, atlas_plan = executor_arguments_from_plan(plan_path, executor="atlas")
    assert atlas_args["run_id"] == "atlas-c"
    assert atlas_args["execution_plan_id"] == atlas_plan["plan_id"]

    seed_path = Path(plan["playhand_arguments"]["seed_plan_path"])
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    seed_path.write_text('{"recipes":{},"sampling_policy":{}}', encoding="utf-8")
    seed_sha256 = "sha256:" + hashlib.sha256(seed_path.read_bytes()).hexdigest()
    (seed_path.parent / "level-c-lineage.json").write_text(
        json.dumps(
            {"artifact_sha256": {"play-hand-seed-plan.json": seed_sha256}}
        ),
        encoding="utf-8",
    )

    playhand_args, playhand_plan = executor_arguments_from_plan(
        plan_path, executor="playhand"
    )
    assert playhand_args["campaign_id"] == "playhand-c"
    assert playhand_args["expected_seed_plan_sha256"] == seed_sha256
    assert playhand_args["execution_plan_id"] == playhand_plan["plan_id"]
    assert plan["bound_contract"]["no_global_priors"] is True
    assert plan["bound_contract"]["no_outer_feedback"] is True
    assert plan["cutoff"]["geometry"]["embargo_days"] == 15
    assert plan["cutoff"]["geometry"]["outer_test_end"] == "2025-03-21T00:00:00Z"
    assert plan["playhand_deferred_binding"]["required_before_execution"] is True
    assert plan["expected_artifacts"]["atlas_run"]["relative_path"] == "derived/atlas/c"
    assert Path(plan["expected_artifacts"]["atlas_run"]["resolved_path"]).is_relative_to(root)
    assert set(inspect.signature(build_level_c_execution_plan).parameters) == {
        "active_runs_root", "protocol_path", "authority_path", "cutoff_key"
    }


@pytest.mark.parametrize("field", [
    "lake_semantic_sha256", "source_snapshot_sha256", "universe_id", "universe_manifest_sha256",
    "worker_contract_id", "worker_contract_sha256", "worker_image", "engine_id", "engine_sha256",
    "scoring_policy_id", "scoring_policy_sha256", "cost_policy_id", "cost_policy_sha256",
])
def test_rejects_every_generation_provenance_identity_mismatch(tmp_path: Path, field: str) -> None:
    root, protocol_path, authority_path, generation = _bound_sources(tmp_path)
    expected = _provenance()
    changed = _hash("9") if field.endswith("_sha256") else f"different-{field}"
    generation["provenance"][field] = changed
    (root / "generation-manifest.json").write_text(json.dumps(generation, sort_keys=True), encoding="utf-8")
    _rewrite_bound_protocol(
        root, protocol_path, authority_path, generation,
        lambda protocol: protocol.__setitem__(field, expected[field]),
    )

    with pytest.raises(LevelCOperatorError, match=field):
        build_level_c_execution_plan(root, protocol_path, authority_path, "A")


def test_rejects_wrong_active_root_generation_and_raw_manifest_identity(tmp_path: Path) -> None:
    root, protocol_path, authority_path, generation = _bound_sources(tmp_path)
    generation["destination_runs_root"] = str(tmp_path / "other-runs")
    (root / "generation-manifest.json").write_text(json.dumps(generation), encoding="utf-8")
    with pytest.raises(LevelCOperatorError, match="destination_runs_root"):
        build_level_c_execution_plan(root, protocol_path, authority_path, "A")

    root, protocol_path, authority_path, generation = _bound_sources(tmp_path / "second")
    generation["new_generation_id"] = "generation-002"
    (root / "generation-manifest.json").write_text(json.dumps(generation), encoding="utf-8")
    with pytest.raises(LevelCOperatorError, match="authority"):
        build_level_c_execution_plan(root, protocol_path, authority_path, "A")

    root, protocol_path, authority_path, _ = _bound_sources(tmp_path / "third")
    generation_path = root / "generation-manifest.json"
    generation_path.write_bytes(generation_path.read_bytes() + b"\n")
    with pytest.raises(LevelCOperatorError, match="authority"):
        build_level_c_execution_plan(root, protocol_path, authority_path, "A")


def test_rejects_protocol_that_agrees_with_generation_but_not_live_universe(tmp_path: Path) -> None:
    root, protocol_path, authority_path, generation = _bound_sources(tmp_path)
    generation["provenance"]["universe_id"] = "not-live-universe"
    (root / "generation-manifest.json").write_text(json.dumps(generation, sort_keys=True), encoding="utf-8")
    _rewrite_bound_protocol(root, protocol_path, authority_path, generation, lambda _: None)

    with pytest.raises(LevelCOperatorError, match="live universe"):
        build_level_c_execution_plan(root, protocol_path, authority_path, "A")


def test_rejects_expected_artifact_escape_and_invalid_cutoff(tmp_path: Path) -> None:
    root, protocol_path, authority_path, generation = _bound_sources(tmp_path)

    def escape(protocol: dict[str, object]) -> None:
        protocol["cutoff_plans"][0]["expected_artifact_locations"]["atlas_run"] = str(tmp_path / "outside")

    with pytest.raises(LevelCProtocolError, match="relative to the active runs root"):
        _rewrite_bound_protocol(root, protocol_path, authority_path, generation, escape)
    root, protocol_path, authority_path, _ = _bound_sources(tmp_path / "valid")
    with pytest.raises(LevelCOperatorError, match="exactly one"):
        build_level_c_execution_plan(root, protocol_path, authority_path, "A,B")


def test_validator_detects_plain_and_rehashed_plan_tampering(tmp_path: Path) -> None:
    root, protocol_path, authority_path, _ = _bound_sources(tmp_path)
    plan = build_level_c_execution_plan(root, protocol_path, authority_path, "B")
    altered = deepcopy(plan)
    altered["atlas_arguments"]["as_of_date"] = "2020-01-01T00:00:00Z"
    with pytest.raises(LevelCOperatorError, match="hash mismatch"):
        validate_level_c_execution_plan(altered)
    altered["plan_id"] = canonical_sha256({key: value for key, value in altered.items() if key != "plan_id"})
    with pytest.raises(LevelCOperatorError, match="authoritative sources"):
        validate_level_c_execution_plan(
            altered,
            active_runs_root=root,
            protocol_path=protocol_path,
            authority_path=authority_path,
        )


def test_create_only_writer_and_source_validating_loader(tmp_path: Path) -> None:
    root, protocol_path, authority_path, _ = _bound_sources(tmp_path)
    plan = build_level_c_execution_plan(root, protocol_path, authority_path, "D")
    target = tmp_path / "plans" / "level-c-d.json"

    assert create_level_c_execution_plan(target, plan) == plan
    assert load_level_c_execution_plan(
        target,
        active_runs_root=root,
        protocol_path=protocol_path,
        authority_path=authority_path,
    ) == plan
    with pytest.raises(LevelCOperatorError, match="already exists"):
        create_level_c_execution_plan(target, plan)
