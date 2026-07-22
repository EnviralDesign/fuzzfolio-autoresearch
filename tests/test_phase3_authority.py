from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from autoresearch import phase3_authority as phase3_authority_module

from autoresearch.phase3_authority import (
    PHASE3_AUTHORITY_FILENAME,
    Phase3AuthorityError,
    build_phase3_authority_payload,
    create_phase3_authority,
    resolve_phase3_playhand_runtime_arguments,
    validate_phase3_authority,
)
from autoresearch.recipe_priors import build_campaign_policy_manifest
from autoresearch.play_hand import DEFAULT_INSTRUMENT_POOL


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _policy() -> dict[str, object]:
    return build_campaign_policy_manifest(
        lane_fractions={"guided": 0.60, "uncertain": 0.25, "wild": 0.15},
        lane_eligible_menus={
            "guided": {
                "recipe_sources": ["curated_recipe_prior", "discovery_recipe_validation"],
                "slot_sampling_lanes": ["high_prior", "medium_prior"],
                "pair_sampling_lanes": ["positive_pair"],
                "allow_generation_eligible_fallback": False,
            },
            "uncertain": {
                "recipe_sources": ["curated_recipe_prior", "discovery_recipe_validation"],
                "slot_sampling_lanes": ["uncertain_prior"],
                "pair_sampling_lanes": ["near_miss_pair"],
                "allow_generation_eligible_fallback": False,
            },
            "wild": {
                "recipe_sources": ["curated_recipe_prior"],
                "slot_sampling_lanes": ["wild_exploration"],
                "pair_sampling_lanes": ["low_pair"],
                "allow_generation_eligible_fallback": True,
            },
        },
        diversity_max_shares={
            "family": 0.05,
            "recipe": 0.30,
            "instrument": 0.10,
            "timeframe": 0.60,
            "indicator": 0.15,
        },
        source_atlas_generation="level-c-v3-phase2-rich-priors",
        source_atlas_run_sequence=4,
    )


def _rewrite_capsule_manifest(capsule: Path) -> None:
    files = []
    for path in sorted(capsule.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or path.name == "phase2-atlas-authority-capsule-manifest.json":
            continue
        files.append(
            {
                "capsule_relative_path": path.relative_to(capsule).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size_bytes": path.stat().st_size,
            }
        )
    manifest = {
        "schema_version": "phase2_atlas_authority_capsule_v1",
        "files": files,
    }
    manifest["capsule_identity_sha256"] = hashlib.sha256(
        (json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode("utf-8")
    ).hexdigest()
    _write_json(capsule / "phase2-atlas-authority-capsule-manifest.json", manifest)


def _seed_plan(cutoff: str, *, candidate: str, marker: str) -> dict[str, object]:
    return {
        "schema_version": "play_hand_seed_plan_v1",
        "historical_lineage": {
            "execution_plan_id": f"plan-{cutoff}",
            "lake_manifest_sha256": "sha256:" + "a" * 64,
            "source_snapshot_sha256": "sha256:" + "b" * 64,
            "universe_id": "fuzzfolio-development-darwinex-zero",
            "universe_manifest_sha256": "sha256:" + "c" * 64,
            "research_generation_id": "level-c-v3-phase2-rich-priors",
        },
        "negative_pairs": [
            {
                "recipe": "core",
                "ordered_pair_id": "ALPHA->BETA",
                "probe_timeframe": "M5",
                "negative_reason_category": "retention_failed",
                "expires_after_atlas_runs": 3,
            }
        ],
        "recipes": {
            "core": {
                "source": "curated_recipe_prior",
                "slot_menus": {
                    "trigger": [
                        {
                            "indicator_id": candidate,
                            "source": "curated_recipe_prior",
                            "sampling_lane": "medium_prior",
                            "sampling_weight": 10.0 if cutoff == "A" else 20.0,
                        }
                    ]
                },
                "pair_menu": [
                    {
                        "canonical_pair_family_id": f"family-{marker}",
                        "ordered_pair_id": "ALPHA->BETA",
                        "probe_timeframe": "M5",
                        "pair_sampling_lane": "positive_pair",
                        "pair_sampling_weight": 10.0 if cutoff == "A" else 20.0,
                    }
                ],
                "recommended_templates": [{"name": "core", "slots": ["trigger"]}],
            }
        },
    }


def _summary(marker: str) -> dict[str, object]:
    return {
        "result_counts": {"marker": marker, "pair_prior_rows": 1},
        "discovered_recipe_validation": {"marker": marker, "source_rows": 1},
        "negative_priors": {"marker": marker, "negative_pair_rows": 1},
        "top_pairs": [{"candidate_identity": f"do-not-use-{marker}"}],
    }


def _make_capsule(tmp_path: Path) -> tuple[Path, Path]:
    capsule = tmp_path / "capsule"
    policy_path = tmp_path / "policy.json"
    _write_json(policy_path, _policy())
    for cutoff in ("A", "B", "C", "D"):
        root = capsule / "atlas-roots" / cutoff
        seed = _seed_plan(cutoff, candidate=f"INDICATOR_{cutoff}", marker=cutoff)
        _write_json(root / "atlas-lab-summary.json", {"status": "completed"})
        _write_json(root / "recipe-priors" / "play-hand-seed-plan.json", seed)
        _write_json(root / "recipe-priors" / "recipe-priors-summary.json", _summary(cutoff))
        _write_json(
            capsule / "level-c-control" / f"execution-plan-{cutoff}.json",
            {
                "plan_id": f"plan-{cutoff}",
                "bound_contract": {
                    "worker_image": "image",
                    "worker_contract_sha256": "sha256:" + "e" * 64,
                },
            },
        )
    _rewrite_capsule_manifest(capsule)
    return capsule, policy_path


def test_authority_uses_a_b_for_menus_and_c_d_only_for_aggregate_diagnostics(tmp_path: Path) -> None:
    capsule, policy_path = _make_capsule(tmp_path)
    authority, seed_plan, report = build_phase3_authority_payload(
        phase2_capsule_root=capsule,
        policy_manifest_path=policy_path,
        authority_id="phase3-test",
        target_runs=100,
    )

    rows = seed_plan["recipes"]["core"]["slot_menus"]["trigger"]
    assert {row["indicator_id"] for row in rows} == {"INDICATOR_A", "INDICATOR_B"}
    assert authority["development_construction"]["allowed_cutoffs"] == ["A", "B"]
    assert authority["validation_diagnostics"]["allowed_cutoffs"] == ["C", "D"]
    assert authority["validation_diagnostics"]["candidate_feedback"] == "forbidden"
    assert "top_pairs" not in authority["validation_diagnostics"]["diagnostics"]["C"]["aggregate"]
    assert report["reserved_tail"]["start"] == "2026-01-14T00:00:00Z"
    assert seed_plan["schema_version"] == "play_hand_seed_plan_v2"
    assert seed_plan["campaign_policy_sha256"] == authority["campaign_policy"]["manifest_sha256"]
    runtime = authority["playhand_runtime_arguments"]
    assert runtime["current_atlas_generation"] == "level-c-v3-phase2-rich-priors"
    assert runtime["current_atlas_run_sequence"] == 4
    assert runtime["target_runs"] == 100
    assert runtime["instrument_pool"] == list(DEFAULT_INSTRUMENT_POOL)
    assert runtime["campaign_policy_manifest_sha256"] == authority["campaign_policy"]["manifest_sha256"]
    assert runtime["campaign_policy_source_file_sha256"] == authority["campaign_policy"]["source_file_sha256"]
    assert runtime["operator_launch_worker_image"] == "image"
    assert "worker_image" not in runtime
    enforcement = authority["worker_execution_enforcement"]
    assert authority["bound_contract"]["operator_launch_worker_image"] == "image"
    assert "worker_image" not in authority["bound_contract"]
    assert enforcement["operator_launch_worker_image"] == "image"
    assert enforcement["worker_image_gateway_claim_enforced"] is False
    assert enforcement["gateway_enforced_worker_contract_sha256"] == "sha256:" + "e" * 64


def test_full_capsule_verification_rejects_ignored_validation_candidate_mutation(tmp_path: Path) -> None:
    capsule, policy_path = _make_capsule(tmp_path)
    c_seed_path = capsule / "atlas-roots" / "C" / "recipe-priors" / "play-hand-seed-plan.json"
    c_seed = json.loads(c_seed_path.read_text(encoding="utf-8"))
    c_seed["recipes"]["core"]["slot_menus"]["trigger"][0]["indicator_id"] = "C_MUTATION"
    _write_json(c_seed_path, c_seed)
    with pytest.raises(Phase3AuthorityError, match="capsule verification failed"):
        build_phase3_authority_payload(
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
            authority_id="phase3-test",
            target_runs=10,
        )


def test_create_and_audit_fail_closed_on_source_policy_and_seed_drift(tmp_path: Path) -> None:
    capsule, policy_path = _make_capsule(tmp_path)
    result = create_phase3_authority(
        phase2_capsule_root=capsule,
        policy_manifest_path=policy_path,
        authority_id="phase3-test",
        target_runs=12,
        out_dir=tmp_path / "authority",
    )
    assert result.authority_path.name == PHASE3_AUTHORITY_FILENAME
    assert validate_phase3_authority(
        authority_path=result.authority_path,
        phase2_capsule_root=capsule,
        policy_manifest_path=policy_path,
    )["status"] == "valid"

    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    policy["lanes"]["guided"]["fraction"] = 0.59
    _write_json(policy_path, policy)
    with pytest.raises(Phase3AuthorityError, match="campaign policy"):
        validate_phase3_authority(
            authority_path=result.authority_path,
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
        )
    _write_json(policy_path, _policy())
    source_seed_path = capsule / "atlas-roots" / "A" / "recipe-priors" / "play-hand-seed-plan.json"
    source_seed = json.loads(source_seed_path.read_text(encoding="utf-8"))
    source_seed["historical_lineage"]["lake_manifest_sha256"] = "sha256:" + "9" * 64
    _write_json(source_seed_path, source_seed)
    _rewrite_capsule_manifest(capsule)
    with pytest.raises(Phase3AuthorityError, match="source identities differ"):
        validate_phase3_authority(
            authority_path=result.authority_path,
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
        )

    source_seed["historical_lineage"]["lake_manifest_sha256"] = "sha256:" + "a" * 64
    _write_json(source_seed_path, source_seed)
    _rewrite_capsule_manifest(capsule)
    seed_path = result.seed_plan_path
    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    seed["recipes"]["core"]["source"] = "drift"
    _write_json(seed_path, seed)
    with pytest.raises(Phase3AuthorityError, match="seed plan digest drift"):
        validate_phase3_authority(
            authority_path=result.authority_path,
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
        )


def test_phase3_can_rebind_execution_without_rewriting_phase2_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capsule, policy_path = _make_capsule(tmp_path)
    current_contract = "sha256:" + "f" * 64

    def _rebind(phase2_contract, *, worker_image, trading_dashboard_root):
        rebound = dict(phase2_contract)
        rebound.update(
            {
                "worker_contract_sha256": current_contract,
                "worker_image": worker_image,
                "profile_model_source_root": str(trading_dashboard_root),
            }
        )
        return rebound

    monkeypatch.setattr(phase3_authority_module, "_live_execution_contract", _rebind)
    result = create_phase3_authority(
        phase2_capsule_root=capsule,
        policy_manifest_path=policy_path,
        authority_id="phase3-current-execution",
        target_runs=12,
        out_dir=tmp_path / "authority-rebound",
        execution_worker_image="registry/worker:sha-current",
        trading_dashboard_root=tmp_path / "dashboard",
    )

    authority = result.authority
    assert authority["bound_contract"]["worker_contract_sha256"] == current_contract
    assert authority["playhand_runtime_arguments"]["worker_contract_hash"] == current_contract
    assert authority["execution_rebinding"]["operator_launch_worker_image"] == (
        "registry/worker:sha-current"
    )
    assert authority["execution_rebinding"]["phase2_selection_contract_sha256"].startswith(
        "sha256:"
    )
    assert validate_phase3_authority(
        authority_path=result.authority_path,
        phase2_capsule_root=capsule,
        policy_manifest_path=policy_path,
    )["status"] == "valid"

def test_rejects_nonfinite_target_and_inconsistent_phase2_contract(tmp_path: Path) -> None:
    capsule, policy_path = _make_capsule(tmp_path)
    with pytest.raises(Phase3AuthorityError, match="positive integer"):
        build_phase3_authority_payload(
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
            authority_id="phase3-test",
            target_runs=0,
        )

    plan_path = capsule / "level-c-control" / "execution-plan-D.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["bound_contract"]["worker_image"] = "different"
    _write_json(plan_path, plan)
    _rewrite_capsule_manifest(capsule)
    with pytest.raises(Phase3AuthorityError, match="one exact runtime authority contract"):
        build_phase3_authority_payload(
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
            authority_id="phase3-test",
            target_runs=5,
        )


def test_formal_v2_runtime_is_plan_bound_and_rejects_generation_sequence_drift(tmp_path: Path) -> None:
    capsule, policy_path = _make_capsule(tmp_path)
    result = create_phase3_authority(
        phase2_capsule_root=capsule,
        policy_manifest_path=policy_path,
        authority_id="phase3-test",
        target_runs=24,
        out_dir=tmp_path / "authority",
    )

    runtime = resolve_phase3_playhand_runtime_arguments(
        authority_path=result.authority_path,
        phase2_capsule_root=capsule,
        policy_manifest_path=policy_path,
    )
    assert runtime["current_atlas_generation"] == "level-c-v3-phase2-rich-priors"
    assert runtime["current_atlas_run_sequence"] == 4
    assert runtime["seed_plan_path"] == str(result.seed_plan_path.resolve())
    assert runtime["as_of_date"] == "2026-01-14T00:00:00Z"

    with pytest.raises(Phase3AuthorityError, match="current_atlas_run_sequence"):
        resolve_phase3_playhand_runtime_arguments(
            authority_path=result.authority_path,
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
            overrides={"current_atlas_run_sequence": 5},
        )
    with pytest.raises(Phase3AuthorityError, match="as_of_date"):
        resolve_phase3_playhand_runtime_arguments(
            authority_path=result.authority_path,
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
            overrides={"as_of_date": "2026-07-14T00:00:00Z"},
        )

    authority = json.loads(result.authority_path.read_text(encoding="utf-8"))
    authority["playhand_runtime_arguments"]["current_atlas_run_sequence"] = 5
    _write_json(result.authority_path, authority)
    with pytest.raises(Phase3AuthorityError, match="differs from its rederived inputs"):
        validate_phase3_authority(
            authority_path=result.authority_path,
            phase2_capsule_root=capsule,
            policy_manifest_path=policy_path,
        )
