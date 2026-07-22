from __future__ import annotations

import copy
import json
import random
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from autoresearch import play_hand_lab as lab
from autoresearch.instrument_universe import universe_provenance
from autoresearch.lake_window import LakeWindowBinding
from autoresearch.recipe_priors import build_campaign_policy_manifest


def _profile_payload() -> dict:
    return {
        "format": "fuzzfolio.scoring-profile",
        "formatVersion": 1,
        "profile": {
            "name": "Lab Smoke",
            "description": "Test profile",
            "directionMode": "both",
            "isActive": False,
            "version": "v1",
            "instruments": ["EURUSD"],
            "notificationThreshold": 80,
            "indicators": [
                {
                    "meta": {"id": "RSI", "instanceId": "test-rsi"},
                    "config": {
                        "label": "RSI",
                        "timeframe": "M5",
                        "lookbackBars": 1,
                        "isActive": True,
                        "weight": 1.0,
                        "talibConfig": [{"name": "timeperiod", "value": 14}],
                    },
                }
            ],
        },
    }


def _test_config(tmp_path: Path) -> SimpleNamespace:
    runs_root = tmp_path / "runs"
    return SimpleNamespace(
        repo_root=tmp_path,
        runs_root=runs_root,
        derived_root=runs_root / "derived",
        fuzzfolio=SimpleNamespace(workspace_root=None),
        research=SimpleNamespace(plot_lower_is_better=False),
    )


def _campaign_ctx(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        run_id="campaign-1",
        run_dir=tmp_path,
        events_path=tmp_path / "events.jsonl",
        io_lock=threading.RLock(),
    )


def _historical_seed_plan() -> dict:
    return {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "pair": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "ADX",
                        "pair_sampling_weight": 1.0,
                    }
                ],
                "slot_menus": {},
            }
        },
    }


def _policy_honest_manifest(
    *,
    diversity_max_shares: dict[str, float] | None = None,
) -> dict:
    return build_campaign_policy_manifest(
        lane_fractions={"guided": 0.60, "uncertain": 0.25, "wild": 0.15},
        lane_eligible_menus={
            "guided": {
                "recipe_sources": [
                    "curated_recipe_prior",
                    "discovery_recipe_validation",
                ],
                "slot_sampling_lanes": ["high_prior", "medium_prior"],
                "pair_sampling_lanes": ["positive_pair"],
                "allow_generation_eligible_fallback": False,
            },
            "uncertain": {
                "recipe_sources": [
                    "curated_recipe_prior",
                    "discovery_recipe_validation",
                ],
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
        diversity_max_shares=diversity_max_shares
        or {
            "family": 1.0,
            "recipe": 1.0,
            "instrument": 1.0,
            "timeframe": 1.0,
            "indicator": 1.0,
        },
        source_atlas_generation="atlas-generation-test",
        source_atlas_run_sequence=7,
    )


def _policy_honest_seed_plan(
    *,
    policy: dict | None = None,
    family_id: str = "family-rsi-adx-m5",
) -> dict:
    policy = policy or _policy_honest_manifest()
    return {
        "schema_version": "play_hand_seed_plan_v2",
        "sampling_policy": {
            "guided_prior_fraction": policy["lanes"]["guided"]["fraction"],
            "uncertain_prior_fraction": policy["lanes"]["uncertain"]["fraction"],
            "wild_exploration_fraction": policy["lanes"]["wild"]["fraction"],
        },
        "campaign_policy_manifest": policy,
        "campaign_policy_sha256": policy["manifest_sha256"],
        "negative_pairs": [],
        "recipes": {
            "guided_recipe": {
                "source": "curated_recipe_prior",
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "ADX",
                        "pair_sampling_lane": "positive_pair",
                        "pair_sampling_weight": 1.0,
                        "canonical_pair_family_id": family_id,
                    }
                ],
                "slot_menus": {},
            },
            "uncertain_recipe": {
                "source": "curated_recipe_prior",
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "MACD",
                        "trigger_id": "SMA",
                        "pair_sampling_lane": "near_miss_pair",
                        "pair_sampling_weight": 1.0,
                        "canonical_pair_family_id": "family-macd-sma-m5",
                    }
                ],
                "slot_menus": {},
            },
            "wild_recipe": {
                "source": "curated_recipe_prior",
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "MACD",
                        "pair_sampling_lane": "low_pair",
                        "pair_sampling_weight": 1.0,
                        "canonical_pair_family_id": "family-rsi-macd-m5",
                    }
                ],
                "slot_menus": {},
            },
        },
    }


def _write_historical_seed_plan(tmp_path: Path, payload: dict | None = None) -> Path:
    path = tmp_path / "historical-seed-plan.json"
    path.write_text(
        json.dumps(payload if payload is not None else _historical_seed_plan()),
        encoding="utf-8",
    )
    return path


def _level_c_runtime(
    tmp_path: Path,
    *,
    seed_plan_payload: dict | None = None,
    **overrides,
) -> lab.PlayHandLabRuntimeConfig:
    seed_plan_path = (
        overrides["seed_plan_path"]
        if "seed_plan_path" in overrides
        else _write_historical_seed_plan(tmp_path, seed_plan_payload)
    )
    expected_seed_plan_sha256 = (
        overrides["expected_seed_plan_sha256"]
        if "expected_seed_plan_sha256" in overrides
        else lab._file_sha256(seed_plan_path)
    )
    values = {
        "as_of_date": "2025-06-30T00:00:00Z",
        "campaign_id": "formal-campaign-2025-06",
        "campaign_mode": "finite",
        "task_mode": "deep_replay",
        "pipeline_mode": "play_hand",
        "target_runs": 1,
        "active_runs": 1,
        "strict_scoring": True,
        "seed": 17,
        "worker_contract_hash": "sha256:" + "a" * 64,
        "lake_manifest_sha256": "sha256:" + "b" * 64,
        "seed_plan_path": seed_plan_path,
        "expected_seed_plan_sha256": expected_seed_plan_sha256,
        "research_generation_id": "generation-2025-06",
        "level_c_protocol_id": "sha256:" + "c" * 64,
        "cutoff_key": "A",
        "source_snapshot_sha256": "sha256:" + "d" * 64,
        "universe_id": str(universe_provenance()["universe_id"]),
        "universe_manifest_sha256": str(universe_provenance()["universe_hash"]),
        "execution_plan_path": tmp_path / "execution-plan.json",
        "execution_plan_id": "sha256:" + "e" * 64,
    }
    values.update(overrides)
    return lab.PlayHandLabRuntimeConfig(**values)


def test_normalize_runtime_loads_existing_gateway_token_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FUZZFOLIO_LAB_GATEWAY_TOKEN", raising=False)
    token_file = tmp_path / "gateway-token.txt"
    token_file.write_text("shared-token", encoding="ascii")
    monkeypatch.setenv("FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE", str(token_file))

    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.gateway_token == "shared-token"


def test_enqueue_gateway_tasks_retries_transient_request_errors(tmp_path: Path) -> None:
    class FlakyGateway:
        def __init__(self) -> None:
            self.calls = 0

        def enqueue_tasks(self, tasks):
            self.calls += 1
            if self.calls < 3:
                raise requests.exceptions.ReadTimeout("gateway timed out")
            return {"enqueued": len(tasks)}

    gateway = FlakyGateway()
    ctx = _campaign_ctx(tmp_path)

    result = lab._enqueue_gateway_tasks_with_retries(
        gateway,
        ctx,
        [{"task_id": "task-1"}],
        reason="test",
        failure_limit=3,
        retry_base_seconds=0.0,
    )

    events = [
        json.loads(line)
        for line in ctx.events_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert result == {"enqueued": 1}
    assert gateway.calls == 3
    assert [event["status"] for event in events] == ["task_enqueue_failed", "task_enqueue_failed"]
    assert events[0]["attempt"] == 1
    assert events[1]["attempt"] == 2


def test_runtime_event_payload_redacts_gateway_token_and_preserves_paths(tmp_path: Path) -> None:
    runtime = lab.PlayHandLabRuntimeConfig(
        gateway_token="super-secret-lab-token",
        profile_path=tmp_path / "profile.json",
        trading_dashboard_root=tmp_path / "Trading-Dashboard",
    )

    payload = lab._runtime_event_payload(runtime)

    assert payload["gateway_token"] == "[redacted]"
    assert payload["profile_path"] == str(tmp_path / "profile.json")
    assert payload["trading_dashboard_root"] == str(tmp_path / "Trading-Dashboard")
    assert "super-secret-lab-token" not in json.dumps(payload)


def test_normalize_runtime_defaults_to_cloud_tolerant_lab_attempts() -> None:
    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.max_attempts == 8


def test_normalize_runtime_requires_lake_identity_for_historical_mode() -> None:
    with pytest.raises(ValueError, match="exact lake_manifest_sha256"):
        lab._normalize_runtime(
            lab.PlayHandLabRuntimeConfig(as_of_date="2025-06-30T00:00:00Z")
        )


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"campaign_mode": "continuous"}, "campaign_mode=finite"),
        ({"target_runs": 0}, "positive, explicit target_runs"),
        ({"strict_scoring": False}, "strict_scoring=True"),
        ({"seed": None}, "explicit seed"),
        ({"worker_contract_hash": None}, "explicit exact worker_contract_hash"),
        ({"lake_manifest_sha256": "sha256:not-a-hash"}, "exact lake_manifest_sha256"),
        ({"expected_seed_plan_sha256": None}, "exact expected_seed_plan_sha256"),
        ({"campaign_id": None}, "campaign_id"),
        ({"campaign_id": "unsafe/campaign"}, "campaign_id"),
        ({"research_generation_id": ""}, "research_generation_id"),
        ({"level_c_protocol_id": "level-c-v2"}, "level_c_protocol_id"),
        ({"cutoff_key": "cutoff-2025-06-30"}, "cutoff_key"),
    ],
)
def test_normalize_runtime_historical_mode_fails_closed_for_level_c_preconditions(
    tmp_path: Path,
    overrides: dict,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        lab._normalize_runtime(_level_c_runtime(tmp_path, **overrides))


def test_normalize_runtime_historical_mode_rejects_non_json_seed_plan(tmp_path: Path) -> None:
    non_json_path = tmp_path / "historical-seed-plan.txt"
    non_json_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="existing JSON seed plan file"):
        lab._normalize_runtime(
            _level_c_runtime(
                tmp_path,
                seed_plan_path=non_json_path,
            )
        )


def test_normalize_runtime_historical_mode_rejects_malformed_seed_plan(tmp_path: Path) -> None:
    seed_plan_path = tmp_path / "historical-seed-plan.json"
    seed_plan_path.write_text("{not json", encoding="utf-8")

    with pytest.raises(ValueError, match="seed plan must be valid JSON"):
        lab._normalize_runtime(
            _level_c_runtime(
                tmp_path,
                seed_plan_path=seed_plan_path,
                expected_seed_plan_sha256=lab._file_sha256(seed_plan_path),
            )
        )


def test_normalize_runtime_historical_mode_requires_matching_seed_plan_hash(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not match expected_seed_plan_sha256"):
        lab._normalize_runtime(
            _level_c_runtime(
                tmp_path,
                expected_seed_plan_sha256="sha256:" + "c" * 64,
            )
        )


def test_normalize_runtime_historical_mode_preserves_verified_level_c_lineage(
    tmp_path: Path,
) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path, target_runs=3))

    assert runtime.research_generation_id == "generation-2025-06"
    assert runtime.campaign_id == "formal-campaign-2025-06"
    assert runtime.level_c_protocol_id == "sha256:" + "c" * 64
    assert runtime.cutoff_key == "A"
    assert runtime.expected_seed_plan_sha256 == lab._file_sha256(runtime.seed_plan_path)
    assert runtime.terminal_lane_retention >= 3


def test_durable_campaign_state_rejects_semantic_runtime_drift(tmp_path: Path) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path, target_runs=1))
    state_path = tmp_path / "state.json"
    lab._write_campaign_state(
        state_path,
        runtime=runtime,
        campaign_id=str(runtime.campaign_id),
        lanes=[],
        history=lab.LabCampaignHistory(),
        next_lane_index=0,
        recorded_result_count=0,
    )

    changed = lab.replace(runtime, final_min_score=runtime.final_min_score + 1.0)
    with pytest.raises(lab.DurableExecutionError, match="lineage mismatch"):
        lab._load_campaign_state(
            state_path,
            runtime=changed,
            campaign_id=str(runtime.campaign_id),
        )


def test_durable_campaign_state_allows_operational_concurrency_change(tmp_path: Path) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path, target_runs=2, active_runs=2))
    state_path = tmp_path / "state.json"
    lab._write_campaign_state(
        state_path,
        runtime=runtime,
        campaign_id=str(runtime.campaign_id),
        lanes=[],
        history=lab.LabCampaignHistory(),
        next_lane_index=0,
        recorded_result_count=0,
    )

    changed = lab.replace(runtime, active_runs=1)
    _lanes, _history, next_lane_index, _reserved, recorded_result_count = (
        lab._load_campaign_state(
            state_path,
            runtime=changed,
            campaign_id=str(runtime.campaign_id),
        )
    )
    assert next_lane_index == 0
    assert recorded_result_count == 0


def test_policy_honest_state_uses_exact_hamilton_lane_allocation() -> None:
    runtime = lab.PlayHandLabRuntimeConfig(
        campaign_mode="finite",
        target_runs=20,
        active_runs=20,
    )
    state = lab._new_campaign_policy_state(
        _policy_honest_manifest(),
        runtime=runtime,
    )

    assert state is not None
    assert state["planned_lane_counts"] == {
        "guided": 12,
        "uncertain": 5,
        "wild": 3,
    }
    assert state["lane_plan"] == (["guided"] * 12) + (["uncertain"] * 5) + (["wild"] * 3)
    assert state["used_lane_counts"] == {"guided": 0, "uncertain": 0, "wild": 0}


def test_policy_honest_cap_accounting_preserves_typed_missing_values() -> None:
    runtime = lab.PlayHandLabRuntimeConfig(
        campaign_mode="finite",
        target_runs=3,
        active_runs=3,
    )
    state = lab._new_campaign_policy_state(_policy_honest_manifest(), runtime=runtime)
    assert state is not None

    attributes_by_recipe_value = []
    for recipe_value in (None, "", "<MISSING>"):
        attributes = lab._policy_candidate_attributes(
            {
                "indicator_deal": {
                    "recipe": recipe_value,
                    "pair": {"canonical_pair_family_id": "FAMILY-A"},
                    "indicators": ["RSI"],
                },
                "primary_instrument": "EURUSD",
                "timeframe": "M5",
            },
            policy_state=state,
        )
        assert attributes is not None
        decision = lab._policy_cap_decision(state, attributes)
        assert decision["outcome"] == "accepted"
        lab._record_policy_assignment(
            state,
            lane="guided",
            cap_decision=decision,
        )
        attributes_by_recipe_value.append(attributes)

    assert [item["recipe_id"] for item in attributes_by_recipe_value] == [
        {"kind": "absent"},
        {"kind": "blank"},
        {"kind": "value", "value": "<MISSING>"},
    ]
    assert len({item["candidate_id"] for item in attributes_by_recipe_value}) == 3
    assert len(state["accounting"]["recipe"]) == 3


def test_policy_honest_caps_fail_closed_after_same_lane_fallbacks(tmp_path: Path) -> None:
    policy = _policy_honest_manifest(
        diversity_max_shares={
            "family": 0.25,
            "recipe": 1.0,
            "instrument": 1.0,
            "timeframe": 1.0,
            "indicator": 0.25,
        }
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        campaign_mode="finite",
        target_runs=4,
        active_runs=4,
        min_indicators=2,
        max_indicators=2,
        instrument=["EURUSD"],
        seed=19,
    )
    state = lab._new_campaign_policy_state(policy, runtime=runtime)
    assert state is not None
    seed_plan = _policy_honest_seed_plan(policy=policy)
    indicators = [lab.SeedIndicator("RSI"), lab.SeedIndicator("ADX"), lab.SeedIndicator("MACD"), lab.SeedIndicator("SMA")]

    deal, first_assignment = lab._select_policy_lane_deal(
        config=_test_config(tmp_path),
        runtime=runtime,
        seed_indicators=indicators,
        seed_plan=seed_plan,
        lane_index=0,
        policy_state=state,
    )
    assert deal is not None
    assert first_assignment["policy_lane"] == "guided"
    assert first_assignment["cap_decision"]["outcome"] == "accepted"
    lab._record_policy_assignment(
        state,
        lane="guided",
        cap_decision=first_assignment["cap_decision"],
    )

    exhausted_deal, exhausted_assignment = lab._select_policy_lane_deal(
        config=_test_config(tmp_path),
        runtime=runtime,
        seed_indicators=indicators,
        seed_plan=seed_plan,
        lane_index=1,
        policy_state=state,
    )
    assert exhausted_deal is None
    assert exhausted_assignment["policy_lane"] == "guided"
    assert exhausted_assignment["policy_outcome_type"] == "policy_cap_exhausted"
    assert all(
        decision["cap_decision"]["outcome"] == "cap_blocked"
        for decision in exhausted_assignment["candidate_fallback_decisions"]
    )


def test_policy_honest_lane_exhaustion_never_borrows_another_lane(tmp_path: Path) -> None:
    policy = _policy_honest_manifest()
    runtime = lab.PlayHandLabRuntimeConfig(
        campaign_mode="finite",
        target_runs=4,
        active_runs=4,
        min_indicators=2,
        max_indicators=2,
        instrument=["EURUSD"],
        seed=23,
    )
    state = lab._new_campaign_policy_state(policy, runtime=runtime)
    assert state is not None
    seed_plan = _policy_honest_seed_plan(policy=policy)
    seed_plan["recipes"] = {}

    deal, assignment = lab._select_policy_lane_deal(
        config=_test_config(tmp_path),
        runtime=runtime,
        seed_indicators=[lab.SeedIndicator("RSI"), lab.SeedIndicator("ADX")],
        seed_plan=seed_plan,
        lane_index=0,
        policy_state=state,
    )

    assert deal is None
    assert assignment["policy_lane"] == "guided"
    assert assignment["policy_outcome_type"] == lab.POLICY_EXHAUSTION_OUTCOME
    assert {
        decision["outcome"] for decision in assignment["candidate_fallback_decisions"]
    } == {lab.POLICY_EXHAUSTION_OUTCOME}


def test_policy_honest_lane_selection_uses_current_atlas_expiry_binding(
    tmp_path: Path,
) -> None:
    policy = _policy_honest_manifest()
    runtime = lab.PlayHandLabRuntimeConfig(
        campaign_mode="finite",
        target_runs=4,
        active_runs=4,
        min_indicators=2,
        max_indicators=2,
        instrument=["EURUSD"],
        seed=29,
        current_atlas_generation="atlas-generation-test",
        current_atlas_run_sequence=9,
    )
    state = lab._new_campaign_policy_state(policy, runtime=runtime)
    assert state is not None
    seed_plan = _policy_honest_seed_plan(policy=policy)
    seed_plan["negative_pairs"] = [
        {
            "first_indicator_id": "RSI",
            "second_indicator_id": "ADX",
            "is_hard_block": True,
            "expires_after_atlas_runs": 1,
        }
    ]

    deal, assignment = lab._select_policy_lane_deal(
        config=_test_config(tmp_path),
        runtime=runtime,
        seed_indicators=[lab.SeedIndicator("RSI"), lab.SeedIndicator("ADX")],
        seed_plan=seed_plan,
        lane_index=0,
        policy_state=state,
    )

    assert deal is not None
    assert assignment["negative_prior_runtime"] == {
        "current_atlas_generation": "atlas-generation-test",
        "current_atlas_run_sequence": 9,
        "binding_source": "runtime_authority",
    }
    assert assignment["negative_prior_decisions"] == [
        {
            "pair": ("ADX", "RSI"),
            "expiry_status": "expired_run_sequence",
            "applied": False,
        }
    ]


def test_formal_policy_honest_runtime_requires_current_atlas_binding(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match="plan-bound current Atlas generation and run sequence",
    ):
        lab._normalize_runtime(
            _level_c_runtime(
                tmp_path,
                seed_plan_payload=_policy_honest_seed_plan(),
            )
        )

    bound = lab._normalize_runtime(
        _level_c_runtime(
            tmp_path,
            seed_plan_payload=_policy_honest_seed_plan(),
            current_atlas_generation="atlas-generation-test",
            current_atlas_run_sequence=9,
        )
    )
    assert bound.current_atlas_generation == "atlas-generation-test"
    assert bound.current_atlas_run_sequence == 9


def test_seed_indicators_rejects_policy_honest_digest_mismatch(tmp_path: Path) -> None:
    seed_plan = _policy_honest_seed_plan()
    seed_plan["campaign_policy_sha256"] = "sha256:" + "0" * 64
    seed_plan_path = tmp_path / "tampered-v2-seed-plan.json"
    seed_plan_path.write_text(json.dumps(seed_plan), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match"):
        lab._seed_indicators(
            config=_test_config(tmp_path),
            cli=object(),
            campaign_ctx=_campaign_ctx(tmp_path),
            runtime=lab.PlayHandLabRuntimeConfig(
                profile_path=tmp_path / "profile.json",
                seed_plan_path=seed_plan_path,
            ),
        )


def test_normalize_runtime_exploratory_campaign_id_is_optional_and_validated() -> None:
    assert lab._normalize_runtime(lab.PlayHandLabRuntimeConfig()).campaign_id is None
    assert (
        lab._normalize_runtime(lab.PlayHandLabRuntimeConfig(campaign_id="explore-42")).campaign_id
        == "explore-42"
    )
    with pytest.raises(ValueError, match="campaign_id"):
        lab._normalize_runtime(lab.PlayHandLabRuntimeConfig(campaign_id="../escape"))


def test_cmd_play_hand_lab_uses_explicit_historical_campaign_id_for_exact_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_config = _test_config(tmp_path)
    runtime = _level_c_runtime(tmp_path, campaign_id="formal-campaign-2025-06")
    captured: dict[str, object] = {}

    class FakeCli:
        def __init__(self, _config) -> None:
            pass

    class StopAfterCampaignSetup(Exception):
        pass

    def stop_after_metadata(campaign_ctx, **_kwargs) -> None:
        captured["campaign_id"] = campaign_ctx.run_id
        captured["campaign_dir"] = campaign_ctx.run_dir
        raise StopAfterCampaignSetup()

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", lambda **_kwargs: object())
    monkeypatch.setattr(
        lab,
        "validate_executor_runtime_binding",
        lambda *_args, **_kwargs: (
            {},
            {"generation": {"active_runs_root": str(fake_config.runs_root)}},
        ),
    )
    monkeypatch.setattr(lab, "validate_profile_model_source_lock", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(lab, "_write_campaign_metadata", stop_after_metadata)

    with pytest.raises(StopAfterCampaignSetup):
        lab.cmd_play_hand_lab(runtime)

    expected_dir = (
        fake_config.runs_root
        / "derived"
        / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR
        / "formal-campaign-2025-06"
    )
    assert captured["campaign_id"] == "formal-campaign-2025-06"
    assert captured["campaign_dir"] == expected_dir
    assert expected_dir.is_dir()


def test_historical_campaign_path_rejects_conflicting_lineage(tmp_path: Path) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path))
    campaign_dir = tmp_path / runtime.campaign_id
    campaign_dir.mkdir()
    (campaign_dir / "run-metadata.json").write_text(
        json.dumps(
            {
                **lab._historical_campaign_lineage(runtime),
                "research_generation_id": "generation-conflict",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="conflicting historical lineage: research_generation_id"):
        lab._reject_existing_historical_campaign_path(campaign_dir, runtime=runtime)


def test_historical_campaign_path_accepts_resume_with_matching_durable_state(tmp_path: Path) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path, resume=True))
    campaign_dir = tmp_path / runtime.campaign_id
    campaign_dir.mkdir()
    (campaign_dir / "run-metadata.json").write_text(
        json.dumps(lab._historical_campaign_lineage(runtime)),
        encoding="utf-8",
    )

    (campaign_dir / "play-hand-lab-state.json").write_text("{}", encoding="utf-8")
    (campaign_dir / "play-hand-lab-execution-journal.json").write_text("{}", encoding="utf-8")

    lab._reject_existing_historical_campaign_path(campaign_dir, runtime=runtime)


def test_historical_campaign_resume_rejects_mismatched_journal_without_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_config = _test_config(tmp_path)
    runtime = _level_c_runtime(tmp_path, resume=True)
    campaign_dir = (
        fake_config.runs_root
        / "derived"
        / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR
        / str(runtime.campaign_id)
    )
    campaign_dir.mkdir(parents=True)
    (campaign_dir / "run-metadata.json").write_text(
        json.dumps(lab._historical_campaign_lineage(runtime)),
        encoding="utf-8",
    )
    (campaign_dir / "play-hand-lab-state.json").write_text("{}", encoding="utf-8")
    journal_path = campaign_dir / "play-hand-lab-execution-journal.json"
    lab.DurableExecutionJournal(
        journal_path,
        execution_id="different-plan",
        lineage={"different": "lineage"},
    ).load(create=True)
    original_journal = journal_path.read_bytes()

    class FakeCli:
        def __init__(self, _config) -> None:
            pass

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", lambda **_kwargs: object())
    monkeypatch.setattr(
        lab,
        "validate_executor_runtime_binding",
        lambda *_args, **_kwargs: (
            {},
            {"generation": {"active_runs_root": str(fake_config.runs_root)}},
        ),
    )
    monkeypatch.setattr(lab, "validate_profile_model_source_lock", lambda *_args, **_kwargs: {})

    with pytest.raises(lab.DurableExecutionError, match="execution journal lineage mismatch"):
        lab.cmd_play_hand_lab(runtime)

    assert journal_path.read_bytes() == original_journal


def test_level_c_lineage_and_seed_hash_are_persisted_in_campaign_and_lane_metadata(
    tmp_path: Path,
) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path))
    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    campaign_ctx = _campaign_ctx(campaign_dir)
    lane_dir = tmp_path / "lane"
    lane_dir.mkdir()
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-1",
        run_dir=lane_dir,
    )

    lab._write_campaign_metadata(
        campaign_ctx,
        runtime=runtime,
        status="starting",
        started_at="2025-06-30T00:00:00Z",
    )
    lab._write_lane_metadata(
        lane,
        campaign_ctx=campaign_ctx,
        runtime=runtime,
        status="queued",
        started_at="2025-06-30T00:00:00Z",
    )

    campaign_metadata = json.loads(
        (campaign_dir / "run-metadata.json").read_text(encoding="utf-8")
    )
    lane_metadata = json.loads(
        (lane_dir / "run-metadata.json").read_text(encoding="utf-8")
    )
    for metadata in [campaign_metadata, lane_metadata]:
        assert metadata["campaign_id"] == "campaign-1"
        assert metadata["research_generation_id"] == "generation-2025-06"
        assert metadata["level_c_protocol_id"] == "sha256:" + "c" * 64
        assert metadata["cutoff_key"] == "A"
        assert metadata["expected_seed_plan_sha256"] == runtime.expected_seed_plan_sha256
        assert metadata["play_hand_seed_plan_sha256"] == runtime.expected_seed_plan_sha256
        assert metadata["formal_historical_level_c"] is True


@pytest.mark.parametrize(
    ("initial_status", "target_runs", "expected_reason"),
    [
        ("stopped", 1, "historical_campaign_stopped"),
        ("completed", 2, "historical_campaign_incomplete"),
    ],
)
def test_historical_campaign_finalization_rejects_stopped_or_partial_promotion(
    tmp_path: Path,
    initial_status: str,
    target_runs: int,
    expected_reason: str,
) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-1",
        run_dir=tmp_path / "lane",
        terminal=True,
        run_promoted=True,
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        campaign_mode="finite",
        target_runs=target_runs,
    )

    status, reason = lab._finalize_historical_campaign_status(
        initial_status,
        lanes=[lane],
        runtime=runtime,
    )

    assert status == "failed"
    assert reason == expected_reason
    assert lane.run_promoted is False
    assert lane.tombstone_reason == expected_reason
    assert lane.current_phase == "incomplete"


def test_historical_campaign_completion_accepts_terminal_research_rejections(
    tmp_path: Path,
) -> None:
    lanes = [
        lab.LabLaneState(
            lane_id="lane_000",
            lane_index=0,
            run_id="lane-0",
            run_dir=tmp_path / "lane-0",
            terminal=True,
            task_ids=["task-0"],
            completed_task_ids={"task-0"},
            tombstone_reason="validation_12mo_failed",
            terminal_outcome_category=lab.TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
        ),
        lab.LabLaneState(
            lane_id="lane_001",
            lane_index=1,
            run_id="lane-1",
            run_dir=tmp_path / "lane-1",
            terminal=True,
            task_ids=["task-1"],
            completed_task_ids={"task-1"},
            tombstone_reason="final_36mo_failed",
            terminal_outcome_category=lab.TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
        ),
        lab.LabLaneState(
            lane_id="lane_002",
            lane_index=2,
            run_id="lane-2",
            run_dir=tmp_path / "lane-2",
            terminal=True,
            task_ids=["task-2"],
            completed_task_ids={"task-2"},
            tombstone_reason="no_signal",
            terminal_outcome_category=lab.TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
        ),
        lab.LabLaneState(
            lane_id="lane_003",
            lane_index=3,
            run_id="lane-3",
            run_dir=tmp_path / "lane-3",
            terminal=True,
            task_ids=["task-3"],
            completed_task_ids={"task-3"},
            tombstone_reason="no-valid-cell",
            terminal_outcome_category=lab.TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
        ),
        lab.LabLaneState(
            lane_id="lane_004",
            lane_index=4,
            run_id="lane-4",
            run_dir=tmp_path / "lane-4",
            terminal=True,
            task_ids=["task-4"],
            completed_task_ids={"task-4"},
            tombstone_reason="nonviable",
            terminal_outcome_category=lab.TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
        ),
    ]
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        campaign_mode="finite",
        target_runs=len(lanes),
    )

    status, reason = lab._finalize_historical_campaign_status(
        "completed",
        lanes=lanes,
        runtime=runtime,
    )

    assert status == "completed"
    assert reason is None
    assert not any(lane.run_promoted for lane in lanes)


@pytest.mark.parametrize(
    ("terminal", "completed", "failed", "tombstone_reason"),
    [
        (False, set(), set(), None),
        (True, set(), {"task-0"}, "lab_stage_worker_failed"),
    ],
    ids=["incomplete", "infrastructure-failed"],
)
def test_historical_campaign_completion_rejects_incomplete_or_failed_lanes(
    tmp_path: Path,
    terminal: bool,
    completed: set[str],
    failed: set[str],
    tombstone_reason: str | None,
) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-0",
        run_dir=tmp_path / "lane-0",
        terminal=terminal,
        task_ids=["task-0"],
        completed_task_ids=completed,
        failed_task_ids=failed,
        tombstone_reason=tombstone_reason,
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        campaign_mode="finite",
        target_runs=1,
    )

    status, reason = lab._finalize_historical_campaign_status(
        "completed",
        lanes=[lane],
        runtime=runtime,
    )

    assert status == "failed"
    assert reason == "historical_campaign_incomplete"


def test_normalize_runtime_defaults_to_random_screen_and_validation_rung() -> None:
    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.screen_anchor_mode == "random"
    assert runtime.screen_anchor_envelope_months == 36
    assert runtime.validation_months == 12
    assert runtime.validation_min_score == 45.0
    assert runtime.scrutiny_months == 36
    assert runtime.final_min_score == 40.0


def test_normalize_runtime_resolves_instrument_pool_presets() -> None:
    runtime = lab._normalize_runtime(
        lab.PlayHandLabRuntimeConfig(
            instrument_pool_preset=["fx-major", "metals"],
            instrument_pool=["DE40"],
        )
    )

    assert runtime.instrument_pool_preset == ["fx-major", "metals"]
    assert runtime.instrument_pool == [
        "AUDUSD",
        "EURUSD",
        "GBPUSD",
        "USDCAD",
        "USDCHF",
        "USDJPY",
        "XAGUSD",
        "XAUUSD",
        "DE40",
    ]


def test_normalize_runtime_uses_target_and_active_runs() -> None:
    runtime = lab._normalize_runtime(
        lab.PlayHandLabRuntimeConfig(
            campaign_mode="finite",
            target_runs=512,
            active_runs=64,
            lanes=4,
        )
    )

    assert runtime.campaign_mode == "finite"
    assert runtime.target_runs == 512
    assert runtime.active_runs == 64
    assert runtime.lanes == 512


def test_normalize_runtime_continuous_has_no_target_by_default() -> None:
    runtime = lab._normalize_runtime(
        lab.PlayHandLabRuntimeConfig(campaign_mode="continuous")
    )

    assert runtime.campaign_mode == "continuous"
    assert runtime.target_runs is None
    assert runtime.active_runs == lab.DEFAULT_LAB_ACTIVE_RUNS


def test_normalize_runtime_defaults_to_barrier_logging() -> None:
    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.log_mode == "barrier"
    assert runtime.barrier_interval_seconds == 5.0
    assert runtime.barrier_lane_limit == 24
    assert runtime.terminal_lane_retention == 512


def test_lane_lifecycle_telemetry_tracks_phase_completion(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
    )

    lab._set_lane_phase(lane, "baseline")
    lab._register_task_spec(
        lane,
        task_id="task-1",
        phase="baseline_3mo",
        task_kind="deep_replay",
        spec={},
    )
    lab._register_task_spec(
        lane,
        task_id="task-2",
        phase="baseline_3mo",
        task_kind="deep_replay",
        spec={},
    )
    lane.completed_task_ids.add("task-1")
    lab._refresh_lane_phase_result_counts(lane, task_id="task-1")

    assert lane.phase_task_counts["baseline_3mo"] == 2
    assert lane.phase_completed_task_counts["baseline_3mo"] == 1
    assert "baseline_3mo" not in lane.phase_completed_at

    lane.failed_task_ids.add("task-2")
    lab._refresh_lane_phase_result_counts(lane, task_id="task-2")

    assert lane.phase_completed_at["baseline_3mo"]
    assert lane.phase_failed_task_counts["baseline_3mo"] == 1
    assert any(
        event["event"] == "phase_tasks_completed"
        and event["phase"] == "baseline_3mo"
        and event["status"] == "failed"
        for event in lane.phase_lifecycle_events
    )


def test_campaign_summary_includes_lane_lifecycle_telemetry(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
    )
    lab._set_lane_phase(lane, "baseline")
    lab._register_task_spec(
        lane,
        task_id="task-1",
        phase="baseline_3mo",
        task_kind="deep_replay",
        spec={},
    )
    lane.completed_task_ids.add("task-1")
    lab._refresh_lane_phase_result_counts(lane, task_id="task-1")

    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    campaign_ctx = SimpleNamespace(
        run_id="campaign-1",
        summary_path=campaign_dir / "play-hand-lab-campaign-summary.json",
    )

    summary = lab._write_summary(
        campaign_ctx,
        [lane],
        runtime=lab.PlayHandLabRuntimeConfig(),
        status="completed",
        started_at="2026-07-05T00:00:00+00:00",
        completed_at="2026-07-05T00:01:00+00:00",
        gateway_snapshot=None,
        recorded_results=[],
    )

    summary_lane = summary["lanes"][0]
    assert summary_lane["phase_started_at"]["baseline_3mo"]
    assert summary_lane["phase_completed_at"]["baseline_3mo"]
    assert summary_lane["phase_task_counts"]["baseline_3mo"] == 1
    assert summary_lane["phase_completed_task_counts"]["baseline_3mo"] == 1
    assert summary_lane["phase_lifecycle_events"]


def test_lab_barrier_snapshot_is_bounded_and_lane_oriented(tmp_path: Path) -> None:
    first_lane = lab.LabLaneState(
        lane_id="lane_007",
        lane_index=7,
        run_id="20260622-playhand-lab-lane-007-v1",
        run_dir=tmp_path / "lane-007",
        instruments=["EURUSD", "XAUUSD"],
        timeframe="M5",
    )
    first_lane.current_phase = "coarse"
    first_lane.task_ids = ["task-1", "task-2"]
    first_lane.completed_task_ids = {"task-1"}
    first_lane.best_score = 78.125
    first_lane.incumbent_phase = "baseline"
    hidden_lane = lab.LabLaneState(
        lane_id="lane_008",
        lane_index=8,
        run_id="20260622-playhand-lab-lane-008-v1",
        run_dir=tmp_path / "lane-008",
        instruments=["GBPUSD"],
        timeframe="M5",
    )
    hidden_lane.current_phase = "baseline"
    hidden_lane.task_ids = ["task-3"]

    text = lab._format_lab_barrier_snapshot(
        barrier_index=3,
        campaign_id="campaign-1",
        runtime=lab.PlayHandLabRuntimeConfig(
            campaign_mode="continuous",
            active_runs=2,
            barrier_lane_limit=1,
        ),
        lanes=[first_lane, hidden_lane],
        tasks=[{"task_id": "task-1"}, {"task_id": "task-2"}, {"task_id": "task-3"}],
        snapshot={
            "worker_count": 4,
            "busy_worker_count": 2,
            "worker_slots": 4,
            "busy_slots": 2,
            "queued_tasks": 5,
            "live_tasks": 7,
            "completed_tasks": 11,
            "failed_tasks": 0,
            "result_backlog": 1,
            "metrics": {"tasks_enqueued": 13, "completions_accepted": 11},
        },
        metric_baseline={"tasks_enqueued": 3, "completions_accepted": 1},
        recorded_result_count=11,
    )

    lines = text.splitlines()
    assert lines[0].startswith("+")
    assert lines[-1].startswith("+")
    assert all(len(line) == lab.LAB_BARRIER_BOX_WIDTH for line in lines)
    assert "PlayHand Massive v2 barrier #0003" in text
    assert "workers=2/4 busy slots=2/4 sat=50%" in text
    assert "lane     | phase" in text
    assert "lane_007" in text
    assert "lane_007 | coarse" in text
    assert "coarse" in text
    assert "78.12" in text
    assert "1 more active lane(s) hidden" in text


def test_lab_barrier_snapshot_prefers_active_lanes_over_terminal_noise(tmp_path: Path) -> None:
    lanes: list[lab.LabLaneState] = []
    for index in range(8):
        lane = lab.LabLaneState(
            lane_id=f"lane_{index:03d}",
            lane_index=index,
            run_id=f"20260622-playhand-lab-lane-{index:03d}-v1",
            run_dir=tmp_path / f"lane-{index:03d}",
            instruments=["EURUSD"],
            timeframe="M5",
        )
        lane.current_phase = "scrutiny"
        lane.task_ids = [f"task-{index}"]
        lanes.append(lane)
    terminal_lane = lab.LabLaneState(
        lane_id="lane_099",
        lane_index=99,
        run_id="20260622-playhand-lab-lane-099-v1",
        run_dir=tmp_path / "lane-099",
        instruments=["GBPUSD"],
        timeframe="M5",
    )
    terminal_lane.terminal = True
    terminal_lane.current_phase = "tombstoned"
    terminal_lane.tombstone_reason = "early_exit_policy_enforced"
    terminal_lane.task_ids = ["task-terminal"]
    terminal_lane.completed_task_ids = {"task-terminal"}
    lanes.append(terminal_lane)

    text = lab._format_lab_barrier_snapshot(
        barrier_index=4,
        campaign_id="campaign-1",
        runtime=lab.PlayHandLabRuntimeConfig(
            campaign_mode="continuous",
            active_runs=8,
            barrier_lane_limit=8,
        ),
        lanes=lanes,
        tasks=[{"task_id": f"task-{index}"} for index in range(8)],
        snapshot={},
        metric_baseline={},
        recorded_result_count=0,
    )

    assert "lane_000" in text
    assert "lane_007" in text
    assert "lane_099" not in text
    assert "terminal lanes summarized: 1 terminal, 0 promoted, 1 tombstoned" in text


def test_lab_barrier_snapshot_includes_pruned_lane_history(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_010",
        lane_index=10,
        run_id="20260622-playhand-lab-lane-010-v1",
        run_dir=tmp_path / "lane-010",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    lane.current_phase = "baseline"
    lane.task_ids = ["task-active"]
    history = lab.LabCampaignHistory(
        pruned_lane_count=10,
        pruned_task_count=25,
        pruned_completed_task_count=20,
        pruned_failed_task_count=3,
        pruned_promoted_lane_count=2,
        pruned_tombstoned_lane_count=8,
        best_score=81.25,
    )

    text = lab._format_lab_barrier_snapshot(
        barrier_index=5,
        campaign_id="campaign-1",
        runtime=lab.PlayHandLabRuntimeConfig(campaign_mode="continuous", active_runs=1),
        lanes=[lane],
        tasks=[{"task_id": "task-active"}],
        snapshot={},
        metric_baseline={},
        recorded_result_count=20,
        history=history,
    )

    assert "created=11 active=1 terminal=10" in text
    assert "promoted=2 tombstoned=8" in text
    assert "tasks=23/26 failed=3" in text
    assert "terminal lanes summarized: 10 terminal, 2 promoted, 8 tombstoned" in text


def test_compact_terminal_lane_state_drops_heavy_payloads(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="run-1",
        run_dir=tmp_path,
        profile_payload={"large": "profile"},
        incumbent_profile_payload={"large": "incumbent"},
    )
    lane.terminal = True
    lane.task_ids = ["task-1"]
    lane.completed_task_ids.add("task-1")
    lane.task_specs["task-1"] = {"payload": "large"}
    lane.phase_rows.append({"row": 1})
    lane.phase_results["baseline"] = [{"result": 1}]
    lane.last_sweep_payload = {"large": "sweep"}
    lane.instrument_scout_result = {"large": "scout"}
    lane.best_score = 77.0

    lab._compact_terminal_lane_state(lane)

    assert lane.profile_payload is None
    assert lane.incumbent_profile_payload is None
    assert lane.last_sweep_payload is None
    assert lane.instrument_scout_result is None
    assert lane.task_specs == {}
    assert lane.phase_rows == []
    assert lane.phase_results == {}
    assert lane.best_score == 77.0
    assert lane.task_ids == ["task-1"]


def test_lab_failure_notice_includes_lane_task_phase_and_reason() -> None:
    line = lab._format_lab_event_notice(
        {
            "phase": "lab_result",
            "status": "failed",
            "run_id": "20260622-playhand-lab-lane-003-v1",
            "task_id": "task-123",
            "task_phase": "baseline",
            "task_kind": "deep_replay",
            "worker_id": "vast-worker-1",
            "lease_id": "lease-abc",
            "error": "remote data lake timeout",
        }
    )

    assert line is not None
    assert line.startswith("! lab_result failed")
    assert "lane=lane_003" in line
    assert "task_id=task-123" in line
    assert "task_phase=baseline" in line
    assert "worker_id=vast-worker-1" in line
    assert "reason=remote data lake timeout" in line
    assert lab._format_lab_event_notice({"phase": "lab_result", "status": "recorded"}) is None


def test_expand_sweep_params_enforces_permutation_budget() -> None:
    axes = [
        {"target": "profile_field", "param_key": "alpha", "values": list(range(100))},
        {"target": "profile_field", "param_key": "beta", "values": list(range(100))},
    ]

    params = lab._expand_sweep_params(axes, max_permutations=8)

    assert len(params) == 8
    assert params[0] == {"alpha": 0, "beta": 0}
    assert params[-1] == {"alpha": 99, "beta": 99}


def test_make_sweep_shard_tasks_honors_permutation_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_payload = {"indicators": [{"meta": {"instanceId": "indicator-1"}}]}
    axis_texts = [
        "indicator[0].talib.fast=1,2,3,4,5,6,7,8,9,10",
        "indicator[0].talib.slow=10,20,30,40,50,60,70,80,90,100",
        "indicator[0].config.threshold=0.1,0.2,0.3,0.4,0.5",
    ]
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path,
        profile_path=tmp_path / "base.json",
        profile_payload=profile_payload,
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
    )

    axis_plan = SimpleNamespace(
        axes=axis_texts,
        selected_permutations=500,
        event_payload=lambda: {
            "selected_axes": axis_texts,
            "selected_permutations": 500,
            "max_permutations": 16,
            "search_mode": "evolutionary",
        },
    )
    monkeypatch.setattr(lab, "plan_sweep_axes", lambda *args, **kwargs: axis_plan)

    tasks = lab._make_sweep_shard_tasks(
        lane,
        phase="coarse_probe",
        runtime=lab.PlayHandLabRuntimeConfig(
            max_sweep_permutations=16,
            sweep_shard_size=4,
            worker_contract_hash="sha256:" + "a" * 64,
            lake_manifest_sha256="sha256:" + "b" * 64,
        ),
        reward_matrix=None,
        worker_contract_hash="sha256:" + "a" * 64,
        profile_payload=profile_payload,
        profile_path=tmp_path / "base.json",
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        lookback_months=3,
        axis_texts=axis_texts,
        mode="evolutionary",
        analysis_window_start="2025-03-30T00:00:00Z",
        analysis_window_end="2025-06-30T00:00:00Z",
    )

    assert len(tasks) == 4
    assert sum(int(task["payload"]["permutation_count"]) for task in tasks) == 16
    assert max(len(task["payload"]["params_by_index"]) for task in tasks) == 4
    assert {task["payload"].get("result_detail") for task in tasks} == {"summary"}
    assert {task["payload"]["definition"]["lookback_months"] for task in tasks} == {None}
    assert {
        task["payload"]["evidence_plan"]["lake_manifest_sha256"] for task in tasks
    } == {"sha256:" + "b" * 64}
    first_spec = lane.task_specs[tasks[0]["task_id"]]
    assert first_spec["permutation_budget_applied"] is True
    assert first_spec["expanded_permutation_count"] == 16


def test_make_sweep_shard_tasks_share_profile_snapshot_without_task_spec_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_payload = {
        "indicators": [{"meta": {"instanceId": "indicator-1"}, "config": {}}],
        "notificationThreshold": 80,
    }
    axis_texts = [
        "indicator[0].talib.fast=1,2,3,4",
        "indicator[0].talib.slow=10,20",
    ]
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-shared-snap",
        run_dir=tmp_path,
        profile_path=tmp_path / "base.json",
        profile_payload=profile_payload,
        profile_ref="lab-inline:run-shared-snap:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    monkeypatch.setattr(
        lab,
        "plan_sweep_axes",
        lambda *args, **kwargs: SimpleNamespace(
            axes=axis_texts,
            selected_permutations=8,
            event_payload=lambda: {
                "selected_axes": axis_texts,
                "selected_permutations": 8,
                "max_permutations": 8,
                "search_mode": "deterministic",
            },
        ),
    )

    tasks = lab._make_sweep_shard_tasks(
        lane,
        phase="coarse_probe",
        runtime=lab.PlayHandLabRuntimeConfig(
            max_sweep_permutations=8,
            sweep_shard_size=2,
            worker_contract_hash="sha256:" + "a" * 64,
            lake_manifest_sha256="sha256:" + "b" * 64,
        ),
        reward_matrix=None,
        worker_contract_hash="sha256:" + "a" * 64,
        profile_payload=profile_payload,
        profile_path=tmp_path / "base.json",
        profile_ref="lab-inline:run-shared-snap:lane_000",
        instruments=["EURUSD"],
        lookback_months=3,
        axis_texts=axis_texts,
        mode="deterministic",
        analysis_window_start="2025-03-30T00:00:00Z",
        analysis_window_end="2025-06-30T00:00:00Z",
    )

    assert len(tasks) >= 2
    snapshots = [task["payload"]["base_profile_snapshot"] for task in tasks]
    assert all(snapshot is snapshots[0] for snapshot in snapshots)
    for task in tasks:
        spec = lane.task_specs[task["task_id"]]
        assert "profile_payload" not in spec
        assert "profile_path" in spec
        assert "profile_ref" in spec


def test_lane_state_payload_omits_profiles_and_params_and_hydrate_reloads(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "profile.json"
    incumbent_path = tmp_path / "incumbent.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    incumbent_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-slim-state",
        run_dir=tmp_path / "run",
        profile_path=profile_path,
        profile_payload={"large": "profile"},
        profile_ref="ref-1",
        incumbent_profile_path=incumbent_path,
        incumbent_profile_payload={"large": "incumbent"},
        incumbent_profile_ref="ref-1",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    lane.task_specs["task-1"] = {
        "phase": "coarse_probe",
        "task_kind": "sweep_shard",
        "profile_path": str(profile_path),
        "profile_ref": "ref-1",
        "profile_payload": {"should": "not-persist"},
        "params_by_index": {0: {"alpha": 1}, 1: {"alpha": 2}},
        "permutation_start": 0,
        "permutation_count": 2,
        "axes": ["indicator[0].talib.timeperiod=10,20"],
        "expanded_permutation_count": 2,
        "sweep_id": "sweep-1",
        "shard_id": "sweep-1-shard-0000",
        "instruments": ["EURUSD"],
        "timeframe": "M5",
    }

    payload = lab._lane_state_payload(lane)
    assert payload["profile_payload"] is None
    assert payload["incumbent_profile_payload"] is None
    slim_spec = payload["task_specs"]["task-1"]
    assert "profile_payload" not in slim_spec
    assert "params_by_index" not in slim_spec
    assert slim_spec["permutation_start"] == 0
    assert slim_spec["profile_path"] == str(profile_path)

    restored = lab._lane_state_from_payload(payload)
    assert restored.profile_payload == _profile_payload()["profile"]
    assert restored.incumbent_profile_payload == _profile_payload()["profile"]
    rebuilt_params = restored.task_specs["task-1"].get("params_by_index")
    assert isinstance(rebuilt_params, dict)
    assert set(rebuilt_params) == {0, 1}


def test_detach_attach_task_profile_snapshots_round_trip(tmp_path: Path) -> None:
    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    profile = {"indicators": [{"meta": {"id": "RSI"}}], "notificationThreshold": 80.0}
    task = {
        "task_id": "task-1",
        "task_kind": "deep_replay",
        "payload": {
            "job_id": "task-1",
            "inline_profile_snapshot": copy.deepcopy(profile),
        },
    }

    detached = lab._detach_task_profile_snapshots(task, campaign_dir)
    assert "inline_profile_snapshot" not in detached["payload"]
    digest = detached["payload"]["inline_profile_snapshot_sha256"]
    assert isinstance(digest, str) and digest.startswith("sha256:")
    hex_digest = digest.removeprefix("sha256:")
    blob_path = campaign_dir / "profile-blobs" / f"{hex_digest}.json"
    assert blob_path.is_file()
    assert "inline_profile_snapshot" in task["payload"]

    attached = lab._attach_task_profile_snapshots(detached, campaign_dir)
    assert attached["payload"]["inline_profile_snapshot"] == profile
    assert attached["payload"]["inline_profile_snapshot_sha256"] == digest


def test_detach_task_profile_snapshots_stores_base_profile_blob(tmp_path: Path) -> None:
    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    profile = {"name": "shared", "indicators": []}
    task = {
        "task_id": "shard-1",
        "payload": {"base_profile_snapshot": copy.deepcopy(profile)},
    }
    detached = lab._detach_task_profile_snapshots(task, campaign_dir)
    assert "base_profile_snapshot" not in detached["payload"]
    assert detached["payload"]["base_profile_snapshot_sha256"].startswith("sha256:")
    restored = lab._attach_task_profile_snapshots(detached, campaign_dir)
    assert restored["payload"]["base_profile_snapshot"] == profile


def test_hydrate_lane_profiles_fails_closed_on_params_sha_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-sha-mismatch",
        run_dir=tmp_path / "run",
        profile_path=tmp_path / "profile.json",
        profile_payload={"indicators": []},
        profile_ref="ref-1",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    lane.task_specs["task-1"] = {
        "phase": "coarse_probe",
        "task_kind": "sweep_shard",
        "params_by_index_sha256": "sha256:" + ("0" * 64),
    }
    monkeypatch.setattr(
        lab,
        "_rebuild_sweep_shard_params_by_index",
        lambda *_args, **_kwargs: {0: {"alpha": 1}},
    )
    with pytest.raises(lab.DurableExecutionError, match="params_by_index_sha256"):
        lab._hydrate_lane_profiles(lane)


def test_make_sweep_shard_tasks_records_params_by_index_sha256(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_payload = {
        "indicators": [{"meta": {"instanceId": "indicator-1"}, "config": {}}],
        "notificationThreshold": 80,
    }
    axis_texts = ["indicator[0].talib.fast=1,2"]
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-params-sha",
        run_dir=tmp_path,
        profile_path=tmp_path / "base.json",
        profile_payload=profile_payload,
        profile_ref="lab-inline:run-params-sha:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    monkeypatch.setattr(
        lab,
        "plan_sweep_axes",
        lambda *args, **kwargs: SimpleNamespace(
            axes=axis_texts,
            selected_permutations=2,
            event_payload=lambda: {
                "selected_axes": axis_texts,
                "selected_permutations": 2,
                "max_permutations": 2,
                "search_mode": "deterministic",
            },
        ),
    )
    tasks = lab._make_sweep_shard_tasks(
        lane,
        phase="coarse_probe",
        runtime=lab.PlayHandLabRuntimeConfig(
            max_sweep_permutations=2,
            sweep_shard_size=2,
            worker_contract_hash="sha256:" + "a" * 64,
            lake_manifest_sha256="sha256:" + "b" * 64,
        ),
        reward_matrix=None,
        worker_contract_hash="sha256:" + "a" * 64,
        profile_payload=profile_payload,
        profile_path=tmp_path / "base.json",
        profile_ref="lab-inline:run-params-sha:lane_000",
        instruments=["EURUSD"],
        lookback_months=3,
        axis_texts=axis_texts,
        mode="deterministic",
    )
    assert tasks
    for task in tasks:
        spec = lane.task_specs[task["task_id"]]
        params = spec["params_by_index"]
        assert spec["params_by_index_sha256"] == lab.canonical_sha256(
            lab._canonical_params(params)
        )
        slim = lab._lane_state_payload(lane)["task_specs"][task["task_id"]]
        assert "params_by_index" not in slim
        assert slim["params_by_index_sha256"] == spec["params_by_index_sha256"]


def test_record_lab_result_appends_attempt_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    attempts_path = run_dir / "attempts.jsonl"
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-append",
        run_dir=run_dir,
        profile_path=tmp_path / "profile.json",
        profile_ref="ref-1",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    lane.task_specs["task-1"] = {
        "phase": "baseline_3mo",
        "task_kind": "fake_compute",
        "profile_path": str(tmp_path / "profile.json"),
        "profile_ref": "ref-1",
        "instruments": ["EURUSD"],
        "timeframe": "M5",
        "lookback_months": 3,
    }
    lane_ctx = SimpleNamespace(
        attempts_path=attempts_path,
        evals_dir=run_dir / "evals",
        run_dir=run_dir,
    )
    lane_ctx.evals_dir.mkdir()
    append_calls: list[dict] = []

    monkeypatch.setattr(
        lab,
        "append_attempt_row",
        lambda path, row: append_calls.append({"path": path, "row": dict(row)}),
    )
    monkeypatch.setattr(lab, "render_progress_artifacts", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        lab,
        "_append_event",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        lab,
        "_fake_attempt_score",
        lambda _payload: lab.AttemptScore(
            primary_score=1.0,
            composite_score=1.0,
            score_basis="fake",
            metrics={},
            best_summary={},
        ),
    )
    config = SimpleNamespace(
        research=SimpleNamespace(plot_lower_is_better=False),
    )

    recorded = lab._record_lab_result(
        config=config,  # type: ignore[arg-type]
        cli=SimpleNamespace(),
        lane_ctx=lane_ctx,  # type: ignore[arg-type]
        lane=lane,
        runtime=lab.PlayHandLabRuntimeConfig(task_mode="fake_compute"),
        lab_result={
            "task_id": "task-1",
            "status": "success",
            "worker_id": "w1",
            "lease_id": "lease-1",
            "result": {"status": "success", "ok": True},
        },
        reward_matrix=None,
        render_progress=False,
    )

    assert recorded["task_id"] == "task-1"
    assert len(append_calls) == 1
    assert append_calls[0]["path"] == attempts_path
    assert append_calls[0]["row"]["lab_campaign_task_id"] == "task-1"


def test_rank_sweep_permutations_accepts_compact_summary_results() -> None:
    payload = lab._rank_sweep_permutations(
        phase="coarse_probe",
        shard_results=[
            {
                "permutation_results": [
                    {
                        "permutation_index": 0,
                        "child_job_id": "child-0",
                        "status": "success",
                        "parameters": {"alpha": 1},
                        "result": {
                            "result_detail": "summary",
                            "aggregate": {"score_lab": {"score": 12.5}},
                            "full_result_omitted": True,
                        },
                    },
                    {
                        "permutation_index": 1,
                        "child_job_id": "child-1",
                        "status": "success",
                        "parameters": {"alpha": 2},
                        "fitness": {"score_lab": 9.0},
                    },
                ]
            }
        ],
    )

    assert payload["best"]["child_job_id"] == "child-0"
    assert payload["best"]["score"] == 12.5
    assert [item["score"] for item in payload["ranked"]] == [12.5, 9.0]


def test_validated_sweep_shard_accepts_scoreless_summary_and_rejects_malformed() -> None:
    task_spec = {
        "sweep_id": "sweep-1",
        "shard_id": "sweep-1-shard-0001",
        "permutation_start": 8,
        "permutation_count": 2,
        "params_by_index": {"8": {"alpha": 8}, "9": {"alpha": 9}},
        "result_detail": "summary",
    }
    worker_result = {
        "result": {
            "sweep_id": "sweep-1",
            "shard_id": "sweep-1-shard-0001",
            "status": "success",
            "started_at": "2026-07-18T00:00:00Z",
            "completed_at": "2026-07-18T00:00:01Z",
            "result_detail": "summary",
            "permutation_results": [
                {
                    "permutation_index": index,
                    "child_job_id": f"sweep-1-{index:06d}",
                    "status": "success",
                    "parameters": {"alpha": index},
                    "result_detail": "summary",
                    "result": {
                        "result_detail": "summary",
                        "full_result_omitted": True,
                        "warnings": ["no score for this permutation"],
                    },
                }
                for index in (8, 9)
            ],
            "failed_permutations": [],
        }
    }

    validated = lab._validated_sweep_shard_payload(
        worker_result=worker_result,
        task_spec=task_spec,
    )
    ranked = lab._rank_sweep_permutations(
        phase="lookback_timing",
        shard_results=[validated],
    )
    assert ranked["best"] is None
    assert ranked["outcome"] == "no_scored_permutation"
    assert ranked["permutation_indices"] == [8, 9]

    malformed = json.loads(json.dumps(worker_result))
    malformed["result"]["permutation_results"][0]["child_job_id"] = "wrong-child"
    with pytest.raises(lab.DurableExecutionError, match="child_job_id"):
        lab._validated_sweep_shard_payload(worker_result=malformed, task_spec=task_spec)


def test_sweep_merge_is_order_independent_and_requires_all_shard_receipts() -> None:
    def shard(
        shard_id: str,
        entries: list[tuple[int, float | None]],
    ) -> dict:
        return lab._rank_sweep_permutations(
            phase="lookback_timing",
            shard_results=[
                {
                    "sweep_id": "sweep-1",
                    "shard_id": shard_id,
                    "permutation_results": [
                        {
                            "permutation_index": index,
                            "child_job_id": f"sweep-1-{index:06d}",
                            "status": "success",
                            "parameters": {"alpha": index},
                            "result": (
                                {"aggregate": {"score_lab": {"score": score}}}
                                if score is not None
                                else {"result_detail": "summary", "full_result_omitted": True}
                            ),
                        }
                        for index, score in entries
                    ],
                    "failed_permutations": [],
                }
            ],
        )

    scored = shard("sweep-1-shard-0000", [(0, 72.0), (1, 68.0)])
    scoreless = shard("sweep-1-shard-0001", [(2, None), (3, None)])
    expected = {
        "sweep-1-shard-0000": {0, 1},
        "sweep-1-shard-0001": {2, 3},
    }
    merged = lab._merge_sweep_payloads(
        "lookback_timing",
        [scoreless, scored],
        expected_sweep_id="sweep-1",
        expected_shards=expected,
    )
    assert merged["outcome"] == "scored"
    assert merged["best"]["permutation_index"] == 0
    assert [item["permutation_index"] for item in merged["ranked"]] == [0, 1, 2, 3]

    all_nonviable = lab._merge_sweep_payloads(
        "lookback_timing",
        [scoreless, shard("sweep-1-shard-0000", [(0, None), (1, None)])],
        expected_sweep_id="sweep-1",
        expected_shards=expected,
    )
    assert all_nonviable["best"] is None
    assert all_nonviable["outcome"] == "no_scored_permutation"

    with pytest.raises(lab.DurableExecutionError, match="missing one or more shard"):
        lab._merge_sweep_payloads(
            "lookback_timing",
            [scored],
            expected_sweep_id="sweep-1",
            expected_shards=expected,
        )


def test_phase_sweep_merge_buckets_multi_parent_receipts_and_rebinds_legacy_shards(
    tmp_path: Path,
) -> None:
    phase = "lookback_timing"
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="multi-parent",
        run_dir=tmp_path / "lane",
    )

    def add_task(sweep_id: str, shard_index: int, permutation_index: int) -> str:
        shard_id = f"{sweep_id}-shard-{shard_index:04d}"
        task_id = f"task-{sweep_id}-{shard_index:04d}"
        lane.phase_task_ids.setdefault(phase, []).append(task_id)
        lane.task_specs[task_id] = {
            "task_kind": "sweep_shard",
            "sweep_id": sweep_id,
            "shard_id": shard_id,
            "permutation_start": permutation_index,
            "permutation_count": 1,
            "params_by_index": {str(permutation_index): {"alpha": permutation_index}},
        }
        return task_id

    a0 = add_task("parent-a", 0, 0)
    a1 = add_task("parent-a", 1, 1)
    b0 = add_task("parent-b", 0, 0)
    b1 = add_task("parent-b", 1, 1)

    def recorded(task_id: str, score: float | None, *, legacy: bool = False) -> dict:
        spec = lane.task_specs[task_id]
        sweep_id = str(spec["sweep_id"])
        index = int(spec["permutation_start"])
        entry = {
            "permutation_index": index,
            "child_job_id": f"{sweep_id}-{index:06d}",
            "status": "success",
            "parameters": {"alpha": index},
            "score": score,
            "fitness_value": score,
        }
        payload = {
            "sweep_id": f"lab-{phase}" if legacy else sweep_id,
            "mode": "lab_sweep_shard",
            "ranked_permutations": [entry],
            "ranked": [entry],
            "failed_permutations": [],
        }
        if not legacy:
            payload.update(
                {
                    "shard_id": spec["shard_id"],
                    "permutation_indices": [index],
                }
            )
        return {"task_id": task_id, "phase": phase, "sweep_payload": payload}

    # This is deliberately out of delivery order. a1 models a receipt written
    # before parent/shard identity was persisted, then rebound to its task spec.
    persisted_rows = [
        recorded(b1, 88.0),
        recorded(a1, None, legacy=True),
        recorded(b0, 91.0),
        recorded(a0, 70.0),
    ]
    lane.phase_results[phase] = persisted_rows[:-1]
    with pytest.raises(lab.DurableExecutionError, match="missing or has duplicate shard receipts"):
        lab._merge_phase_sweep_receipts(lane, phase=phase)

    # The final exact shard arrives after restart; already-terminal receipts are
    # reused rather than recreated, and delivery order is irrelevant.
    lane.phase_results[phase] = persisted_rows

    merged = lab._merge_phase_sweep_receipts(lane, phase=phase)
    assert [item["sweep_id"] for item in merged["parent_sweeps"]] == ["parent-a", "parent-b"]
    assert merged["best"]["child_job_id"] == "parent-b-000000"
    assert merged["best"]["score"] == 91.0
    assert lab._merge_phase_sweep_receipts(lane, phase=phase) == merged

    cross_parent = json.loads(json.dumps(lane.phase_results[phase]))
    cross_parent[0]["sweep_payload"]["sweep_id"] = "other-parent"
    lane.phase_results[phase] = cross_parent
    with pytest.raises(lab.DurableExecutionError, match="identity does not match task spec"):
        lab._merge_phase_sweep_receipts(lane, phase=phase)


class _IndicatorIndexCli:
    def __init__(self, ids: list[str]):
        self.ids = ids

    def run(self, args, **_kwargs):
        assert args == ["indicators", "--mode", "index"]
        return SimpleNamespace(parsed_json={"data": {"ids": self.ids}})


def test_seed_indicators_filter_unscaffoldable_seed_plan_ids(
    tmp_path: Path,
    monkeypatch,
) -> None:
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "pair": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "SPEARMAN_RANK_CORRELATION",
                        "pair_sampling_weight": 1.0,
                    }
                ],
                "slot_menus": {
                    "trigger": [
                        {"indicator_id": "TTF_DSL_TRANSITION", "sampling_weight": 1.0},
                        {"indicator_id": "ADX", "sampling_weight": 1.0},
                    ]
                },
            }
        },
    }
    config = _test_config(tmp_path)
    monkeypatch.setattr(
        lab,
        "_load_play_hand_seed_plan",
        lambda _config, _seed_plan_path=None: (seed_plan, tmp_path / "seed-plan.json"),
    )

    indicators, loaded_seed_plan, _seed_plan_path = lab._seed_indicators(
        config=config,
        cli=_IndicatorIndexCli(["RSI", "ADX"]),
        campaign_ctx=_campaign_ctx(tmp_path),
        runtime=lab.PlayHandLabRuntimeConfig(min_indicators=2, max_indicators=2),
    )

    assert [indicator.id for indicator in indicators] == ["RSI", "ADX"]
    deal = lab._deal_lane(
        config=config,
        runtime=lab.PlayHandLabRuntimeConfig(
            min_indicators=2,
            max_indicators=2,
            instrument=["EURUSD"],
        ),
        seed_indicators=indicators,
        seed_plan=loaded_seed_plan,
        rng=random.Random(4),
    )

    assert set(deal["dealt"]) <= {"RSI", "ADX"}
    assert "SPEARMAN_RANK_CORRELATION" not in deal["dealt"]
    assert "TTF_DSL_TRANSITION" not in deal["dealt"]


def test_seed_indicators_uses_runtime_seed_plan_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    expected_path = tmp_path / "isolated" / "play-hand-seed-plan.json"
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "pair": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "ADX",
                        "pair_sampling_weight": 1.0,
                    }
                ],
                "slot_menus": {},
            }
        },
    }
    seen_paths: list[Path | None] = []

    def fake_load_seed_plan(_config, seed_plan_path=None):
        seen_paths.append(seed_plan_path)
        return seed_plan, expected_path

    monkeypatch.setattr(lab, "_load_play_hand_seed_plan", fake_load_seed_plan)

    indicators, loaded_seed_plan, loaded_seed_plan_path = lab._seed_indicators(
        config=_test_config(tmp_path),
        cli=_IndicatorIndexCli(["RSI", "ADX"]),
        campaign_ctx=_campaign_ctx(tmp_path),
        runtime=lab.PlayHandLabRuntimeConfig(
            seed_plan_path=expected_path,
            min_indicators=2,
            max_indicators=2,
        ),
    )

    assert seen_paths == [expected_path]
    assert loaded_seed_plan is seed_plan
    assert loaded_seed_plan_path == expected_path
    assert [indicator.id for indicator in indicators] == ["RSI", "ADX"]


def test_indicator_deal_metadata_is_json_safe_and_health_compatible() -> None:
    metadata = lab._indicator_deal_metadata(
        {
            "source": "play_hand_seed_plan",
            "reason": None,
            "recipe": "discovered_recipe_012",
            "recipe_source": "discovery_recipe_validation",
            "recipe_confidence": "high_candidate",
            "guided_recipe_source_bucket": "discovery_recipe_validation",
            "guided_recipe_source_bucket_matched": True,
            "guided_recipe_source_bucket_fallback": False,
            "indicators": [
                lab.SeedIndicator("MOM_MEAN_REVERSION"),
                {"indicator_id": "MFI_TREND"},
            ],
            "pair": {
                "anchor_id": "MOM_MEAN_REVERSION",
                "trigger_id": "MFI_TREND",
                "horizon_stability_bucket": "retained_36m",
            },
            "family_policy": {"family_policy": "template_guarded"},
            "policy_target_count": 2,
            "selected_slots": ["pair_menu"],
        }
    )

    assert metadata["indicator_deal"]["indicator_ids"] == ["MOM_MEAN_REVERSION", "MFI_TREND"]
    assert metadata["dealt_indicator_source"] == "play_hand_seed_plan"
    assert metadata["dealt_recipe"] == "discovered_recipe_012"
    assert metadata["dealt_recipe_source"] == "discovery_recipe_validation"
    assert metadata["dealt_recipe_pair"]["horizon_stability_bucket"] == "retained_36m"
    json.dumps(metadata)


def test_seed_indicators_reject_unscaffoldable_pinned_ids(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not scaffoldable"):
        lab._seed_indicators(
            config=_test_config(tmp_path),
            cli=_IndicatorIndexCli(["RSI", "ADX"]),
            campaign_ctx=_campaign_ctx(tmp_path),
            runtime=lab.PlayHandLabRuntimeConfig(
                indicator=["RSI", "SPEARMAN_RANK_CORRELATION"],
                min_indicators=2,
                max_indicators=2,
            ),
        )


def test_historical_seed_indicators_reject_undersized_plan_without_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    undersized_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "single": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [{"anchor_id": "RSI", "pair_sampling_weight": 1.0}],
                "slot_menus": {},
            }
        },
    }
    seed_plan_path = _write_historical_seed_plan(tmp_path, undersized_plan)
    runtime = _level_c_runtime(
        tmp_path,
        seed_plan_path=seed_plan_path,
        expected_seed_plan_sha256=lab._file_sha256(seed_plan_path),
        min_indicators=2,
        max_indicators=2,
    )

    def unexpected_seed_hand(*_args, **_kwargs):
        raise AssertionError("historical seed selection must not call _seed_hand")

    monkeypatch.setattr(lab, "_seed_hand", unexpected_seed_hand)
    with pytest.raises(RuntimeError, match="smaller than --min-indicators"):
        lab._seed_indicators(
            config=_test_config(tmp_path),
            cli=_IndicatorIndexCli(["RSI", "ADX", "MACD", "SMA"]),
            campaign_ctx=_campaign_ctx(tmp_path),
            runtime=runtime,
        )


def test_historical_v1_lane_deal_keeps_formal_guided_override(
    tmp_path: Path,
) -> None:
    fallback_plan = _historical_seed_plan()
    fallback_plan["sampling_policy"] = {"guided_prior_fraction": 0.0}
    runtime = _level_c_runtime(
        tmp_path,
        seed_plan_payload=fallback_plan,
        min_indicators=1,
        max_indicators=1,
    )

    indicators, seed_plan, seed_plan_path = lab._seed_indicators(
        config=_test_config(tmp_path),
        cli=_IndicatorIndexCli(["RSI", "ADX"]),
        campaign_ctx=_campaign_ctx(tmp_path),
        runtime=runtime,
    )

    deal = lab._deal_lane(
        config=_test_config(tmp_path),
        runtime=runtime,
        seed_indicators=indicators,
        seed_plan=seed_plan,
        rng=random.Random(7),
    )

    assert seed_plan_path == runtime.seed_plan_path
    assert deal["indicator_deal"]["source"] == "play_hand_seed_plan"
    assert deal["indicator_deal"].get("policy_lane") is None
    assert deal["indicator_deal"].get("policy_outcome_type") is None


def test_historical_v2_lane_deal_keeps_the_assigned_uncertain_lane(tmp_path: Path) -> None:
    seed_plan_payload = _policy_honest_seed_plan()
    runtime = _level_c_runtime(
        tmp_path,
        seed_plan_payload=seed_plan_payload,
        min_indicators=2,
        max_indicators=2,
        instrument=["EURUSD"],
    )
    indicators, seed_plan, _seed_plan_path = lab._seed_indicators(
        config=_test_config(tmp_path),
        cli=_IndicatorIndexCli(["RSI", "ADX", "MACD", "SMA"]),
        campaign_ctx=_campaign_ctx(tmp_path),
        runtime=runtime,
    )

    deal = lab._deal_lane(
        config=_test_config(tmp_path),
        runtime=runtime,
        seed_indicators=indicators,
        seed_plan=seed_plan,
        rng=random.Random(7),
        policy_lane="uncertain",
    )

    assert deal["indicator_deal"]["policy_lane"] == "uncertain"
    assert deal["indicator_deal"]["pair"]["canonical_pair_family_id"] == "family-macd-sma-m5"


def test_historical_lane_deal_rejects_role_balanced_fill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _level_c_runtime(tmp_path, min_indicators=1, max_indicators=1)
    monkeypatch.setattr(
        lab,
        "deal_seed_plan_indicators",
        lambda *_args, **_kwargs: {
            "source": "play_hand_seed_plan",
            "selected_slots": ["role_balanced_fill"],
            "indicators": [lab.SeedIndicator("RSI")],
        },
    )

    with pytest.raises(RuntimeError, match="rejects fallback indicator deals"):
        lab._deal_lane(
            config=_test_config(tmp_path),
            runtime=runtime,
            seed_indicators=[lab.SeedIndicator("RSI")],
            seed_plan=_historical_seed_plan(),
            rng=random.Random(7),
        )


def test_deep_replay_dry_run_uses_real_scaffold_for_profile_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ctx = SimpleNamespace(
        config=_test_config(tmp_path),
        dry_run=True,
        profiles_dir=tmp_path / "profiles",
        events_path=tmp_path / "events.jsonl",
        run_id="lane-run",
        io_lock=threading.RLock(),
    )
    ctx.profiles_dir.mkdir()
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-run",
        run_dir=tmp_path / "lane-run",
    )

    def fake_scaffold_profile(scaffold_ctx, indicator_ids, instruments, timeframe, candidate_name):
        assert scaffold_ctx.dry_run is False
        assert indicator_ids == ["RSI"]
        assert instruments == ["EURUSD"]
        assert timeframe == "M5"
        profile_path = ctx.profiles_dir / f"{candidate_name}.json"
        profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
        return profile_path

    monkeypatch.setattr(lab, "_scaffold_profile", fake_scaffold_profile)
    monkeypatch.setattr(
        lab,
        "_worker_ready_profile_snapshot",
        lambda profile_payload, **_kwargs: profile_payload,
    )

    lab._prepare_lane_profile(
        ctx,
        runtime=lab.PlayHandLabRuntimeConfig(task_mode="deep_replay", dry_run=True),
        lane=lane,
        seed_plan=None,
        deal={
            "dealt": ["RSI"],
            "dealt_entries": [lab.SeedIndicator("RSI")],
            "indicator_deal": {},
            "instruments": ["EURUSD"],
        },
        rng=random.Random(1),
    )

    assert lane.profile_path == ctx.profiles_dir / "lane_000_base.json"
    assert lane.indicator_ids == ["RSI"]


def test_deep_replay_tasks_are_self_contained_and_contract_pinned(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "runs" / "run-1",
        profile_path=profile_path,
        profile_payload=_profile_payload()["profile"],
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
        indicator_ids=["RSI"],
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        tasks_per_lane=1,
        bar_limit=250,
        worker_contract_hash="sha256:" + "a" * 64,
        seed=123,
    )
    lab._sample_lane_screen_anchor(lane, runtime)

    tasks = lab._build_tasks(
        [lane],
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
    )

    assert len(tasks) == 1
    task = tasks[0]
    payload = task["payload"]
    assert task["task_kind"] == "deep_replay"
    assert payload["job_id"] == task["task_id"]
    assert payload["required_worker_contract_hash"] == runtime.worker_contract_hash
    assert payload["required_worker_contract_schema"] == "replay-worker-contract-v1"
    assert payload["required_capabilities"] == ["deep_replay"]
    assert task["required_worker_capabilities"] == [
        "deep_replay",
        lab.PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
    ]
    assert payload["bar_limit"] == 250
    assert payload["inline_profile_snapshot"]["name"] == "Lab Smoke"
    assert payload["instruments"] == ["EURUSD"]
    assert payload["market_data_source"] == "lake_bars"
    assert payload["analysis_window_start"]
    assert payload["analysis_window_end"]
    assert payload["analysis_window_start"].endswith("Z")
    assert payload["analysis_window_end"].endswith("Z")
    assert payload["lookback_months"] is None
    assert payload["evidence_plan"]["evidence_role"] == "training"
    assert payload["evidence_plan"]["profile_snapshot_sha256"].startswith(
        "sha256:"
    )
    assert lane.screen_anchor_mode == "random"
    assert lane.screen_anchor_offset_days is not None
    assert lane.task_specs[task["task_id"]]["analysis_window_start"] == payload["analysis_window_start"]
    assert lane.task_specs[task["task_id"]]["analysis_window_end"] == payload["analysis_window_end"]


def _fake_window_binding(request, *, legacy_selection_manifest_sha256, **_kwargs):
    return LakeWindowBinding(
        request=request,
        window_semantic_sha256="sha256:" + "d" * 64,
        attestation_sha256="sha256:" + "e" * 64,
        creation_global_coverage_sha256="sha256:" + "f" * 64,
        legacy_selection_manifest_sha256=legacy_selection_manifest_sha256,
    )


def test_fixed_as_of_date_bounds_screen_validation_and_scrutiny(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lab, "resolve_lake_window_binding", _fake_window_binding)
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
        profile_path=tmp_path / "profile.json",
        profile_payload=_profile_payload()["profile"],
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        incumbent_profile_path=tmp_path / "profile.json",
        incumbent_profile_payload=_profile_payload()["profile"],
        incumbent_profile_ref="lab-inline:run-1:lane_000",
        incumbent_instruments=["EURUSD"],
        incumbent_timeframe="M5",
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        as_of_date="2025-06-30T00:00:00Z",
        lookback_months=3,
        validation_months=12,
        scrutiny_months=36,
        worker_contract_hash="sha256:" + "a" * 64,
    )
    lab._sample_lane_screen_anchor(lane, runtime)

    validation = lab._enqueue_validation_stage(
        lane,
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
    )[0]["payload"]
    scrutiny = lab._enqueue_final_stage(
        lane,
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
    )[0]["payload"]

    assert lane.screen_anchor_mode == "fixed_as_of"
    assert lane.screen_analysis_window_end == "2025-06-30T00:00:00Z"
    assert validation["analysis_window_end"] == "2025-06-30T00:00:00Z"
    assert validation["analysis_window_start"] == "2024-06-30T00:00:00Z"
    assert scrutiny["analysis_window_end"] == "2025-06-30T00:00:00Z"
    assert scrutiny["analysis_window_start"] == "2022-06-30T00:00:00Z"
    assert validation["evidence_plan"]["selection_data_end"] == validation["analysis_window_end"]
    assert scrutiny["evidence_plan"]["selection_data_end"] == scrutiny["analysis_window_end"]
    assert validation["lookback_months"] is None
    assert scrutiny["lookback_months"] is None
    assert validation["evidence_plan"]["data_availability_cutoff"] == runtime.as_of_date
    assert scrutiny["evidence_plan"]["data_availability_cutoff"] == runtime.as_of_date
    assert scrutiny["evidence_plan"]["evidence_role"] == "training"


def test_historical_replay_and_sweep_tasks_require_explicit_bounds_and_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    profile_payload = _profile_payload()["profile"]
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
        profile_path=profile_path,
        profile_payload=profile_payload,
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        lake_manifest_sha256="sha256:" + "b" * 64,
        worker_contract_hash="sha256:" + "a" * 64,
    )
    axis_texts = ["indicator[0].talib.timeperiod=7,14"]
    axis_plan = SimpleNamespace(
        axes=axis_texts,
        selected_permutations=2,
        event_payload=lambda: {"selected_axes": axis_texts},
    )
    monkeypatch.setattr(lab, "plan_sweep_axes", lambda *args, **kwargs: axis_plan)
    monkeypatch.setattr(lab, "resolve_lake_window_binding", _fake_window_binding)

    with pytest.raises(ValueError, match="require explicit analysis window bounds"):
        lab._deep_replay_job_payload(
            task_id="missing-bounds",
            lane=lane,
            runtime=runtime,
            reward_matrix=None,
            worker_contract_hash=runtime.worker_contract_hash,
        )
    with pytest.raises(ValueError, match="require explicit analysis window bounds"):
        lab._sweep_definition_payload(
            lane=lane,
            runtime=runtime,
            reward_matrix=None,
            axes=[],
            instruments=["EURUSD"],
            profile_ref=lane.profile_ref,
            profile_payload=profile_payload,
            lookback_months=3,
            analysis_window_start=None,
            analysis_window_end=None,
            mode="deterministic",
        )

    def assert_historical_evidence(payload: dict) -> None:
        evidence_plan = payload["evidence_plan"]
        assert payload["analysis_window_start"]
        assert payload["analysis_window_end"] == runtime.as_of_date
        assert payload["lookback_months"] is None
        assert evidence_plan["evidence_role"] == "training"
        assert evidence_plan["selection_data_end"] == runtime.as_of_date
        assert evidence_plan["data_availability_cutoff"] == runtime.as_of_date
        assert evidence_plan["schema_version"] == "fuzzfolio.replay-evidence-plan.v2"
        assert evidence_plan["lake_manifest_sha256"] is None
        assert (
            evidence_plan["lake_window_binding"]["legacy_selection_manifest_sha256"]
            == runtime.lake_manifest_sha256
        )

    for phase, months in [
        ("baseline_3mo", 3),
        ("instrument_scout_EURUSD_12mo", 12),
        ("validation_12mo", 12),
        ("final_36mo", 36),
    ]:
        start, end = lab._runtime_as_of_window(runtime, months)
        task = lab._make_deep_replay_task(
            lane,
            phase=phase,
            runtime=runtime,
            reward_matrix=None,
            worker_contract_hash=runtime.worker_contract_hash,
            profile_payload=profile_payload,
            profile_path=profile_path,
            profile_ref=lane.profile_ref,
            instruments=["EURUSD"],
            timeframe="M5",
            lookback_months=months,
            analysis_window_start=start,
            analysis_window_end=end,
        )
        assert_historical_evidence(task["payload"])

    for phase in ["lookback_timing", "coarse_probe", "coarse_expand", "focused"]:
        start, end = lab._runtime_as_of_window(runtime, 3)
        tasks = lab._make_sweep_shard_tasks(
            lane,
            phase=phase,
            runtime=runtime,
            reward_matrix=None,
            worker_contract_hash=runtime.worker_contract_hash,
            profile_payload=profile_payload,
            profile_path=profile_path,
            profile_ref=lane.profile_ref,
            instruments=["EURUSD"],
            lookback_months=3,
            axis_texts=axis_texts,
            mode="deterministic",
            analysis_window_start=start,
            analysis_window_end=end,
        )
        assert tasks
        for task in tasks:
            assert_historical_evidence(task["payload"]["definition"])


def test_historical_execution_receipt_must_match_plan() -> None:
    plan = {
        "plan_id": "sha256:" + "a" * 64,
        "profile_snapshot_sha256": "sha256:" + "b" * 64,
        "execution_cell_sha256": None,
        "lake_manifest_sha256": "sha256:" + "c" * 64,
    }
    receipt = {
        "plan_id": plan["plan_id"],
        "profile_snapshot_sha256": plan["profile_snapshot_sha256"],
        "execution_cell_sha256": None,
        "observed_lake_manifest_sha256": plan["lake_manifest_sha256"],
    }

    assert lab._validated_execution_evidence(
        {"execution_evidence": receipt}, plan
    ) == receipt
    with pytest.raises(RuntimeError, match="omitted execution_evidence"):
        lab._validated_execution_evidence({}, plan)


def test_historical_v2_execution_receipt_validates_window_identity() -> None:
    request = {
        "schema_version": "fuzzfolio.market-data-window-request.v1",
        "dataset": "bars",
        "pairs": ["EURUSD"],
        "timeframes": ["M5"],
        "data_start": "2023-01-01T00:00:00Z",
        "data_end": "2026-01-01T00:00:00Z",
        "coverage_policy": "require_complete",
    }
    plan = {
        "plan_id": "sha256:" + "a" * 64,
        "profile_snapshot_sha256": "sha256:" + "b" * 64,
        "execution_cell_sha256": None,
        "lake_manifest_sha256": None,
        "lake_window_binding": {
            "request": request,
            "window_semantic_sha256": "sha256:" + "c" * 64,
            "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
            "attestation_sha256": "sha256:" + "d" * 64,
        },
    }
    receipt = {
        "plan_id": plan["plan_id"],
        "profile_snapshot_sha256": plan["profile_snapshot_sha256"],
        "execution_cell_sha256": None,
        "expected_window_semantic_sha256": "sha256:" + "c" * 64,
        "observed_window_semantic_sha256": "sha256:" + "c" * 64,
        "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
        "expected_attestation_sha256": "sha256:" + "d" * 64,
        "lake_window_request": request,
    }

    assert lab._validated_execution_evidence(
        {"execution_evidence": receipt}, plan
    ) == receipt
    mutated = dict(receipt)
    mutated["observed_window_semantic_sha256"] = "sha256:" + "e" * 64
    with pytest.raises(RuntimeError, match="observed_window_semantic_sha256 mismatch"):
        lab._validated_execution_evidence({"execution_evidence": mutated}, plan)


def test_fake_compute_tasks_require_lab_protocol_capability(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "runs" / "run-1",
        instruments=["EURUSD"],
        timeframe="M5",
        indicator_ids=["RSI"],
    )

    tasks = lab._build_tasks(
        [lane],
        runtime=lab.PlayHandLabRuntimeConfig(task_mode="fake_compute", tasks_per_lane=1),
        reward_matrix=None,
    )

    payload = tasks[0]["payload"]
    assert payload["required_capabilities"] == [
        lab.PLAY_HAND_LAB_FAKE_COMPUTE_CAPABILITY,
        lab.PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
    ]


def test_play_hand_lab_validation_and_final_score_gates() -> None:
    runtime = lab.PlayHandLabRuntimeConfig(validation_min_score=45.0, final_min_score=40.0)

    validation = lab._validation_outcome(44.9, runtime)
    final = lab._lab_final_scrutiny_outcome(0.1, runtime)

    assert validation["passed"] is False
    assert validation["reason"] == "validation_score_below_45"
    assert "validation_12mo_failed" in validation["reasons"]
    assert lab._validation_outcome(45.0, runtime)["passed"] is True
    assert final["passed"] is False
    assert final["reason"] == "final_36mo_score_below_40"
    assert lab.PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON in final["reasons"]
    assert lab._lab_final_scrutiny_outcome(40.0, runtime)["passed"] is True


def test_play_hand_lab_validation_failure_tombstones_before_final(tmp_path: Path) -> None:
    fake_config = _test_config(tmp_path)
    lane_dir = fake_config.runs_root / "lane-validation-fail"
    lane_dir.mkdir(parents=True)
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    lane_ctx = _campaign_ctx(lane_dir)
    lane_ctx.attempts_path = lane_dir / "attempts.jsonl"
    runtime = lab.PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        pipeline_mode="play_hand",
        validation_min_score=45.0,
        worker_contract_hash="sha256:" + "a" * 64,
    )
    phase = lab._validation_phase(runtime)
    task_id = "lane-validation-fail-task-00001-validation_12mo"
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-validation-fail",
        run_dir=lane_dir,
        profile_path=profile_path,
        profile_payload=_profile_payload()["profile"],
        profile_ref="lab-inline:lane-validation-fail:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
        indicator_ids=["RSI"],
        incumbent_profile_path=profile_path,
        incumbent_profile_payload=_profile_payload()["profile"],
        incumbent_profile_ref="focused_top_3mo",
        incumbent_instruments=["EURUSD"],
        current_phase="validation",
    )
    lane.task_ids.append(task_id)
    lane.completed_task_ids.add(task_id)
    lane.phase_task_ids[phase] = [task_id]
    lab.write_run_metadata(lane_dir, {"run_status": "running"})

    follow_up = lab._advance_lane_after_result(
        config=fake_config,
        lane_ctx=lane_ctx,
        lane=lane,
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
        recorded={"phase": phase, "score": 44.0, "status": "success"},
    )

    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    assert follow_up == []
    assert lane.terminal is True
    assert lane.tombstone_reason == "validation_score_below_45"
    assert lane.terminal_outcome_category == lab.TERMINAL_OUTCOME_RESEARCH_NONVIABLE
    assert "validation_12mo_failed" in lane.tombstone_reasons
    assert metadata["run_status"] == "tombstoned"
    assert metadata["final_scrutiny_passed"] is False
    assert "final_36mo" not in lane.phase_task_ids


def test_missing_validation_score_is_typed_infrastructure_failure(tmp_path: Path) -> None:
    fake_config = _test_config(tmp_path)
    lane_dir = fake_config.runs_root / "lane-missing-score"
    lane_dir.mkdir(parents=True)
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    lane_ctx = _campaign_ctx(lane_dir)
    lane_ctx.attempts_path = lane_dir / "attempts.jsonl"
    runtime = lab.PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        pipeline_mode="play_hand",
        worker_contract_hash="sha256:" + "a" * 64,
    )
    phase = lab._validation_phase(runtime)
    task_id = "missing-score-validation"
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-missing-score",
        run_dir=lane_dir,
        incumbent_profile_path=profile_path,
        incumbent_profile_payload=_profile_payload()["profile"],
        incumbent_profile_ref="focused",
        incumbent_instruments=["EURUSD"],
        task_ids=[task_id],
        completed_task_ids={task_id},
        phase_task_ids={phase: [task_id]},
    )
    lab.write_run_metadata(lane_dir, {"run_status": "running"})

    assert lab._advance_lane_after_result(
        config=fake_config,
        lane_ctx=lane_ctx,
        lane=lane,
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=str(runtime.worker_contract_hash),
        recorded={"phase": phase, "score": None, "status": "success"},
    ) == []
    assert lane.terminal_outcome_category == lab.TERMINAL_OUTCOME_INFRASTRUCTURE_FAILURE
    assert lane.tombstone_reason == "canonical_score_missing"
    assert not lab._historical_lane_has_legitimate_terminal_outcome(lane)


def test_validated_no_valid_cell_is_typed_research_nonviability() -> None:
    plan = {
        "plan_id": "sha256:" + "1" * 64,
        "profile_snapshot_sha256": "sha256:" + "2" * 64,
        "execution_cell_sha256": None,
        "lake_manifest_sha256": "sha256:" + "3" * 64,
    }
    terminal = {
        "schema": "fuzzfolio-replay-terminal-result-v1",
        "status": "nonviable",
        "outcome": "no_valid_cell",
        "diagnostics": {
            "signal_count": 0,
            "resolved_trade_count_max": 0,
            "market_data_window": {"filtered_bar_count": 100},
        },
        "execution_evidence": {
            "plan_id": plan["plan_id"],
            "profile_snapshot_sha256": plan["profile_snapshot_sha256"],
            "execution_cell_sha256": None,
            "observed_lake_manifest_sha256": plan["lake_manifest_sha256"],
        },
    }
    result = {"status": "failed", "result": {"terminal_result": terminal}}
    assert lab._validated_no_valid_cell_terminal(result, plan) == terminal

    malformed = json.loads(json.dumps(result))
    malformed["result"]["terminal_result"]["diagnostics"]["signal_count"] = 1
    with pytest.raises(lab.DurableExecutionError, match="canonical diagnostics"):
        lab._validated_no_valid_cell_terminal(malformed, plan)


class _DurabilityFakeCli:
    def __init__(self, config) -> None:
        self.config = config


class _DurabilityFakeGateway:
    enqueued_task_ids: list[str] = []
    contradictory_duplicate = False
    mutate_enqueued_payload = False

    def __init__(self, **_kwargs) -> None:
        self.results: list[dict] = []

    def health(self) -> dict:
        return {"ok": True}

    def enqueue_tasks(self, tasks: list[dict]) -> dict:
        for task in tasks:
            task_id = str(task["task_id"])
            self.enqueued_task_ids.append(task_id)
            if self.mutate_enqueued_payload:
                nested_payload = task.get("payload")
                if isinstance(nested_payload, dict):
                    nested_payload["gateway_observation"] = {"mutated_after_enqueue": True}
            result = {
                "task_id": task_id,
                "lane_id": task["lane_id"],
                "attempt_id": task["attempt_id"],
                "status": "success",
                "worker_id": "durability-worker",
                "lease_id": f"lease-{len(self.enqueued_task_ids)}",
                "result": {"status": "success", "result": {"task_id": task_id}},
            }
            self.results.append(result)
            if self.contradictory_duplicate:
                duplicate = json.loads(json.dumps(result))
                duplicate["lease_id"] += "-duplicate"
                duplicate["result"]["result"]["contradiction"] = True
                self.results.append(duplicate)
        return {"enqueued": len(tasks)}

    def read_results(self, *, limit: int) -> list[dict]:
        return self.results[:limit]

    def drain_results(self, *, limit: int) -> list[dict]:
        return self.read_results(limit=limit)

    def ack_results(self, lease_ids: list[str]) -> int:
        requested = set(lease_ids)
        before = len(self.results)
        self.results = [row for row in self.results if row.get("lease_id") not in requested]
        return before - len(self.results)

    def snapshot(self) -> dict:
        return {"ok": True, "queued_tasks": len(self.results), "completed_tasks": 0, "metrics": {}}


class _RetainedResumeGateway:
    results: list[dict] = []
    queued_count = 0
    acked_count = 0
    ack_limit: int | None = None
    enqueue_observations: list[dict[str, int]] = []

    def __init__(self, **_kwargs) -> None:
        pass

    @classmethod
    def reset(cls) -> None:
        cls.results = []
        cls.queued_count = 0
        cls.acked_count = 0
        cls.ack_limit = None
        cls.enqueue_observations = []

    @staticmethod
    def _result(task: dict, sequence: int) -> dict:
        task_id = str(task["task_id"])
        return {
            "task_id": task_id,
            "lane_id": task["lane_id"],
            "attempt_id": task["attempt_id"],
            "status": "success",
            "worker_id": "retained-resume-worker",
            "lease_id": f"retained-lease-{sequence:05d}",
            "result": {"status": "success", "result": {"task_id": task_id}},
        }

    def health(self) -> dict:
        return {"ok": True}

    def enqueue_tasks(self, tasks: list[dict]) -> dict:
        type(self).enqueue_observations.append(
            {
                "result_backlog": len(type(self).results),
                "acked": type(self).acked_count,
                "task_count": len(tasks),
            }
        )
        start = type(self).acked_count + len(type(self).results)
        type(self).results.extend(
            self._result(task, start + offset + 1)
            for offset, task in enumerate(tasks)
        )
        type(self).queued_count = max(type(self).queued_count - len(tasks), 0)
        return {"enqueued": len(tasks)}

    def read_results(self, *, limit: int) -> list[dict]:
        return type(self).results[:limit]

    def ack_results(self, lease_ids: list[str]) -> int:
        accepted_ids = (
            lease_ids[: type(self).ack_limit]
            if type(self).ack_limit is not None
            else lease_ids
        )
        requested = set(accepted_ids)
        before = len(type(self).results)
        type(self).results = [
            row for row in type(self).results if row.get("lease_id") not in requested
        ]
        acknowledged = before - len(type(self).results)
        type(self).acked_count += acknowledged
        return acknowledged

    def snapshot(self) -> dict:
        return {
            "ok": True,
            "gateway_id": "retained-resume-gateway",
            "queued_tasks": type(self).queued_count,
            "completed_tasks": type(self).acked_count,
            "result_backlog": len(type(self).results),
            "worker_slots": 64,
            "metrics": {"results_acked": type(self).acked_count},
        }


def _durability_runtime(profile_path: Path, *, campaign_id: str, resume: bool = False):
    return lab.PlayHandLabRuntimeConfig(
        campaign_id=campaign_id,
        campaign_mode="finite",
        task_mode="fake_compute",
        pipeline_mode="screen",
        target_runs=1,
        active_runs=1,
        tasks_per_lane=1,
        profile_path=profile_path,
        indicator=["RSI"],
        fake_work_seconds=0.0,
        poll_interval_seconds=0.01,
        max_wait_seconds=1.0,
        resume=resume,
    )


@pytest.mark.parametrize(
    "checkpoint",
    [
        "after_index_reservation",
        "after_lane_registration",
        "after_task_registration",
        "before_gateway_enqueue",
    ],
)
def test_lane_index_crash_boundaries_resume_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    checkpoint: str,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    config = _test_config(tmp_path)
    _DurabilityFakeGateway.enqueued_task_ids = []
    _DurabilityFakeGateway.contradictory_duplicate = False
    monkeypatch.setattr(lab, "load_config", lambda: config)
    monkeypatch.setattr(lab, "FuzzfolioCli", _DurabilityFakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", _DurabilityFakeGateway)

    def crash(name: str) -> None:
        if name == checkpoint:
            raise RuntimeError(f"crash:{name}")

    monkeypatch.setattr(lab, "_lane_allocation_checkpoint", crash)
    with pytest.raises(RuntimeError, match=f"crash:{checkpoint}"):
        lab.cmd_play_hand_lab(_durability_runtime(profile_path, campaign_id=f"crash-{checkpoint}"))

    monkeypatch.setattr(lab, "_lane_allocation_checkpoint", lambda _name: None)
    assert lab.cmd_play_hand_lab(
        _durability_runtime(
            profile_path,
            campaign_id=f"crash-{checkpoint}",
            resume=True,
        )
    ) == 0
    assert len(_DurabilityFakeGateway.enqueued_task_ids) == 1
    assert len(set(_DurabilityFakeGateway.enqueued_task_ids)) == 1


def test_policy_honest_resume_after_crash_preserves_assignments_and_counters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    seed_plan_path = tmp_path / "policy-honest-seed-plan.json"
    seed_plan_path.write_text(json.dumps(_policy_honest_seed_plan()), encoding="utf-8")
    config = _test_config(tmp_path)
    _DurabilityFakeGateway.enqueued_task_ids = []
    _DurabilityFakeGateway.contradictory_duplicate = False
    monkeypatch.setattr(lab, "load_config", lambda: config)
    monkeypatch.setattr(lab, "FuzzfolioCli", _DurabilityFakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", _DurabilityFakeGateway)

    def runtime(*, resume: bool) -> lab.PlayHandLabRuntimeConfig:
        return lab.PlayHandLabRuntimeConfig(
            campaign_id="policy-crash-resume",
            campaign_mode="finite",
            target_runs=4,
            active_runs=4,
            task_mode="fake_compute",
            pipeline_mode="screen",
            tasks_per_lane=1,
            profile_path=profile_path,
            seed_plan_path=seed_plan_path,
            indicator=["RSI", "ADX", "MACD", "SMA"],
            min_indicators=2,
            max_indicators=2,
            instrument=["EURUSD"],
            seed=23,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=1.0,
            resume=resume,
        )

    monkeypatch.setattr(
        lab,
        "_lane_allocation_checkpoint",
        lambda name: (_ for _ in ()).throw(RuntimeError("crash:after_lane_registration"))
        if name == "after_lane_registration"
        else None,
    )
    with pytest.raises(RuntimeError, match="crash:after_lane_registration"):
        lab.cmd_play_hand_lab(runtime(resume=False))

    state_path = (
        config.derived_root
        / "play-hand-lab-campaigns"
        / "policy-crash-resume"
        / "play-hand-lab-state.json"
    )
    before_resume = json.loads(state_path.read_text(encoding="utf-8"))
    policy_before = before_resume["campaign_policy_state"]
    assert policy_before["planned_lane_counts"] == {
        "guided": 2,
        "uncertain": 1,
        "wild": 1,
    }
    assert policy_before["used_lane_counts"] == policy_before["planned_lane_counts"]
    assignments_before = [lane["policy_assignment"] for lane in before_resume["lanes"]]

    monkeypatch.setattr(lab, "_lane_allocation_checkpoint", lambda _name: None)
    assert lab.cmd_play_hand_lab(runtime(resume=True)) == 0

    after_resume = json.loads(state_path.read_text(encoding="utf-8"))
    assert [lane["policy_assignment"] for lane in after_resume["lanes"]] == assignments_before
    assert after_resume["campaign_policy_state"]["used_lane_counts"] == policy_before[
        "used_lane_counts"
    ]
    for lane_payload in after_resume["lanes"]:
        task_specs = lane_payload["task_specs"]
        assert task_specs
        assert all(
            spec["policy_assignment"] == lane_payload["policy_assignment"]
            for spec in task_specs.values()
        )
        attempts = lab.load_attempts(
            Path(lane_payload["run_dir"]) / "attempts.jsonl"
        )
        assert len(attempts) == 1
        assert attempts[0]["policy_assignment"] == lane_payload["policy_assignment"]
    assert len(_DurabilityFakeGateway.enqueued_task_ids) == 4
    assert len(set(_DurabilityFakeGateway.enqueued_task_ids)) == 4

    compacted = copy.deepcopy(after_resume)
    for lane_payload in compacted["lanes"]:
        if not lane_payload.get("terminal"):
            continue
        lane_payload["profile_payload"] = None
        lane_payload["incumbent_profile_payload"] = None
        lane_payload["last_sweep_payload"] = None
        lane_payload["instrument_scout_result"] = None
        lane_payload["task_specs"] = {}
        lane_payload["phase_rows"] = []
        lane_payload["phase_results"] = {}
    state_path.write_text(json.dumps(compacted), encoding="utf-8")
    journal_path = (
        config.derived_root
        / "play-hand-lab-campaigns"
        / "policy-crash-resume"
        / "play-hand-lab-execution-journal.json"
    )
    rewritten_lines: list[str] = []
    for raw in journal_path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        record = json.loads(raw)
        if record.get("record_type") == "register" and isinstance(record.get("payload"), dict):
            task_payload = dict(record["payload"])
            assignment = task_payload.pop("policy_assignment", None)
            nested_payload = task_payload.get("payload")
            if isinstance(assignment, dict):
                if not isinstance(nested_payload, dict):
                    nested_payload = {}
                    task_payload["payload"] = nested_payload
                nested_payload["policy_assignment"] = assignment
            record["payload"] = task_payload
            record["payload_sha256"] = lab.DurableExecutionJournal.task_payload_sha256(
                task_payload
            )
            body = dict(record)
            body.pop("record_sha256", None)
            record["record_sha256"] = lab.canonical_sha256(body)
        rewritten_lines.append(lab.canonical_json(record))
    journal_path.write_text("\n".join(rewritten_lines) + "\n", encoding="utf-8")

    journal_loads = 0
    journal_registers = 0
    real_journal_load = lab.DurableExecutionJournal.load
    real_journal_register = lab.DurableExecutionJournal.register

    def counted_journal_load(self, *args, **kwargs):
        nonlocal journal_loads
        journal_loads += 1
        return real_journal_load(self, *args, **kwargs)

    def counted_journal_register(self, *args, **kwargs):
        nonlocal journal_registers
        journal_registers += 1
        return real_journal_register(self, *args, **kwargs)

    with monkeypatch.context() as resume_patch:
        resume_patch.setattr(
            lab.DurableExecutionJournal,
            "load",
            counted_journal_load,
        )
        resume_patch.setattr(
            lab.DurableExecutionJournal,
            "register",
            counted_journal_register,
        )
        assert lab.cmd_play_hand_lab(runtime(resume=True)) == 0
    # Existing recovered tasks must be checked against the validated snapshot
    # rather than re-registering (and reloading) once per task.
    assert journal_loads <= 2
    assert journal_registers == 0
    compacted_after_resume = json.loads(state_path.read_text(encoding="utf-8"))
    assert all(
        lane_payload["task_specs"] == {}
        for lane_payload in compacted_after_resume["lanes"]
        if lane_payload.get("terminal")
    )

    for damage in ("counter", "task_assignment"):
        tampered = copy.deepcopy(after_resume)
        if damage == "counter":
            tampered["campaign_policy_state"]["used_lane_counts"]["guided"] += 1
            expected_error = "policy counters do not match persisted lane assignments"
        else:
            first_lane = tampered["lanes"][0]
            first_task = next(iter(first_lane["task_specs"].values()))
            first_task["policy_assignment"]["policy_lane"] = "wild"
            expected_error = "task policy assignment mismatch"
        state_path.write_text(json.dumps(tampered), encoding="utf-8")
        with pytest.raises(lab.DurableExecutionError, match=expected_error):
            lab.cmd_play_hand_lab(runtime(resume=True))

    state_path.write_text(json.dumps(after_resume), encoding="utf-8")
    validate_payload = lab._validate_task_result_receipt_payload
    validate_file = lab._validate_task_result_receipt

    def conflicting_terminal_receipt(validator, *args, **kwargs):
        receipt = copy.deepcopy(validator(*args, **kwargs))
        receipt["recorded_result"]["policy_assignment"]["policy_lane"] = "wild"
        return receipt

    monkeypatch.setattr(
        lab,
        "_validate_task_result_receipt_payload",
        lambda *args, **kwargs: conflicting_terminal_receipt(
            validate_payload, *args, **kwargs
        ),
    )
    monkeypatch.setattr(
        lab,
        "_validate_task_result_receipt",
        lambda *args, **kwargs: conflicting_terminal_receipt(
            validate_file, *args, **kwargs
        ),
    )
    with pytest.raises(
        lab.DurableExecutionError,
        match="terminal receipt policy assignment mismatch",
    ):
        lab.cmd_play_hand_lab(runtime(resume=True))


def test_durable_task_policy_assignment_reads_lab_task_envelope() -> None:
    assignment = {"policy_lane": "guided", "policy_outcome_type": "policy_lane_selected"}
    assert lab._durable_task_policy_assignment(
        {"task_id": "task-1", "policy_assignment": assignment}
    ) == assignment
    assert lab._durable_task_policy_assignment(
        {"task_id": "task-1", "payload": {"policy_assignment": assignment}}
    ) == assignment
    assert lab._durable_task_policy_assignment({"task_id": "task-1", "payload": {}}) is None


def _prepare_phase3_retained_resume_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    campaign_id: str,
    target_runs: int = 128,
    retained_count: int = 79,
) -> tuple[lab.PlayHandLabRuntimeConfig, list[dict]]:
    profile_path = tmp_path / f"{campaign_id}-profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    config = _test_config(tmp_path)
    _RetainedResumeGateway.reset()
    monkeypatch.setenv("PLAY_HAND_LAB_PREPARE_WORKERS", "2")
    monkeypatch.setattr(lab, "load_config", lambda: config)
    monkeypatch.setattr(lab, "FuzzfolioCli", _DurabilityFakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", _RetainedResumeGateway)

    runtime = replace(
        _durability_runtime(profile_path, campaign_id=campaign_id),
        target_runs=target_runs,
        active_runs=target_runs,
        formal_authority_kind="phase3",
        result_batch_size=16,
        max_results_per_cycle=32,
        strict_scoring=True,
        log_mode="quiet",
        max_wait_seconds=120.0,
    )
    monkeypatch.setattr(
        lab,
        "_lane_allocation_checkpoint",
        lambda name: (_ for _ in ()).throw(RuntimeError("stop-before-enqueue"))
        if name == "before_gateway_enqueue"
        else None,
    )
    with pytest.raises(RuntimeError, match="stop-before-enqueue"):
        lab.cmd_play_hand_lab(runtime)

    journal_path = (
        config.derived_root
        / "play-hand-lab-campaigns"
        / campaign_id
        / "play-hand-lab-execution-journal.json"
    )
    header = json.loads(
        next(
            line
            for line in journal_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    )
    journal_payload = lab.DurableExecutionJournal(
        journal_path,
        execution_id=str(header["execution_id"]),
        lineage=header["lineage"],
    ).load()
    durable_tasks = [
        dict(row["payload"])
        for _task_id, row in sorted(journal_payload["tasks"].items())
    ]
    assert len(durable_tasks) == target_runs
    _RetainedResumeGateway.results = [
        _RetainedResumeGateway._result(task, index + 1)
        for index, task in enumerate(durable_tasks[:retained_count])
    ]
    _RetainedResumeGateway.queued_count = target_runs - retained_count
    monkeypatch.setattr(lab, "_lane_allocation_checkpoint", lambda _name: None)
    return replace(runtime, resume=True), durable_tasks


def test_phase3_resume_drains_retained_results_before_any_enqueue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _durable_tasks = _prepare_phase3_retained_resume_fixture(
        tmp_path,
        monkeypatch,
        campaign_id="phase3-retained-ordering",
    )

    assert lab.cmd_play_hand_lab(runtime) == 0

    assert _RetainedResumeGateway.enqueue_observations
    assert _RetainedResumeGateway.enqueue_observations[0] == {
        "result_backlog": 0,
        "acked": 79,
        "task_count": 49,
    }
    assert _RetainedResumeGateway.acked_count == 128
    assert all(
        observation["result_backlog"] == 0 and observation["acked"] >= 79
        for observation in _RetainedResumeGateway.enqueue_observations
    )


def test_phase3_resume_batches_journal_and_campaign_state_rewrites(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retained_count = 79
    batch_size = 16
    runtime, _durable_tasks = _prepare_phase3_retained_resume_fixture(
        tmp_path,
        monkeypatch,
        campaign_id="phase3-retained-batched-writes",
        target_runs=retained_count,
        retained_count=retained_count,
    )
    journal_writes = 0
    state_writes = 0
    real_journal_append = lab.DurableExecutionJournal._append_records
    real_atomic_write_json = lab.atomic_write_json

    def counted_journal_append(self, records):
        nonlocal journal_writes
        journal_writes += 1
        return real_journal_append(self, records)

    def counted_atomic_write_json(path, payload):
        nonlocal state_writes
        if Path(path).name == "play-hand-lab-state.json":
            state_writes += 1
        return real_atomic_write_json(path, payload)

    monkeypatch.setattr(lab.DurableExecutionJournal, "_append_records", counted_journal_append)
    monkeypatch.setattr(lab, "atomic_write_json", counted_atomic_write_json)

    assert lab.cmd_play_hand_lab(runtime) == 0

    expected_batches = (retained_count + batch_size - 1) // batch_size
    assert journal_writes == expected_batches
    assert state_writes == expected_batches + 1  # one validated resume snapshot
    assert _RetainedResumeGateway.acked_count == retained_count


def test_phase3_resume_result_failure_prevents_enqueue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _durable_tasks = _prepare_phase3_retained_resume_fixture(
        tmp_path,
        monkeypatch,
        campaign_id="phase3-retained-failure",
    )
    monkeypatch.setattr(
        lab,
        "_record_lab_result",
        lambda **_kwargs: (_ for _ in ()).throw(
            lab.DurableExecutionError("retained result validation failed")
        ),
    )

    with pytest.raises(lab.DurableExecutionError, match="retained result validation failed"):
        lab.cmd_play_hand_lab(runtime)

    assert _RetainedResumeGateway.enqueue_observations == []
    assert _RetainedResumeGateway.acked_count == 0


def test_phase3_resume_unknown_retained_result_fails_without_ack_or_enqueue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _durable_tasks = _prepare_phase3_retained_resume_fixture(
        tmp_path,
        monkeypatch,
        campaign_id="phase3-retained-unknown",
    )
    _RetainedResumeGateway.results.insert(
        0,
        {
            "task_id": "unknown-retained-task",
            "lane_id": "unknown-lane",
            "attempt_id": "unknown-attempt",
            "status": "success",
            "worker_id": "retained-resume-worker",
            "lease_id": "unknown-retained-lease",
            "result": {"status": "success", "result": {}},
        },
    )

    with pytest.raises(lab.DurableExecutionError, match="references unknown task"):
        lab.cmd_play_hand_lab(runtime)

    assert _RetainedResumeGateway.enqueue_observations == []
    assert _RetainedResumeGateway.acked_count == 0
    assert _RetainedResumeGateway.results[0]["task_id"] == "unknown-retained-task"


@pytest.mark.parametrize("ack_limit", [0, 15])
def test_phase3_resume_partial_ack_blocks_enqueue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ack_limit: int,
) -> None:
    runtime, _durable_tasks = _prepare_phase3_retained_resume_fixture(
        tmp_path,
        monkeypatch,
        campaign_id="phase3-retained-partial-ack",
    )
    _RetainedResumeGateway.ack_limit = ack_limit

    with pytest.raises(lab.DurableExecutionError, match="could not be acknowledged"):
        lab.cmd_play_hand_lab(runtime)

    assert _RetainedResumeGateway.enqueue_observations == []
    assert _RetainedResumeGateway.acked_count == ack_limit
    assert len(_RetainedResumeGateway.results) == 79 - ack_limit


@pytest.mark.parametrize(
    "checkpoint",
    [
        "after_source_terminal_receipt",
        "after_derived_task_registration",
        "before_result_ack",
    ],
)
def test_phase3_resume_reconstructs_followup_after_record_before_ack_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    checkpoint: str,
) -> None:
    campaign_id = f"phase3-retained-followup-{checkpoint}"
    runtime, durable_tasks = _prepare_phase3_retained_resume_fixture(
        tmp_path,
        monkeypatch,
        campaign_id=campaign_id,
        target_runs=1,
        retained_count=1,
    )
    source_task = copy.deepcopy(durable_tasks[0])
    source_task_id = str(source_task["task_id"])
    derived_task_id = f"{source_task_id}-derived"

    def advance_with_one_followup(*, lane, recorded, **_kwargs):
        if str(recorded.get("task_id") or "") != source_task_id:
            return []
        derived = copy.deepcopy(source_task)
        derived["task_id"] = derived_task_id
        derived["attempt_id"] = derived_task_id
        derived["payload"]["task_id"] = derived_task_id
        derived["payload"]["attempt_id"] = derived_task_id
        if derived_task_id not in lane.task_ids:
            lane.task_ids.append(derived_task_id)
        lane.task_specs[derived_task_id] = copy.deepcopy(derived["payload"])
        return [derived]

    monkeypatch.setattr(lab, "_advance_lane_after_result", advance_with_one_followup)
    monkeypatch.setattr(
        lab,
        "_result_consumption_checkpoint",
        lambda name: (_ for _ in ()).throw(RuntimeError(f"crash:{checkpoint}"))
        if name == checkpoint
        else None,
    )

    with pytest.raises(RuntimeError, match=f"crash:{checkpoint}"):
        lab.cmd_play_hand_lab(runtime)

    assert _RetainedResumeGateway.acked_count == 0
    assert _RetainedResumeGateway.enqueue_observations == []
    monkeypatch.setattr(lab, "_result_consumption_checkpoint", lambda _name: None)

    assert lab.cmd_play_hand_lab(runtime) == 0

    assert _RetainedResumeGateway.enqueue_observations[0] == {
        "result_backlog": 0,
        "acked": 1,
        "task_count": 1,
    }
    assert _RetainedResumeGateway.acked_count == 2
    journal_path = (
        tmp_path
        / "runs"
        / "derived"
        / "play-hand-lab-campaigns"
        / campaign_id
        / "play-hand-lab-execution-journal.json"
    )
    header = json.loads(
        next(
            line
            for line in journal_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    )
    journal = lab.DurableExecutionJournal(
        journal_path,
        execution_id=str(header["execution_id"]),
        lineage=header["lineage"],
    ).load()
    assert journal["tasks"][source_task_id]["status"] == "terminal"
    assert journal["tasks"][derived_task_id]["status"] == "terminal"


def _legacy_phase3_follow_on_receipt_fixture(tmp_path: Path) -> tuple[
    Path,
    dict,
    dict,
    dict,
    list[dict],
]:
    task_id = "phase3-lane-00023-task-00001-baseline_3mo"
    artifact_dir = tmp_path / "evals" / "eval_lab_baseline_3mo_fixture"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "result.json").write_text('{"status":"success"}', encoding="utf-8")
    lab_result = {
        "task_id": task_id,
        "worker_id": "legacy-worker",
        "lease_id": "legacy-lease",
        "result": {"status": "success", "result": {"task_id": task_id}},
    }
    worker_result_sha256 = lab._worker_result_identity(lab_result)
    policy_assignment = {"policy_lane": "guided", "policy_outcome_type": "selected"}
    recorded = {
        "task_id": task_id,
        "attempt_id": "phase3-lane-00023-attempt-00001",
        "artifact_dir": str(artifact_dir.resolve()),
        "score": 55.5022,
        "score_basis": "score_lab_v2_5_3",
        "status": "success",
        "phase": "baseline_3mo",
        "task_kind": "deep_replay",
        "profile_path": str(tmp_path / "profiles" / "lane_023_base.json"),
        "profile_ref": "lab-inline:phase3-lane-00023:lane_023",
        "instruments": ["AUDCHF"],
        "timeframe": "M15",
        "lookback_months": 3,
        "analysis_window_start": "2025-10-14T00:00:00Z",
        "analysis_window_end": "2026-01-14T00:00:00Z",
        "evidence_plan_id": "sha256:evidence",
        "evidence_role": "training",
        "policy_assignment": policy_assignment,
    }
    recovered_row = {
        "attempt_id": recorded["attempt_id"],
        "artifact_dir": recorded["artifact_dir"],
        "composite_score": recorded["score"],
        "score_basis": recorded["score_basis"],
        "lab_scoring_warning": None,
        "play_hand_phase": recorded["phase"],
        "lab_task_kind": recorded["task_kind"],
        "profile_path": recorded["profile_path"],
        "profile_ref": recorded["profile_ref"],
        "play_hand_selected_instruments": recorded["instruments"],
        "effective_timeframe": recorded["timeframe"],
        "requested_horizon_months": recorded["lookback_months"],
        "analysis_window_start": recorded["analysis_window_start"],
        "analysis_window_end": recorded["analysis_window_end"],
        "evidence_plan_id": recorded["evidence_plan_id"],
        "evidence_role": recorded["evidence_role"],
        "policy_assignment": policy_assignment,
        "lab_worker_result_sha256": worker_result_sha256,
    }
    derived_tasks = [
        {
            "task_id": "phase3-lane-00023-task-00002-lookback_timing-shard-0000",
            "attempt_id": "phase3-lane-00023-task-00002-lookback_timing-shard-0000",
            "payload": {"task_id": "phase3-lane-00023-task-00002-lookback_timing-shard-0000"},
        }
    ]
    receipt_path = artifact_dir / "task-result-receipt.json"
    lab._write_task_result_receipt(
        receipt_path,
        task_id=task_id,
        worker_result_sha256=worker_result_sha256,
        recorded_result=recorded,
    )
    legacy = json.loads(receipt_path.read_text(encoding="utf-8"))
    legacy["derived_tasks"] = copy.deepcopy(derived_tasks)
    # This is the exact interrupted predecessor shape: derived work was added
    # after the original v2 receipt hash had been computed.
    receipt_path.write_text(json.dumps(legacy, sort_keys=True), encoding="utf-8")
    return receipt_path, lab_result, recorded, recovered_row, derived_tasks


def test_phase3_legacy_follow_on_receipt_migrates_only_after_exact_proof(
    tmp_path: Path,
) -> None:
    receipt_path, lab_result, recorded, recovered_row, derived_tasks = (
        _legacy_phase3_follow_on_receipt_fixture(tmp_path)
    )

    proven = lab._legacy_phase3_receipt_recorded_result(
        receipt_path,
        task_id=recorded["task_id"],
        worker_result_sha256=recovered_row["lab_worker_result_sha256"],
        recovered_row=recovered_row,
    )
    assert proven == recorded

    migrated = lab._terminal_receipt_for_result(
        proven,
        lab_result,
        derived_tasks=derived_tasks,
        allow_legacy_phase3_receipt_migration=True,
    )
    assert migrated["derived_tasks"] == derived_tasks
    assert migrated["compatibility_migration"]["schema_version"] == (
        lab.PHASE3_LEGACY_FOLLOW_ON_RECEIPT_MIGRATION_SCHEMA
    )
    assert lab._validate_task_result_receipt(
        receipt_path,
        task_id=recorded["task_id"],
        worker_result_sha256=recovered_row["lab_worker_result_sha256"],
    ) == migrated

    # A crash after the durable upgrade but before ACK can only revalidate it;
    # it cannot derive a second or altered follow-on graph.
    assert lab._terminal_receipt_for_result(
        proven,
        lab_result,
        derived_tasks=derived_tasks,
        allow_legacy_phase3_receipt_migration=True,
    ) == migrated


def test_task_result_receipt_sealing_detaches_nested_inputs(tmp_path: Path) -> None:
    task_id = "phase3-lane-00024-task-00001-baseline_3mo"
    artifact_dir = tmp_path / "evals" / "eval_lab_baseline_3mo_detached"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "result.json").write_text('{"status":"success"}', encoding="utf-8")
    recorded = {
        "task_id": task_id,
        "artifact_dir": str(artifact_dir.resolve()),
        "policy_assignment": {"policy_lane": "guided"},
    }
    worker_result_sha256 = "sha256:" + "a" * 64

    receipt = lab._write_task_result_receipt(
        artifact_dir / "task-result-receipt.json",
        task_id=task_id,
        worker_result_sha256=worker_result_sha256,
        recorded_result=recorded,
    )
    recorded["policy_assignment"]["policy_lane"] = "mutated"

    assert receipt["recorded_result"]["policy_assignment"]["policy_lane"] == "guided"
    assert lab._validate_task_result_receipt(
        artifact_dir / "task-result-receipt.json",
        task_id=task_id,
        worker_result_sha256=worker_result_sha256,
    ) == receipt


def test_task_result_receipt_follow_on_update_is_resealed(tmp_path: Path) -> None:
    receipt_path, lab_result, recorded, _recovered_row, derived_tasks = (
        _legacy_phase3_follow_on_receipt_fixture(tmp_path)
    )
    initial = json.loads(receipt_path.read_text(encoding="utf-8"))
    initial.pop("derived_tasks")
    initial.pop("receipt_sha256")
    lab._persist_task_result_receipt(
        receipt_path,
        initial,
        task_id=recorded["task_id"],
        worker_result_sha256=lab._worker_result_identity(lab_result),
    )

    updated = lab._terminal_receipt_for_result(
        recorded,
        lab_result,
        derived_tasks=derived_tasks,
        allow_legacy_phase3_receipt_migration=True,
    )
    derived_tasks[0]["payload"]["task_id"] = "mutated-after-persist"

    assert updated["derived_tasks"][0]["payload"]["task_id"].endswith("shard-0000")
    assert lab._validate_task_result_receipt(
        receipt_path,
        task_id=recorded["task_id"],
        worker_result_sha256=lab._worker_result_identity(lab_result),
    ) == updated


def test_phase3_legacy_follow_on_receipt_normalizes_json_object_param_keys(
    tmp_path: Path,
) -> None:
    receipt_path, lab_result, recorded, recovered_row, _derived_tasks = (
        _legacy_phase3_follow_on_receipt_fixture(tmp_path)
    )
    derived_tasks = [
        {
            "task_id": "phase3-lane-00023-task-00002-lookback_timing-shard-0000",
            "attempt_id": "phase3-lane-00023-task-00002-lookback_timing-shard-0000",
            "payload": {
                "task_id": "phase3-lane-00023-task-00002-lookback_timing-shard-0000",
                "permutation_start": 0,
                "permutation_count": 2,
                "permutation_indices": [0, 1],
                "params_by_index": {
                    0: {"alpha": 0.0, "beta": 1},
                    1: {"alpha": 1.0, "beta": 2},
                },
            },
        },
        {
            "task_id": "phase3-lane-00023-task-00003-lookback_timing-shard-0001",
            "attempt_id": "phase3-lane-00023-task-00003-lookback_timing-shard-0001",
            "payload": {
                "task_id": "phase3-lane-00023-task-00003-lookback_timing-shard-0001",
                "permutation_start": 2,
                "permutation_count": 2,
                "permutation_indices": [2, 3],
                "params_by_index": {
                    2: {"alpha": 2.0, "beta": 3},
                    3: {"alpha": 3.0, "beta": 4},
                },
            },
        },
    ]
    legacy = json.loads(receipt_path.read_text(encoding="utf-8"))
    legacy["derived_tasks"] = copy.deepcopy(derived_tasks)
    receipt_path.write_text(json.dumps(legacy, sort_keys=True), encoding="utf-8")

    proven = lab._legacy_phase3_receipt_recorded_result(
        receipt_path,
        task_id=recorded["task_id"],
        worker_result_sha256=recovered_row["lab_worker_result_sha256"],
        recovered_row=recovered_row,
    )
    migrated = lab._terminal_receipt_for_result(
        proven,
        lab_result,
        derived_tasks=derived_tasks,
        allow_legacy_phase3_receipt_migration=True,
    )

    assert migrated["derived_tasks"] == derived_tasks
    assert lab._validate_task_result_receipt(
        receipt_path,
        task_id=recorded["task_id"],
        worker_result_sha256=recovered_row["lab_worker_result_sha256"],
    )["derived_tasks"] == derived_tasks


def test_phase3_legacy_follow_on_receipt_rejects_any_evidence_or_graph_drift(
    tmp_path: Path,
) -> None:
    receipt_path, lab_result, recorded, recovered_row, derived_tasks = (
        _legacy_phase3_follow_on_receipt_fixture(tmp_path)
    )
    legacy = json.loads(receipt_path.read_text(encoding="utf-8"))
    legacy["recorded_result"]["score"] = 999.0
    receipt_path.write_text(json.dumps(legacy, sort_keys=True), encoding="utf-8")
    with pytest.raises(lab.DurableExecutionError, match="receipt evidence conflicts"):
        lab._legacy_phase3_receipt_recorded_result(
            receipt_path,
            task_id=recorded["task_id"],
            worker_result_sha256=recovered_row["lab_worker_result_sha256"],
            recovered_row=recovered_row,
        )

    legacy["recorded_result"]["score"] = recorded["score"]
    legacy["derived_tasks"][0]["payload"]["task_id"] = "different-derived-task"
    receipt_path.write_text(json.dumps(legacy, sort_keys=True), encoding="utf-8")
    proven = lab._legacy_phase3_receipt_recorded_result(
        receipt_path,
        task_id=recorded["task_id"],
        worker_result_sha256=recovered_row["lab_worker_result_sha256"],
        recovered_row=recovered_row,
    )
    with pytest.raises(lab.DurableExecutionError, match="follow-on graph conflicts"):
        lab._terminal_receipt_for_result(
            proven,
            lab_result,
            derived_tasks=derived_tasks,
            allow_legacy_phase3_receipt_migration=True,
        )


@pytest.mark.parametrize("damage", ["mutate", "delete"])
def test_resume_revalidates_terminal_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    config = _test_config(tmp_path)
    _DurabilityFakeGateway.enqueued_task_ids = []
    _DurabilityFakeGateway.contradictory_duplicate = False
    monkeypatch.setattr(lab, "load_config", lambda: config)
    monkeypatch.setattr(lab, "FuzzfolioCli", _DurabilityFakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", _DurabilityFakeGateway)
    campaign_id = f"receipt-{damage}"
    assert lab.cmd_play_hand_lab(_durability_runtime(profile_path, campaign_id=campaign_id)) == 0
    artifact = next(config.runs_root.glob("*-playhand-lab-lane-*-v1/evals/eval_lab_*/*result.json"))
    if damage == "mutate":
        artifact.write_text('{"mutated":true}', encoding="utf-8")
    else:
        artifact.unlink()

    with pytest.raises(lab.DurableExecutionError, match="artifact receipt verification failed"):
        lab.cmd_play_hand_lab(
            _durability_runtime(profile_path, campaign_id=campaign_id, resume=True)
        )


def test_process_result_batch_rejects_contradictory_terminal_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    config = _test_config(tmp_path)
    _DurabilityFakeGateway.enqueued_task_ids = []
    _DurabilityFakeGateway.contradictory_duplicate = True
    monkeypatch.setattr(lab, "load_config", lambda: config)
    monkeypatch.setattr(lab, "FuzzfolioCli", _DurabilityFakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", _DurabilityFakeGateway)

    with pytest.raises(lab.DurableExecutionError, match="worker result identity conflicts"):
        lab.cmd_play_hand_lab(
            _durability_runtime(profile_path, campaign_id="contradictory-duplicate")
        )


def test_playhand_transition_and_resume_survive_gateway_payload_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    config = _test_config(tmp_path)
    _DurabilityFakeGateway.enqueued_task_ids = []
    _DurabilityFakeGateway.contradictory_duplicate = False
    monkeypatch.setattr(_DurabilityFakeGateway, "mutate_enqueued_payload", True)
    monkeypatch.setattr(lab, "load_config", lambda: config)
    monkeypatch.setattr(lab, "FuzzfolioCli", _DurabilityFakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", _DurabilityFakeGateway)

    campaign_id = "gateway-payload-mutation"
    assert lab.cmd_play_hand_lab(_durability_runtime(profile_path, campaign_id=campaign_id)) == 0
    assert lab.cmd_play_hand_lab(
        _durability_runtime(profile_path, campaign_id=campaign_id, resume=True)
    ) == 0


def test_deep_replay_rejects_duplicate_tasks_per_lane() -> None:
    with pytest.raises(ValueError, match="tasks-per-lane 1"):
        lab._normalize_runtime(
            lab.PlayHandLabRuntimeConfig(task_mode="deep_replay", tasks_per_lane=2)
        )


def test_worker_ready_profile_snapshot_converts_stored_profile(tmp_path: Path, monkeypatch) -> None:
    class FakeScoringProfile:
        @classmethod
        def model_validate(cls, _payload):
            raise ValueError("not full")

    class FakeFullProfile:
        def model_dump(self, *, mode: str):
            assert mode == "json"
            return {
                "name": "Lab Smoke",
                "description": "Test profile",
                "instruments": ["EURUSD"],
                "isActive": False,
                "notificationThreshold": 80,
                "directionMode": "both",
                "version": "v1",
                "indicators": [
                    {
                        "meta": {
                            "id": "RSI",
                            "instanceId": "test-rsi",
                            "name": "Relative Strength Index",
                            "namespace": "TA-Lib",
                            "talibFunction": "RSI",
                            "supportsTradingMode": True,
                            "usesRangeConfiguration": True,
                            "description": "RSI",
                            "inputs": [],
                            "valueRange": {"min": 0, "max": 100},
                        },
                        "config": {"timeframe": "M5"},
                    }
                ],
            }

    class FakeStoredProfile:
        @classmethod
        def model_validate(cls, payload):
            assert payload["indicators"][0]["meta"]["id"] == "RSI"
            return cls()

        def to_full_profile(self):
            return FakeFullProfile()

    monkeypatch.setattr(
        lab,
        "_load_fuzzfolio_profile_models",
        lambda **_kwargs: (FakeScoringProfile, FakeStoredProfile),
    )

    snapshot = lab._worker_ready_profile_snapshot(
        _profile_payload(),
        config=_test_config(tmp_path),
        runtime=lab.PlayHandLabRuntimeConfig(task_mode="deep_replay"),
    )

    assert snapshot["indicators"][0]["meta"]["name"] == "Relative Strength Index"


def test_play_hand_lab_fake_compute_writes_lane_attempts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        tasks: list[dict] = []
        results: list[dict] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "lane_id": task["lane_id"],
                            "attempt_id": task["attempt_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def drain_results(self, *, limit: int) -> list[dict]:
            drained = self.results[:limit]
            self.results = self.results[limit:]
            return drained

        def snapshot(self) -> dict:
            return {"ok": True, "completed_tasks": len(self.tasks), "queued_tasks": 0}

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=2,
            tasks_per_lane=2,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.1,
            max_wait_seconds=5.0,
        )
    )

    assert exit_code == 0
    campaign_dirs = list(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dirs = sorted(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    assert len(campaign_dirs) == 1
    assert len(lane_dirs) == 2

    summary = json.loads(
        (campaign_dirs[0] / "play-hand-lab-campaign-summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["status"] == "completed"
    assert summary["total_tasks"] == 4
    assert summary["completed_tasks"] == 4
    assert summary["generated_by_runner"] == lab.PLAY_HAND_LAB_RUNNER

    for lane_dir in lane_dirs:
        metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
        attempts = [
            json.loads(line)
            for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert metadata["generated_by_runner"] == lab.PLAY_HAND_LAB_RUNNER
        assert metadata["run_kind"] == "play_hand_lab_lane"
        assert metadata["completed_task_count"] == 2
        assert len(attempts) == 2
        assert {attempt["generated_by_runner"] for attempt in attempts} == {lab.PLAY_HAND_LAB_RUNNER}
        assert {attempt["attempt_role"] for attempt in attempts} == {"lab_smoke"}


def test_play_hand_lab_burst_drains_full_batches_and_coalesces_progress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)
    render_calls: list[Path] = []

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        read_limits: list[int] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "lane_id": task["lane_id"],
                            "attempt_id": task["attempt_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            type(self).read_limits.append(limit)
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "gateway_id": "stable",
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    FakeGateway.read_limits = []
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(
        lab,
        "render_progress_artifacts",
        lambda _attempts, output_path, **_kwargs: render_calls.append(output_path),
    )

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=4,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            result_batch_size=2,
            max_results_per_cycle=4,
            max_drain_seconds=60.0,
            poll_interval_seconds=5.0,
            max_wait_seconds=5.0,
        )
    )

    assert exit_code == 0
    assert FakeGateway.read_limits[:2] == [2, 2]
    assert len(render_calls) == 1


def test_play_hand_lab_retries_transient_result_read_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        read_calls = 0

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            type(self).read_calls += 1
            if type(self).read_calls == 1:
                raise requests.ConnectTimeout("temporary gateway accept stall")
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    FakeGateway.read_calls = 0
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.1,
            max_wait_seconds=2.0,
            result_read_failure_limit=3,
        )
    )

    assert exit_code == 0
    assert FakeGateway.read_calls >= 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    events = [
        json.loads(line)
        for line in (campaign_dir / "play-hand-lab-campaign-events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    assert summary["status"] == "completed"
    assert summary["completed_tasks"] == 1
    assert any(
        event["phase"] == "gateway"
        and event["status"] == "result_read_failed"
        and event["consecutive_failures"] == 1
        for event in events
    )


def test_play_hand_lab_rolls_finite_runs_with_active_run_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        enqueue_batches: list[int] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.enqueue_batches.append(len(tasks))
            start = len(self.tasks)
            self.tasks.extend(tasks)
            self.results.extend(
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{start + index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            )
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:1]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    FakeGateway.enqueue_batches = []
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            target_runs=5,
            active_runs=2,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    assert FakeGateway.enqueue_batches[0] == 2
    assert max(FakeGateway.enqueue_batches) <= 2
    assert sum(FakeGateway.enqueue_batches) == 5

    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["campaign_mode"] == "finite"
    assert summary["target_runs"] == 5
    assert summary["active_runs"] == 2
    assert summary["lane_count"] == 5
    assert summary["total_tasks"] == 5
    assert summary["completed_tasks"] == 5


def test_play_hand_lab_refreshes_gateway_snapshot_after_final_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.drain_calls = 0
            self.completed = False
            self.historical_completed = 100
            self.historical_results_dropped = 5

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            return {"enqueued": len(tasks)}

        def drain_results(self, *, limit: int) -> list[dict]:
            self.drain_calls += 1
            if self.drain_calls == 1 or self.completed:
                return []
            self.completed = True
            task = self.tasks[0]
            return [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
            ]

        def snapshot(self) -> dict:
            campaign_completed = len(self.tasks) if self.completed else 0
            return {
                "ok": True,
                "completed_tasks": self.historical_completed + campaign_completed,
                "queued_tasks": 0 if self.completed else len(self.tasks),
                "metrics": {
                    "completions_accepted": self.historical_completed + campaign_completed,
                    "results_dropped": self.historical_results_dropped,
                },
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    assert summary["completed_tasks"] == 1
    assert summary["gateway_snapshot"]["completed_tasks"] == 1
    assert summary["gateway_snapshot"]["raw_completed_tasks"] == 101
    assert summary["gateway_snapshot"]["metrics"]["results_dropped"] == 0
    assert summary["gateway_snapshot"]["raw_metrics"]["results_dropped"] == 5
    assert summary["gateway_snapshot"]["queued_tasks"] == 0


def test_play_hand_lab_records_terminal_worker_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            task = self.tasks[0]
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "failed",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "status": "failed",
                        "error": "simulated worker failure",
                    },
                }
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": 0,
                "failed_tasks": len(self.tasks),
                "queued_tasks": 0,
                "metrics": {},
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert summary["status"] == "failed"
    assert summary["completed_tasks"] == 0
    assert summary["failed_tasks"] == 1
    assert metadata["run_status"] == "failed"
    assert metadata["failed_task_count"] == 1
    assert attempts[0]["run_status"] == "failed"
    assert attempts[0]["score_basis"] == "lab_worker_failed"


def test_play_hand_lab_scoring_warning_fails_deep_replay_task(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            task = self.tasks[0]
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {"matrix": {"ok": True}},
                    },
                }
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks),
                "failed_tasks": 0,
                "queued_tasks": 0,
                "metrics": {},
            }

    def fake_prepare_lane_profile(lane_ctx, *, runtime, lane, seed_plan, deal, rng) -> None:
        profile_path = lane_ctx.profiles_dir / "profile.json"
        profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
        lane.profile_path = profile_path
        lane.profile_payload = _profile_payload()["profile"]
        lane.profile_ref = f"lab-inline:{lane.run_id}:{lane.lane_id}"
        lane.instruments = ["EURUSD"]
        lane.timeframe = "M5"
        lane.indicator_ids = ["RSI"]

    def fake_score_lab_artifact(*, cli, artifact_dir, strict):
        return (
            lab.AttemptScore(
                primary_score=None,
                composite_score=None,
                score_basis="lab_scoring_failed",
                metrics={},
                best_summary={"error": "simulated scoring failure"},
            ),
            {"error": "simulated scoring failure", "error_type": "RuntimeError"},
        )

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(lab, "_seed_indicators", lambda **_kwargs: (["RSI"], None, None))
    monkeypatch.setattr(lab, "_deal_lane", lambda **_kwargs: object())
    monkeypatch.setattr(lab, "_prepare_lane_profile", fake_prepare_lane_profile)
    monkeypatch.setattr(lab, "_score_lab_artifact", fake_score_lab_artifact)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="deep_replay",
            lanes=1,
            tasks_per_lane=1,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
            worker_contract_hash="sha256:" + "a" * 64,
        )
    )

    assert exit_code == 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert summary["status"] == "failed"
    assert summary["completed_tasks"] == 0
    assert summary["failed_tasks"] == 1
    assert attempts[0]["run_status"] == "failed"
    assert attempts[0]["score_basis"] == "lab_scoring_failed"
    assert attempts[0]["lab_scoring_warning"]["error"] == "simulated scoring failure"


def test_play_hand_lab_ack_failure_does_not_turn_success_into_failed_attempt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            task = self.tasks[0]
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            raise RuntimeError("transient ack failure")

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks),
                "failed_tasks": 0,
                "queued_tasks": 0,
                "metrics": {},
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    events = [
        json.loads(line)
        for line in (campaign_dir / "play-hand-lab-campaign-events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    assert summary["status"] == "completed"
    assert summary["completed_tasks"] == 1
    assert summary["failed_tasks"] == 0
    assert len(attempts) == 1
    assert attempts[0]["run_status"] == "screened"
    assert any(event["phase"] == "result_ack" and event["status"] == "failed" for event in events)


def test_play_hand_lab_summary_keeps_bounded_recorded_result_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    ack_calls: list[list[str]] = []

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "lane_id": task["lane_id"],
                            "attempt_id": task["attempt_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            ack_calls.append(list(lease_ids))
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks),
                "queued_tasks": 0,
                "metrics": {},
            }

    monkeypatch.setattr(lab, "SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT", 2)
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=3,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert summary["recorded_result_count"] == 3
    assert summary["recorded_results_sample_limit"] == 2
    assert summary["recorded_results_truncated"] is True
    assert len(summary["recorded_results"]) == 2
    assert len(attempts) == 3
    assert ack_calls == [["lease-0", "lease-1", "lease-2"]]


def test_play_hand_lab_fails_fast_when_gateway_result_read_dies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            raise requests.ConnectionError("gateway is gone")

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": 0,
                "queued_tasks": len(self.tasks),
                "metrics": {},
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    events = [
        json.loads(line)
        for line in (campaign_dir / "play-hand-lab-campaign-events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    assert summary["status"] == "gateway_unreachable"
    assert summary["recorded_result_count"] == 0
    assert any(event["phase"] == "gateway" and event["status"] == "result_read_failed" for event in events)


def test_play_hand_lab_pipeline_early_exits_after_bad_baseline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            start = len(self.tasks)
            self.tasks.extend(tasks)
            for index, task in enumerate(tasks):
                self.results.append(
                    {
                        "task_id": task["task_id"],
                        "lane_id": task["lane_id"],
                        "attempt_id": task["attempt_id"],
                        "status": "success",
                        "worker_id": "fake-worker",
                        "lease_id": f"lease-{start + index}",
                        "result": {
                            "job_id": task["task_id"],
                            "status": "success",
                            "result": {"aggregate": {"score_lab": {"score": -1.0}}},
                        },
                    }
                )
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [result for result in self.results if result.get("lease_id") not in requested]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    def fake_score_lab_artifact(*, cli, artifact_dir, strict):
        return (
            lab.AttemptScore(
                primary_score=-1.0,
                composite_score=-1.0,
                score_basis="test",
                metrics={"score_lab": -1.0},
                best_summary={"score_lab": {"score": -1.0}},
            ),
            None,
        )

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(lab, "_worker_ready_profile_snapshot", lambda profile, **_kwargs: profile)
    monkeypatch.setattr(lab, "_score_lab_artifact", fake_score_lab_artifact)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="deep_replay",
            pipeline_mode="play_hand",
            target_runs=1,
            active_runs=1,
            profile_path=profile_path,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
            worker_contract_hash="sha256:" + "a" * 64,
        )
    )

    assert exit_code == 0
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert metadata["run_status"] == "tombstoned"
    assert metadata["tombstone_reason"] == lab.PLAY_HAND_EARLY_EXIT_TOMBSTONE_REASON
    assert metadata["completed_task_count"] == 1
    assert metadata["play_hand_phase_scores"]["baseline"] == -1.0
    assert len(attempts) == 1


def test_play_hand_lab_pipeline_promotes_good_lane_with_sweep_shards(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        enqueue_batches: list[list[str]] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def _score_for_task(self, task: dict, offset: int = 0) -> float:
            task_id = str(task["task_id"])
            if "baseline_3mo" in task_id:
                return 60.0
            if "lookback_timing" in task_id:
                return 62.0 + offset
            if "coarse_probe" in task_id:
                return 66.0 + offset
            if "coarse_expand" in task_id:
                return 68.0 + offset
            if "focused" in task_id:
                return 70.0 + offset
            if "validation_12mo" in task_id:
                return 65.0
            if "instrument_scout" in task_id:
                return 58.0 + offset
            if "final_36mo" in task_id:
                return 72.0
            return 55.0

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.enqueue_batches.append([str(task.get("task_kind")) for task in tasks])
            start = len(self.tasks)
            self.tasks.extend(tasks)
            for index, task in enumerate(tasks):
                task_kind = str(task.get("task_kind"))
                if task_kind == "sweep_shard":
                    task_payload = task.get("payload") or {}
                    params_by_index = dict(task_payload.get("params_by_index") or {})
                    permutation_results = []
                    for raw_index, params in params_by_index.items():
                        permutation_index = int(raw_index)
                        score = None
                        if not str(task_payload.get("shard_id") or "").endswith("-0001"):
                            score = self._score_for_task(task, offset=permutation_index % 3)
                        permutation_results.append(
                            {
                                "permutation_index": permutation_index,
                                "child_job_id": f"{task_payload['sweep_id']}-{permutation_index:06d}",
                                "status": "success",
                                "parameters": dict(params),
                                "result_detail": "summary",
                                "result": (
                                    {"result_detail": "summary", "aggregate": {"score_lab": {"score": score}}}
                                    if score is not None
                                    else {
                                        "result_detail": "summary",
                                        "full_result_omitted": True,
                                        "warnings": ["scoreless intermediate shard entry"],
                                    }
                                ),
                                "completed_at": "2026-06-20T00:00:00Z",
                            }
                        )
                    worker_result = {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "shard_id": (task.get("payload") or {}).get("shard_id"),
                            "sweep_id": (task.get("payload") or {}).get("sweep_id"),
                            "status": "success",
                            "started_at": "2026-06-20T00:00:00Z",
                            "completed_at": "2026-06-20T00:00:01Z",
                            "result_detail": "summary",
                            "permutation_results": permutation_results,
                            "failed_permutations": [],
                            "worker_attribution": {},
                        },
                    }
                else:
                    score = self._score_for_task(task)
                    worker_result = {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {"aggregate": {"score_lab": {"score": score}}},
                    }
                envelope = {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{start + index}",
                    "result": worker_result,
                }
                self.results.append(envelope)
                if task_kind == "sweep_shard" and str((task.get("payload") or {}).get("shard_id") or "").endswith("-0001"):
                    duplicate = json.loads(json.dumps(envelope))
                    duplicate["lease_id"] = f"lease-{start + index}-redelivery"
                    self.results.append(duplicate)
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [result for result in self.results if result.get("lease_id") not in requested]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    def fake_score_lab_artifact(*, cli, artifact_dir, strict):
        path = str(artifact_dir)
        score = 60.0
        if "validation_12mo" in path:
            score = 65.0
        if "instrument_scout" in path:
            score = 58.0
        if "final_36mo" in path:
            score = 72.0
        return (
            lab.AttemptScore(
                primary_score=score,
                composite_score=score,
                score_basis="test",
                metrics={"score_lab": score},
                best_summary={"score_lab": {"score": score}},
            ),
            None,
        )

    FakeGateway.enqueue_batches = []
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(lab, "_worker_ready_profile_snapshot", lambda profile, **_kwargs: profile)
    monkeypatch.setattr(lab, "_score_lab_artifact", fake_score_lab_artifact)

    runtime = lab.PlayHandLabRuntimeConfig(
        campaign_id="sweep-shard-redelivery",
        gateway_url="http://127.0.0.1:8799",
        task_mode="deep_replay",
        pipeline_mode="play_hand",
        target_runs=1,
        active_runs=1,
        profile_path=profile_path,
        max_sweep_permutations=4,
        coarse_probe_budget=2,
        sweep_shard_size=2,
        instrument_scout_size=1,
        instrument_scout_max_selected=1,
        poll_interval_seconds=0.01,
        max_wait_seconds=5.0,
        worker_contract_hash="sha256:" + "a" * 64,
    )
    exit_code = lab.cmd_play_hand_lab(runtime)

    assert exit_code == 0
    assert any("sweep_shard" in batch for batch in FakeGateway.enqueue_batches)

    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert metadata["run_status"] == "promoted"
    assert metadata["validation_months"] == 12
    assert metadata["validation_min_score"] == 45.0
    assert metadata["screen_anchor_mode"] == "random"
    assert metadata["screen_analysis_window_start"]
    assert metadata["screen_analysis_window_end"]
    assert metadata["final_scrutiny_passed"] is True
    assert metadata["final_scrutiny_score"] == 72.0
    assert metadata["completed_task_count"] > 1
    assert "coarse_halving" in metadata
    assert {
        "baseline",
        "lookback_top_3mo",
        "coarse_top_3mo",
        "validation_12mo",
        "final_36mo",
    } <= set(metadata["play_hand_phase_scores"])
    assert any(attempt["lab_task_kind"] == "sweep_shard" for attempt in attempts)
    sweep_attempts = [attempt for attempt in attempts if attempt["lab_task_kind"] == "sweep_shard"]
    assert len({attempt["lab_campaign_task_id"] for attempt in sweep_attempts}) == len(sweep_attempts)
    assert any(
        attempt["play_hand_phase"] == "validation_12mo"
        and attempt["requested_horizon_months"] == 12
        for attempt in attempts
    )
    assert any(
        attempt["play_hand_phase"] == "final_36mo"
        and attempt["requested_horizon_months"] == 36
        for attempt in attempts
    )
    initial_enqueue_batches = len(FakeGateway.enqueue_batches)
    assert lab.cmd_play_hand_lab(replace(runtime, resume=True)) == 0
    assert len(FakeGateway.enqueue_batches) == initial_enqueue_batches
