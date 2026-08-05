from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.temporal_prebroad_canary import audit_canary, run_canary
from autoresearch.temporal_prebroad_canary_artifacts import (
    CanaryArtifactError,
    GuardSatisfier,
    MAX_OBSERVATIONS,
    _published_pair_artifact,
    _public_context,
    build_artifacts,
    greedy_set_cover,
)
from autoresearch.temporal_qd_pair_factory import (
    default_hold_operator_policy,
    freeze_pair_run_config,
)


ROOT = Path(__file__).resolve().parents[1]
POPULATION = ROOT / ".tmp" / "codex-pair-admission-8-v2" / "population.json"
CONFIG = ROOT / ".tmp" / "codex-pair-smoke-config.json"
CANDIDATE = "qd_4ac39ee1171902318d0fccd88460"


def test_guard_satisfier_uses_only_closed_market_facts() -> None:
    facts = GuardSatisfier().satisfy({"kind": "all", "guards": [
        {"kind": "evidence_at_least", "groupId": "g", "thresholdPercent": 35.0},
        {"kind": "fresh_event", "eventId": "e"},
        {"kind": "position_exists", "expected": False},
    ]})
    assert facts == {"evidence": {"g": 100.0}, "freshEvents": ["e"]}
    with pytest.raises(CanaryArtifactError, match="unsupported"):
        GuardSatisfier().satisfy({"kind": "open_ended_unknown"})


def test_greedy_cover_is_stable_and_bounded() -> None:
    assert greedy_set_cover({"b": {"x"}, "a": {"x", "y"}, "c": {"y"}}, {"x", "y"}) == ["a"]
    with pytest.raises(CanaryArtifactError, match="incomplete"):
        greedy_set_cover({"a": {"x"}}, {"x", "missing"})


def test_public_context_accepts_the_frozen_normalized_spelling() -> None:
    normalized = {
        "instrument": "EURUSD",
        "indicators": [{"id": "indicator"}],
        "groups": [{"id": "group"}],
        "events": [{"id": "event"}],
        "executionConfig": {"managementPlans": []},
        "budgets": {"maxStates": 32},
    }
    assert _public_context(normalized) == {
        "instrument": "EURUSD",
        "indicators": [{"id": "indicator"}],
        "evidenceGroups": [{"id": "group"}],
        "eventBindings": [{"id": "event"}],
        "executionConfig": {"managementPlans": []},
        "budgets": {"maxStates": 32},
    }


def test_published_v1_artifact_omits_transient_transition_aliases() -> None:
    pair = {
        "candidateId": "candidate",
        "longModule": {
            "nativeArtifact": {},
            "transitionAliases": {"aliases": ["transient"]},
        },
        "shortModule": {
            "nativeArtifact": {},
            "transitionAliases": {"aliases": ["transient"]},
        },
    }
    published = _published_pair_artifact(pair)
    assert "transitionAliases" not in published["longModule"]
    assert "transitionAliases" not in published["shortModule"]
    assert "transitionAliases" in pair["longModule"]


def test_one_pair_build_runs_native_canary_and_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first.json"; second = tmp_path / "second.json"
    stale_config = json.loads(CONFIG.read_text(encoding="utf-8"))
    stale_config.pop("pairRunConfigSha256", None)
    stale_config["holdOperatorPolicy"] = default_hold_operator_policy()
    config = tmp_path / "pair-config-v2.json"
    config.write_text(
        json.dumps(freeze_pair_run_config(stale_config), sort_keys=True),
        encoding="utf-8",
    )
    one = build_artifacts(POPULATION, config, first, candidate_ids=[CANDIDATE])
    two = build_artifacts(POPULATION, config, second, candidate_ids=[CANDIDATE])
    assert one["inputSha256"] == two["inputSha256"]
    assert first.read_bytes() == second.read_bytes()
    payload = json.loads(first.read_text(encoding="utf-8"))
    pair = payload["pairs"][0]
    assert {row["expectedOutcome"] for row in pair["scenarios"]} == {"long", "short", "neither", "conflict_abstention"}
    long_stream = next(row["observationStream"] for row in pair["scenarios"] if row["expectedOutcome"] == "long")
    long_observations = long_stream["observations"]
    facts = [observation["event"]["facts"] for observation in long_observations]
    assert facts[1]["freshEvents"]
    assert max(facts[1]["evidenceScores"].values()) == 100.0
    assert max(facts[2]["evidenceScores"].values()) == 0.0
    # A fresh-event arm plus gate-delay enters its next watch state on the
    # low reset.  It must still receive high evidence afterward so an
    # enter_on_level fragment is satisfiable.
    assert max(facts[3]["evidenceScores"].values()) == 100.0
    assert all(len(row["observationStream"]["observations"]) <= MAX_OBSERVATIONS for row in pair["scenarios"])
    assert len(pair["productionClaims"]) == sum(len(pair[key]["program"]["fragments"]) for key in ("longModule", "shortModule"))
    result = run_canary(first, tmp_path / "run")
    assert result["marketEvidenceRead"] is result["lakeRead"] is result["gatewayContacted"] is False
    assert audit_canary(tmp_path / "run")["ok"] is True
