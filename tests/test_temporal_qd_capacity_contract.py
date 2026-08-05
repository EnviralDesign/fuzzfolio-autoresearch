from __future__ import annotations

from copy import deepcopy

import pytest

import autoresearch.temporal_qd_supervisor as supervisor
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError


def _fresh_g0_config(*, pool: int = 4000, width: int = 1024, count: int = 5, first: int = 1) -> dict:
    return {
        "generationPlan": {
            "firstGenerationIndex": first,
            "generationCount": count,
            "targetUniqueCandidatesPerGeneration": width,
        },
        "g0Bootstrap": {
            "schemaVersion": "temporal_qd_g0_bootstrap_config_v1",
            "initialConstructionPoolSize": pool,
            "evaluationPopulationSize": width,
            "activation": "generation_1_pair_random_immigrants_only",
        },
    }


def test_fresh_g0_construction_capacity_is_pool_plus_later_normal_widths() -> None:
    config = _fresh_g0_config()
    contract = {"immigrantConstructionCandidateRequirement": 8096}
    assert supervisor._require_frozen_immigrant_capacity_requirement(config, contract) == 8096


def test_exact_g0_restart_reuses_the_frozen_construction_capacity() -> None:
    config = _fresh_g0_config()
    contract = {"immigrantConstructionCandidateRequirement": 8096}
    restarted = deepcopy(config)
    assert supervisor._require_frozen_immigrant_capacity_requirement(restarted, contract) == 8096


@pytest.mark.parametrize(
    "mutate",
    [
        lambda config: config["g0Bootstrap"].update({"initialConstructionPoolSize": 4001}),
        lambda config: config["generationPlan"].update({"targetUniqueCandidatesPerGeneration": 1023}),
        lambda config: config["generationPlan"].update({"generationCount": 4}),
        lambda config: config["generationPlan"].update({"firstGenerationIndex": 2}),
    ],
)
def test_g0_frozen_capacity_rejects_pool_width_or_generation_phase_drift(mutate) -> None:
    config = _fresh_g0_config()
    mutate(config)
    with pytest.raises(TemporalDiscoveryContractError):
        supervisor._require_frozen_immigrant_capacity_requirement(
            config, {"immigrantConstructionCandidateRequirement": 8096}
        )


def test_continuation_without_g0_retains_normal_width_capacity_semantics() -> None:
    continuation = {
        "generationPlan": {
            "firstGenerationIndex": 6,
            "generationCount": 4,
            "targetUniqueCandidatesPerGeneration": 1024,
        }
    }
    assert supervisor._require_frozen_immigrant_capacity_requirement(
        continuation, {"immigrantConstructionCandidateRequirement": 4096}
    ) == 4096
    # Pre-G0 normal blocks did not carry the explicit field and remain readable.
    assert supervisor._require_frozen_immigrant_capacity_requirement(continuation, {}) == 4096
