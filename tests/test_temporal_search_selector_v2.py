from __future__ import annotations

import random

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_search_selector_v2 import select_policy_v2
from autoresearch.temporal_search_selector_v2_admission import _synthetic_aggregate


def _population() -> list[dict]:
    return [
        {
            "candidateId": f"td_{index:028x}",
            "sourceMode": "broad_seed_mutation" if index % 2 else "seed_derived",
            "seedId": f"seed_{index % 8}",
            "sourceProfile": {"ignored": index},
            "ignoredEconomics": 10_000 - index,
        }
        for index in range(256)
    ]


def test_selector_v2_is_order_independent_and_has_exact_control() -> None:
    population = _population()
    aggregates = [_synthetic_aggregate(item) for item in population]
    expected = select_policy_v2(
        population_candidates=population,
        screening_aggregates=aggregates,
    )
    assert expected["eligibleCandidateCount"] >= 32
    assert expected["stratifiedControlCount"] == 32
    assert expected["confirmationCandidateCount"] <= 96
    assert not (
        set(expected["selectedCandidateIds"])
        & set(expected["stratifiedControlCandidateIds"])
    )
    variants = [(list(reversed(population)), list(reversed(aggregates)))]
    for seed in range(5):
        left = list(population)
        right = list(aggregates)
        random.Random(seed).shuffle(left)
        random.Random(seed + 100).shuffle(right)
        variants.append((left, right))
    for candidates, rows in variants:
        actual = select_policy_v2(
            population_candidates=candidates,
            screening_aggregates=rows,
        )
        assert actual["selectionSha256"] == expected["selectionSha256"]


def test_selector_v2_control_ignores_profiles_and_population_economics() -> None:
    population = _population()
    aggregates = [_synthetic_aggregate(item) for item in population]
    expected = select_policy_v2(
        population_candidates=population,
        screening_aggregates=aggregates,
    )
    mutated = []
    for index, item in enumerate(population):
        row = dict(item)
        row["sourceProfile"] = {"entirely": "different", "index": index}
        row["ignoredEconomics"] = -1_000_000 + index
        mutated.append(row)
    actual = select_policy_v2(
        population_candidates=mutated,
        screening_aggregates=aggregates,
    )
    assert actual["stratifiedControlCandidateIds"] == expected[
        "stratifiedControlCandidateIds"
    ]
    assert actual["selectionSha256"] == expected["selectionSha256"]


def test_selector_v2_fails_closed_when_robust_envelope_is_too_small() -> None:
    population = _population()
    aggregates = [_synthetic_aggregate(item) for item in population]
    for index, row in enumerate(aggregates):
        if index >= 10:
            row["tradeCountsByWindow"] = [0, 0]
            row["totalTrades"] = 0
            row["costDragR"] = 0.0
    with pytest.raises(TemporalDiscoveryContractError, match="refusing to relax"):
        select_policy_v2(
            population_candidates=population,
            screening_aggregates=aggregates,
        )


def test_management_activity_is_not_an_eligibility_gate() -> None:
    population = _population()
    aggregates = [_synthetic_aggregate(item) for item in population]
    for row in aggregates:
        row["managementActivationCount"] = 0
        row["rejectedIntentCount"] = 999
    result = select_policy_v2(
        population_candidates=population,
        screening_aggregates=aggregates,
    )
    assert result["eligibleCandidateCount"] >= 32
    assert all(
        "managementActivation" not in check
        and "rejectedIntent" not in check
        for row in result["eligibility"]
        for check in row["checks"]
    )
    assert all(
        row["archive"] == "diagnostic_pure_novelty_non_promotable"
        for row in result["diagnosticPureNoveltyArchive"]
    )
