from __future__ import annotations

import os
import random
import subprocess
import sys

import pytest

from autoresearch.temporal_search_quality import (
    TemporalSearchQualityError,
    _activation_for_capability,
    _cliffs_delta,
    select_control_candidate_ids,
)


def _population() -> list[dict]:
    candidates = []
    ordinal = 0
    for source_mode, seed_id, count in (
        ("de_novo", "alpha", 29),
        ("de_novo", "beta", 23),
        ("seed_derived", "alpha", 17),
        ("seed_derived", "gamma", 11),
    ):
        for index in range(count):
            candidates.append(
                {
                    "candidateId": f"td_{ordinal:028x}",
                    "sourceMode": source_mode,
                    "seedId": seed_id,
                    "irrelevantEconomics": 10_000 - index,
                    "sourceProfile": {"irrelevant": ordinal},
                }
            )
            ordinal += 1
    return candidates


def test_control_selection_is_stratified_and_order_independent() -> None:
    population = _population()
    excluded = [item["candidateId"] for item in population[::5]]
    expected, expected_strata = select_control_candidate_ids(
        population_candidates=population,
        excluded_candidate_ids=excluded,
        sample_size=32,
    )
    assert len(expected) == 32
    assert not (set(expected) & set(excluded))
    assert sum(row["allocatedCount"] for row in expected_strata) == 32
    assert all(row["allocatedCount"] >= 1 for row in expected_strata)

    variants = [list(reversed(population))]
    for seed in range(5):
        shuffled = list(population)
        random.Random(seed).shuffle(shuffled)
        variants.append(shuffled)
    for variant in variants:
        actual, actual_strata = select_control_candidate_ids(
            population_candidates=variant,
            excluded_candidate_ids=list(reversed(excluded)),
            sample_size=32,
        )
        assert actual == expected
        assert actual_strata == expected_strata


def test_control_selection_does_not_consume_profiles_or_economics() -> None:
    population = _population()
    excluded = [item["candidateId"] for item in population[:12]]
    expected, _ = select_control_candidate_ids(
        population_candidates=population,
        excluded_candidate_ids=excluded,
        sample_size=24,
    )
    mutated = []
    for index, candidate in enumerate(population):
        row = dict(candidate)
        row["irrelevantEconomics"] = -1_000_000 + index
        row["sourceProfile"] = {"entirely": "different", "index": index}
        mutated.append(row)
    actual, _ = select_control_candidate_ids(
        population_candidates=mutated,
        excluded_candidate_ids=excluded,
        sample_size=24,
    )
    assert actual == expected


def test_control_selection_is_hash_seed_independent() -> None:
    script = "\n".join(
        (
            "import json",
            "from autoresearch.temporal_search_quality import "
            "select_control_candidate_ids",
            "rows = [",
            " {'candidateId': f'td_{i:028x}',",
            "  'sourceMode': 'de_novo' if i % 3 else 'seed_derived',",
            "  'seedId': f'seed_{i % 7}'}",
            " for i in range(120)",
            "]",
            "excluded = [row['candidateId'] for row in rows[::4]]",
            "print(json.dumps(select_control_candidate_ids(",
            " population_candidates=rows,",
            " excluded_candidate_ids=excluded,",
            " sample_size=48,",
            "), sort_keys=True, separators=(',', ':')))",
        )
    )
    outputs = []
    for seed in ("1", "2", "3", "4"):
        environment = dict(os.environ)
        environment["PYTHONHASHSEED"] = seed
        outputs.append(
            subprocess.check_output(
                [sys.executable, "-c", script],
                env=environment,
                text=True,
            ).strip()
        )
    assert len(set(outputs)) == 1


def test_control_selection_rejects_impossible_minimum() -> None:
    population = [
        {
            "candidateId": f"td_{index:028x}",
            "sourceMode": "de_novo",
            "seedId": f"seed_{index}",
        }
        for index in range(5)
    ]
    with pytest.raises(TemporalSearchQualityError, match="at least one"):
        select_control_candidate_ids(
            population_candidates=population,
            excluded_candidate_ids=[],
            sample_size=4,
        )


def test_action_activation_is_bound_to_authored_transition() -> None:
    behavior = {
        "_transitionActionStatus": {
            ("entry_a", "enter_next_open", "scheduled"): 3,
            ("entry_a", "enter_next_open", "filled"): 2,
            ("entry_b", "enter_next_open", "scheduled"): 7,
            ("entry_b", "enter_next_open", "filled"): 7,
        },
        "closeReasonCounts": {},
    }
    aggregate = {"stateOccupancyDistribution": {"ready": 0.25}}
    row = _activation_for_capability(
        {
            "capability": "action:enter_next_open",
            "capabilityType": "enter_next_open",
            "transitionId": "entry_a",
            "sourceStateId": "ready",
        },
        behavior,
        aggregate,
    )
    assert row["actuallyEvaluated"] is True
    assert row["acceptedIntentOrEffectCount"] == 3
    assert row["positionChangeCount"] == 2
    assert row["neverActivated"] is False


def test_cliffs_delta_direction_is_explicit() -> None:
    assert _cliffs_delta([3, 4], [1, 2]) == 1.0
    assert _cliffs_delta([1, 2], [3, 4]) == -1.0
    assert _cliffs_delta([1, 2], [1, 2]) == 0.0
