from __future__ import annotations

import random

import autoresearch.temporal_generator_v2_continuation as continuation


def test_runtime_cache_is_source_identity_and_ordinal_bound(monkeypatch) -> None:
    monkeypatch.setattr(continuation, "_CONTINUATION_RUNTIME_CACHE", {})
    state = continuation._ContinuationRuntimeState(
        source_identity={"sourceIdentitySha256": "sha256:" + "a" * 64},
        parameters={"seed": 7},
        targets={"seed_derived": 1},
        mode_counts={"seed_derived": 1},
        seeds=[],
        absolute_start=12,
        next_continuation_ordinal=34,
        rng_state=random.Random(7).getstate(),
    )
    continuation._CONTINUATION_RUNTIME_CACHE["sha256:" + "a" * 64] = state

    assert continuation._cached_runtime_state(
        "sha256:" + "a" * 64, continuation_ordinal=34
    ) is state
    assert (
        continuation._cached_runtime_state(
            "sha256:" + "b" * 64, continuation_ordinal=34
        )
        is None
    )
    assert (
        continuation._cached_runtime_state(
            "sha256:" + "a" * 64, continuation_ordinal=35
        )
        is None
    )
