from __future__ import annotations

from dataclasses import replace

import pytest

from autoresearch.config import load_config
from autoresearch.runtime_policy_lock import (
    RuntimePolicyLockError,
    build_runtime_policy_lock,
    policy_lock_provenance,
    validate_runtime_policy_lock,
)


def test_runtime_policy_lock_is_content_addressed_and_revalidates_live_surfaces() -> None:
    config = load_config()
    contract = "sha256:" + "a" * 64
    lock = build_runtime_policy_lock(config, worker_contract_sha256=contract)

    assert lock["policy_lock_sha256"].startswith("sha256:")
    assert set(policy_lock_provenance(lock)) == {
        "engine_id",
        "engine_sha256",
        "scoring_policy_id",
        "scoring_policy_sha256",
        "cost_policy_id",
        "cost_policy_sha256",
    }
    assert validate_runtime_policy_lock(
        lock, config, worker_contract_sha256=contract
    ) == lock


def test_runtime_policy_lock_fails_closed_on_contract_or_scoring_drift() -> None:
    config = load_config()
    lock = build_runtime_policy_lock(
        config, worker_contract_sha256="sha256:" + "a" * 64
    )
    with pytest.raises(RuntimePolicyLockError, match="differs"):
        validate_runtime_policy_lock(
            lock,
            config,
            worker_contract_sha256="sha256:" + "b" * 64,
        )

    changed = replace(
        config,
        research=replace(config.research, quality_score_preset="policy-drift"),
    )
    with pytest.raises(RuntimePolicyLockError, match="differs"):
        validate_runtime_policy_lock(
            lock,
            changed,
            worker_contract_sha256="sha256:" + "a" * 64,
        )
