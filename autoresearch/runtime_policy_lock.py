"""Canonical runtime policy identities for frozen Level-C execution."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .evidence_plan import canonical_sha256
from .execution_costs import execution_cost_manifest_payload
from .scoring import CANONICAL_SCORE_LAB_VERSION


POLICY_LOCK_SCHEMA = "autoresearch-runtime-policy-lock-v1"
ENGINE_POLICY_ID = "fuzzfolio-replay-engine-v1"
SCORING_POLICY_ID = "score-lab-policy-v1"
COST_POLICY_ID = "autoresearch-execution-cost-policy-v1"


class RuntimePolicyLockError(RuntimeError):
    """Raised when live engine/scoring/cost policy differs from a frozen lock."""


def build_runtime_policy_lock(
    config: Any,
    *,
    worker_contract_sha256: str,
    trading_dashboard_root: Path | None = None,
) -> dict[str, Any]:
    """Derive policy identities from named, executable runtime surfaces."""
    research = getattr(config, "research", None)
    engine_surface = {
        "worker_contract_sha256": str(worker_contract_sha256),
        "contract_builder": "fuzzfolio_core.contracts.worker_contract.build_replay_worker_contract",
        "source_root_kind": "trading-dashboard" if trading_dashboard_root else "worker-contract-attested",
    }
    scoring_surface = {
        "score_lab_version": CANONICAL_SCORE_LAB_VERSION,
        "quality_score_preset": str(getattr(research, "quality_score_preset", "") or ""),
        "score_builder": "autoresearch.scoring.build_attempt_score",
    }
    cost_surface = {
        **execution_cost_manifest_payload(config),
        "cost_builder": "autoresearch.execution_costs.execution_cost_manifest_payload",
    }
    components = {
        "engine": {
            "policy_id": ENGINE_POLICY_ID,
            "policy_sha256": canonical_sha256(engine_surface),
            "surface": engine_surface,
        },
        "scoring": {
            "policy_id": SCORING_POLICY_ID,
            "policy_sha256": canonical_sha256(scoring_surface),
            "surface": scoring_surface,
        },
        "cost": {
            "policy_id": COST_POLICY_ID,
            "policy_sha256": canonical_sha256(cost_surface),
            "surface": cost_surface,
        },
    }
    payload: dict[str, Any] = {
        "schema_version": POLICY_LOCK_SCHEMA,
        "components": components,
    }
    payload["policy_lock_sha256"] = canonical_sha256(payload)
    return payload


def policy_lock_provenance(lock: Mapping[str, Any]) -> dict[str, str]:
    """Return the canonical generation/protocol identity fields for a lock."""
    components = lock.get("components") if isinstance(lock, Mapping) else None
    if not isinstance(components, Mapping):
        raise RuntimePolicyLockError("runtime policy lock has no components")
    try:
        engine = components["engine"]
        scoring = components["scoring"]
        cost = components["cost"]
        return {
            "engine_id": str(engine["policy_id"]),
            "engine_sha256": str(engine["policy_sha256"]),
            "scoring_policy_id": str(scoring["policy_id"]),
            "scoring_policy_sha256": str(scoring["policy_sha256"]),
            "cost_policy_id": str(cost["policy_id"]),
            "cost_policy_sha256": str(cost["policy_sha256"]),
        }
    except (KeyError, TypeError) as exc:
        raise RuntimePolicyLockError("runtime policy lock components are malformed") from exc


def validate_runtime_policy_lock(
    expected: Mapping[str, Any],
    config: Any,
    *,
    worker_contract_sha256: str,
    trading_dashboard_root: Path | None = None,
) -> dict[str, Any]:
    supplied = dict(expected)
    expected_identity = str(supplied.pop("policy_lock_sha256", ""))
    if supplied.get("schema_version") != POLICY_LOCK_SCHEMA:
        raise RuntimePolicyLockError("runtime policy lock schema mismatch")
    if expected_identity != canonical_sha256(supplied):
        raise RuntimePolicyLockError("runtime policy lock identity mismatch")
    supplied["policy_lock_sha256"] = expected_identity
    live = build_runtime_policy_lock(
        config,
        worker_contract_sha256=worker_contract_sha256,
        trading_dashboard_root=trading_dashboard_root,
    )
    if supplied != live:
        raise RuntimePolicyLockError("live engine, scoring, or cost policy differs from the frozen lock")
    return live
