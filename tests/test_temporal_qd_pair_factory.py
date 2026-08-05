from __future__ import annotations

import json

import pytest

from autoresearch import temporal_qd_pair_factory as pair_factory_module
from autoresearch.temporal_bidirectional_genome import canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import _frozen_catalog_for_predeclared_scope
from autoresearch.temporal_typed_motif_grammar import (
    EntryRouteDecisionIndicatorCapError,
)


class _PreviewOperator:
    def preview(self, _profile, plan):
        return {"planSha256": plan["planSha256"]}


class _PreviewRegistry:
    def __init__(self, plans):
        self._plans = plans
        self._operator = _PreviewOperator()

    def enumerate_plans(self, _profile):
        return self._plans

    def get(self, operator_id):
        assert operator_id == "raw_trigger"
        return self._operator


def _indicator_factory() -> object:
    # `_apply_indicator_steps` is deliberately isolated from the native
    # authority.  Construct the smallest frozen-runtime-shaped object so this
    # test proves the selector/rejection semantics without subprocess I/O.
    factory = object.__new__(pair_factory_module._Factory)
    factory._cache_immutable_runtime = False
    factory.construction_policy = {"indicatorMutationDepthBuckets": [1]}
    return factory


def test_indicator_construction_skips_cap_invalid_preview_deterministically(
    monkeypatch,
) -> None:
    """A fourth entry condition cannot abort an otherwise legal immigrant.

    This is the exact construction shape behind the focused admission defect:
    a raw trigger preview may make a three-member fuzzy entry route illegal.
    The selector must continue through its fixed ordering, accept the first
    legal plan, and make the rejected-plan evidence identity-bound.
    """

    plans = [
        {
            "operatorId": "raw_trigger",
            "planSha256": "sha256:bad",
            "construction": {"kind": "raw_trigger"},
        },
        {
            "operatorId": "raw_trigger",
            "planSha256": "sha256:good",
            "construction": {"kind": "substitution"},
        },
    ]

    factory = _indicator_factory()
    ordered = factory._seeded_order(
        plans,
        seed="known-cap-rejection-seed",
        axis="indicator_plan_0_raw_trigger",
    )
    rejected_plan_sha256 = ordered[0]["planSha256"]
    accepted_plan_sha256 = ordered[1]["planSha256"]

    def validate(profile):
        if profile["planSha256"] == rejected_plan_sha256:
            raise EntryRouteDecisionIndicatorCapError(
                "entry decision route exceeds the distinct decision-indicator cap"
            )
        return {
            "observedMaximumDistinctDecisionIndicatorInstances": 3,
        }

    monkeypatch.setattr(
        pair_factory_module,
        "validate_entry_route_decision_indicator_cap",
        validate,
    )
    registry = _PreviewRegistry(ordered)

    first = factory._apply_indicator_steps(
        registry,
        {"kind": "base"},
        side_seed="known-cap-rejection-seed",
    )
    second = factory._apply_indicator_steps(
        registry,
        {"kind": "base"},
        side_seed="known-cap-rejection-seed",
    )

    assert first == second
    profile, trace, planned_depth, rejections = first
    assert planned_depth == 1
    assert profile == {"planSha256": accepted_plan_sha256}
    assert [item["planSha256"] for item in trace] == [accepted_plan_sha256]
    assert rejections == {
        "count": 1,
        "rowsSha256": canonical_sha256(
            [
                {
                    "step": 0,
                    "operatorId": "raw_trigger",
                    "planSha256": rejected_plan_sha256,
                }
            ]
        ),
    }


def test_indicator_construction_keeps_legal_parent_when_cap_exhausts_step(
    monkeypatch,
) -> None:
    """A finite all-invalid plan set stops the step rather than admitting it."""

    plan = {
        "operatorId": "raw_trigger",
        "planSha256": "sha256:bad",
        "construction": {"kind": "raw_trigger"},
    }
    monkeypatch.setattr(
        pair_factory_module,
        "validate_entry_route_decision_indicator_cap",
        lambda _profile: (_ for _ in ()).throw(
            EntryRouteDecisionIndicatorCapError(
                "entry decision route exceeds the distinct decision-indicator cap"
            )
        ),
    )

    profile, trace, planned_depth, rejections = _indicator_factory()._apply_indicator_steps(
        _PreviewRegistry([plan]),
        {"kind": "base"},
        side_seed="all-invalid-cap-seed",
    )

    assert profile == {"kind": "base"}
    assert trace == []
    assert planned_depth == 1
    assert rejections["count"] == 1


def test_pair_predeclared_scope_catalog_drift_fails_closed(tmp_path) -> None:
    catalog_path = tmp_path / "catalog.json"
    catalog = {"timeframes": {"M5": {}, "M30": {}}}
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
    context = {
        "orderedWindowPlanSemantic": [{"windowId": "development"}],
        "constructionCatalog": {
            "path": str(catalog_path),
            "catalogSha256": canonical_sha256(catalog),
        },
    }
    assert _frozen_catalog_for_predeclared_scope(context) == catalog
    catalog_path.write_text('{"timeframes":{"M5":{}}}', encoding="utf-8")
    with pytest.raises(TemporalDiscoveryContractError, match="catalog identity mismatch"):
        _frozen_catalog_for_predeclared_scope(context)
