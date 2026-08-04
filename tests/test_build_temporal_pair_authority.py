from __future__ import annotations

import copy
import json
import sys

import pytest

from scripts import build_temporal_pair_authority as authority
from autoresearch import temporal_qd_pair_factory
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_factory import (
    PAIR_RUN_CONFIG_SCHEMA,
    default_immigrant_construction_policy,
    default_hold_operator_policy,
    freeze_pair_run_config,
    immigrant_capacity_audit,
)


ROLE_IDS = {
    "RSI_MEAN_REVERSION",
    "RSI_CROSSBACK",
    "MA_SLOPE_TREND",
    "PRICE_RECLAIM_MA",
    "DONCHIAN_CHANNEL_BREAKOUT",
    "BUFFERED_RANGE_BREAKOUT_SIGNAL",
    "OBV_TREND",
    "NVO_VOLUME_IMPULSE",
}


def _catalog(tmp_path):
    rows = []
    for indicator_id in sorted(ROLE_IDS):
        rows.append(
            {
                "meta": {
                    "id": indicator_id,
                    "label": f"{indicator_id} label",
                    "docs": {"description": "UI-only documentation"},
                },
                "config": {"timeframe": "M15", "useFormingBar": True},
            }
        )
    path = tmp_path / "catalog.json"
    path.write_text(
        json.dumps(
            {
                "indicators": rows,
                "timeframes": {
                    "M5": {"value": "M5"},
                    "M15": {"value": "M15"},
                    "H1": {"value": "H1"},
                },
            }
        ),
        encoding="utf-8",
    )
    return authority._catalog(path)


def test_catalog_context_and_freeze_are_catalog_bound(tmp_path, monkeypatch):
    catalog = _catalog(tmp_path)
    context = authority._context(catalog, timeframe="H1")

    assert set(
        item["meta"]["id"]
        for item in context["indicators"]
    ) == ROLE_IDS
    assert all("docs" not in item for item in context["indicators"])
    assert all(item["config"]["timeframe"] == "H1" for item in context["indicators"])
    assert all(item["config"]["useFormingBar"] is False for item in context["indicators"])
    assert {group["id"] for group in context["evidenceGroups"]} == {
        "g_mean_reversion",
        "g_trend",
        "g_breakout",
        "g_volume",
    }
    assert {event["id"] for event in context["eventBindings"]} == {
        "e_mean_reversion",
        "e_trend",
        "e_breakout",
        "e_volume",
    }

    fake_transport = {
        "command": ["python", "validator.py"],
        "timeoutSeconds": 60,
        "persistentJsonl": True,
        "maxLineBytes": 8 * 1024 * 1024,
        "stderrLimitBytes": 64 * 1024,
        "interpreterPath": "python",
        "validatorScriptPath": "validator.py",
        "dashboardSourceRoot": "dashboard",
        "environment": {"PYTHONPATH": ["dashboard/shared/python"]},
    }

    def fake_bound(value):
        return {
            **copy.deepcopy(value),
            "authorityContent": {
                "schemaVersion": "test_authority_content_v1",
                "dashboardSourceGitCommit": "0" * 40,
            },
        }

    monkeypatch.setattr(temporal_qd_pair_factory, "_bound_transport", fake_bound)
    side = {
        "seedNames": ["mean_reversion", "breakout", "trend"],
        "context": context,
        "catalog": catalog,
        "policy": {
            "schemaVersion": "temporal_pair_catalog_seed_policy_v1",
            "resourceRoles": authority.RESOURCE_ROLES,
        },
    }
    frozen = freeze_pair_run_config(
        {
            "schemaVersion": PAIR_RUN_CONFIG_SCHEMA,
            "longModule": side,
            "shortModule": copy.deepcopy(side),
            "nativeJsonlAuthority": fake_transport,
            "holdOperatorPolicy": default_hold_operator_policy(),
        }
    )

    assert frozen["longModule"]["seedNames"] == ["breakout", "mean_reversion", "trend"]
    assert frozen["shortModule"]["seedNames"] == frozen["longModule"]["seedNames"]
    assert frozen["longModule"]["context"] == frozen["shortModule"]["context"]
    assert frozen["longModule"]["context"]["indicators"] == context["indicators"]
    assert frozen["schemaVersion"] == PAIR_RUN_CONFIG_SCHEMA
    assert frozen["pairRunConfigSha256"].startswith("sha256:")
    assert frozen["holdOperatorPolicy"] == default_hold_operator_policy()
    assert (
        frozen["immigrantConstructionPolicy"]
        == default_immigrant_construction_policy()
    )
    assert frozen["operatorImplementation"]["schemaVersion"] == (
        "temporal_qd_pair_operator_implementation_v3"
    )
    assert frozen["operatorImplementation"]["richImmigrantBuilderVersion"] == (
        "temporal_qd_rich_immigrant_builder_v2"
    )

    rejected_policy = default_hold_operator_policy()
    rejected_policy["choices"][-1]["hours"] = 169.0
    with pytest.raises(TemporalDiscoveryContractError, match="closed admitted policy"):
        freeze_pair_run_config(
            {
                "schemaVersion": PAIR_RUN_CONFIG_SCHEMA,
                "longModule": side,
                "shortModule": copy.deepcopy(side),
                "nativeJsonlAuthority": fake_transport,
                "holdOperatorPolicy": rejected_policy,
            }
        )


def test_builder_requires_dashboard_runtime_catalog_path(tmp_path, monkeypatch):
    dashboard_root = tmp_path / "dashboard"
    canonical_path = dashboard_root / "shared" / "constants" / "indicators.json"
    canonical_path.parent.mkdir(parents=True)
    catalog = _catalog(tmp_path)
    canonical_path.write_text(json.dumps(catalog), encoding="utf-8")
    alternate_path = tmp_path / "alternate-catalog.json"
    alternate_path.write_text(json.dumps(catalog), encoding="utf-8")

    monkeypatch.setattr(
        authority,
        "freeze_pair_run_config",
        lambda _raw: {
            "pairRunConfigSha256": "sha256:test",
            "longModule": {
                "catalogSha256": "sha256:catalog",
                "seedNames": ["breakout", "mean_reversion", "trend"],
            },
        },
    )
    monkeypatch.setattr(authority, "_write_immutable", lambda *_args: None)

    def invoke(catalog_path):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "build_temporal_pair_authority.py",
                "--catalog",
                str(catalog_path),
                "--dashboard-root",
                str(dashboard_root),
                "--dashboard-python",
                str(tmp_path / "python.exe"),
                "--validator-script",
                str(tmp_path / "validator.py"),
                "--output",
                str(tmp_path / "frozen.json"),
            ],
        )
        return authority.main()

    with pytest.raises(ValueError, match="must resolve to <dashboard-root>"):
        invoke(alternate_path)
    assert invoke(canonical_path) == 0


def test_rich_immigrant_capacity_and_selector_entropy_cover_broad_campaign() -> None:
    side = {
        "seedNames": ["breakout", "mean_reversion", "trend"],
        "context": {
            "groups": [{"id": f"g_{index}"} for index in range(4)],
            "events": [{"id": f"e_{index}"} for index in range(4)],
            "plans": ["base"],
        },
    }
    frozen = {
        "pairRunConfigSha256": "sha256:" + "a" * 64,
        "longModule": side,
        "shortModule": copy.deepcopy(side),
        "holdOperatorPolicy": default_hold_operator_policy(),
        "immigrantConstructionPolicy": default_immigrant_construction_policy(),
    }
    audit = immigrant_capacity_audit(
        frozen, required_unique_candidates=4096
    )
    assert audit["pairExpressibleCapacityFloor"] == 746_496
    assert audit["uniqueSelectorFingerprintCount"] >= 4096
    assert audit["grammarAndIndicatorEntropyIncludedInCapacityFloor"] is False

    collapsed = copy.deepcopy(frozen)
    collapsed["holdOperatorPolicy"]["choices"] = [{"kind": "none"}]
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="expressible capacity",
    ):
        immigrant_capacity_audit(
            collapsed, required_unique_candidates=4096
        )
