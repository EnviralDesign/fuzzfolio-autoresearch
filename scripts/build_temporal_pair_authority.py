"""Freeze a production bidirectional pair authority from the live catalog.

This is deliberately a no-market operation.  It copies the complete current
indicator catalog into the run authority and creates a small, typed set of
setup/trigger resources from concrete catalog entries.  Search may later
substitute indicators and parameters through the frozen learning registry;
the seed resources are starting points, not evaluator special cases.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

from autoresearch.temporal_qd_pair_factory import (
    PAIR_RUN_CONFIG_SCHEMA,
    _write_immutable,
    default_hold_operator_policy,
    freeze_pair_run_config,
)


RESOURCE_ROLES = {
    "mean_reversion": {
        "setup": ("RSI_MEAN_REVERSION", "mean_reversion_setup"),
        "trigger": ("RSI_CROSSBACK", "mean_reversion_trigger"),
    },
    "trend": {
        "setup": ("MA_SLOPE_TREND", "trend_setup"),
        "trigger": ("PRICE_RECLAIM_MA", "trend_trigger"),
    },
    "breakout": {
        "setup": ("DONCHIAN_CHANNEL_BREAKOUT", "breakout_setup"),
        "trigger": ("BUFFERED_RANGE_BREAKOUT_SIGNAL", "breakout_trigger"),
    },
    "volume": {
        "setup": ("OBV_TREND", "volume_setup"),
        "trigger": ("NVO_VOLUME_IMPULSE", "volume_trigger"),
    },
}


def _catalog(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != {"indicators", "timeframes"}:
        raise ValueError("indicator catalog must have the exact indicators/timeframes schema")
    return value


def _context(catalog: dict, *, timeframe: str) -> dict:
    rows = {
        str(item.get("meta", {}).get("id") or ""): item
        for item in catalog["indicators"]
        if isinstance(item, dict)
    }
    indicators = []
    groups = []
    events = []
    for role, bindings in RESOURCE_ROLES.items():
        setup_id, setup_instance = bindings["setup"]
        trigger_id, trigger_instance = bindings["trigger"]
        for indicator_id, instance_id in (bindings["setup"], bindings["trigger"]):
            if indicator_id not in rows:
                raise ValueError(f"required catalog indicator is unavailable: {indicator_id}")
            indicator = copy.deepcopy(rows[indicator_id])
            # Catalog documentation is not authored profile material.  Keep
            # the complete meta/config authority but project away the UI-only
            # sibling before the grammar snapshots the indicator.
            indicator.pop("docs", None)
            indicator["meta"]["instanceId"] = instance_id
            indicator["config"]["timeframe"] = timeframe
            indicator["config"]["useFormingBar"] = False
            indicators.append(indicator)
        groups.append({"id": f"g_{role}", "indicatorInstanceIds": [setup_instance]})
        events.append(
            {
                "id": f"e_{role}",
                "indicatorInstanceId": trigger_instance,
                "longOutput": "bullish",
                "shortOutput": "bearish",
            }
        )
    return {
        "instrument": "EURUSD",
        "indicators": indicators,
        "evidenceGroups": groups,
        "eventBindings": events,
        "executionConfig": {
            "managementLibrary": {
                "version": "temporal_management_v1",
                "defaultPlanId": "base",
                "plans": [
                    {
                        "id": "base",
                        "initialStop": {"kind": "fixed_percent", "percent": 1.0},
                        "initialTarget": {"kind": "reward_multiple", "multiple": 2.0},
                    }
                ],
            }
        },
        "budgets": {
            "states": 16,
            "transitions": 63,
            "guardDepth": 4,
            "indicators": 16,
            "groups": 4,
            "events": 8,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--dashboard-root", type=Path, required=True)
    parser.add_argument("--dashboard-python", type=Path, required=True)
    parser.add_argument("--validator-script", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeframe", default="M5")
    args = parser.parse_args()

    dashboard_root = args.dashboard_root.resolve()
    catalog_path = args.catalog.resolve()
    expected_catalog_path = (
        dashboard_root / "shared" / "constants" / "indicators.json"
    ).resolve()
    if catalog_path != expected_catalog_path:
        raise ValueError(
            "--catalog must resolve to <dashboard-root>/shared/constants/indicators.json"
        )
    catalog = _catalog(catalog_path)
    if args.timeframe not in catalog["timeframes"]:
        raise ValueError(f"timeframe is absent from the current catalog: {args.timeframe}")
    context = _context(catalog, timeframe=args.timeframe)
    side = {
        "seedNames": ["mean_reversion", "breakout", "trend"],
        "context": context,
        "catalog": catalog,
        "policy": {
            "schemaVersion": "temporal_pair_catalog_seed_policy_v1",
            "resourceRoles": RESOURCE_ROLES,
            # These catalog labels select deterministic seed resources only.
            # Indicator-learning eligibility is derived from technical output
            # contracts in the frozen catalog, never from a role label.
            "resourceRoleDisposition": "seed_priors_only_v1",
        },
    }
    frozen = freeze_pair_run_config(
        {
            "schemaVersion": PAIR_RUN_CONFIG_SCHEMA,
            "longModule": side,
            "shortModule": copy.deepcopy(side),
            "nativeJsonlAuthority": {
                "command": [str(args.dashboard_python.resolve()), str(args.validator_script.resolve())],
                "timeoutSeconds": 60,
                "persistentJsonl": True,
                "maxLineBytes": 8 * 1024 * 1024,
                "stderrLimitBytes": 64 * 1024,
                "interpreterPath": str(args.dashboard_python.resolve()),
                "validatorScriptPath": str(args.validator_script.resolve()),
                "dashboardSourceRoot": str(dashboard_root),
                "environment": {"PYTHONPATH": [str((dashboard_root / "shared" / "python").resolve())]},
            },
            "holdOperatorPolicy": default_hold_operator_policy(),
        }
    )
    _write_immutable(args.output.resolve(), frozen)
    print(json.dumps({"output": str(args.output.resolve()), "pairRunConfigSha256": frozen["pairRunConfigSha256"], "catalogSha256": frozen["longModule"]["catalogSha256"], "seedNames": frozen["longModule"]["seedNames"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
