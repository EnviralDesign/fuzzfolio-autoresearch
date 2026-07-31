"""Build one bounded ATR-management candidate from admitted development evidence.

This is a preparation tool, not a search loop.  It rotates an existing v2
development evidence plan onto one content-bound scalar-management profile while
preserving the exact Lake window binding.  The separate temporal-search authority
command then freezes the finite candidate/window matrix.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys
from typing import Any, Mapping

from .evidence_plan import build_replay_evidence_plan, canonical_sha256
from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TemporalSearchContractError,
)


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalSearchContractError(f"{name} must be an object")
    return deepcopy(dict(value))


def _read(path: Path) -> dict[str, Any]:
    try:
        return _mapping(json.loads(path.read_text(encoding="utf-8")), name=str(path))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchContractError(f"could not read JSON file: {path}") from exc


def _write_immutable(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchContractError(f"refusing to overwrite divergent file: {path}")
    path.write_text(encoded, encoding="utf-8")


def _source_payload(task_document: Mapping[str, Any]) -> dict[str, Any]:
    payload = task_document.get("payload", task_document)
    return _mapping(payload, name="source task payload")


def _atr_candidate(source_profile: Mapping[str, Any]) -> dict[str, Any]:
    profile = deepcopy(dict(source_profile))
    indicators = profile.get("indicators")
    if not isinstance(indicators, list):
        raise TemporalSearchContractError("source profile indicators must be a list")
    existing_instances = {
        str((_mapping(item, name="indicator").get("meta") or {}).get("instanceId") or "")
        for item in indicators
    }
    instance_id = "atr_management_m5"
    if instance_id in existing_instances:
        raise TemporalSearchContractError(f"source profile already uses {instance_id!r}")
    indicators.append(
        {
            "meta": {
                "id": "ATR_VOLATILITY_FILTER",
                "instanceId": instance_id,
            },
            "config": {
                "isActive": True,
                "useFormingBar": False,
                "timeframe": "M5",
                "lookbackBars": 1,
                "ranges": {"buy": [20, 60], "sell": [20, 60]},
                "talibConfig": [{"name": "timeperiod", "value": 14}],
            },
        }
    )
    profile["indicators"] = indicators
    profile["name"] = f"{str(profile.get('name') or 'Temporal candidate')} — ATR management preflight"
    profile["description"] = (
        "Bounded Stage 5D-4/5D-5 preflight candidate. Entry protection and "
        "completed-bar trailing use only the declared raw ATR distance scalar."
    )
    profile["isActive"] = False
    profile["executionConfig"] = {
        "managementLibrary": {
            "version": "temporal_management_v1",
            "defaultPlanId": "atr_dynamic",
            "scalarBindings": [
                {
                    "id": "atr_distance",
                    "indicatorInstanceId": instance_id,
                    "outputKey": "atr_raw",
                    "valueKind": "price_distance",
                    "availability": "completed_bar",
                }
            ],
            "plans": [
                {
                    "id": "atr_dynamic",
                    "initialStop": {
                        "kind": "indicator_distance_multiple",
                        "bindingId": "atr_distance",
                        "multiple": 2.0,
                    },
                    "initialTarget": {"kind": "reward_multiple", "multiple": 2.0},
                    "trailingStop": {
                        "anchor": {"kind": "bar_close"},
                        "distance": {
                            "kind": "indicator_distance_multiple",
                            "bindingId": "atr_distance",
                            "multiple": 2.0,
                        },
                        "activation": {"kind": "immediate"},
                        "minimumStepInitialR": 0.1,
                    },
                }
            ],
        }
    }
    return profile


def build_preparation(args: argparse.Namespace) -> dict[str, Any]:
    if args.confirm_non_reserved_development_window is not True:
        raise TemporalSearchContractError(
            "--confirm-non-reserved-development-window is required"
        )
    source = _source_payload(_read(args.source_task))
    profile = _atr_candidate(
        _mapping(source.get("inline_profile_snapshot"), name="source profile")
    )
    instruments = profile.get("instruments")
    if not isinstance(instruments, list) or len(instruments) != 1:
        raise TemporalSearchContractError("source profile must have exactly one instrument")
    source_plan = _mapping(source.get("evidence_plan"), name="source evidence plan")
    if source_plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
        raise TemporalSearchContractError("source task requires a v2 evidence plan")
    binding = _mapping(
        source_plan.get("lake_window_binding"), name="source Lake window binding"
    )
    plan = build_replay_evidence_plan(
        evidence_role="development_parity",
        selection_data_end=source_plan.get("selection_data_end"),
        analysis_window_start=source_plan.get("analysis_window_start"),
        analysis_window_end=source_plan.get("analysis_window_end"),
        requested_horizon_months=int(source_plan.get("requested_horizon_months")),
        profile_snapshot=profile,
        campaign_plan_id=str(args.authority_label),
        execution_cell_sha256=None,
        lake_window_binding=binding,
        data_availability_cutoff=source_plan.get("data_availability_cutoff"),
        coverage_policy="require_complete",
    ).model_dump(mode="json", exclude_none=False)
    preparation = {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": str(args.authority_label),
        "workerContract": {
            "workerContractSha256": str(args.worker_contract_sha256),
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": [
            {
                "candidateId": str(args.candidate_id),
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "instrument": str(instruments[0]).upper(),
                "timeframe": str(source.get("timeframe") or "").upper(),
                "barLimit": int(args.bar_limit),
                "windowInputs": [
                    {"windowId": str(args.window_id), "evidencePlan": plan}
                ],
            }
        ],
        "developmentWindows": [
            {
                "windowId": str(args.window_id),
                "analysisWindowStart": plan["analysis_window_start"],
                "analysisWindowEnd": plan["analysis_window_end"],
            }
        ],
        "prohibitedEvidence": [
            {
                "windowId": str(args.prohibited_window_id),
                "analysisWindowStart": str(args.prohibited_window_start),
                "analysisWindowEnd": str(args.prohibited_window_end),
                "reason": str(args.prohibited_reason),
            }
        ],
        "bounds": {
            "maxCandidates": 1,
            "maxDevelopmentWindows": 1,
            "maxTasks": 1,
            "maxAttempts": 2,
            "deadlineSeconds": float(args.deadline_seconds),
        },
    }
    output_root = args.output_root.resolve()
    _write_immutable(output_root / "candidate-profile.json", profile)
    _write_immutable(output_root / "evidence-plan.json", plan)
    _write_immutable(output_root / "preparation.json", preparation)
    return {
        "schemaVersion": "temporal_graph_candidate_window_preflight_preparation_result_v1",
        "outputRoot": str(output_root),
        "candidateId": str(args.candidate_id),
        "sourceProfileSha256": canonical_sha256(profile),
        "evidencePlanId": plan["plan_id"],
        "lakeWindowSemanticSha256": binding["window_semantic_sha256"],
        "taskCount": 1,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare one finite ATR-management temporal-search preflight."
    )
    parser.add_argument("--source-task", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--authority-label", required=True)
    parser.add_argument("--candidate-id", default="atr-management-preflight")
    parser.add_argument("--window-id", default="development-window-a")
    parser.add_argument("--bar-limit", type=int, default=5000)
    parser.add_argument("--deadline-seconds", type=float, default=900.0)
    parser.add_argument("--worker-contract-sha256", required=True)
    parser.add_argument("--prohibited-window-id", default="reserved-and-future")
    parser.add_argument("--prohibited-window-start", required=True)
    parser.add_argument("--prohibited-window-end", required=True)
    parser.add_argument("--prohibited-reason", required=True)
    parser.add_argument("--confirm-non-reserved-development-window", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        result = build_preparation(_parser().parse_args(argv))
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schemaVersion": "temporal_graph_candidate_window_preflight_preparation_error_v1",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
