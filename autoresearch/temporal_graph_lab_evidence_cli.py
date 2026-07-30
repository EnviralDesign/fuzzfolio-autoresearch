from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hmac
import json
from pathlib import Path
import sys
from typing import Any

from .evidence_plan import (
    build_execution_cell_sha256,
    build_replay_evidence_plan,
    canonical_sha256,
)
from .lake_window import LakeWindowRequest, resolve_replay_lake_window_request
from .lake_window_client import resolve_lake_window_binding
from .temporal_graph_lab import _normalize_execution_cell, _normalize_profile


AUTHORITY_SCHEMA = "temporal_graph_lab_window_authority_v1"
EVIDENCE_ROLE = "development_parity"
SCOPE_RESOLUTION_SCHEMA = "temporal_graph_lake_scope_resolution_v1"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def _scope_resolution_identity_payload(payload: dict[str, Any]) -> dict[str, Any]:
    value = dict(payload)
    value.pop("resolutionSha256", None)
    return value


def _load_scope_resolution(
    path: Path,
    *,
    profile: dict[str, Any],
    timeframe: str,
    analysis_window_start: str,
    analysis_window_end: str,
) -> dict[str, Any]:
    """Validate a FuzzFolio-produced resolved scope without importing its code.

    The evidence profile remains authored and is independently identity-bound.
    The Lake request is allowed to come only from the content-bound catalog
    resolution, never from a second sparse-profile derivation.
    """

    payload = _load_json(path)
    if payload.get("schemaVersion") != SCOPE_RESOLUTION_SCHEMA:
        raise ValueError("unsupported scope resolution schema")
    expected = canonical_sha256(_scope_resolution_identity_payload(payload))
    actual = str(payload.get("resolutionSha256") or "")
    if not hmac.compare_digest(actual, expected):
        raise ValueError("scope resolution SHA-256 does not match its contents")
    required = (
        "evidenceProfileSnapshotSha256",
        "temporalSourceProfileSha256",
        "resolvedProfileSnapshotSha256",
        "programSha256",
        "baseDecisionTimeframe",
        "analysisWindowStart",
        "analysisWindowEnd",
        "coveredWarmupMinutes",
        "catalogIndicatorRequirements",
        "lakeWindowRequest",
    )
    for field in required:
        if field not in payload:
            raise ValueError(f"scope resolution requires {field}")
    if payload["evidenceProfileSnapshotSha256"] != canonical_sha256(profile):
        raise ValueError("scope resolution evidence profile does not match --profile")
    if str(payload["baseDecisionTimeframe"]).upper() != str(timeframe).upper():
        raise ValueError("scope resolution base decision timeframe does not match --timeframe")
    if payload["analysisWindowStart"] != analysis_window_start:
        raise ValueError("scope resolution analysis window start does not match request")
    if payload["analysisWindowEnd"] != analysis_window_end:
        raise ValueError("scope resolution analysis window end does not match request")
    LakeWindowRequest.model_validate(payload["lakeWindowRequest"])
    return payload


def freeze_temporal_graph_lab_evidence(args: argparse.Namespace) -> dict[str, Any]:
    if args.confirm_non_reserved_development_window is not True:
        raise ValueError(
            "refusing to freeze evidence without --confirm-non-reserved-development-window"
        )
    authority_note = str(args.window_authority_note or "").strip()
    if not authority_note:
        raise ValueError("--window-authority-note is required")

    profile = _normalize_profile(_load_json(args.profile))
    instruments = list(profile["instruments"])
    execution_cell = _normalize_execution_cell(
        profile["executionConfig"]["exitPolicy"]["selectedCell"]
    )
    scope_resolution_path = getattr(args, "scope_resolution", None)
    scope_resolution: dict[str, Any] | None = None
    if scope_resolution_path is not None:
        scope_resolution = _load_scope_resolution(
            scope_resolution_path,
            profile=profile,
            timeframe=args.timeframe,
            analysis_window_start=args.analysis_window_start,
            analysis_window_end=args.analysis_window_end,
        )
        lake_request = LakeWindowRequest.model_validate(
            scope_resolution["lakeWindowRequest"]
        )
    else:
        lake_request = resolve_replay_lake_window_request(
            pairs=instruments,
            base_timeframe=args.timeframe,
            profile_snapshot=profile,
            analysis_window_start=args.analysis_window_start,
            analysis_window_end=args.analysis_window_end,
        )
    binding = resolve_lake_window_binding(
        lake_request,
        legacy_selection_manifest_sha256=args.legacy_selection_manifest_sha256,
        timeout_seconds=args.timeout_seconds,
    )
    execution_cell_sha256 = build_execution_cell_sha256(execution_cell)
    selection_data_end = args.selection_data_end or args.analysis_window_end
    data_availability_cutoff = (
        args.data_availability_cutoff or args.analysis_window_end
    )
    plan = build_replay_evidence_plan(
        evidence_role=EVIDENCE_ROLE,
        selection_data_end=selection_data_end,
        analysis_window_start=args.analysis_window_start,
        analysis_window_end=args.analysis_window_end,
        requested_horizon_months=args.requested_horizon_months,
        profile_snapshot=profile,
        campaign_plan_id=args.campaign_plan_id,
        execution_cell_sha256=execution_cell_sha256,
        lake_window_binding=binding,
        data_availability_cutoff=data_availability_cutoff,
        coverage_policy="require_complete",
    )
    plan_payload = plan.model_dump(mode="json", exclude_none=False)
    authority = {
        "schemaVersion": AUTHORITY_SCHEMA,
        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "authorityNote": authority_note,
        "confirmedNonReservedDevelopmentWindow": True,
        "evidenceRole": EVIDENCE_ROLE,
        "profilePath": str(args.profile.expanduser().resolve()),
        "profileSnapshotSha256": canonical_sha256(profile),
        "executionCellSha256": execution_cell_sha256,
        "analysisWindowStart": plan.analysis_window_start,
        "analysisWindowEnd": plan.analysis_window_end,
        "selectionDataEnd": plan.selection_data_end,
        "dataAvailabilityCutoff": plan.data_availability_cutoff,
        "requestedHorizonMonths": plan.requested_horizon_months,
        "lakeWindowRequest": lake_request.canonical_payload(),
        "lakeWindowBinding": binding.model_dump(mode="json", exclude_none=False),
        "evidencePlanId": plan.plan_id,
    }
    if scope_resolution is not None:
        authority.update(
            {
                "scopeResolutionPath": str(scope_resolution_path.expanduser().resolve()),
                "scopeResolutionSha256": scope_resolution["resolutionSha256"],
                "temporalSourceProfileSha256": scope_resolution[
                    "temporalSourceProfileSha256"
                ],
                "resolvedProfileSnapshotSha256": scope_resolution[
                    "resolvedProfileSnapshotSha256"
                ],
                "programSha256": scope_resolution["programSha256"],
                "coveredWarmupMinutes": scope_resolution["coveredWarmupMinutes"],
                "catalogIndicatorRequirements": scope_resolution[
                    "catalogIndicatorRequirements"
                ],
            }
        )
    _write_json(args.evidence_plan_out, plan_payload)
    _write_json(args.authority_out, authority)
    return {
        "schemaVersion": "temporal_graph_lab_evidence_freeze_result_v1",
        "evidencePlanPath": str(args.evidence_plan_out.expanduser().resolve()),
        "authorityPath": str(args.authority_out.expanduser().resolve()),
        "evidencePlanId": plan.plan_id,
        "windowSemanticSha256": binding.window_semantic_sha256,
        "attestationSha256": binding.attestation_sha256,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve and cryptographically verify one non-reserved development "
            "lake window, then freeze the strict v2 evidence plan used by the "
            "Stage 4 temporal graph Lab parity gate."
        )
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--timeframe", required=True)
    parser.add_argument("--analysis-window-start", required=True)
    parser.add_argument("--analysis-window-end", required=True)
    parser.add_argument("--requested-horizon-months", type=int, required=True)
    parser.add_argument("--selection-data-end")
    parser.add_argument("--data-availability-cutoff")
    parser.add_argument("--legacy-selection-manifest-sha256")
    parser.add_argument("--campaign-plan-id")
    parser.add_argument(
        "--scope-resolution",
        type=Path,
        help="Content-bound catalog-resolved temporal lake scope (required by Stage 5C).",
    )
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--evidence-plan-out", type=Path, required=True)
    parser.add_argument("--authority-out", type=Path, required=True)
    parser.add_argument("--window-authority-note", required=True)
    parser.add_argument(
        "--confirm-non-reserved-development-window",
        action="store_true",
        help=(
            "Required explicit assertion that the selected dates are development "
            "evidence and do not overlap any reserved unseen/scrutiny window."
        ),
    )
    return parser


def main() -> int:
    try:
        result = freeze_temporal_graph_lab_evidence(_parser().parse_args())
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schemaVersion": "temporal_graph_lab_evidence_freeze_error_v1",
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
