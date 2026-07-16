from __future__ import annotations

import hashlib
import json
from calendar import monthrange
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


EVIDENCE_PLAN_SCHEMA = "fuzzfolio.replay-evidence-plan.v1"
CoveragePolicy = Literal["require_complete", "allow_truncated"]
SELECTION_CONSUMING_ROLES = frozenset(
    {
        "training",
        "inner_validation",
        "cell_selection",
        "portfolio_selection",
        "full_backtest",
        "scrutiny",
    }
)


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def canonical_sha256(payload: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def normalize_evidence_profile_snapshot(
    profile_snapshot: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(profile_snapshot)
    threshold = normalized.get("notificationThreshold")
    if threshold is None:
        threshold = 80.0
    try:
        normalized["notificationThreshold"] = float(threshold)
    except (TypeError, ValueError) as exc:
        raise ValueError("profile notificationThreshold must be numeric") from exc
    return normalized


def build_execution_cell_sha256(execution_cell: Any) -> str:
    if hasattr(execution_cell, "model_dump"):
        execution_cell = execution_cell.model_dump(mode="json")
    if not isinstance(execution_cell, dict):
        raise ValueError("execution cell must be a JSON object")
    return canonical_sha256(execution_cell)


def canonical_timestamp(value: Any) -> str:
    token = str(value or "").strip()
    if not token:
        raise ValueError("timestamp is required")
    normalized = token[:-1] + "+00:00" if token.endswith("Z") else token
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def subtract_calendar_months(value: Any, months: int) -> str:
    parsed = datetime.fromisoformat(canonical_timestamp(value).replace("Z", "+00:00"))
    month_index = parsed.year * 12 + parsed.month - 1 - max(1, int(months))
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(parsed.day, monthrange(year, month)[1])
    return parsed.replace(year=year, month=month, day=day).isoformat().replace(
        "+00:00", "Z"
    )


class ReplayEvidencePlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["fuzzfolio.replay-evidence-plan.v1"] = EVIDENCE_PLAN_SCHEMA
    plan_id: str = Field(..., min_length=1)
    campaign_plan_id: str | None = None
    evidence_role: str = Field(..., min_length=1)
    selection_data_end: str
    analysis_window_start: str
    analysis_window_end: str
    requested_horizon_months: int = Field(..., ge=1, le=120)
    profile_snapshot_sha256: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    execution_cell_sha256: str | None = Field(
        default=None, pattern=r"^sha256:[0-9a-f]{64}$"
    )
    lake_manifest_sha256: str | None = Field(
        default=None, pattern=r"^sha256:[0-9a-f]{64}$"
    )
    data_availability_cutoff: str | None = None
    coverage_policy: CoveragePolicy = "require_complete"

    @field_validator(
        "selection_data_end",
        "analysis_window_start",
        "analysis_window_end",
        "data_availability_cutoff",
        mode="before",
    )
    @classmethod
    def _normalize_timestamp(cls, value: Any) -> Any:
        return None if value is None else canonical_timestamp(value)

    @field_validator("evidence_role", mode="before")
    @classmethod
    def _normalize_role(cls, value: Any) -> str:
        token = str(value or "").strip().lower().replace("-", "_")
        if not token:
            raise ValueError("evidence_role is required")
        return token

    def identity_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"plan_id"})

    def expected_plan_id(self) -> str:
        return canonical_sha256(self.identity_payload())

    @model_validator(mode="after")
    def _validate_contract(self) -> "ReplayEvidencePlan":
        start = datetime.fromisoformat(self.analysis_window_start.replace("Z", "+00:00"))
        end = datetime.fromisoformat(self.analysis_window_end.replace("Z", "+00:00"))
        selection_end = datetime.fromisoformat(self.selection_data_end.replace("Z", "+00:00"))
        if start >= end:
            raise ValueError("analysis_window_start must be earlier than analysis_window_end")
        if self.evidence_role in SELECTION_CONSUMING_ROLES and end > selection_end:
            raise ValueError(
                "selection-consuming evidence cannot extend beyond selection_data_end"
            )
        expected = self.expected_plan_id()
        if self.plan_id != expected:
            raise ValueError(
                f"evidence plan hash mismatch: expected {expected}, received {self.plan_id}"
            )
        return self


def build_replay_evidence_plan(
    *,
    evidence_role: str,
    selection_data_end: Any,
    analysis_window_start: Any,
    analysis_window_end: Any,
    requested_horizon_months: int,
    profile_snapshot: dict[str, Any],
    campaign_plan_id: str | None = None,
    execution_cell_sha256: str | None = None,
    lake_manifest_sha256: str | None = None,
    data_availability_cutoff: Any | None = None,
    coverage_policy: CoveragePolicy = "require_complete",
) -> ReplayEvidencePlan:
    normalized_profile_snapshot = normalize_evidence_profile_snapshot(profile_snapshot)
    normalized_payload = {
        "schema_version": EVIDENCE_PLAN_SCHEMA,
        "campaign_plan_id": campaign_plan_id,
        "evidence_role": str(evidence_role).strip().lower().replace("-", "_"),
        "selection_data_end": canonical_timestamp(selection_data_end),
        "analysis_window_start": canonical_timestamp(analysis_window_start),
        "analysis_window_end": canonical_timestamp(analysis_window_end),
        "requested_horizon_months": int(requested_horizon_months),
        "profile_snapshot_sha256": canonical_sha256(normalized_profile_snapshot),
        "execution_cell_sha256": execution_cell_sha256,
        "lake_manifest_sha256": lake_manifest_sha256,
        "data_availability_cutoff": (
            canonical_timestamp(data_availability_cutoff)
            if data_availability_cutoff is not None
            else None
        ),
        "coverage_policy": coverage_policy,
    }
    return ReplayEvidencePlan.model_validate(
        {"plan_id": canonical_sha256(normalized_payload), **normalized_payload}
    )


def validate_replay_evidence_plan(payload: Any) -> ReplayEvidencePlan:
    if isinstance(payload, ReplayEvidencePlan):
        return payload
    return ReplayEvidencePlan.model_validate(payload)


def enforce_replay_evidence_plan(
    plan: ReplayEvidencePlan | dict[str, Any],
    *,
    profile_snapshot: dict[str, Any],
    analysis_window_start: Any,
    analysis_window_end: Any,
    lookback_months: int | None,
    execution_cell: Any | None = None,
) -> ReplayEvidencePlan:
    resolved = validate_replay_evidence_plan(plan)
    if lookback_months is not None:
        raise ValueError("evidence-bound replay must not use lookback_months fallback")
    if canonical_timestamp(analysis_window_start) != resolved.analysis_window_start:
        raise ValueError("analysis_window_start does not match evidence plan")
    if canonical_timestamp(analysis_window_end) != resolved.analysis_window_end:
        raise ValueError("analysis_window_end does not match evidence plan")
    observed_profile = canonical_sha256(
        normalize_evidence_profile_snapshot(profile_snapshot)
    )
    if observed_profile != resolved.profile_snapshot_sha256:
        raise ValueError(
            "profile snapshot does not match evidence plan: "
            f"expected {resolved.profile_snapshot_sha256}, observed {observed_profile}"
        )
    if resolved.execution_cell_sha256 is not None:
        if execution_cell is None:
            raise ValueError("evidence plan requires a frozen execution cell")
        observed_cell = build_execution_cell_sha256(execution_cell)
        if observed_cell != resolved.execution_cell_sha256:
            raise ValueError(
                "execution cell does not match evidence plan: "
                f"expected {resolved.execution_cell_sha256}, observed {observed_cell}"
            )
    return resolved
