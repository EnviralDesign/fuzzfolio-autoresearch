from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .evidence_plan import (
    ReplayEvidencePlan,
    build_execution_cell_sha256,
    build_replay_evidence_plan,
    canonical_timestamp,
    canonical_sha256,
    normalize_evidence_profile_snapshot,
)


NESTED_EVIDENCE_SCHEMA = "autoresearch-nested-evidence-fold-v1"


class FrozenExecutionCellReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["autoresearch-frozen-execution-cell-v1"] = (
        "autoresearch-frozen-execution-cell-v1"
    )
    campaign_plan_id: str
    fold_id: str
    profile_snapshot_sha256: str
    train_evidence_plan_id: str
    selection_basis: Literal["best_cell", "recommended_cell", "robust_cell"]
    execution_cell: dict[str, float]
    execution_cell_sha256: str
    source: dict[str, Any] | None = None
    lake_manifest_sha256: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )

    @model_validator(mode="after")
    def _validate_cell_hash(self) -> "FrozenExecutionCellReceipt":
        observed = build_execution_cell_sha256(self.execution_cell)
        if observed != self.execution_cell_sha256:
            raise ValueError("execution-cell receipt hash mismatch")
        return self


class NestedEvidenceFold(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["autoresearch-nested-evidence-fold-v1"] = (
        NESTED_EVIDENCE_SCHEMA
    )
    campaign_plan_id: str
    fold_id: str
    train_plan: ReplayEvidencePlan
    cell_receipt: FrozenExecutionCellReceipt | None = None
    outer_test_plan: ReplayEvidencePlan | None = None
    embargo_days: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_sequence(self) -> "NestedEvidenceFold":
        if (self.cell_receipt is None) != (self.outer_test_plan is None):
            raise ValueError("cell receipt and outer-test plan must be frozen together")
        if self.cell_receipt is not None:
            if self.cell_receipt.train_evidence_plan_id != self.train_plan.plan_id:
                raise ValueError("cell receipt does not belong to the train plan")
            if (
                self.outer_test_plan is None
                or self.outer_test_plan.execution_cell_sha256
                != self.cell_receipt.execution_cell_sha256
            ):
                raise ValueError("outer-test plan is not bound to the frozen cell")
            if (
                self.cell_receipt.lake_manifest_sha256
                != self.train_plan.lake_manifest_sha256
                or self.outer_test_plan.lake_manifest_sha256
                != self.train_plan.lake_manifest_sha256
            ):
                raise ValueError("nested evidence lake coverage identity changed across stages")
        return self


def build_nested_train_fold(
    *,
    campaign_plan_id: str,
    fold_id: str,
    profile_snapshot: dict[str, Any],
    train_start: Any,
    train_end: Any,
    train_horizon_months: int,
    embargo_days: int,
    lake_manifest_sha256: str | None = None,
) -> NestedEvidenceFold:
    train_plan = build_replay_evidence_plan(
        campaign_plan_id=campaign_plan_id,
        evidence_role="cell_selection",
        selection_data_end=train_end,
        analysis_window_start=train_start,
        analysis_window_end=train_end,
        requested_horizon_months=train_horizon_months,
        profile_snapshot=profile_snapshot,
        lake_manifest_sha256=lake_manifest_sha256,
    )
    return NestedEvidenceFold(
        campaign_plan_id=campaign_plan_id,
        fold_id=fold_id,
        train_plan=train_plan,
        embargo_days=embargo_days,
    )


def freeze_nested_outer_test(
    fold: NestedEvidenceFold,
    *,
    profile_snapshot: dict[str, Any],
    selected_cell: dict[str, float],
    selection_basis: Literal["best_cell", "recommended_cell", "robust_cell"],
    test_start: Any,
    test_end: Any,
    test_horizon_months: int,
) -> NestedEvidenceFold:
    train_end_exclusive = datetime.fromisoformat(
        fold.train_plan.analysis_window_end.replace("Z", "+00:00")
    )
    normalized_test_start = canonical_timestamp(test_start)
    parsed_test_start = datetime.fromisoformat(normalized_test_start.replace("Z", "+00:00"))
    train_end_inclusive = train_end_exclusive.date() - timedelta(days=1)
    minimum_test_start = train_end_inclusive + timedelta(days=fold.embargo_days + 1)
    if parsed_test_start.date() < minimum_test_start:
        raise ValueError(
            "outer test must start after the configured train-window embargo"
        )
    if (
        canonical_sha256(normalize_evidence_profile_snapshot(profile_snapshot))
        != fold.train_plan.profile_snapshot_sha256
    ):
        raise ValueError("outer profile snapshot differs from the train plan")
    cell_hash = build_execution_cell_sha256(selected_cell)
    receipt = FrozenExecutionCellReceipt(
        campaign_plan_id=fold.campaign_plan_id,
        fold_id=fold.fold_id,
        profile_snapshot_sha256=fold.train_plan.profile_snapshot_sha256,
        train_evidence_plan_id=fold.train_plan.plan_id,
        selection_basis=selection_basis,
        execution_cell=selected_cell,
        execution_cell_sha256=cell_hash,
        lake_manifest_sha256=fold.train_plan.lake_manifest_sha256,
    )
    outer_plan = build_replay_evidence_plan(
        campaign_plan_id=fold.campaign_plan_id,
        evidence_role="outer_test",
        selection_data_end=fold.train_plan.analysis_window_end,
        analysis_window_start=normalized_test_start,
        analysis_window_end=test_end,
        requested_horizon_months=test_horizon_months,
        profile_snapshot=profile_snapshot,
        execution_cell_sha256=cell_hash,
        lake_manifest_sha256=fold.train_plan.lake_manifest_sha256,
    )
    return NestedEvidenceFold.model_validate(
        {
            **fold.model_dump(mode="json"),
            "cell_receipt": receipt.model_dump(mode="json"),
            "outer_test_plan": outer_plan.model_dump(mode="json"),
        }
    )
