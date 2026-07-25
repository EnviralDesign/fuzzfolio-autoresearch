from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any, Mapping

from .durable_execution import DurableExecutionError


logger = logging.getLogger(__name__)

_INSTALLED = False
_ORIGINAL_RECORD_LAB_RESULT: Any = None
_ORIGINAL_TERMINAL_RECEIPT_FOR_RESULT: Any = None
_ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD: Any = None


def _is_worker_identity_conflict(exc: BaseException) -> bool:
    text = str(exc).lower()
    return (
        "worker result identity conflicts" in text
        or "ledger result identity conflicts with duplicate task" in text
    )


def _authoritative_receipt_for_ledger_row(
    play_hand_lab: Any,
    *,
    task_id: str,
    recovered_row: Mapping[str, Any],
) -> dict[str, Any]:
    artifact_dir = Path(str(recovered_row.get("artifact_dir") or "")).resolve(
        strict=False
    )
    if not artifact_dir.is_dir():
        raise DurableExecutionError(
            f"ledger artifact is missing for duplicate task {task_id}"
        )
    receipt = play_hand_lab._validate_task_result_receipt(
        artifact_dir / "task-result-receipt.json",
        task_id=task_id,
        worker_result_sha256=None,
    )
    ledger_identity = str(recovered_row.get("lab_worker_result_sha256") or "")
    receipt_identity = str(receipt.get("worker_result_sha256") or "")
    if not ledger_identity or ledger_identity != receipt_identity:
        raise DurableExecutionError(
            f"ledger and sealed receipt identities conflict for duplicate task {task_id}"
        )
    recorded = receipt.get("recorded_result")
    if not isinstance(recorded, dict):
        raise DurableExecutionError(
            f"sealed receipt has no recorded result for duplicate task {task_id}"
        )
    return receipt


def _recover_recorded_result_from_sealed_receipt(
    play_hand_lab: Any,
    *,
    lane_ctx: Any,
    task_id: str,
) -> dict[str, Any]:
    attempts = play_hand_lab.load_attempts(lane_ctx.attempts_path)
    matches = [
        row
        for row in attempts
        if str(row.get("lab_campaign_task_id") or "") == task_id
    ]
    if len(matches) != 1:
        raise DurableExecutionError(
            f"sealed duplicate recovery requires one ledger row for task {task_id}"
        )
    receipt = _authoritative_receipt_for_ledger_row(
        play_hand_lab,
        task_id=task_id,
        recovered_row=matches[0],
    )
    logger.warning(
        "sealed_duplicate_result_recovered task_id=%s authoritative_worker_result_sha256=%s",
        task_id,
        receipt.get("worker_result_sha256"),
    )
    return dict(receipt["recorded_result"])


def install_play_hand_lab_duplicate_result_recovery() -> None:
    """Make sealed first-writer results authoritative across crash/retry windows.

    This changes no durable format or task identity. A later completion can only be
    ignored when the existing task receipt, artifact receipt, and ledger identity all
    validate. Missing or contradictory durable evidence remains fail-closed.
    """

    global _INSTALLED
    global _ORIGINAL_RECORD_LAB_RESULT
    global _ORIGINAL_TERMINAL_RECEIPT_FOR_RESULT
    global _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD

    if _INSTALLED:
        return

    from . import play_hand_lab

    _ORIGINAL_RECORD_LAB_RESULT = play_hand_lab._record_lab_result
    _ORIGINAL_TERMINAL_RECEIPT_FOR_RESULT = play_hand_lab._terminal_receipt_for_result
    _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD = (
        play_hand_lab._validate_task_result_receipt_payload
    )

    def validate_task_result_receipt_payload(
        receipt: Any,
        *,
        task_id: str,
        worker_result_sha256: str | None = None,
    ) -> dict[str, Any]:
        try:
            return _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD(
                receipt,
                task_id=task_id,
                worker_result_sha256=worker_result_sha256,
            )
        except DurableExecutionError as exc:
            if worker_result_sha256 is None or not _is_worker_identity_conflict(exc):
                raise
            sealed = _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD(
                receipt,
                task_id=task_id,
                worker_result_sha256=None,
            )
            logger.warning(
                "sealed_duplicate_result_ignored task_id=%s incoming_worker_result_sha256=%s "
                "authoritative_worker_result_sha256=%s",
                task_id,
                worker_result_sha256,
                sealed.get("worker_result_sha256"),
            )
            return sealed

    def record_lab_result(*args: Any, **kwargs: Any) -> dict[str, Any]:
        try:
            return _ORIGINAL_RECORD_LAB_RESULT(*args, **kwargs)
        except DurableExecutionError as exc:
            if not _is_worker_identity_conflict(exc):
                raise
            lab_result = kwargs.get("lab_result")
            lane_ctx = kwargs.get("lane_ctx")
            if not isinstance(lab_result, dict) or lane_ctx is None:
                raise
            task_id = str(lab_result.get("task_id") or "")
            if not task_id:
                raise
            return _recover_recorded_result_from_sealed_receipt(
                play_hand_lab,
                lane_ctx=lane_ctx,
                task_id=task_id,
            )

    def terminal_receipt_for_result(
        recorded: dict[str, Any],
        lab_result: dict[str, Any],
        *,
        derived_tasks: list[dict[str, Any]] | None = None,
        allow_legacy_phase3_receipt_migration: bool = False,
    ) -> dict[str, Any]:
        try:
            return _ORIGINAL_TERMINAL_RECEIPT_FOR_RESULT(
                recorded,
                lab_result,
                derived_tasks=derived_tasks,
                allow_legacy_phase3_receipt_migration=(
                    allow_legacy_phase3_receipt_migration
                ),
            )
        except DurableExecutionError as exc:
            if not _is_worker_identity_conflict(exc):
                raise

            task_id = str(lab_result.get("task_id") or "")
            artifact_dir = Path(str(recorded.get("artifact_dir") or "")).resolve(
                strict=False
            )
            receipt_path = artifact_dir / "task-result-receipt.json"
            receipt = play_hand_lab._validate_task_result_receipt(
                receipt_path,
                task_id=task_id,
                worker_result_sha256=None,
            )
            if receipt.get("recorded_result") != dict(recorded):
                raise DurableExecutionError(
                    f"sealed receipt recorded result conflicts for duplicate task {task_id}"
                )
            if derived_tasks is None:
                return receipt

            canonical_tasks = play_hand_lab._validated_receipt_derived_tasks(
                {"derived_tasks": derived_tasks},
                task_id=task_id,
                required=True,
            )
            existing = receipt.get("derived_tasks")
            if existing is not None and existing != canonical_tasks:
                raise DurableExecutionError(
                    f"derived task receipt conflicts for source task {task_id}"
                )
            updated = copy.deepcopy(receipt)
            updated["derived_tasks"] = canonical_tasks
            authoritative_identity = str(receipt.get("worker_result_sha256") or "")
            if not authoritative_identity:
                raise DurableExecutionError(
                    f"sealed receipt has no worker identity for duplicate task {task_id}"
                )
            logger.warning(
                "sealed_duplicate_follow_on_recovered task_id=%s "
                "authoritative_worker_result_sha256=%s",
                task_id,
                authoritative_identity,
            )
            return play_hand_lab._persist_task_result_receipt(
                receipt_path,
                updated,
                task_id=task_id,
                worker_result_sha256=authoritative_identity,
            )

    play_hand_lab._validate_task_result_receipt_payload = (
        validate_task_result_receipt_payload
    )
    play_hand_lab._record_lab_result = record_lab_result
    play_hand_lab._terminal_receipt_for_result = terminal_receipt_for_result
    _INSTALLED = True


__all__ = [
    "install_play_hand_lab_duplicate_result_recovery",
]
