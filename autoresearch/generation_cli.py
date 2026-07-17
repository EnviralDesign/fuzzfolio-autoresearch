"""Operator commands for safe generation archive cutovers.

This module deliberately keeps the mutation boundary narrow: an archive
cutover is only possible through ``--apply`` and the archive service.  Restore
support is planning-only; no command here copies, moves, or deletes runs.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from .config import load_config
from .generation_archive import GenerationArchiveError, GenerationArchiveService


REQUIRED_PROVENANCE_FIELDS = (
    "operator_command",
    "autoresearch_git_revision",
    "trading_dashboard_git_revision",
    "market_data_lake_git_revision",
    "lake_semantic_sha256",
    "source_snapshot_sha256",
    "universe_id",
    "universe_manifest_sha256",
    "worker_contract_id",
    "worker_contract_sha256",
    "worker_image",
    "engine_id",
    "engine_sha256",
    "scoring_policy_id",
    "scoring_policy_sha256",
    "cost_policy_id",
    "cost_policy_sha256",
)


def _load_provenance(path: Path | str) -> dict[str, Any]:
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"could not read provenance JSON: {source}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid provenance JSON: {source}") from exc
    if not isinstance(payload, dict):
        raise ValueError("provenance JSON must contain an object")
    return payload


def _missing_required_provenance_fields(provenance: Mapping[str, Any]) -> list[str]:
    return [
        field
        for field in REQUIRED_PROVENANCE_FIELDS
        if field not in provenance or provenance[field] is None or provenance[field] == ""
    ]


def _validate_provenance_identity(provenance: Mapping[str, Any]) -> None:
    for field in (
        "lake_semantic_sha256",
        "source_snapshot_sha256",
        "worker_contract_sha256",
        "universe_manifest_sha256",
        "engine_sha256",
        "scoring_policy_sha256",
        "cost_policy_sha256",
    ):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(provenance.get(field) or "")):
            raise ValueError(f"provenance {field} must be an exact sha256:<64 lowercase hex> identity")
    for field in (
        "autoresearch_git_revision",
        "trading_dashboard_git_revision",
        "market_data_lake_git_revision",
    ):
        if not re.fullmatch(r"[0-9a-f]{40}", str(provenance.get(field) or "")):
            raise ValueError(f"provenance {field} must be an exact 40-character lowercase git revision")


def _missing_apply_cutover_contract(provenance: Mapping[str, Any]) -> list[str]:
    """Return the explicit preview pin and writer-fence inputs required for apply.

    The service still re-verifies the marker contents and that the identity is
    the freshly observed tree identity while holding its exclusive lock.
    """

    missing: list[str] = []
    if not re.fullmatch(r"[0-9a-f]{64}", str(provenance.get("reviewed_inventory_identity") or "")):
        missing.append("reviewed_inventory_identity")
    proof = provenance.get("cutover_quiescence")
    if not isinstance(proof, Mapping):
        return [*missing, "cutover_quiescence"]
    if not isinstance(proof.get("marker_path"), str) or not proof["marker_path"]:
        missing.append("cutover_quiescence.marker_path")
    if not re.fullmatch(r"[0-9a-f]{64}", str(proof.get("marker_sha256") or "")):
        missing.append("cutover_quiescence.marker_sha256")
    return missing


def _emit(payload: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return
    if payload.get("error"):
        print(f"{payload['command']}: {payload['error']}")
        return
    if payload.get("command") == "restore-generation-plan":
        print(
            "Restore plan only: "
            f"{payload['source_archived_runs_root']} -> "
            f"{payload['proposed_destination_runs_root']}"
        )
        return
    if payload.get("dry_run"):
        missing = payload["missing_required_provenance_fields"]
        if missing:
            print("Dry run blocked: missing provenance fields: " + ", ".join(missing))
        else:
            print(
                "Dry run ready: "
                f"archive {payload['archive_id']} -> generation {payload['new_generation_id']}. "
                "Re-run with --apply to cut over."
            )
        return
    print(
        "Archive cutover complete: "
        f"{payload['archive_id']} -> generation {payload['new_generation_id']}."
    )


def cmd_archive_generation(
    *,
    archive_id: str,
    new_generation_id: str,
    provenance_json: Path | str,
    critical_artifacts: list[str] | None,
    apply: bool,
    as_json: bool,
) -> int:
    """Preview or explicitly perform an archive-generation cutover."""

    try:
        provenance = _load_provenance(provenance_json)
        service = GenerationArchiveService(load_config().runs_root)
    except (GenerationArchiveError, ValueError) as exc:
        _emit(
            {
                "command": "archive-generation",
                "error": str(exc),
                "dry_run": True,
                "requested_apply": apply,
            },
            as_json=as_json,
        )
        return 1

    missing_fields = _missing_required_provenance_fields(provenance)
    if missing_fields:
        payload = {
            "command": "archive-generation",
            "dry_run": True,
            "ready": False,
            "requested_apply": apply,
            "missing_required_provenance_fields": missing_fields,
        }
        _emit(payload, as_json=as_json)
        return 1
    try:
        _validate_provenance_identity(provenance)
    except ValueError as exc:
        _emit(
            {
                "command": "archive-generation",
                "error": str(exc),
                "dry_run": True,
                "requested_apply": apply,
            },
            as_json=as_json,
        )
        return 1

    if apply:
        missing_contract = _missing_apply_cutover_contract(provenance)
        if missing_contract:
            _emit(
                {
                    "command": "archive-generation",
                    "error": "apply requires pinned preview and quiescence contract fields: "
                    + ", ".join(missing_contract),
                    "dry_run": True,
                    "requested_apply": True,
                },
                as_json=as_json,
            )
            return 1

    if not apply:
        try:
            plan = service.dry_run(
                archive_id,
                new_generation_id,
                provenance=provenance,
                critical_artifacts=critical_artifacts,
            )
        except (GenerationArchiveError, ValueError) as exc:
            _emit(
                {
                    "command": "archive-generation",
                    "error": str(exc),
                    "dry_run": True,
                    "requested_apply": False,
                },
                as_json=as_json,
            )
            return 1
        payload = {
            **plan,
            "command": "archive-generation",
            "ready": True,
            "requested_apply": False,
            "missing_required_provenance_fields": [],
        }
        _emit(payload, as_json=as_json)
        return 0

    try:
        result = service.cutover(
            archive_id,
            new_generation_id,
            provenance=provenance,
            critical_artifacts=critical_artifacts,
        )
    except (GenerationArchiveError, ValueError) as exc:
        _emit(
            {
                "command": "archive-generation",
                "error": str(exc),
                "dry_run": True,
                "requested_apply": True,
            },
            as_json=as_json,
        )
        return 1

    payload = {
        **result,
        "command": "archive-generation",
        "archive_id": archive_id,
        "new_generation_id": new_generation_id,
        "dry_run": False,
        "requested_apply": True,
        "ready": True,
        "missing_required_provenance_fields": [],
    }
    _emit(payload, as_json=as_json)
    return 0


def cmd_restore_generation_plan(
    *, archive_id: str, destination_runs_root: Path | str | None, as_json: bool
) -> int:
    """Emit a restore plan only; restoration is intentionally not implemented."""

    try:
        plan = GenerationArchiveService(load_config().runs_root).restore_plan(
            archive_id,
            destination_runs_root=destination_runs_root,
        )
    except (GenerationArchiveError, ValueError) as exc:
        _emit(
            {
                "command": "restore-generation-plan",
                "error": str(exc),
                "dry_run": True,
            },
            as_json=as_json,
        )
        return 1
    _emit({**plan, "command": "restore-generation-plan"}, as_json=as_json)
    return 0
