from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

from .evidence_plan import (
    canonical_sha256,
    canonical_timestamp,
    normalize_evidence_profile_snapshot,
    validate_replay_evidence_plan,
)
from .ledger import load_attempts, load_run_metadata, list_run_dirs
from .evidence_artifacts import discover_evidence_artifact_bundles


LEVEL_C_COHORT_SCHEMA = "autoresearch-level-c-frozen-cohort-v2"
LEVEL_C_COHORT_OUTCOME_CANDIDATES_FROZEN = "candidates_frozen"
LEVEL_C_COHORT_OUTCOME_NO_DEFENSIBLE_CANDIDATES = "no_defensible_candidates"
LEVEL_C_NO_DEFENSIBLE_CANDIDATES_REASON = "no_canonical_cutoff_bounded_candidates"
PLAYHAND_V2_CAMPAIGNS_DIR = "play-hand-lab-campaigns"
PLAYHAND_V2_CAMPAIGN_SCHEMA = "play_hand_lab_campaign_v1"
PLAYHAND_V2_LANE_SCHEMA = "play_hand_lab_lane_v1"
PLAYHAND_V2_RUNNER = "play_hand_lab_v1"


class LevelCCohortError(RuntimeError):
    pass


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _write_immutable(path: Path, payload: dict[str, Any]) -> None:
    serialized = _canonical_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            existing = _canonical_json(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            existing = path.read_text(encoding="utf-8")
        if existing != serialized:
            raise LevelCCohortError(f"Frozen Level C cohort already differs: {path}")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)


def _hash_tree(root: Path, *, namespace: str) -> dict[str, str]:
    if not root.is_dir():
        raise LevelCCohortError(f"Evidence root does not exist: {root}")
    output: dict[str, str] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = str(path.relative_to(root)).replace("\\", "/")
        output[f"{namespace}/{relative}"] = _file_sha256(path)
    if not output:
        raise LevelCCohortError(f"Evidence root is empty: {root}")
    return output


def _require_equal(label: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise LevelCCohortError(
            f"{label} mismatch: expected {expected!r}, observed {observed!r}"
        )


def _require_sha256_identity(label: str, value: Any) -> str:
    identity = str(value or "").strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", identity):
        raise LevelCCohortError(f"{label} must be a sha256: identity")
    return identity


_SAFE_RESEARCH_GENERATION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def _require_historical_lineage(
    payload: Any,
    *,
    cutoff: str,
    lake_manifest_sha256: str,
    label: str,
) -> dict[str, str]:
    """Validate the non-interchangeable Level C lineage fields.

    A formal historical run may not borrow artifacts across generations,
    protocol manifests, or cutoff plans.  The protocol id itself is a
    content-addressed sha256 identity; its manifest is resolved by the caller
    before the run begins.
    """
    if not isinstance(payload, dict):
        raise LevelCCohortError(f"{label} historical lineage is missing")
    generation_id = str(payload.get("research_generation_id") or "").strip()
    if not _SAFE_RESEARCH_GENERATION_RE.fullmatch(generation_id):
        raise LevelCCohortError(f"{label} research_generation_id is missing or malformed")
    cutoff_key = str(payload.get("cutoff_key") or "").strip()
    if cutoff_key not in {"A", "B", "C", "D"}:
        raise LevelCCohortError(f"{label} cutoff_key is missing or malformed")
    lineage = {
        "research_generation_id": generation_id,
        "level_c_protocol_id": _require_sha256_identity(
            f"{label} level_c_protocol_id", payload.get("level_c_protocol_id")
        ),
        "cutoff_key": cutoff_key,
        "as_of_date": canonical_timestamp(payload.get("as_of_date")),
        "lake_manifest_sha256": _require_sha256_identity(
            f"{label} lake_manifest_sha256", payload.get("lake_manifest_sha256")
        ),
        "source_snapshot_sha256": _require_sha256_identity(
            f"{label} source_snapshot_sha256", payload.get("source_snapshot_sha256")
        ),
        "universe_id": str(payload.get("universe_id") or "").strip(),
        "universe_manifest_sha256": _require_sha256_identity(
            f"{label} universe_manifest_sha256", payload.get("universe_manifest_sha256")
        ),
    }
    if not lineage["universe_id"]:
        raise LevelCCohortError(f"{label} universe_id is missing")
    _require_equal(f"{label} as_of_date", lineage["as_of_date"], cutoff)
    _require_equal(
        f"{label} lake identity", lineage["lake_manifest_sha256"], lake_manifest_sha256
    )
    return lineage


def _require_playhand_lineage(
    payload: dict[str, Any], *, lineage: dict[str, str], label: str
) -> None:
    for field in ("research_generation_id", "level_c_protocol_id", "cutoff_key"):
        _require_equal(f"{label} {field}", payload.get(field), lineage[field])


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _safe_relative_path(value: Any, *, label: str) -> Path:
    path = Path(str(value or ""))
    if (
        not str(value or "").strip()
        or path.is_absolute()
        or any(part == ".." for part in path.parts)
        or str(path) in {".", ""}
    ):
        raise LevelCCohortError(f"{label} must be a non-empty relative path")
    return path


def _relative_to_runs_root(path: Path, runs_root: Path, *, label: str) -> str:
    resolved_path = path.expanduser().resolve()
    resolved_root = runs_root.expanduser().resolve()
    if not _path_is_within(resolved_path, resolved_root):
        raise LevelCCohortError(f"{label} escapes the generation runs root: {path}")
    return resolved_path.relative_to(resolved_root).as_posix()


def _resolve_relative_under_runs_root(
    relative: Any, runs_root: Path, *, label: str
) -> Path:
    safe_relative = _safe_relative_path(relative, label=label)
    root = runs_root.expanduser().resolve()
    resolved = (root / safe_relative).resolve()
    if not _path_is_within(resolved, root):
        raise LevelCCohortError(f"{label} escapes the generation runs root: {relative}")
    return resolved


def _rebase_recorded_runs_path(
    value: Any,
    *,
    recorded_runs_root: Path,
    active_runs_root: Path,
    label: str,
) -> Path:
    """Resolve an in-tree recorded path against a safely relocated runs root."""
    raw_value = str(value or "").strip()
    if not raw_value:
        raise LevelCCohortError(f"{label} is missing")
    raw = Path(raw_value).expanduser()
    if raw.is_absolute():
        try:
            relative = raw.resolve(strict=False).relative_to(
                recorded_runs_root.expanduser().resolve(strict=False)
            )
        except ValueError as exc:
            raise LevelCCohortError(
                f"{label} escapes the recorded generation runs root: {raw}"
            ) from exc
    else:
        relative = _safe_relative_path(raw_value, label=label)
    return _resolve_relative_under_runs_root(relative, active_runs_root, label=label)


def _cohort_evidence_roots(
    payload: dict[str, Any], *, active_runs_root: Path
) -> tuple[Path, Path, Path]:
    roots = payload.get("evidence_roots")
    if not isinstance(roots, dict):
        raise LevelCCohortError("Frozen Level C evidence roots are missing")
    atlas_root = _resolve_relative_under_runs_root(
        roots.get("atlas_run_root"), active_runs_root, label="Level C Atlas evidence root"
    )
    campaign_root = _resolve_relative_under_runs_root(
        roots.get("playhand_campaign_root"),
        active_runs_root,
        label="Level C PlayHand campaign evidence root",
    )
    if not atlas_root.is_dir() or not campaign_root.is_dir():
        raise LevelCCohortError("Frozen Level C evidence root does not exist")
    return active_runs_root, atlas_root, campaign_root


def _resolve_validation_runs_root(
    payload: dict[str, Any],
    *,
    cohort_path: Path,
    relocated_runs_root: Path | None,
) -> Path:
    raw_recorded_runs_root = str(payload.get("runs_root") or "").strip()
    if not raw_recorded_runs_root:
        raise LevelCCohortError("Frozen Level C runs root is missing")
    recorded_runs_root = Path(raw_recorded_runs_root).expanduser()
    if relocated_runs_root is not None:
        requested_root = relocated_runs_root.expanduser()
        active_root = requested_root.resolve()
        if requested_root.is_symlink() or not active_root.is_dir() or active_root.is_symlink():
            raise LevelCCohortError("Relocated Level C runs root must be a real directory")
        return active_root
    original_root = recorded_runs_root.resolve(strict=False)
    if original_root.is_dir() and not original_root.is_symlink():
        return original_root
    manifest_relative = payload.get("cohort_path_relative_to_runs_root")
    safe_relative = _safe_relative_path(
        manifest_relative, label="Frozen Level C cohort manifest location"
    )
    candidate = cohort_path.expanduser().resolve().parent
    for _ in safe_relative.parts[:-1]:
        candidate = candidate.parent
    candidate = candidate.resolve()
    if candidate.is_dir() and (candidate / safe_relative).resolve() == cohort_path.expanduser().resolve():
        return candidate
    raise LevelCCohortError(
        "Frozen Level C runs root is unavailable; provide the relocated runs root explicitly"
    )


def _campaign_id_path_component(campaign_id: str) -> str:
    token = str(campaign_id or "").strip()
    if not token or Path(token).name != token or token in {".", ".."}:
        raise LevelCCohortError("PlayHand campaign id must name one campaign directory")
    return token


def _resolve_playhand_campaign(
    runs_root: Path, campaign_id: str
) -> tuple[Path, str]:
    """Resolve the v2 campaign location, with an unambiguous legacy fallback."""
    campaign_id = _campaign_id_path_component(campaign_id)
    root = runs_root.expanduser().resolve()
    v2_root = root / "derived" / PLAYHAND_V2_CAMPAIGNS_DIR / campaign_id
    legacy_root = root / campaign_id
    v2_exists = v2_root.is_dir()
    legacy_exists = legacy_root.is_dir()
    if v2_exists and legacy_exists:
        raise LevelCCohortError(
            "Ambiguous PlayHand campaign layout: both v2 and legacy roots exist "
            f"for {campaign_id}"
        )
    if v2_exists:
        return v2_root.resolve(), "playhand_lab_v2"
    if legacy_exists:
        return legacy_root.resolve(), "legacy_top_level"
    raise LevelCCohortError(f"Missing PlayHand campaign metadata: {v2_root}")


def _validate_atlas_root(
    atlas_root: Path, *, cutoff: str, lake_manifest_sha256: str
) -> dict[str, str]:
    atlas_metadata_path = atlas_root / "atlas-lab-run.json"
    try:
        atlas_metadata = json.loads(atlas_metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(f"Invalid Atlas run metadata: {atlas_metadata_path}") from exc
    atlas_runtime = atlas_metadata.get("runtime") or {}
    _require_equal("Atlas as_of_date", canonical_timestamp(atlas_runtime.get("as_of_date")), cutoff)
    _require_equal("Atlas lake identity", atlas_runtime.get("lake_manifest_sha256"), lake_manifest_sha256)
    _require_equal("Atlas signal executor", atlas_runtime.get("signal_atlas_executor"), "gateway")
    lineage = _require_historical_lineage(
        atlas_metadata.get("historical_lineage"),
        cutoff=cutoff,
        lake_manifest_sha256=lake_manifest_sha256,
        label="Atlas",
    )
    runtime_lineage = {
        key: (
            canonical_timestamp(atlas_runtime.get(key))
            if key == "as_of_date"
            else atlas_runtime.get(key)
        )
        for key in lineage
    }
    _require_equal("Atlas runtime historical lineage", runtime_lineage, lineage)
    universe_contract = atlas_metadata.get("universe_contract") or {}
    _require_equal("Atlas universe id", universe_contract.get("universe_id"), lineage["universe_id"])
    _require_equal(
        "Atlas universe manifest", universe_contract.get("universe_hash"), lineage["universe_manifest_sha256"]
    )
    if str(atlas_metadata.get("status") or "").lower() not in {"complete", "completed"}:
        raise LevelCCohortError("Atlas run is not complete")
    signal_atlas_path = atlas_root / "signal-atlas" / "signal-atlas.json"
    if not signal_atlas_path.is_file():
        signal_atlas_path = atlas_root / "signal-atlas.json"
    try:
        signal_atlas = json.loads(signal_atlas_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(
            f"Historical Atlas signal evidence is missing: {signal_atlas_path}"
        ) from exc
    successful_signal_rows = [
        row
        for row in signal_atlas.get("rows") or []
        if isinstance(row, dict) and row.get("status") == "ok"
    ]
    if not successful_signal_rows:
        raise LevelCCohortError("Historical Atlas has no successful signal-cell receipts")
    for row in successful_signal_rows:
        if not row.get("evidence_plan_id"):
            raise LevelCCohortError("Historical Atlas signal row omitted evidence_plan_id")
        _require_equal(
            "Atlas signal observed lake identity",
            row.get("observed_lake_manifest_sha256"),
            lake_manifest_sha256,
        )
    return lineage


def _validate_campaign(
    campaign_root: Path,
    *,
    recorded_runs_root: Path,
    active_runs_root: Path,
    campaign_id: str,
    layout: str,
    cutoff: str,
    lake_manifest_sha256: str,
    atlas_root: Path,
    lineage: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    campaign_metadata = load_run_metadata(campaign_root)
    if not campaign_metadata:
        raise LevelCCohortError(f"Missing PlayHand campaign metadata: {campaign_root}")
    if layout == "playhand_lab_v2":
        _require_equal("PlayHand campaign schema", campaign_metadata.get("schema_version"), PLAYHAND_V2_CAMPAIGN_SCHEMA)
        _require_equal("PlayHand campaign runner", campaign_metadata.get("generated_by_runner"), PLAYHAND_V2_RUNNER)
        _require_equal("PlayHand campaign kind", campaign_metadata.get("run_kind"), "play_hand_lab_campaign")
        _require_equal("PlayHand campaign id", campaign_metadata.get("run_id"), campaign_id)
    _require_equal(
        "PlayHand campaign as_of_date",
        canonical_timestamp(campaign_metadata.get("as_of_date")),
        cutoff,
    )
    _require_equal(
        "PlayHand campaign lake identity",
        campaign_metadata.get("lake_manifest_sha256"),
        lake_manifest_sha256,
    )
    _require_playhand_lineage(campaign_metadata, lineage=lineage, label="PlayHand campaign")
    if str(campaign_metadata.get("run_status") or "").lower() not in {"complete", "completed", "promoted"}:
        raise LevelCCohortError("PlayHand campaign is not complete")
    if int(campaign_metadata.get("failed_task_count") or 0) != 0:
        raise LevelCCohortError("PlayHand campaign contains failed infrastructure tasks")
    if campaign_metadata.get("historical_completion_failure_reason"):
        raise LevelCCohortError("PlayHand campaign failed its historical completion contract")
    _require_equal(
        "PlayHand campaign mode", campaign_metadata.get("campaign_mode"), "finite"
    )
    target_runs = campaign_metadata.get("target_runs")
    if isinstance(target_runs, bool) or not isinstance(target_runs, int) or target_runs <= 0:
        raise LevelCCohortError(
            "PlayHand campaign must declare a positive finite target_runs count"
        )
    _require_equal(
        "PlayHand formal historical marker",
        campaign_metadata.get("formal_historical_level_c"),
        True,
    )
    summary_path = _rebase_recorded_runs_path(
        campaign_metadata.get("summary_path"),
        recorded_runs_root=recorded_runs_root,
        active_runs_root=active_runs_root,
        label="PlayHand campaign summary path",
    )
    if not summary_path.is_file() or not _path_is_within(summary_path, campaign_root):
        raise LevelCCohortError("PlayHand campaign summary is missing or escapes its campaign")
    try:
        campaign_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(f"Invalid PlayHand campaign summary: {summary_path}") from exc
    if not isinstance(campaign_summary, dict):
        raise LevelCCohortError("PlayHand campaign summary must be an object")
    _require_equal(
        "PlayHand campaign summary id",
        campaign_summary.get("campaign_id"),
        campaign_id,
    )
    if str(campaign_summary.get("status") or "").lower() not in {
        "complete",
        "completed",
        "promoted",
    }:
        raise LevelCCohortError("PlayHand campaign summary is not complete")
    _require_equal(
        "PlayHand campaign summary target",
        campaign_summary.get("target_runs"),
        target_runs,
    )
    _require_equal(
        "PlayHand campaign summary lane count",
        campaign_summary.get("lane_count"),
        target_runs,
    )
    _require_equal(
        "PlayHand campaign summary terminal lanes",
        campaign_summary.get("terminal_lanes"),
        target_runs,
    )
    _require_equal(
        "PlayHand campaign summary failed tasks",
        campaign_summary.get("failed_tasks"),
        0,
    )
    atlas_seed_plan = atlas_root / "recipe-priors" / "play-hand-seed-plan.json"
    recorded_seed_plan = _rebase_recorded_runs_path(
        campaign_metadata.get("play_hand_seed_plan_path"),
        recorded_runs_root=recorded_runs_root,
        active_runs_root=active_runs_root,
        label="PlayHand Atlas seed plan path",
    )
    if not atlas_seed_plan.is_file():
        raise LevelCCohortError(
            f"Atlas run has no frozen PlayHand seed plan: {atlas_seed_plan}"
        )
    _require_equal(
        "PlayHand Atlas seed plan path",
        str(recorded_seed_plan) if recorded_seed_plan.is_file() else None,
        str(atlas_seed_plan.resolve()),
    )
    _require_equal(
        "PlayHand Atlas seed plan hash",
        campaign_metadata.get("play_hand_seed_plan_sha256"),
        _file_sha256(atlas_seed_plan),
    )
    try:
        seed_plan = json.loads(atlas_seed_plan.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(f"Invalid Atlas PlayHand seed plan: {atlas_seed_plan}") from exc
    if not isinstance(seed_plan, dict):
        raise LevelCCohortError("Atlas PlayHand seed plan must be an object")
    _require_equal("Atlas PlayHand seed plan lineage", seed_plan.get("historical_lineage"), lineage)
    recipe_priors_path = atlas_root / "recipe-priors" / "recipe-priors.json"
    summary_path = atlas_root / "recipe-priors" / "recipe-priors-summary.json"
    lineage_path = atlas_root / "recipe-priors" / "level-c-lineage.json"
    for artifact_path, artifact_label in (
        (recipe_priors_path, "recipe priors"),
        (summary_path, "recipe-priors summary"),
    ):
        try:
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LevelCCohortError(f"Invalid Atlas {artifact_label}: {artifact_path}") from exc
        if not isinstance(artifact, dict):
            raise LevelCCohortError(f"Atlas {artifact_label} must be an object")
        _require_equal(f"Atlas {artifact_label} lineage", artifact.get("historical_lineage"), lineage)
    try:
        lineage_artifact = json.loads(lineage_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(f"Invalid Atlas recipe-prior lineage: {lineage_path}") from exc
    if not isinstance(lineage_artifact, dict):
        raise LevelCCohortError("Atlas recipe-prior lineage must be an object")
    _require_equal(
        "Atlas recipe-prior lineage schema", lineage_artifact.get("schema_version"), "atlas_level_c_lineage_v1"
    )
    _require_equal("Atlas recipe-prior lineage", lineage_artifact.get("historical_lineage"), lineage)
    recorded_hashes = lineage_artifact.get("artifact_sha256")
    if not isinstance(recorded_hashes, dict):
        raise LevelCCohortError("Atlas recipe-prior lineage artifact inventory is missing")
    for artifact_path in (atlas_seed_plan, recipe_priors_path, summary_path):
        _require_equal(
            f"Atlas recipe-prior lineage hash {artifact_path.name}",
            recorded_hashes.get(artifact_path.name),
            _file_sha256(artifact_path),
        )
    return campaign_metadata, campaign_summary


def _validate_discovery_plan(
    payload: Any,
    *,
    cutoff: str,
    lake_manifest_sha256: str,
    label: str,
) -> dict[str, Any]:
    try:
        plan = validate_replay_evidence_plan(payload)
    except Exception as exc:
        raise LevelCCohortError(f"{label} has no valid evidence plan: {exc}") from exc
    if canonical_timestamp(plan.selection_data_end) > cutoff:
        raise LevelCCohortError(f"{label} reads beyond the Level C cutoff")
    if canonical_timestamp(plan.analysis_window_end) > cutoff:
        raise LevelCCohortError(f"{label} analyzes beyond the Level C cutoff")
    if (
        plan.data_availability_cutoff is None
        or canonical_timestamp(plan.data_availability_cutoff) > cutoff
    ):
        raise LevelCCohortError(f"{label} has future-aware data availability")
    _require_equal(
        f"{label} lake identity", plan.lake_manifest_sha256, lake_manifest_sha256
    )
    return plan.model_dump(mode="json")


def _discover_candidates(
    *,
    runs_root: Path,
    recorded_runs_root: Path,
    campaign_root: Path,
    campaign_id: str,
    layout: str,
    cutoff: str,
    lake_manifest_sha256: str,
    lineage: dict[str, str],
    target_runs: int,
    campaign_summary: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[Path]]:
    candidates: list[dict[str, Any]] = []
    lane_roots: list[Path] = []
    lane_indexes: set[int] = set()
    lane_run_ids: set[str] = set()
    for run_dir in list_run_dirs(runs_root):
        metadata = load_run_metadata(run_dir)
        parent_campaign_id = str(metadata.get("parent_campaign_id") or "")
        lab_campaign_id = str(metadata.get("lab_campaign_id") or "")
        if campaign_id not in {parent_campaign_id, lab_campaign_id}:
            continue
        if layout == "playhand_lab_v2":
            _require_equal(f"PlayHand lane {run_dir.name} schema", metadata.get("schema_version"), PLAYHAND_V2_LANE_SCHEMA)
            _require_equal(f"PlayHand lane {run_dir.name} runner", metadata.get("generated_by_runner"), PLAYHAND_V2_RUNNER)
            _require_equal(f"PlayHand lane {run_dir.name} kind", metadata.get("run_kind"), "play_hand_lab_lane")
            _require_equal(f"PlayHand lane {run_dir.name} run id", metadata.get("run_id"), run_dir.name)
            _require_equal(f"PlayHand lane {run_dir.name} parent campaign", parent_campaign_id, campaign_id)
            _require_equal(f"PlayHand lane {run_dir.name} lab campaign", lab_campaign_id, campaign_id)
            lane_index = metadata.get("lab_lane_index")
            if (
                isinstance(lane_index, bool)
                or not isinstance(lane_index, int)
                or lane_index < 0
                or lane_index >= target_runs
            ):
                raise LevelCCohortError(
                    f"PlayHand lane {run_dir.name} has an invalid campaign lane index"
                )
            if lane_index in lane_indexes:
                raise LevelCCohortError(
                    f"PlayHand campaign has duplicate lane index {lane_index}"
                )
            lane_indexes.add(lane_index)
            recorded_campaign_dir = _rebase_recorded_runs_path(
                metadata.get("campaign_dir"),
                recorded_runs_root=recorded_runs_root,
                active_runs_root=runs_root,
                label=f"PlayHand lane {run_dir.name} campaign path",
            )
            _require_equal(
                f"PlayHand lane {run_dir.name} campaign path",
                str(recorded_campaign_dir) if recorded_campaign_dir.is_dir() else None,
                str(campaign_root.resolve()),
            )
        _require_equal(
            f"PlayHand lane {run_dir.name} as_of_date",
            canonical_timestamp(metadata.get("as_of_date")),
            cutoff,
        )
        _require_equal(
            f"PlayHand lane {run_dir.name} lake identity",
            metadata.get("lake_manifest_sha256"),
            lake_manifest_sha256,
        )
        _require_playhand_lineage(
            metadata, lineage=lineage, label=f"PlayHand lane {run_dir.name}"
        )
        lane_roots.append(run_dir)
        lane_run_ids.add(run_dir.name)
        canonical_attempt_id = str(metadata.get("canonical_attempt_id") or "").strip()
        status = str(metadata.get("run_status") or "").lower()
        if metadata.get("terminal") is not True:
            raise LevelCCohortError(f"PlayHand lane {run_dir.name} is not terminal")
        if int(metadata.get("failed_task_count") or 0) != 0:
            raise LevelCCohortError(
                f"PlayHand lane {run_dir.name} contains failed infrastructure tasks"
            )
        if status in {"complete", "completed", "promoted"} and not canonical_attempt_id:
            raise LevelCCohortError(f"PlayHand lane {run_dir.name} has no canonical attempt")
        if not canonical_attempt_id:
            reason = str(metadata.get("tombstone_reason") or status).strip().lower().replace("-", "_")
            if reason != "early_exit_policy_enforced" and not reason.startswith(
                ("validation_", "final_", "no_valid_cell", "no_signal", "nonviable")
            ):
                raise LevelCCohortError(
                    f"PlayHand lane {run_dir.name} has no legitimate terminal research outcome"
                )
            continue
        attempts = load_attempts(run_dir / "attempts.jsonl")
        matching_attempts = [
            row
            for row in attempts
            if str(row.get("attempt_id") or "") == canonical_attempt_id
        ]
        if len(matching_attempts) != 1:
            raise LevelCCohortError(
                f"PlayHand lane {run_dir.name} has ambiguous canonical attempt {canonical_attempt_id}"
            )
        attempt = matching_attempts[0]
        if not isinstance(attempt, dict):
            raise LevelCCohortError(f"Invalid canonical attempt {canonical_attempt_id}")
        if layout == "playhand_lab_v2":
            _require_equal(
                f"canonical attempt {canonical_attempt_id} run id",
                attempt.get("run_id"),
                run_dir.name,
            )
            _require_equal(
                f"canonical attempt {canonical_attempt_id} runner",
                attempt.get("runner"),
                "play_hand_v1",
            )
            _require_equal(
                f"canonical attempt {canonical_attempt_id} stage",
                attempt.get("play_hand_stage"),
                "final_36mo",
            )
        evidence_plan = _validate_discovery_plan(
            attempt.get("evidence_plan"),
            cutoff=cutoff,
            lake_manifest_sha256=lake_manifest_sha256,
            label=f"canonical attempt {canonical_attempt_id}",
        )
        _require_equal(
            f"canonical attempt {canonical_attempt_id} campaign plan",
            evidence_plan.get("campaign_plan_id"),
            f"playhand-lab:{run_dir.name}",
        )
        _require_equal(
            f"canonical attempt {canonical_attempt_id} evidence role",
            evidence_plan.get("evidence_role"),
            "training",
        )
        receipt = attempt.get("execution_evidence")
        if not isinstance(receipt, dict):
            raise LevelCCohortError(
                f"Canonical attempt {canonical_attempt_id} omitted execution_evidence"
            )
        expected_receipt = {
            "plan_id": evidence_plan["plan_id"],
            "profile_snapshot_sha256": evidence_plan["profile_snapshot_sha256"],
            "execution_cell_sha256": evidence_plan.get("execution_cell_sha256"),
            "observed_lake_manifest_sha256": lake_manifest_sha256,
        }
        for key, value in expected_receipt.items():
            _require_equal(
                f"canonical attempt {canonical_attempt_id} receipt {key}",
                receipt.get(key),
                value,
            )
        profile_path = _rebase_recorded_runs_path(
            attempt.get("profile_path"),
            recorded_runs_root=recorded_runs_root,
            active_runs_root=runs_root,
            label=f"Canonical attempt profile {canonical_attempt_id}",
        )
        if not profile_path.is_file():
            raise LevelCCohortError(
                f"Canonical attempt profile is missing: {canonical_attempt_id}"
            )
        if layout == "playhand_lab_v2" and not _path_is_within(profile_path, run_dir):
            raise LevelCCohortError(
                f"Canonical attempt profile escapes its PlayHand lane: {canonical_attempt_id}"
            )
        try:
            profile_payload = json.loads(profile_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LevelCCohortError(
                f"Canonical attempt profile is invalid: {canonical_attempt_id}"
            ) from exc
        profile_identity = canonical_sha256(
            normalize_evidence_profile_snapshot(profile_payload)
        )
        _require_equal(
            f"canonical attempt {canonical_attempt_id} profile snapshot",
            profile_identity,
            evidence_plan["profile_snapshot_sha256"],
        )
        candidates.append(
            {
                "run_id": run_dir.name,
                "attempt_id": canonical_attempt_id,
                "profile_path_relative_to_runs_root": _relative_to_runs_root(
                    profile_path,
                    runs_root,
                    label=f"Canonical attempt profile {canonical_attempt_id}",
                ),
                "profile_sha256": _file_sha256(profile_path),
                "discovery_evidence_plan_id": evidence_plan["plan_id"],
            }
        )
    if len({row["attempt_id"] for row in candidates}) != len(candidates):
        raise LevelCCohortError("PlayHand campaign has duplicate canonical attempt ids")
    if len(lane_roots) != target_runs:
        raise LevelCCohortError(
            f"PlayHand campaign lane accounting mismatch: expected {target_runs}, found {len(lane_roots)}"
        )
    if layout == "playhand_lab_v2" and lane_indexes != set(range(target_runs)):
        raise LevelCCohortError("PlayHand campaign lane index accounting is incomplete")
    summary_lanes = campaign_summary.get("lanes")
    if not isinstance(summary_lanes, list):
        raise LevelCCohortError("PlayHand campaign summary lane receipts are missing")
    summary_run_ids: set[str] = set()
    for row in summary_lanes:
        if not isinstance(row, dict):
            raise LevelCCohortError("PlayHand campaign summary contains an invalid lane receipt")
        run_id = str(row.get("run_id") or "").strip()
        if not run_id or run_id in summary_run_ids or run_id not in lane_run_ids:
            raise LevelCCohortError("PlayHand campaign summary lane receipts do not match evidence")
        if row.get("terminal") is not True or int(row.get("failed_task_count") or 0) != 0:
            raise LevelCCohortError("PlayHand campaign summary contains a non-terminal lane")
        summary_run_ids.add(run_id)
    retained_lane_count = campaign_summary.get("retained_lane_count")
    pruned_lane_count = campaign_summary.get("pruned_lane_count")
    if (
        isinstance(retained_lane_count, bool)
        or not isinstance(retained_lane_count, int)
        or isinstance(pruned_lane_count, bool)
        or not isinstance(pruned_lane_count, int)
        or retained_lane_count < 0
        or pruned_lane_count < 0
        or retained_lane_count + pruned_lane_count != target_runs
        or len(summary_run_ids) != retained_lane_count
    ):
        raise LevelCCohortError("PlayHand campaign retained/pruned lane accounting is invalid")
    return sorted(candidates, key=lambda row: row["attempt_id"]), lane_roots


def _frozen_cohort_outcome(candidates: list[dict[str, Any]]) -> tuple[str, str | None]:
    if candidates:
        return LEVEL_C_COHORT_OUTCOME_CANDIDATES_FROZEN, None
    return (
        LEVEL_C_COHORT_OUTCOME_NO_DEFENSIBLE_CANDIDATES,
        LEVEL_C_NO_DEFENSIBLE_CANDIDATES_REASON,
    )


def _validate_frozen_cohort_outcome(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates")
    candidate_count = payload.get("candidate_count")
    if not isinstance(candidates, list):
        raise LevelCCohortError("Frozen Level C candidates must be a list")
    if isinstance(candidate_count, bool) or not isinstance(candidate_count, int):
        raise LevelCCohortError("Frozen Level C candidate count is malformed")
    _require_equal("Frozen Level C candidate count", candidate_count, len(candidates))
    outcome = str(payload.get("outcome") or "")
    expected_outcome, expected_reason = _frozen_cohort_outcome(candidates)
    _require_equal("Frozen Level C outcome", outcome, expected_outcome)
    _require_equal(
        "Frozen Level C outcome reason", payload.get("outcome_reason"), expected_reason
    )
    return outcome


def freeze_level_c_cohort(
    *,
    runs_root: Path,
    atlas_run_root: Path,
    playhand_campaign_id: str,
    as_of_date: str,
    lake_manifest_sha256: str,
    output_path: Path,
    cohort_id: str,
) -> dict[str, Any]:
    cutoff = canonical_timestamp(as_of_date)
    lake_identity = _require_sha256_identity("lake_manifest_sha256", lake_manifest_sha256)

    resolved_runs_root = runs_root.expanduser().resolve()
    if not resolved_runs_root.is_dir() or resolved_runs_root.is_symlink():
        raise LevelCCohortError("Level C runs root must be a real directory")
    atlas_root = atlas_run_root.expanduser().resolve()
    atlas_root_relative = _relative_to_runs_root(
        atlas_root, resolved_runs_root, label="Level C Atlas evidence root"
    )
    lineage = _validate_atlas_root(
        atlas_root, cutoff=cutoff, lake_manifest_sha256=lake_identity
    )
    campaign_id = _campaign_id_path_component(playhand_campaign_id)
    campaign_dir, campaign_layout = _resolve_playhand_campaign(resolved_runs_root, campaign_id)
    campaign_metadata, campaign_summary = _validate_campaign(
        campaign_dir,
        recorded_runs_root=resolved_runs_root,
        active_runs_root=resolved_runs_root,
        campaign_id=campaign_id,
        layout=campaign_layout,
        cutoff=cutoff,
        lake_manifest_sha256=lake_identity,
        atlas_root=atlas_root,
        lineage=lineage,
    )
    candidates, lane_roots = _discover_candidates(
        runs_root=resolved_runs_root,
        recorded_runs_root=resolved_runs_root,
        campaign_root=campaign_dir,
        campaign_id=campaign_id,
        layout=campaign_layout,
        cutoff=cutoff,
        lake_manifest_sha256=lake_identity,
        lineage=lineage,
        target_runs=int(campaign_metadata["target_runs"]),
        campaign_summary=campaign_summary,
    )

    artifact_sha256 = _hash_tree(atlas_root, namespace="atlas")
    artifact_sha256.update(_hash_tree(campaign_dir, namespace="playhand-campaign"))
    for lane_root in lane_roots:
        artifact_sha256.update(
            _hash_tree(lane_root, namespace=f"playhand-lanes/{lane_root.name}")
        )
    outcome, outcome_reason = _frozen_cohort_outcome(candidates)
    resolved_output_path = output_path.expanduser().resolve()
    output_relative = _relative_to_runs_root(
        resolved_output_path,
        resolved_runs_root,
        label="Level C cohort manifest location",
    )
    identity = {
        "schema": LEVEL_C_COHORT_SCHEMA,
        "cohort_id": str(cohort_id).strip(),
        "as_of_date": cutoff,
        "lake_manifest_sha256": lake_identity,
        "historical_lineage": lineage,
        "atlas_run_id": atlas_root.name,
        "atlas_run_root": str(atlas_root),
        "runs_root": str(resolved_runs_root),
        "cohort_path_relative_to_runs_root": output_relative,
        "evidence_roots": {
            "atlas_run_root": atlas_root_relative,
            "playhand_campaign_root": _relative_to_runs_root(
                campaign_dir,
                resolved_runs_root,
                label="Level C PlayHand campaign evidence root",
            ),
        },
        "playhand_campaign_id": campaign_id,
        "playhand_campaign_root": str(campaign_dir),
        "playhand_campaign_layout": campaign_layout,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "outcome": outcome,
        "outcome_reason": outcome_reason,
        "artifact_sha256": dict(sorted(artifact_sha256.items())),
    }
    payload = {
        **identity,
        "manifest_id": _sha256_bytes(_canonical_json(identity).encode("utf-8")),
    }
    _write_immutable(resolved_output_path, payload)
    return payload


def validate_level_c_cohort(
    path: Path, *, relocated_runs_root: Path | None = None
) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(f"Invalid Level C cohort: {path}") from exc
    identity = {key: value for key, value in payload.items() if key != "manifest_id"}
    expected_id = _sha256_bytes(_canonical_json(identity).encode("utf-8"))
    _require_equal("Level C manifest id", payload.get("manifest_id"), expected_id)
    _require_equal("Level C schema", payload.get("schema"), LEVEL_C_COHORT_SCHEMA)
    _validate_frozen_cohort_outcome(payload)
    cutoff = canonical_timestamp(payload.get("as_of_date"))
    lake_identity = _require_sha256_identity(
        "Level C lake_manifest_sha256", payload.get("lake_manifest_sha256")
    )
    cohort_path = path.expanduser().resolve()
    recorded_runs_root = Path(str(payload.get("runs_root") or "")).expanduser()
    runs_root = _resolve_validation_runs_root(
        payload,
        cohort_path=cohort_path,
        relocated_runs_root=relocated_runs_root,
    )
    _, atlas_root, recorded_campaign_root = _cohort_evidence_roots(
        payload, active_runs_root=runs_root
    )
    campaign_id = _campaign_id_path_component(str(payload.get("playhand_campaign_id") or ""))
    _require_equal("Level C Atlas run id", payload.get("atlas_run_id"), atlas_root.name)
    lineage = _validate_atlas_root(
        atlas_root, cutoff=cutoff, lake_manifest_sha256=lake_identity
    )
    _require_equal("Frozen Level C historical lineage", payload.get("historical_lineage"), lineage)
    campaign_root, campaign_layout = _resolve_playhand_campaign(runs_root, campaign_id)
    _require_equal(
        "Level C PlayHand campaign root", str(campaign_root), str(recorded_campaign_root)
    )
    stored_campaign_layout = payload.get("playhand_campaign_layout")
    if stored_campaign_layout is not None:
        _require_equal(
            "Level C PlayHand campaign layout", campaign_layout, stored_campaign_layout
        )
    campaign_metadata, campaign_summary = _validate_campaign(
        campaign_root,
        recorded_runs_root=recorded_runs_root,
        active_runs_root=runs_root,
        campaign_id=campaign_id,
        layout=campaign_layout,
        cutoff=cutoff,
        lake_manifest_sha256=lake_identity,
        atlas_root=atlas_root,
        lineage=lineage,
    )
    live_candidates, lane_roots = _discover_candidates(
        runs_root=runs_root,
        recorded_runs_root=recorded_runs_root,
        campaign_root=campaign_root,
        campaign_id=campaign_id,
        layout=campaign_layout,
        cutoff=cutoff,
        lake_manifest_sha256=lake_identity,
        lineage=lineage,
        target_runs=int(campaign_metadata["target_runs"]),
        campaign_summary=campaign_summary,
    )
    _require_equal("Frozen Level C candidates", payload.get("candidates"), live_candidates)
    live_outcome, live_outcome_reason = _frozen_cohort_outcome(live_candidates)
    _require_equal("Frozen Level C live outcome", payload.get("outcome"), live_outcome)
    _require_equal(
        "Frozen Level C live outcome reason", payload.get("outcome_reason"), live_outcome_reason
    )

    expected_hashes = payload.get("artifact_sha256")
    if not isinstance(expected_hashes, dict) or not expected_hashes:
        raise LevelCCohortError("Frozen Level C artifact inventory is missing")
    live_hashes = _hash_tree(atlas_root, namespace="atlas")
    live_hashes.update(_hash_tree(campaign_root, namespace="playhand-campaign"))
    for lane_root in lane_roots:
        live_hashes.update(
            _hash_tree(lane_root, namespace=f"playhand-lanes/{lane_root.name}")
        )
    _require_equal(
        "Frozen Level C evidence inventory",
        dict(sorted(expected_hashes.items())),
        dict(sorted(live_hashes.items())),
    )
    return payload


def cohort_attempt_ids(payload: dict[str, Any]) -> list[str]:
    if payload.get("outcome") == LEVEL_C_COHORT_OUTCOME_NO_DEFENSIBLE_CANDIDATES:
        _validate_frozen_cohort_outcome(payload)
        return []
    return [
        str(row.get("attempt_id") or "")
        for row in payload.get("candidates") or []
        if str(row.get("attempt_id") or "").strip()
    ]


def bind_level_c_evidence_rows(
    rows: Iterable[dict[str, Any]],
    *,
    cohort: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if cohort.get("outcome") == LEVEL_C_COHORT_OUTCOME_NO_DEFENSIBLE_CANDIDATES:
        _validate_frozen_cohort_outcome(cohort)
    allowed = set(cohort_attempt_ids(cohort))
    campaign_plan_id = str(cohort.get("manifest_id") or "")
    cutoff = canonical_timestamp(cohort.get("as_of_date"))
    lake_identity = str(cohort.get("lake_manifest_sha256") or "")
    bound: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for source_row in rows:
        row = dict(source_row)
        attempt_id = str(row.get("attempt_id") or "")
        if attempt_id not in allowed:
            rejected.append({"attempt_id": attempt_id, "reason": "outside_frozen_cohort"})
            continue
        records = discover_evidence_artifact_bundles(row.get("artifact_dir"))
        matches = [
            record
            for record in records
            if record.get("campaign_plan_id") == campaign_plan_id
            and record.get("evidence_role") == "portfolio_selection"
            and record.get("selection_data_end") == cutoff
            and record.get("lake_manifest_sha256") == lake_identity
        ]
        if len(matches) != 1 or matches[0].get("validation_status") != "valid":
            rejected.append(
                {
                    "attempt_id": attempt_id,
                    "reason": "missing_or_invalid_level_c_portfolio_evidence",
                    "match_count": len(matches),
                }
            )
            continue
        record = matches[0]
        result_path = Path(str(record.get("result_path") or ""))
        calendar_path = Path(str(record.get("calendar_curve_path") or ""))
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            rejected.append(
                {"attempt_id": attempt_id, "reason": "invalid_level_c_result"}
            )
            continue
        if not calendar_path.is_file():
            rejected.append(
                {"attempt_id": attempt_id, "reason": "missing_level_c_calendar_curve"}
            )
            continue
        aggregate = ((result.get("data") or {}).get("aggregate") or {})
        score_lab = aggregate.get("score_lab") or aggregate.get("scoreLab") or {}
        metrics = aggregate.get("best_cell_path_metrics") or {}
        score = score_lab.get("score")
        trade_count = (
            aggregate.get("resolved_trade_count_max")
            or metrics.get("trade_count")
            or row.get("trade_count_36m")
        )
        trades_per_month = (
            metrics.get("trades_per_month")
            or aggregate.get("trades_per_month")
            or row.get("trades_per_month_36m")
        )
        row.update(
            {
                "score_36m": score,
                "score_lab_score_36m": score,
                "score_basis_36m": score_lab.get("version") or "level_c_bounded_score_lab",
                "trade_count_36m": trade_count,
                "trades_per_month_36m": trades_per_month,
                "full_backtest_validation_status_36m": "valid",
                "full_backtest_validation_reason_codes_36m": [],
                "full_backtest_result_path_36m": str(result_path),
                "full_backtest_curve_path_36m": record.get("curve_path"),
                "full_backtest_calendar_curve_path_36m": str(calendar_path),
                "full_backtest_recommended_curve_path_36m": record.get(
                    "recommended_curve_path"
                ),
                "full_backtest_evidence_plan_id_36m": record.get("evidence_plan_id"),
                "full_backtest_evidence_role_36m": record.get("evidence_role"),
                "full_backtest_requested_horizon_months_36m": record.get(
                    "requested_horizon_months"
                ),
                "level_c_cohort_id": cohort.get("cohort_id"),
                "level_c_manifest_id": campaign_plan_id,
                "level_c_as_of_date": cutoff,
                "level_c_lake_manifest_sha256": lake_identity,
            }
        )
        bound.append(row)
    missing = sorted(allowed - {str(row.get("attempt_id") or "") for row in bound})
    for attempt_id in missing:
        if not any(item.get("attempt_id") == attempt_id for item in rejected):
            rejected.append({"attempt_id": attempt_id, "reason": "cohort_candidate_missing"})
    return bound, rejected
