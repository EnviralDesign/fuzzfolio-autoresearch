from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable

from .evidence_plan import canonical_timestamp, validate_replay_evidence_plan
from .ledger import load_attempts, load_run_metadata, list_run_dirs
from .evidence_artifacts import discover_evidence_artifact_bundles


LEVEL_C_COHORT_SCHEMA = "autoresearch-level-c-frozen-cohort-v1"


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
    _require_equal(
        f"{label} lake identity", plan.lake_manifest_sha256, lake_manifest_sha256
    )
    return plan.model_dump(mode="json")


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
    lake_identity = str(lake_manifest_sha256 or "").strip()
    if not lake_identity.startswith("sha256:") or len(lake_identity) != 71:
        raise LevelCCohortError("lake_manifest_sha256 must be a sha256: identity")

    atlas_root = atlas_run_root.expanduser().resolve()
    atlas_metadata_path = atlas_root / "atlas-lab-run.json"
    try:
        atlas_metadata = json.loads(atlas_metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(f"Invalid Atlas run metadata: {atlas_metadata_path}") from exc
    atlas_runtime = atlas_metadata.get("runtime") or {}
    _require_equal("Atlas as_of_date", canonical_timestamp(atlas_runtime.get("as_of_date")), cutoff)
    _require_equal("Atlas lake identity", atlas_runtime.get("lake_manifest_sha256"), lake_identity)
    _require_equal("Atlas signal executor", atlas_runtime.get("signal_atlas_executor"), "gateway")
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
            lake_identity,
        )

    campaign_dir = runs_root.expanduser().resolve() / playhand_campaign_id
    campaign_metadata = load_run_metadata(campaign_dir)
    if not campaign_metadata:
        raise LevelCCohortError(f"Missing PlayHand campaign metadata: {campaign_dir}")
    _require_equal(
        "PlayHand campaign as_of_date",
        canonical_timestamp(campaign_metadata.get("as_of_date")),
        cutoff,
    )
    _require_equal(
        "PlayHand campaign lake identity",
        campaign_metadata.get("lake_manifest_sha256"),
        lake_identity,
    )
    if str(campaign_metadata.get("run_status") or "").lower() not in {
        "complete",
        "completed",
        "promoted",
    }:
        raise LevelCCohortError("PlayHand campaign is not complete")
    atlas_seed_plan = atlas_root / "recipe-priors" / "play-hand-seed-plan.json"
    recorded_seed_plan = Path(
        str(campaign_metadata.get("play_hand_seed_plan_path") or "")
    )
    if not atlas_seed_plan.is_file():
        raise LevelCCohortError(
            f"Atlas run has no frozen PlayHand seed plan: {atlas_seed_plan}"
        )
    _require_equal(
        "PlayHand Atlas seed plan path",
        str(recorded_seed_plan.resolve()) if recorded_seed_plan.is_file() else None,
        str(atlas_seed_plan.resolve()),
    )
    _require_equal(
        "PlayHand Atlas seed plan hash",
        campaign_metadata.get("play_hand_seed_plan_sha256"),
        _file_sha256(atlas_seed_plan),
    )

    candidates: list[dict[str, Any]] = []
    lane_roots: list[Path] = []
    for run_dir in list_run_dirs(runs_root):
        metadata = load_run_metadata(run_dir)
        if str(metadata.get("parent_campaign_id") or metadata.get("lab_campaign_id") or "") != playhand_campaign_id:
            continue
        _require_equal(
            f"PlayHand lane {run_dir.name} as_of_date",
            canonical_timestamp(metadata.get("as_of_date")),
            cutoff,
        )
        _require_equal(
            f"PlayHand lane {run_dir.name} lake identity",
            metadata.get("lake_manifest_sha256"),
            lake_identity,
        )
        canonical_attempt_id = str(metadata.get("canonical_attempt_id") or "").strip()
        attempts = load_attempts(run_dir / "attempts.jsonl")
        attempt = next(
            (
                row
                for row in attempts
                if str(row.get("attempt_id") or "") == canonical_attempt_id
            ),
            None,
        )
        if not canonical_attempt_id or not isinstance(attempt, dict):
            continue
        evidence_plan = _validate_discovery_plan(
            attempt.get("evidence_plan"),
            cutoff=cutoff,
            lake_manifest_sha256=lake_identity,
            label=f"canonical attempt {canonical_attempt_id}",
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
            "observed_lake_manifest_sha256": lake_identity,
        }
        for key, value in expected_receipt.items():
            _require_equal(
                f"canonical attempt {canonical_attempt_id} receipt {key}",
                receipt.get(key),
                value,
            )
        profile_path = Path(str(attempt.get("profile_path") or ""))
        if not profile_path.is_file():
            raise LevelCCohortError(
                f"Canonical attempt profile is missing: {canonical_attempt_id}"
            )
        candidates.append(
            {
                "run_id": run_dir.name,
                "attempt_id": canonical_attempt_id,
                "profile_path": str(profile_path.resolve()),
                "profile_sha256": _file_sha256(profile_path),
                "discovery_evidence_plan_id": evidence_plan["plan_id"],
            }
        )
        lane_roots.append(run_dir)
    if not candidates:
        raise LevelCCohortError(
            f"No canonical cutoff-bounded PlayHand candidates found for {playhand_campaign_id}"
        )

    artifact_sha256 = _hash_tree(atlas_root, namespace="atlas")
    artifact_sha256.update(_hash_tree(campaign_dir, namespace="playhand-campaign"))
    for lane_root in lane_roots:
        artifact_sha256.update(
            _hash_tree(lane_root, namespace=f"playhand-lanes/{lane_root.name}")
        )
    identity = {
        "schema": LEVEL_C_COHORT_SCHEMA,
        "cohort_id": str(cohort_id).strip(),
        "as_of_date": cutoff,
        "lake_manifest_sha256": lake_identity,
        "atlas_run_id": atlas_root.name,
        "atlas_run_root": str(atlas_root),
        "runs_root": str(runs_root.expanduser().resolve()),
        "playhand_campaign_id": playhand_campaign_id,
        "candidate_count": len(candidates),
        "candidates": sorted(candidates, key=lambda row: row["attempt_id"]),
        "artifact_sha256": dict(sorted(artifact_sha256.items())),
    }
    payload = {
        **identity,
        "manifest_id": _sha256_bytes(_canonical_json(identity).encode("utf-8")),
    }
    _write_immutable(output_path.expanduser().resolve(), payload)
    return payload


def validate_level_c_cohort(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCCohortError(f"Invalid Level C cohort: {path}") from exc
    identity = {key: value for key, value in payload.items() if key != "manifest_id"}
    expected_id = _sha256_bytes(_canonical_json(identity).encode("utf-8"))
    _require_equal("Level C manifest id", payload.get("manifest_id"), expected_id)
    _require_equal("Level C schema", payload.get("schema"), LEVEL_C_COHORT_SCHEMA)
    expected_hashes = payload.get("artifact_sha256") or {}
    for candidate in payload.get("candidates") or []:
        profile_path = Path(str(candidate.get("profile_path") or ""))
        if not profile_path.is_file() or _file_sha256(profile_path) != candidate.get(
            "profile_sha256"
        ):
            raise LevelCCohortError(
                f"Frozen Level C profile changed: {profile_path}"
            )
    # Every evidence file is addressed by a stable namespace and relative path.
    atlas_root = Path(str(payload.get("atlas_run_root") or ""))
    runs_root = Path(str(payload.get("runs_root") or ""))
    campaign_root = runs_root / str(payload.get("playhand_campaign_id") or "")
    for key, expected in expected_hashes.items():
        if key.startswith("atlas/"):
            source = atlas_root / key.removeprefix("atlas/")
        elif key.startswith("playhand-campaign/"):
            source = campaign_root / key.removeprefix("playhand-campaign/")
        elif key.startswith("playhand-lanes/"):
            lane_relative = key.removeprefix("playhand-lanes/")
            source = runs_root / lane_relative
        else:
            raise LevelCCohortError(f"Unknown Level C artifact namespace: {key}")
        if not source.is_file() or _file_sha256(source) != expected:
            raise LevelCCohortError(f"Frozen Level C evidence changed: {source}")
    return payload


def cohort_attempt_ids(payload: dict[str, Any]) -> list[str]:
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
