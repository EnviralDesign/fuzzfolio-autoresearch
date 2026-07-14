from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .instrument_universe import research_eligibility_report, universe_provenance


FIXED_COHORT_SCHEMA = "autoresearch-fixed-corpus-cohort-v1"


class FixedCohortError(RuntimeError):
    pass


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise FixedCohortError(f"Missing cohort source artifact: {path}") from exc
    return digest.hexdigest()


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FixedCohortError(f"Invalid {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise FixedCohortError(f"{label} must be a JSON object: {path}")
    return payload


def _require_sha256(value: Any, *, label: str) -> str:
    token = str(value or "").strip().lower()
    if token.startswith("sha256:"):
        token = token.removeprefix("sha256:")
    if len(token) != 64 or any(character not in "0123456789abcdef" for character in token):
        raise FixedCohortError(f"{label} must be a SHA-256 digest")
    return token


def _normalized_attempt_id(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise FixedCohortError(f"Malformed attempt id in {label}")
    token = value.strip()
    if not token or token != value:
        raise FixedCohortError(f"Malformed attempt id in {label}")
    return token


def _validate_snapshot_manifest(
    *,
    campaign_root: Path,
    snapshot_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    manifest = _read_json(manifest_path, label="candidate snapshot manifest")
    manifest_candidate_count = manifest.get("candidate_count")
    if isinstance(manifest_candidate_count, bool) or not isinstance(
        manifest_candidate_count, int
    ) or manifest_candidate_count < 0:
        raise FixedCohortError("Candidate snapshot manifest has malformed candidate_count")
    observed_snapshot_sha256 = _file_sha256(snapshot_path)
    artifact_hashes = manifest.get("artifact_sha256")
    if isinstance(artifact_hashes, dict):
        try:
            key = str(snapshot_path.relative_to(campaign_root)).replace("\\", "/")
        except ValueError as exc:
            raise FixedCohortError("Candidate snapshot is outside its campaign root") from exc
        expected_snapshot_sha256 = _require_sha256(
            artifact_hashes.get(key), label="candidate snapshot manifest artifact hash"
        )
    else:
        declared_path = Path(str(manifest.get("path") or "")).expanduser()
        if not declared_path.is_absolute():
            declared_path = (campaign_root / declared_path).resolve()
        if declared_path.resolve() != snapshot_path:
            raise FixedCohortError("Candidate snapshot manifest path does not match source")
        expected_snapshot_sha256 = _require_sha256(
            manifest.get("sha256"), label="candidate snapshot manifest hash"
        )
    if observed_snapshot_sha256 != expected_snapshot_sha256:
        raise FixedCohortError("Immutable candidate snapshot changed or is missing")
    return {
        "path": str(manifest_path),
        "sha256": _file_sha256(manifest_path),
        "snapshot_sha256": observed_snapshot_sha256,
        "candidate_count": manifest_candidate_count,
    }


def _snapshot_candidates(snapshot_path: Path) -> list[dict[str, Any]]:
    snapshot = _read_json(snapshot_path, label="candidate snapshot")
    if snapshot.get("schema_version") != 1:
        raise FixedCohortError("Candidate snapshot has an unsupported schema version")
    candidates = snapshot.get("candidates")
    if not isinstance(candidates, list):
        raise FixedCohortError("Candidate snapshot has no candidate list")
    if snapshot.get("candidate_count") != len(candidates):
        raise FixedCohortError("Candidate snapshot candidate_count does not match candidates")
    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, candidate in enumerate(candidates):
        if not isinstance(candidate, dict):
            raise FixedCohortError(f"Candidate snapshot entry {index} is not an object")
        attempt_id = _normalized_attempt_id(
            candidate.get("attempt_id"), label=f"candidate snapshot entry {index}"
        )
        if attempt_id in seen_ids:
            raise FixedCohortError(f"Candidate snapshot contains duplicate attempt id: {attempt_id}")
        instruments = candidate.get("instruments")
        if not isinstance(instruments, list):
            raise FixedCohortError(
                f"Candidate snapshot entry {attempt_id} has malformed instruments"
            )
        if any(
            not isinstance(item, str) or not item or item != item.strip()
            for item in instruments
        ):
            raise FixedCohortError(
                f"Candidate snapshot entry {attempt_id} has malformed instruments"
            )
        seen_ids.add(attempt_id)
        normalized.append(
            {"attempt_id": attempt_id, "instruments": [str(item) for item in instruments]}
        )
    return normalized


def _exclusion_reasons(instruments: Iterable[str]) -> list[dict[str, Any]]:
    report = research_eligibility_report(instruments)
    reasons: list[dict[str, Any]] = []
    if not report["instruments"]:
        reasons.append({"code": "no_instruments"})
    if report["ineligible"]:
        reasons.append(
            {"code": "ineligible_instruments", "instruments": sorted(report["ineligible"])}
        )
    if report["unknown"]:
        reasons.append(
            {"code": "unknown_instruments", "instruments": sorted(report["unknown"])}
        )
    return reasons


def _material_identity(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in {"created", "manifest_id"}}


def _manifest_content(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "manifest_id"}


def _write_create_only(path: Path, payload: dict[str, Any]) -> None:
    serialized = _canonical_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FixedCohortError(f"Fixed cohort output already exists: {path}")
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(serialized)
        os.link(temporary, path)
    except FileExistsError as exc:
        raise FixedCohortError(f"Fixed cohort output creation raced: {path}") from exc
    finally:
        if temporary.exists():
            temporary.unlink()


def freeze_fixed_corpus_cohort(
    *, campaign_root: Path, cohort_id: str, output_path: Path
) -> dict[str, Any]:
    resolved_campaign_root = campaign_root.expanduser().resolve()
    resolved_output_path = output_path.expanduser().resolve()
    cohort_token = str(cohort_id or "").strip()
    if not cohort_token or any(character in cohort_token for character in "\\/"):
        raise FixedCohortError("cohort_id must be a non-empty path-safe token")
    snapshot_path = resolved_campaign_root / "inputs" / "candidate-snapshot.json"
    snapshot_manifest_path = (
        resolved_campaign_root / "inputs" / "candidate-snapshot-manifest.json"
    )
    manifest_source = _validate_snapshot_manifest(
        campaign_root=resolved_campaign_root,
        snapshot_path=snapshot_path,
        manifest_path=snapshot_manifest_path,
    )
    candidates = _snapshot_candidates(snapshot_path)
    if manifest_source["candidate_count"] != len(candidates):
        raise FixedCohortError("Candidate snapshot manifest candidate_count does not match source")
    included: list[str] = []
    excluded: list[dict[str, Any]] = []
    for candidate in candidates:
        reasons = _exclusion_reasons(candidate["instruments"])
        if reasons:
            excluded.append(
                {
                    "attempt_id": candidate["attempt_id"],
                    "instruments": sorted(
                        {
                            str(item).strip().upper()
                            for item in candidate["instruments"]
                            if str(item).strip()
                        }
                    ),
                    "reasons": reasons,
                }
            )
        else:
            included.append(candidate["attempt_id"])
    attempt_ids = sorted(included)
    excluded = sorted(excluded, key=lambda item: item["attempt_id"])
    identity = {
        "schema": FIXED_COHORT_SCHEMA,
        "cohort_id": cohort_token,
        "source": {
            "campaign_id": resolved_campaign_root.name,
            "campaign_root": str(resolved_campaign_root),
            "candidate_snapshot_path": str(snapshot_path),
            "candidate_snapshot_sha256": manifest_source["snapshot_sha256"],
            "candidate_snapshot_manifest_path": manifest_source["path"],
            "candidate_snapshot_manifest_sha256": manifest_source["sha256"],
        },
        "development_universe": universe_provenance(),
        "source_candidate_count": len(candidates),
        "attempt_ids": attempt_ids,
        "excluded_candidates": excluded,
    }
    if len(attempt_ids) != len(set(attempt_ids)):
        raise FixedCohortError("Fixed cohort contains duplicate attempt ids")
    if resolved_output_path.exists():
        existing = validate_fixed_corpus_cohort(resolved_output_path)
        if _material_identity(existing) != identity:
            raise FixedCohortError(f"Fixed cohort output collides with different content: {resolved_output_path}")
        return existing
    payload = {
        **identity,
        "created": {"created_at": datetime.now(timezone.utc).isoformat()},
    }
    payload["manifest_id"] = _sha256_bytes(
        _canonical_json(_manifest_content(payload)).encode("utf-8")
    )
    _write_create_only(resolved_output_path, payload)
    return payload


def validate_fixed_corpus_cohort(path: Path) -> dict[str, Any]:
    resolved_path = path.expanduser().resolve()
    payload = _read_json(resolved_path, label="fixed cohort manifest")
    if payload.get("schema") != FIXED_COHORT_SCHEMA:
        raise FixedCohortError("Unsupported fixed cohort schema")
    created = payload.get("created")
    if not isinstance(created, dict) or not str(created.get("created_at") or "").strip():
        raise FixedCohortError("Fixed cohort is missing creation metadata")
    identity = _material_identity(payload)
    expected_manifest_id = _sha256_bytes(
        _canonical_json(_manifest_content(payload)).encode("utf-8")
    )
    if payload.get("manifest_id") != expected_manifest_id:
        raise FixedCohortError("Fixed cohort manifest id does not authenticate its content")
    attempt_ids = payload.get("attempt_ids")
    if not isinstance(attempt_ids, list):
        raise FixedCohortError("Fixed cohort has malformed attempt_ids")
    normalized_ids = [_normalized_attempt_id(value, label="fixed cohort") for value in attempt_ids]
    if normalized_ids != sorted(normalized_ids) or len(normalized_ids) != len(set(normalized_ids)):
        raise FixedCohortError("Fixed cohort attempt_ids must be sorted and unique")
    excluded = payload.get("excluded_candidates")
    if not isinstance(excluded, list):
        raise FixedCohortError("Fixed cohort has malformed excluded_candidates")
    excluded_ids = [_normalized_attempt_id(item.get("attempt_id"), label="excluded candidate") for item in excluded if isinstance(item, dict)]
    if (
        len(excluded_ids) != len(excluded)
        or excluded_ids != sorted(excluded_ids)
        or len(excluded_ids) != len(set(excluded_ids))
    ):
        raise FixedCohortError("Fixed cohort excluded candidates must be sorted objects")
    if set(normalized_ids) & set(excluded_ids):
        raise FixedCohortError("Fixed cohort includes an excluded attempt id")
    source = payload.get("source")
    if not isinstance(source, dict):
        raise FixedCohortError("Fixed cohort has malformed source provenance")
    campaign_root = Path(str(source.get("campaign_root") or "")).expanduser().resolve()
    snapshot_path = Path(str(source.get("candidate_snapshot_path") or "")).expanduser().resolve()
    manifest_path = Path(str(source.get("candidate_snapshot_manifest_path") or "")).expanduser().resolve()
    if snapshot_path != campaign_root / "inputs" / "candidate-snapshot.json":
        raise FixedCohortError("Fixed cohort source snapshot path is malformed")
    if manifest_path != campaign_root / "inputs" / "candidate-snapshot-manifest.json":
        raise FixedCohortError("Fixed cohort source snapshot manifest path is malformed")
    if str(source.get("campaign_id") or "") != campaign_root.name:
        raise FixedCohortError("Fixed cohort source campaign id is malformed")
    source_check = _validate_snapshot_manifest(
        campaign_root=campaign_root,
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
    )
    if source_check["snapshot_sha256"] != _require_sha256(
        source.get("candidate_snapshot_sha256"), label="fixed cohort source snapshot hash"
    ):
        raise FixedCohortError("Fixed cohort source snapshot changed")
    if source_check["sha256"] != _require_sha256(
        source.get("candidate_snapshot_manifest_sha256"), label="fixed cohort source manifest hash"
    ):
        raise FixedCohortError("Fixed cohort source snapshot manifest changed")
    if payload.get("development_universe") != universe_provenance():
        raise FixedCohortError("Fixed cohort development universe contract mismatch")
    candidates = _snapshot_candidates(snapshot_path)
    if source_check["candidate_count"] != len(candidates):
        raise FixedCohortError("Fixed cohort source manifest candidate count changed")
    if payload.get("source_candidate_count") != len(candidates):
        raise FixedCohortError("Fixed cohort source candidate count changed")
    expected_included: list[str] = []
    expected_excluded: list[dict[str, Any]] = []
    for candidate in candidates:
        reasons = _exclusion_reasons(candidate["instruments"])
        if reasons:
            expected_excluded.append(
                {
                    "attempt_id": candidate["attempt_id"],
                    "instruments": sorted(
                        {
                            str(item).strip().upper()
                            for item in candidate["instruments"]
                            if str(item).strip()
                        }
                    ),
                    "reasons": reasons,
                }
            )
        else:
            expected_included.append(candidate["attempt_id"])
    if normalized_ids != sorted(expected_included) or excluded != sorted(
        expected_excluded, key=lambda item: item["attempt_id"]
    ):
        raise FixedCohortError("Fixed cohort eligibility selection no longer matches source")
    return payload


def fixed_cohort_attempt_ids(payload: dict[str, Any]) -> list[str]:
    return list(payload["attempt_ids"])
