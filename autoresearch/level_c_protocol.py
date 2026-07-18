"""Immutable Level C protocol manifests.

This module deliberately has no execution dependency: it records the bounded
research contract that Atlas and PlayHand must later satisfy.
"""

from __future__ import annotations

import json
import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .evidence_plan import canonical_json, canonical_sha256, canonical_timestamp


LEVEL_C_PROTOCOL_SCHEMA = "autoresearch-level-c-protocol-v1"
LEVEL_C_PROTOCOL_AUTHORITY_SCHEMA = "autoresearch-level-c-protocol-authority-v1"
_SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
_SAFE_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
_WORKER_IMAGE_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,254}$"
_INITIAL_CUTOFF_KEYS = ("A", "B", "C", "D")
_MUTABLE_PATH_TOKENS = ("global", "prior", "mutable", "latest", "current")


class LevelCProtocolError(RuntimeError):
    """Raised when a Level C protocol is malformed, mutable, or tampered with."""


class LevelCProtocolAuthority(BaseModel):
    """External trust anchor binding a protocol to one generation manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["autoresearch-level-c-protocol-authority-v1"] = (
        LEVEL_C_PROTOCOL_AUTHORITY_SCHEMA
    )
    research_generation_id: str
    generation_manifest_sha256: str = Field(pattern=_SHA256_PATTERN)
    protocol_manifest_id: str = Field(pattern=_SHA256_PATTERN)
    protocol_file_sha256: str = Field(pattern=_SHA256_PATTERN)
    authority_id: str = Field(pattern=_SHA256_PATTERN)

    @field_validator("research_generation_id")
    @classmethod
    def _validate_generation_id(cls, value: str) -> str:
        return _safe_identifier(value, label="authority research_generation_id")

    @model_validator(mode="after")
    def _validate_authority_id(self) -> "LevelCProtocolAuthority":
        identity = self.model_dump(mode="json", exclude={"authority_id"})
        if self.authority_id != canonical_sha256(identity):
            raise ValueError("Level C protocol authority hash mismatch")
        return self


def _as_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _midnight_utc(value: Any) -> str:
    timestamp = canonical_timestamp(value)
    if not timestamp.endswith("T00:00:00Z"):
        raise ValueError("protocol boundaries must be canonical UTC midnight instants")
    return timestamp


def _safe_identifier(value: Any, *, label: str) -> str:
    token = str(value or "").strip()
    import re

    if not re.fullmatch(_SAFE_IDENTIFIER_PATTERN, token):
        raise ValueError(f"{label} must be a safe identifier")
    return token


def _worker_image_reference(value: Any) -> str:
    token = str(value or "").strip()
    import re

    if not re.fullmatch(_WORKER_IMAGE_PATTERN, token) or ".." in token:
        raise ValueError("worker_image must be a safe OCI image reference")
    return token


def _safe_artifact_path(value: Any, *, label: str) -> str:
    token = str(value or "").strip().replace("\\", "/")
    if not token:
        raise ValueError(f"{label} must be a non-empty immutable artifact location")
    candidate = Path(token)
    if candidate.is_absolute() or candidate.drive or token.startswith("/"):
        raise ValueError(f"{label} must be relative to the active runs root")
    parts = [part.casefold() for part in token.split("/") if part]
    if not parts or parts[0] != "derived" or ".." in parts or any(
        mutable_token in part
        for part in parts
        for mutable_token in _MUTABLE_PATH_TOKENS
    ):
        raise ValueError(
            f"{label} must name an immutable location beneath the active runs derived root"
        )
    return token


class LevelCCutoffPlan(BaseModel):
    """One fixed selection/train/embargo/outer-test cutoff."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    cutoff_key: Literal["A", "B", "C", "D"]
    role: Literal["development", "validation"]
    selection_start: str
    selection_end: str
    training_start: str
    training_end: str
    embargo_start: str | None = None
    embargo_end: str | None = None
    embargo_days: int | None = Field(default=None, ge=0)
    outer_test_start: str
    outer_test_end: str
    geometry_sha256: str = Field(pattern=_SHA256_PATTERN)
    atlas_run_id: str
    playhand_campaign_id: str
    cohort_id: str
    seed: int = Field(ge=0, le=2**63 - 1)
    expected_artifact_locations: dict[str, str]

    @model_validator(mode="before")
    @classmethod
    def _normalize_boundaries(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        payload = dict(value)
        for field in (
            "selection_start",
            "selection_end",
            "training_start",
            "training_end",
            "outer_test_start",
            "outer_test_end",
        ):
            if field in payload:
                payload[field] = _midnight_utc(payload[field])
        selection_end = payload.get("selection_end")
        start = payload.get("embargo_start")
        end = payload.get("embargo_end")
        days = payload.get("embargo_days")
        if (start is None) != (end is None):
            raise ValueError("embargo_start and embargo_end must be supplied together")
        if start is None and days is None:
            raise ValueError("cutoff requires embargo start/end or embargo_days")
        if start is None:
            if selection_end is None:
                raise ValueError("selection_end is required to derive embargo boundaries")
            start = selection_end
            end = (_as_datetime(start) + timedelta(days=int(days))).isoformat().replace(
                "+00:00", "Z"
            )
        payload["embargo_start"] = _midnight_utc(start)
        payload["embargo_end"] = _midnight_utc(end)
        computed_days = (_as_datetime(payload["embargo_end"]) - _as_datetime(payload["embargo_start"])).days
        if computed_days < 0:
            raise ValueError("embargo_end must not precede embargo_start")
        if days is not None and int(days) != computed_days:
            raise ValueError("embargo_days does not match embargo boundaries")
        payload["embargo_days"] = computed_days
        return payload

    @field_validator("atlas_run_id", "playhand_campaign_id", "cohort_id")
    @classmethod
    def _validate_ids(cls, value: str, info: Any) -> str:
        return _safe_identifier(value, label=info.field_name)

    @field_validator("expected_artifact_locations")
    @classmethod
    def _validate_artifact_locations(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("expected_artifact_locations is required")
        normalized: dict[str, str] = {}
        for name, location in value.items():
            key = _safe_identifier(name, label="artifact location key")
            if any(token in key.casefold() for token in _MUTABLE_PATH_TOKENS):
                raise ValueError("artifact location may not name a mutable/global-prior location")
            normalized[key] = _safe_artifact_path(location, label=f"artifact location {key}")
        return dict(sorted(normalized.items()))

    @model_validator(mode="after")
    def _validate_geometry(self) -> "LevelCCutoffPlan":
        selection_start = _as_datetime(self.selection_start)
        selection_end = _as_datetime(self.selection_end)
        training_start = _as_datetime(self.training_start)
        training_end = _as_datetime(self.training_end)
        embargo_start = _as_datetime(self.embargo_start or "")
        embargo_end = _as_datetime(self.embargo_end or "")
        outer_start = _as_datetime(self.outer_test_start)
        outer_end = _as_datetime(self.outer_test_end)
        if not selection_start < selection_end or not training_start < training_end:
            raise ValueError("selection and training ranges must be end-exclusive and non-empty")
        if not (selection_start <= training_start < training_end <= selection_end):
            raise ValueError("training range must remain inside the selection range")
        if not (selection_end <= embargo_start <= embargo_end <= outer_start < outer_end):
            raise ValueError("selection, embargo, and outer-test ranges overlap or are unordered")
        if self.geometry_sha256 != canonical_sha256(_cutoff_geometry_payload(self)):
            raise ValueError("cutoff geometry hash mismatch")
        return self


def _cutoff_geometry_payload(value: Mapping[str, Any] | LevelCCutoffPlan) -> dict[str, Any]:
    source = value.model_dump(mode="json") if isinstance(value, LevelCCutoffPlan) else value
    return {
        field: source[field]
        for field in (
            "selection_start",
            "selection_end",
            "training_start",
            "training_end",
            "embargo_start",
            "embargo_end",
            "embargo_days",
            "outer_test_start",
            "outer_test_end",
        )
    }


class LevelCProtocol(BaseModel):
    """The fully bound, content-addressed Level C research contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["autoresearch-level-c-protocol-v1"] = LEVEL_C_PROTOCOL_SCHEMA
    protocol_name: str
    protocol_version: str
    status: Literal["frozen"]
    research_generation_id: str
    research_generation_manifest_sha256: str = Field(pattern=_SHA256_PATTERN)
    lake_semantic_sha256: str = Field(pattern=_SHA256_PATTERN)
    source_snapshot_sha256: str = Field(pattern=_SHA256_PATTERN)
    source_coverage_end: str
    universe_id: str
    universe_manifest_sha256: str = Field(pattern=_SHA256_PATTERN)
    worker_contract_id: str
    worker_contract_sha256: str = Field(pattern=_SHA256_PATTERN)
    worker_image: str
    engine_id: str
    engine_sha256: str = Field(pattern=_SHA256_PATTERN)
    scoring_policy_id: str
    scoring_policy_sha256: str = Field(pattern=_SHA256_PATTERN)
    cost_policy_id: str
    cost_policy_sha256: str = Field(pattern=_SHA256_PATTERN)
    global_seed: int = Field(ge=0, le=2**63 - 1)
    no_global_priors: Literal[True]
    no_outer_feedback: Literal[True]
    cutoff_plans: list[LevelCCutoffPlan] = Field(min_length=4, max_length=4)
    protocol_manifest_id: str = Field(pattern=_SHA256_PATTERN)

    @field_validator("source_coverage_end", mode="before")
    @classmethod
    def _normalize_coverage(cls, value: Any) -> str:
        return _midnight_utc(value)

    @field_validator("no_global_priors", "no_outer_feedback", mode="before")
    @classmethod
    def _require_explicit_no_feedback_flags(cls, value: Any, info: Any) -> bool:
        if value is not True:
            raise ValueError(f"{info.field_name} must be explicitly true")
        return True

    @field_validator(
        "protocol_name",
        "protocol_version",
        "research_generation_id",
        "universe_id",
        "worker_contract_id",
        "engine_id",
        "scoring_policy_id",
        "cost_policy_id",
    )
    @classmethod
    def _validate_protocol_ids(cls, value: str, info: Any) -> str:
        return _safe_identifier(value, label=info.field_name)

    @field_validator("worker_image")
    @classmethod
    def _validate_worker_image(cls, value: str) -> str:
        return _worker_image_reference(value)

    @model_validator(mode="after")
    def _validate_contract(self) -> "LevelCProtocol":
        keys = [plan.cutoff_key for plan in self.cutoff_plans]
        if keys != list(_INITIAL_CUTOFF_KEYS):
            raise ValueError("initial Level C protocol requires cutoffs A/B/C/D in order")
        for field in ("atlas_run_id", "playhand_campaign_id", "cohort_id"):
            values = [getattr(plan, field) for plan in self.cutoff_plans]
            if len(set(values)) != len(values):
                raise ValueError(f"cutoff {field}s must be unique")
        roles = [plan.role for plan in self.cutoff_plans]
        if roles != ["development", "development", "validation", "validation"]:
            raise ValueError("initial Level C roles must be development, development, validation, validation")
        coverage_end = _as_datetime(self.source_coverage_end)
        previous_outer_start: datetime | None = None
        for plan in self.cutoff_plans:
            if _as_datetime(plan.outer_test_end) > coverage_end:
                raise ValueError("cutoff outer test extends beyond available source coverage")
            outer_start = _as_datetime(plan.outer_test_start)
            if previous_outer_start is not None and outer_start <= previous_outer_start:
                raise ValueError("cutoff plans must be ordered by outer-test start")
            previous_outer_start = outer_start
        identity = self.identity_payload()
        expected_id = canonical_sha256(identity)
        if self.protocol_manifest_id != expected_id:
            raise ValueError("Level C protocol manifest hash mismatch")
        return self

    def identity_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"protocol_manifest_id"})


def _with_manifest_id(payload: Mapping[str, Any]) -> dict[str, Any]:
    identity = dict(payload)
    identity.pop("protocol_manifest_id", None)
    identity.setdefault("schema_version", LEVEL_C_PROTOCOL_SCHEMA)
    # Normalize nested cutoffs before hashing; their embargo boundaries may have
    # been supplied as a day count rather than explicit timestamps.
    if "source_coverage_end" in identity:
        identity["source_coverage_end"] = _midnight_utc(identity["source_coverage_end"])
    if "cutoff_plans" in identity:
        identity["cutoff_plans"] = [
            LevelCCutoffPlan.model_validate(plan).model_dump(mode="json")
            for plan in identity["cutoff_plans"]
        ]
    # Validate the whole schema except for the self-referential manifest field.
    provisional = {**identity, "protocol_manifest_id": canonical_sha256(identity)}
    normalized = LevelCProtocol.model_validate(provisional)
    normalized_identity = normalized.identity_payload()
    return {**normalized_identity, "protocol_manifest_id": canonical_sha256(normalized_identity)}


def validate_level_c_protocol(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a loaded protocol, including constraints beyond its own hash."""
    try:
        return LevelCProtocol.model_validate(payload).model_dump(mode="json")
    except (ValidationError, TypeError, ValueError) as exc:
        raise LevelCProtocolError(f"Invalid Level C protocol: {exc}") from exc


def create_level_c_protocol(path: Path, protocol: Mapping[str, Any]) -> dict[str, Any]:
    """Atomically create a new protocol file; an existing path is never reused."""
    target = Path(path).expanduser().resolve()
    try:
        payload = _with_manifest_id(protocol)
    except (ValidationError, TypeError, ValueError) as exc:
        raise LevelCProtocolError(f"Invalid Level C protocol: {exc}") from exc
    serialized = canonical_json(payload).encode("utf-8")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + f".{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise LevelCProtocolError(f"Level C protocol already exists: {target}") from exc
    finally:
        temporary.unlink(missing_ok=True)
    return load_level_c_protocol(target)


def load_level_c_protocol(
    path: Path, *, expected_manifest_id: str | None = None
) -> dict[str, Any]:
    """Load and fully validate a Level C protocol from immutable storage."""
    target = Path(path).expanduser().resolve()
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCProtocolError(f"Invalid Level C protocol file: {target}") from exc
    if not isinstance(payload, Mapping):
        raise LevelCProtocolError("Level C protocol must be a JSON object")
    protocol = validate_level_c_protocol(payload)
    if expected_manifest_id is not None and protocol["protocol_manifest_id"] != expected_manifest_id:
        raise LevelCProtocolError("Level C protocol does not match its external authority")
    return protocol


def _raw_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def create_level_c_protocol_authority(
    path: Path,
    *,
    generation_manifest_path: Path,
    protocol_path: Path,
) -> dict[str, Any]:
    """Create the immutable external trust anchor for one frozen protocol."""
    generation_source = Path(generation_manifest_path).expanduser().resolve()
    protocol_source = Path(protocol_path).expanduser().resolve()
    protocol = load_level_c_protocol(protocol_source)
    generation_sha256 = _raw_file_sha256(generation_source)
    if protocol["research_generation_manifest_sha256"] != generation_sha256:
        raise LevelCProtocolError(
            "Level C protocol does not bind the supplied generation manifest"
        )
    identity = {
        "schema_version": LEVEL_C_PROTOCOL_AUTHORITY_SCHEMA,
        "research_generation_id": protocol["research_generation_id"],
        "generation_manifest_sha256": generation_sha256,
        "protocol_manifest_id": protocol["protocol_manifest_id"],
        "protocol_file_sha256": _raw_file_sha256(protocol_source),
    }
    payload = {**identity, "authority_id": canonical_sha256(identity)}
    try:
        normalized = LevelCProtocolAuthority.model_validate(payload).model_dump(mode="json")
    except (ValidationError, TypeError, ValueError) as exc:
        raise LevelCProtocolError(f"Invalid Level C protocol authority: {exc}") from exc
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + f".{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(canonical_json(normalized).encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise LevelCProtocolError(
                f"Level C protocol authority already exists: {target}"
            ) from exc
    finally:
        temporary.unlink(missing_ok=True)
    return load_level_c_protocol_authority(
        target,
        generation_manifest_path=generation_source,
        protocol_path=protocol_source,
    )


def load_level_c_protocol_authority(
    path: Path,
    *,
    generation_manifest_path: Path,
    protocol_path: Path,
) -> dict[str, Any]:
    """Validate an authority and both exact files it anchors."""
    source = Path(path).expanduser().resolve()
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
        authority = LevelCProtocolAuthority.model_validate(payload).model_dump(mode="json")
    except (OSError, json.JSONDecodeError, ValidationError, TypeError, ValueError) as exc:
        raise LevelCProtocolError(f"Invalid Level C protocol authority: {exc}") from exc
    generation_source = Path(generation_manifest_path).expanduser().resolve()
    protocol_source = Path(protocol_path).expanduser().resolve()
    if authority["generation_manifest_sha256"] != _raw_file_sha256(generation_source):
        raise LevelCProtocolError("generation manifest does not match Level C authority")
    if authority["protocol_file_sha256"] != _raw_file_sha256(protocol_source):
        raise LevelCProtocolError("protocol file does not match Level C authority")
    protocol = load_level_c_protocol(
        protocol_source, expected_manifest_id=authority["protocol_manifest_id"]
    )
    if protocol["research_generation_id"] != authority["research_generation_id"]:
        raise LevelCProtocolError("protocol generation does not match Level C authority")
    if (
        protocol["research_generation_manifest_sha256"]
        != authority["generation_manifest_sha256"]
    ):
        raise LevelCProtocolError("protocol generation manifest does not match Level C authority")
    return authority


def _date_boundary(value: Any, *, end_exclusive: bool) -> str:
    token = str(value or "").strip()
    if "T" in token:
        parsed = _as_datetime(canonical_timestamp(token))
    else:
        parsed = datetime.fromisoformat(f"{token[:10]}T00:00:00+00:00")
    if end_exclusive:
        parsed += timedelta(days=1)
    return parsed.isoformat().replace("+00:00", "Z")


def build_initial_four_cutoff_plans(
    report_path: Path,
    *,
    global_seed: int,
    playhand_campaign_id: str | None = None,
) -> list[dict[str, Any]]:
    """Derive A/B/C/D mechanically from a completed nested-evidence report.

    The report's fold records are used only as a source of date geometry; old
    train/outer outcomes and artifact paths never enter the new protocol.  No
    cutoff date is accepted as an argument.  The initial policy intentionally
    requires the existing fifteen-day embargo geometry.
    """
    source = Path(report_path).expanduser().resolve()
    try:
        report = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCProtocolError(f"Invalid nested-evidence report: {source}") from exc
    if not isinstance(report, Mapping) or str(report.get("status") or "").lower() != "complete":
        raise LevelCProtocolError("nested-evidence report must be complete")
    fold_results = report.get("fold_results")
    if not isinstance(fold_results, list):
        raise LevelCProtocolError("nested-evidence report has no fold_results")
    fold_rows = [row for row in fold_results if isinstance(row, Mapping) and isinstance(row.get("fold"), Mapping)]
    if len(fold_rows) < 4:
        raise LevelCProtocolError("nested-evidence report needs at least four fold records")
    campaign_base = _safe_identifier(
        playhand_campaign_id or "level-c-playhand",
        label="playhand_campaign_id base",
    )
    plans: list[dict[str, Any]] = []
    for index, (key, fold_result) in enumerate(zip(_INITIAL_CUTOFF_KEYS, fold_rows[:4])):
        fold = dict(fold_result["fold"])
        if int(fold.get("embargo_days") or -1) != 15:
            raise LevelCProtocolError("initial cutoff policy requires a 15-day embargo")
        _safe_identifier(fold.get("fold_id"), label="nested fold_id")
        selection_start = _date_boundary(fold.get("train_start"), end_exclusive=False)
        selection_end = _date_boundary(fold.get("train_end"), end_exclusive=True)
        outer_start = _date_boundary(fold.get("test_start"), end_exclusive=False)
        outer_end = _date_boundary(fold.get("test_end"), end_exclusive=True)
        geometry = {
            "selection_start": selection_start,
            "selection_end": selection_end,
            "training_start": selection_start,
            "training_end": selection_end,
            "embargo_start": selection_end,
            "embargo_end": outer_start,
            "embargo_days": 15,
            "outer_test_start": outer_start,
            "outer_test_end": outer_end,
        }
        geometry_sha256 = canonical_sha256(geometry)
        digest = geometry_sha256[-16:]
        atlas_run_id = f"atlas-lc-{key.lower()}-{digest}"
        cutoff_campaign_id = f"{campaign_base}-{key.lower()}-{digest}"
        cohort_id = f"level-c-{key.lower()}-{digest}"
        artifact_locations = {
            # AtlasLab persists every run beneath ``derived/atlas-runs``.  This
            # path is a formal execution input, not a display label: PlayHand
            # later reads the seed and lineage receipts from this exact root.
            "atlas_run": f"derived/atlas-runs/{atlas_run_id}",
            "playhand_campaign": f"derived/play-hand-lab-campaigns/{cutoff_campaign_id}",
            "frozen_cohort": f"derived/level-c-cohorts/{cohort_id}.json",
            "campaign_receipt": f"derived/level-c-campaigns/{cohort_id}/campaign-state.json",
        }
        plans.append(
            {
                "cutoff_key": key,
                "role": "development" if key in {"A", "B"} else "validation",
                **geometry,
                "geometry_sha256": geometry_sha256,
                "atlas_run_id": atlas_run_id,
                "playhand_campaign_id": cutoff_campaign_id,
                "cohort_id": cohort_id,
                "seed": int(global_seed) + index,
                "expected_artifact_locations": artifact_locations,
            }
        )
    try:
        validated = [LevelCCutoffPlan.model_validate(plan).model_dump(mode="json") for plan in plans]
    except (ValidationError, ValueError) as exc:
        raise LevelCProtocolError(f"Invalid derived Level C cutoff plan: {exc}") from exc
    if [plan["role"] for plan in validated] != ["development", "development", "validation", "validation"]:
        raise AssertionError("initial Level C cutoff role policy changed")
    return validated
