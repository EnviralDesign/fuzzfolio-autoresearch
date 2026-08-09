"""Post-generation immutable materialization for proposal-lineage evidence.

This module is deliberately a sidecar. Evolution and archive selection never
import it: after a generation is complete, a caller supplies a compact,
immutable proposal-lineage-source.json envelope and this module seals the
lineage input/report beside the completed artifacts. The sidecar is therefore
safe to retry and cannot alter a live campaign's population or selection.

Historical generations do not contain the complete operator + observed
execution + realized behavior tuple required for causal attribution. They are
read-only: absence of the source envelope is a normal None result, never a
reason to reconstruct or guess history from aggregate funnel records.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_proposal_lineage import (
    build_proposal_lineage_report,
    seal_proposal_lineage_inputs,
    verify_proposal_lineage_report,
)


PROPOSAL_LINEAGE_SOURCE_SCHEMA = "temporal_proposal_lineage_source_v1"
PROPOSAL_LINEAGE_ARTIFACT_SCHEMA = "temporal_proposal_lineage_artifact_v1"
PROPOSAL_LINEAGE_ARTIFACT_MANIFEST_SCHEMA = "temporal_proposal_lineage_artifact_manifest_v1"
PROPOSAL_LINEAGE_UNAVAILABLE_SCHEMA = "temporal_proposal_lineage_unavailable_v1"
DEFAULT_SOURCE_RELATIVE_PATH = Path("native-finalization") / "proposal-lineage-source.json"
DEFAULT_OUTPUT_RELATIVE_PATH = Path("native-finalization") / "proposal-lineage"
DEFAULT_UNAVAILABLE_RELATIVE_PATH = Path("native-finalization") / "proposal-lineage-unavailable.json"

_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,239}$")
_FILE_NAMES = {
    "source": "proposal-lineage-source.json",
    "input": "proposal-lineage-input.json",
    "report": "proposal-lineage-report.json",
    "manifest": "proposal-lineage-artifact.json",
}
_UNAVAILABLE_REASONS = frozenset({
    "operator_application_not_sealed",
    "observed_execution_attribution_not_sealed",
    "canonical_evidence_components_not_sealed",
    "realized_behavior_evidence_binding_not_sealed",
    "retention_evidence_not_sealed",
    "full_ancestry_or_external_parent_evidence_not_sealed",
})


def _clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be finite canonical JSON") from exc


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "")
    if not _SHA.fullmatch(token):
        raise TemporalDiscoveryContractError(f"{name} must be a lowercase sha256 identity")
    return token


def _token(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SAFE_TOKEN.fullmatch(token):
        raise TemporalDiscoveryContractError(f"{name} must be a safe explicit identifier")
    return token


def _integer(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TemporalDiscoveryContractError(f"{name} must be a non-negative integer")
    return value


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return value


def _rows(value: Any, *, name: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise TemporalDiscoveryContractError(f"{name} must be an array")
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _relative_path(value: Any, *, name: str) -> str:
    token = str(value or "").replace("\\", "/")
    candidate = Path(token)
    if not token or candidate.is_absolute() or ".." in candidate.parts or candidate.name in {"", "."}:
        raise TemporalDiscoveryContractError(f"{name} must be a safe relative path")
    return candidate.as_posix()


def _normalized_source_artifacts(value: Any) -> list[dict[str, str]]:
    rows = _rows(value, name="lineage source artifacts")
    if not rows:
        raise TemporalDiscoveryContractError("lineage source must bind at least one completed artifact")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        source = _mapping(row, name="lineage source artifact")
        path = _relative_path(source.get("relativePath"), name="lineage source artifact relativePath")
        if path in seen:
            raise TemporalDiscoveryContractError("lineage source artifact paths must be unique")
        seen.add(path)
        result.append({"relativePath": path, "sha256": _sha(source.get("sha256"), name="lineage source artifact SHA-256")})
    return sorted(result, key=lambda item: item["relativePath"])


def seal_proposal_lineage_source(source: Mapping[str, Any]) -> dict[str, Any]:
    """Canonicalize a caller-owned completed-generation lineage envelope."""

    raw = _mapping(source, name="proposal lineage source")
    if raw.get("schemaVersion") != PROPOSAL_LINEAGE_SOURCE_SCHEMA:
        raise TemporalDiscoveryContractError("proposal lineage source schema is unsupported")
    entries = [_clone(_mapping(row, name="proposal lineage source entry"), name="proposal lineage source entry") for row in _rows(raw.get("entries"), name="proposal lineage source entries")]
    if not entries:
        raise TemporalDiscoveryContractError("proposal lineage source must contain at least one entry")
    external = [_clone(_mapping(row, name="external parent evidence"), name="external parent evidence") for row in _rows(raw.get("externalParentEvidence", []), name="external parent evidence")]
    # Source producers may iterate a map or a worker result set. Make the
    # sidecar content-addressed by its identities rather than that incidental
    # arrival order; the core validator still proves parent closure.
    entries.sort(key=lambda item: str(item.get("candidateId", "")))
    external.sort(key=lambda item: str(item.get("candidateId", "")))
    result: dict[str, Any] = {
        "schemaVersion": PROPOSAL_LINEAGE_SOURCE_SCHEMA,
        "campaignId": _token(raw.get("campaignId"), name="lineage campaign ID"),
        "completedGenerationIndex": _integer(raw.get("completedGenerationIndex"), name="completed generation index"),
        "sourceArtifacts": _normalized_source_artifacts(raw.get("sourceArtifacts")),
        "entries": entries,
        "externalParentEvidence": external,
    }
    result["sourceSha256"] = canonical_sha256(result)
    supplied = raw.get("sourceSha256")
    if supplied is not None and _sha(supplied, name="lineage source SHA-256") != result["sourceSha256"]:
        raise TemporalDiscoveryContractError("proposal lineage source identity is stale")
    return result


def verify_source_artifacts(source: Mapping[str, Any], *, generation_root: Path) -> None:
    """Verify raw, caller-declared completed artifacts before sealing a report."""

    sealed = seal_proposal_lineage_source(source)
    root = Path(generation_root).resolve()
    for row in sealed["sourceArtifacts"]:
        target = (root / row["relativePath"]).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise TemporalDiscoveryContractError("lineage source artifact escapes generation root") from exc
        if not target.is_file():
            raise TemporalDiscoveryContractError("lineage source artifact is absent")
        if _file_sha256(target) != row["sha256"]:
            raise TemporalDiscoveryContractError("lineage source artifact identity is stale")


def build_proposal_lineage_artifact(source: Mapping[str, Any]) -> dict[str, Any]:
    """Build a complete in-memory sidecar without writing campaign state."""

    sealed_source = seal_proposal_lineage_source(source)
    sealed_input = seal_proposal_lineage_inputs(
        sealed_source["entries"], external_parent_evidence=sealed_source["externalParentEvidence"]
    )
    report = build_proposal_lineage_report(sealed_input)
    verify_proposal_lineage_report(sealed_input, report)
    manifest = {
        "schemaVersion": PROPOSAL_LINEAGE_ARTIFACT_MANIFEST_SCHEMA,
        "sourceSha256": sealed_source["sourceSha256"],
        "inputSha256": sealed_input["inputSha256"],
        "reportSha256": report["reportSha256"],
        "entryCount": len(sealed_input["records"]),
    }
    manifest["artifactSha256"] = canonical_sha256(manifest)
    return {
        "schemaVersion": PROPOSAL_LINEAGE_ARTIFACT_SCHEMA,
        "source": sealed_source,
        "input": sealed_input,
        "report": report,
        "manifest": manifest,
    }


def _read_json_object(path: Path, *, name: str) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"{name} is unreadable canonical JSON") from exc
    if not isinstance(loaded, dict):
        raise TemporalDiscoveryContractError(f"{name} must be a JSON object")
    return loaded


def _write_or_verify_once(path: Path, value: Mapping[str, Any]) -> None:
    expected = _canonical_bytes(value)
    if path.exists():
        try:
            actual = path.read_bytes()
        except OSError as exc:
            raise TemporalDiscoveryContractError("existing lineage artifact is unreadable") from exc
        if actual != expected:
            raise TemporalDiscoveryContractError("existing lineage artifact disagrees with immutable replay")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(expected)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if path.read_bytes() != expected:
            raise TemporalDiscoveryContractError("concurrent lineage artifact disagrees with immutable replay")


def write_proposal_lineage_source(
    source: Mapping[str, Any], *, generation_root: Path, source_relative_path: Path = DEFAULT_SOURCE_RELATIVE_PATH
) -> dict[str, Any]:
    """Write the opt-in source envelope once, without running selection.

    This is the narrow writer intended for a completed-generation finalizer:
    construct its records from already-sealed proposal/evidence/archive facts,
    write this envelope, then invoke the completed-generation materializer.
    """

    sealed = seal_proposal_lineage_source(source)
    _write_or_verify_once(Path(generation_root) / source_relative_path, sealed)
    return sealed


def write_proposal_lineage_unavailable(
    *,
    generation_root: Path,
    campaign_id: str,
    completed_generation_index: int,
    reasons: Sequence[str],
    source_artifacts: Sequence[Mapping[str, Any]],
    output_relative_path: Path = DEFAULT_UNAVAILABLE_RELATIVE_PATH,
) -> dict[str, Any]:
    """Seal why a completed generation cannot honestly emit causal lineage.

    This is not a degraded lineage report. It explicitly proves the observer
    abstained because one or more causal inputs are absent, and binds that
    abstention to the completed artifacts that were inspected.
    """

    normalized_reasons = sorted({_token(reason, name="lineage unavailable reason") for reason in reasons})
    if not normalized_reasons or any(reason not in _UNAVAILABLE_REASONS for reason in normalized_reasons):
        raise TemporalDiscoveryContractError("proposal lineage unavailable reasons are unsupported")
    normalized_artifacts = _normalized_source_artifacts(list(source_artifacts))
    root = Path(generation_root).resolve()
    for row in normalized_artifacts:
        target = (root / row["relativePath"]).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise TemporalDiscoveryContractError("lineage unavailable source artifact escapes generation root") from exc
        if not target.is_file() or _file_sha256(target) != row["sha256"]:
            raise TemporalDiscoveryContractError("lineage unavailable source artifact identity is stale")
    result: dict[str, Any] = {
        "schemaVersion": PROPOSAL_LINEAGE_UNAVAILABLE_SCHEMA,
        "campaignId": _token(campaign_id, name="lineage campaign ID"),
        "completedGenerationIndex": _integer(completed_generation_index, name="completed generation index"),
        "reasons": normalized_reasons,
        "sourceArtifacts": normalized_artifacts,
    }
    result["lineageUnavailableSha256"] = canonical_sha256(result)
    _write_or_verify_once(root / output_relative_path, result)
    return result


def materialize_proposal_lineage_artifact(
    source: Mapping[str, Any], *, generation_root: Path, output_root: Path, verify_sources: bool = True
) -> dict[str, Any]:
    """Idempotently write an immutable lineage sidecar outside live selection."""

    if verify_sources:
        verify_source_artifacts(source, generation_root=Path(generation_root))
    artifact = build_proposal_lineage_artifact(source)
    output = Path(output_root)
    for key in ("source", "input", "report", "manifest"):
        _write_or_verify_once(output / _FILE_NAMES[key], artifact[key])
    return artifact


def materialize_completed_generation_lineage(
    generation_root: Path,
    *,
    source_relative_path: Path = DEFAULT_SOURCE_RELATIVE_PATH,
    output_relative_path: Path = DEFAULT_OUTPUT_RELATIVE_PATH,
    verify_sources: bool = True,
) -> dict[str, Any] | None:
    """Explicit post-generation hook; returns None for legacy runs."""

    root = Path(generation_root)
    source_path = root / source_relative_path
    if not source_path.is_file():
        return None
    source = _read_json_object(source_path, name="proposal lineage source")
    return materialize_proposal_lineage_artifact(
        source,
        generation_root=root,
        output_root=root / output_relative_path,
        verify_sources=verify_sources,
    )
