"""Filesystem discovery for the original V38 pair/native authority.

This module never launches market evaluation or generation. It only records
the exact paths that exist, the identity SHAs they bind, and the precise
error when a required file is missing.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_qd_v38_followup_audit import DEFAULT_CATALOG, DEFAULT_V38_ROOT

DEFAULT_AUTHORITY_ROOT = Path(
    r"C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-native-4000x1024x5-20260813-v1\authority"
)
DEFAULT_PAIR_RUN_CONFIG = DEFAULT_AUTHORITY_ROOT / "pair-run-config.json"
DEFAULT_EVOLVABLE_AUTHORITY = DEFAULT_AUTHORITY_ROOT / "evolvable-authority.json"
DEFAULT_ROTATING_EVIDENCE = DEFAULT_AUTHORITY_ROOT / "rotating-evidence" / "rotating-evidence-config.json"
EXPECTED_PAIR_RUN_CONFIG_SHA256 = "sha256:2fe936bec06e5b8541eff8869703350ae7203c22e555e9eac038aa4dad752e8c"
EXPECTED_WORKER_CONTRACT_SHA256 = "sha256:40292e2a62171f1d13fda9c5e9ba953d3e04d4270845889caabb5aa80648f4c4"
DISCOVERY_SCHEMA = "temporal_qd_v38_followup_authority_discovery_v7"


def _load_json(path: Path) -> tuple[Mapping[str, Any] | None, str | None]:
    if not path.is_file():
        return None, f"missing file: {path}"
    try:
        return json.loads(path.read_text(encoding="utf-8-sig")), None
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"{type(exc).__name__}: {exc} ({path})"


def _record(path: Path, *, role: str, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    payload, error = _load_json(path) if path.suffix in {".json"} else (None, None)
    row = {
        "role": role,
        "path": str(path),
        "exists": path.exists(),
        "isFile": path.is_file(),
        "isDir": path.is_dir(),
        "sizeBytes": path.stat().st_size if path.exists() else None,
        "error": error if path.suffix in {".json"} else (None if path.exists() else f"missing: {path}"),
    }
    if extra:
        row.update(dict(extra))
    if isinstance(payload, Mapping) and "schemaVersion" in payload:
        row["schemaVersion"] = payload.get("schemaVersion")
    return row


def discover_v38_followup_authority_v7(
    *,
    v38_root: Path | None = None,
    authority_root: Path | None = None,
    pair_run_config: Path | None = None,
    evolvable_authority: Path | None = None,
    rotating_evidence: Path | None = None,
    catalog_path: Path | None = None,
) -> dict[str, Any]:
    checked: list[dict[str, Any]] = []
    errors: list[str] = []

    campaign = Path(v38_root or DEFAULT_V38_ROOT)
    launch_path = campaign / "launch-identity.json"
    launch, launch_error = _load_json(launch_path)
    checked.append(_record(launch_path, role="launchIdentity", extra={"loadError": launch_error}))
    if launch_error:
        errors.append(launch_error)

    discovered_authority = None
    discovered_run_root = None
    if isinstance(launch, Mapping):
        if isinstance(launch.get("authorityRoot"), str):
            discovered_authority = Path(launch["authorityRoot"])
        if isinstance(launch.get("runRoot"), str):
            discovered_run_root = Path(launch["runRoot"])

    authority = Path(authority_root or discovered_authority or DEFAULT_AUTHORITY_ROOT)
    pair_cfg_path = Path(pair_run_config or (authority / "pair-run-config.json"))
    evo_path = Path(evolvable_authority or (authority / "evolvable-authority.json"))
    rotating_path = Path(rotating_evidence or (authority / "rotating-evidence" / "rotating-evidence-config.json"))
    catalog = Path(catalog_path or DEFAULT_CATALOG)
    generation = campaign / "run" / "g2-parents-800" / "generations" / "generation-0003"
    if discovered_run_root is not None:
        generation = discovered_run_root / "generations" / "generation-0003"
    parent_material = generation / "proposal" / "parent-material.jsonl"
    v38_archive = generation / "native-finalization" / "archive.json"
    cumulative = generation / "native-finalization" / "evidence" / "cumulative-archive.json"
    state_path = (discovered_run_root or (campaign / "run" / "g2-parents-800")) / "state.json"

    paths = {
        "v38CampaignRoot": campaign,
        "v38RunRoot": discovered_run_root or (campaign / "run" / "g2-parents-800"),
        "authorityRoot": authority,
        "pairRunConfig": pair_cfg_path,
        "evolvableAuthority": evo_path,
        "rotatingEvidenceConfig": rotating_path,
        "constructionCatalog": authority / "rotating-evidence" / "construction-catalog.json",
        "catalog": catalog,
        "parentMaterial": parent_material,
        "v38Archive": v38_archive,
        "cumulativeArchive": cumulative,
        "runState": state_path,
        "panel1Template": authority / "rotating-evidence" / "panel-1-template-preparation.json",
        "panel2Template": authority / "rotating-evidence" / "panel-2-template-preparation.json",
        "panel3Template": authority / "rotating-evidence" / "panel-3-template-preparation.json",
        "panel4Template": authority / "rotating-evidence" / "panel-4-template-preparation.json",
    }
    for role, path in paths.items():
        checked.append(_record(path, role=role))
        row = checked[-1]
        if row["error"]:
            errors.append(str(row["error"]))

    pair_payload, pair_error = _load_json(pair_cfg_path)
    pair_sha = pair_payload.get("pairRunConfigSha256") if isinstance(pair_payload, Mapping) else None
    worker_sha = None
    if isinstance(launch, Mapping) and isinstance(launch.get("worker"), Mapping):
        worker_sha = launch["worker"].get("contractSha256")

    body = {
        "schemaVersion": DISCOVERY_SCHEMA,
        "v38Root": str(campaign),
        "authorityRoot": str(authority),
        "pairRunConfigPath": str(pair_cfg_path),
        "evolvableAuthorityPath": str(evo_path),
        "rotatingEvidenceConfigPath": str(rotating_path),
        "catalogPath": str(catalog),
        "parentMaterialPath": str(parent_material),
        "v38ArchivePath": str(v38_archive),
        "cumulativeArchivePath": str(cumulative),
        "runStatePath": str(state_path),
        "launchIdentityPath": str(launch_path),
        "pairRunConfigSha256": pair_sha,
        "expectedPairRunConfigSha256": EXPECTED_PAIR_RUN_CONFIG_SHA256,
        "pairRunConfigShaMatchesExpected": pair_sha == EXPECTED_PAIR_RUN_CONFIG_SHA256,
        "workerContractSha256": worker_sha,
        "expectedWorkerContractSha256": EXPECTED_WORKER_CONTRACT_SHA256,
        "checkedPaths": checked,
        "errors": errors,
        "readyToOpenPairAuthority": (
            not errors
            and pair_sha == EXPECTED_PAIR_RUN_CONFIG_SHA256
            and pair_cfg_path.is_file()
            and evo_path.is_file()
            and parent_material.is_file()
        ),
        "syntheticFallbackUsed": False,
        "marketEvaluationLaunched": False,
        "generationLaunched": False,
    }
    if pair_error:
        body["pairRunConfigLoadError"] = pair_error
        body["readyToOpenPairAuthority"] = False
    body["discoverySha256"] = canonical_sha256(body)
    return body
