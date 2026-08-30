#!/usr/bin/env python3
"""Run the write-neutral Rust one-step neighborhood exporter from a V3 fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any


REQUEST_SCHEMA = "temporal_qd_v5_static_neighborhood_request_v1"
REPORT_SCHEMA = "temporal_qd_v5_static_neighborhood_report_v1"
MATRIX_SCHEMA = "temporal_qd_operator_family_matrix_v1"
MATRIX_MODE = "frozen_parent_one_change_v1"


class NeighborhoodError(ValueError):
    """Raised when a portable fixture cannot be safely resolved locally."""


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise NeighborhoodError(f"expected an object in {path}")
    return value


def resolved_artifact(
    fixture: dict[str, Any], resolver: dict[str, Any], name: str
) -> Path:
    portable = {
        str(item.get("name")): item
        for item in fixture.get("contents", [])
        if isinstance(item, dict)
    }.get(name)
    local = {
        str(item.get("name")): item
        for item in resolver.get("contents", [])
        if isinstance(item, dict)
    }.get(name)
    if not isinstance(portable, dict) or not isinstance(local, dict):
        raise NeighborhoodError(f"fixture/resolver lacks required artifact {name}")
    for field in ("rootRole", "relativePath", "bytes", "sha256"):
        if portable.get(field) != local.get(field):
            raise NeighborhoodError(f"resolver binding drifted for {name}.{field}")
    path = local.get("absolutePath")
    if not isinstance(path, str):
        raise NeighborhoodError(f"resolver path is invalid for {name}")
    resolved = Path(path)
    if not resolved.is_file():
        raise NeighborhoodError(f"resolved artifact is missing: {resolved}")
    if resolved.stat().st_size != portable["bytes"] or sha256_file(resolved) != portable["sha256"]:
        raise NeighborhoodError(f"resolved artifact identity drifted: {name}")
    return resolved


def matrix_parents(config: dict[str, Any]) -> list[dict[str, str]]:
    matrix = config.get("operatorFamilyMatrix")
    if not isinstance(matrix, dict):
        raise NeighborhoodError("V38 config lacks operatorFamilyMatrix")
    if (
        matrix.get("schemaVersion") != MATRIX_SCHEMA
        or matrix.get("mode") != MATRIX_MODE
        or matrix.get("includeCrossover") is not False
        or matrix.get("mutationDepth") != 1
    ):
        raise NeighborhoodError("V38 matrix is not the frozen one-step non-crossover contract")
    parents = matrix.get("parents")
    if not isinstance(parents, list) or not parents:
        raise NeighborhoodError("V38 matrix parent list is invalid")
    output: list[dict[str, str]] = []
    seen: set[str] = set()
    for parent in parents:
        if not isinstance(parent, dict):
            raise NeighborhoodError("V38 matrix parent is invalid")
        candidate_id, role = parent.get("candidateId"), parent.get("role")
        if not isinstance(candidate_id, str) or not isinstance(role, str) or not candidate_id or not role:
            raise NeighborhoodError("V38 matrix parent fields are invalid")
        if candidate_id in seen:
            raise NeighborhoodError("V38 matrix repeats a parent candidate")
        seen.add(candidate_id)
        output.append({"candidateId": candidate_id, "role": role})
    return output


def build_request(fixture: dict[str, Any], resolver: dict[str, Any], max_plans: int) -> dict[str, Any]:
    if fixture.get("schemaVersion") != "evolutionary_substrate_existing_construction_fixture_v3":
        raise NeighborhoodError("portable fixture schema is incompatible")
    if resolver.get("fixtureSha256") != fixture.get("fixtureSha256"):
        raise NeighborhoodError("resolver does not bind the supplied portable fixture")
    if not 1 <= max_plans <= 4_000:
        raise NeighborhoodError("max plans must be between 1 and 4000")
    frozen = resolved_artifact(fixture, resolver, "frozen-authority")
    parent_material = resolved_artifact(fixture, resolver, "v38-parent-material")
    config = read_json(resolved_artifact(fixture, resolver, "v38-run-config"))
    manifest_sha = fixture.get("manifestSha256")
    if not isinstance(manifest_sha, str) or not manifest_sha.startswith("sha256:"):
        raise NeighborhoodError("portable fixture manifest identity is invalid")
    return {
        "schemaVersion": REQUEST_SCHEMA,
        "frozenAuthorityPath": str(frozen),
        "parentMaterialPath": str(parent_material),
        "parents": matrix_parents(config),
        "analysisSeed": manifest_sha,
        "maxPlans": max_plans,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    fixture = read_json(args.fixture)
    resolver = read_json(args.resolver)
    request = build_request(fixture, resolver, args.max_plans)
    args.scratch_dir.mkdir(parents=True, exist_ok=True)
    request_path = args.scratch_dir / "v5-static-neighborhood-request.json"
    request_path.write_bytes(canonical_bytes(request) + b"\n")
    completed = subprocess.run(
        [str(args.binary), "--static-neighborhood", str(request_path)],
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise NeighborhoodError(
            "static neighborhood exporter failed: " + completed.stderr.decode("utf-8", errors="replace")
        )
    report = json.loads(completed.stdout)
    if not isinstance(report, dict) or report.get("schemaVersion") != REPORT_SCHEMA:
        raise NeighborhoodError("static neighborhood exporter returned an incompatible report")
    if report.get("selectedPlanCount", 0) > args.max_plans:
        raise NeighborhoodError("static neighborhood exporter exceeded the requested cap")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_bytes(report) + b"\n")
    local_report = {
        "schemaVersion": "evolutionary_substrate_neighborhood_local_run_v1",
        "fixtureSha256": fixture["fixtureSha256"],
        "requestSha256": "sha256:" + hashlib.sha256(canonical_bytes(request)).hexdigest(),
        "requestPath": str(request_path.resolve()),
        "outputPath": str(args.output.resolve()),
        "outputSha256": sha256_file(args.output),
        "selectedPlanCount": report["selectedPlanCount"],
    }
    local_report["runSha256"] = "sha256:" + hashlib.sha256(canonical_bytes(local_report)).hexdigest()
    args.local_report_output.parent.mkdir(parents=True, exist_ok=True)
    args.local_report_output.write_bytes(canonical_bytes(local_report) + b"\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--resolver", required=True, type=Path)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--max-plans", type=int, default=4_000)
    parser.add_argument("--scratch-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--local-report-output", required=True, type=Path)
    run(parser.parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
