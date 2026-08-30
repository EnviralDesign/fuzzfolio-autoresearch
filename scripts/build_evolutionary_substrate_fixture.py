#!/usr/bin/env python3
"""Create a portable, hash-only V37/V38 existing-construction fixture.

The committed fixture identifies every retained input by its role, relative
coordinate, byte size, and SHA-256.  A separate ignored resolver report binds
those portable coordinates to one local checkout.  Neither output copies
market data, proposal rows, result packs, or candidate payloads.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from autoresearch.temporal_qd_v5_native import (
    build_v5_bidirectional_pair_policy,
    build_v5_native_operator_authority,
    validate_v5_frozen_authority,
    validate_v5_proposal_manifest,
)


SCHEMA = "evolutionary_substrate_existing_construction_fixture_v3"
DEFAULT_MANIFEST_SHA256 = "490cac548bd735945219a8c3d85add4348d476f6190b238b2371255d37391c72"


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def artifact(
    name: str,
    path: Path,
    *,
    role: str,
    root_role: str,
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not path.is_file():
        raise RuntimeError(f"required retained artifact is missing: {path}")
    try:
        relative_path = path.relative_to(root).as_posix()
    except ValueError as error:
        raise RuntimeError(f"artifact {name} is outside its declared root {root}: {path}") from error
    portable = {
        "name": name,
        "role": role,
        "rootRole": root_role,
        "relativePath": relative_path,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }
    resolver = {**portable, "absolutePath": str(path.resolve())}
    return portable, resolver


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalized_sha256(value: str) -> str:
    value = value.removeprefix("sha256:")
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"invalid SHA-256 identity: {value!r}")
    return value


def resolve_invocation(v38_run_root: Path, manifest_sha256: str) -> Path:
    """Select exactly one retained V38 invocation by its manifest identity."""

    expected = normalized_sha256(manifest_sha256)
    invocation_root = (
        v38_run_root
        / "generations"
        / "generation-0003"
        / "proposal"
        / "native-batch"
        / "v5-proposal"
    )
    if not invocation_root.is_dir():
        raise RuntimeError(f"retained invocation directory is missing: {invocation_root}")
    matches: list[Path] = []
    for candidate in sorted(invocation_root.iterdir(), key=lambda item: item.name):
        manifest_path = candidate / "manifest.json"
        if not candidate.is_dir() or not manifest_path.is_file():
            continue
        manifest = read_json(manifest_path)
        actual = manifest.get("manifestSha256")
        if isinstance(actual, str) and normalized_sha256(actual) == expected:
            matches.append(candidate)
    if len(matches) != 1:
        raise RuntimeError(
            "expected exactly one retained invocation matching "
            f"sha256:{expected}; found {len(matches)} under {invocation_root}"
        )
    return matches[0]


def validate_twice(
    *, pair: dict[str, Any], evolvable: dict[str, Any], frozen: dict[str, Any], manifest: dict[str, Any]
) -> list[dict[str, str]]:
    passes: list[dict[str, str]] = []
    for pass_number in (1, 2):
        checked_frozen = validate_v5_frozen_authority(frozen)
        checked_manifest = validate_v5_proposal_manifest(manifest)
        native_operator = build_v5_native_operator_authority(
            pair_source_authority=pair, evolvable_module_authority=evolvable
        )
        pair_policy = build_v5_bidirectional_pair_policy(pair_source_authority=pair)
        authority = checked_frozen["authority"]
        if authority["nativeOperatorAuthority"] != native_operator:
            raise RuntimeError("current source cannot reconstruct frozen native operator authority")
        if authority["bidirectionalPairPolicy"] != pair_policy:
            raise RuntimeError("current source cannot reconstruct frozen bidirectional pair policy")
        if checked_manifest["frozenAuthority"] != checked_frozen:
            raise RuntimeError("retained proposal manifest disagrees with retained frozen authority")
        passes.append(
            {
                "pass": str(pass_number),
                "frozenAuthoritySha256": checked_frozen["authoritySha256"],
                "manifestSha256": checked_manifest["manifestSha256"],
                "nativeOperatorAuthoritySha256": native_operator["nativeOperatorAuthoritySha256"],
            }
        )
    return passes


def build(args: argparse.Namespace) -> dict[str, Any]:
    proposal_root = args.v38_run_root / "generations" / "generation-0003" / "proposal"
    invocation = resolve_invocation(args.v38_run_root, args.manifest_sha256)
    v37_generation = args.v37_run_root / "generations" / f"generation-{args.v37_parent_generation:04d}"
    v37_ledger = v37_generation / "proposal" / "identity-ledger.json"
    v37_archive = v37_generation / "native-finalization" / "archive.json"
    pair_path = args.authority_root / "pair-run-config.json"
    evolvable_path = args.authority_root / "evolvable-authority.json"
    frozen_path = invocation / "frozen-authority.json"
    manifest_path = invocation / "manifest.json"
    authority_path = invocation / "authority.json"
    pair = read_json(pair_path)
    evolvable = read_json(evolvable_path)
    frozen = read_json(frozen_path)
    manifest = read_json(manifest_path)
    validation = validate_twice(pair=pair, evolvable=evolvable, frozen=frozen, manifest=manifest)
    artifact_requests = [
        ("pair-run-config", pair_path, "both-side static source authority", "authorityRoot", args.authority_root),
        ("evolvable-authority", evolvable_path, "compiler/admission/budget authority", "authorityRoot", args.authority_root),
        ("parameters", args.authority_root / "parameters.json", "retained authority parameters", "authorityRoot", args.authority_root),
        ("frozen-authority", frozen_path, "sealed native construction closure", "v38RunRoot", args.v38_run_root),
        ("proposal-manifest", manifest_path, "existing evolved construction manifest", "v38RunRoot", args.v38_run_root),
        ("native-batch-authority", authority_path, "retained executable authority receipt", "v38RunRoot", args.v38_run_root),
        ("v37-parent-archive", v37_archive, "evolved construction parent archive input", "v37RunRoot", args.v37_run_root),
        ("v37-identity-ledger", v37_ledger, "evolved construction identity ledger input", "v37RunRoot", args.v37_run_root),
        ("proposal-attempts-receipt", proposal_root / "proposal-attempts-receipt.json", "retained attempt journal receipt", "v38RunRoot", args.v38_run_root),
        ("proposal-identity-ledger", proposal_root / "identity-ledger.json", "retained evolved proposal identity ledger", "v38RunRoot", args.v38_run_root),
        ("native-finalization-manifest", args.v38_run_root / "generations" / "generation-0003" / "native-finalization" / "manifest.json", "retained compile/finalization receipt", "v38RunRoot", args.v38_run_root),
    ]
    contents, resolver_contents = zip(
        *(artifact(name, path, role=role, root_role=root_role, root=root)
          for name, path, role, root_role, root in artifact_requests),
        strict=True,
    )
    output = {
        "schemaVersion": SCHEMA,
        "purpose": "read-only, content-addressed fixture for the existing native evolved-construction path",
        "nonGoals": [
            "no raw market data copied",
            "no proposal/result payload copied",
            "no candidate generation or archive mutation",
            "no replacement constructor",
        ],
        "entryManagementExitRecovery": {
            "entry": "pair-run-config supplies both-side module catalog, indicator policy, seed names, grammar, and operator authority",
            "management": "frozen authority binds initial protection, hold, topology, resource, and temporal policies",
            "exit": "frozen grammar contains exit productions and compiled runtime action paths",
            "recovery": "frozen grammar contains recovery/cooldown production; retained manifest enforces receipt-last native output",
        },
        "bothSides": {
            "long": {"catalogSha256": pair["longModule"]["catalogSha256"], "seedNames": pair["longModule"]["seedNames"]},
            "short": {"catalogSha256": pair["shortModule"]["catalogSha256"], "seedNames": pair["shortModule"]["seedNames"]},
        },
        "frozenAuthoritySha256": frozen["authoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "manifestSelection": {
            "manifestSha256": "sha256:" + normalized_sha256(args.manifest_sha256),
            "invocationRelativePath": invocation.relative_to(args.v38_run_root).as_posix(),
        },
        "roots": {
            "authorityRoot": "supplied argument",
            "v37RunRoot": "supplied argument",
            "v38RunRoot": "supplied argument",
        },
        "contents": list(contents),
        "validation": validation,
    }
    output["fixtureSha256"] = "sha256:" + hashlib.sha256(canonical_bytes(output)).hexdigest()
    resolver = {
        "schemaVersion": "evolutionary_substrate_fixture_local_resolver_v1",
        "fixtureSha256": output["fixtureSha256"],
        "contents": list(resolver_contents),
    }
    resolver["resolverSha256"] = "sha256:" + hashlib.sha256(canonical_bytes(resolver)).hexdigest()
    return {"portableFixture": output, "localResolverReport": resolver}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authority-root", required=True, type=Path)
    parser.add_argument("--v37-run-root", required=True, type=Path)
    parser.add_argument("--v38-run-root", required=True, type=Path)
    parser.add_argument("--v37-parent-generation", type=int, default=2)
    parser.add_argument("--manifest-sha256", default=DEFAULT_MANIFEST_SHA256)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--resolver-output", required=True, type=Path)
    args = parser.parse_args()
    value = build(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_bytes(value["portableFixture"]) + b"\n")
    args.resolver_output.parent.mkdir(parents=True, exist_ok=True)
    args.resolver_output.write_bytes(canonical_bytes(value["localResolverReport"]) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
