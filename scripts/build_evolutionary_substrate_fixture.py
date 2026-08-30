#!/usr/bin/env python3
"""Create a compact, hash-only V37/V38 existing-construction fixture.

The fixture references immutable local authority and retained construction
inputs by absolute path and SHA-256.  It deliberately does not copy market
data, proposal rows, result packs, or candidate payloads into the repository.
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


SCHEMA = "evolutionary_substrate_existing_construction_fixture_v2"


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


def artifact(name: str, path: Path, *, role: str) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"required retained artifact is missing: {path}")
    return {
        "name": name,
        "role": role,
        "absolutePath": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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
    invocation = next((proposal_root / "native-batch" / "v5-proposal").iterdir())
    v37_ledger = Path(
        r"C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-4000x1024x5-20260818-v37\run\broad-4000x1024x5\generations\generation-0002\proposal\identity-ledger.json"
    )
    v37_archive = Path(
        r"\\?\C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-4000x1024x5-20260818-v37\run\broad-4000x1024x5\generations\generation-0002\native-finalization\archive.json"
    )
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
    contents = [
        artifact("pair-run-config", pair_path, role="both-side static source authority"),
        artifact("evolvable-authority", evolvable_path, role="compiler/admission/budget authority"),
        artifact("parameters", args.authority_root / "parameters.json", role="retained authority parameters"),
        artifact("frozen-authority", frozen_path, role="sealed native construction closure"),
        artifact("proposal-manifest", manifest_path, role="existing evolved construction manifest"),
        artifact("native-batch-authority", authority_path, role="retained executable authority receipt"),
        artifact("v37-parent-archive", v37_archive, role="evolved construction parent archive input"),
        artifact("v37-identity-ledger", v37_ledger, role="evolved construction identity ledger input"),
        artifact("proposal-attempts-receipt", proposal_root / "proposal-attempts-receipt.json", role="retained attempt journal receipt"),
        artifact("proposal-identity-ledger", proposal_root / "identity-ledger.json", role="retained evolved proposal identity ledger"),
        artifact("native-finalization-manifest", args.v38_run_root / "generations" / "generation-0003" / "native-finalization" / "manifest.json", role="retained compile/finalization receipt"),
    ]
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
        "contents": contents,
        "validation": validation,
    }
    output["fixtureSha256"] = "sha256:" + hashlib.sha256(canonical_bytes(output)).hexdigest()
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authority-root", required=True, type=Path)
    parser.add_argument("--v38-run-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    value = build(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_bytes(value) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
