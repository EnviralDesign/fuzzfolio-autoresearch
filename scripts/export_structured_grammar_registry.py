#!/usr/bin/env python3
"""Export the executable Rust grammar registry and bind it to frozen authority.

This is a read-only Stage 4.5D audit utility.  It never parses Rust source;
the complete production records come from ``temporal-qd-batch
--grammar-registry`` and are checked against the retained V38 frozen-authority
registry before being written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any


SCHEMA = "evolutionary_substrate_structured_grammar_inventory_v4"
EXECUTABLE_SCHEMA = "temporal_typed_fragment_grammar_registry_projection_v1"


class GrammarExportError(RuntimeError):
    """Raised when an executable or frozen registry binding is incompatible."""


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def sha256(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GrammarExportError(f"could not load {label}: {error}") from error
    if not isinstance(value, dict):
        raise GrammarExportError(f"{label} must be a JSON object")
    return value


def executable_registry(binary: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [str(binary), "--grammar-registry"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode:
        raise GrammarExportError(f"executable grammar export failed: {completed.stderr.strip()}")
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise GrammarExportError(f"executable grammar export was not JSON: {error}") from error
    if not isinstance(value, dict) or value.get("schemaVersion") != EXECUTABLE_SCHEMA:
        raise GrammarExportError("executable grammar export has an incompatible schema")
    productions = value.get("productions")
    if not isinstance(productions, list) or len(productions) != 23:
        raise GrammarExportError("executable grammar export must contain exactly 23 productions")
    return value


def keyed_productions(productions: object, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(productions, list):
        raise GrammarExportError(f"{label} productions must be a list")
    keyed: dict[str, dict[str, Any]] = {}
    for production in productions:
        if not isinstance(production, dict) or not isinstance(production.get("productionId"), str):
            raise GrammarExportError(f"{label} production is missing productionId")
        identifier = production["productionId"]
        if identifier in keyed:
            raise GrammarExportError(f"{label} repeats productionId {identifier}")
        keyed[identifier] = production
    return keyed


def frozen_registry(frozen_authority: dict[str, Any]) -> dict[str, Any]:
    authority = frozen_authority.get("authority")
    if not isinstance(authority, dict):
        raise GrammarExportError("frozen authority lacks authority object")
    registry = authority.get("grammarRegistry")
    if not isinstance(registry, dict) or registry.get("schemaVersion") != "temporal_typed_fragment_registry_identity_v1":
        raise GrammarExportError("frozen authority grammar registry has an incompatible schema")
    productions = registry.get("productions")
    if not isinstance(productions, list) or len(productions) != 23:
        raise GrammarExportError("frozen authority must contain exactly 23 grammar productions")
    return registry


def authority_projection(production: dict[str, Any]) -> dict[str, Any]:
    keys = ("productionId", "family", "consumes", "produces", "resourceSlots", "choiceDomains")
    if any(key not in production for key in keys):
        raise GrammarExportError("executable grammar production lacks an authority-bound field")
    return {key: production[key] for key in keys}


def verify_registry(executable: dict[str, Any], frozen: dict[str, Any]) -> dict[str, Any]:
    executable_by_id = keyed_productions(executable["productions"], "executable registry")
    frozen_by_id = keyed_productions(frozen["productions"], "frozen registry")
    if set(executable_by_id) != set(frozen_by_id):
        raise GrammarExportError("executable and frozen registries name different productions")
    for identifier, executable_production in executable_by_id.items():
        if authority_projection(executable_production) != frozen_by_id[identifier]:
            raise GrammarExportError(f"frozen grammar registry drifted for {identifier}")
    expected_domains = {
        ("arm_level", "threshold"): [35, 50, 65, 75],
        ("gate_level", "threshold"): [40, 55, 70, 85],
        ("tighten_stop", "multiple"): [-0.5, 0, 0.5],
    }
    assertions: list[dict[str, Any]] = []
    for (identifier, field), expected in expected_domains.items():
        actual = executable_by_id[identifier].get("choiceDomains", {}).get(field)
        if actual != expected:
            raise GrammarExportError(f"executable grammar domain drifted for {identifier}.{field}")
        assertions.append({"productionId": identifier, "field": field, "expected": expected, "actual": actual})
    return {
        "authorityBoundFieldsMatch": True,
        "frozenRegistrySha256": frozen.get("registrySha256"),
        "representativeDomainAssertions": assertions,
    }


def generate(binary: Path, frozen_authority_path: Path) -> dict[str, Any]:
    executable = executable_registry(binary)
    frozen_authority = load_json(frozen_authority_path, "frozen authority")
    frozen = frozen_registry(frozen_authority)
    verification = verify_registry(executable, frozen)
    return {
        "schemaVersion": SCHEMA,
        "scope": "read_only_executable_registry_projection_and_frozen_authority_binding",
        "executableRegistry": executable,
        "frozenAuthorityBinding": {
            "path": str(frozen_authority_path.resolve()),
            "frozenAuthoritySha256": frozen_authority.get("authoritySha256"),
            "grammarRegistrySha256": frozen.get("registrySha256"),
        },
        "verification": verification,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--frozen-authority", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = generate(args.binary.resolve(), args.frozen_authority.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_bytes(report) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
