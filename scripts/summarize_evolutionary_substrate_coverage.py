#!/usr/bin/env python3
"""Summarize retained V37/V38 construction coverage without copying raw rows."""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def artifact(path: Path, *, status: str, detail: str) -> dict[str, Any]:
    return {
        "absolutePath": str(path),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "status": status,
        "detail": detail,
    }


def attempt_summary(path: Path) -> dict[str, Any]:
    dispositions: collections.Counter[str] = collections.Counter()
    origin_kinds: collections.Counter[str] = collections.Counter()
    ledger_effects: collections.Counter[str] = collections.Counter()
    records = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = json.loads(line)
            records += 1
            dispositions[str(value["disposition"])] += 1
            origin_kinds[str(value["originKind"])] += 1
            ledger_effects[str(value["identityLedgerEffect"])] += 1
    return {
        **artifact(path, status="observed", detail="retained proposal-attempt journal"),
        "recordCount": records,
        "dispositions": dict(sorted(dispositions.items())),
        "originKinds": dict(sorted(origin_kinds.items())),
        "identityLedgerEffects": dict(sorted(ledger_effects.items())),
    }


def archive_summary(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return {
        **artifact(path, status="observed", detail="retained finalization archive"),
        "generationIndex": value["generationIndex"],
        "candidateCountSeen": value["candidateCountSeen"],
        "candidateCountReducedThisGeneration": value["candidateCountReducedThisGeneration"],
        "memberCount": value["memberCount"],
        "occupiedCellCount": value["occupiedCellCount"],
        "paretoAdmissionCount": value["paretoAdmissionCount"],
        "paretoEvictionCount": value["paretoEvictionCount"],
        "archiveSha256": value["archiveSha256"],
    }


def coverage_for_generation(root: Path, generation: int, *, attempts_expected: bool) -> dict[str, Any]:
    proposal = root / "generations" / f"generation-{generation:04d}" / "proposal"
    archive = root / "generations" / f"generation-{generation:04d}" / "native-finalization" / "archive.json"
    ledger = proposal / "identity-ledger.json"
    attempts = proposal / "proposal-attempts.jsonl"
    data: dict[str, Any] = {
        "authored": {
            "status": "observed" if attempts.is_file() else "unavailable",
            "artifact": attempt_summary(attempts) if attempts.is_file() else None,
            "reason": None if attempts.is_file() else "no retained attempt journal at this V37 generation",
        },
        "compiled": {
            "status": "observed",
            "artifact": artifact(ledger, status="observed", detail="retained proposal identity ledger"),
            "reason": "identity ledger is a proposal construction artifact; it does not establish runtime activation",
        },
        "operatorAttempt": {
            "status": "observed" if attempts.is_file() else "unavailable",
            "artifact": attempt_summary(attempts) if attempts.is_file() else None,
            "reason": None if attempts.is_file() else "V37 retained artifact has no per-attempt journal",
        },
        "activation": {
            "status": "unavailable",
            "artifact": None,
            "reason": "no market/runtime trace is read by this offline construction audit",
        },
        "reducedEvidence": {
            "status": "observed",
            "artifact": archive_summary(archive),
            "reason": "archive reduction is observed; no component-local causal credit is inferred",
        },
        "parentArchive": {
            "status": "observed",
            "artifact": archive_summary(archive),
            "reason": "archive is a retained parent/selection authority artifact",
        },
        "selectedSurvivor": {
            "status": "observed",
            "artifact": archive_summary(archive),
            "reason": "archive membership/admission is observed; it is not claimed as profitability evidence",
        },
    }
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v37-run-root", required=True, type=Path)
    parser.add_argument("--v38-run-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    value = {
        "schemaVersion": "evolutionary_substrate_historical_coverage_v2",
        "scope": "retained V37/V38 structural construction artifacts only; no market data or result packs copied",
        "coverage": {
            "v37": coverage_for_generation(args.v37_run_root, 2, attempts_expected=False),
            "v38": coverage_for_generation(args.v38_run_root, 3, attempts_expected=True),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
