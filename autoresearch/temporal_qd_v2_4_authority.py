"""Generate the content-addressed V2.4 reducer contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_production_reducer_v2 import (
    CONTRACT_SCHEMA,
    GRAPH_SCHEMA,
    REPLICATION_RULE_SHA256,
    SCHEMA,
    SCIENTIFIC_SHA256,
)


def _raw_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def build_contract(repo_root: Path) -> dict[str, Any]:
    reducer = repo_root / "autoresearch" / "temporal_qd_topology_production_reducer_v2.py"
    reporting = repo_root / "autoresearch" / "temporal_qd_topology_replication_survival_v2.py"
    rust_opener = repo_root / "rust" / "temporal-qd" / "crates" / "qd-campaign-seal" / "src" / "campaign_output.rs"
    rust_projection = repo_root / "rust" / "temporal-qd" / "crates" / "qd-kernel" / "src" / "topology_replication_survival_v2.rs"
    parity = json.loads(
        (repo_root / "research" / "temporal-qd" / "rust-canonical-authority-v2-4" / "topology-replication-parity-corpus-v2.json").read_text()
    )
    contract: dict[str, Any] = {
        "schemaVersion": CONTRACT_SCHEMA,
        "analysisSchema": SCHEMA,
        "authenticatedGraphSchema": GRAPH_SCHEMA,
        "scientificContractSha256": SCIENTIFIC_SHA256,
        "replicationRuleSha256": REPLICATION_RULE_SHA256,
        "parityCorpusSha256": parity["corpusSha256"],
        "sources": [
            {"path": path.relative_to(repo_root).as_posix(), "rawSha256": _raw_sha(path)}
            for path in (reducer, reporting, rust_opener, rust_projection)
        ],
        "primaryInputs": "exact_three_authenticated_campaign_output_checkpoints_plus_frozen_v2_3_authorities",
        "callerComputedMetricDictionariesAccepted": False,
        "callerIdentityValidityBooleanAccepted": False,
        "metricEquality": "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
        "fixedPnlMarginPermitted": False,
        "crossPanelPoolingPermitted": False,
        "crossPanelCompensationPermitted": False,
        "typedUnavailableMechanismEvidenceRequired": True,
        "untouchedConfirmationRequired": True,
    }
    contract["reducerContractSha256"] = canonical_sha256(contract)
    return contract


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    contract = build_contract(args.repo_root.resolve())
    args.output.write_text(canonical_json(contract) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
