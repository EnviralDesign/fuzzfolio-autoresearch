"""V2.5 portable-authority and corrected-analysis cross-root verifier."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256

SCHEMA = "temporal_qd_topology_v2_5_cross_root_report_v1"


def _sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _pair(left: Path, right: Path, logical_id: str) -> dict[str, Any]:
    left_sha = _sha(left)
    right_sha = _sha(right)
    return {
        "logicalId": logical_id,
        "leftRawSha256": left_sha,
        "rightRawSha256": right_sha,
        "leftSizeBytes": left.stat().st_size,
        "rightSizeBytes": right.stat().st_size,
        "observedByteIdentical": left_sha == right_sha,
    }


def _verify_self_hash(path: Path, field: str) -> str:
    value = json.loads(path.read_text(encoding="utf-8"))
    stored = value.pop(field)
    if stored != canonical_sha256(value):
        raise ValueError(f"{path.name} self-hash mismatch")
    return stored


def build_report(
    *,
    authority_root_a: Path,
    authority_root_b: Path,
    analysis_path_a: Path,
    analysis_path_b: Path,
    inherited_v2_4_report_path: Path,
    launch_gate_path_a: Path | None = None,
    launch_gate_path_b: Path | None = None,
) -> dict[str, Any]:
    portable = [
        _pair(
            authority_root_a / "topology-panel-usefulness-policy-v2.json",
            authority_root_b / "topology-panel-usefulness-policy-v2.json",
            "topology-panel-usefulness-policy-v2.json",
        ),
        _pair(
            authority_root_a / "topology-production-reducer-contract-v3.json",
            authority_root_b / "topology-production-reducer-contract-v3.json",
            "topology-production-reducer-contract-v3.json",
        ),
        _pair(
            authority_root_a / "topology-policy-parity-corpus-v2.json",
            authority_root_b / "topology-policy-parity-corpus-v2.json",
            "topology-policy-parity-corpus-v2.json",
        ),
        _pair(analysis_path_a, analysis_path_b, "topology-production-analysis-v3.json"),
    ]
    if launch_gate_path_a is not None and launch_gate_path_b is not None:
        portable.append(
            _pair(
                launch_gate_path_a,
                launch_gate_path_b,
                "topology-production-launch-gate-v2-5.json",
            )
        )
    policy_sha = _verify_self_hash(
        authority_root_a / "topology-panel-usefulness-policy-v2.json",
        "panelUsefulnessPolicySha256",
    )
    contract_sha = _verify_self_hash(
        authority_root_a / "topology-production-reducer-contract-v3.json",
        "reducerContractSha256",
    )
    parity_sha = _verify_self_hash(
        authority_root_a / "topology-policy-parity-corpus-v2.json",
        "parityCorpusSha256",
    )
    analysis_sha = _verify_self_hash(analysis_path_a, "analysisSha256")
    inherited = json.loads(inherited_v2_4_report_path.read_text(encoding="utf-8"))
    inherited_unsigned = dict(inherited)
    inherited_sha = inherited_unsigned.pop("crossRootReportSha256")
    if inherited_sha != canonical_sha256(inherited_unsigned):
        raise ValueError("inherited V2.4 cross-root report self-hash mismatch")
    if inherited.get("allPortableArtifactsByteIdentical") is not True:
        raise ValueError("inherited candidate/task/campaign cross-root proof failed")
    forbidden_roots = {
        str(authority_root_a.resolve()).replace("\\", "/"),
        str(authority_root_b.resolve()).replace("\\", "/"),
        str(analysis_path_a.parent.resolve()).replace("\\", "/"),
        str(analysis_path_b.parent.resolve()).replace("\\", "/"),
    }
    portable_text = "\n".join(
        path.read_text(encoding="utf-8").replace("\\", "/")
        for path in (
            authority_root_a / "topology-panel-usefulness-policy-v2.json",
            authority_root_a / "topology-production-reducer-contract-v3.json",
            authority_root_a / "topology-policy-parity-corpus-v2.json",
            analysis_path_a,
        )
    )
    report: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "portableArtifacts": portable,
        "allPortableArtifactsByteIdentical": all(row["observedByteIdentical"] for row in portable),
        "noAbsoluteHostRootInScientificAuthority": all(root not in portable_text for root in forbidden_roots),
        "panelUsefulnessPolicySha256": policy_sha,
        "reducerContractSha256": contract_sha,
        "parityCorpusSha256": parity_sha,
        "analysisSha256": analysis_sha,
        "inheritedV2_4CandidateTaskCampaignCrossRootReportSha256": inherited_sha,
        "existingCandidateTaskCampaignIdentitiesPreserved": True,
        "operationalRootBoundClassification": [
            ".native-v5-campaign-freeze-manifest.json",
            "campaign-output-manifest.json",
            "campaign-output-checkpoint.json",
        ],
    }
    report["crossRootReportSha256"] = canonical_sha256(report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority-root-a", type=Path, required=True)
    parser.add_argument("--authority-root-b", type=Path, required=True)
    parser.add_argument("--analysis-a", type=Path, required=True)
    parser.add_argument("--analysis-b", type=Path, required=True)
    parser.add_argument("--inherited-v2-4-report", type=Path, required=True)
    parser.add_argument("--launch-gate-a", type=Path)
    parser.add_argument("--launch-gate-b", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = build_report(
        authority_root_a=args.authority_root_a,
        authority_root_b=args.authority_root_b,
        analysis_path_a=args.analysis_a,
        analysis_path_b=args.analysis_b,
        inherited_v2_4_report_path=args.inherited_v2_4_report,
        launch_gate_path_a=args.launch_gate_a,
        launch_gate_path_b=args.launch_gate_b,
    )
    if not report["allPortableArtifactsByteIdentical"] or not report["noAbsoluteHostRootInScientificAuthority"]:
        raise SystemExit("V2.5 portable artifact drifted across roots")
    args.output.write_text(canonical_json(report) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
