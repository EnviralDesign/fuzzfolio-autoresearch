"""Complete frozen V4 outcome reporting without reopening feature or outcome authorities."""
from __future__ import annotations
import argparse, hashlib, json, math
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
EXPECTED_TOPOLOGY = "sha256:f5c49eae57aace0c254b43f3c22e479aa02f807de7db3b283d096f9f30fa60d0"

def canonical_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")

def sha256_prefixed(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""): digest.update(chunk)
    return "sha256:" + digest.hexdigest()

def read_self_hashed_json(path: Path, field: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8")); expected = payload.pop(field, None); actual = sha256_prefixed(canonical_bytes(payload))
    if expected != actual: raise RuntimeError(f"self-hash mismatch for {path.name}: {expected!r} != {actual!r}")
    payload[field] = expected; return payload

def rank_correlation(pairs: list[tuple[float | None, float | None]]) -> dict[str, Any]:
    clean = [(float(x), float(y)) for x, y in pairs if x is not None and y is not None]
    if len(clean) < 3: return {"n": len(clean), "spearmanRho": None, "reason": "fewer than three finite pairs"}
    x = pd.Series([item[0] for item in clean]).rank(method="average").to_numpy(dtype=float); y = pd.Series([item[1] for item in clean]).rank(method="average").to_numpy(dtype=float)
    if not math.isfinite(float(np.std(x))) or not math.isfinite(float(np.std(y))) or np.std(x) == 0 or np.std(y) == 0: return {"n": len(clean), "spearmanRho": None, "reason": "constant rank vector"}
    return {"n": len(clean), "spearmanRho": float(np.corrcoef(x, y)[0, 1]), "reason": None}

def association(rows: list[dict[str, Any]], feature: str, metric: str) -> dict[str, Any]:
    result = rank_correlation([(row["features"]["panel-3"].get(feature), row[metric]) for row in rows]); result.update({"feature": feature, "outcomeMetric": metric}); return result

def transfer_all(rows: list[dict[str, Any]], features: list[str]) -> dict[str, Any]:
    exact = [row for row in rows if row["p1Comparable"] and row["p2Comparable"]]
    result: dict[str, Any] = {"exactComparableContextCount": len(exact), "panels": {}}
    for panel, net, worst, beat in (("panel-1", "p1DeltaNetR", "p1DeltaWorstWindowR", "p1BeatParent"), ("panel-2", "p2DeltaNetR", "p2DeltaWorstWindowR", "p2BeatParent")):
        result["panels"][panel] = {"usesP3FeatureOnly": True, "featureAssociations": [association(exact, feature, net) for feature in features], "worstWindowAssociations": [association(exact, feature, worst) for feature in features], "beatParentEnrichment": [{"feature": feature, "rho": rank_correlation([(row["features"]["panel-3"].get(feature), float(row[beat])) for row in exact])} for feature in features]}
    result["successOnBoth"] = sum(row["p1BeatParent"] and row["p2BeatParent"] for row in exact)
    result["backfillSelectionBias"] = "all P1/P2 comparisons are retained backfill cases; they are retrospective selected sensitivity evidence"
    return result

def within_parent_side(rows: list[dict[str, Any]], feature: str) -> dict[str, Any]:
    pairs: list[tuple[float, float]] = []; groups = 0
    for parent, side in sorted({(r["parentCandidateId"], r["side"]) for r in rows}):
        group = [r for r in rows if (r["parentCandidateId"], r["side"]) == (parent, side) and r["features"]["panel-3"].get(feature) is not None and r.get("deltaNetR") is not None]
        if len(group) < 2: continue
        groups += 1
        pairs.extend(zip(pd.Series([r["features"]["panel-3"][feature] for r in group]).rank(method="average").tolist(), pd.Series([r["deltaNetR"] for r in group]).rank(method="average").tolist(), strict=True))
    result = rank_correlation(pairs); result.update({"feature": feature, "outcomeMetric": "deltaNetR", "eligibleParentSideGroups": groups}); return result

def topology_crosswalk(topology: dict[str, Any], joined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ids = {row["candidateId"] for row in joined}; result: list[dict[str, Any]] = []
    for block in topology["blocks"]:
        _, parent, side = block["blockId"].split("|")
        children = [row for row in joined if row["parentCandidateId"] == parent and row["side"] == side]
        result.append({"blockId": block["blockId"], "parentCandidateId": parent, "side": side, "arms": block["arms"], "labels": block["labels"], "descriptiveTotals": block["descriptiveTotals"], "matchingFrozenComponentRows": [{"candidateId": row["candidateId"], "componentIdentitySha256": row["componentIdentitySha256"], "componentContextIdentitySha256": row["componentContextIdentitySha256"], "indicatorId": row["indicatorId"]} for row in children], "eventArmPresentInFrozenComponentOutcomeCohort": block["arms"]["E"] in ids, "correctedMetricDistinguishesEventArm": "not_evaluable", "limitation": "The authenticated P/E/T/TE arm ids are native topology-run identities, not component-cohort candidate ids. The frozen component-only corpus crosswalks to the same parent and side, but cannot identify an E arm as one component feature without reopening the topology execution path."})
    return result

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True); parser.add_argument("--topology", type=Path, required=True); parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists(): raise RuntimeError("refusing to overwrite V4 completion output")
    base = read_self_hashed_json(args.base, "outcomeAnalysisCanonicalPayloadSha256")
    topology_text = args.topology.read_text(encoding="utf-8")
    if EXPECTED_TOPOLOGY not in topology_text: raise RuntimeError("required authenticated topology analysis SHA is absent")
    topology = json.loads(topology_text); p3 = [row for row in base["joinedRows"] if row["p3Comparable"]]; features = base["featureContract"]["usableDescriptors"]
    payload: dict[str, Any] = {"schemaVersion": "temporal_qd_component_surrogate_outcome_completion_v4", "baseOutcomeAnalysisCanonicalPayloadSha256": base["outcomeAnalysisCanonicalPayloadSha256"], "featureCorpusCanonicalPayloadSha256": base["featureCorpusCanonicalPayloadSha256"], "featureCorpusSealCanonicalPayloadSha256": base["featureCorpusSealCanonicalPayloadSha256"], "parentEntryConditionedCanonicalPayloadSha256": base["parentEntryConditionedCanonicalPayloadSha256"], "completion": {"allHorizonCrossPanelTransfer": transfer_all(p3, features), "suppressionWorstWindow": [association(p3, feature, "deltaWorstWindowR") for feature in features], "withinParentSideRankAssociation": [within_parent_side(p3, feature) for feature in features], "topologyMechanismCheck": {"sourceSha256": sha256_file(args.topology), "requiredAuthenticatedAnalysisSha256": EXPECTED_TOPOLOGY, "authenticatedShaTextVerified": True, "caseCrosswalk": topology_crosswalk(topology, base["joinedRows"])}, "effectiveConclusion": {"contextFreeComponentSurrogate": "not_supported", "parentConditionedComponentSurrogate": "insufficient_evidence", "suppressionActivitySurrogate": "insufficient_evidence", "overallNextStep": "operator-context conditioning"}, "nextRungDesignOnly": {"status": "design_only_not_executed", "proposal": "If separately authorized, test predeclared operator-context strata before any component-surrogate selection claim.", "notAuthorizedOrExecuted": ["candidate strategy replay", "child evaluation", "worker or gateway work", "generation or calibration", "archive or production-policy mutation"]}}, "safetyBoundary": base["safetyBoundary"]}
    payload["outcomeCompletionCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload)); args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"); print(json.dumps({"outcomeCompletionCanonicalPayloadSha256": payload["outcomeCompletionCanonicalPayloadSha256"]}, sort_keys=True)); return 0

if __name__ == "__main__": raise SystemExit(main())
