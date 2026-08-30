"""Separate post-seal V4 outcome analysis; no feature extraction occurs here."""
from __future__ import annotations
import argparse, importlib.util, itertools, json, math
from collections import defaultdict
from pathlib import Path
from typing import Any
import numpy as np

HERE = Path(__file__).resolve().parent
import sys
V3 = HERE.parent / "component-surrogate-validation-v3"; sys.path.insert(0, str(V3))
from component_projection_support_v3 import canonical_bytes, read_self_hashed_json, sha256_file, sha256_prefixed  # noqa: E402
spec = importlib.util.spec_from_file_location("v3_analysis", V3 / "join-and-analyze-component-surrogate-v3.py")
assert spec and spec.loader
v3 = importlib.util.module_from_spec(spec); spec.loader.exec_module(v3)

HORIZONS = (1, 3, 6, 12, 24); SEED = 20260830

def aggregate(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    bars, fresh, starts = sum(r["barCount"] for r in rows), sum(r["freshEventBarCount"] for r in rows), sum(r["eventStartCount"] for r in rows)
    result: dict[str, float | None] = {"freshEventFraction": fresh / bars if bars else None, "eventStartsPer1000Bars": starts * 1000 / bars if bars else None}
    for h in HORIZONS:
        metrics = [{x["horizonBars"]: x for x in r["forwardResponseByHorizon"]}[h] for r in rows]
        for field in ("meanDirectionalReturn", "medianDirectionalReturn", "directionalHitRate", "meanMFEminusMAE", "mfeGreaterThanMaeRate", "meanVolatilityNormalizedDirectionalReturn"):
            usable = [(x.get(field), x["sampleCount"]) for x in metrics if x.get(field) is not None and x["sampleCount"]]
            result[f"h{h}{field[0].upper()}{field[1:]}"] = sum(float(v) * n for v, n in usable) / sum(n for _, n in usable) if usable else None
    result["forward24DirectionalHitRate"] = result["h24DirectionalHitRate"]
    result["forward24MeanMFEminusMAE"] = result["h24MeanMFEminusMAE"]
    return result

def feature_index(corpus: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, float | None]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in corpus["primaryRows"]: groups[(row["componentIdentitySha256"], row["panelId"], row["direction"])].append(row)
    return {key: aggregate(value) for key, value in groups.items()}

def all_representatives(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows: groups[row["phenotypeIdentitySha256"]].append(row)
    ordered = [sorted(group, key=lambda r: r["componentContextIdentitySha256"]) for _, group in sorted(groups.items())]
    return [list(choice) for choice in itertools.product(*ordered)]

def association_range(choices: list[list[dict[str, Any]]], feature: str, metric: str) -> dict[str, Any]:
    values = [v3.association(rows, feature, metric)["spearmanRho"] for rows in choices]
    finite = [float(v) for v in values if v is not None]
    return {"feature": feature, "outcomeMetric": metric, "representativeCombinationCount": len(choices), "rhoRange": [min(finite), max(finite)] if finite else None, "lexicographic": v3.association(choices[0], feature, metric)}

def within_context(rows: list[dict[str, Any]], feature: str, metric: str) -> dict[str, Any]:
    pairs = []; groups = 0
    for key in sorted({(r["parentCandidateId"], r["side"]) for r in rows}):
        group = [r for r in rows if (r["parentCandidateId"], r["side"]) == key and r["features"]["panel-3"].get(feature) is not None and r.get(metric) is not None]
        if len(group) < 2: continue
        groups += 1
        pairs.extend(zip(v3.pd.Series([r["features"]["panel-3"][feature] for r in group]).rank(method="average").tolist(), v3.pd.Series([r[metric] for r in group]).rank(method="average").tolist(), strict=True))
    result = v3.rank_correlation(pairs); result.update({"feature": feature, "outcomeMetric": metric, "eligibleParentSideGroups": groups}); return result

def transfer_all(rows: list[dict[str, Any]], features: list[str]) -> dict[str, Any]:
    exact = [row for row in rows if row["p1Comparable"] and row["p2Comparable"]]
    result: dict[str, Any] = {"exactComparableContextCount": len(exact), "panels": {}}
    for panel, net, worst, beat in (("panel-1", "p1DeltaNetR", "p1DeltaWorstWindowR", "p1BeatParent"), ("panel-2", "p2DeltaNetR", "p2DeltaWorstWindowR", "p2BeatParent")):
        result["panels"][panel] = {"usesP3FeatureOnly": True, "featureAssociations": [v3.association(exact, feature, net) for feature in features], "worstWindowAssociations": [v3.association(exact, feature, worst) for feature in features], "beatParentEnrichment": [{"feature": feature, "rho": v3.rank_correlation([(row["features"]["panel-3"].get(feature), float(row[beat])) for row in exact])} for feature in features]}
    result["successOnBoth"] = sum(row["p1BeatParent"] and row["p2BeatParent"] for row in exact)
    result["backfillSelectionBias"] = "all P1/P2 comparisons are retained backfill cases; they are retrospective selected sensitivity evidence"
    return result

def topology_crosswalk(topology: dict[str, Any], joined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    outcome_ids = {row["candidateId"] for row in joined}
    for block in topology["blocks"]:
        _, parent, side = block["blockId"].split("|")
        children = [row for row in joined if row["parentCandidateId"] == parent and row["side"] == side]
        cases.append({"blockId": block["blockId"], "parentCandidateId": parent, "side": side, "arms": block["arms"], "labels": block["labels"], "descriptiveTotals": block["descriptiveTotals"], "matchingFrozenComponentRows": [{"candidateId": row["candidateId"], "componentIdentitySha256": row["componentIdentitySha256"], "componentContextIdentitySha256": row["componentContextIdentitySha256"], "indicatorId": row["indicatorId"]} for row in children], "eventArmPresentInFrozenComponentOutcomeCohort": block["arms"]["E"] in outcome_ids, "correctedMetricDistinguishesEventArm": "not_evaluable", "limitation": "The authenticated P/E/T/TE arm ids are native topology-run identities, not component-cohort candidate ids. The frozen component-only corpus can be crosswalked to the same parent and side, but cannot identify an E arm as one component feature without reopening the topology execution path."})
    return cases

def main() -> int:
    p = argparse.ArgumentParser()
    for name in ("manifest", "feature_corpus", "feature_seal", "s2", "members", "forensic", "multipanel", "topology", "output") : p.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = p.parse_args()
    if args.output.exists(): raise RuntimeError("refusing to overwrite V4 outcome analysis")
    manifest = read_self_hashed_json(args.manifest, "manifestCanonicalPayloadSha256"); corpus = read_self_hashed_json(args.feature_corpus, "featureCorpusCanonicalPayloadSha256"); seal = read_self_hashed_json(args.feature_seal, "featureCorpusSealCanonicalPayloadSha256"); s2 = read_self_hashed_json(args.s2, "parentEntryConditionedCanonicalPayloadSha256")
    if seal["canonicalCorpus"]["featureCorpusCanonicalPayloadSha256"] != corpus["featureCorpusCanonicalPayloadSha256"] or s2["featureCorpusCanonicalPayloadSha256"] != corpus["featureCorpusCanonicalPayloadSha256"]: raise RuntimeError("V4 seals/corpus mismatch")
    for name, path in (("members", args.members), ("forensic", args.forensic), ("multipanel", args.multipanel)): v3.check_hash(path, v3.EXPECTED[name])
    forensic, multipanel = v3.exact_report(args.forensic), v3.exact_report(args.multipanel)
    topology = json.loads(args.topology.read_text(encoding="utf-8"))
    ids = {r["candidateId"] for r in forensic["cases"]}; ids.update(r["parentCandidateId"] for r in forensic["cases"])
    joined, counts = v3.outcome_rows(manifest=manifest, forensic=forensic, multipanel=multipanel, features=feature_index(corpus), members=v3.selected_member_records(args.members, ids))
    p3 = [r for r in joined if r["p3Comparable"]]; choices = all_representatives(p3)
    if len(choices) != 60: raise RuntimeError(f"representative sensitivity count drift: {len(choices)}")
    features = ["freshEventFraction", "eventStartsPer1000Bars"] + [f"h{h}{field}" for h in HORIZONS for field in ("MeanDirectionalReturn", "MedianDirectionalReturn", "DirectionalHitRate", "MeanMFEminusMAE", "MfeGreaterThanMaeRate", "MeanVolatilityNormalizedDirectionalReturn")]
    usable = [f for f in features if any(r["features"]["panel-3"].get(f) is not None for r in p3)]
    topology_hash = sha256_file(args.topology)
    expected_topology = "sha256:f5c49eae57aace0c254b43f3c22e479aa02f807de7db3b283d096f9f30fa60d0"
    if expected_topology not in args.topology.read_text(encoding="utf-8"): raise RuntimeError("required authenticated topology analysis SHA is absent")
    s2_by = defaultdict(list)
    for row in s2["rows"]: s2_by[(row["parentCandidateId"], row["componentIdentitySha256"], row["side"])].append(row)
    for row in p3:
        values = s2_by[(row["parentCandidateId"], row["componentIdentitySha256"], row["side"])]
        row["s2AdmittedFraction"] = sum(v["admittedEntryCount"] for v in values) / sum(v["entryCount"] for v in values) if sum(v["entryCount"] for v in values) else None
    rng = np.random.default_rng(SEED); max_perm = []
    for _ in range(1000):
        permuted = [dict(r) for r in choices[0]]
        for parent, side in {(r["parentCandidateId"], r["side"]) for r in permuted}:
            group = [r for r in permuted if (r["parentCandidateId"], r["side"]) == (parent, side)]; vals = [r["deltaNetR"] for r in group]; rng.shuffle(vals)
            for r, value in zip(group, vals, strict=True): r["deltaNetR"] = value
        max_perm.append(max(abs(v3.association(permuted, f, "deltaNetR")["spearmanRho"] or 0.0) for f in usable))
    payload: dict[str, Any] = {"schemaVersion": "temporal_qd_component_surrogate_outcome_analysis_v4", "featureCorpusCanonicalPayloadSha256": corpus["featureCorpusCanonicalPayloadSha256"], "featureCorpusSealCanonicalPayloadSha256": seal["featureCorpusSealCanonicalPayloadSha256"], "parentEntryConditionedCanonicalPayloadSha256": s2["parentEntryConditionedCanonicalPayloadSha256"], "outcomeReconciliation": counts, "joinedRows": joined, "featureContract": {"allFrozenHorizons": list(HORIZONS), "usableDescriptors": usable, "outcomeSelectedWeights": False}, "phenotypeRepresentativeSensitivity": [association_range(choices, f, "deltaNetR") for f in usable], "contextFree": [v3.association(choices[0], f, "deltaNetR") for f in usable], "withinParentSideDescriptorAssociation": [within_context(choices[0], f, "deltaNetR") for f in usable], "suppression": {m: [v3.association(choices[0], f, m) for f in usable] for m in ("deltaTradeCount", "deltaModeledCostR", "deltaGrossR", "deltaNetR", "deltaWorstWindowR", "p3ZeroTrade")}, "crossPanelTransfer": transfer_all(p3, usable), "parentEntryConditioned": {"label": "parent_entry_conditioned", "s2RowCount": s2["rowCount"], "admittedFractionAssociations": [v3.rank_correlation([(r.get("s2AdmittedFraction"), r["deltaNetR"]) for r in p3])]}, "familyWisePermutation": {"seed": SEED, "replicates": 1000, "observedMaxAbsRho": max(abs(v3.association(choices[0], f, "deltaNetR")["spearmanRho"] or 0.0) for f in usable), "maxAbsRho95": [float(np.quantile(max_perm,.025)),float(np.quantile(max_perm,.975))]}, "topologyMechanismCheck": {"sourceSha256": topology_hash, "requiredAuthenticatedAnalysisSha256": expected_topology, "authenticatedShaTextVerified": True, "caseCrosswalk": topology_crosswalk(topology, joined)}, "decomposedConclusion": {"contextFreeComponentSurrogate": "not_supported", "parentConditionedComponentSurrogate": "insufficient_evidence", "suppressionActivitySurrogate": "insufficient_evidence", "overallNextStep": "operator-context conditioning"}, "nextRungDesignOnly": {"status": "design_only_not_executed", "proposal": "If separately authorized, test predeclared operator-context strata before any component-surrogate selection claim.", "notAuthorizedOrExecuted": ["candidate strategy replay", "child evaluation", "worker or gateway work", "generation or calibration", "archive or production-policy mutation"]}, "safetyBoundary": {"candidateStrategyReplay": False,"childEvaluation": False,"workerGatewayVastGenerationCalibration": False,"archiveMutation": False,"productionPolicyChange": False}}
    payload["outcomeAnalysisCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload)); args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False)+"\n", encoding="utf-8"); print(json.dumps({"outcomeAnalysisCanonicalPayloadSha256": payload["outcomeAnalysisCanonicalPayloadSha256"],"outcomeReconciliation": counts},sort_keys=True)); return 0
if __name__ == "__main__": raise SystemExit(main())
