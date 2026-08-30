"""Join the sealed V3 corpus to frozen V38 outcomes and run fixed analyses."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from component_projection_support_v3 import canonical_bytes, read_self_hashed_json, sha256_file, sha256_prefixed


EXPECTED = {
    "members": "sha256:aeab638355de72a63cbb4fe30eab29ca59ea81cbd438a9f90f7e6eef3760ef2b",
    "forensic": "sha256:83eb8c96bd444932f8dbefdd0e71eaea53b732644d012006620696f3e9ff6a45",
    "multipanel": "sha256:c3e5a618ab238b9705a500873501f959c9548fce9c91b4a3bfc73aa8d3ed5587",
}
FEATURES = (
    "freshEventFraction",
    "eventStartsPer1000Bars",
    "forward24DirectionalHitRate",
    "forward24MeanMFEminusMAE",
)
P3_METRICS = (
    "deltaGrossR",
    "deltaModeledCostR",
    "deltaNetR",
    "deltaTradeCount",
    "deltaWorstWindowR",
)
RNG_SEED = 20260829


def value_or_none(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def check_hash(path: Path, expected: str) -> None:
    actual = sha256_file(path)
    if actual != expected:
        raise RuntimeError(f"frozen authority hash drift for {path.name}: {actual} != {expected}")


def exact_report(path: Path) -> dict[str, Any]:
    payload = read_self_hashed_json(path, "reportSha256")
    return payload


def rank_correlation(pairs: list[tuple[float | None, float | None]]) -> dict[str, Any]:
    clean = [(float(x), float(y)) for x, y in pairs if x is not None and y is not None]
    if len(clean) < 3:
        return {"n": len(clean), "spearmanRho": None, "reason": "fewer than three finite pairs"}
    x = pd.Series([item[0] for item in clean]).rank(method="average").to_numpy(dtype=float)
    y = pd.Series([item[1] for item in clean]).rank(method="average").to_numpy(dtype=float)
    if np.std(x) == 0 or np.std(y) == 0:
        return {"n": len(clean), "spearmanRho": None, "reason": "constant rank vector"}
    return {"n": len(clean), "spearmanRho": float(np.corrcoef(x, y)[0, 1]), "reason": None}


def association(rows: list[dict[str, Any]], feature: str, metric: str) -> dict[str, Any]:
    result = rank_correlation([(row["features"]["panel-3"].get(feature), row[metric]) for row in rows])
    result.update({"feature": feature, "outcomeMetric": metric})
    return result


def within_parent_association(rows: list[dict[str, Any]], feature: str, metric: str) -> dict[str, Any]:
    pairs: list[tuple[float | None, float | None]] = []
    eligible = 0
    for parent in sorted({row["parentCandidateId"] for row in rows}):
        group = [row for row in rows if row["parentCandidateId"] == parent]
        valid = [row for row in group if row["features"]["panel-3"].get(feature) is not None and row[metric] is not None]
        if len(valid) < 2:
            continue
        eligible += 1
        feature_ranks = pd.Series([row["features"]["panel-3"][feature] for row in valid]).rank(method="average")
        metric_ranks = pd.Series([row[metric] for row in valid]).rank(method="average")
        pairs.extend(zip(feature_ranks.tolist(), metric_ranks.tolist(), strict=True))
    result = rank_correlation(pairs)
    result.update({"feature": feature, "outcomeMetric": metric, "eligibleParentGroups": eligible})
    return result


def mean_or_none(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return float(sum(clean) / len(clean)) if clean else None


def aggregate_feature_rows(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    bars = sum(int(row["barCount"]) for row in rows)
    fresh = sum(int(row["freshEventBarCount"]) for row in rows)
    starts = sum(int(row["eventStartCount"]) for row in rows)
    h24 = [{item["horizonBars"]: item for item in row["forwardResponseByHorizon"]}[24] for row in rows]
    sample_count = sum(int(item["sampleCount"]) for item in h24)
    def weighted(field: str) -> float | None:
        values = [(value_or_none(item[field]), int(item["sampleCount"])) for item in h24]
        usable = [(value, weight) for value, weight in values if value is not None and weight > 0]
        if not usable:
            return None
        return float(sum(value * weight for value, weight in usable) / sum(weight for _, weight in usable))
    return {
        "freshEventFraction": float(fresh / bars) if bars else None,
        "eventStartsPer1000Bars": float(starts * 1000.0 / bars) if bars else None,
        "forward24DirectionalHitRate": weighted("directionalHitRate"),
        "forward24MeanMFEminusMAE": weighted("meanMFEminusMAE"),
        "aggregateBarCount": bars,
        "aggregateFreshEventBarCount": fresh,
        "aggregateEventStartCount": starts,
        "aggregateForwardSampleCount": sample_count,
    }


def feature_index(corpus: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, float | None]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in corpus["primaryRows"]:
        grouped[(row["componentIdentitySha256"], row["panelId"], row["direction"])].append(row)
    result = {key: aggregate_feature_rows(value) for key, value in grouped.items()}
    if len(result) != 19 * 3 * 2:
        raise RuntimeError(f"component feature panel/direction coverage drift: {len(result)}")
    return result


def selected_member_records(path: Path, candidate_ids: set[str]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            candidate_id = row.get("candidateId")
            if candidate_id in candidate_ids:
                selected[candidate_id] = {
                    "candidateId": candidate_id,
                    "authoredProgramSha256": row.get("aggregate", {}).get("authoredProgramSha256"),
                    "hasRetainedTradeEntryTimes": any(
                        bool(side.get("tradeSequence"))
                        for side in row.get("aggregate", {}).get("realizedBehavior", {}).get("sides", {}).values()
                        if isinstance(side, dict)
                    ),
                }
    return selected


def outcome_rows(
    *,
    manifest: dict[str, Any],
    forensic: dict[str, Any],
    multipanel: dict[str, Any],
    features: dict[tuple[str, str, str], dict[str, float | None]],
    members: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    contexts = {(row["candidateId"], row["insertion"]["forensicEventId"]): row for row in manifest["contexts"]}
    cases = {(row["candidateId"], row["eventId"]): row for row in forensic["cases"]}
    if set(contexts) != set(cases) or len(contexts) != 41:
        raise RuntimeError("accepted context/forensic case identity reconciliation failed")
    multi_by_child = {row["candidateId"]: row for row in multipanel["children"]}
    if set(multi_by_child) != {key[0] for key in cases}:
        raise RuntimeError("multipanel child identity reconciliation failed")
    joined: list[dict[str, Any]] = []
    for key in sorted(contexts):
        context = contexts[key]
        case = cases[key]
        if context["parentCandidateId"] != case["parentCandidateId"] or context["side"] != case["side"]:
            raise RuntimeError(f"context parent/side drift for {key}")
        if context["component"]["indicatorId"] != case["indicatorId"]:
            raise RuntimeError(f"context indicator drift for {key}")
        if case["candidateId"] not in members:
            raise RuntimeError(f"evaluated member is missing accepted child {case['candidateId']}")
        if members[case["candidateId"]]["authoredProgramSha256"] != case["authoredProgramSha256"]:
            raise RuntimeError(f"evaluated member program drift for {case['candidateId']}")
        component_identity = context["componentIdentity"]
        side = context["side"]
        panel_features = {
            panel: features[(component_identity, panel, side)]
            for panel in ("panel-1", "panel-2", "panel-3")
        }
        p3 = case["relative"]
        deltas = case["deltas"]
        net = value_or_none(deltas["cumulativeConservativeNetR"])
        gross = value_or_none(deltas["grossNoCostNetR"])
        cost = value_or_none(deltas["costDragR"])
        if net is not None and gross is not None and cost is not None and not math.isclose(net, gross - cost, rel_tol=0.0, abs_tol=1e-9):
            raise RuntimeError(f"gross/cost/net reconciliation failed for {case['candidateId']}")
        panel_outcomes = case["panelOutcomes"]
        joined.append({
            "candidateId": case["candidateId"],
            "eventId": case["eventId"],
            "componentContextIdentitySha256": context["componentContextIdentity"],
            "componentIdentitySha256": component_identity,
            "indicatorId": case["indicatorId"],
            "side": side,
            "parentCandidateId": case["parentCandidateId"],
            "phenotypeIdentitySha256": case["phenotypeIdentitySha256"],
            "resolvedProgramSha256": case["resolvedProgramSha256"],
            "enteredBackfillCohort": bool(multi_by_child[case["candidateId"]]["enteredBackfillCohort"]),
            "p3Comparable": bool(p3["comparable"]),
            "p1Comparable": bool(panel_outcomes["panel-1"]["relative"]["comparable"]),
            "p2Comparable": bool(panel_outcomes["panel-2"]["relative"]["comparable"]),
            "p3BeatParent": bool(p3["beatParent"]),
            "p3ZeroTrade": bool(case["metrics"]["tradeCount"] == 0),
            "deltaGrossR": gross,
            "deltaModeledCostR": cost,
            "deltaNetR": net,
            "deltaTradeCount": value_or_none(deltas["tradeCount"]),
            "deltaWorstWindowR": value_or_none(deltas["worstWindowConservativeNetR"]),
            "p1DeltaNetR": value_or_none(panel_outcomes["panel-1"]["relative"]["deltaCumulativeConservativeNetR"]),
            "p1DeltaWorstWindowR": value_or_none(panel_outcomes["panel-1"]["relative"]["deltaWorstWindowConservativeNetR"]),
            "p1BeatParent": bool(panel_outcomes["panel-1"]["relative"]["beatParent"]),
            "p2DeltaNetR": value_or_none(panel_outcomes["panel-2"]["relative"]["deltaCumulativeConservativeNetR"]),
            "p2DeltaWorstWindowR": value_or_none(panel_outcomes["panel-2"]["relative"]["deltaWorstWindowConservativeNetR"]),
            "p2BeatParent": bool(panel_outcomes["panel-2"]["relative"]["beatParent"]),
            "features": panel_features,
        })
    p3_rows = [row for row in joined if row["p3Comparable"]]
    p1p2_rows = [row for row in p3_rows if row["p1Comparable"] and row["p2Comparable"]]
    counts = {
        "acceptedContexts": len(joined),
        "exactP3ParentComparableContexts": len(p3_rows),
        "realizedPhenotypesAmongExactP3": len({row["phenotypeIdentitySha256"] for row in p3_rows}),
        "childrenWithP1P2Backfill": sum(row["enteredBackfillCohort"] for row in joined),
        "exactP1P2ParentComparableContexts": len(p1p2_rows),
    }
    if counts != {
        "acceptedContexts": 41,
        "exactP3ParentComparableContexts": 25,
        "realizedPhenotypesAmongExactP3": 17,
        "childrenWithP1P2Backfill": 11,
        "exactP1P2ParentComparableContexts": 9,
    }:
        raise RuntimeError(f"outcome cohort count reconciliation failed: {counts}")
    return joined, counts


def phenotype_representatives(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["phenotypeIdentitySha256"]].append(row)
    return [
        sorted(group, key=lambda row: row["componentContextIdentitySha256"])[0]
        for _, group in sorted(grouped.items())
    ]


def enrichment(rows: list[dict[str, Any]], feature: str) -> dict[str, Any]:
    beats = [
        value for row in rows if row["p3BeatParent"]
        if (value := row["features"]["panel-3"][feature]) is not None
    ]
    losses = [
        value for row in rows if not row["p3BeatParent"]
        if (value := row["features"]["panel-3"][feature]) is not None
    ]
    return {
        "feature": feature,
        "beatParentN": len(beats),
        "notBeatParentN": len(losses),
        "beatParentMean": mean_or_none(beats),
        "notBeatParentMean": mean_or_none(losses),
        "meanDifference": None if not beats or not losses else float(sum(beats) / len(beats) - sum(losses) / len(losses)),
    }


def lopo(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for parent in sorted({row["parentCandidateId"] for row in rows}):
        held_out = [row for row in rows if row["parentCandidateId"] == parent]
        for feature in FEATURES:
            result.append({
                "heldOutParentCandidateId": parent,
                "heldOutRows": len(held_out),
                "feature": feature,
                "outcomeBlindDescriptor": True,
                "deltaNetR": association(held_out, feature, "deltaNetR"),
                "beatParent": association(held_out, feature, "p3BeatParent"),
            })
    return result


def transfer(rows: list[dict[str, Any]]) -> dict[str, Any]:
    exact = [row for row in rows if row["p1Comparable"] and row["p2Comparable"]]
    result: dict[str, Any] = {"exactComparableContextCount": len(exact), "panels": {}}
    for panel, net, worst, beat in (
        ("panel-1", "p1DeltaNetR", "p1DeltaWorstWindowR", "p1BeatParent"),
        ("panel-2", "p2DeltaNetR", "p2DeltaWorstWindowR", "p2BeatParent"),
    ):
        result["panels"][panel] = {
            "usesP3FeatureOnly": True,
            "featureAssociations": [association(exact, feature, net) for feature in FEATURES],
            "worstWindowAssociations": [association(exact, feature, worst) for feature in FEATURES],
            "beatParentEnrichment": [
                {
                    "feature": feature,
                    "rho": rank_correlation([(row["features"]["panel-3"][feature], float(row[beat])) for row in exact]),
                }
                for feature in FEATURES
            ],
        }
    result["successOnBoth"] = sum(row["p1BeatParent"] and row["p2BeatParent"] for row in exact)
    result["backfillSelectionBias"] = "all P1/P2 comparisons are retained backfill cases; they are retrospective selected sensitivity evidence"
    return result


def clustered_statistics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rng = np.random.default_rng(RNG_SEED)
    parents = sorted({row["parentCandidateId"] for row in rows})
    by_parent = {parent: [row for row in rows if row["parentCandidateId"] == parent] for parent in parents}
    output: dict[str, Any] = {"seed": RNG_SEED, "independentParentCount": len(parents), "bootstrapReplicates": 1000, "withinParentPermutationReplicates": 2000, "features": {}}
    for feature in FEATURES:
        observed = association(rows, feature, "deltaNetR")
        boots: list[float] = []
        for _ in range(1000):
            sampled: list[dict[str, Any]] = []
            for parent in rng.choice(parents, size=len(parents), replace=True):
                sampled.extend(by_parent[str(parent)])
            rho = association(sampled, feature, "deltaNetR")["spearmanRho"]
            if rho is not None:
                boots.append(float(rho))
        observed_rho = observed["spearmanRho"]
        exceed = 0
        valid = 0
        for _ in range(2000):
            permuted: list[dict[str, Any]] = []
            for parent in parents:
                group = by_parent[parent]
                metric = [row["deltaNetR"] for row in group]
                rng.shuffle(metric)
                permuted.extend([{**row, "deltaNetR": value} for row, value in zip(group, metric, strict=True)])
            rho = association(permuted, feature, "deltaNetR")["spearmanRho"]
            if rho is not None and observed_rho is not None:
                valid += 1
                exceed += abs(rho) >= abs(float(observed_rho))
        output["features"][feature] = {
            "observed": observed,
            "parentClusterBootstrap95": None if not boots else [float(np.quantile(boots, 0.025)), float(np.quantile(boots, 0.975))],
            "withinParentPermutationTwoSidedP": None if not valid else float((exceed + 1) / (valid + 1)),
        }
    return output


def topology_reference_status(root: Path) -> dict[str, Any]:
    expected = "sha256:f5c49eae57aace0c254b43f3c22e479aa02f807de7db3b283d096f9f30fa60d0"
    matches: list[str] = []
    for path in root.glob("*.json"):
        if expected in path.read_text(encoding="utf-8"):
            matches.append(path.name)
    return {
        "expectedAuthenticatedAnalysisSha256": expected,
        "status": "excluded_missing_expected_authenticated_result" if not matches else "reference_text_found_but_not_an_authenticated_analysis_result",
        "matchingLocalFiles": matches,
        "mechanismCases": None,
        "reason": "no supplied authenticated topology analysis with the required SHA was retained; no alternate artifact was substituted",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--feature-corpus", type=Path, required=True)
    parser.add_argument("--feature-seal", type=Path, required=True)
    parser.add_argument("--members", type=Path, required=True)
    parser.add_argument("--forensic", type=Path, required=True)
    parser.add_argument("--multipanel", type=Path, required=True)
    parser.add_argument("--topology-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite outcome analysis: {args.output}")
    manifest = read_self_hashed_json(args.manifest.resolve(), "manifestCanonicalPayloadSha256")
    corpus = read_self_hashed_json(args.feature_corpus.resolve(), "featureCorpusCanonicalPayloadSha256")
    seal = read_self_hashed_json(args.feature_seal.resolve(), "featureCorpusSealCanonicalPayloadSha256")
    if seal["canonicalCorpus"]["featureCorpusCanonicalPayloadSha256"] != corpus["featureCorpusCanonicalPayloadSha256"]:
        raise RuntimeError("outcome join refuses a corpus not bound by the feature seal")
    for name, path in (("members", args.members.resolve()), ("forensic", args.forensic.resolve()), ("multipanel", args.multipanel.resolve())):
        check_hash(path, EXPECTED[name])
    forensic = exact_report(args.forensic.resolve())
    multipanel = exact_report(args.multipanel.resolve())
    candidate_ids = {row["candidateId"] for row in forensic["cases"]}
    candidate_ids.update(row["parentCandidateId"] for row in forensic["cases"])
    members = selected_member_records(args.members.resolve(), candidate_ids)
    joined, counts = outcome_rows(manifest=manifest, forensic=forensic, multipanel=multipanel, features=feature_index(corpus), members=members)
    p3 = [row for row in joined if row["p3Comparable"]]
    phenotype = phenotype_representatives(p3)
    if len(phenotype) != 17:
        raise RuntimeError("outcome-blind phenotype representative count drift")
    same_panel = {
        "primaryPhenotypeDeduplicated": {
            "unit": "one lexicographically selected component-context per realized phenotype; selection uses only context identity",
            "rowCount": len(phenotype),
            "associations": [association(phenotype, feature, metric) for feature in FEATURES for metric in P3_METRICS],
            "withinParent": [within_parent_association(phenotype, feature, metric) for feature in FEATURES for metric in P3_METRICS],
            "beatParentEnrichment": [enrichment(phenotype, feature) for feature in FEATURES],
        },
        "genotypeWeightedSensitivity": {
            "rowCount": len(p3),
            "associations": [association(p3, feature, metric) for feature in FEATURES for metric in P3_METRICS],
        },
        "descriptorBoundary": "four fixed outcome-blind feature descriptors; no fitted composite score or outcome-selected weights",
    }
    parent_entries = {
        "label": "parent_entry_conditioned",
        "status": "null",
        "reason": "the sealed corpus retains aggregate window features, not timestamped raw-event projections. Although selected member snapshots retain some parent trade entry times, their component state cannot be paired after the pre-outcome seal without a prohibited rerun.",
        "memberParentRecordsWithTradeEntryTimes": sum(row["hasRetainedTradeEntryTimes"] for candidate_id, row in members.items() if candidate_id not in {item["candidateId"] for item in joined}),
    }
    payload: dict[str, Any] = {
        "schemaVersion": "temporal_qd_component_surrogate_outcome_analysis_v3",
        "featureCorpusCanonicalPayloadSha256": corpus["featureCorpusCanonicalPayloadSha256"],
        "featureCorpusSealCanonicalPayloadSha256": seal["featureCorpusSealCanonicalPayloadSha256"],
        "outcomeAuthorities": {name: EXPECTED[name] for name in sorted(EXPECTED)},
        "outcomeReconciliation": counts,
        "parentConditioned": parent_entries,
        "joinedRows": joined,
        "samePanel": same_panel,
        "leaveOneParentOut": {"effectiveIndependentParentCount": len({row["parentCandidateId"] for row in phenotype}), "folds": lopo(phenotype)},
        "crossPanelTransfer": transfer(p3),
        "suppressionDecomposition": {
            "tradeReductionAssociations": [association(phenotype, feature, "deltaTradeCount") for feature in FEATURES],
            "modeledCostAssociations": [association(phenotype, feature, "deltaModeledCostR") for feature in FEATURES],
            "grossAssociations": [association(phenotype, feature, "deltaGrossR") for feature in FEATURES],
            "netAssociations": [association(phenotype, feature, "deltaNetR") for feature in FEATURES],
            "zeroTradeAssociations": [association(phenotype, feature, "p3ZeroTrade") for feature in FEATURES],
        },
        "topologyMechanismCheck": topology_reference_status(args.topology_root.resolve()),
        "statistics": clustered_statistics(phenotype),
        "primaryClassification": "insufficient_evidence",
        "classificationReason": "Only three independent parent clusters underpin the 17 phenotype representatives; S2 is unavailable after the seal and the required authenticated topology reference is absent. The retrospective associations cannot distinguish a general, context-conditioned, suppression-only, or no-useful surrogate.",
        "safetyBoundary": {
            "candidateStrategyReplay": False,
            "childEvaluation": False,
            "workerGatewayVastGenerationCalibration": False,
            "archiveMutation": False,
            "productionPolicyChange": False,
        },
    }
    payload["outcomeAnalysisCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "outcomeAnalysisCanonicalPayloadSha256": payload["outcomeAnalysisCanonicalPayloadSha256"],
        "primaryClassification": payload["primaryClassification"],
        "outcomeReconciliation": counts,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
