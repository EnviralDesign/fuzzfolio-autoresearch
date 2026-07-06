from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .anchor_pair_atlas import (
    _clean_token,
    _clean_upper,
    _float_value,
    _int_value,
    _read_csv_rows,
    _write_csv,
    _write_json,
)
from .config import AppConfig
from .discovery_pair_atlas import DEFAULT_DISCOVERY_PAIR_DIRNAME


SCHEMA_VERSION = "discovery_cluster_atlas_v1"
DEFAULT_DISCOVERY_CLUSTER_DIRNAME = "discovery-cluster-atlas"
DEFAULT_MIN_POSITIVE_SCORE = 50.0
DEFAULT_STRONG_SCORE = 70.0
DEFAULT_MIN_SIMILARITY = 0.50
DEFAULT_MIN_SHARED_PARTNERS = 1
DEFAULT_MAX_RECIPES = 128


@dataclass(frozen=True)
class DiscoveryClusterAtlasBuildResult:
    atlas_path: Path
    indicator_clusters_csv_path: Path
    indicator_signatures_csv_path: Path
    cluster_pair_matrix_csv_path: Path
    discovered_recipes_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "discovery_cluster_atlas_json": str(self.atlas_path),
            "indicator_clusters_csv": str(self.indicator_clusters_csv_path),
            "indicator_success_signatures_csv": str(self.indicator_signatures_csv_path),
            "cluster_pair_matrix_csv": str(self.cluster_pair_matrix_csv_path),
            "discovered_recipes_json": str(self.discovered_recipes_path),
            "discovery_cluster_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    return _read_csv_rows(path)


def _merge_discovery_rows(discovery_pair_dir: Path) -> list[dict[str, Any]]:
    results_path = discovery_pair_dir / "discovery-pair-probe-results.csv"
    queue_path = discovery_pair_dir / "discovery-pair-queue.csv"
    result_rows = _load_csv_rows(results_path)
    queue_by_probe_id = {
        _clean_token(row.get("probe_id")): row
        for row in _load_csv_rows(queue_path)
        if _clean_token(row.get("probe_id"))
    }
    merged_rows: list[dict[str, Any]] = []
    for result in result_rows:
        probe_id = _clean_token(result.get("probe_id"))
        row = dict(queue_by_probe_id.get(probe_id, {}))
        row.update(result)
        row["composite_score_number"] = _float_value(row.get("composite_score"), math.nan)
        row["best_trades_number"] = _float_value(row.get("best_trades"), 0.0)
        row["signal_count_number"] = _float_value(row.get("signal_count"), 0.0)
        row["best_expectancy_r_number"] = _float_value(row.get("best_expectancy_r"), 0.0)
        row["best_profit_factor_number"] = _float_value(row.get("best_profit_factor"), 0.0)
        merged_rows.append(row)
    return merged_rows


def _score_number(row: dict[str, Any]) -> float | None:
    value = row.get("composite_score_number")
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    score = _float_value(row.get("composite_score"), math.nan)
    return score if math.isfinite(score) else None


def _weight_for_score(score: float, min_positive_score: float) -> float:
    return max(0.0, float(score) - float(min_positive_score) + 1.0)


def _counter_add(counter: dict[str, float], key: Any, amount: float = 1.0) -> None:
    token = _clean_token(key) or "unknown"
    counter[token] = counter.get(token, 0.0) + amount


def _top_keys(counter: dict[str, float], limit: int = 6) -> list[str]:
    return [
        key
        for key, _value in sorted(
            counter.items(),
            key=lambda item: (-float(item[1]), str(item[0])),
        )[:limit]
    ]


def _top_counts(counter: dict[str, float], limit: int = 6) -> list[dict[str, Any]]:
    return [
        {"key": key, "weight": round(float(value), 4)}
        for key, value in sorted(
            counter.items(),
            key=lambda item: (-float(item[1]), str(item[0])),
        )[:limit]
    ]


def _count_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        token = _clean_token(row.get(key)) or "unknown"
        counts[token] = counts.get(token, 0) + 1
    return dict(sorted(counts.items()))


def _cluster_shape(clusters: list[dict[str, Any]]) -> dict[str, Any]:
    member_counts = sorted(
        [_int_value(cluster.get("member_count")) for cluster in clusters],
        reverse=True,
    )
    return {
        "cluster_count": len(clusters),
        "singleton_clusters": sum(1 for count in member_counts if count <= 1),
        "max_member_count": member_counts[0] if member_counts else 0,
        "top_member_counts": member_counts[:8],
    }


def build_indicator_success_signatures(
    rows: list[dict[str, Any]],
    *,
    side: str,
    min_positive_score: float = DEFAULT_MIN_POSITIVE_SCORE,
    strong_score: float = DEFAULT_STRONG_SCORE,
) -> dict[str, dict[str, Any]]:
    if side not in {"first", "second"}:
        raise ValueError("side must be 'first' or 'second'")
    indicator_key = f"{side}_indicator_id"
    partner_side = "second" if side == "first" else "first"
    partner_key = f"{partner_side}_indicator_id"
    signatures: dict[str, dict[str, Any]] = {}
    for row in rows:
        indicator_id = _clean_upper(row.get(indicator_key))
        partner_id = _clean_upper(row.get(partner_key))
        if not indicator_id or not partner_id:
            continue
        signature = signatures.setdefault(
            indicator_id,
            {
                "side": side,
                "indicator_id": indicator_id,
                "signal_role": _clean_token(row.get(f"{side}_signal_role")),
                "strategy_role": _clean_token(row.get(f"{side}_strategy_role")),
                "namespace": _clean_token(row.get(f"{side}_namespace")),
                "tested_count": 0,
                "positive_count": 0,
                "strong_count": 0,
                "best_score": 0.0,
                "avg_positive_score": 0.0,
                "positive_scores": [],
                "partner_vector": {},
                "partner_role_vector": {},
                "partner_strategy_vector": {},
                "timeframe_vector": {},
                "lane_vector": {},
                "top_examples": [],
            },
        )
        signature["tested_count"] = _int_value(signature.get("tested_count")) + 1
        score = _score_number(row)
        if score is None:
            continue
        if score > _float_value(signature.get("best_score")):
            signature["best_score"] = score
        if score < min_positive_score:
            continue
        weight = _weight_for_score(score, min_positive_score)
        signature["positive_count"] = _int_value(signature.get("positive_count")) + 1
        if score >= strong_score:
            signature["strong_count"] = _int_value(signature.get("strong_count")) + 1
        signature["positive_scores"].append(score)
        vector = signature["partner_vector"]
        vector[partner_id] = max(_float_value(vector.get(partner_id)), weight)
        _counter_add(
            signature["partner_role_vector"],
            row.get(f"{partner_side}_signal_role"),
            weight,
        )
        _counter_add(
            signature["partner_strategy_vector"],
            row.get(f"{partner_side}_strategy_role"),
            weight,
        )
        _counter_add(signature["timeframe_vector"], row.get("probe_timeframe"), weight)
        _counter_add(signature["lane_vector"], row.get("discovery_lane"), weight)
        signature["top_examples"].append(
            {
                "probe_id": row.get("probe_id"),
                "partner_id": partner_id,
                "probe_timeframe": row.get("probe_timeframe"),
                "discovery_lane": row.get("discovery_lane"),
                "composite_score": round(score, 4),
                "best_trades": row.get("best_trades"),
                "best_expectancy_r": row.get("best_expectancy_r"),
                "best_profit_factor": row.get("best_profit_factor"),
            }
        )
    for signature in signatures.values():
        positive_scores = signature.pop("positive_scores", [])
        if positive_scores:
            signature["avg_positive_score"] = round(
                sum(positive_scores) / len(positive_scores),
                4,
            )
        signature["top_examples"].sort(
            key=lambda row: (
                -_float_value(row.get("composite_score")),
                str(row.get("probe_id") or ""),
            )
        )
        signature["top_examples"] = signature["top_examples"][:8]
    return signatures


def _cosine_similarity(left: dict[str, float], right: dict[str, float]) -> float:
    keys = set(left) | set(right)
    if not keys:
        return 0.0
    dot = sum(_float_value(left.get(key)) * _float_value(right.get(key)) for key in keys)
    left_norm = math.sqrt(sum(_float_value(left.get(key)) ** 2 for key in keys))
    right_norm = math.sqrt(sum(_float_value(right.get(key)) ** 2 for key in keys))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 0.0
    return dot / (left_norm * right_norm)


def _weighted_jaccard(left: dict[str, float], right: dict[str, float]) -> float:
    keys = set(left) | set(right)
    if not keys:
        return 0.0
    numerator = sum(
        min(_float_value(left.get(key)), _float_value(right.get(key)))
        for key in keys
    )
    denominator = sum(
        max(_float_value(left.get(key)), _float_value(right.get(key)))
        for key in keys
    )
    return numerator / denominator if denominator > 0 else 0.0


def signature_similarity(left: dict[str, Any], right: dict[str, Any]) -> tuple[float, int]:
    left_partners = left.get("partner_vector") if isinstance(left.get("partner_vector"), dict) else {}
    right_partners = right.get("partner_vector") if isinstance(right.get("partner_vector"), dict) else {}
    shared_partners = len(set(left_partners) & set(right_partners))
    partner_similarity = _cosine_similarity(left_partners, right_partners)
    strategy_similarity = _weighted_jaccard(
        left.get("partner_strategy_vector")
        if isinstance(left.get("partner_strategy_vector"), dict)
        else {},
        right.get("partner_strategy_vector")
        if isinstance(right.get("partner_strategy_vector"), dict)
        else {},
    )
    timeframe_similarity = _weighted_jaccard(
        left.get("timeframe_vector") if isinstance(left.get("timeframe_vector"), dict) else {},
        right.get("timeframe_vector") if isinstance(right.get("timeframe_vector"), dict) else {},
    )
    same_strategy_bonus = 0.08 if left.get("strategy_role") and left.get("strategy_role") == right.get("strategy_role") else 0.0
    similarity = (
        partner_similarity * 0.72
        + strategy_similarity * 0.16
        + timeframe_similarity * 0.04
        + same_strategy_bonus
    )
    return round(min(1.0, similarity), 4), shared_partners


def _cluster_centroid(signatures: list[dict[str, Any]]) -> dict[str, Any]:
    centroid: dict[str, Any] = {
        "partner_vector": {},
        "partner_strategy_vector": {},
        "timeframe_vector": {},
        "strategy_role": "",
    }
    strategy_counts: dict[str, float] = {}
    for signature in signatures:
        for vector_name in ("partner_vector", "partner_strategy_vector", "timeframe_vector"):
            target = centroid[vector_name]
            source = signature.get(vector_name) if isinstance(signature.get(vector_name), dict) else {}
            for key, value in source.items():
                target[key] = target.get(key, 0.0) + _float_value(value)
        _counter_add(strategy_counts, signature.get("strategy_role"), 1.0)
    centroid["strategy_role"] = _top_keys(strategy_counts, limit=1)[0] if strategy_counts else ""
    return centroid


def cluster_indicator_signatures(
    signatures_by_indicator: dict[str, dict[str, Any]],
    *,
    side: str,
    min_similarity: float = DEFAULT_MIN_SIMILARITY,
    min_shared_partners: int = DEFAULT_MIN_SHARED_PARTNERS,
) -> list[dict[str, Any]]:
    candidates = [
        signature
        for signature in signatures_by_indicator.values()
        if _int_value(signature.get("positive_count")) > 0
    ]
    candidates.sort(
        key=lambda signature: (
            -_int_value(signature.get("strong_count")),
            -_float_value(signature.get("best_score")),
            -_int_value(signature.get("positive_count")),
            str(signature.get("indicator_id") or ""),
        )
    )
    clusters: list[dict[str, Any]] = []
    for signature in candidates:
        best_index: int | None = None
        best_similarity = 0.0
        best_shared = 0
        for index, cluster in enumerate(clusters):
            centroid = _cluster_centroid(cluster["member_signatures"])
            similarity, shared = signature_similarity(signature, centroid)
            if shared < min_shared_partners:
                continue
            if similarity > best_similarity:
                best_similarity = similarity
                best_shared = shared
                best_index = index
        if best_index is not None and best_similarity >= min_similarity:
            clusters[best_index]["member_signatures"].append(signature)
            clusters[best_index]["assignment_similarities"].append(best_similarity)
            clusters[best_index]["assignment_shared_partners"].append(best_shared)
        else:
            clusters.append(
                {
                    "side": side,
                    "member_signatures": [signature],
                    "assignment_similarities": [1.0],
                    "assignment_shared_partners": [
                        len(signature.get("partner_vector") or {})
                    ],
                }
            )
    summarized = [_summarize_cluster(side=side, index=index + 1, cluster=cluster) for index, cluster in enumerate(clusters)]
    summarized.sort(
        key=lambda cluster: (
            -_float_value(cluster.get("best_score")),
            -_int_value(cluster.get("strong_count")),
            str(cluster.get("cluster_id") or ""),
        )
    )
    for index, cluster in enumerate(summarized, start=1):
        cluster["cluster_rank"] = index
    return summarized


def _summarize_cluster(*, side: str, index: int, cluster: dict[str, Any]) -> dict[str, Any]:
    members = list(cluster["member_signatures"])
    partner_counter: dict[str, float] = {}
    role_counter: dict[str, float] = {}
    strategy_counter: dict[str, float] = {}
    namespace_counter: dict[str, float] = {}
    partner_strategy_counter: dict[str, float] = {}
    timeframe_counter: dict[str, float] = {}
    lane_counter: dict[str, float] = {}
    examples: list[dict[str, Any]] = []
    positive_count = 0
    strong_count = 0
    tested_count = 0
    best_score = 0.0
    positive_score_sum = 0.0
    for signature in members:
        tested_count += _int_value(signature.get("tested_count"))
        positive_count += _int_value(signature.get("positive_count"))
        strong_count += _int_value(signature.get("strong_count"))
        best_score = max(best_score, _float_value(signature.get("best_score")))
        positive_score_sum += _float_value(signature.get("avg_positive_score")) * _int_value(
            signature.get("positive_count")
        )
        _counter_add(role_counter, signature.get("signal_role"), 1.0)
        _counter_add(strategy_counter, signature.get("strategy_role"), 1.0)
        _counter_add(namespace_counter, signature.get("namespace"), 1.0)
        for partner_id, value in (signature.get("partner_vector") or {}).items():
            _counter_add(partner_counter, partner_id, _float_value(value))
        for strategy, value in (signature.get("partner_strategy_vector") or {}).items():
            _counter_add(partner_strategy_counter, strategy, _float_value(value))
        for timeframe, value in (signature.get("timeframe_vector") or {}).items():
            _counter_add(timeframe_counter, timeframe, _float_value(value))
        for lane, value in (signature.get("lane_vector") or {}).items():
            _counter_add(lane_counter, lane, _float_value(value))
        examples.extend(signature.get("top_examples") or [])
    examples.sort(key=lambda row: -_float_value(row.get("composite_score")))
    top_member_ids = [
        signature["indicator_id"]
        for signature in sorted(
            members,
            key=lambda item: (
                -_float_value(item.get("best_score")),
                -_int_value(item.get("positive_count")),
                str(item.get("indicator_id") or ""),
            ),
        )
    ]
    dominant_member_strategy = _top_keys(strategy_counter, limit=1)[0] if strategy_counter else "unknown"
    top_partner = _top_keys(partner_counter, limit=1)[0] if partner_counter else "unknown"
    cluster_id = f"{side}_cluster_{index:02d}"
    label = f"{side}:{dominant_member_strategy}->with:{top_partner}".lower()
    return {
        "cluster_id": cluster_id,
        "cluster_rank": index,
        "side": side,
        "behavioral_label": label,
        "member_count": len(members),
        "members": top_member_ids,
        "top_members": top_member_ids[:12],
        "tested_count": tested_count,
        "positive_count": positive_count,
        "strong_count": strong_count,
        "best_score": round(best_score, 4),
        "avg_positive_score": round(positive_score_sum / positive_count, 4)
        if positive_count
        else 0.0,
        "dominant_signal_roles": _top_counts(role_counter, limit=4),
        "dominant_strategy_roles": _top_counts(strategy_counter, limit=4),
        "dominant_namespaces": _top_counts(namespace_counter, limit=4),
        "top_partner_indicators": _top_counts(partner_counter, limit=10),
        "top_partner_strategy_roles": _top_counts(partner_strategy_counter, limit=6),
        "top_timeframes": _top_counts(timeframe_counter, limit=4),
        "top_lanes": _top_counts(lane_counter, limit=4),
        "mean_assignment_similarity": round(
            sum(cluster.get("assignment_similarities") or [1.0])
            / max(1, len(cluster.get("assignment_similarities") or [])),
            4,
        ),
        "top_examples": examples[:10],
    }


def _cluster_lookup(clusters: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for cluster in clusters:
        for indicator_id in cluster.get("members") or []:
            lookup[_clean_upper(indicator_id)] = cluster
    return lookup


def _compatibility_score(
    *,
    best_score: float,
    avg_positive_score: float,
    positive_pair_count: int,
    strong_pair_count: int,
    positive_rate: float,
) -> float:
    support_score = min(100.0, 24.0 * math.log1p(max(0, positive_pair_count)))
    strong_support_score = min(100.0, 30.0 * math.log1p(max(0, strong_pair_count)))
    return round(
        best_score * 0.35
        + avg_positive_score * 0.25
        + support_score * 0.18
        + strong_support_score * 0.12
        + (positive_rate * 100.0) * 0.10,
        4,
    )


def build_cluster_pair_rows(
    rows: list[dict[str, Any]],
    *,
    first_clusters: list[dict[str, Any]],
    second_clusters: list[dict[str, Any]],
    min_positive_score: float = DEFAULT_MIN_POSITIVE_SCORE,
    strong_score: float = DEFAULT_STRONG_SCORE,
) -> list[dict[str, Any]]:
    first_lookup = _cluster_lookup(first_clusters)
    second_lookup = _cluster_lookup(second_clusters)
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        first_cluster = first_lookup.get(_clean_upper(row.get("first_indicator_id")))
        second_cluster = second_lookup.get(_clean_upper(row.get("second_indicator_id")))
        if not first_cluster or not second_cluster:
            continue
        key = (first_cluster["cluster_id"], second_cluster["cluster_id"])
        bucket = buckets.setdefault(
            key,
            {
                "first_cluster_id": first_cluster["cluster_id"],
                "second_cluster_id": second_cluster["cluster_id"],
                "first_cluster_label": first_cluster["behavioral_label"],
                "second_cluster_label": second_cluster["behavioral_label"],
                "tested_pair_count": 0,
                "positive_pair_count": 0,
                "strong_pair_count": 0,
                "best_score": 0.0,
                "positive_score_sum": 0.0,
                "timeframe_counts": {},
                "lane_counts": {},
                "examples": [],
            },
        )
        score = _score_number(row)
        if score is None:
            continue
        bucket["tested_pair_count"] += 1
        if score > bucket["best_score"]:
            bucket["best_score"] = score
        if score >= min_positive_score:
            bucket["positive_pair_count"] += 1
            bucket["positive_score_sum"] += score
            if score >= strong_score:
                bucket["strong_pair_count"] += 1
            _counter_add(bucket["timeframe_counts"], row.get("probe_timeframe"), 1.0)
            _counter_add(bucket["lane_counts"], row.get("discovery_lane"), 1.0)
            bucket["examples"].append(
                {
                    "probe_id": row.get("probe_id"),
                    "first_indicator_id": row.get("first_indicator_id"),
                    "second_indicator_id": row.get("second_indicator_id"),
                    "probe_timeframe": row.get("probe_timeframe"),
                    "discovery_lane": row.get("discovery_lane"),
                    "composite_score": round(score, 4),
                    "best_trades": row.get("best_trades"),
                    "best_expectancy_r": row.get("best_expectancy_r"),
                    "best_profit_factor": row.get("best_profit_factor"),
                }
            )
    result_rows: list[dict[str, Any]] = []
    for bucket in buckets.values():
        positive_count = _int_value(bucket.get("positive_pair_count"))
        tested_count = max(1, _int_value(bucket.get("tested_pair_count")))
        avg_positive = (
            _float_value(bucket.get("positive_score_sum")) / positive_count
            if positive_count
            else 0.0
        )
        positive_rate = positive_count / tested_count
        examples = bucket["examples"]
        examples.sort(key=lambda row: -_float_value(row.get("composite_score")))
        compatibility = _compatibility_score(
            best_score=_float_value(bucket.get("best_score")),
            avg_positive_score=avg_positive,
            positive_pair_count=positive_count,
            strong_pair_count=_int_value(bucket.get("strong_pair_count")),
            positive_rate=positive_rate,
        )
        result_rows.append(
            {
                "first_cluster_id": bucket["first_cluster_id"],
                "second_cluster_id": bucket["second_cluster_id"],
                "first_cluster_label": bucket["first_cluster_label"],
                "second_cluster_label": bucket["second_cluster_label"],
                "tested_pair_count": tested_count,
                "positive_pair_count": positive_count,
                "strong_pair_count": _int_value(bucket.get("strong_pair_count")),
                "positive_rate": round(positive_rate, 4),
                "best_score": round(_float_value(bucket.get("best_score")), 4),
                "avg_positive_score": round(avg_positive, 4),
                "compatibility_score": compatibility,
                "top_timeframes": ",".join(_top_keys(bucket["timeframe_counts"], limit=3)),
                "top_lanes": ",".join(_top_keys(bucket["lane_counts"], limit=3)),
                "top_examples": examples[:8],
            }
        )
    result_rows.sort(
        key=lambda row: (
            -_float_value(row.get("compatibility_score")),
            -_float_value(row.get("best_score")),
            str(row.get("first_cluster_id") or ""),
            str(row.get("second_cluster_id") or ""),
        )
    )
    return result_rows


def _recipe_confidence(row: dict[str, Any]) -> str:
    strong_count = _int_value(row.get("strong_pair_count"))
    positive_count = _int_value(row.get("positive_pair_count"))
    best_score = _float_value(row.get("best_score"))
    if strong_count >= 3 and positive_count >= 8 and best_score >= 72.0:
        return "high_candidate"
    if strong_count >= 1 and positive_count >= 3 and best_score >= 70.0:
        return "promising_candidate"
    if positive_count >= 5:
        return "needs_more_evidence"
    return "sparse_watch"


def build_discovered_recipes(
    cluster_pair_rows: list[dict[str, Any]],
    *,
    first_clusters: list[dict[str, Any]],
    second_clusters: list[dict[str, Any]],
    max_recipes: int = DEFAULT_MAX_RECIPES,
) -> list[dict[str, Any]]:
    first_by_id = {cluster["cluster_id"]: cluster for cluster in first_clusters}
    second_by_id = {cluster["cluster_id"]: cluster for cluster in second_clusters}
    recipes: list[dict[str, Any]] = []
    for row in cluster_pair_rows:
        if _int_value(row.get("positive_pair_count")) <= 0:
            continue
        first_cluster = first_by_id.get(_clean_token(row.get("first_cluster_id")))
        second_cluster = second_by_id.get(_clean_token(row.get("second_cluster_id")))
        if not first_cluster or not second_cluster:
            continue
        recipes.append(
            {
                "recipe_id": f"discovered_recipe_{len(recipes) + 1:03d}",
                "source": "discovery_pair_cluster_atlas",
                "status": "needs_12m_validation",
                "confidence": _recipe_confidence(row),
                "name": (
                    f"{first_cluster['behavioral_label']} + "
                    f"{second_cluster['behavioral_label']}"
                ),
                "compatibility_score": row.get("compatibility_score"),
                "best_score": row.get("best_score"),
                "positive_pair_count": row.get("positive_pair_count"),
                "strong_pair_count": row.get("strong_pair_count"),
                "top_timeframes": row.get("top_timeframes"),
                "slots": {
                    "context_or_setup_cluster": {
                        "cluster_id": first_cluster["cluster_id"],
                        "label": first_cluster["behavioral_label"],
                        "recommended_indicators": first_cluster.get("top_members", [])[:10],
                    },
                    "trigger_or_response_cluster": {
                        "cluster_id": second_cluster["cluster_id"],
                        "label": second_cluster["behavioral_label"],
                        "recommended_indicators": second_cluster.get("top_members", [])[:10],
                    },
                },
                "evidence_examples": row.get("top_examples") or [],
                "operator_note": (
                    "Empirical two-indicator cluster relationship. Treat as a recipe "
                    "template candidate, not a finished Play Hand recipe, until 12m "
                    "validation confirms retention."
                ),
            }
        )
        if len(recipes) >= max_recipes:
            break
    return recipes


def _signature_csv_rows(signatures: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for signature in signatures.values():
        rows.append(
            {
                "side": signature.get("side"),
                "indicator_id": signature.get("indicator_id"),
                "signal_role": signature.get("signal_role"),
                "strategy_role": signature.get("strategy_role"),
                "namespace": signature.get("namespace"),
                "tested_count": signature.get("tested_count"),
                "positive_count": signature.get("positive_count"),
                "strong_count": signature.get("strong_count"),
                "best_score": signature.get("best_score"),
                "avg_positive_score": signature.get("avg_positive_score"),
                "top_partners": ",".join(_top_keys(signature.get("partner_vector") or {}, limit=8)),
                "top_partner_strategy_roles": ",".join(
                    _top_keys(signature.get("partner_strategy_vector") or {}, limit=6)
                ),
                "top_timeframes": ",".join(_top_keys(signature.get("timeframe_vector") or {}, limit=4)),
                "top_lanes": ",".join(_top_keys(signature.get("lane_vector") or {}, limit=4)),
            }
        )
    rows.sort(
        key=lambda row: (
            str(row.get("side") or ""),
            -_float_value(row.get("best_score")),
            str(row.get("indicator_id") or ""),
        )
    )
    return rows


def _cluster_csv_rows(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cluster in clusters:
        rows.append(
            {
                "cluster_id": cluster.get("cluster_id"),
                "cluster_rank": cluster.get("cluster_rank"),
                "side": cluster.get("side"),
                "behavioral_label": cluster.get("behavioral_label"),
                "member_count": cluster.get("member_count"),
                "members": ",".join(cluster.get("members") or []),
                "tested_count": cluster.get("tested_count"),
                "positive_count": cluster.get("positive_count"),
                "strong_count": cluster.get("strong_count"),
                "best_score": cluster.get("best_score"),
                "avg_positive_score": cluster.get("avg_positive_score"),
                "top_partner_indicators": ",".join(
                    item["key"] for item in cluster.get("top_partner_indicators", [])
                ),
                "top_partner_strategy_roles": ",".join(
                    item["key"] for item in cluster.get("top_partner_strategy_roles", [])
                ),
                "top_timeframes": ",".join(item["key"] for item in cluster.get("top_timeframes", [])),
                "top_lanes": ",".join(item["key"] for item in cluster.get("top_lanes", [])),
                "mean_assignment_similarity": cluster.get("mean_assignment_similarity"),
            }
        )
    return rows


def _cluster_pair_csv_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "first_cluster_id": row.get("first_cluster_id"),
            "second_cluster_id": row.get("second_cluster_id"),
            "first_cluster_label": row.get("first_cluster_label"),
            "second_cluster_label": row.get("second_cluster_label"),
            "tested_pair_count": row.get("tested_pair_count"),
            "positive_pair_count": row.get("positive_pair_count"),
            "strong_pair_count": row.get("strong_pair_count"),
            "positive_rate": row.get("positive_rate"),
            "best_score": row.get("best_score"),
            "avg_positive_score": row.get("avg_positive_score"),
            "compatibility_score": row.get("compatibility_score"),
            "top_timeframes": row.get("top_timeframes"),
            "top_lanes": row.get("top_lanes"),
            "top_example": json.dumps((row.get("top_examples") or [{}])[0], ensure_ascii=True),
        }
        for row in rows
    ]


def build_discovery_cluster_atlas(
    config: AppConfig,
    *,
    discovery_pair_dir: Path | None = None,
    out_dir: Path | None = None,
    min_positive_score: float = DEFAULT_MIN_POSITIVE_SCORE,
    strong_score: float = DEFAULT_STRONG_SCORE,
    min_similarity: float = DEFAULT_MIN_SIMILARITY,
    min_shared_partners: int = DEFAULT_MIN_SHARED_PARTNERS,
    max_recipes: int = DEFAULT_MAX_RECIPES,
) -> DiscoveryClusterAtlasBuildResult:
    source_dir = (
        discovery_pair_dir.expanduser().resolve()
        if discovery_pair_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_PAIR_DIRNAME
    )
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_CLUSTER_DIRNAME
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    rows = _merge_discovery_rows(source_dir)
    scored_rows = [row for row in rows if _score_number(row) is not None]
    positive_rows = [
        row for row in scored_rows if (_score_number(row) or 0.0) >= min_positive_score
    ]
    strong_rows = [
        row for row in scored_rows if (_score_number(row) or 0.0) >= strong_score
    ]
    first_signatures = build_indicator_success_signatures(
        scored_rows,
        side="first",
        min_positive_score=min_positive_score,
        strong_score=strong_score,
    )
    second_signatures = build_indicator_success_signatures(
        scored_rows,
        side="second",
        min_positive_score=min_positive_score,
        strong_score=strong_score,
    )
    first_clusters = cluster_indicator_signatures(
        first_signatures,
        side="first",
        min_similarity=min_similarity,
        min_shared_partners=min_shared_partners,
    )
    second_clusters = cluster_indicator_signatures(
        second_signatures,
        side="second",
        min_similarity=min_similarity,
        min_shared_partners=min_shared_partners,
    )
    cluster_pair_rows = build_cluster_pair_rows(
        scored_rows,
        first_clusters=first_clusters,
        second_clusters=second_clusters,
        min_positive_score=min_positive_score,
        strong_score=strong_score,
    )
    recipe_candidates = build_discovered_recipes(
        cluster_pair_rows,
        first_clusters=first_clusters,
        second_clusters=second_clusters,
        max_recipes=max(1, len(cluster_pair_rows)),
    )
    max_recipes = max(1, int(max_recipes))
    recipes = recipe_candidates[:max_recipes]
    lane_counts: dict[str, float] = {}
    for row in positive_rows:
        _counter_add(lane_counts, row.get("discovery_lane"), 1.0)
    positive_cluster_pairs = [
        row for row in cluster_pair_rows if _int_value(row.get("positive_pair_count")) > 0
    ]
    strong_cluster_pairs = [
        row for row in cluster_pair_rows if _int_value(row.get("strong_pair_count")) > 0
    ]
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "discovery_pair_dir": str(source_dir),
            "results_csv": str(source_dir / "discovery-pair-probe-results.csv"),
            "queue_csv": str(source_dir / "discovery-pair-queue.csv"),
        },
        "settings": {
            "min_positive_score": min_positive_score,
            "strong_score": strong_score,
            "min_similarity": min_similarity,
            "min_shared_partners": min_shared_partners,
            "max_recipes": max_recipes,
        },
        "result_counts": {
            "rows": len(rows),
            "scored_rows": len(scored_rows),
            "positive_rows": len(positive_rows),
            "strong_rows": len(strong_rows),
            "first_signatures": len(first_signatures),
            "second_signatures": len(second_signatures),
            "first_clusters": len(first_clusters),
            "second_clusters": len(second_clusters),
            "cluster_pair_rows": len(cluster_pair_rows),
            "positive_cluster_pair_rows": len(positive_cluster_pairs),
            "strong_cluster_pair_rows": len(strong_cluster_pairs),
            "recipe_candidates_before_max": len(recipe_candidates),
            "discovered_recipes": len(recipes),
            "recipes_truncated_by_max": max(0, len(recipe_candidates) - len(recipes)),
            "recipe_candidate_confidence_counts": _count_by_key(recipe_candidates, "confidence"),
            "recipe_confidence_counts": _count_by_key(recipes, "confidence"),
            "positive_lane_counts": dict(sorted(lane_counts.items())),
        },
        "cluster_shape": {
            "first": _cluster_shape(first_clusters),
            "second": _cluster_shape(second_clusters),
        },
        "top_cluster_pairs": cluster_pair_rows[:10],
        "top_discovered_recipes": recipes[:10],
    }
    atlas_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "summary": summary,
        "first_clusters": first_clusters,
        "second_clusters": second_clusters,
        "cluster_pair_rows": cluster_pair_rows,
        "discovered_recipes": recipes,
    }
    atlas_path = target_dir / "discovery-cluster-atlas.json"
    indicator_clusters_csv_path = target_dir / "indicator-clusters.csv"
    indicator_signatures_csv_path = target_dir / "indicator-success-signatures.csv"
    cluster_pair_matrix_csv_path = target_dir / "cluster-pair-matrix.csv"
    discovered_recipes_path = target_dir / "discovered-recipes.json"
    summary_path = target_dir / "discovery-cluster-summary.json"
    _write_json(atlas_path, atlas_payload)
    _write_csv(
        indicator_clusters_csv_path,
        _cluster_csv_rows([*first_clusters, *second_clusters]),
        [
            "cluster_id",
            "cluster_rank",
            "side",
            "behavioral_label",
            "member_count",
            "members",
            "tested_count",
            "positive_count",
            "strong_count",
            "best_score",
            "avg_positive_score",
            "top_partner_indicators",
            "top_partner_strategy_roles",
            "top_timeframes",
            "top_lanes",
            "mean_assignment_similarity",
        ],
    )
    _write_csv(
        indicator_signatures_csv_path,
        _signature_csv_rows({**first_signatures, **{f"second:{k}": v for k, v in second_signatures.items()}}),
        [
            "side",
            "indicator_id",
            "signal_role",
            "strategy_role",
            "namespace",
            "tested_count",
            "positive_count",
            "strong_count",
            "best_score",
            "avg_positive_score",
            "top_partners",
            "top_partner_strategy_roles",
            "top_timeframes",
            "top_lanes",
        ],
    )
    _write_csv(
        cluster_pair_matrix_csv_path,
        _cluster_pair_csv_rows(cluster_pair_rows),
        [
            "first_cluster_id",
            "second_cluster_id",
            "first_cluster_label",
            "second_cluster_label",
            "tested_pair_count",
            "positive_pair_count",
            "strong_pair_count",
            "positive_rate",
            "best_score",
            "avg_positive_score",
            "compatibility_score",
            "top_timeframes",
            "top_lanes",
            "top_example",
        ],
    )
    _write_json(discovered_recipes_path, {"schema_version": SCHEMA_VERSION, "recipes": recipes})
    _write_json(summary_path, summary)
    return DiscoveryClusterAtlasBuildResult(
        atlas_path=atlas_path,
        indicator_clusters_csv_path=indicator_clusters_csv_path,
        indicator_signatures_csv_path=indicator_signatures_csv_path,
        cluster_pair_matrix_csv_path=cluster_pair_matrix_csv_path,
        discovered_recipes_path=discovered_recipes_path,
        summary_path=summary_path,
        summary=summary,
    )
