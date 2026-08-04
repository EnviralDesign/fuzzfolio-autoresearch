"""Run a bounded, reproducible no-market admission of pair proposal mechanics.

This is an operator diagnostic, not an evaluator.  It audits a previously
frozen local pair authority and catalog, asks the Dashboard's native validator
to compile the generated v3/both candidates, and records only construction
facts.  It intentionally does not import a lake, gateway, worker, or archive
reducer and it never writes economic scores or archive lanes.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from autoresearch.temporal_bidirectional_genome import FrozenPair
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_indicator_learning_v1 import (
    FAMILY_SUBSTITUTION,
    IndicatorLearningRegistry,
    _event_contract,
    _fuzzy_evidence_contract,
)
from autoresearch.temporal_qd_pair_factory import (
    PairAuthorityBundle,
    load_pair_run_config,
    pair_policy_from_config,
)
from autoresearch.temporal_qd_pair_generation import generate_pair_population
from autoresearch.temporal_qd_pair_generation import (
    _mutation_depth_for_seed,
    _propose_pair_sequence,
    _unbiased_choice,
    replay_pair_proposal,
)


SUMMARY_SCHEMA = "temporal_qd_no_market_pair_admission_summary_v1"
EXPECTED_DIRECTIONAL_EVENT_SUBSTITUTIONS = 21
EXPECTED_FUZZY_STATE_RANGE_INDICATORS = 62
DEFAULT_TARGET_UNIQUE_CANDIDATES = 64
DEFAULT_INTERRUPT_AFTER = 12
DEPTH_PROBE_SCHEMA = "temporal_qd_no_market_pair_depth_probe_v1"
_DEPTH_PROBE_BUCKETS = (0, 14, 19)
_DEPTH_PROBE_MAX_BUCKET_ATTEMPTS = 16


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"JSON root must be an object: {path}")
    return value


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing to overwrite divergent no-market admission summary: {path}")
    path.write_text(encoded, encoding="utf-8")


def _outside_repository(output_root: Path) -> Path:
    root = output_root.resolve()
    repository = Path(__file__).resolve().parents[1]
    try:
        inside_repository = os.path.commonpath((str(root), str(repository))) == str(repository)
    except ValueError:
        inside_repository = False
    if inside_repository:
        raise TemporalDiscoveryContractError("--output-root must be external to the autoresearch repository")
    return root


def audit_construction_catalog(catalog: Mapping[str, Any], frozen: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed if the current catalog no longer has the admitted surface."""

    registry = IndicatorLearningRegistry(catalog)
    catalog_sha = registry.catalog.catalog_sha256
    for direction in ("long", "short"):
        side = frozen.get(f"{direction}Module")
        if not isinstance(side, Mapping) or side.get("catalog") != catalog:
            raise TemporalDiscoveryContractError(f"frozen {direction} pair catalog differs from --construction-catalog")
        if side.get("catalogSha256") != catalog_sha:
            raise TemporalDiscoveryContractError(f"frozen {direction} pair catalog identity differs from --construction-catalog")
        policy = side.get("policy")
        if not isinstance(policy, Mapping) or policy.get("resourceRoleDisposition") != "seed_priors_only_v1":
            raise TemporalDiscoveryContractError("pair resource roles must be frozen as seed priors only")

    directional_ids = sorted(
        indicator_id
        for indicator_id, entry in registry.catalog.indicators.items()
        if _event_contract(entry["meta"]) is not None
    )
    fuzzy_ids = sorted(
        indicator_id
        for indicator_id, entry in registry.catalog.indicators.items()
        if _fuzzy_evidence_contract(entry["meta"]) is not None
    )
    if len(directional_ids) != EXPECTED_DIRECTIONAL_EVENT_SUBSTITUTIONS:
        raise TemporalDiscoveryContractError(
            "current catalog directional event substitution count is not exactly "
            f"{EXPECTED_DIRECTIONAL_EVENT_SUBSTITUTIONS}: {len(directional_ids)}"
        )
    if len(fuzzy_ids) != EXPECTED_FUZZY_STATE_RANGE_INDICATORS:
        raise TemporalDiscoveryContractError(
            "current catalog fuzzy state/range capability count is not exactly "
            f"{EXPECTED_FUZZY_STATE_RANGE_INDICATORS}: {len(fuzzy_ids)}"
        )
    policy = registry.policy
    if policy.get("maxBoundFuzzyInstancesPerDirection") != 3 or policy.get("maxEvidenceGroupMembers") != 3:
        raise TemporalDiscoveryContractError("frozen indicator learning policy must cap fuzzy members at 3 per side/group")
    return {
        "catalogSha256": catalog_sha,
        "directionalEventSubstitutionCount": len(directional_ids),
        "directionalEventSubstitutionIds": directional_ids,
        "fuzzyStateRangeCapableIndicatorCount": len(fuzzy_ids),
        "fuzzyStateRangeCapableIndicatorIds": fuzzy_ids,
        "roles": "seed_priors_only_not_eligibility",
        "fuzzyCap": {"perSide": 3, "perEvidenceGroup": 3},
    }


def audit_fresh_event_substitution_reachability(
    authority: PairAuthorityBundle, directional_ids: Sequence[str]
) -> dict[str, Any]:
    """Prove native-hydrated event slots can reach every other capability peer.

    A source does not replace itself, so a 21-member directional capability
    family yields exactly 20 plans per fresh event-bound source.  This is kept
    separate from the catalog cardinality audit to catch native hydration that
    silently strips the event substitution metadata.
    """

    pair = authority.factory.create_pair(
        proposal_seed="no_market_event_substitution_reachability_v1"
    )
    capability_ids = set(directional_ids)
    rows: list[dict[str, Any]] = []
    for module in (pair.long, pair.short):
        profile = module.canonical_payload()["profile"]
        indicators = {
            str((item.get("meta") or {}).get("instanceId") or ""): item
            for item in profile.get("indicators") or []
            if isinstance(item, Mapping)
        }
        event_bindings = (profile.get("graph") or {}).get("eventBindings") or []
        plans = authority.operator.indicator_plans(module)
        for binding in event_bindings:
            if not isinstance(binding, Mapping):
                raise TemporalDiscoveryContractError("fresh module event binding is malformed")
            instance_id = str(binding.get("indicatorInstanceId") or "")
            item = indicators.get(instance_id)
            meta = item.get("meta") if isinstance(item, Mapping) else None
            source_id = str((meta or {}).get("id") or "")
            if not isinstance(meta, Mapping) or source_id not in capability_ids or _event_contract(meta) is None:
                raise TemporalDiscoveryContractError("fresh native-hydrated event slot lacks directional substitution metadata")
            replacements = {
                str(plan["construction"].get("afterIndicatorId") or "")
                for plan in plans
                if plan.get("operatorId") == FAMILY_SUBSTITUTION
                and isinstance(plan.get("construction"), Mapping)
                and plan["construction"].get("eventBound") is True
                and plan["construction"].get("indicatorInstanceId") == instance_id
                and plan["construction"].get("beforeIndicatorId") == source_id
            }
            expected = capability_ids - {source_id}
            if replacements != expected:
                raise TemporalDiscoveryContractError(
                    f"fresh event source {source_id} does not reach every other directional capability peer"
                )
            rows.append(
                {
                    "side": module.direction,
                    "indicatorInstanceId": instance_id,
                    "sourceIndicatorId": source_id,
                    "reachablePeerCount": len(replacements),
                    "reachablePeerIds": sorted(replacements),
                }
            )
    if not rows:
        raise TemporalDiscoveryContractError("fresh native pair has no event-bound sources to audit")
    return {
        "capabilityFamilySize": len(capability_ids),
        "perFreshSourceReplacementCount": len(capability_ids) - 1,
        "freshNativeHydratedEventSources": rows,
    }


def _population_pairs(root: Path) -> list[FrozenPair]:
    population = _read(root / "population.json")
    candidates = population.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise TemporalDiscoveryContractError("seed population has no candidates")
    pairs = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise TemporalDiscoveryContractError("seed population candidate is malformed")
        profile = candidate.get("sourceProfile")
        if not isinstance(profile, Mapping) or profile.get("version") != "v3" or profile.get("directionMode") != "both":
            raise TemporalDiscoveryContractError("seed population is not native v3/both material")
        pairs.append(FrozenPair.from_payload(candidate.get("bidirectionalGenome")))
    return pairs


def _depth_probe_seed(bucket: int, search_ordinal: int) -> str:
    """Return one deterministic candidate from a fixed depth-probe namespace."""

    if bucket not in _DEPTH_PROBE_BUCKETS:
        raise TemporalDiscoveryContractError("depth probe bucket is not admitted")
    if search_ordinal < 0:
        raise TemporalDiscoveryContractError("depth probe search ordinal is invalid")
    return f"temporal_qd_no_market_depth_probe_v1:{bucket}:{search_ordinal}"


def probe_structural_mutation_depths(
    authority: PairAuthorityBundle, parent: FrozenPair
) -> dict[str, Any]:
    """Native-materialize and replay one exact 1/2/3-depth structural probe."""

    root = FrozenPair.from_payload(parent.canonical_payload())
    probes = []
    for bucket in _DEPTH_PROBE_BUCKETS:
        selected: tuple[str, int, int, FrozenPair, dict[str, Any]] | None = None
        bucket_attempt = 0
        for search_ordinal in range(10_000):
            seed = _depth_probe_seed(bucket, search_ordinal)
            if _unbiased_choice(seed, size=20) != bucket:
                continue
            bucket_attempt += 1
            depth = _mutation_depth_for_seed(seed)
            pair, proposal = _propose_pair_sequence(
                proposal_seed=seed,
                parent=root,
                mutation_depth=depth,
                module_authority=authority.operator,
                native_validator=authority.validator,
                pair_compiler=authority.compiler,
            )
            if proposal.get("mutationDepth") != depth:
                raise TemporalDiscoveryContractError("depth probe proposal does not bind its exact depth")
            if pair is not None:
                selected = (seed, search_ordinal, bucket_attempt, pair, proposal)
                break
            if bucket_attempt >= _DEPTH_PROBE_MAX_BUCKET_ATTEMPTS:
                break
        if selected is None:
            raise TemporalDiscoveryContractError(
                f"depth probe did not materialize a native v3/both offspring at depth bucket {bucket}"
            )
        seed, search_ordinal, bucket_attempt, pair, proposal = selected
        replayed = replay_pair_proposal(
            payload=proposal,
            module_authority=authority.operator,
            native_validator=authority.validator,
            pair_compiler=authority.compiler,
        )
        if (pair is None) != (replayed is None) or (
            pair is not None and replayed is not None and pair.canonical_payload() != replayed.canonical_payload()
        ):
            raise TemporalDiscoveryContractError("depth probe canonical replay diverged")
        if (
            pair.profile.get("version") != "v3"
            or pair.profile.get("directionMode") != "both"
            or pair.validation.get("candidateAcceptable") is not True
        ):
            raise TemporalDiscoveryContractError("depth probe pair is not native-admitted v3/both")
        probes.append(
            {
                "bucket": bucket,
                "proposalSeed": seed,
                "selectionSearchOrdinal": search_ordinal,
                "matchingBucketAttempt": bucket_attempt,
                "mutationDepth": _mutation_depth_for_seed(seed),
                "proposalSha256": proposal["proposalSha256"],
                "pairIdentitySha256": pair.identity_sha256,
                "nativeProfileShape": "v3/both",
            }
        )
    if {item["mutationDepth"] for item in probes} != {1, 2, 3}:
        raise TemporalDiscoveryContractError("depth probe did not cover exact 1/2/3 depths")
    return {
        "schemaVersion": DEPTH_PROBE_SCHEMA,
        "parentPairIdentitySha256": root.identity_sha256,
        "exactBucketSchedule": {"1": 14, "2": 5, "3": 1},
        "probes": probes,
    }


def _entries(root: Path) -> list[dict[str, Any]]:
    journal_root = root / "proposal-journal"
    entries = []
    for ordinal, path in enumerate(sorted(journal_root.glob("*.json"))):
        if path.name != f"{ordinal:08d}.json":
            raise TemporalDiscoveryContractError("proposal journal has a gap")
        entries.append(_read(path))
    if not entries:
        raise TemporalDiscoveryContractError("offspring proposal journal is empty")
    return entries


def audit_offspring_journal(root: Path) -> dict[str, Any]:
    entries = _entries(root)
    origins = Counter(str(entry.get("originKind") or "") for entry in entries)
    dispositions = Counter(str(entry.get("disposition") or "") for entry in entries)
    depths = Counter()
    crossover_dispositions = Counter()
    structural_accepted = 0
    for ordinal, entry in enumerate(entries):
        expected = "random_immigrant" if ordinal % 5 == 4 else "structural_offspring"
        if entry.get("originKind") != expected:
            raise TemporalDiscoveryContractError("offspring origin schedule is not exact four structural to one immigrant")
        proposal = entry.get("proposal")
        if not isinstance(proposal, Mapping):
            raise TemporalDiscoveryContractError("offspring entry lacks proposal")
        if expected == "structural_offspring":
            depth = proposal.get("mutationDepth")
            if isinstance(depth, int):
                depths[str(depth)] += 1
            if entry.get("disposition") == "accepted":
                structural_accepted += 1
        if proposal.get("proposalKind") == "temporal_qd_same_side_crossover_v1":
            crossover_dispositions[str(proposal.get("disposition") or "")] += 1
    expected_origins = {
        "random_immigrant": len(entries) // 5,
        "structural_offspring": len(entries) - (len(entries) // 5),
    }
    observed_origins = {
        "random_immigrant": origins.get("random_immigrant", 0),
        "structural_offspring": origins.get("structural_offspring", 0),
    }
    if observed_origins != expected_origins or sum(origins.values()) != len(entries):
        raise TemporalDiscoveryContractError("observed proposal origins do not satisfy the exact four-to-one schedule")
    if structural_accepted < 1:
        raise TemporalDiscoveryContractError("bounded offspring run did not accept a structural offspring")
    if not crossover_dispositions:
        raise TemporalDiscoveryContractError("bounded offspring run did not exercise a versioned same-side crossover disposition")
    return {
        "proposalCount": len(entries),
        "originProposalCounts": dict(sorted(observed_origins.items())),
        "expectedOriginProposalCounts": expected_origins,
        "dispositionCounts": dict(sorted(dispositions.items())),
        "mutationDepthAttemptCounts": dict(sorted(depths.items())),
        "observedMutationDepths": sorted(int(depth) for depth, count in depths.items() if count),
        "mutationDepthSchedule": "exact_hash_bucket_14_5_1_for_depths_1_2_3",
        "structuralAcceptedCount": structural_accepted,
        "crossoverDispositionCounts": dict(sorted(crossover_dispositions.items())),
        "uniquenessAndRejectionDispositions": {
            key: dispositions.get(key, 0)
            for key in ("accepted", "duplicate_pair_genome", "duplicate_candidate_identity", "no_op_proposal", "operation_rejected")
        },
    }


def _generation_kwargs(
    *, root: Path, target: int, frozen: Mapping[str, Any], authority: PairAuthorityBundle, parent_pairs: Sequence[FrozenPair] = ()
) -> dict[str, Any]:
    return {
        "output_root": root,
        "generation_index": 0 if not parent_pairs else 1,
        "target_unique_candidates": target,
        "run_config": {
            "schemaVersion": "temporal_qd_no_market_pair_admission_run_v1",
            "pairRunConfigSha256": frozen["pairRunConfigSha256"],
            "mode": "no_market_no_economic_evidence",
        },
        "pair_policy": pair_policy_from_config(frozen),
        "parent_pairs": parent_pairs,
        "pair_factory": authority.factory,
        "module_authority": authority.operator,
        "native_validator": authority.validator,
        "pair_compiler": authority.compiler,
        "operator_implementation_identity": frozen["operatorImplementation"],
    }


def run_admission(*, pair_config_path: Path, construction_catalog_path: Path, output_root: Path, target_unique_candidates: int = DEFAULT_TARGET_UNIQUE_CANDIDATES, interrupt_after: int = DEFAULT_INTERRUPT_AFTER) -> dict[str, Any]:
    if not 32 <= target_unique_candidates <= 64:
        raise TemporalDiscoveryContractError("target unique candidates must be within the bounded 32..64 admission range")
    if interrupt_after < 1:
        raise TemporalDiscoveryContractError("interrupt-after must be positive")
    root = _outside_repository(output_root)
    frozen = load_pair_run_config(_read(pair_config_path))
    catalog = _read(construction_catalog_path)
    catalog_audit = audit_construction_catalog(catalog, frozen)
    with PairAuthorityBundle(frozen) as authority:
        event_reachability_audit = audit_fresh_event_substitution_reachability(
            authority, catalog_audit["directionalEventSubstitutionIds"]
        )
        seed_root = root / "seed-population"
        seed_result = generate_pair_population(
            **_generation_kwargs(root=seed_root, target=8, frozen=frozen, authority=authority)
        )
        if not seed_result.get("completed"):
            raise TemporalDiscoveryContractError("bounded seed population did not complete")
        parents = _population_pairs(seed_root)
        depth_probe = probe_structural_mutation_depths(authority, parents[0])
        resumed_root = root / "offspring-resumed"
        offspring_kwargs = _generation_kwargs(
            root=resumed_root,
            target=target_unique_candidates,
            frozen=frozen,
            authority=authority,
            parent_pairs=parents,
        )
        interrupted = generate_pair_population(max_new_proposals=interrupt_after, **offspring_kwargs)
        resumed = generate_pair_population(**offspring_kwargs)
        if not resumed.get("completed"):
            raise TemporalDiscoveryContractError("resumed offspring population did not complete")
        uninterrupted_root = root / "offspring-uninterrupted"
        uninterrupted = generate_pair_population(
            **_generation_kwargs(
                root=uninterrupted_root,
                target=target_unique_candidates,
                frozen=frozen,
                authority=authority,
                parent_pairs=parents,
            )
        )
        if not uninterrupted.get("completed"):
            raise TemporalDiscoveryContractError("uninterrupted offspring population did not complete")
    if (
        resumed["journalSha256"] != uninterrupted["journalSha256"]
        or resumed["populationSha256"] != uninterrupted["populationSha256"]
    ):
        raise TemporalDiscoveryContractError("resumed pair generation diverged from uninterrupted generation")
    offspring_audit = audit_offspring_journal(resumed_root)
    summary = {
        "schemaVersion": SUMMARY_SCHEMA,
        "mode": "no-market/no-economic-evidence",
        "marketEvidenceRead": False,
        "lakeContacted": False,
        "gatewayContacted": False,
        "vastContacted": False,
        "economicMetricsProduced": False,
        "archiveLanesProduced": False,
        "pairRunConfigSha256": frozen["pairRunConfigSha256"],
        "catalogAudit": catalog_audit,
        "freshNativeEventSubstitutionAudit": event_reachability_audit,
        "seedPopulation": {
            "candidateCount": seed_result["candidateCount"],
            "populationSha256": seed_result["populationSha256"],
            "journalSha256": seed_result["journalSha256"],
            "nativeProfileShape": "v3/both",
        },
        "structuralMutationDepthProbe": depth_probe,
        "offspringPopulation": offspring_audit,
        "interruptResume": {
            "interruptProgress": interrupted,
            "resumedJournalSha256": resumed["journalSha256"],
            "uninterruptedJournalSha256": uninterrupted["journalSha256"],
            "resumedPopulationSha256": resumed["populationSha256"],
            "uninterruptedPopulationSha256": uninterrupted["populationSha256"],
            "matchesUninterrupted": True,
        },
    }
    summary["summarySha256"] = canonical_sha256(summary)
    _write_immutable(root / "no-market-admission-summary.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair-config", required=True, type=Path)
    parser.add_argument("--construction-catalog", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path, help="external root for immutable no-market artifacts")
    parser.add_argument("--target-unique-candidates", type=int, default=DEFAULT_TARGET_UNIQUE_CANDIDATES)
    parser.add_argument("--interrupt-after", type=int, default=DEFAULT_INTERRUPT_AFTER)
    args = parser.parse_args(argv)
    summary = run_admission(
        pair_config_path=args.pair_config,
        construction_catalog_path=args.construction_catalog,
        output_root=args.output_root,
        target_unique_candidates=args.target_unique_candidates,
        interrupt_after=args.interrupt_after,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
