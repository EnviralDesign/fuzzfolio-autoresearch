"""Materialize compact, deterministic Python-oracle front-half admission cases.

This is deliberately a *test authority*, not a second generator.  It drives
the currently admitted optimized Python pair generator with the same small
fake native/compiler boundary used by the Temporal-QD unit suite.  The
resulting roots are suitable for byte/semantic comparison by
``temporal_qd_front_half_oracle.py`` without contacting Dashboard, the lake,
or a worker gateway.

The corpus keeps only an input/coverage manifest in the repository.  Callers
materialize rich JSON under their own temporary output directory because that
is the data Rust must reproduce, not a source-controlled fixture blob.
"""

from __future__ import annotations

import argparse
import copy
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from autoresearch.temporal_bidirectional_genome import (
    FrozenModule,
    FrozenPair,
    IdentitySnapshot,
    canonical_sha256,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import generate_pair_population

from scripts import temporal_qd_front_half_oracle as semantic_oracle


CORPUS_SCHEMA = "temporal_qd_front_half_python_oracle_corpus_v1"
OPERATOR_IDENTITY = {
    "schemaVersion": "temporal_qd_front_half_fixture_operator_v1",
    "grammar": "deterministic-fixture",
    "indicator": "deterministic-fixture",
    "hold": "deterministic-fixture",
    "initialProtection": "deterministic-fixture",
}


class FixtureNativeValidator:
    """Deterministic no-process native-admission boundary for an oracle only."""

    def validate_v2(self, *, profile: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
        raw = canonical_sha256(profile)
        return {
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": raw,
            "profileSnapshotSha256": canonical_sha256({"snapshot": profile}),
            "programSha256": canonical_sha256({"fixtureNativeV2": profile}),
            "validationReportSha256": canonical_sha256(
                {"fixtureNativeValidation": profile}
            ),
            "status": "valid_evaluable",
            "candidateAcceptable": True,
        }


class FixturePairCompiler:
    """Reuses the test-suite's deterministic compiler shape, with no I/O."""

    def compile_pair(
        self,
        *,
        long_profile: Mapping[str, Any],
        short_profile: Mapping[str, Any],
        candidate_id: str,
    ) -> dict[str, Any]:
        def module(side: str, source: Mapping[str, Any]) -> dict[str, Any]:
            state_ids = [f"{side}_watch", f"{side}_pending", f"{side}_open"]
            return {
                "direction": side,
                "id": f"fixture_{side}",
                "stateIds": state_ids,
                "transitionIds": [
                    f"{side}_arm",
                    f"{side}_enter",
                    f"{side}_fill",
                    f"{side}_exit",
                ],
                "indicatorIds": [
                    item["meta"]["instanceId"] for item in source["indicators"]
                ],
                "eventBindingIds": [f"{side}_event"],
                "evidenceGroupIds": [f"{side}_group"],
                "sourceProfileSnapshotSha256": canonical_sha256({"snapshot": source}),
                "sourceProgramSha256": canonical_sha256({"fixtureNativeV2": source}),
            }

        modules = [module("long", long_profile), module("short", short_profile)]
        transitions: list[dict[str, Any]] = []
        for item in modules:
            side = item["direction"]
            watch, pending, opened = item["stateIds"]
            arm, enter, fill, exit_ = item["transitionIds"]
            transitions.extend(
                [
                    {
                        "id": arm,
                        "sourceStateId": "flat",
                        "destinationStateId": watch,
                        "eventClass": "decision",
                        "guard": {"kind": "fresh_event", "eventId": f"{side}_event"},
                        "actions": [],
                    },
                    {
                        "id": enter,
                        "sourceStateId": watch,
                        "destinationStateId": pending,
                        "eventClass": "decision",
                        "guard": {
                            "kind": "evidence_at_least",
                            "groupId": f"{side}_group",
                            "thresholdPercent": 55.5,
                        },
                        "actions": [
                            {"kind": "enter_next_open", "managementPlanId": f"{side}_base"}
                        ],
                    },
                    {
                        "id": fill,
                        "sourceStateId": pending,
                        "destinationStateId": opened,
                        "eventClass": "execution",
                        "guard": {"kind": "execution_status_is", "status": "filled"},
                        "actions": [],
                    },
                    {
                        "id": exit_,
                        "sourceStateId": opened,
                        "destinationStateId": "flat",
                        "eventClass": "decision",
                        "priority": 7,
                        "guard": {"kind": "state_age_at_least", "events": 4},
                        "actions": [{"kind": "exit_next_open"}],
                    },
                ]
            )
        plans = []
        for side, source in (("long", long_profile), ("short", short_profile)):
            plan = copy.deepcopy(source["executionConfig"]["managementLibrary"]["plans"][0])
            plan["id"] = f"{side}_base"
            plans.append(plan)
        profile = {
            "version": "v3",
            "directionMode": "both",
            "name": candidate_id,
            "executionConfig": {
                "managementLibrary": {"defaultPlanId": "long_base", "plans": plans}
            },
            "graph": {
                "initialStateId": "flat",
                "states": [{"id": "flat"}]
                + [
                    {"id": state}
                    for item in modules
                    for state in item["stateIds"]
                ],
                "entryArbitration": {"modules": modules},
                "transitions": transitions,
                "eventBindings": [
                    {
                        "id": "long_event",
                        "indicatorInstanceId": long_profile["indicators"][1]["meta"]["instanceId"],
                        "longOutput": "bullish",
                        "shortOutput": "bearish",
                    },
                    {
                        "id": "short_event",
                        "indicatorInstanceId": short_profile["indicators"][1]["meta"]["instanceId"],
                        "longOutput": "bullish",
                        "shortOutput": "bearish",
                    },
                ],
                "evidenceGroups": [
                    {
                        "id": "long_group",
                        "indicatorInstanceIds": [
                            long_profile["indicators"][0]["meta"]["instanceId"]
                        ],
                    },
                    {
                        "id": "short_group",
                        "indicatorInstanceIds": [
                            short_profile["indicators"][0]["meta"]["instanceId"]
                        ],
                    },
                ],
            },
        }
        return {
            "profile": profile,
            "validation": {
                "schemaVersion": "temporal_search_candidate_validation_v1",
                "rawSourceProfileSha256": canonical_sha256(profile),
                "profileSnapshotSha256": canonical_sha256({"snapshot": profile}),
                "programSha256": canonical_sha256({"fixtureNativeV3": profile}),
                "validationReportSha256": canonical_sha256(
                    {"fixtureNativeV3Validation": profile}
                ),
                "status": "valid_evaluable",
                "candidateAcceptable": True,
            },
        }


def _snapshot(kind: str, label: str) -> IdentitySnapshot:
    return IdentitySnapshot.create(
        kind=kind,
        schema_version=f"{kind}_v1",
        payload={"fixture": label},
    )


def _program(side: str, marker: str) -> dict[str, Any]:
    return {
        "schemaVersion": "temporal_typed_fragment_grammar_v2",
        "grammarVersion": "3",
        "direction": side,
        "fragments": [
            {
                "productionId": "fixture_arm_level",
                "resources": {"group": "fixture_signal"},
                "choices": {"threshold": 37.125, "marker": marker},
            }
        ],
    }


def _profile(side: str, marker: str) -> dict[str, Any]:
    """Keep decimal indicator and protection data in every rich journal entry."""

    return {
        "version": "v2",
        "directionMode": side,
        "name": f"fixture-{side}-{marker}",
        "instruments": ["EURUSD"],
        "indicators": [
            {
                "meta": {"instanceId": f"{side}-rsi", "id": "RSI_MEAN_REVERSION"},
                "config": {"period": 14, "buyMin": 20.125, "buyMax": 39.875},
            },
            {
                "meta": {"instanceId": f"{side}-atr", "id": "ATR"},
                "config": {"period": 21, "multiplier": 1.375},
            },
        ],
        "executionConfig": {
            "managementLibrary": {
                "defaultPlanId": "base",
                "plans": [
                    {
                        "id": "base",
                        "initialStop": {"kind": "fixed_percent", "percent": 1.125},
                        "initialTarget": {
                            "kind": "reward_multiple",
                            "multiple": 2.375,
                        },
                        "breakEven": {
                            "activation": {"kind": "initial_r", "multiple": 1.625},
                            "offsetInitialR": 0.125,
                        },
                        "holdPolicy": {
                            "kind": "elapsed_calendar",
                            "hours": 36.5,
                        },
                    }
                ],
            }
        },
        "graph": {"states": [], "fixtureMarker": marker},
    }


def _module(side: str, marker: str) -> FrozenModule:
    catalog = IdentitySnapshot.create(
        kind="catalog",
        schema_version="catalog_v1",
        payload={
            "catalog": {
                "indicators": [
                    {"meta": {"id": "RSI_MEAN_REVERSION"}},
                    {"meta": {"id": "ATR"}},
                ]
            }
        },
    )
    return FrozenModule.validate_native(
        program=_program(side, marker),
        profile=_profile(side, marker),
        grammar_context=_snapshot("grammarContext", "front-half-oracle"),
        catalog=catalog,
        policy=_snapshot("policy", "front-half-oracle"),
        native_authority_identity=_snapshot("nativeAuthority", "front-half-oracle"),
        native_validator=FixtureNativeValidator(),
        candidate_id=f"fixture-module-{side}-{marker}",
    )


def _pair(long_marker: str, short_marker: str) -> FrozenPair:
    return FrozenPair.compile(
        long=_module("long", long_marker),
        short=_module("short", short_marker),
        pair_compiler_identity=_snapshot("pairCompiler", "front-half-oracle"),
        pair_compiler=FixturePairCompiler(),
        candidate_id=f"fixture-pair-{long_marker}-{short_marker}",
    )


class UniqueFixtureFactory:
    """One genuinely unique rich immigrant per frozen proposal seed."""

    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        return _pair(f"immigrant-long-{proposal_seed}", f"immigrant-short-{proposal_seed}")


class DuplicateFixtureFactory:
    """Intentional exact-executable duplicate used for rejection audit only."""

    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        del proposal_seed
        return _pair("duplicate-long", "duplicate-short")


class FixtureOperator:
    """Small deterministic structural authority modelled on the Python tests."""

    def __init__(self, *, reject_crossover: bool = False) -> None:
        self.reject_crossover = reject_crossover

    def grammar_plans(self, module: FrozenModule) -> list[dict[str, str]]:
        return [{"operator": "fixture_grammar", "side": module.direction}]

    def indicator_plans(self, module: FrozenModule) -> list[dict[str, str]]:
        return [{"operator": "fixture_indicator", "side": module.direction}]

    def hold_policy_choices(self, module: FrozenModule) -> list[dict[str, Any]]:
        del module
        return []

    def initial_protection_plans(self, module: FrozenModule) -> list[dict[str, Any]]:
        return [
            {
                "operator": "fixture_initial_protection",
                "side": module.direction,
                "mutationClass": "adjacent",
                "stopPercent": 0.875,
                "targetR": 2.625,
            }
        ]

    def apply_grammar(
        self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str
    ) -> tuple[FrozenModule, dict[str, Any]]:
        return self._changed(module, plan, candidate_id=candidate_id, family="grammar")

    def apply_indicator(
        self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str
    ) -> tuple[FrozenModule, dict[str, Any]]:
        return self._changed(module, plan, candidate_id=candidate_id, family="indicator")

    def apply_initial_protection(
        self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str
    ) -> tuple[FrozenModule, dict[str, Any]]:
        return self._changed(module, plan, candidate_id=candidate_id, family="protection")

    def _changed(
        self,
        module: FrozenModule,
        plan: Mapping[str, Any],
        *,
        candidate_id: str,
        family: str,
    ) -> tuple[FrozenModule, dict[str, Any]]:
        marker = f"{family}-{candidate_id}"
        child = _module(module.direction, marker)
        audit = {
            "schemaVersion": "temporal_qd_front_half_fixture_operator_audit_v1",
            "family": family,
            "candidateId": candidate_id,
            "planSha256": canonical_sha256(plan),
        }
        return child, audit

    def crossover(
        self,
        left_program: Mapping[str, Any],
        right_program: Mapping[str, Any],
        *,
        direction: str,
        proposal_seed: str,
    ) -> Mapping[str, Any]:
        del right_program, direction, proposal_seed
        return copy.deepcopy(dict(left_program))

    def compile_program(
        self, template: FrozenModule, program: Mapping[str, Any], *, candidate_id: str
    ) -> FrozenModule:
        if self.reject_crossover:
            raise TemporalDiscoveryContractError("fixture expected crossover rejection")
        del program
        return _module(template.direction, f"crossover-{candidate_id}")


def _policy(pair: FrozenPair) -> dict[str, Any]:
    return {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": pair.pair_compiler.canonical_payload(),
    }


def _arguments(
    *,
    generation_index: int,
    target_unique_candidates: int,
    pair_factory: Any,
    parents: Iterable[FrozenPair] = (),
    reject_crossover: bool = False,
    g0_evaluation_width: int | None = None,
    max_proposal_attempts: int | None = None,
) -> dict[str, Any]:
    parent_rows = tuple(parents)
    authority_pair = parent_rows[0] if parent_rows else _pair("policy-long", "policy-short")
    return {
        "generation_index": generation_index,
        "target_unique_candidates": target_unique_candidates,
        "run_config": {
            "seed": f"front-half-oracle-{generation_index}-{target_unique_candidates}",
            "fixture": "no-market-no-gateway",
        },
        "pair_policy": _policy(authority_pair),
        "parent_pairs": parent_rows,
        "pair_factory": pair_factory,
        "module_authority": FixtureOperator(reject_crossover=reject_crossover),
        "native_validator": FixtureNativeValidator(),
        "pair_compiler": FixturePairCompiler(),
        "operator_implementation_identity": dict(OPERATOR_IDENTITY),
        "max_proposal_attempts": max_proposal_attempts
        if max_proposal_attempts is not None
        else max(target_unique_candidates * 6, 16),
        "implementation": "optimized",
        "population_finalizer": "python",
        **(
            {"g0_evaluation_width": g0_evaluation_width}
            if g0_evaluation_width is not None
            else {}
        ),
    }


def _run_full_and_restart(
    *, root: Path, common: Mapping[str, Any], split_at: int
) -> tuple[dict[str, Any], dict[str, Any]]:
    full_root = root / "full"
    split_root = root / "split-restart"
    full = generate_pair_population(output_root=full_root, **common)
    progress = generate_pair_population(
        output_root=split_root, max_new_proposals=split_at, **common
    )
    resumed = generate_pair_population(output_root=split_root, **common)
    if progress.get("completed") is True:
        raise AssertionError("oracle split must exercise a real restart")
    if resumed != full:
        raise AssertionError("optimized Python restart result diverged from uninterrupted result")
    return full, semantic_oracle.compare_roots(full_root, split_root, shape=int(common["target_unique_candidates"]))


def _proposal_dispositions(root: Path) -> list[str]:
    return [
        str(json.loads(path.read_text(encoding="utf-8"))["disposition"])
        for path in sorted((root / "proposal-journal").glob("*.json"))
    ]


def materialize_python_oracle_corpus(output_root: Path | str) -> dict[str, Any]:
    """Create roots for the fixed 1/8 admission cases and compact coverage facts."""

    root = Path(output_root).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"oracle corpus output must be absent or empty: {root}")
    root.mkdir(parents=True, exist_ok=True)

    shape1_common = _arguments(
        generation_index=1,
        target_unique_candidates=1,
        pair_factory=UniqueFixtureFactory(),
        g0_evaluation_width=1,
    )
    shape8_common = _arguments(
        generation_index=1,
        target_unique_candidates=8,
        pair_factory=UniqueFixtureFactory(),
        g0_evaluation_width=8,
    )
    shape1_result, shape1_compare = _run_full_and_restart(
        root=root / "shape-1-g0", common=shape1_common, split_at=0
    )
    shape8_result, shape8_compare = _run_full_and_restart(
        root=root / "shape-8-g0", common=shape8_common, split_at=3
    )

    parents = (_pair("parent-a-long", "parent-a-short"), _pair("parent-b-long", "parent-b-short"))
    offspring_common = _arguments(
        generation_index=2,
        target_unique_candidates=8,
        pair_factory=UniqueFixtureFactory(),
        parents=parents,
    )
    offspring_result, offspring_compare = _run_full_and_restart(
        root=root / "shape-8-offspring", common=offspring_common, split_at=5
    )
    offspring_root = root / "shape-8-offspring" / "full"
    offspring_rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((offspring_root / "proposal-journal").glob("*.json"))
    ]
    if not any(int(row["proposal"].get("mutationDepth") or 0) > 1 for row in offspring_rows):
        raise AssertionError("fixture did not cover a depth-greater-than-one mutation")
    if not any(
        row["proposal"].get("proposalKind") == "temporal_qd_same_side_crossover_v1"
        for row in offspring_rows
    ):
        raise AssertionError("fixture did not cover same-side crossover")

    rejected_common = _arguments(
        generation_index=2,
        target_unique_candidates=7,
        pair_factory=UniqueFixtureFactory(),
        parents=parents,
        reject_crossover=True,
        max_proposal_attempts=8,
    )
    rejected_root = root / "shape-8-rejected-crossover"
    rejected = generate_pair_population(output_root=rejected_root, **rejected_common)
    rejected_rows = _proposal_dispositions(rejected_root)
    if "operation_rejected" not in rejected_rows:
        raise AssertionError("fixture did not record rejected crossover")

    duplicate_common = _arguments(
        generation_index=2,
        target_unique_candidates=2,
        pair_factory=DuplicateFixtureFactory(),
        max_proposal_attempts=3,
    )
    duplicate_root = root / "shape-8-duplicate"
    duplicate = generate_pair_population(output_root=duplicate_root, **duplicate_common)
    duplicate_rows = _proposal_dispositions(duplicate_root)
    if not any(item.startswith("duplicate_pair_genome") for item in duplicate_rows):
        raise AssertionError("fixture did not record duplicate rejection")

    def row(result: Mapping[str, Any], comparison: Mapping[str, Any], *, shape: int) -> dict[str, Any]:
        return {
            "shape": shape,
            "completed": bool(result.get("completed")),
            "populationSha256": result.get("populationSha256"),
            "generationJournalSha256": result.get("generationJournalSha256"),
            "restartSemanticExact": comparison["semanticExact"],
            "restartByteExact": comparison["byteExact"],
            "semanticTreeSha256": comparison["leftSemanticTreeSha256"],
        }

    manifest = {
        "schemaVersion": CORPUS_SCHEMA,
        "scope": "optimized_python_pair_generation_no_market_no_gateway",
        "cases": {
            "shape1G0": row(shape1_result, shape1_compare, shape=1),
            "shape8G0": row(shape8_result, shape8_compare, shape=8),
            "shape8Offspring": row(offspring_result, offspring_compare, shape=8),
            "rejectedCrossover": {
                "completed": bool(rejected.get("completed")),
                "dispositions": rejected_rows,
            },
            "duplicateRejection": {
                "completed": bool(duplicate.get("completed")),
                "dispositions": duplicate_rows,
            },
        },
        "coverage": {
            "g0RandomImmigrants": True,
            "parentMutationDepthGreaterThanOne": True,
            "sameSideCrossover": True,
            "crossoverRejection": True,
            "duplicateRejection": True,
            "splitRestart": True,
            "decimalIndicatorAndProtectionValues": [
                20.125,
                39.875,
                1.375,
                1.125,
                2.375,
                1.625,
                0.125,
                36.5,
            ],
        },
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    (root / "corpus-manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return manifest


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    print(json.dumps(materialize_python_oracle_corpus(args.output_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
