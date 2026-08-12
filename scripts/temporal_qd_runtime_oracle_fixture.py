"""Materialize a compact real-authority RuntimePairAuthority admission fixture.

This invokes only the local Dashboard JSONL validator using its frozen current
catalog/grammar authority.  It does not read market data, contact the lake or
gateway, or run economic replay.  Rich artifact bytes are written only to the
caller-selected output root.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from autoresearch.temporal_bidirectional_genome import FrozenPair, IdentitySnapshot
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_discovery_validation import SubprocessCandidateValidator
from autoresearch.temporal_qd_evolution import (
    _empty_identity_ledger,
    _predeclared_lake_scope_report,
    canonical_empty_bidirectional_archive_template,
    initialize_empty_bidirectional_archive,
)
from autoresearch.temporal_qd_pair_factory import (
    PairAuthorityBundle,
    default_hold_operator_policy,
    freeze_pair_run_config,
    pair_policy_from_config,
)
from autoresearch.temporal_qd_pair_generation import (
    _mutation_depth_for_seed,
    _pair_genome_semantic_sha256,
    _propose_crossover,
    _propose_pair_sequence,
    _unbiased_choice,
    materialize_pair_candidate,
    proposal_side,
)
from scripts import build_temporal_pair_authority as authority_builder


FIXTURE_SCHEMA = "temporal_qd_runtime_pair_authority_oracle_fixture_v1"
TRANSCRIPT_SCHEMA = "temporal_qd_dashboard_jsonl_transcript_v1"
RUNTIME_MANIFEST_SCHEMA = "temporal_qd_runtime_manifest_v1"
GENERATOR_SOURCE_FILES = (
    "scripts/temporal_qd_runtime_oracle_fixture.py",
    "scripts/build_temporal_pair_authority.py",
    "autoresearch/temporal_bidirectional_genome.py",
    "autoresearch/temporal_discovery_validation.py",
    "autoresearch/temporal_qd_evolution.py",
    "autoresearch/temporal_qd_pair_factory.py",
    "autoresearch/temporal_qd_pair_generation.py",
    "autoresearch/temporal_typed_motif_grammar.py",
)


class _RecordingClient(SubprocessCandidateValidator):
    """Record real local JSONL semantics with stable fixture request IDs."""

    records: list[dict[str, Any]] = []

    @classmethod
    def _request_id(cls) -> str:
        return f"runtime-oracle-{len(cls.records):04d}"

    def validate(self, *, candidate_id: str, source_profile: Mapping[str, Any], expected_raw_source_profile_sha256: str) -> dict[str, Any]:
        report = super().validate(candidate_id=candidate_id, source_profile=source_profile, expected_raw_source_profile_sha256=expected_raw_source_profile_sha256)
        request_id = self._request_id()
        request = {"schemaVersion": "temporal_search_candidate_validation_jsonl_request_v1", "operation": "validate_candidate", "requestId": request_id, "candidateId": candidate_id, "expectedRawSourceProfileSha256": expected_raw_source_profile_sha256, "sourceProfile": dict(source_profile)}
        response = {"schemaVersion": "temporal_search_candidate_validation_jsonl_response_v1", "requestId": request_id, "operation": "validate_candidate", "semanticExitCode": 0 if report["candidateAcceptable"] else 2, "report": report}
        self.records.append({"ordinal": len(self.records), "request": request, "response": response})
        return report

    def compile_pair(self, *, candidate_id: str, long_profile: Mapping[str, Any], short_profile: Mapping[str, Any], expected_long_raw_source_profile_sha256: str, expected_short_raw_source_profile_sha256: str) -> dict[str, Any]:
        result = super().compile_pair(candidate_id=candidate_id, long_profile=long_profile, short_profile=short_profile, expected_long_raw_source_profile_sha256=expected_long_raw_source_profile_sha256, expected_short_raw_source_profile_sha256=expected_short_raw_source_profile_sha256)
        request_id = self._request_id()
        request = {"schemaVersion": "temporal_search_bidirectional_compile_jsonl_request_v1", "operation": "compile_bidirectional", "requestId": request_id, "candidateId": candidate_id, "longProfile": dict(long_profile), "shortProfile": dict(short_profile), "expectedLongRawSourceProfileSha256": expected_long_raw_source_profile_sha256, "expectedShortRawSourceProfileSha256": expected_short_raw_source_profile_sha256}
        response = {"schemaVersion": "temporal_search_bidirectional_compile_jsonl_response_v1", "requestId": request_id, "operation": "compile_bidirectional", "semanticExitCode": 0, "result": result}
        self.records.append({"ordinal": len(self.records), "request": request, "response": response})
        return result


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8", newline="\n")


def _portable_source_sha256(raw: bytes) -> str:
    """Hash source semantics independently of Git's CRLF checkout policy."""
    return "sha256:" + hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def _generator_source_identity() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    files = {
        relative: _portable_source_sha256((root / relative).read_bytes())
        for relative in GENERATOR_SOURCE_FILES
    }
    value = {
        "schemaVersion": "temporal_qd_runtime_oracle_generator_source_v1",
        "files": files,
    }
    value["generatorSourceSha256"] = canonical_sha256(value)
    return value


def _frozen_config(dashboard_root: Path) -> dict[str, Any]:
    catalog = authority_builder._catalog(dashboard_root / "shared/constants/indicators.json")
    context = authority_builder._context(catalog, timeframe="M5")
    side = {"seedNames": ["mean_reversion", "breakout", "trend"], "context": context, "catalog": catalog, "policy": {"schemaVersion": "temporal_pair_catalog_seed_policy_v1", "resourceRoles": authority_builder.RESOURCE_ROLES, "resourceRoleDisposition": "seed_priors_only_v1"}}
    interpreter = dashboard_root / "compute-service/.venv/Scripts/python.exe"
    validator = dashboard_root / "scripts/temporal_search_validate_candidate.py"
    return freeze_pair_run_config({"schemaVersion": "temporal_qd_bidirectional_pair_run_config_v2", "longModule": side, "shortModule": copy.deepcopy(side), "nativeJsonlAuthority": {"command": [str(interpreter), str(validator)], "timeoutSeconds": 60, "persistentJsonl": True, "maxLineBytes": 8 * 1024 * 1024, "stderrLimitBytes": 64 * 1024, "interpreterPath": str(interpreter), "validatorScriptPath": str(validator), "dashboardSourceRoot": str(dashboard_root), "environment": {"PYTHONPATH": [str(dashboard_root / "shared/python")]}} , "holdOperatorPolicy": default_hold_operator_policy()})


def _depth_seed(depth: int) -> str:
    bucket = {2: 14, 3: 19}[depth]
    for ordinal in range(10_000):
        seed = f"runtime-oracle-depth:{bucket}:{ordinal}"
        if _unbiased_choice(seed, size=20) == bucket and _mutation_depth_for_seed(seed) == depth:
            return seed
    raise AssertionError("fixed depth witness seed was not found")


def _materialized_depth_witness(
    *,
    depth: int,
    parent: FrozenPair,
    runtime: PairAuthorityBundle,
) -> tuple[FrozenPair, dict[str, Any]]:
    """Find the first deterministic executable witness for the requested depth.

    The fixture is explicitly coupled to the current local Dashboard authority.
    A syntactically valid seeded mutation can legitimately be rejected when a
    newly enforced invariant (for example the entry-indicator cap) applies.
    Search only the predeclared seed stream and retain the first *admitted*
    depth witness, rather than weakening that invariant or claiming a rejected
    operation exercised the requested positive path.
    """

    seed_prefix = {2: 14, 3: 19}[depth]
    for ordinal in range(10_000):
        seed = f"runtime-oracle-depth:{seed_prefix}:{ordinal}"
        if _unbiased_choice(seed, size=20) != seed_prefix or _mutation_depth_for_seed(seed) != depth:
            continue
        transcript_start = len(_RecordingClient.records)
        pair, proposal = _propose_pair_sequence(
            proposal_seed=seed,
            parent=parent,
            mutation_depth=depth,
            module_authority=runtime.operator,
            native_validator=runtime.validator,
            pair_compiler=runtime.compiler,
        )
        if pair is not None and len(proposal.get("mutationSteps") or []) == depth:
            return pair, proposal
        # Witness probing must not leak Dashboard requests for a proposal the
        # runtime transcript will never replay.  The retained record range is
        # therefore exactly the selected, materialized operation.
        del _RecordingClient.records[transcript_start:]
    raise AssertionError(f"depth-{depth} real authority witness did not materialize")


def _materialized_crossover_witness(
    *,
    parent: FrozenPair,
    mate: FrozenPair,
    runtime: PairAuthorityBundle,
) -> tuple[FrozenPair, dict[str, Any]]:
    """Find the first deterministic same-side crossover the authority admits.

    The fixture must cover the positive crossover path, but a particular
    syntactically deterministic splice may now be invalid under a tightened
    program policy or may be a semantic no-op.  Keep the witness selection
    reproducible and require a real materialized pair; never loosen either
    the crossover or compiler admission rules merely to retain this fixture.
    """

    for ordinal in range(10_000):
        seed = f"runtime-oracle-crossover-materialized:{ordinal}"
        transcript_start = len(_RecordingClient.records)
        pair, proposal = _propose_crossover(
            proposal_seed=seed,
            parent=parent,
            mate=mate,
            module_authority=runtime.operator,
            native_validator=runtime.validator,
            pair_compiler=runtime.compiler,
            parent_selection=None,
            mate_selection=None,
            mate_selection_attempts=[],
        )
        if pair is not None and proposal.get("disposition") == "materialized":
            return pair, proposal
        # As above, do not leave orphaned native requests from rejected or
        # semantic-no-op crossover probes ahead of the selected witness.
        del _RecordingClient.records[transcript_start:]
    raise AssertionError("same-side crossover real authority witness did not materialize")


def _scope_context() -> dict[str, Any]:
    value = {
        "schemaVersion": "temporal_qd_predeclared_evidence_context_v3",
        "baseDecisionTimeframe": "M5",
        "orderedWindowPlanSemantic": [
            {
                "windowId": "oracle-development",
                "window": {
                    "analysisWindowStart": "2024-02-01T00:00:00Z",
                    "analysisWindowEnd": "2024-03-01T00:00:00Z",
                },
                "evidencePlanSemantic": {
                    "lake_window_binding": {
                        "window_semantic_sha256": "sha256:" + "e" * 64,
                        "request": {
                            "pairs": ["EURUSD"],
                            "timeframes": ["M5", "M15", "H1"],
                            "data_start": "2024-01-01T00:00:00Z",
                            "data_end": "2024-03-01T00:00:00Z",
                        },
                    },
                },
            }
        ],
        "workerContractSha256": None,
        "constructionCatalog": None,
        "costViews": {
            "none": {"spreadBps": 0.0, "slippageBps": 0.0, "commissionBps": 0.0},
            "research_conservative": {"spreadBps": 2.0, "slippageBps": 1.0, "commissionBps": 0.5},
        },
    }
    value["predeclaredEvidenceContextSha256"] = canonical_sha256(value)
    return value


def materialize_runtime_oracle_fixture(output_root: Path | str, *, dashboard_root: Path | str = r"C:\repos\Trading-Dashboard") -> dict[str, Any]:
    root = Path(output_root).resolve()
    dashboard = Path(dashboard_root).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("runtime oracle output must be absent or empty")
    root.mkdir(parents=True, exist_ok=True)
    frozen = _frozen_config(dashboard)
    policy = pair_policy_from_config(frozen)
    _RecordingClient.records = []
    import autoresearch.temporal_qd_pair_factory as pair_factory_module
    original = pair_factory_module.SubprocessCandidateValidator
    pair_factory_module.SubprocessCandidateValidator = _RecordingClient
    try:
        with PairAuthorityBundle(frozen) as runtime:
            immigrant = runtime.factory.create_pair(proposal_seed="runtime-oracle-rich-immigrant-v1")
            rich_proposal = {"schemaVersion": "temporal_qd_pair_proposal_v2", "proposalSeed": "runtime-oracle-rich-immigrant-v1", "originKind": "random_immigrant", "side": "long", "factoryPair": immigrant.canonical_payload(), "pairIdentitySha256": immigrant.identity_sha256, "disposition": "materialized"}
            rich_proposal["proposalSha256"] = canonical_sha256(rich_proposal)
            cases: dict[str, tuple[FrozenPair | None, dict[str, Any]]] = {"richImmigrant": (immigrant, rich_proposal)}
            for depth in (2, 3):
                pair, proposal = _materialized_depth_witness(
                    depth=depth,
                    parent=immigrant,
                    runtime=runtime,
                )
                cases[f"sequentialMutationDepth{depth}"] = (pair, proposal)
            materialized, crossover = _materialized_crossover_witness(
                parent=cases["sequentialMutationDepth2"][0],
                mate=cases["sequentialMutationDepth3"][0],
                runtime=runtime,
            )
            cases["sameSideCrossoverMaterialized"] = (materialized, crossover)
            rejection_seed = next(
                f"runtime-oracle-crossover-rejected:{ordinal}"
                for ordinal in range(100)
                if proposal_side(f"runtime-oracle-crossover-rejected:{ordinal}") == "long"
            )
            foreign_authority = IdentitySnapshot.create(
                kind="nativeAuthority",
                schema_version="temporal_qd_runtime_oracle_negative_fixture_v1",
                payload={"reason": "deliberately foreign frozen authority for rejection coverage"},
            )
            foreign_mate = replace(
                immigrant,
                long=replace(immigrant.long, native_authority=foreign_authority),
            )
            rejected_pair, rejected = _propose_crossover(proposal_seed=rejection_seed, parent=immigrant, mate=foreign_mate, module_authority=runtime.operator, native_validator=runtime.validator, pair_compiler=runtime.compiler, parent_selection=None, mate_selection=None, mate_selection_attempts=[])
            if rejected_pair is not None or rejected.get("disposition") != "operation_rejected":
                raise AssertionError("foreign-authority crossover must be the rejection witness")
            cases["sameSideCrossoverRejected"] = (rejected_pair, rejected)
            candidates: dict[str, dict[str, Any]] = {}
            for ordinal, (name, (pair, proposal)) in enumerate(cases.items()):
                if pair is not None:
                    candidates[name] = materialize_pair_candidate(pair=pair, proposal=proposal, pair_policy=policy, generation_index=1, birth_ordinal=ordinal, proposal_ordinal=ordinal)
    finally:
        pair_factory_module.SubprocessCandidateValidator = original
    sample = candidates["richImmigrant"]
    scope_context = _scope_context()
    source = copy.deepcopy(immigrant.long.canonical_payload()["profile"])
    source["instruments"] = ["EURUSD"]
    source["indicators"][0]["config"]["timeframe"] = "M15"
    in_scope = _predeclared_lake_scope_report(source, scope_context, frozen_construction_catalog=frozen["longModule"]["catalog"])
    outside = copy.deepcopy(source); outside["indicators"][0]["config"]["timeframe"] = "M1"
    out_scope = _predeclared_lake_scope_report(outside, scope_context, frozen_construction_catalog=frozen["longModule"]["catalog"])
    if not in_scope["acceptable"] or out_scope["acceptable"]:
        raise AssertionError("predeclared scope fixture did not prove in/out containment")
    archive = initialize_empty_bidirectional_archive(canonical_empty_bidirectional_archive_template(), policy)
    generation_config = {"schemaVersion": "temporal_qd_pair_generation_v3", "generationIndex": 1, "targetUniqueCandidates": 8, "pairRunConfigSha256": frozen["pairRunConfigSha256"]}
    manifest = {"schemaVersion": RUNTIME_MANIFEST_SCHEMA, "pairRunConfig": frozen, "pairRunConfigSha256": frozen["pairRunConfigSha256"], "bidirectionalPairPolicy": policy, "bidirectionalPairPolicySha256": canonical_sha256(policy), "evidenceIdentityContext": scope_context, "evidenceIdentityContextSha256": scope_context["predeclaredEvidenceContextSha256"], "generationIndex": 1, "pairGenerationConfigSha256": canonical_sha256(generation_config), "parentArchive": archive, "parentArchiveSha256": archive["archiveSha256"], "identityLedger": _empty_identity_ledger()}
    transcript = {"schemaVersion": TRANSCRIPT_SCHEMA, "authorityContentSha256": canonical_sha256(frozen["nativeJsonlAuthority"]["authorityContent"]), "records": _RecordingClient.records}
    detail: dict[str, Any] = {"schemaVersion": FIXTURE_SCHEMA, "mode": "local_dashboard_jsonl_no_market_no_lake_no_gateway", "cases": {}, "predeclaredScope": {"context": scope_context, "inScope": in_scope, "outOfScope": out_scope}, "runtimeManifestSha256": canonical_sha256(manifest), "generatorSourceIdentity": _generator_source_identity(), "transcriptSha256": canonical_sha256(transcript)}
    for name, (pair, proposal) in cases.items():
        operation = {
            key: proposal[key]
            for key in ("proposalKind", "proposalSeed", "originKind", "side", "mutationDepth", "disposition")
            if key in proposal
        }
        row: dict[str, Any] = {
            "operation": operation,
            "proposalSha256": proposal["proposalSha256"],
            "predeclaredEvidenceContextSha256": scope_context["predeclaredEvidenceContextSha256"],
        }
        if pair is not None:
            candidate = candidates[name]
            funnel = {
                "schemaVersion": "temporal_qd_runtime_oracle_front_half_funnel_v1",
                "candidateIdentitySha256": candidate["candidateIdentitySha256"],
                "stages": [
                    "proposal_materialized",
                    "dashboard_native_admission",
                    "bidirectional_candidate_materialized",
                ],
                "economicEvaluation": "intentionally_absent_no_market_fixture",
            }
            funnel["funnelSha256"] = canonical_sha256(funnel)
            row.update({"pairIdentitySha256": pair.identity_sha256, "pairExecutableSemanticSha256": _pair_genome_semantic_sha256(pair), "candidateIdentitySha256": candidate["candidateIdentitySha256"], "sourceProfileSha256": candidate["sourceProfileSha256"], "programSha256": candidate["programSha256"], "validationReportSha256": candidate["validationReportSha256"], "funnel": funnel})
        detail["cases"][name] = row
    detail["fixtureSha256"] = canonical_sha256(detail)
    _write(root / "runtime-manifest.json", manifest); _write(root / "dashboard-jsonl-transcript.json", transcript); _write(root / "fixture.json", detail)
    return detail
