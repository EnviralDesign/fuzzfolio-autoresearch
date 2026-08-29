"""Freeze the V3 component-extraction census without admitting outcome values.

The only row input is V1's separately materialized component-context identity
corpus.  It contains the frozen profile, component, insertion, and source
identities needed for extraction, but no realized economics or outcome labels.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


EXPECTED_CONTEXT_COUNT = 41
EXPECTED_COMPONENT_COUNT = 19
EXPECTED_P3_SAME_PANEL_PARENT_COMPARABLE_COUNT = 25
EXPECTED_REALIZED_PHENOTYPE_COUNT = 17
EXPECTED_MULTIPANEL_CHILD_COUNT = 11
EXPECTED_P1_P2_PARENT_COMPARABLE_COUNT = 9
HISTORICAL_ENGINE_COMMIT = "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2"
V38_SOURCE_COMMIT = "51c2f9175f441166e7fc997109e939a9f9103b5d"
V3_PROMPT_SHA256 = "sha256:97522e780e19f8b399bc465b6358660a11ee4613a3b8ee0e8d1f7e07be588a26"

SAFE_AUTHORITY_FIELDS = (
    "authoredProgramSha256",
    "catalogAuthorityAvailability",
    "catalogAuthoritySha256",
    "evidenceWindowIdentities",
    "phenotypeIdentitySha256",
    "proposalEntrySha256",
    "receiptAvailability",
    "receiptSha256",
    "resolvedProgramSha256",
    "sourceProfileSha256",
    "sourceProfileSnapshotSha256",
    "terminalOperatorApplicationSha256",
    "terminalOperatorPlanSha256",
)


def canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_prefixed(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def read_rows(path: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not all(isinstance(row, dict) for row in rows):
        raise RuntimeError("V1 identity corpus must contain JSON objects")
    return rows


def read_candidate_sources(path: Path, candidate_ids: set[str]) -> dict[str, dict[str, Any]]:
    """Read only the frozen candidate/profile portion of evaluated members.

    The source file also carries outcomes for the later separate join.  This
    census reads no aggregate, objective, or outcome field and never serializes
    any of them into its manifest.
    """
    sources: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            candidate_id = record.get("candidateId")
            if candidate_id not in candidate_ids:
                continue
            candidate = record.get("candidate")
            if not isinstance(candidate, dict):
                raise RuntimeError(f"candidate {candidate_id!r} has no source profile")
            sources[str(candidate_id)] = candidate
    missing = candidate_ids.difference(sources)
    if missing:
        raise RuntimeError(f"frozen candidate source is missing IDs: {sorted(missing)}")
    return sources


def reject_outcome_fields(value: Any, path: str = "$") -> None:
    """Fail closed if an outcome-shaped field reaches the V3 manifest."""
    if isinstance(value, dict):
        for key, nested in value.items():
            lowered = str(key).lower()
            if any(
                token in lowered
                for token in ("economics", "outcome", "beat", "rank", "quality", "delta")
            ):
                raise RuntimeError(f"outcome-shaped field is forbidden in census: {path}.{key}")
            reject_outcome_fields(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            reject_outcome_fields(nested, f"{path}[{index}]")


def candidate_profile_binding(row: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    component = row["component"]
    insertion = row["insertion"]
    profile = candidate.get("sourceProfile")
    if not isinstance(profile, dict):
        raise RuntimeError(f"candidate {row['candidateId']!r} sourceProfile is missing")
    indicators = profile.get("indicators")
    graph = profile.get("graph")
    if not isinstance(indicators, list) or not isinstance(graph, dict):
        raise RuntimeError(f"candidate {row['candidateId']!r} sourceProfile is malformed")
    matching_indicators = [
        item
        for item in indicators
        if isinstance(item, dict)
        and isinstance(item.get("meta"), dict)
        and item["meta"].get("instanceId") == component["indicatorInstanceId"]
    ]
    if len(matching_indicators) != 1:
        raise RuntimeError(f"candidate {row['candidateId']!r} does not bind its inserted indicator exactly once")
    indicator = matching_indicators[0]
    if indicator["meta"].get("id") != component["indicatorId"]:
        raise RuntimeError("frozen source profile indicator ID differs from the V1 identity corpus")
    if indicator.get("config") != component["fullConfiguration"]:
        raise RuntimeError("frozen source profile indicator configuration differs from the V1 identity corpus")
    bindings = graph.get("eventBindings")
    if not isinstance(bindings, list):
        raise RuntimeError("frozen source profile eventBindings are missing")
    matching_bindings = [
        item for item in bindings
        if isinstance(item, dict) and item.get("id") == insertion["exactBindingId"]
    ]
    if len(matching_bindings) != 1:
        raise RuntimeError("frozen source profile does not bind the inserted event exactly once")
    binding = matching_bindings[0]
    if binding.get("indicatorInstanceId") != component["indicatorInstanceId"]:
        raise RuntimeError("frozen source profile binding points at a different indicator instance")
    if binding.get("longOutput") != component["eventOutputs"]["longOutput"]:
        raise RuntimeError("frozen source profile long event output differs from the V1 identity corpus")
    if binding.get("shortOutput") != component["eventOutputs"]["shortOutput"]:
        raise RuntimeError("frozen source profile short event output differs from the V1 identity corpus")
    history = candidate.get("structuralOperatorHistory")
    history = history if isinstance(history, list) else []
    terminal_entries = [
        item for item in history
        if isinstance(item, dict)
        and item.get("side") == row["side"]
        and item.get("terminalOperatorApplicationSha256")
    ]
    if len(terminal_entries) > 1:
        raise RuntimeError("candidate has ambiguous terminal operator applications for the inserted side")
    terminal = terminal_entries[0] if terminal_entries else None
    return {
        "candidateIdentitySha256": candidate.get("candidateIdentitySha256"),
        "profileSnapshotSha256": candidate.get("profileSnapshotSha256"),
        "programSha256": candidate.get("programSha256"),
        "proposalEntrySha256": candidate.get("proposalEntrySha256"),
        "sourceProfileSha256": candidate.get("sourceProfileSha256"),
        "clockRequirement": graph.get("clockRequirement"),
        "terminalOperatorApplication": (
            {
                "availability": "retained_in_structural_operator_history",
                "operation": terminal.get("operation"),
                "operatorTraceSha256": terminal.get("operatorTraceSha256"),
                "terminalOperatorApplicationSha256": terminal.get("terminalOperatorApplicationSha256"),
                "terminalOperatorPlanSha256": terminal.get("terminalOperatorPlanSha256"),
            }
            if terminal is not None
            else {"availability": "not_retained_in_candidate_structural_operator_history"}
        ),
    }


def normalized_context(row: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    component = row.get("component")
    insertion = row.get("insertion")
    authorities = row.get("authorities")
    if not isinstance(component, dict) or not isinstance(insertion, dict) or not isinstance(authorities, dict):
        raise RuntimeError("V1 identity row is missing component, insertion, or authorities")
    for key in (
        "componentContextIdentity",
        "componentIdentity",
        "candidateId",
        "parentCandidateId",
        "side",
    ):
        if not row.get(key):
            raise RuntimeError(f"V1 identity row is missing {key}")
    for key in (
        "indicatorId",
        "baseIndicatorId",
        "indicatorInstanceId",
        "eventOutputs",
        "timeframe",
        "lookbackBars",
        "signalPersistence",
        "fullConfiguration",
    ):
        if key not in component:
            raise RuntimeError(f"V1 component is missing {key}")
    for key in (
        "exactBindingId",
        "bindingIndicatorInstanceId",
        "routeAvailability",
        "routeReferencePaths",
    ):
        if key not in insertion:
            raise RuntimeError(f"V1 insertion is missing {key}")
    if component["indicatorInstanceId"] != insertion["bindingIndicatorInstanceId"]:
        raise RuntimeError("indicator instance and event binding instance differ")
    configuration = component["fullConfiguration"]
    if not isinstance(configuration, dict):
        raise RuntimeError("V1 component configuration is not an object")
    if bool(configuration.get("useFormingBar")):
        raise RuntimeError("event-insert census contains a forming-bar component")
    if int(component["lookbackBars"]) != int(configuration.get("lookbackBars", 0)):
        raise RuntimeError("component lookback does not match its full configuration")
    safe_authorities = {key: authorities.get(key) for key in SAFE_AUTHORITY_FIELDS}
    context = {
        "componentContextIdentity": row["componentContextIdentity"],
        "componentIdentity": row["componentIdentity"],
        "candidateId": row["candidateId"],
        "parentCandidateId": row["parentCandidateId"],
        "side": row["side"],
        "component": component,
        "insertion": insertion,
        "sourceAuthorities": safe_authorities,
        "candidateProfileBinding": candidate_profile_binding(row, candidate),
    }
    reject_outcome_fields(context)
    return context


def build_manifest(identity_path: Path, evaluated_members_path: Path) -> dict[str, Any]:
    raw_rows = read_rows(identity_path)
    candidate_sources = read_candidate_sources(
        evaluated_members_path,
        {str(row.get("candidateId")) for row in raw_rows},
    )
    contexts = [
        normalized_context(row, candidate_sources[str(row["candidateId"])])
        for row in raw_rows
    ]
    contexts.sort(key=lambda row: row["componentContextIdentity"])
    identities = [row["componentContextIdentity"] for row in contexts]
    if len(contexts) != EXPECTED_CONTEXT_COUNT or len(set(identities)) != EXPECTED_CONTEXT_COUNT:
        raise RuntimeError("V1 context census is not exactly 41 unique identities")
    component_ids = {row["componentIdentity"] for row in contexts}
    if len(component_ids) != EXPECTED_COMPONENT_COUNT:
        raise RuntimeError("V1 context census is not exactly 19 unique components")
    component_payloads: dict[str, bytes] = {}
    for context in contexts:
        component_id = context["componentIdentity"]
        # The same immutable component can be inserted into several child
        # profiles under child-specific instance IDs.  Preserve those instance
        # IDs in each context, but do not mistake them for component drift.
        component_definition = {
            key: value
            for key, value in context["component"].items()
            if key != "indicatorInstanceId"
        }
        current = canonical_bytes(component_definition)
        previous = component_payloads.setdefault(component_id, current)
        if previous != current:
            raise RuntimeError(f"component identity {component_id} has divergent configurations")
    m5_context = contexts[0]
    m15_contexts = [
        context for context in contexts if context["component"]["timeframe"] == "M15"
    ]
    if not m15_contexts:
        raise RuntimeError("V1 census has no M15 component context")
    manifest = {
        "schemaVersion": "temporal_qd_component_surrogate_extraction_census_v3",
        "sourcePins": {
            "historicalEngineCommit": HISTORICAL_ENGINE_COMMIT,
            "frozenV38AutoResearchCommit": V38_SOURCE_COMMIT,
            "v3HumanDirectiveSha256": V3_PROMPT_SHA256,
            "v1IdentityCorpus": {
                "path": str(identity_path.resolve()),
                "sha256": sha256_file(identity_path),
                "role": "outcome-value-free component/context identity source",
            },
            "v38CandidateProfileSource": {
                "path": str(evaluated_members_path.resolve()),
                "sha256": sha256_file(evaluated_members_path),
                "role": "frozen candidate/profile/application provenance; only candidate/profile fields are admitted",
            },
        },
        "cohortCounts": {
            "acceptedDirectionalEventInsertContexts": EXPECTED_CONTEXT_COUNT,
            "uniqueComponentIdentities": EXPECTED_COMPONENT_COUNT,
            "p3SamePanelParentComparableContexts": EXPECTED_P3_SAME_PANEL_PARENT_COMPARABLE_COUNT,
            "realizedPhenotypesAmongP3Contexts": EXPECTED_REALIZED_PHENOTYPE_COUNT,
            "childrenWithP1P2Backfill": EXPECTED_MULTIPANEL_CHILD_COUNT,
            "exactParentComparableP1P2Cases": EXPECTED_P1_P2_PARENT_COMPARABLE_COUNT,
        },
        "selectionRules": {
            "cohort": "all exact accepted directional_event_insert contexts; no performance-based exclusion",
            "m5Canary": {
                "rule": "lexicographically first componentContextIdentity across the 41 frozen contexts",
                "selectedComponentContextIdentity": m5_context["componentContextIdentity"],
            },
            "m15Canary": {
                "rule": "lexicographically first componentContextIdentity among M15 frozen contexts",
                "selectedComponentContextIdentity": m15_contexts[0]["componentContextIdentity"],
            },
        },
        "contexts": contexts,
        "valueScope": "identity_source_and_configuration_only",
    }
    reject_outcome_fields(manifest)
    manifest["manifestCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(manifest))
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v1-context-identities", type=Path, required=True)
    parser.add_argument("--v38-evaluated-members", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite existing output: {args.output}")
    manifest = build_manifest(
        args.v1_context_identities.resolve(),
        args.v38_evaluated_members.resolve(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "contexts": len(manifest["contexts"]),
                "manifestCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
