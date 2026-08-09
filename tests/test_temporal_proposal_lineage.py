from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_proposal_lineage_artifact import (
    PROPOSAL_LINEAGE_SOURCE_SCHEMA,
    build_proposal_lineage_artifact,
    materialize_completed_generation_lineage,
    materialize_proposal_lineage_artifact,
    seal_proposal_lineage_source,
    write_proposal_lineage_unavailable,
    write_proposal_lineage_source,
)
from autoresearch.temporal_proposal_lineage import (
    PROPOSAL_LINEAGE_REPORT_SCHEMA,
    build_proposal_lineage_report,
    seal_proposal_lineage_inputs,
    verify_proposal_lineage_report,
)


def _sha(label: str) -> str:
    return canonical_sha256({"fixture": label})


def _evidence(label: str = "shared") -> dict:
    material = {
        "schemaVersion": "temporal_proposal_lineage_evidence_identity_v1",
        "panelWindowSetSha256": _sha("panel:" + label),
        "costViewSha256": _sha("cost:" + label),
        "observationStreamSetSha256": _sha("streams:" + label),
    }
    return {**material, "canonicalEvidenceIdentitySha256": canonical_sha256(material)}


def _observed(label: str, *, activations: int = 1, positions: int = 1, evidence: str = "shared") -> dict:
    return {
        "attributionSha256": _sha("attribution:" + label),
        "evidenceIdentity": _evidence(evidence),
        "guardEvaluatedCount": 11,
        "guardTrueCount": 3,
        "transitionSelectedCount": 2,
        "priorityShadowedCount": 1,
        "actionScheduledCount": activations,
        "actionAppliedCount": activations,
        "actionRejectedCount": 0,
        "actionCanceledCount": 0,
        "activationCount": activations,
        "acceptedIntentOrEffectCount": activations,
        "rejectedIntentOrEffectCount": 0,
        "positionChangeCount": positions,
        "tradeCloseCount": positions,
        "neverActivated": False,
    }


def _retention(label: str, outcome: str = "retained") -> dict:
    value = {"outcome": outcome, "retentionEvidenceSha256": _sha("retention:" + label)}
    if outcome == "retained":
        value["archiveMemberIdentitySha256"] = _sha("archive:" + label)
    return value


def _entry(
    candidate_id: str,
    *,
    generation: int,
    parents: tuple[dict, ...] = (),
    operator: str | None = None,
    behavior: str = "same",
    observed: dict | None = None,
    retention: dict | None = None,
    evidence: str = "shared",
) -> dict:
    parent_ids = [parent["candidateId"] for parent in parents]
    value = {
        "candidateId": candidate_id,
        "candidateIdentitySha256": _sha("candidate:" + candidate_id),
        "programSha256": _sha("program:" + candidate_id),
        "generationIndex": generation,
        "originKind": "offspring" if parents else "immigrant",
        "parentCandidateIds": parent_ids,
        "parentProgramSha256s": [parent["programSha256"] for parent in parents],
        "operator": None if not parents else {
            "operatorId": operator or "resource_mutation_v1",
            "planSha256": _sha("plan:" + candidate_id),
            "applicationSha256": _sha("application:" + candidate_id),
            "parentCandidateIds": parent_ids,
            "semanticDelta": [{"operation": "replace_indicator", "before": "A", "after": candidate_id}],
        },
        "observedExecution": observed or _observed(candidate_id, evidence=evidence),
        "realizedBehavior": {"identitySha256": _sha("behavior:" + behavior), "evidenceIdentity": _evidence(evidence)},
        "archiveRetention": retention or _retention(candidate_id),
    }
    return value


def test_single_parent_passenger_and_measured_effects_are_explicit_not_selection_inputs() -> None:
    parent = _entry("parent", generation=0, behavior="same", observed=_observed("parent"), retention=_retention("parent"))
    passenger = _entry("passenger", generation=1, parents=(parent,), behavior="same", observed=_observed("passenger"), retention=_retention("passenger"))
    effect = _entry("effect", generation=1, parents=(parent,), behavior="changed", observed=_observed("effect", activations=2, positions=2), retention=_retention("effect", "not_retained"))
    sealed = seal_proposal_lineage_inputs([effect, passenger, parent])
    report = build_proposal_lineage_report(sealed)

    assert report["schemaVersion"] == PROPOSAL_LINEAGE_REPORT_SCHEMA
    assert [row["candidateId"] for row in report["records"]] == ["effect", "parent", "passenger"]
    rows = {row["candidateId"]: row for row in report["records"]}
    assert rows["passenger"]["comparison"] == {
        "parentCount": 1,
        "parentCandidateId": "parent",
        "parentEvidenceSources": ["cohort"],
        "evidenceComparable": True,
        "observedExecutionChanged": False,
        "realizedBehaviorChanged": False,
        "archiveRetentionChanged": False,
        "passengerMutation": True,
        "classification": "passenger_candidate",
    }
    assert rows["effect"]["comparison"]["classification"] == "measured_effect"
    assert rows["effect"]["comparison"]["observedExecutionChanged"] is True
    assert rows["effect"]["comparison"]["realizedBehaviorChanged"] is True
    assert rows["effect"]["comparison"]["archiveRetentionChanged"] is True
    assert report["summary"]["passengerCandidateCount"] == 1
    assert report["selectionImpact"] == "none_observation_only"
    assert verify_proposal_lineage_report(sealed, report)["allChecksPassed"] is True


def test_multi_parent_is_reported_as_non_comparable_not_misattributed_as_passenger() -> None:
    left = _entry("left", generation=0, behavior="left")
    right = _entry("right", generation=0, behavior="right")
    child = _entry("cross", generation=1, parents=(left, right), operator="motif_crossover_v1", behavior="cross")
    report = build_proposal_lineage_report(seal_proposal_lineage_inputs([child, right, left]))
    comparison = next(row["comparison"] for row in report["records"] if row["candidateId"] == "cross")
    assert comparison["classification"] == "not_assessable_multi_parent"
    assert comparison["passengerMutation"] is None
    assert report["summary"]["multiParentNotAssessableCount"] == 1


def test_different_rotating_evidence_is_never_attributed_to_the_mutation_even_with_identical_counts() -> None:
    parent = _entry("parent", generation=0, behavior="same", observed=_observed("parent", evidence="panel_a"), evidence="panel_a")
    child = _entry("child", generation=1, parents=(parent,), behavior="same", observed=_observed("child", evidence="panel_b"), evidence="panel_b")
    row = next(
        item for item in build_proposal_lineage_report(seal_proposal_lineage_inputs([parent, child]))["records"]
        if item["candidateId"] == "child"
    )
    assert row["comparison"] == {
        "parentCount": 1,
        "parentCandidateId": "parent",
        "parentEvidenceSources": ["cohort"],
        "evidenceComparable": False,
        "observedExecutionChanged": None,
        "realizedBehaviorChanged": None,
        "archiveRetentionChanged": None,
        "passengerMutation": None,
        "classification": "not_assessable_evidence_changed",
    }


def test_stale_evidence_identity_is_rejected_and_external_parent_evidence_is_explicitly_supported() -> None:
    parent = _entry("parent", generation=0)
    stale = copy.deepcopy(parent)
    stale["observedExecution"]["evidenceIdentity"]["canonicalEvidenceIdentitySha256"] = _sha("stale-evidence")
    with pytest.raises(TemporalDiscoveryContractError, match="canonical evidence identity is stale"):
        seal_proposal_lineage_inputs([stale])

    child = _entry("child", generation=1, parents=(parent,), behavior="changed")
    external_parent = {
        key: parent[key]
        for key in (
            "candidateId", "candidateIdentitySha256", "programSha256", "generationIndex",
            "observedExecution", "realizedBehavior", "archiveRetention",
        )
    }
    report = build_proposal_lineage_report(
        seal_proposal_lineage_inputs([child], external_parent_evidence=[external_parent])
    )
    row = report["records"][0]
    assert row["comparison"]["parentEvidenceSources"] == ["external_parent_evidence"]
    assert report["summary"]["externalParentEvidenceCount"] == 1


def test_sealing_rejects_stale_parent_contracts_cycles_and_invalid_immigrant_operator() -> None:
    parent = _entry("parent", generation=0)
    child = _entry("child", generation=1, parents=(parent,))
    stale = copy.deepcopy(child)
    stale["parentProgramSha256s"] = [_sha("stale")]
    with pytest.raises(TemporalDiscoveryContractError, match="parent program identity is stale"):
        seal_proposal_lineage_inputs([parent, stale])

    cyclic = copy.deepcopy(parent)
    cyclic["candidateId"] = "cycle"
    cyclic["candidateIdentitySha256"] = _sha("candidate:cycle")
    cyclic["programSha256"] = _sha("program:cycle")
    cyclic["generationIndex"] = 0
    cyclic["originKind"] = "offspring"
    cyclic["parentCandidateIds"] = ["cycle"]
    cyclic["parentProgramSha256s"] = [cyclic["programSha256"]]
    cyclic["operator"] = {"operatorId": "test", "planSha256": _sha("p"), "applicationSha256": _sha("a"), "parentCandidateIds": ["cycle"], "semanticDelta": [{"operation": "self"}]}
    with pytest.raises(TemporalDiscoveryContractError, match="self-reference"):
        seal_proposal_lineage_inputs([cyclic])

    immigrant = _entry("immigrant", generation=0)
    immigrant["operator"] = {"operatorId": "wrong", "planSha256": _sha("p2"), "applicationSha256": _sha("a2"), "parentCandidateIds": [], "semanticDelta": [{"operation": "wrong"}]}
    with pytest.raises(TemporalDiscoveryContractError, match="immigrant proposal"):
        seal_proposal_lineage_inputs([immigrant])


def test_input_and_report_tamper_and_noncanonical_order_fail_closed() -> None:
    parent = _entry("parent", generation=0)
    child = _entry("child", generation=1, parents=(parent,))
    sealed = seal_proposal_lineage_inputs([child, parent])
    report = build_proposal_lineage_report(sealed)

    changed_input = copy.deepcopy(sealed)
    changed_input["records"][0]["observedExecution"]["counters"]["activationCount"] += 1
    with pytest.raises(TemporalDiscoveryContractError, match="stale or tampered"):
        build_proposal_lineage_report(changed_input)

    changed_report = copy.deepcopy(report)
    changed_report["summary"]["passengerCandidateCount"] = 99
    with pytest.raises(TemporalDiscoveryContractError, match="stale or tampered"):
        verify_proposal_lineage_report(sealed, changed_report)

    noncanonical = copy.deepcopy(sealed)
    noncanonical["records"] = list(reversed(noncanonical["records"]))
    noncanonical["inputSha256"] = canonical_sha256({"schemaVersion": noncanonical["schemaVersion"], "records": noncanonical["records"], "externalParentEvidence": noncanonical["externalParentEvidence"]})
    with pytest.raises(TemporalDiscoveryContractError, match="not canonical"):
        build_proposal_lineage_report(noncanonical)


def test_never_activated_evidence_cannot_claim_execution_or_trade_side_effects() -> None:
    parent = _entry("parent", generation=0)
    child = _entry("child", generation=1, parents=(parent,))
    child["observedExecution"] = {**_observed("idle", activations=1), "neverActivated": True}
    with pytest.raises(TemporalDiscoveryContractError, match="never-activated"):
        seal_proposal_lineage_inputs([parent, child])


def _raw_file_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _lineage_source(root: Path, entries: list[dict]) -> dict:
    provenance = root / "proposal" / "generation-journal.json"
    provenance.parent.mkdir(parents=True, exist_ok=True)
    provenance.write_text(json.dumps({"sealed": "completed-generation-fixture"}), encoding="utf-8")
    return {
        "schemaVersion": PROPOSAL_LINEAGE_SOURCE_SCHEMA,
        "campaignId": "fresh-v5",
        "completedGenerationIndex": max(item["generationIndex"] for item in entries),
        "sourceArtifacts": [
            {"relativePath": "proposal/generation-journal.json", "sha256": _raw_file_sha(provenance)}
        ],
        "entries": entries,
        "externalParentEvidence": [],
    }


def test_post_generation_sidecar_is_deterministic_idempotent_and_binds_full_contract(tmp_path: Path) -> None:
    parent = _entry("parent", generation=0)
    child = _entry("child", generation=1, parents=(parent,), behavior="changed")
    source = _lineage_source(tmp_path, [child, parent])

    artifact = materialize_proposal_lineage_artifact(
        source, generation_root=tmp_path, output_root=tmp_path / "native-finalization" / "proposal-lineage"
    )
    replay = materialize_proposal_lineage_artifact(
        source, generation_root=tmp_path, output_root=tmp_path / "native-finalization" / "proposal-lineage"
    )
    assert artifact == replay
    reordered = _lineage_source(tmp_path, [parent, child])
    assert build_proposal_lineage_artifact(reordered)["manifest"] == artifact["manifest"]
    assert [item["candidateId"] for item in artifact["report"]["records"]] == ["child", "parent"]
    assert artifact["input"]["inputSha256"] == artifact["manifest"]["inputSha256"]
    assert artifact["report"]["reportSha256"] == artifact["manifest"]["reportSha256"]
    assert artifact["report"]["records"][0]["operator"]["applicationSha256"] == child["operator"]["applicationSha256"]
    assert artifact["report"]["records"][0]["observedExecution"]["evidenceIdentity"] == _evidence()
    assert set(path.name for path in (tmp_path / "native-finalization" / "proposal-lineage").iterdir()) == {
        "proposal-lineage-source.json",
        "proposal-lineage-input.json",
        "proposal-lineage-report.json",
        "proposal-lineage-artifact.json",
    }


def test_post_generation_sidecar_rejects_stale_provenance_and_tampered_restart(tmp_path: Path) -> None:
    parent = _entry("parent", generation=0)
    source = _lineage_source(tmp_path, [parent])
    output = tmp_path / "native-finalization" / "proposal-lineage"
    materialize_proposal_lineage_artifact(source, generation_root=tmp_path, output_root=output)

    (tmp_path / "proposal" / "generation-journal.json").write_text('{"sealed":"tampered"}', encoding="utf-8")
    with pytest.raises(TemporalDiscoveryContractError, match="source artifact identity is stale"):
        materialize_proposal_lineage_artifact(source, generation_root=tmp_path, output_root=output)

    # Restore the exact source artifact, then prove that a restart will not
    # overwrite an already-present but non-identical sidecar.
    (tmp_path / "proposal" / "generation-journal.json").write_text(
        json.dumps({"sealed": "completed-generation-fixture"}), encoding="utf-8"
    )
    report_path = output / "proposal-lineage-report.json"
    report_path.write_text('{"tampered":true}', encoding="utf-8")
    with pytest.raises(TemporalDiscoveryContractError, match="immutable replay"):
        materialize_proposal_lineage_artifact(source, generation_root=tmp_path, output_root=output)


def test_source_identity_and_legacy_generation_fail_closed_without_writes(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy-generation"
    legacy.mkdir()
    assert materialize_completed_generation_lineage(legacy) is None
    assert not (legacy / "native-finalization").exists()

    parent = _entry("parent", generation=0)
    source = _lineage_source(tmp_path, [parent])
    sealed = seal_proposal_lineage_source(source)
    stale = copy.deepcopy(sealed)
    stale["entries"][0]["archiveRetention"]["outcome"] = "not_retained"
    with pytest.raises(TemporalDiscoveryContractError, match="source identity is stale"):
        build_proposal_lineage_artifact(stale)


def test_explicit_completed_generation_hook_reads_source_sidecar_without_touching_selection(tmp_path: Path) -> None:
    parent = _entry("parent", generation=0)
    source = _lineage_source(tmp_path, [parent])
    sealed_source = write_proposal_lineage_source(source, generation_root=tmp_path)
    assert sealed_source["sourceSha256"] == seal_proposal_lineage_source(source)["sourceSha256"]

    artifact = materialize_completed_generation_lineage(tmp_path)
    assert artifact is not None
    assert artifact["manifest"]["entryCount"] == 1
    assert not (tmp_path / "proposal" / "population.json").exists()


def test_unavailable_marker_is_sealed_and_never_becomes_a_partial_lineage_report(tmp_path: Path) -> None:
    parent = _entry("parent", generation=0)
    source = _lineage_source(tmp_path, [parent])
    marker = write_proposal_lineage_unavailable(
        generation_root=tmp_path,
        campaign_id="fresh-v5",
        completed_generation_index=0,
        reasons=[
            "retention_evidence_not_sealed",
            "observed_execution_attribution_not_sealed",
            "canonical_evidence_components_not_sealed",
        ],
        source_artifacts=source["sourceArtifacts"],
    )
    assert marker["reasons"] == [
        "canonical_evidence_components_not_sealed",
        "observed_execution_attribution_not_sealed",
        "retention_evidence_not_sealed",
    ]
    assert marker["lineageUnavailableSha256"] == canonical_sha256(
        {key: value for key, value in marker.items() if key != "lineageUnavailableSha256"}
    )
    replay = write_proposal_lineage_unavailable(
        generation_root=tmp_path,
        campaign_id="fresh-v5",
        completed_generation_index=0,
        reasons=list(reversed(marker["reasons"])),
        source_artifacts=source["sourceArtifacts"],
    )
    assert replay == marker
