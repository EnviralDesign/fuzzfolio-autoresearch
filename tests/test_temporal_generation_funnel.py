from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from autoresearch.temporal_generation_funnel import (
    GenerationFunnelContractError,
    build_generation_funnel_artifact,
    proposal_attempt_from_journal_entry,
    proposal_candidate_from_journal_entry,
    supervisor_funnel_snapshot,
    write_generation_funnel_artifact,
)


RAW = "sha256:" + "a" * 64
RESOLVED = "sha256:" + "b" * 64
PROGRAM = "sha256:" + "c" * 64
VALIDATION = "sha256:" + "d" * 64
VALIDATION_ID = "sha256:" + "e" * 64
EVIDENCE = "sha256:" + "f" * 64
ARCHIVE = "sha256:" + "1" * 64
RESULT_A = "sha256:" + "2" * 64
RESULT_B = "sha256:" + "3" * 64
ACTIVATION = "sha256:" + "4" * 64
ATTEMPT = "sha256:" + "5" * 64


def _identity(candidate_id: str, **overrides: str) -> dict[str, str]:
    value = {
        "candidateId": candidate_id,
        "rawSourceProfileSha256": RAW,
        "resolvedProfileSha256": RESOLVED,
        "programSha256": PROGRAM,
        "validationReportSha256": VALIDATION,
        "validationIdentitySha256": VALIDATION_ID,
        "canonicalEvidenceIdentitySha256": EVIDENCE,
        "archiveMemberIdentitySha256": ARCHIVE,
    }
    value.update(overrides)
    return value


def _inputs(candidate_id: str = "qd_alpha") -> dict:
    proposal = _identity(candidate_id)
    proposal.update(
        {
            "directionMode": "long",
            "mutationTrace": [
                {"operatorId": "motif_splice", "motifId": "pullback_reclaim"}
            ],
        }
    )
    for name in (
        "resolvedProfileSha256",
        "programSha256",
        "validationReportSha256",
        "validationIdentitySha256",
        "canonicalEvidenceIdentitySha256",
        "archiveMemberIdentitySha256",
    ):
        proposal.pop(name)
    base = _identity(candidate_id)
    return {
        "proposal_attempt_ledger": [
            {
                "attemptIdentitySha256": ATTEMPT,
                "proposalOrdinal": 0,
                "originKind": "structural_offspring",
                "disposition": "accepted",
                "candidateId": candidate_id,
                "rawSourceProfileSha256": RAW,
            }
        ],
        "proposal_journal": [proposal],
        "static_reachability_records": [
            {"candidateId": candidate_id, "outcome": "reachable", "rawSourceProfileSha256": RAW}
        ],
        "native_validation_records": [{**base, "outcome": "valid"}],
        "admission_records": [{**base, "outcome": "admitted"}],
        "synthetic_evidence_records": [{**base, "outcome": "passed"}],
        "evaluation_plans": [
            {**base, "outcome": "evaluated", "expectedWindowIds": ["w1", "w2"]}
        ],
        "evaluation_results": [
            {**base, "windowId": "w1", "resultSha256": RESULT_A},
            {**base, "windowId": "w2", "resultSha256": RESULT_B},
        ],
        "activation_quality_records": [
            {
                **base,
                "outcome": "recorded",
                "qualityDisposition": "eligible",
                "activationIdentitySha256": ACTIVATION,
            }
        ],
        "archive_retention_records": [{**base, "outcome": "retained"}],
        "proposal_accounting": {
            "dispositionCounts": {"accepted": 1},
            "originProposalCounts": {"structural_offspring": 1},
        },
    }


def _build(inputs: dict) -> dict:
    return build_generation_funnel_artifact(**inputs)


def test_happy_path_builds_immutable_identity_bound_funnel(tmp_path: Path) -> None:
    artifact = _build(_inputs())
    candidate = artifact["candidates"][0]
    assert candidate["terminalDisposition"] == "retained"
    assert candidate["stages"]["syntheticEvidence"]["outcome"] == "passed"
    assert artifact["stageCounts"]["proposed:proposed"] == 1
    assert artifact["stageCounts"]["nativeValid:valid"] == 1
    assert artifact["operatorBreakdown"] == {"motif_splice": 1}
    assert artifact["motifBreakdown"] == {"pullback_reclaim": 1}
    assert artifact["directionBreakdown"] == {"long": 1}
    target = tmp_path / "funnel.json"
    write_generation_funnel_artifact(target, artifact)
    write_generation_funnel_artifact(target, artifact)
    assert json.loads(target.read_text(encoding="utf-8"))["artifactSha256"] == artifact["artifactSha256"]


def test_static_and_native_rejections_are_preserved_as_terminal_candidates() -> None:
    static = _inputs("qd_static")
    static["static_reachability_records"] = [
        {"candidateId": "qd_static", "rawSourceProfileSha256": RAW, "outcome": "rejected", "reasons": ["dead_transition"]}
    ]
    static["proposal_attempt_ledger"][0]["disposition"] = "static_reachability_rejected"
    static["proposal_accounting"]["dispositionCounts"] = {"static_reachability_rejected": 1}
    for name in (
        "native_validation_records", "admission_records", "synthetic_evidence_records",
        "evaluation_plans", "evaluation_results", "activation_quality_records", "archive_retention_records",
    ):
        static[name] = []
    artifact = _build(static)
    assert artifact["candidates"][0]["terminalDisposition"] == "static_reachability_rejected"
    assert artifact["candidates"][0]["stages"]["staticallyReachable"]["reasons"] == ["dead_transition"]

    native = _inputs("qd_native")
    native["native_validation_records"] = [
        {**_identity("qd_native"), "outcome": "rejected", "issueCodes": ["unknown_indicator"]}
    ]
    native["proposal_attempt_ledger"][0]["disposition"] = "native_validator_rejected"
    native["proposal_accounting"]["dispositionCounts"] = {"native_validator_rejected": 1}
    for name in (
        "admission_records", "synthetic_evidence_records", "evaluation_plans",
        "evaluation_results", "activation_quality_records", "archive_retention_records",
    ):
        native[name] = []
    artifact = _build(native)
    assert artifact["candidates"][0]["terminalDisposition"] == "native_validation_rejected"


def test_missing_optional_synthetic_evidence_is_truthfully_unmeasured() -> None:
    inputs = _inputs()
    inputs["synthetic_evidence_records"] = []
    artifact = _build(inputs)
    assert artifact["candidates"][0]["stages"]["syntheticEvidence"] == {"outcome": "unmeasured", "reasons": []}


def test_nonmaterialized_attempts_are_accounted_without_fake_candidates() -> None:
    artifact = build_generation_funnel_artifact(
        proposal_attempt_ledger=[
            {
                "attemptIdentitySha256": ATTEMPT,
                "proposalOrdinal": 0,
                "originKind": "structural_offspring",
                "disposition": "no_eligible_operator",
            }
        ],
        proposal_journal=[],
        static_reachability_records=[],
        native_validation_records=[],
        admission_records=[],
        synthetic_evidence_records=[],
        evaluation_plans=[],
        evaluation_results=[],
        activation_quality_records=[],
        archive_retention_records=[],
        proposal_accounting={
            "dispositionCounts": {"no_eligible_operator": 1},
            "originProposalCounts": {"structural_offspring": 1},
        },
    )
    assert artifact["candidateCount"] == 0
    assert artifact["attemptLedger"]["nonMaterializedAttemptCount"] == 1
    assert artifact["attemptToMaterializedAttrition"]["notMaterialized"] == 1


def test_attempt_accounting_and_candidate_crosschecks_are_exact() -> None:
    inputs = _inputs()
    inputs["proposal_accounting"]["dispositionCounts"] = {"accepted": 2}
    with pytest.raises(GenerationFunnelContractError, match="does not exactly match"):
        _build(inputs)

    inputs = _inputs()
    inputs["proposal_attempt_ledger"][0]["candidateId"] = "qd_substituted"
    with pytest.raises(GenerationFunnelContractError, match="no matching materialized attempt"):
        _build(inputs)


def test_identity_substitution_is_rejected_across_stage_join() -> None:
    inputs = _inputs()
    inputs["evaluation_results"][1]["programSha256"] = "sha256:" + "9" * 64
    with pytest.raises(GenerationFunnelContractError, match="programSha256 differs"):
        _build(inputs)


def test_duplicate_rows_and_unknown_rows_are_rejected() -> None:
    inputs = _inputs()
    inputs["native_validation_records"].append(dict(inputs["native_validation_records"][0]))
    with pytest.raises(GenerationFunnelContractError, match="duplicate native validation"):
        _build(inputs)

    inputs = _inputs()
    inputs["synthetic_evidence_records"].append({**_identity("qd_unknown"), "outcome": "passed"})
    with pytest.raises(GenerationFunnelContractError, match="unknown candidates"):
        _build(inputs)


def test_partial_evaluation_is_explicit_and_cannot_have_archive_decisions() -> None:
    inputs = _inputs()
    inputs["evaluation_plans"][0]["outcome"] = "partial"
    inputs["evaluation_results"] = inputs["evaluation_results"][:1]
    inputs["activation_quality_records"] = []
    inputs["archive_retention_records"] = []
    artifact = _build(inputs)
    row = artifact["candidates"][0]
    assert row["terminalDisposition"] == "evaluation_partial"
    assert row["stages"]["evaluated"]["observedWindowIds"] == ["w1"]

    inputs = _inputs()
    inputs["evaluation_plans"][0]["outcome"] = "partial"
    inputs["evaluation_results"] = inputs["evaluation_results"][:1]
    inputs["activation_quality_records"] = []
    with pytest.raises(GenerationFunnelContractError, match="extra post-terminal"):
        _build(inputs)


def test_duplicate_admission_is_counted_as_an_explicit_stage_outcome() -> None:
    inputs = _inputs()
    inputs["admission_records"][0]["outcome"] = "rejected_duplicate"
    inputs["proposal_attempt_ledger"][0]["disposition"] = "duplicate_candidate_identity"
    inputs["proposal_accounting"]["dispositionCounts"] = {"duplicate_candidate_identity": 1}
    for name in (
        "synthetic_evidence_records", "evaluation_plans", "evaluation_results",
        "activation_quality_records", "archive_retention_records",
    ):
        inputs[name] = []
    artifact = _build(inputs)
    assert artifact["candidates"][0]["terminalDisposition"] == "duplicate_rejected"
    assert artifact["stageCounts"]["uniqueAdmitted:rejected_duplicate"] == 1


def test_negative_activation_is_an_explicit_terminal_without_archive_inference() -> None:
    inputs = _inputs()
    inputs["activation_quality_records"][0]["outcome"] = "not_activated"
    inputs["activation_quality_records"][0]["qualityDisposition"] = "not_eligible"
    inputs["archive_retention_records"] = []
    artifact = _build(inputs)
    assert artifact["candidates"][0]["terminalDisposition"] == "activation_not_activated"
    assert artifact["stageCounts"]["activationQuality:not_activated"] == 1


def test_archive_retention_identity_and_not_retained_decision_are_consistent() -> None:
    inputs = _inputs()
    inputs["archive_retention_records"][0].pop("archiveMemberIdentitySha256")
    with pytest.raises(GenerationFunnelContractError, match="lacks archive member identity"):
        _build(inputs)

    inputs = _inputs()
    inputs["archive_retention_records"][0]["outcome"] = "not_retained"
    inputs["archive_retention_records"][0].pop("archiveMemberIdentitySha256")
    artifact = _build(inputs)
    assert artifact["candidates"][0]["terminalDisposition"] == "not_retained"


def test_reordered_inputs_have_the_same_artifact_identity() -> None:
    forward = _build(_inputs())
    reverse_inputs = _inputs()
    for name in (
        "proposal_attempt_ledger",
        "proposal_journal", "static_reachability_records", "native_validation_records",
        "admission_records", "synthetic_evidence_records", "evaluation_plans",
        "evaluation_results", "activation_quality_records", "archive_retention_records",
    ):
        reverse_inputs[name] = list(reversed(reverse_inputs[name]))
    assert forward["artifactSha256"] == _build(reverse_inputs)["artifactSha256"]


def test_hash_seed_independence_for_the_same_inputs() -> None:
    root = Path(__file__).resolve().parents[1]
    program = """
from autoresearch.temporal_generation_funnel import build_generation_funnel_artifact
raw = 'sha256:' + 'a'*64
base = {'candidateId':'qd_seed','rawSourceProfileSha256':raw,'resolvedProfileSha256':'sha256:'+'b'*64,'programSha256':'sha256:'+'c'*64,'validationReportSha256':'sha256:'+'d'*64,'validationIdentitySha256':'sha256:'+'e'*64,'canonicalEvidenceIdentitySha256':'sha256:'+'f'*64,'archiveMemberIdentitySha256':'sha256:'+'1'*64}
p = {'candidateId':'qd_seed','rawSourceProfileSha256':raw,'directionMode':'short'}
attempt = {'attemptIdentitySha256':'sha256:'+'5'*64,'proposalOrdinal':0,'originKind':'structural_offspring','disposition':'accepted','candidateId':'qd_seed','rawSourceProfileSha256':raw}
accounting = {'dispositionCounts':{'accepted':1},'originProposalCounts':{'structural_offspring':1}}
kwargs = dict(proposal_attempt_ledger=[attempt], proposal_journal=[p], static_reachability_records=[{'candidateId':'qd_seed','rawSourceProfileSha256':raw,'outcome':'reachable'}], native_validation_records=[dict(base,outcome='valid')], admission_records=[dict(base,outcome='admitted')], synthetic_evidence_records=[], evaluation_plans=[dict(base,outcome='evaluated',expectedWindowIds=['a'])], evaluation_results=[dict(base,windowId='a',resultSha256='sha256:'+'2'*64)], activation_quality_records=[dict(base,outcome='recorded',qualityDisposition='eligible')], archive_retention_records=[dict(base,outcome='not_retained')], proposal_accounting=accounting)
print(build_generation_funnel_artifact(**kwargs)['artifactSha256'])
"""
    outputs = []
    for seed in ("1", "777"):
        environment = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=str(root))
        outputs.append(subprocess.check_output([sys.executable, "-c", program], text=True, env=environment).strip())
    assert outputs[0] == outputs[1]


def test_supervisor_snapshot_is_narrow_and_content_bound() -> None:
    artifact = _build(_inputs())
    snapshot = supervisor_funnel_snapshot(artifact)
    assert snapshot["funnelArtifactSha256"] == artifact["artifactSha256"]
    assert snapshot["candidateTerminals"] == [{"candidateId": "qd_alpha", "terminalDisposition": "retained"}]


def test_uncoupled_journal_adapters_preserve_candidate_free_attempts() -> None:
    empty = proposal_attempt_from_journal_entry(
        {
            "entrySha256": ATTEMPT,
            "proposalOrdinal": 0,
            "originKind": "structural_offspring",
            "disposition": "no_eligible_operator",
            "proposal": {"rawSourceProfileSha256": None},
        }
    )
    assert "candidateId" not in empty
    assert proposal_candidate_from_journal_entry(
        {
            "entrySha256": ATTEMPT,
            "proposalOrdinal": 0,
            "originKind": "structural_offspring",
            "disposition": "no_eligible_operator",
            "proposal": {},
        }
    ) is None
