"""Immutable, identity-bound accounting for a temporal generation funnel.

This module intentionally does *not* generate, validate, execute, or select a
candidate.  It reduces records produced by those independently-owned stages
into one auditable candidate ledger.  A stage is represented only when its
own record says so; passing a later stage never manufactures an earlier stage.

The narrow future supervisor seam is :func:`supervisor_funnel_snapshot`.  The
supervisor can publish the source records after its archive reduction and
consume that small immutable snapshot without this module taking ownership of
the evolution controller.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any


GENERATION_FUNNEL_SCHEMA = "temporal_generation_funnel_v1"
GENERATION_FUNNEL_POLICY_SCHEMA = "temporal_generation_funnel_completeness_v1"
SUPERVISOR_FUNNEL_SNAPSHOT_SCHEMA = "temporal_generation_funnel_supervisor_snapshot_v1"


class GenerationFunnelContractError(RuntimeError):
    """Raised when independently-produced records cannot form one truthful funnel."""


DEFAULT_COMPLETENESS_POLICY: dict[str, Any] = {
    "schemaVersion": GENERATION_FUNNEL_POLICY_SCHEMA,
    "policyName": "strict_identity_bound_generation_funnel",
    "proposal": "every source row must have a unique candidateId and raw source SHA",
    "staticReachability": "every proposal must have one explicit disposition",
    "nativeValidation": "statically reachable candidates require one explicit disposition",
    "uniqueAdmission": "native-valid candidates require one explicit disposition",
    "syntheticEvidence": {
        "required": False,
        "missingDisposition": "unmeasured",
        "rule": "absence is reported as unmeasured and never inferred from evaluation",
    },
    "evaluation": "uniquely admitted candidates require an explicit plan/disposition",
    "activationQuality": "evaluated candidates require one explicit activation/quality record",
    "archiveRetention": "quality-recorded candidates require one explicit retained/not_retained decision; quality-rejected candidates may record only non-promotable scheduled negative-novelty exploration retention",
    "identity": "records may omit unavailable fields but may never contradict an available binding",
    "attemptLedger": "every generation attempt is recorded independently; attempts without a materialized candidate remain attempt-only",
}


def canonical_sha256(value: Any) -> str:
    """Return the content identity of finite JSON material."""

    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise GenerationFunnelContractError("value is not finite canonical JSON") from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise GenerationFunnelContractError(f"{name} is not finite canonical JSON") from exc


def _string(value: Any, *, name: str, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise GenerationFunnelContractError(f"{name} is required")
        return None
    if not isinstance(value, str) or not value:
        raise GenerationFunnelContractError(f"{name} must be a non-empty string")
    return value


def _field(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    identity = row.get("identity")
    if isinstance(identity, Mapping):
        for name in names:
            if name in identity:
                return identity[name]
    return None


def _candidate_id(row: Mapping[str, Any], *, source: str) -> str:
    return _string(
        _field(row, "candidateId", "candidate_id"),
        name=f"{source}.candidateId",
        required=True,
    ) or ""


def _identity_from(row: Mapping[str, Any], *, source: str, proposal: bool = False) -> dict[str, str | None]:
    result = {
        "candidateId": _candidate_id(row, source=source),
        "rawSourceProfileSha256": _string(
            _field(row, "rawSourceProfileSha256", "sourceProfileSha256", "raw_source_profile_sha256"),
            name=f"{source}.rawSourceProfileSha256",
            required=proposal,
        ),
        "resolvedProfileSha256": _string(
            _field(row, "resolvedProfileSha256", "profileSnapshotSha256", "resolved_profile_sha256"),
            name=f"{source}.resolvedProfileSha256",
        ),
        "programSha256": _string(
            _field(row, "programSha256", "validatedProgramSha256", "program_sha256"),
            name=f"{source}.programSha256",
        ),
        "validationReportSha256": _string(
            _field(row, "validationReportSha256", "validation_report_sha256"),
            name=f"{source}.validationReportSha256",
        ),
        "validationIdentitySha256": _string(
            _field(row, "validationIdentitySha256", "authoredValidationBindingSha256", "validation_identity_sha256"),
            name=f"{source}.validationIdentitySha256",
        ),
        "canonicalEvidenceIdentitySha256": _string(
            _field(row, "canonicalEvidenceIdentitySha256", "canonical_evidence_identity_sha256", "evaluationIdentitySha256"),
            name=f"{source}.canonicalEvidenceIdentitySha256",
        ),
        "archiveMemberIdentitySha256": _string(
            _field(row, "archiveMemberIdentitySha256", "archive_member_identity_sha256", "memberIdentitySha256"),
            name=f"{source}.archiveMemberIdentitySha256",
        ),
    }
    return result


def _outcome(row: Mapping[str, Any], *, source: str, allowed: set[str]) -> str:
    outcome = _string(_field(row, "outcome", "disposition", "status"), name=f"{source}.outcome", required=True)
    if outcome not in allowed:
        raise GenerationFunnelContractError(
            f"{source}.outcome {outcome!r} is not one of {sorted(allowed)!r}"
        )
    return outcome or ""


def _reasons(row: Mapping[str, Any], *, source: str) -> list[str]:
    value = _field(row, "reasons", "reasonCodes", "issueCodes", "reason")
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise GenerationFunnelContractError(f"{source}.reasons must be a string array")
    values = [_string(item, name=f"{source}.reason") for item in value]
    return sorted(set(item for item in values if item is not None))


def _index_single(
    rows: Sequence[Mapping[str, Any]], *, source: str, proposal: bool = False
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise GenerationFunnelContractError(f"{source} row must be an object")
        row = _clone(raw, name=source)
        candidate_id = _candidate_id(row, source=source)
        if candidate_id in indexed:
            raise GenerationFunnelContractError(f"duplicate {source} record for {candidate_id}")
        # Validate every available identity field now, even if this row later
        # terminates the candidate.
        _identity_from(row, source=source, proposal=proposal)
        indexed[candidate_id] = row
    return indexed


def _assert_no_extra(index: Mapping[str, Any], proposals: Mapping[str, Any], *, source: str) -> None:
    extra = sorted(set(index) - set(proposals))
    if extra:
        raise GenerationFunnelContractError(f"{source} has records for unknown candidates: {extra!r}")


def _merge_identity(
    binding: dict[str, str | None], row: Mapping[str, Any], *, source: str, proposal: bool = False
) -> None:
    observed = _identity_from(row, source=source, proposal=proposal)
    if observed["candidateId"] != binding["candidateId"]:
        raise GenerationFunnelContractError(f"{source} candidateId cannot join candidate")
    for field, value in observed.items():
        if field == "candidateId" or value is None:
            continue
        existing = binding.get(field)
        if existing is not None and existing != value:
            raise GenerationFunnelContractError(
                f"identity mismatch for {binding['candidateId']}: {field} differs in {source}"
            )
        binding[field] = value


def _normalized_policy(policy: Mapping[str, Any] | None) -> dict[str, Any]:
    if policy is None:
        return _clone(DEFAULT_COMPLETENESS_POLICY, name="default completeness policy")
    supplied = _clone(policy, name="completeness policy")
    if supplied.get("schemaVersion") != GENERATION_FUNNEL_POLICY_SCHEMA:
        raise GenerationFunnelContractError("unsupported completeness policy schema")
    merged = _clone(DEFAULT_COMPLETENESS_POLICY, name="default completeness policy")
    merged.update(supplied)
    return merged


def _operator_motif_direction(proposal: Mapping[str, Any]) -> tuple[list[str], list[str], str]:
    operators: set[str] = set()
    motifs: set[str] = set()
    for step in proposal.get("structuralOperatorHistory") or proposal.get("mutationTrace") or []:
        if not isinstance(step, Mapping):
            continue
        operator = _field(step, "operatorId", "operator", "id")
        if isinstance(operator, str) and operator:
            operators.add(operator)
        motif = _field(step, "motifId", "motif", "motifType")
        if isinstance(motif, str) and motif:
            motifs.add(motif)
    direction = _field(proposal, "direction", "directionMode")
    profile = proposal.get("sourceProfile")
    if direction is None and isinstance(profile, Mapping):
        direction = _field(profile, "direction", "directionMode")
    return sorted(operators) or ["unclassified"], sorted(motifs) or ["unclassified"], str(direction or "unclassified")


def _stage_row(outcome: str, reasons: Sequence[str] = ()) -> dict[str, Any]:
    return {"outcome": outcome, "reasons": list(sorted(set(reasons)))}


def _required(index: Mapping[str, Any], candidate_id: str, *, stage: str) -> Mapping[str, Any]:
    row = index.get(candidate_id)
    if row is None:
        raise GenerationFunnelContractError(f"missing {stage} record for {candidate_id}")
    return row


def _evaluation_index(rows: Sequence[Mapping[str, Any]], proposals: Mapping[str, Any]) -> dict[str, dict[str, Mapping[str, Any]]]:
    indexed: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise GenerationFunnelContractError("evaluation result row must be an object")
        row = _clone(raw, name="evaluation result")
        candidate_id = _candidate_id(row, source="evaluation result")
        if candidate_id not in proposals:
            raise GenerationFunnelContractError(f"evaluation result has unknown candidate {candidate_id}")
        window_id = _string(_field(row, "windowId", "window_id"), name="evaluation result.windowId", required=True) or ""
        if window_id in indexed[candidate_id]:
            raise GenerationFunnelContractError(f"duplicate evaluation result for {(candidate_id, window_id)!r}")
        _identity_from(row, source="evaluation result")
        result_sha = _string(_field(row, "resultSha256", "artifactSha256", "result_sha256"), name="evaluation result.resultSha256", required=True)
        if result_sha is None:
            raise AssertionError("required result SHA vanished")
        indexed[candidate_id][window_id] = row
    return dict(indexed)


def _count_by(candidates: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for candidate in candidates:
        value = candidate[field]
        if isinstance(value, str):
            counter[value] += 1
        else:
            for item in value:
                counter[str(item)] += 1
    return dict(sorted(counter.items()))


def _attempt_identity(row: Mapping[str, Any]) -> str:
    return _string(
        _field(row, "attemptIdentitySha256", "entrySha256", "attemptSha256"),
        name="proposal attempt.attemptIdentitySha256",
        required=True,
    ) or ""


def _attempt_ordinal(row: Mapping[str, Any]) -> int:
    value = _field(row, "proposalOrdinal", "attemptOrdinal", "ordinal")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GenerationFunnelContractError("proposal attempt.proposalOrdinal must be a non-negative integer")
    return value


def _attempt_disposition(row: Mapping[str, Any]) -> str:
    return _string(
        _field(row, "disposition", "outcome", "status"),
        name="proposal attempt.disposition",
        required=True,
    ) or ""


def _attempt_candidate_identity(row: Mapping[str, Any]) -> tuple[str | None, str | None]:
    candidate_id = _field(row, "candidateId", "candidate_id")
    raw_sha = _field(
        row, "rawSourceProfileSha256", "sourceProfileSha256", "raw_source_profile_sha256"
    )
    if candidate_id is None and raw_sha is None:
        return None, None
    if candidate_id is None or raw_sha is None:
        raise GenerationFunnelContractError(
            "candidate-bearing proposal attempt must contain both candidateId and raw source SHA"
        )
    return (
        _string(candidate_id, name="proposal attempt.candidateId", required=True),
        _string(raw_sha, name="proposal attempt.rawSourceProfileSha256", required=True),
    )


def _build_attempt_ledger(
    rows: Sequence[Mapping[str, Any]], *, proposals: Mapping[str, Mapping[str, Any]], proposal_accounting: Mapping[str, Any]
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    identities: set[str] = set()
    ordinals: set[int] = set()
    candidate_attempts: dict[str, tuple[str, str]] = {}
    dispositions: Counter[str] = Counter()
    origins: Counter[str] = Counter()
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise GenerationFunnelContractError("proposal attempt row must be an object")
        row = _clone(raw, name="proposal attempt")
        identity = _attempt_identity(row)
        ordinal = _attempt_ordinal(row)
        disposition = _attempt_disposition(row)
        origin = _string(_field(row, "originKind", "origin_kind"), name="proposal attempt.originKind", required=True) or ""
        if identity in identities:
            raise GenerationFunnelContractError(f"duplicate proposal attempt identity {identity}")
        if ordinal in ordinals:
            raise GenerationFunnelContractError(f"duplicate proposal attempt ordinal {ordinal}")
        identities.add(identity)
        ordinals.add(ordinal)
        candidate_id, raw_sha = _attempt_candidate_identity(row)
        if candidate_id is not None:
            if candidate_id in candidate_attempts:
                raise GenerationFunnelContractError(f"candidate {candidate_id} appears in more than one proposal attempt")
            candidate_attempts[candidate_id] = (identity, raw_sha or "")
        attempts.append(
            {
                "proposalOrdinal": ordinal,
                "attemptIdentitySha256": identity,
                "originKind": origin,
                "disposition": disposition,
                **({"candidateId": candidate_id, "rawSourceProfileSha256": raw_sha} if candidate_id is not None else {}),
            }
        )
        dispositions[disposition] += 1
        origins[origin] += 1
    if not attempts:
        raise GenerationFunnelContractError("proposal attempt ledger must not be empty")
    if ordinals != set(range(len(attempts))):
        raise GenerationFunnelContractError("proposal attempt ledger has an ordinal gap")
    for candidate_id, proposal in proposals.items():
        attempted = candidate_attempts.get(candidate_id)
        if attempted is None:
            raise GenerationFunnelContractError(f"proposal candidate {candidate_id} has no matching materialized attempt")
        proposal_raw = _identity_from(proposal, source="proposal journal", proposal=True)["rawSourceProfileSha256"]
        if attempted[1] != proposal_raw:
            raise GenerationFunnelContractError(f"proposal attempt raw source SHA differs for {candidate_id}")
    extra = sorted(set(candidate_attempts) - set(proposals))
    if extra:
        raise GenerationFunnelContractError(f"proposal attempts materialize unknown candidates: {extra!r}")
    accounting = _clone(proposal_accounting, name="proposal accounting")
    disposition_counts = accounting.get("dispositionCounts")
    origin_counts = accounting.get("originProposalCounts")
    if not isinstance(disposition_counts, Mapping) or not isinstance(origin_counts, Mapping):
        raise GenerationFunnelContractError("proposal accounting must include dispositionCounts and originProposalCounts")
    normalized_dispositions = {str(key): value for key, value in disposition_counts.items()}
    normalized_origins = {str(key): value for key, value in origin_counts.items()}
    expected_dispositions = dict(sorted(dispositions.items()))
    expected_origins = dict(sorted(origins.items()))
    if normalized_dispositions != expected_dispositions:
        raise GenerationFunnelContractError("proposal accounting dispositionCounts does not exactly match attempt ledger")
    if normalized_origins != expected_origins:
        raise GenerationFunnelContractError("proposal accounting originProposalCounts does not exactly match attempt ledger")
    for mapping_name, mapping in (("dispositionCounts", normalized_dispositions), ("originProposalCounts", normalized_origins)):
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in mapping.values()):
            raise GenerationFunnelContractError(f"proposal accounting {mapping_name} must contain non-negative integers")
    ledger = {
        "attemptCount": len(attempts),
        "materializedCandidateCount": len(candidate_attempts),
        "nonMaterializedAttemptCount": len(attempts) - len(candidate_attempts),
        "attemptDispositionCounts": expected_dispositions,
        "attemptOriginCounts": expected_origins,
        "attempts": sorted(attempts, key=lambda item: item["proposalOrdinal"]),
    }
    ledger["attemptLedgerSha256"] = canonical_sha256(ledger)
    return ledger


def proposal_attempt_from_journal_entry(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Adapt one existing QD journal entry into the uncoupled attempt schema.

    This helper only reshapes a record; it does not read directories or import
    the QD controller.  Entries with no generated profile remain candidate-free.
    """

    if not isinstance(entry, Mapping):
        raise GenerationFunnelContractError("journal entry must be an object")
    proposal = entry.get("proposal") if isinstance(entry.get("proposal"), Mapping) else {}
    candidate = entry.get("candidate") if isinstance(entry.get("candidate"), Mapping) else {}
    candidate_id = candidate.get("candidateId") or proposal.get("candidateId")
    raw_sha = candidate.get("sourceProfileSha256") or proposal.get("rawSourceProfileSha256")
    output = {
        "attemptIdentitySha256": entry.get("entrySha256"),
        "proposalOrdinal": entry.get("proposalOrdinal"),
        "originKind": entry.get("originKind"),
        "disposition": entry.get("disposition"),
    }
    if candidate_id is not None or raw_sha is not None:
        output["candidateId"] = candidate_id
        output["rawSourceProfileSha256"] = raw_sha
    return output


def proposal_candidate_from_journal_entry(entry: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the candidate-bearing projection of one QD journal entry, if any."""

    attempt = proposal_attempt_from_journal_entry(entry)
    candidate_id, raw_sha = _attempt_candidate_identity(attempt)
    if candidate_id is None:
        return None
    candidate = entry.get("candidate") if isinstance(entry.get("candidate"), Mapping) else {}
    proposal = entry.get("proposal") if isinstance(entry.get("proposal"), Mapping) else {}
    output = {"candidateId": candidate_id, "rawSourceProfileSha256": raw_sha}
    for target, names in (
        ("resolvedProfileSha256", ("profileSnapshotSha256", "resolvedProfileSha256")),
        ("programSha256", ("programSha256", "validatedProgramSha256")),
        ("validationReportSha256", ("validationReportSha256",)),
        ("validationIdentitySha256", ("authoredValidationBindingSha256", "validationIdentitySha256")),
        ("canonicalEvidenceIdentitySha256", ("canonicalEvidenceIdentitySha256",)),
    ):
        value = _field(candidate, *names) or _field(proposal, *names)
        if value is not None:
            output[target] = value
    if isinstance(candidate.get("sourceProfile"), Mapping):
        output["sourceProfile"] = _clone(candidate["sourceProfile"], name="journal candidate source profile")
    return output


def _verified_g0_bootstrap_proof(row: Mapping[str, Any]) -> bool:
    proof = row.get("g0BootstrapProof")
    if not isinstance(proof, Mapping) or set(proof) != {"schemaVersion", "candidateId", "rawSourceProfileSha256", "constructionPoolIdentitySha256", "acceptedPoolSha256", "selectionSha256", "ledgerSha256", "constructionProposalOrdinal", "proposalEntrySha256", "nativeStaticProofSha256"}:
        return False
    if proof.get("schemaVersion") != "temporal_qd_g0_funnel_proof_v1":
        return False
    if isinstance(proof.get("constructionProposalOrdinal"), bool) or not isinstance(proof.get("constructionProposalOrdinal"), int):
        return False
    for key in ("rawSourceProfileSha256", "constructionPoolIdentitySha256", "acceptedPoolSha256", "selectionSha256", "ledgerSha256", "proposalEntrySha256", "nativeStaticProofSha256"):
        value = proof.get(key)
        if not isinstance(value, str) or not value.startswith("sha256:"):
            return False
    return True


def _g0_proof_authority(authority: Mapping[str, Any] | None) -> dict[str, Mapping[str, Any]]:
    if authority is None:
        return {}
    if not isinstance(authority, Mapping) or set(authority) != {"schemaVersion", "proofs", "authoritySha256"} or authority.get("schemaVersion") != "temporal_qd_g0_funnel_proof_authority_v1":
        raise GenerationFunnelContractError("G0 funnel proof authority schema is invalid")
    if authority.get("authoritySha256") != canonical_sha256({key: value for key, value in authority.items() if key != "authoritySha256"}):
        raise GenerationFunnelContractError("G0 funnel proof authority identity mismatch")
    result: dict[str, Mapping[str, Any]] = {}
    proofs = authority.get("proofs")
    if not isinstance(proofs, Sequence) or isinstance(proofs, (str, bytes)):
        raise GenerationFunnelContractError("G0 funnel proof authority rows are invalid")
    for proof in proofs:
        if not isinstance(proof, Mapping) or not _verified_g0_bootstrap_proof({"g0BootstrapProof": proof}):
            raise GenerationFunnelContractError("G0 funnel proof authority contains invalid proof")
        candidate_id, raw = proof.get("candidateId"), proof.get("rawSourceProfileSha256")
        if not isinstance(candidate_id, str) or not isinstance(raw, str) or not raw.startswith("sha256:") or candidate_id in result:
            raise GenerationFunnelContractError("G0 funnel proof authority candidate binding is invalid")
        result[candidate_id] = proof
    return result


def build_generation_funnel_artifact(
    *,
    proposal_attempt_ledger: Sequence[Mapping[str, Any]],
    proposal_journal: Sequence[Mapping[str, Any]],
    static_reachability_records: Sequence[Mapping[str, Any]],
    native_validation_records: Sequence[Mapping[str, Any]],
    admission_records: Sequence[Mapping[str, Any]],
    evaluation_plans: Sequence[Mapping[str, Any]],
    evaluation_results: Sequence[Mapping[str, Any]],
    activation_quality_records: Sequence[Mapping[str, Any]],
    archive_retention_records: Sequence[Mapping[str, Any]],
    synthetic_evidence_records: Sequence[Mapping[str, Any]] | None = None,
    proposal_accounting: Mapping[str, Any],
    completeness_policy: Mapping[str, Any] | None = None,
    g0_proof_authority: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Join independently authored stage records into one immutable artifact.

    ``proposal_attempt_ledger`` records every generator attempt, including
    attempts with no candidate/profile. ``proposal_journal`` is its
    candidate-bearing subset. All other collections are explicit stage
    evidence. ``synthetic_evidence`` is intentionally optional; absent records
    are represented as ``unmeasured``.
    """

    policy = _normalized_policy(completeness_policy)
    trusted_g0_proofs = _g0_proof_authority(g0_proof_authority)
    proposals = _index_single(proposal_journal, source="proposal journal", proposal=True)
    attempt_ledger = _build_attempt_ledger(
        proposal_attempt_ledger,
        proposals=proposals,
        proposal_accounting=proposal_accounting,
    )
    static_rows = _index_single(static_reachability_records, source="static reachability")
    native_rows = _index_single(native_validation_records, source="native validation")
    admission_rows = _index_single(admission_records, source="unique admission")
    plan_rows = _index_single(evaluation_plans, source="evaluation plan")
    activation_rows = _index_single(activation_quality_records, source="activation quality")
    archive_rows = _index_single(archive_retention_records, source="archive retention")
    synthetic_rows = _index_single(synthetic_evidence_records or [], source="synthetic evidence")
    for source, index in (
        ("static reachability", static_rows), ("native validation", native_rows),
        ("unique admission", admission_rows), ("evaluation plan", plan_rows),
        ("activation quality", activation_rows), ("archive retention", archive_rows),
        ("synthetic evidence", synthetic_rows),
    ):
        _assert_no_extra(index, proposals, source=source)
    result_rows = _evaluation_index(evaluation_results, proposals)

    candidates: list[dict[str, Any]] = []
    for candidate_id in sorted(proposals):
        proposal = proposals[candidate_id]
        binding = _identity_from(proposal, source="proposal journal", proposal=True)
        operators, motifs, direction = _operator_motif_direction(proposal)
        stages: dict[str, dict[str, Any]] = {"proposed": _stage_row("proposed")}
        terminal: str | None = None

        static = _required(static_rows, candidate_id, stage="static reachability")
        _merge_identity(binding, static, source="static reachability")
        static_outcome = _outcome(static, source="static reachability", allowed={"reachable", "rejected"})
        stages["staticallyReachable"] = _stage_row(static_outcome, _reasons(static, source="static reachability"))
        if static_outcome == "rejected":
            terminal = "static_reachability_rejected"

        if terminal is None:
            native = _required(native_rows, candidate_id, stage="native validation")
            _merge_identity(binding, native, source="native validation")
            native_outcome = _outcome(native, source="native validation", allowed={"valid", "rejected"})
            native_identity = _identity_from(native, source="native validation")
            if native_outcome == "valid" and any(
                native_identity[field] is None
                for field in ("resolvedProfileSha256", "programSha256", "validationReportSha256")
            ):
                raise GenerationFunnelContractError(
                    f"native-valid candidate {candidate_id} lacks resolved/program/report validation identity"
                )
            stages["nativeValid"] = _stage_row(native_outcome, _reasons(native, source="native validation"))
            if native_outcome == "rejected":
                terminal = "native_validation_rejected"

        if terminal is None:
            admission = _required(admission_rows, candidate_id, stage="unique admission")
            _merge_identity(binding, admission, source="unique admission")
            admission_outcome = _outcome(admission, source="unique admission", allowed={"admitted", "rejected_duplicate"})
            trusted_proof = trusted_g0_proofs.get(candidate_id)
            if admission_outcome == "admitted" and not _identity_from(
                admission, source="unique admission"
            )["canonicalEvidenceIdentitySha256"] and not (
                trusted_proof is not None
                and admission.get("g0BootstrapProof") == trusted_proof
                and trusted_proof.get("rawSourceProfileSha256") == binding["rawSourceProfileSha256"]
            ):
                raise GenerationFunnelContractError(
                    f"admitted candidate {candidate_id} lacks canonical evidence identity"
                )
            stages["uniqueAdmitted"] = _stage_row(admission_outcome, _reasons(admission, source="unique admission"))
            if admission_outcome == "rejected_duplicate":
                terminal = "duplicate_rejected"

        if terminal is None:
            synthetic = synthetic_rows.get(candidate_id)
            if synthetic is None:
                stages["syntheticEvidence"] = _stage_row("unmeasured")
            else:
                _merge_identity(binding, synthetic, source="synthetic evidence")
                outcome = _outcome(synthetic, source="synthetic evidence", allowed={"passed", "failed", "unmeasured"})
                stages["syntheticEvidence"] = _stage_row(outcome, _reasons(synthetic, source="synthetic evidence"))

            plan = _required(plan_rows, candidate_id, stage="evaluation plan")
            _merge_identity(binding, plan, source="evaluation plan")
            evaluation_outcome = _outcome(plan, source="evaluation plan", allowed={"evaluated", "partial", "rejected"})
            if evaluation_outcome in {"evaluated", "partial"} and not _identity_from(
                plan, source="evaluation plan"
            )["canonicalEvidenceIdentitySha256"] and not (
                trusted_proof is not None
                and plan.get("g0BootstrapProof") == trusted_proof
                and trusted_proof.get("rawSourceProfileSha256") == binding["rawSourceProfileSha256"]
            ):
                raise GenerationFunnelContractError(
                    f"evaluation plan for {candidate_id} lacks canonical evidence identity"
                )
            expected = _field(plan, "expectedWindowIds", "expected_window_ids")
            if not isinstance(expected, Sequence) or isinstance(expected, (str, bytes)) or not expected:
                raise GenerationFunnelContractError(f"evaluation plan expectedWindowIds is required for {candidate_id}")
            expected_windows = [_string(value, name="evaluation plan.windowId", required=True) or "" for value in expected]
            if len(set(expected_windows)) != len(expected_windows):
                raise GenerationFunnelContractError(f"evaluation plan has duplicate windows for {candidate_id}")
            actual = result_rows.get(candidate_id, {})
            if not set(actual).issubset(expected_windows):
                raise GenerationFunnelContractError(f"evaluation results contain an unplanned window for {candidate_id}")
            for result in actual.values():
                _merge_identity(binding, result, source="evaluation result")
            if evaluation_outcome == "evaluated" and set(actual) != set(expected_windows):
                raise GenerationFunnelContractError(f"evaluated candidate {candidate_id} has incomplete exact window results")
            if evaluation_outcome == "partial" and not (0 < len(actual) < len(expected_windows)):
                raise GenerationFunnelContractError(f"partial evaluation for {candidate_id} must contain a non-empty strict subset of windows")
            if evaluation_outcome == "rejected" and actual:
                raise GenerationFunnelContractError(f"rejected evaluation for {candidate_id} cannot include result rows")
            stages["evaluated"] = _stage_row(evaluation_outcome, _reasons(plan, source="evaluation plan"))
            stages["evaluated"]["expectedWindowIds"] = sorted(expected_windows)
            stages["evaluated"]["observedWindowIds"] = sorted(actual)
            if evaluation_outcome != "evaluated":
                terminal = "evaluation_" + evaluation_outcome

        if terminal is None:
            activation = _required(activation_rows, candidate_id, stage="activation quality")
            _merge_identity(binding, activation, source="activation quality")
            activation_outcome = _outcome(
                activation,
                source="activation quality",
                allowed={"recorded", "not_activated", "quality_rejected"},
            )
            quality = _string(_field(activation, "qualityDisposition", "quality_disposition"), name="activation quality.qualityDisposition", required=True)
            stages["activationQuality"] = _stage_row(activation_outcome, _reasons(activation, source="activation quality"))
            stages["activationQuality"]["qualityDisposition"] = quality
            if activation_outcome == "recorded":
                retention = _required(archive_rows, candidate_id, stage="archive retention")
                _merge_identity(binding, retention, source="archive retention")
                retention_outcome = _outcome(retention, source="archive retention", allowed={"retained", "not_retained"})
                if retention_outcome == "retained" and not _identity_from(
                    retention, source="archive retention"
                )["archiveMemberIdentitySha256"]:
                    raise GenerationFunnelContractError(f"retained candidate {candidate_id} lacks archive member identity")
                stages["archiveRetention"] = _stage_row(retention_outcome, _reasons(retention, source="archive retention"))
                terminal = retention_outcome
            else:
                terminal = "activation_" + activation_outcome
                retention = archive_rows.get(candidate_id)
                if retention is not None:
                    _merge_identity(binding, retention, source="archive retention")
                    retention_outcome = _outcome(
                        retention,
                        source="archive retention",
                        allowed={"retained"},
                    )
                    archive_lane = _string(
                        _field(retention, "archiveLane", "archive_lane"),
                        name="archive retention.archiveLane",
                        required=True,
                    )
                    retention_classification = _string(
                        _field(
                            retention,
                            "retentionClassification",
                            "retention_classification",
                        ),
                        name="archive retention.retentionClassification",
                        required=True,
                    )
                    if (
                        activation_outcome != "quality_rejected"
                        or quality == "eligible"
                        or archive_lane != "negative_novelty"
                        or retention_classification
                        != "non_promotable_scheduled_exploration"
                    ):
                        raise GenerationFunnelContractError(
                            "archive retention after non-recorded activation must be non-promotable negative-novelty exploration"
                        )
                    if not _identity_from(
                        retention, source="archive retention"
                    )["archiveMemberIdentitySha256"]:
                        raise GenerationFunnelContractError(
                            f"exploratory-retained candidate {candidate_id} lacks archive member identity"
                        )
                    stages["exploratoryRetention"] = _stage_row(
                        "retained_for_scheduled_negative_novelty_exploration",
                        _reasons(retention, source="archive retention"),
                    )
                    stages["exploratoryRetention"]["archiveLane"] = archive_lane
                    stages["exploratoryRetention"]["promotionEligible"] = False

        # Completeness is also exclusivity.  A terminal row may not be padded
        # with evidence from a stage that was never authorized to run; that is
        # how a stale worker result would otherwise look like real progress.
        allowed_sources = {"static reachability"}
        if "nativeValid" in stages:
            allowed_sources.add("native validation")
        if "uniqueAdmitted" in stages:
            allowed_sources.add("unique admission")
        if "syntheticEvidence" in stages:
            allowed_sources.add("synthetic evidence")
        if "evaluated" in stages:
            allowed_sources.update({"evaluation plan", "evaluation result"})
        if "activationQuality" in stages:
            allowed_sources.add("activation quality")
        if "archiveRetention" in stages:
            allowed_sources.add("archive retention")
        if "exploratoryRetention" in stages:
            allowed_sources.add("archive retention")
        present_sources = {
            source
            for source, index in (
                ("static reachability", static_rows),
                ("native validation", native_rows),
                ("unique admission", admission_rows),
                ("synthetic evidence", synthetic_rows),
                ("evaluation plan", plan_rows),
                ("activation quality", activation_rows),
                ("archive retention", archive_rows),
            )
            if candidate_id in index
        }
        if candidate_id in result_rows:
            present_sources.add("evaluation result")
        forbidden = sorted(present_sources - allowed_sources)
        if forbidden:
            raise GenerationFunnelContractError(
                f"candidate {candidate_id} has extra post-terminal records: {forbidden!r}"
            )

        candidates.append(
            {
                "candidateId": candidate_id,
                "identity": binding,
                "operatorIds": operators,
                "motifIds": motifs,
                "direction": direction,
                "stages": stages,
                "terminalDisposition": terminal,
            }
        )

    stage_counts: Counter[str] = Counter()
    attrition: Counter[str] = Counter()
    for candidate in candidates:
        for stage, record in candidate["stages"].items():
            stage_counts[f"{stage}:{record['outcome']}"] += 1
        attrition[str(candidate["terminalDisposition"])] += 1
    artifact = {
        "schemaVersion": GENERATION_FUNNEL_SCHEMA,
        "completenessPolicy": policy,
        "proposalAccounting": _clone(proposal_accounting, name="proposal accounting"),
        "attemptLedger": attempt_ledger,
        "attemptToMaterializedAttrition": {
            "attempted": attempt_ledger["attemptCount"],
            "materializedCandidates": attempt_ledger["materializedCandidateCount"],
            "notMaterialized": attempt_ledger["nonMaterializedAttemptCount"],
            "attemptDispositionCounts": attempt_ledger["attemptDispositionCounts"],
        },
        "candidateCount": len(candidates),
        "candidates": candidates,
        "stageCounts": dict(sorted(stage_counts.items())),
        "terminalDispositionCounts": dict(sorted(attrition.items())),
        "operatorBreakdown": _count_by(candidates, "operatorIds"),
        "motifBreakdown": _count_by(candidates, "motifIds"),
        "directionBreakdown": _count_by(candidates, "direction"),
    }
    artifact["artifactSha256"] = canonical_sha256(artifact)
    return artifact


def write_generation_funnel_artifact(path: Path | str, artifact: Mapping[str, Any]) -> None:
    """Publish an artifact once; divergent content is a contract failure."""

    material = _clone(artifact, name="generation funnel artifact")
    supplied = _string(material.pop("artifactSha256", None), name="artifactSha256", required=True)
    if canonical_sha256(material) != supplied:
        raise GenerationFunnelContractError("generation funnel artifact identity mismatch")
    encoded = json.dumps(artifact, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.read_text(encoding="utf-8") != encoded:
        raise GenerationFunnelContractError(f"refusing to overwrite divergent immutable file: {destination}")
    if not destination.exists():
        destination.write_text(encoded, encoding="utf-8")


def supervisor_funnel_snapshot(artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Return the future supervisor's deliberately narrow post-reduction seam.

    This does not inspect controller state or alter archive selection.  It
    exposes only immutable artifact identity, summary counts, and final
    candidate terminal dispositions.
    """

    material = _clone(artifact, name="generation funnel artifact")
    supplied = _string(material.pop("artifactSha256", None), name="artifactSha256", required=True)
    if material.get("schemaVersion") != GENERATION_FUNNEL_SCHEMA or canonical_sha256(material) != supplied:
        raise GenerationFunnelContractError("generation funnel artifact is not identity-valid")
    snapshot = {
        "schemaVersion": SUPERVISOR_FUNNEL_SNAPSHOT_SCHEMA,
        "funnelArtifactSha256": supplied,
        "candidateCount": material["candidateCount"],
        "terminalDispositionCounts": material["terminalDispositionCounts"],
        "candidateTerminals": [
            {"candidateId": row["candidateId"], "terminalDisposition": row["terminalDisposition"]}
            for row in material["candidates"]
        ],
    }
    snapshot["snapshotSha256"] = canonical_sha256(snapshot)
    return snapshot
