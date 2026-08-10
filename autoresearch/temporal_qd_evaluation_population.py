"""Compact, immutable evaluation view of a rich temporal QD population."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256
from .temporal_qd_g0_bootstrap import (
    verify_campaign_ledger,
    verify_g0_bootstrap_selection,
)


EVALUATION_POPULATION_SCHEMA = "temporal_qd_evaluation_population_v1"
ROTATING_COHORT_POPULATION_SCHEMA = "temporal_qd_rotating_cohort_population_v1"
G0_NATIVE_RECEIPT_SCHEMA = "temporal_qd_native_g0_funnel_receipt_v2"
G0_NATIVE_RECEIPT_PATH = Path("internal") / "g0-funnel" / "receipt.json"


def evaluation_population_path(population_path: Path | str) -> Path:
    return Path(population_path).with_name("evaluation-population.json")


def raw_file_sha256(path: Path | str) -> str:
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise TemporalDiscoveryContractError(
            f"could not read QD population bytes: {path}"
        ) from exc
    return "sha256:" + digest.hexdigest()


def is_optimized_pair_population(payload: Mapping[str, Any]) -> bool:
    """Recognize only the post-sidecar pair-population contract."""

    return (
        payload.get("schemaVersion") == "temporal_qd_generation_population_v3"
        and isinstance(payload.get("pairGenerationConfigSha256"), str)
        and payload.get("bidirectionalPairPolicy") is not None
    )


def _identity(payload: Mapping[str, Any], field: str, *, name: str) -> str:
    material = _clone(payload, name=name)
    supplied = material.pop(field, None)
    if not isinstance(supplied, str) or not supplied.startswith("sha256:"):
        raise TemporalDiscoveryContractError(f"{name} {field} is invalid")
    if canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    return supplied


def _sha(value: Any, *, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise TemporalDiscoveryContractError(f"{name} must be a canonical sha256 digest")
    return value


def _regular_file_status(path: Path, *, name: str) -> os.stat_result | None:
    """Return only a non-reparse regular file, never a path-following alias."""

    try:
        status = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise TemporalDiscoveryContractError(f"could not inspect {name}: {path}") from exc
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    if (
        stat.S_ISLNK(status.st_mode)
        or bool(attributes & reparse_point)
        or not stat.S_ISREG(status.st_mode)
    ):
        raise TemporalDiscoveryContractError(f"{name} is not a real regular file")
    return status


def _receipt_artifact(
    *,
    receipt: Mapping[str, Any],
    root: Path,
    key: str,
    relative_path: str,
) -> Mapping[str, Any]:
    artifact = receipt.get(key)
    if not isinstance(artifact, Mapping) or set(artifact) != {
        "relativePath", "semanticSha256", "fileSha256", "encodedBytes"
    }:
        raise TemporalDiscoveryContractError(f"native G0 receipt {key} binding is invalid")
    if artifact.get("relativePath") != relative_path:
        raise TemporalDiscoveryContractError(f"native G0 receipt {key} path drifted")
    _sha(artifact.get("semanticSha256"), name=f"native G0 receipt {key} semantic identity")
    _sha(artifact.get("fileSha256"), name=f"native G0 receipt {key} file identity")
    encoded_bytes = artifact.get("encodedBytes")
    if isinstance(encoded_bytes, bool) or not isinstance(encoded_bytes, int) or encoded_bytes < 1:
        raise TemporalDiscoveryContractError(f"native G0 receipt {key} byte length is invalid")
    path = root / relative_path
    status = _regular_file_status(path, name=f"native G0 sealed {key}")
    if status is None or status.st_size != encoded_bytes:
        raise TemporalDiscoveryContractError(f"native G0 sealed {key} byte length drifted")
    before_identity = (status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns)
    actual_file_sha = raw_file_sha256(path)
    after = _regular_file_status(path, name=f"native G0 sealed {key}")
    if after is None or (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) != before_identity:
        raise TemporalDiscoveryContractError(
            f"native G0 sealed {key} changed while its bytes were hashed"
        )
    if actual_file_sha != artifact["fileSha256"]:
        raise TemporalDiscoveryContractError(
            f"native G0 sealed {key} file identity drifted"
        )
    return artifact


def _load_native_g0_receipt(
    *, source: Path, payload: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Verify the compact sealed native chain without reopening G0 pool artifacts.

    The receipt is written last by Rust.  It binds the sidecar, compact
    selection/ledger identities, public semantic identities, and cached rich
    population file identity.  This deliberately replaces the old Python
    pool/selection/ledger replay on a production G0 restart.
    """

    receipt_path = source.parent / G0_NATIVE_RECEIPT_PATH
    status = _regular_file_status(receipt_path, name="native G0 receipt")
    if status is None:
        return None
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TemporalDiscoveryContractError("could not read native G0 receipt") from exc
    expected = {
        "schemaVersion", "requestSha256", "authoritySha256", "executionAuthority", "configSha256",
        "generationIndex", "constructionPoolSize", "evaluationPopulationSize",
        "operatorImplementationSha256", "archivePolicyAuthoritySha256",
        "journalInventorySha256", "sourceHandoffSha256", "globalIdentityLedger", "identityLedgerBinding",
        "g0Bootstrap", "population", "evaluationPopulation", "generationJournal",
        "pairGenerationResult", "constructionPoolIdentitySha256", "acceptedPoolSha256",
        "selectionSha256", "ledgerSha256", "receiptSha256",
    }
    if not isinstance(receipt, Mapping) or set(receipt) != expected:
        raise TemporalDiscoveryContractError("native G0 receipt fields are not exact")
    result = dict(receipt)
    if result.get("schemaVersion") != G0_NATIVE_RECEIPT_SCHEMA:
        raise TemporalDiscoveryContractError("native G0 receipt schema is incompatible")
    _identity(result, "receiptSha256", name="native G0 receipt")
    for key in (
        "requestSha256", "authoritySha256", "configSha256",
        "operatorImplementationSha256", "journalInventorySha256",
        "constructionPoolIdentitySha256", "acceptedPoolSha256", "selectionSha256",
        "ledgerSha256",
    ):
        _sha(result.get(key), name=f"native G0 receipt {key}")
    execution_authority = result.get("executionAuthority")
    if not isinstance(execution_authority, Mapping) or set(execution_authority) != {
        "schemaVersion", "g0FinalizationRuntimeSha256", "nativeBatchAuthority",
        "nativeBatchAuthoritySha256", "authoritySha256",
    } or execution_authority.get("schemaVersion") != "temporal_qd_native_g0_execution_authority_v1":
        raise TemporalDiscoveryContractError("native G0 receipt execution authority is invalid")
    if execution_authority.get("authoritySha256") != result["authoritySha256"]:
        raise TemporalDiscoveryContractError("native G0 receipt execution authority binding drifted")
    _identity(dict(execution_authority), "authoritySha256", name="native G0 execution authority")
    batch = execution_authority.get("nativeBatchAuthority")
    if not isinstance(batch, Mapping) or execution_authority.get("nativeBatchAuthoritySha256") != batch.get("authoritySha256"):
        raise TemporalDiscoveryContractError("native G0 receipt batch authority binding drifted")
    _identity(dict(batch), "authoritySha256", name="native G0 batch authority")
    for key in ("archivePolicyAuthoritySha256", "sourceHandoffSha256"):
        if result.get(key) is not None:
            _sha(result[key], name=f"native G0 receipt {key}")
    for key in ("generationIndex", "constructionPoolSize", "evaluationPopulationSize"):
        value = result.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise TemporalDiscoveryContractError(f"native G0 receipt {key} is invalid")
    if (
        result["configSha256"] != payload.get("pairGenerationConfigSha256")
        or result["generationIndex"] != payload.get("generationIndex")
        or result["operatorImplementationSha256"]
        != payload.get("operatorImplementationSha256")
    ):
        raise TemporalDiscoveryContractError("native G0 receipt sidecar binding drifted")
    bootstrap = payload.get("g0Bootstrap")
    expected_bootstrap = {
        "constructionPoolIdentitySha256", "acceptedPoolSha256", "selectionSha256", "ledgerSha256"
    }
    if not isinstance(bootstrap, Mapping) or set(bootstrap) != expected_bootstrap:
        raise TemporalDiscoveryContractError("native G0 receipt requires an exact G0 sidecar binding")
    if result["g0Bootstrap"] != bootstrap or any(
        result[key] != bootstrap[key] for key in expected_bootstrap
    ):
        raise TemporalDiscoveryContractError("native G0 receipt bootstrap binding drifted")
    archive = payload.get("archivePolicyAuthority")
    expected_archive_sha = canonical_sha256(archive) if archive is not None else None
    if result["archivePolicyAuthoritySha256"] != expected_archive_sha:
        raise TemporalDiscoveryContractError("native G0 receipt archive authority binding drifted")
    population = _receipt_artifact(
        receipt=result, root=source.parent, key="population", relative_path="population.json"
    )
    evaluation = _receipt_artifact(
        receipt=result,
        root=source.parent,
        key="evaluationPopulation",
        relative_path="evaluation-population.json",
    )
    _receipt_artifact(
        receipt=result,
        root=source.parent,
        key="generationJournal",
        relative_path="generation-journal.json",
    )
    if (
        population["semanticSha256"] != payload.get("populationSha256")
        or population["fileSha256"] != payload.get("populationFileSha256")
        or evaluation["semanticSha256"] != payload.get("evaluationPopulationSha256")
    ):
        raise TemporalDiscoveryContractError("native G0 receipt public sidecar identities drifted")
    pair_result = result.get("pairGenerationResult")
    if not isinstance(pair_result, Mapping) or (
        pair_result.get("configSha256") != result["configSha256"]
        or pair_result.get("g0Bootstrap") != bootstrap
        or pair_result.get("populationSha256") != population["semanticSha256"]
        or pair_result.get("evaluationPopulationSha256") != evaluation["semanticSha256"]
        or not isinstance(pair_result.get("reproductionAllocation"), Mapping)
        or not isinstance(pair_result.get("reproductionAllocationAccounting"), Mapping)
    ):
        raise TemporalDiscoveryContractError("native G0 receipt result binding drifted")
    ledger = result.get("globalIdentityLedger")
    if ledger is not None:
        if not isinstance(ledger, Mapping) or set(ledger) != {
            "pairExecutableSemanticCount", "pairExecutableSemanticDuplicateRejections", "identityLedgerSha256"
        }:
            raise TemporalDiscoveryContractError("native G0 receipt global ledger is invalid")
        for key in ("pairExecutableSemanticCount", "pairExecutableSemanticDuplicateRejections"):
            value = ledger.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise TemporalDiscoveryContractError("native G0 receipt global ledger count is invalid")
        _sha(ledger.get("identityLedgerSha256"), name="native G0 receipt global ledger")
    ledger_binding = result.get("identityLedgerBinding")
    if ledger is None:
        if ledger_binding is not None:
            raise TemporalDiscoveryContractError("native G0 receipt ledger binding drifted")
    elif not isinstance(ledger_binding, Mapping) or set(ledger_binding) != {
        "schemaVersion", "ledgerPath", "policyName", "policySha256", "identityPolicy", "identityPolicySha256"
    } or ledger_binding.get("schemaVersion") != "temporal_qd_native_g0_identity_ledger_binding_v1":
        raise TemporalDiscoveryContractError("native G0 receipt identity ledger binding is invalid")
    return result


def _verify_native_g0_receipt_journal(
    *, receipt: Mapping[str, Any], source: Path, journal_path: Path, journal: Mapping[str, Any]
) -> None:
    """Bind the already-verified receipt to the compact public journal only."""

    expected_path = source.parent / "generation-journal.json"
    if journal_path != expected_path:
        raise TemporalDiscoveryContractError("native G0 receipt requires the canonical generation journal path")
    generation_journal = receipt["generationJournal"]
    population = receipt["population"]
    evaluation = receipt["evaluationPopulation"]
    pair_result = receipt["pairGenerationResult"]
    if (
        journal.get("journalSha256") != generation_journal["semanticSha256"]
        or journal.get("populationSha256") != population["semanticSha256"]
        or journal.get("populationFileSha256") != population["fileSha256"]
        or journal.get("evaluationPopulationSha256") != evaluation["semanticSha256"]
        or journal.get("g0Bootstrap") != receipt["g0Bootstrap"]
        or journal.get("globalIdentityLedger") != receipt["globalIdentityLedger"]
        or journal.get("reproductionAllocation") != pair_result.get("reproductionAllocation")
        or journal.get("reproductionAllocationAccounting")
        != pair_result.get("reproductionAllocationAccounting")
        or pair_result.get("journalSha256") != generation_journal["semanticSha256"]
    ):
        raise TemporalDiscoveryContractError("native G0 receipt generation journal binding drifted")


def _canonical_evidence_identity(
    candidate: Mapping[str, Any], evidence_context: Mapping[str, Any]
) -> str:
    """Local verifier for the v3 sidecar identity, avoiding an evolution import cycle.

    The source-profile digest is the raw/authored profile identity; the profile
    snapshot digest is the normalized validator/evaluator snapshot.  The two
    are deliberately independent inputs to the evidence identity.
    """
    context = _clone(evidence_context, name="QD predeclared evidence context")
    supplied = _sha(
        context.pop("predeclaredEvidenceContextSha256", None),
        name="QD predeclared evidence context identity",
    )
    if canonical_sha256(context) != supplied:
        raise TemporalDiscoveryContractError("QD predeclared evidence context diverged")
    profile = candidate.get("sourceProfile")
    if not isinstance(profile, Mapping):
        raise TemporalDiscoveryContractError("QD evidence source profile must be an object")
    source_sha = _sha(
        candidate.get("sourceProfileSha256"), name="QD evidence raw/authored source profile"
    )
    if canonical_sha256(profile) != source_sha:
        raise TemporalDiscoveryContractError("QD evidence raw/authored source profile mismatch")
    return canonical_sha256({
        "schemaVersion": "temporal_qd_canonical_evidence_identity_v3",
        "programSha256": _sha(candidate.get("programSha256"), name="QD evidence program"),
        "sourceProfileSha256": source_sha,
        "profileSnapshotSha256": _sha(
            candidate.get("profileSnapshotSha256") or source_sha,
            name="QD evidence normalized profile snapshot",
        ),
        "orderedWindowPlanSemantic": context.get("orderedWindowPlanSemantic"),
        "costViews": context.get("costViews"),
        "workerContractSha256": context.get("workerContractSha256"),
        "executionConfigSha256": canonical_sha256(profile.get("executionConfig") or {}),
    })


def load_evaluation_population(
    *,
    population_path: Path | str,
    journal_path: Path | str | None = None,
    verify_population_file: bool = True,
) -> dict[str, Any]:
    """Load a compact sidecar and bind it to unchanged rich-population bytes.

    The raw file verification is intentionally streaming: callers can validate
    provenance without decoding the population-sized JSON document.  A caller
    that consumes only this compact projection may disable that scan, but must
    still supply the frozen journal so the population byte identity remains
    authority-bound rather than being inferred from mutable filesystem state.
    """

    source = Path(population_path)
    sidecar = evaluation_population_path(source)
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read QD evaluation population: {sidecar}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schemaVersion") != EVALUATION_POPULATION_SCHEMA:
        raise TemporalDiscoveryContractError("QD evaluation population schema is invalid")
    _identity(payload, "evaluationPopulationSha256", name="QD evaluation population")
    native_g0_receipt = _load_native_g0_receipt(source=source, payload=payload)
    if not verify_population_file and journal_path is None and native_g0_receipt is None:
        raise TemporalDiscoveryContractError(
            "skipping QD population byte verification requires its frozen journal"
        )
    if (
        verify_population_file
        and native_g0_receipt is None
        and payload.get("populationFileSha256") != raw_file_sha256(source)
    ):
        raise TemporalDiscoveryContractError(
            "QD evaluation population raw source file identity mismatch"
        )
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or payload.get("candidateCount") != len(candidates):
        raise TemporalDiscoveryContractError("QD evaluation population candidate count mismatch")
    pair_policy = payload.get("bidirectionalPairPolicy")
    if not isinstance(pair_policy, Mapping) or payload.get("pairPolicySha256") != canonical_sha256(pair_policy):
        raise TemporalDiscoveryContractError("QD evaluation population pair policy identity mismatch")
    archive_policy_authority = payload.get("archivePolicyAuthority")
    if archive_policy_authority is not None:
        if not isinstance(archive_policy_authority, Mapping) or set(archive_policy_authority) != {
            "qdVersion", "policyName", "policySha256", "frozenPolicy"
        }:
            raise TemporalDiscoveryContractError(
                "QD evaluation population archive policy authority is invalid"
            )
        if (
            archive_policy_authority.get("policyName") != payload.get("policyName")
            or archive_policy_authority.get("policySha256") != payload.get("policySha256")
            or canonical_sha256(archive_policy_authority["frozenPolicy"])
            != archive_policy_authority.get("policySha256")
        ):
            raise TemporalDiscoveryContractError(
                "QD evaluation population archive policy authority binding mismatch"
            )
    declared_context = payload.get("predeclaredEvidenceContext")
    declared_context_sha = payload.get("predeclaredEvidenceContextSha256")
    if declared_context is not None:
        if not isinstance(declared_context, Mapping):
            raise TemporalDiscoveryContractError("QD evaluation population predeclared evidence context is invalid")
        if _sha(declared_context_sha, name="QD evaluation population predeclared evidence context") != declared_context.get("predeclaredEvidenceContextSha256"):
            raise TemporalDiscoveryContractError("QD evaluation population predeclared evidence context binding mismatch")
        # This payload field is additive for v1 sidecars.  Old sidecars carry
        # only the frozen hash and remain readable for audit; any sidecar that
        # carries the context must prove each candidate against it.
    elif declared_context_sha is not None:
        _sha(declared_context_sha, name="QD evaluation population predeclared evidence context")
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise TemporalDiscoveryContractError("QD evaluation population candidate is invalid")
        candidate_id = candidate.get("candidateId")
        source_mode = candidate.get("sourceMode")
        seed_id = candidate.get("seedId")
        profile = candidate.get("sourceProfile")
        if not isinstance(candidate_id, str) or not candidate_id or not isinstance(profile, Mapping):
            raise TemporalDiscoveryContractError("QD evaluation population candidate lacks executable material")
        if not isinstance(source_mode, str) or not source_mode.strip():
            raise TemporalDiscoveryContractError(
                "QD evaluation population candidate sourceMode is invalid"
            )
        if not isinstance(seed_id, str) or not seed_id.strip():
            raise TemporalDiscoveryContractError(
                "QD evaluation population candidate seedId is invalid"
            )
        if candidate_id in seen:
            raise TemporalDiscoveryContractError("QD evaluation population candidate identities are not unique")
        seen.add(candidate_id)
        if canonical_sha256(profile) != candidate.get("sourceProfileSha256"):
            raise TemporalDiscoveryContractError("QD evaluation population profile identity mismatch")
        evidence_required = declared_context_sha is not None
        for field in (
            "candidateIdentitySha256",
            "programSha256",
            "sourceProfileSha256",
            "profileSnapshotSha256",
            "proposalEntrySha256",
        ):
            value = candidate.get(field)
            if not isinstance(value, str) or not value.startswith("sha256:"):
                raise TemporalDiscoveryContractError(
                    f"QD evaluation population candidate {field} is invalid"
                )
        evidence = candidate.get("canonicalEvidenceIdentitySha256")
        if evidence_required and (
            not isinstance(evidence, str) or not evidence.startswith("sha256:")
        ):
            raise TemporalDiscoveryContractError(
                "QD evaluation population candidate canonical evidence identity is invalid"
            )
        if declared_context is not None and evidence != _canonical_evidence_identity(candidate, declared_context):
            raise TemporalDiscoveryContractError(
                "QD evaluation population candidate canonical evidence identity mismatch"
            )
    funnel_entries = payload.get("funnelEntries")
    if (
        not isinstance(funnel_entries, list)
        or payload.get("proposalAttempts") != len(funnel_entries)
    ):
        raise TemporalDiscoveryContractError("QD evaluation population proposal accounting mismatch")
    g0_bootstrap = payload.get("g0Bootstrap")
    g0_selected_ordinals: set[int] | None = None
    if g0_bootstrap is not None:
        if not isinstance(g0_bootstrap, Mapping) or set(g0_bootstrap) != {
            "constructionPoolIdentitySha256", "acceptedPoolSha256", "selectionSha256", "ledgerSha256"
        }:
            raise TemporalDiscoveryContractError("QD evaluation population G0 binding is invalid")
        if native_g0_receipt is None:
            # Explicit Python-oracle compatibility only.  Production G0 has a
            # sealed Rust receipt and must never pay this pool/selection/ledger
            # replay on restart.
            base = source.parent / "g0-bootstrap"
            try:
                pool = json.loads((base / "accepted-pool.json").read_text(encoding="utf-8"))
                selection = json.loads((base / "selection.json").read_text(encoding="utf-8"))
                ledger = json.loads((base / "campaign-construction-ledger.json").read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise TemporalDiscoveryContractError("QD G0 bootstrap artifacts are unavailable") from exc
            verified_selection = verify_g0_bootstrap_selection(artifact=selection, accepted_pool=pool)
            verify_campaign_ledger(
                ledger=ledger,
                accepted_pool=pool,
                selected_reference_sha256s=[str(row["referenceSha256"]) for row in verified_selection["selected"]],
            )
            expected = {
                "constructionPoolIdentitySha256": pool.get("constructionPoolIdentitySha256"),
                "acceptedPoolSha256": pool.get("acceptedPoolSha256"),
                "selectionSha256": verified_selection.get("selectionSha256"),
                "ledgerSha256": ledger.get("ledgerSha256"),
            }
            if dict(g0_bootstrap) != expected:
                raise TemporalDiscoveryContractError("QD evaluation population G0 binding drift")
            g0_selected_ordinals = {int(row["proposalOrdinal"]) for row in verified_selection["selected"]}
    seen_funnel_ordinals: set[int] = set()
    for ordinal, entry in enumerate(funnel_entries):
        if (
            not isinstance(entry, Mapping)
            or isinstance(entry.get("proposalOrdinal"), bool)
            or not isinstance(entry.get("proposalOrdinal"), int)
            or entry.get("proposalOrdinal") in seen_funnel_ordinals
            or not isinstance(entry.get("entrySha256"), str)
            or not isinstance(entry.get("originKind"), str)
            or not isinstance(entry.get("disposition"), str)
        ):
            raise TemporalDiscoveryContractError("QD evaluation population funnel entry is invalid")
        seen_funnel_ordinals.add(int(entry["proposalOrdinal"]))
    if g0_selected_ordinals is not None and seen_funnel_ordinals != g0_selected_ordinals:
        raise TemporalDiscoveryContractError("QD evaluation population G0 funnel is not the selected subset")
    if journal_path is not None:
        try:
            journal = json.loads(Path(journal_path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise TemporalDiscoveryContractError(
                f"could not read QD generation journal: {journal_path}"
            ) from exc
        if not isinstance(journal, Mapping):
            raise TemporalDiscoveryContractError("QD generation journal is invalid")
        _identity(journal, "journalSha256", name="QD generation journal")
        if native_g0_receipt is not None:
            _verify_native_g0_receipt_journal(
                receipt=native_g0_receipt,
                source=source,
                journal_path=Path(journal_path),
                journal=journal,
            )
        for field in (
            "populationSha256",
            "configSha256",
            "policyName",
            "policySha256",
            "generationIndex",
            "evaluationPopulationSha256",
        ):
            projected = "pairGenerationConfigSha256" if field == "configSha256" else field
            if journal.get(field) != payload.get(projected):
                raise TemporalDiscoveryContractError(
                    f"QD evaluation population journal {field} binding mismatch"
                )
        if (
            payload.get("populationFileSha256") != journal.get("populationFileSha256")
            or not isinstance(journal.get("operatorImplementation"), Mapping)
            or payload.get("operatorImplementationSha256")
            != canonical_sha256(journal["operatorImplementation"])
            or payload.get("predeclaredEvidenceContextSha256")
            != journal.get("predeclaredEvidenceContextSha256")
        ):
            raise TemporalDiscoveryContractError(
                "QD evaluation population journal identity binding mismatch"
            )
        if payload.get("archivePolicyAuthority") != journal.get(
            "archivePolicyAuthority"
        ):
            raise TemporalDiscoveryContractError(
                "QD evaluation population journal archive policy authority mismatch"
            )
        entries = journal.get("entrySha256s")
        if not isinstance(entries, list):
            raise TemporalDiscoveryContractError("QD generation journal entry identities are invalid")
        if (
            len(entries) != len(funnel_entries)
            or journal.get("proposalCount") != len(funnel_entries)
        ):
            raise TemporalDiscoveryContractError("QD evaluation population journal proposal count mismatch")
        for entry_sha, entry in zip(entries, funnel_entries, strict=True):
            if entry_sha != entry.get("entrySha256"):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population journal funnel reference mismatch"
                )
        bindings = journal.get("evaluationCandidateBindings")
        if not isinstance(bindings, list) or journal.get("acceptedCount") != len(candidates):
            raise TemporalDiscoveryContractError("QD evaluation population journal accepted accounting mismatch")
        for candidate in candidates:
            ordinal = candidate.get("proposalOrdinal")
            if (
                isinstance(ordinal, bool)
                or not isinstance(ordinal, int)
                or ordinal not in seen_funnel_ordinals
            ):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population candidate proposal ordinal is invalid"
                )
        expected_bindings = [
            {
                "candidateId": row["candidateId"],
                "proposalOrdinal": row["proposalOrdinal"],
                "proposalEntrySha256": row["proposalEntrySha256"],
                "candidateProjectionSha256": canonical_sha256(row),
            }
            for row in candidates
        ]
        if bindings != expected_bindings:
            raise TemporalDiscoveryContractError(
                "QD evaluation population journal candidate bindings mismatch"
            )
        accepted_ordinals: set[int] = set()
        for candidate, binding in zip(candidates, bindings, strict=True):
            ordinal = binding["proposalOrdinal"]
            if ordinal in accepted_ordinals:
                raise TemporalDiscoveryContractError("QD evaluation population accepted ordinals are not unique")
            accepted_ordinals.add(ordinal)
            funnel = next(row for row in funnel_entries if row["proposalOrdinal"] == ordinal)
            funnel_candidate = funnel.get("candidate")
            if (
                funnel.get("disposition") != "accepted"
                or not isinstance(funnel_candidate, Mapping)
                or funnel_candidate.get("candidateId") != candidate["candidateId"]
                or funnel_candidate.get("sourceProfileSha256")
                != candidate["sourceProfileSha256"]
            ):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population accepted funnel binding mismatch"
                )
        for candidate in candidates:
            ordinal = candidate.get("proposalOrdinal")
            if not isinstance(ordinal, int) or not any(
                row["proposalOrdinal"] == ordinal and row["entrySha256"] == candidate.get("proposalEntrySha256")
                for row in funnel_entries
            ):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population journal candidate reference mismatch"
                )
    return _clone(payload, name="QD evaluation population")


def hydrate_evaluation_candidate(
    candidate: Mapping[str, Any], *, proposal_root: Path | str
) -> dict[str, Any]:
    """Reopen the rich, immutable proposal behind one compact projection.

    Retained parents are already rich and pass through unchanged.  New pair
    proposals are hydrated from their own append-only proposal journal; this
    avoids decoding the population-sized source document during rotating
    cohort construction.
    """

    compact = _clone(candidate, name="QD evaluation candidate")
    if isinstance(compact.get("bidirectionalGenome"), Mapping):
        return compact
    ordinal = compact.get("proposalOrdinal")
    entry_sha = compact.get("proposalEntrySha256")
    if (
        isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or ordinal < 0
        or not isinstance(entry_sha, str)
        or not entry_sha.startswith("sha256:")
    ):
        raise TemporalDiscoveryContractError(
            "QD compact candidate lacks an immutable proposal reference"
        )
    path = Path(proposal_root) / f"{ordinal:08d}.json"
    try:
        entry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read QD proposal journal entry: {path}"
        ) from exc
    if not isinstance(entry, Mapping):
        raise TemporalDiscoveryContractError("QD proposal journal entry is invalid")
    supplied = entry.get("entrySha256")
    material = {key: value for key, value in entry.items() if key != "entrySha256"}
    if supplied != entry_sha or canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError("QD proposal journal entry identity mismatch")
    rich = entry.get("candidate")
    if not isinstance(rich, Mapping):
        raise TemporalDiscoveryContractError("QD accepted proposal lacks rich candidate material")
    for field in (
        "candidateId",
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfileSha256",
    ):
        if rich.get(field) != compact.get(field):
            raise TemporalDiscoveryContractError(
                f"QD rich candidate {field} differs from its evaluation projection"
            )
    return _clone(rich, name="QD rich proposal candidate")


def build_rotating_cohort_population(
    *,
    candidates: Sequence[Mapping[str, Any]],
    generation_index: int,
    panel_id: str,
    cohort_role: str,
    rotating_evidence_sha256: str,
) -> dict[str, Any]:
    """Build a small executable population for parent/backfill campaigns.

    This schema is intentionally not a proposal population: it has no proposal
    ordinals or funnel entries, and therefore cannot inflate proposal counts.
    """

    if generation_index < 1 or not panel_id or not cohort_role:
        raise TemporalDiscoveryContractError("rotating cohort identity is invalid")
    rows = sorted(
        (_clone(row, name="rotating cohort candidate") for row in candidates),
        key=lambda row: str(row.get("candidateId")),
    )
    seen: set[str] = set()
    for row in rows:
        candidate_id = row.get("candidateId")
        profile = row.get("sourceProfile")
        if (
            not isinstance(candidate_id, str)
            or not candidate_id
            or candidate_id in seen
            or not isinstance(profile, Mapping)
            or canonical_sha256(profile) != row.get("sourceProfileSha256")
        ):
            raise TemporalDiscoveryContractError(
                "rotating cohort contains invalid or duplicate candidate material"
            )
        for field in ("candidateIdentitySha256", "programSha256"):
            value = row.get(field)
            if not isinstance(value, str) or not value.startswith("sha256:"):
                raise TemporalDiscoveryContractError(
                    f"rotating cohort candidate {field} is invalid"
                )
        seen.add(candidate_id)
    output = {
        "schemaVersion": ROTATING_COHORT_POPULATION_SCHEMA,
        "generationIndex": generation_index,
        "panelId": panel_id,
        "cohortRole": cohort_role,
        "rotatingEvidenceSha256": rotating_evidence_sha256,
        "candidateCount": len(rows),
        "candidates": rows,
        "proposalPopulation": False,
    }
    output["populationSha256"] = canonical_sha256(output)
    return output


__all__ = [
    "EVALUATION_POPULATION_SCHEMA",
    "ROTATING_COHORT_POPULATION_SCHEMA",
    "build_rotating_cohort_population",
    "evaluation_population_path",
    "hydrate_evaluation_candidate",
    "is_optimized_pair_population",
    "load_evaluation_population",
    "raw_file_sha256",
]
