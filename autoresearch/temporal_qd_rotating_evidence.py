"""Versioned rotating development evidence for temporal QD campaigns.

This is deliberately an orchestration contract.  It never reuses a raw task
result under a new authority: raw result records remain owned by the task
matrix which produced them.  The compact records here are *derived evidence*
with the old task authority retained as provenance.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
import math
import statistics
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256
from .temporal_direction_selection import classify_direction_selection
from .temporal_realized_behavior import (
    REALIZED_BEHAVIOR_SCHEMA,
    aggregate_realized_behavior,
)


ROTATING_EVIDENCE_INPUT_SCHEMA = "temporal_qd_rotating_evidence_input_v1"
ROTATING_EVIDENCE_SCHEMA = "temporal_qd_rotating_evidence_v1"
CANDIDATE_WINDOW_EVIDENCE_SCHEMA = "temporal_qd_candidate_window_evidence_v1"
CANDIDATE_PANEL_BUNDLE_SCHEMA = "temporal_qd_candidate_panel_evidence_bundle_v1"
CUMULATIVE_ARCHIVE_SCHEMA = "temporal_qd_cumulative_breeder_archive_v1"
GENERATION_EVIDENCE_CHECKPOINT_SCHEMA = "temporal_qd_rotating_evidence_checkpoint_v1"
OUTER_TAIL_START = "2026-01-01T00:00:00Z"


def _stamp(value: Any, *, name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise TemporalDiscoveryContractError(f"{name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TemporalDiscoveryContractError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise TemporalDiscoveryContractError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _add_months(value: datetime, months: int) -> datetime:
    month = value.month - 1 + months
    return value.replace(year=value.year + month // 12, month=month % 12 + 1)


def _window(value: Mapping[str, Any], *, name: str, months: int) -> dict[str, str]:
    start = _stamp(value.get("analysisWindowStart"), name=f"{name}.analysisWindowStart")
    end = _stamp(value.get("analysisWindowEnd"), name=f"{name}.analysisWindowEnd")
    if (start.day, start.hour, start.minute, start.second, start.microsecond) != (1, 0, 0, 0, 0):
        raise TemporalDiscoveryContractError(f"{name} must start at a UTC month boundary")
    if end != _add_months(start, months):
        raise TemporalDiscoveryContractError(f"{name} must span exactly {months} calendar months")
    return {"analysisWindowStart": _iso(start), "analysisWindowEnd": _iso(end)}


def _identity(value: Mapping[str, Any], *, field: str, name: str) -> str:
    material = _clone(value, name=name)
    supplied = material.pop(field, None)
    if not isinstance(supplied, str) or not supplied.startswith("sha256:"):
        raise TemporalDiscoveryContractError(f"{name} {field} is invalid")
    if canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    return supplied


def _sha(value: Any, *, name: str) -> str:
    """Require a canonical digest, never merely a SHA-shaped placeholder."""
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise TemporalDiscoveryContractError(f"{name} must be a canonical sha256 digest")
    return value


def _validated_realized_behavior(value: Any) -> dict[str, Any]:
    """Require the exact aggregate behavior identity before v5 aggregation.

    The direction classifier deliberately consumes a compact aggregate.  At
    the rotating evidence seam that aggregate must still carry its immutable
    identity material, otherwise a caller could relabel a side projection
    while retaining only a SHA-shaped string.
    """
    row = _clone(value, name="window realized behavior")
    if row.get("schemaVersion") != REALIZED_BEHAVIOR_SCHEMA:
        raise TemporalDiscoveryContractError("window realized behavior schema is invalid")
    identity = row.get("identityMaterial")
    supplied = row.get("identitySha256")
    if not isinstance(identity, Mapping) or canonical_sha256(identity) != supplied:
        raise TemporalDiscoveryContractError(
            "window realized behavior identity mismatch"
        )
    return row


def _candidate_execution_binding(candidate: Mapping[str, Any], *, name: str) -> dict[str, str]:
    """Keep authored source and normalized execution snapshots distinct.

    ``sourceProfileSha256`` names the raw/authored profile.  ``profileSnapshotSha256``
    names the normalized snapshot accepted by the evaluator.  They may be equal,
    but are not aliases and must never be silently substituted for one another.
    Historical receipts which predate ``sourceProfileSha256`` explicitly use
    their only frozen snapshot as the raw-source alias; newly materialized
    records always write the distinct field.
    """
    candidate_id = candidate.get("candidateId")
    if not isinstance(candidate_id, str) or not candidate_id:
        raise TemporalDiscoveryContractError(f"{name} lacks candidateId")
    binding = {
        "candidateId": candidate_id,
        "candidateIdentitySha256": _sha(
            candidate.get("candidateIdentitySha256"), name=f"{name}.candidateIdentitySha256"
        ),
        "programSha256": _sha(candidate.get("programSha256"), name=f"{name}.programSha256"),
        "sourceProfileSha256": _sha(
            candidate.get("sourceProfileSha256") or candidate.get("profileSnapshotSha256"),
            name=f"{name}.sourceProfileSha256",
        ),
        "profileSnapshotSha256": _sha(
            candidate.get("profileSnapshotSha256"), name=f"{name}.profileSnapshotSha256"
        ),
    }
    source_profile = candidate.get("sourceProfile")
    if source_profile is not None and candidate.get("sourceProfileSha256") is not None:
        if not isinstance(source_profile, Mapping) or canonical_sha256(source_profile) != binding["sourceProfileSha256"]:
            raise TemporalDiscoveryContractError(f"{name} raw/authored source profile identity mismatch")
    return binding


def _bound_window(window: Mapping[str, Any], *, name: str) -> dict[str, str]:
    window_id = window.get("windowId")
    if not isinstance(window_id, str) or not window_id:
        raise TemporalDiscoveryContractError(f"{name}.windowId is required")
    start = _stamp(window.get("analysisWindowStart"), name=f"{name}.analysisWindowStart")
    end = _stamp(window.get("analysisWindowEnd"), name=f"{name}.analysisWindowEnd")
    if start >= end:
        raise TemporalDiscoveryContractError(f"{name} must use a nonempty half-open interval")
    return {
        "windowId": window_id,
        "analysisWindowStart": _iso(start),
        "analysisWindowEnd": _iso(end),
    }


def _quarter_windows(years: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for index, year in enumerate(years, start=1):
        start = _stamp(year["analysisWindowStart"], name="development year")
        for quarter in range(4):
            q_start = _add_months(start, quarter * 3)
            result.append({
                "windowId": f"year-{index}-q{quarter + 1}",
                "yearIndex": index,
                "quarterIndex": quarter + 1,
                "analysisWindowStart": _iso(q_start),
                "analysisWindowEnd": _iso(_add_months(q_start, 3)),
            })
    return result


def build_rotating_evidence_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze four development years and a Latin-square panel cycle.

    Validation/scrutiny are intentionally allowed to overlap development.  The
    outer tail is the only window described as untouched by this contract.
    """
    source = _clone(config, name="rotating QD evidence input")
    allowed = {
        "schemaVersion", "developmentYears", "validationWindow", "scrutinyWindow",
        "outerTailStart", "provisionalSurvivorCount", "breederWidth",
        "robustBreederPolicy", "panelTemplates",
    }
    if source.get("schemaVersion") != ROTATING_EVIDENCE_INPUT_SCHEMA:
        raise TemporalDiscoveryContractError("unsupported rotating QD evidence input schema")
    if set(source) - allowed:
        raise TemporalDiscoveryContractError("rotating QD evidence input has unknown fields")
    outer = _stamp(source.get("outerTailStart", OUTER_TAIL_START), name="rotating outer tail")
    if _iso(outer) != OUTER_TAIL_START:
        raise TemporalDiscoveryContractError("rotating QD outer tail must begin at 2026-01-01T00:00:00Z")
    raw_years = source.get("developmentYears")
    if not isinstance(raw_years, list) or len(raw_years) != 4:
        raise TemporalDiscoveryContractError("rotating QD evidence requires exactly four development years")
    years = [_window(row, name=f"developmentYears[{index}]", months=12) if isinstance(row, Mapping) else None for index, row in enumerate(raw_years)]
    if any(row is None for row in years):
        raise TemporalDiscoveryContractError("rotating QD development years must be objects")
    years = list(years)  # type: ignore[assignment]
    for left, right in zip(years, years[1:]):
        if left["analysisWindowEnd"] > right["analysisWindowStart"]:
            raise TemporalDiscoveryContractError("rotating QD development years overlap")
    if any(_stamp(row["analysisWindowEnd"], name="development year") > outer for row in years):
        raise TemporalDiscoveryContractError("rotating QD development year touches the untouched outer tail")
    validation = _window(_mapping(source.get("validationWindow"), "validationWindow"), name="validationWindow", months=12)
    scrutiny = _window(_mapping(source.get("scrutinyWindow"), "scrutinyWindow"), name="scrutinyWindow", months=36)
    if _stamp(validation["analysisWindowEnd"], name="validation window") > outer or _stamp(scrutiny["analysisWindowEnd"], name="scrutiny window") > outer:
        raise TemporalDiscoveryContractError("rotating QD research scrutiny touches the untouched outer tail")
    width = source.get("provisionalSurvivorCount", 128)
    if isinstance(width, bool) or not isinstance(width, int) or not 1 <= width <= 100_000:
        raise TemporalDiscoveryContractError("rotating QD provisional survivor count is invalid")
    breeder_width = source.get("breederWidth", width)
    if (
        isinstance(breeder_width, bool)
        or not isinstance(breeder_width, int)
        or not 1 <= breeder_width <= width
    ):
        raise TemporalDiscoveryContractError("rotating QD breeder width is invalid")
    robust_policy = robust_breeder_policy(source.get("robustBreederPolicy"))
    quarters = _quarter_windows(years)
    by_key = {(row["yearIndex"], row["quarterIndex"]): row for row in quarters}
    panels: list[dict[str, Any]] = []
    for phase in range(4):
        windows = [by_key[(year, ((year + phase - 1) % 4) + 1)] for year in range(1, 5)]
        panels.append({
            "panelId": f"panel-{phase + 1}", "phase": phase + 1,
            "selection": "latin_square_year_to_quarter_v1",
            "windowIds": [row["windowId"] for row in windows],
            "windows": windows,
            "totalMonths": 12,
        })
    # A panel template is optional until materialization.  When supplied, its
    # content identity is part of the campaign contract, not an ambient path.
    templates = source.get("panelTemplates")
    normalized_templates: dict[str, dict[str, str]] | None = None
    if templates is not None:
        if not isinstance(templates, Mapping) or set(templates) != {row["panelId"] for row in panels}:
            raise TemporalDiscoveryContractError("rotating QD panel templates must bind every panel exactly once")
        normalized_templates = {}
        for panel_id in sorted(templates):
            row = templates[panel_id]
            if not isinstance(row, Mapping) or not isinstance(row.get("path"), str) or not row["path"] or not isinstance(row.get("preparationSha256"), str) or not isinstance(row.get("authorityId"), str):
                raise TemporalDiscoveryContractError("rotating QD panel template identity is invalid")
            normalized_templates[panel_id] = {"path": row["path"], "preparationSha256": row["preparationSha256"], "authorityId": row["authorityId"]}
    output: dict[str, Any] = {
        "schemaVersion": ROTATING_EVIDENCE_SCHEMA,
        "developmentYears": years,
        "quarterWindows": quarters,
        "panels": panels,
        "absoluteGenerationMapping": {"schemaVersion": "temporal_qd_absolute_panel_phase_v1", "firstGenerationIndex": 1, "cycleLength": 4, "mapping": "one_based_modulo_cycle"},
        "cumulativeCoveragePolicy": {"schemaVersion": "temporal_qd_cumulative_coverage_v1", "deduplicateRepeatedPanelIds": True, "backfillOnlyMissingPriorPanels": True, "currentPanelUnionNewCandidatesAndRetainedParents": True, "rawTaskResultsRemainAuthorityBound": True},
        "provisionalReduction": {"maxCandidates": width, "selection": "current_panel_diverse_round_robin_v1", "economicEvidence": "current_panel_conservative_cost_only"},
        "robustSelection": {
            "breederWidth": breeder_width,
            "policy": robust_policy,
            "archiveMode": "replace",
        },
        "researchScrutiny": {"validation": {"window": validation, "months": 12, "overlapsDevelopmentPermitted": True}, "scrutiny": {"window": scrutiny, "months": 36, "overlapsDevelopmentPermitted": True}, "selectionInput": False, "label": "overlapping_research_scrutiny_not_untouched"},
        "outerTail": {"analysisWindowStart": OUTER_TAIL_START, "touched": False, "selectionInput": False, "label": "only_untouched_evidence"},
    }
    if normalized_templates is not None:
        output["panelTemplates"] = normalized_templates
    output["rotatingEvidenceSha256"] = canonical_sha256(output)
    return output


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return value


def validate_rotating_evidence_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    value = _clone(contract, name="rotating QD evidence contract")
    supplied = _identity(value, field="rotatingEvidenceSha256", name="rotating QD evidence contract")
    rebuilt = build_rotating_evidence_contract({
        "schemaVersion": ROTATING_EVIDENCE_INPUT_SCHEMA,
        "developmentYears": value.get("developmentYears"),
        "validationWindow": (value.get("researchScrutiny") or {}).get("validation", {}).get("window"),
        "scrutinyWindow": (value.get("researchScrutiny") or {}).get("scrutiny", {}).get("window"),
        "outerTailStart": (value.get("outerTail") or {}).get("analysisWindowStart"),
        "provisionalSurvivorCount": (value.get("provisionalReduction") or {}).get("maxCandidates"),
        "breederWidth": (value.get("robustSelection") or {}).get("breederWidth"),
        "robustBreederPolicy": (value.get("robustSelection") or {}).get("policy"),
        **({"panelTemplates": value["panelTemplates"]} if "panelTemplates" in value else {}),
    })
    if rebuilt != value or rebuilt["rotatingEvidenceSha256"] != supplied:
        raise TemporalDiscoveryContractError("rotating QD evidence contract drifted")
    return rebuilt


def panel_for_generation(contract: Mapping[str, Any], generation_index: int) -> dict[str, Any]:
    contract = validate_rotating_evidence_contract(contract)
    if isinstance(generation_index, bool) or not isinstance(generation_index, int) or generation_index < 1:
        raise TemporalDiscoveryContractError("absolute generation index must be positive")
    phase = (generation_index - 1) % int(contract["absoluteGenerationMapping"]["cycleLength"])
    return _clone(contract["panels"][phase], name="rotating QD panel")


def required_panel_ids(contract: Mapping[str, Any], generation_index: int) -> list[str]:
    """Return first-seen panel coverage; cycle repeats never require duplicate work."""
    contract = validate_rotating_evidence_contract(contract)
    result: list[str] = []
    for index in range(1, generation_index + 1):
        panel_id = str(panel_for_generation(contract, index)["panelId"])
        if panel_id not in result:
            result.append(panel_id)
    return result


def template_for_generation(contract: Mapping[str, Any], generation_index: int) -> dict[str, str]:
    contract = validate_rotating_evidence_contract(contract)
    templates = contract.get("panelTemplates")
    if not isinstance(templates, Mapping):
        raise TemporalDiscoveryContractError("rotating QD campaign requires materialized panel templates")
    panel = panel_for_generation(contract, generation_index)
    template = templates.get(panel["panelId"])
    if not isinstance(template, Mapping):
        raise TemporalDiscoveryContractError("rotating QD panel template is missing")
    return _clone(template, name="rotating QD panel template")


def validate_generation_template(
    template: Mapping[str, Any], contract: Mapping[str, Any], generation_index: int
) -> None:
    """Verify a materialized template is the exact absolute-generation panel."""
    contract = validate_rotating_evidence_contract(contract)
    panel = panel_for_generation(contract, generation_index)
    validate_panel_template(template, contract, str(panel["panelId"]))


def validate_panel_template(
    template: Mapping[str, Any], contract: Mapping[str, Any], panel_id: str
) -> None:
    """Verify one materialized template against an explicit panel authority."""
    contract = validate_rotating_evidence_contract(contract)
    panel = next((row for row in contract["panels"] if row["panelId"] == panel_id), None)
    if panel is None:
        raise TemporalDiscoveryContractError("rotating QD template names an unknown panel")
    actual = [
        {
            "windowId": row.get("windowId"),
            "analysisWindowStart": row.get("analysisWindowStart"),
            "analysisWindowEnd": row.get("analysisWindowEnd"),
        }
        for row in template.get("developmentWindows") or []
        if isinstance(row, Mapping)
    ]
    expected = [
        {
            "windowId": row["windowId"],
            "analysisWindowStart": row["analysisWindowStart"],
            "analysisWindowEnd": row["analysisWindowEnd"],
        }
        for row in panel["windows"]
    ]
    if actual != expected:
        raise TemporalDiscoveryContractError(
            "rotating QD template does not bind the absolute generation panel"
        )
    templates = contract.get("panelTemplates")
    if not isinstance(templates, Mapping) or not isinstance(templates.get(panel_id), Mapping):
        raise TemporalDiscoveryContractError("rotating QD panel template is missing")
    template_identity = templates[panel_id]
    if canonical_sha256(template) != template_identity["preparationSha256"]:
        raise TemporalDiscoveryContractError("rotating QD template preparation identity mismatch")
    # Local import keeps the date/evidence contract cheap to import.
    from .temporal_search import build_authority
    if build_authority(template)["authorityId"] != template_identity["authorityId"]:
        raise TemporalDiscoveryContractError("rotating QD template authority identity mismatch")


def panel_scoped_evaluation_identity(*, candidate: Mapping[str, Any], evidence_context: Mapping[str, Any], contract: Mapping[str, Any], generation_index: int, panel_id: str | None = None, campaign_role: str = "proposal_current_panel") -> str:
    """v1 rotating identity: stable genome plus one explicit panel/evidence view.

    This intentionally does not replace legacy ``canonicalEvidenceIdentitySha256``.
    Old artifacts remain v3/auditable; rotating campaigns opt into this new
    evaluation-only identity at campaign freeze time.
    """
    contract = validate_rotating_evidence_contract(contract)
    panel = (
        panel_for_generation(contract, generation_index)
        if panel_id is None
        else next((row for row in contract["panels"] if row["panelId"] == panel_id), None)
    )
    if panel is None:
        raise TemporalDiscoveryContractError("panel-scoped evaluation identity names an unknown panel")
    for field in ("candidateIdentitySha256", "programSha256", "sourceProfileSha256"):
        if not isinstance(candidate.get(field), str) or not str(candidate[field]).startswith("sha256:"):
            raise TemporalDiscoveryContractError("panel-scoped evaluation identity lacks stable genome identity")
    context = _clone(evidence_context, name="panel-scoped evidence context")
    supplied = context.pop("predeclaredEvidenceContextSha256", None)
    if supplied is not None and canonical_sha256(context) != supplied:
        raise TemporalDiscoveryContractError("panel-scoped evidence context identity mismatch")
    return canonical_sha256({
        "schemaVersion": "temporal_qd_panel_scoped_evaluation_identity_v1",
        "candidateIdentitySha256": candidate["candidateIdentitySha256"],
        "programSha256": candidate["programSha256"],
        "sourceProfileSha256": candidate["sourceProfileSha256"],
        "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"],
        "panelId": panel["panelId"], "absoluteGenerationIndex": generation_index,
        "campaignRole": campaign_role,
        "panelEvidenceContextSha256": supplied or canonical_sha256(context),
    })


def build_candidate_window_evidence(*, candidate: Mapping[str, Any], panel: Mapping[str, Any], window: Mapping[str, Any], metrics: Mapping[str, Any], evidence_plan_semantic_sha256: str, provenance: Mapping[str, Any]) -> dict[str, Any]:
    """Make one authority-independent result digest with authority-bound provenance."""
    binding = _candidate_execution_binding(candidate, name="candidate-window candidate")
    candidate_id = binding["candidateId"]
    if not isinstance(panel.get("panelId"), str) or not panel["panelId"]:
        raise TemporalDiscoveryContractError("candidate-window evidence lacks panel/window identity")
    frozen_window = _bound_window(window, name="candidate-window window")
    evidence_plan_semantic_sha256 = _sha(
        evidence_plan_semantic_sha256, name="candidate-window evidence plan semantic identity"
    )
    if not isinstance(provenance.get("authorityId"), str) or not isinstance(provenance.get("taskMatrixSha256"), str) or not isinstance(provenance.get("taskId"), str) or not isinstance(provenance.get("resultSha256"), str):
        raise TemporalDiscoveryContractError("candidate-window raw result provenance is incomplete")
    for field in ("authorityId", "taskMatrixSha256", "resultSha256"):
        _sha(provenance.get(field), name=f"candidate-window provenance.{field}")
    if not provenance["taskId"]:
        raise TemporalDiscoveryContractError("candidate-window raw result provenance taskId is invalid")
    frozen_metrics = _clone(metrics, name="candidate window metrics")
    source_snapshot = _sha(
        frozen_metrics.get("sourceProfileSnapshotSha256"),
        name="candidate-window normalized authored profile identity",
    )
    resolved_snapshot = _sha(
        frozen_metrics.get("resolvedProfileSnapshotSha256"),
        name="candidate-window resolved profile identity",
    )
    resolved_program = _sha(
        frozen_metrics.get("resolvedProgramSha256"),
        name="candidate-window resolved program identity",
    )
    if source_snapshot != binding["profileSnapshotSha256"]:
        raise TemporalDiscoveryContractError(
            "candidate-window normalized authored profile identity drifted"
        )
    expected_resolved_program = candidate.get("resolvedProgramSha256")
    if expected_resolved_program is not None and resolved_program != _sha(
        expected_resolved_program, name="candidate-window expected resolved program identity"
    ):
        raise TemporalDiscoveryContractError(
            "candidate-window resolved program identity drifted"
        )
    digest_material = {
        "schemaVersion": CANDIDATE_WINDOW_EVIDENCE_SCHEMA, "candidateId": candidate_id,
        "candidateIdentitySha256": binding["candidateIdentitySha256"], "programSha256": binding["programSha256"],
        # These explicit aliases resolve the historic source/snapshot naming
        # ambiguity without changing either identity's hash semantics.
        "rawSourceProfileSha256": binding["sourceProfileSha256"],
        "normalizedProfileSnapshotSha256": binding["profileSnapshotSha256"],
        "panelId": panel["panelId"], "windowId": frozen_window["windowId"],
        "analysisWindowStart": frozen_window["analysisWindowStart"], "analysisWindowEnd": frozen_window["analysisWindowEnd"],
        "evidencePlanSemanticSha256": evidence_plan_semantic_sha256, "metrics": frozen_metrics,
    }
    # The digest intentionally excludes authority/task identifiers.  Those
    # prove origin but cannot make one authority's raw result reusable by another.
    output = {**digest_material, "evidenceDigestSha256": canonical_sha256(digest_material), "rawTaskProvenance": _clone(provenance, name="raw task provenance")}
    output["recordSha256"] = canonical_sha256(output)
    return output


def _validate_candidate_window_evidence(
    record: Mapping[str, Any], *, candidate: Mapping[str, Any], panel_id: str,
    window: Mapping[str, Any], name: str,
) -> dict[str, Any]:
    """Rebind a stored record before any panel or cumulative reduction uses it."""
    row = _clone(record, name=name)
    _identity(row, field="recordSha256", name=name)
    binding = _candidate_execution_binding(candidate, name=f"{name}.candidate")
    expected_window = _bound_window(window, name=f"{name}.expectedWindow")
    expected = {
        "schemaVersion": CANDIDATE_WINDOW_EVIDENCE_SCHEMA,
        "candidateId": binding["candidateId"],
        "candidateIdentitySha256": binding["candidateIdentitySha256"],
        "programSha256": binding["programSha256"],
        "rawSourceProfileSha256": binding["sourceProfileSha256"],
        "normalizedProfileSnapshotSha256": binding["profileSnapshotSha256"],
        "panelId": panel_id,
        **expected_window,
    }
    if any(row.get(field) != value for field, value in expected.items()):
        raise TemporalDiscoveryContractError(f"{name} is not canonically bound to its candidate/panel/window")
    metrics = _mapping(row.get("metrics"), f"{name}.metrics")
    if (
        metrics.get("sourceProfileSnapshotSha256") != binding["profileSnapshotSha256"]
        or (
            candidate.get("resolvedProgramSha256") is not None
            and metrics.get("resolvedProgramSha256")
            != _sha(candidate.get("resolvedProgramSha256"), name=f"{name}.candidate.resolvedProgramSha256")
        )
    ):
        raise TemporalDiscoveryContractError(f"{name} execution identities are not bound to its candidate")
    for field in (
        "evidenceDigestSha256", "evidencePlanSemanticSha256", "rawSourceProfileSha256",
        "normalizedProfileSnapshotSha256",
    ):
        _sha(row.get(field), name=f"{name}.{field}")
    provenance = _mapping(row.get("rawTaskProvenance"), f"{name}.rawTaskProvenance")
    for field in ("authorityId", "taskMatrixSha256", "resultSha256"):
        _sha(provenance.get(field), name=f"{name}.rawTaskProvenance.{field}")
    if not isinstance(provenance.get("taskId"), str) or not provenance["taskId"]:
        raise TemporalDiscoveryContractError(f"{name}.rawTaskProvenance.taskId is invalid")
    digest_material = {
        key: row[key]
        for key in (
            "schemaVersion", "candidateId", "candidateIdentitySha256", "programSha256",
            "rawSourceProfileSha256", "normalizedProfileSnapshotSha256", "panelId", "windowId",
            "analysisWindowStart", "analysisWindowEnd", "evidencePlanSemanticSha256", "metrics",
        )
    }
    if canonical_sha256(digest_material) != row["evidenceDigestSha256"]:
        raise TemporalDiscoveryContractError(f"{name} evidence digest mismatch")
    return row


def build_candidate_panel_bundle(*, contract: Mapping[str, Any], candidate: Mapping[str, Any], panel_id: str, records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    contract = validate_rotating_evidence_contract(contract)
    panel = next((row for row in contract["panels"] if row["panelId"] == panel_id), None)
    if panel is None:
        raise TemporalDiscoveryContractError("candidate panel bundle names an unknown panel")
    binding = _candidate_execution_binding(candidate, name="candidate panel bundle candidate")
    expected_windows = {
        str(window["windowId"]): _bound_window(window, name="candidate panel window")
        for window in panel["windows"]
    }
    expected = list(expected_windows)
    rows = sorted((_clone(row, name="candidate window evidence") for row in records), key=lambda row: str(row.get("windowId")))
    if [row.get("windowId") for row in rows] != sorted(expected):
        raise TemporalDiscoveryContractError("candidate panel bundle must cover each panel window exactly once")
    for row in rows:
        _validate_candidate_window_evidence(
            row, candidate=candidate, panel_id=panel_id,
            window=expected_windows[str(row["windowId"])], name="candidate window evidence",
        )
    output = {
        "schemaVersion": CANDIDATE_PANEL_BUNDLE_SCHEMA, "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"],
        "candidateId": binding["candidateId"], "candidateIdentitySha256": binding["candidateIdentitySha256"],
        "programSha256": binding["programSha256"],
        "rawSourceProfileSha256": binding["sourceProfileSha256"],
        "normalizedProfileSnapshotSha256": binding["profileSnapshotSha256"], "panelId": panel_id,
        "windowEvidenceDigests": [{"windowId": row["windowId"], "evidenceDigestSha256": row["evidenceDigestSha256"], "recordSha256": row["recordSha256"]} for row in rows],
        # Exact semantic records are retained so cumulative reduction never
        # depends on reopening mutable or authority-specific raw result paths.
        "windowEvidence": rows,
        "rawTaskProvenance": [{"windowId": row["windowId"], **row["rawTaskProvenance"]} for row in rows],
    }
    output["bundleSha256"] = canonical_sha256(output)
    return output


def _validate_candidate_panel_bundle(
    bundle: Mapping[str, Any], *, contract: Mapping[str, Any], candidate: Mapping[str, Any],
    panel_id: str, name: str,
) -> dict[str, Any]:
    """Validate the aggregate hash *and* every embedded evidence binding."""
    row = _clone(bundle, name=name)
    _identity(row, field="bundleSha256", name=name)
    binding = _candidate_execution_binding(candidate, name=f"{name}.candidate")
    panel = next(
        (item for item in contract["panels"] if item["panelId"] == panel_id), None
    )
    if panel is None:
        raise TemporalDiscoveryContractError(f"{name} names an unknown panel")
    expected = {
        "schemaVersion": CANDIDATE_PANEL_BUNDLE_SCHEMA,
        "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"],
        "candidateId": binding["candidateId"],
        "candidateIdentitySha256": binding["candidateIdentitySha256"],
        "programSha256": binding["programSha256"],
        "rawSourceProfileSha256": binding["sourceProfileSha256"],
        "normalizedProfileSnapshotSha256": binding["profileSnapshotSha256"],
        "panelId": panel_id,
    }
    if any(row.get(field) != value for field, value in expected.items()):
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    records = row.get("windowEvidence")
    if not isinstance(records, list):
        raise TemporalDiscoveryContractError(f"{name} lacks exact window evidence")
    windows = {str(window["windowId"]): window for window in panel["windows"]}
    if len(records) != len(windows) or {record.get("windowId") for record in records if isinstance(record, Mapping)} != set(windows):
        raise TemporalDiscoveryContractError(f"{name} must cover each panel window exactly once")
    validated = [
        _validate_candidate_window_evidence(
            _mapping(record, f"{name}.windowEvidence"), candidate=candidate,
            panel_id=panel_id, window=windows[str(record["windowId"])],
            name=f"{name}.windowEvidence[{record['windowId']}]",
        )
        for record in records
    ]
    expected_digests = [
        {"windowId": item["windowId"], "evidenceDigestSha256": item["evidenceDigestSha256"], "recordSha256": item["recordSha256"]}
        for item in sorted(validated, key=lambda item: item["windowId"])
    ]
    if row.get("windowEvidenceDigests") != expected_digests:
        raise TemporalDiscoveryContractError(f"{name} window digest projection mismatch")
    expected_provenance = [
        {"windowId": item["windowId"], **item["rawTaskProvenance"]}
        for item in sorted(validated, key=lambda item: item["windowId"])
    ]
    if row.get("rawTaskProvenance") != expected_provenance:
        raise TemporalDiscoveryContractError(f"{name} raw provenance projection mismatch")
    return row


def reduce_provisional_diverse_survivors(rows: Iterable[Mapping[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    """Deterministic current-panel round robin.  Rows must already use costed metrics."""
    if not isinstance(limit, int) or limit < 1:
        raise TemporalDiscoveryContractError("provisional survivor limit must be positive")
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = _clone(raw, name="current panel candidate")
        if not isinstance(row.get("candidateId"), str) or not isinstance(row.get("cellId"), str):
            raise TemporalDiscoveryContractError("current panel candidate lacks candidate/cell identity")
        if row.get("costView") != "research_conservative":
            raise TemporalDiscoveryContractError("provisional reduction requires conservative-cost evidence")
        rank = row.get("currentPanelRank")
        if isinstance(rank, bool) or not isinstance(rank, (int, float)):
            raise TemporalDiscoveryContractError("current panel candidate rank is invalid")
        groups[row["cellId"]].append(row)
    for values in groups.values():
        values.sort(key=lambda row: (-float(row["currentPanelRank"]), str(row["candidateId"])))
    selected: list[dict[str, Any]] = []
    while len(selected) < limit:
        added = False
        for cell in sorted(groups):
            if groups[cell] and len(selected) < limit:
                selected.append(groups[cell].pop(0)); added = True
        if not added:
            break
    return selected


def build_current_panel_evaluation_cohort(
    *, new_candidates: Sequence[Mapping[str, Any]], retained_parents: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any], generation_index: int,
) -> dict[str, Any]:
    """Create the current-panel union without relabelling parents as proposals."""
    contract = validate_rotating_evidence_contract(contract)
    rows: dict[str, dict[str, Any]] = {}
    for role, values in (("new_proposal", new_candidates), ("retained_parent_evaluation", retained_parents)):
        for raw in values:
            candidate = _clone(raw, name="current panel cohort candidate")
            candidate_id = candidate.get("candidateId")
            identity = candidate.get("candidateIdentitySha256")
            if not isinstance(candidate_id, str) or not candidate_id or not isinstance(identity, str) or not identity:
                raise TemporalDiscoveryContractError("current panel cohort candidate lacks immutable identity")
            old = rows.get(candidate_id)
            if old is not None:
                if old["candidateIdentitySha256"] != identity:
                    raise TemporalDiscoveryContractError("current panel cohort has conflicting candidate identity")
                if role == "new_proposal":
                    old["cohortRole"] = role
                continue
            rows[candidate_id] = {"candidateId": candidate_id, "candidateIdentitySha256": identity, "cohortRole": role}
    panel = panel_for_generation(contract, generation_index)
    output = {
        "schemaVersion": "temporal_qd_current_panel_evaluation_cohort_v1",
        "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"], "generationIndex": generation_index,
        "panelId": panel["panelId"], "candidates": [rows[key] for key in sorted(rows)],
        "newProposalCandidateIds": sorted(key for key, row in rows.items() if row["cohortRole"] == "new_proposal"),
        "retainedParentEvaluationCandidateIds": sorted(key for key, row in rows.items() if row["cohortRole"] == "retained_parent_evaluation"),
        "parentReevaluationIsProposal": False,
    }
    output["cohortSha256"] = canonical_sha256(output)
    return output


def robust_breeder_policy(overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Frozen defaults for v1 rotating selection; no worst-quarter veto."""
    source = dict(overrides or {})
    defaults = {
        "minimumAverageClosedTradesPerCandidateMonth": 4.0,
        "minimumActiveWindowFraction": 0.75,
        "qualityRequiresPositiveCumulativeConservativeNetR": True,
        "qualityRequiresPositiveMedianWindowConservativeNetR": True,
        "frontierMaximumFraction": 0.20,
        "worstWindowConservativeNetRIsHardGate": False,
        "drawdownIsHardGate": False,
        "objectiveDimensions": ["worstWindowConservativeNetR", "drawdown", "costDrag", "novelty"],
    }
    supplied_sha = source.pop("policySha256", None)
    supplied_schema = source.pop("schemaVersion", None)
    if set(source) - set(defaults):
        raise TemporalDiscoveryContractError("rotating robust breeder policy has unknown fields")
    value = {**defaults, **source}
    fractions = (
        float(value["minimumActiveWindowFraction"]),
        float(value["frontierMaximumFraction"]),
    )
    if (
        not all(math.isfinite(item) for item in fractions)
        or not 0 < fractions[0] <= 1
        or not 0 <= fractions[1] <= 0.20
    ):
        raise TemporalDiscoveryContractError("rotating robust breeder policy fraction is invalid")
    trade_threshold = float(value["minimumAverageClosedTradesPerCandidateMonth"])
    if not math.isfinite(trade_threshold) or trade_threshold <= 0:
        raise TemporalDiscoveryContractError("rotating robust breeder policy trade threshold is invalid")
    if (
        value["qualityRequiresPositiveCumulativeConservativeNetR"] is not True
        or value["qualityRequiresPositiveMedianWindowConservativeNetR"] is not True
        or value["worstWindowConservativeNetRIsHardGate"] is not False
        or value["drawdownIsHardGate"] is not False
        or value["objectiveDimensions"] != defaults["objectiveDimensions"]
    ):
        raise TemporalDiscoveryContractError(
            "rotating robust breeder policy weakens its frozen economic semantics"
        )
    value["schemaVersion"] = "temporal_qd_robust_breeder_policy_v1"
    value["policySha256"] = canonical_sha256(value)
    if supplied_schema is not None and supplied_schema != value["schemaVersion"]:
        raise TemporalDiscoveryContractError("rotating robust breeder policy schema drifted")
    if supplied_sha is not None and supplied_sha != value["policySha256"]:
        raise TemporalDiscoveryContractError("rotating robust breeder policy identity drifted")
    return value


def _robust_objective_vector(row: Mapping[str, Any]) -> tuple[float, ...]:
    objectives = _mapping(row.get("robustObjectives"), "robust breeder objectives")
    # Normalize every dimension to maximize for dominance/crowding.
    return (
        float(objectives["worstWindowConservativeNetR"]),
        -float(objectives["drawdown"]),
        -float(objectives["costDrag"]),
        float(objectives["novelty"]),
    )


def _robust_pareto_reduce(
    rows: Sequence[Mapping[str, Any]], *, capacity: int
) -> list[dict[str, Any]]:
    """Deterministic nondominated/crowding reduction over frozen objectives."""

    if capacity <= 0 or not rows:
        return []
    values = [_clone(row, name="robust Pareto member") for row in rows]
    vectors = [_robust_objective_vector(row) for row in values]
    dominates: list[set[int]] = [set() for _ in values]
    dominated_by = [0 for _ in values]
    for left in range(len(values)):
        for right in range(left + 1, len(values)):
            left_dominates = all(
                a >= b for a, b in zip(vectors[left], vectors[right])
            ) and any(a > b for a, b in zip(vectors[left], vectors[right]))
            right_dominates = all(
                b >= a for a, b in zip(vectors[left], vectors[right])
            ) and any(b > a for a, b in zip(vectors[left], vectors[right]))
            if left_dominates:
                dominates[left].add(right)
                dominated_by[right] += 1
            elif right_dominates:
                dominates[right].add(left)
                dominated_by[left] += 1
    fronts: list[list[int]] = []
    current = [index for index, count in enumerate(dominated_by) if count == 0]
    while current:
        current.sort(key=lambda index: str(values[index].get("candidateId")))
        fronts.append(current)
        following: list[int] = []
        for index in current:
            for target in sorted(dominates[index]):
                dominated_by[target] -= 1
                if dominated_by[target] == 0:
                    following.append(target)
        current = following

    selected: list[dict[str, Any]] = []
    for front_index, front in enumerate(fronts):
        distances = {index: 0.0 for index in front}
        if len(front) <= 2:
            distances = {index: math.inf for index in front}
        else:
            for dimension in range(4):
                ordered = sorted(
                    front,
                    key=lambda index: (
                        vectors[index][dimension],
                        str(values[index].get("candidateId")),
                    ),
                )
                distances[ordered[0]] = math.inf
                distances[ordered[-1]] = math.inf
                low = vectors[ordered[0]][dimension]
                high = vectors[ordered[-1]][dimension]
                if high <= low:
                    continue
                for position in range(1, len(ordered) - 1):
                    index = ordered[position]
                    if math.isinf(distances[index]):
                        continue
                    distances[index] += (
                        vectors[ordered[position + 1]][dimension]
                        - vectors[ordered[position - 1]][dimension]
                    ) / (high - low)
        ordered_front = sorted(
            front,
            key=lambda index: (
                -distances[index],
                -float(
                    values[index]["robustEconomics"][
                        "cumulativeConservativeNetR"
                    ]
                ),
                -float(
                    values[index]["robustEconomics"][
                        "medianWindowConservativeNetR"
                    ]
                ),
                str(values[index].get("candidateId")),
            ),
        )
        for index in ordered_front[: max(0, capacity - len(selected))]:
            selected.append(
                {
                    **values[index],
                    "robustParetoFront": front_index,
                    "robustCrowdingDistance": (
                        None if math.isinf(distances[index]) else distances[index]
                    ),
                }
            )
        if len(selected) >= capacity:
            break
    return selected


def classify_robust_breeders(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    breeder_width: int,
    policy: Mapping[str, Any] | None = None,
    direction_aware: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Classify equal-coverage rows into quality and a bounded frontier lane."""
    frozen = robust_breeder_policy(policy)
    if breeder_width < 1:
        raise TemporalDiscoveryContractError("rotating breeder width is invalid")
    quality, frontier = [], []
    for raw in candidate_rows:
        row = _clone(raw, name="robust breeder candidate")
        windows = row.get("windowMetrics")
        months = row.get("coveredMonths")
        if not isinstance(windows, list) or not windows or not isinstance(months, (int, float)) or months <= 0:
            raise TemporalDiscoveryContractError("robust breeder candidate coverage is invalid")
        try:
            net = [float(item["conservativeNetR"]) for item in windows]
            drawdowns = [max(0.0, float(item.get("maxDrawdownR") or 0.0)) for item in windows]
            cost_drag = [
                float(item.get("noCostNetR", item["conservativeNetR"]))
                - float(item["conservativeNetR"])
                for item in windows
            ]
            novelty = float(row.get("novelty", 0.0))
        except (TypeError, ValueError, KeyError) as exc:
            raise TemporalDiscoveryContractError("robust breeder metrics are invalid") from exc
        if not all(math.isfinite(value) for value in [*net, *drawdowns, *cost_drag, novelty]):
            raise TemporalDiscoveryContractError("robust breeder metrics must be finite")
        active = sum(float(item.get("closedTrades") or 0) > 0 for item in windows)
        trades = sum(float(item.get("closedTrades") or 0) for item in windows)
        supported = active / len(windows) >= float(frozen["minimumActiveWindowFraction"]) and trades / float(months) >= float(frozen["minimumAverageClosedTradesPerCandidateMonth"])
        median = float(statistics.median(net))
        direction_selection = None
        if direction_aware:
            realized_behavior = row.get("cumulativeRealizedBehavior")
            if not isinstance(realized_behavior, Mapping):
                raise TemporalDiscoveryContractError(
                    "direction-aware robust breeder lacks cumulative realized behavior"
                )
            direction_selection = classify_direction_selection(realized_behavior)
        enriched = {
            **row,
            "robustSupport": {
                "activeWindowFraction": active / len(windows),
                "averageClosedTradesPerMonth": trades / float(months),
                "coveredWindowCount": len(windows),
                "coveredMonths": float(months),
            },
            "robustEconomics": {
                "cumulativeConservativeNetR": sum(net),
                "medianWindowConservativeNetR": median,
                "worstWindowConservativeNetR": min(net),
                "maximumWindowDrawdownR": max(drawdowns),
                "cumulativeCostDragR": sum(cost_drag),
            },
            "robustObjectives": {
                "worstWindowConservativeNetR": min(net),
                "drawdown": max(drawdowns),
                "costDrag": sum(cost_drag),
                "novelty": novelty,
            },
            **(
                {
                    "directionSelection": direction_selection,
                    "directionBehaviorLane": direction_selection["lane"],
                    "directionBreedingLane": (
                        direction_selection["lane"]
                        if direction_selection["selectionEligible"] is True
                        else None
                    ),
                }
                if direction_selection is not None
                else {}
            ),
        }
        positive_cumulative = sum(net) > 0
        positive_median = median > 0
        direction_eligible = (
            direction_selection is None
            or direction_selection["selectionEligible"] is True
        )
        if supported and direction_eligible and (
            positive_cumulative
            if frozen["qualityRequiresPositiveCumulativeConservativeNetR"]
            else True
        ) and (
            positive_median
            if frozen["qualityRequiresPositiveMedianWindowConservativeNetR"]
            else True
        ):
            quality.append(enriched)
        elif supported and direction_eligible:
            frontier.append(enriched)
    quality = _robust_pareto_reduce(quality, capacity=breeder_width)
    frontier_cap = int(breeder_width * float(frozen["frontierMaximumFraction"]))
    frontier = _robust_pareto_reduce(
        frontier, capacity=min(frontier_cap, breeder_width - len(quality))
    )
    return {"quality": quality, "frontier": frontier, "policy": frozen}


def cumulative_candidate_row(
    *,
    contract: Mapping[str, Any],
    generation_index: int,
    candidate: Mapping[str, Any],
    bundles: Sequence[Mapping[str, Any]],
    cell_id: str,
    current_panel_rank: float,
    novelty: float = 0.0,
    direction_aware: bool = False,
) -> dict[str, Any]:
    """Rebuild one candidate's robust metrics from exact equal coverage."""

    contract = validate_rotating_evidence_contract(contract)
    required = required_panel_ids(contract, generation_index)
    candidate_binding = _candidate_execution_binding(candidate, name="cumulative candidate")
    by_panel: dict[str, Mapping[str, Any]] = {}
    for bundle in bundles:
        panel_id = str(bundle.get("panelId"))
        if panel_id in by_panel:
            raise TemporalDiscoveryContractError("cumulative candidate repeats a panel bundle")
        by_panel[panel_id] = _validate_candidate_panel_bundle(
            bundle, contract=contract, candidate=candidate, panel_id=panel_id,
            name="cumulative candidate panel bundle",
        )
    if set(by_panel) != set(required):
        raise TemporalDiscoveryContractError("cumulative breeder candidate lacks exact required panel coverage")
    windows: list[dict[str, Any]] = []
    source_profile_snapshots: set[str] = set()
    resolved_profile_snapshots: set[str] = set()
    resolved_programs: set[str] = set()
    realized_behavior_windows: list[dict[str, Any]] = []
    for panel_id in required:
        bundle = by_panel[panel_id]
        records = bundle.get("windowEvidence")
        if not isinstance(records, list):
            raise TemporalDiscoveryContractError("candidate panel bundle lacks exact window evidence")
        for record in records:
            if not isinstance(record, Mapping):
                raise TemporalDiscoveryContractError("candidate window evidence is invalid")
            # Repeat record-level rebinding at the cumulative boundary.  A
            # valid bundle hash alone cannot authorize a copied record under a
            # different candidate, panel, or half-open window.
            panel = next(item for item in contract["panels"] if item["panelId"] == panel_id)
            panel_windows = {str(item["windowId"]): item for item in panel["windows"]}
            _validate_candidate_window_evidence(
                record, candidate=candidate, panel_id=panel_id,
                window=panel_windows.get(str(record.get("windowId"))) or {},
                name="cumulative candidate window evidence",
            )
            metrics = _mapping(record.get("metrics"), "candidate window metrics")
            conservative = metrics.get("conservativeNetR", metrics.get("netR"))
            closed = metrics.get("closedTrades", metrics.get("trades", 0))
            try:
                row = {
                    "panelId": panel_id,
                    "windowId": record["windowId"],
                    "conservativeNetR": float(conservative),
                    "noCostNetR": float(metrics.get("noCostNetR", conservative)),
                    "maxDrawdownR": float(metrics.get("maxDrawdownR", 0.0)),
                    "closedTrades": int(closed),
                    "sourceProfileSnapshotSha256": _sha(
                        metrics["sourceProfileSnapshotSha256"],
                        name="cumulative normalized authored profile identity",
                    ),
                    "resolvedProfileSnapshotSha256": _sha(
                        metrics["resolvedProfileSnapshotSha256"],
                        name="cumulative resolved profile identity",
                    ),
                    "resolvedProgramSha256": _sha(
                        metrics["resolvedProgramSha256"],
                        name="cumulative resolved program identity",
                    ),
                }
            except (TypeError, ValueError, KeyError) as exc:
                raise TemporalDiscoveryContractError("candidate window metrics are incomplete") from exc
            if not all(
                math.isfinite(float(value))
                for key, value in row.items()
                if key
                not in {
                    "panelId",
                    "windowId",
                    "closedTrades",
                    "sourceProfileSnapshotSha256",
                    "resolvedProfileSnapshotSha256",
                    "resolvedProgramSha256",
                }
            ) or row["closedTrades"] < 0:
                raise TemporalDiscoveryContractError("candidate window metrics are invalid")
            source_profile_snapshots.add(row["sourceProfileSnapshotSha256"])
            resolved_profile_snapshots.add(row["resolvedProfileSnapshotSha256"])
            resolved_programs.add(row["resolvedProgramSha256"])
            if direction_aware:
                realized = metrics.get("realizedBehavior")
                if not isinstance(realized, Mapping):
                    raise TemporalDiscoveryContractError(
                        "direction-aware cumulative evidence lacks realized behavior"
                    )
                realized_behavior_windows.append(
                    {"realizedBehavior": _validated_realized_behavior(realized)}
                )
            windows.append(row)
    if (
        source_profile_snapshots != {candidate_binding["profileSnapshotSha256"]}
        or len(resolved_profile_snapshots) != 1
        or len(resolved_programs) != 1
    ):
        raise TemporalDiscoveryContractError(
            "cumulative candidate execution identity changed across panels"
        )
    months = sum(int(next(panel["totalMonths"] for panel in contract["panels"] if panel["panelId"] == panel_id)) for panel_id in required)
    output = {
        "candidateId": candidate["candidateId"],
        "candidateIdentitySha256": candidate["candidateIdentitySha256"],
        "programSha256": candidate["programSha256"],
        "cellId": cell_id,
        "currentPanelRank": float(current_panel_rank),
        "coveredMonths": months,
        "windowMetrics": windows,
        "panelBundleSha256s": [by_panel[panel_id]["bundleSha256"] for panel_id in required],
        "rawSourceProfileSha256": candidate_binding["sourceProfileSha256"],
        "normalizedProfileSnapshotSha256": candidate_binding["profileSnapshotSha256"],
        "sourceProfileSnapshotSha256": next(iter(source_profile_snapshots)),
        "resolvedProfileSnapshotSha256": next(iter(resolved_profile_snapshots)),
        "resolvedProgramSha256": next(iter(resolved_programs)),
        "novelty": float(novelty),
    }
    if direction_aware:
        output["cumulativeRealizedBehavior"] = aggregate_realized_behavior(
            realized_behavior_windows
        )
    return output


def missing_backfill_panel_ids(*, contract: Mapping[str, Any], generation_index: int, bundles: Sequence[Mapping[str, Any]]) -> list[str]:
    """Only missing distinct earlier panels are backfilled; repeated cycle panels are not."""
    contract = validate_rotating_evidence_contract(contract)
    known = set()
    for bundle in bundles:
        _identity(bundle, field="bundleSha256", name="candidate panel bundle")
        if bundle.get("rotatingEvidenceSha256") != contract["rotatingEvidenceSha256"]:
            raise TemporalDiscoveryContractError("candidate backfill bundle has different curriculum")
        known.add(str(bundle.get("panelId")))
    return [panel_id for panel_id in required_panel_ids(contract, generation_index) if panel_id not in known]


def build_generation_evidence_checkpoint(*, contract: Mapping[str, Any], generation_index: int, stage: str, cohort: Mapping[str, Any], provisional_candidate_ids: Sequence[str] = (), cumulative_archive: Mapping[str, Any] | None = None, stage_artifacts: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Restart-exact cursor for current evaluation, reduction, backfill and archive."""
    contract = validate_rotating_evidence_contract(contract)
    if stage not in {"current_panel_evaluation", "provisional_reduction", "cumulative_backfill", "cumulative_archive"}:
        raise TemporalDiscoveryContractError("unknown rotating evidence supervisor stage")
    _identity(cohort, field="cohortSha256", name="current panel evaluation cohort")
    if cohort.get("rotatingEvidenceSha256") != contract["rotatingEvidenceSha256"] or cohort.get("generationIndex") != generation_index:
        raise TemporalDiscoveryContractError("rotating evidence checkpoint cohort binding mismatch")
    output: dict[str, Any] = {
        "schemaVersion": GENERATION_EVIDENCE_CHECKPOINT_SCHEMA,
        "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"], "generationIndex": generation_index,
        "panelId": panel_for_generation(contract, generation_index)["panelId"], "requiredPanelIds": required_panel_ids(contract, generation_index),
        "stage": stage, "cohortSha256": cohort["cohortSha256"], "provisionalCandidateIds": sorted(set(provisional_candidate_ids)),
    }
    if cumulative_archive is not None:
        _identity(cumulative_archive, field="archiveSha256", name="cumulative breeder archive")
        output["cumulativeArchiveSha256"] = cumulative_archive["archiveSha256"]
    if stage_artifacts is not None:
        output["stageArtifacts"] = _clone(
            stage_artifacts, name="rotating evidence stage artifacts"
        )
    output["checkpointSha256"] = canonical_sha256(output)
    return output


def build_cumulative_breeder_archive(
    *,
    contract: Mapping[str, Any],
    generation_index: int,
    provisional: Sequence[Mapping[str, Any]],
    bundles: Mapping[str, Sequence[Mapping[str, Any]],],
    previous_archive: Mapping[str, Any] | None = None,
    direction_aware: bool = False,
) -> dict[str, Any]:
    """Replace-mode archive: every member is rebuilt from equal required coverage."""
    contract = validate_rotating_evidence_contract(contract)
    if previous_archive is not None:
        _identity(previous_archive, field="archiveSha256", name="previous cumulative breeder archive")
        if previous_archive.get("rotatingEvidenceSha256") != contract["rotatingEvidenceSha256"]:
            raise TemporalDiscoveryContractError("previous cumulative breeder archive has different curriculum")
    required = required_panel_ids(contract, generation_index)
    members: list[dict[str, Any]] = []
    for raw in sorted(provisional, key=lambda row: str(row.get("candidateId"))):
        candidate_id = raw.get("candidateId")
        _candidate_execution_binding(raw, name="cumulative archive provisional candidate")
        candidate_bundles = list(bundles.get(str(candidate_id)) or [])
        by_panel: dict[str, Mapping[str, Any]] = {}
        for bundle in candidate_bundles:
            panel_id = bundle.get("panelId")
            if panel_id in by_panel:
                raise TemporalDiscoveryContractError("cumulative candidate has duplicate panel bundle")
            by_panel[str(panel_id)] = _validate_candidate_panel_bundle(
                bundle, contract=contract, candidate=raw, panel_id=str(panel_id),
                name="cumulative candidate panel bundle",
            )
        missing = [panel_id for panel_id in required if panel_id not in by_panel]
        if missing:
            raise TemporalDiscoveryContractError("cumulative breeder candidate lacks required panel coverage")
        row = cumulative_candidate_row(
            contract=contract,
            generation_index=generation_index,
            candidate=raw,
            bundles=candidate_bundles,
            cell_id=str(raw.get("cellId")),
            current_panel_rank=float(raw.get("currentPanelRank")),
            novelty=float(raw.get("novelty", 0.0)),
            direction_aware=direction_aware,
        )
        members.append({
            **row,
            "currentPanelId": panel_for_generation(contract, generation_index)["panelId"],
            "requiredPanelIds": required,
            "panelBundles": [by_panel[panel_id]["bundleSha256"] for panel_id in required],
        })
    classified = classify_robust_breeders(
        candidate_rows=members,
        breeder_width=int(contract["robustSelection"]["breederWidth"]),
        policy=contract["robustSelection"]["policy"],
        direction_aware=direction_aware,
    )
    classified_rows = {
        str(row["candidateId"]): {
            **row,
            "robustBreederLane": lane,
            "robustBreederEligible": True,
        }
        for lane, rows in (
            ("quality", classified["quality"]),
            ("frontier", classified["frontier"]),
        )
        for row in rows
    }
    members = [
        classified_rows.get(
            str(row["candidateId"]),
            {**row, "robustBreederLane": "unsupported", "robustBreederEligible": False},
        )
        for row in members
    ]
    output = {
        "schemaVersion": CUMULATIVE_ARCHIVE_SCHEMA, "mode": "replace",
        "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"], "generationIndex": generation_index,
        "currentPanelId": panel_for_generation(contract, generation_index)["panelId"], "requiredPanelIds": required,
        "previousArchiveSha256": previous_archive.get("archiveSha256") if previous_archive is not None else None,
        "members": members,
        "candidatePanelBundles": [
            _clone(bundle, name="cumulative candidate panel bundle")
            for candidate_id in sorted(bundles)
            for bundle in sorted(
                bundles[candidate_id], key=lambda row: str(row.get("panelId"))
            )
            if candidate_id in {str(row.get("candidateId")) for row in provisional}
        ],
        "robustBreederPolicy": classified["policy"],
        "breederWidth": int(contract["robustSelection"]["breederWidth"]),
        "qualityCandidateIds": [row["candidateId"] for row in classified["quality"]],
        "frontierCandidateIds": [row["candidateId"] for row in classified["frontier"]],
        "qualityMemberCount": len(classified["quality"]),
        "frontierMemberCount": len(classified["frontier"]),
        "staleAggregateCarryPermitted": False,
    }
    output["archiveSha256"] = canonical_sha256(output)
    return output


__all__ = [
    "ROTATING_EVIDENCE_INPUT_SCHEMA", "ROTATING_EVIDENCE_SCHEMA", "CANDIDATE_WINDOW_EVIDENCE_SCHEMA",
    "CANDIDATE_PANEL_BUNDLE_SCHEMA", "CUMULATIVE_ARCHIVE_SCHEMA", "GENERATION_EVIDENCE_CHECKPOINT_SCHEMA", "OUTER_TAIL_START",
    "build_rotating_evidence_contract", "validate_rotating_evidence_contract", "panel_for_generation",
    "required_panel_ids", "template_for_generation", "build_candidate_window_evidence",
    "validate_generation_template", "validate_panel_template", "panel_scoped_evaluation_identity", "build_candidate_panel_bundle", "reduce_provisional_diverse_survivors",
    "build_current_panel_evaluation_cohort", "robust_breeder_policy", "classify_robust_breeders", "cumulative_candidate_row", "missing_backfill_panel_ids", "build_generation_evidence_checkpoint", "build_cumulative_breeder_archive",
]
