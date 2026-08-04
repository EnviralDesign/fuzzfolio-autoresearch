"""Frozen, non-leaking evidence ladder for temporal QD campaigns."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import re
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256

EVIDENCE_LADDER_INPUT_SCHEMA = "temporal_qd_evidence_ladder_input_v1"
EVIDENCE_LADDER_SCHEMA = "temporal_qd_evidence_ladder_v1"
OUTER_TAIL_START = "2024-06-29T00:00:00Z"
MATERIALIZED_LADDER_SCHEMA = "temporal_qd_evidence_ladder_materialization_v1"
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_BASE_INPUT_FIELDS = {
    "schemaVersion", "frozenSeed", "historicalMonthStarts", "validationWindow",
    "scrutinyWindow", "outerTailStart",
}
_MATERIALIZED_FIELDS = {
    "materializationSchema", "evidenceLadderSha256", "evidenceLadderConfigSha256",
    *(field for stage in ("discovery", "validation", "scrutiny") for field in (
        f"{stage}TemplatePreparationPath", f"{stage}TemplatePreparationSha256", f"{stage}TemplateAuthorityId",
    )),
}


def _stamp(value: Any, *, name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise TemporalDiscoveryContractError(f"{name} must be an ISO timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TemporalDiscoveryContractError(f"{name} must be an ISO timestamp") from exc
    if result.tzinfo is None:
        raise TemporalDiscoveryContractError(f"{name} must include a timezone")
    return result.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _add_months(value: datetime, months: int) -> datetime:
    month = value.month - 1 + months
    return value.replace(year=value.year + month // 12, month=month % 12 + 1)


def _window(value: Mapping[str, Any], *, name: str, months: int) -> dict[str, str]:
    start = _stamp(value.get("analysisWindowStart"), name=f"{name}.analysisWindowStart")
    end = _stamp(value.get("analysisWindowEnd"), name=f"{name}.analysisWindowEnd")
    if start.day != 1 or start.hour or start.minute or start.second or start.microsecond:
        raise TemporalDiscoveryContractError(f"{name} must start at a UTC month boundary")
    if end != _add_months(start, months):
        raise TemporalDiscoveryContractError(f"{name} must span exactly {months} calendar months")
    return {"analysisWindowStart": _iso(start), "analysisWindowEnd": _iso(end)}


def _disjoint(left: Mapping[str, str], right: Mapping[str, str]) -> bool:
    return left["analysisWindowEnd"] <= right["analysisWindowStart"] or right["analysisWindowEnd"] <= left["analysisWindowStart"]


def build_evidence_ladder(config: Mapping[str, Any]) -> dict[str, Any]:
    """Select three separated discovery months and freeze later evidence gates.

    The caller supplies the historical month-start universe, never any outer
    tail material.  Selection is a canonical seed ranking plus a fixed two
    month separation rule, so process ordering cannot affect a campaign.
    """
    source = _clone(config, name="QD evidence ladder input")
    if source.get("schemaVersion") != EVIDENCE_LADDER_INPUT_SCHEMA:
        raise TemporalDiscoveryContractError("unsupported QD evidence ladder input schema")
    unknown = set(source) - _BASE_INPUT_FIELDS - _MATERIALIZED_FIELDS
    if unknown:
        raise TemporalDiscoveryContractError("QD evidence ladder input has unknown fields")
    # Path-only ladder inputs predate the materializer and remain readable for
    # existing paused runs.  New materialized configs identify themselves and
    # must carry the complete closed identity bundle below.
    materialized = source.get("materializationSchema") == MATERIALIZED_LADDER_SCHEMA
    if materialized and not _MATERIALIZED_FIELDS.issubset(source):
        raise TemporalDiscoveryContractError("materialized QD evidence ladder config fields are incomplete")
    seed = source.get("frozenSeed")
    if isinstance(seed, bool) or not isinstance(seed, (str, int)):
        raise TemporalDiscoveryContractError("QD evidence ladder frozenSeed is required")
    tail = _stamp(source.get("outerTailStart", OUTER_TAIL_START), name="QD outer tail start")
    if _iso(tail) != OUTER_TAIL_START:
        raise TemporalDiscoveryContractError("QD outer tail must begin at 2024-06-29T00:00:00Z")
    starts_raw = source.get("historicalMonthStarts")
    if not isinstance(starts_raw, list) or len(starts_raw) < 3:
        raise TemporalDiscoveryContractError("QD evidence ladder requires at least three historical month starts")
    starts = sorted({_iso(_stamp(item, name="QD historical month start")) for item in starts_raw})
    windows = []
    for start_text in starts:
        start = _stamp(start_text, name="QD historical month start")
        if start.day != 1 or start.hour or start.minute or start.second or start.microsecond:
            raise TemporalDiscoveryContractError("QD historical month starts must be UTC month boundaries")
        end = _add_months(start, 1)
        if end > tail:
            raise TemporalDiscoveryContractError("QD discovery month reaches untouched outer tail")
        windows.append({"analysisWindowStart": _iso(start), "analysisWindowEnd": _iso(end)})
    ranked = sorted(windows, key=lambda item: (canonical_sha256({"seed": seed, "start": item["analysisWindowStart"]}), item["analysisWindowStart"]))
    selected: list[dict[str, str]] = []
    for item in ranked:
        start = _stamp(item["analysisWindowStart"], name="QD discovery window")
        if all(abs((start - _stamp(other["analysisWindowStart"], name="QD discovery window")).days) >= 60 for other in selected):
            selected.append(item)
        if len(selected) == 3:
            break
    if len(selected) != 3:
        raise TemporalDiscoveryContractError("QD evidence ladder cannot select three separated discovery months")
    selected.sort(key=lambda item: item["analysisWindowStart"])
    validation = _window(_mapping(source.get("validationWindow"), name="QD validation window"), name="QD validation window", months=12)
    scrutiny = _window(_mapping(source.get("scrutinyWindow"), name="QD scrutiny window"), name="QD scrutiny window", months=36)
    all_windows = [*selected, validation, scrutiny]
    if any(_stamp(item["analysisWindowEnd"], name="QD ladder window") > tail for item in all_windows):
        raise TemporalDiscoveryContractError("QD evidence ladder leaks the untouched outer tail")
    if any(not _disjoint(left, right) for index, left in enumerate(all_windows) for right in all_windows[index + 1 :]):
        raise TemporalDiscoveryContractError("QD evidence ladder windows must be pairwise disjoint")
    output = {
        "schemaVersion": EVIDENCE_LADDER_SCHEMA,
        "frozenSeed": seed,
        "discovery": {
            "windowCount": 3,
            "totalMonths": 3,
            "selection": "canonical_seed_rank_with_minimum_two_month_start_separation_v1",
            "windows": selected,
        },
        "validation": {"window": validation, "maxDiverseSurvivorCount": 128, "selectionInput": True},
        "scrutiny": {"window": scrutiny, "maxFinalistCount": 32, "selectionInput": True},
        "outerTail": {"analysisWindowStart": OUTER_TAIL_START, "selectionInput": False, "touched": False},
    }
    output["evidenceLadderSha256"] = canonical_sha256(output)
    if materialized:
        if source["materializationSchema"] != MATERIALIZED_LADDER_SCHEMA:
            raise TemporalDiscoveryContractError("materialized QD evidence ladder config schema mismatch")
        if source["evidenceLadderSha256"] != output["evidenceLadderSha256"]:
            raise TemporalDiscoveryContractError("materialized QD evidence ladder identity mismatch")
        for stage in ("discovery", "validation", "scrutiny"):
            path = source[f"{stage}TemplatePreparationPath"]
            template_sha = source[f"{stage}TemplatePreparationSha256"]
            authority_id = source[f"{stage}TemplateAuthorityId"]
            if not isinstance(path, str) or not path or not isinstance(template_sha, str) or not _SHA.fullmatch(template_sha) or not isinstance(authority_id, str) or not _SHA.fullmatch(authority_id):
                raise TemporalDiscoveryContractError("materialized QD evidence ladder template identity is malformed")
        material = dict(source)
        supplied_config_sha = material.pop("evidenceLadderConfigSha256")
        if not isinstance(supplied_config_sha, str) or not _SHA.fullmatch(supplied_config_sha) or canonical_sha256(material) != supplied_config_sha:
            raise TemporalDiscoveryContractError("materialized QD evidence ladder config identity mismatch")
        # Keep these bindings outside evidenceLadderSha256: that hash is the
        # date-selection identity used by already-frozen QD state, whereas
        # evidenceLadderConfigSha256 binds the launch-only immutable files.
        output["materializedTemplates"] = {
            stage: {
                "path": source[f"{stage}TemplatePreparationPath"],
                "preparationSha256": source[f"{stage}TemplatePreparationSha256"],
                "authorityId": source[f"{stage}TemplateAuthorityId"],
            }
            for stage in ("discovery", "validation", "scrutiny")
        }
    return output


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return value


def validate_template_discovery_windows(template: Mapping[str, Any], ladder: Mapping[str, Any]) -> None:
    material = _clone(ladder, name="QD evidence ladder")
    supplied = material.pop("evidenceLadderSha256", None)
    template_identities = material.pop("materializedTemplates", None)
    if material.get("schemaVersion") != EVIDENCE_LADDER_SCHEMA or canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError("QD evidence ladder identity mismatch")
    windows = template.get("developmentWindows")
    expected = material["discovery"]["windows"]
    actual = [
        {"analysisWindowStart": row.get("analysisWindowStart"), "analysisWindowEnd": row.get("analysisWindowEnd")}
        for row in windows or []
        if isinstance(row, Mapping)
    ]
    if actual != expected:
        raise TemporalDiscoveryContractError("QD template does not exactly bind the frozen three-month discovery ladder")
    _validate_materialized_template_identity(template, template_identities, stage="discovery")


def validate_template_stage_window(
    template: Mapping[str, Any], ladder: Mapping[str, Any], *, stage: str
) -> None:
    if stage not in {"validation", "scrutiny"}:
        raise TemporalDiscoveryContractError("unknown QD evidence ladder stage")
    expected = ladder[stage]["window"]
    windows = template.get("developmentWindows")
    actual = [
        {"analysisWindowStart": row.get("analysisWindowStart"), "analysisWindowEnd": row.get("analysisWindowEnd")}
        for row in windows or []
        if isinstance(row, Mapping)
    ]
    if actual != [expected]:
        raise TemporalDiscoveryContractError(
            f"QD {stage} template does not exactly bind its frozen ladder window"
        )
    _validate_materialized_template_identity(template, ladder.get("materializedTemplates"), stage=stage)


def _validate_materialized_template_identity(
    template: Mapping[str, Any], identities: Any, *, stage: str
) -> None:
    if identities is None:
        return
    if not isinstance(identities, Mapping) or not isinstance(identities.get(stage), Mapping):
        raise TemporalDiscoveryContractError("materialized QD evidence ladder template identities are malformed")
    expected = identities[stage]
    if canonical_sha256(template) != expected.get("preparationSha256"):
        raise TemporalDiscoveryContractError("QD materialized template preparation identity mismatch")
    # Import locally to keep the light-weight ladder date resolver independent
    # of the temporal-search execution module at import time.
    from .temporal_search import build_authority

    if build_authority(template)["authorityId"] != expected.get("authorityId"):
        raise TemporalDiscoveryContractError("QD materialized template authority identity mismatch")


__all__ = ["EVIDENCE_LADDER_INPUT_SCHEMA", "EVIDENCE_LADDER_SCHEMA", "MATERIALIZED_LADDER_SCHEMA", "OUTER_TAIL_START", "build_evidence_ladder", "validate_template_discovery_windows", "validate_template_stage_window"]
