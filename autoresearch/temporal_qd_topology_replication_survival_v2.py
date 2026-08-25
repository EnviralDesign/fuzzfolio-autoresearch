"""Versioned reporting projection over the frozen V1 Boolean rule."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .evidence_plan import canonical_sha256

SCHEMA = "temporal_qd_topology_replication_survival_projection_v2"
PANELS = ("panel-3", "panel-1", "panel-2")


def evaluate_replication_survival_v2(
    panel_useful: Mapping[str, bool | None], *, identities_valid: bool
) -> dict[str, Any]:
    values = {panel: panel_useful.get(panel) for panel in PANELS}
    complete = identities_valid and all(type(values[panel]) is bool for panel in PANELS)
    development = complete and values["panel-3"] is True
    replication = complete and values["panel-1"] is True and values["panel-2"] is True
    promising = development and replication
    if not complete:
        category = "incomplete_invalid"
    elif promising:
        category = "inspected_promising_pending_untouched_confirmation"
    elif development and values["panel-1"] is False and values["panel-2"] is False:
        category = "development_only_not_replicated"
    elif development:
        category = "mixed_panel_nonqualifying"
    elif values["panel-1"] is True or values["panel-2"] is True:
        category = "replication_only_discordant_not_promising"
    else:
        category = "complete_no_useful_panel"
    result: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "panelUsefulProgressiveInnovation": values,
        "evidenceCompleteAndIdentityValid": complete,
        "developmentQualified": development,
        "replicationSurviving": replication,
        "inspectedPromising": promising,
        "reportingCategory": category,
        "confirmationStatus": "pending",
    }
    result["projectionSha256"] = canonical_sha256(result)
    return result


__all__ = ["PANELS", "SCHEMA", "evaluate_replication_survival_v2"]
