"""Experiment-only topology co-adaptation matrix contract.

Production rotating 4/5 breeding must omit `topologyCoadaptationMatrix`.
This overlay never launches a market evaluation by itself.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_operator_family_matrix import MATRIX_FAMILIES
from .temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA

COADAPTATION_SCHEMA = "temporal_qd_topology_coadaptation_matrix_v1"
COADAPTATION_MODE = "frozen_parent_topology_then_local_resource_settling_v1"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
ARMS = (
    "exact_parent_clone",
    "topology_only_child",
    "resource_parameter_only_control",
    "topology_then_bounded_resource_settling",
)
NURSERY_SCHEMA = "temporal_qd_morphology_nursery_archive_v1"


def topology_coadaptation_from_config(
    config: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    overlay = config.get("topologyCoadaptationMatrix")
    if overlay is None:
        return None
    return validate_topology_coadaptation_matrix(overlay)


def validate_topology_coadaptation_matrix(contract: Mapping[str, Any]) -> dict[str, Any]:
    if contract.get("schemaVersion") != COADAPTATION_SCHEMA:
        raise TemporalDiscoveryContractError("topology coadaptation schema is incompatible")
    if contract.get("mode") != COADAPTATION_MODE:
        raise TemporalDiscoveryContractError("topology coadaptation mode is incompatible")
    if contract.get("includeCrossover") is not False:
        raise TemporalDiscoveryContractError("topology coadaptation must keep crossover out of this lane")
    if contract.get("cloneControl") != CLONE_CONTROL:
        raise TemporalDiscoveryContractError("topology coadaptation clone control must re-evaluate parents")
    if contract.get("productionArchiveWrite") is not False:
        raise TemporalDiscoveryContractError("topology coadaptation must not write the production archive")
    if int(contract.get("mutationDepth") or 0) != 1:
        raise TemporalDiscoveryContractError("topology coadaptation topology arm must be one exact plan")
    arms = tuple(contract.get("arms") or ())
    if arms != ARMS:
        raise TemporalDiscoveryContractError("topology coadaptation arms drifted")
    parents = contract.get("parents")
    if not isinstance(parents, list) or not parents:
        raise TemporalDiscoveryContractError("topology coadaptation requires frozen parents")
    settling = contract.get("settling")
    if not isinstance(settling, Mapping):
        raise TemporalDiscoveryContractError("topology coadaptation requires a bounded settling budget")
    if int(settling.get("maxResourceSteps") or 0) < 1:
        raise TemporalDiscoveryContractError("settling budget must be a positive resource-step cap")
    if settling.get("families") != ["resource"]:
        raise TemporalDiscoveryContractError("settling may only use the resource family")
    nursery = contract.get("morphologyNursery")
    if not isinstance(nursery, Mapping) or nursery.get("schemaVersion") != NURSERY_SCHEMA:
        raise TemporalDiscoveryContractError("topology coadaptation requires a morphology nursery sidecar")
    if nursery.get("productionBreedingRights") is not False:
        raise TemporalDiscoveryContractError("nursery members must not receive production breeding rights")
    return {
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": COADAPTATION_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "mutationDepth": 1,
        "arms": list(ARMS),
        "parents": parents,
        "settling": dict(settling),
        "morphologyNursery": dict(nursery),
        "families": list(MATRIX_FAMILIES),
    }


def attach_topology_coadaptation_matrix(
    generation_config: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> dict[str, Any]:
    if generation_config.get("schemaVersion") != PAIR_GENERATION_SCHEMA:
        raise TemporalDiscoveryContractError("topology coadaptation overlay requires pair generation v2")
    if "topologyCoadaptationMatrix" in generation_config:
        raise TemporalDiscoveryContractError("topology coadaptation overlay was supplied twice")
    config = dict(generation_config)
    config["topologyCoadaptationMatrix"] = validate_topology_coadaptation_matrix(matrix)
    return config
