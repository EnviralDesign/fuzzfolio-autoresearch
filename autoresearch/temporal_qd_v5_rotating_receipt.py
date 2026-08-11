"""Fail-closed v5 compact-tail to rotating-panel receipt bridge.

This is the only Python seam permitted before the Rust rotating
pre-finalizer: it invokes the native-tail v4 validator/compact evidence API
and assembles immutable panel bundles.  It never opens a raw result, accepts
a v3 index, or falls back to legacy rotating evidence reducers.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_qd_rotating_evidence import build_candidate_panel_bundle
from .temporal_qd_v5_native_tail import (
    validate_v5_directional_tail_authority,
    validate_v5_directional_tail_index,
    v5_directional_compact_window_evidence,
)


def build_v5_rotating_panel_bundle_receipt(
    *, campaign_role: str, campaign_seal: Mapping[str, Any], tail_authority: Mapping[str, Any],
    tail_index: Mapping[str, Any], tail_index_relative_path: str, contract: Mapping[str, Any],
    panel: Mapping[str, Any], candidates: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Build one v5-only compact evidence receipt for the Rust pre-finalizer."""
    if campaign_role not in {"proposal_current_panel", "retained_parent_current_panel", "prior_panel_backfill"}:
        raise TemporalDiscoveryContractError("v5 rotating receipt role is invalid")
    if not isinstance(campaign_seal, Mapping) or campaign_seal.get("schemaVersion") != "temporal_qd_campaign_seal_v1":
        raise TemporalDiscoveryContractError("v5 rotating receipt lacks native campaign seal")
    seal = dict(campaign_seal)
    supplied = seal.pop("campaignSealSha256", None)
    if canonical_sha256(seal) != supplied:
        raise TemporalDiscoveryContractError("v5 rotating receipt campaign seal drifted")
    runtime = tail_authority.get("runtimeAuthoritySha256")
    generation = tail_authority.get("generationIndex")
    authority = validate_v5_directional_tail_authority(
        tail_authority, runtime_authority_sha256=runtime, generation_index=generation
    )
    index = validate_v5_directional_tail_index(tail_index, authority=authority)
    if not isinstance(tail_index_relative_path, str) or not tail_index_relative_path or "/../" in f"/{tail_index_relative_path}":
        raise TemporalDiscoveryContractError("v5 rotating receipt tail index path is invalid")
    records = v5_directional_compact_window_evidence(
        index=index, authority=authority, panel=panel, candidates=candidates
    )
    bundles = [
        build_candidate_panel_bundle(contract=contract, candidate=candidates[candidate_id], panel_id=str(panel["panelId"]), records=records[candidate_id])
        for candidate_id in sorted(candidates)
    ]
    source = {
        "schemaVersion": "temporal_qd_v5_rotating_compact_evidence_source_v1",
        "tailAuthority": authority,
        "tailResultIndex": {
            "schemaVersion": "temporal_qd_v5_tail_result_index_v4_descriptor_v1",
            "relativePath": tail_index_relative_path.replace("\\", "/"),
            "tailResultIndexSha256": index["tailResultIndexSha256"],
        },
    }
    source["compactEvidenceSourceSha256"] = canonical_sha256(source)
    value = {
        "schemaVersion": "temporal_qd_v5_rotating_panel_bundle_receipt_v1",
        "role": campaign_role,
        "campaignSeal": dict(campaign_seal),
        "compactEvidenceSource": source,
        "candidatePanelBundles": bundles,
    }
    value["receiptSha256"] = canonical_sha256(value)
    return value
