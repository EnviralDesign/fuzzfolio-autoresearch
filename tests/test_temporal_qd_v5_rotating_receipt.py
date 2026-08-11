from __future__ import annotations

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_v5_rotating_receipt import (
    build_v5_rotating_panel_bundle_receipt,
)


def test_v5_rotating_receipt_rejects_non_native_campaign_seal_before_any_fallback() -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="native campaign seal"):
        build_v5_rotating_panel_bundle_receipt(
            campaign_role="proposal_current_panel",
            campaign_seal={"schemaVersion": "legacy"},
            tail_authority={},
            tail_index={},
            tail_index_relative_path="tail-result-index-v3.json",
            contract={},
            panel={},
            candidates={},
        )
