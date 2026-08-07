from __future__ import annotations

from types import MappingProxyType

from autoresearch.temporal_search import canonical_sha256
from autoresearch.temporal_typed_motif_grammar import _thaw_lineage_snapshot


def test_thaw_lineage_snapshot_preserves_canonical_json_bytes() -> None:
    canonical = {
        "operation": "same_side_crossover",
        "nested": {"steps": [{"side": "long", "ordinal": 1}]},
    }
    frozen = MappingProxyType(
        {
            "operation": "same_side_crossover",
            "nested": MappingProxyType(
                {"steps": (MappingProxyType({"side": "long", "ordinal": 1}),)}
            ),
        }
    )

    thawed = _thaw_lineage_snapshot(frozen)

    assert thawed == canonical
    assert canonical_sha256(thawed) == canonical_sha256(canonical)
