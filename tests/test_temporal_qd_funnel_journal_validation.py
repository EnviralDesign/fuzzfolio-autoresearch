from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_qd_evolution import QD_ENTRY_SCHEMA, _load_entries


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def test_authoritative_qd_journal_loader_rejects_tampered_entry_before_funnel_use(tmp_path: Path) -> None:
    entry = {
        "schemaVersion": QD_ENTRY_SCHEMA,
        "proposalOrdinal": 0,
        "originKind": "random_immigrant",
        "disposition": "proposal_unavailable",
        "proposal": {"rawSourceProfileSha256": None},
    }
    entry["entrySha256"] = canonical_sha256(entry)
    path = tmp_path / "proposal-journal" / "00000000.json"
    _write(path, entry)
    assert _load_entries(tmp_path)[0]["entrySha256"] == entry["entrySha256"]
    entry["disposition"] = "accepted"
    _write(path, entry)
    with pytest.raises(TemporalDiscoveryContractError, match="identity mismatch"):
        _load_entries(tmp_path)
