from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "generate_evolutionary_substrate_atlas_v3.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("atlas_v3", SCRIPT)
assert SPEC and SPEC.loader
atlas_v3 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(atlas_v3)


def test_indicator_inputs_are_not_represented_as_outputs(tmp_path: Path) -> None:
    indicators = []
    for index in range(88):
        indicator_id = "MA_CROSSOVER" if index == 0 else f"INDICATOR_{index:02d}"
        meta = {"id": indicator_id, "inputs": ["close"], "signalRole": "trigger"}
        if index == 0:
            meta["familySubstitution"] = {
                "substitutionClass": "directional_event_v1",
                "eventOutputSchema": {
                    "kind": "directional_tokens",
                    "longOutput": "bullish",
                    "shortOutput": "bearish",
                },
            }
        indicators.append({"meta": meta, "config": {}})
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"indicators": indicators, "timeframes": {}}), encoding="utf-8")
    pair_authority = {
        "longModule": {"catalog": {"indicators": []}},
        "shortModule": {"catalog": {"indicators": []}},
    }

    record = atlas_v3.indicator_records(catalog_path, pair_authority, {"component": "catalog"})[
        "indicators"
    ][0]

    assert record["requiredInputs"]["values"] == ["close"]
    assert record["rawImplementationOutputs"]["values"] == []
    assert record["rawImplementationOutputs"]["status"] == "unavailable_in_catalog"
    assert record["eventBindingOutputs"]["values"] == [
        {"kind": "directional_tokens", "longOutput": "bullish", "shortOutput": "bearish"}
    ]
