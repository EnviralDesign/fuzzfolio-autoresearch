"""Build sealed parent_entry_conditioned evidence without child outcomes."""
from __future__ import annotations
import argparse, importlib.util, json
from collections import defaultdict
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(HERE.parent / "component-surrogate-validation-v3"))
from component_projection_support_v3 import canonical_bytes, read_self_hashed_json, sha256_prefixed  # noqa: E402
from component_surrogate_v4_metrics import event_series_payload, forward_response_by_horizon  # noqa: E402

spec = importlib.util.spec_from_file_location("v4_extract", HERE / "extract-component-feature-corpus-v4.py")
assert spec and spec.loader
extract = importlib.util.module_from_spec(spec); spec.loader.exec_module(extract)

def main() -> int:
    parser = argparse.ArgumentParser()
    for name in ("manifest", "recovery_report", "feature_corpus", "feature_seal", "parent_entry_authority", "output"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists(): raise RuntimeError("refusing to overwrite V4 S2 evidence")
    manifest = read_self_hashed_json(args.manifest, "manifestCanonicalPayloadSha256")
    recovery = read_self_hashed_json(args.recovery_report, "recoveryCanonicalPayloadSha256")
    corpus = read_self_hashed_json(args.feature_corpus, "featureCorpusCanonicalPayloadSha256")
    seal = read_self_hashed_json(args.feature_seal, "featureCorpusSealCanonicalPayloadSha256")
    authority = read_self_hashed_json(args.parent_entry_authority, "parentEntryAuthorityCanonicalPayloadSha256")
    if seal["canonicalCorpus"]["featureCorpusCanonicalPayloadSha256"] != corpus["featureCorpusCanonicalPayloadSha256"]: raise RuntimeError("S2 corpus not bound by V4 feature seal")
    windows = {(row["panelId"], row["windowId"]): row for row in recovery["windows"] if row["panelId"] == "panel-3"}
    if len(windows) != 4: raise RuntimeError("S2 requires four panel-3 windows")
    corpus_rows = {(r["componentIdentitySha256"], r["panelId"], r["windowId"], r["direction"]): r for r in corpus["primaryRows"]}
    by_time: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for parent in authority["records"]:
        for entry in parent["entries"]:
            timestamp = pd.Timestamp(entry["entryTime"])
            matching = [w for w in windows.values() if pd.Timestamp(w["analysisWindowStart"]) <= timestamp < pd.Timestamp(w["analysisWindowEnd"])]
            if len(matching) == 1: by_time[(parent["candidateId"], entry["side"])].append({**entry, "panelId": matching[0]["panelId"], "windowId": matching[0]["windowId"]})
    rows = []
    for component in extract.representatives(manifest):
        for (panel, window_id), window in sorted(windows.items()):
            for side in ("long", "short"):
                events, prices = extract.primary_event_projection(component, window, side)
                corpus_row = corpus_rows[(component["componentIdentity"], panel, window_id, side)]
                actual_hash = sha256_prefixed(canonical_bytes(event_series_payload(events.index, events.to_numpy(bool))))
                if actual_hash != corpus_row["freshEventSeriesCanonicalPayloadSha256"]: raise RuntimeError("S2 event projection does not match sealed V4 feature series")
                for parent in authority["records"]:
                    entries = [e for e in by_time[(parent["candidateId"], side)] if e["windowId"] == window_id]
                    positions = [int(events.index.get_loc(pd.Timestamp(e["entryTime"]))) for e in entries if pd.Timestamp(e["entryTime"]) in events.index]
                    missing = len(entries) - len(positions)
                    admitted = np.array([pos for pos in positions if bool(events.iloc[pos])], dtype=int)
                    suppressed = np.array([pos for pos in positions if not bool(events.iloc[pos])], dtype=int)
                    rows.append({"label": "parent_entry_conditioned", "parentCandidateId": parent["candidateId"], "parentSourceRecordSha256": parent["sourceRecordSha256"], "componentIdentitySha256": component["componentIdentity"], "indicatorId": component["representative"]["component"]["indicatorId"], "side": side, "panelId": panel, "windowId": window_id, "entryCount": len(positions), "entryTimestampCount": len(entries), "missingExactEntryTimestampCount": missing, "admittedEntryCount": len(admitted), "suppressedEntryCount": len(suppressed), "admittedFraction": len(admitted) / len(positions) if positions else None, "suppressedFraction": len(suppressed) / len(positions) if positions else None, "windowBreadthBars": len(events), "freshEventSeriesCanonicalPayloadSha256": actual_hash, "admittedForwardResponseByHorizon": forward_response_by_horizon(close=prices["close"].to_numpy(float), high=prices["high"].to_numpy(float), low=prices["low"].to_numpy(float), starts=admitted, direction=side), "suppressedForwardResponseByHorizon": forward_response_by_horizon(close=prices["close"].to_numpy(float), high=prices["high"].to_numpy(float), low=prices["low"].to_numpy(float), starts=suppressed, direction=side), "routeSiteLimitation": "retained parent trade sequences expose side and timestamp, not per-entry route/site; S2 measures side-level compatibility only"})
    rows.sort(key=lambda r: (r["componentIdentitySha256"], r["parentCandidateId"], r["side"], r["windowId"]))
    payload: dict[str, Any] = {"schemaVersion": "temporal_qd_component_surrogate_parent_entry_conditioned_v4", "label": "parent_entry_conditioned", "childOutcomeLabelsRead": False, "featureCorpusCanonicalPayloadSha256": corpus["featureCorpusCanonicalPayloadSha256"], "featureCorpusSealCanonicalPayloadSha256": seal["featureCorpusSealCanonicalPayloadSha256"], "parentEntryAuthorityCanonicalPayloadSha256": authority["parentEntryAuthorityCanonicalPayloadSha256"], "rowCount": len(rows), "rows": rows}
    payload["parentEntryConditionedCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload)); args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"parentEntryConditionedCanonicalPayloadSha256": payload["parentEntryConditionedCanonicalPayloadSha256"], "rowCount": len(rows)}, sort_keys=True)); return 0
if __name__ == "__main__": raise SystemExit(main())
