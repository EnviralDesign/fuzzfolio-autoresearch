"""Extract exact parent trade-entry timestamps without reading child outcomes."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(HERE.parent / "component-surrogate-validation-v3"))
from component_projection_support_v3 import canonical_bytes, read_self_hashed_json, sha256_file, sha256_prefixed  # noqa: E402

MEMBERS_SHA256 = "sha256:aeab638355de72a63cbb4fe30eab29ca59ea81cbd438a9f90f7e6eef3760ef2b"

def extract_record(raw: str) -> dict[str, Any]:
    row = json.loads(raw); aggregate = row.get("aggregate", {}); behavior = aggregate.get("realizedBehavior", {})
    windows = []
    for item in aggregate.get("windowRecords", []):
        windows.append({key: item.get(key) for key in ("windowId", "analysisWindowStart", "analysisWindowEnd")})
    entries = []
    for side, payload in sorted((behavior.get("sides") or {}).items()):
        for trade in payload.get("tradeSequence") or []:
            if trade.get("entryTime") is not None:
                entries.append({"side": side, "entryTime": str(trade["entryTime"]), "windowOrdinal": trade.get("windowOrdinal")})
    entries.sort(key=lambda item: (item["entryTime"], item["side"], -1 if item["windowOrdinal"] is None else int(item["windowOrdinal"])))
    return {"candidateId": row.get("candidateId"), "authoredProgramSha256": aggregate.get("authoredProgramSha256"), "resolvedProgramSha256": aggregate.get("resolvedProgramSha256"), "sourceRecordSha256": "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest(), "windowAuthority": windows, "entries": entries}

def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--manifest", type=Path, required=True); parser.add_argument("--members", type=Path, required=True); parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    if args.output.exists(): raise RuntimeError("refusing to overwrite V4 parent-entry authority")
    if sha256_file(args.members.resolve()) != MEMBERS_SHA256: raise RuntimeError("members authority hash drift")
    manifest = read_self_hashed_json(args.manifest.resolve(), "manifestCanonicalPayloadSha256")
    parent_ids = {row["parentCandidateId"] for row in manifest["contexts"]}
    records = []
    with args.members.open(encoding="utf-8") as handle:
        for raw in handle:
            row = json.loads(raw)
            if row.get("candidateId") in parent_ids: records.append(extract_record(raw.rstrip("\n")))
    records.sort(key=lambda item: item["candidateId"])
    if len(records) != 3: raise RuntimeError(f"expected exactly three retained parent records, found {len(records)}")
    payload: dict[str, Any] = {"schemaVersion": "temporal_qd_component_surrogate_parent_entry_authority_v4", "membersSourceSha256": MEMBERS_SHA256, "manifestCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"], "retainedParentRecordCount": len(records), "records": records, "childOutcomeLabelsRead": False, "economicFieldsRetained": False}
    payload["parentEntryAuthorityCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload)); args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"parentEntryAuthorityCanonicalPayloadSha256": payload["parentEntryAuthorityCanonicalPayloadSha256"], "retainedParentRecordCount": len(records), "entryCount": sum(len(r["entries"]) for r in records)}, sort_keys=True)); return 0
if __name__ == "__main__": raise SystemExit(main())
