"""Tracked check seam for the ignored local Procman bundle."""
from __future__ import annotations
import argparse, json
from pathlib import Path
from typing import Any, Mapping

MANIFEST = Path(__file__).resolve().parents[1] / "scripts" / "prebroad-procman-manifest.json"

def _read(path: Path) -> dict[str, Any]:
    value=json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value,dict): raise ValueError(f"object required: {path}")
    return value

def check(local_path: Path, *, manifest_path: Path = MANIFEST) -> dict[str, Any]:
    manifest=_read(manifest_path); local=_read(local_path)
    group=next((x for x in local.get("groups",[]) if x.get("name")==manifest["groupName"]),None)
    indexed={x.get("id"):x for x in local.get("processes",[]) if isinstance(x,Mapping)}
    if not isinstance(group,Mapping) or group.get("process_ids") != manifest["orderedProcessIds"]: raise ValueError("local Normal Operations ordering differs from tracked manifest")
    for expected in manifest["prebroadProcesses"]:
        actual=indexed.get(expected["id"])
        if not isinstance(actual,Mapping) or any(actual.get(k)!=v for k,v in expected.items()): raise ValueError(f"local process differs from tracked manifest: {expected['id']}")
        if any(actual.get(k) is not v for k,v in manifest["requiredSafetyFlags"].items()): raise ValueError(f"unsafe automatic flag: {expected['id']}")
    return {"schemaVersion":"temporal_prebroad_procman_check_v1","ok":True,"processCount":len(manifest["prebroadProcesses"])}

def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(description="Check the ignored Procman bundle against the tracked pre-broad manifest.")
    parser.add_argument("--local-path",type=Path,default=Path("scripts/processes.json")); args=parser.parse_args(argv)
    try: print(json.dumps(check(args.local_path),indent=2,sort_keys=True)); return 0
    except Exception as exc: print(json.dumps({"ok":False,"errorType":type(exc).__name__,"message":str(exc)},indent=2,sort_keys=True)); return 1
if __name__ == "__main__": raise SystemExit(main())
