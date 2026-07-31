from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .temporal_discovery_base import *

def _refresh_manifest(
    root: Path,
    authority_id: str,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.json")):
        if path.name == "manifest.json":
            continue
        relative = path.relative_to(root).as_posix()
        data = path.read_bytes()
        entries.append(
            {
                "relativePath": relative,
                "length": len(data),
                "sha256": hashlib_sha256(data),
            }
        )
    manifest = {
        "schemaVersion": TEMPORAL_DISCOVERY_MANIFEST_SCHEMA,
        "authorityId": authority_id,
        "fileCount": len(entries),
        "files": entries,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    manifest_path = root / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest


def hashlib_sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest().upper()



__all__ = ["_refresh_manifest", "hashlib_sha256"]
