"""Seal two independent corrected V4 feature extractions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(HERE.parent / "component-surrogate-validation-v3"))
from component_projection_support_v3 import canonical_bytes, read_self_hashed_json, sha256_file, sha256_prefixed  # noqa: E402


def summary(path: Path) -> dict[str, Any]:
    payload = read_self_hashed_json(path, "featureCorpusCanonicalPayloadSha256")
    if payload.get("schemaVersion") != "temporal_qd_component_surrogate_feature_corpus_v4": raise RuntimeError("unexpected V4 feature schema")
    if len(payload.get("primaryRows", [])) != 456 or len(payload.get("m15NativeSensitivityRows", [])) != 24: raise RuntimeError("V4 corpus row count drift")
    if payload.get("componentOnlyBoundary", {}).get("outcomeModuleImported") is not False: raise RuntimeError("V4 corpus boundary drift")
    return {"fileSha256": sha256_file(path), "byteCount": path.stat().st_size, "featureCorpusCanonicalPayloadSha256": payload["featureCorpusCanonicalPayloadSha256"], "correctionProtocolCanonicalPayloadSha256": payload["correctionProtocolCanonicalPayloadSha256"], "primaryRowCount": len(payload["primaryRows"]), "m15NativeSensitivityRowCount": len(payload["m15NativeSensitivityRows"]), "componentImplementationCount": len(payload["componentImplementations"])}


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--first", type=Path, required=True); parser.add_argument("--second", type=Path, required=True); parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    if args.output.exists(): raise RuntimeError("refusing to overwrite V4 corpus seal")
    if args.first.read_bytes() != args.second.read_bytes(): raise RuntimeError("independent V4 feature corpora are not byte-identical")
    first, second = summary(args.first), summary(args.second)
    if first != second: raise RuntimeError("independent V4 feature summaries drift")
    payload = {"schemaVersion": "temporal_qd_component_surrogate_feature_corpus_seal_v4", "twoCleanProcessRunsByteIdentical": True, "componentOnlyOutcomeBoundarySealedBeforeOutcomeJoin": True, "canonicalCorpus": first, "independentRunLabels": ["pass-1", "pass-2"]}
    payload["featureCorpusSealCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload))
    args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"featureCorpusSealCanonicalPayloadSha256": payload["featureCorpusSealCanonicalPayloadSha256"], "fileSha256": sha256_file(args.output)}, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
