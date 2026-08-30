"""Seal two independent, byte-identical V3 component feature extractions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from component_projection_support_v3 import canonical_bytes, read_self_hashed_json, sha256_file, sha256_prefixed


def corpus_summary(path: Path) -> dict[str, Any]:
    payload = read_self_hashed_json(path, "featureCorpusCanonicalPayloadSha256")
    if payload.get("schemaVersion") != "temporal_qd_component_surrogate_feature_corpus_v3":
        raise RuntimeError(f"unexpected feature corpus schema: {path}")
    if len(payload.get("primaryRows", [])) != 19 * 12 * 2:
        raise RuntimeError(f"unexpected primary row count: {path}")
    if len(payload.get("m15NativeSensitivityRows", [])) != 12 * 2:
        raise RuntimeError(f"unexpected M15 sensitivity row count: {path}")
    boundary = payload.get("componentOnlyBoundary")
    if boundary != {
        "outcomePathArgument": False,
        "outcomeModuleImported": False,
        "temporalGraphImported": False,
        "networkClientImported": False,
        "allBarReadsFromRecoveredIsolatedRoots": True,
    }:
        raise RuntimeError(f"component-only boundary drift: {path}")
    return {
        "fileSha256": sha256_file(path),
        "byteCount": path.stat().st_size,
        "featureCorpusCanonicalPayloadSha256": payload["featureCorpusCanonicalPayloadSha256"],
        "manifestCanonicalPayloadSha256": payload["manifestCanonicalPayloadSha256"],
        "authorityAddendumCanonicalPayloadSha256": payload["authorityAddendumCanonicalPayloadSha256"],
        "historicalExecutionEngineCommit": payload["historicalExecutionEngineCommit"],
        "primaryRowCount": len(payload["primaryRows"]),
        "m15NativeSensitivityRowCount": len(payload["m15NativeSensitivityRows"]),
        "componentImplementationCount": len(payload["componentImplementations"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--first", type=Path, required=True)
    parser.add_argument("--second", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    first = args.first.resolve()
    second = args.second.resolve()
    output = args.output.resolve()
    if output.exists():
        raise RuntimeError(f"refusing to overwrite feature corpus seal: {output}")
    if first.read_bytes() != second.read_bytes():
        raise RuntimeError("independent component feature corpora are not byte-identical")
    first_summary = corpus_summary(first)
    second_summary = corpus_summary(second)
    if first_summary != second_summary:
        raise RuntimeError("independent component corpus summaries drift")
    payload: dict[str, Any] = {
        "schemaVersion": "temporal_qd_component_surrogate_feature_corpus_seal_v3",
        "twoCleanProcessRunsByteIdentical": True,
        "componentOnlyOutcomeBoundarySealedBeforeOutcomeJoin": True,
        "canonicalCorpus": first_summary,
        "independentRunLabels": ["pass-1", "pass-2"],
    }
    payload["featureCorpusSealCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "featureCorpusSealCanonicalPayloadSha256": payload["featureCorpusSealCanonicalPayloadSha256"],
        "fileSha256": sha256_file(output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
