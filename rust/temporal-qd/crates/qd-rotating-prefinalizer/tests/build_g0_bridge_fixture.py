"""Publish one real, minimal G0 v5 tree for prefinalizer extractor tests."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO))

from autoresearch import temporal_qd_v5_native as v5
from autoresearch.result_codec import canonical_json_bytes, sha256
from tests.test_temporal_qd_v5_native_bridge import _native_run_kwargs


def write(path: Path, value: object) -> dict[str, object]:
    raw = canonical_json_bytes(value) + b"\n"
    path.write_bytes(raw)
    return {"path": str(path), "rawSha256": sha256(raw), "sizeBytes": len(raw)}


def main(root: Path) -> None:
    # The bridge fail-closes on a stale executable. This integration fixture
    # consumes the already sealed production publisher and never triggers a
    # nested release build from `cargo test`.
    binary = REPO / "rust" / "temporal-qd" / "target" / "release" / "temporal-qd-batch.exe"
    if not binary.is_file():
        raise RuntimeError(f"sealed native batch fixture prerequisite is absent: {binary}")
    kwargs = _native_run_kwargs(root / "output")
    kwargs["evaluation_population_size"] = 2
    result, manifest = v5.run_native_v5_proposal_construction(
        **kwargs, _return_manifest=True
    )
    adapter = v5.build_v5_generation_construction_adapter(result=result, manifest=manifest)
    chain = {
        "schemaVersion": "temporal_qd_v5_g0_funnel_source_chain_input_v1",
        "contractVersion": "temporal_qd_native_foundation_v1",
        "manifest": write(root / "manifest.json", manifest),
        "result": write(root / "result.json", result),
        "adapter": write(root / "adapter.json", adapter),
    }
    chain["inputSha256"] = sha256(canonical_json_bytes(chain))
    write(root / "chain.json", chain)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
