"""Exercise the Python-authored v5 manifest against the real Rust batch parser.

This is intentionally a contract gate, not a campaign launcher.  It seals a
temporary G0 proposal from the checked-in stopped-run authority fixture, asks
the current release batch binary to open it, and expects the explicit
fail-closed ``typed core/operator transaction parity`` response.  Reaching
that response proves the Rust dispatcher accepted the exact Python manifest,
execution authority, and full v2 shared authority without constructing a
candidate or starting a live run.
"""

from __future__ import annotations

import gzip
import json
import tempfile
from pathlib import Path

from autoresearch import temporal_qd_native as native
from autoresearch import temporal_qd_v5_native as v5
from autoresearch.result_codec import canonical_json_bytes, sha256


def _fixture() -> dict[str, object]:
    path = (
        Path(__file__).resolve().parents[1]
        / "tests"
        / "fixtures"
        / "temporal_qd_v5_shared_authority_oracle.json.gz"
    )
    return json.loads(gzip.decompress(path.read_bytes()))


def _generation_config() -> dict[str, object]:
    value: dict[str, object] = {
        "schemaVersion": native.PAIR_GENERATION_SCHEMA,
        "generationIndex": 1,
        "targetUniqueCandidates": 1,
        "maxProposalAttempts": 1,
    }
    value["configSha256"] = sha256(canonical_json_bytes(value))
    return value


def _write_canonical_once(path: Path, value: object) -> None:
    native._write_bytes_once(path, canonical_json_bytes(value) + b"\n")


def main() -> None:
    fixture = _fixture()
    inputs = fixture["authorityInputs"]
    if not isinstance(inputs, dict):
        raise SystemExit("checked-in v5 authority fixture lacks authority inputs")
    binary, batch_authority = native.ensure_native_batch()

    with tempfile.TemporaryDirectory(prefix="temporal-qd-v5-bridge-contract-") as temporary:
        output_root = Path(temporary).resolve() / "proposal"
        manifest = v5.build_v5_proposal_manifest(
            output_root=output_root,
            generation_config=_generation_config(),
            pair_source_authority=inputs["pairSourceAuthority"],
            evolvable_module_authority=inputs["evolvableModuleAuthority"],
            bidirectional_pair_policy=inputs["bidirectionalPairPolicy"],
            native_operator_authority=inputs["nativeOperatorAuthority"],
            qd_engine_version=inputs["qdEngineVersion"],
            native_batch_authority=batch_authority,
            evaluation_population_size=1,
            thread_cap=1,
        )
        invocation_root = (
            output_root
            / "native-batch"
            / "v5-proposal"
            / manifest["manifestSha256"].removeprefix("sha256:")
        )
        _write_canonical_once(
            invocation_root / "authority.json",
            manifest["executionAuthority"]["nativeBatchAuthority"],
        )
        _write_canonical_once(
            invocation_root / "frozen-authority.json", manifest["frozenAuthority"]
        )
        _write_canonical_once(invocation_root / "manifest.json", manifest)
        _write_canonical_once(
            output_root / "v5-native" / "authority" / "shared-authority.json",
            manifest["frozenAuthority"],
        )
        completed = native._run_checked(
            (str(binary), "--manifest", str(invocation_root / "manifest.json")),
            cwd=native._repo_root(),
            timeout=90,
            raise_on_nonzero=False,
        )
        detail = (completed.stderr.strip() or completed.stdout.strip()).decode(
            "utf-8", errors="replace"
        )
        if completed.returncode != 2 or "typed core/operator transaction parity" not in detail:
            raise SystemExit(
                "Rust did not accept the Python v5 bridge contract before its "
                f"intentional construction gate (exit={completed.returncode}): {detail}"
            )

    print(
        json.dumps(
            {
                "schemaVersion": "temporal_qd_v5_bridge_contract_gate_v1",
                "status": "rust_parser_accepted_python_manifest",
                "proposalReconstructionCount": 0,
                "legacyRichExpansionCount": 0,
                "liveCampaignStarted": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
