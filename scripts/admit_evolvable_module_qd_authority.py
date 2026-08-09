"""No-market native capacity admission for the opt-in evolvable QD authority.

This command is deliberately separate from the historical pair admission.  It
opens a frozen legacy pair authority only as the native v2/v3 compiler host,
then proves a new explicit v5-bound module authority through the same native
admission boundary.  It never requests lake data or economic evaluation.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from autoresearch.evolvable_module_qd_authority import (
    build_evolvable_module_authority_config,
    capacity_probe,
    capacity_receipt,
)
from autoresearch.temporal_qd_pair_factory import (
    PairAuthorityBundle,
    load_pair_run_config,
)


def _read(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"could not read authority JSON: {path}") from exc
    if not isinstance(value, Mapping):
        raise SystemExit("authority JSON must be an object")
    return value


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise SystemExit(f"refusing to overwrite divergent capacity admission: {path}")
    path.write_text(encoded, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prove 4,096 evolvable module candidates through no-market native v2/v3 admission."
    )
    parser.add_argument("--pair-config", type=Path, required=True)
    parser.add_argument("--authority-config", type=Path)
    parser.add_argument("--capacity-receipt", type=Path, help="exact prior no-market receipt to bind/validate")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--preview-stream-size", type=int)
    args = parser.parse_args(argv)

    frozen = load_pair_run_config(_read(args.pair_config))
    supplied_receipt = _read(args.capacity_receipt) if args.capacity_receipt else None
    if args.authority_config is None:
        authority_config = build_evolvable_module_authority_config(
            pair_run_config_sha256=frozen["pairRunConfigSha256"],
            catalog_sha256=frozen["longModule"]["catalogSha256"],
            capacity_receipt=supplied_receipt,
        )
    else:
        authority_config = _read(args.authority_config)
        if supplied_receipt is not None and authority_config.get("capacityReceipt") != supplied_receipt:
            raise SystemExit("authority config does not bind the supplied exact capacity receipt")
    with PairAuthorityBundle(frozen) as bundle:
        authority = bundle.open_evolvable_module_authority(authority_config)
        result = capacity_probe(authority, preview_stream_size=args.preview_stream_size)
        receipt = capacity_receipt(authority, result)
    if supplied_receipt is not None and receipt != supplied_receipt:
        raise SystemExit(
            "supplied capacity receipt does not match the exact authority-owned capacity probe"
        )
    _write_once(args.output, receipt)
    print(json.dumps({
        "ok": True,
        "noMarket": receipt["noMarket"],
        "authoritySha256": receipt["authoritySha256"],
        "compiledAdmittedCandidateCount": receipt["compiledAdmittedCandidateCount"],
        "uniqueSemanticPairCount": receipt["uniqueSemanticPairCount"],
        "semanticReceiptSha256": receipt["semanticReceiptSha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
