from __future__ import annotations

import argparse
import json
from pathlib import Path

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_evolution import initialize_empty_bidirectional_archive
from autoresearch.temporal_qd_pair_factory import load_pair_run_config, pair_policy_from_config


def _read(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"JSON root must be an object: {path}")
    return value


def _write_immutable(path: Path, value: dict) -> None:
    encoded = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing to overwrite divergent archive: {path}")
    path.write_text(encoded, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Initialize an empty QD archive for one frozen bidirectional pair authority.")
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--pair-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    config = load_pair_run_config(_read(args.pair_config))
    archive = initialize_empty_bidirectional_archive(_read(args.template), pair_policy_from_config(config))
    _write_immutable(args.output, archive)
    print(json.dumps({"ok": True, "archiveSha256": archive["archiveSha256"], "pairRunConfigSha256": config["pairRunConfigSha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
