"""Extract one compact, reproducible v5 construction oracle fixture.

The source proposal root is intentionally read-only.  This tool takes a single
real rich v5 entry from the stopped campaign and retains only the frozen
construction inputs plus the selected evaluation-profile projection needed for
native parity tests.  It never enumerates or rewrites the campaign tree.

The checked-in fixture is deliberately small: it is *not* a copy of a rich
proposal or a 4,000-candidate fixture.  In particular, the old
``bidirectionalGenome`` blob is excluded.  The native path must construct a
compact journal first and materialize rich data only for an admitted evaluation
candidate.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from autoresearch.result_codec import canonical_json_bytes


FIXTURE_SCHEMA = "temporal_qd_v5_stopped_run_oracle_fixture_v1"
DEFAULT_SOURCE_ROOT = Path(
    r"C:\fuzzfolio-research\temporal-qd-v5-fresh-4000x1024x5-20260810-v1\run"
    r"\broad-4000x1024x5\generations\generation-0001\proposal"
)
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "temporal_qd_v5_stopped_run_oracle.json"
)
DEFAULT_AUTHORITY_OUTPUT = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "temporal_qd_v5_shared_authority_oracle.json.gz"
)
AUTHORITY_FIXTURE_SCHEMA = "temporal_qd_v5_shared_authority_oracle_fixture_v1"


class FixtureError(RuntimeError):
    """The source stopped-run fixture cannot be safely reduced."""


def _real_regular_file(path: Path, *, name: str) -> None:
    try:
        status = path.lstat()
    except OSError as exc:
        raise FixtureError(f"could not inspect {name}: {path}") from exc
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    if (
        stat.S_ISLNK(status.st_mode)
        or bool(attributes & reparse_point)
        or not stat.S_ISREG(status.st_mode)
    ):
        raise FixtureError(f"{name} is not a real regular file: {path}")


def _read_json(path: Path, *, name: str) -> tuple[bytes, dict[str, Any]]:
    _real_regular_file(path, name=name)
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, ValueError) as exc:
        raise FixtureError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise FixtureError(f"{name} must be a JSON object")
    return payload, value


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _mapping(value: object, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FixtureError(f"{name} must be an object")
    return dict(value)


def _sha(value: object, *, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise FixtureError(f"{name} must be a canonical SHA-256 identity")
    return value


def _fixture_material(
    *, source_root: Path, ordinal: int
) -> dict[str, Any]:
    if ordinal < 0:
        raise FixtureError("ordinal must be nonnegative")
    pair_config_path = source_root / "pair-config.json"
    entry_path = source_root / "proposal-journal" / f"{ordinal:08d}.json"
    pair_config_bytes, pair_config = _read_json(pair_config_path, name="pair config")
    entry_bytes, entry = _read_json(entry_path, name="proposal journal entry")

    proposal = _mapping(entry.get("proposal"), name="rich proposal")
    factory_pair = _mapping(proposal.get("factoryPair"), name="rich factory pair")
    candidate = _mapping(entry.get("candidate"), name="rich candidate")
    construction: dict[str, Any] = {
        "generationConfigSha256": _sha(
            entry.get("configSha256"), name="entry configSha256"
        ),
        "generationIndex": entry.get("generationIndex"),
        "proposalOrdinal": entry.get("proposalOrdinal"),
        "proposalSeed": proposal.get("proposalSeed"),
        "originKind": proposal.get("originKind"),
        "sides": {},
        "selectedEvaluationProjection": {
            key: candidate.get(key)
            for key in (
                "candidateId",
                "candidateIdentitySha256",
                "programSha256",
                "sourceProfile",
                "sourceProfileSha256",
                "profileSnapshotSha256",
                "proposalOrdinal",
                "sourceMode",
                "seedId",
            )
        },
    }
    if (
        construction["generationIndex"] != 1
        or construction["proposalOrdinal"] != ordinal
        or not isinstance(construction["proposalSeed"], str)
        or not construction["proposalSeed"].startswith("sha256:")
        or construction["originKind"] != "random_immigrant"
    ):
        raise FixtureError("source entry is not a v5 G0 random immigrant")
    if pair_config.get("configSha256") != construction["generationConfigSha256"]:
        raise FixtureError("pair config/source entry config identity drifted")
    for side in ("long", "short"):
        module = _mapping(factory_pair.get(side), name=f"rich {side} module")
        grammar_context = _mapping(
            module.get("grammarContext"), name=f"rich {side} grammar context"
        )
        grammar_payload = _mapping(
            grammar_context.get("payload"), name=f"rich {side} grammar payload"
        )
        program = _mapping(module.get("program"), name=f"rich {side} program")
        identities = _mapping(module.get("identities"), name=f"rich {side} identities")
        context = _mapping(
            grammar_payload.get("context"), name=f"rich {side} grammar context body"
        )
        if grammar_payload.get("side") != side or program.get("direction") != side:
            raise FixtureError(f"rich {side} context/program direction drifted")
        construction["sides"][side] = {
            "context": context,
            "budget": _mapping(program.get("budget"), name=f"rich {side} budget"),
            "program": program,
            "programSha256": _sha(
                identities.get("programSha256"), name=f"rich {side} program SHA-256"
            ),
        }
    selected = _mapping(
        construction["selectedEvaluationProjection"],
        name="selected evaluation projection",
    )
    required_selected = (
        "candidateId",
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfile",
        "sourceProfileSha256",
        "profileSnapshotSha256",
        "proposalOrdinal",
        "sourceMode",
        "seedId",
    )
    if any(selected.get(key) is None for key in required_selected):
        raise FixtureError("source candidate lacks selected evaluation projection fields")
    return {
        "schemaVersion": FIXTURE_SCHEMA,
        "source": {
            "campaign": "temporal-qd-v5-fresh-4000x1024x5-20260810-v1",
            "proposalRoot": str(source_root),
            "pairConfigRelativePath": "pair-config.json",
            "pairConfigFileSha256": _sha256_bytes(pair_config_bytes),
            "journalEntryRelativePath": f"proposal-journal/{ordinal:08d}.json",
            "journalEntryFileSha256": _sha256_bytes(entry_bytes),
            "journalEntrySha256": _sha(entry.get("entrySha256"), name="entrySha256"),
        },
        "construction": construction,
    }


def build_fixture(*, source_root: Path, ordinal: int) -> dict[str, Any]:
    value = _fixture_material(source_root=source_root, ordinal=ordinal)
    value["fixtureSha256"] = "placeholder"
    value["fixtureSha256"] = "sha256:" + hashlib.sha256(
        canonical_json_bytes({key: item for key, item in value.items() if key != "fixtureSha256"})
    ).hexdigest()
    return value


def build_authority_fixture(*, source_root: Path, ordinal: int) -> dict[str, Any]:
    """Seal a real static v5 closure once, without changing the stopped run."""

    from autoresearch.temporal_qd_v5_native import (
        build_v5_frozen_authority,
        build_v5_native_operator_authority,
    )

    run_root = source_root.parents[2]
    config_bytes, config = _read_json(run_root / "config.json", name="run config")
    source = _mapping(
        config.get("bidirectionalPairSourceAuthority"), name="pair source authority"
    )
    evolvable = _mapping(
        config.get("evolvableModuleAuthority"), name="evolvable module authority"
    )
    qd_version = config.get("qdVersion")
    if not isinstance(qd_version, str) or not qd_version:
        raise FixtureError("run config lacks QD version")
    # This is the closed pair-policy form; it is intentionally constructed
    # directly rather than opening the old Python compiler/factory.
    pair_policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": source.get("pairCompilerAuthority"),
    }
    native_operator = build_v5_native_operator_authority(
        pair_source_authority=source,
        evolvable_module_authority=evolvable,
    )
    sealed = build_v5_frozen_authority(
        pair_source_authority=source,
        evolvable_module_authority=evolvable,
        bidirectional_pair_policy=pair_policy,
        native_operator_authority=native_operator,
        qd_engine_version=qd_version,
    )
    entry_path = source_root / "proposal-journal" / f"{ordinal:08d}.json"
    _, entry = _read_json(entry_path, name="proposal journal entry")
    factory_pair = _mapping(
        _mapping(entry.get("proposal"), name="proposal").get("factoryPair"),
        name="factory pair",
    )
    public_policy_snapshots = {
        side: _mapping(factory_pair.get(side), name=f"factory {side}").get("policy")
        for side in ("long", "short")
    }
    if not all(isinstance(value, Mapping) for value in public_policy_snapshots.values()):
        raise FixtureError("factory pair lacks public policy snapshots")
    value: dict[str, Any] = {
        "schemaVersion": AUTHORITY_FIXTURE_SCHEMA,
        "source": {
            "campaign": "temporal-qd-v5-fresh-4000x1024x5-20260810-v1",
            "runConfigRelativePath": "config.json",
            "runConfigFileSha256": _sha256_bytes(config_bytes),
            "proposalRoot": str(source_root),
            "journalEntryRelativePath": f"proposal-journal/{ordinal:08d}.json",
        },
        "authorityInputs": {
            "pairSourceAuthority": source,
            "evolvableModuleAuthority": evolvable,
            "bidirectionalPairPolicy": pair_policy,
            "nativeOperatorAuthority": native_operator,
            "qdEngineVersion": qd_version,
        },
        "sealedAuthority": sealed,
        "publicPolicySnapshots": public_policy_snapshots,
    }
    value["fixtureSha256"] = "placeholder"
    value["fixtureSha256"] = "sha256:" + hashlib.sha256(
        canonical_json_bytes({key: item for key, item in value.items() if key != "fixtureSha256"})
    ).hexdigest()
    return value


def _write_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise FixtureError(f"refusing to overwrite divergent fixture: {path}")
        return
    temporary = path.with_name(path.name + ".tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _replace_generated_fixture(path: Path, payload: bytes) -> None:
    """Atomically replace only a fixture regenerated from the read-only oracle.

    This is intentionally opt-in.  It exists because an authority-schema
    migration legitimately changes the compressed checked-in fixture while
    preserving the stopped campaign as a read-only source of truth.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        _real_regular_file(path, name="existing generated authority fixture")
    temporary = path.with_name(path.name + ".tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _gzip_canonical(value: Mapping[str, Any]) -> bytes:
    return gzip.compress(canonical_json_bytes(value) + b"\n", compresslevel=9, mtime=0)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--ordinal", type=int, default=0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--authority-output", type=Path, default=DEFAULT_AUTHORITY_OUTPUT)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="compare the existing fixture to the source without writing",
    )
    parser.add_argument(
        "--replace-authority-output",
        action="store_true",
        help="atomically replace only the generated shared-authority fixture",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.verify and args.replace_authority_output:
        raise FixtureError("--verify and --replace-authority-output are incompatible")
    fixture = build_fixture(
        source_root=args.source_root.resolve(), ordinal=int(args.ordinal)
    )
    payload = canonical_json_bytes(fixture) + b"\n"
    authority_fixture = build_authority_fixture(
        source_root=args.source_root.resolve(), ordinal=int(args.ordinal)
    )
    authority_payload = _gzip_canonical(authority_fixture)
    if args.verify:
        _real_regular_file(args.output, name="oracle fixture")
        if args.output.read_bytes() != payload:
            raise FixtureError("checked-in oracle fixture differs from stopped run")
        _real_regular_file(args.authority_output, name="shared authority oracle fixture")
        if args.authority_output.read_bytes() != authority_payload:
            raise FixtureError("checked-in shared authority fixture differs from stopped run")
    else:
        _write_once(args.output, payload)
        if args.replace_authority_output:
            _replace_generated_fixture(args.authority_output, authority_payload)
        else:
            _write_once(args.authority_output, authority_payload)
    print(json.dumps({"fixture": str(args.output), "fixtureSha256": fixture["fixtureSha256"]}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except FixtureError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
