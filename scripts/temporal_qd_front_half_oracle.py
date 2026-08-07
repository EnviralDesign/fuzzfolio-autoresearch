"""Export and compare deterministic Temporal-QD front-half oracle fixtures.

This utility intentionally does not run proposal generation.  It reads an
already completed Python (or candidate-native) run root, copies the public
front-half semantic files byte-for-byte, and derives a canonical JSON witness
from the same files.  The witness makes an intentionally different private
storage representation comparable without weakening byte-exact checks for the
public tree.

Performance telemetry and private compact-storage directories are deliberately
out of scope.  They are operational evidence, not front-half search semantics.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any


FIXTURE_SCHEMA = "temporal_qd_front_half_oracle_fixture_v1"
WITNESS_SCHEMA = "temporal_qd_front_half_semantic_witness_v1"
COMPARE_SCHEMA = "temporal_qd_front_half_oracle_comparison_v1"
BOUNDED_COMPARE_SCHEMA = "temporal_qd_front_half_bounded_exact_comparison_v1"

# These shapes are deliberately fixed admission labels, rather than an
# implementation-controlled tuning input.  A fixture binds one exact shape.
FIXED_SHAPE_METADATA: dict[int, dict[str, Any]] = {
    1: {
        "label": "unit_exhaustive",
        "pythonOracleReplay": "all_proposals_and_candidates",
        "admissionRole": "exhaustive_branch_and_fault_coverage",
    },
    8: {
        "label": "small_restart",
        "pythonOracleReplay": "all_proposals_and_candidates",
        "admissionRole": "seed_order_and_split_restart_coverage",
    },
    64: {
        "label": "bounded_real_authority",
        "pythonOracleReplay": "all_proposals_and_candidates",
        "admissionRole": "full_oracle_and_fault_admission",
    },
    128: {
        "label": "medium_real_authority",
        "pythonOracleReplay": "all_proposals_and_candidates",
        "admissionRole": "full_oracle_and_restart_admission",
    },
    1024: {
        "label": "production_shape",
        "pythonOracleReplay": "all_for_kernel_admission_sampled_only_for_post_admission_monitoring",
        "admissionRole": "full_scale_kernel_admission",
    },
}

PUBLIC_ROOT_FILES = frozenset(
    {
        "pair-config.json",
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
        "identity-ledger.json",
    }
)
PUBLIC_TREE_DIRECTORIES = frozenset({"proposal-journal", "g0-bootstrap"})
INTERNAL_TOP_LEVEL_DIRECTORIES = frozenset(
    {
        "performance",
        "oracle-admission",
        "object-store",
        "objects",
        "sealed-proposal-journal",
        "internal",
    }
)
_SHA_PREFIX = "sha256:"
_CHUNK_BYTES = 1024 * 1024


class OracleFixtureError(ValueError):
    """A source tree or persisted oracle fixture violates the admission contract."""


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise OracleFixtureError("witness value must be finite canonical JSON") from exc


def _canonical_sha256(value: Any) -> str:
    return _SHA_PREFIX + hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return _SHA_PREFIX + hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_BYTES):
            digest.update(chunk)
    return _SHA_PREFIX + digest.hexdigest()


def _safe_relative_path(relative: Path | PurePosixPath | str) -> str:
    candidate = PurePosixPath(str(relative).replace("\\", "/"))
    if (
        not candidate.parts
        or candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or ":" in candidate.parts[0]
    ):
        raise OracleFixtureError(f"public semantic path is unsafe: {relative!r}")
    return candidate.as_posix()


def _read_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OracleFixtureError(f"{label} is not readable JSON: {path}") from exc


def _is_public_semantic_file(root: Path, path: Path) -> bool:
    relative = Path(path.relative_to(root))
    if len(relative.parts) == 1:
        return relative.name in PUBLIC_ROOT_FILES
    if relative.parts[0] in INTERNAL_TOP_LEVEL_DIRECTORIES:
        return False
    return relative.parts[0] in PUBLIC_TREE_DIRECTORIES and path.suffix == ".json"


def public_semantic_paths(root: Path | str) -> tuple[Path, ...]:
    """Return the deterministic, explicitly public front-half tree for ``root``."""

    source = Path(root).resolve()
    if not source.is_dir() or source.is_symlink():
        raise OracleFixtureError(f"source root must be a real directory: {source}")
    paths = [
        path
        for path in source.rglob("*")
        if path.is_file() and not path.is_symlink() and _is_public_semantic_file(source, path)
    ]
    return tuple(sorted(paths, key=lambda path: _safe_relative_path(path.relative_to(source))))


def _artifact_record(root: Path, path: Path) -> dict[str, Any]:
    payload = path.read_bytes()
    value = _read_json(path, label="public semantic artifact")
    normalized = _canonical_json(value).encode("utf-8")
    return {
        "path": _safe_relative_path(path.relative_to(root)),
        "byteLength": len(payload),
        "fileSha256": _sha256_bytes(payload),
        "normalizedJsonByteLength": len(normalized),
        "semanticSha256": _sha256_bytes(normalized),
    }


def build_semantic_witness(root: Path | str, *, shape: int) -> dict[str, Any]:
    """Derive a normalized semantic witness without copying private run state."""

    if shape not in FIXED_SHAPE_METADATA:
        raise OracleFixtureError(f"unsupported fixed oracle shape: {shape}")
    source = Path(root).resolve()
    artifacts = [_artifact_record(source, path) for path in public_semantic_paths(source)]
    if not artifacts:
        raise OracleFixtureError("source root has no public front-half semantic artifacts")
    witness = {
        "schemaVersion": WITNESS_SCHEMA,
        "shape": int(shape),
        "shapeMetadata": FIXED_SHAPE_METADATA[shape],
        "artifacts": artifacts,
    }
    witness["semanticTreeSha256"] = _canonical_sha256(
        {
            "shape": witness["shape"],
            "shapeMetadata": witness["shapeMetadata"],
            "artifacts": [
                {
                    "path": artifact["path"],
                    "semanticSha256": artifact["semanticSha256"],
                }
                for artifact in artifacts
            ],
        }
    )
    return witness


def _require_sha(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.startswith(_SHA_PREFIX) or len(value) != 71:
        raise OracleFixtureError(f"{label} must be a sha256 identity")
    if any(character not in "0123456789abcdef" for character in value[len(_SHA_PREFIX) :]):
        raise OracleFixtureError(f"{label} must be lowercase hexadecimal")
    return value


def validate_semantic_witness(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a persisted witness and return a detached canonical copy."""

    if not isinstance(value, Mapping) or value.get("schemaVersion") != WITNESS_SCHEMA:
        raise OracleFixtureError("semantic witness schema is invalid")
    shape = value.get("shape")
    if isinstance(shape, bool) or not isinstance(shape, int) or shape not in FIXED_SHAPE_METADATA:
        raise OracleFixtureError("semantic witness shape is not a fixed admission shape")
    if value.get("shapeMetadata") != FIXED_SHAPE_METADATA[shape]:
        raise OracleFixtureError("semantic witness shape metadata drifted")
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise OracleFixtureError("semantic witness artifacts are missing")
    normalized_artifacts: list[dict[str, Any]] = []
    previous_path: str | None = None
    for artifact in artifacts:
        if not isinstance(artifact, Mapping):
            raise OracleFixtureError("semantic witness artifact is not an object")
        path = _safe_relative_path(str(artifact.get("path") or ""))
        if previous_path is not None and path <= previous_path:
            raise OracleFixtureError("semantic witness artifacts are not strictly path-sorted")
        previous_path = path
        row: dict[str, Any] = {"path": path}
        for key in ("byteLength", "normalizedJsonByteLength"):
            number = artifact.get(key)
            if isinstance(number, bool) or not isinstance(number, int) or number < 0:
                raise OracleFixtureError(f"semantic witness artifact {key} is invalid")
            row[key] = number
        row["fileSha256"] = _require_sha(artifact.get("fileSha256"), label="artifact file SHA-256")
        row["semanticSha256"] = _require_sha(artifact.get("semanticSha256"), label="artifact semantic SHA-256")
        normalized_artifacts.append(row)
    expected_tree_sha = _canonical_sha256(
        {
            "shape": shape,
            "shapeMetadata": FIXED_SHAPE_METADATA[shape],
            "artifacts": [
                {"path": row["path"], "semanticSha256": row["semanticSha256"]}
                for row in normalized_artifacts
            ],
        }
    )
    if value.get("semanticTreeSha256") != expected_tree_sha:
        raise OracleFixtureError("semantic witness tree identity mismatch")
    return {
        "schemaVersion": WITNESS_SCHEMA,
        "shape": shape,
        "shapeMetadata": dict(FIXED_SHAPE_METADATA[shape]),
        "artifacts": normalized_artifacts,
        "semanticTreeSha256": expected_tree_sha,
    }


def _first_json_difference(left: Any, right: Any, *, pointer: str = "") -> dict[str, Any] | None:
    if type(left) is not type(right):
        return {"pointer": pointer or "/", "reason": "json_type", "left": type(left).__name__, "right": type(right).__name__}
    if isinstance(left, Mapping):
        left_keys = sorted(str(key) for key in left)
        right_keys = sorted(str(key) for key in right)
        if left_keys != right_keys:
            return {
                "pointer": pointer or "/",
                "reason": "object_keys",
                "left": left_keys,
                "right": right_keys,
            }
        for key in left_keys:
            child = _first_json_difference(
                left[key], right[key], pointer=f"{pointer}/{key.replace('~', '~0').replace('/', '~1')}"
            )
            if child is not None:
                return child
        return None
    if isinstance(left, list):
        if len(left) != len(right):
            return {"pointer": pointer or "/", "reason": "array_length", "left": len(left), "right": len(right)}
        for index, (left_value, right_value) in enumerate(zip(left, right, strict=True)):
            child = _first_json_difference(left_value, right_value, pointer=f"{pointer}/{index}")
            if child is not None:
                return child
        return None
    if left != right:
        return {"pointer": pointer or "/", "reason": "value", "left": _preview(left), "right": _preview(right)}
    return None


def _preview(value: Any) -> str:
    encoded = _canonical_json(value)
    return encoded if len(encoded) <= 240 else encoded[:237] + "..."


def _records_by_path(witness: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(row["path"]): row for row in witness["artifacts"]}


def _bounded_byte_records(root: Path) -> tuple[dict[str, Any], ...]:
    """Hash the public tree without materializing any artifact payload.

    This is the admission-safe comparator for production shapes.  Exact bytes
    imply exact JSON semantics, so the successful path does not need to decode
    a potentially multi-gigabyte population.  A byte divergence fails closed
    and is reported by path and streaming digest; callers that need a rich JSON
    pointer for a small fixture can continue to use :func:`compare_roots`.
    """

    return tuple(
        {
            "path": _safe_relative_path(path.relative_to(root)),
            "byteLength": path.stat().st_size,
            "fileSha256": _sha256_file(path),
        }
        for path in public_semantic_paths(root)
    )


def compare_roots_bounded_exact(
    left_root: Path | str,
    right_root: Path | str,
    *,
    shape: int,
) -> dict[str, Any]:
    """Compare exact public bytes with memory bounded by one hash chunk.

    The public file list is small (one row per proposal plus fixed root/G0
    artifacts), while every file body is streamed in ``_CHUNK_BYTES`` chunks.
    A mismatch is deliberately considered a semantic mismatch too: admission
    requires byte identity and therefore never attempts an unbounded fallback
    JSON normalization.
    """

    if shape not in FIXED_SHAPE_METADATA:
        raise OracleFixtureError(f"unsupported fixed oracle shape: {shape}")
    left = Path(left_root).resolve()
    right = Path(right_root).resolve()
    left_records = _bounded_byte_records(left)
    right_records = _bounded_byte_records(right)
    left_by_path = {str(row["path"]): row for row in left_records}
    right_by_path = {str(row["path"]): row for row in right_records}
    left_paths = sorted(left_by_path)
    right_paths = sorted(right_by_path)
    result: dict[str, Any] = {
        "schemaVersion": BOUNDED_COMPARE_SCHEMA,
        "shape": shape,
        "comparisonMode": "streaming_public_byte_exact_fail_closed",
        "leftByteTreeSha256": _canonical_sha256(
            {"shape": shape, "artifacts": list(left_records)}
        ),
        "rightByteTreeSha256": _canonical_sha256(
            {"shape": shape, "artifacts": list(right_records)}
        ),
        "semanticExact": False,
        "byteExact": False,
        "firstDivergence": None,
    }
    if left_paths != right_paths:
        result["firstDivergence"] = {
            "kind": "public_tree_file_set",
            "leftOnly": sorted(set(left_paths) - set(right_paths)),
            "rightOnly": sorted(set(right_paths) - set(left_paths)),
        }
        return result
    for relative in left_paths:
        left_record = left_by_path[relative]
        right_record = right_by_path[relative]
        if left_record == right_record:
            continue
        result["firstDivergence"] = {
            "kind": "public_artifact_bytes",
            "path": relative,
            "leftByteLength": left_record["byteLength"],
            "rightByteLength": right_record["byteLength"],
            "leftFileSha256": left_record["fileSha256"],
            "rightFileSha256": right_record["fileSha256"],
        }
        return result
    result["semanticExact"] = True
    result["byteExact"] = True
    return result


def compare_roots(left_root: Path | str, right_root: Path | str, *, shape: int) -> dict[str, Any]:
    """Compare two run roots and report the first deterministic divergence."""

    left = Path(left_root).resolve()
    right = Path(right_root).resolve()
    left_witness = validate_semantic_witness(build_semantic_witness(left, shape=shape))
    right_witness = validate_semantic_witness(build_semantic_witness(right, shape=shape))
    left_records = _records_by_path(left_witness)
    right_records = _records_by_path(right_witness)
    left_paths = sorted(left_records)
    right_paths = sorted(right_records)
    result: dict[str, Any] = {
        "schemaVersion": COMPARE_SCHEMA,
        "shape": shape,
        "leftSemanticTreeSha256": left_witness["semanticTreeSha256"],
        "rightSemanticTreeSha256": right_witness["semanticTreeSha256"],
        "semanticExact": left_witness["semanticTreeSha256"] == right_witness["semanticTreeSha256"],
        "byteExact": False,
        "firstDivergence": None,
    }
    if left_paths != right_paths:
        result["firstDivergence"] = {
            "kind": "public_tree_file_set",
            "leftOnly": sorted(set(left_paths) - set(right_paths)),
            "rightOnly": sorted(set(right_paths) - set(left_paths)),
        }
        return result
    first_byte_difference: dict[str, Any] | None = None
    for relative in left_paths:
        left_record = left_records[relative]
        right_record = right_records[relative]
        if left_record["fileSha256"] == right_record["fileSha256"]:
            continue
        if left_record["semanticSha256"] == right_record["semanticSha256"]:
            if first_byte_difference is None:
                first_byte_difference = {
                    "kind": "public_artifact_bytes",
                    "path": relative,
                    "leftFileSha256": left_record["fileSha256"],
                    "rightFileSha256": right_record["fileSha256"],
                    "semanticSha256": left_record["semanticSha256"],
                }
            continue
        left_value = _read_json(left / relative, label="left public semantic artifact")
        right_value = _read_json(right / relative, label="right public semantic artifact")
        result["firstDivergence"] = {
            "kind": "semantic_json",
            "path": relative,
            "leftSemanticSha256": left_record["semanticSha256"],
            "rightSemanticSha256": right_record["semanticSha256"],
            **(_first_json_difference(left_value, right_value) or {"pointer": "/", "reason": "canonical_json"}),
        }
        return result
    if first_byte_difference is not None:
        result["firstDivergence"] = first_byte_difference
        return result
    result["byteExact"] = True
    return result


def export_fixture(*, source_root: Path | str, output_root: Path | str, shape: int) -> dict[str, Any]:
    """Copy public semantic bytes and persist their independently validated witness."""

    source = Path(source_root).resolve()
    output = Path(output_root).resolve()
    if source == output:
        raise OracleFixtureError("fixture output root cannot equal the source root")
    if output.exists() and any(output.iterdir()):
        raise OracleFixtureError(f"fixture output root must be absent or empty: {output}")
    witness = validate_semantic_witness(build_semantic_witness(source, shape=shape))
    tree_root = output / "public-semantic-tree"
    output.mkdir(parents=True, exist_ok=True)
    try:
        for artifact in witness["artifacts"]:
            relative = Path(str(artifact["path"]))
            destination = tree_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / relative, destination)
            if _sha256_file(destination) != artifact["fileSha256"]:
                raise OracleFixtureError(f"fixture copy diverged for {artifact['path']}")
        fixture = {
            "schemaVersion": FIXTURE_SCHEMA,
            "shape": shape,
            "shapeMetadata": FIXED_SHAPE_METADATA[shape],
            "publicSemanticTree": witness["artifacts"],
            "semanticWitness": witness,
        }
        fixture["fixtureSha256"] = _canonical_sha256(fixture)
        (output / "oracle-fixture.json").write_text(
            _canonical_json(fixture) + "\n", encoding="utf-8", newline="\n"
        )
        return fixture
    except BaseException:
        # A failed fixture is intentionally left in place for inspection.  A
        # later export refuses a non-empty root instead of silently replacing
        # possibly useful forensic bytes.
        raise


def validate_fixture(path: Path | str) -> dict[str, Any]:
    """Validate a fixture manifest and every copied public semantic file."""

    fixture_path = Path(path).resolve()
    fixture_root = fixture_path.parent
    fixture = _read_json(fixture_path, label="oracle fixture")
    if not isinstance(fixture, dict) or fixture.get("schemaVersion") != FIXTURE_SCHEMA:
        raise OracleFixtureError("oracle fixture schema is invalid")
    witness = fixture.get("semanticWitness")
    if not isinstance(witness, Mapping):
        raise OracleFixtureError("oracle fixture has no semantic witness")
    validated = validate_semantic_witness(witness)
    if fixture.get("shape") != validated["shape"] or fixture.get("shapeMetadata") != validated["shapeMetadata"]:
        raise OracleFixtureError("oracle fixture shape binding drifted")
    if fixture.get("publicSemanticTree") != validated["artifacts"]:
        raise OracleFixtureError("oracle fixture public tree differs from its witness")
    expected = _canonical_sha256({key: value for key, value in fixture.items() if key != "fixtureSha256"})
    if fixture.get("fixtureSha256") != expected:
        raise OracleFixtureError("oracle fixture identity mismatch")
    tree_root = fixture_root / "public-semantic-tree"
    for artifact in validated["artifacts"]:
        copied_path = tree_root / str(artifact["path"])
        if not copied_path.is_file() or copied_path.is_symlink():
            raise OracleFixtureError(
                f"oracle fixture public semantic artifact is missing: {artifact['path']}"
            )
        if copied_path.stat().st_size != artifact["byteLength"]:
            raise OracleFixtureError(
                f"oracle fixture public semantic artifact length drifted: {artifact['path']}"
            )
        if _sha256_file(copied_path) != artifact["fileSha256"]:
            raise OracleFixtureError(
                f"oracle fixture public semantic artifact bytes drifted: {artifact['path']}"
            )
    return fixture


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    export_parser = subparsers.add_parser("export", help="export one Python-oracle fixture")
    export_parser.add_argument("--source-root", required=True, type=Path)
    export_parser.add_argument("--output-root", required=True, type=Path)
    export_parser.add_argument("--shape", required=True, type=int, choices=sorted(FIXED_SHAPE_METADATA))
    compare_parser = subparsers.add_parser("compare", help="compare two front-half run roots")
    compare_parser.add_argument("--left-root", required=True, type=Path)
    compare_parser.add_argument("--right-root", required=True, type=Path)
    compare_parser.add_argument("--shape", required=True, type=int, choices=sorted(FIXED_SHAPE_METADATA))
    validate_parser = subparsers.add_parser("validate", help="validate an exported fixture")
    validate_parser.add_argument("--fixture", required=True, type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.command == "export":
        result = export_fixture(source_root=args.source_root, output_root=args.output_root, shape=args.shape)
    elif args.command == "compare":
        result = compare_roots(args.left_root, args.right_root, shape=args.shape)
        if result["byteExact"] is not True:
            print(json.dumps(result, indent=2, sort_keys=True))
            return 2
    else:
        result = validate_fixture(args.fixture)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
