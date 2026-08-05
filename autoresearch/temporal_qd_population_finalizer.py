"""Native journal-backed finalization for temporal QD population artifacts.

Python remains the semantic oracle.  It authors the canonical shell and exact
journal identity manifest; the narrow Rust executable may only validate those
inputs, splice already-canonical candidate bytes, derive the population hash,
and atomically install the write-once population artifact.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any

from .temporal_bidirectional_genome import canonical_json, canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError

POPULATION_FINALIZER_PYTHON = "python"
POPULATION_FINALIZER_RUST = "rust"
POPULATION_FINALIZERS = frozenset(
    (POPULATION_FINALIZER_PYTHON, POPULATION_FINALIZER_RUST)
)
RUST_FINALIZER_CONTRACT_VERSION = "temporal_qd_population_finalizer_v1"
_MANIFEST_SCHEMA = "temporal_qd_population_finalizer_manifest_v1"
_POPULATION_SHA_PLACEHOLDER = (
    "sha256:" + ("0" * 64)
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _rust_manifest() -> Path:
    return (
        _repo_root()
        / "rust"
        / "temporal-qd-population-finalizer"
        / "Cargo.toml"
    )


def _rust_binary() -> Path:
    suffix = ".exe" if sys.platform.startswith("win") else ""
    return (
        _rust_manifest().parent
        / "target"
        / "release"
        / f"temporal-qd-population-finalizer{suffix}"
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _source_sha256() -> str:
    crate_root = _rust_manifest().parent
    sources = [crate_root / "Cargo.toml", crate_root / "Cargo.lock"]
    sources.extend(sorted((crate_root / "src").rglob("*.rs")))
    digest = hashlib.sha256()
    for path in sources:
        if not path.exists():
            raise TemporalDiscoveryContractError(
                f"Rust population finalizer source is incomplete: {path}"
            )
        relative = path.relative_to(crate_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        payload = path.read_bytes()
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return "sha256:" + digest.hexdigest()


def _run_checked(command: Sequence[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise TemporalDiscoveryContractError(
            f"Rust population finalizer command failed ({completed.returncode}): {detail}"
        )
    return completed


@lru_cache(maxsize=1)
def ensure_rust_population_finalizer() -> tuple[Path, dict[str, Any]]:
    """Build or resolve one exact release binary; never fall back to Python."""

    override = os.environ.get("FUZZFOLIO_TEMPORAL_QD_POPULATION_FINALIZER")
    if override:
        binary = Path(override).expanduser().resolve()
        if not binary.is_file():
            raise TemporalDiscoveryContractError(
                "FUZZFOLIO_TEMPORAL_QD_POPULATION_FINALIZER is not a file"
            )
    else:
        manifest = _rust_manifest()
        if not manifest.is_file():
            raise TemporalDiscoveryContractError(
                f"Rust population finalizer manifest is absent: {manifest}"
            )
        _run_checked(
            (
                "cargo",
                "build",
                "--quiet",
                "--release",
                "--locked",
                "--manifest-path",
                str(manifest),
            ),
            cwd=_repo_root(),
        )
        binary = _rust_binary().resolve()
        if not binary.is_file():
            raise TemporalDiscoveryContractError(
                f"Rust population finalizer build did not produce {binary}"
            )
    version = _run_checked((str(binary), "--version-json"), cwd=_repo_root())
    try:
        version_payload = json.loads(version.stdout)
    except json.JSONDecodeError as exc:
        raise TemporalDiscoveryContractError(
            "Rust population finalizer returned invalid version JSON"
        ) from exc
    if (
        not isinstance(version_payload, dict)
        or version_payload.get("contractVersion")
        != RUST_FINALIZER_CONTRACT_VERSION
    ):
        raise TemporalDiscoveryContractError(
            "Rust population finalizer contract version is incompatible"
        )
    authority = {
        "schemaVersion": "temporal_qd_population_finalizer_authority_v1",
        "contractVersion": RUST_FINALIZER_CONTRACT_VERSION,
        "crateVersion": version_payload.get("crateVersion"),
        "buildProfile": "release",
        "executableSha256": _sha256_file(binary),
        "sourceSha256": _source_sha256(),
    }
    authority["authoritySha256"] = canonical_sha256(authority)
    return binary, authority


def _write_bytes_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise TemporalDiscoveryContractError(
                f"refusing to overwrite divergent population-finalizer artifact: {path}"
            )
        return
    temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            if path.read_bytes() != payload:
                raise TemporalDiscoveryContractError(
                    f"refusing to overwrite divergent population-finalizer artifact: {path}"
                )
            temporary.unlink(missing_ok=True)
            return
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _write_canonical_once(path: Path, value: Mapping[str, Any]) -> None:
    _write_bytes_once(
        path,
        (canonical_json(value) + os.linesep).encode("utf-8"),
    )


def finalize_population_with_rust(
    *,
    output_root: Path | str,
    population_without_sha: Mapping[str, Any],
    expected_entry_sha256s: Sequence[str],
    accepted_candidates: Sequence[Mapping[str, Any]],
    g0_bootstrap: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Finalize exactly one population from immutable proposal journal bytes."""

    root = Path(output_root)
    binary, authority = ensure_rust_population_finalizer()
    finalizer_root = root / "performance" / "population-finalizer"
    authority_path = finalizer_root / "authority.json"
    shell_path = finalizer_root / "population-shell.json"
    manifest_path = finalizer_root / "manifest.json"
    _write_canonical_once(authority_path, authority)

    shell = dict(population_without_sha)
    shell["candidates"] = []
    shell["populationSha256"] = _POPULATION_SHA_PLACEHOLDER
    shell_bytes = (canonical_json(shell) + os.linesep).encode("utf-8")
    _write_bytes_once(shell_path, shell_bytes)

    references = [
        {
            "proposalOrdinal": int(reference["proposalOrdinal"]),
            "candidateId": str(reference["candidateId"]),
            "candidateIdentitySha256": str(reference["candidateIdentitySha256"]),
        }
        for reference in accepted_candidates
    ]
    g0_artifacts: dict[str, Any] | None = None
    if g0_bootstrap is not None:
        artifact_root = root / "g0-bootstrap"
        paths = {
            "acceptedPool": artifact_root / "accepted-pool.json",
            "selection": artifact_root / "selection.json",
            "ledger": artifact_root / "campaign-construction-ledger.json",
        }
        try:
            pool = json.loads(paths["acceptedPool"].read_text(encoding="utf-8"))
            selection = json.loads(paths["selection"].read_text(encoding="utf-8"))
            ledger = json.loads(paths["ledger"].read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise TemporalDiscoveryContractError("G0 native finalizer artifacts are unavailable") from exc
        if (
            pool.get("constructionPoolIdentitySha256") != g0_bootstrap["constructionPoolIdentitySha256"]
            or pool.get("acceptedPoolSha256") != g0_bootstrap["acceptedPoolSha256"]
            or selection.get("selectionSha256") != g0_bootstrap["selectionSha256"]
            or selection.get("campaignLedgerSha256") != g0_bootstrap["ledgerSha256"]
            or ledger.get("ledgerSha256") != g0_bootstrap["ledgerSha256"]
        ):
            raise TemporalDiscoveryContractError("G0 native finalizer artifact identity drift")
        refs = {str(row.get("referenceSha256")): row for row in pool.get("acceptedReferences") or [] if isinstance(row, Mapping)}
        selected = list(selection.get("selected") or [])
        selected_manifest = []
        for selected_row in selected:
            ref = refs.get(str(selected_row.get("referenceSha256"))) if isinstance(selected_row, Mapping) else None
            if not isinstance(ref, Mapping) or any(ref.get(key) != selected_row.get(key) for key in ("proposalOrdinal", "candidateId", "candidateIdentitySha256", "referenceSha256")):
                raise TemporalDiscoveryContractError("G0 native finalizer selected reference drift")
            selected_manifest.append({
                "proposalOrdinal": int(ref["proposalOrdinal"]), "candidateId": str(ref["candidateId"]),
                "candidateIdentitySha256": str(ref["candidateIdentitySha256"]),
                "acceptedPairEntrySha256": str(ref["acceptedPairEntrySha256"]), "referenceSha256": str(ref["referenceSha256"]),
            })
        if sorted(references, key=lambda row: int(row["proposalOrdinal"])) != sorted([{key: row[key] for key in ("proposalOrdinal", "candidateId", "candidateIdentitySha256")} for row in selected_manifest], key=lambda row: int(row["proposalOrdinal"])):
            raise TemporalDiscoveryContractError("G0 native finalizer candidates are not the authoritative selection")
        references = selected_manifest
        g0_artifacts = {
            key: {"path": "../../g0-bootstrap/" + path.name, "fileSha256": _sha256_file(path)}
            for key, path in paths.items()
        }
    manifest: dict[str, Any] = {
        "schemaVersion": _MANIFEST_SCHEMA,
        "contractVersion": RUST_FINALIZER_CONTRACT_VERSION,
        "configSha256": str(population_without_sha["configSha256"]),
        "generationIndex": int(population_without_sha["generationIndex"]),
        "expectedProposalCount": len(expected_entry_sha256s),
        "expectedEntrySha256s": [str(value) for value in expected_entry_sha256s],
        "acceptedCandidates": references,
        "candidateCount": len(references),
        "journalDirectory": "../../proposal-journal",
        "outputPath": "../../population.json",
        "populationShellPath": "population-shell.json",
        "populationShellFileSha256": (
            "sha256:" + hashlib.sha256(shell_bytes).hexdigest()
        ),
        "finalNewline": "crlf" if os.linesep == "\r\n" else "lf",
    }
    # The journal is deliberately wider than the evaluated first generation.
    # Rust still scans and authenticates every immutable construction entry,
    # but copies only the closed selector subset.  These identities make the
    # subset authority part of the native finalization contract.
    if g0_bootstrap is not None:
        required = {
            "constructionPoolIdentitySha256",
            "acceptedPoolSha256",
            "selectionSha256",
            "ledgerSha256",
        }
        if set(g0_bootstrap) != required or any(
            not isinstance(g0_bootstrap[key], str)
            or not g0_bootstrap[key].startswith("sha256:")
            for key in required
        ):
            raise TemporalDiscoveryContractError("G0 native finalizer binding is invalid")
        manifest["g0Bootstrap"] = dict(g0_bootstrap)
        manifest["g0Artifacts"] = g0_artifacts
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_canonical_once(manifest_path, manifest)

    completed = _run_checked(
        (str(binary), "--manifest", str(manifest_path.resolve())),
        cwd=_repo_root(),
    )
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TemporalDiscoveryContractError(
            "Rust population finalizer returned invalid result JSON"
        ) from exc
    if (
        not isinstance(result, dict)
        or result.get("contractVersion") != RUST_FINALIZER_CONTRACT_VERSION
        or result.get("candidateCount") != len(references)
        or not isinstance(result.get("populationSha256"), str)
        or int(result.get("encodedBytes", -1)) < 1
    ):
        raise TemporalDiscoveryContractError(
            "Rust population finalizer returned an invalid result contract"
        )
    return result


__all__ = [
    "POPULATION_FINALIZERS",
    "POPULATION_FINALIZER_PYTHON",
    "POPULATION_FINALIZER_RUST",
    "RUST_FINALIZER_CONTRACT_VERSION",
    "ensure_rust_population_finalizer",
    "finalize_population_with_rust",
]
