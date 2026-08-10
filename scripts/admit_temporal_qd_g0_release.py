"""Release-gate one existing v5 G0 proposal root through the Rust transaction.

This is deliberately not a supervisor or campaign entry point.  It derives
the compact G0 request from the run's frozen authority, calls the real native
bridge exactly once, and records canonical performance evidence outside the
immutable proposal artifacts.  It never proposes, evaluates, queues, or
contacts an economic worker.

``audit`` means a receiptless full native source admission (normally used for
an existing Python-oracle public tree), so it seals the same ordinary receipt
that ``adopt`` can immediately verify.  It does not use the Rust diagnostic
``audit=true`` request variant, whose distinct request identity is unsuitable
for a release receipt that must subsequently be adopted.
"""

from __future__ import annotations

import argparse
import os
import stat
import sys
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

import psutil

from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
import autoresearch.temporal_qd_native as native
from autoresearch.temporal_qd_native import (
    G0_FINALIZATION_RUNTIME_RUST,
    G0_LEGACY_V5_RUNTIME_MIGRATION_PATH,
    PAIR_GENERATION_RUNTIME_PYTHON,
    TemporalQDNativeError,
    build_g0_finalization_runtime_config,
    derive_legacy_v5_g0_finalization_runtime,
    ensure_native_batch,
    run_native_g0_funnel,
    seal_legacy_v5_g0_runtime_migration,
    validate_g0_finalization_runtime_config,
    validate_pair_generation_runtime_config,
)
from autoresearch.result_codec import canonical_json_bytes


RELEASE_EVIDENCE_SCHEMA = "temporal_qd_g0_release_admission_evidence_v1"
_MODE_FRESH = "fresh"
_MODE_AUDIT = "audit"
_MODE_ADOPT = "adopt"
_MODES = (_MODE_FRESH, _MODE_AUDIT, _MODE_ADOPT)
_PUBLIC_ARTIFACTS = (
    "g0-bootstrap/accepted-pool.json",
    "g0-bootstrap/selection.json",
    "g0-bootstrap/campaign-construction-ledger.json",
    "population.json",
    "evaluation-population.json",
    "generation-journal.json",
)
_SOURCE_JOURNAL_DIRECTORY = "proposal-journal"
_G0_RECEIPT_RELATIVE = Path("internal") / "g0-funnel" / "receipt.json"


class _NativeProcessTreeTelemetry:
    """Bounded best-effort process-tree telemetry for one qd-batch command."""

    def __init__(self, *, sample_interval_seconds: float = 0.025) -> None:
        self._sample_interval_seconds = sample_interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._root: psutil.Process | None = None
        self._root_pid: int | None = None
        self._lock = threading.Lock()
        self._started = False
        self._samples = 0
        self._peak_rss_bytes = 0
        self._peak_private_bytes: int | None = None
        self._peak_commit_bytes: int | None = None
        self._cpu_seconds = 0.0
        self._read_bytes = 0
        self._write_bytes = 0
        self._private_metric: str | None = None
        self._commit_metric: str | None = None

    def start(self, process: Any) -> None:
        """Called by the bridge after tree containment but before Windows resume."""

        with self._lock:
            if self._started:
                raise RuntimeError("native release telemetry started twice")
            self._root = psutil.Process(int(process.pid))
            self._root_pid = int(process.pid)
            self._started = True
        self.sample()
        self._thread = threading.Thread(
            target=self._sample_until_stopped,
            name="temporal-qd-g0-release-telemetry",
            daemon=True,
        )
        self._thread.start()

    def _sample_until_stopped(self) -> None:
        while not self._stop.wait(self._sample_interval_seconds):
            self.sample()

    def sample(self) -> None:
        with self._lock:
            root = self._root
        if root is None:
            return
        try:
            processes = [root, *root.children(recursive=True)]
        except (psutil.Error, OSError):
            processes = []
        rss = 0
        private_total = 0
        commit_total = 0
        private_seen = False
        commit_seen = False
        cpu = 0.0
        reads = 0
        writes = 0
        for process in {item.pid: item for item in processes}.values():
            try:
                memory = process.memory_info()
                rss += int(memory.rss)
                times = process.cpu_times()
                cpu += float(times.user + times.system)
                io = process.io_counters()
                reads += int(io.read_bytes)
                writes += int(io.write_bytes)
            except (psutil.Error, OSError):
                continue
            try:
                full = process.memory_full_info()
            except (psutil.Error, OSError, AttributeError):
                full = None
            if full is not None:
                private = getattr(full, "private", None)
                private_name = "private"
                if not isinstance(private, int):
                    private = getattr(full, "uss", None)
                    private_name = "uss"
                if isinstance(private, int) and private >= 0:
                    private_seen = True
                    private_total += private
                    self._private_metric = private_name
            commit = getattr(memory, "pagefile", None)
            if isinstance(commit, int) and commit >= 0:
                commit_seen = True
                commit_total += commit
                self._commit_metric = "pagefile"
        with self._lock:
            self._samples += 1
            self._peak_rss_bytes = max(self._peak_rss_bytes, rss)
            self._cpu_seconds = max(self._cpu_seconds, cpu)
            self._read_bytes = max(self._read_bytes, reads)
            self._write_bytes = max(self._write_bytes, writes)
            if private_seen:
                self._peak_private_bytes = max(
                    self._peak_private_bytes or 0, private_total
                )
            if commit_seen:
                self._peak_commit_bytes = max(self._peak_commit_bytes or 0, commit_total)

    def close(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.sample()
        with self._lock:
            return {
                "measurementScope": "native_qd_batch_and_recursive_descendants",
                "sampleIntervalMilliseconds": int(self._sample_interval_seconds * 1_000),
                "sampleCount": self._samples,
                "peakRssBytes": self._peak_rss_bytes,
                "processTreeCpuSeconds": self._cpu_seconds,
                "osReadBytes": self._read_bytes,
                "osWriteBytes": self._write_bytes,
                "peakPrivateBytes": self._peak_private_bytes,
                "privateBytesMetric": self._private_metric,
                "peakCommitBytes": self._peak_commit_bytes,
                "commitBytesMetric": self._commit_metric,
                "nativeProcessObserved": self._started,
            }


def _read_canonical_object(path: Path, *, label: str) -> dict[str, Any]:
    """Use the native bridge's no-alias immutable JSON-file primitive."""

    return native._load_immutable_json_object(path, name=label)


def _check_self_hash(value: Mapping[str, Any], *, field: str, label: str) -> str:
    supplied = value.get(field)
    if not isinstance(supplied, str) or not supplied.startswith("sha256:"):
        raise TemporalDiscoveryContractError(f"{label} {field} is invalid")
    material = {key: item for key, item in value.items() if key != field}
    if canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(f"{label} {field} does not match bytes")
    return supplied


def _require_mapping(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{label} must be an object")
    return dict(value)


def _require_positive_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TemporalDiscoveryContractError(f"{label} must be a positive integer")
    return value


def _run_layout(proposal_root: Path | str) -> tuple[Path, Path, Path, Path]:
    proposal = native._require_existing_real_directory_tree(
        proposal_root, name="G0 release proposal root"
    )
    generation = proposal.parent
    generations = generation.parent
    if (
        proposal.name != "proposal"
        or generation.name != "generation-0001"
        or generations.name != "generations"
    ):
        raise TemporalDiscoveryContractError(
            "G0 release proposal root must be run/generations/generation-0001/proposal"
        )
    run_root = native._require_existing_real_directory_tree(
        generations.parent, name="G0 release run root"
    )
    if proposal != run_root / "generations" / "generation-0001" / "proposal":
        raise TemporalDiscoveryContractError("G0 release proposal root escaped its run root")
    return run_root, proposal, run_root / "config.json", run_root / "identity-ledger.json"


def _derive_g0_runtime(
    supervisor_config: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    pair_runtime = validate_pair_generation_runtime_config(
        supervisor_config.get("pairGenerationRuntime")
    )
    explicit = supervisor_config.get("g0FinalizationRuntime")
    if explicit is not None:
        runtime = validate_g0_finalization_runtime_config(explicit)
        source = {
            "kind": "frozen_g0_finalization_runtime",
            "pairGenerationRuntimeSha256": pair_runtime["runtimeSha256"],
            "runtimeSha256": runtime["runtimeSha256"],
        }
    else:
        try:
            runtime = derive_legacy_v5_g0_finalization_runtime(
                supervisor_config_sha256=supervisor_config.get("configSha256"),
                pair_generation_runtime=pair_runtime,
            )
        except TemporalQDNativeError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc
        source = {
            "kind": "singleton_pre_cutover_v5_projection",
            "pairGenerationRuntimeSha256": pair_runtime["runtimeSha256"],
            "runtimeSha256": runtime["runtimeSha256"],
        }
    if runtime["engine"] != G0_FINALIZATION_RUNTIME_RUST:
        raise TemporalDiscoveryContractError("release admission requires the Rust G0 runtime")
    if pair_runtime["engine"] != PAIR_GENERATION_RUNTIME_PYTHON:
        raise TemporalDiscoveryContractError(
            "release admission requires frozen Python v5 construction before Rust G0"
        )
    return runtime, source


def _derive_authority(proposal_root: Path | str) -> dict[str, Any]:
    run_root, proposal, config_path, ledger_path = _run_layout(proposal_root)
    config = _read_canonical_object(config_path, label="G0 release supervisor config")
    config_sha = _check_self_hash(
        config, field="configSha256", label="G0 release supervisor config"
    )
    pair_config_path = proposal / "pair-config.json"
    pair_config = _read_canonical_object(pair_config_path, label="G0 release pair config")
    pair_config_sha = _check_self_hash(
        pair_config, field="configSha256", label="G0 release pair config"
    )
    if (
        pair_config.get("schemaVersion") != native.PAIR_GENERATION_SCHEMA
        or pair_config.get("generationIndex") != 1
    ):
        raise TemporalDiscoveryContractError("G0 release pair config is not generation-1")
    runtime, runtime_source = _derive_g0_runtime(config)
    g0 = _require_mapping(config.get("g0Bootstrap"), label="frozen G0 bootstrap")
    if set(g0) != {
        "schemaVersion",
        "activation",
        "initialConstructionPoolSize",
        "evaluationPopulationSize",
    } or g0.get("schemaVersion") != "temporal_qd_g0_bootstrap_config_v1":
        raise TemporalDiscoveryContractError("frozen G0 bootstrap fields are not exact")
    construction_width = _require_positive_int(
        g0.get("initialConstructionPoolSize"), label="G0 construction width"
    )
    evaluation_width = _require_positive_int(
        g0.get("evaluationPopulationSize"), label="G0 evaluation width"
    )
    if evaluation_width > construction_width:
        raise TemporalDiscoveryContractError("G0 evaluation width exceeds construction width")
    if pair_config.get("targetUniqueCandidates") != construction_width:
        raise TemporalDiscoveryContractError("G0 construction width drifts from pair config")
    if not isinstance(pair_config.get("maxProposalAttempts"), int) or pair_config[
        "maxProposalAttempts"
    ] < construction_width:
        raise TemporalDiscoveryContractError("G0 proposal ceiling drifts from construction width")
    run_config = _require_mapping(pair_config.get("runConfig"), label="pair run config")
    pair_g0 = _require_mapping(run_config.get("g0Bootstrap"), label="pair G0 bootstrap")
    if pair_g0 != {
        "initialConstructionPoolSize": construction_width,
        "evaluationPopulationSize": evaluation_width,
    }:
        raise TemporalDiscoveryContractError("pair G0 bootstrap binding drifted")
    parameters = _require_mapping(run_config.get("parameters"), label="pair parameters")
    if (
        # v5 keeps its normal/evaluation width in the frozen search policy;
        # the pair config's target is the larger one-time construction width.
        parameters.get("targetUniqueCandidates") != evaluation_width
        or parameters.get("maxProposalAttempts") != pair_config["maxProposalAttempts"]
    ):
        raise TemporalDiscoveryContractError("pair parameter width/ceiling binding drifted")
    pair_authority = _require_mapping(
        config.get("bidirectionalPairGeneration"), label="frozen pair authority"
    )
    operator_identity = _require_mapping(
        pair_config.get("operatorImplementation"), label="pair operator identity"
    )
    if operator_identity != pair_authority.get("operatorImplementation"):
        raise TemporalDiscoveryContractError("pair operator identity drifts from frozen authority")
    evolvable = _require_mapping(
        config.get("evolvableModuleAuthority"), label="frozen evolvable authority"
    )
    archive_authority = _require_mapping(
        evolvable.get("archivePolicyAuthority"), label="frozen archive policy authority"
    )
    if run_config.get("archivePolicyAuthority") != archive_authority:
        raise TemporalDiscoveryContractError("pair archive policy authority drifted")
    frozen_policy = _require_mapping(config.get("frozenPolicy"), label="frozen policy")
    if (
        archive_authority.get("qdVersion") != config.get("qdVersion")
        or archive_authority.get("policyName") != config.get("policyName")
        or archive_authority.get("policySha256") != config.get("policySha256")
        or archive_authority.get("frozenPolicy") != frozen_policy
    ):
        raise TemporalDiscoveryContractError("archive publication authority drifted")
    ledger_config = _require_mapping(config.get("identityLedger"), label="identity ledger")
    if ledger_config.get("policySha256") != config.get("policySha256"):
        raise TemporalDiscoveryContractError("identity ledger policy binding drifted")
    native._require_regular_file(ledger_path, name="G0 release identity ledger")
    identity_policy = _require_mapping(
        frozen_policy.get("identity"), label="frozen identity policy"
    )
    identity_binding = {
        "schemaVersion": native.G0_IDENTITY_LEDGER_BINDING_SCHEMA,
        "ledgerPath": str(ledger_path.resolve()),
        "policyName": config.get("policyName"),
        "policySha256": config.get("policySha256"),
        "identityPolicy": identity_policy,
        "identityPolicySha256": canonical_sha256(identity_policy),
    }
    publication_policy = {
        "qdVersion": config.get("qdVersion"),
        "policyName": config.get("policyName"),
        "policySha256": config.get("policySha256"),
        "pairPolicy": _require_mapping(pair_config.get("pairPolicy"), label="pair policy"),
        "operatorImplementationIdentity": operator_identity,
        "predeclaredEvidenceContextSha256": None,
        "archivePolicyAuthority": archive_authority,
    }
    authority: dict[str, Any] = {
        "schemaVersion": "temporal_qd_g0_release_authority_v1",
        "runRoot": str(run_root.resolve()),
        "supervisorConfigPath": str(config_path.resolve()),
        "supervisorConfigSha256": config_sha,
        "proposalRoot": str(proposal.resolve()),
        "proposalConfigPath": str(pair_config_path.resolve()),
        "proposalConfigSha256": pair_config_sha,
        "g0FinalizationRuntime": runtime,
        "runtimeDerivation": runtime_source,
        "constructionPoolSize": construction_width,
        "evaluationPopulationSize": evaluation_width,
        "maxProposalAttempts": pair_config["maxProposalAttempts"],
        "publicationPolicy": publication_policy,
        "publicationPolicySha256": canonical_sha256(publication_policy),
        "identityLedger": identity_binding,
        "identityLedgerConfig": ledger_config,
    }
    authority["authoritySha256"] = canonical_sha256(authority)
    return authority


def _metadata_journal_inventory(proposal_root: Path) -> dict[str, Any]:
    journal_root = native._require_existing_real_directory_tree(
        proposal_root / _SOURCE_JOURNAL_DIRECTORY,
        name="G0 release proposal journal root",
    )
    file_count = 0
    declared_bytes = 0
    try:
        entries = list(os.scandir(journal_root))
    except OSError as exc:
        raise TemporalDiscoveryContractError("could not enumerate G0 proposal journal") from exc
    for entry in entries:
        if not entry.name.endswith(".json"):
            continue
        try:
            status = os.lstat(entry.path)
        except OSError as exc:
            raise TemporalDiscoveryContractError(
                f"could not inspect G0 proposal journal entry: {entry.name}"
            ) from exc
        if native._is_link_or_reparse(status) or not stat.S_ISREG(status.st_mode):
            raise TemporalDiscoveryContractError(
                f"G0 proposal journal entry is not a real regular file: {entry.name}"
            )
        file_count += 1
        declared_bytes += int(status.st_size)
    return {
        "measurement": "metadata_only_no_journal_content_read_by_harness",
        "fileCount": file_count,
        "declaredBytes": declared_bytes,
        "contentBytesReadByHarness": 0,
    }


def _public_output_inventory(proposal_root: Path) -> dict[str, Any]:
    files: dict[str, int] = {}
    for relative in _PUBLIC_ARTIFACTS:
        path = proposal_root / Path(relative)
        try:
            status = os.lstat(path)
        except OSError as exc:
            raise TemporalDiscoveryContractError(
                f"native G0 public output is absent: {relative}"
            ) from exc
        if native._is_link_or_reparse(status) or not stat.S_ISREG(status.st_mode):
            raise TemporalDiscoveryContractError(
                f"native G0 public output is not a real regular file: {relative}"
            )
        files[relative] = int(status.st_size)
    return {
        "files": files,
        "totalBytes": sum(files.values()),
        "selectedArtifactBytes": files["g0-bootstrap/selection.json"],
        "g0ArtifactBytes": sum(
            files[path]
            for path in (
                "g0-bootstrap/accepted-pool.json",
                "g0-bootstrap/selection.json",
                "g0-bootstrap/campaign-construction-ledger.json",
            )
        ),
    }


def _require_evidence_outside_proposal_root(
    *, evidence_output: Path | str, proposal_root: Path
) -> Path:
    output = Path(os.path.abspath(os.fspath(evidence_output)))
    try:
        output.relative_to(proposal_root)
    except ValueError:
        return output
    raise TemporalDiscoveryContractError(
        "release evidence must be outside immutable proposal artifacts"
    )


def _write_evidence(path: Path, value: Mapping[str, Any]) -> None:
    native._write_bytes_once(path, canonical_json_bytes(value) + b"\n")


NativeRunner = Callable[..., dict[str, Any]]
NativePreflight = Callable[[], tuple[Path, dict[str, str]]]


def run_release_admission(
    *,
    proposal_root: Path | str,
    mode: str,
    evidence_output: Path | str,
    admission_thread_cap: int = native.G0_ADMISSION_THREAD_CAP_DEFAULT,
    native_runner: NativeRunner = run_native_g0_funnel,
    native_preflight: NativePreflight = ensure_native_batch,
) -> dict[str, Any]:
    """Admit one existing proposal root and write its immutable evidence record."""

    if mode not in _MODES:
        raise TemporalDiscoveryContractError("G0 release admission mode is invalid")
    try:
        admission_thread_cap = native.validate_g0_admission_thread_cap(
            admission_thread_cap
        )
    except TemporalQDNativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    authority = _derive_authority(proposal_root)
    proposal = Path(authority["proposalRoot"])
    evidence_path = _require_evidence_outside_proposal_root(
        evidence_output=evidence_output, proposal_root=proposal
    )
    receipt_path = proposal / _G0_RECEIPT_RELATIVE
    receipt_exists = receipt_path.exists()
    if mode == _MODE_FRESH and receipt_exists:
        raise TemporalDiscoveryContractError(
            "fresh G0 release admission requires an unsealed native receipt"
        )
    if mode == _MODE_AUDIT and receipt_exists:
        raise TemporalDiscoveryContractError(
            "audit G0 release admission requires a receiptless root"
        )
    if mode == _MODE_ADOPT and not receipt_exists:
        raise TemporalDiscoveryContractError(
            "adoption G0 release admission requires a sealed native receipt"
        )
    source_inventory_before = _metadata_journal_inventory(proposal)
    preflight_started = time.perf_counter()
    binary, batch_authority = native_preflight()
    preflight_wall_seconds = time.perf_counter() - preflight_started
    batch_authority = native.validate_native_authority(batch_authority)
    telemetry = _NativeProcessTreeTelemetry()
    native_diagnostics: dict[str, Any] | None = None

    def receive_native_diagnostics(value: dict[str, Any]) -> None:
        nonlocal native_diagnostics
        if native_diagnostics is not None:
            raise TemporalDiscoveryContractError(
                "native G0 release bridge emitted duplicate diagnostics"
            )
        native_diagnostics = dict(value)

    bridge_started = time.perf_counter()
    result: dict[str, Any]
    try:
        result = native_runner(
            output_root=proposal,
            g0_finalization_runtime=authority["g0FinalizationRuntime"],
            generation_config=_read_canonical_object(
                Path(authority["proposalConfigPath"]), label="G0 release pair config"
            ),
            evaluation_population_size=authority["evaluationPopulationSize"],
            publication_policy=authority["publicationPolicy"],
            identity_ledger_binding=authority["identityLedger"],
            # A receiptless root necessarily makes the native transaction
            # stream-admit the journal.  Keep the sealed request's `audit`
            # flag false so this release audit produces the ordinary receipt
            # that the immediately following adoption timing can verify.
            # Native `audit=true` is intentionally a destructive diagnostic
            # receipt variant, not a release-admission path.
            audit=False,
            admission_thread_cap=admission_thread_cap,
            on_process_started=telemetry.start,
            on_diagnostics=receive_native_diagnostics,
        )
    finally:
        bridge_wall_seconds = time.perf_counter() - bridge_started
        process_observation = telemetry.close()
    status = result.get("status")
    expected_status = "adopted" if mode == _MODE_ADOPT else "completed"
    if status != expected_status:
        raise TemporalDiscoveryContractError(
            f"G0 release {mode} expected {expected_status}, got {status!r}"
        )
    if native_diagnostics is None:
        raise TemporalDiscoveryContractError(
            "native G0 release did not emit required external diagnostics"
        )
    if (
        native_diagnostics.get("mode") != status
        or native_diagnostics.get("threadCap") != admission_thread_cap
    ):
        raise TemporalDiscoveryContractError(
            "native G0 release diagnostics disagree with the bridge outcome"
        )
    journal_diagnostics = native_diagnostics.get("journalAdmission")
    if status == "completed":
        if (
            not isinstance(journal_diagnostics, Mapping)
            or journal_diagnostics.get("sourceBytesRead", 0) <= 0
        ):
            raise TemporalDiscoveryContractError(
                "fresh native G0 release diagnostics omitted source admission bytes"
            )
    elif journal_diagnostics is not None:
        raise TemporalDiscoveryContractError(
            "native G0 adoption diagnostics reported a journal admission"
        )
    receipt = _require_mapping(result.get("receipt"), label="native G0 receipt")
    execution_authority = _require_mapping(
        receipt.get("executionAuthority"), label="native G0 execution authority"
    )
    if execution_authority.get("nativeBatchAuthority") != batch_authority:
        raise TemporalDiscoveryContractError(
            "native G0 executable authority drifted after release preflight"
        )
    if receipt.get("configSha256") != authority["proposalConfigSha256"]:
        raise TemporalDiscoveryContractError("native G0 receipt proposal config binding drifted")
    # The native receipt binds proposal config and runtime authority. Re-open
    # the small supervisor/pair authority surfaces as well so a concurrent
    # config/policy edit cannot be represented by an earlier harness snapshot.
    if _derive_authority(proposal) != authority:
        raise TemporalDiscoveryContractError(
            "frozen G0 release authority changed during native admission"
        )
    source_inventory_after = _metadata_journal_inventory(proposal)
    if source_inventory_after != source_inventory_before:
        raise TemporalDiscoveryContractError(
            "proposal journal inventory changed during native G0 release admission"
        )
    output_inventory = _public_output_inventory(proposal)
    adoption = result.get("adoptionVerification")
    if mode == _MODE_ADOPT:
        adoption = _require_mapping(adoption, label="native G0 adoption verification")
        if adoption.get("proposalJournalBytesRead") != 0:
            raise TemporalDiscoveryContractError(
                "native G0 adoption read proposal journal bytes"
            )
    elif adoption is not None:
        raise TemporalDiscoveryContractError("non-adoption G0 release returned adoption telemetry")
    migration: dict[str, Any] | None = None
    if authority["runtimeDerivation"]["kind"] == "singleton_pre_cutover_v5_projection":
        migration = seal_legacy_v5_g0_runtime_migration(
            supervisor_config=_read_canonical_object(
                Path(authority["supervisorConfigPath"]),
                label="G0 release supervisor config",
            ),
            run_root=Path(authority["runRoot"]),
        )
    pair_result = _require_mapping(
        result.get("pairGenerationResult"), label="native G0 pair generation result"
    )
    evidence: dict[str, Any] = {
        "schemaVersion": RELEASE_EVIDENCE_SCHEMA,
        "mode": mode,
        "operation": "existing_proposal_root_native_g0_only",
        "authority": authority,
        "nativePreflight": {
            "wallSeconds": preflight_wall_seconds,
            "binaryPath": str(binary.resolve()),
            "nativeBatchAuthority": batch_authority,
        },
        "sourceJournal": source_inventory_after,
        "nativeBridge": {
            "wallSeconds": bridge_wall_seconds,
            "nativeManifestAuditFlag": False,
            "receiptlessFullNativeAudit": mode == _MODE_AUDIT,
            "admissionThreadCap": admission_thread_cap,
            "nativeDiagnostics": native_diagnostics,
            **process_observation,
        },
        "publicOutputs": output_inventory,
        "outcome": {
            "nativeStatus": status,
            "receiptSha256": receipt.get("receiptSha256"),
            "requestSha256": receipt.get("requestSha256"),
            "executionAuthoritySha256": receipt.get("authoritySha256"),
            "proposalCount": pair_result.get("proposalCount"),
            "candidateCount": pair_result.get("candidateCount"),
            "constructionPoolSize": pair_result.get("constructionPoolSize"),
            "constructedAcceptedCount": pair_result.get("constructedAcceptedCount"),
            "adoptionVerification": adoption,
            "legacyRuntimeMigrationSha256": (
                migration.get("migrationSha256") if migration is not None else None
            ),
            "legacyRuntimeMigrationPath": (
                str((proposal / G0_LEGACY_V5_RUNTIME_MIGRATION_PATH).resolve())
                if migration is not None
                else None
            ),
        },
        "economicWorkPerformed": False,
        "supervisorContinued": False,
        "reportSha256": None,
    }
    evidence["reportSha256"] = canonical_sha256(
        {key: item for key, item in evidence.items() if key != "reportSha256"}
    )
    _write_evidence(evidence_path, evidence)
    return evidence


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proposal-root", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=_MODES)
    parser.add_argument(
        "--evidence-output",
        required=True,
        type=Path,
        help="new canonical JSON path outside the proposal root",
    )
    parser.add_argument(
        "--admission-thread-cap",
        type=int,
        default=native.G0_ADMISSION_THREAD_CAP_DEFAULT,
        help=(
            "bounded native proposal-journal admission workers "
            f"(1..={native.G0_ADMISSION_THREAD_CAP_MAXIMUM}, default: "
            f"{native.G0_ADMISSION_THREAD_CAP_DEFAULT})"
        ),
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        evidence = run_release_admission(
            proposal_root=args.proposal_root,
            mode=args.mode,
            evidence_output=args.evidence_output,
            admission_thread_cap=args.admission_thread_cap,
        )
    except (TemporalDiscoveryContractError, TemporalQDNativeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        canonical_json_bytes(
            {
                "admitted": True,
                "evidence": str(Path(args.evidence_output).resolve()),
                "reportSha256": evidence["reportSha256"],
                "nativeStatus": evidence["outcome"]["nativeStatus"],
            }
        ).decode("utf-8")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
