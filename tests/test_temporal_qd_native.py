from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch import temporal_qd_native as native
from autoresearch.result_codec import canonical_json_bytes, sha256


AUTHORITY_SHA256 = "sha256:" + "a" * 64


@pytest.fixture(autouse=True)
def _stable_native_prelaunch_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make native contract tests independent of the host running pytest."""

    monkeypatch.setattr(
        native.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(available=64 * 1024**3),
    )
    monkeypatch.setattr(
        native.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=256 * 1024**3),
    )


def _runtime_fixture() -> dict[str, object]:
    import json

    path = (
        Path(__file__).parent
        / "fixtures"
        / "temporal_qd_runtime_oracle"
        / "runtime-manifest.json"
    )
    return json.loads(path.read_text(encoding="utf-8"))


def _generation_material(tmp_path: Path) -> tuple[dict[str, object], dict[str, object]]:
    runtime = _runtime_fixture()
    archive = runtime["parentArchive"]
    ledger = runtime["identityLedger"]
    archive_path = tmp_path / "archive.json"
    ledger_path = tmp_path / "identity-ledger.json"
    archive_path.write_text(__import__("json").dumps(archive), encoding="utf-8")
    ledger_path.write_bytes(
        (
            json.dumps(
                ledger,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("ascii")
    )
    config: dict[str, object] = {
        "schemaVersion": native.PAIR_GENERATION_SCHEMA,
        "generationIndex": 1,
        "targetUniqueCandidates": 1,
        "maxProposalAttempts": 1,
        "runConfig": {
            "parentArchiveSha256": archive["archiveSha256"],
            "parameters": {},
            "evidenceIdentityContext": runtime["evidenceIdentityContext"],
        },
        "pairPolicy": runtime["bidirectionalPairPolicy"],
        "operatorImplementation": runtime["pairRunConfig"]["operatorImplementation"],
        "mutationDepthProbabilities": {"1": 0.7, "2": 0.25, "3": 0.05},
        "immigrantConstructionPolicy": runtime["pairRunConfig"][
            "immigrantConstructionPolicy"
        ],
        "globalIdentityLedger": {
            "schemaVersion": "temporal_qd_identity_ledger_v3",
            "locationPolicy": "caller_supplied_generation_global_ledger",
        },
    }
    config["configSha256"] = sha256(canonical_json_bytes(config))
    authority = native.build_generation_runtime_authority(
        pair_run_config=runtime["pairRunConfig"],
        pair_policy=runtime["bidirectionalPairPolicy"],
        evidence_identity_context=runtime["evidenceIdentityContext"],
        generation_config=config,
    )
    manifest = native.build_generation_manifest(
        authority_sha256=AUTHORITY_SHA256,
        runtime_authority=authority,
        parent_archive_path=archive_path,
        parent_archive_sha256=archive["archiveSha256"],
        identity_ledger_path=ledger_path,
        identity_ledger_sha256=ledger["ledgerSha256"],
        output_root=tmp_path / "output",
        generation_config=config,
        max_new_proposals=0,
        native_execution_timeout_seconds=3600,
        allow_empty_quality_bootstrap=True,
        g0_evaluation_width=None,
        frozen_construction_catalog=None,
        qd_version=archive["qdVersion"],
        policy_name=archive["policyName"],
        policy_sha256=archive["policySha256"],
        frozen_policy=archive["frozenPolicy"],
    )
    return manifest, config


def _version() -> dict[str, str]:
    return {
        "schemaVersion": native.NATIVE_VERSION_SCHEMA,
        "contractVersion": native.NATIVE_CONTRACT_VERSION,
        "crateVersion": "0.1.0",
        "binaryName": native.NATIVE_BINARY_NAME,
    }


def test_native_version_contract_is_closed_and_exact() -> None:
    assert native.validate_native_version(_version()) == _version()

    with pytest.raises(native.TemporalQDNativeError, match="fields are not exact"):
        native.validate_native_version({**_version(), "unexpected": "field"})

    incompatible = _version()
    incompatible["binaryName"] = "another-binary"
    with pytest.raises(native.TemporalQDNativeError, match="incompatible"):
        native.validate_native_version(incompatible)


def test_native_jsonl_contract_rejects_crlf_normalization() -> None:
    with pytest.raises(native.TemporalQDNativeError, match="canonical JSON with LF"):
        native._parse_canonical_json_line(b'{"a":1}\r\n', name="native test document")


def test_foundation_manifest_commits_to_exact_body() -> None:
    manifest = native.build_foundation_manifest(authority_sha256=AUTHORITY_SHA256)

    assert native.validate_foundation_manifest(manifest) == manifest
    assert manifest["manifestSha256"] == sha256(
        canonical_json_bytes({key: value for key, value in manifest.items() if key != "manifestSha256"})
    )

    tampered = dict(manifest)
    tampered["operation"] = "candidate_generation"
    with pytest.raises(native.TemporalQDNativeError, match="incompatible"):
        native.validate_foundation_manifest(tampered)

    tampered = dict(manifest)
    tampered["manifestSha256"] = "sha256:" + "b" * 64
    with pytest.raises(native.TemporalQDNativeError, match="identity mismatch"):
        native.validate_foundation_manifest(tampered)


def test_foundation_result_must_bind_its_manifest() -> None:
    manifest = native.build_foundation_manifest(authority_sha256=AUTHORITY_SHA256)
    result = {
        "schemaVersion": native.NATIVE_RESULT_SCHEMA,
        "contractVersion": native.NATIVE_CONTRACT_VERSION,
        "operation": native.NATIVE_OPERATION,
        "authoritySha256": manifest["authoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "status": "completed",
    }
    assert native.validate_foundation_result(result, manifest=manifest) == result

    result["manifestSha256"] = "sha256:" + "f" * 64
    with pytest.raises(native.TemporalQDNativeError, match="incompatible"):
        native.validate_foundation_result(result, manifest=manifest)


def test_native_public_exports_all_resolve() -> None:
    assert all(hasattr(native, name) for name in native.__all__)


def test_pair_generation_runtime_choice_is_self_hashed_and_binds_timeout() -> None:
    assert native.PAIR_GENERATION_RUNTIME_DEFAULT == native.PAIR_GENERATION_RUNTIME_RUST
    value = native.build_pair_generation_runtime_config(
        engine=native.PAIR_GENERATION_RUNTIME_DEFAULT
    )
    assert value["executionTimeoutSeconds"] == 3600
    assert value["fallbackPolicy"] == "forbidden"
    assert native.validate_pair_generation_runtime_config(value) == value

    tampered = dict(value)
    tampered["executionTimeoutSeconds"] = 7200
    with pytest.raises(native.TemporalQDNativeError, match="identity mismatch"):
        native.validate_pair_generation_runtime_config(tampered)

    with pytest.raises(native.TemporalQDNativeError, match="incompatible"):
        native.build_pair_generation_runtime_config(
            engine=native.PAIR_GENERATION_RUNTIME_RUST,
            execution_timeout_seconds=30,
        )


def test_native_prelaunch_resource_guard_binds_memory_and_output_volume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admitted = native._assert_native_prelaunch_resources(
        output_root=tmp_path,
        target_unique_candidates=4000,
    )
    assert admitted == {
        "hostAvailableBytes": 64 * 1024**3,
        "minimumHostAvailableBytes": 12 * 1024**3,
        "outputFreeBytes": 256 * 1024**3,
        "requiredOutputFreeBytes": 4 * 1024**3 + 4000 * 8 * 1024**2,
        "targetUniqueCandidates": 4000,
    }

    monkeypatch.setattr(
        native.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(available=11 * 1024**3),
    )
    with pytest.raises(
        native.TemporalQDNativeError,
        match="minimum_host_available_breached",
    ):
        native._assert_native_prelaunch_resources(
            output_root=tmp_path,
            target_unique_candidates=1,
        )

    monkeypatch.setattr(
        native.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(available=64 * 1024**3),
    )
    monkeypatch.setattr(
        native.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=35 * 1024**3),
    )
    with pytest.raises(
        native.TemporalQDNativeError,
        match="minimum_output_volume_free_space_breached",
    ):
        native._assert_native_prelaunch_resources(
            output_root=tmp_path,
            target_unique_candidates=4000,
        )


def test_native_command_timeout_is_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(native.TemporalQDNativeError, match="frozen 0.1s timeout"):
        native._run_checked(
            (sys.executable, "-c", "import time; time.sleep(60)"),
            cwd=tmp_path,
            timeout=0.1,
        )


def test_native_checked_command_forwards_explicit_environment(tmp_path: Path) -> None:
    environment = {**os.environ, "FUZZFOLIO_NATIVE_ENV_TEST": "forwarded"}
    completed = native._run_checked(
        (
            sys.executable,
            "-c",
            "import os; print(os.environ['FUZZFOLIO_NATIVE_ENV_TEST'])",
        ),
        cwd=tmp_path,
        timeout=5,
        env=environment,
    )
    expected = b"forwarded\r\n" if os.name == "nt" else b"forwarded\n"
    assert completed.stdout == expected


def test_native_build_uses_owned_launcher_with_cargo_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def failed_build(*_args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(dict(kwargs))
        return subprocess.CompletedProcess(
            ["cargo"], 9, stdout=b"", stderr=b"synthetic cargo failure"
        )

    monkeypatch.setattr(native, "resolve_native_batch_binary", lambda: None)
    monkeypatch.setattr(native, "_run_checked", failed_build)
    with pytest.raises(
        native.TemporalQDNativeError,
        match="failed to build native Temporal QD batch binary: synthetic cargo failure",
    ):
        native.ensure_native_batch()
    assert len(calls) == 1
    assert calls[0]["timeout"] == 600
    assert calls[0]["raise_on_nonzero"] is False
    assert isinstance(calls[0]["env"], dict)
    assert calls[0]["env"]["CARGO_BUILD_JOBS"] == "2"


def _pid_is_alive(pid: int) -> bool:
    if os.name == "nt":
        result = subprocess.run(
            ("tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"),
            capture_output=True,
            text=True,
            check=False,
        )
        return f'"{pid}"' in result.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    status = Path(f"/proc/{pid}/stat")
    if status.is_file():
        return status.read_text(encoding="utf-8").split()[2] != "Z"
    return True


def test_native_timeout_reaps_spawned_grandchild(tmp_path: Path) -> None:
    marker = tmp_path / "grandchild-pid.txt"
    grandchild = (
        "import os,pathlib,sys,time; "
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()), encoding='ascii'); "
        "time.sleep(60)"
    )
    parent = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable, '-c', {grandchild!r}, {str(marker)!r}]); "
        "time.sleep(60)"
    )
    with pytest.raises(native.TemporalQDNativeError, match="frozen 0.2s timeout"):
        native._run_checked(
            (sys.executable, "-c", parent), cwd=tmp_path, timeout=0.2
        )
    deadline = time.monotonic() + 5
    while not marker.is_file() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert marker.is_file(), "the helper did not spawn its grandchild before timeout"
    pid = int(marker.read_text(encoding="ascii"))
    while _pid_is_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not _pid_is_alive(pid), "native timeout orphaned a descendant process"


def test_native_generation_requires_caller_bound_parent_archive_identity(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "archive.json"
    archive_path.write_text("{}", encoding="utf-8")

    with pytest.raises(
        native.TemporalQDNativeError,
        match="parent archiveSha256 must be a lowercase sha256 identity",
    ):
        native.run_native_generation(
            output_root=tmp_path / "output",
            parent_archive_path=archive_path,
            parent_archive_sha256=None,
            runtime_authority={},
            generation_config={},
            identity_ledger_path=tmp_path / "identity-ledger.json",
            max_new_proposals=0,
            native_execution_timeout_seconds=3600,
            allow_empty_quality_bootstrap=True,
            g0_evaluation_width=None,
            frozen_construction_catalog=None,
            qd_version="unused",
            policy_name="unused",
            policy_sha256="sha256:" + "a" * 64,
            frozen_policy={},
        )


def test_native_generation_leaves_parent_archive_parsing_to_rust(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_path = tmp_path / "archive.json"
    archive_path.write_bytes(b"not parsed by Python")
    ledger = {"records": []}
    ledger["ledgerSha256"] = sha256(canonical_json_bytes(ledger))
    loaded: list[str] = []

    def load_facade(_path: Path, *, name: str) -> dict[str, object]:
        loaded.append(name)
        return ledger

    def stop_before_binary_resolution() -> object:
        raise native.TemporalQDNativeError("injected after facade validation")

    monkeypatch.setattr(native, "_load_json_object", load_facade)
    monkeypatch.setattr(native, "ensure_native_batch", stop_before_binary_resolution)

    with pytest.raises(native.TemporalQDNativeError, match="injected after facade"):
        native.run_native_generation(
            output_root=tmp_path / "output",
            parent_archive_path=archive_path,
            parent_archive_sha256="sha256:" + "a" * 64,
            runtime_authority={},
            generation_config={},
            identity_ledger_path=tmp_path / "identity-ledger.json",
            max_new_proposals=0,
            native_execution_timeout_seconds=3600,
            allow_empty_quality_bootstrap=True,
            g0_evaluation_width=None,
            frozen_construction_catalog=None,
            qd_version="unused",
            policy_name="unused",
            policy_sha256="sha256:" + "b" * 64,
            frozen_policy={},
        )

    assert loaded == ["native generation identity ledger"]


def test_generation_manifest_uses_only_path_and_identity_for_large_inputs(
    tmp_path: Path,
) -> None:
    manifest, _config = _generation_material(tmp_path)

    assert native.validate_generation_manifest(manifest) == manifest
    assert "parentArchive" not in manifest["runtimeAuthority"]
    assert "identityLedger" not in manifest["runtimeAuthority"]
    assert manifest["parentArchivePath"] == str((tmp_path / "archive.json").resolve())
    assert manifest["identityLedgerPath"] == str(
        (tmp_path / "identity-ledger.json").resolve()
    )
    assert manifest["nativeExecutionTimeoutSeconds"] == 3600
    assert manifest["finalNewline"] == ("crlf" if os.linesep == "\r\n" else "lf")
    runtime_authority = manifest["runtimeAuthority"]
    assert runtime_authority["pairRunConfigSha256"] == runtime_authority[
        "pairRunConfig"
    ]["pairRunConfigSha256"]
    assert runtime_authority["evidenceIdentityContextSha256"] == runtime_authority[
        "evidenceIdentityContext"
    ]["predeclaredEvidenceContextSha256"]

    substituted = dict(manifest)
    substituted["identityLedgerSha256"] = "sha256:" + "f" * 64
    substituted["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: item for key, item in substituted.items() if key != "manifestSha256"}
        )
    )
    assert (
        native.validate_generation_manifest(substituted)["manifestSha256"]
        != manifest["manifestSha256"]
    )


def test_generation_manifest_rejects_non_platform_final_newline(tmp_path: Path) -> None:
    manifest, _config = _generation_material(tmp_path)
    tampered = dict(manifest)
    tampered["finalNewline"] = "lf" if manifest["finalNewline"] == "crlf" else "crlf"
    tampered["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: item for key, item in tampered.items() if key != "manifestSha256"}
        )
    )

    with pytest.raises(native.TemporalQDNativeError, match="incompatible with this platform"):
        native.validate_generation_manifest(tampered)


@pytest.mark.parametrize(
    ("container", "identity_field"),
    [
        ("pairRunConfig", "pairRunConfigSha256"),
        ("evidenceIdentityContext", "predeclaredEvidenceContextSha256"),
    ],
)
def test_generation_manifest_rejects_nested_identity_substitution(
    tmp_path: Path, container: str, identity_field: str
) -> None:
    manifest, _config = _generation_material(tmp_path)
    tampered = __import__("copy").deepcopy(manifest)
    authority = tampered["runtimeAuthority"]
    authority[container][identity_field] = "sha256:" + "f" * 64
    authority["runtimeAuthoritySha256"] = sha256(
        canonical_json_bytes(
            {
                key: item
                for key, item in authority.items()
                if key != "runtimeAuthoritySha256"
            }
        )
    )
    tampered["runtimeAuthoritySha256"] = authority["runtimeAuthoritySha256"]
    tampered["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: item for key, item in tampered.items() if key != "manifestSha256"}
        )
    )

    with pytest.raises(native.TemporalQDNativeError, match="embedded identity mismatch"):
        native.validate_generation_manifest(tampered)


@pytest.mark.parametrize(
    ("container", "material_field"),
    [
        ("pairRunConfig", "schemaVersion"),
        ("evidenceIdentityContext", "baseDecisionTimeframe"),
    ],
)
def test_generation_manifest_rejects_nested_material_tamper(
    tmp_path: Path, container: str, material_field: str
) -> None:
    manifest, _config = _generation_material(tmp_path)
    tampered = __import__("copy").deepcopy(manifest)
    authority = tampered["runtimeAuthority"]
    authority[container][material_field] = "tampered"
    authority["runtimeAuthoritySha256"] = sha256(
        canonical_json_bytes(
            {
                key: item
                for key, item in authority.items()
                if key != "runtimeAuthoritySha256"
            }
        )
    )
    tampered["runtimeAuthoritySha256"] = authority["runtimeAuthoritySha256"]
    tampered["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: item for key, item in tampered.items() if key != "manifestSha256"}
        )
    )

    with pytest.raises(native.TemporalQDNativeError, match="self-hash mismatch"):
        native.validate_generation_manifest(tampered)


def test_generation_manifest_rejects_frozen_publication_policy_tamper(
    tmp_path: Path,
) -> None:
    manifest, _config = _generation_material(tmp_path)
    tampered = __import__("copy").deepcopy(manifest)
    tampered["publicationPolicy"]["frozenPolicy"]["policyName"] = "changed"
    tampered["manifestSha256"] = sha256(
        canonical_json_bytes(
            {key: item for key, item in tampered.items() if key != "manifestSha256"}
        )
    )
    with pytest.raises(native.TemporalQDNativeError, match="publication policy authority"):
        native.validate_generation_manifest(tampered)


def test_generation_result_unwrap_preserves_existing_inner_shape(tmp_path: Path) -> None:
    manifest, config = _generation_material(tmp_path)
    inner = {
        "schemaVersion": native.PAIR_GENERATION_PROGRESS_SCHEMA,
        "configSha256": config["configSha256"],
        "proposalCount": 0,
        "acceptedCount": 0,
        "maxProposalAttempts": 1,
        "terminationReason": "max_new_proposals_reached",
        "completed": False,
    }
    result = {
        "schemaVersion": native.NATIVE_GENERATION_RESULT_SCHEMA,
        "contractVersion": native.NATIVE_CONTRACT_VERSION,
        "operation": native.NATIVE_GENERATION_OPERATION,
        "status": "progress",
        "authoritySha256": manifest["authoritySha256"],
        "manifestSha256": manifest["manifestSha256"],
        "runtimeAuthoritySha256": manifest["runtimeAuthoritySha256"],
        "parentArchiveSha256": manifest["parentArchiveSha256"],
        "inputIdentityLedgerSha256": manifest["identityLedgerSha256"],
        "outputIdentityLedgerSha256": manifest["identityLedgerSha256"],
        "generationConfigSha256": manifest["generationConfigSha256"],
        "pairGenerationResult": inner,
    }
    result["resultSha256"] = sha256(canonical_json_bytes(result))

    checked = native.validate_generation_result(result, manifest=manifest)

    assert checked["pairGenerationResult"] is inner
    assert checked["pairGenerationResult"] == inner


def test_qd_batch_progress_injects_path_inputs_and_repairs_caller_ledger(
    tmp_path: Path,
) -> None:
    manifest, _config = _generation_material(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")
    workspace = native.native_workspace_manifest()
    completed = subprocess.run(
        (
            "cargo",
            "run",
            "--quiet",
            "--locked",
            "--jobs",
            "2",
            "--manifest-path",
            str(workspace),
            "-p",
            native.NATIVE_BINARY_NAME,
            "--",
            "--manifest",
            str(manifest_path.resolve()),
        ),
        cwd=workspace.parents[2],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
        timeout=120,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["status"] == "progress"
    assert result["pairGenerationResult"]["completed"] is False
    repaired = json.loads((tmp_path / "identity-ledger.json").read_text())
    assert repaired["ledgerSha256"] == result["outputIdentityLedgerSha256"]
    assert result["inputIdentityLedgerSha256"] == manifest["identityLedgerSha256"]
    expected_ledger = (
        json.dumps(
            repaired,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")
    assert (tmp_path / "identity-ledger.json").read_bytes() == expected_ledger
    assert (tmp_path / "output" / "pair-config.json").read_bytes() == (
        canonical_json_bytes(manifest["generationConfig"]) + os.linesep.encode("ascii")
    )
    private_checkpoint = (
        tmp_path / "output" / "internal" / "checkpoints" / "00000000.json"
    ).read_bytes()
    assert private_checkpoint.endswith(b"\n")
    assert not private_checkpoint.endswith(b"\r\n")


def test_native_binary_override_requires_an_existing_regular_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(native.NATIVE_BINARY_ENV, raising=False)
    assert native.resolve_native_batch_binary() is None

    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"not executed by this narrow resolver test")
    monkeypatch.setenv(native.NATIVE_BINARY_ENV, str(binary))
    assert native.resolve_native_batch_binary() == binary.resolve()

    binary.unlink()
    with pytest.raises(native.TemporalQDNativeError, match="regular file"):
        native.resolve_native_batch_binary()


def test_native_authority_hash_binds_source_and_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = tmp_path / "temporal-qd-batch.exe"
    binary.write_bytes(b"exact executable")
    monkeypatch.setattr(native, "native_source_sha256", lambda: "sha256:" + "c" * 64)

    authority = native.build_native_authority(binary=binary, version=_version())

    assert authority["sourceSha256"] == "sha256:" + "c" * 64
    assert authority["executableSha256"] == native._sha256_file(binary)
    assert authority["authoritySha256"] == sha256(
        canonical_json_bytes({key: value for key, value in authority.items() if key != "authoritySha256"})
    )


def _publication_temporaries(target: Path) -> list[Path]:
    return list(target.parent.glob(target.name + ".*.tmp"))


def test_immutable_publication_orders_payload_link_and_directory_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"
    events: list[str] = []
    original_write = native._write_payload
    original_sync = native._sync_payload
    original_link = native.os.link

    def write(handle: object, payload: bytes) -> None:
        events.append("payload-write")
        original_write(handle, payload)

    def sync(handle: object) -> None:
        events.append("payload-sync")
        original_sync(handle)

    def link(source: Path, destination: Path) -> None:
        events.append("publish-link")
        original_link(source, destination)

    monkeypatch.setattr(native, "_write_payload", write)
    monkeypatch.setattr(native, "_sync_payload", sync)
    monkeypatch.setattr(native.os, "link", link)
    monkeypatch.setattr(
        native,
        "fsync_directory",
        lambda _parent: events.append("directory-sync") or True,
    )

    native._write_bytes_once(target, b"exact\n")

    assert events == ["payload-write", "payload-sync", "publish-link", "directory-sync"]
    assert target.read_bytes() == b"exact\n"
    assert _publication_temporaries(target) == []


@pytest.mark.parametrize("failure_point", ["write", "sync", "link"])
def test_publication_failures_never_leave_a_partial_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    target = tmp_path / "result.json"
    stale = tmp_path / "result.json.interrupted.tmp"
    stale.write_bytes(b"unowned-stale")

    def fail(*_args: object, **_kwargs: object) -> None:
        raise OSError(f"injected {failure_point} failure")

    if failure_point == "write":
        monkeypatch.setattr(native, "_write_payload", fail)
    elif failure_point == "sync":
        monkeypatch.setattr(native, "_sync_payload", fail)
    else:
        monkeypatch.setattr(native.os, "link", fail)

    expected_error = "publish" if failure_point == "link" else failure_point
    with pytest.raises((OSError, native.TemporalQDNativeError), match=expected_error):
        native._write_bytes_once(target, b"exact\n")

    assert not target.exists()
    assert stale.read_bytes() == b"unowned-stale"
    assert _publication_temporaries(target) == [stale]


def test_directory_sync_failure_is_reported_after_safe_publication_and_recoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"

    def fail_directory_sync(_parent: Path) -> bool:
        raise OSError("injected directory-sync failure")

    monkeypatch.setattr(native, "fsync_directory", fail_directory_sync)
    with pytest.raises(native.TemporalQDNativeError, match="synchronize.*directory"):
        native._write_bytes_once(target, b"exact\n")

    assert target.read_bytes() == b"exact\n"
    assert _publication_temporaries(target) == []

    syncs: list[Path] = []
    monkeypatch.setattr(
        native,
        "fsync_directory",
        lambda parent: syncs.append(parent) or True,
    )
    native._write_bytes_once(target, b"exact\n")
    assert syncs == [tmp_path]


def test_documented_windows_directory_sync_fallback_is_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(native, "fsync_directory", lambda _parent: False)
    monkeypatch.setattr(native, "_allows_unsupported_directory_sync", lambda: True)

    target = tmp_path / "result.json"
    native._write_bytes_once(target, b"exact\n")
    assert target.read_bytes() == b"exact\n"


def test_unsupported_posix_directory_sync_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(native, "fsync_directory", lambda _parent: False)
    monkeypatch.setattr(native, "_allows_unsupported_directory_sync", lambda: False)

    with pytest.raises(native.TemporalQDNativeError, match="could not be synchronized"):
        native._write_bytes_once(tmp_path / "result.json", b"exact\n")


def test_sharing_violation_retries_are_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"
    calls = 0
    sleeps: list[float] = []

    def sharing_violation(_source: Path, _target: Path) -> None:
        nonlocal calls
        calls += 1
        raise OSError("injected sharing violation")

    monkeypatch.setattr(native.os, "link", sharing_violation)
    monkeypatch.setattr(native, "_is_windows_sharing_violation", lambda _error: True)
    monkeypatch.setattr(native.time, "sleep", sleeps.append)

    with pytest.raises(native.TemporalQDNativeError, match="publish immutable"):
        native._write_bytes_once(target, b"exact\n")

    assert calls == len(native._SHARING_RETRY_DELAYS_SECONDS) + 1
    assert sleeps == list(native._SHARING_RETRY_DELAYS_SECONDS)
    assert not target.exists()
    assert _publication_temporaries(target) == []


def test_transient_sharing_violation_converges_without_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"
    original_link = native.os.link
    calls = 0

    def link(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OSError("injected sharing violation")
        original_link(source, destination)

    monkeypatch.setattr(native.os, "link", link)
    monkeypatch.setattr(native, "_is_windows_sharing_violation", lambda _error: True)
    monkeypatch.setattr(native.time, "sleep", lambda _delay: None)

    native._write_bytes_once(target, b"exact\n")
    assert calls == 3
    assert target.read_bytes() == b"exact\n"


def test_existing_divergent_result_is_never_replaced(tmp_path: Path) -> None:
    target = tmp_path / "result.json"
    target.write_bytes(b"attacker\n")

    with pytest.raises(native.TemporalQDNativeError, match="divergent"):
        native._write_bytes_once(target, b"exact\n")

    assert target.read_bytes() == b"attacker\n"
    assert _publication_temporaries(target) == []


def test_target_swap_during_link_is_refused_without_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"

    def adversarial_link(_source: Path, destination: Path) -> None:
        destination.write_bytes(b"attacker\n")
        raise FileExistsError("injected target swap")

    monkeypatch.setattr(native.os, "link", adversarial_link)

    with pytest.raises(native.TemporalQDNativeError, match="divergent"):
        native._write_bytes_once(target, b"exact\n")

    assert target.read_bytes() == b"attacker\n"
    assert _publication_temporaries(target) == []


def test_target_swap_after_link_is_detected_by_post_sync_reverification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"

    def adversarial_directory_sync(_parent: Path) -> bool:
        target.unlink()
        target.write_bytes(b"attacker\n")
        return True

    monkeypatch.setattr(native, "fsync_directory", adversarial_directory_sync)

    with pytest.raises(native.TemporalQDNativeError, match="divergent"):
        native._write_bytes_once(target, b"exact\n")

    assert target.read_bytes() == b"attacker\n"
    assert _publication_temporaries(target) == []


def test_owned_temp_cleanup_refuses_a_swapped_unknown_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"
    swapped_temporary: Path | None = None

    def adversarial_link(source: Path, _destination: Path) -> None:
        nonlocal swapped_temporary
        source.unlink()
        source.write_bytes(b"unknown-file")
        swapped_temporary = source
        raise OSError("injected link failure after temp swap")

    monkeypatch.setattr(native.os, "link", adversarial_link)

    with pytest.raises(native.TemporalQDNativeError, match="could not be safely removed"):
        native._write_bytes_once(target, b"exact\n")

    assert swapped_temporary is not None
    assert swapped_temporary.read_bytes() == b"unknown-file"
    assert not target.exists()


def test_existing_symlink_target_is_rejected(tmp_path: Path) -> None:
    victim = tmp_path / "victim.json"
    victim.write_bytes(b"victim\n")
    target = tmp_path / "result.json"
    try:
        target.symlink_to(victim)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    with pytest.raises(native.TemporalQDNativeError, match="regular file"):
        native._write_bytes_once(target, b"exact\n")
    assert victim.read_bytes() == b"victim\n"


def test_symlink_parent_is_rejected(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink creation is unavailable: {exc}")

    with pytest.raises(native.TemporalQDNativeError, match="real directory"):
        native._write_bytes_once(linked_parent / "result.json", b"exact\n")
    assert list(real_parent.iterdir()) == []


def test_reparse_target_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "result.json"
    target.write_bytes(b"attacker\n")
    original_lstat = native.os.lstat

    def lstat(path: Path | str) -> object:
        status = original_lstat(path)
        if Path(path) == target:
            return SimpleNamespace(
                st_mode=status.st_mode,
                st_dev=status.st_dev,
                st_ino=status.st_ino,
                st_file_attributes=0x0400,
            )
        return status

    monkeypatch.setattr(native.os, "lstat", lstat)
    with pytest.raises(native.TemporalQDNativeError, match="regular file"):
        native._write_bytes_once(target, b"exact\n")
