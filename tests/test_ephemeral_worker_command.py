"""Tests for generate-ephemeral-worker-command."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from autoresearch.ephemeral_worker_sessions import (
    DEFAULT_REQUIRED_CAPABILITY,
    EphemeralSessionError,
)
from autoresearch.ephemeral_worker_command import (
    ENROLLMENT_TOKEN_PLACEHOLDER,
    build_launch_command,
    build_mint_request,
    build_redacted_launch_command,
    compute_script_sha256,
    run_generate_ephemeral_worker_command,
)

CONTRACT = "sha256:" + ("a" * 64)
IMAGE = "lucasmorgan/fuzzfolio-replay-worker:sha-235b61f62fc0"
AUTHORITY = {
    "authority_id": "sha256:" + ("d" * 64),
    "bound_contract": {
        "operator_launch_worker_image": IMAGE,
        "worker_contract_sha256": CONTRACT,
    },
}


def _args(**overrides) -> argparse.Namespace:
    payload = {
        "authority_path": Path("authority.json"),
        "duration": "3m",
        "workers": "1",
        "max_workers": 2,
        "enrollment_ttl": "20m",
        "minimum_free_disk_gb": 5.0,
        "registration_timeout": "5m",
        "bootstrap_url": "",
        "public_gateway_url": "http://host.docker.internal:8799",
        "enrollment_url": "http://127.0.0.1:8799/ephemeral-sessions/redeem",
        "admin_gateway_url": "http://127.0.0.1:8799",
        "local_bootstrap_script": Path(
            r"C:\repos\Trading-Dashboard\backend\app\resources\ephemeral_worker_session.ps1"
        ),
        "local_smoke": True,
        "copy": False,
        "print_command": False,
        "json_redacted": True,
        "dry_run": False,
        "keep_image": True,
    }
    payload.update(overrides)
    return argparse.Namespace(**payload)


def test_compute_script_sha256_missing_file_uses_zero_placeholder() -> None:
    assert compute_script_sha256(Path("missing-bootstrap.ps1")) == "sha256:" + ("0" * 64)


def test_build_launch_command_local_smoke_shape() -> None:
    command = build_launch_command(
        enrollment_url="http://127.0.0.1:8799/ephemeral-sessions/redeem",
        enrollment_token="secret-token",
        duration="3m",
        workers=1,
        max_workers=2,
        keep_image=True,
        local_bootstrap_script=Path(
            r"C:\repos\Trading-Dashboard\backend\app\resources\ephemeral_worker_session.ps1"
        ),
        bootstrap_url=None,
    )
    assert command.startswith(
        '& "C:\\repos\\Trading-Dashboard\\backend\\app\\resources\\ephemeral_worker_session.ps1"'
    )
    assert '-EnrollmentToken "secret-token"' in command
    assert '-Duration "3m"' in command
    assert '-Workers "1"' in command
    assert "-MaxWorkers 2" in command
    assert command.endswith("-KeepImage")


def test_build_launch_command_bootstrap_url_uses_irm_form() -> None:
    command = build_launch_command(
        enrollment_url="https://playhand-lab.example.com/ephemeral-sessions/redeem",
        enrollment_token="secret-token",
        duration="2h",
        workers="auto",
        max_workers=6,
        keep_image=False,
        local_bootstrap_script=Path("ignored.ps1"),
        bootstrap_url="https://backend.example.com/api/worker-gateway/ephemeral-bootstrap.ps1",
    )
    assert "Invoke-WebRequest" in command
    assert " -OutFile $p" in command
    assert "ephemeral-bootstrap.ps1" in command
    assert "& $p -EnrollmentUrl" in command
    assert "-Workers auto" in command
    assert "\n" not in command
    assert "-KeepImage" not in command


def test_apply_profile_defaults_wan_uses_irm_urls() -> None:
    from autoresearch.ephemeral_worker_command import (
        DEFAULT_BOOTSTRAP_URL,
        DEFAULT_ENROLLMENT_URL,
        DEFAULT_PUBLIC_GATEWAY_URL,
        apply_profile_defaults,
    )

    args = apply_profile_defaults(
        argparse.Namespace(
            local_smoke=False,
            public_gateway_url=None,
            enrollment_url=None,
            bootstrap_url=None,
            minimum_free_disk_gb=None,
            max_workers=6,
        )
    )
    assert args.bootstrap_url == DEFAULT_BOOTSTRAP_URL
    assert args.enrollment_url == DEFAULT_ENROLLMENT_URL
    assert args.public_gateway_url == DEFAULT_PUBLIC_GATEWAY_URL
    assert args.minimum_free_disk_gb == 30.0


def test_dry_run_wan_command_shape_uses_irm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(AUTHORITY), encoding="utf-8")
    monkeypatch.setattr(
        "autoresearch.ephemeral_worker_command.resolve_script_sha256",
        lambda **_kwargs: "sha256:" + ("0" * 64),
    )
    exit_code = run_generate_ephemeral_worker_command(
        _args(
            authority_path=authority_path,
            dry_run=True,
            json_redacted=True,
            local_smoke=False,
            bootstrap_url=None,
            public_gateway_url=None,
            enrollment_url=None,
            minimum_free_disk_gb=None,
            max_workers=6,
        )
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert "Invoke-WebRequest" in payload["command_shape"]
    assert "backend.enviral-design.com" in payload["command_shape"]
    assert "playhand-lab.enviral-design.com" in payload["command_shape"]
    assert ENROLLMENT_TOKEN_PLACEHOLDER in payload["command_shape"]
    assert "\n" not in payload["command_shape"]


def test_build_redacted_launch_command() -> None:
    command = '& "script.ps1" -EnrollmentToken "abc123"'
    assert build_redacted_launch_command(command, "abc123") == (
        '& "script.ps1" -EnrollmentToken "<enrollment-token>"'
    )


def test_build_mint_request_includes_authority_binding() -> None:
    binding = {
        "authority_id": AUTHORITY["authority_id"],
        "image": IMAGE,
        "expected_worker_contract": CONTRACT,
        "required_capabilities": [DEFAULT_REQUIRED_CAPABILITY],
    }
    request = build_mint_request(
        binding=binding,
        duration_seconds=180,
        workers=1,
        max_workers=2,
        enrollment_ttl_seconds=1200,
        public_gateway_url="http://host.docker.internal:8799",
        enrollment_url="http://127.0.0.1:8799/ephemeral-sessions/redeem",
        script_sha256="sha256:" + ("0" * 64),
        minimum_free_disk_gb=5.0,
        registration_timeout_seconds=300,
        remove_image_when_safe=False,
    )
    assert request["authority_id"] == binding["authority_id"]
    assert request["minimum_free_disk_gb"] == 5.0
    assert request["remove_image_when_safe"] is False


def test_dry_run_validates_authority_and_redacts_without_mint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(AUTHORITY), encoding="utf-8")

    def _fail_mint(**_kwargs):
        raise AssertionError("mint should not be called during dry-run")

    monkeypatch.setattr(
        "autoresearch.ephemeral_worker_command.mint_ephemeral_session",
        _fail_mint,
    )

    exit_code = run_generate_ephemeral_worker_command(
        _args(authority_path=authority_path, dry_run=True, json_redacted=True)
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["status"] == "dry_run"
    assert payload["command_shape"]
    assert ENROLLMENT_TOKEN_PLACEHOLDER in payload["command_shape"]
    assert "secret-token" not in captured.out
    assert payload["duration"] == "PT3M"


def test_duration_validation_rejects_below_minimum(tmp_path: Path) -> None:
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(AUTHORITY), encoding="utf-8")

    with pytest.raises(EphemeralSessionError, match="duration_below_minimum"):
        run_generate_ephemeral_worker_command(
            _args(authority_path=authority_path, duration="1m", dry_run=True)
        )


def test_mint_success_redacts_json_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(AUTHORITY), encoding="utf-8")

    monkeypatch.setattr(
        "autoresearch.ephemeral_worker_command.load_lab_gateway_token",
        lambda create=False: "durable-admin-token",
    )
    monkeypatch.setattr(
        "autoresearch.ephemeral_worker_command.mint_ephemeral_session",
        lambda **kwargs: {
            "session_id": "ews-test",
            "enrollment_token": "one-time-enrollment-token",
            "enrollment_expires_at": "2026-07-24T00:00:00Z",
            "deadline": "2026-07-24T00:03:00Z",
            "pool": "ephemeral-windows-deadbeefcafe",
        },
    )

    exit_code = run_generate_ephemeral_worker_command(
        _args(authority_path=authority_path, json_redacted=True)
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["status"] == "minted"
    assert payload["session_id"] == "ews-test"
    assert "one-time-enrollment-token" not in captured.out
    assert ENROLLMENT_TOKEN_PLACEHOLDER in payload["command_shape"]
