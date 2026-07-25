"""Generate ephemeral Windows worker bootstrap commands."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from autoresearch.ephemeral_worker_sessions import (
    EphemeralSessionError,
    extract_authority_binding,
    format_duration_iso8601,
    parse_duration_seconds,
)
from autoresearch.play_hand_lab_auth import load_lab_gateway_token

# Operator/WAN defaults: remote office PCs paste an irm one-liner (no local repo).
DEFAULT_BOOTSTRAP_URL = (
    "https://backend.enviral-design.com/api/worker-gateway/ephemeral-bootstrap.ps1"
)
DEFAULT_PUBLIC_GATEWAY_URL = "https://playhand-lab.enviral-design.com"
DEFAULT_ENROLLMENT_URL = (
    "https://playhand-lab.enviral-design.com/ephemeral-sessions/redeem"
)
# Mint stays loopback against the operator's Lab Gateway process.
DEFAULT_ADMIN_GATEWAY_URL = "http://127.0.0.1:8799"
DEFAULT_LOCAL_BOOTSTRAP_SCRIPT = Path(
    r"C:\repos\Trading-Dashboard\backend\app\resources\ephemeral_worker_session.ps1"
)
LOCAL_SMOKE_PUBLIC_GATEWAY_URL = "http://host.docker.internal:8799"
LOCAL_SMOKE_ENROLLMENT_URL = "http://127.0.0.1:8799/ephemeral-sessions/redeem"
DEFAULT_MINIMUM_FREE_DISK_GB = 30.0
LOCAL_SMOKE_MINIMUM_FREE_DISK_GB = 5.0
DEFAULT_MAX_WORKERS = 6
LOCAL_SMOKE_MAX_WORKERS = 2
DEFAULT_ENROLLMENT_TTL = "20m"
DEFAULT_REGISTRATION_TIMEOUT = "5m"
DEFAULT_STATUS_INTERVAL_SECONDS = 15
DEFAULT_CLEANUP_GRACE_SECONDS = 600
ENROLLMENT_TOKEN_PLACEHOLDER = "<enrollment-token>"


def add_generate_ephemeral_worker_command_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "generate-ephemeral-worker-command",
        help=(
            "Mint an ephemeral worker enrollment and build a local PowerShell "
            "bootstrap command without printing durable secrets."
        ),
    )
    parser.add_argument("--authority-path", type=Path, required=True)
    parser.add_argument("--duration", required=True)
    parser.add_argument("--workers", default="auto")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument("--enrollment-ttl", default=DEFAULT_ENROLLMENT_TTL)
    parser.add_argument(
        "--minimum-free-disk-gb",
        type=float,
        default=None,
        help="Defaults to 30 (WAN) or 5 with --local-smoke.",
    )
    parser.add_argument("--registration-timeout", default=DEFAULT_REGISTRATION_TIMEOUT)
    parser.add_argument(
        "--bootstrap-url",
        default=None,
        help="HTTPS ephemeral-bootstrap.ps1 URL for irm. Default: production backend.",
    )
    parser.add_argument("--public-gateway-url", default=None)
    parser.add_argument("--enrollment-url", default=None)
    parser.add_argument("--admin-gateway-url", default=DEFAULT_ADMIN_GATEWAY_URL)
    parser.add_argument(
        "--local-bootstrap-script",
        type=Path,
        default=DEFAULT_LOCAL_BOOTSTRAP_SCRIPT,
    )
    parser.add_argument(
        "--local-smoke",
        action="store_true",
        help=(
            "Local Docker Desktop smoke: file bootstrap, loopback enrollment, "
            "host.docker.internal worker gateway."
        ),
    )
    parser.add_argument("--copy", action="store_true")
    parser.add_argument(
        "--print-command",
        action="store_true",
        help="Print the full command including the enrollment token (unsafe for Procman).",
    )
    parser.add_argument("--json-redacted", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    image_policy = parser.add_mutually_exclusive_group()
    image_policy.add_argument(
        "--keep-image",
        dest="keep_image",
        action="store_true",
        help="Preserve the worker image at cleanup even if it was pulled for this session.",
    )
    image_policy.add_argument(
        "--remove-image-when-safe",
        dest="keep_image",
        action="store_false",
        help="Delete the session-pulled worker image at cleanup when safe (default).",
    )
    parser.set_defaults(keep_image=False)


def apply_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    """Fill URL/disk defaults after argparse (supports --local-smoke)."""
    local_smoke = bool(getattr(args, "local_smoke", False))
    if local_smoke:
        if args.public_gateway_url is None:
            args.public_gateway_url = LOCAL_SMOKE_PUBLIC_GATEWAY_URL
        if args.enrollment_url is None:
            args.enrollment_url = LOCAL_SMOKE_ENROLLMENT_URL
        if args.bootstrap_url is None:
            args.bootstrap_url = ""
        if args.minimum_free_disk_gb is None:
            args.minimum_free_disk_gb = LOCAL_SMOKE_MINIMUM_FREE_DISK_GB
        if int(args.max_workers) == DEFAULT_MAX_WORKERS:
            args.max_workers = LOCAL_SMOKE_MAX_WORKERS
    else:
        if args.public_gateway_url is None:
            args.public_gateway_url = DEFAULT_PUBLIC_GATEWAY_URL
        if args.enrollment_url is None:
            args.enrollment_url = DEFAULT_ENROLLMENT_URL
        if args.bootstrap_url is None:
            args.bootstrap_url = DEFAULT_BOOTSTRAP_URL
        if args.minimum_free_disk_gb is None:
            args.minimum_free_disk_gb = DEFAULT_MINIMUM_FREE_DISK_GB
    return args


def _ps_quote(value: str) -> str:
    return '"' + str(value).replace('"', '`"') + '"'


def compute_script_sha256(path: Path) -> str:
    if not path.is_file():
        return "sha256:" + ("0" * 64)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return f"sha256:{digest}"


def compute_script_sha256_from_url(url: str) -> str:
    """Hash the publicly served bootstrap script so redeem matches irm downloads."""
    try:
        response = httpx.get(url, timeout=30.0, trust_env=False, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"failed to fetch bootstrap script for hashing: {exc}"
        ) from exc
    digest = hashlib.sha256(response.content).hexdigest()
    return f"sha256:{digest}"


def resolve_script_sha256(*, bootstrap_url: str | None, local_bootstrap_script: Path) -> str:
    if bootstrap_url:
        return compute_script_sha256_from_url(bootstrap_url)
    return compute_script_sha256(local_bootstrap_script)


def _parse_workers(value: str) -> str | int:
    raw = str(value or "").strip().lower()
    if raw == "auto":
        return "auto"
    if raw.isdigit():
        parsed = int(raw)
        if parsed <= 0:
            raise EphemeralSessionError("invalid_workers")
        return parsed
    raise EphemeralSessionError("invalid_workers")


def _validate_bootstrap_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise EphemeralSessionError("invalid_bootstrap_url")
    return str(url).strip()


def build_launch_command(
    *,
    enrollment_url: str,
    enrollment_token: str,
    duration: str,
    workers: str | int,
    max_workers: int,
    keep_image: bool,
    local_bootstrap_script: Path,
    bootstrap_url: str | None,
) -> str:
    workers_arg = "auto" if workers == "auto" else str(workers)
    if bootstrap_url:
        # Single-line paste: download exact bytes to a local file, then invoke it.
        # Avoids multiline backtick breakage and irm/scriptblock (no MyCommand.Path).
        # Trailing exit closes a dedicated paste window after cleanup ("leave no trace").
        workers_flag = (
            "-Workers auto"
            if workers == "auto"
            else f"-Workers {_ps_quote(workers_arg)}"
        )
        keep = " -KeepImage" if keep_image else " -RemoveImage"
        return (
            "$p=Join-Path $env:LOCALAPPDATA 'Fuzzfolio\\EphemeralWorkers\\_bootstrap\\"
            "ephemeral_worker_session.ps1'; "
            "New-Item -ItemType Directory -Force -Path (Split-Path $p)|Out-Null; "
            f"Invoke-WebRequest -UseBasicParsing -Uri {_ps_quote(bootstrap_url)} -OutFile $p; "
            f"& $p -EnrollmentUrl {_ps_quote(enrollment_url)} "
            f"-EnrollmentToken {_ps_quote(enrollment_token)} "
            f"-Duration {_ps_quote(duration)} {workers_flag} "
            f"-MaxWorkers {max_workers}{keep}; "
            "exit $LASTEXITCODE"
        )

    parts = [
        f"& {_ps_quote(str(local_bootstrap_script))}",
        f"-EnrollmentUrl {_ps_quote(enrollment_url)}",
        f"-EnrollmentToken {_ps_quote(enrollment_token)}",
        f"-Duration {_ps_quote(duration)}",
        f"-Workers {_ps_quote(workers_arg)}",
        f"-MaxWorkers {max_workers}",
    ]
    if keep_image:
        parts.append("-KeepImage")
    else:
        parts.append("-RemoveImage")
    # Close dedicated paste hosts after cleanup; harmless if already exiting.
    return " ".join(parts) + "; exit $LASTEXITCODE"


def build_redacted_launch_command(command: str, enrollment_token: str) -> str:
    if enrollment_token:
        return command.replace(enrollment_token, ENROLLMENT_TOKEN_PLACEHOLDER)
    return command


def build_mint_request(
    *,
    binding: dict[str, Any],
    duration_seconds: int,
    workers: str | int,
    max_workers: int,
    enrollment_ttl_seconds: int,
    public_gateway_url: str,
    enrollment_url: str,
    script_sha256: str,
    minimum_free_disk_gb: float,
    registration_timeout_seconds: int,
    remove_image_when_safe: bool,
) -> dict[str, Any]:
    return {
        "authority_id": binding["authority_id"],
        "image": binding["image"],
        "expected_worker_contract": binding["expected_worker_contract"],
        "required_capabilities": list(binding["required_capabilities"]),
        "duration_seconds": duration_seconds,
        "workers": workers,
        "max_workers": max_workers,
        "enrollment_ttl_seconds": enrollment_ttl_seconds,
        "cleanup_grace_seconds": DEFAULT_CLEANUP_GRACE_SECONDS,
        "public_gateway_url": public_gateway_url,
        "enrollment_url": enrollment_url,
        "script_sha256": script_sha256,
        "minimum_free_disk_gb": minimum_free_disk_gb,
        "registration_timeout_seconds": registration_timeout_seconds,
        "status_interval_seconds": DEFAULT_STATUS_INTERVAL_SECONDS,
        "remove_image_when_safe": remove_image_when_safe,
    }


def mint_ephemeral_session(
    *,
    admin_gateway_url: str,
    durable_token: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    url = f"{admin_gateway_url.rstrip('/')}/admin/ephemeral-sessions"
    try:
        response = httpx.post(
            url,
            json=body,
            headers={"Authorization": f"Bearer {durable_token}"},
            timeout=30.0,
            trust_env=False,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text.strip() or exc.response.reason_phrase
        raise RuntimeError(
            f"ephemeral session mint failed ({exc.response.status_code}): {detail}"
        ) from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"ephemeral session mint failed: {exc}") from exc
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("ephemeral session mint returned non-object response")
    return payload


def copy_to_clipboard(text: str) -> None:
    env = dict(os.environ)
    env["CLIPBOARD_PAYLOAD"] = text
    completed = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            "Set-Clipboard -Value $env:CLIPBOARD_PAYLOAD",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(
            "clipboard copy failed; run from an interactive PowerShell session "
            f"and retry --copy. {stderr}".strip()
        )


def build_redacted_payload(
    *,
    status: str,
    minted: dict[str, Any] | None,
    binding: dict[str, Any],
    duration_seconds: int,
    workers: str | int,
    max_workers: int,
    command_copied: bool,
    command_shape: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "duration": format_duration_iso8601(duration_seconds),
        "duration_seconds": duration_seconds,
        "workers": workers,
        "max_workers": max_workers,
        "image": binding["image"],
        "expected_contract": binding["expected_worker_contract"],
        "authority_id": binding["authority_id"],
        "command_copied": command_copied,
        "command_shape": command_shape,
    }
    if minted is not None:
        payload.update(
            {
                "session_id": minted.get("session_id"),
                "enrollment_expires_at": minted.get("enrollment_expires_at"),
                "pool": minted.get("pool"),
                "deadline": minted.get("deadline"),
            }
        )
    return payload


def run_generate_ephemeral_worker_command(args: argparse.Namespace) -> int:
    args = apply_profile_defaults(args)
    authority_path = Path(args.authority_path)
    if not authority_path.is_file():
        raise EphemeralSessionError("missing_authority_path")

    authority = json.loads(authority_path.read_text(encoding="utf-8"))
    binding = extract_authority_binding(authority)
    duration_seconds = parse_duration_seconds(args.duration)
    workers = _parse_workers(args.workers)
    max_workers = int(args.max_workers)
    if max_workers <= 0:
        raise EphemeralSessionError("invalid_max_workers")
    if isinstance(workers, int) and workers > max_workers:
        raise EphemeralSessionError("workers_exceed_max")

    enrollment_ttl_seconds = parse_duration_seconds(args.enrollment_ttl)
    registration_timeout_seconds = parse_duration_seconds(args.registration_timeout)
    raw_bootstrap = str(args.bootstrap_url or "").strip()
    bootstrap_url = _validate_bootstrap_url(raw_bootstrap) if raw_bootstrap else None
    remove_image_when_safe = not bool(args.keep_image)
    script_sha256 = resolve_script_sha256(
        bootstrap_url=bootstrap_url,
        local_bootstrap_script=Path(args.local_bootstrap_script),
    )

    mint_request = build_mint_request(
        binding=binding,
        duration_seconds=duration_seconds,
        workers=workers,
        max_workers=max_workers,
        enrollment_ttl_seconds=enrollment_ttl_seconds,
        public_gateway_url=str(args.public_gateway_url).rstrip("/"),
        enrollment_url=str(args.enrollment_url),
        script_sha256=script_sha256,
        minimum_free_disk_gb=float(args.minimum_free_disk_gb),
        registration_timeout_seconds=registration_timeout_seconds,
        remove_image_when_safe=remove_image_when_safe,
    )

    command_shape = build_launch_command(
        enrollment_url=str(args.enrollment_url),
        enrollment_token=ENROLLMENT_TOKEN_PLACEHOLDER,
        duration=str(args.duration),
        workers=workers,
        max_workers=max_workers,
        keep_image=bool(args.keep_image),
        local_bootstrap_script=Path(args.local_bootstrap_script),
        bootstrap_url=bootstrap_url,
    )

    if args.dry_run:
        payload = build_redacted_payload(
            status="dry_run",
            minted=None,
            binding=binding,
            duration_seconds=duration_seconds,
            workers=workers,
            max_workers=max_workers,
            command_copied=False,
            command_shape=command_shape,
        )
        if args.json_redacted or not args.print_command:
            print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        return 0

    durable_token = load_lab_gateway_token(create=False)
    if not durable_token:
        raise RuntimeError(
            "Lab Gateway durable token unavailable; start the gateway or configure "
            "FUZZFOLIO_LAB_GATEWAY_TOKEN before minting."
        )

    minted = mint_ephemeral_session(
        admin_gateway_url=str(args.admin_gateway_url),
        durable_token=durable_token,
        body=mint_request,
    )
    enrollment_token = str(minted.get("enrollment_token") or "")
    if not enrollment_token:
        raise RuntimeError("ephemeral session mint response missing enrollment_token")

    command = build_launch_command(
        enrollment_url=str(args.enrollment_url),
        enrollment_token=enrollment_token,
        duration=str(args.duration),
        workers=workers,
        max_workers=max_workers,
        keep_image=bool(args.keep_image),
        local_bootstrap_script=Path(args.local_bootstrap_script),
        bootstrap_url=bootstrap_url,
    )

    command_copied = False
    if args.copy:
        copy_to_clipboard(command)
        command_copied = True

    if args.print_command:
        print(
            "WARNING: printing the enrollment token may expose it to shell history "
            "and Procman logs.",
            file=sys.stderr,
        )
        print(command)

    redacted_shape = build_redacted_launch_command(command, enrollment_token)
    payload = build_redacted_payload(
        status="copied" if command_copied else "minted",
        minted=minted,
        binding=binding,
        duration_seconds=duration_seconds,
        workers=workers,
        max_workers=max_workers,
        command_copied=command_copied,
        command_shape=redacted_shape,
    )
    if args.json_redacted or (not args.print_command and not args.copy):
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="generate-ephemeral-worker-command",
        description="Mint ephemeral worker enrollments and build bootstrap commands.",
    )
    subparsers = parser.add_subparsers(dest="command")
    add_generate_ephemeral_worker_command_parser(subparsers)
    args = parser.parse_args(argv)
    if args.command != "generate-ephemeral-worker-command":
        parser.error("generate-ephemeral-worker-command is required")
    try:
        return run_generate_ephemeral_worker_command(args)
    except EphemeralSessionError as exc:
        parser.error(exc.code)
    except RuntimeError as exc:
        parser.error(str(exc))


__all__ = [
    "add_generate_ephemeral_worker_command_parser",
    "apply_profile_defaults",
    "build_launch_command",
    "build_mint_request",
    "build_redacted_launch_command",
    "compute_script_sha256",
    "compute_script_sha256_from_url",
    "copy_to_clipboard",
    "main",
    "mint_ephemeral_session",
    "resolve_script_sha256",
    "run_generate_ephemeral_worker_command",
]
