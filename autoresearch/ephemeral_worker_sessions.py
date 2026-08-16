"""Ephemeral Windows worker session mint/redeem and AuthPrincipal helpers."""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Literal
from urllib.parse import urlparse

AuthPrincipalKind = Literal["admin", "durable_worker", "ephemeral_worker"]
SessionStatus = Literal["issued", "active", "expired", "revoked"]

MIN_DURATION_SECONDS = 2 * 60
MAX_DURATION_SECONDS = 12 * 60 * 60
DEFAULT_ENROLLMENT_TTL_SECONDS = 20 * 60
MAX_ENROLLMENT_TTL_SECONDS = 60 * 60
DEFAULT_CLEANUP_GRACE_SECONDS = 600
DEFAULT_MAX_SESSIONS = 64
DEFAULT_DIAGNOSTIC_RETENTION_SECONDS = 60 * 60
DEFAULT_REQUIRED_CAPABILITY = "playhand_lab_protocol:playhand-lab-worker-v1"
BOOTSTRAP_SCHEMA_VERSION = 1
CONTRACT_HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
DURATION_TOKEN_RE = re.compile(r"^(\d+)(h|m)")
MUTABLE_IMAGE_TAGS = frozenset({"latest", "main", "vast"})
GENERIC_ENROLLMENT_ERROR = "enrollment_unavailable"
LakeCredentialsLoader = Callable[[], tuple[str, str]]


class EphemeralSessionError(ValueError):
    """Validation or policy failure for ephemeral sessions."""

    def __init__(self, code: str, *, http_status: int = 400) -> None:
        super().__init__(code)
        self.code = str(code)
        self.http_status = int(http_status)


class LakeCredentialsMissing(EphemeralSessionError):
    def __init__(self) -> None:
        super().__init__("lake_credentials_unavailable", http_status=503)


@dataclass(frozen=True, slots=True)
class AuthPrincipal:
    kind: AuthPrincipalKind
    session_id: str | None = None
    expires_at: float | None = None

    @property
    def is_durable(self) -> bool:
        return self.kind in {"admin", "durable_worker"}

    @property
    def is_ephemeral(self) -> bool:
        return self.kind == "ephemeral_worker"


@dataclass(slots=True)
class EphemeralSession:
    session_id: str
    enrollment_token_hash: str
    enrollment_expires_at: float
    authority_id: str
    image: str
    expected_contract: str
    required_capabilities: list[str]
    workers: str | int
    max_workers: int
    pool: str
    deadline: float
    cleanup_grace_seconds: int
    public_gateway_url: str
    enrollment_url: str
    script_sha256: str
    minimum_free_disk_gb: float
    registration_timeout_seconds: int
    status_interval_seconds: int
    remove_image_when_safe: bool
    issued_at: float = field(default_factory=lambda: time.time())
    redeemed_at: float | None = None
    worker_token_hash: str | None = None
    worker_token_expires_at: float | None = None
    revoked_at: float | None = None
    enrollment_consumed: bool = False

    @property
    def worker_token_valid_until(self) -> float:
        return float(self.deadline) + float(self.cleanup_grace_seconds)

    def status_label(self, *, now: float | None = None) -> SessionStatus:
        current = time.time() if now is None else float(now)
        if self.revoked_at is not None:
            return "revoked"
        if current >= self.deadline:
            return "expired"
        if self.redeemed_at is not None:
            return "active"
        if current >= self.enrollment_expires_at and not self.enrollment_consumed:
            return "expired"
        return "issued"


def hash_token(token: str) -> str:
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def tokens_match(token: str, token_hash: str) -> bool:
    return hmac.compare_digest(hash_token(token), str(token_hash or ""))


def parse_duration_seconds(value: str | int | float) -> int:
    if isinstance(value, bool):
        raise EphemeralSessionError("invalid_duration")
    if isinstance(value, (int, float)):
        seconds = int(value)
        if seconds != value:
            raise EphemeralSessionError("invalid_duration")
        return _validate_duration_seconds(seconds)

    raw = str(value or "").strip().lower()
    if not raw or "." in raw:
        raise EphemeralSessionError("invalid_duration")
    if raw.isdigit():
        return _validate_duration_seconds(int(raw))

    total = 0
    remaining = raw
    matched_any = False
    while remaining:
        match = DURATION_TOKEN_RE.match(remaining)
        if match is None:
            raise EphemeralSessionError("invalid_duration")
        amount = int(match.group(1))
        unit = match.group(2)
        if amount <= 0:
            raise EphemeralSessionError("invalid_duration")
        total += amount * (3600 if unit == "h" else 60)
        remaining = remaining[match.end() :]
        matched_any = True
    if not matched_any:
        raise EphemeralSessionError("invalid_duration")
    return _validate_duration_seconds(total)


def format_duration_iso8601(seconds: int) -> str:
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    parts = ["PT"]
    if hours:
        parts.append(f"{hours}H")
    if minutes:
        parts.append(f"{minutes}M")
    if secs and not hours and not minutes:
        parts.append(f"{secs}S")
    elif secs:
        parts.append(f"{secs}S")
    if parts == ["PT"]:
        parts.append("0S")
    return "".join(parts)


def _validate_duration_seconds(seconds: int) -> int:
    if seconds < MIN_DURATION_SECONDS:
        raise EphemeralSessionError("duration_below_minimum")
    if seconds > MAX_DURATION_SECONDS:
        raise EphemeralSessionError("duration_above_maximum")
    return int(seconds)


def wall_time_iso(ts: float | None = None) -> str:
    current = time.time() if ts is None else float(ts)
    return (
        datetime.fromtimestamp(current, tz=timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def new_session_id(*, now: float | None = None) -> str:
    stamp = (
        datetime.fromtimestamp(time.time() if now is None else float(now), tz=timezone.utc)
        .strftime("%Y%m%dT%H%M%SZ")
    )
    suffix = secrets.token_hex(6)
    return f"ews-{stamp}-{suffix}"


def pool_for_session_id(session_id: str) -> str:
    suffix = str(session_id).rsplit("-", 1)[-1].lower()
    if not re.fullmatch(r"[0-9a-f]{12}", suffix):
        raise EphemeralSessionError("invalid_session_id")
    return f"ephemeral-windows-{suffix}"


def validate_contract_hash(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not CONTRACT_HASH_RE.fullmatch(normalized):
        raise EphemeralSessionError("invalid_worker_contract")
    return normalized


def validate_immutable_image(image: str) -> str:
    normalized = str(image or "").strip()
    if not normalized:
        raise EphemeralSessionError("missing_image")
    if "://" in normalized or " " in normalized:
        raise EphemeralSessionError("invalid_image")
    tag = ""
    if ":" in normalized.rsplit("/", 1)[-1]:
        tag = normalized.rsplit(":", 1)[-1].strip().lower()
    if not tag or tag in MUTABLE_IMAGE_TAGS:
        raise EphemeralSessionError("mutable_image_forbidden")
    return normalized


def extract_authority_binding(authority: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(authority, dict):
        raise EphemeralSessionError("invalid_authority")
    authority_id = str(authority.get("authority_id") or "").strip()
    if not authority_id:
        raise EphemeralSessionError("missing_authority_id")
    bound = authority.get("bound_contract")
    if not isinstance(bound, dict):
        raise EphemeralSessionError("missing_bound_contract")
    image = validate_immutable_image(str(bound.get("operator_launch_worker_image") or ""))
    contract = validate_contract_hash(str(bound.get("worker_contract_sha256") or ""))
    return {
        "authority_id": authority_id,
        "image": image,
        "expected_worker_contract": contract,
        "required_capabilities": [DEFAULT_REQUIRED_CAPABILITY],
    }


def validate_gateway_url(url: str) -> str:
    raw = str(url or "").strip()
    parsed = urlparse(raw)
    host = str(parsed.hostname or "").strip().lower()
    if parsed.scheme not in {"http", "https"} or not host:
        raise EphemeralSessionError("invalid_gateway_url")
    if parsed.scheme == "http" and not _allows_http_gateway_host(host):
        raise EphemeralSessionError("gateway_url_https_required")
    return raw.rstrip("/")


def _allows_http_gateway_host(host: str) -> bool:
    if host in {"localhost", "127.0.0.1", "::1", "host.docker.internal"}:
        return True
    try:
        import ipaddress

        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def is_loopback_client(host: str | None) -> bool:
    normalized = str(host or "").strip().lower()
    if normalized.startswith("::ffff:"):
        normalized = normalized.split("::ffff:", 1)[1]
    if normalized in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        import ipaddress

        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def default_lake_credentials() -> tuple[str, str]:
    from .lake_window_client import _lake_credentials

    return _lake_credentials()


def build_bootstrap_manifest(
    session: EphemeralSession,
    *,
    gateway_token: str,
    lake_url: str,
    lake_token: str,
    issued_at: float | None = None,
) -> dict[str, Any]:
    issued = time.time() if issued_at is None else float(issued_at)
    return {
        "schema_version": BOOTSTRAP_SCHEMA_VERSION,
        "session_id": session.session_id,
        "issued_at": wall_time_iso(issued),
        "deadline": wall_time_iso(session.deadline),
        "cleanup_grace_seconds": int(session.cleanup_grace_seconds),
        "authority": {
            "authority_id": session.authority_id,
            "image": session.image,
            "expected_worker_contract": session.expected_contract,
            "required_capabilities": list(session.required_capabilities),
        },
        "worker": {
            "transport": "lab_ws",
            "pool": session.pool,
            "gateway_url": session.public_gateway_url,
            "gateway_token": gateway_token,
            "lake_url": lake_url,
            "lake_token": lake_token,
            "workers": session.workers,
            "max_workers": int(session.max_workers),
            "worker_memory_mb": 768,
            "worker_memory_reserve_mb": 2048,
            "cpu_reserve": 1,
            "startup_jitter_seconds": 30,
            "lake_download_slots": 8,
        },
        "bootstrap": {
            "script_sha256": session.script_sha256,
            "minimum_free_disk_gb": float(session.minimum_free_disk_gb),
            "registration_timeout_seconds": int(session.registration_timeout_seconds),
            "status_interval_seconds": int(session.status_interval_seconds),
            "remove_image_when_safe": bool(session.remove_image_when_safe),
        },
    }


def resolve_auth_principal(
    authorization: str | None,
    *,
    durable_token: str | None,
    registry: "EphemeralSessionRegistry | None" = None,
) -> AuthPrincipal | None:
    if not durable_token:
        return AuthPrincipal(kind="admin")
    bearer = _extract_bearer(authorization)
    if bearer is None:
        return None
    if hmac.compare_digest(bearer, durable_token):
        # Historical shared durable token acts as both admin and durable worker.
        return AuthPrincipal(kind="admin")
    if registry is None:
        return None
    return registry.authenticate_worker_token(bearer)


def ephemeral_http_operation_allowed(method: str, path: str) -> bool:
    method_u = str(method or "").upper()
    normalized = str(path or "").rstrip("/") or "/"
    if method_u == "POST" and normalized in {"/register", "/heartbeat", "/claim"}:
        return True
    parts = [part for part in normalized.split("/") if part]
    if (
        method_u == "POST"
        and len(parts) == 3
        and parts[0] == "leases"
        and parts[2] in {"heartbeat", "complete", "completion-upload", "fail"}
    ):
        return True
    if (
        method_u == "PUT"
        and len(parts) == 6
        and parts[0] == "leases"
        and parts[2] == "completion-upload"
        and parts[4] == "chunks"
        and parts[5].isdigit()
    ):
        return True
    if (
        method_u == "POST"
        and len(parts) == 5
        and parts[0] == "leases"
        and parts[2] == "completion-upload"
        and parts[4] == "finalize"
    ):
        return True
    if (
        method_u == "GET"
        and len(parts) == 3
        and parts[0] == "ephemeral-sessions"
        and parts[2] == "status"
    ):
        return True
    if (
        method_u == "POST"
        and len(parts) == 3
        and parts[0] == "ephemeral-sessions"
        and parts[2] == "revoke"
    ):
        return True
    return False


def ephemeral_ws_message_allowed(message_type: str) -> bool:
    return str(message_type or "") in {
        "register",
        "heartbeat",
        "claim",
        "lease_heartbeat",
        "complete",
        "fail",
    }


def principal_allows_http(principal: AuthPrincipal, method: str, path: str) -> bool:
    if principal.is_durable:
        return True
    if principal.is_ephemeral:
        return ephemeral_http_operation_allowed(method, path)
    return False


def _extract_bearer(authorization: str | None) -> str | None:
    header = str(authorization or "")
    if not header.startswith("Bearer "):
        return None
    token = header[len("Bearer ") :]
    return token if token else None


def _parse_workers(value: Any) -> str | int:
    if value is None or value == "auto":
        return "auto"
    if isinstance(value, bool):
        raise EphemeralSessionError("invalid_workers")
    if isinstance(value, int):
        if value <= 0:
            raise EphemeralSessionError("invalid_workers")
        return int(value)
    raw = str(value).strip().lower()
    if raw == "auto":
        return "auto"
    if raw.isdigit():
        parsed = int(raw)
        if parsed <= 0:
            raise EphemeralSessionError("invalid_workers")
        return parsed
    raise EphemeralSessionError("invalid_workers")


class EphemeralSessionRegistry:
    def __init__(
        self,
        *,
        max_sessions: int = DEFAULT_MAX_SESSIONS,
        diagnostic_retention_seconds: int = DEFAULT_DIAGNOSTIC_RETENTION_SECONDS,
        lake_credentials_loader: LakeCredentialsLoader | None = None,
    ) -> None:
        self.max_sessions = max(int(max_sessions), 1)
        self.diagnostic_retention_seconds = max(int(diagnostic_retention_seconds), 0)
        self._lake_credentials_loader = lake_credentials_loader or default_lake_credentials
        self._lock = threading.RLock()
        self._sessions: dict[str, EphemeralSession] = {}
        self._enrollment_index: dict[str, str] = {}
        self._worker_token_index: dict[str, str] = {}

    def mint(self, request: dict[str, Any], *, now: float | None = None) -> dict[str, Any]:
        current = time.time() if now is None else float(now)
        with self._lock:
            self.prune(now=current)
            if len(self._sessions) >= self.max_sessions:
                raise EphemeralSessionError("session_capacity_exceeded", http_status=503)

            authority_id = str(request.get("authority_id") or "").strip()
            image = validate_immutable_image(str(request.get("image") or ""))
            expected_contract = validate_contract_hash(
                str(request.get("expected_worker_contract") or "")
            )
            if not authority_id:
                raise EphemeralSessionError("missing_authority_id")

            raw_capabilities = request.get("required_capabilities")
            if raw_capabilities is None:
                required_capabilities = [DEFAULT_REQUIRED_CAPABILITY]
            elif isinstance(raw_capabilities, (list, tuple, set)):
                required_capabilities = [str(item) for item in raw_capabilities if str(item)]
            else:
                raise EphemeralSessionError("invalid_required_capabilities")
            if DEFAULT_REQUIRED_CAPABILITY not in required_capabilities:
                required_capabilities = [DEFAULT_REQUIRED_CAPABILITY, *required_capabilities]

            if "duration_seconds" in request and request.get("duration_seconds") is not None:
                duration_seconds = parse_duration_seconds(request.get("duration_seconds"))
            elif request.get("duration") is not None:
                duration_seconds = parse_duration_seconds(request.get("duration"))
            else:
                raise EphemeralSessionError("missing_duration")

            workers = _parse_workers(request.get("workers", "auto"))
            max_workers = int(request.get("max_workers") or 2)
            if max_workers <= 0:
                raise EphemeralSessionError("invalid_max_workers")
            if isinstance(workers, int) and workers > max_workers:
                raise EphemeralSessionError("workers_exceed_max")

            enrollment_ttl = int(
                request.get("enrollment_ttl_seconds") or DEFAULT_ENROLLMENT_TTL_SECONDS
            )
            if enrollment_ttl <= 0 or enrollment_ttl > MAX_ENROLLMENT_TTL_SECONDS:
                raise EphemeralSessionError("invalid_enrollment_ttl")
            cleanup_grace = int(
                request.get("cleanup_grace_seconds") or DEFAULT_CLEANUP_GRACE_SECONDS
            )
            if cleanup_grace < 0:
                raise EphemeralSessionError("invalid_cleanup_grace")

            public_gateway_url = validate_gateway_url(str(request.get("public_gateway_url") or ""))
            enrollment_url = str(request.get("enrollment_url") or "").strip()
            if not enrollment_url:
                raise EphemeralSessionError("missing_enrollment_url")
            script_sha256 = str(request.get("script_sha256") or "").strip().lower()
            if script_sha256 and not CONTRACT_HASH_RE.fullmatch(script_sha256):
                raise EphemeralSessionError("invalid_script_sha256")
            if not script_sha256:
                script_sha256 = "sha256:" + ("0" * 64)

            session_id = new_session_id(now=current)
            pool = pool_for_session_id(session_id)
            enrollment_token = secrets.token_urlsafe(32)
            enrollment_hash = hash_token(enrollment_token)
            deadline = current + float(duration_seconds)

            session = EphemeralSession(
                session_id=session_id,
                enrollment_token_hash=enrollment_hash,
                enrollment_expires_at=current + float(enrollment_ttl),
                authority_id=authority_id,
                image=image,
                expected_contract=expected_contract,
                required_capabilities=required_capabilities,
                workers=workers,
                max_workers=max_workers,
                pool=pool,
                deadline=deadline,
                cleanup_grace_seconds=cleanup_grace,
                public_gateway_url=public_gateway_url,
                enrollment_url=enrollment_url,
                script_sha256=script_sha256,
                minimum_free_disk_gb=float(request.get("minimum_free_disk_gb") or 10),
                registration_timeout_seconds=int(request.get("registration_timeout_seconds") or 300),
                status_interval_seconds=int(request.get("status_interval_seconds") or 15),
                remove_image_when_safe=bool(request.get("remove_image_when_safe") or False),
                issued_at=current,
            )
            self._sessions[session_id] = session
            self._enrollment_index[enrollment_hash] = session_id
            return {
                "session_id": session_id,
                "enrollment_token": enrollment_token,
                "enrollment_expires_at": wall_time_iso(session.enrollment_expires_at),
                "deadline": wall_time_iso(session.deadline),
                "duration": format_duration_iso8601(duration_seconds),
                "duration_seconds": duration_seconds,
                "pool": pool,
                "authority_id": authority_id,
                "image": image,
                "expected_worker_contract": expected_contract,
                "required_capabilities": list(required_capabilities),
                "workers": workers,
                "max_workers": max_workers,
                "cleanup_grace_seconds": cleanup_grace,
                "public_gateway_url": public_gateway_url,
                "enrollment_url": enrollment_url,
            }

    def redeem(
        self,
        request: dict[str, Any],
        *,
        now: float | None = None,
    ) -> dict[str, Any]:
        enrollment_token = str(request.get("enrollment_token") or "")
        if not enrollment_token:
            raise EphemeralSessionError(GENERIC_ENROLLMENT_ERROR, http_status=401)
        current = time.time() if now is None else float(now)
        with self._lock:
            self.prune(now=current)
            token_hash = hash_token(enrollment_token)
            session_id = self._enrollment_index.get(token_hash)
            session = self._sessions.get(session_id) if session_id else None
            if session is None or not tokens_match(enrollment_token, session.enrollment_token_hash):
                raise EphemeralSessionError(GENERIC_ENROLLMENT_ERROR, http_status=401)
            if session.enrollment_consumed or session.revoked_at is not None:
                raise EphemeralSessionError(GENERIC_ENROLLMENT_ERROR, http_status=401)
            if current >= session.enrollment_expires_at:
                raise EphemeralSessionError(GENERIC_ENROLLMENT_ERROR, http_status=401)
            if current >= session.deadline:
                raise EphemeralSessionError(GENERIC_ENROLLMENT_ERROR, http_status=401)

            script_sha256 = str(request.get("script_sha256") or "").strip().lower()
            if script_sha256 and script_sha256 != session.script_sha256:
                # Malformed/mismatched redeem must not consume enrollment.
                raise EphemeralSessionError("script_sha256_mismatch", http_status=400)

            lake_url, lake_token = self._load_lake_credentials()
            worker_token = secrets.token_urlsafe(32)
            worker_hash = hash_token(worker_token)
            session.enrollment_consumed = True
            session.redeemed_at = current
            session.worker_token_hash = worker_hash
            session.worker_token_expires_at = session.worker_token_valid_until
            self._enrollment_index.pop(token_hash, None)
            self._worker_token_index[worker_hash] = session.session_id
            return build_bootstrap_manifest(
                session,
                gateway_token=worker_token,
                lake_url=lake_url,
                lake_token=lake_token,
                issued_at=current,
            )

    def status(
        self,
        session_id: str,
        *,
        principal: AuthPrincipal,
        worker_counts: dict[str, int] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        current = time.time() if now is None else float(now)
        with self._lock:
            session = self._require_session_access(session_id, principal=principal, now=current)
            counts = worker_counts or {}
            expected_workers = (
                int(session.workers)
                if isinstance(session.workers, int)
                else int(session.max_workers)
            )
            return {
                "session_id": session.session_id,
                "status": session.status_label(now=current),
                "deadline": wall_time_iso(session.deadline),
                "registered_workers": int(counts.get("registered_workers", 0)),
                "compatible_workers": int(counts.get("compatible_workers", 0)),
                "busy_workers": int(counts.get("busy_workers", 0)),
                "expected_workers": expected_workers,
                "expected_contract": session.expected_contract,
            }

    def revoke(
        self,
        session_id: str,
        *,
        principal: AuthPrincipal,
        now: float | None = None,
    ) -> dict[str, Any]:
        current = time.time() if now is None else float(now)
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                # Idempotent success without revealing presence for authorized callers.
                if principal.is_durable or (
                    principal.is_ephemeral and principal.session_id == session_id
                ):
                    return {"status": "revoked", "session_id": session_id}
                raise EphemeralSessionError("unauthorized", http_status=401)
            if principal.is_ephemeral and principal.session_id != session_id:
                raise EphemeralSessionError("unauthorized", http_status=401)
            if not principal.is_durable and not principal.is_ephemeral:
                raise EphemeralSessionError("unauthorized", http_status=401)
            if session.revoked_at is None:
                session.revoked_at = current
                if session.worker_token_hash:
                    self._worker_token_index.pop(session.worker_token_hash, None)
                self._enrollment_index.pop(session.enrollment_token_hash, None)
            return {"status": "revoked", "session_id": session_id}

    def authenticate_worker_token(
        self,
        token: str,
        *,
        now: float | None = None,
    ) -> AuthPrincipal | None:
        current = time.time() if now is None else float(now)
        with self._lock:
            token_hash = hash_token(token)
            session_id = self._worker_token_index.get(token_hash)
            if not session_id:
                return None
            session = self._sessions.get(session_id)
            if session is None or session.worker_token_hash is None:
                return None
            if not tokens_match(token, session.worker_token_hash):
                return None
            if session.revoked_at is not None:
                return None
            expires_at = session.worker_token_expires_at or session.worker_token_valid_until
            if current >= expires_at:
                return None
            return AuthPrincipal(
                kind="ephemeral_worker",
                session_id=session.session_id,
                expires_at=expires_at,
            )

    def get_session(self, session_id: str) -> EphemeralSession | None:
        with self._lock:
            return self._sessions.get(session_id)

    def validate_ephemeral_registration(
        self,
        principal: AuthPrincipal,
        *,
        pool: str,
        contract_hash: str | None,
        capabilities: list[str] | set[str] | tuple[str, ...] | None,
        now: float | None = None,
    ) -> EphemeralSession:
        if not principal.is_ephemeral or not principal.session_id:
            raise EphemeralSessionError("unauthorized", http_status=401)
        current = time.time() if now is None else float(now)
        with self._lock:
            session = self._sessions.get(principal.session_id)
            if session is None or session.revoked_at is not None:
                raise EphemeralSessionError("unauthorized", http_status=401)
            if current >= session.deadline:
                raise EphemeralSessionError("session_deadline_passed", http_status=401)
            if str(pool or "") != session.pool:
                raise EphemeralSessionError("session_pool_mismatch", http_status=400)
            if not contract_hash or validate_contract_hash(contract_hash) != session.expected_contract:
                raise EphemeralSessionError("session_contract_mismatch", http_status=400)
            provided = {str(item) for item in (capabilities or []) if str(item)}
            missing = [item for item in session.required_capabilities if item not in provided]
            if missing:
                raise EphemeralSessionError("session_capabilities_mismatch", http_status=400)
            return session

    def prune(self, *, now: float | None = None) -> int:
        current = time.time() if now is None else float(now)
        removed = 0
        with self._lock:
            for session_id, session in list(self._sessions.items()):
                enrollment_expired = (
                    not session.enrollment_consumed
                    and current >= session.enrollment_expires_at
                )
                token_expired = (
                    session.worker_token_expires_at is not None
                    and current >= session.worker_token_expires_at
                )
                terminal = session.revoked_at is not None or current >= session.deadline
                retain_until = None
                if session.revoked_at is not None:
                    retain_until = session.revoked_at + self.diagnostic_retention_seconds
                elif current >= session.deadline:
                    retain_until = session.deadline + self.diagnostic_retention_seconds
                should_drop = False
                if enrollment_expired and not session.enrollment_consumed and session.redeemed_at is None:
                    should_drop = True
                if retain_until is not None and current >= retain_until:
                    should_drop = True
                if should_drop:
                    self._enrollment_index.pop(session.enrollment_token_hash, None)
                    if session.worker_token_hash:
                        self._worker_token_index.pop(session.worker_token_hash, None)
                    self._sessions.pop(session_id, None)
                    removed += 1
                elif terminal and token_expired and session.worker_token_hash:
                    self._worker_token_index.pop(session.worker_token_hash, None)
        return removed

    def _require_session_access(
        self,
        session_id: str,
        *,
        principal: AuthPrincipal,
        now: float,
    ) -> EphemeralSession:
        session = self._sessions.get(session_id)
        if session is None:
            raise EphemeralSessionError("not_found", http_status=404)
        if principal.is_ephemeral and principal.session_id != session_id:
            raise EphemeralSessionError("unauthorized", http_status=401)
        if not principal.is_durable and not principal.is_ephemeral:
            raise EphemeralSessionError("unauthorized", http_status=401)
        _ = now
        return session

    def _load_lake_credentials(self) -> tuple[str, str]:
        lake_url, lake_token = self._lake_credentials_loader()
        lake_url = str(lake_url or "").strip().rstrip("/")
        lake_token = str(lake_token or "").strip()
        if not lake_url or not lake_token:
            raise LakeCredentialsMissing()
        return lake_url, lake_token


__all__ = [
    "AuthPrincipal",
    "BOOTSTRAP_SCHEMA_VERSION",
    "DEFAULT_REQUIRED_CAPABILITY",
    "EphemeralSession",
    "EphemeralSessionError",
    "EphemeralSessionRegistry",
    "LakeCredentialsMissing",
    "MAX_DURATION_SECONDS",
    "MIN_DURATION_SECONDS",
    "build_bootstrap_manifest",
    "ephemeral_http_operation_allowed",
    "ephemeral_ws_message_allowed",
    "extract_authority_binding",
    "format_duration_iso8601",
    "hash_token",
    "is_loopback_client",
    "new_session_id",
    "parse_duration_seconds",
    "pool_for_session_id",
    "principal_allows_http",
    "resolve_auth_principal",
    "tokens_match",
    "validate_contract_hash",
    "validate_gateway_url",
    "validate_immutable_image",
    "wall_time_iso",
]
