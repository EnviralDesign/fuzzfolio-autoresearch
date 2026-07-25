"""Tests for ephemeral worker session registry and helpers."""

from __future__ import annotations

import threading
import time

import pytest

from autoresearch.ephemeral_worker_sessions import (
    AuthPrincipal,
    BOOTSTRAP_SCHEMA_VERSION,
    DEFAULT_REQUIRED_CAPABILITY,
    EphemeralSessionError,
    EphemeralSessionRegistry,
    LakeCredentialsMissing,
    extract_authority_binding,
    format_duration_iso8601,
    hash_token,
    new_session_id,
    parse_duration_seconds,
    pool_for_session_id,
    resolve_auth_principal,
    tokens_match,
    validate_gateway_url,
    validate_immutable_image,
)


CONTRACT = "sha256:" + ("a" * 64)
IMAGE = "lucasmorgan/fuzzfolio-replay-worker:sha-235b61f62fc0"


def _mint_request(**overrides):
    payload = {
        "authority_id": "sha256:" + ("b" * 64),
        "image": IMAGE,
        "expected_worker_contract": CONTRACT,
        "required_capabilities": [DEFAULT_REQUIRED_CAPABILITY],
        "duration_seconds": 180,
        "workers": "auto",
        "max_workers": 2,
        "enrollment_ttl_seconds": 1200,
        "cleanup_grace_seconds": 600,
        "public_gateway_url": "http://host.docker.internal:8799",
        "enrollment_url": "http://127.0.0.1:8799/ephemeral-sessions/redeem",
        "script_sha256": "sha256:" + ("c" * 64),
        "minimum_free_disk_gb": 10,
        "registration_timeout_seconds": 300,
        "status_interval_seconds": 15,
        "remove_image_when_safe": False,
    }
    payload.update(overrides)
    return payload


def _registry(*, lake_url: str = "https://lake.example/", lake_token: str = "lake-secret"):
    return EphemeralSessionRegistry(
        lake_credentials_loader=lambda: (lake_url, lake_token),
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("15m", 15 * 60),
        ("90m", 90 * 60),
        ("2h", 2 * 3600),
        ("1h30m", 5400),
        ("3m", 180),
        ("5m", 300),
        (180, 180),
    ],
)
def test_parse_duration_accepts_supported_grammar(raw, expected) -> None:
    assert parse_duration_seconds(raw) == expected
    assert format_duration_iso8601(expected).startswith("PT")


@pytest.mark.parametrize(
    "raw",
    ["1.5h", "0m", "-5m", "", "abc", "2hours", "1h30", "30s"],
)
def test_parse_duration_rejects_invalid(raw) -> None:
    with pytest.raises(EphemeralSessionError):
        parse_duration_seconds(raw)


def test_parse_duration_enforces_min_max() -> None:
    with pytest.raises(EphemeralSessionError, match="duration_below_minimum"):
        parse_duration_seconds("1m")
    with pytest.raises(EphemeralSessionError, match="duration_above_maximum"):
        parse_duration_seconds("13h")


def test_session_id_and_pool_shape() -> None:
    session_id = new_session_id(now=1_753_286_940.0)
    assert session_id.startswith("ews-")
    parts = session_id.split("-")
    assert len(parts) == 3
    assert parts[1].endswith("Z")
    assert len(parts[2]) == 12
    assert pool_for_session_id(session_id) == f"ephemeral-windows-{parts[2]}"


def test_validate_immutable_image_and_gateway_url() -> None:
    assert validate_immutable_image(IMAGE) == IMAGE
    for bad in (
        "lucasmorgan/fuzzfolio-replay-worker:latest",
        "lucasmorgan/fuzzfolio-replay-worker:main",
        "lucasmorgan/fuzzfolio-replay-worker:vast",
        "lucasmorgan/fuzzfolio-replay-worker",
    ):
        with pytest.raises(EphemeralSessionError):
            validate_immutable_image(bad)

    assert validate_gateway_url("http://host.docker.internal:8799").endswith(":8799")
    assert validate_gateway_url("http://127.0.0.1:8799").endswith(":8799")
    assert validate_gateway_url("https://playhand-lab.example.com").startswith("https://")
    with pytest.raises(EphemeralSessionError, match="gateway_url_https_required"):
        validate_gateway_url("http://playhand-lab.example.com")


def test_extract_authority_binding() -> None:
    binding = extract_authority_binding(
        {
            "authority_id": "sha256:" + ("d" * 64),
            "bound_contract": {
                "operator_launch_worker_image": IMAGE,
                "worker_contract_sha256": CONTRACT,
            },
        }
    )
    assert binding["image"] == IMAGE
    assert binding["expected_worker_contract"] == CONTRACT
    assert DEFAULT_REQUIRED_CAPABILITY in binding["required_capabilities"]


def test_mint_redeem_status_revoke_happy_path() -> None:
    registry = _registry()
    minted = registry.mint(_mint_request())
    assert minted["enrollment_token"]
    assert minted["session_id"].startswith("ews-")
    assert minted["pool"].startswith("ephemeral-windows-")

    manifest = registry.redeem(
        {
            "enrollment_token": minted["enrollment_token"],
            "client_nonce": "n1",
            "script_sha256": _mint_request()["script_sha256"],
        }
    )
    assert manifest["schema_version"] == BOOTSTRAP_SCHEMA_VERSION
    assert manifest["session_id"] == minted["session_id"]
    assert manifest["worker"]["gateway_url"] == "http://host.docker.internal:8799"
    assert manifest["worker"]["pool"] == minted["pool"]
    assert manifest["worker"]["lake_url"] == "https://lake.example"
    assert manifest["worker"]["lake_token"] == "lake-secret"
    worker_token = manifest["worker"]["gateway_token"]
    assert worker_token
    assert worker_token != minted["enrollment_token"]

    principal = registry.authenticate_worker_token(worker_token)
    assert principal is not None
    assert principal.kind == "ephemeral_worker"
    assert principal.session_id == minted["session_id"]

    status = registry.status(minted["session_id"], principal=principal)
    assert status["status"] == "active"
    assert status["expected_contract"] == CONTRACT
    assert set(status) == {
        "session_id",
        "status",
        "deadline",
        "registered_workers",
        "compatible_workers",
        "busy_workers",
        "expected_workers",
        "expected_contract",
    }

    revoked = registry.revoke(minted["session_id"], principal=principal)
    assert revoked["status"] == "revoked"
    assert registry.authenticate_worker_token(worker_token) is None


def test_one_time_redeem_and_expired_enrollment() -> None:
    registry = _registry()
    now = time.time()
    minted = registry.mint(_mint_request(enrollment_ttl_seconds=60), now=now)
    first = registry.redeem(
        {"enrollment_token": minted["enrollment_token"], "script_sha256": _mint_request()["script_sha256"]},
        now=now + 1,
    )
    assert first["schema_version"] == 1
    with pytest.raises(EphemeralSessionError, match="enrollment_unavailable"):
        registry.redeem(
            {"enrollment_token": minted["enrollment_token"], "script_sha256": _mint_request()["script_sha256"]},
            now=now + 2,
        )

    minted2 = registry.mint(_mint_request(enrollment_ttl_seconds=60), now=now)
    with pytest.raises(EphemeralSessionError, match="enrollment_unavailable"):
        registry.redeem(
            {"enrollment_token": minted2["enrollment_token"], "script_sha256": _mint_request()["script_sha256"]},
            now=now + 120,
        )
    # Expired enrollment is never redeemed; a later attempt still fails generically.
    with pytest.raises(EphemeralSessionError, match="enrollment_unavailable"):
        registry.redeem(
            {"enrollment_token": minted2["enrollment_token"], "script_sha256": _mint_request()["script_sha256"]},
            now=now + 121,
        )


def test_concurrent_double_redeem_exactly_one_success() -> None:
    registry = _registry()
    minted = registry.mint(_mint_request())
    results: list[object] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def _attempt() -> None:
        barrier.wait(timeout=5)
        try:
            results.append(
                registry.redeem(
                    {
                        "enrollment_token": minted["enrollment_token"],
                        "script_sha256": _mint_request()["script_sha256"],
                    }
                )
            )
        except BaseException as exc:  # noqa: BLE001 - collect for assertion
            errors.append(exc)

    threads = [threading.Thread(target=_attempt) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], EphemeralSessionError)
    assert errors[0].code == "enrollment_unavailable"


def test_redeem_without_lake_credentials() -> None:
    registry = EphemeralSessionRegistry(lake_credentials_loader=lambda: ("", ""))
    minted = registry.mint(_mint_request())
    with pytest.raises(LakeCredentialsMissing):
        registry.redeem(
            {
                "enrollment_token": minted["enrollment_token"],
                "script_sha256": _mint_request()["script_sha256"],
            }
        )
    session = registry.get_session(minted["session_id"])
    assert session is not None
    assert session.enrollment_consumed is False


def test_script_mismatch_does_not_consume() -> None:
    registry = _registry()
    minted = registry.mint(_mint_request())
    with pytest.raises(EphemeralSessionError, match="script_sha256_mismatch"):
        registry.redeem(
            {
                "enrollment_token": minted["enrollment_token"],
                "script_sha256": "sha256:" + ("e" * 64),
            }
        )
    session = registry.get_session(minted["session_id"])
    assert session is not None
    assert session.enrollment_consumed is False


def test_token_hash_constant_time_helpers() -> None:
    token = "abc"
    digest = hash_token(token)
    assert tokens_match(token, digest)
    assert not tokens_match("abd", digest)


def test_resolve_auth_principal_durable_and_ephemeral() -> None:
    registry = _registry()
    durable = resolve_auth_principal("Bearer durable-secret", durable_token="durable-secret", registry=registry)
    assert durable is not None
    assert durable.kind == "admin"
    assert durable.is_durable

    minted = registry.mint(_mint_request())
    manifest = registry.redeem(
        {
            "enrollment_token": minted["enrollment_token"],
            "script_sha256": _mint_request()["script_sha256"],
        }
    )
    ephemeral = resolve_auth_principal(
        f"Bearer {manifest['worker']['gateway_token']}",
        durable_token="durable-secret",
        registry=registry,
    )
    assert ephemeral is not None
    assert ephemeral.kind == "ephemeral_worker"
    assert ephemeral.session_id == minted["session_id"]
    assert resolve_auth_principal("Bearer nope", durable_token="durable-secret", registry=registry) is None


def test_validate_ephemeral_registration_constraints() -> None:
    registry = _registry()
    minted = registry.mint(_mint_request())
    manifest = registry.redeem(
        {
            "enrollment_token": minted["enrollment_token"],
            "script_sha256": _mint_request()["script_sha256"],
        }
    )
    principal = AuthPrincipal(kind="ephemeral_worker", session_id=minted["session_id"])
    session = registry.validate_ephemeral_registration(
        principal,
        pool=minted["pool"],
        contract_hash=CONTRACT,
        capabilities=[DEFAULT_REQUIRED_CAPABILITY],
    )
    assert session.session_id == minted["session_id"]
    with pytest.raises(EphemeralSessionError, match="session_pool_mismatch"):
        registry.validate_ephemeral_registration(
            principal,
            pool="wrong-pool",
            contract_hash=CONTRACT,
            capabilities=[DEFAULT_REQUIRED_CAPABILITY],
        )
    registry.revoke(minted["session_id"], principal=principal)
    with pytest.raises(EphemeralSessionError, match="unauthorized"):
        registry.validate_ephemeral_registration(
            principal,
            pool=minted["pool"],
            contract_hash=CONTRACT,
            capabilities=[DEFAULT_REQUIRED_CAPABILITY],
        )
    _ = manifest
