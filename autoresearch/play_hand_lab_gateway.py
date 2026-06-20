from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import ipaddress
import json
import math
import os
import random
import socket
import statistics
import threading
import time
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import requests
import httpx
from websockets.asyncio.client import connect as websocket_connect


TaskStatus = Literal["queued", "leased", "completed", "failed"]
ClaimStatus = Literal["leased", "no_work"]
CompletionStatus = Literal["accepted", "duplicate", "lease_lost"]
FailureStatus = Literal["requeued", "failed", "lease_lost"]
DEFAULT_MAX_BODY_BYTES = 64 * 1024 * 1024


def _now() -> float:
    return time.monotonic()


def _wall_time_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _string_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return []


def _parse_bool(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _is_loopback_host(host: str) -> bool:
    normalized = str(host or "").strip().lower()
    if normalized in {"localhost", "127.0.0.1", "::1"}:
        return True
    if normalized in {"", "0.0.0.0", "::", "*", "+"}:
        return False
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _worker_contract_fields(payload: dict[str, Any]) -> tuple[str | None, list[str]]:
    contract = payload.get("contract") if isinstance(payload.get("contract"), dict) else {}
    raw_hash = payload.get("contract_hash") or contract.get("contract_hash")
    raw_capabilities = payload.get("capabilities")
    if raw_capabilities is None:
        raw_capabilities = contract.get("capabilities")
    contract_hash = str(raw_hash).strip() if raw_hash else None
    return contract_hash, _string_list(raw_capabilities)


@dataclass(slots=True)
class LabTask:
    task_id: str
    lane_id: str
    attempt_id: str
    task_kind: str = "fake_compute"
    payload: dict[str, Any] = field(default_factory=dict)
    required_worker_capabilities: set[str] = field(default_factory=set)
    deadline_seconds: float = 300.0
    max_attempts: int = 2
    created_at: float = field(default_factory=_now)
    attempt_number: int = 0
    status: TaskStatus = "queued"
    last_error: str | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "LabTask":
        task_id = str(payload.get("task_id") or uuid4())
        lane_id = str(payload.get("lane_id") or "default")
        attempt_id = str(payload.get("attempt_id") or task_id)
        task_kind = str(payload.get("task_kind") or "fake_compute")
        raw_payload = payload.get("payload")
        task_payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
        raw_required_capabilities = payload.get("required_worker_capabilities")
        if raw_required_capabilities is None:
            raw_required_capabilities = payload.get("scheduling_required_capabilities")
        required_worker_capabilities = {
            str(item)
            for item in _string_list(raw_required_capabilities)
            if str(item)
        }
        return cls(
            task_id=task_id,
            lane_id=lane_id,
            attempt_id=attempt_id,
            task_kind=task_kind,
            payload=task_payload,
            required_worker_capabilities=required_worker_capabilities,
            deadline_seconds=max(float(payload.get("deadline_seconds") or 300.0), 1.0),
            max_attempts=max(int(payload.get("max_attempts") or 2), 1),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "lane_id": self.lane_id,
            "attempt_id": self.attempt_id,
            "task_kind": self.task_kind,
            "payload": dict(self.payload),
            "deadline_seconds": self.deadline_seconds,
            "max_attempts": self.max_attempts,
            "attempt_number": self.attempt_number,
        }


@dataclass(slots=True)
class LabWorker:
    worker_id: str
    pool: str
    slots: int = 1
    registered_at: float = field(default_factory=_now)
    heartbeat_at: float = field(default_factory=_now)
    status_detail: str = "registered"
    active_lease_ids: set[str] = field(default_factory=set)
    progress: dict[str, Any] | None = None
    contract_hash: str | None = None
    capabilities: set[str] = field(default_factory=set)

    def to_payload(self, now: float | None = None) -> dict[str, Any]:
        current = _now() if now is None else now
        payload: dict[str, Any] = {
            "worker_id": self.worker_id,
            "pool": self.pool,
            "slots": self.slots,
            "registered_age_seconds": round(current - self.registered_at, 3),
            "heartbeat_age_seconds": round(current - self.heartbeat_at, 3),
            "status_detail": self.status_detail,
            "active_lease_count": len(self.active_lease_ids),
        }
        if self.progress is not None:
            payload["progress"] = dict(self.progress)
        if self.contract_hash:
            payload["contract_hash"] = self.contract_hash
        if self.capabilities:
            payload["capabilities"] = sorted(self.capabilities)
        return payload


@dataclass(slots=True)
class LabLease:
    lease_id: str
    task_id: str
    worker_id: str
    pool: str
    started_at: float
    heartbeat_at: float
    expires_at: float
    attempt_number: int
    progress: dict[str, Any] | None = None

    def to_payload(self, task: LabTask) -> dict[str, Any]:
        return {
            "lease_id": self.lease_id,
            "task": task.to_payload(),
            "task_id": self.task_id,
            "lane_id": task.lane_id,
            "attempt_id": task.attempt_id,
            "task_kind": task.task_kind,
            "attempt_number": self.attempt_number,
            "deadline_seconds": task.deadline_seconds,
        }


@dataclass(slots=True)
class LabResult:
    task_id: str
    lease_id: str
    worker_id: str
    lane_id: str
    attempt_id: str
    status: str
    accepted_at: float
    result: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "lease_id": self.lease_id,
            "worker_id": self.worker_id,
            "lane_id": self.lane_id,
            "attempt_id": self.attempt_id,
            "status": self.status,
            "accepted_at": self.accepted_at,
            "accepted_at_wall": _wall_time_iso(),
            "result": dict(self.result),
        }


@dataclass(slots=True)
class LabGatewayConfig:
    lease_ttl_seconds: float = 120.0
    max_recent_completions: int = 20_000
    max_result_backlog: int = 100_000
    no_work_retry_after_seconds: float = 1.0


@dataclass(slots=True)
class SaturationSimulationConfig:
    worker_count: int
    target_completions: int | None = None
    backlog_multiplier: int = 4
    fixed_work_seconds: float = 10.0
    time_scale: float = 0.001
    max_wall_seconds: float = 15.0
    sample_interval_seconds: float = 0.02
    runtime_distribution: Literal["fixed", "lognormal"] = "fixed"
    failure_rate: float = 0.0
    worker_crash_rate: float = 0.0
    seed: int = 1


@dataclass(slots=True)
class HttpSaturationSimulationConfig:
    worker_count: int
    target_completions: int | None = None
    backlog_multiplier: int = 4
    work_seconds: float = 0.01
    runtime_distribution: Literal["fixed", "lognormal"] = "fixed"
    startup_jitter_seconds: float = 0.5
    max_wall_seconds: float = 30.0
    sample_interval_seconds: float = 0.05
    token: str = "lab-sim-token"
    seed: int = 1


@dataclass(slots=True)
class WebSocketSaturationSimulationConfig:
    worker_count: int
    target_completions: int | None = None
    backlog_multiplier: int = 4
    work_seconds: float = 0.01
    runtime_distribution: Literal["fixed", "lognormal"] = "fixed"
    startup_jitter_seconds: float = 0.5
    max_wall_seconds: float = 30.0
    sample_interval_seconds: float = 0.05
    token: str = "lab-sim-token"
    seed: int = 1


class PlayHandLabGateway:
    def __init__(self, config: LabGatewayConfig | None = None) -> None:
        self.config = config or LabGatewayConfig()
        self.gateway_id = str(uuid4())
        self.started_at_wall = _wall_time_iso()
        self._lock = threading.RLock()
        self._pending: deque[str] = deque()
        self._tasks: dict[str, LabTask] = {}
        self._leases: dict[str, LabLease] = {}
        self._workers: dict[str, LabWorker] = {}
        self._results: deque[LabResult] = deque()
        self._completed_by_lease: dict[str, LabResult] = {}
        self._recent_completed_order: deque[str] = deque()
        self._failed_tasks: set[str] = set()
        self._metrics: dict[str, int] = {
            "tasks_enqueued": 0,
            "claims": 0,
            "no_work_claims": 0,
            "completions_accepted": 0,
            "duplicate_completions": 0,
            "lost_completions": 0,
            "failures_requeued": 0,
            "failures_final": 0,
            "failed_completions": 0,
            "expired_leases_requeued": 0,
            "slot_limited_claims": 0,
            "incompatible_claims": 0,
            "results_acked": 0,
            "results_dropped": 0,
        }

    def enqueue(self, task: LabTask) -> None:
        with self._lock:
            if task.task_id in self._tasks:
                raise ValueError(f"Duplicate lab task id: {task.task_id}")
            task.status = "queued"
            self._tasks[task.task_id] = task
            self._pending.append(task.task_id)
            self._metrics["tasks_enqueued"] += 1

    def enqueue_many(self, tasks: list[LabTask]) -> None:
        for task in tasks:
            self.enqueue(task)

    def register_worker(
        self,
        worker_id: str,
        pool: str = "lab",
        slots: int = 1,
        *,
        contract_hash: str | None = None,
        capabilities: list[str] | set[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        now = _now()
        with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                worker = LabWorker(worker_id=worker_id, pool=pool, slots=max(int(slots), 1))
                self._workers[worker_id] = worker
            worker.pool = pool
            worker.slots = max(int(slots), 1)
            worker.heartbeat_at = now
            worker.status_detail = "registered"
            if contract_hash:
                worker.contract_hash = str(contract_hash)
            if capabilities is not None:
                worker.capabilities = {str(item) for item in capabilities if str(item)}
            return worker.to_payload(now)

    def heartbeat_worker(
        self,
        worker_id: str,
        *,
        pool: str | None = None,
        status_detail: str | None = None,
    ) -> bool:
        now = _now()
        with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return False
            if pool:
                worker.pool = pool
            worker.heartbeat_at = now
            if status_detail is not None:
                worker.status_detail = status_detail
            return True

    def claim(
        self,
        worker_id: str,
        pool: str = "lab",
        *,
        contract_hash: str | None = None,
        capabilities: list[str] | set[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._requeue_expired_leases_locked(_now())
            if worker_id not in self._workers:
                self.register_worker(
                    worker_id,
                    pool=pool,
                    contract_hash=contract_hash,
                    capabilities=capabilities,
                )
            worker = self._workers[worker_id]
            worker.heartbeat_at = _now()
            if contract_hash:
                worker.contract_hash = str(contract_hash)
            if capabilities is not None:
                worker.capabilities = {str(item) for item in capabilities if str(item)}
            worker.status_detail = "claiming"
            if len(worker.active_lease_ids) >= max(int(worker.slots), 1):
                worker.status_detail = "slot_limited"
                self._metrics["slot_limited_claims"] += 1
                return {
                    "status": "no_work",
                    "retry_after_seconds": self.config.no_work_retry_after_seconds,
                    "reason": "worker_slots_full",
                }
            task_id: str | None = None
            initial_pending = len(self._pending)
            for _ in range(initial_pending):
                candidate = self._pending.popleft()
                task = self._tasks.get(candidate)
                if task is None or task.status != "queued":
                    continue
                if not self._worker_matches_task(worker, task):
                    self._pending.append(candidate)
                    continue
                task_id = candidate
                break
            if task_id is None:
                self._metrics["no_work_claims"] += 1
                if self._pending:
                    self._metrics["incompatible_claims"] += 1
                    return {
                        "status": "no_work",
                        "retry_after_seconds": self.config.no_work_retry_after_seconds,
                        "reason": "no_compatible_work",
                    }
                return {
                    "status": "no_work",
                    "retry_after_seconds": self.config.no_work_retry_after_seconds,
                }

            task = self._tasks[task_id]
            task.attempt_number += 1
            task.status = "leased"
            now = _now()
            lease = LabLease(
                lease_id=str(uuid4()),
                task_id=task_id,
                worker_id=worker_id,
                pool=pool,
                started_at=now,
                heartbeat_at=now,
                expires_at=now + max(task.deadline_seconds, self.config.lease_ttl_seconds),
                attempt_number=task.attempt_number,
            )
            self._leases[lease.lease_id] = lease
            worker.active_lease_ids.add(lease.lease_id)
            worker.status_detail = "busy"
            self._metrics["claims"] += 1
            return {"status": "leased", **lease.to_payload(task)}

    def _worker_matches_task(self, worker: LabWorker, task: LabTask) -> bool:
        payload = task.payload if isinstance(task.payload, dict) else {}
        required_hash = str(payload.get("required_worker_contract_hash") or "").strip()
        if required_hash and worker.contract_hash != required_hash:
            return False
        raw_capabilities = payload.get("required_capabilities")
        if task.required_worker_capabilities:
            required_capabilities = set(task.required_worker_capabilities)
        elif isinstance(raw_capabilities, list):
            required_capabilities = {str(item) for item in raw_capabilities if str(item)}
        else:
            required_capabilities = set()
        if required_capabilities and not required_capabilities.issubset(worker.capabilities):
            return False
        return True

    def heartbeat_lease(
        self,
        worker_id: str,
        lease_id: str,
        *,
        progress: dict[str, Any] | None = None,
    ) -> bool:
        now = _now()
        with self._lock:
            lease = self._leases.get(lease_id)
            if lease is None or lease.worker_id != worker_id:
                return False
            if lease.expires_at <= now:
                self._expire_lease_locked(lease, now)
                return False
            task = self._tasks[lease.task_id]
            lease.heartbeat_at = now
            lease.expires_at = now + max(task.deadline_seconds, self.config.lease_ttl_seconds)
            if progress is not None:
                lease.progress = dict(progress)
            worker = self._workers.get(worker_id)
            if worker is not None:
                worker.heartbeat_at = now
                worker.active_lease_ids.add(lease_id)
                if progress is not None:
                    worker.progress = dict(progress)
            return True

    def complete(
        self,
        worker_id: str,
        lease_id: str,
        *,
        status: str = "success",
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _now()
        with self._lock:
            if lease_id in self._completed_by_lease:
                self._metrics["duplicate_completions"] += 1
                return {
                    "status": "duplicate",
                    "completion": self._completed_by_lease[lease_id].to_payload(),
                }

            lease = self._leases.get(lease_id)
            if lease is None or lease.worker_id != worker_id:
                self._metrics["lost_completions"] += 1
                return {"status": "lease_lost", "lease_id": lease_id}
            if lease.expires_at <= now:
                self._expire_lease_locked(lease, now)
                self._metrics["lost_completions"] += 1
                return {"status": "lease_lost", "lease_id": lease_id}

            task = self._tasks[lease.task_id]
            result_payload = dict(result or {})
            result_status = str(result_payload.get("status") or status or "success").lower()
            failed_completion = result_status in {"failed", "error"}
            completion_status = "failed" if failed_completion else "success"
            task.status = "failed" if failed_completion else "completed"
            if failed_completion:
                self._failed_tasks.add(task.task_id)
                self._metrics["failed_completions"] += 1
            completion = LabResult(
                task_id=task.task_id,
                lease_id=lease_id,
                worker_id=worker_id,
                lane_id=task.lane_id,
                attempt_id=task.attempt_id,
                status=completion_status,
                accepted_at=now,
                result=result_payload,
            )
            self._remove_lease_locked(lease)
            self._completed_by_lease[lease_id] = completion
            self._recent_completed_order.append(lease_id)
            self._append_result_locked(completion)
            self._metrics["completions_accepted"] += 1
            self._trim_completion_history_locked()
            return {"status": "accepted", "completion": completion.to_payload()}

    def fail(
        self,
        worker_id: str,
        lease_id: str,
        *,
        error: str,
        retryable: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            now = _now()
            lease = self._leases.get(lease_id)
            if lease is None or lease.worker_id != worker_id:
                return {"status": "lease_lost", "lease_id": lease_id}
            if lease.expires_at <= now:
                self._expire_lease_locked(lease, now)
                return {"status": "lease_lost", "lease_id": lease_id}
            task = self._tasks[lease.task_id]
            task.last_error = error
            self._remove_lease_locked(lease)
            if retryable and task.attempt_number < task.max_attempts:
                task.status = "queued"
                self._pending.append(task.task_id)
                self._metrics["failures_requeued"] += 1
                return {"status": "requeued", "task_id": task.task_id}
            task.status = "failed"
            self._failed_tasks.add(task.task_id)
            failure = LabResult(
                task_id=task.task_id,
                lease_id=lease_id,
                worker_id=worker_id,
                lane_id=task.lane_id,
                attempt_id=task.attempt_id,
                status="failed",
                accepted_at=_now(),
                result={
                    "status": "failed",
                    "error": str(error),
                    "retryable": bool(retryable),
                    "attempt_number": task.attempt_number,
                },
            )
            self._append_result_locked(failure)
            self._metrics["failures_final"] += 1
            return {"status": "failed", "task_id": task.task_id, "failure": failure.to_payload()}

    def reap_expired_leases(self) -> int:
        with self._lock:
            return self._requeue_expired_leases_locked(_now())

    def drain_results(self, limit: int | None = None) -> list[dict[str, Any]]:
        results = self.read_results(limit=limit)
        self.ack_results([str(result.get("lease_id") or "") for result in results])
        return results

    def read_results(self, limit: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            self._requeue_expired_leases_locked(_now())
            count = len(self._results) if limit is None else min(max(int(limit), 0), len(self._results))
            return [self._results[index].to_payload() for index in range(count)]

    def ack_results(self, lease_ids: list[str]) -> int:
        requested = {str(lease_id) for lease_id in lease_ids if str(lease_id)}
        if not requested:
            return 0
        with self._lock:
            kept: deque[LabResult] = deque()
            acked = 0
            while self._results:
                result = self._results.popleft()
                if result.lease_id in requested:
                    acked += 1
                    continue
                kept.append(result)
            self._results = kept
            self._metrics["results_acked"] += acked
            return acked

    def snapshot(self, *, include_workers: bool = False) -> dict[str, Any]:
        now = _now()
        with self._lock:
            self._requeue_expired_leases_locked(now)
            active_workers = sum(1 for worker in self._workers.values() if worker.active_lease_ids)
            worker_count = len(self._workers)
            worker_slots = sum(max(int(worker.slots), 1) for worker in self._workers.values())
            busy_slots = sum(len(worker.active_lease_ids) for worker in self._workers.values())
            completed_count = sum(1 for task in self._tasks.values() if task.status == "completed")
            failed_count = sum(1 for task in self._tasks.values() if task.status == "failed")
            queued_count = sum(1 for task in self._tasks.values() if task.status == "queued")
            leased_count = len(self._leases)
            payload: dict[str, Any] = {
                "ok": True,
                "gateway_id": self.gateway_id,
                "started_at_wall": self.started_at_wall,
                "worker_count": worker_count,
                "busy_worker_count": active_workers,
                "worker_busy_rate": (active_workers / worker_count) if worker_count else 0.0,
                "worker_slots": worker_slots,
                "busy_slots": busy_slots,
                "slot_busy_rate": (busy_slots / worker_slots) if worker_slots else 0.0,
                "queued_tasks": queued_count,
                "active_leases": leased_count,
                "completed_tasks": completed_count,
                "failed_tasks": failed_count,
                "live_tasks": queued_count + leased_count,
                "result_backlog": len(self._results),
                "metrics": dict(self._metrics),
            }
            if include_workers:
                payload["workers"] = [worker.to_payload(now) for worker in self._workers.values()]
            return payload

    def _remove_lease_locked(self, lease: LabLease) -> None:
        self._leases.pop(lease.lease_id, None)
        worker = self._workers.get(lease.worker_id)
        if worker is not None:
            worker.active_lease_ids.discard(lease.lease_id)
            if not worker.active_lease_ids:
                worker.status_detail = "idle"

    def _requeue_expired_leases_locked(self, now: float) -> int:
        expired = [lease for lease in self._leases.values() if lease.expires_at <= now]
        for lease in expired:
            self._expire_lease_locked(lease, now)
        return len(expired)

    def _expire_lease_locked(self, lease: LabLease, now: float) -> None:
        task = self._tasks.get(lease.task_id)
        self._remove_lease_locked(lease)
        if task is None or task.status in {"completed", "failed"}:
            return
        if task.attempt_number < task.max_attempts:
            task.status = "queued"
            task.last_error = "lease_expired"
            self._pending.append(task.task_id)
            self._metrics["expired_leases_requeued"] += 1
            return
        task.status = "failed"
        task.last_error = "lease_expired_retry_limit"
        self._failed_tasks.add(task.task_id)
        self._append_result_locked(
            LabResult(
                task_id=task.task_id,
                lease_id=lease.lease_id,
                worker_id=lease.worker_id,
                lane_id=task.lane_id,
                attempt_id=task.attempt_id,
                status="failed",
                accepted_at=now,
                result={
                    "status": "failed",
                    "error": "lease_expired_retry_limit",
                    "retryable": False,
                    "attempt_number": task.attempt_number,
                },
            )
        )
        self._metrics["failures_final"] += 1

    def _trim_completion_history_locked(self) -> None:
        max_recent = max(int(self.config.max_recent_completions), 0)
        while len(self._recent_completed_order) > max_recent:
            old_lease_id = self._recent_completed_order.popleft()
            self._completed_by_lease.pop(old_lease_id, None)

    def _append_result_locked(self, result: LabResult) -> None:
        self._results.append(result)
        self._trim_result_backlog_locked()

    def _trim_result_backlog_locked(self) -> None:
        max_backlog = max(int(self.config.max_result_backlog), 0)
        while len(self._results) > max_backlog:
            self._results.popleft()
            self._metrics["results_dropped"] += 1


class LabGatewayHttpServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        gateway: PlayHandLabGateway,
        *,
        token: str | None = None,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    ) -> None:
        super().__init__(server_address, LabGatewayRequestHandler)
        self.gateway = gateway
        self.token = token or None
        self.max_body_bytes = max(int(max_body_bytes), 1024)


class LabGatewayRequestHandler(BaseHTTPRequestHandler):
    server: LabGatewayHttpServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._write_json({"ok": True})
            return
        if parsed.path == "/snapshot":
            if not self._authorized():
                self._write_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            query = parse_qs(parsed.query)
            include_workers = str(query.get("include_workers", ["false"])[0]).lower() in {
                "1",
                "true",
                "yes",
            }
            self._write_json(self.server.gateway.snapshot(include_workers=include_workers))
            return
        if parsed.path == "/results":
            if not self._authorized():
                self._write_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            query = parse_qs(parsed.query)
            raw_limit = query.get("limit", [None])[0]
            limit = int(raw_limit) if raw_limit not in {None, ""} else None
            self._write_json({"results": self.server.gateway.read_results(limit=limit)})
            return
        self._write_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        if not self._authorized():
            self._write_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
            return
        payload = self._read_json()
        if payload is None:
            return

        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == "/register":
            contract_hash, capabilities = _worker_contract_fields(payload)
            worker = self.server.gateway.register_worker(
                worker_id=str(payload.get("worker_id") or ""),
                pool=str(payload.get("pool") or "lab"),
                slots=int(payload.get("slots") or 1),
                contract_hash=contract_hash,
                capabilities=capabilities,
            )
            self._write_json({"status": "registered", "worker": worker})
            return
        if path == "/heartbeat":
            ok = self.server.gateway.heartbeat_worker(
                str(payload.get("worker_id") or ""),
                pool=str(payload.get("pool") or "lab"),
                status_detail=(
                    str(payload.get("status_detail"))
                    if payload.get("status_detail") is not None
                    else None
                ),
            )
            if not ok:
                self._write_json({"error": "worker_not_registered"}, status=HTTPStatus.NOT_FOUND)
                return
            self._write_json({"status": "ok"})
            return
        if path == "/claim":
            contract_hash, capabilities = _worker_contract_fields(payload)
            result = self.server.gateway.claim(
                worker_id=str(payload.get("worker_id") or ""),
                pool=str(payload.get("pool") or "lab"),
                contract_hash=contract_hash,
                capabilities=capabilities,
            )
            self._write_json(result)
            return
        if path == "/tasks":
            raw_tasks = payload.get("tasks")
            if isinstance(raw_tasks, list):
                tasks = [LabTask.from_payload(item) for item in raw_tasks if isinstance(item, dict)]
            else:
                tasks = [LabTask.from_payload(payload)]
            self.server.gateway.enqueue_many(tasks)
            self._write_json({"status": "accepted", "enqueued": len(tasks)})
            return
        if path == "/results/ack":
            raw_lease_ids = payload.get("lease_ids")
            lease_ids = [str(item) for item in raw_lease_ids] if isinstance(raw_lease_ids, list) else []
            acked = self.server.gateway.ack_results(lease_ids)
            self._write_json({"status": "acked", "acked": acked})
            return

        parts = [part for part in path.split("/") if part]
        if len(parts) == 3 and parts[0] == "leases":
            lease_id = parts[1]
            action = parts[2]
            worker_id = str(payload.get("worker_id") or "")
            if action == "heartbeat":
                progress = payload.get("progress")
                ok = self.server.gateway.heartbeat_lease(
                    worker_id,
                    lease_id,
                    progress=dict(progress) if isinstance(progress, dict) else None,
                )
                if not ok:
                    self._write_json({"status": "lease_lost", "lease_id": lease_id}, status=HTTPStatus.NOT_FOUND)
                    return
                self._write_json({"status": "ok"})
                return
            if action == "complete":
                result_payload = payload.get("result")
                if not isinstance(result_payload, dict):
                    result_payload = payload.get("final_state")
                result = self.server.gateway.complete(
                    worker_id,
                    lease_id,
                    status=str(payload.get("status") or "success"),
                    result=dict(result_payload) if isinstance(result_payload, dict) else {},
                )
                status = HTTPStatus.NOT_FOUND if result.get("status") == "lease_lost" else HTTPStatus.OK
                self._write_json(result, status=status)
                return
            if action == "fail":
                result = self.server.gateway.fail(
                    worker_id,
                    lease_id,
                    error=str(payload.get("error") or "worker_failed"),
                    retryable=_parse_bool(payload.get("retryable"), default=True),
                )
                status = HTTPStatus.NOT_FOUND if result.get("status") == "lease_lost" else HTTPStatus.OK
                self._write_json(result, status=status)
                return

        self._write_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def _authorized(self) -> bool:
        token = self.server.token
        if not token:
            return True
        header = self.headers.get("Authorization", "")
        expected = f"Bearer {token}"
        return hmac.compare_digest(header, expected)

    def _read_json(self) -> dict[str, Any] | None:
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or "0")
        except ValueError:
            self._write_json({"error": "invalid_content_length"}, status=HTTPStatus.BAD_REQUEST)
            return None
        if length > self.server.max_body_bytes:
            self._write_json({"error": "body_too_large"}, status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            return None
        try:
            body = self.rfile.read(length) if length else b"{}"
            parsed = json.loads(body.decode("utf-8"))
        except Exception:
            self._write_json({"error": "invalid_json"}, status=HTTPStatus.BAD_REQUEST)
            return None
        if not isinstance(parsed, dict):
            self._write_json({"error": "json_object_required"}, status=HTTPStatus.BAD_REQUEST)
            return None
        return parsed

    def _write_json(self, payload: dict[str, Any], *, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_lab_gateway_http_server(
    *,
    host: str,
    port: int,
    token: str | None = None,
    gateway: PlayHandLabGateway | None = None,
) -> LabGatewayHttpServer:
    return LabGatewayHttpServer((host, int(port)), gateway or PlayHandLabGateway(), token=token)


class LabGatewayAsgiApp:
    def __init__(
        self,
        gateway: PlayHandLabGateway,
        *,
        token: str | None = None,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    ) -> None:
        self.gateway = gateway
        self.token = token or None
        self.max_body_bytes = max(int(max_body_bytes), 1024)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") == "websocket":
            await self._handle_websocket(scope, receive, send)
            return
        if scope.get("type") != "http":
            await self._send_json(send, {"error": "unsupported_scope"}, status=500)
            return
        method = str(scope.get("method") or "GET").upper()
        path = str(scope.get("path") or "/").rstrip("/") or "/"
        query = parse_qs(bytes(scope.get("query_string") or b"").decode("utf-8"))
        headers = {
            bytes(key).decode("latin-1").lower(): bytes(value).decode("latin-1")
            for key, value in scope.get("headers", [])
        }

        if method == "GET" and path == "/healthz":
            await self._send_json(send, {"ok": True})
            return
        if not self._authorized(headers.get("authorization")):
            await self._send_json(send, {"error": "unauthorized"}, status=401)
            return

        try:
            if method == "GET" and path == "/snapshot":
                include_workers = str(query.get("include_workers", ["false"])[0]).lower() in {"1", "true", "yes"}
                await self._send_json(send, self.gateway.snapshot(include_workers=include_workers))
                return
            if method == "GET" and path == "/results":
                raw_limit = query.get("limit", [None])[0]
                limit = int(raw_limit) if raw_limit not in {None, ""} else None
                await self._send_json(send, {"results": self.gateway.read_results(limit=limit)})
                return

            payload = await self._read_json(receive)
            if method == "POST" and path == "/tasks":
                raw_tasks = payload.get("tasks")
                if isinstance(raw_tasks, list):
                    tasks = [LabTask.from_payload(item) for item in raw_tasks if isinstance(item, dict)]
                else:
                    tasks = [LabTask.from_payload(payload)]
                self.gateway.enqueue_many(tasks)
                await self._send_json(send, {"status": "accepted", "enqueued": len(tasks)})
                return
            if method == "POST" and path == "/results/ack":
                raw_lease_ids = payload.get("lease_ids")
                lease_ids = [str(item) for item in raw_lease_ids] if isinstance(raw_lease_ids, list) else []
                acked = self.gateway.ack_results(lease_ids)
                await self._send_json(send, {"status": "acked", "acked": acked})
                return
            if method == "POST" and path == "/register":
                contract_hash, capabilities = _worker_contract_fields(payload)
                worker = self.gateway.register_worker(
                    worker_id=str(payload.get("worker_id") or ""),
                    pool=str(payload.get("pool") or "lab"),
                    slots=int(payload.get("slots") or 1),
                    contract_hash=contract_hash,
                    capabilities=capabilities,
                )
                await self._send_json(send, {"status": "registered", "worker": worker})
                return
            if method == "POST" and path == "/heartbeat":
                ok = self.gateway.heartbeat_worker(
                    str(payload.get("worker_id") or ""),
                    pool=str(payload.get("pool") or "lab"),
                    status_detail=(
                        str(payload.get("status_detail")) if payload.get("status_detail") is not None else None
                    ),
                )
                if not ok:
                    await self._send_json(send, {"error": "worker_not_registered"}, status=404)
                    return
                await self._send_json(send, {"status": "ok"})
                return
            if method == "POST" and path == "/claim":
                contract_hash, capabilities = _worker_contract_fields(payload)
                await self._send_json(
                    send,
                    self.gateway.claim(
                        worker_id=str(payload.get("worker_id") or ""),
                        pool=str(payload.get("pool") or "lab"),
                        contract_hash=contract_hash,
                        capabilities=capabilities,
                    ),
                )
                return

            parts = [part for part in path.split("/") if part]
            if method == "POST" and len(parts) == 3 and parts[0] == "leases":
                lease_id = parts[1]
                action = parts[2]
                worker_id = str(payload.get("worker_id") or "")
                if action == "heartbeat":
                    progress = payload.get("progress")
                    ok = self.gateway.heartbeat_lease(
                        worker_id,
                        lease_id,
                        progress=dict(progress) if isinstance(progress, dict) else None,
                    )
                    if not ok:
                        await self._send_json(send, {"status": "lease_lost", "lease_id": lease_id}, status=404)
                        return
                    await self._send_json(send, {"status": "ok"})
                    return
                if action == "complete":
                    result_payload = payload.get("result")
                    if not isinstance(result_payload, dict):
                        result_payload = payload.get("final_state")
                    result = self.gateway.complete(
                        worker_id,
                        lease_id,
                        status=str(payload.get("status") or "success"),
                        result=dict(result_payload) if isinstance(result_payload, dict) else {},
                    )
                    status = 404 if result.get("status") == "lease_lost" else 200
                    await self._send_json(send, result, status=status)
                    return
                if action == "fail":
                    result = self.gateway.fail(
                        worker_id,
                        lease_id,
                        error=str(payload.get("error") or "worker_failed"),
                        retryable=_parse_bool(payload.get("retryable"), default=True),
                    )
                    status = 404 if result.get("status") == "lease_lost" else 200
                    await self._send_json(send, result, status=status)
                    return

            await self._send_json(send, {"error": "not_found"}, status=404)
        except json.JSONDecodeError:
            await self._send_json(send, {"error": "invalid_json"}, status=400)
        except ValueError as exc:
            status = 413 if str(exc) == "body_too_large" else 400
            await self._send_json(send, {"error": str(exc)}, status=status)
        except Exception as exc:
            await self._send_json(send, {"error": str(exc)}, status=500)

    async def _handle_websocket(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = str(scope.get("path") or "/").rstrip("/") or "/"
        query = parse_qs(bytes(scope.get("query_string") or b"").decode("utf-8"))
        headers = {
            bytes(key).decode("latin-1").lower(): bytes(value).decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        query_token = query.get("token", [None])[0]
        if path != "/ws":
            await send({"type": "websocket.close", "code": 1008})
            return
        if not self._authorized(headers.get("authorization"), query_token=query_token):
            await send({"type": "websocket.close", "code": 1008})
            return
        await send({"type": "websocket.accept"})
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "websocket.disconnect":
                return
            if message_type != "websocket.receive":
                continue
            raw_payload = message.get("text")
            if raw_payload is None and message.get("bytes") is not None:
                raw_bytes = bytes(message["bytes"])
                if len(raw_bytes) > self.max_body_bytes:
                    await send({"type": "websocket.close", "code": 1009})
                    return
                raw_payload = raw_bytes.decode("utf-8")
            try:
                if raw_payload is not None and len(str(raw_payload).encode("utf-8")) > self.max_body_bytes:
                    await send({"type": "websocket.close", "code": 1009})
                    return
                payload = json.loads(str(raw_payload or "{}"))
                if not isinstance(payload, dict):
                    raise ValueError("json_object_required")
                response = self._handle_worker_message(payload)
            except Exception as exc:
                response = {"type": "error", "error": str(exc)}
            await send(
                {
                    "type": "websocket.send",
                    "text": json.dumps(response, separators=(",", ":"), sort_keys=True),
                }
            )

    def _handle_worker_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        message_type = str(payload.get("type") or "")
        worker_id = str(payload.get("worker_id") or "")
        pool = str(payload.get("pool") or "lab")
        if message_type == "register":
            contract_hash, capabilities = _worker_contract_fields(payload)
            worker = self.gateway.register_worker(
                worker_id=worker_id,
                pool=pool,
                slots=int(payload.get("slots") or 1),
                contract_hash=contract_hash,
                capabilities=capabilities,
            )
            return {"type": "registered", "status": "registered", "worker": worker}
        if message_type == "heartbeat":
            ok = self.gateway.heartbeat_worker(
                worker_id,
                pool=pool,
                status_detail=(
                    str(payload.get("status_detail")) if payload.get("status_detail") is not None else None
                ),
            )
            return {"type": "heartbeat", "status": "ok" if ok else "worker_not_registered"}
        if message_type == "claim":
            contract_hash, capabilities = _worker_contract_fields(payload)
            result = self.gateway.claim(
                worker_id=worker_id,
                pool=pool,
                contract_hash=contract_hash,
                capabilities=capabilities,
            )
            result["type"] = "claim"
            return result
        if message_type == "lease_heartbeat":
            lease_id = str(payload.get("lease_id") or "")
            progress = payload.get("progress")
            ok = self.gateway.heartbeat_lease(
                worker_id,
                lease_id,
                progress=dict(progress) if isinstance(progress, dict) else None,
            )
            return {"type": "lease_heartbeat", "status": "ok" if ok else "lease_lost", "lease_id": lease_id}
        if message_type == "complete":
            lease_id = str(payload.get("lease_id") or "")
            result_payload = payload.get("result")
            result = self.gateway.complete(
                worker_id,
                lease_id,
                status=str(payload.get("status") or "success"),
                result=dict(result_payload) if isinstance(result_payload, dict) else {},
            )
            result["type"] = "complete"
            return result
        if message_type == "fail":
            lease_id = str(payload.get("lease_id") or "")
            result = self.gateway.fail(
                worker_id,
                lease_id,
                error=str(payload.get("error") or "worker_failed"),
                retryable=_parse_bool(payload.get("retryable"), default=True),
            )
            result["type"] = "fail"
            return result
        return {"type": "error", "error": f"unsupported_message_type:{message_type}"}

    def _authorized(self, authorization: str | None, *, query_token: str | None = None) -> bool:
        if not self.token:
            return True
        return hmac.compare_digest(str(authorization or ""), f"Bearer {self.token}") or hmac.compare_digest(
            str(query_token or ""), self.token
        )

    async def _read_json(self, receive: Any) -> dict[str, Any]:
        chunks: list[bytes] = []
        total_bytes = 0
        while True:
            message = await receive()
            if message.get("type") != "http.request":
                continue
            body = message.get("body") or b""
            if body:
                total_bytes += len(body)
                if total_bytes > self.max_body_bytes:
                    raise ValueError("body_too_large")
                chunks.append(body)
            if not message.get("more_body", False):
                break
        if not chunks:
            return {}
        parsed = json.loads(b"".join(chunks).decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("json_object_required")
        return parsed

    async def _send_json(self, send: Any, payload: dict[str, Any], *, status: int = 200) -> None:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": int(status),
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def create_lab_gateway_app(
    gateway: PlayHandLabGateway,
    *,
    token: str | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> LabGatewayAsgiApp:
    return LabGatewayAsgiApp(gateway, token=token, max_body_bytes=max_body_bytes)


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_uvicorn_gateway_thread(
    gateway: PlayHandLabGateway,
    *,
    token: str | None,
    host: str = "127.0.0.1",
    port: int | None = None,
) -> tuple[Any, threading.Thread, str]:
    import uvicorn

    selected_port = _find_free_tcp_port() if port is None else int(port)
    app = create_lab_gateway_app(gateway, token=token)
    config = uvicorn.Config(
        app,
        host=host,
        port=selected_port,
        log_level="warning",
        access_log=False,
        lifespan="off",
        http="httptools",
        backlog=4096,
        timeout_keep_alive=60,
    )
    server = uvicorn.Server(config)

    def run() -> None:
        asyncio.run(server.serve())

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    base_url = f"http://{host}:{selected_port}"
    deadline = time.time() + 10.0
    while time.time() < deadline:
        try:
            response = requests.get(f"{base_url}/healthz", timeout=1)
            if response.status_code == 200:
                return server, thread, base_url
        except Exception:
            time.sleep(0.05)
    server.should_exit = True
    thread.join(timeout=5)
    raise RuntimeError("Timed out waiting for PlayHand Lab Gateway HTTP server to start.")


def serve_lab_gateway(
    *,
    host: str,
    port: int,
    token: str | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> None:
    import uvicorn

    gateway = PlayHandLabGateway()
    app = create_lab_gateway_app(gateway, token=token, max_body_bytes=max_body_bytes)
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=False,
        http="httptools",
        backlog=4096,
        timeout_keep_alive=60,
    )


def cmd_play_hand_lab_gateway(
    *,
    host: str,
    port: int,
    token: str | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> int:
    token = token or os.environ.get("FUZZFOLIO_LAB_GATEWAY_TOKEN")
    if not token and not _is_loopback_host(host):
        raise RuntimeError("PlayHand Lab gateway requires --token or FUZZFOLIO_LAB_GATEWAY_TOKEN for non-loopback binds.")
    serve_lab_gateway(host=host, port=port, token=token, max_body_bytes=max_body_bytes)
    return 0


def build_fake_tasks(count: int, *, lane_count: int = 1, work_seconds: float = 10.0) -> list[LabTask]:
    tasks: list[LabTask] = []
    lanes = max(int(lane_count), 1)
    for index in range(max(int(count), 0)):
        lane = f"lane-{index % lanes:04d}"
        task_id = f"task-{index:08d}"
        tasks.append(
            LabTask(
                task_id=task_id,
                lane_id=lane,
                attempt_id=f"attempt-{index:08d}",
                task_kind="fake_compute",
                payload={"work_seconds": work_seconds},
                deadline_seconds=max(float(work_seconds) * 4.0, 30.0),
            )
        )
    return tasks


def _sample_work_seconds(config: SaturationSimulationConfig, rng: random.Random) -> float:
    base = max(float(config.fixed_work_seconds), 0.001)
    if config.runtime_distribution == "lognormal":
        # Median stays near base while allowing a long tail.
        return max(rng.lognormvariate(math.log(base), 0.65), 0.001)
    return base


def _sample_loopback_work_seconds(
    *,
    base_work_seconds: float,
    runtime_distribution: Literal["fixed", "lognormal"],
    rng: random.Random,
) -> float:
    base = max(float(base_work_seconds), 0.0)
    if runtime_distribution == "lognormal":
        median = max(base, 0.001)
        return max(rng.lognormvariate(math.log(median), 0.65), 0.0)
    return base


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * min(max(percentile, 0.0), 100.0) / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[int(position)]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


async def run_http_saturation_simulation(config: HttpSaturationSimulationConfig) -> dict[str, Any]:
    worker_count = max(int(config.worker_count), 1)
    target = config.target_completions or worker_count * max(int(config.backlog_multiplier), 1)
    gateway = PlayHandLabGateway()
    gateway.enqueue_many(
        build_fake_tasks(
            target + worker_count * max(int(config.backlog_multiplier), 1),
            lane_count=max(worker_count // 8, 1),
            work_seconds=max(float(config.work_seconds), 0.0),
        )
    )
    server, server_thread, base_url = _start_uvicorn_gateway_thread(gateway, token=config.token)
    headers = {"Authorization": f"Bearer {config.token}"}
    stop = asyncio.Event()
    completed_lock = asyncio.Lock()
    completed = 0
    claim_latencies: list[float] = []
    initial_claim_latencies: list[float] = []
    warm_claim_latencies: list[float] = []
    completion_latencies: list[float] = []
    errors: list[str] = []
    register_retries = 0
    samples: list[dict[str, Any]] = []
    rng = random.Random(config.seed)
    jitter_by_worker = [
        rng.uniform(0.0, max(float(config.startup_jitter_seconds), 0.0)) for _ in range(worker_count)
    ]

    async def mark_completed() -> None:
        nonlocal completed
        async with completed_lock:
            completed += 1
            if completed >= target:
                stop.set()

    async def worker(index: int) -> None:
        nonlocal register_retries
        worker_id = f"http-sim-worker-{index:05d}"
        local_rng = random.Random(config.seed + index * 7919)
        leased_count = 0
        limits = httpx.Limits(max_connections=2, max_keepalive_connections=1)
        timeout = httpx.Timeout(10.0, connect=5.0, pool=5.0)
        try:
            if jitter_by_worker[index] > 0:
                await asyncio.sleep(jitter_by_worker[index])
            if stop.is_set():
                return
            async with httpx.AsyncClient(
                headers=headers,
                timeout=timeout,
                limits=limits,
                trust_env=False,
            ) as client:
                registered = False
                for attempt in range(20):
                    if stop.is_set():
                        return
                    try:
                        response = await client.post(
                            f"{base_url}/register",
                            json={"worker_id": worker_id, "pool": "http-sim", "slots": 1},
                        )
                        response.raise_for_status()
                        registered = True
                        break
                    except Exception:
                        if attempt >= 19 or stop.is_set():
                            raise
                        register_retries += 1
                        await asyncio.sleep(min(0.01 * (attempt + 1), 0.25))
                if not registered:
                    raise RuntimeError("registration failed")
                while not stop.is_set():
                    started = time.perf_counter()
                    claim_response = await client.post(
                        f"{base_url}/claim",
                        json={"worker_id": worker_id, "pool": "http-sim"},
                    )
                    claim_elapsed = time.perf_counter() - started
                    claim_latencies.append(claim_elapsed)
                    if leased_count == 0:
                        initial_claim_latencies.append(claim_elapsed)
                    else:
                        warm_claim_latencies.append(claim_elapsed)
                    claim_response.raise_for_status()
                    claim_payload = claim_response.json()
                    if claim_payload.get("status") != "leased":
                        await asyncio.sleep(float(claim_payload.get("retry_after_seconds") or 0.01))
                        continue
                    lease_id = str(claim_payload["lease_id"])
                    leased_count += 1
                    work_seconds = _sample_loopback_work_seconds(
                        base_work_seconds=config.work_seconds,
                        runtime_distribution=config.runtime_distribution,
                        rng=local_rng,
                    )
                    if work_seconds > 0:
                        await asyncio.sleep(work_seconds)
                    started = time.perf_counter()
                    complete_response = await client.post(
                        f"{base_url}/leases/{lease_id}/complete",
                        json={
                            "worker_id": worker_id,
                            "status": "success",
                            "result": {"simulated": True, "worker_index": index},
                        },
                    )
                    completion_latencies.append(time.perf_counter() - started)
                    complete_response.raise_for_status()
                    if complete_response.json().get("status") in {"accepted", "duplicate"}:
                        await mark_completed()
        except Exception as exc:
            errors.append(f"{worker_id}: {exc}")
            stop.set()

    async def sampler(client: httpx.AsyncClient) -> None:
        while not stop.is_set():
            try:
                response = await client.get(f"{base_url}/snapshot")
                response.raise_for_status()
                samples.append(response.json())
            except Exception as exc:
                errors.append(f"sampler: {exc}")
                stop.set()
                return
            await asyncio.sleep(max(float(config.sample_interval_seconds), 0.001))

    started_at = time.perf_counter()
    async with httpx.AsyncClient(headers=headers, timeout=10.0, trust_env=False) as client:
        sampler_task = asyncio.create_task(sampler(client))
        workers = [asyncio.create_task(worker(index)) for index in range(worker_count)]
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(float(config.max_wall_seconds), 0.1))
        except asyncio.TimeoutError:
            stop.set()
        await asyncio.gather(*workers, return_exceptions=True)
        sampler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sampler_task
    wall_seconds = time.perf_counter() - started_at
    snapshot = gateway.snapshot()
    saturated_samples = [
        sample
        for sample in samples
        if int(sample.get("worker_count") or 0) >= worker_count
        and int(sample.get("queued_tasks") or 0) >= worker_count
        and int(sample.get("active_leases") or 0) > 0
    ]
    saturated_busy_rates = [float(sample.get("worker_busy_rate") or 0.0) for sample in saturated_samples]
    server.should_exit = True
    server_thread.join(timeout=5)
    return {
        "ok": not errors and snapshot["completed_tasks"] >= target,
        "base_url": base_url,
        "target_completions": target,
        "runtime_distribution": config.runtime_distribution,
        "wall_seconds": round(wall_seconds, 3),
        "snapshot": snapshot,
        "sample_count": len(samples),
        "saturated_sample_count": len(saturated_samples),
        "saturated_busy_rate_avg": statistics.fmean(saturated_busy_rates) if saturated_busy_rates else 0.0,
        "saturated_busy_rate_min": min(saturated_busy_rates) if saturated_busy_rates else 0.0,
        "claim_latency_p50_ms": round(_percentile(claim_latencies, 50) * 1000.0, 3),
        "claim_latency_p95_ms": round(_percentile(claim_latencies, 95) * 1000.0, 3),
        "claim_latency_p99_ms": round(_percentile(claim_latencies, 99) * 1000.0, 3),
        "initial_claim_latency_p95_ms": round(_percentile(initial_claim_latencies, 95) * 1000.0, 3),
        "warm_claim_latency_p95_ms": round(_percentile(warm_claim_latencies, 95) * 1000.0, 3),
        "completion_latency_p50_ms": round(_percentile(completion_latencies, 50) * 1000.0, 3),
        "completion_latency_p95_ms": round(_percentile(completion_latencies, 95) * 1000.0, 3),
        "completion_latency_p99_ms": round(_percentile(completion_latencies, 99) * 1000.0, 3),
        "register_retries": register_retries,
        "errors": errors[:20],
    }


def run_http_saturation_simulation_sync(config: HttpSaturationSimulationConfig) -> dict[str, Any]:
    return asyncio.run(run_http_saturation_simulation(config))


async def run_websocket_saturation_simulation(config: WebSocketSaturationSimulationConfig) -> dict[str, Any]:
    worker_count = max(int(config.worker_count), 1)
    target = config.target_completions or worker_count * max(int(config.backlog_multiplier), 1)
    gateway = PlayHandLabGateway()
    gateway.enqueue_many(
        build_fake_tasks(
            target + worker_count * max(int(config.backlog_multiplier), 1),
            lane_count=max(worker_count // 8, 1),
            work_seconds=max(float(config.work_seconds), 0.0),
        )
    )
    server, server_thread, base_url = _start_uvicorn_gateway_thread(gateway, token=config.token)
    ws_url = base_url.replace("http://", "ws://", 1).replace("https://", "wss://", 1) + f"/ws?token={config.token}"
    headers = {"Authorization": f"Bearer {config.token}"}
    stop = asyncio.Event()
    completed_lock = asyncio.Lock()
    completed = 0
    claim_latencies: list[float] = []
    initial_claim_latencies: list[float] = []
    warm_claim_latencies: list[float] = []
    completion_latencies: list[float] = []
    errors: list[str] = []
    samples: list[dict[str, Any]] = []
    rng = random.Random(config.seed)
    jitter_by_worker = [
        rng.uniform(0.0, max(float(config.startup_jitter_seconds), 0.0)) for _ in range(worker_count)
    ]

    async def mark_completed() -> None:
        nonlocal completed
        async with completed_lock:
            completed += 1
            if completed >= target:
                stop.set()

    async def send_message(websocket: Any, payload: dict[str, Any]) -> dict[str, Any]:
        await websocket.send(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        response = json.loads(await websocket.recv())
        return response if isinstance(response, dict) else {}

    async def worker(index: int) -> None:
        worker_id = f"ws-sim-worker-{index:05d}"
        local_rng = random.Random(config.seed + index * 7919)
        leased_count = 0
        try:
            if jitter_by_worker[index] > 0:
                await asyncio.sleep(jitter_by_worker[index])
            if stop.is_set():
                return
            async with websocket_connect(
                ws_url,
                additional_headers=headers,
                open_timeout=10,
                ping_interval=20,
                ping_timeout=20,
                max_queue=4,
                compression=None,
            ) as websocket:
                registered = await send_message(
                    websocket,
                    {
                        "type": "register",
                        "worker_id": worker_id,
                        "pool": "ws-sim",
                        "slots": 1,
                    },
                )
                if registered.get("status") != "registered":
                    raise RuntimeError(f"registration failed: {registered}")
                while not stop.is_set():
                    started = time.perf_counter()
                    claim_payload = await send_message(
                        websocket,
                        {"type": "claim", "worker_id": worker_id, "pool": "ws-sim"},
                    )
                    claim_elapsed = time.perf_counter() - started
                    claim_latencies.append(claim_elapsed)
                    if leased_count == 0:
                        initial_claim_latencies.append(claim_elapsed)
                    else:
                        warm_claim_latencies.append(claim_elapsed)
                    if claim_payload.get("status") != "leased":
                        await asyncio.sleep(float(claim_payload.get("retry_after_seconds") or 0.01))
                        continue
                    lease_id = str(claim_payload["lease_id"])
                    leased_count += 1
                    work_seconds = _sample_loopback_work_seconds(
                        base_work_seconds=config.work_seconds,
                        runtime_distribution=config.runtime_distribution,
                        rng=local_rng,
                    )
                    if work_seconds > 0:
                        await asyncio.sleep(work_seconds)
                    started = time.perf_counter()
                    complete_payload = await send_message(
                        websocket,
                        {
                            "type": "complete",
                            "worker_id": worker_id,
                            "lease_id": lease_id,
                            "status": "success",
                            "result": {"simulated": True, "worker_index": index},
                        },
                    )
                    completion_latencies.append(time.perf_counter() - started)
                    if complete_payload.get("status") in {"accepted", "duplicate"}:
                        await mark_completed()
        except Exception as exc:
            errors.append(f"{worker_id}: {exc}")
            stop.set()

    async def sampler(client: httpx.AsyncClient) -> None:
        while not stop.is_set():
            try:
                response = await client.get(f"{base_url}/snapshot")
                response.raise_for_status()
                samples.append(response.json())
            except Exception as exc:
                errors.append(f"sampler: {exc}")
                stop.set()
                return
            await asyncio.sleep(max(float(config.sample_interval_seconds), 0.001))

    started_at = time.perf_counter()
    async with httpx.AsyncClient(headers=headers, timeout=10.0, trust_env=False) as client:
        sampler_task = asyncio.create_task(sampler(client))
        workers = [asyncio.create_task(worker(index)) for index in range(worker_count)]
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(float(config.max_wall_seconds), 0.1))
        except asyncio.TimeoutError:
            stop.set()
        await asyncio.gather(*workers, return_exceptions=True)
        sampler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sampler_task
    wall_seconds = time.perf_counter() - started_at
    snapshot = gateway.snapshot()
    saturated_samples = [
        sample
        for sample in samples
        if int(sample.get("worker_count") or 0) >= worker_count
        and int(sample.get("queued_tasks") or 0) >= worker_count
        and int(sample.get("active_leases") or 0) > 0
    ]
    saturated_busy_rates = [float(sample.get("worker_busy_rate") or 0.0) for sample in saturated_samples]
    server.should_exit = True
    server_thread.join(timeout=5)
    return {
        "ok": not errors and snapshot["completed_tasks"] >= target,
        "base_url": base_url,
        "target_completions": target,
        "runtime_distribution": config.runtime_distribution,
        "wall_seconds": round(wall_seconds, 3),
        "snapshot": snapshot,
        "sample_count": len(samples),
        "saturated_sample_count": len(saturated_samples),
        "saturated_busy_rate_avg": statistics.fmean(saturated_busy_rates) if saturated_busy_rates else 0.0,
        "saturated_busy_rate_min": min(saturated_busy_rates) if saturated_busy_rates else 0.0,
        "claim_latency_p50_ms": round(_percentile(claim_latencies, 50) * 1000.0, 3),
        "claim_latency_p95_ms": round(_percentile(claim_latencies, 95) * 1000.0, 3),
        "claim_latency_p99_ms": round(_percentile(claim_latencies, 99) * 1000.0, 3),
        "initial_claim_latency_p95_ms": round(_percentile(initial_claim_latencies, 95) * 1000.0, 3),
        "warm_claim_latency_p95_ms": round(_percentile(warm_claim_latencies, 95) * 1000.0, 3),
        "completion_latency_p50_ms": round(_percentile(completion_latencies, 50) * 1000.0, 3),
        "completion_latency_p95_ms": round(_percentile(completion_latencies, 95) * 1000.0, 3),
        "completion_latency_p99_ms": round(_percentile(completion_latencies, 99) * 1000.0, 3),
        "errors": errors[:20],
    }


def run_websocket_saturation_simulation_sync(config: WebSocketSaturationSimulationConfig) -> dict[str, Any]:
    return asyncio.run(run_websocket_saturation_simulation(config))


async def run_saturation_simulation(config: SaturationSimulationConfig) -> dict[str, Any]:
    worker_count = max(int(config.worker_count), 1)
    target = config.target_completions or worker_count * max(int(config.backlog_multiplier), 1)
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            lease_ttl_seconds=max(config.fixed_work_seconds * config.time_scale * 10.0, 5.0),
            no_work_retry_after_seconds=0.001,
        )
    )
    gateway.enqueue_many(
        build_fake_tasks(
            target + worker_count * max(config.backlog_multiplier, 1),
            lane_count=max(worker_count // 8, 1),
            work_seconds=config.fixed_work_seconds,
        )
    )
    rng = random.Random(config.seed)
    stop = asyncio.Event()
    samples: list[dict[str, Any]] = []

    async def sampler() -> None:
        while not stop.is_set():
            snapshot = gateway.snapshot()
            samples.append(
                {
                    "at": time.monotonic(),
                    "busy_worker_count": snapshot["busy_worker_count"],
                    "worker_count": snapshot["worker_count"],
                    "worker_busy_rate": snapshot["worker_busy_rate"],
                    "queued_tasks": snapshot["queued_tasks"],
                    "active_leases": snapshot["active_leases"],
                    "completed_tasks": snapshot["completed_tasks"],
                }
            )
            await asyncio.sleep(max(config.sample_interval_seconds, 0.001))

    async def worker(index: int) -> None:
        worker_id = f"sim-worker-{index:05d}"
        gateway.register_worker(worker_id, pool="sim")
        local_rng = random.Random(config.seed + index + 17)
        if config.worker_crash_rate > 0 and local_rng.random() < config.worker_crash_rate:
            return
        while not stop.is_set():
            claim = gateway.claim(worker_id, pool="sim")
            if claim.get("status") != "leased":
                await asyncio.sleep(0.001)
                continue
            lease_id = str(claim["lease_id"])
            work_seconds = _sample_work_seconds(config, local_rng) * max(float(config.time_scale), 0.000001)
            await asyncio.sleep(work_seconds)
            if config.failure_rate > 0 and local_rng.random() < config.failure_rate:
                gateway.fail(worker_id, lease_id, error="simulated_retryable_failure", retryable=True)
            else:
                gateway.complete(
                    worker_id,
                    lease_id,
                    result={
                        "simulated": True,
                        "worker_index": index,
                        "work_seconds": work_seconds,
                    },
                )
            snapshot = gateway.snapshot()
            if snapshot["completed_tasks"] >= target:
                stop.set()

    started_at = time.monotonic()
    sampler_task = asyncio.create_task(sampler())
    workers = [asyncio.create_task(worker(index)) for index in range(worker_count)]
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(float(config.max_wall_seconds), 0.1))
    except asyncio.TimeoutError:
        stop.set()
    await asyncio.gather(*workers, return_exceptions=True)
    sampler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await sampler_task
    finished_at = time.monotonic()
    snapshot = gateway.snapshot()
    steady_samples = samples[max(len(samples) // 4, 0) :]
    busy_rates = [float(sample["worker_busy_rate"]) for sample in steady_samples]
    saturated_samples = [
        sample
        for sample in samples
        if int(sample["worker_count"]) >= worker_count
        and int(sample["queued_tasks"]) >= worker_count
        and int(sample["active_leases"]) > 0
    ]
    saturated_busy_rates = [float(sample["worker_busy_rate"]) for sample in saturated_samples]
    return {
        "ok": snapshot["completed_tasks"] >= target,
        "target_completions": target,
        "wall_seconds": round(finished_at - started_at, 3),
        "snapshot": snapshot,
        "sample_count": len(samples),
        "steady_busy_rate_avg": statistics.fmean(busy_rates) if busy_rates else 0.0,
        "steady_busy_rate_min": min(busy_rates) if busy_rates else 0.0,
        "steady_busy_rate_p50": statistics.median(busy_rates) if busy_rates else 0.0,
        "saturated_sample_count": len(saturated_samples),
        "saturated_busy_rate_avg": statistics.fmean(saturated_busy_rates) if saturated_busy_rates else 0.0,
        "saturated_busy_rate_min": min(saturated_busy_rates) if saturated_busy_rates else 0.0,
        "saturated_busy_rate_p50": statistics.median(saturated_busy_rates) if saturated_busy_rates else 0.0,
        "samples_tail": samples[-10:],
    }


def run_saturation_simulation_sync(config: SaturationSimulationConfig) -> dict[str, Any]:
    return asyncio.run(run_saturation_simulation(config))


def cmd_play_hand_lab_sim(
    *,
    workers: int,
    target_completions: int | None = None,
    fixed_work_seconds: float = 10.0,
    time_scale: float = 0.001,
    max_wall_seconds: float = 15.0,
    runtime_distribution: str = "fixed",
) -> dict[str, Any]:
    distribution: Literal["fixed", "lognormal"] = "lognormal" if runtime_distribution == "lognormal" else "fixed"
    return run_saturation_simulation_sync(
        SaturationSimulationConfig(
            worker_count=workers,
            target_completions=target_completions,
            fixed_work_seconds=fixed_work_seconds,
            time_scale=time_scale,
            max_wall_seconds=max_wall_seconds,
            runtime_distribution=distribution,
        )
    )


def cmd_play_hand_lab_http_sim(
    *,
    workers: int,
    target_completions: int | None = None,
    work_seconds: float = 0.01,
    runtime_distribution: str = "fixed",
    startup_jitter_seconds: float = 0.5,
    max_wall_seconds: float = 30.0,
) -> dict[str, Any]:
    distribution: Literal["fixed", "lognormal"] = "lognormal" if runtime_distribution == "lognormal" else "fixed"
    return run_http_saturation_simulation_sync(
        HttpSaturationSimulationConfig(
            worker_count=workers,
            target_completions=target_completions,
            work_seconds=work_seconds,
            runtime_distribution=distribution,
            startup_jitter_seconds=startup_jitter_seconds,
            max_wall_seconds=max_wall_seconds,
        )
    )


def cmd_play_hand_lab_ws_sim(
    *,
    workers: int,
    target_completions: int | None = None,
    work_seconds: float = 0.01,
    runtime_distribution: str = "fixed",
    startup_jitter_seconds: float = 0.5,
    max_wall_seconds: float = 30.0,
) -> dict[str, Any]:
    distribution: Literal["fixed", "lognormal"] = "lognormal" if runtime_distribution == "lognormal" else "fixed"
    return run_websocket_saturation_simulation_sync(
        WebSocketSaturationSimulationConfig(
            worker_count=workers,
            target_completions=target_completions,
            work_seconds=work_seconds,
            runtime_distribution=distribution,
            startup_jitter_seconds=startup_jitter_seconds,
            max_wall_seconds=max_wall_seconds,
        )
    )


__all__ = [
    "HttpSaturationSimulationConfig",
    "LabGatewayConfig",
    "DEFAULT_MAX_BODY_BYTES",
    "LabLease",
    "LabResult",
    "LabTask",
    "LabWorker",
    "PlayHandLabGateway",
    "SaturationSimulationConfig",
    "WebSocketSaturationSimulationConfig",
    "build_fake_tasks",
    "build_lab_gateway_http_server",
    "cmd_play_hand_lab_gateway",
    "cmd_play_hand_lab_http_sim",
    "cmd_play_hand_lab_sim",
    "cmd_play_hand_lab_ws_sim",
    "create_lab_gateway_app",
    "run_http_saturation_simulation_sync",
    "run_saturation_simulation",
    "run_saturation_simulation_sync",
    "run_websocket_saturation_simulation",
    "run_websocket_saturation_simulation_sync",
    "serve_lab_gateway",
]
