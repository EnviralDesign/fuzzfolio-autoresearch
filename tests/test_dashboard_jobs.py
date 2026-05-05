from __future__ import annotations

import time
from types import SimpleNamespace
from threading import Event

import pytest

from autoresearch.dashboard_viewer import DashboardJobManager


class FakeProcess:
    def __init__(self, returncode: int = 0):
        self.returncode = returncode
        self.done = Event()
        self.terminated = False

    def wait(self) -> int:
        self.done.wait(timeout=5)
        return self.returncode

    def finish(self, returncode: int | None = None) -> None:
        if returncode is not None:
            self.returncode = returncode
        self.done.set()

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15
        self.done.set()


def _manager(tmp_path, processes: list[FakeProcess]) -> DashboardJobManager:
    config = SimpleNamespace(repo_root=tmp_path, derived_root=tmp_path / "runs" / "derived")

    def fake_popen(command, **_kwargs):
        process = FakeProcess()
        process.command = command
        processes.append(process)
        return process

    return DashboardJobManager(config, popen_factory=fake_popen)


def _wait_status(manager: DashboardJobManager, job_id: str, status: str) -> dict:
    deadline = time.time() + 5
    while time.time() < deadline:
        record = manager.get(job_id)
        if record and record.get("status") == status:
            return record
        time.sleep(0.02)
    raise AssertionError(f"Timed out waiting for {job_id} to reach {status}")


def test_dashboard_job_manager_starts_completes_and_persists(tmp_path) -> None:
    processes: list[FakeProcess] = []
    manager = _manager(tmp_path, processes)

    job = manager.start("finalize-corpus", {"dry_run": True})
    assert job["status"] == "running"
    assert "finalize-corpus" in job["command"]

    processes[0].finish(0)
    completed = _wait_status(manager, job["id"], "completed")
    assert completed["returncode"] == 0

    reloaded = DashboardJobManager(
        SimpleNamespace(repo_root=tmp_path, derived_root=tmp_path / "runs" / "derived")
    )
    assert reloaded.get(job["id"])["status"] == "completed"


def test_dashboard_job_manager_rejects_second_active_job(tmp_path) -> None:
    processes: list[FakeProcess] = []
    manager = _manager(tmp_path, processes)

    first = manager.start("finalize-corpus", {})
    with pytest.raises(RuntimeError):
        manager.start("build-portfolio", {})

    processes[0].finish(0)
    _wait_status(manager, first["id"], "completed")


def test_dashboard_job_manager_marks_failures(tmp_path) -> None:
    processes: list[FakeProcess] = []
    manager = _manager(tmp_path, processes)

    job = manager.start("finalize-corpus", {})
    processes[0].finish(7)

    failed = _wait_status(manager, job["id"], "failed")
    assert failed["returncode"] == 7


def test_dashboard_job_manager_cancels_active_job(tmp_path) -> None:
    processes: list[FakeProcess] = []
    manager = _manager(tmp_path, processes)

    job = manager.start("finalize-corpus", {})
    canceled = manager.cancel(job["id"])

    assert canceled["status"] == "canceling"
    assert processes[0].terminated is True
    assert _wait_status(manager, job["id"], "canceled")["returncode"] == -15


def test_dashboard_job_manager_build_portfolio_writes_dashboard_config(tmp_path) -> None:
    processes: list[FakeProcess] = []
    manager = _manager(tmp_path, processes)

    job = manager.start(
        "build-portfolio",
        {
            "portfolio_config": {
                "portfolio_name": "test-auto",
                "sleeves": [{"name": "core", "shortlist_size": 2}],
            }
        },
    )

    assert "build-portfolio" in job["command"]
    assert "--portfolio-config" in job["command"]
    assert job["portfolio_config_path"].endswith("dashboard-auto-portfolio.json")
    processes[0].finish(0)
    _wait_status(manager, job["id"], "completed")
