"""Regression checks for the AutoResearch Process Manager configuration."""

from __future__ import annotations

import json
from pathlib import Path
import re


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPOSITORY_ROOT / "scripts" / "processes.json"
VENV_PREFIX = "C:\\repos\\fuzzfolio-autoresearch\\.venv\\Scripts\\"


def _command_argument(command: str, name: str) -> str:
    match = re.search(rf"(?:^|\s){re.escape(name)}\s+(?:\"([^\"]+)\"|(\S+))", command)
    assert match is not None, f"missing {name}"
    return match.group(1) or match.group(2)
def _config() -> dict[str, object]:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _processes(config: dict[str, object]) -> dict[str, dict[str, object]]:
    processes = config["processes"]
    assert isinstance(processes, list)
    indexed = {str(process["id"]): process for process in processes}
    assert len(indexed) == len(processes)
    return indexed


def _group(config: dict[str, object], name: str) -> dict[str, object]:
    groups = config["groups"]
    assert isinstance(groups, list)
    return next(group for group in groups if group["name"] == name)


def test_process_manager_groups_reference_unique_processes() -> None:
    config = _config()
    processes = _processes(config)
    grouped_ids = [
        process_id
        for group in config["groups"]
        for process_id in group["process_ids"]
    ]

    assert len(grouped_ids) == len(set(grouped_ids))
    assert set(grouped_ids) == set(processes)
    assert set(group["name"] for group in config["groups"]) == {
        "Normal Operations",
        "Corpus Maintenance (Manual)",
        "Safe Maintenance Preview",
        "Historical Evidence (Advanced)",
        "Atlas Manual (Advanced)",
    }


def test_normal_operations_are_authority_bound_and_semantically_closed() -> None:
    config = _config()
    processes = _processes(config)
    normal = _group(config, "Normal Operations")
    normal_processes = [processes[process_id] for process_id in normal["process_ids"]]

    assert [process["name"] for process in normal_processes[2:]] == [
        "Temporal Search - Fresh",
        "Temporal Search - Resume",
        "AutoResearch Dashboard",
        "Temporal Search Authority Audit",
    ]
    assert normal_processes[0]["name"] == "Lab Gateway"
    worker = normal_processes[1]
    assert worker["name"] in {
        "Temporal Graph Local Worker",
        "Temporal Graph Frozen Worker (b69e)",
    }
    if worker["name"] == "Temporal Graph Local Worker":
        assert "start-local-lab-ws-worker.ps1" in str(worker["command"])
        worker_launcher = (
            REPOSITORY_ROOT / "scripts" / "start-local-lab-ws-worker.ps1"
        ).read_text(encoding="utf-8")
        assert "sim-worker-replay.exe" in worker_launcher
        assert "from app.cli import sim_worker_replay" not in worker_launcher
        assert "FUZZFOLIO_WORKER_LAUNCHER_PID" in worker_launcher
    else:
        assert worker["process_type"] == "Docker"
        assert worker["command"] == "fuzzfolio-stage5e0-containment-worker"

    fresh = normal_processes[2]
    resume = normal_processes[3]
    authority_paths = set()
    output_roots = set()
    for process, lifecycle_flag in ((fresh, "--fresh"), (resume, "--resume")):
        command = str(process["command"])
        assert "uv run temporal-search" in command
        assert lifecycle_flag in command
        assert "--gateway-url http://127.0.0.1:8799" in command
        assert "--timeout-seconds 900" in command
        authority_paths.add(_command_argument(command, "--authority-path"))
        output_roots.add(_command_argument(command, "--output-root"))
        assert process["auto_restart"] is False
        assert "--plan-only" not in command

    assert "--fresh" not in str(resume["command"])
    assert "--resume" not in str(fresh["command"])
    assert len(authority_paths) == 1
    assert len(output_roots) == 1
    authority_path = next(iter(authority_paths))
    assert authority_path.lower().endswith("authority.json")

    authority_audit = normal_processes[5]
    audit_command = str(authority_audit["command"])
    assert "uv run temporal-search-authority" in audit_command
    assert "--audit" in audit_command
    assert _command_argument(audit_command, "--authority-path") == authority_path


def test_legacy_level_c_and_destructive_apply_controls_are_absent() -> None:
    config = _config()
    names = [process["name"] for process in config["processes"]]

    assert "Level C Bootstrap" not in names
    assert "Level C Run Cutoff A" not in names
    assert "Level C Run Cutoff B" not in names
    assert "Level C Run Cutoff C" not in names
    assert "Level C Run Cutoff D" not in names
    assert "Cleanup Atlas Artifacts - Apply" not in names
    assert "Portfolio Research - Darwin Master" not in names
    assert "Portfolio Research - Resume Latest" not in names
    assert "Portfolio Research Report - Latest" not in names


def test_all_configured_commands_use_direct_venv_wrappers() -> None:
    config = _config()

    for process in config["processes"]:
        command = str(process["command"])
        if process.get("process_type") == "Docker":
            assert command.strip()
            continue
        if command.startswith(("uv run ", "powershell ", "pwsh ")):
            continue
        assert command.startswith(VENV_PREFIX)
        assert "uv run" not in command


def test_retired_phase3_ephemeral_worker_entries_are_absent() -> None:
    config = _config()
    group_names = {str(group["name"]) for group in config["groups"]}
    process_names = {str(process["name"]) for process in config["processes"]}

    assert "Ephemeral Workers (Manual)" not in group_names
    assert not any(name.startswith("Generate Ephemeral Windows Workers") for name in process_names)
