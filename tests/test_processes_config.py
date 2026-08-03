"""Regression checks for the AutoResearch Process Manager configuration."""

from __future__ import annotations

import json
from pathlib import Path
import re

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPOSITORY_ROOT / "scripts" / "processes.json"
MANIFEST_PATH = REPOSITORY_ROOT / "scripts" / "prebroad-procman-manifest.json"
VENV_PREFIX = "C:\\repos\\fuzzfolio-autoresearch\\.venv\\Scripts\\"


def _command_argument(command: str, name: str) -> str:
    match = re.search(rf"(?:^|\s){re.escape(name)}\s+(?:\"([^\"]+)\"|(\S+))", command)
    assert match is not None, f"missing {name}"
    return match.group(1) or match.group(2)
def _config() -> dict[str, object]:
    if not CONFIG_PATH.exists():
        pytest.skip("local Procman configuration is intentionally ignored")
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _manifest() -> dict[str, object]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


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

    assert [process["name"] for process in normal_processes] == [
        "Lab Gateway",
        "Temporal Pre-Broad No-Market Activation Canary",
        "Temporal Pre-Broad Prepare 16 Tasks",
        "Temporal Pre-Broad Fresh 16 Tasks",
        "Temporal Pre-Broad Resume 16 Tasks",
        "Temporal Pre-Broad Authority Audit",
        "AutoResearch Dashboard",
    ]
    assert "Temporal QD Broad Search (10k)" not in [
        process["name"] for process in processes.values()
    ]
    for process in normal_processes:
        assert process["auto_start"] is False
        assert process["auto_restart"] is False
        assert process["respond_to_start_all"] is False
        assert process["respond_to_restart_all"] is False
        command = str(process["command"])
        assert "--broad" not in command
        assert "temporal-qd-supervisor" not in command
        assert "worker" not in process["name"].lower()

    canary = normal_processes[1]
    assert "temporal_prebroad_canary run" in str(canary["command"])
    assert "--dashboard-python" in str(canary["command"])

    prepare, fresh, resume, audit = normal_processes[2:6]
    assert "temporal_prebroad_control prepare" in str(prepare["command"])
    authority_paths = {
        _command_argument(str(process["command"]), "--authority-path")
        for process in (fresh, resume, audit)
    }
    authority_id_paths = {
        _command_argument(str(process["command"]), "--required-authority-id-path")
        for process in (fresh, resume, audit)
    }
    assert len(authority_paths) == len(authority_id_paths) == 1
    assert "temporal_prebroad_control fresh" in str(fresh["command"])
    assert "temporal_prebroad_control resume" in str(resume["command"])
    assert "temporal_prebroad_control audit" in str(audit["command"])


def test_tracked_prebroad_procman_manifest_is_canonical_and_closed() -> None:
    manifest = _manifest()
    assert manifest["schemaVersion"] == "temporal_prebroad_procman_manifest_v1"
    assert len(manifest["prebroadProcesses"]) == 5
    assert manifest["requiredSafetyFlags"] == {
        "auto_start": False,
        "auto_restart": False,
        "respond_to_start_all": False,
        "respond_to_restart_all": False,
    }
    assert manifest["orderedProcessIds"][1:6] == [
        process["id"] for process in manifest["prebroadProcesses"]
    ]
    assert all("temporal-qd-supervisor" not in process["command"] for process in manifest["prebroadProcesses"])
    assert all("--broad" not in process["command"] for process in manifest["prebroadProcesses"])


def test_local_procman_config_matches_tracked_manifest_when_present() -> None:
    if not CONFIG_PATH.exists():
        pytest.skip("local Procman configuration is intentionally ignored")
    from autoresearch.temporal_prebroad_procman import check

    assert check(CONFIG_PATH)["ok"] is True


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
