"""Regression checks for the AutoResearch Process Manager configuration."""

from __future__ import annotations

import json
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPOSITORY_ROOT / "scripts" / "processes.json"
VENV_PREFIX = "C:\\repos\\fuzzfolio-autoresearch\\.venv\\Scripts\\"
AUTHORITY_PATH = (
    "C:\\repos\\fuzzfolio-autoresearch\\runs\\derived\\phase3-authorities\\"
    "phase3-darwin-rich-ab-v2\\phase3-playhand-authority.json"
)
CAPSULE_PATH = (
    "C:\\repos\\fuzzfolio-autoresearch\\runs\\derived\\phase2-atlas-capsules\\"
    "phase2-atlas-authority-capsule-20260721"
)
POLICY_PATH = "C:\\repos\\fuzzfolio-autoresearch\\configs\\phase3-campaign-policy.json"


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

    assert [process["name"] for process in normal_processes] == [
        "Lab Gateway",
        "Phase 3 PlayHand - Fresh",
        "Phase 3 PlayHand - Resume",
        "AutoResearch Dashboard",
        "Phase 3 Authority Audit",
    ]
    assert all(
        str(process["command"]).startswith(VENV_PREFIX)
        for process in normal_processes
    )

    fresh = normal_processes[1]
    resume = normal_processes[2]
    for process, lifecycle_flag in ((fresh, "--fresh"), (resume, "--resume")):
        command = str(process["command"])
        assert "phase3-playhand.exe" in command
        assert lifecycle_flag in command
        assert "--gateway-url http://127.0.0.1:8799" in command
        assert "--active-runs 128" in command
        assert "--trading-dashboard-root C:\\repos\\Trading-Dashboard" in command
        assert AUTHORITY_PATH in command
        assert CAPSULE_PATH in command
        assert POLICY_PATH in command
        assert process["auto_restart"] is False

        for forbidden_override in (
            "--target-runs",
            "--campaign-id",
            "--as-of-date",
            "--seed",
            "--instrument",
            "--timeframe",
            "--lookback-months",
            "--policy-manifest-sha256",
        ):
            assert forbidden_override not in command

    assert "--fresh" not in str(resume["command"])
    assert "--resume" not in str(fresh["command"])

    authority_audit = normal_processes[4]
    audit_command = str(authority_audit["command"])
    assert "phase3-playhand-authority.exe" in audit_command
    assert "--audit" in audit_command
    assert AUTHORITY_PATH in audit_command
    assert CAPSULE_PATH in audit_command
    assert POLICY_PATH in audit_command


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
        assert command.startswith(VENV_PREFIX)
        assert "uv run" not in command
