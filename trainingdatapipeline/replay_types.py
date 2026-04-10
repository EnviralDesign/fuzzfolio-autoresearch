"""Shared dataclasses for pipeline discovery and replay stages."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class RunArtifactInventory:
    required_present: dict[str, bool]
    optional_present: dict[str, bool]
    optional_dirs_present: dict[str, bool]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DiscoveredRun:
    run_id: str
    run_dir: Path
    root: Path
    artifact_inventory: RunArtifactInventory
    parsed_started_at: str | None = None
    controller_log_bytes: int | None = None
    attempts_count_hint: int | None = None
    profile_count_hint: int | None = None
    eval_dir_count_hint: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "root": str(self.root),
            "artifact_inventory": self.artifact_inventory.to_dict(),
            "parsed_started_at": self.parsed_started_at,
            "controller_log_bytes": self.controller_log_bytes,
            "attempts_count_hint": self.attempts_count_hint,
            "profile_count_hint": self.profile_count_hint,
            "eval_dir_count_hint": self.eval_dir_count_hint,
        }
