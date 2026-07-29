from __future__ import annotations

from pathlib import Path
from typing import Any

from . import temporal_graph_lab_core as _core
from .temporal_graph_lab_core import *  # noqa: F401,F403
from .temporal_graph_lab_core import (
    LabGatewayClientProtocol,
    _normalize_execution_cell,
    _normalize_profile,
)


def run_temporal_graph_lab_tasks(
    client: LabGatewayClientProtocol,
    tasks: list[dict[str, Any]],
    *,
    output_root: Path | str,
    timeout_seconds: float = 900.0,
    poll_interval_seconds: float = 0.25,
) -> list[dict[str, Any]]:
    """Run the canonical resumable temporal Lab coordinator.

    Importing the coordinator lazily avoids a module cycle while ensuring callers
    of the original public module receive the same preexisting-result recovery,
    validate/materialize-before-ack ordering, and duplicate conflict checks used
    by the CLI and native acceptance probe.
    """

    from .temporal_graph_lab_coordinator import (
        run_temporal_graph_lab_tasks as _run_temporal_graph_lab_tasks,
    )

    return _run_temporal_graph_lab_tasks(
        client,
        tasks,
        output_root=output_root,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


def __getattr__(name: str) -> Any:
    """Preserve access to implementation helpers during the facade split."""

    return getattr(_core, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_core)))


__all__ = list(_core.__all__)
