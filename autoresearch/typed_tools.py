"""Named constants and helpers for the typed research tool surface.

Typed tools wrap the Fuzzfolio CLI so the model can pass domain-shaped arguments
instead of reconstructing shell syntax. `run_cli` remains a fallback.
"""

from __future__ import annotations

from typing import Any

PRIMARY_TYPED_TOOLS = frozenset(
    {
        "prepare_profile",
        "mutate_profile",
        "validate_profile",
        "register_profile",
        "evaluate_candidate",
        "run_parameter_sweep",
        "inspect_artifact",
        "compare_artifacts",
    }
)

SECONDARY_TOOLS = frozenset(
    {
        "run_cli",
        "write_file",
        "read_file",
        "list_dir",
        "log_attempt",
        "finish",
    }
)

ALL_CONTROLLER_TOOLS = PRIMARY_TYPED_TOOLS | SECONDARY_TOOLS

# Tools whose results include CLI `ok` + embedded `result` payload like run_cli.
TYPED_TOOLS_CLI_WRAPPER = frozenset(
    {
        "evaluate_candidate",
        "prepare_profile",
        "mutate_profile",
        "validate_profile",
        "register_profile",
        "run_parameter_sweep",
    }
)


def tools_with_cli_ok_semantics() -> frozenset[str]:
    return frozenset({"run_cli", *TYPED_TOOLS_CLI_WRAPPER})


CLI_OK_TOOLS = tools_with_cli_ok_semantics()


def normalized_tool_envelope(
    tool: str,
    *,
    ok: bool,
    warnings: list[str] | None = None,
    errors: list[str] | None = None,
    artifacts: dict[str, Any] | None = None,
    state_updates: dict[str, Any] | None = None,
    next_recommended_action: str | None = None,
    status: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "tool": tool,
        "ok": ok,
        "status": status or ("ok" if ok else "failed"),
        "warnings": list(warnings or []),
        "errors": list(errors or []),
        "artifacts": dict(artifacts or {}),
        "state_updates": dict(state_updates or {}),
        "next_recommended_action": next_recommended_action,
    }
    for key, value in extra.items():
        if value is not None:
            payload[key] = value
    return payload
