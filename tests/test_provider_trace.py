from __future__ import annotations

from pathlib import Path

from autoresearch.__main__ import build_parser
from autoresearch.provider import (
    ChatMessage,
    _write_provider_request_snapshot,
    provider_trace_scope,
)


def test_request_snapshot_writes_human_readable_text_files(tmp_path: Path) -> None:
    with provider_trace_scope(
        label="explorer_step",
        run_id="run-a",
        step=7,
        phase="explorer_provider",
        provider_type="lmstudio",
        model="gemma-4-E4B-it",
        request_snapshot_dir=str(tmp_path),
    ):
        _write_provider_request_snapshot(
            "chat_completions_request",
            messages=[
                ChatMessage(role="system", content="Return JSON only."),
                ChatMessage(role="user", content="Pick the next tool."),
            ],
            request_payload={"model": "gemma-4-E4B-it", "temperature": 0.2},
        )
        _write_provider_request_snapshot(
            "chat_completions_request",
            messages=[ChatMessage(role="user", content="Retry with one action.")],
            request_payload={"model": "gemma-4-E4B-it", "temperature": 0.2},
        )

    snapshots = sorted(tmp_path.glob("*.txt"))
    assert [path.name for path in snapshots] == [
        "step-0007__req-001__chat-completions-request__explorer-provider__explorer-step.txt",
        "step-0007__req-002__chat-completions-request__explorer-provider__explorer-step.txt",
    ]
    content = snapshots[0].read_text(encoding="utf-8")
    assert (
        "===== INFORMATIONAL ONLY: DIAGNOSTIC SNAPSHOT METADATA (NOT SENT TO API OR MODEL) ====="
        in content
    )
    assert "===== LITERALLY SENT TO API: HTTP REQUEST JSON BODY =====" in content
    assert (
        "===== INFORMATIONAL ONLY: READABLE VIEW OF MESSAGE CONTENT FROM THE REQUEST ====="
        in content
    )
    assert '"request_sequence": 1' in content
    assert "[message 1] role=system" in content
    assert "Return JSON only." in content
    assert '"model": "gemma-4-E4B-it"' in content


def test_request_snapshot_is_noop_without_snapshot_dir(tmp_path: Path) -> None:
    with provider_trace_scope(
        label="explorer_step",
        run_id="run-a",
        step=7,
        phase="explorer_provider",
        provider_type="lmstudio",
        model="gemma-4-E4B-it",
    ):
        _write_provider_request_snapshot(
            "chat_completions_request",
            messages=[ChatMessage(role="user", content="hello")],
        )

    assert list(tmp_path.glob("*.txt")) == []


def test_build_parser_accepts_llm_request_snapshot_flag() -> None:
    parser = build_parser()
    args = parser.parse_args(["run", "--llm-request-snapshots", "--max-steps", "3"])
    assert args.command == "run"
    assert args.llm_request_snapshots is True

    args = parser.parse_args(["supervise", "--llm-request-snapshots"])
    assert args.command == "supervise"
    assert args.llm_request_snapshots is True
