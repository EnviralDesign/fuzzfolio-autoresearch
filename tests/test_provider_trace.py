from __future__ import annotations

from pathlib import Path
from datetime import datetime

import autoresearch.provider as provider_module
from autoresearch.__main__ import build_parser
from autoresearch.provider import (
    ChatMessage,
    CodexAppServerProvider,
    ProviderProfileConfig,
    ProviderError,
    _CodexAppServerSession,
    _parse_codex_usage_limit_delay_seconds,
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


def test_request_snapshot_pretty_prints_assistant_actions_and_tool_results(tmp_path: Path) -> None:
    with provider_trace_scope(
        label="explorer",
        run_id="run-b",
        step=3,
        phase="explorer_provider",
        provider_type="lmstudio",
        model="gemma-4-E4B-it",
        request_snapshot_dir=str(tmp_path),
    ):
        _write_provider_request_snapshot(
            "chat_completions_request",
            messages=[
                ChatMessage(
                    role="assistant",
                    content=(
                        "Reasoning: Validation succeeded.\n"
                        "Planned actions:\n"
                        '- {"tool":"register_profile","candidate_name":"cand-a"}'
                    ),
                ),
                ChatMessage(
                    role="user",
                    content=(
                        'Tool results:\n[{"tool":"validate_profile","ok":true,'
                        '"candidate_name":"cand-a","indicator_ids":["ADX","SAR_TREND"]}]'
                    ),
                ),
            ],
            request_payload={"model": "gemma-4-E4B-it", "temperature": 0.2},
        )

    content = next(tmp_path.glob("*.txt")).read_text(encoding="utf-8")
    assert "[message 1] role=assistant" in content
    assert "Planned actions:" in content
    assert '- {' in content
    assert '  "tool": "register_profile"' in content
    assert "[message 2] role=user" in content
    assert "Tool results:\n[" in content
    assert '    "tool": "validate_profile"' in content
    assert '        "ADX",' in content


def test_codex_app_server_command_uses_stdio_without_legacy_session_source() -> None:
    session = object.__new__(_CodexAppServerSession)
    session.config = ProviderProfileConfig(provider_type="codex", command="codex")

    command = session._command()

    assert command[1:] == ["app-server", "--listen", "stdio://"]
    assert Path(command[0]).name.lower() in {"codex", "codex.cmd", "codex.exe"}
    assert "--session-source" not in command


def test_codex_app_server_turn_start_params_skip_summary_for_spark() -> None:
    session = object.__new__(_CodexAppServerSession)
    session.config = ProviderProfileConfig(
        provider_type="codex",
        command="codex",
        model="gpt-5.3-codex-spark",
    )

    params = session._turn_start_params("hello", include_summary=session.default_turn_summary_enabled())

    assert "summary" not in params
    assert params["model"] == "gpt-5.3-codex-spark"


def test_codex_app_server_turn_start_params_keep_summary_for_non_spark() -> None:
    session = object.__new__(_CodexAppServerSession)
    session.config = ProviderProfileConfig(
        provider_type="codex",
        command="codex",
        model="gpt-5.4",
    )

    params = session._turn_start_params("hello", include_summary=session.default_turn_summary_enabled())

    assert params["summary"] == "concise"
    assert params["model"] == "gpt-5.4"


def test_codex_provider_retries_without_summary_on_unsupported_summary_error(monkeypatch) -> None:
    class FakeSession:
        def __init__(self, config: ProviderProfileConfig):
            self.config = config
            self.start_turn_calls: list[bool] = []

        def default_turn_summary_enabled(self) -> bool:
            return True

        def start_turn(self, prompt: str, *, include_summary: bool | None = None) -> str:
            assert include_summary is not None
            self.start_turn_calls.append(include_summary)
            return f"turn-{len(self.start_turn_calls)}"

        def collect_turn_text(self, turn_id: str) -> str:
            if self.start_turn_calls[-1]:
                raise ProviderError(
                    "Codex app-server turn failed with status 'failed': "
                    "{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\","
                    "\"code\":\"unsupported_parameter\",\"message\":\"Unsupported parameter: "
                    "'reasoning.summary' is not supported with the 'gpt-5.3-codex-spark' model.\","
                    "\"param\":\"reasoning.summary\"},\"status\":400}"
                )
            return (
                '{"response_json":"{\\"mode\\":\\"runtime_shape\\",'
                '\\"reasoning\\":\\"ok\\",\\"actions\\":[]}"}'
            )

        def close(self) -> None:
            return None

    holder: dict[str, FakeSession] = {}

    def fake_session_factory(config: ProviderProfileConfig) -> FakeSession:
        session = FakeSession(config)
        holder["session"] = session
        return session

    monkeypatch.setattr(provider_module, "_CodexAppServerSession", fake_session_factory)

    provider = CodexAppServerProvider(
        ProviderProfileConfig(
            provider_type="codex",
            command="codex",
            model="gpt-5.4",
        )
    )

    result = provider.complete_json([ChatMessage(role="user", content="hello")])

    assert result == {"mode": "runtime_shape", "reasoning": "ok", "actions": []}
    assert holder["session"].start_turn_calls == [True, False]


def test_codex_provider_repairs_invalid_inner_response_json(monkeypatch) -> None:
    class FakeSession:
        def __init__(self, config: ProviderProfileConfig):
            self.config = config
            self.turn_count = 0

        def default_turn_summary_enabled(self) -> bool:
            return False

        def start_turn(self, prompt: str, *, include_summary: bool | None = None) -> str:
            self.turn_count += 1
            return f"turn-{self.turn_count}"

        def collect_turn_text(self, turn_id: str) -> str:
            if turn_id == "turn-1":
                return (
                    '{"response_json":"{\\"reasoning\\":\\"cand3 passed validation\\",'
                    '\\"actions\\":[{\\"tool\\":\\"register_profile\\",'
                    '\\"candidate_name\\":\\"cand3\\",\\"operation\\":\\"create\\"}]"}'
                )
            return (
                '{"response_json":"{\\"reasoning\\":\\"cand3 passed validation\\",'
                '\\"actions\\":[{\\"tool\\":\\"register_profile\\",'
                '\\"candidate_name\\":\\"cand3\\",\\"operation\\":\\"create\\"}]}"}'
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(provider_module, "_CodexAppServerSession", FakeSession)

    provider = CodexAppServerProvider(
        ProviderProfileConfig(
            provider_type="codex",
            command="codex",
            model="gpt-5.3-codex-spark",
        )
    )

    result = provider.complete_json([ChatMessage(role="user", content="hello")])

    assert result == {
        "reasoning": "cand3 passed validation",
        "actions": [
            {
                "tool": "register_profile",
                "candidate_name": "cand3",
                "operation": "create",
            }
        ],
    }


def test_parse_codex_usage_limit_delay_seconds_uses_reset_clock() -> None:
    now = datetime.now().astimezone().replace(
        year=2026,
        month=4,
        day=13,
        hour=15,
        minute=0,
        second=0,
        microsecond=0,
    )

    delay = _parse_codex_usage_limit_delay_seconds(
        "You've hit your usage limit for GPT-5.3-Codex-Spark. Switch to another model now, or try again at 4:10 PM.",
        now=now,
    )

    assert delay == (70 * 60) + 60


def test_codex_provider_waits_and_retries_once_on_usage_limit(monkeypatch) -> None:
    class FakeSession:
        instance_count = 0

        def __init__(self, config: ProviderProfileConfig):
            self.config = config
            FakeSession.instance_count += 1
            self.instance_id = FakeSession.instance_count

        def default_turn_summary_enabled(self) -> bool:
            return False

        def start_turn(self, prompt: str, *, include_summary: bool | None = None) -> str:
            return f"turn-{self.instance_id}"

        def collect_turn_text(self, turn_id: str) -> str:
            if self.instance_id == 1:
                raise ProviderError(
                    "Codex app-server turn failed with status 'failed': "
                    "You've hit your usage limit for GPT-5.3-Codex-Spark. "
                    "Switch to another model now, or try again at 4:10 PM."
                )
            return (
                '{"response_json":"{\\"mode\\":\\"runtime_shape\\",'
                '\\"reasoning\\":\\"ok\\",\\"actions\\":[]}"}'
            )

        def close(self) -> None:
            return None

    sleep_calls: list[int] = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(int(seconds))

    monkeypatch.setattr(provider_module, "_CodexAppServerSession", FakeSession)
    monkeypatch.setattr(provider_module.time, "sleep", fake_sleep)
    monkeypatch.setattr(
        provider_module,
        "_parse_codex_usage_limit_delay_seconds",
        lambda text, now=None: 123,
    )

    provider = CodexAppServerProvider(
        ProviderProfileConfig(
            provider_type="codex",
            command="codex",
            model="gpt-5.3-codex-spark",
        )
    )

    result = provider.complete_json([ChatMessage(role="user", content="hello")])

    assert result == {"mode": "runtime_shape", "reasoning": "ok", "actions": []}
    assert sleep_calls == [123]


def test_codex_provider_retries_blocked_tool_disabled_payload(monkeypatch) -> None:
    class FakeSession:
        def __init__(self, config: ProviderProfileConfig):
            self.config = config
            self.turn_count = 0

        def default_turn_summary_enabled(self) -> bool:
            return False

        def start_turn(self, prompt: str, *, include_summary: bool | None = None) -> str:
            self.turn_count += 1
            return f"turn-{self.turn_count}"

        def collect_turn_text(self, turn_id: str) -> str:
            if turn_id == "turn-1":
                return (
                    '{"response_json":"{\\"status\\":\\"blocked\\",'
                    '\\"reason\\":\\"Tool use is disabled in this turn.\\",'
                    '\\"message\\":\\"No run action was taken.\\"}"}'
                )
            return (
                '{"response_json":"{\\"reasoning\\":\\"continue\\",'
                '\\"actions\\":[{\\"tool\\":\\"register_profile\\",'
                '\\"candidate_name\\":\\"cand3\\",\\"operation\\":\\"create\\"}]}"}'
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(provider_module, "_CodexAppServerSession", FakeSession)

    provider = CodexAppServerProvider(
        ProviderProfileConfig(
            provider_type="codex",
            command="codex",
            model="gpt-5.4-mini",
        )
    )

    result = provider.complete_json([ChatMessage(role="user", content="hello")])

    assert result == {
        "reasoning": "continue",
        "actions": [
            {
                "tool": "register_profile",
                "candidate_name": "cand3",
                "operation": "create",
            }
        ],
    }
