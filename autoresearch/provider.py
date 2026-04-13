from __future__ import annotations

import contextvars
import subprocess
import json
import re
import shutil
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import requests

from .config import ProviderProfileConfig
from .typed_tools import ALL_CONTROLLER_TOOLS


class ProviderError(RuntimeError):
    """Raised when the provider cannot satisfy a request."""


@dataclass
class ChatMessage:
    role: str
    content: str


@dataclass
class ProviderTraceContext:
    label: str
    run_id: str | None = None
    step: int | None = None
    phase: str | None = None
    provider_type: str | None = None
    model: str | None = None
    capture_path: str | None = None
    request_snapshot_dir: str | None = None
    request_sequence: int = 0


_PROVIDER_TRACE_STDERR_MODE = "verbose"


def set_provider_trace_stderr_mode(mode: str) -> None:
    global _PROVIDER_TRACE_STDERR_MODE
    normalized = str(mode or "").strip().lower()
    if normalized not in {"verbose", "warnings_only", "off"}:
        normalized = "verbose"
    _PROVIDER_TRACE_STDERR_MODE = normalized


def _should_emit_provider_trace_event(event: str) -> bool:
    mode = _PROVIDER_TRACE_STDERR_MODE
    if mode == "verbose":
        return True
    if mode == "off":
        return False
    lowered = str(event or "").strip().lower()
    if not lowered:
        return False
    warning_markers = (
        "exception",
        "retry",
        "failure",
        "failed",
        "rate_limit",
        "quota",
    )
    return any(marker in lowered for marker in warning_markers)


JSON_REPAIR_PROMPT = """Your previous assistant response was invalid or incomplete JSON.

Return one complete corrected JSON object only.
Preserve the original intent, reasoning, and planned actions.
Do not add Markdown or explanations.
Escape Windows paths correctly inside JSON strings.
"""

DEFAULT_RATE_LIMIT_BACKOFF_SECONDS = [15, 30, 60, 120, 180, 240, 300]
DEFAULT_RATE_LIMIT_MAX_RETRIES = 18
DEFAULT_TRANSIENT_MAX_RETRIES = 4
DEFAULT_MALFORMED_SUCCESS_RETRIES = 5
DEFAULT_MALFORMED_SUCCESS_BACKOFF_SECONDS = [2, 5, 10, 20, 30]
DEFAULT_CODEX_USAGE_LIMIT_RETRIES = 1
DEFAULT_CODEX_USAGE_LIMIT_FALLBACK_SECONDS = 5 * 60 * 60 + 60
DEFAULT_CODEX_USAGE_LIMIT_BUFFER_SECONDS = 60
DEFAULT_CODEX_USAGE_LIMIT_MAX_WAIT_SECONDS = 6 * 60 * 60
TRANSIENT_STATUS_CODES = {408, 425, 500, 502, 503, 504}
RATE_LIMIT_STATUS_CODES = {429, 529}
HARD_QUOTA_MARKERS = (
    "insufficient_quota",
    "quota exceeded",
    "exceeded your current quota",
    "out of credits",
    "credit balance is too low",
    "billing",
    "payment required",
)
RATE_LIMIT_MARKERS = (
    "rate limit",
    "too many requests",
    "retry after",
    "requests rate limit",
    "temporarily rate limited",
)
CODEX_USAGE_LIMIT_MARKERS = (
    "usage limit",
    "try again at",
)
TOOL_USE_FAILED_MARKERS = (
    "tool_use_failed",
    "tool choice is none, but model called a tool",
)
JSON_VALIDATE_FAILED_MARKERS = (
    "json_validate_failed",
    "failed to generate json",
)
CODEX_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "response_json": {"type": "string"},
    },
    "required": ["response_json"],
    "additionalProperties": False,
}


_PROVIDER_TRACE_CONTEXT: contextvars.ContextVar[ProviderTraceContext | None] = contextvars.ContextVar(
    "provider_trace_context",
    default=None,
)


@contextmanager
def provider_trace_scope(
    *,
    label: str,
    run_id: str | None = None,
    step: int | None = None,
    phase: str | None = None,
    provider_type: str | None = None,
    model: str | None = None,
    capture_path: str | None = None,
    request_snapshot_dir: str | None = None,
):
    token = _PROVIDER_TRACE_CONTEXT.set(
        ProviderTraceContext(
            label=label,
            run_id=run_id,
            step=step,
            phase=phase,
            provider_type=provider_type,
            model=model,
            capture_path=capture_path,
            request_snapshot_dir=request_snapshot_dir,
        )
    )
    try:
        yield
    finally:
        _PROVIDER_TRACE_CONTEXT.reset(token)


def _trace_provider_event(event: str, **fields: Any) -> None:
    if not _should_emit_provider_trace_event(event):
        return
    context = _PROVIDER_TRACE_CONTEXT.get()
    parts = [f"provider_trace event={event}"]
    if context is not None:
        parts.append(f"label={context.label}")
        if context.run_id:
            parts.append(f"run_id={context.run_id}")
        if context.step is not None:
            parts.append(f"step={context.step}")
        if context.phase:
            parts.append(f"phase={context.phase}")
        if context.provider_type:
            parts.append(f"provider_type={context.provider_type}")
        if context.model:
            parts.append(f"model={context.model}")
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).replace("\n", " ").strip()
        if not text:
            continue
        if len(text) > 240:
            text = text[:237] + "..."
        parts.append(f"{key}={text}")
    print(" ".join(parts), file=sys.stderr, flush=True)


def _provider_capture_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {
            str(key): _provider_capture_safe_value(item) for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_provider_capture_safe_value(item) for item in value]
    try:
        json.dumps(value, ensure_ascii=True)
        return value
    except (TypeError, ValueError):
        return str(value)


def _append_provider_capture(
    event: str,
    *,
    content: str | None = None,
    parsed_payload: Any = None,
    **fields: Any,
) -> None:
    context = _PROVIDER_TRACE_CONTEXT.get()
    if context is None or not context.capture_path:
        return
    payload: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "label": context.label,
        "run_id": context.run_id,
        "step": context.step,
        "phase": context.phase,
        "provider_type": context.provider_type,
        "model": context.model,
        "source": "provider",
    }
    if content is not None:
        payload["payload_text"] = content
        payload["payload_text_chars"] = len(content)
    if parsed_payload is not None:
        payload["payload_json"] = _provider_capture_safe_value(parsed_payload)
    for key, value in fields.items():
        if value is not None:
            payload[key] = _provider_capture_safe_value(value)
    path = Path(context.capture_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except OSError:
        return


def _snapshot_safe_name(value: str | None, *, fallback: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return fallback
    normalized = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return normalized or fallback


def _pretty_json_text(raw: str) -> str | None:
    text = str(raw or "").strip()
    if not text or text[0] not in "[{":
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return json.dumps(payload, ensure_ascii=True, indent=4)


def _format_planned_action_line(line: str) -> str:
    body = str(line or "").rstrip()
    if not body.startswith("- "):
        return body
    payload = body[2:].strip()
    pretty = _pretty_json_text(payload)
    if pretty is None:
        return body
    pretty_lines = pretty.splitlines()
    return "\n".join(
        [f"- {pretty_lines[0]}"] + [f"  {item}" for item in pretty_lines[1:]]
    )


def _format_request_snapshot_message_content(content: str) -> str:
    text = str(content or "")
    if text.startswith("===== TOOL RESULTS FROM PRIOR STEP ====="):
        lines = text.splitlines()
        rendered: list[str] = []
        current_header: str | None = None
        current_body: list[str] = []

        def flush_section() -> None:
            if current_header is None:
                return
            rendered.append(current_header)
            body_text = "\n".join(current_body).strip()
            if "TOOL RESULTS FROM PRIOR STEP" in current_header:
                pretty = _pretty_json_text(body_text)
                if pretty is not None:
                    body_text = pretty
            elif body_text:
                pretty = _pretty_json_text(body_text)
                if pretty is not None:
                    body_text = pretty
            if body_text:
                rendered.append(body_text)
            rendered.append("")

        for line in lines:
            if re.match(r"^===== .+ =====$", line.strip()):
                flush_section()
                current_header = line.strip()
                current_body = []
            else:
                current_body.append(line)
        flush_section()
        return "\n".join(rendered).rstrip()
    if text.startswith("Tool results:\n"):
        raw_json = text.partition("\n")[2]
        pretty = _pretty_json_text(raw_json)
        if pretty is not None:
            return "Tool results:\n" + pretty
        return text.rstrip()
    if text.startswith("Reasoning:"):
        lines = text.splitlines()
        rendered: list[str] = []
        in_actions = False
        for line in lines:
            stripped = line.strip()
            if stripped == "Planned actions:":
                in_actions = True
                rendered.append("Planned actions:")
                continue
            if in_actions:
                rendered.append(_format_planned_action_line(line))
            else:
                rendered.append(line)
        return "\n".join(rendered).rstrip()
    pretty = _pretty_json_text(text)
    if pretty is not None:
        return pretty
    return text.rstrip()


def _format_request_snapshot_messages(messages: list[ChatMessage]) -> str:
    sections: list[str] = []
    for index, message in enumerate(messages, start=1):
        sections.append(f"[message {index}] role={message.role}")
        sections.append(_format_request_snapshot_message_content(message.content))
        sections.append("")
    return "\n".join(sections).rstrip()


def _write_provider_request_snapshot(
    request_kind: str,
    *,
    messages: list[ChatMessage],
    request_payload: Any = None,
    prompt_text: str | None = None,
    **fields: Any,
) -> None:
    context = _PROVIDER_TRACE_CONTEXT.get()
    if context is None or not context.request_snapshot_dir:
        return
    context.request_sequence += 1
    snapshot_dir = Path(context.request_snapshot_dir)
    filename = "__".join(
        [
            f"step-{int(context.step or 0):04d}",
            f"req-{context.request_sequence:03d}",
            _snapshot_safe_name(request_kind, fallback="request"),
            _snapshot_safe_name(context.phase, fallback="phase"),
            _snapshot_safe_name(context.label, fallback="label"),
        ]
    ) + ".txt"
    metadata: dict[str, Any] = {
        "request_kind": request_kind,
        "run_id": context.run_id,
        "step": context.step,
        "phase": context.phase,
        "label": context.label,
        "provider_type": context.provider_type,
        "model": context.model,
        "request_sequence": context.request_sequence,
        "message_count": len(messages),
    }
    for key, value in fields.items():
        if value is not None:
            metadata[key] = _provider_capture_safe_value(value)
    lines = [
        "===== INFORMATIONAL ONLY: DIAGNOSTIC SNAPSHOT METADATA (NOT SENT TO API OR MODEL) =====",
        json.dumps(metadata, ensure_ascii=True, indent=2),
    ]
    if request_payload is not None:
        lines.extend(
            [
                "",
                "===== LITERALLY SENT TO API: HTTP REQUEST JSON BODY =====",
                json.dumps(
                    _provider_capture_safe_value(request_payload),
                    ensure_ascii=True,
                    indent=2,
                ),
            ]
        )
    lines.extend(
        [
            "",
            "===== INFORMATIONAL ONLY: READABLE VIEW OF MESSAGE CONTENT FROM THE REQUEST =====",
            _format_request_snapshot_messages(messages),
        ]
    )
    if prompt_text is not None:
        lines.extend(
            [
                "",
                "===== LITERALLY SENT TO PROVIDER: RENDERED PROMPT TEXT =====",
                prompt_text,
            ]
        )
    snapshot_path = snapshot_dir / filename
    try:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    except OSError:
        return


def _escape_invalid_json_backslashes(text: str) -> str:
    result: list[str] = []
    in_string = False
    string_quote = '"'
    index = 0
    length = len(text)
    while index < length:
        ch = text[index]
        if not in_string:
            result.append(ch)
            if ch == '"':
                in_string = True
                string_quote = ch
            index += 1
            continue

        if ch == "\\":
            next_ch = text[index + 1] if index + 1 < length else ""
            if next_ch in {'"', "\\", "/"}:
                result.append(ch)
                result.append(next_ch)
                index += 2
                continue
            result.append("\\\\")
            index += 1
            continue

        result.append(ch)
        if ch == string_quote:
            backslashes = 0
            lookback = index - 1
            while lookback >= 0 and text[lookback] == "\\":
                backslashes += 1
                lookback -= 1
            if backslashes % 2 == 0:
                in_string = False
        index += 1
    return "".join(result)


def _parse_provider_json_object(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    trimmed = text.strip()
    candidates = [trimmed]
    start = trimmed.find("{")
    if start > 0:
        candidates.append(trimmed[start:])

    for candidate in candidates:
        try:
            value, _ = decoder.raw_decode(candidate)
            if isinstance(value, dict):
                return value
        except json.JSONDecodeError:
            sanitized = _escape_invalid_json_backslashes(candidate)
            if sanitized != candidate:
                try:
                    value, _ = decoder.raw_decode(sanitized)
                    if isinstance(value, dict):
                        return value
                except json.JSONDecodeError:
                    pass
    raise ProviderError(f"Provider did not return valid JSON. Raw response: {trimmed[:800]}")


def _configured_rate_limit_backoff_seconds(config: ProviderProfileConfig) -> list[int]:
    values = config.rate_limit_backoff_seconds or DEFAULT_RATE_LIMIT_BACKOFF_SECONDS
    cleaned = [max(1, int(item)) for item in values]
    return cleaned or list(DEFAULT_RATE_LIMIT_BACKOFF_SECONDS)


def _configured_rate_limit_max_retries(config: ProviderProfileConfig) -> int:
    if config.rate_limit_max_retries is not None:
        return max(1, int(config.rate_limit_max_retries))
    return DEFAULT_RATE_LIMIT_MAX_RETRIES


def _parse_retry_after_seconds(response: requests.Response) -> int | None:
    header = response.headers.get("Retry-After")
    if not header:
        return None
    value = header.strip()
    if not value:
        return None
    if value.isdigit():
        return max(1, int(value))
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    return max(1, int(retry_at.timestamp() - time.time()))


def _looks_like_hard_quota(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in HARD_QUOTA_MARKERS)


def _looks_like_rate_limit(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in RATE_LIMIT_MARKERS)


def _looks_like_codex_usage_limit(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in CODEX_USAGE_LIMIT_MARKERS)


def _parse_codex_usage_limit_delay_seconds(
    text: str,
    *,
    now: datetime | None = None,
) -> int | None:
    if not _looks_like_codex_usage_limit(text):
        return None
    local_now = (now or datetime.now().astimezone()).astimezone()
    match = re.search(r"try again at\s+(\d{1,2}:\d{2}(?:\s*[AaPp][Mm])?)", text)
    if match:
        raw_time = " ".join(match.group(1).split())
        parsed_time = None
        for fmt in ("%I:%M %p", "%H:%M"):
            try:
                parsed_time = datetime.strptime(raw_time.upper(), fmt).time()
                break
            except ValueError:
                continue
        if parsed_time is not None:
            candidate = local_now.replace(
                hour=parsed_time.hour,
                minute=parsed_time.minute,
                second=0,
                microsecond=0,
            )
            if candidate <= local_now:
                candidate = candidate + timedelta(days=1)
            delay_seconds = int((candidate - local_now).total_seconds()) + DEFAULT_CODEX_USAGE_LIMIT_BUFFER_SECONDS
            if 0 < delay_seconds <= DEFAULT_CODEX_USAGE_LIMIT_MAX_WAIT_SECONDS:
                return delay_seconds
    return DEFAULT_CODEX_USAGE_LIMIT_FALLBACK_SECONDS


def _is_malformed_success_error(error: ProviderError) -> bool:
    lowered = str(error).lower()
    return (
        "no choices" in lowered
        or "did not contain text content" in lowered
        or "empty content" in lowered
    )


def _is_invalid_json_error(error: ProviderError) -> bool:
    return "did not return valid json" in str(error).lower()


def _is_tool_use_failed_error(error: ProviderError) -> bool:
    lowered = str(error).lower()
    return any(marker in lowered for marker in TOOL_USE_FAILED_MARKERS)


def _is_json_validate_failed_error(error: ProviderError) -> bool:
    lowered = str(error).lower()
    return any(marker in lowered for marker in JSON_VALIDATE_FAILED_MARKERS)


def _extract_failed_generation_text(error: ProviderError) -> str | None:
    text = str(error)
    json_start = text.find("{")
    if json_start >= 0:
        try:
            payload = json.loads(text[json_start:])
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            error_payload = payload.get("error")
            if isinstance(error_payload, dict):
                failed_generation = error_payload.get("failed_generation")
                if isinstance(failed_generation, str) and failed_generation.strip():
                    return failed_generation.strip()
    marker = "failed_generation="
    index = text.find(marker)
    if index < 0:
        return None
    failed_generation = text[index + len(marker):].strip()
    if not failed_generation:
        return None
    if failed_generation[0] in {'"', "'"}:
        try:
            decoded = json.loads(failed_generation)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, str) and decoded.strip():
            return decoded.strip()
    return failed_generation


def _salvage_failed_generation_action(text: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    name = payload.get("name")
    arguments = payload.get("arguments")
    if not isinstance(name, str) or not name.strip():
        return None
    if not isinstance(arguments, dict):
        return None
    allowed_tools = set(ALL_CONTROLLER_TOOLS)
    tool_name = name.strip()
    if tool_name not in allowed_tools:
        return None
    action = {"tool": tool_name}
    for key, value in arguments.items():
        action[key] = value
    return {
        "reasoning": "",
        "actions": [action],
    }


def _malformed_success_delay_seconds(attempt_index: int) -> int:
    if attempt_index < len(DEFAULT_MALFORMED_SUCCESS_BACKOFF_SECONDS):
        return DEFAULT_MALFORMED_SUCCESS_BACKOFF_SECONDS[attempt_index]
    return DEFAULT_MALFORMED_SUCCESS_BACKOFF_SECONDS[-1]


def _classify_retryable_response(response: requests.Response) -> tuple[str, int | None] | None:
    status = int(response.status_code)
    body_text = response.text[:1200] if response.text else ""
    retry_after = _parse_retry_after_seconds(response)
    if status in RATE_LIMIT_STATUS_CODES:
        if _looks_like_hard_quota(body_text):
            return None
        return ("rate_limit", retry_after)
    if _looks_like_rate_limit(body_text):
        if _looks_like_hard_quota(body_text):
            return None
        return ("rate_limit", retry_after)
    if status in TRANSIENT_STATUS_CODES:
        return ("transient", retry_after)
    return None


def _classify_retryable_success_payload(payload: Any) -> tuple[str, int | None] | None:
    if not isinstance(payload, dict):
        return None
    error_payload = payload.get("error")
    if not isinstance(error_payload, dict):
        return None
    message = str(error_payload.get("message") or "")
    code_value = error_payload.get("code")
    code_text = str(code_value or "").strip().lower()
    retry_after = None
    if code_text in {"429", "529"} or _looks_like_rate_limit(message):
        if _looks_like_hard_quota(message):
            return None
        return ("rate_limit", retry_after)
    if code_text in {"408", "425", "500", "502", "503", "504"}:
        return ("transient", retry_after)
    if "upstream error" in message.lower() or "server had an error" in message.lower():
        return ("transient", retry_after)
    return None


def _rate_limit_delay_seconds(config: ProviderProfileConfig, attempt_index: int, retry_after: int | None) -> int:
    if retry_after is not None and retry_after > 0:
        return retry_after
    schedule = _configured_rate_limit_backoff_seconds(config)
    if attempt_index < len(schedule):
        return schedule[attempt_index]
    return schedule[-1]


def _transient_delay_seconds(config: ProviderProfileConfig, attempt_index: int, retry_after: int | None) -> int:
    if retry_after is not None and retry_after > 0:
        return retry_after
    schedule = _configured_rate_limit_backoff_seconds(config)
    capped_index = min(attempt_index, max(0, min(2, len(schedule) - 1)))
    return schedule[capped_index]


def _post_json_with_retry(
    session: requests.Session,
    *,
    url: str,
    headers: dict[str, str],
    body: dict[str, Any],
    timeout_seconds: int,
    config: ProviderProfileConfig,
) -> dict[str, Any]:
    rate_limit_retries = 0
    transient_retries = 0
    request_attempt = 0
    while True:
        request_attempt += 1
        _trace_provider_event(
            "http_request_start",
            attempt=request_attempt,
            timeout_seconds=timeout_seconds,
            url=url,
        )
        try:
            response = session.post(
                url,
                headers=headers,
                json=body,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            transient_retries += 1
            _trace_provider_event(
                "http_request_exception",
                attempt=request_attempt,
                transient_retries=transient_retries,
                error=exc,
            )
            if transient_retries > DEFAULT_TRANSIENT_MAX_RETRIES:
                raise ProviderError(
                    f"Provider request failed after {transient_retries} transient retries: {exc}"
                ) from exc
            delay_seconds = _transient_delay_seconds(config, transient_retries - 1, None)
            _trace_provider_event(
                "http_request_retry_scheduled",
                attempt=request_attempt,
                retry_kind="transient",
                delay_seconds=delay_seconds,
            )
            time.sleep(delay_seconds)
            continue

        classification = _classify_retryable_response(response)
        if classification is not None:
            retry_kind, retry_after = classification
            if retry_kind == "rate_limit":
                rate_limit_retries += 1
                _trace_provider_event(
                    "http_response_retryable",
                    attempt=request_attempt,
                    retry_kind=retry_kind,
                    status_code=response.status_code,
                    rate_limit_retries=rate_limit_retries,
                    retry_after=retry_after,
                )
                if rate_limit_retries > _configured_rate_limit_max_retries(config):
                    raise ProviderError(
                        f"Provider remained rate-limited after {rate_limit_retries} retries: "
                        f"{response.status_code} {response.text[:800]}"
                    )
                delay_seconds = _rate_limit_delay_seconds(config, rate_limit_retries - 1, retry_after)
                _trace_provider_event(
                    "http_request_retry_scheduled",
                    attempt=request_attempt,
                    retry_kind=retry_kind,
                    delay_seconds=delay_seconds,
                )
                time.sleep(delay_seconds)
                continue
            transient_retries += 1
            _trace_provider_event(
                "http_response_retryable",
                attempt=request_attempt,
                retry_kind=retry_kind,
                status_code=response.status_code,
                transient_retries=transient_retries,
                retry_after=retry_after,
            )
            if transient_retries > DEFAULT_TRANSIENT_MAX_RETRIES:
                raise ProviderError(
                    f"Provider request kept failing transiently after {transient_retries} retries: "
                    f"{response.status_code} {response.text[:800]}"
                )
            delay_seconds = _transient_delay_seconds(config, transient_retries - 1, retry_after)
            _trace_provider_event(
                "http_request_retry_scheduled",
                attempt=request_attempt,
                retry_kind=retry_kind,
                delay_seconds=delay_seconds,
            )
            time.sleep(delay_seconds)
            continue

        if response.status_code >= 400:
            _trace_provider_event(
                "http_response_error",
                attempt=request_attempt,
                status_code=response.status_code,
                body_preview=response.text[:240] if response.text else "",
            )
            if response.status_code == 400:
                try:
                    error_payload = response.json().get("error")
                except Exception:
                    error_payload = None
                if isinstance(error_payload, dict):
                    error_code = str(error_payload.get("code") or "")
                    failed_generation = error_payload.get("failed_generation")
                    if (
                        error_code == "json_validate_failed"
                        and isinstance(failed_generation, str)
                        and failed_generation.strip()
                    ):
                        raise ProviderError(
                            "Provider request failed with 400: "
                            f"json_validate_failed failed_generation={failed_generation[:4000]}"
                        )
            raise ProviderError(
                f"Provider request failed with {response.status_code}: {response.text[:800]}"
            )
        payload = response.json()
        payload_classification = _classify_retryable_success_payload(payload)
        if payload_classification is not None:
            retry_kind, retry_after = payload_classification
            if retry_kind == "rate_limit":
                rate_limit_retries += 1
                _trace_provider_event(
                    "http_payload_retryable",
                    attempt=request_attempt,
                    retry_kind=retry_kind,
                    rate_limit_retries=rate_limit_retries,
                    retry_after=retry_after,
                )
                if rate_limit_retries > _configured_rate_limit_max_retries(config):
                    raise ProviderError(
                        f"Provider remained rate-limited after {rate_limit_retries} retries: "
                        f"{json.dumps(payload, ensure_ascii=True)[:800]}"
                    )
                delay_seconds = _rate_limit_delay_seconds(config, rate_limit_retries - 1, retry_after)
                _trace_provider_event(
                    "http_request_retry_scheduled",
                    attempt=request_attempt,
                    retry_kind=retry_kind,
                    delay_seconds=delay_seconds,
                )
                time.sleep(delay_seconds)
                continue
            transient_retries += 1
            _trace_provider_event(
                "http_payload_retryable",
                attempt=request_attempt,
                retry_kind=retry_kind,
                transient_retries=transient_retries,
                retry_after=retry_after,
            )
            if transient_retries > DEFAULT_TRANSIENT_MAX_RETRIES:
                raise ProviderError(
                    f"Provider kept returning transient upstream error payloads after {transient_retries} retries: "
                    f"{json.dumps(payload, ensure_ascii=True)[:800]}"
                )
            delay_seconds = _transient_delay_seconds(config, transient_retries - 1, retry_after)
            _trace_provider_event(
                "http_request_retry_scheduled",
                attempt=request_attempt,
                retry_kind=retry_kind,
                delay_seconds=delay_seconds,
            )
            time.sleep(delay_seconds)
            continue
        _trace_provider_event(
            "http_request_success",
            attempt=request_attempt,
            status_code=response.status_code,
        )
        return payload


class JsonCompletionProvider(Protocol):
    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        ...


class _CodexAppServerSession:
    def __init__(self, config: ProviderProfileConfig):
        self.config = config
        self.process: subprocess.Popen[str] | None = None
        self._request_id = 0
        self._stderr_lines: deque[str] = deque(maxlen=40)
        self._stderr_thread: threading.Thread | None = None
        self._start()

    def _command(self) -> list[str]:
        command = (self.config.command or "codex").strip() or "codex"
        resolved = shutil.which(command) or command
        return [resolved, "app-server", "--listen", "stdio://"]

    def _start(self) -> None:
        try:
            self.process = subprocess.Popen(
                self._command(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except FileNotFoundError as exc:
            raise ProviderError(
                "Could not start Codex app-server. Install Codex or set providers.<profile>.command "
                "to the local Codex executable."
            ) from exc
        if self.process.stdin is None or self.process.stdout is None or self.process.stderr is None:
            self.close()
            raise ProviderError("Codex app-server did not expose the expected stdio pipes.")
        self._stderr_thread = threading.Thread(target=self._capture_stderr, daemon=True)
        self._stderr_thread.start()
        try:
            self._request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "fuzzfolio_autoresearch",
                        "title": "Fuzzfolio Autoresearch",
                        "version": "0.1.0",
                    }
                },
            )
            self._notify("initialized", {})
            account_result = self._request("account/read", {"refreshToken": True})
        except Exception:
            self.close()
            raise
        if not isinstance(account_result, dict):
            self.close()
            raise ProviderError("Codex app-server returned an invalid account/read response.")
        if account_result.get("requiresOpenaiAuth") and not account_result.get("account"):
            self.close()
            raise ProviderError(
                "Codex app-server requires OpenAI auth but no local account is active. "
                "Run `codex login` first."
            )

    def _capture_stderr(self) -> None:
        if self.process is None or self.process.stderr is None:
            return
        for line in self.process.stderr:
            stripped = line.rstrip()
            if stripped:
                self._stderr_lines.append(stripped)

    def _stderr_preview(self) -> str:
        if not self._stderr_lines:
            return ""
        return " | stderr=" + " || ".join(self._stderr_lines)

    def _send(self, payload: dict[str, Any]) -> None:
        if self.process is None or self.process.stdin is None:
            raise ProviderError("Codex app-server is not running.")
        self.process.stdin.write(json.dumps(payload, ensure_ascii=True) + "\n")
        self.process.stdin.flush()

    def _read_message(self) -> dict[str, Any]:
        if self.process is None or self.process.stdout is None:
            raise ProviderError("Codex app-server is not running.")
        while True:
            line = self.process.stdout.readline()
            if not line:
                code = self.process.poll()
                raise ProviderError(
                    "Codex app-server closed unexpectedly"
                    + (f" with exit code {code}" if code is not None else "")
                    + self._stderr_preview()
                )
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload

    def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._request_id += 1
        request_id = self._request_id
        self._send({"method": method, "id": request_id, "params": params})
        while True:
            payload = self._read_message()
            if payload.get("id") != request_id:
                self._handle_unsolicited_message(payload)
                continue
            error = payload.get("error")
            if isinstance(error, dict):
                message = error.get("message") or "unknown Codex app-server error"
                raise ProviderError(f"Codex app-server {method} failed: {message}{self._stderr_preview()}")
            result = payload.get("result")
            if not isinstance(result, dict):
                raise ProviderError(
                    f"Codex app-server {method} returned an invalid result payload.{self._stderr_preview()}"
                )
            return result

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        self._send({"method": method, "params": params})

    def _handle_unsolicited_message(self, payload: dict[str, Any]) -> None:
        method = payload.get("method")
        request_id = payload.get("id")
        if isinstance(method, str) and request_id is not None:
            self._send(
                {
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": (
                            "Autoresearch Codex provider does not support interactive server requests. "
                            "This turn must complete without approvals or tool callbacks."
                        ),
                    },
                }
            )

    def default_turn_summary_enabled(self) -> bool:
        model_name = str(self.config.model or "").strip().lower()
        if "spark" in model_name:
            return False
        return True

    def _turn_start_params(self, prompt: str, *, include_summary: bool) -> dict[str, Any]:
        params: dict[str, Any] = {
            "threadId": "",
            "input": [{"type": "text", "text": prompt}],
            "approvalPolicy": "never",
            "sandboxPolicy": {"type": "readOnly"},
            "model": self.config.model,
            "personality": "none",
            "outputSchema": CODEX_OUTPUT_SCHEMA,
        }
        if include_summary:
            params["summary"] = "concise"
        return params

    def start_turn(self, prompt: str, *, include_summary: bool | None = None) -> str:
        if include_summary is None:
            include_summary = self.default_turn_summary_enabled()
        thread = self._request(
            "thread/start",
            {
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "personality": "none",
            },
        ).get("thread")
        if not isinstance(thread, dict) or not isinstance(thread.get("id"), str):
            raise ProviderError("Codex app-server did not return a usable thread id.")
        thread_id = thread["id"]
        turn_params = self._turn_start_params(prompt, include_summary=include_summary)
        turn_params["threadId"] = thread_id
        turn_result = self._request(
            "turn/start",
            turn_params,
        )
        turn = turn_result.get("turn")
        if not isinstance(turn, dict) or not isinstance(turn.get("id"), str):
            raise ProviderError("Codex app-server did not return a usable turn id.")
        return turn["id"]

    def collect_turn_text(self, turn_id: str) -> str:
        deltas: list[str] = []
        final_text: str | None = None
        while True:
            payload = self._read_message()
            if payload.get("id") is not None:
                self._handle_unsolicited_message(payload)
                continue
            method = payload.get("method")
            params = payload.get("params")
            if not isinstance(method, str) or not isinstance(params, dict):
                continue
            if method == "item/agentMessage/delta":
                if params.get("itemId") and isinstance(params.get("delta"), str):
                    deltas.append(str(params["delta"]))
                continue
            if method == "item/completed":
                item = params.get("item")
                if isinstance(item, dict) and item.get("type") == "agentMessage":
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        final_text = text
                continue
            if method != "turn/completed":
                continue
            turn = params.get("turn")
            if not isinstance(turn, dict) or turn.get("id") != turn_id:
                continue
            status = str(turn.get("status") or "")
            if status != "completed":
                error_payload = turn.get("error")
                detail = ""
                if isinstance(error_payload, dict):
                    detail = str(error_payload.get("message") or "")
                raise ProviderError(
                    f"Codex app-server turn failed with status {status!r}"
                    + (f": {detail}" if detail else "")
                    + self._stderr_preview()
                )
            combined = (final_text or "".join(deltas)).strip()
            if not combined:
                raise ProviderError(
                    "Codex app-server turn completed without assistant text." + self._stderr_preview()
                )
            return combined

    def close(self) -> None:
        if self.process is None:
            return
        try:
            self.process.terminate()
            self.process.wait(timeout=5)
        except Exception:
            try:
                self.process.kill()
            except Exception:
                pass
        finally:
            self.process = None


class CodexAppServerProvider:
    def __init__(self, config: ProviderProfileConfig):
        self.config = config

    def _prompt_from_messages(self, messages: list[ChatMessage]) -> str:
        sections = [
            "You are not allowed to use native tools, shell commands, file edits, MCP tools, or approvals.",
            "Answer directly as plain assistant text.",
            "Your final assistant message is schema-constrained.",
            "Set response_json to the exact raw JSON object string the caller expects.",
            "Do not wrap it in Markdown. Escape Windows paths correctly inside JSON strings.",
            "",
            "Conversation transcript:",
        ]
        for message in messages:
            sections.append(f"{message.role.upper()}:\n{message.content}")
        return "\n\n".join(sections)

    def _repair_invalid_json(
        self,
        session: _CodexAppServerSession,
        messages: list[ChatMessage],
        raw_text: str,
    ) -> dict[str, Any] | None:
        _trace_provider_event(
            "json_repair_start",
            raw_preview=raw_text[:240],
        )
        repair_messages = [
            *messages,
            ChatMessage(role="assistant", content=raw_text),
            ChatMessage(role="user", content=JSON_REPAIR_PROMPT),
        ]
        _append_provider_capture(
            "repair_request",
            content=raw_text,
            payload_kind="assistant_text",
        )
        try:
            repair_prompt = self._prompt_from_messages(repair_messages)
            turn_id = session.start_turn(repair_prompt)
            repaired_outer_text = session.collect_turn_text(turn_id)
            _append_provider_capture(
                "repair_response",
                content=repaired_outer_text,
                payload_kind="assistant_text",
            )
            repaired_outer_payload = _parse_provider_json_object(repaired_outer_text)
            repaired_response_json = repaired_outer_payload.get("response_json")
            if isinstance(repaired_response_json, str) and repaired_response_json.strip():
                repaired = _parse_provider_json_object(repaired_response_json)
            else:
                repaired = repaired_outer_payload
            _append_provider_capture(
                "provider_parsed_json",
                parsed_payload=repaired,
                payload_kind="json_object",
            )
            _trace_provider_event("json_repair_success")
            return repaired
        except ProviderError as exc:
            _append_provider_capture(
                "repair_failed",
                content=raw_text,
                payload_kind="assistant_text",
                error=str(exc),
            )
            _trace_provider_event("json_repair_failed", error=exc)
            return None

    @staticmethod
    def _is_unsupported_turn_summary_error(exc: ProviderError) -> bool:
        text = str(exc)
        return "unsupported_parameter" in text and "reasoning.summary" in text

    @staticmethod
    def _codex_usage_limit_delay_seconds(exc: ProviderError) -> int | None:
        return _parse_codex_usage_limit_delay_seconds(str(exc))

    @staticmethod
    def _is_tool_disabled_payload(payload: dict[str, Any]) -> bool:
        status = str(payload.get("status") or "").strip().lower()
        reason = str(payload.get("reason") or "").strip().lower()
        message = str(payload.get("message") or "").strip().lower()
        if status != "blocked":
            return False
        combined = " ".join(part for part in (reason, message) if part)
        return "tool use is disabled" in combined or "no run action was taken" in combined

    def _blocked_tool_retry_messages(
        self,
        messages: list[ChatMessage],
        blocked_payload_text: str,
    ) -> list[ChatMessage]:
        return [
            *messages,
            ChatMessage(role="assistant", content=blocked_payload_text),
            ChatMessage(
                role="user",
                content=(
                    "The previous response incorrectly claimed tool use is disabled.\n\n"
                    "Native/provider tools are disabled, but the controller still requires a normal JSON response "
                    "with reasoning and a non-empty actions array using typed controller tools.\n"
                    "Do not return status/reason/message wrappers.\n"
                    "Do not say 'No run action was taken.'\n"
                    "Do not use finish unless the current run rules explicitly allow it.\n"
                    "Return exactly one valid JSON object matching the controller contract."
                ),
            ),
        ]

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        _trace_provider_event(
            "complete_json_start",
            message_count=len(messages),
        )
        prompt_text = self._prompt_from_messages(messages)
        _write_provider_request_snapshot(
            "codex_app_server_request",
            messages=messages,
            prompt_text=prompt_text,
        )
        usage_limit_retries = 0
        while True:
            session = _CodexAppServerSession(self.config)
            try:
                try:
                    include_summary = session.default_turn_summary_enabled()
                    try:
                        turn_id = session.start_turn(prompt_text, include_summary=include_summary)
                        raw_text = session.collect_turn_text(turn_id)
                    except ProviderError as exc:
                        if not include_summary or not self._is_unsupported_turn_summary_error(exc):
                            raise
                        _trace_provider_event(
                            "codex_turn_retry_without_summary",
                            model=self.config.model,
                        )
                        turn_id = session.start_turn(prompt_text, include_summary=False)
                        raw_text = session.collect_turn_text(turn_id)
                    _append_provider_capture(
                        "provider_raw_text",
                        content=raw_text,
                        payload_kind="assistant_text",
                    )
                    try:
                        outer_payload = _parse_provider_json_object(raw_text)
                    except ProviderError:
                        repaired = self._repair_invalid_json(session, messages, raw_text)
                        if repaired is not None:
                            _trace_provider_event("complete_json_success", path="outer_invalid_json_repair")
                            return repaired
                        raise
                    if self._is_tool_disabled_payload(outer_payload):
                        _trace_provider_event("codex_blocked_payload_retry", model=self.config.model)
                        retry_messages = self._blocked_tool_retry_messages(
                            messages,
                            json.dumps(outer_payload, ensure_ascii=False),
                        )
                        retry_prompt = self._prompt_from_messages(retry_messages)
                        retry_turn_id = session.start_turn(
                            retry_prompt,
                            include_summary=session.default_turn_summary_enabled(),
                        )
                        retry_raw_text = session.collect_turn_text(retry_turn_id)
                        _append_provider_capture(
                            "provider_raw_text",
                            content=retry_raw_text,
                            payload_kind="assistant_text",
                            retry_kind="tool_disabled_payload",
                        )
                        outer_payload = _parse_provider_json_object(retry_raw_text)
                        if self._is_tool_disabled_payload(outer_payload):
                            raise ProviderError(
                                "Codex provider returned a repeated blocked payload claiming tool use is disabled."
                            )
                    response_json = outer_payload.get("response_json")
                    if isinstance(response_json, str) and response_json.strip():
                        try:
                            parsed = _parse_provider_json_object(response_json)
                        except ProviderError:
                            salvaged = _salvage_failed_generation_action(response_json)
                            if salvaged is not None:
                                _trace_provider_event("complete_json_success", path="tool_salvage")
                                return salvaged
                            repaired = self._repair_invalid_json(session, messages, response_json)
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="invalid_json_repair")
                                return repaired
                            raise
                        if self._is_tool_disabled_payload(parsed):
                            _trace_provider_event("codex_blocked_payload_retry", model=self.config.model)
                            retry_messages = self._blocked_tool_retry_messages(messages, response_json)
                            retry_prompt = self._prompt_from_messages(retry_messages)
                            retry_turn_id = session.start_turn(
                                retry_prompt,
                                include_summary=session.default_turn_summary_enabled(),
                            )
                            retry_raw_text = session.collect_turn_text(retry_turn_id)
                            _append_provider_capture(
                                "provider_raw_text",
                                content=retry_raw_text,
                                payload_kind="assistant_text",
                                retry_kind="tool_disabled_payload",
                            )
                            retry_outer_payload = _parse_provider_json_object(retry_raw_text)
                            retry_response_json = retry_outer_payload.get("response_json")
                            if isinstance(retry_response_json, str) and retry_response_json.strip():
                                parsed = _parse_provider_json_object(retry_response_json)
                            else:
                                parsed = retry_outer_payload
                            if self._is_tool_disabled_payload(parsed):
                                raise ProviderError(
                                    "Codex provider returned a repeated blocked payload claiming tool use is disabled."
                                )
                        _append_provider_capture(
                            "provider_parsed_json",
                            parsed_payload=parsed,
                            payload_kind="json_object",
                        )
                        _trace_provider_event("complete_json_success", path="direct")
                        return parsed
                    _append_provider_capture(
                        "provider_parsed_json",
                        parsed_payload=outer_payload,
                        payload_kind="json_object",
                    )
                    _trace_provider_event("complete_json_success", path="outer_direct")
                    return outer_payload
                except ProviderError:
                    raise
            except ProviderError as exc:
                delay_seconds = self._codex_usage_limit_delay_seconds(exc)
                if delay_seconds is None or usage_limit_retries >= DEFAULT_CODEX_USAGE_LIMIT_RETRIES:
                    raise
                usage_limit_retries += 1
                _trace_provider_event(
                    "codex_usage_limit_wait",
                    model=self.config.model,
                    delay_seconds=delay_seconds,
                    retry_index=usage_limit_retries,
                )
                _append_provider_capture(
                    "codex_usage_limit_wait",
                    payload_kind="provider_backoff",
                    delay_seconds=delay_seconds,
                    retry_index=usage_limit_retries,
                    error=str(exc),
                )
                time.sleep(delay_seconds)
                continue
            finally:
                session.close()


class ChatCompletionsJsonProvider:
    def __init__(self, config: ProviderProfileConfig):
        self.config = config
        self.session = requests.Session()

    def _build_url(self) -> str:
        api_base = (self.config.api_base or "").rstrip("/")
        if not api_base:
            raise ProviderError(
                f"Provider {self.config.provider_type!r} is missing api_base."
            )
        return api_base + "/chat/completions"

    def _build_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def _refresh_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = requests.Session()

    def _build_body(
        self,
        messages: list[ChatMessage],
        max_completion_tokens: int,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": self.config.model,
            "messages": [{"role": message.role, "content": message.content} for message in messages],
            "temperature": self.config.temperature,
        }
        body.update(self._completion_budget_fields(max_completion_tokens))
        response_format = self._response_format()
        if response_format is not None:
            body["response_format"] = response_format
        return body

    def _completion_budget_fields(self, max_completion_tokens: int) -> dict[str, Any]:
        return {"max_completion_tokens": max_completion_tokens}

    def _response_format(self) -> dict[str, Any] | None:
        return {"type": "json_object"}

    def _messages_for_tool_retry(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        return [
            *messages,
            ChatMessage(
                role="system",
                content=(
                    "Do not call any native tools, function calls, provider tools, MCP tools, "
                    "or built-in tools. Return plain assistant text containing only the required JSON object."
                ),
            ),
        ]

    def _messages_for_json_retry(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        return [
            *messages,
            ChatMessage(
                role="system",
                content=(
                    "Return plain assistant text containing exactly one valid JSON object that matches the required schema. "
                    "Do not emit empty output. Do not emit markdown, bullets, prose outside JSON, or native tool calls."
                ),
            ),
        ]

    def _max_completion_tokens_cap(self) -> int | None:
        return None

    def _completion_budgets(self) -> list[int]:
        base_budget = max(1, int(self.config.max_tokens))
        fallback_budget = max(base_budget * 2, 4800)
        cap = self._max_completion_tokens_cap()
        if cap is not None:
            base_budget = min(base_budget, cap)
            fallback_budget = min(fallback_budget, cap)
        budgets: list[int] = []
        for budget in (base_budget, fallback_budget):
            if budget not in budgets:
                budgets.append(budget)
        return budgets

    def _extract_content(self, payload: dict[str, Any]) -> str:
        choices = payload.get("choices") or []
        if not choices:
            error_payload = payload.get("error")
            if isinstance(error_payload, dict):
                detail = json.dumps(error_payload, ensure_ascii=True)[:800]
                raise ProviderError(f"Provider response had no choices. error={detail}")
            preview = json.dumps(payload, ensure_ascii=True)[:800]
            raise ProviderError(f"Provider response had no choices. payload={preview}")
        choice = choices[0]
        message = choice.get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            if not content.strip():
                finish_reason = choice.get("finish_reason")
                raise ProviderError(
                    f"Provider returned empty content. finish_reason={finish_reason!r}"
                )
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                item_type = str(item.get("type") or "")
                if item_type in {"text", "output_text"}:
                    parts.append(str(item.get("text", "")))
                    continue
                text_payload = item.get("text")
                if isinstance(text_payload, str) and text_payload.strip():
                    parts.append(text_payload)
            if parts:
                return "".join(parts)
        route_provider = payload.get("provider")
        finish_reason = choice.get("finish_reason")
        completion_tokens = None
        usage = payload.get("usage")
        if isinstance(usage, dict):
            completion_tokens = usage.get("completion_tokens")
        preview = json.dumps(payload, ensure_ascii=True)[:800]
        raise ProviderError(
            "Provider response did not contain text content. "
            f"provider={route_provider!r} finish_reason={finish_reason!r} "
            f"completion_tokens={completion_tokens!r} payload={preview}"
        )

    def _parse_json_object(self, text: str) -> dict[str, Any]:
        return _parse_provider_json_object(text)

    def _request_json(
        self,
        messages: list[ChatMessage],
        max_completion_tokens: int,
    ) -> dict[str, Any]:
        body = self._build_body(messages, max_completion_tokens)
        _write_provider_request_snapshot(
            "chat_completions_request",
            messages=messages,
            request_payload=body,
            max_completion_tokens=max_completion_tokens,
        )
        return _post_json_with_retry(
            self.session,
            url=self._build_url(),
            headers=self._build_headers(),
            body=body,
            timeout_seconds=self.config.timeout_seconds,
            config=self.config,
        )

    def _repair_invalid_json(
        self,
        messages: list[ChatMessage],
        raw_text: str,
        max_completion_tokens: int,
    ) -> dict[str, Any] | None:
        _trace_provider_event(
            "json_repair_start",
            max_completion_tokens=max_completion_tokens,
            raw_preview=raw_text[:240],
        )
        repair_messages = [
            *messages,
            ChatMessage(role="assistant", content=raw_text),
            ChatMessage(role="user", content=JSON_REPAIR_PROMPT),
        ]
        _append_provider_capture(
            "repair_request",
            content=raw_text,
            payload_kind="assistant_text",
            max_completion_tokens=max_completion_tokens,
        )
        try:
            payload = self._request_json(repair_messages, max_completion_tokens)
            repaired_text = self._extract_content(payload)
            _append_provider_capture(
                "repair_response",
                content=repaired_text,
                payload_kind="assistant_text",
                max_completion_tokens=max_completion_tokens,
            )
            repaired = self._parse_json_object(repaired_text)
            _append_provider_capture(
                "provider_parsed_json",
                parsed_payload=repaired,
                payload_kind="json_object",
                max_completion_tokens=max_completion_tokens,
            )
            _trace_provider_event("json_repair_success")
            return repaired
        except ProviderError as exc:
            _append_provider_capture(
                "repair_failed",
                content=raw_text,
                payload_kind="assistant_text",
                max_completion_tokens=max_completion_tokens,
                error=str(exc),
            )
            _trace_provider_event("json_repair_failed", error=exc)
            return None

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        if not self.config.api_key:
            env_hint = self.config.api_key_env or "provider-specific env var"
            raise ProviderError(
                f"No API key configured for provider profile using model {self.config.model!r}. "
                f"Put it in .agentsecrets under providers.<profile>.api_key or set {env_hint}."
            )
        _trace_provider_event(
            "complete_json_start",
            message_count=len(messages),
            budgets=",".join(str(item) for item in self._completion_budgets()),
        )
        budgets = self._completion_budgets()
        last_error: ProviderError | None = None
        for index, budget in enumerate(budgets):
            malformed_attempts = 0
            tool_use_attempts = 0
            request_messages = messages
            while True:
                try:
                    payload = self._request_json(request_messages, budget)
                except ProviderError as exc:
                    last_error = exc
                    if _is_json_validate_failed_error(exc):
                        failed_generation = _extract_failed_generation_text(exc)
                        if failed_generation:
                            repaired = self._repair_invalid_json(
                                request_messages,
                                failed_generation,
                                max(budget, budgets[-1]),
                            )
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="json_validate_repair")
                                return repaired
                        if malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                            self._refresh_session()
                            request_messages = self._messages_for_json_retry(messages)
                            time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                            malformed_attempts += 1
                            continue
                    if _is_tool_use_failed_error(exc):
                        failed_generation = _extract_failed_generation_text(exc)
                        if failed_generation:
                            salvaged = _salvage_failed_generation_action(failed_generation)
                            if salvaged is not None:
                                return salvaged
                            repaired = self._repair_invalid_json(
                                request_messages,
                                failed_generation,
                                max(budget, budgets[-1]),
                            )
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="tool_use_repair")
                                return repaired
                    if _is_tool_use_failed_error(exc) and tool_use_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                        self._refresh_session()
                        request_messages = self._messages_for_tool_retry(messages)
                        time.sleep(_malformed_success_delay_seconds(tool_use_attempts))
                        tool_use_attempts += 1
                        continue
                    raise
                raw_text: str | None = None
                choices = payload.get("choices") or []
                finish_reason = None
                if choices and isinstance(choices[0], dict):
                    finish_reason = choices[0].get("finish_reason")
                try:
                    raw_text = self._extract_content(payload)
                    _append_provider_capture(
                        "provider_raw_text",
                        content=raw_text,
                        payload_kind="assistant_text",
                        budget=budget,
                        finish_reason=finish_reason,
                    )
                    parsed = self._parse_json_object(raw_text)
                    _append_provider_capture(
                        "provider_parsed_json",
                        parsed_payload=parsed,
                        payload_kind="json_object",
                        budget=budget,
                        finish_reason=finish_reason,
                    )
                    _trace_provider_event("complete_json_success", path="direct")
                    return parsed
                except ProviderError as exc:
                    last_error = exc
                    error_text = str(exc).lower()
                    invalid_json = raw_text is not None and _is_invalid_json_error(exc)
                    malformed_retry = (
                        (
                            raw_text is None
                            and _is_malformed_success_error(exc)
                        )
                        or invalid_json
                    ) and (
                        malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES
                    )
                    if malformed_retry:
                        self._refresh_session()
                        if invalid_json and raw_text:
                            _append_provider_capture(
                                "provider_invalid_json_before_repair",
                                content=raw_text,
                                payload_kind="assistant_text",
                                budget=budget,
                                finish_reason=finish_reason,
                                error=str(exc),
                            )
                            repaired = self._repair_invalid_json(
                                messages,
                                raw_text,
                                max(budget, budgets[-1]),
                            )
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="invalid_json_repair")
                                return repaired
                        time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                        malformed_attempts += 1
                        continue
                    should_retry = (
                        index < len(budgets) - 1
                        and (
                            (raw_text is None and "empty content" in error_text and finish_reason == "length")
                            or (raw_text is not None and finish_reason == "length")
                            or (raw_text is None and _is_malformed_success_error(exc))
                            or invalid_json
                        )
                    )
                    if should_retry:
                        break
                    if raw_text:
                        _append_provider_capture(
                            "provider_final_repair_input",
                            content=raw_text,
                            payload_kind="assistant_text",
                            budget=budget,
                            finish_reason=finish_reason,
                            error=str(exc),
                        )
                        repaired = self._repair_invalid_json(messages, raw_text, max(budget, budgets[-1]))
                        if repaired is not None:
                            _trace_provider_event("complete_json_success", path="final_repair")
                            return repaired
                    raise
        _trace_provider_event("complete_json_failure", error=last_error or "unknown")
        raise last_error or ProviderError("Provider request failed without a specific error.")


class OpenAIProvider(ChatCompletionsJsonProvider):
    pass


class OpenAICompatibleProvider(ChatCompletionsJsonProvider):
    pass


class LMStudioProvider(OpenAICompatibleProvider):
    def _build_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        return headers

    def _response_format(self) -> dict[str, Any] | None:
        # LM Studio's OpenAI-compatible server rejects json_object here.
        # Keep the prompt contract authoritative and repair invalid JSON if needed.
        return None

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        _trace_provider_event(
            "complete_json_start",
            message_count=len(messages),
            budgets=",".join(str(item) for item in self._completion_budgets()),
        )
        budgets = self._completion_budgets()
        last_error: ProviderError | None = None
        for index, budget in enumerate(budgets):
            malformed_attempts = 0
            tool_use_attempts = 0
            request_messages = messages
            while True:
                try:
                    payload = self._request_json(request_messages, budget)
                except ProviderError as exc:
                    last_error = exc
                    if _is_json_validate_failed_error(exc):
                        failed_generation = _extract_failed_generation_text(exc)
                        if failed_generation:
                            repaired = self._repair_invalid_json(
                                request_messages,
                                failed_generation,
                                max(budget, budgets[-1]),
                            )
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="json_validate_repair")
                                return repaired
                        if malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                            self._refresh_session()
                            request_messages = self._messages_for_json_retry(messages)
                            time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                            malformed_attempts += 1
                            continue
                    if _is_tool_use_failed_error(exc):
                        failed_generation = _extract_failed_generation_text(exc)
                        if failed_generation:
                            salvaged = _salvage_native_tool_failed_generation(
                                failed_generation
                            )
                            if salvaged is not None:
                                return salvaged
                        if tool_use_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                            self._refresh_session()
                            request_messages = self._messages_for_tool_retry(messages)
                            time.sleep(_malformed_success_delay_seconds(tool_use_attempts))
                            tool_use_attempts += 1
                            continue
                    if _is_malformed_success_error(exc):
                        if malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                            self._refresh_session()
                            request_messages = self._messages_for_json_retry(messages)
                            time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                            malformed_attempts += 1
                            continue
                    break
                try:
                    raw_text = self._extract_content(payload)
                except ProviderError as exc:
                    last_error = exc
                    if malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                        self._refresh_session()
                        request_messages = self._messages_for_json_retry(messages)
                        time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                        malformed_attempts += 1
                        continue
                    break
                try:
                    return self._parse_json_object(raw_text)
                except ProviderError as exc:
                    last_error = exc
                    repaired = self._repair_invalid_json(
                        request_messages,
                        raw_text,
                        max(budget, budgets[-1]),
                    )
                    if repaired is not None:
                        _trace_provider_event("complete_json_success", path="invalid_json_repair")
                        return repaired
                    break
            if index < len(budgets) - 1:
                continue
        _trace_provider_event("complete_json_failure", error=last_error or "unknown")
        raise last_error or ProviderError(
            "Provider request failed without a specific error."
        )


_TRANSFORMERS_LOCAL_CACHE: dict[tuple[str, str | None, str, bool], tuple[Any, Any]] = {}
_TRANSFORMERS_LOCAL_CACHE_LOCK = threading.Lock()


class TransformersLocalProvider:
    LOCAL_MALFORMED_RETRY_LIMIT = 1
    LOCAL_REPAIR_MAX_NEW_TOKENS = 256

    def __init__(self, config: ProviderProfileConfig):
        self.config = config

    def _preferred_dtype(self) -> Any:
        import torch

        if torch.cuda.is_available() and torch.cuda.is_bf16_supported():
            return torch.bfloat16
        return torch.float16

    def _resolved_adapter_path(self) -> Path | None:
        raw = str(self.config.adapter_path or "").strip()
        if not raw:
            return None
        path = Path(raw)
        if not path.is_absolute():
            root = self.config.repo_root or Path.cwd()
            path = root / path
        return path.resolve()

    def _cache_key(self) -> tuple[str, str | None, str, bool]:
        adapter_path = self._resolved_adapter_path()
        return (
            str(self.config.model or "").strip(),
            str(adapter_path) if adapter_path is not None else None,
            str(self.config.quantization or "none").strip().lower(),
            bool(self.config.trust_remote_code),
        )

    def _load_components(self) -> tuple[Any, Any]:
        cache_key = self._cache_key()
        with _TRANSFORMERS_LOCAL_CACHE_LOCK:
            cached = _TRANSFORMERS_LOCAL_CACHE.get(cache_key)
            if cached is not None:
                return cached

            from peft import PeftModel
            from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig

            adapter_path = self._resolved_adapter_path()
            tokenizer_source = str(adapter_path) if adapter_path and adapter_path.exists() else self.config.model
            tokenizer = AutoTokenizer.from_pretrained(
                tokenizer_source,
                trust_remote_code=bool(self.config.trust_remote_code),
            )
            if tokenizer.pad_token is None and tokenizer.eos_token is not None:
                tokenizer.pad_token = tokenizer.eos_token

            model_kwargs: dict[str, Any] = {
                "trust_remote_code": bool(self.config.trust_remote_code),
                "dtype": self._preferred_dtype(),
            }
            if str(self.config.quantization or "none").strip().lower() == "4bit":
                model_kwargs["quantization_config"] = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_use_double_quant=True,
                    bnb_4bit_quant_type="nf4",
                    bnb_4bit_compute_dtype=self._preferred_dtype(),
                )

            model = AutoModelForCausalLM.from_pretrained(
                str(self.config.model),
                **model_kwargs,
            )
            if adapter_path is not None and adapter_path.exists():
                model = PeftModel.from_pretrained(model, str(adapter_path), is_trainable=False)
            model.eval()
            if getattr(model.config, "use_cache", None) is not None:
                model.config.use_cache = True
            cached = (model, tokenizer)
            _TRANSFORMERS_LOCAL_CACHE[cache_key] = cached
            return cached

    def _build_prompt_text(self, messages: list[ChatMessage], tokenizer: Any) -> str:
        message_payload = [
            {"role": message.role, "content": message.content}
            for message in messages
        ]
        if hasattr(tokenizer, "apply_chat_template"):
            return tokenizer.apply_chat_template(
                message_payload,
                tokenize=False,
                add_generation_prompt=True,
            )
        return "\n".join(f"{message.role}: {message.content}" for message in messages)

    def _messages_for_json_retry(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        return [
            *messages,
            ChatMessage(
                role="system",
                content=(
                    "Return plain assistant text containing exactly one valid JSON object that matches the required schema. "
                    "Keep reasoning very short. Do not emit markdown, bullets, prose outside JSON, duplicate JSON objects, or suffix junk. "
                    "Stop immediately after the closing brace."
                ),
            ),
        ]

    def _generate_text(
        self,
        messages: list[ChatMessage],
        *,
        max_new_tokens: int | None = None,
    ) -> str:
        import torch

        model, tokenizer = self._load_components()
        prompt_text = self._build_prompt_text(messages, tokenizer)
        model_inputs = tokenizer(prompt_text, return_tensors="pt")
        prompt_tokens = int(model_inputs["input_ids"].shape[1])
        max_tokens = max(
            1,
            int(
                max_new_tokens
                if max_new_tokens is not None
                else self.config.max_tokens
            ),
        )
        do_sample = float(self.config.temperature or 0.0) > 0.05
        _write_provider_request_snapshot(
            "transformers_local_generation",
            messages=messages,
            prompt_text=prompt_text,
            request_payload={
                "max_new_tokens": max_tokens,
                "do_sample": do_sample,
                "temperature": float(self.config.temperature or 0.0),
            },
            prompt_tokens=prompt_tokens,
        )
        _append_provider_capture(
            "local_generation_request",
            payload_kind="local_generation_request",
            prompt_chars=len(prompt_text),
            prompt_tokens=prompt_tokens,
            message_count=len(messages),
            max_new_tokens=max_tokens,
            do_sample=do_sample,
            model_device=str(getattr(model, "device", "unknown")),
        )
        _trace_provider_event(
            "local_generation_request",
            prompt_chars=len(prompt_text),
            prompt_tokens=prompt_tokens,
            message_count=len(messages),
            max_new_tokens=max_tokens,
        )
        model_inputs = {
            key: value.to(model.device) if hasattr(value, "to") else value
            for key, value in model_inputs.items()
        }
        generate_kwargs: dict[str, Any] = {
            "max_new_tokens": max_tokens,
            "do_sample": do_sample,
            "pad_token_id": tokenizer.pad_token_id,
            "eos_token_id": tokenizer.eos_token_id,
        }
        if do_sample:
            generate_kwargs["temperature"] = float(self.config.temperature)
        with torch.inference_mode():
            outputs = model.generate(**model_inputs, **generate_kwargs)
        prompt_length = int(model_inputs["input_ids"].shape[1])
        generated = outputs[0][prompt_length:]
        generated_text = tokenizer.decode(generated, skip_special_tokens=True)
        _append_provider_capture(
            "local_generation_response",
            payload_kind="local_generation_response",
            generated_chars=len(generated_text),
            generated_tokens=int(generated.shape[0]),
        )
        _trace_provider_event(
            "local_generation_response",
            generated_chars=len(generated_text),
            generated_tokens=int(generated.shape[0]),
        )
        return generated_text

    def _repair_invalid_json(self, raw_text: str) -> dict[str, Any] | None:
        repair_messages = [
            ChatMessage(role="system", content=JSON_REPAIR_PROMPT),
            ChatMessage(
                role="user",
                content=(
                    "Repair this invalid or incomplete JSON into one complete valid JSON object only.\n\n"
                    + raw_text
                ),
            ),
        ]
        try:
            repaired_text = self._generate_text(
                repair_messages,
                max_new_tokens=min(
                    self.LOCAL_REPAIR_MAX_NEW_TOKENS,
                    max(1, int(self.config.max_tokens)),
                ),
            )
            _append_provider_capture(
                "repair_response",
                content=repaired_text,
                payload_kind="assistant_text",
            )
            repaired = _parse_provider_json_object(repaired_text)
            _append_provider_capture(
                "provider_parsed_json",
                parsed_payload=repaired,
                payload_kind="json_object",
            )
            return repaired
        except Exception:
            return None

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        _trace_provider_event(
            "complete_json_start",
            message_count=len(messages),
            local_provider="transformers_local",
            quantization=str(self.config.quantization or "none"),
        )
        malformed_attempts = 0
        request_messages = messages
        last_error: ProviderError | None = None
        while True:
            raw_text: str | None = None
            try:
                raw_text = self._generate_text(request_messages)
                _append_provider_capture(
                    "provider_raw_text",
                    content=raw_text,
                    payload_kind="assistant_text",
                )
                parsed = _parse_provider_json_object(raw_text)
                _append_provider_capture(
                    "provider_parsed_json",
                    parsed_payload=parsed,
                    payload_kind="json_object",
                )
                _trace_provider_event("complete_json_success", path="direct")
                return parsed
            except Exception as exc:
                last_error = exc if isinstance(exc, ProviderError) else ProviderError(str(exc))
                salvaged = _salvage_failed_generation_action(raw_text or "")
                if salvaged is not None:
                    _trace_provider_event("complete_json_success", path="tool_salvage")
                    return salvaged
                repaired = self._repair_invalid_json(raw_text or "")
                if repaired is not None:
                    _trace_provider_event("complete_json_success", path="invalid_json_repair")
                    return repaired
                if malformed_attempts < self.LOCAL_MALFORMED_RETRY_LIMIT:
                    request_messages = self._messages_for_json_retry(messages)
                    time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                    malformed_attempts += 1
                    continue
                break
        _trace_provider_event("complete_json_failure", error=last_error or "unknown")
        raise last_error or ProviderError("Local transformers provider failed without a specific error.")


class MiniMaxProvider(OpenAICompatibleProvider):
    def _completion_budget_fields(self, max_completion_tokens: int) -> dict[str, Any]:
        return {"max_tokens": max_completion_tokens}

    def _response_format(self) -> dict[str, Any] | None:
        # MiniMax documents OpenAI compatibility, but this runtime only needs
        # plain JSON text. Keep the prompt contract authoritative and repair
        # malformed JSON locally instead of assuming strict response_format support.
        return None


class OpenRouterProvider(ChatCompletionsJsonProvider):
    def _completion_budget_fields(self, max_completion_tokens: int) -> dict[str, Any]:
        return {"max_tokens": max_completion_tokens}


class XAIChatProvider(ChatCompletionsJsonProvider):
    def _completion_budget_fields(self, max_completion_tokens: int) -> dict[str, Any]:
        return {"max_tokens": max_completion_tokens}

    def _response_format(self) -> dict[str, Any] | None:
        # xAI supports structured outputs, but the current runtime only needs
        # a raw JSON object string. Keep the prompt contract authoritative here.
        return None


class GroqProvider(ChatCompletionsJsonProvider):
    def _max_completion_tokens_cap(self) -> int | None:
        model = (self.config.model or "").strip().lower()
        if model in {"openai/gpt-oss-20b", "openai/gpt-oss-120b"}:
            return 8192
        return None

    def _build_body(
        self,
        messages: list[ChatMessage],
        max_completion_tokens: int,
    ) -> dict[str, Any]:
        body = super()._build_body(messages, max_completion_tokens)
        body["tool_choice"] = "none"
        body["parallel_tool_calls"] = False
        return body


class ResponsesJsonProvider:
    def __init__(self, config: ProviderProfileConfig):
        self.config = config
        self.session = requests.Session()

    def _build_url(self) -> str:
        api_base = (self.config.api_base or "").rstrip("/")
        if not api_base:
            raise ProviderError(
                f"Provider {self.config.provider_type!r} is missing api_base."
            )
        return api_base + "/responses"

    def _build_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def _refresh_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = requests.Session()

    def _build_body(
        self,
        messages: list[ChatMessage],
        max_completion_tokens: int,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": self.config.model,
            "input": [{"role": message.role, "content": message.content} for message in messages],
            "temperature": self.config.temperature,
        }
        body.update(self._completion_budget_fields(max_completion_tokens))
        return body

    def _completion_budget_fields(self, max_completion_tokens: int) -> dict[str, Any]:
        return {"max_output_tokens": max_completion_tokens}

    def _messages_for_tool_retry(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        return [
            *messages,
            ChatMessage(
                role="system",
                content=(
                    "Do not call any native tools, function calls, provider tools, MCP tools, "
                    "or built-in tools. Return plain assistant text containing only the required JSON object."
                ),
            ),
        ]

    def _messages_for_json_retry(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        return [
            *messages,
            ChatMessage(
                role="system",
                content=(
                    "Return plain assistant text containing exactly one valid JSON object that matches the required schema. "
                    "Do not emit empty output. Do not emit markdown, bullets, prose outside JSON, or native tool calls."
                ),
            ),
        ]

    def _max_completion_tokens_cap(self) -> int | None:
        return None

    def _completion_budgets(self) -> list[int]:
        base_budget = max(1, int(self.config.max_tokens))
        fallback_budget = max(base_budget * 2, 4800)
        cap = self._max_completion_tokens_cap()
        if cap is not None:
            base_budget = min(base_budget, cap)
            fallback_budget = min(fallback_budget, cap)
        budgets: list[int] = []
        for budget in (base_budget, fallback_budget):
            if budget not in budgets:
                budgets.append(budget)
        return budgets

    def _extract_content(self, payload: dict[str, Any]) -> str:
        output_text = payload.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text
        output = payload.get("output")
        if isinstance(output, list):
            parts: list[str] = []
            for item in output:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "message":
                    continue
                content = item.get("content")
                if not isinstance(content, list):
                    continue
                for content_item in content:
                    if not isinstance(content_item, dict):
                        continue
                    content_type = content_item.get("type")
                    if content_type in {"output_text", "text"}:
                        parts.append(str(content_item.get("text", "")))
            combined = "".join(parts).strip()
            if combined:
                return combined
        raise ProviderError("Provider response did not contain text content.")

    def _parse_json_object(self, text: str) -> dict[str, Any]:
        return _parse_provider_json_object(text)

    def _request_json(
        self,
        messages: list[ChatMessage],
        max_completion_tokens: int,
    ) -> dict[str, Any]:
        body = self._build_body(messages, max_completion_tokens)
        _write_provider_request_snapshot(
            "responses_api_request",
            messages=messages,
            request_payload=body,
            max_completion_tokens=max_completion_tokens,
        )
        return _post_json_with_retry(
            self.session,
            url=self._build_url(),
            headers=self._build_headers(),
            body=body,
            timeout_seconds=self.config.timeout_seconds,
            config=self.config,
        )

    def _repair_invalid_json(
        self,
        messages: list[ChatMessage],
        raw_text: str,
        max_completion_tokens: int,
    ) -> dict[str, Any] | None:
        _trace_provider_event(
            "json_repair_start",
            max_completion_tokens=max_completion_tokens,
            raw_preview=raw_text[:240],
        )
        repair_messages = [
            *messages,
            ChatMessage(role="assistant", content=raw_text),
            ChatMessage(role="user", content=JSON_REPAIR_PROMPT),
        ]
        _append_provider_capture(
            "repair_request",
            content=raw_text,
            payload_kind="assistant_text",
            max_completion_tokens=max_completion_tokens,
        )
        try:
            payload = self._request_json(repair_messages, max_completion_tokens)
            repaired_text = self._extract_content(payload)
            _append_provider_capture(
                "repair_response",
                content=repaired_text,
                payload_kind="assistant_text",
                max_completion_tokens=max_completion_tokens,
            )
            repaired = self._parse_json_object(repaired_text)
            _append_provider_capture(
                "provider_parsed_json",
                parsed_payload=repaired,
                payload_kind="json_object",
                max_completion_tokens=max_completion_tokens,
            )
            _trace_provider_event("json_repair_success")
            return repaired
        except ProviderError as exc:
            _append_provider_capture(
                "repair_failed",
                content=raw_text,
                payload_kind="assistant_text",
                max_completion_tokens=max_completion_tokens,
                error=str(exc),
            )
            _trace_provider_event("json_repair_failed", error=exc)
            return None

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        if not self.config.api_key:
            env_hint = self.config.api_key_env or "provider-specific env var"
            raise ProviderError(
                f"No API key configured for provider profile using model {self.config.model!r}. "
                f"Put it in .agentsecrets under providers.<profile>.api_key or set {env_hint}."
            )
        _trace_provider_event(
            "complete_json_start",
            message_count=len(messages),
            budgets=",".join(str(item) for item in self._completion_budgets()),
        )
        budgets = self._completion_budgets()
        last_error: ProviderError | None = None
        for index, budget in enumerate(budgets):
            malformed_attempts = 0
            tool_use_attempts = 0
            request_messages = messages
            while True:
                try:
                    payload = self._request_json(request_messages, budget)
                except ProviderError as exc:
                    last_error = exc
                    if _is_json_validate_failed_error(exc):
                        failed_generation = _extract_failed_generation_text(exc)
                        if failed_generation:
                            repaired = self._repair_invalid_json(
                                request_messages,
                                failed_generation,
                                max(budget, budgets[-1]),
                            )
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="json_validate_repair")
                                return repaired
                        if malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                            self._refresh_session()
                            request_messages = self._messages_for_json_retry(messages)
                            time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                            malformed_attempts += 1
                            continue
                    if _is_tool_use_failed_error(exc):
                        failed_generation = _extract_failed_generation_text(exc)
                        if failed_generation:
                            salvaged = _salvage_failed_generation_action(failed_generation)
                            if salvaged is not None:
                                return salvaged
                            repaired = self._repair_invalid_json(
                                request_messages,
                                failed_generation,
                                max(budget, budgets[-1]),
                            )
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="tool_use_repair")
                                return repaired
                    if _is_tool_use_failed_error(exc) and tool_use_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES:
                        self._refresh_session()
                        request_messages = self._messages_for_tool_retry(messages)
                        time.sleep(_malformed_success_delay_seconds(tool_use_attempts))
                        tool_use_attempts += 1
                        continue
                    raise
                raw_text: str | None = None
                finish_reason = payload.get("status")
                try:
                    raw_text = self._extract_content(payload)
                    _append_provider_capture(
                        "provider_raw_text",
                        content=raw_text,
                        payload_kind="assistant_text",
                        budget=budget,
                        finish_reason=finish_reason,
                    )
                    parsed = self._parse_json_object(raw_text)
                    _append_provider_capture(
                        "provider_parsed_json",
                        parsed_payload=parsed,
                        payload_kind="json_object",
                        budget=budget,
                        finish_reason=finish_reason,
                    )
                    _trace_provider_event("complete_json_success", path="direct")
                    return parsed
                except ProviderError as exc:
                    last_error = exc
                    invalid_json = raw_text is not None and _is_invalid_json_error(exc)
                    malformed_retry = (
                        (
                            raw_text is None
                            and _is_malformed_success_error(exc)
                        )
                        or invalid_json
                    ) and (
                        malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES
                    )
                    if malformed_retry:
                        self._refresh_session()
                        if invalid_json and raw_text:
                            _append_provider_capture(
                                "provider_invalid_json_before_repair",
                                content=raw_text,
                                payload_kind="assistant_text",
                                budget=budget,
                                finish_reason=finish_reason,
                                error=str(exc),
                            )
                            repaired = self._repair_invalid_json(
                                messages,
                                raw_text,
                                max(budget, budgets[-1]),
                            )
                            if repaired is not None:
                                _trace_provider_event("complete_json_success", path="invalid_json_repair")
                                return repaired
                        time.sleep(_malformed_success_delay_seconds(malformed_attempts))
                        malformed_attempts += 1
                        continue
                    should_retry = index < len(budgets) - 1 and (
                        finish_reason == "incomplete" or invalid_json
                    )
                    if should_retry:
                        break
                    if raw_text:
                        _append_provider_capture(
                            "provider_final_repair_input",
                            content=raw_text,
                            payload_kind="assistant_text",
                            budget=budget,
                            finish_reason=finish_reason,
                            error=str(exc),
                        )
                        repaired = self._repair_invalid_json(messages, raw_text, max(budget, budgets[-1]))
                        if repaired is not None:
                            _trace_provider_event("complete_json_success", path="final_repair")
                            return repaired
                    raise
        _trace_provider_event("complete_json_failure", error=last_error or "unknown")
        raise last_error or ProviderError("Provider request failed without a specific error.")


class XAIResponsesProvider(ResponsesJsonProvider):
    def _completion_budget_fields(self, max_completion_tokens: int) -> dict[str, Any]:
        # xAI multi-agent explicitly does not support output-token caps.
        if "multi-agent" in self.config.model:
            return {}
        return {"max_output_tokens": max_completion_tokens}


def create_provider(config: ProviderProfileConfig) -> JsonCompletionProvider:
    normalized = (config.provider_type or "openai").strip().lower()
    transport = (config.transport or "chat_completions").strip().lower()
    if normalized == "codex":
        return CodexAppServerProvider(config)
    if normalized == "transformers_local":
        return TransformersLocalProvider(config)
    if normalized == "minimax":
        return MiniMaxProvider(config)
    if normalized == "xai":
        if transport == "responses":
            return XAIResponsesProvider(config)
        return XAIChatProvider(config)
    if normalized == "groq":
        if transport == "responses":
            return ResponsesJsonProvider(config)
        return GroqProvider(config)
    if transport == "responses":
        return ResponsesJsonProvider(config)
    if normalized == "openrouter":
        return OpenRouterProvider(config)
    if normalized == "lmstudio":
        return LMStudioProvider(config)
    if normalized == "openai_compatible":
        return OpenAICompatibleProvider(config)
    if normalized == "openai":
        return OpenAIProvider(config)
    raise ProviderError(f"Unsupported provider type: {config.provider_type!r}")
