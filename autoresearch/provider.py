from __future__ import annotations

import json
import time
from email.utils import parsedate_to_datetime
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from .config import ProviderProfileConfig


class ProviderError(RuntimeError):
    """Raised when the provider cannot satisfy a request."""


@dataclass
class ChatMessage:
    role: str
    content: str


JSON_REPAIR_PROMPT = """Your previous assistant response was invalid or incomplete JSON.

Return one complete corrected JSON object only.
Preserve the original intent, reasoning, and planned actions.
Do not add Markdown or explanations.
Escape Windows paths correctly inside JSON strings.
"""

DEFAULT_RATE_LIMIT_BACKOFF_SECONDS = [15, 30, 60, 120, 180, 240, 300]
DEFAULT_RATE_LIMIT_MAX_RETRIES = 18
DEFAULT_TRANSIENT_MAX_RETRIES = 4
DEFAULT_MALFORMED_SUCCESS_RETRIES = 2
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


def _is_malformed_success_error(error: ProviderError) -> bool:
    lowered = str(error).lower()
    return (
        "no choices" in lowered
        or "did not contain text content" in lowered
        or "empty content" in lowered
    )


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
    while True:
        try:
            response = session.post(
                url,
                headers=headers,
                json=body,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            transient_retries += 1
            if transient_retries > DEFAULT_TRANSIENT_MAX_RETRIES:
                raise ProviderError(
                    f"Provider request failed after {transient_retries} transient retries: {exc}"
                ) from exc
            time.sleep(_transient_delay_seconds(config, transient_retries - 1, None))
            continue

        classification = _classify_retryable_response(response)
        if classification is not None:
            retry_kind, retry_after = classification
            if retry_kind == "rate_limit":
                rate_limit_retries += 1
                if rate_limit_retries > _configured_rate_limit_max_retries(config):
                    raise ProviderError(
                        f"Provider remained rate-limited after {rate_limit_retries} retries: "
                        f"{response.status_code} {response.text[:800]}"
                    )
                time.sleep(_rate_limit_delay_seconds(config, rate_limit_retries - 1, retry_after))
                continue
            transient_retries += 1
            if transient_retries > DEFAULT_TRANSIENT_MAX_RETRIES:
                raise ProviderError(
                    f"Provider request kept failing transiently after {transient_retries} retries: "
                    f"{response.status_code} {response.text[:800]}"
                )
            time.sleep(_transient_delay_seconds(config, transient_retries - 1, retry_after))
            continue

        if response.status_code >= 400:
            raise ProviderError(
                f"Provider request failed with {response.status_code}: {response.text[:800]}"
            )
        return response.json()


class JsonCompletionProvider(Protocol):
    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        ...


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
        preview = json.dumps(payload, ensure_ascii=True)[:800]
        raise ProviderError(f"Provider response did not contain text content. payload={preview}")

    def _parse_json_object(self, text: str) -> dict[str, Any]:
        return _parse_provider_json_object(text)

    def _request_json(
        self,
        messages: list[ChatMessage],
        max_completion_tokens: int,
    ) -> dict[str, Any]:
        body = self._build_body(messages, max_completion_tokens)
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
        repair_messages = [
            *messages,
            ChatMessage(role="assistant", content=raw_text),
            ChatMessage(role="user", content=JSON_REPAIR_PROMPT),
        ]
        try:
            payload = self._request_json(repair_messages, max_completion_tokens)
            return self._parse_json_object(self._extract_content(payload))
        except ProviderError:
            return None

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        if not self.config.api_key:
            env_hint = self.config.api_key_env or "provider-specific env var"
            raise ProviderError(
                f"No API key configured for provider profile using model {self.config.model!r}. "
                f"Put it in .agentsecrets under providers.<profile>.api_key or set {env_hint}."
            )
        budgets = [self.config.max_tokens, max(self.config.max_tokens * 2, 4800)]
        last_error: ProviderError | None = None
        for index, budget in enumerate(budgets):
            malformed_attempts = 0
            while True:
                payload = self._request_json(messages, budget)
                raw_text: str | None = None
                choices = payload.get("choices") or []
                finish_reason = None
                if choices and isinstance(choices[0], dict):
                    finish_reason = choices[0].get("finish_reason")
                try:
                    raw_text = self._extract_content(payload)
                    return self._parse_json_object(raw_text)
                except ProviderError as exc:
                    last_error = exc
                    error_text = str(exc).lower()
                    malformed_retry = (
                        raw_text is None
                        and _is_malformed_success_error(exc)
                        and malformed_attempts < DEFAULT_MALFORMED_SUCCESS_RETRIES
                    )
                    if malformed_retry:
                        malformed_attempts += 1
                        continue
                    should_retry = (
                        index < len(budgets) - 1
                        and (
                            (raw_text is None and "empty content" in error_text and finish_reason == "length")
                            or (raw_text is not None and finish_reason == "length")
                            or (raw_text is None and _is_malformed_success_error(exc))
                        )
                    )
                    if should_retry:
                        break
                    if raw_text:
                        repaired = self._repair_invalid_json(messages, raw_text, max(budget, budgets[-1]))
                        if repaired is not None:
                            return repaired
                    raise
        raise last_error or ProviderError("Provider request failed without a specific error.")


class OpenAIProvider(ChatCompletionsJsonProvider):
    pass


class OpenAICompatibleProvider(ChatCompletionsJsonProvider):
    pass


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
        repair_messages = [
            *messages,
            ChatMessage(role="assistant", content=raw_text),
            ChatMessage(role="user", content=JSON_REPAIR_PROMPT),
        ]
        try:
            payload = self._request_json(repair_messages, max_completion_tokens)
            return self._parse_json_object(self._extract_content(payload))
        except ProviderError:
            return None

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        if not self.config.api_key:
            env_hint = self.config.api_key_env or "provider-specific env var"
            raise ProviderError(
                f"No API key configured for provider profile using model {self.config.model!r}. "
                f"Put it in .agentsecrets under providers.<profile>.api_key or set {env_hint}."
            )
        budgets = [self.config.max_tokens, max(self.config.max_tokens * 2, 4800)]
        last_error: ProviderError | None = None
        for index, budget in enumerate(budgets):
            payload = self._request_json(messages, budget)
            raw_text: str | None = None
            finish_reason = payload.get("status")
            try:
                raw_text = self._extract_content(payload)
                return self._parse_json_object(raw_text)
            except ProviderError as exc:
                last_error = exc
                should_retry = index < len(budgets) - 1 and finish_reason == "incomplete"
                if should_retry:
                    continue
                if raw_text:
                    repaired = self._repair_invalid_json(messages, raw_text, max(budget, budgets[-1]))
                    if repaired is not None:
                        return repaired
                raise
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
    if normalized == "xai":
        if transport == "responses":
            return XAIResponsesProvider(config)
        return XAIChatProvider(config)
    if transport == "responses":
        return ResponsesJsonProvider(config)
    if normalized == "openrouter":
        return OpenRouterProvider(config)
    if normalized == "openai_compatible":
        return OpenAICompatibleProvider(config)
    if normalized == "openai":
        return OpenAIProvider(config)
    raise ProviderError(f"Unsupported provider type: {config.provider_type!r}")
