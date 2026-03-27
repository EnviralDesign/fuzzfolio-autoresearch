from __future__ import annotations

import json
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
            raise ProviderError("Provider response had no choices.")
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
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(str(item.get("text", "")))
            if parts:
                return "".join(parts)
        raise ProviderError("Provider response did not contain text content.")

    def _parse_json_object(self, text: str) -> dict[str, Any]:
        return _parse_provider_json_object(text)

    def _request_json(
        self,
        messages: list[ChatMessage],
        max_completion_tokens: int,
    ) -> dict[str, Any]:
        body = self._build_body(messages, max_completion_tokens)
        response = self.session.post(
            self._build_url(),
            headers=self._build_headers(),
            json=body,
            timeout=self.config.timeout_seconds,
        )
        if response.status_code >= 400:
            raise ProviderError(
                f"Provider request failed with {response.status_code}: {response.text[:800]}"
            )
        return response.json()

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
            choices = payload.get("choices") or []
            finish_reason = None
            if choices and isinstance(choices[0], dict):
                finish_reason = choices[0].get("finish_reason")
            try:
                raw_text = self._extract_content(payload)
                return self._parse_json_object(raw_text)
            except ProviderError as exc:
                last_error = exc
                should_retry = (
                    index < len(budgets) - 1
                    and (
                        (raw_text is None and "empty content" in str(exc).lower() and finish_reason == "length")
                        or (raw_text is not None and finish_reason == "length")
                    )
                )
                if should_retry:
                    continue
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
        response = self.session.post(
            self._build_url(),
            headers=self._build_headers(),
            json=body,
            timeout=self.config.timeout_seconds,
        )
        if response.status_code >= 400:
            raise ProviderError(
                f"Provider request failed with {response.status_code}: {response.text[:800]}"
            )
        return response.json()

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
