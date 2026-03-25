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
        decoder = json.JSONDecoder()
        text = text.strip()
        try:
            value, _ = decoder.raw_decode(text)
            if isinstance(value, dict):
                return value
        except json.JSONDecodeError:
            start = text.find("{")
            if start >= 0:
                try:
                    value, _ = decoder.raw_decode(text[start:])
                    if isinstance(value, dict):
                        return value
                except json.JSONDecodeError:
                    pass
        raise ProviderError(f"Provider did not return valid JSON. Raw response: {text[:800]}")

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
            try:
                return self._parse_json_object(self._extract_content(payload))
            except ProviderError as exc:
                last_error = exc
                choices = payload.get("choices") or []
                finish_reason = None
                if choices and isinstance(choices[0], dict):
                    finish_reason = choices[0].get("finish_reason")
                should_retry = (
                    index < len(budgets) - 1
                    and "empty content" in str(exc).lower()
                    and finish_reason == "length"
                )
                if should_retry:
                    continue
                raise
        raise last_error or ProviderError("Provider request failed without a specific error.")


class OpenAIProvider(ChatCompletionsJsonProvider):
    pass


class OpenAICompatibleProvider(ChatCompletionsJsonProvider):
    pass


class OpenRouterProvider(ChatCompletionsJsonProvider):
    pass


class XAIProvider(ChatCompletionsJsonProvider):
    def _completion_budget_fields(self, max_completion_tokens: int) -> dict[str, Any]:
        return {"max_tokens": max_completion_tokens}

    def _response_format(self) -> dict[str, Any] | None:
        # xAI supports structured outputs, but the current runtime only needs
        # a raw JSON object string. Keep the prompt contract authoritative here.
        return None


def create_provider(config: ProviderProfileConfig) -> JsonCompletionProvider:
    normalized = (config.provider_type or "openai").strip().lower()
    if normalized == "xai":
        return XAIProvider(config)
    if normalized == "openrouter":
        return OpenRouterProvider(config)
    if normalized == "openai_compatible":
        return OpenAICompatibleProvider(config)
    if normalized == "openai":
        return OpenAIProvider(config)
    raise ProviderError(f"Unsupported provider type: {config.provider_type!r}")
