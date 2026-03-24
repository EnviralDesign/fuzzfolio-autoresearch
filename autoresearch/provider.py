from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests

from .config import ProviderConfig


class ProviderError(RuntimeError):
    """Raised when the provider cannot satisfy a request."""


@dataclass
class ChatMessage:
    role: str
    content: str


class OpenAICompatibleProvider:
    def __init__(self, config: ProviderConfig):
        self.config = config
        self.session = requests.Session()

    def _build_url(self) -> str:
        return self.config.api_base.rstrip("/") + "/chat/completions"

    def _extract_content(self, payload: dict[str, Any]) -> str:
        choices = payload.get("choices") or []
        if not choices:
            raise ProviderError("Provider response had no choices.")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
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
                value, _ = decoder.raw_decode(text[start:])
                if isinstance(value, dict):
                    return value
        raise ProviderError(f"Provider did not return valid JSON. Raw response: {text[:800]}")

    def complete_json(self, messages: list[ChatMessage]) -> dict[str, Any]:
        if not self.config.api_key:
            raise ProviderError(
                "No provider API key configured. Put it in .agentsecrets under provider.api_key or set OPENAI_API_KEY."
            )
        body = {
            "model": self.config.model,
            "messages": [{"role": message.role, "content": message.content} for message in messages],
            "temperature": self.config.temperature,
            "max_completion_tokens": self.config.max_tokens,
        }
        response = self.session.post(
            self._build_url(),
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=self.config.timeout_seconds,
        )
        if response.status_code >= 400:
            raise ProviderError(
                f"Provider request failed with {response.status_code}: {response.text[:800]}"
            )
        payload = response.json()
        return self._parse_json_object(self._extract_content(payload))
