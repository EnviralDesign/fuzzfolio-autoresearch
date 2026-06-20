from __future__ import annotations

import os
import secrets
from pathlib import Path


LAB_GATEWAY_TOKEN_ENV = "FUZZFOLIO_LAB_GATEWAY_TOKEN"
LAB_GATEWAY_TOKEN_FILE_ENV = "FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE"
DEFAULT_LAB_GATEWAY_TOKEN_FILE_NAME = "play-hand-lab-gateway-token.txt"


def default_lab_gateway_token_file() -> Path:
    explicit = os.environ.get(LAB_GATEWAY_TOKEN_FILE_ENV)
    if explicit:
        return Path(explicit).expanduser()

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "FuzzfolioAutoResearch" / DEFAULT_LAB_GATEWAY_TOKEN_FILE_NAME

    return Path.home() / ".fuzzfolio-autoresearch" / DEFAULT_LAB_GATEWAY_TOKEN_FILE_NAME


def load_lab_gateway_token(*, create: bool = False) -> str | None:
    env_token = str(os.environ.get(LAB_GATEWAY_TOKEN_ENV) or "").strip()
    if env_token:
        return env_token

    token_file = default_lab_gateway_token_file()
    os.environ.setdefault(LAB_GATEWAY_TOKEN_FILE_ENV, str(token_file))

    try:
        token = token_file.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        token = ""

    if token:
        os.environ.setdefault(LAB_GATEWAY_TOKEN_ENV, token)
        return token

    if not create:
        return None

    token_file.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(48)
    token_file.write_text(token, encoding="ascii")
    os.environ.setdefault(LAB_GATEWAY_TOKEN_ENV, token)
    return token

