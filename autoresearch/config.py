from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CONFIG_FILE_NAME = "autoresearch.config.json"
SECRETS_FILE_NAME = ".agentsecrets"


@dataclass
class ProviderConfig:
    api_base: str = "https://api.openai.com/v1"
    model: str = "gpt-5.4-mini"
    supervisor_model: str = "gpt-5.4"
    api_key: str | None = None
    temperature: float = 0.2
    max_tokens: int = 3200
    timeout_seconds: int = 120


@dataclass
class FuzzfolioConfig:
    cli_command: str = "fuzzfolio-agent-cli"
    base_url: str = "http://localhost:7946/api/dev"
    auth_profile: str = "robot"
    api_key: str | None = None
    workspace_root: Path | None = None
    email: str | None = None
    password: str | None = None


@dataclass
class ResearchConfig:
    max_steps: int = 40
    recent_attempts_window: int = 16
    label_prefix: str = "agentic"
    auto_seed_prompt: bool = True
    plot_lower_is_better: bool = False
    compact_trigger_tokens: int = 12000
    compact_keep_recent_messages: int = 4
    supervisor_recent_steps: int = 6


@dataclass
class SupervisorConfig:
    max_steps: int | None = None
    window_start: str | None = None
    window_end: str | None = None
    timezone: str = "America/Chicago"
    stop_mode: str = "after_step"


@dataclass
class AppConfig:
    repo_root: Path
    config_path: Path
    secrets_path: Path
    provider: ProviderConfig
    fuzzfolio: FuzzfolioConfig
    research: ResearchConfig
    supervisor: SupervisorConfig

    @property
    def runs_root(self) -> Path:
        return self.repo_root / "runs"

    @property
    def attempts_path(self) -> Path:
        return self.runs_root / "attempts.jsonl"

    @property
    def progress_plot_path(self) -> Path:
        return self.runs_root / "progress.png"

    @property
    def latest_run_link(self) -> Path:
        return self.runs_root / "latest-run.txt"

    @property
    def program_path(self) -> Path:
        return self.repo_root / "program.md"


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for path in [current, *current.parents]:
        if (path / ".git").exists() and (path / "program.md").exists():
            return path
    raise FileNotFoundError("Could not locate repo root containing .git and program.md.")


def load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _env_or_value(*keys: str, fallback: str | None = None) -> str | None:
    for key in keys:
        value = os.environ.get(key)
        if value:
            return value
    return fallback


def load_config(repo_root: Path | None = None) -> AppConfig:
    root = find_repo_root(repo_root)
    config_path = root / CONFIG_FILE_NAME
    secrets_path = root / SECRETS_FILE_NAME
    raw_config = load_json_file(config_path)
    raw_secrets = load_json_file(secrets_path)

    provider_cfg = raw_config.get("provider", {})
    provider_secrets = raw_secrets.get("provider", {})
    fuzzfolio_cfg = raw_config.get("fuzzfolio", {})
    fuzzfolio_secrets = raw_secrets.get("fuzzfolio", {})
    research_cfg = raw_config.get("research", {})
    supervisor_cfg = raw_config.get("supervisor", {})
    provider = ProviderConfig(
        api_base=_env_or_value("AUTORESEARCH_PROVIDER_BASE_URL", fallback=provider_cfg.get("api_base"))
        or ProviderConfig.api_base,
        model=_env_or_value("AUTORESEARCH_PROVIDER_MODEL", fallback=provider_cfg.get("model"))
        or ProviderConfig.model,
        supervisor_model=_env_or_value(
            "AUTORESEARCH_SUPERVISOR_MODEL",
            fallback=provider_cfg.get("supervisor_model"),
        )
        or ProviderConfig.supervisor_model,
        api_key=_env_or_value(
            "AUTORESEARCH_PROVIDER_API_KEY",
            "OPENAI_API_KEY",
            fallback=provider_secrets.get("api_key") or provider_cfg.get("api_key"),
        ),
        temperature=float(provider_cfg.get("temperature", ProviderConfig.temperature)),
        max_tokens=int(provider_cfg.get("max_tokens", ProviderConfig.max_tokens)),
        timeout_seconds=int(provider_cfg.get("timeout_seconds", ProviderConfig.timeout_seconds)),
    )

    fuzzfolio = FuzzfolioConfig(
        cli_command=_env_or_value("AUTORESEARCH_FUZZFOLIO_CLI", fallback=fuzzfolio_cfg.get("cli_command"))
        or FuzzfolioConfig.cli_command,
        base_url=_env_or_value(
            "PROFILE_DROP_BASE_URL",
            "FUZZFOLIO_AGENT_BASE_URL",
            fallback=fuzzfolio_cfg.get("base_url"),
        )
        or FuzzfolioConfig.base_url,
        auth_profile=_env_or_value(
            "PROFILE_DROP_AUTH_PROFILE",
            "FUZZFOLIO_AGENT_AUTH_PROFILE",
            fallback=fuzzfolio_cfg.get("auth_profile"),
        )
        or FuzzfolioConfig.auth_profile,
        api_key=_env_or_value(
            "PROFILE_DROP_API_KEY",
            "DEV_PROFILE_DROP_API_KEY",
            fallback=fuzzfolio_secrets.get("api_key") or fuzzfolio_cfg.get("api_key"),
        ),
        workspace_root=(
            Path(fuzzfolio_cfg["workspace_root"]).resolve()
            if fuzzfolio_cfg.get("workspace_root")
            else None
        ),
        email=_env_or_value("AUTORESEARCH_FUZZFOLIO_EMAIL", fallback=fuzzfolio_secrets.get("email")),
        password=_env_or_value("AUTORESEARCH_FUZZFOLIO_PASSWORD", fallback=fuzzfolio_secrets.get("password")),
    )

    research = ResearchConfig(
        max_steps=int(research_cfg.get("max_steps", ResearchConfig.max_steps)),
        recent_attempts_window=int(research_cfg.get("recent_attempts_window", ResearchConfig.recent_attempts_window)),
        label_prefix=research_cfg.get("label_prefix", ResearchConfig.label_prefix),
        auto_seed_prompt=bool(research_cfg.get("auto_seed_prompt", ResearchConfig.auto_seed_prompt)),
        plot_lower_is_better=bool(research_cfg.get("plot_lower_is_better", ResearchConfig.plot_lower_is_better)),
        compact_trigger_tokens=int(
            research_cfg.get("compact_trigger_tokens", ResearchConfig.compact_trigger_tokens)
        ),
        compact_keep_recent_messages=int(
            research_cfg.get(
                "compact_keep_recent_messages",
                ResearchConfig.compact_keep_recent_messages,
            )
        ),
        supervisor_recent_steps=int(
            research_cfg.get(
                "supervisor_recent_steps",
                ResearchConfig.supervisor_recent_steps,
            )
        ),
    )
    supervisor = SupervisorConfig(
        max_steps=(
            int(supervisor_cfg["max_steps"])
            if supervisor_cfg.get("max_steps") is not None
            else None
        ),
        window_start=supervisor_cfg.get("window_start"),
        window_end=supervisor_cfg.get("window_end"),
        timezone=supervisor_cfg.get("timezone", SupervisorConfig.timezone),
        stop_mode=supervisor_cfg.get("stop_mode", SupervisorConfig.stop_mode),
    )

    return AppConfig(
        repo_root=root,
        config_path=config_path,
        secrets_path=secrets_path,
        provider=provider,
        fuzzfolio=fuzzfolio,
        research=research,
        supervisor=supervisor,
    )
