from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CONFIG_FILE_NAME = "autoresearch.config.json"
SECRETS_FILE_NAME = ".agentsecrets"


@dataclass
class ProviderProfileConfig:
    provider_type: str = "openai"
    api_base: str | None = None
    command: str | None = None
    model: str = "gpt-5.4-mini"
    api_key: str | None = None
    api_key_ref: str | None = None
    api_key_env: str | None = None
    temperature: float = 0.2
    max_tokens: int = 3200
    timeout_seconds: int = 120
    transport: str = "chat_completions"
    compact_trigger_tokens: int | None = None
    rate_limit_backoff_seconds: list[int] | None = None
    rate_limit_max_retries: int | None = None


@dataclass
class LlmConfig:
    explorer_profile: str = "openai-mini"
    supervisor_profile: str = "openai-supervisor"


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
    quality_score_preset: str = "profile-drop"
    plot_lower_is_better: bool = False
    compact_trigger_tokens: int = 12000
    compact_keep_recent_messages: int = 4
    supervisor_recent_steps: int = 6
    finish_min_attempts: int = 4
    run_wrap_up_steps: int = 3
    phase_early_ratio: float = 0.35
    phase_late_ratio: float = 0.75
    horizon_early_months: int = 3
    horizon_mid_months: int = 12
    horizon_late_months: int = 24
    horizon_wrap_up_months: int = 36
    coverage_reference_timeframe: str = "M15"
    coverage_min_mid_months: int = 11
    coverage_min_wrap_up_months: int = 34


@dataclass
class SupervisorConfig:
    max_steps: int | None = None
    window_start: str | None = None
    window_end: str | None = None
    timezone: str = "America/Chicago"
    stop_mode: str = "after_step"
    soft_wrap_minutes: int = 30


@dataclass
class AppConfig:
    repo_root: Path
    config_path: Path
    secrets_path: Path
    llm: LlmConfig
    providers: dict[str, ProviderProfileConfig]
    fuzzfolio: FuzzfolioConfig
    research: ResearchConfig
    supervisor: SupervisorConfig

    @property
    def provider(self) -> ProviderProfileConfig:
        return self.providers[self.llm.explorer_profile]

    @property
    def supervisor_provider(self) -> ProviderProfileConfig:
        return self.providers[self.llm.supervisor_profile]

    def compact_trigger_tokens_for(self, profile_name: str) -> int:
        profile = self.providers[profile_name]
        if profile.compact_trigger_tokens is not None:
            return int(profile.compact_trigger_tokens)
        return int(self.research.compact_trigger_tokens)

    @property
    def runs_root(self) -> Path:
        return self.repo_root / "runs"

    @property
    def derived_root(self) -> Path:
        return self.runs_root / "derived"

    @property
    def aggregate_plot_path(self) -> Path:
        return self.derived_root / "progress-all-runs.png"

    @property
    def leaderboard_plot_path(self) -> Path:
        return self.derived_root / "leaderboard.png"

    @property
    def leaderboard_json_path(self) -> Path:
        return self.derived_root / "leaderboard.json"

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


def _provider_defaults(provider_type: str) -> dict[str, Any]:
    normalized = (provider_type or "openai").strip().lower()
    if normalized == "codex":
        return {
            "api_base": None,
            "command": "codex",
            "api_key_env": None,
            "timeout_seconds": 180,
            "transport": "app_server",
        }
    if normalized == "xai":
        return {
            "api_base": "https://api.x.ai/v1",
            "command": None,
            "api_key_env": "XAI_API_KEY",
            "timeout_seconds": 180,
            "transport": "chat_completions",
        }
    if normalized == "groq":
        return {
            "api_base": "https://api.groq.com/openai/v1",
            "command": None,
            "api_key_env": "GROQ_API_KEY",
            "timeout_seconds": 120,
            "transport": "chat_completions",
        }
    if normalized == "openrouter":
        return {
            "api_base": "https://openrouter.ai/api/v1",
            "command": None,
            "api_key_env": "OPENROUTER_API_KEY",
            "timeout_seconds": 120,
            "transport": "chat_completions",
        }
    if normalized == "openai_compatible":
        return {
            "api_base": None,
            "command": None,
            "api_key_env": "AUTORESEARCH_PROVIDER_API_KEY",
            "timeout_seconds": 120,
            "transport": "chat_completions",
        }
    return {
        "api_base": "https://api.openai.com/v1",
        "command": None,
        "api_key_env": "OPENAI_API_KEY",
        "timeout_seconds": 120,
        "transport": "chat_completions",
    }


def _load_provider_profiles(
    raw_config: dict[str, Any],
    raw_secrets: dict[str, Any],
) -> tuple[LlmConfig, dict[str, ProviderProfileConfig]]:
    llm_cfg = raw_config.get("llm", {})
    providers_cfg = raw_config.get("providers", {})
    provider_secrets_map = raw_secrets.get("providers", {})
    api_keys_map = raw_secrets.get("api_keys", {})

    if providers_cfg:
        profiles: dict[str, ProviderProfileConfig] = {}
        for profile_name, profile_cfg in providers_cfg.items():
            if not isinstance(profile_cfg, dict):
                continue
            provider_type = str(profile_cfg.get("type") or "openai")
            defaults = _provider_defaults(provider_type)
            secret_cfg = (
                provider_secrets_map.get(profile_name, {})
                if isinstance(provider_secrets_map.get(profile_name, {}), dict)
                else {}
            )
            api_key_ref = (
                str(secret_cfg.get("api_key_ref") or profile_cfg.get("api_key_ref") or "").strip()
                or None
            )
            referenced_api_key = None
            if api_key_ref and isinstance(api_keys_map, dict):
                candidate = api_keys_map.get(api_key_ref)
                if candidate:
                    referenced_api_key = str(candidate)
            api_key_env_value = profile_cfg.get("api_key_env")
            if api_key_env_value is None:
                api_key_env_value = defaults["api_key_env"]
            api_key_env = str(api_key_env_value).strip() if api_key_env_value else None
            profiles[profile_name] = ProviderProfileConfig(
                provider_type=provider_type,
                api_base=_env_or_value(
                    f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_BASE_URL",
                    fallback=profile_cfg.get("api_base") or defaults["api_base"],
                ),
                command=_env_or_value(
                    f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_COMMAND",
                    fallback=profile_cfg.get("command") or defaults.get("command"),
                ),
                model=_env_or_value(
                    f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_MODEL",
                    fallback=profile_cfg.get("model"),
                )
                or ProviderProfileConfig.model,
                api_key=_env_or_value(
                    f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_API_KEY",
                    api_key_env,
                    fallback=secret_cfg.get("api_key") or profile_cfg.get("api_key") or referenced_api_key,
                ),
                api_key_ref=api_key_ref,
                api_key_env=api_key_env,
                temperature=float(profile_cfg.get("temperature", ProviderProfileConfig.temperature)),
                max_tokens=int(profile_cfg.get("max_tokens", ProviderProfileConfig.max_tokens)),
                timeout_seconds=int(profile_cfg.get("timeout_seconds", defaults["timeout_seconds"])),
                transport=str(profile_cfg.get("transport") or defaults["transport"]),
                compact_trigger_tokens=(
                    int(profile_cfg["compact_trigger_tokens"])
                    if profile_cfg.get("compact_trigger_tokens") is not None
                    else None
                ),
                rate_limit_backoff_seconds=(
                    [int(item) for item in profile_cfg.get("rate_limit_backoff_seconds", [])]
                    if profile_cfg.get("rate_limit_backoff_seconds") is not None
                    else None
                ),
                rate_limit_max_retries=(
                    int(profile_cfg["rate_limit_max_retries"])
                    if profile_cfg.get("rate_limit_max_retries") is not None
                    else None
                ),
            )

        explorer_profile = _env_or_value(
            "AUTORESEARCH_EXPLORER_PROFILE",
            fallback=llm_cfg.get("explorer_profile"),
        ) or next(iter(profiles.keys()), LlmConfig.explorer_profile)
        supervisor_profile = _env_or_value(
            "AUTORESEARCH_SUPERVISOR_PROFILE",
            fallback=llm_cfg.get("supervisor_profile"),
        ) or explorer_profile

        if explorer_profile not in profiles:
            raise KeyError(f"Configured explorer_profile {explorer_profile!r} is not defined in providers.")
        if supervisor_profile not in profiles:
            raise KeyError(f"Configured supervisor_profile {supervisor_profile!r} is not defined in providers.")

        return LlmConfig(
            explorer_profile=explorer_profile,
            supervisor_profile=supervisor_profile,
        ), profiles

    legacy_provider_cfg = raw_config.get("provider", {})
    legacy_provider_secrets = raw_secrets.get("provider", {})
    provider_type = str(legacy_provider_cfg.get("type") or "openai")
    defaults = _provider_defaults(provider_type)
    shared_api_key = _env_or_value(
        "AUTORESEARCH_PROVIDER_API_KEY",
        defaults["api_key_env"],
        fallback=legacy_provider_secrets.get("api_key") or legacy_provider_cfg.get("api_key"),
    )
    shared_api_base = _env_or_value(
        "AUTORESEARCH_PROVIDER_BASE_URL",
        fallback=legacy_provider_cfg.get("api_base") or defaults["api_base"],
    )
    shared_temperature = float(legacy_provider_cfg.get("temperature", ProviderProfileConfig.temperature))
    shared_max_tokens = int(legacy_provider_cfg.get("max_tokens", ProviderProfileConfig.max_tokens))
    shared_timeout = int(legacy_provider_cfg.get("timeout_seconds", defaults["timeout_seconds"]))
    explorer_model = _env_or_value(
        "AUTORESEARCH_PROVIDER_MODEL",
        fallback=legacy_provider_cfg.get("model"),
    ) or ProviderProfileConfig.model
    supervisor_model = _env_or_value(
        "AUTORESEARCH_SUPERVISOR_MODEL",
        fallback=legacy_provider_cfg.get("supervisor_model"),
    ) or "gpt-5.4"

    profiles = {
        "openai-mini": ProviderProfileConfig(
            provider_type=provider_type,
            api_base=shared_api_base,
            command=defaults.get("command"),
            model=explorer_model,
            api_key=shared_api_key,
            api_key_env=(
                str(defaults["api_key_env"]).strip()
                if defaults.get("api_key_env")
                else None
            ),
            temperature=shared_temperature,
            max_tokens=shared_max_tokens,
            timeout_seconds=shared_timeout,
            transport=str(defaults["transport"]),
            compact_trigger_tokens=None,
            rate_limit_backoff_seconds=None,
            rate_limit_max_retries=None,
        ),
        "openai-supervisor": ProviderProfileConfig(
            provider_type=provider_type,
            api_base=shared_api_base,
            command=defaults.get("command"),
            model=supervisor_model,
            api_key=shared_api_key,
            api_key_env=(
                str(defaults["api_key_env"]).strip()
                if defaults.get("api_key_env")
                else None
            ),
            temperature=shared_temperature,
            max_tokens=shared_max_tokens,
            timeout_seconds=shared_timeout,
            transport=str(defaults["transport"]),
            compact_trigger_tokens=None,
            rate_limit_backoff_seconds=None,
            rate_limit_max_retries=None,
        ),
    }
    return LlmConfig(), profiles


def load_config(repo_root: Path | None = None) -> AppConfig:
    root = find_repo_root(repo_root)
    config_path = root / CONFIG_FILE_NAME
    secrets_path = root / SECRETS_FILE_NAME
    raw_config = load_json_file(config_path)
    raw_secrets = load_json_file(secrets_path)

    fuzzfolio_cfg = raw_config.get("fuzzfolio", {})
    fuzzfolio_secrets = raw_secrets.get("fuzzfolio", {})
    research_cfg = raw_config.get("research", {})
    supervisor_cfg = raw_config.get("supervisor", {})
    llm, providers = _load_provider_profiles(raw_config, raw_secrets)

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
        quality_score_preset=str(
            research_cfg.get("quality_score_preset", ResearchConfig.quality_score_preset)
        ).strip()
        or ResearchConfig.quality_score_preset,
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
        finish_min_attempts=int(
            research_cfg.get(
                "finish_min_attempts",
                ResearchConfig.finish_min_attempts,
            )
        ),
        run_wrap_up_steps=int(
            research_cfg.get(
                "run_wrap_up_steps",
                ResearchConfig.run_wrap_up_steps,
            )
        ),
        phase_early_ratio=float(
            research_cfg.get(
                "phase_early_ratio",
                ResearchConfig.phase_early_ratio,
            )
        ),
        phase_late_ratio=float(
            research_cfg.get(
                "phase_late_ratio",
                ResearchConfig.phase_late_ratio,
            )
        ),
        horizon_early_months=int(
            research_cfg.get(
                "horizon_early_months",
                ResearchConfig.horizon_early_months,
            )
        ),
        horizon_mid_months=int(
            research_cfg.get(
                "horizon_mid_months",
                ResearchConfig.horizon_mid_months,
            )
        ),
        horizon_late_months=int(
            research_cfg.get(
                "horizon_late_months",
                ResearchConfig.horizon_late_months,
            )
        ),
        horizon_wrap_up_months=int(
            research_cfg.get(
                "horizon_wrap_up_months",
                ResearchConfig.horizon_wrap_up_months,
            )
        ),
        coverage_reference_timeframe=str(
            research_cfg.get(
                "coverage_reference_timeframe",
                ResearchConfig.coverage_reference_timeframe,
            )
        ).strip().upper()
        or ResearchConfig.coverage_reference_timeframe,
        coverage_min_mid_months=int(
            research_cfg.get(
                "coverage_min_mid_months",
                ResearchConfig.coverage_min_mid_months,
            )
        ),
        coverage_min_wrap_up_months=int(
            research_cfg.get(
                "coverage_min_wrap_up_months",
                ResearchConfig.coverage_min_wrap_up_months,
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
        soft_wrap_minutes=int(
            supervisor_cfg.get(
                "soft_wrap_minutes",
                SupervisorConfig.soft_wrap_minutes,
            )
        ),
    )

    return AppConfig(
        repo_root=root,
        config_path=config_path,
        secrets_path=secrets_path,
        llm=llm,
        providers=providers,
        fuzzfolio=fuzzfolio,
        research=research,
        supervisor=supervisor,
    )
