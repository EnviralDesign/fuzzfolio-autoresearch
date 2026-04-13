from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
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
    repo_root: Path | None = None
    adapter_path: str | None = None
    quantization: str | None = None
    trust_remote_code: bool = False
    temperature: float = 0.2
    max_tokens: int = 3200
    timeout_seconds: int = 120
    transport: str = "chat_completions"
    compact_trigger_tokens: int | None = None
    history_strategy: str | None = None
    history_trim_keep_recent_steps: int | None = None
    history_trim_target_ratio: float | None = None
    rate_limit_backoff_seconds: list[int] | None = None
    rate_limit_max_retries: int | None = None


@dataclass
class LlmConfig:
    explorer_profile: str = "openai-mini"


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
    history_strategy: str = "chunked_tail"
    history_trim_keep_recent_steps: int = 10
    history_trim_target_ratio: float = 0.75
    recent_step_window_steps: int = 6
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
    validation_max_concurrency: int = 4
    retention_strong_candidate_threshold: float = 55.0
    # Minimum best_score for validated-leader overlay; use 0 to disable the floor.
    validated_leader_min_score: float = 45.0
    # Provisional/shadow leader selection uses this score floor (not the strong-candidate bar).
    provisional_leader_min_score: float = 0.0
    # Retention baseline (for long-horizon vs baseline checks) is only established once score reaches this.
    # When None, uses retention_strong_candidate_threshold.
    retention_baseline_establish_min_score: float | None = None
    retention_max_same_family_mutations_before_check: int = 2
    retention_fail_delta: float = -12.0
    retention_fail_ratio: float = 0.82
    retention_pass_delta: float = -6.0
    retention_check_months_sparse: int = 12
    retention_check_months_normal: int = 9
    timeframe_intent_enforcement: str = "warn_and_require_resolution"
    timeframe_adjustment_repeat_block: bool = True
    same_family_exploit_cap: int = 3
    sweep_oversized_warning: int = 64
    sweep_oversized_hard_block: int = 256
    validated_leader_min_horizon_months: int = 12
    bankruptcy_cooldown_steps: int = 4
    reseed_min_remaining_steps: int = 6
    reseed_max_recent_failures_window: int = 6
    reseed_after_stale_validation_steps: int = 10
    reseed_min_scored_attempts: int = 4
    unresolved_coverage_harden_after: int = 3
    collapse_recovery_max_steps: int = 5
    max_bankrupt_families_before_force_breadth: int = 2
    horizon_failure_counts_as_retention_fail: bool = True
    validated_portability_check_required: bool = False
    late_phase_new_family_budget: float = 0.25
    wrap_up_requires_validated_leader: bool = False
    effective_coverage_min_ratio: float = 0.88
    retention_digest_high_risk_fail_weight: float = 1.5


@dataclass
class SuperviseConfig:
    max_steps: int | None = None
    window_enabled: bool = True
    window_start: str | None = None
    window_end: str | None = None
    timezone: str = "America/Chicago"
    stop_mode: str = "after_step"
    soft_wrap_minutes: int = 30
    auto_restart_terminal_sessions: bool = False

@dataclass
class ManagerConfig:
    """Event-driven branch adjudicator (LLM). Does not execute research tools.

    When enabled with at least one profile, heuristic leader recomputation is not
    used; overlay leaders come from manager actions (and existing state).
    """

    enabled: bool = False
    profiles: list[str] = field(default_factory=list)
    max_candidate_families_in_packet: int = 8


@dataclass
class AppConfig:
    repo_root: Path
    config_path: Path
    secrets_path: Path
    llm: LlmConfig
    providers: dict[str, ProviderProfileConfig]
    fuzzfolio: FuzzfolioConfig
    research: ResearchConfig
    supervise: SuperviseConfig
    manager: ManagerConfig

    @property
    def provider(self) -> ProviderProfileConfig:
        return self.providers[self.llm.explorer_profile]

    def compact_trigger_tokens_for(self, profile_name: str) -> int:
        profile = self.providers[profile_name]
        if profile.compact_trigger_tokens is not None:
            return int(profile.compact_trigger_tokens)
        return int(self.research.compact_trigger_tokens)

    def history_strategy_for(self, profile_name: str) -> str:
        profile = self.providers[profile_name]
        candidate = str(
            profile.history_strategy or self.research.history_strategy
        ).strip().lower()
        if candidate not in {"chunked_tail", "llm_summary"}:
            return ResearchConfig.history_strategy
        return candidate

    def history_trim_keep_recent_steps_for(self, profile_name: str) -> int:
        profile = self.providers[profile_name]
        if profile.history_trim_keep_recent_steps is not None:
            return max(1, int(profile.history_trim_keep_recent_steps))
        return max(1, int(self.research.history_trim_keep_recent_steps))

    def history_trim_target_ratio_for(self, profile_name: str) -> float:
        profile = self.providers[profile_name]
        value = (
            float(profile.history_trim_target_ratio)
            if profile.history_trim_target_ratio is not None
            else float(self.research.history_trim_target_ratio)
        )
        if value <= 0 or value >= 1:
            return ResearchConfig.history_trim_target_ratio
        return value

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
    def model_leaderboard_plot_path(self) -> Path:
        return self.derived_root / "leaderboard-model-averages.png"

    @property
    def model_leaderboard_json_path(self) -> Path:
        return self.derived_root / "leaderboard-model-averages.json"

    @property
    def tradeoff_leaderboard_plot_path(self) -> Path:
        return self.derived_root / "leaderboard-score-vs-trades.png"

    @property
    def tradeoff_leaderboard_json_path(self) -> Path:
        return self.derived_root / "leaderboard-score-vs-trades.json"

    @property
    def validation_leaderboard_json_path(self) -> Path:
        return self.derived_root / "leaderboard-validation.json"

    @property
    def validation_scatter_plot_path(self) -> Path:
        return self.derived_root / "leaderboard-validation-12m-vs-36m.png"

    @property
    def validation_delta_plot_path(self) -> Path:
        return self.derived_root / "leaderboard-validation-delta.png"

    @property
    def similarity_leaderboard_json_path(self) -> Path:
        return self.derived_root / "leaderboard-similarity.json"

    @property
    def similarity_heatmap_plot_path(self) -> Path:
        return self.derived_root / "leaderboard-similarity-heatmap.png"

    @property
    def similarity_scatter_plot_path(self) -> Path:
        return self.derived_root / "leaderboard-score-vs-sameness.png"

    @property
    def validation_cache_root(self) -> Path:
        return self.derived_root / "validation-cache"

    @property
    def attempt_catalog_json_path(self) -> Path:
        return self.derived_root / "attempt-catalog.json"

    @property
    def attempt_catalog_csv_path(self) -> Path:
        return self.derived_root / "attempt-catalog.csv"

    @property
    def attempt_catalog_summary_path(self) -> Path:
        return self.derived_root / "attempt-catalog-summary.json"

    @property
    def attempt_catalog_manifest_path(self) -> Path:
        return self.derived_root / "attempt-catalog-manifest.json"

    @property
    def promotion_board_json_path(self) -> Path:
        return self.derived_root / "promotion-board.json"

    @property
    def promotion_board_csv_path(self) -> Path:
        return self.derived_root / "promotion-board.csv"

    @property
    def full_backtest_audit_json_path(self) -> Path:
        return self.derived_root / "full-backtest-audit.json"

    @property
    def full_backtest_failures_json_path(self) -> Path:
        return self.derived_root / "full-backtest-failures.json"

    @property
    def corpus_tradeoff_plot_path(self) -> Path:
        return self.derived_root / "corpus-score-vs-trades-36mo.png"

    @property
    def corpus_tradeoff_json_path(self) -> Path:
        return self.derived_root / "corpus-score-vs-trades-36mo.json"

    @property
    def program_path(self) -> Path:
        return self.repo_root / "program.md"


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for path in [current, *current.parents]:
        if (path / ".git").exists() and (path / "program.md").exists():
            return path
    raise FileNotFoundError(
        "Could not locate repo root containing .git and program.md."
    )


def load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _env_or_value(*keys: str | None, fallback: str | None = None) -> str | None:
    for key in keys:
        if not key or not isinstance(key, str):
            continue
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
    if normalized == "lmstudio":
        return {
            "api_base": "http://localhost:1234/v1",
            "command": None,
            "api_key_env": None,
            "timeout_seconds": 120,
            "transport": "chat_completions",
        }
    if normalized == "minimax":
        return {
            "api_base": "https://api.minimax.io/v1",
            "command": None,
            "api_key_env": "MINIMAX_API_KEY",
            "timeout_seconds": 180,
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
    if normalized == "transformers_local":
        return {
            "api_base": None,
            "command": None,
            "api_key_env": None,
            "timeout_seconds": 600,
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
    repo_root: Path,
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
                str(
                    secret_cfg.get("api_key_ref")
                    or profile_cfg.get("api_key_ref")
                    or ""
                ).strip()
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
            compact_trigger_value = _env_or_value(
                f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_COMPACT_TRIGGER_TOKENS",
                fallback=(
                    str(profile_cfg.get("compact_trigger_tokens"))
                    if profile_cfg.get("compact_trigger_tokens") is not None
                    else None
                ),
            )
            history_strategy_value = _env_or_value(
                f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_HISTORY_STRATEGY",
                fallback=(
                    str(profile_cfg.get("history_strategy")).strip()
                    if profile_cfg.get("history_strategy") is not None
                    else None
                ),
            )
            history_trim_keep_recent_steps_value = _env_or_value(
                f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_HISTORY_TRIM_KEEP_RECENT_STEPS",
                fallback=(
                    str(profile_cfg.get("history_trim_keep_recent_steps"))
                    if profile_cfg.get("history_trim_keep_recent_steps") is not None
                    else None
                ),
            )
            history_trim_target_ratio_value = _env_or_value(
                f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_HISTORY_TRIM_TARGET_RATIO",
                fallback=(
                    str(profile_cfg.get("history_trim_target_ratio"))
                    if profile_cfg.get("history_trim_target_ratio") is not None
                    else None
                ),
            )
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
                    fallback=secret_cfg.get("api_key")
                    or profile_cfg.get("api_key")
                    or referenced_api_key,
                ),
                api_key_ref=api_key_ref,
                api_key_env=api_key_env,
                repo_root=repo_root,
                adapter_path=(
                    _env_or_value(
                        f"AUTORESEARCH_PROVIDER_{profile_name.upper().replace('-', '_')}_ADAPTER_PATH",
                        fallback=profile_cfg.get("adapter_path"),
                    )
                    or None
                ),
                quantization=(
                    str(profile_cfg.get("quantization") or "").strip() or None
                ),
                trust_remote_code=bool(profile_cfg.get("trust_remote_code", False)),
                temperature=float(
                    profile_cfg.get("temperature", ProviderProfileConfig.temperature)
                ),
                max_tokens=int(
                    profile_cfg.get("max_tokens", ProviderProfileConfig.max_tokens)
                ),
                timeout_seconds=int(
                    profile_cfg.get("timeout_seconds", defaults["timeout_seconds"])
                ),
                transport=str(profile_cfg.get("transport") or defaults["transport"]),
                compact_trigger_tokens=(
                    int(compact_trigger_value)
                    if compact_trigger_value is not None
                    else None
                ),
                history_strategy=(
                    str(history_strategy_value).strip().lower()
                    if history_strategy_value is not None
                    else None
                ),
                history_trim_keep_recent_steps=(
                    int(history_trim_keep_recent_steps_value)
                    if history_trim_keep_recent_steps_value is not None
                    else None
                ),
                history_trim_target_ratio=(
                    float(history_trim_target_ratio_value)
                    if history_trim_target_ratio_value is not None
                    else None
                ),
                rate_limit_backoff_seconds=(
                    [
                        int(item)
                        for item in profile_cfg.get("rate_limit_backoff_seconds", [])
                    ]
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
        if explorer_profile not in profiles:
            raise KeyError(
                f"Configured explorer_profile {explorer_profile!r} is not defined in providers."
            )
        return LlmConfig(explorer_profile=explorer_profile), profiles

    legacy_provider_cfg = raw_config.get("provider", {})
    legacy_provider_secrets = raw_secrets.get("provider", {})
    provider_type = str(legacy_provider_cfg.get("type") or "openai")
    defaults = _provider_defaults(provider_type)
    shared_api_key = _env_or_value(
        "AUTORESEARCH_PROVIDER_API_KEY",
        defaults["api_key_env"],
        fallback=legacy_provider_secrets.get("api_key")
        or legacy_provider_cfg.get("api_key"),
    )
    shared_api_base = _env_or_value(
        "AUTORESEARCH_PROVIDER_BASE_URL",
        fallback=legacy_provider_cfg.get("api_base") or defaults["api_base"],
    )
    shared_temperature = float(
        legacy_provider_cfg.get("temperature", ProviderProfileConfig.temperature)
    )
    shared_max_tokens = int(
        legacy_provider_cfg.get("max_tokens", ProviderProfileConfig.max_tokens)
    )
    shared_timeout = int(
        legacy_provider_cfg.get("timeout_seconds", defaults["timeout_seconds"])
    )
    explorer_model = (
        _env_or_value(
            "AUTORESEARCH_PROVIDER_MODEL",
            fallback=legacy_provider_cfg.get("model"),
        )
        or ProviderProfileConfig.model
    )
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
            repo_root=repo_root,
            adapter_path=None,
            quantization=None,
            trust_remote_code=False,
            temperature=shared_temperature,
            max_tokens=shared_max_tokens,
            timeout_seconds=shared_timeout,
            transport=str(defaults["transport"]),
            compact_trigger_tokens=None,
            history_strategy=None,
            history_trim_keep_recent_steps=None,
            history_trim_target_ratio=None,
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
    supervise_cfg = raw_config.get("supervise", raw_config.get("supervisor", {}))
    manager_cfg = raw_config.get("manager", {})
    llm, providers = _load_provider_profiles(root, raw_config, raw_secrets)

    fuzzfolio = FuzzfolioConfig(
        cli_command=_env_or_value(
            "AUTORESEARCH_FUZZFOLIO_CLI", fallback=fuzzfolio_cfg.get("cli_command")
        )
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
        email=_env_or_value(
            "AUTORESEARCH_FUZZFOLIO_EMAIL", fallback=fuzzfolio_secrets.get("email")
        ),
        password=_env_or_value(
            "AUTORESEARCH_FUZZFOLIO_PASSWORD",
            fallback=fuzzfolio_secrets.get("password"),
        ),
    )

    research = ResearchConfig(
        max_steps=int(research_cfg.get("max_steps", ResearchConfig.max_steps)),
        recent_attempts_window=int(
            research_cfg.get(
                "recent_attempts_window", ResearchConfig.recent_attempts_window
            )
        ),
        label_prefix=research_cfg.get("label_prefix", ResearchConfig.label_prefix),
        auto_seed_prompt=bool(
            research_cfg.get("auto_seed_prompt", ResearchConfig.auto_seed_prompt)
        ),
        quality_score_preset=str(
            research_cfg.get(
                "quality_score_preset", ResearchConfig.quality_score_preset
            )
        ).strip()
        or ResearchConfig.quality_score_preset,
        plot_lower_is_better=bool(
            research_cfg.get(
                "plot_lower_is_better", ResearchConfig.plot_lower_is_better
            )
        ),
        compact_trigger_tokens=int(
            research_cfg.get(
                "compact_trigger_tokens", ResearchConfig.compact_trigger_tokens
            )
        ),
        compact_keep_recent_messages=int(
            research_cfg.get(
                "compact_keep_recent_messages",
                ResearchConfig.compact_keep_recent_messages,
            )
        ),
        history_strategy=str(
            research_cfg.get("history_strategy", ResearchConfig.history_strategy)
        ).strip()
        or ResearchConfig.history_strategy,
        history_trim_keep_recent_steps=int(
            research_cfg.get(
                "history_trim_keep_recent_steps",
                ResearchConfig.history_trim_keep_recent_steps,
            )
        ),
        history_trim_target_ratio=float(
            research_cfg.get(
                "history_trim_target_ratio",
                ResearchConfig.history_trim_target_ratio,
            )
        ),
        recent_step_window_steps=int(
            research_cfg.get(
                "recent_step_window_steps",
                research_cfg.get(
                    "supervisor_recent_steps",
                    ResearchConfig.recent_step_window_steps,
                ),
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
        )
        .strip()
        .upper()
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
        validation_max_concurrency=max(
            1,
            int(
                research_cfg.get(
                    "validation_max_concurrency",
                    ResearchConfig.validation_max_concurrency,
                )
            ),
        ),
        retention_strong_candidate_threshold=float(
            research_cfg.get(
                "retention_strong_candidate_threshold",
                ResearchConfig.retention_strong_candidate_threshold,
            )
        ),
        validated_leader_min_score=(
            float(research_cfg["validated_leader_min_score"])
            if research_cfg.get("validated_leader_min_score") is not None
            else ResearchConfig.validated_leader_min_score
        ),
        retention_baseline_establish_min_score=(
            float(research_cfg["retention_baseline_establish_min_score"])
            if research_cfg.get("retention_baseline_establish_min_score")
            is not None
            else ResearchConfig.retention_baseline_establish_min_score
        ),
        provisional_leader_min_score=float(
            research_cfg.get(
                "provisional_leader_min_score",
                ResearchConfig.provisional_leader_min_score,
            )
        ),
        retention_max_same_family_mutations_before_check=int(
            research_cfg.get(
                "retention_max_same_family_mutations_before_check",
                ResearchConfig.retention_max_same_family_mutations_before_check,
            )
        ),
        retention_fail_delta=float(
            research_cfg.get(
                "retention_fail_delta",
                ResearchConfig.retention_fail_delta,
            )
        ),
        retention_fail_ratio=float(
            research_cfg.get(
                "retention_fail_ratio",
                ResearchConfig.retention_fail_ratio,
            )
        ),
        retention_pass_delta=float(
            research_cfg.get(
                "retention_pass_delta",
                ResearchConfig.retention_pass_delta,
            )
        ),
        retention_check_months_sparse=int(
            research_cfg.get(
                "retention_check_months_sparse",
                ResearchConfig.retention_check_months_sparse,
            )
        ),
        retention_check_months_normal=int(
            research_cfg.get(
                "retention_check_months_normal",
                ResearchConfig.retention_check_months_normal,
            )
        ),
        timeframe_intent_enforcement=str(
            research_cfg.get(
                "timeframe_intent_enforcement",
                ResearchConfig.timeframe_intent_enforcement,
            )
        ).strip(),
        timeframe_adjustment_repeat_block=bool(
            research_cfg.get(
                "timeframe_adjustment_repeat_block",
                ResearchConfig.timeframe_adjustment_repeat_block,
            )
        ),
        same_family_exploit_cap=int(
            research_cfg.get(
                "same_family_exploit_cap",
                ResearchConfig.same_family_exploit_cap,
            )
        ),
        sweep_oversized_warning=int(
            research_cfg.get(
                "sweep_oversized_warning",
                ResearchConfig.sweep_oversized_warning,
            )
        ),
        sweep_oversized_hard_block=int(
            research_cfg.get(
                "sweep_oversized_hard_block",
                ResearchConfig.sweep_oversized_hard_block,
            )
        ),
        validated_leader_min_horizon_months=int(
            research_cfg.get(
                "validated_leader_min_horizon_months",
                ResearchConfig.validated_leader_min_horizon_months,
            )
        ),
        bankruptcy_cooldown_steps=max(
            0,
            int(
                research_cfg.get(
                    "bankruptcy_cooldown_steps",
                    ResearchConfig.bankruptcy_cooldown_steps,
                )
            ),
        ),
        reseed_min_remaining_steps=max(
            0,
            int(
                research_cfg.get(
                    "reseed_min_remaining_steps",
                    ResearchConfig.reseed_min_remaining_steps,
                )
            ),
        ),
        reseed_max_recent_failures_window=max(
            1,
            int(
                research_cfg.get(
                    "reseed_max_recent_failures_window",
                    ResearchConfig.reseed_max_recent_failures_window,
                )
            ),
        ),
        reseed_after_stale_validation_steps=max(
            0,
            int(
                research_cfg.get(
                    "reseed_after_stale_validation_steps",
                    ResearchConfig.reseed_after_stale_validation_steps,
                )
            ),
        ),
        reseed_min_scored_attempts=max(
            0,
            int(
                research_cfg.get(
                    "reseed_min_scored_attempts",
                    ResearchConfig.reseed_min_scored_attempts,
                )
            ),
        ),
        unresolved_coverage_harden_after=max(
            1,
            int(
                research_cfg.get(
                    "unresolved_coverage_harden_after",
                    ResearchConfig.unresolved_coverage_harden_after,
                )
            ),
        ),
        collapse_recovery_max_steps=max(
            0,
            int(
                research_cfg.get(
                    "collapse_recovery_max_steps",
                    ResearchConfig.collapse_recovery_max_steps,
                )
            ),
        ),
        max_bankrupt_families_before_force_breadth=max(
            1,
            int(
                research_cfg.get(
                    "max_bankrupt_families_before_force_breadth",
                    ResearchConfig.max_bankrupt_families_before_force_breadth,
                )
            ),
        ),
        horizon_failure_counts_as_retention_fail=bool(
            research_cfg.get(
                "horizon_failure_counts_as_retention_fail",
                ResearchConfig.horizon_failure_counts_as_retention_fail,
            )
        ),
        validated_portability_check_required=bool(
            research_cfg.get(
                "validated_portability_check_required",
                ResearchConfig.validated_portability_check_required,
            )
        ),
        late_phase_new_family_budget=float(
            research_cfg.get(
                "late_phase_new_family_budget",
                ResearchConfig.late_phase_new_family_budget,
            )
        ),
        wrap_up_requires_validated_leader=bool(
            research_cfg.get(
                "wrap_up_requires_validated_leader",
                ResearchConfig.wrap_up_requires_validated_leader,
            )
        ),
        effective_coverage_min_ratio=float(
            research_cfg.get(
                "effective_coverage_min_ratio",
                ResearchConfig.effective_coverage_min_ratio,
            )
        ),
        retention_digest_high_risk_fail_weight=float(
            research_cfg.get(
                "retention_digest_high_risk_fail_weight",
                ResearchConfig.retention_digest_high_risk_fail_weight,
            )
        ),
    )
    supervise = SuperviseConfig(
        max_steps=(
            int(supervise_cfg["max_steps"])
            if supervise_cfg.get("max_steps") is not None
            else None
        ),
        window_enabled=bool(
            supervise_cfg.get("window_enabled", SuperviseConfig.window_enabled)
        ),
        window_start=supervise_cfg.get("window_start"),
        window_end=supervise_cfg.get("window_end"),
        timezone=supervise_cfg.get("timezone", SuperviseConfig.timezone),
        stop_mode=supervise_cfg.get("stop_mode", SuperviseConfig.stop_mode),
        soft_wrap_minutes=int(
            supervise_cfg.get(
                "soft_wrap_minutes",
                SuperviseConfig.soft_wrap_minutes,
            )
        ),
        auto_restart_terminal_sessions=bool(
            supervise_cfg.get(
                "auto_restart_terminal_sessions",
                SuperviseConfig.auto_restart_terminal_sessions,
            )
        ),
    )

    manager_profiles = (
        [
            str(item).strip()
            for item in manager_cfg.get("profiles", [])
            if str(item).strip()
        ]
        if isinstance(manager_cfg.get("profiles", []), list)
        else []
    )
    unknown_manager_profiles = [
        item for item in manager_profiles if item not in providers
    ]
    if unknown_manager_profiles:
        raise KeyError(
            "Configured manager profile(s) are not defined in providers: "
            + ", ".join(sorted(unknown_manager_profiles))
        )
    manager = ManagerConfig(
        enabled=bool(manager_cfg.get("enabled", ManagerConfig.enabled)),
        profiles=manager_profiles,
        max_candidate_families_in_packet=max(
            2,
            int(
                manager_cfg.get(
                    "max_candidate_families_in_packet",
                    ManagerConfig.max_candidate_families_in_packet,
                )
            ),
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
        supervise=supervise,
        manager=manager,
    )
