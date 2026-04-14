import json
from pathlib import Path
from types import SimpleNamespace

from autoresearch import __main__ as ar_main
from autoresearch import portfolio as pf


def test_load_portfolio_spec_defaults_when_file_missing(tmp_path: Path) -> None:
    spec, defaulted = pf.load_portfolio_spec(tmp_path / "missing.json")

    assert defaulted is True
    assert spec["portfolio_name"] == "default-portfolio"
    assert spec["full_backtest_job_timeout_seconds"] == 2400
    assert [sleeve["name"] for sleeve in spec["sleeves"]] == [
        "quality",
        "cadence",
        "breadth",
    ]


def test_build_sleeve_selection_annotates_selected_rows() -> None:
    rows = [
        {
            "attempt_id": "A",
            "run_id": "run-a",
            "score_36m": 80.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "B",
            "run_id": "run-b",
            "score_36m": 70.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    sleeve = pf.build_sleeve_selection(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "quality",
            "shortlist_size": 1,
        },
    )

    assert sleeve["name"] == "quality"
    assert len(sleeve["selected_rows"]) == 1
    assert sleeve["selected_rows"][0]["sleeve_name"] == "quality"
    assert sleeve["selected_rows"][0]["sleeve_selection_rank"] == 1


def test_resolve_prefilter_limit_uses_legacy_candidate_limit_when_present() -> None:
    assert (
        pf.resolve_prefilter_limit(
            {
                **pf.DEFAULT_SLEEVE_SPEC,
                "prefilter_limit": -1,
                "candidate_limit": 77,
            }
        )
        == 77
    )


def test_build_sleeve_prefilter_uses_sleeve_local_scalar_utility() -> None:
    rows = [
        {
            "attempt_id": "slow-high-score",
            "run_id": "run-a",
            "score_36m": 80.0,
            "trades_per_month_36m": 0.2,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "fast-lower-score",
            "run_id": "run-b",
            "score_36m": 78.0,
            "trades_per_month_36m": 6.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    result = pf.build_sleeve_prefilter(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "cadence",
            "prefilter_limit": 1,
            "trade_rate_bonus_weight": 3.0,
            "trade_rate_bonus_target": 6.0,
        },
    )

    assert result["qualified_rows"][0]["attempt_id"] == "slow-high-score"
    assert [row["attempt_id"] for row in result["candidate_rows"]] == ["fast-lower-score"]
    assert result["prefilter_limit"] == 1
    assert result["prefilter_excluded_count"] == 1


def test_build_sleeve_prefilter_can_reward_breadth_metrics() -> None:
    rows = [
        {
            "attempt_id": "narrow-higher-score",
            "run_id": "run-a",
            "score_36m": 80.0,
            "durability_score_36m": 0.1,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "broad-lower-score",
            "run_id": "run-b",
            "score_36m": 76.0,
            "durability_score_36m": 1.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    result = pf.build_sleeve_prefilter(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "breadth",
            "prefilter_limit": 1,
            "scalar_metric_terms": [
                {
                    "name": "breadth_score",
                    "field_candidates": ["breadth_score_36m", "durability_score_36m"],
                    "direction": "higher",
                    "target": 1.0,
                    "weight": 6.0,
                }
            ],
        },
    )

    assert [row["attempt_id"] for row in result["candidate_rows"]] == ["broad-lower-score"]
    assert float(result["candidate_rows"][0]["prefilter_scalar_metric_bonus_component"]) == 6.0
    assert result["candidate_rows"][0]["prefilter_scalar_metric_bonus_terms"][0]["field"] == (
        "durability_score_36m"
    )


def test_build_sleeve_selection_reports_prefilter_counts() -> None:
    rows = [
        {
            "attempt_id": "A",
            "run_id": "run-a",
            "score_36m": 80.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "B",
            "run_id": "run-b",
            "score_36m": 79.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "C",
            "run_id": "run-c",
            "score_36m": 78.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    sleeve = pf.build_sleeve_selection(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "quality",
            "prefilter_limit": 2,
            "shortlist_size": 1,
        },
    )

    assert len(sleeve["qualified_rows"]) == 3
    assert len(sleeve["candidate_rows"]) == 2
    assert sleeve["prefilter_excluded_count"] == 1
    assert sleeve["prefilter_limit"] == 2


def test_materialized_corpus_index_requires_fresh_manifest(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "run-a"
    run_dir.mkdir(parents=True, exist_ok=True)
    attempts_path = run_dir / "attempts.jsonl"
    attempts_path.write_text("{}", encoding="utf-8")
    (run_dir / "run-metadata.json").write_text("{}", encoding="utf-8")

    derived_root = runs_root / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    rows = [
        {"attempt_id": "B", "composite_score": 1.0},
        {"attempt_id": "A", "composite_score": 2.0},
    ]
    (derived_root / "attempt-catalog.json").write_text(
        json.dumps(rows, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    (derived_root / "attempt-catalog-manifest.json").write_text(
        json.dumps(
            ar_main._attempt_catalog_manifest_payload([run_dir], rows),
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    config = SimpleNamespace(
        attempt_catalog_json_path=derived_root / "attempt-catalog.json",
        attempt_catalog_manifest_path=derived_root / "attempt-catalog-manifest.json",
    )

    loaded_rows = ar_main._load_materialized_corpus_index_rows(config, run_dirs=[run_dir])
    assert [row["attempt_id"] for row in loaded_rows] == ["A", "B"]

    attempts_path.write_text("{}\n{}", encoding="utf-8")
    assert ar_main._load_materialized_corpus_index_rows(config, run_dirs=[run_dir]) is None


def test_merge_portfolio_sleeves_unions_and_labels_overlap() -> None:
    merged = pf.merge_portfolio_sleeves(
        [
            {
                "name": "quality",
                "selected_rows": [
                    {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0},
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                ],
                "candidate_rows": [
                    {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0},
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                ],
            },
            {
                "name": "cadence",
                "selected_rows": [
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                    {"attempt_id": "C", "run_id": "run-c", "score_36m": 69.0},
                ],
                "candidate_rows": [
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                    {"attempt_id": "C", "run_id": "run-c", "score_36m": 69.0},
                ],
            },
        ]
    )

    selected_rows = merged["selected_rows"]
    assert [row["attempt_id"] for row in selected_rows] == ["B", "A", "C"]
    assert selected_rows[0]["selected_by_sleeves"] == ["quality", "cadence"]
    assert selected_rows[0]["selected_by_sleeves_count"] == 2
    assert merged["selected_union_count"] == 3
    assert merged["selected_overlap_count"] == 1


def test_build_selection_basket_summary_uses_curve_terminal_values(tmp_path: Path) -> None:
    curve_path = tmp_path / "curve.json"
    curve_path.write_text(
        """
        {
          "curve": {
            "points": [
              {"date": "2026-01-01", "realized_r": 4.0},
              {"date": "2026-02-01", "realized_r": 9.0}
            ]
          }
        }
        """.strip(),
        encoding="utf-8",
    )

    summary = ar_main._build_selection_basket_summary(
        [
            {
                "trades_per_month_36m": 2.5,
                "score_36m": 80.0,
                "max_drawdown_r_36m": 6.0,
                "effective_window_months_36m": 3.0,
                "full_backtest_curve_path_36m": str(curve_path),
            }
        ]
    )

    assert summary["strategy_count"] == 1
    assert summary["trades_per_month"]["sum"] == 2.5
    assert summary["realized_r_total_36m"]["sum"] == 9.0
    assert summary["realized_r_per_month_36m"]["mean"] == 3.0
    assert summary["max_drawdown_r_per_month_36m"]["mean"] == 2.0


def test_profile_drop_attempt_token_prefers_row_identity() -> None:
    token = ar_main._profile_drop_attempt_token(
        {
            "selection_rank": 1,
            "attempt_id": "RUN-attempt-00051",
            "candidate_name": "winner",
        },
        {
            "attempt_id": "RUN-attempt-00046",
            "candidate_name": "stale",
        },
    )

    assert token == "1-run-attempt-00051"


def test_nuke_deep_cache_artifacts_removes_rebuildable_outputs_only(
    tmp_path: Path,
) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    artifact_dir = runs_root / "run-a" / "evals" / "candidate-a"
    scrutiny_dir = artifact_dir / "scrutiny-cache" / "36mo"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    scrutiny_dir.mkdir(parents=True, exist_ok=True)
    derived_root.mkdir(parents=True, exist_ok=True)

    full_curve = artifact_dir / "full-backtest-36mo-curve.json"
    full_result = artifact_dir / "full-backtest-36mo-result.json"
    profile_drop_png = runs_root / "run-a" / "profile-drop-36mo.png"
    profile_drop_manifest = runs_root / "run-a" / "profile-drop-36mo.manifest.json"
    source_snapshot = artifact_dir / "sensitivity-response.json"
    derived_payload = derived_root / "portfolio-report.json"

    full_curve.write_text("{}", encoding="utf-8")
    full_result.write_text("{}", encoding="utf-8")
    (scrutiny_dir / "manifest.json").write_text("{}", encoding="utf-8")
    profile_drop_png.write_text("png", encoding="utf-8")
    profile_drop_manifest.write_text("{}", encoding="utf-8")
    source_snapshot.write_text('{"keep": true}', encoding="utf-8")
    derived_payload.write_text("{}", encoding="utf-8")

    summary = ar_main._nuke_deep_cache_artifacts(
        runs_root=runs_root,
        derived_root=derived_root,
        summary_timestamp="20260406T170000",
    )

    assert summary["deleted_full_backtest_curve_files"] == 1
    assert summary["deleted_full_backtest_result_files"] == 1
    assert summary["deleted_scrutiny_cache_dirs"] == 1
    assert summary["deleted_run_profile_drop_pngs"] == 1
    assert summary["deleted_run_profile_drop_manifests"] == 1
    assert summary["derived_entries_before_reset"] == 1
    assert full_curve.exists() is False
    assert full_result.exists() is False
    assert scrutiny_dir.parent.exists() is False
    assert profile_drop_png.exists() is False
    assert profile_drop_manifest.exists() is False
    assert source_snapshot.exists() is True
    assert derived_root.exists() is True
    assert (derived_root / "portfolio-report.json").exists() is False
    assert (derived_root / "deep-cache-reset-20260406T170000.json").exists() is True


def test_export_portfolio_bundle_uses_human_strategy_folders(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    report_root = derived_root / "portfolio-report" / "default-portfolio"
    report_root.mkdir(parents=True, exist_ok=True)

    profile_path = runs_root / "run-a" / "profiles" / "alpha.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text('{"name":"alpha"}', encoding="utf-8")

    source_drop_root = report_root / "profile-drops" / "1-attempt-a-alpha"
    source_drop_root.mkdir(parents=True, exist_ok=True)
    source_drop_png = source_drop_root / "profile-drop-36mo.png"
    source_drop_manifest = source_drop_root / "profile-drop-36mo.manifest.json"
    source_drop_png.write_text("png", encoding="utf-8")
    source_drop_manifest.write_text("{}", encoding="utf-8")

    report_path = report_root / "portfolio-report.json"
    report_path.write_text("{}", encoding="utf-8")

    payload = {
        "portfolio_name": "default-portfolio",
        "selected": [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "profile_ref": "abc123",
            }
        ],
        "profile_drops": [
            {
                "attempt_id": "attempt-a",
                "profile_ref": "abc123",
                "png_path": str(source_drop_png),
                "manifest_path": str(source_drop_manifest),
            }
        ],
    }

    config = SimpleNamespace(derived_root=derived_root, runs_root=runs_root)
    summary = ar_main._export_portfolio_bundle(
        config=config,
        payload=payload,
        report_root=report_root,
        report_path=report_path,
    )

    bundle_root = Path(summary["bundle_root"])
    item_root = bundle_root / "alpha"

    assert item_root.exists() is True
    assert (item_root / "alpha.json").exists() is True
    assert (item_root / "alpha.png").exists() is True
    assert (item_root / "profile-drop-36mo.manifest.json").exists() is False
    assert (bundle_root / "portfolio-report.json").exists() is False
    assert (bundle_root / "portfolio-report.csv").exists() is False
    assert (bundle_root / "selected-attempts.csv").exists() is False
    assert (bundle_root / "bundle-manifest.json").exists() is False
    assert summary["exported_profiles"] == 1
    assert summary["exported_drop_pngs"] == 1


def test_render_portfolio_profile_drops_updates_existing_report(
    tmp_path: Path, monkeypatch
) -> None:
    derived_root = tmp_path / "runs" / "derived"
    report_root = derived_root / "portfolio-report" / "default-portfolio"
    report_root.mkdir(parents=True, exist_ok=True)
    report_path = report_root / "portfolio-report.json"
    payload = {
        "portfolio_name": "default-portfolio",
        "portfolio_spec": {
            "generate_profile_drops": False,
            "profile_drop_lookback_months": 36,
            "profile_drop_timeout_seconds": 1800,
            "profile_drop_workers": 3,
        },
        "profile_drop_phase": "skipped",
        "profile_drops": [],
        "selected": [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
            }
        ],
    }
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    config = SimpleNamespace(
        derived_root=derived_root,
        repo_root=tmp_path,
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    seen: dict[str, object] = {}

    def fake_render_profile_drop_rows(**kwargs):
        seen.update(kwargs)
        return [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "status": "rendered",
                "png_path": str(report_root / "profile-drops" / "alpha" / "profile-drop-36mo.png"),
                "manifest_path": str(
                    report_root / "profile-drops" / "alpha" / "profile-drop-36mo.manifest.json"
                ),
            }
        ]

    monkeypatch.setattr(ar_main, "_render_profile_drop_rows", fake_render_profile_drop_rows)

    exit_code = ar_main.cmd_render_portfolio_profile_drops(
        portfolio_report=str(report_path),
        profile_drop_workers=None,
        force_rebuild=False,
        as_json=True,
    )

    assert exit_code == 0
    assert seen["rows"] == payload["selected"]
    assert seen["output_root"] == report_root / "profile-drops"
    assert seen["lookback_months"] == 36
    assert seen["timeout_seconds"] == 1800
    assert seen["profile_drop_workers"] == 3
    assert seen["force_rebuild"] is False
    assert seen["progress_label"] == "portfolio profile drops"

    updated_payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert updated_payload["profile_drop_phase"] == "complete"
    assert len(updated_payload["profile_drops"]) == 1
    assert updated_payload["profile_drops"][0]["status"] == "rendered"
    assert "generated_at" in updated_payload


def test_public_portfolio_row_strips_path_fields() -> None:
    public = ar_main._public_portfolio_row(
        {
            "attempt_id": "attempt-a",
            "candidate_name": "alpha",
            "profile_ref": "ref-a",
            "profile_path": r"C:\runs\run-a\profiles\alpha.json",
            "destination_path": r"C:\runs\run-a\profiles\beta.json",
            "source_profile_path": r"C:\runs\run-a\profiles\alpha.json",
        }
    )

    assert public == {
        "attempt_id": "attempt-a",
        "candidate_name": "alpha",
        "profile_ref": "ref-a",
    }


def test_human_bundle_item_token_avoids_collisions() -> None:
    used: set[str] = set()

    first = ar_main._human_bundle_item_token("alpha", "attempt-a", used)
    second = ar_main._human_bundle_item_token("alpha", "attempt-b", used)

    assert first == "alpha"
    assert second == "alpha-2"


def test_profile_drop_description_fallback_detects_generic_text() -> None:
    assert (
        ar_main._is_generic_profile_description(
            "Portable scoring profile scaffolded from live indicator templates."
        )
        is True
    )
    assert ar_main._is_generic_profile_description("") is True
    assert ar_main._is_generic_profile_description("Custom mean reversion profile.") is False


def test_build_profile_drop_description_uses_profile_structure() -> None:
    payload = {
        "profile": {
            "description": "Portable scoring profile scaffolded from live indicator templates.",
            "directionMode": "both",
            "indicators": [
                {
                    "config": {
                        "isActive": True,
                        "isTrendFollowing": True,
                        "label": "RSI",
                        "timeframe": "M5",
                    },
                    "meta": {"id": "RSI"},
                },
                {
                    "config": {
                        "isActive": True,
                        "isTrendFollowing": False,
                        "label": "WILLR",
                        "timeframe": "H1",
                    },
                    "meta": {"id": "WILLR"},
                },
            ],
        }
    }

    description = ar_main._build_profile_drop_description(
        payload,
        package_inputs={"instruments": ["EURUSD", "GBPUSD"], "timeframe": "M5"},
        row={},
        attempt={},
    )

    assert "Both-direction basket profile across EURUSD and GBPUSD on M5" in description
    assert "RSI and WILLR" in description
    assert "M5 and H1 timeframes" in description
    assert "1 trend-following and 1 mean-reversion" in description


def test_apply_profile_drop_description_fallback_rewrites_bundle_profile(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    profile_document_path = bundle_dir / "profile-document.json"
    profile_document_path.write_text(
        """
        {
          "format": "fuzzfolio.scoring-profile",
          "formatVersion": 1,
          "profile": {
            "description": "Portable scoring profile scaffolded from live indicator templates.",
            "directionMode": "both",
            "indicators": [
              {
                "config": {
                  "isActive": true,
                  "isTrendFollowing": true,
                  "label": "ADX",
                  "timeframe": "M15"
                },
                "meta": {"id": "ADX"}
              }
            ]
          }
        }
        """.strip(),
        encoding="utf-8",
    )

    applied = ar_main._apply_profile_drop_description_fallback(
        bundle_dir,
        package_inputs={"instruments": ["EURUSD"], "timeframe": "M15"},
        row={},
        attempt={},
    )
    updated = ar_main.load_json_if_exists(profile_document_path)

    assert applied is not None
    assert "EURUSD profile on M15 using ADX" in applied
    assert updated["profile"]["description"] == applied


def test_render_profile_drop_for_attempt_generates_reuses_and_regenerates_metadata(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    run_dir.mkdir(parents=True, exist_ok=True)
    output_root = tmp_path / "profile-drops"
    working_dir = tmp_path
    writer_calls = {"count": 0}
    renderer_calls: list[list[str]] = []
    workspace_root = tmp_path / "Trading-Dashboard"
    renderer_source_root = workspace_root / "scripts" / "profile-drop-renderer"
    renderer_source_root.mkdir(parents=True, exist_ok=True)
    (renderer_source_root / "index.html").write_text("<html></html>", encoding="utf-8")

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            output_root_arg = Path(args[args.index("--output-root") + 1])
            bundle_dir = output_root_arg / "bundle-0001"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "profile-document.json").write_text(
                json.dumps(
                    {
                        "profile": {
                            "name": "cand7",
                            "description": "Portable scoring profile scaffolded from live indicator templates.",
                            "directionMode": "both",
                            "indicators": [
                                {
                                    "meta": {"id": "BBANDS"},
                                    "config": {
                                        "label": "Bollinger Bands",
                                        "isActive": True,
                                        "isTrendFollowing": False,
                                        "timeframe": "M15",
                                    },
                                },
                                {
                                    "meta": {"id": "ATR"},
                                    "config": {
                                        "label": "ATR",
                                        "isActive": True,
                                        "isTrendFollowing": False,
                                        "timeframe": "M15",
                                    },
                                },
                            ],
                        }
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    class FakeProvider:
        def complete_json(self, messages):
            writer_calls["count"] += 1
            return {
                "display_name": "Band Snapback Filter",
                "tagline": "Fade stretched band breaks after ATR expansion.",
                "short_description": "Targets Bollinger overshoots only when ATR says the move was forceful enough to mean revert.",
                "long_description": (
                    "This profile fades Bollinger overshoots after ATR expands, so it targets forceful "
                    "pushes that look exhausted and avoids quiet drift that lacks enough pressure to snap back."
                ),
            }

    def fake_run_external(argv, cwd=None, timeout_seconds=None):
        renderer_calls.append(list(argv))
        png_path = Path(argv[argv.index("--out") + 1])
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_text("png", encoding="utf-8")

    config = SimpleNamespace(
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            presentation_metadata_provider_profile="writer",
        ),
        fuzzfolio=SimpleNamespace(workspace_root=workspace_root),
        providers={"writer": SimpleNamespace(type="codex")},
    )
    row = {
        "attempt_id": "attempt-0001",
        "run_id": "run-a",
        "candidate_name": "cand7",
        "profile_ref": "prof-7",
        "composite_score": 67.4,
        "effective_window_months": 24.0,
    }
    attempt = {
        "attempt_id": "attempt-0001",
        "candidate_name": "cand7",
        "profile_ref": "prof-7",
    }

    monkeypatch.setattr(
        ar_main,
        "_build_package_inputs",
        lambda *_args, **_kwargs: {
            "profile_ref": "prof-7",
            "profile_path": tmp_path / "profiles" / "cand7.json",
            "timeframe": "M15",
            "instruments": ["EURUSD"],
            "lookback_months": 36,
        },
    )
    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(ar_main, "create_provider", lambda _profile: FakeProvider())
    monkeypatch.setattr(ar_main, "_run_external", fake_run_external)

    result_first = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    manifest_path = Path(result_first["manifest_path"])
    metadata_path = Path(result_first["presentation_metadata_path"])
    manifest_payload = ar_main.load_json_if_exists(manifest_path)
    bundle_dir = output_root / ar_main._profile_drop_attempt_token(row, attempt) / "bundle" / "bundle-0001"
    bundle_payload = ar_main.load_json_if_exists(bundle_dir / "profile-document.json")

    assert result_first["status"] == "rendered"
    assert writer_calls["count"] == 1
    assert "--renderer-root" in renderer_calls[0]
    assert renderer_calls[0][renderer_calls[0].index("--renderer-root") + 1] == str(
        renderer_source_root
    )
    assert metadata_path.exists() is True
    assert manifest_payload["display_name"] == "Band Snapback Filter"
    assert manifest_payload["tagline"] == "Fade stretched band breaks after ATR expansion."
    assert manifest_payload["presentation_signature"]
    assert bundle_payload["profile"]["name"] == "Band Snapback Filter"
    assert bundle_payload["profile"]["description"] == manifest_payload["long_description"]

    result_cached = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    assert result_cached["status"] == "cached"
    assert writer_calls["count"] == 1

    stale_metadata = ar_main.load_json_if_exists(metadata_path)
    stale_metadata["presentation_signature"] = "stale-signature"
    metadata_path.write_text(json.dumps(stale_metadata, ensure_ascii=True, indent=2), encoding="utf-8")

    result_regenerated = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    assert result_regenerated["status"] == "rendered"
    assert writer_calls["count"] == 2
    assert ar_main.load_json_if_exists(metadata_path)["presentation_signature"] != "stale-signature"


def test_render_profile_drop_for_attempt_falls_back_when_metadata_writer_fails(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-b"
    run_dir.mkdir(parents=True, exist_ok=True)
    output_root = tmp_path / "profile-drops"
    working_dir = tmp_path

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            output_root_arg = Path(args[args.index("--output-root") + 1])
            bundle_dir = output_root_arg / "bundle-0001"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "profile-document.json").write_text(
                json.dumps(
                    {
                        "profile": {
                            "name": "cand9",
                            "description": "Portable scoring profile scaffolded from live indicator templates.",
                            "directionMode": "both",
                            "indicators": [
                                {
                                    "meta": {"id": "ADX"},
                                    "config": {
                                        "label": "ADX",
                                        "isActive": True,
                                        "isTrendFollowing": True,
                                        "timeframe": "M15",
                                    },
                                }
                            ],
                        }
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    class FailingProvider:
        def complete_json(self, messages):
            raise ar_main.ProviderError("metadata writer offline")

    def fake_run_external(argv, cwd=None, timeout_seconds=None):
        png_path = Path(argv[argv.index("--out") + 1])
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_text("png", encoding="utf-8")

    config = SimpleNamespace(
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            presentation_metadata_provider_profile="writer",
        ),
        fuzzfolio=SimpleNamespace(workspace_root=None),
        providers={"writer": SimpleNamespace(type="codex")},
    )
    row = {
        "attempt_id": "attempt-0002",
        "run_id": "run-b",
        "candidate_name": "cand9",
        "profile_ref": "prof-9",
    }
    attempt = {
        "attempt_id": "attempt-0002",
        "candidate_name": "cand9",
        "profile_ref": "prof-9",
    }

    monkeypatch.setattr(
        ar_main,
        "_build_package_inputs",
        lambda *_args, **_kwargs: {
            "profile_ref": "prof-9",
            "profile_path": tmp_path / "profiles" / "cand9.json",
            "timeframe": "M15",
            "instruments": ["EURUSD"],
            "lookback_months": 36,
        },
    )
    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(ar_main, "create_provider", lambda _profile: FailingProvider())
    monkeypatch.setattr(ar_main, "_run_external", fake_run_external)

    result = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    manifest_payload = ar_main.load_json_if_exists(Path(result["manifest_path"]))
    bundle_dir = output_root / ar_main._profile_drop_attempt_token(row, attempt) / "bundle" / "bundle-0001"
    bundle_payload = ar_main.load_json_if_exists(bundle_dir / "profile-document.json")

    assert result["status"] == "rendered"
    assert manifest_payload["tagline"] is None
    assert manifest_payload["short_description"] is None
    assert "EURUSD profile on M15 using ADX" in manifest_payload["long_description"]
    assert bundle_payload["profile"]["description"] == manifest_payload["long_description"]
    assert Path(result["presentation_metadata_path"]).exists() is False
