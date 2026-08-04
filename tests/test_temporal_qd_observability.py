from __future__ import annotations

import json
import time

import pytest

from autoresearch.temporal_qd_observability import (
    PerformanceResourcePressureError,
    PerformanceTrace,
    activate_performance_trace,
    flush_performance_events,
    record_performance_interval,
    start_performance_interval,
    timed_span,
    timing_scope,
)


def _jsonl(path):
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_trace_records_nested_phase_cpu_memory_and_io_evidence(tmp_path) -> None:
    trace = PerformanceTrace(
        output_root=tmp_path,
        generation_index=3,
        sample_interval_seconds=0.1,
    )
    interval = start_performance_interval()
    with activate_performance_trace(trace):
        with trace.span("generation.total"):
            with timing_scope(proposalOrdinal=7, side="long"):
                with timed_span("immigrant.native_round_trip") as span:
                    span.annotate(requestBytes=1234)
                    time.sleep(0.12)
            record_performance_interval(
                "proposal.total",
                interval,
                proposalOrdinal=7,
                disposition="accepted",
            )
        flush_performance_events()
    trace.set_result(
        {
            "completed": True,
            "proposalCount": 1,
            "candidateCount": 1,
            "populationSha256": "sha256:" + "a" * 64,
        }
    )
    trace.close(outcome="completed")

    events = _jsonl(trace.events_path)
    resources = _jsonl(trace.resources_path)
    summary = json.loads(trace.summary_path.read_text(encoding="utf-8"))
    phases = {event["phase"]: event for event in events}

    assert set(phases) == {
        "generation.total",
        "immigrant.native_round_trip",
        "proposal.total",
    }
    child = phases["immigrant.native_round_trip"]
    assert child["attributes"] == {
        "proposalOrdinal": 7,
        "requestBytes": 1234,
        "side": "long",
    }
    assert child["parentSpanId"] == phases["generation.total"]["spanId"]
    assert phases["proposal.total"]["attributes"]["aggregationRole"] == (
        "overlapping_total"
    )
    assert summary["phaseBreakdown"]["proposal.total"]["aggregationRole"] == (
        "overlapping_total"
    )
    assert phases["generation.total"]["wallExclusiveNs"] < (
        phases["generation.total"]["wallInclusiveNs"]
    )

    assert len(resources) >= 2
    successful_samples = [item for item in resources if item.get("tree")]
    assert successful_samples
    assert all(item["tree"]["rssBytes"] > 0 for item in successful_samples)
    assert any(
        item["activeContext"]["phase"] == "immigrant.native_round_trip"
        and item["activeContext"]["proposalOrdinal"] == 7
        and item["activeContext"]["side"] == "long"
        for item in successful_samples
    )
    assert any(
        process["role"] == "coordinator"
        for item in successful_samples
        for process in item["processes"]
    )
    assert all(
        "commandLine" not in process
        for item in successful_samples
        for process in item["processes"]
    )

    assert summary["outcome"] == "completed"
    assert summary["result"]["candidateCount"] == 1
    assert summary["resources"]["peakTreeRssBytes"] > 0
    assert summary["resources"]["minimumHostAvailableBytes"] > 0
    assert summary["resources"]["sampleCount"] >= 2
    assert summary["resources"]["peakContexts"]["treeRss"]["valueBytes"] > 0
    assert summary["phaseBreakdown"]["immigrant.native_round_trip"][
        "inclusiveWall"
    ]["count"] == 1
    assert summary["semanticIdentityParticipation"] == (
        "excluded_observational_artifacts"
    )
    assert trace.latest_summary_path.read_text(encoding="utf-8") == (
        trace.summary_path.read_text(encoding="utf-8")
    )


def test_trace_seals_error_status_without_exception_text(tmp_path) -> None:
    trace = PerformanceTrace(
        output_root=tmp_path,
        generation_index=0,
        sample_interval_seconds=0.1,
    )
    with pytest.raises(RuntimeError, match="sensitive detail"):
        try:
            with activate_performance_trace(trace):
                with trace.span("generation.total"):
                    raise RuntimeError("sensitive detail")
        except RuntimeError as exc:
            trace.close(outcome="error", error_type=type(exc).__name__)
            raise

    events = _jsonl(trace.events_path)
    summary = json.loads(trace.summary_path.read_text(encoding="utf-8"))
    assert events[-1]["status"] == "error"
    assert events[-1]["errorType"] == "RuntimeError"
    assert "sensitive detail" not in trace.events_path.read_text(encoding="utf-8")
    assert "sensitive detail" not in trace.summary_path.read_text(encoding="utf-8")
    assert summary["outcome"] == "error"
    assert summary["errorType"] == "RuntimeError"


def test_process_tree_memory_guard_fails_closed_and_preserves_summary(tmp_path) -> None:
    trace = PerformanceTrace(
        output_root=tmp_path,
        generation_index=0,
        sample_interval_seconds=0.1,
        maximum_tree_rss_bytes=1,
        minimum_host_available_bytes=1,
    )
    with pytest.raises(
        PerformanceResourcePressureError,
        match="maximum_tree_rss_exceeded",
    ):
        trace.assert_resource_guard()
    trace.close(
        outcome="error",
        error_type="PerformanceResourcePressureError",
    )
    summary = json.loads(trace.summary_path.read_text(encoding="utf-8"))
    guard = summary["resources"]["resourceGuard"]
    assert guard["status"] == "breached"
    assert guard["breach"]["reasonCodes"] == ["maximum_tree_rss_exceeded"]
    assert guard["breach"]["treeRssBytes"] > 1
