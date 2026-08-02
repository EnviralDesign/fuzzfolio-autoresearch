from __future__ import annotations

from pathlib import Path

import pytest

import autoresearch.result_codec as result_codec
import autoresearch.temporal_qd_evolution as qd
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)


def _candidate() -> dict[str, object]:
    return {
        "candidateId": "candidate_a",
        "programSha256": canonical_sha256({"program": "candidate_a"}),
    }


@pytest.mark.parametrize(
    ("windows", "match"),
    [
        (
            [{"programSha256": None, "v3Admissible": True}],
            "result window 0 program SHA-256",
        ),
        (
            [
                {
                    "programSha256": canonical_sha256({"program": "wrong"}),
                    "v3Admissible": True,
                }
            ],
            "result window program identity does not match",
        ),
        (
            [
                {
                    "programSha256": canonical_sha256({"program": "candidate_a"}),
                    "v3Admissible": True,
                },
                {
                    "programSha256": canonical_sha256({"program": "drifted"}),
                    "v3Admissible": True,
                },
            ],
            "result window program identity does not match",
        ),
    ],
    ids=("missing-program", "wrong-program", "window-program-drift"),
)
def test_qd_archive_rejects_unbound_or_drifted_window_program_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    windows: list[dict[str, object]],
    match: str,
) -> None:
    candidate = _candidate()
    monkeypatch.setattr(qd, "_load_population", lambda _path: ([candidate], "sha256:" + "a" * 64))
    monkeypatch.setattr(
        qd,
        "load_stage_results",
        lambda _root: {"candidate_a": windows},
    )

    with pytest.raises(TemporalDiscoveryContractError, match=match):
        qd.build_qd_archive(
            population_path=tmp_path / "population.json",
            result_root=tmp_path / "result-root",
            output_path=tmp_path / "archive.json",
            generation_index=0,
        )


def test_qd_archive_keeps_v3_admissibility_gate_after_program_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate()
    monkeypatch.setattr(qd, "_load_population", lambda _path: ([candidate], "sha256:" + "a" * 64))
    monkeypatch.setattr(
        qd,
        "load_stage_results",
        lambda _root: {
            "candidate_a": [
                {
                    "programSha256": candidate["programSha256"],
                    "v3Admissible": False,
                }
            ]
        },
    )

    with pytest.raises(TemporalDiscoveryContractError, match="requires terminal-adjusted"):
        qd.build_qd_archive(
            population_path=tmp_path / "population.json",
            result_root=tmp_path / "result-root",
            output_path=tmp_path / "archive.json",
            generation_index=0,
        )


def test_gzip_publication_fsyncs_payload_before_link_and_directory_after(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    original_fsync = result_codec.os.fsync
    original_link = result_codec.os.link

    def fsync(descriptor: int) -> None:
        events.append("payload-fsync")
        original_fsync(descriptor)

    def link(source: Path, target: Path) -> None:
        events.append("publish-link")
        original_link(source, target)

    monkeypatch.setattr(result_codec.os, "fsync", fsync)
    monkeypatch.setattr(result_codec.os, "link", link)
    monkeypatch.setattr(
        result_codec,
        "fsync_directory",
        lambda _directory: events.append("directory-fsync") or True,
    )

    result_codec.write_gzip_json_once(tmp_path / "candidate.json.gz", {"value": 1})
    assert events == ["payload-fsync", "publish-link", "directory-fsync"]


def test_qd_immutable_and_mutable_publication_have_durable_ordering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    original_fsync = qd.os.fsync
    original_link = qd.os.link
    original_replace = qd.os.replace

    def fsync(descriptor: int) -> None:
        events.append("payload-fsync")
        original_fsync(descriptor)

    def link(source: Path, target: Path) -> None:
        events.append("publish-link")
        original_link(source, target)

    def replace(source: Path, target: Path) -> None:
        events.append("publish-replace")
        original_replace(source, target)

    monkeypatch.setattr(qd.os, "fsync", fsync)
    monkeypatch.setattr(qd.os, "link", link)
    monkeypatch.setattr(qd.os, "replace", replace)
    monkeypatch.setattr(
        qd,
        "fsync_directory",
        lambda _directory: events.append("directory-fsync") or True,
    )

    immutable = tmp_path / "immutable.json"
    qd._write_once(immutable, {"value": 1})
    assert events == ["payload-fsync", "publish-link", "directory-fsync"]
    assert immutable.read_text(encoding="utf-8") == '{\n  "value": 1\n}\n'

    events.clear()
    qd._replace(tmp_path / "checkpoint.json", {"value": 2})
    assert events == ["payload-fsync", "publish-replace", "directory-fsync"]


def test_windows_directory_sync_fallback_is_safe_when_native_open_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A Windows host can support directory FlushFileBuffers (as this machine
    # often does), so force the unavailable-native-API path explicitly.  It
    # must be a safe no-op rather than failing an interrupted publication or
    # pretending the file payload itself was not already durable.
    import ctypes

    def unavailable(*_args: object, **_kwargs: object) -> object:
        raise OSError("native directory handles unavailable")

    monkeypatch.setattr(ctypes, "WinDLL", unavailable)
    assert result_codec._fsync_directory_windows(tmp_path) is False
    assert result_codec._unsupported_windows_directory_sync_error(5) is True
    assert result_codec._unsupported_windows_directory_sync_error(12345) is False
