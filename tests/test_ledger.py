from __future__ import annotations

from autoresearch.ledger import append_attempt_row, load_attempts


def test_load_attempts_skips_nul_padding_lines(tmp_path):
    attempts_path = tmp_path / "attempts.jsonl"
    attempts_path.write_text(
        '{"attempt_id": "a1", "score": 1}\n'
        "\x00\x00\x00\x00\n"
        '{"attempt_id": "a2", "score": 2}\n',
        encoding="utf-8",
    )

    rows = load_attempts(attempts_path)

    assert [row["attempt_id"] for row in rows] == ["a1", "a2"]


def test_append_attempt_row_appends_jsonl(tmp_path):
    attempts_path = tmp_path / "nested" / "attempts.jsonl"

    append_attempt_row(attempts_path, {"attempt_id": "a1", "score": 1.5})
    append_attempt_row(attempts_path, {"attempt_id": "a2", "note": "raw"})

    rows = load_attempts(attempts_path)
    assert rows == [
        {"attempt_id": "a1", "score": 1.5},
        {"attempt_id": "a2", "note": "raw"},
    ]
