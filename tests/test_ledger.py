from __future__ import annotations

from autoresearch.ledger import load_attempts


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
