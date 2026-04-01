"""Tests for stable profile JSON fingerprinting."""

from __future__ import annotations

import json
from pathlib import Path

from autoresearch.profile_identity import compute_profile_fingerprint, fingerprint_for_json_object


def test_fingerprint_order_independent(tmp_path: Path) -> None:
    a = {"z": 1, "a": {"m": 2, "b": 3}}
    b = {"a": {"b": 3, "m": 2}, "z": 1}
    assert fingerprint_for_json_object(a) == fingerprint_for_json_object(b)


def test_fingerprint_file_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "prof.json"
    p.write_text(json.dumps({"b": 2, "a": 1}), encoding="utf-8")
    h1, e1 = compute_profile_fingerprint(p)
    assert e1 is None
    assert h1 and len(h1) == 64
    p.write_text(json.dumps({"a": 1, "b": 2}), encoding="utf-8")
    h2, e2 = compute_profile_fingerprint(p)
    assert e2 is None
    assert h1 == h2


def test_invalid_json(tmp_path: Path) -> None:
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    h, err = compute_profile_fingerprint(p)
    assert h is None
    assert err
