from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "generate_evolutionary_substrate_atlas_v2.py"
SPEC = importlib.util.spec_from_file_location("atlas_v2", SCRIPT)
assert SPEC and SPEC.loader
atlas_v2 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(atlas_v2)


def git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


def commit_file(root: Path, payload: bytes, message: str) -> str:
    (root / "source.txt").write_bytes(payload)
    git(root, "add", "source.txt")
    git(root, "-c", "user.name=Atlas Test", "-c", "user.email=atlas@example.test", "commit", "-m", message)
    return subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def test_git_blob_identity_is_independent_of_checkout_line_endings_and_rotates_on_commit(tmp_path: Path) -> None:
    git(tmp_path, "init")
    first = commit_file(tmp_path, b"alpha\n", "first")
    lf = atlas_v2.source_binding(
        repository="test", root=tmp_path, commit=first, relative_path="source.txt", component="source"
    )

    (tmp_path / "source.txt").write_bytes(b"alpha\r\n")
    crlf = atlas_v2.source_binding(
        repository="test", root=tmp_path, commit=first, relative_path="source.txt", component="source"
    )
    assert lf["gitBlobObjectId"] == crlf["gitBlobObjectId"]
    assert lf["gitBlobSha256"] == crlf["gitBlobSha256"]
    assert crlf["worktreeLineEndingMode"] == "crlf"
    assert crlf["worktreeSemanticallyMatchesGitBlob"] is True
    assert crlf["worktreeRawSha256"] != crlf["gitBlobSha256"]

    second = commit_file(tmp_path, b"bravo\n", "second")
    changed = atlas_v2.source_binding(
        repository="test", root=tmp_path, commit=second, relative_path="source.txt", component="source"
    )
    assert changed["gitBlobObjectId"] != lf["gitBlobObjectId"]
    assert changed["gitBlobSha256"] != lf["gitBlobSha256"]
