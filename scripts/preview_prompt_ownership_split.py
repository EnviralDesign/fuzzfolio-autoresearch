from __future__ import annotations

import argparse
import json
from pathlib import Path


BODY_MARKER = "===== LITERALLY SENT TO API: HTTP REQUEST JSON BODY ====="
MESSAGES_MARKER = (
    "===== INFORMATIONAL ONLY: READABLE VIEW OF MESSAGE CONTENT FROM THE REQUEST ====="
)


def _extract_request_body(text: str) -> str | None:
    body_start = text.find(BODY_MARKER)
    if body_start < 0:
        return None
    body_start += len(BODY_MARKER)
    if text[body_start : body_start + 1] == "\n":
        body_start += 1
    messages_start = text.find(MESSAGES_MARKER)
    if messages_start < 0 or body_start > messages_start:
        return text[body_start:].strip()
    return text[body_start:messages_start].strip()


def _extract_labeled_block(text: str, start_label: str, end_label: str | None) -> tuple[str | None, str]:
    start = text.find(start_label)
    if start < 0:
        return None, text
    if end_label is None:
        block = text[start:].strip()
        remaining = text[:start].rstrip()
        return block, remaining
    end = text.find(end_label, start)
    if end < 0:
        return None, text
    block = text[start:end].strip()
    remaining = (text[:start].rstrip() + "\n\n" + text[end:].lstrip()).strip()
    return block, remaining


def _split_user_ownership(user_text: str) -> tuple[str, list[str]]:
    remaining = str(user_text or "")
    moved_blocks: list[str] = []
    for start_label, end_label in (
        ("Program:\n", "Current seed hand:\n"),
        ("Portable profile template note:\n", "Sticky indicator context:\n"),
        (
            "Sensitivity artifact layout (on disk after evaluations):\n",
            "Run-owned profiles so far:\n",
        ),
        ("Tool reference:\n", None),
    ):
        block, remaining = _extract_labeled_block(remaining, start_label, end_label)
        if block:
            moved_blocks.append(block)
    cleaned = "\n\n".join(part.strip() for part in remaining.split("\n\n") if part.strip())
    return cleaned.strip(), moved_blocks


def _preview_request_payload(payload: dict[str, object]) -> tuple[dict[str, object], list[str]]:
    preview = json.loads(json.dumps(payload))
    moved_labels: list[str] = []
    messages = preview.get("messages")
    if not isinstance(messages, list) or len(messages) < 2:
        return preview, moved_labels
    first = messages[0] if isinstance(messages[0], dict) else None
    second = messages[1] if isinstance(messages[1], dict) else None
    if not isinstance(first, dict) or not isinstance(second, dict):
        return preview, moved_labels
    if str(first.get("role")) != "system" or str(second.get("role")) != "user":
        return preview, moved_labels
    user_content = str(second.get("content") or "")
    new_user, moved_blocks = _split_user_ownership(user_content)
    if not moved_blocks:
        return preview, moved_labels
    first["content"] = str(first.get("content") or "").rstrip() + "\n\n" + "\n\n".join(moved_blocks)
    second["content"] = new_user
    for block in moved_blocks:
        label = block.splitlines()[0].rstrip(":").strip()
        if label:
            moved_labels.append(label)
    return preview, moved_labels


def _format_messages(messages: list[dict[str, object]]) -> str:
    sections: list[str] = []
    for index, message in enumerate(messages, start=1):
        sections.append(f"[message {index}] role={message.get('role')}")
        sections.append(str(message.get("content") or ""))
        sections.append("")
    return "\n".join(sections).rstrip()


def _render_preview(snapshot_path: Path, output_path: Path) -> bool:
    text = snapshot_path.read_text(encoding="utf-8")
    body_text = _extract_request_body(text)
    if not body_text:
        return False
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    preview_payload, moved_labels = _preview_request_payload(payload)
    messages = preview_payload.get("messages")
    rendered_messages = (
        _format_messages(messages)
        if isinstance(messages, list) and all(isinstance(item, dict) for item in messages)
        else "No chat-style messages found."
    )
    metadata = {
        "source_snapshot": str(snapshot_path),
        "preview_type": "ownership_refactor_preview",
        "moved_blocks": moved_labels,
        "note": "This preview was not sent to the API. It shows the proposed system/user ownership split.",
    }
    output_text = "\n".join(
        [
            "===== INFORMATIONAL ONLY: PREVIEW OF REFACTORED OWNERSHIP (NOT ACTUALLY SENT) =====",
            json.dumps(metadata, ensure_ascii=True, indent=2),
            "",
            "===== LITERALLY SENT TO API IN THIS PREVIEW: HTTP REQUEST JSON BODY =====",
            json.dumps(preview_payload, ensure_ascii=True, indent=2),
            "",
            "===== INFORMATIONAL ONLY: READABLE VIEW OF MESSAGE CONTENT FROM THE PREVIEW REQUEST =====",
            rendered_messages,
        ]
    ).rstrip() + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_text, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render system/user ownership preview snapshots from an existing run-local diagnostics folder."
    )
    parser.add_argument("--run-dir", required=True, help="Run directory containing llm-request-snapshots.")
    parser.add_argument(
        "--input-dir-name",
        default="llm-request-snapshots",
        help="Existing snapshot folder name under the run directory.",
    )
    parser.add_argument(
        "--output-dir-name",
        default="llm-request-snapshots-ownership-preview",
        help="Preview folder name to create under the run directory.",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    input_dir = run_dir / str(args.input_dir_name)
    output_dir = run_dir / str(args.output_dir_name)
    if not input_dir.exists():
        raise SystemExit(f"Input snapshot directory does not exist: {input_dir}")
    rendered = 0
    copied = 0
    for snapshot_path in sorted(input_dir.glob("*.txt")):
        output_path = output_dir / snapshot_path.name
        if _render_preview(snapshot_path, output_path):
            rendered += 1
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(snapshot_path.read_text(encoding="utf-8"), encoding="utf-8")
            copied += 1
    print(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "input_dir": str(input_dir),
                "output_dir": str(output_dir),
                "rendered": rendered,
                "copied": copied,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
