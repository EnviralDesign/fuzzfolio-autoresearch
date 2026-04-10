"""Training environment checks for Gemma adapter work."""

from __future__ import annotations

import json
import os
from importlib.metadata import PackageNotFoundError, version
from typing import Any


def _pkg(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def main() -> int:
    payload: dict[str, Any] = {
        "torch_imported": False,
        "cuda_available": False,
        "cuda_device_count": 0,
        "cuda_devices": [],
        "bf16_supported": False,
        "bitsandbytes_imported": False,
        "gemma_tokenizer_access": False,
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "transformers_version": _pkg("transformers"),
        "peft_version": _pkg("peft"),
        "trl_version": _pkg("trl"),
        "bitsandbytes_version": _pkg("bitsandbytes"),
    }
    try:
        import torch

        payload["torch_imported"] = True
        payload["torch_version"] = torch.__version__
        payload["cuda_available"] = torch.cuda.is_available()
        payload["cuda_device_count"] = torch.cuda.device_count()
        payload["bf16_supported"] = (
            torch.cuda.is_available() and torch.cuda.is_bf16_supported()
        )
        for index in range(torch.cuda.device_count()):
            payload["cuda_devices"].append(torch.cuda.get_device_name(index))
    except Exception as exc:
        payload["torch_error"] = str(exc)

    try:
        import bitsandbytes  # noqa: F401

        payload["bitsandbytes_imported"] = True
    except Exception as exc:
        payload["bitsandbytes_error"] = str(exc)

    try:
        from transformers import AutoConfig, AutoTokenizer

        cfg = AutoConfig.from_pretrained("google/gemma-4-E4B-it")
        tok = AutoTokenizer.from_pretrained("google/gemma-4-E4B-it")
        payload["gemma_tokenizer_access"] = True
        payload["gemma_model_type"] = getattr(cfg, "model_type", None)
        payload["gemma_vocab_size"] = getattr(tok, "vocab_size", None)
    except Exception as exc:
        payload["gemma_access_error"] = str(exc)

    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
