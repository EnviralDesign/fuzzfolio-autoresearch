"""Merge a PEFT LoRA adapter into its Hugging Face base model."""

from __future__ import annotations

import argparse
import json
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import torch
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer


def _package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _resolve_dtype(name: str, device: str) -> torch.dtype:
    if name == "float16":
        return torch.float16
    if name == "bfloat16":
        return torch.bfloat16
    if name == "float32":
        return torch.float32
    if device in {"cuda", "auto"} and torch.cuda.is_available():
        if torch.cuda.is_bf16_supported():
            return torch.bfloat16
        return torch.float16
    return torch.float32


def _load_tokenizer(model_id: str, adapter_dir: Path, trust_remote_code: bool):
    source = str(adapter_dir) if (adapter_dir / "tokenizer_config.json").exists() else model_id
    tokenizer = AutoTokenizer.from_pretrained(source, trust_remote_code=trust_remote_code)
    if tokenizer.pad_token is None and tokenizer.eos_token is not None:
        tokenizer.pad_token = tokenizer.eos_token
    return tokenizer


def _load_model(
    *,
    model_id: str,
    adapter_dir: Path,
    dtype: torch.dtype,
    device: str,
    trust_remote_code: bool,
):
    model_kwargs: dict[str, object] = {
        "trust_remote_code": trust_remote_code,
        "dtype": dtype,
        "low_cpu_mem_usage": True,
    }
    if device == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA requested but torch.cuda.is_available() is false.")
        model_kwargs["device_map"] = {"": "cuda:0"}
    elif device == "auto" and torch.cuda.is_available():
        model_kwargs["device_map"] = {"": "cuda:0"}

    model = AutoModelForCausalLM.from_pretrained(model_id, **model_kwargs)
    model = PeftModel.from_pretrained(model, str(adapter_dir), is_trainable=False)
    return model


def _write_report(
    *,
    report_path: Path,
    model_id: str,
    adapter_dir: Path,
    output_dir: Path,
    dtype: torch.dtype,
    device: str,
    trust_remote_code: bool,
    report_only: bool,
) -> None:
    payload = {
        "model_id": model_id,
        "adapter_dir": str(adapter_dir),
        "output_dir": str(output_dir),
        "dtype": str(dtype),
        "device": device,
        "trust_remote_code": trust_remote_code,
        "report_only": report_only,
        "cuda_available": torch.cuda.is_available(),
        "cuda_device_count": torch.cuda.device_count() if torch.cuda.is_available() else 0,
        "transformers_version": _package_version("transformers"),
        "peft_version": _package_version("peft"),
        "torch_version": _package_version("torch"),
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge a Gemma LoRA adapter into a Hugging Face model.")
    parser.add_argument(
        "--model-id",
        default="google/gemma-4-E4B-it",
        help="Hugging Face model ID or local base model directory.",
    )
    parser.add_argument("--adapter-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--dtype",
        choices=("auto", "float16", "bfloat16", "float32"),
        default="auto",
        help="Merged model dtype before GGUF conversion.",
    )
    parser.add_argument(
        "--device",
        choices=("auto", "cpu", "cuda"),
        default="auto",
        help="Where to run the merge. Auto uses CUDA if available.",
    )
    parser.add_argument("--trust-remote-code", action="store_true")
    parser.add_argument(
        "--max-shard-size",
        default="4GB",
        help="Hugging Face save_pretrained max shard size.",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Write the merge plan report without loading model weights.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.adapter_dir = args.adapter_dir.resolve()
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    dtype = _resolve_dtype(args.dtype, args.device)
    _write_report(
        report_path=args.output_dir / "merge_report.json",
        model_id=args.model_id,
        adapter_dir=args.adapter_dir,
        output_dir=args.output_dir,
        dtype=dtype,
        device=args.device,
        trust_remote_code=args.trust_remote_code,
        report_only=args.report_only,
    )

    if args.report_only:
        return

    tokenizer = _load_tokenizer(args.model_id, args.adapter_dir, args.trust_remote_code)
    model = _load_model(
        model_id=args.model_id,
        adapter_dir=args.adapter_dir,
        dtype=dtype,
        device=args.device,
        trust_remote_code=args.trust_remote_code,
    )
    merged = model.merge_and_unload()
    if getattr(merged.config, "use_cache", None) is not None:
        merged.config.use_cache = True
    merged.save_pretrained(
        str(args.output_dir),
        safe_serialization=True,
        max_shard_size=args.max_shard_size,
    )
    tokenizer.save_pretrained(str(args.output_dir))


if __name__ == "__main__":
    main()
