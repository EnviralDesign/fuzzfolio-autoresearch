"""LoRA / QLoRA fine-tuning entrypoint for Gemma explorer SFT."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from importlib.metadata import version, PackageNotFoundError

import torch
from datasets import load_dataset
from peft import LoraConfig, PeftModel, TaskType, prepare_model_for_kbit_training
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    set_seed,
)
from trl import SFTConfig, SFTTrainer


DEFAULT_TARGET_MODULES = [
    "q_proj",
    "k_proj",
    "v_proj",
    "o_proj",
    "gate_proj",
    "up_proj",
    "down_proj",
]

GEMMA4_LANGUAGE_REGEX = (
    r"^model\.language_model\.layers\.\d+\."
    r"(self_attn\.(q_proj|k_proj|v_proj|o_proj)|mlp\.(gate_proj|up_proj|down_proj))$"
)
MULTIMODAL_EXCLUDE_REGEX = r"^model\.(vision_tower|audio_tower)\..*$"


def _package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _preferred_dtype() -> torch.dtype:
    if torch.cuda.is_available() and torch.cuda.is_bf16_supported():
        return torch.bfloat16
    return torch.float16


def _load_model(
    *,
    model_id: str,
    dtype: torch.dtype,
    quantization: str,
    trust_remote_code: bool,
    adapter_init_dir: Path | None,
) -> tuple[AutoModelForCausalLM, AutoTokenizer]:
    tokenizer = AutoTokenizer.from_pretrained(
        str(adapter_init_dir) if adapter_init_dir else model_id,
        trust_remote_code=trust_remote_code,
    )
    if tokenizer.pad_token is None and tokenizer.eos_token is not None:
        tokenizer.pad_token = tokenizer.eos_token

    model_kwargs: dict[str, object] = {
        "trust_remote_code": trust_remote_code,
        "dtype": dtype,
    }
    if quantization == "4bit":
        model_kwargs["quantization_config"] = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=dtype,
        )
    model = AutoModelForCausalLM.from_pretrained(model_id, **model_kwargs)
    if quantization == "4bit":
        model = prepare_model_for_kbit_training(model)
    if adapter_init_dir is not None:
        # Continuation runs must reopen the adapter in trainable mode; PEFT defaults to frozen.
        model = PeftModel.from_pretrained(model, str(adapter_init_dir), is_trainable=True)
    if getattr(model.config, "use_cache", None) is not None:
        model.config.use_cache = False
    return model, tokenizer


def _parameter_report(model: torch.nn.Module) -> dict[str, int]:
    total = 0
    trainable = 0
    for parameter in model.parameters():
        count = int(parameter.numel())
        total += count
        if parameter.requires_grad:
            trainable += count
    return {
        "total_parameters": total,
        "trainable_parameters": trainable,
    }


def _resolve_target_modules(args: argparse.Namespace) -> tuple[str | list[str], str | list[str] | None]:
    if args.target_module_preset == "gemma4_language_regex":
        return GEMMA4_LANGUAGE_REGEX, None
    if args.target_module_preset == "gemma4_suffix":
        return list(args.target_modules), MULTIMODAL_EXCLUDE_REGEX
    return list(args.target_modules), None


def _peft_config(args: argparse.Namespace) -> LoraConfig:
    target_modules, exclude_modules = _resolve_target_modules(args)
    return LoraConfig(
        task_type=TaskType.CAUSAL_LM,
        r=args.lora_r,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        bias="none",
        target_modules=target_modules,
        exclude_modules=exclude_modules,
    )


def _dataset_paths(args: argparse.Namespace) -> dict[str, str]:
    files = {"train": str(args.train_file)}
    if args.val_file is not None:
        files["validation"] = str(args.val_file)
    return files


def _write_run_config(args: argparse.Namespace, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    target_modules, exclude_modules = _resolve_target_modules(args)
    payload = {
        "model_id": args.model_id,
        "train_file": str(args.train_file),
        "val_file": str(args.val_file) if args.val_file else None,
        "adapter_init_dir": str(args.adapter_init_dir) if args.adapter_init_dir else None,
        "output_dir": str(output_dir),
        "max_seq_length": args.max_seq_length,
        "per_device_train_batch_size": args.per_device_train_batch_size,
        "gradient_accumulation_steps": args.gradient_accumulation_steps,
        "num_train_epochs": args.num_train_epochs,
        "max_steps": args.max_steps,
        "learning_rate": args.learning_rate,
        "adapter_mode": args.adapter_mode,
        "quantization": args.quantization,
        "target_module_preset": args.target_module_preset,
        "target_modules_resolved": target_modules,
        "exclude_modules_resolved": exclude_modules,
        "target_modules_explicit": list(args.target_modules),
    }
    (output_dir / "run_config.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _env_report(args: argparse.Namespace, dtype: torch.dtype) -> dict[str, object]:
    target_modules, exclude_modules = _resolve_target_modules(args)
    return {
        "model_id": args.model_id,
        "adapter_mode": args.adapter_mode,
        "quantization": args.quantization,
        "target_module_preset": args.target_module_preset,
        "target_modules_resolved": target_modules,
        "exclude_modules_resolved": exclude_modules,
        "explicit_target_modules": list(args.target_modules),
        "dtype": str(dtype),
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "torch_version": _package_version("torch"),
        "transformers_version": _package_version("transformers"),
        "peft_version": _package_version("peft"),
        "trl_version": _package_version("trl"),
        "bitsandbytes_version": _package_version("bitsandbytes"),
        "cuda_available": torch.cuda.is_available(),
        "cuda_device_count": torch.cuda.device_count() if torch.cuda.is_available() else 0,
        "bf16_supported": torch.cuda.is_available() and torch.cuda.is_bf16_supported(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a Gemma LoRA adapter on explorer SFT data.")
    parser.add_argument("--model-id", default="google/gemma-4-E4B-it")
    parser.add_argument("--train-file", type=Path, required=True)
    parser.add_argument("--val-file", type=Path)
    parser.add_argument(
        "--adapter-init-dir",
        type=Path,
        help="Optional existing adapter directory to continue fine-tuning from.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-seq-length", type=int, default=4096)
    parser.add_argument("--per-device-train-batch-size", type=int, default=1)
    parser.add_argument("--per-device-eval-batch-size", type=int, default=1)
    parser.add_argument("--gradient-accumulation-steps", type=int, default=8)
    parser.add_argument("--num-train-epochs", type=float, default=1.0)
    parser.add_argument(
        "--max-steps",
        type=int,
        default=-1,
        help="Optional hard cap on optimizer steps for controlled pilot runs.",
    )
    parser.add_argument("--learning-rate", type=float, default=2e-4)
    parser.add_argument("--weight-decay", type=float, default=0.0)
    parser.add_argument("--warmup-ratio", type=float, default=0.03)
    parser.add_argument("--logging-steps", type=int, default=5)
    parser.add_argument("--save-steps", type=int, default=100)
    parser.add_argument("--eval-steps", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--adapter-mode",
        choices=("lora", "qlora"),
        default="qlora",
        help="Adapter strategy. qlora implies 4-bit quantization by default.",
    )
    parser.add_argument(
        "--quantization",
        choices=("none", "4bit"),
        default="4bit",
        help="Base model quantization mode.",
    )
    parser.add_argument("--qlora", action="store_true", help="Backward-compatible alias for --adapter-mode qlora --quantization 4bit.")
    parser.add_argument("--gradient-checkpointing", action="store_true")
    parser.add_argument("--trust-remote-code", action="store_true")
    parser.add_argument("--report-only", action="store_true", help="Write environment and target-module report, then exit before model load.")
    parser.add_argument("--lora-r", type=int, default=16)
    parser.add_argument("--lora-alpha", type=int, default=32)
    parser.add_argument("--lora-dropout", type=float, default=0.05)
    parser.add_argument(
        "--target-modules",
        nargs="+",
        default=DEFAULT_TARGET_MODULES,
    )
    parser.add_argument(
        "--target-module-preset",
        choices=("gemma4_language_regex", "gemma4_suffix", "explicit"),
        default="gemma4_language_regex",
        help="How to resolve LoRA target modules.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.qlora:
        args.adapter_mode = "qlora"
        args.quantization = "4bit"
    set_seed(args.seed)
    dtype = _preferred_dtype()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_run_config(args, output_dir)
    report = _env_report(args, dtype)
    (output_dir / "env_report.json").write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    if args.report_only:
        print(json.dumps(report, ensure_ascii=True, indent=2))
        return 0
    dataset = load_dataset("json", data_files=_dataset_paths(args))
    model, tokenizer = _load_model(
        model_id=args.model_id,
        dtype=dtype,
        quantization=args.quantization,
        trust_remote_code=args.trust_remote_code,
        adapter_init_dir=args.adapter_init_dir,
    )
    (output_dir / "parameter_report.json").write_text(
        json.dumps(_parameter_report(model), ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    peft_config = None if args.adapter_init_dir else _peft_config(args)
    training_args = SFTConfig(
        output_dir=str(output_dir),
        max_length=args.max_seq_length,
        per_device_train_batch_size=args.per_device_train_batch_size,
        per_device_eval_batch_size=args.per_device_eval_batch_size,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        learning_rate=args.learning_rate,
        num_train_epochs=args.num_train_epochs,
        max_steps=args.max_steps,
        weight_decay=args.weight_decay,
        warmup_ratio=args.warmup_ratio,
        logging_steps=args.logging_steps,
        save_steps=args.save_steps,
        eval_steps=args.eval_steps if "validation" in dataset else None,
        eval_strategy="steps" if "validation" in dataset else "no",
        save_strategy="steps",
        gradient_checkpointing=args.gradient_checkpointing,
        bf16=(dtype == torch.bfloat16),
        fp16=(dtype == torch.float16),
        report_to=[],
    )
    trainer = SFTTrainer(
        model=model,
        args=training_args,
        train_dataset=dataset["train"],
        eval_dataset=dataset.get("validation"),
        processing_class=tokenizer,
        peft_config=peft_config,
    )
    trainer.train()
    trainer.save_model(str(output_dir / "adapter"))
    tokenizer.save_pretrained(str(output_dir / "adapter"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
