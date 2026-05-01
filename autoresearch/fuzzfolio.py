from __future__ import annotations

import json
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .config import FuzzfolioConfig


class CliError(RuntimeError):
    """Raised when the wrapped CLI fails."""


@dataclass
class CommandResult:
    argv: list[str]
    cwd: Path
    returncode: int
    stdout: str
    stderr: str
    parsed_json: dict[str, Any] | list[Any] | None = None


class FuzzfolioCli:
    def __init__(self, config: FuzzfolioConfig):
        self.config = config

    def resolve_executable(self) -> str:
        resolved = shutil.which(self.config.cli_command)
        if resolved:
            return resolved
        if Path(self.config.cli_command).exists():
            return str(Path(self.config.cli_command).resolve())
        raise FileNotFoundError(f"Could not resolve fuzzfolio CLI executable: {self.config.cli_command}")

    def build_base_argv(self) -> list[str]:
        argv = [self.resolve_executable()]
        if self.config.base_url:
            argv.extend(["--base-url", self.config.base_url])
        if self.config.auth_profile:
            argv.extend(["--auth-profile", self.config.auth_profile])
        if self.config.api_key:
            argv.extend(["--api-key", self.config.api_key])
        if self.config.workspace_root:
            argv.extend(["--workspace-root", str(self.config.workspace_root)])
        return argv

    def run(
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        check: bool = True,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        argv = [*self.build_base_argv(), *args]
        working_dir = cwd or self.config.workspace_root or Path.cwd()
        try:
            proc = subprocess.run(
                argv,
                cwd=str(working_dir),
                text=True,
                capture_output=True,
                encoding="utf-8",
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise CliError(
                f"Command timed out after {timeout_seconds:.0f}s: {' '.join(argv)}\n"
                f"cwd: {working_dir}\n"
                f"stdout:\n{(exc.stdout or '').strip()[:1600]}\n\n"
                f"stderr:\n{(exc.stderr or '').strip()[:1600]}"
            ) from exc
        parsed_json = None
        stdout = proc.stdout.strip()
        if stdout.startswith("{") or stdout.startswith("["):
            try:
                parsed_json = json.loads(stdout)
            except json.JSONDecodeError:
                parsed_json = None
        result = CommandResult(
            argv=argv,
            cwd=Path(working_dir),
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            parsed_json=parsed_json,
        )
        if check and proc.returncode != 0:
            raise CliError(self.format_result(result))
        return result

    def run_with_heartbeat(
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        check: bool = True,
        timeout_seconds: float | None = None,
        heartbeat_seconds: float = 30.0,
        heartbeat: Callable[[float], None] | None = None,
        echo_output: bool = False,
    ) -> CommandResult:
        argv = [*self.build_base_argv(), *args]
        working_dir = cwd or self.config.workspace_root or Path.cwd()
        proc = subprocess.Popen(
            argv,
            cwd=str(working_dir),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []

        def reader(stream: Any, parts: list[str], *, stderr: bool = False) -> None:
            try:
                for line in iter(stream.readline, ""):
                    parts.append(line)
                    if echo_output:
                        target = sys.stderr if stderr else sys.stdout
                        print(line, end="", file=target)
            finally:
                stream.close()

        stdout_thread = threading.Thread(
            target=reader,
            args=(proc.stdout, stdout_parts),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=reader,
            args=(proc.stderr, stderr_parts),
            kwargs={"stderr": True},
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()

        started = time.monotonic()
        next_heartbeat = started + max(1.0, float(heartbeat_seconds))
        while True:
            returncode = proc.poll()
            now = time.monotonic()
            if returncode is not None:
                break
            elapsed = now - started
            if timeout_seconds is not None and elapsed > timeout_seconds:
                proc.kill()
                stdout_thread.join(timeout=2)
                stderr_thread.join(timeout=2)
                raise CliError(
                    f"Command timed out after {timeout_seconds:.0f}s: {' '.join(argv)}\n"
                    f"cwd: {working_dir}\n"
                    f"stdout:\n{''.join(stdout_parts).strip()[:1600]}\n\n"
                    f"stderr:\n{''.join(stderr_parts).strip()[:1600]}"
                )
            if heartbeat is not None and now >= next_heartbeat:
                heartbeat(elapsed)
                next_heartbeat = now + max(1.0, float(heartbeat_seconds))
            time.sleep(0.25)

        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        stdout = "".join(stdout_parts)
        stderr = "".join(stderr_parts)
        parsed_json = None
        stdout_stripped = stdout.strip()
        if stdout_stripped.startswith("{") or stdout_stripped.startswith("["):
            try:
                parsed_json = json.loads(stdout_stripped)
            except json.JSONDecodeError:
                parsed_json = None
        result = CommandResult(
            argv=argv,
            cwd=Path(working_dir),
            returncode=int(proc.returncode or 0),
            stdout=stdout,
            stderr=stderr,
            parsed_json=parsed_json,
        )
        if check and result.returncode != 0:
            raise CliError(self.format_result(result))
        return result

    @staticmethod
    def format_result(result: CommandResult, limit: int = 1600) -> str:
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        return (
            f"Command failed: {' '.join(result.argv)}\n"
            f"cwd: {result.cwd}\n"
            f"exit: {result.returncode}\n"
            f"stdout:\n{stdout[:limit]}\n\nstderr:\n{stderr[:limit]}"
        )

    def whoami(self) -> CommandResult:
        return self.run(["auth", "whoami", "--pretty"])

    def ensure_login(self) -> CommandResult:
        whoami = self.run(["auth", "whoami", "--pretty"], check=False)
        if whoami.returncode == 0:
            return whoami
        if not self.config.email or not self.config.password:
            raise CliError(
                "CLI auth is missing and no fallback credentials were found in .agentsecrets for fuzzfolio.email/password."
            )
        self.run(
            [
                "auth",
                "login",
                "--profile",
                self.config.auth_profile,
                "--email",
                self.config.email,
                "--password",
                self.config.password,
                "--pretty",
            ]
        )
        return self.whoami()

    def seed_prompt(self, out_path: Path | None = None) -> CommandResult:
        args = ["seed", "prompt", "--pretty"]
        if out_path is not None:
            args.extend(["--out", str(out_path)])
        return self.run(args)

    def score_artifact(self, artifact_dir: Path) -> dict[str, Any]:
        result = self.run(["compare-sensitivity", "--input", str(artifact_dir), "--pretty"])
        if not isinstance(result.parsed_json, dict):
            raise CliError(f"compare-sensitivity did not emit JSON for {artifact_dir}")
        return result.parsed_json

    def help_text(self, args: list[str] | None = None) -> str:
        result = self.run([*(args or []), "--help"], check=False)
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        return stdout or stderr

    def create_cloud_profile(self, profile_path: Path) -> str:
        result = self.run(
            ["profiles", "create", "--file", str(profile_path), "--pretty"]
        )
        payload = result.parsed_json if isinstance(result.parsed_json, dict) else None
        data = payload.get("data") if isinstance(payload, dict) else None
        profile_id = str((data or {}).get("id") or "").strip()
        if not profile_id:
            raise CliError(
                f"profiles create did not return a profile id for {profile_path}"
            )
        return profile_id
