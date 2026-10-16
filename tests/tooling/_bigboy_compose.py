"""Shared helpers of the bigboy compose tests (CHAOS-9962): render the ONE compose file, ci/bigboy/compose.bigboy.yml, the way
a cut does (project directory = the bigboy root, ops/.env then the pins env file), in a scratch root with fixture values only."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "ci" / "bigboy"
COMPOSE_FILE = TOOLS / "compose.bigboy.yml"
PINS_FILE = TOOLS / "images.env"
OPERATOR_IMAGE = "ghcr.io/full-chaos/dev-health-go-operator@sha256:" + "0" * 64


def scratch_root(tmp_path: Path) -> Path:
    """A bigboy root holding the files the compose file reads: the env files and the credentials directory."""
    root = tmp_path / "bigboy-root"
    for rel in (
        "ops/.env",
        "web/.env",
        "_records/images.env",
        ".go-api-dev/go-api.env",
        ".go-api-dev/dho_api_ch.xml",
    ):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(PINS_FILE.read_text() if rel.endswith("images.env") else "")
    return root


def compose_argv(root: Path, *profiles: str) -> list[str]:
    argv = [
        "docker",
        "compose",
        "-p",
        "dev-health",
        "--project-directory",
        str(root),
        "--env-file",
        str(root / "ops" / ".env"),
        "--env-file",
        str(root / "_records" / "images.env"),
        "-f",
        str(COMPOSE_FILE),
    ]
    for profile in profiles:
        argv += ["--profile", profile]
    return argv


def compose_env(root: Path, **extra: str) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": str(root),
        "BIGBOY_OPERATOR_IMAGE": OPERATOR_IMAGE,
        "DHO_API_CH_USERS_XML": str(root / ".go-api-dev" / "dho_api_ch.xml"),
    }
    env.update(extra)
    return env


def config(
    root: Path, *profiles: str, **extra: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*compose_argv(root, *profiles), "config", "--format", "json"],
        capture_output=True,
        text=True,
        env=compose_env(root, **extra),
        timeout=120,
        check=False,
    )


def services(root: Path, *profiles: str, **extra: str) -> dict:
    proc = config(root, *profiles, **extra)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)["services"]
