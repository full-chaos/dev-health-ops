from __future__ import annotations

import os
import subprocess
import sys

import pytest


def _run_cli_help(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DISABLE_DOTENV"] = "1"
    env["PYTHONPATH"] = "src"
    return subprocess.run(
        [sys.executable, "-m", "dev_health_ops.cli", *args],
        check=False,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    "args",
    [
        ("--help",),
        ("sync", "--help"),
        ("metrics", "--help"),
        ("audit", "--help"),
        ("fixtures", "--help"),
        ("api", "--help"),
        ("billing", "--help"),
        ("admin", "--help"),
        ("work-graph", "--help"),
        ("backfill", "--help"),
        ("recommendations", "--help"),
        ("migrate", "--help"),
        ("migrate", "clickhouse", "repair", "--help"),
        ("workers", "--help"),
        ("workers", "inspect", "--help"),
        ("maintenance", "--help"),
    ],
)
def test_help_has_no_startup_noise_before_usage(args: tuple[str, ...]) -> None:
    result = _run_cli_help(*args)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith("usage:")


def test_rate_limit_import_has_no_startup_noise() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import dev_health_ops.api.middleware.rate_limit",
        ],
        check=False,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
