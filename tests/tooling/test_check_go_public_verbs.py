"""Public command contract for the Go quality-gate wrapper."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

PUBLIC_VERBS = (
    "fmt",
    "vet",
    "test",
    "race",
    "live-python-oracles",
    "build",
    "contract",
    "grant-advisory",
    "integration-vet",
    "integration-coverage",
    "integration",
    "fast",
    "all",
)


def test_help_completes_and_documents_every_public_verb() -> None:
    """Help must not deadlock while Bash prepares its output transport."""

    result = subprocess.run(
        ["bash", "ci/check_go.sh", "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stderr == ""
    documented_verbs = {
        line.split(maxsplit=1)[0]
        for line in result.stdout.splitlines()
        if line.startswith("  ") and line.strip()
    }
    assert set(PUBLIC_VERBS) <= documented_verbs
