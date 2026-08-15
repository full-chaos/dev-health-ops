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
    "multi-replica-workers",
    "integration-vet",
    "integration-coverage",
    "integration-shard-plan",
    "integration-prepull",
    "integration-shard",
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
    # The Go toolchain directive may fetch a newer patch release on a cold
    # cache and announces it on stderr ("go: downloading go1.25.9 ...").
    # That is environment noise, not help output — anything else on stderr
    # still fails.
    unexpected_stderr = [
        line
        for line in result.stderr.splitlines()
        if line.strip() and not line.startswith("go: downloading ")
    ]
    assert unexpected_stderr == []
    documented_verbs = {
        line.split(maxsplit=1)[0]
        for line in result.stdout.splitlines()
        if line.startswith("  ") and line.strip()
    }
    assert set(PUBLIC_VERBS) <= documented_verbs


def test_multi_replica_gate_is_cold_measured_and_required() -> None:
    source = (ROOT / "ci" / "check_go.sh").read_text(encoding="utf-8")
    function = source.split("check_multi_replica_workers() {", 1)[1].split("\n}", 1)[0]

    assert "-tags=integration -count=1" in function
    assert "DEV_HEALTH_MULTI_REPLICA_PROOF" in function
    assert "measured zero jobs" in function
    # One public verb plus the required fast and all paths.
    assert source.count("    check_multi_replica_workers\n") == 3
