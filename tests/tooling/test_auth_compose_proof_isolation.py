"""The auth compose proof must not depend on a host-global container name or port (CHAOS-10182)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "check_auth_compose.sh"
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"


def _compose_files() -> list[Path]:
    text = SCRIPT.read_text(encoding="utf-8")
    line = re.search(r'docker compose -p "\$\{PROJECT\}"(?P<args>[^\n]*) "\$@"', text)
    assert line, "the proof's compose() wrapper moved"
    files = re.findall(r'-f "\$\{(ROOT|SCRIPT_DIR)\}/([^"]+)"', line.group("args"))
    return [
        (ROOT / "ci" if base == "SCRIPT_DIR" else ROOT) / rel for base, rel in files
    ]


@pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker compose is not installed"
)
def test_proof_postgres_has_no_fixed_name_or_published_port() -> None:
    argv = ["docker", "compose", "-p", "authproofx"]
    for f in _compose_files():
        argv += ["-f", str(f)]
    env = {
        **os.environ,
        "BUGSINK_SECRET_KEY": "x",
        "POSTGRES_PASSWORD": "x",
        "AUTH_RUNTIME_DATABASE_PASSWORD": "x",
    }
    result = subprocess.run(
        [*argv, "--profile", "auth", "config", "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        stdin=subprocess.DEVNULL,
    )
    assert result.returncode == 0, result.stderr
    postgres = json.loads(result.stdout)["services"]["postgres"]
    assert "container_name" not in postgres, (
        f"fixed container_name {postgres.get('container_name')!r} is host-global"
    )
    assert not postgres.get("ports"), (
        f"published host ports {postgres.get('ports')!r} collide with other stacks"
    )


def test_the_ci_job_runs_the_proof_beside_a_decoy_postgres() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    job = text[text.index("  go-auth-compose-proof:") :]
    job = job[: job.index("\n  graphql-wire-parity:")]
    assert "--name postgres" in job, (
        "the proof job no longer starts a decoy container named postgres"
    )
    assert job.index("--name postgres") < job.index("bash ci/check_auth_compose.sh")
    assert "docker inspect" in job.split("bash ci/check_auth_compose.sh", 1)[1], (
        "the decoy is not checked after the proof"
    )
