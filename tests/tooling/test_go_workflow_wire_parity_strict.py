"""The go.yml graphql-wire-parity job runs the check strictly, paired or not."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"
STEP_NAME = "Run graphql-wire-parity check against this repo's HEAD"


def _job() -> dict[str, Any]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return workflow["jobs"]["graphql-wire-parity"]


def _step() -> dict[str, Any]:
    steps = _job()["steps"]
    matching = [step for step in steps if step.get("name") == STEP_NAME]
    assert len(matching) == 1, [step.get("name") for step in steps]
    return matching[0]


def _run(env_overrides: dict[str, str]) -> list[str]:
    """Execute the step's own script with a recording `pnpm`; return its argv."""
    script = _step()["run"].replace("${{ github.workspace }}", "/ws")
    assert "${{" not in script, script
    with tempfile.TemporaryDirectory() as directory:
        log = Path(directory) / "pnpm.log"
        pnpm = Path(directory) / "pnpm"
        pnpm.write_text(
            '#!/usr/bin/env bash\nprintf \'%s\\n\' "$@" > "$PNPM_LOG"\n',
            encoding="utf-8",
        )
        pnpm.chmod(0o755)
        env = {
            **os.environ,
            "PATH": f"{directory}:{os.environ['PATH']}",
            "PNPM_LOG": str(log),
            **env_overrides,
        }
        result = subprocess.run(
            ["bash", "-e", "-c", script],
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return log.read_text(encoding="utf-8").splitlines()


def test_the_step_takes_no_tolerance_input() -> None:
    step = _step()
    assert "env" not in step, step.get("env")
    assert "tolerate" not in WORKFLOW.read_text(encoding="utf-8").lower()


def test_the_check_runs_with_the_ops_root_and_nothing_else() -> None:
    assert _run({}) == ["graphql:wire-parity:check", "--ops-root", "/ws"]


def test_an_ambient_tolerance_variable_changes_nothing() -> None:
    # The old step read TOLERATE_MANIFEST_ONLY; a leftover value in the
    # environment must not put a flag on the command.
    for value in ("1", "true", ""):
        assert _run({"TOLERATE_MANIFEST_ONLY": value}) == [
            "graphql:wire-parity:check",
            "--ops-root",
            "/ws",
        ]
