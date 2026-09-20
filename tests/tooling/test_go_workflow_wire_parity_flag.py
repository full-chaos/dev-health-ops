"""The go.yml graphql-wire-parity job tolerates a manifest-only operation only when unpaired."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"
STEP_NAME = "Run graphql-wire-parity check against this repo's HEAD"


def _step() -> dict[str, Any]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["graphql-wire-parity"]["steps"]
    matching = [step for step in steps if step.get("name") == STEP_NAME]
    assert len(matching) == 1, [step.get("name") for step in steps]
    return matching[0]


def _run(tolerate: str | None) -> list[str]:
    """Execute the step's own script with a recording `pnpm`; return its argv."""
    step = _step()
    script = step["run"].replace("${{ github.workspace }}", "/ws")
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
        }
        env.pop("TOLERATE_MANIFEST_ONLY", None)
        if tolerate is not None:
            env["TOLERATE_MANIFEST_ONLY"] = tolerate
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


def test_tolerance_is_decided_by_the_resolved_web_ref() -> None:
    expression = _step()["env"]["TOLERATE_MANIFEST_ONLY"]
    assert expression == (
        "${{ steps.resolve-web-ref.outputs.ref == 'main' && '1' || '' }}"
    )


@pytest.mark.parametrize(
    ("tolerate", "expect_flag"),
    [("1", True), ("", False), (None, False), ("0", False), ("true", False)],
)
def test_flag_is_passed_only_for_the_exact_unpaired_value(
    tolerate: str | None, expect_flag: bool
) -> None:
    argv = _run(tolerate)
    assert ("--tolerate-manifest-only" in argv) is expect_flag
    assert argv[:3] == ["graphql:wire-parity:check", "--ops-root", "/ws"]
