"""The relevance step of the `venue-oracles` workflow, EXECUTED in a scratch repo.

The workflow-shape tests (one declaring workflow, push to main and workflow_dispatch only, no path filter, no job-level
`if:`, the wired `github.event.before`) are in internal/cigates/workflows/workflows_venue_always_runs_test.go (CHAOS-10177); only the
two tests that execute the step stay here until the venue-script port (CHAOS-10178). Original rationale follows.

WHY THIS TEST EXISTS
---------------------
The hosted venue-oracles job is not a PR gate. A PR proves parity by
running the venue oracle of the package it changes locally, before review;
the merge waits only on the normal CI. The job runs on every push to main
(its result there is informational for merges: a red run is fixed forward or
reverted, and no group roll starts until it is green) and on
workflow_dispatch.

WHAT THIS PINS
---------------
1. The job id `venue-oracles` is declared in exactly one workflow file, so
   one commit has exactly one `venue-oracles` context.
2. That workflow triggers on `push` to main and on `workflow_dispatch`, and
   on no pull request or merge-queue event. A `pull_request` trigger added
   back makes it a PR check again; a lost `push` trigger means main is never
   checked, silently.
3. The `push` trigger has no path filter: a filter skips the run on main
   with no record. Relevance is decided INSIDE the job (cmd/cigate plan-verdict),
   which reports its skip.
4. The job has no job-level `if:` (GitHub reports a skipped job as Success).
5. The relevance step takes the push range from `github.event.before`, and
   a workflow_dispatch run (which has no push range) runs the full suite.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"

JOB_ID = "venue-oracles"


def _workflows_declaring_job(job_id: str) -> list[Path]:
    declaring: list[Path] = []
    for path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            continue
        jobs = document.get("jobs") or {}
        if job_id in jobs:
            declaring.append(path)
    return declaring


def _job_definition(path: Path, job_id: str) -> dict:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return (document.get("jobs") or {})[job_id]


def _relevance_script() -> str:
    (declaring,) = _workflows_declaring_job(JOB_ID)
    job = _job_definition(declaring, JOB_ID)
    (step,) = [s for s in job.get("steps") or [] if s.get("id") == "relevance"]
    return step["run"]


def _run_relevance_step(
    tmp_path: Path, changed_file: str | None, event_name: str
) -> tuple[str, str]:
    """Execute the workflow's own relevance step in a scratch repo.

    The repo holds the real ci/go_relevant_diff.sh and go.yml, one base commit,
    and one pushed commit that changes `changed_file`. Returns the step's
    GITHUB_OUTPUT and stdout.
    """
    repo = tmp_path / "repo"
    for relative in (
        "ci/go_relevant_diff.sh",
        ".github/workflows/go.yml",
    ):
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO_ROOT / relative, target)

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=repo, capture_output=True, text=True, check=True
        ).stdout.strip()

    git("init", "-q")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "test")
    git("add", "-A")
    git("commit", "-q", "-m", "base")
    base = git("rev-parse", "HEAD")
    if changed_file is not None:
        target = repo / changed_file
        target.parent.mkdir(parents=True, exist_ok=True)
        # Append a comment rather than overwrite: a copied script must keep
        # working, or the step fails open and passes for the wrong reason.
        previous = target.read_text(encoding="utf-8") if target.exists() else ""
        target.write_text(previous + "\n# changed\n", encoding="utf-8")
        git("add", "-A")
        git("commit", "-q", "-m", "push")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # A wrapper, not a symlink: a symlinked venv interpreter no longer finds
    # its pyvenv.cfg, and the decider needs the venv's PyYAML.
    python = bin_dir / "python"
    python.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n', encoding="utf-8")
    python.chmod(0o755)
    output = tmp_path / "github_output"
    output.write_text("", encoding="utf-8")
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin:/usr/local/bin",
        "HOME": os.environ.get("HOME", str(tmp_path)),
        "GITHUB_OUTPUT": str(output),
        "EVENT_NAME": event_name,
        "BASE_SHA": base if event_name == "push" else "",
    }
    proc = subprocess.run(
        ["bash", "-c", _relevance_script()],
        cwd=repo,
        capture_output=True,
        text=True,
        env=env,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    return output.read_text(encoding="utf-8"), proc.stdout


@pytest.mark.parametrize(
    ("changed_file", "want", "reason"),
    [
        # The gate itself: go.yml's pattern list names none of these files,
        # and a change to any of them must still run the suite that proves it.
        (".github/workflows/venue-oracles.yml", "relevant=true", "gate itself changed"),
        ("ci/venue_oracle_discovery.awk", "relevant=true", "gate itself changed"),
        ("ci/go_relevant_diff.sh", "relevant=true", "gate itself changed"),
    ],
)
def test_a_push_to_main_runs_the_suite_when_the_change_needs_it(
    tmp_path: Path, changed_file: str, want: str, reason: str
) -> None:
    """Runs the relevance step itself, not a text search of it. The reason
    line pins the branch that decided, so a fail-open cannot pass for it."""
    output, stdout = _run_relevance_step(tmp_path, changed_file, "push")
    assert output.splitlines() == [want], (
        f"a push changing only {changed_file} wrote {output!r} to "
        f"GITHUB_OUTPUT, want exactly {want!r}. stdout: {stdout!r}"
    )
    assert reason in stdout and "failing open" not in stdout, (
        f"a push changing only {changed_file} decided on the wrong branch: "
        f"want {reason!r} in stdout, got {stdout!r}"
    )


def test_a_dispatch_run_writes_relevant_true(tmp_path: Path) -> None:
    """Runs the step for a workflow_dispatch event: no push range, full suite."""
    output, stdout = _run_relevance_step(tmp_path, None, "workflow_dispatch")
    assert output.splitlines() == ["relevant=true"], (
        f"a workflow_dispatch run wrote {output!r} to GITHUB_OUTPUT, want "
        f"exactly 'relevant=true'. stdout: {stdout!r}"
    )
