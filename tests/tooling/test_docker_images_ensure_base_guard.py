"""The ensure-base step must decline on pre-mirror trees, not fail on them.

WHY THIS EXISTS (CHAOS-4949)

On a ``pull_request`` GitHub evaluates the workflow from the MERGE REF (branch +
main) while ``docker-images.yml``'s checkout pins to the PR HEAD::

    ref: ${{ github.event.inputs.ref || github.event.pull_request.head.sha || github.sha }}

So the STEP comes from main and the TREE comes from the PR branch, and those are
different commits. #2152 added an ensure-base step that runs
``ci/python_base_ref.sh`` -- safe on main, and fatal on any branch opened before
that script existed::

    bash: ci/python_base_ref.sh: No such file or directory

Measured with the tree state held constant, the only variable being run-creation
time relative to #2152 merging at 07:53:11Z:

    run 33726620458   created 07:08   step ABSENT from the job   PASSED
    run 33732034142   created 08:11   step PRESENT               FAILED

WHAT THIS ASSERTS

The guard's three branches, driven as shell against synthetic trees:

  1. script present                      -> falls through to the real work
  2. script absent, Dockerfile unmirrored -> skips, saying why
  3. script absent, Dockerfile MIRRORED   -> fails loudly

Branch 3 is the point. A bare ``[ -f ci/python_base_ref.sh ] || exit 0`` treats
ABSENCE AS EVIDENCE of the pre-mirror case, when absence is equally consistent
with a broken tree that consumes the mirrored base with no derivation available.
This guard instead ASSERTS the condition that makes skipping correct. Unrecognised
means dangerous, not safe.

TWO THINGS THIS TEST DOES DELIBERATELY

* It DISCRIMINATES ON OUTPUT, not on exit status. Two cases exit 0 for OPPOSITE
  reasons -- one skips on purpose, one falls through to the real work -- so an
  exit-code-only assertion scores them identically and proves nothing about which
  branch ran. That ambiguity is invisible in a passing test, which is exactly why
  it is called out here.

* It extracts the guard from the PARSED YAML rather than grepping the file, so
  what runs is what the shell receives after block-scalar stripping. A grep-based
  extraction tests the diff's appearance instead of the step's behaviour.

The extraction fails loudly if the step is renamed or missing, so this test cannot
pass vacuously against a workflow that no longer contains the thing it describes.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docker-images.yml"
STEP_PREFIX = "Ensure the Python base image"
SKIP_MARKER = "Nothing to ensure; skipping"

MIRRORED = "FROM ghcr.io/full-chaos/python:3.14-slim@sha256:aa"
MIRRORED_OTHER_OWNER = "FROM ghcr.io/other-owner/python:3.14-slim@sha256:aa"
UNMIRRORED = "FROM python:3.14-slim"


def _guard_script() -> str:
    """Return the guard prologue as the shell receives it.

    Parsed from the workflow rather than grepped, and raises if the step is gone
    -- a test that silently finds nothing to check is worse than no test.
    """
    document = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8")) or {}
    for job in (document.get("jobs") or {}).values():
        for step in job.get("steps") or []:
            if str(step.get("name", "")).startswith(STEP_PREFIX):
                body = step["run"].replace(
                    "${{ github.repository_owner }}", "full-chaos"
                )
                prologue, sep, _ = body.partition("# ONE derivation")
                assert sep, (
                    f"{WORKFLOW.name}: the ensure-base step no longer contains the "
                    "'# ONE derivation' boundary this test splits on; the guard may "
                    "have been restructured and this assertion has gone stale"
                )
                return prologue
    raise AssertionError(
        f"{WORKFLOW.name}: no step named {STEP_PREFIX!r}. Either it was renamed or "
        "removed; either way the guard this test describes is not being exercised."
    )


def _run_guard(tmp_path: Path, dockerfile: str, *, script: bool) -> tuple[int, str]:
    (tmp_path / "docker").mkdir(exist_ok=True)
    (tmp_path / "ci").mkdir(exist_ok=True)
    (tmp_path / "docker" / "Dockerfile").write_text(dockerfile + "\n", encoding="utf-8")
    if script:
        (tmp_path / "ci" / "python_base_ref.sh").write_text(
            "echo x\n", encoding="utf-8"
        )
    proc = subprocess.run(
        ["bash", "-c", _guard_script()],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return proc.returncode, proc.stdout + proc.stderr


@pytest.mark.parametrize(
    ("dockerfile", "script", "want_code", "want_skip", "why"),
    [
        (UNMIRRORED, False, 0, True, "pre-mirror tree: nothing to ensure, decline"),
        (
            MIRRORED,
            False,
            1,
            False,
            "consumes the mirror with no derivation: broken tree, fail loudly",
        ),
        (
            MIRRORED_OTHER_OWNER,
            False,
            1,
            False,
            "mirrored under another owner: still a mirrored base, still must fail",
        ),
        (MIRRORED, True, 0, False, "script present: guard is invisible, falls through"),
        (
            UNMIRRORED,
            True,
            0,
            False,
            "script present on a pre-mirror Dockerfile: must not skip",
        ),
    ],
)
def test_guard_branches(
    tmp_path: Path,
    dockerfile: str,
    script: bool,
    want_code: int,
    want_skip: bool,
    why: str,
) -> None:
    code, output = _run_guard(tmp_path, dockerfile, script=script)
    skipped = SKIP_MARKER in output
    assert code == want_code, f"{why}: exit {code}, wanted {want_code}\n{output}"
    # Asserted separately and deliberately: exit status alone cannot tell a
    # deliberate skip from falling through to the real work.
    assert skipped is want_skip, (
        f"{why}: skipped={skipped}, wanted {want_skip}. Exit status matched, so an "
        f"exit-code-only assertion would have passed here.\n{output}"
    )
