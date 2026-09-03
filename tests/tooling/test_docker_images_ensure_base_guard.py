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

MIRRORED = 'ARG PYTHON_BASE_IMAGE="ghcr.io/full-chaos/python:3.14-slim@sha256:aa"'
MIRRORED_OTHER_OWNER = 'ARG PYTHON_BASE_IMAGE="ghcr.io/other-owner/python@sha256:aa"'
# Assembled from parts: no single line contains "ghcr.io/<owner>/python", which
# is what defeated the first discriminator (codex round 1, HIGH).
MIRRORED_VIA_ARGS = (
    "ARG REGISTRY=ghcr.io\n"
    "ARG OWNER=full-chaos\n"
    'ARG PYTHON_BASE_IMAGE="${REGISTRY}/${OWNER}/python:3.14-slim@sha256:aa"'
)
UNMIRRORED = "FROM python:3.14-slim"
# The marker a stubbed derivation script prints. Seeing it is the ONLY proof that
# control reached the real work rather than merely declining to skip.
DERIVATION_MARKER = "DERIVATION-REACHED"


def _guard_script() -> str:
    """Return the step's WHOLE run block as the shell receives it.

    Two things this deliberately does not do:

    * It does not return only the guard prologue. An earlier version split on
      "# ONE derivation" and tested the prologue alone, so "falls through" really
      asserted "the prologue did not skip". Inserting ``exit 0`` immediately
      before that boundary -- which prevents the ensure work entirely -- still
      gave 5 passed. The test could not detect a guard that silently does
      nothing, which is the failure it exists to catch.

    * It does not take the FIRST matching step. `go-build` has no ensure-base
      step and arguably should; if one is added, returning the first match would
      exercise whichever job dict-orders first and report success while leaving
      the other guard untested. Duplication and renaming are both "silently
      selects the wrong step"; the rename was caught and the duplicate was not.
    """
    document = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8")) or {}
    matches = [
        step
        for job in (document.get("jobs") or {}).values()
        for step in (job.get("steps") or [])
        if str(step.get("name", "")).startswith(STEP_PREFIX)
    ]
    assert matches, (
        f"{WORKFLOW.name}: no step named {STEP_PREFIX!r}. Either it was renamed or "
        "removed; either way the guard this test describes is not being exercised."
    )
    assert len(matches) == 1, (
        f"{WORKFLOW.name}: {len(matches)} steps named {STEP_PREFIX!r}. This test "
        "would exercise only one of them and report success for both. If a second "
        "ensure-base step is intentional, parametrise this test over them."
    )
    return matches[0]["run"].replace("${{ github.repository_owner }}", "full-chaos")


def _run_guard(
    tmp_path: Path, dockerfile: str | None, *, script: bool
) -> tuple[int, str]:
    (tmp_path / "docker").mkdir(exist_ok=True)
    (tmp_path / "ci").mkdir(exist_ok=True)
    # dockerfile=None means "no docker/Dockerfile at all" -- the grep status-2
    # case. An unreadable tree is not a pre-mirror tree.
    if dockerfile is not None:
        (tmp_path / "docker" / "Dockerfile").write_text(
            dockerfile + "\n", encoding="utf-8"
        )
    if script:
        # Stubbed to PRINT A MARKER and then fail, so the run block stops before
        # its real registry work. Seeing the marker is the ONLY proof control
        # reached the derivation; "exit 0 and no skip message" is equally
        # consistent with the guard having done nothing at all, which is the
        # defect an earlier version of this test could not detect.
        (tmp_path / "ci" / "python_base_ref.sh").write_text(
            f"echo {DERIVATION_MARKER} >&2\nexit 9\n", encoding="utf-8"
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
    ("dockerfile", "script", "want_skip", "want_error", "want_derivation", "why"),
    [
        (
            UNMIRRORED,
            False,
            True,
            False,
            False,
            "pre-mirror tree: nothing to ensure, decline",
        ),
        (
            MIRRORED,
            False,
            False,
            True,
            False,
            "declares PYTHON_BASE_IMAGE, no derivation: broken tree, fail loudly",
        ),
        (
            MIRRORED_OTHER_OWNER,
            False,
            False,
            True,
            False,
            "mirrored under another owner: still mirrored, still must fail",
        ),
        # codex round 1, HIGH: reproduced skipping on this shape before the fix.
        (
            MIRRORED_VIA_ARGS,
            False,
            False,
            True,
            False,
            "base assembled from ARGs: no line matches a registry regex, must NOT skip",
        ),
        # codex round 1, MEDIUM: reproduced skipping on grep status 2.
        (
            None,
            False,
            False,
            True,
            False,
            "docker/Dockerfile missing entirely: unreadable is not pre-mirror",
        ),
        (
            MIRRORED,
            True,
            False,
            False,
            True,
            "script present: must reach the derivation, not merely decline to skip",
        ),
        (
            UNMIRRORED,
            True,
            False,
            False,
            True,
            "script present on a pre-mirror Dockerfile: must still reach it",
        ),
    ],
)
def test_guard_branches(
    tmp_path: Path,
    dockerfile: str | None,
    script: bool,
    want_skip: bool,
    want_error: bool,
    want_derivation: bool,
    why: str,
) -> None:
    code, output = _run_guard(tmp_path, dockerfile, script=script)
    skipped = SKIP_MARKER in output
    errored = "::error::" in output
    reached = DERIVATION_MARKER in output

    # Three independent axes. Exit status alone cannot tell a deliberate skip from
    # a fall-through, and neither can distinguish a fall-through that reaches the
    # derivation from one that quietly does nothing -- the defect an earlier
    # version of this test could not detect.
    assert skipped is want_skip, (
        f"{why}: skipped={skipped}, wanted {want_skip}\n{output}"
    )
    assert errored is want_error, (
        f"{why}: ::error:: emitted={errored}, wanted {want_error}. A bare non-zero "
        f"exit is not enough; the failure must say what it could not classify.\n{output}"
    )
    assert reached is want_derivation, (
        f"{why}: derivation reached={reached}, wanted {want_derivation}. This is the "
        f"assertion that fails if the guard exits before the real work.\n{output}"
    )
    if want_error:
        assert code == 1, (
            f"{why}: expected exit 1 alongside ::error::, got {code}\n{output}"
        )
    if want_skip:
        assert code == 0, f"{why}: a deliberate skip must exit 0, got {code}\n{output}"
