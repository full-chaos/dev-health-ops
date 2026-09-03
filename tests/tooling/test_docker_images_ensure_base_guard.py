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

import os
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
# A tree that RENAMES the ARG. This row documents an ACCEPTED LIMIT, not a
# protection: `grep PYTHON_BASE_IMAGE` finds nothing, exits 1, and 1 is the
# "readable file, positively lacks the ARG" case the skip path is defined on --
# so it SKIPS. There is no fourth branch for it to land in.
#
# UN-INDIRECTABLE IS NOT UN-RENAMEABLE. The ARG-name probe closes ASSEMBLY (a base
# built from ${REGISTRY}/${OWNER}/... cannot hide the ARG's own name, which was
# the HIGH and is real) and cannot close RENAMING. No single-token probe can.
#
# Deliberately NOT fixed: reaching it needs a triply inconsistent tree -- script
# absent, ARG renamed, mirrored base still consumed. The row exists so the next
# reader meets the limit as a recorded fact rather than discovering it as a defect
# and widening the discriminator to chase it.
#
# Two reviewers asserted in prose that this case "fails loudly". Both were wrong.
# Written as a row, it would have failed on its first run.
RENAMED_ARG = 'ARG PY_BASE="ghcr.io/full-chaos/python:3.14-slim@sha256:aa"'
# The marker a stubbed derivation script prints. Seeing it is the ONLY proof that
# control reached the real work rather than merely declining to skip.
# Recorded by the `docker` shim below. Seeing an `imagetools` invocation is the
# ONLY proof the ensure WORK ran. An earlier version stubbed the derivation script
# to print a marker and abort, which proved the SCRIPT was reached and nothing
# more -- inserting `exit 0` after the derivation call still passed every axis
# (codex round 2, warning). That is the same defect one level deeper than the one
# the marker was introduced to fix.
DOCKER_LOG = "docker-invocations.txt"

# A tree that consumes the mirror via a DIRECT FROM, with no ARG anywhere. The
# ARG-name probe alone skips this (codex round 2, critical 1) -- and the registry
# regex it replaced would have caught it, so the token-only version was a
# regression. Requires the conjunction.
MIRRORED_DIRECT_FROM = "FROM ghcr.io/full-chaos/python:3.14-slim@sha256:aa"

# A genuinely PRE-MIRROR tree that merely MENTIONS the token in a comment. The
# unstripped probe falsely refused this (codex round 2, critical 2).
# A genuinely PRE-MIRROR tree whose FROM line carries a TRAILING comment naming a
# mirrored ref. This is the shape comment-stripping exists for, and finding it took
# a mutation: my first fixture used a WHOLE-LINE comment, which the anchored probes
# already reject (a comment line starts with `#`, not ARG/FROM), so removing the
# stripping left that row passing and proved nothing. Here `.*` in the FROM probe
# spans the trailing comment and matches `ghcr.io/.../python` inside it -- a false
# refusal of a tree that pulls its base straight from Docker Hub.
PRE_MIRROR_TRAILING_COMMENT = (
    "# PYTHON_BASE_IMAGE arrives in CHAOS-4922\n"
    "FROM python:3.14-slim  # was ghcr.io/full-chaos/python before the revert"
)


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
) -> tuple[int, str, list[str]]:
    """Run the whole step, with `docker` shimmed so the ensure WORK is observable.

    The shim records every invocation and returns success, so the block runs to
    completion instead of dying at the first real registry call. What the test
    then asserts is the RECORDED INVOCATIONS -- an observation of the work, not an
    inference from "we got past the guard".
    """
    (tmp_path / "docker").mkdir(exist_ok=True)
    (tmp_path / "ci").mkdir(exist_ok=True)
    if dockerfile is not None:
        (tmp_path / "docker" / "Dockerfile").write_text(
            dockerfile + "\n", encoding="utf-8"
        )
    if script:
        # Emits what the real derivation emits, so the block proceeds normally.
        (tmp_path / "ci" / "python_base_ref.sh").write_text(
            "echo 'ghcr ghcr.io/full-chaos/python:3.14-slim@sha256:aa'\n"
            "echo 'upstream python:3.14-slim'\n",
            encoding="utf-8",
        )
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    log = tmp_path / DOCKER_LOG
    shim = bindir / "docker"
    shim.write_text(
        f'#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> {log}\nexit 0\n',
        encoding="utf-8",
    )
    shim.chmod(0o755)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    proc = subprocess.run(
        ["bash", "-c", _guard_script()],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    invocations = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    return proc.returncode, proc.stdout + proc.stderr, invocations


@pytest.mark.parametrize(
    ("dockerfile", "script", "want_skip", "want_error", "want_ensure", "why"),
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
        # round 1 HIGH: assembled reference, no line matches a registry regex.
        (
            MIRRORED_VIA_ARGS,
            False,
            False,
            True,
            False,
            "base assembled from ARGs: ARG-name probe must catch it",
        ),
        # round 2 CRITICAL 1: direct FROM, no ARG. The token probe alone SKIPS this.
        (
            MIRRORED_DIRECT_FROM,
            False,
            False,
            True,
            False,
            "direct mirrored FROM with no ARG: FROM-regex probe must catch it",
        ),
        # round 2 CRITICAL 2: token mentioned only in comments on a pre-mirror tree.
        (
            PRE_MIRROR_TRAILING_COMMENT,
            False,
            True,
            False,
            False,
            "mirrored ref only in a TRAILING comment: stripping must prevent refusal",
        ),
        # round 1 MEDIUM: unreadable is not pre-mirror.
        (
            None,
            False,
            False,
            True,
            False,
            "docker/Dockerfile missing entirely: unreadable is not pre-mirror",
        ),
        # Accepted limit: renaming defeats both probes. Asserted as TRUE behaviour.
        (
            RENAMED_ARG,
            False,
            True,
            False,
            False,
            "renamed ARG with an unmirrored-looking value: SKIPS (known limit)",
        ),
        # round 2 WARNING: these prove the ensure WORK ran, not that we got past
        # the guard. want_ensure is checked against RECORDED docker invocations.
        (
            MIRRORED,
            True,
            False,
            False,
            True,
            "script present: the ensure work must actually run",
        ),
        (
            UNMIRRORED,
            True,
            False,
            False,
            True,
            "script present on a pre-mirror Dockerfile: ensure work must still run",
        ),
    ],
)
def test_guard_branches(
    tmp_path: Path,
    dockerfile: str | None,
    script: bool,
    want_skip: bool,
    want_error: bool,
    want_ensure: bool,
    why: str,
) -> None:
    code, output, invocations = _run_guard(tmp_path, dockerfile, script=script)
    skipped = SKIP_MARKER in output
    errored = "::error::" in output
    ensured = any("imagetools" in call for call in invocations)

    assert skipped is want_skip, (
        f"{why}: skipped={skipped}, wanted {want_skip}\n{output}"
    )
    assert errored is want_error, (
        f"{why}: ::error:: emitted={errored}, wanted {want_error}. A bare non-zero "
        f"exit is not enough; the failure must say what it could not classify.\n{output}"
    )
    # The axis that survives an `exit 0` inserted anywhere before the registry
    # work: it observes the invocation rather than inferring it from control flow.
    assert ensured is want_ensure, (
        f"{why}: imagetools invoked={ensured}, wanted {want_ensure}. Recorded "
        f"docker calls: {invocations!r}\n{output}"
    )
    if want_error:
        assert code == 1, (
            f"{why}: expected exit 1 alongside ::error::, got {code}\n{output}"
        )
    if want_skip:
        assert code == 0, f"{why}: a deliberate skip must exit 0, got {code}\n{output}"
