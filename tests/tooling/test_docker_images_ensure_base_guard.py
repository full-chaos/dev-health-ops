"""The ensure-base step must decline on pre-mirror trees, not fail on them.

WHY THIS EXISTS (CHAOS-4949)

On a ``pull_request`` GitHub evaluates the workflow from the MERGE REF while
``docker-images.yml``'s checkout pins to the PR HEAD, so the STEP comes from main
and the TREE comes from the PR branch. A branch opened before #2152 gets a step
that runs ``ci/python_base_ref.sh`` against a tree where that file does not exist.

Measured, tree state held constant, the only variable being run-creation time
relative to #2152 merging at 07:53:11Z::

    run 33726620458   created 07:08   step ABSENT from the job   PASSED
    run 33732034142   created 08:11   step PRESENT               FAILED

WHY THE GUARD IS STRUCTURAL AND NOT TEXTUAL

Three codex rounds found nine defects in a Dockerfile-parsing version of this
guard, every one a grammar feature the regexes did not model: assembled
references, a direct FROM with no ARG, lowercase ``from``, escaped-newline
continuations, RUN heredoc bodies read as instructions, missing token boundaries,
and the fact that ``#`` outside line start is argument text rather than a comment
-- which made "comment stripping" a literal-corrupting rewrite.

Each fix was correct and the next round found the next feature. The guard now
asks git instead: #2152 added ``ci/python_base_ref.sh`` and the mirrored
Dockerfile in ONE commit, so the script's presence IS the mirror.

WHAT THIS ASSERTS

Three states, built as real scratch git repositories rather than described:

  1. script present                    -> run (the ensure work is observed)
  2. absent, #2152 IS an ancestor      -> fail loudly (inconsistent tree)
  3. absent, #2152 is NOT an ancestor  -> skip, saying why

Plus that the build checkouts keep ``fetch-depth: 0``. Without full history
``merge-base --is-ancestor`` cannot answer, and a depth change would silently
turn state 2 into state 3 -- a wrong answer rather than an error.

TWO THINGS THIS DOES DELIBERATELY

* It runs the WHOLE step with a ``docker`` shim first on PATH that RECORDS
  ``buildx imagetools`` invocations, so "the ensure work ran" is an observation.
  An earlier version stubbed the derivation script to print a marker and abort,
  which proved the SCRIPT was reached and nothing more: inserting ``exit 0``
  after the derivation still passed every axis.

* It extracts the step from the PARSED yaml and asserts EXACTLY ONE match, so a
  renamed step fails loudly and a duplicated one (``go-build`` has no ensure-base
  step and arguably should) cannot leave one guard untested while reporting
  success.
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
# NOTE: comma, not semicolon. A `;` in this message made the step unparseable
# to tests/tooling's registry guard, which splits on `;` without respecting
# quotes -- see the comment beside the echo in docker-images.yml.
SKIP_MARKER = "Nothing to ensure, skipping"
# The #2152 merge, which the guard treats as the moment the mirror landed.
MIRROR_LANDED = "6fbd7dc8d4651b82f78b823a2b6f55cd53f0ff1f"


def _guard_script() -> str:
    """Return the step's whole run block as the shell receives it."""
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
        "would exercise only one and report success for both. If a second is "
        "intentional, parametrise this test over them."
    )
    return matches[0]["run"].replace("${{ github.repository_owner }}", "full-chaos")


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()


MIRRORED_FROM = (
    'ARG PYTHON_BASE_IMAGE="ghcr.io/full-chaos/python:3.14-slim@sha256:aa"\n'
    "FROM ${PYTHON_BASE_IMAGE} AS builder\n"
)
DIRECT_FROM = "FROM python:3.14-slim\n"
# A pre-mirror tree that references ghcr for something that is NOT the Python
# base. The guard must skip this: it predates the mirror and consumes no
# mirrored base, so failing it would break a branch doing nothing wrong.
OTHER_GHCR_FROM = (
    "FROM ghcr.io/full-chaos/other-tool:1.0 AS tools\nFROM python:3.14-slim\n"
)


def _scratch_repo(
    tmp_path: Path, *, script: bool, mirrored: bool = False, other_ghcr: bool = False
) -> Path:
    """Build a real repo in one of the states the guard distinguishes.

    ``mirrored`` controls whether docker/Dockerfile names the ghcr mirror. It is
    the axis the cherry-pick residual turns on: a tree can carry the mirrored
    Dockerfile while lacking the derivation script.
    """
    name = "scratch-with-script" if script else "scratch-no-script"
    if mirrored:
        name += "-mirrored"
    if other_ghcr:
        name += "-otherghcr"
    repo = tmp_path / name
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.invalid")
    _git(repo, "config", "user.name", "test")
    (repo / "seed").write_text("seed\n", encoding="utf-8")
    _git(repo, "add", "seed")
    _git(repo, "commit", "-qm", "seed")
    (repo / "docker").mkdir()
    if other_ghcr:
        dockerfile_text = OTHER_GHCR_FROM
    elif mirrored:
        dockerfile_text = MIRRORED_FROM
    else:
        dockerfile_text = DIRECT_FROM
    (repo / "docker" / "Dockerfile").write_text(dockerfile_text, encoding="utf-8")
    (repo / "ci").mkdir()
    if script:
        (repo / "ci" / "python_base_ref.sh").write_text(
            "echo 'ghcr ghcr.io/full-chaos/python:3.14-slim@sha256:aa'\n"
            "echo 'upstream python:3.14-slim'\n",
            encoding="utf-8",
        )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "state")
    return repo


def _run_guard(
    repo: Path, tmp_path: Path, *, mirror_sha: str | None = None
) -> tuple[int, str, list[str]]:
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    log = tmp_path / "docker-invocations.txt"
    shim = bindir / "docker"
    shim.write_text(
        f'#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> {log}\nexit 0\n',
        encoding="utf-8",
    )
    shim.chmod(0o755)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    script = _guard_script()
    if mirror_sha is not None:
        script = script.replace(MIRROR_LANDED, mirror_sha)
    proc = subprocess.run(
        ["bash", "-c", script],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    calls = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    return proc.returncode, proc.stdout + proc.stderr, calls


def test_script_present_runs_the_ensure_work(tmp_path: Path) -> None:
    """State 1. Presence of the script IS the mirror; the work must actually run."""
    repo = _scratch_repo(tmp_path, script=True)
    code, out, calls = _run_guard(repo, tmp_path)
    assert SKIP_MARKER not in out, f"must not skip when the script is present\n{out}"
    assert any("imagetools" in c for c in calls), (
        f"the ensure work must actually run, not merely be reached. Recorded "
        f"docker calls: {calls!r}\n{out}"
    )


def test_absent_and_not_ancestor_skips(tmp_path: Path) -> None:
    """State 3. A genuine pre-mirror tree: decline, and say why.

    The sha must EXIST in the repository while not being an ancestor of HEAD.
    That distinction is the whole point and my first version of this test missed
    it: a scratch repo that has never heard of the sha makes `--is-ancestor` exit
    128, which the guard correctly treats as "cannot answer" and fails closed on.
    A real PR branch is different -- it lives in a repo where #2152's object
    exists on main, so the question is answerable and the answer is "no".
    Building the object on a side branch reproduces the real situation; omitting
    it tests a state that cannot occur and would have argued for weakening the
    128 handling.
    """
    repo = _scratch_repo(tmp_path, script=False)
    _git(repo, "checkout", "-q", "-b", "sidebranch")
    (repo / "side").write_text("side\n", encoding="utf-8")
    _git(repo, "add", "side")
    _git(repo, "commit", "-qm", "side commit -- exists but is not an ancestor of main")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "main")
    # NEGATIVE CONTROL for the cherry-pick check. This row is only a skip case
    # because the Dockerfile does NOT name the mirror; assert that rather than
    # rely on _scratch_repo's default, so a change to the default surfaces here
    # instead of silently turning this into a different test.
    assert "ghcr.io/full-chaos/" not in (repo / "docker" / "Dockerfile").read_text(
        encoding="utf-8"
    ), "this row's premise is a Dockerfile that does not reference the mirror"
    code, out, calls = _run_guard(repo, tmp_path, mirror_sha=side)
    assert code == 0, f"a deliberate skip must exit 0, got {code}\n{out}"
    assert SKIP_MARKER in out, f"the skip must say why\n{out}"
    assert not calls, f"a skip must not invoke docker at all: {calls!r}"


def test_absent_but_ancestor_fails_loudly(tmp_path: Path) -> None:
    """State 2. Script gone from a tree that contains #2152: inconsistent, not pre-mirror."""
    repo = _scratch_repo(tmp_path, script=False)
    # Make MIRROR_LANDED resolvable AND an ancestor by pointing a ref at HEAD~,
    # then asking the guard the same question against that ref.
    head = _git(repo, "rev-parse", "HEAD")
    _git(repo, "update-ref", f"refs/heads/pretend-{MIRROR_LANDED[:8]}", head)
    script = _guard_script().replace(MIRROR_LANDED, head)
    bindir = tmp_path / "bin2"
    bindir.mkdir(exist_ok=True)
    (bindir / "docker").write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    (bindir / "docker").chmod(0o755)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    proc = subprocess.run(
        ["bash", "-c", script],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, (
        f"an inconsistent tree must fail, got {proc.returncode}\n{out}"
    )
    assert "::error::" in out, f"the failure must say what it found\n{out}"
    assert SKIP_MARKER not in out, f"must not skip an inconsistent tree\n{out}"


def test_authoring_branch_shape_runs(tmp_path: Path) -> None:
    """Script present, #2152's merge ABSENT -- the authoring branch's own shape.

    `0c1d9ff66`, the branch that introduced #2152, has ci/python_base_ref.sh
    while NOT containing 6fbd7dc8d: it *is* the change, so it precedes its own
    merge commit. This is why "script present" must be checked BEFORE ancestry
    -- a correctness requirement, not a convenience. Reversing them would fail
    every #2152-descended feature branch that has not merged main.
    """
    repo = _scratch_repo(tmp_path, script=True)
    # A resolvable sha that is NOT an ancestor: the authoring-branch situation.
    _git(repo, "checkout", "-q", "-b", "sidebranch")
    (repo / "side").write_text("side\n", encoding="utf-8")
    _git(repo, "add", "side")
    _git(repo, "commit", "-qm", "not an ancestor of main")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "main")
    code, out, calls = _run_guard(repo, tmp_path, mirror_sha=side)
    assert SKIP_MARKER not in out, (
        f"the authoring branch must NOT be treated as pre-mirror -- it has the "
        f"script\n{out}"
    )
    assert "::error::" not in out, f"and must not be treated as inconsistent\n{out}"
    assert any("imagetools" in c for c in calls), (
        f"the ensure work must run for the authoring branch. Recorded: {calls!r}\n{out}"
    )


def test_unresolvable_mirror_sha_fails_loudly(tmp_path: Path) -> None:
    """The THIRD value of `--is-ancestor`, which nothing else here constrains.

    0 = ancestor, 1 = not an ancestor, anything else (128 on a missing object,
    e.g. a shallow clone) = could not answer. Collapsing >=2 into "not an
    ancestor" would make an unanswerable history skip silently -- the same
    grep-status-1-vs-2 collapse this guard already made once, and the same shape
    as a default that turns "I don't know" into an answer.

    Added because a mutation exposed it: changing `-eq 1` to `-ge 1` left every
    other test passing. Careful three-valued handling with no row for the third
    value is a decision nothing observes.
    """
    repo = _scratch_repo(tmp_path, script=False)
    # A well-formed sha that exists nowhere: `--is-ancestor` exits 128, not 1.
    absent_sha = "0" * 40
    code, out, calls = _run_guard(repo, tmp_path, mirror_sha=absent_sha)
    assert code == 1, f"an unanswerable history must fail, got {code}\n{out}"
    assert "::error::" in out, f"the failure must say it could not answer\n{out}"
    assert SKIP_MARKER not in out, (
        f"an unresolvable sha must NOT be treated as pre-mirror -- that is the "
        f"collapse this guard exists to avoid\n{out}"
    )
    assert not calls, f"must not invoke docker: {calls!r}"


def test_shallow_repository_refuses(tmp_path: Path) -> None:
    """A shallow clone cannot answer the ancestry question, so the guard refuses.

    Added because a mutation exposed it: replacing the `--is-shallow-repository`
    check with `if false` left every other test passing. That is the third time
    on this guard that a deliberately-written branch had nothing observing it --
    the >=2 exit path, the ensure work itself, and now this. A guard clause with
    no row is a decision nobody can see.

    Note the refusal is not redundant with the 128 handling below. 084-prod
    measured that a depth-1 clone reaches 128 because the OBJECT IS ABSENT, not
    because git reports truncation -- so a shallow repo that somehow had the
    object present-but-unreachable would return 1, a wrong answer rather than an
    error. Refusing on shallowness catches the class; the 128 path catches the
    instance.
    """
    source = _scratch_repo(tmp_path, script=False)
    shallow = tmp_path / "shallow"
    subprocess.run(
        ["git", "clone", "--depth", "1", f"file://{source}", str(shallow)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert _git(shallow, "rev-parse", "--is-shallow-repository") == "true", (
        "the fixture must actually be shallow, or this row proves nothing"
    )
    (shallow / "ci").mkdir(exist_ok=True)
    code, out, calls = _run_guard(shallow, tmp_path)
    assert code == 1, f"a shallow repository must be refused, got {code}\n{out}"
    assert "::error::" in out, f"the refusal must say why\n{out}"
    # DISCRIMINATING ASSERTION. Both paths error and both mention "shallow", so
    # a substring check on that word passes whichever fires -- verified by
    # mutation: removing the refusal left this row green, because the repo then
    # fell through to the 128 handler, which also exits 1 with an ::error::
    # naming shallow clones. The row proved the OUTCOME and not the PATH.
    #
    # `this is a shallow repository` appears only in the pre-flight refusal, so
    # it identifies which branch ran.
    assert "this is a shallow repository" in out, (
        f"the pre-flight shallow refusal must be what fires. If this fails while "
        f"the exit code is still 1, the 128 handler caught it instead -- correct "
        f"outcome, wrong path, and the refusal is doing nothing.\n{out}"
    )
    assert SKIP_MARKER not in out, f"must not treat a shallow repo as pre-mirror\n{out}"


def test_build_checkouts_keep_full_history() -> None:
    """`--is-ancestor` cannot answer on a shallow clone.

    A depth change would turn state 2 into state 3 -- a WRONG answer rather than
    an error -- so the guard's correctness depends on this and it is asserted
    here rather than assumed.
    """
    document = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8")) or {}
    checkouts = [
        (job_name, step)
        for job_name in ("build", "go-build")
        for step in (document["jobs"][job_name].get("steps") or [])
        if "actions/checkout" in str(step.get("uses", ""))
    ]
    assert checkouts, "no actions/checkout step found in build/go-build"
    # The guard fires on pull_request, so the checkout that matters is the one
    # those runs use -- both build jobs check out via the same step, and both
    # are asserted rather than just the first found.
    assert len(checkouts) >= 2, (
        f"expected a checkout in each of build and go-build, found {len(checkouts)}"
    )
    for job_name, step in checkouts:
        depth = (step.get("with") or {}).get("fetch-depth")
        assert str(depth) == "0", (
            "the ensure-base guard resolves #2152 by ancestry, which needs full "
            f"history; {job_name}'s checkout has fetch-depth={depth!r}. A shallow "
            "checkout does not make the guard error -- it makes `--is-ancestor` "
            "return 1 (object absent reads as 'not an ancestor' only if the object "
            "is reachable-but-absent), i.e. a WRONG ANSWER rather than a failure."
        )


def test_cherry_picked_mirror_dockerfile_without_script_fails_loudly(
    tmp_path: Path,
) -> None:
    """Round 4's P1: the residual the ancestry-only guard left open.

    A branch that cherry-picks #2152's Dockerfile WITHOUT ci/python_base_ref.sh
    has 6fbd7dc8d as a non-ancestor, so ancestry alone concludes "pre-mirror"
    and skips -- while the tree consumes a mirrored base whose digest may never
    have been mirrored. docker/Dockerfile holds the digest and the script only
    reads it, so that state is expressible.

    Before the fixed-string check this printed `Nothing to ensure; skipping.`,
    a confident false statement, and the build then failed pulling a ghcr ref
    nothing had created.

    The assertion names the cherry-pick message specifically, NOT merely a
    non-zero exit: the ancestor branch and the >=2 branch also exit 1 with an
    `::error::`, so an outcome-shaped assertion would pass whichever fired.
    """
    repo = _scratch_repo(tmp_path, script=False, mirrored=True)
    assert "ghcr.io/full-chaos/" in (repo / "docker" / "Dockerfile").read_text(
        encoding="utf-8"
    ), "the fixture must actually name the mirror, or this row proves nothing"
    _git(repo, "checkout", "-q", "-b", "sidebranch")
    (repo / "side").write_text("side\n", encoding="utf-8")
    _git(repo, "add", "side")
    _git(repo, "commit", "-qm", "side commit -- exists but is not an ancestor of main")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "main")
    code, out, calls = _run_guard(repo, tmp_path, mirror_sha=side)
    assert code == 1, f"a cherry-picked mirror Dockerfile must fail, got {code}\n{out}"
    assert "cherry-picked mirror Dockerfile without" in out, (
        f"the failure must name the cherry-pick case. If this fails while the "
        f"exit code is 1, a different error branch fired -- right outcome, "
        f"wrong path.\n{out}"
    )
    assert SKIP_MARKER not in out, f"must not also claim it skipped\n{out}"
    assert not calls, f"a refusal must not invoke docker: {calls!r}"


def test_shallow_with_present_non_ancestor_target_is_refused(
    tmp_path: Path,
) -> None:
    """Shallow, target PRESENT, and genuinely not an ancestor: refuse anyway.

    DOCSTRING CORRECTED (084-prod). An earlier version of this row claimed it
    built the "wrong answer" state. It does not: the target here is a side
    branch, so it really is not an ancestor, and `--is-ancestor` returning 1 is
    CORRECT -- a full clone agrees. Believing otherwise is the trap 084-prod hit
    first time and caught only by pinning the truth from the source repo.

    What this row does assert, and it is worth asserting: the refusal fires on a
    shallow repository REGARDLESS of what ancestry would have said. It is a
    working detector -- narrow the refusal to `is-shallow && ! cat-file -e` and
    the object's presence stops it firing, the guard skips, and this goes red.

    The genuinely-wrong-answer case is the row below, where the target IS an
    ancestor and 1 is provably false. Both are needed: this one covers the
    refusal's breadth, that one covers the reason it exists.
    """
    source = _scratch_repo(tmp_path, script=False)
    _git(source, "checkout", "-q", "-b", "sidebranch")
    (source / "side").write_text("side\n", encoding="utf-8")
    _git(source, "add", "side")
    _git(source, "commit", "-qm", "side commit")
    side = _git(source, "rev-parse", "HEAD")
    _git(source, "checkout", "-q", "main")
    # THE PREMISE, STATED AS AN ASSERTION: this target is NOT an ancestor, so 1
    # is the honest answer and this row is about breadth, not wrongness.
    assert (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", side, "main"], cwd=source
        ).returncode
        == 1
    ), "this row's premise is a target that genuinely is not an ancestor"

    shallow = tmp_path / "shallow-present-nonancestor"
    subprocess.run(
        ["git", "clone", "--depth", "1", f"file://{source}", str(shallow)],
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(
        ["git", "fetch", "--depth", "1", "origin", "sidebranch"],
        cwd=shallow,
        capture_output=True,
        text=True,
        check=True,
    )
    assert _git(shallow, "rev-parse", "--is-shallow-repository") == "true", (
        "the fixture must be shallow"
    )
    assert (
        subprocess.run(
            ["git", "cat-file", "-e", f"{side}^{{commit}}"], cwd=shallow
        ).returncode
        == 0
    ), "the target object must be PRESENT, or the narrowing mutation survives"

    (shallow / "ci").mkdir(exist_ok=True)
    code, out, calls = _run_guard(shallow, tmp_path, mirror_sha=side)
    assert code == 1, f"a shallow repository must be refused, got {code}\n{out}"
    assert "this is a shallow repository" in out, (
        f"the pre-flight refusal must fire even though ancestry could have "
        f"answered honestly here -- shallowness alone is the refusal's "
        f"trigger.\n{out}"
    )
    assert SKIP_MARKER not in out, f"must not treat this as pre-mirror\n{out}"
    assert not calls, f"a refusal must not invoke docker: {calls!r}"


def test_shallow_with_target_present_but_unreachable_is_refused(
    tmp_path: Path,
) -> None:
    """Round 4's P2: the case the previous shallow row did NOT observe.

    test_shallow_repository_refuses clones at depth 1 from a repo that has never
    heard of the target sha, so the target is ABSENT. That makes this mutation
    survive -- verified, it left all seven rows green::

        if [ "$(git rev-parse --is-shallow-repository ...)" = true ] \\
          && ! git cat-file -e "${MIRROR_LANDED}^{commit}" 2>/dev/null; then

    THE FIXTURE MUST BE A TRUE ANCESTOR, AND THAT IS THE WHOLE ROW (084-prod).
    My first version fetched a SIDE BRANCH: object present, unreachable,
    `--is-ancestor` returned 1, and all three assertions passed -- while proving
    nothing, because a side commit genuinely is not an ancestor, so 1 was the
    CORRECT answer reached for the wrong reason. The row passed whether or not
    the guard did anything. 084-prod built the same false positive independently
    and caught it only by pinning the truth first.

    So the target here IS an ancestor of main, and the row asserts `exit 0` in
    the FULL repo before cloning. Only then does `1` in the shallow clone mean
    what this row claims: a WRONG answer, silently, with no error -- the state
    where the guard would take the skip path and print the pre-mirror message on
    a tree that is nothing of the sort.

    The trap is the file's third of this shape: a fixture in a state where the
    correct and the incorrect answer coincide cannot discriminate.

    Fetching a bare sha is refused server-side ("not our ref"), which is why
    084-prod's earlier attempt failed; fetching a REF NAME works.
    """
    source = _scratch_repo(tmp_path, script=False)
    # A third commit, so the target can be a non-parent ancestor rather than
    # HEAD's immediate parent.
    (source / "third").write_text("third\n", encoding="utf-8")
    _git(source, "add", "third")
    _git(source, "commit", "-qm", "third")
    ancestor = _git(source, "rev-parse", "HEAD~2")
    _git(source, "branch", "ancref", ancestor)

    # PIN THE TRUTH FIRST, in the full repo, where the answer is knowable.
    assert (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, "main"], cwd=source
        ).returncode
        == 0
    ), (
        "the target must be a GENUINE ancestor in the full repo. Without this "
        "the shallow clone's `1` is simply correct, and this row degenerates "
        "into the side-branch false positive it exists to avoid."
    )

    shallow = tmp_path / "shallow-present"
    subprocess.run(
        ["git", "clone", "--depth", "1", f"file://{source}", str(shallow)],
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(
        ["git", "fetch", "--depth", "1", "origin", "ancref"],
        cwd=shallow,
        capture_output=True,
        text=True,
        check=True,
    )

    # THE THREE PRECONDITIONS, ASSERTED RATHER THAN ASSUMED. A fixture that
    # silently fails to establish them is a row that passes for the wrong
    # reason, which is the same defect one level down.
    assert _git(shallow, "rev-parse", "--is-shallow-repository") == "true", (
        "the fixture must be shallow"
    )
    assert (
        subprocess.run(
            ["git", "cat-file", "-e", f"{ancestor}^{{commit}}"], cwd=shallow
        ).returncode
        == 0
    ), "the target object must be PRESENT -- that is what distinguishes this row"
    assert (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, "HEAD"], cwd=shallow
        ).returncode
        == 1
    ), (
        "ancestry must return 1 -- the WRONG answer, since the truth pinned "
        "above is 0. If this returns 0 the clone is not truncating the walk; "
        "if it returns 128 the object is absent and this row has collapsed "
        "into test_shallow_repository_refuses."
    )

    (shallow / "ci").mkdir(exist_ok=True)
    code, out, calls = _run_guard(shallow, tmp_path, mirror_sha=ancestor)
    assert code == 1, f"a shallow repository must be refused, got {code}\n{out}"
    assert "this is a shallow repository" in out, (
        f"the pre-flight refusal must fire. Ancestry here returns 1 -- a wrong "
        f"answer -- so without the refusal the guard SKIPS: exit 0 with the "
        f"pre-mirror message, on a tree that contains the mirror.\n{out}"
    )
    assert SKIP_MARKER not in out, f"must not treat this as pre-mirror\n{out}"
    assert not calls, f"a refusal must not invoke docker: {calls!r}"


@pytest.mark.skipif(
    os.geteuid() == 0, reason="root reads mode-000 files, so grep cannot return >=2"
)
def test_unreadable_dockerfile_fails_closed(tmp_path: Path) -> None:
    """The grep >=2 branch, which nothing observed until this row.

    Found by mutating my own fix rather than by review: collapsing the status
    check to `-ge 1` left all nine rows green, so "could not read the file" was
    silently becoming "the file does not name the mirror" -- the same
    grep-1-vs-2 collapse this guard already made once, reintroduced by the fix
    for a different defect.

    That is the fifth deliberately-written branch on this guard to be written
    without an observer. The pattern is consistent: branches reasoned about
    while writing the GUARD get no row; only branches reasoned about while
    writing a TEST do.
    """
    repo = _scratch_repo(tmp_path, script=False, mirrored=True)
    _git(repo, "checkout", "-q", "-b", "sidebranch")
    (repo / "side").write_text("side\n", encoding="utf-8")
    _git(repo, "add", "side")
    _git(repo, "commit", "-qm", "side commit")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "main")
    dockerfile = repo / "docker" / "Dockerfile"
    dockerfile.chmod(0o000)
    assert not os.access(dockerfile, os.R_OK), (
        "the fixture must actually be unreadable, or this row proves nothing"
    )
    try:
        code, out, calls = _run_guard(repo, tmp_path, mirror_sha=side)
    finally:
        dockerfile.chmod(0o644)
    assert code == 1, f"an unreadable Dockerfile must fail closed, got {code}\n{out}"
    assert "could not be read" in out, (
        f"the failure must name the unreadable-file case, not the cherry-pick "
        f"one: both exit 1 with an ::error::, so an outcome-shaped assertion "
        f"would pass whichever fired.\n{out}"
    )
    assert SKIP_MARKER not in out, f"must not skip a tree it could not read\n{out}"


def test_other_ghcr_image_is_not_the_mirrored_base(tmp_path: Path) -> None:
    """NEGATIVE CONTROL for the containment string's breadth (084-prod).

    The check matches `ghcr.io/<owner>/python`, not the bare owner prefix. A
    genuinely pre-mirror branch whose Dockerfile pulls some OTHER ghcr image --
    a tool stage, a builder -- is doing nothing wrong. The bare prefix would
    hard-fail it and tell it its tree was inconsistent, which is both a broken
    build and a wrong diagnosis.

    This row fails if the string is ever widened back to the owner prefix.
    """
    repo = _scratch_repo(tmp_path, script=False, other_ghcr=True)
    text = (repo / "docker" / "Dockerfile").read_text(encoding="utf-8")
    assert "ghcr.io/full-chaos/" in text, "the fixture must reference ghcr at all"
    assert "ghcr.io/full-chaos/python" not in text, (
        "...but must NOT name the mirrored Python base, or this row proves nothing"
    )
    _git(repo, "checkout", "-q", "-b", "sidebranch")
    (repo / "side").write_text("side\n", encoding="utf-8")
    _git(repo, "add", "side")
    _git(repo, "commit", "-qm", "side commit")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "main")
    code, out, calls = _run_guard(repo, tmp_path, mirror_sha=side)
    assert code == 0, (
        f"a pre-mirror tree using an unrelated ghcr image must skip, got {code}. "
        f"If this fails, the containment string is matching more than the "
        f"mirrored Python base.\n{out}"
    )
    assert SKIP_MARKER in out, f"the skip must say why\n{out}"
    assert not calls, f"a skip must not invoke docker: {calls!r}"
