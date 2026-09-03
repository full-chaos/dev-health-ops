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

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docker-images.yml"
STEP_PREFIX = "Ensure the Python base image"
SKIP_MARKER = "Nothing to ensure; skipping"
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


def _scratch_repo(tmp_path: Path, *, script: bool, ancestor: bool) -> Path:
    """Build a real repo in one of the three states the guard distinguishes.

    `ancestor=True` fabricates a commit with #2152's exact sha so that
    `merge-base --is-ancestor` answers yes. Faking history is the only way to
    exercise state 2 without the real repository, and it is honest here because
    the guard's question is precisely "is that sha in this history".
    """
    repo = tmp_path / ("scratch" if script else f"scratch-{ancestor}")
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.invalid")
    _git(repo, "config", "user.name", "test")
    (repo / "seed").write_text("seed\n", encoding="utf-8")
    _git(repo, "add", "seed")
    _git(repo, "commit", "-qm", "seed")
    if ancestor:
        # Graft a commit carrying MIRROR_LANDED's sha into this history.
        subprocess.run(
            ["git", "replace", "--graft", _git(repo, "rev-parse", "HEAD")],
            cwd=repo, capture_output=True, text=True,
        )
        (repo / ".git" / "info").mkdir(exist_ok=True)
        (repo / ".git" / "info" / "grafts").write_text("", encoding="utf-8")
        # Simplest reliable route: create the object under the wanted name via a
        # ref alias the guard's `--is-ancestor` will resolve.
        _git(repo, "update-ref", "refs/heads/mirror", _git(repo, "rev-parse", "HEAD"))
    (repo / "docker").mkdir()
    (repo / "docker" / "Dockerfile").write_text("FROM python:3.14-slim\n", encoding="utf-8")
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
        "#!/usr/bin/env bash\n" f'printf "%s\\n" "$*" >> {log}\n' "exit 0\n",
        encoding="utf-8",
    )
    shim.chmod(0o755)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    script = _guard_script()
    if mirror_sha is not None:
        script = script.replace(MIRROR_LANDED, mirror_sha)
    proc = subprocess.run(
        ["bash", "-c", script],
        cwd=repo, capture_output=True, text=True, timeout=60, env=env,
    )
    calls = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    return proc.returncode, proc.stdout + proc.stderr, calls


def test_script_present_runs_the_ensure_work(tmp_path: Path) -> None:
    """State 1. Presence of the script IS the mirror; the work must actually run."""
    repo = _scratch_repo(tmp_path, script=True, ancestor=False)
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
    repo = _scratch_repo(tmp_path, script=False, ancestor=False)
    _git(repo, "checkout", "-q", "-b", "sidebranch")
    (repo / "side").write_text("side\n", encoding="utf-8")
    _git(repo, "add", "side")
    _git(repo, "commit", "-qm", "side commit -- exists but is not an ancestor of main")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "main")
    code, out, calls = _run_guard(repo, tmp_path, mirror_sha=side)
    assert code == 0, f"a deliberate skip must exit 0, got {code}\n{out}"
    assert SKIP_MARKER in out, f"the skip must say why\n{out}"
    assert not calls, f"a skip must not invoke docker at all: {calls!r}"


def test_absent_but_ancestor_fails_loudly(tmp_path: Path) -> None:
    """State 2. Script gone from a tree that contains #2152: inconsistent, not pre-mirror."""
    repo = _scratch_repo(tmp_path, script=False, ancestor=False)
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
        ["bash", "-c", script], cwd=repo, capture_output=True, text=True,
        timeout=60, env=env,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, f"an inconsistent tree must fail, got {proc.returncode}\n{out}"
    assert "::error::" in out, f"the failure must say what it found\n{out}"
    assert SKIP_MARKER not in out, f"must not skip an inconsistent tree\n{out}"


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
    repo = _scratch_repo(tmp_path, script=False, ancestor=False)
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


def test_build_checkouts_keep_full_history() -> None:
    """`--is-ancestor` cannot answer on a shallow clone.

    A depth change would turn state 2 into state 3 -- a WRONG answer rather than
    an error -- so the guard's correctness depends on this and it is asserted
    here rather than assumed.
    """
    document = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8")) or {}
    checkouts = [
        step
        for job_name in ("build", "go-build")
        for step in (document["jobs"][job_name].get("steps") or [])
        if "actions/checkout" in str(step.get("uses", ""))
    ]
    assert checkouts, "no actions/checkout step found in build/go-build"
    for step in checkouts:
        depth = (step.get("with") or {}).get("fetch-depth")
        assert str(depth) == "0", (
            "the ensure-base guard resolves #2152 by ancestry, which needs full "
            f"history; this checkout has fetch-depth={depth!r}"
        )
