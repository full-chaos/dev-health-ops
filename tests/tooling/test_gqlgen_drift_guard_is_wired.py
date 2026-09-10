"""The gqlgen drift guard is expensive; this test proves it is still WIRED.

CHAOS-5489. `ci/check_gqlgen_drift.sh` regenerates the GraphQL layer and
asserts the diff against the checked-in files equals a documented allowlist. It
needs the Go toolchain, so it runs as a `go-quality` step rather than in this
suite -- which means nothing in the default test run would notice if the step
were deleted, the allowlist emptied, or the workflow renamed.

That is the failure this file exists for, and it is the likely one: the guard's
whole purpose is to stop a silent revert, so a guard that is silently unwired
fails in exactly the way it was built to prevent. These assertions are cheap --
no toolchain, no network, no generation -- and they run on every push.

They deliberately do NOT re-derive the drift. That is the CI step's job. What
is checked here is that the step exists, that the allowlist it reads is
present, non-empty and well-formed, and that a change to any generator input
actually triggers the workflow that runs it.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = REPO_ROOT / "contracts" / "gqlgen" / "v1" / "expected-drift.allowlist"
GUARD = REPO_ROOT / "ci" / "check_gqlgen_drift.sh"
WRAPPER = REPO_ROOT / "ci" / "gqlgen_generate.sh"
DIGEST = REPO_ROOT / "contracts" / "gqlgen" / "v1" / "expected-drift.sha256"
WRAPPER = REPO_ROOT / "ci" / "gqlgen_generate.sh"
GO_QUALITY = REPO_ROOT / ".github" / "workflows" / "go-quality.yml"
GO_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "go.yml"
README = REPO_ROOT / "cmd" / "query-api" / "README.md"

# Every input that changes what a regeneration produces. A PR touching only
# one of these must still reach the drift step, or the check passes vacuously
# on precisely the change most likely to break it (CHAOS-5406's shape).
GENERATOR_INPUTS = (
    "cmd/query-api/gqlgen.yml",
    "contracts/graphql/v1/schema.graphql",
    "contracts/gqlgen/v1/expected-drift.allowlist",
    # r5: the digest was checked for existence and shape but never for
    # reachability, so a path-filter mutation that made it non-Go-relevant
    # passed pytest while ci/go_relevance.py said relevant=false.
    "contracts/gqlgen/v1/expected-drift.sha256",
    "cmd/query-api/internal/graph/generated.go",
    "cmd/query-api/internal/graph/model/models_gen.go",
    "tools.go",
    "go.mod",
    "go.sum",
)

# The guard's OWN machinery, as opposed to the generator's inputs above.
# CHAOS-5489 round r3: these were missing from go.yml, so a PR that changed
# only the guard script was classified non-Go and skipped go-quality -- the
# guard did not run on the one change most able to break or weaken it. That
# is the same vacuity this file already checks for the generator's inputs;
# it simply was not checked for the checker.
GUARD_INPUTS = (
    "ci/check_gqlgen_drift.sh",
    "ci/gqlgen_generate.sh",
    # The shared output-scope check. Added after noticing it was NOT
    # Go-relevant on its own -- the third time this file has caught the same
    # class, and the first time it was caught before a review round found it.
    "ci/gqlgen_output_scope.sh",
)


def _go_yml_path_patterns() -> list[str]:
    """go.yml's own `on.pull_request.paths`, which ci/go_relevance.py reads."""
    document = yaml.safe_load(GO_WORKFLOW.read_text())
    # `on` is parsed as the boolean True by YAML 1.1; accept either spelling
    # rather than assuming which loader ran.
    triggers = document.get("on", document.get(True))
    return list(triggers["pull_request"]["paths"])


def _matches_any(path: str, patterns: list[str]) -> bool:
    for pattern in patterns:
        if pattern == path:
            return True
        regex = (
            re.escape(pattern)
            .replace(r"\*\*/", "(?:.*/)?")
            .replace(r"\*\*", ".*")
            .replace(r"\*", "[^/]*")
        )
        if re.fullmatch(regex, path):
            return True
    return False


def test_the_guard_and_wrapper_exist_and_are_executable() -> None:
    for script in (GUARD, WRAPPER):
        assert script.exists(), f"{script.relative_to(REPO_ROOT)} is missing"
        assert script.stat().st_mode & 0o111, (
            f"{script.relative_to(REPO_ROOT)} is not executable; CI invokes it via bash, "
            "but a developer following the README will not"
        )


def _drift_guard_steps() -> list[tuple[dict, dict]]:
    """Every go-quality step that actually invokes the guard.

    Parsed, not grepped. A substring match cannot tell a live step from one
    that is present and inert -- `if: false`, a commented-out `run`, a step
    that only echoes the path -- and "present but inert" is the failure mode a
    guard against silent reverts must not have. The review round proved the
    string assertion passes against a disabled step.
    """
    document = yaml.safe_load(GO_QUALITY.read_text())
    steps: list[tuple[dict, dict]] = []
    for job in document.get("jobs", {}).values():
        for step in job.get("steps", []) or []:
            run = step.get("run")
            if isinstance(run, str) and "ci/check_gqlgen_drift.sh" in run:
                steps.append((job, step))
    return steps


def _is_disabled(condition: object) -> bool:
    """A condition that can never be true, in the forms GitHub accepts."""
    text = str(condition).strip().lower()
    text = text.removeprefix("${{").removesuffix("}}").strip()
    return text in {"false", "0", "'false'", '"false"'}


def test_go_quality_still_runs_the_drift_guard() -> None:
    """The step can be renamed; it cannot be removed or disabled.

    Round r2 executed the disabling forms against this test and found three
    that it accepted: `continue-on-error: true`, a `run` ending in `|| true`,
    and a job switched off at the job level or given an empty matrix. Each
    leaves a green report with the guard doing nothing, which is the exact
    state this test exists to make impossible. All are rejected below.
    """
    steps = _drift_guard_steps()
    assert steps, (
        "no go-quality step runs ci/check_gqlgen_drift.sh. The gqlgen output "
        "drift is then unguarded: a regeneration silently reverts the "
        "nullability hand-edits (CHAOS-4650/4657/4658/4701/4703) and nothing "
        "fails. Restore the step, or delete this test with a ticket saying why."
    )
    for job, step in steps:
        run = step["run"]
        # The command must be reachable, not merely mentioned -- a `run` that
        # only echoes the path, or comments it out, satisfies a grep.
        assert any(
            line.strip().startswith(
                (
                    "bash ci/check_gqlgen_drift.sh",
                    "ci/check_gqlgen_drift.sh",
                    "./ci/check_gqlgen_drift.sh",
                )
            )
            for line in run.splitlines()
        ), f"the guard step mentions the script but does not invoke it:\n{run}"

        assert not _is_disabled(step.get("if", "")), (
            "the guard step is present but disabled by its `if:` condition. That is "
            "indistinguishable from a working guard in every report, and leaves the "
            "hand-edits unprotected. Remove the step honestly, with a ticket, rather "
            "than switching it off."
        )

        # A step that cannot fail cannot guard. `continue-on-error` turns a
        # real drift failure into a green check with a warning nobody reads.
        assert str(step.get("continue-on-error", "")).strip().lower() not in {
            "true",
            "${{ true }}",
        }, (
            "the guard step sets continue-on-error, so drift can no longer fail the "
            "build. The step still appears and still passes -- which is worse than "
            "deleting it, because the report claims the hand-edits are protected."
        )

        # ... and neither can a command whose failure is swallowed in the shell.
        for line in run.splitlines():
            stripped = line.strip()
            if "ci/check_gqlgen_drift.sh" not in stripped:
                continue
            assert not stripped.endswith(("|| true", "|| :", "; true")), (
                "the guard's exit status is discarded by the shell, so a drift "
                f"failure passes silently:\n{line}"
            )

        # A custom shell, or `set +e` inside the run, discards the guard's
        # status just as effectively as `|| true`. Round r4 executed all
        # three and confirmed exit 0 with the guard failing underneath.
        assert "shell" not in step, (
            "the guard step overrides `shell:`. A custom shell can drop the "
            "errexit behaviour the guard's failure depends on -- run it under the "
            "default shell, or the failure is silently discarded."
        )
        assert not re.search(r"set\s+\+e|set\s+\+o\s+errexit", run), (
            "the guard step disables errexit inside its `run`, so a drift failure "
            "no longer fails the step."
        )

        # A line that ends the shell before the guard is reached passes every
        # textual check: the invocation is still present, still spelled
        # correctly, still not commented out -- and never runs. Round r6 got
        # `exit 0` on the preceding line past all thirteen tests.
        for line in run.splitlines():
            stripped = line.strip()
            if "ci/check_gqlgen_drift.sh" in stripped:
                break
            assert not re.match(r"^(exit|return)\b", stripped), (
                "the guard step short-circuits before it invokes the guard, so the "
                f"step reports success without running it:\n{run}"
            )

        # The job itself must be able to run. A job-level `if: false`, or a
        # matrix with no entries, disables every step inside it while leaving
        # this step visibly present in the file.
        assert not _is_disabled(job.get("if", "")), (
            "the guard's step is live but its JOB is disabled by a job-level `if:`, "
            "so the guard never runs."
        )
        matrix = (job.get("strategy", {}) or {}).get("matrix")
        if isinstance(matrix, dict):
            for axis, values in matrix.items():
                if axis in {"include", "exclude"}:
                    continue
                assert not (isinstance(values, list) and not values), (
                    f"the guard's job has an EMPTY matrix axis {axis!r}, so it expands "
                    "to zero jobs and the guard never runs, while the step remains "
                    "visible in the workflow file."
                )


def test_every_generator_input_triggers_the_workflow_that_runs_the_guard() -> None:
    """A guard that never fires is indistinguishable from a guard that passes."""
    patterns = _go_yml_path_patterns()
    unreachable = [p for p in GENERATOR_INPUTS if not _matches_any(p, patterns)]
    assert not unreachable, (
        "these generator inputs do not match go.yml's path filters, so a PR "
        f"touching only them skips go-quality and the drift guard with it: {unreachable}. "
        "Add each to go.yml's on.pull_request.paths with a comment saying why."
    )


def test_the_guards_own_files_trigger_the_workflow_that_runs_it() -> None:
    """A guard that its own edits cannot trigger reviews nothing that matters.

    The generator-input check above asks whether a change to what gqlgen READS
    reaches the guard. This asks the other half: whether a change to the guard
    ITSELF does. Round r3 found it did not -- `ci/check_gqlgen_drift.sh`
    matched no path filter, so weakening or breaking the guard was precisely
    the change that would not run it.
    """
    patterns = _go_yml_path_patterns()
    unreachable = [p for p in GUARD_INPUTS if not _matches_any(p, patterns)]
    assert not unreachable, (
        "these files ARE the drift guard, but do not match go.yml's path filters, "
        "so a PR touching only them is classified non-Go and never runs the guard "
        f"against its own change: {unreachable}. Add each to go.yml's "
        "on.pull_request.paths with a comment saying why."
    )


def test_the_positional_digest_is_present_and_well_formed() -> None:
    """The exact half of the contract.

    The readable allowlist is position-free on purpose, and position-free keys
    cannot be unique: generated.go repeats itself so heavily that even twenty
    lines of surrounding context still leave colliding keys, which is how two
    successive fixes to the key were still defeated. The digest covers the raw
    unified diff, positions included, so a hand-edit MOVED to an
    identical-looking position fails even though every readable entry matches.
    """
    assert DIGEST.exists(), (
        f"{DIGEST.relative_to(REPO_ROOT)} is missing. Without it the guard is back "
        "to a position-free comparison, which cannot detect a moved hand-edit. "
        "Create it with ci/check_gqlgen_drift.sh --update."
    )
    text = DIGEST.read_text().strip()
    assert re.fullmatch(r"[0-9a-f]{64}", text), (
        f"the drift digest is not a sha256: {text!r}. An unreadable digest must not "
        "be treated as 'no drift'."
    )


def test_the_guard_compares_the_digest_not_only_the_allowlist() -> None:
    """A digest that is written but never compared protects nothing."""
    body = GUARD.read_text()
    assert "actual_digest" in body and "expected_digest" in body, (
        "ci/check_gqlgen_drift.sh no longer computes and compares a digest of the "
        "raw diff. The allowlist alone cannot detect a moved hand-edit."
    )
    assert '"${expected_digest}" = "${actual_digest}"' in body, (
        "the guard's success path no longer requires the digest to match, so a "
        "moved hand-edit passes silently again."
    )


def test_the_allowlist_is_present_and_well_formed() -> None:
    assert ALLOWLIST.exists(), f"{ALLOWLIST.relative_to(REPO_ROOT)} is missing"
    lines = ALLOWLIST.read_text().splitlines()
    entries = [ln for ln in lines if ln and not ln.startswith("#")]
    assert entries, (
        "the allowlist has no entries. An EMPTY allowlist does not mean 'no drift' -- "
        "it means the guard now demands a byte-identical regeneration, which reverts "
        "every documented hand-edit. If the drift genuinely went to zero, the "
        "hand-edits are gone and that is the bug."
    )
    for entry in entries:
        # <file><TAB><preceding decl @ after:context><TAB><+|-><content>. The
        # middle field exists because content alone collides: the same comment
        # line removed from two different types normalises to one entry, so the
        # guard would see one change where there are two (CHAOS-5489 round r1).
        # It is the PRECEDING declaration, not the enclosing one -- `diff -F`
        # names the last declaration strictly before the hunk start, so an
        # entry may legitimately name its neighbour. The `after:` context was
        # added in round r2, which proved by execution that the declaration
        # alone still collides for a line duplicated inside ONE hunk.
        parts = entry.split("\t", 2)
        assert len(parts) == 3, (
            f"entry is not <file><TAB><decl><TAB><+|->content: {entry!r}"
        )
        path, decl, change = parts
        assert decl, f"entry has an empty declaration field: {entry!r}"
        assert change[:1] in "+-", f"entry does not start with + or -: {entry!r}"
        assert (REPO_ROOT / path).exists(), (
            f"the allowlist names {path}, which does not exist. Either the generated "
            "file moved and the allowlist is stale, or it was deleted by a failed "
            "generation (see ci/gqlgen_generate.sh)."
        )


def test_the_allowlist_names_the_tickets_it_documents() -> None:
    """Entries are mechanical; the header is where a reader learns WHY."""
    header = "\n".join(
        ln for ln in ALLOWLIST.read_text().splitlines() if ln.startswith("#")
    )
    missing = [
        t
        for t in (
            "CHAOS-4650",
            "CHAOS-4657",
            "CHAOS-4658",
            "CHAOS-4701",
            "CHAOS-4703",
            "CHAOS-5483",
        )
        if t not in header
    ]
    assert not missing, f"the allowlist header no longer cites {missing}"


def test_the_readme_documents_the_safe_command() -> None:
    """The raw `go run` deletes the generated files when it fails."""
    text = README.read_text()
    assert "ci/gqlgen_generate.sh" in text, (
        "cmd/query-api/README.md no longer documents the wrapper. The raw "
        "`go run github.com/99designs/gqlgen generate` it replaced deletes "
        "generated.go and models_gen.go before regenerating and does not put "
        "them back when the run fails -- observed exiting 1 with an empty error."
    )


def test_the_guards_comment_count_matches_the_allowlist() -> None:
    """The rationale cites a number; prose drifts from the file it describes.

    ci/check_gqlgen_drift.sh justifies the entry format by saying how many
    entries are comment lines. That number has been wrong TWICE: first 81,
    which was a per-FILE line count, then 58, from an awk that split the
    entry on tabs and so missed every INDENTED comment (the majority). A
    wrong number in the rationale is how a reader talks themselves out of the
    format, and it survived two hand-corrections, which is the argument for
    computing it here instead. Asserted against the real file rather than
    pinned, so a deliberate re-bless updates one number and this test says
    which.
    """
    match = re.search(r"(\d+) of these entries are comment lines", GUARD.read_text())
    assert match, (
        "ci/check_gqlgen_drift.sh no longer states how many entries are comment "
        "lines. That sentence is the justification for the entry format; if it "
        "moved, move this assertion with it."
    )
    claimed = int(match.group(1))

    entries = [
        ln for ln in ALLOWLIST.read_text().splitlines() if ln and not ln.startswith("#")
    ]
    actual = 0
    for entry in entries:
        parts = entry.split("\t", 2)
        if len(parts) == 3 and parts[2][1:].lstrip().startswith("//"):
            actual += 1

    assert claimed == actual, (
        f"ci/check_gqlgen_drift.sh claims {claimed} of the allowlist entries are "
        f"comment lines; {actual} of them are. Update the comment (or re-bless "
        "the allowlist), because that count is the stated reason the entry "
        "format carries a declaration at all."
    )


# ---------------------------------------------------------------------------
# Behavioural tests.
#
# NOTHING BELOW TOUCHES THE WORKING TREE. An earlier version ran the wrapper in
# the repo and repaired it afterwards with `git checkout` plus a deletion loop
# for untracked files. Review round r7 showed what that costs: a developer's
# uncommitted edit and their untracked file were both destroyed by the cleanup,
# which is a far worse bug than any this suite is looking for. Every scenario
# now runs against a private copy of the tree, and one test proves that.
# ---------------------------------------------------------------------------

GENERATED_AREA_REL = "cmd/query-api/internal/graph"


@pytest.fixture(scope="module")
def tree_copy(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One private copy of the repository, reused by the tests below.

    Copied once because it is the expensive part; each test that mutates a
    script restores that one file afterwards, so the copy stays pristine
    between tests without paying for a fresh copy every time.
    """
    root = tmp_path_factory.mktemp("gqlgen-tree") / "repo"
    root.mkdir()
    result = subprocess.run(
        "tar -C . --exclude=./.git --exclude=./.venv --exclude=./.uv-cache "
        f"--exclude=./node_modules --exclude=./site -cf - . | tar -C {root} -xf -",
        cwd=REPO_ROOT,
        shell=True,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, f"could not copy the tree: {result.stderr}"
    return root


def _run_in(
    tree: Path, script: str, **env_extra: str
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, **env_extra)
    return subprocess.run(
        ["bash", str(tree / "ci" / script)],
        cwd=tree,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )


def _digests(tree: Path) -> dict[str, str]:
    import hashlib

    area = tree / GENERATED_AREA_REL
    return {
        str(p.relative_to(area)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(area.rglob("*.go"))
    }


@contextmanager
def _pristine_area(tree: Path) -> Iterator[None]:
    """Put the copy's generated area back exactly as it was.

    The module-scoped copy is shared, and a mutant with a protection disabled
    deliberately leaves damage -- that is what makes it a mutant. Without this,
    the damage leaks into whichever test runs next and it fails for a reason
    that has nothing to do with what it is testing. Cost me a debugging round.
    """
    area = tree / GENERATED_AREA_REL
    saved = {p.relative_to(area): p.read_bytes() for p in area.rglob("*.go")}
    try:
        yield
    finally:
        for path in list(area.rglob("*.go")):
            if path.relative_to(area) not in saved:
                path.unlink()
        for rel_path, blob in saved.items():
            target = area / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists() or target.read_bytes() != blob:
                target.write_bytes(blob)


@contextmanager
def _mutated(tree: Path, rel: str, mutate: Callable[[str], str]) -> Iterator[None]:
    """Apply a source mutation inside the COPY, then put that file back."""
    target = tree / rel
    original = target.read_text()
    changed = mutate(original)
    assert changed != original, f"mutation for {rel} did not change anything"
    target.write_text(changed)
    try:
        yield
    finally:
        target.write_text(original)


def test_the_suite_never_touches_the_developers_working_tree(tree_copy: Path) -> None:
    """The bug r7 found in this file, asserted against.

    A dirty edit and an untracked file are planted in the COPY, the destructive
    scenarios are run, and both must survive. The earlier cleanup -- `git
    checkout` plus deleting every untracked .go file -- destroyed exactly these
    two things, in the developer's real tree.
    """
    dirty = tree_copy / GENERATED_AREA_REL / "resolver.go"
    marker = "// developer's uncommitted edit\n"
    dirty.write_text(dirty.read_text() + marker)
    untracked = tree_copy / GENERATED_AREA_REL / "scratch_notes.go"
    untracked.write_text("package graph\n// untracked scratch file\n")

    _run_in(
        tree_copy,
        "gqlgen_generate.sh",
        GQLGEN_GENERATE_CMD="rm -f internal/graph/generated.go; exit 1",
    )
    _run_in(tree_copy, "check_gqlgen_drift.sh", GQLGEN_DRIFT_GENERATE_CMD="true")

    assert dirty.read_text().endswith(marker), (
        "a pre-existing uncommitted edit was destroyed. Whatever this suite finds, "
        "it must not cost a developer their work."
    )
    assert untracked.exists(), "a pre-existing untracked file was deleted."
    untracked.unlink()
    dirty.write_text(dirty.read_text()[: -len(marker)])


def test_the_wrapper_restores_the_area_when_generation_fails(tree_copy: Path) -> None:
    before = _digests(tree_copy)
    result = _run_in(
        tree_copy,
        "gqlgen_generate.sh",
        GQLGEN_GENERATE_CMD=(
            "rm -f internal/graph/generated.go internal/graph/model/models_gen.go; exit 1"
        ),
    )
    assert _digests(tree_copy) == before, (
        f"the wrapper did not restore the area after a failed run.\n{result.stderr}"
    )
    assert result.returncode != 0, "generation failed and the wrapper reported success."


def test_the_wrapper_restores_a_hand_written_file_it_does_not_generate(
    tree_copy: Path,
) -> None:
    """Overwriting an existing file leaves the file SET unchanged."""
    before = _digests(tree_copy)
    result = _run_in(
        tree_copy,
        "gqlgen_generate.sh",
        GQLGEN_GENERATE_CMD="printf 'package graph\\n' > internal/graph/resolver.go; exit 1",
    )
    assert _digests(tree_copy).get("resolver.go") == before["resolver.go"], (
        f"the hand-written resolver.go was left overwritten.\n{result.stderr}"
    )


def test_the_wrapper_removes_files_its_failed_run_created(tree_copy: Path) -> None:
    before = _digests(tree_copy)
    result = _run_in(
        tree_copy,
        "gqlgen_generate.sh",
        GQLGEN_GENERATE_CMD=(
            "mkdir -p internal/graph/nested && printf 'package nested\\n' "
            "> internal/graph/nested/generated.go; exit 1"
        ),
    )
    assert set(_digests(tree_copy)) == set(before), (
        f"a failed run left untracked generator output behind.\n{result.stderr}"
    )


def test_the_wrapper_refuses_a_config_that_writes_outside_the_area(
    tree_copy: Path,
) -> None:
    """Refusal, not recovery: recovery cannot cover the whole repository."""
    victim = tree_copy / "ZZ_victim.go"
    victim.write_text("package main\n// hand-written, outside the generated area\n")
    before = victim.read_text()
    with _mutated(
        tree_copy,
        "cmd/query-api/gqlgen.yml",
        lambda t: t.replace(
            "  filename: internal/graph/generated.go",
            "  filename: ../../ZZ_victim.go",
            1,
        ),
    ):
        result = _run_in(tree_copy, "gqlgen_generate.sh")
    assert result.returncode == 2, (
        "the wrapper did not REFUSE a config pointing outside the protected area; "
        f"it returned {result.returncode}.\n{result.stderr}"
    )
    assert victim.read_text() == before, (
        "the wrapper ran anyway and destroyed a hand-written file it cannot restore."
    )
    assert "REFUSING" in result.stderr and "ZZ_victim.go" in result.stderr, (
        f"the refusal did not name the offending path.\n{result.stderr}"
    )
    victim.unlink()


def test_the_guard_passes_on_the_real_tree(tree_copy: Path) -> None:
    """The positive control, running the REAL script with the REAL generator.

    r7 found the previous positive control replayed only the GraphQL directory,
    so it never saw the go tool tidying go.sum -- which made the real script
    fail on a pristine tree while all seventeen tests passed.
    """
    result = _run_in(tree_copy, "check_gqlgen_drift.sh")
    assert result.returncode == 0, (
        "the guard FAILED on an unmodified tree, so CI cannot pass.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "all documented" in result.stdout, result.stdout


# Each entry disables ONE protection in a copy of the scripts and names the
# scenario that must then fail. Round r7's headline P3 was that all seventeen
# tests passed with each protection individually disabled -- the mutations were
# only ever run by hand, so the SUITE did not kill them. It does now.
def _resolver_intact(tree: Path, before: dict[str, str], _r: object) -> bool:
    return _digests(tree).get("resolver.go") == before["resolver.go"]


def _no_new_files(tree: Path, before: dict[str, str], _r: object) -> bool:
    return set(_digests(tree)) == set(before)


def _failed_naming(fragment: str) -> Callable[..., bool]:
    """Failed, AND for this specific reason.

    Exit status alone is not enough for the guard's checks: the digest and the
    allowlist catch several of the same scenarios by a different route, so a
    mutant with one check disabled still exits non-zero. That is defence in
    depth working, and it means "did it fail?" cannot tell whether the check
    under test did anything. The diagnostic can.
    """

    def observable(_t: Path, _b: dict[str, str], result: object) -> bool:
        return getattr(result, "returncode", 0) != 0 and fragment in getattr(
            result, "stderr", ""
        )

    return observable


# Each protection, the scenario that exercises it, and THE OBSERVABLE THAT
# PROVES IT WORKED. The observable is not always the exit status: a wrapper
# that fails to restore still exits non-zero, because generation itself failed
# -- the thing that differs is the file content. Conflating the two is how a
# mutation test ends up proving nothing, which is what r7 found here.
_MUTANTS: list[
    tuple[str, str, Callable[[str], str], str, dict[str, str], Callable[..., bool], str]
] = [
    (
        "area restoration disabled",
        "ci/gqlgen_generate.sh",
        lambda t: t.replace("restore_area() {", "restore_area() { return 0;", 1),
        "gqlgen_generate.sh",
        {
            "GQLGEN_GENERATE_CMD": (
                "printf 'package graph\\n' > internal/graph/resolver.go; exit 1"
            )
        },
        _resolver_intact,
        "the hand-written resolver.go comes back byte-identical",
    ),
    (
        "created-file purge disabled",
        "ci/gqlgen_generate.sh",
        lambda t: t.replace(
            "purge_unexpected() {", "purge_unexpected() { return 0;", 1
        ),
        "gqlgen_generate.sh",
        {
            "GQLGEN_GENERATE_CMD": (
                "mkdir -p internal/graph/nested && printf 'package nested\\n' "
                "> internal/graph/nested/generated.go; exit 1"
            )
        },
        _no_new_files,
        "output the failed run created is removed",
    ),
    (
        "hand-written-file rejection disabled",
        "ci/check_gqlgen_drift.sh",
        # Disables the DETECTION, not the summary `if` after it -- the
        # diagnostic is printed inside the loop, so neutering the trailing
        # condition left the message intact and the mutant looked alive.
        lambda t: t.replace(
            'case " ${GENERATED_FILES[*]} " in *" ${full} "*) continue ;; esac',
            "continue",
            1,
        ),
        "check_gqlgen_drift.sh",
        {
            "GQLGEN_DRIFT_GENERATE_CMD": (
                "printf 'package graph\\n' > internal/graph/resolver.go"
            )
        },
        _failed_naming("MODIFIED files it does not generate"),
        "the guard fails NAMING an overwritten hand-written file",
    ),
    (
        "outside-root rejection disabled",
        "ci/check_gqlgen_drift.sh",
        lambda t: t.replace('if [ -s "${WORK}/.outside" ]; then', "if false; then", 1),
        "check_gqlgen_drift.sh",
        {"GQLGEN_DRIFT_GENERATE_CMD": "printf 'x\\n' > ../../ZZ_escaped.txt"},
        _failed_naming("wrote OUTSIDE the generated root"),
        "the guard fails NAMING a write outside the generated root",
    ),
]


@pytest.mark.parametrize(
    ("label", "rel", "mutate", "script", "env", "observable", "claim"),
    _MUTANTS,
    ids=[m[0].replace(" ", "-") for m in _MUTANTS],
)
def test_every_protection_is_load_bearing(
    tree_copy: Path,
    label: str,
    rel: str,
    mutate: Callable[[str], str],
    script: str,
    env: dict[str, str],
    observable: Callable[..., bool],
    claim: str,
) -> None:
    """Prove each protection is what produces its guarantee.

    Round r7's headline P3: all seventeen tests still passed with each
    protection individually disabled, because the mutations were only ever run
    by hand. Two executed claims per protection:

    intact   -> the guarantee HOLDS
    disabled -> the guarantee BREAKS

    The second half is the one that makes the first mean anything. If the
    guarantee still holds with the protection removed, something else is
    providing it and this protection is untested.

    Everything runs in a private copy of the tree.
    """
    before = _digests(tree_copy)
    with _pristine_area(tree_copy):
        intact = _run_in(tree_copy, script, **env)
        intact_held = observable(tree_copy, before, intact)
    assert intact_held, (
        f"{label}: with every protection intact, {claim} -- did not hold.\n"
        f"stdout:\n{intact.stdout}\nstderr:\n{intact.stderr}"
    )

    before = _digests(tree_copy)
    with _pristine_area(tree_copy), _mutated(tree_copy, rel, mutate):
        mutant = _run_in(tree_copy, script, **env)
        broke = not observable(tree_copy, before, mutant)

    assert broke, (
        f"{label}: {claim} -- STILL held with this protection disabled, so the "
        "protection is not what provides it and nothing here tests it.\n"
        f"stdout:\n{mutant.stdout}\nstderr:\n{mutant.stderr}"
    )


SCOPE_LIB = REPO_ROOT / "ci" / "gqlgen_output_scope.sh"


def test_the_output_scope_check_is_shared_by_both_scripts() -> None:
    """One answer to "where may the generator write", not two.

    Round r8 exploited the asymmetry directly: the wrapper refused an
    out-of-area config, the guard did not, and the guard's belief that a temp
    copy isolates it is false for an ABSOLUTE path -- copying a tree does not
    relocate one. Both scripts source the same function now, so neither can
    drift into being the lenient one.
    """
    assert SCOPE_LIB.exists(), f"{SCOPE_LIB.relative_to(REPO_ROOT)} is missing"
    for script in (GUARD, WRAPPER):
        body = script.read_text()
        assert (
            "gqlgen_output_scope.sh" in body and "gqlgen_output_scope_check" in body
        ), (
            f"{script.name} no longer uses the shared output-scope check, so the two "
            "scripts can disagree about where generation may write -- which is the "
            "exact gap r8 used."
        )


@pytest.mark.parametrize(
    ("label", "mutate"),
    [
        (
            "absolute exec.filename",
            lambda t, victim: t.replace(
                "  filename: internal/graph/generated.go", f"  filename: {victim}", 1
            ),
        ),
        (
            "filename_template escaping with ../",
            lambda t, victim: t.replace(
                '  filename_template: "{name}.resolvers.go"',
                f'  filename_template: "{victim}"',
                1,
            ),
        ),
        (
            "exec.filename omitted, gqlgen default used",
            lambda t, _v: t.replace("  filename: internal/graph/generated.go\n", "", 1),
        ),
    ],
    ids=["absolute-path", "escaping-template", "omitted-default"],
)
def test_both_scripts_refuse_configs_that_write_outside_the_area(
    tree_copy: Path, label: str, mutate: Callable[[str, str], str]
) -> None:
    """Every shape r8 got past the wrapper's first validator.

    Absolute paths, `../` inside a filename_template, and an OMITTED key whose
    gqlgen default still writes somewhere -- a missing key is an output path
    too, which is why defaults are validated explicitly rather than only what
    the config happens to name.
    """
    victim = tree_copy / "ZZ_victim.go"
    victim.write_text("package main\n// hand-written, outside the generated area\n")
    default_victim = tree_copy / "cmd" / "query-api" / "generated.go"
    default_victim.write_text("package graph\n// at gqlgen's default path\n")
    before = (victim.read_bytes(), default_victim.read_bytes())

    for script in ("gqlgen_generate.sh", "check_gqlgen_drift.sh"):
        with (
            _pristine_area(tree_copy),
            _mutated(
                tree_copy, "cmd/query-api/gqlgen.yml", lambda t: mutate(t, str(victim))
            ),
        ):
            result = _run_in(tree_copy, script)
        assert result.returncode == 2, (
            f"{script} did not REFUSE ({label}); it exited {result.returncode}. "
            f"Generation into an unprotected path is unrecoverable.\n{result.stderr}"
        )
        assert (victim.read_bytes(), default_victim.read_bytes()) == before, (
            f"{script} ran anyway ({label}) and overwrote a file it cannot restore."
        )

    victim.unlink()
    default_victim.unlink()


def test_the_wrapper_restores_modules_on_a_false_success(tree_copy: Path) -> None:
    """The branch that had no restore_modules call.

    A generator that exits 0 while deleting an output is the empty-error
    failure seen from outside. That branch restored the GraphQL area and left
    the module files tidied, unlike the nonzero-exit and signal branches --
    three paths, one of them different, which is how this kind of gap survives.
    """
    gomod = tree_copy / "go.mod"
    before = gomod.read_bytes()
    with _pristine_area(tree_copy):
        result = _run_in(
            tree_copy,
            "gqlgen_generate.sh",
            GQLGEN_GENERATE_CMD=(
                'rm internal/graph/generated.go; printf "\\n// tidy\\n" >> ../../go.mod; true'
            ),
        )
        restored = gomod.read_bytes() == before
        gomod.write_bytes(before)
    assert result.returncode != 0, (
        "a success that deleted an output was reported as success."
    )
    assert restored, (
        "the false-success branch restored the generated area but left go.mod modified."
    )
