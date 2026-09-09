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

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = REPO_ROOT / "contracts" / "gqlgen" / "v1" / "expected-drift.allowlist"
GUARD = REPO_ROOT / "ci" / "check_gqlgen_drift.sh"
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
    "cmd/query-api/internal/graph/generated.go",
    "cmd/query-api/internal/graph/model/models_gen.go",
    "tools.go",
    "go.mod",
    "go.sum",
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


def _drift_guard_steps() -> list[dict]:
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
