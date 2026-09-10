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
import tempfile
from pathlib import Path

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


def _run_guard_with_fake_generator(script: str) -> subprocess.CompletedProcess[str]:
    """Run the real guard, substituting a fake generator for gqlgen.

    Everything else is the real script: the tree copy, the file-set check, the
    diff, the digest, the allowlist comparison and the exit status.
    """
    env = dict(os.environ, GQLGEN_DRIFT_GENERATE_CMD=script)
    return subprocess.run(
        ["bash", str(GUARD)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_the_guard_fails_when_every_hand_edit_is_reverted() -> None:
    """The behavioural test the text assertions could not be.

    Round r5 mutated the guard to exit early, to skip the digest comparison,
    and to be a no-op, and all ten of the assertions here still passed --
    because none of them ran the guard and looked at what it DID. A generator
    that writes nothing leaves the regenerated copy identical to the tree, so
    every documented hand-edit reads as REVERTED. A guard that does not fail
    here is not guarding.
    """
    result = _run_guard_with_fake_generator("true")
    assert result.returncode != 0, (
        "the guard exited 0 when every documented hand-edit had vanished. It is "
        "not comparing anything -- an early exit, a skipped comparison or a "
        f"swallowed status.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "LOST drift" in result.stderr, (
        "the guard failed, but not for the right reason: it should report LOST "
        f"drift when hand-edits disappear.\nstderr:\n{result.stderr}"
    )


def test_the_guard_fails_on_generator_output_it_does_not_track() -> None:
    """An untracked output is an unguarded file, not a harmless extra.

    Round r5 pointed a resolver stanza at a new path; generation succeeded, the
    guard diffed only its three listed files and reported everything documented,
    and the extra file broke the build with a redeclared Resolver.
    """
    result = _run_guard_with_fake_generator(
        "printf 'package graph\\n' > internal/graph/zz_untracked_output.go"
    )
    assert result.returncode != 0, (
        "generation wrote a file the guard does not track and the guard passed. "
        "Hand-edits in such a file would be unprotected, and the file itself can "
        f"break the build.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "zz_untracked_output.go" in result.stderr, (
        f"the guard failed but never named the untracked output.\n{result.stderr}"
    )


def test_ci_neither_re_blesses_the_allowlist_nor_fakes_the_generator() -> None:
    """Two ways to make the CI step pass while checking nothing."""
    for _job, step in _drift_guard_steps():
        run = step["run"]
        assert "--update" not in run, (
            "the CI step runs the guard with --update, which REWRITES the "
            "allowlist to match whatever was generated. It then always passes, "
            "and the hand-edit contract is silently re-blessed on every run."
        )
        assert "GQLGEN_GENERATE_CMD" not in run, (
            "the CI step overrides the generator. That hook exists so this file "
            "can test the guard's behaviour; in CI it means the guard is checking "
            "output that gqlgen never produced."
        )
        # Step-level env is not the only place an override can live: a
        # job-level (or workflow-level) env reaches the step just as well, and
        # round r6 escaped the step-only assertion that way.
        for scope, holder in (("step", step), ("job", _job)):
            env_text = str(holder.get("env", ""))
            for hook in ("GQLGEN_DRIFT_GENERATE_CMD", "GQLGEN_GENERATE_CMD"):
                assert hook not in env_text, (
                    f"{hook} is set in the guard's {scope}-level env, so the guard "
                    "runs against a fake generator instead of gqlgen."
                )


GENERATED_AREA = REPO_ROOT / "cmd" / "query-api" / "internal" / "graph"


def _area_digests() -> dict[str, str]:
    """Content of every .go file in the generated area, by relative path."""
    import hashlib

    out: dict[str, str] = {}
    for path in sorted(GENERATED_AREA.rglob("*.go")):
        out[str(path.relative_to(GENERATED_AREA))] = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
    return out


def _run_wrapper(fake_generator: str) -> subprocess.CompletedProcess[str]:
    """Run the wrapper against a fake generator, then put the area back.

    These tests deliberately make generation destructive, and the thing under
    test is whether the wrapper repairs it. When the wrapper is BROKEN -- which
    is exactly the case a mutation run creates -- the damage is real and lands
    in the developer's working tree: one mutation run here left resolver.go
    stripped of 21 lines. The assertions capture state before restoring, so a
    failure still reports honestly, but the tree is not left broken either way.
    """
    env = dict(os.environ, GQLGEN_GENERATE_CMD=fake_generator)
    try:
        return subprocess.run(
            ["bash", str(WRAPPER)],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
    finally:
        subprocess.run(
            ["git", "-C", str(REPO_ROOT), "checkout", "--", str(GENERATED_AREA)],
            capture_output=True,
            text=True,
        )
        for stray in GENERATED_AREA.rglob("*.go"):
            tracked = subprocess.run(
                [
                    "git",
                    "-C",
                    str(REPO_ROOT),
                    "ls-files",
                    "--error-unmatch",
                    str(stray),
                ],
                capture_output=True,
                text=True,
            )
            if tracked.returncode != 0:
                stray.unlink()


def _run_wrapper_capturing(
    fake_generator: str,
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    """Run the wrapper and read the area BEFORE the tree is put back."""
    captured: dict[str, str] = {}
    env = dict(os.environ, GQLGEN_GENERATE_CMD=fake_generator)
    try:
        result = subprocess.run(
            ["bash", str(WRAPPER)],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        captured = _area_digests()
        return result, captured
    finally:
        subprocess.run(
            ["git", "-C", str(REPO_ROOT), "checkout", "--", str(GENERATED_AREA)],
            capture_output=True,
            text=True,
        )
        for stray in list(GENERATED_AREA.rglob("*.go")):
            tracked = subprocess.run(
                [
                    "git",
                    "-C",
                    str(REPO_ROOT),
                    "ls-files",
                    "--error-unmatch",
                    str(stray),
                ],
                capture_output=True,
                text=True,
            )
            if tracked.returncode != 0:
                stray.unlink()


def test_the_wrapper_restores_the_area_when_generation_fails() -> None:
    """Executes the WRAPPER, which nothing did before.

    Round r6 mutated it to `exit 0`, disabled its restore, removed its signal
    traps and disabled its missing-output check; every mutation survived,
    because the suite only ever read the file. This runs it against a
    generator that behaves like the real one on failure -- deleting its
    outputs first -- and requires the tree to come back byte-identical.
    """
    before = _area_digests()
    result, after = _run_wrapper_capturing(
        "rm -f internal/graph/generated.go internal/graph/model/models_gen.go; exit 1"
    )
    assert after == before, (
        "the wrapper did not restore the generated area after a failed run. "
        f"changed: {sorted(k for k in before if before.get(k) != after.get(k))}; "
        f"missing: {sorted(set(before) - set(after))}\n{result.stderr}"
    )
    assert result.returncode != 0, (
        "generation failed and the wrapper reported success. A wrapper that "
        "cannot fail cannot protect anything."
    )


def test_the_wrapper_restores_a_hand_written_file_it_does_not_generate() -> None:
    """The P1 a file-SET comparison could not see.

    Pointing exec.filename at internal/graph/resolver.go makes gqlgen
    OVERWRITE a hand-written file. It still exists afterwards, so a set
    comparison sees nothing, and the earlier wrapper restored its three listed
    files, announced success and left resolver.go destroyed.
    """
    before = _area_digests()
    assert "resolver.go" in before, "resolver.go is the file under test"
    result = _run_wrapper(
        "printf 'package graph\\n' > internal/graph/resolver.go; exit 1"
    )
    after = _area_digests()
    assert after.get("resolver.go") == before["resolver.go"], (
        "generation overwrote the hand-written resolver.go and the wrapper left it "
        f"that way, so `type Resolver` is gone and the build is broken.\n{result.stderr}"
    )


def test_the_wrapper_removes_files_its_failed_run_created() -> None:
    before = _area_digests()
    result, after = _run_wrapper_capturing(
        "mkdir -p internal/graph/nested && printf 'package nested\\n' "
        "> internal/graph/nested/generated.go; exit 1"
    )
    assert set(after) == set(before), (
        "a failed run left files behind that it created. They are untracked "
        f"generator output and can break the build.\n{result.stderr}"
    )


def test_the_guard_passes_on_a_faithful_regeneration() -> None:
    """The positive control. Without it, 'always fail' is a passing guard.

    Every other behavioural test here asserts the guard FAILS. Round r6 mutated
    it to fail unconditionally with the expected diagnostics and that survived
    the whole suite. This replays a real regeneration and requires exit 0,
    which no always-fail mutant can satisfy.

    Generation happens in a COPY of the tree, never in the working tree: the
    generator deletes its outputs before writing them, and a test that had to
    `git checkout` afterwards would discard a developer's uncommitted work in
    that directory on any failure.
    """
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / "tree"
        copy.mkdir()
        copied = subprocess.run(
            "tar -C . --exclude=./.git --exclude=./.venv --exclude=./.uv-cache "
            f"--exclude=./node_modules -cf - . | tar -C {copy} -xf -",
            cwd=REPO_ROOT,
            shell=True,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert copied.returncode == 0, f"could not copy the tree: {copied.stderr}"
        generated = subprocess.run(
            "go run github.com/99designs/gqlgen generate --config gqlgen.yml",
            cwd=copy / "cmd" / "query-api",
            shell=True,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert generated.returncode == 0, (
            f"could not produce a real regeneration to replay: {generated.stderr}"
        )
        captured = copy / "cmd" / "query-api" / "internal" / "graph"
        result = _run_guard_with_fake_generator(f'cp -a "{captured}/." internal/graph/')

    assert result.returncode == 0, (
        "the guard FAILED on output identical to a real regeneration. It is "
        "failing unconditionally, which passes every failure-only test here "
        f"while protecting nothing.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "all documented" in result.stdout, (
        f"the guard exited 0 without reporting a comparison.\n{result.stdout}"
    )
