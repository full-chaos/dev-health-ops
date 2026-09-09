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


def test_go_quality_still_runs_the_drift_guard() -> None:
    """The step can be renamed; it cannot be removed."""
    assert "ci/check_gqlgen_drift.sh" in GO_QUALITY.read_text(), (
        "go-quality.yml no longer invokes ci/check_gqlgen_drift.sh. The gqlgen "
        "output drift is then unguarded: a regeneration silently reverts the "
        "nullability hand-edits (CHAOS-4650/4657/4658/4701/4703) and nothing "
        "fails. Restore the step or delete this test with a ticket saying why."
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
        assert "\t" in entry, f"entry is not <file><TAB><+|->content: {entry!r}"
        path, change = entry.split("\t", 1)
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
