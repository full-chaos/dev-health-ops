"""Decide whether a change set is typecheck-relevant, for the always-run job.

WHY THIS EXISTS (CHAOS-4843, same shape as CHAOS-4834's go-quality fix).
`typecheck-mypy` used to be gated by a separate `changes` job
(`dorny/paths-filter`) with a job-level `if:` on its output. That means a
change the filter does not match makes GitHub report the job SKIPPED --
which satisfies a required check exactly as well as a real pass, and is
indistinguishable from one in the merge UI. The fix already applied to
`go-quality` (CHAOS-4834) is to stop deciding relevance in a job the
required check can see skip, and decide it INSIDE the one job that always
runs and always reports honestly.

THE PATTERN LIST IS COPIED, NOT SHARED, from the `changes` job this
replaces -- deliberately, not an oversight. `go_relevance.py` can read its
list from `go.yml`'s own `on.pull_request.paths` because that trigger-level
list is itself still load-bearing (it gates go.yml's OTHER jobs).
typecheck.yml's trigger carries no `paths:` filter at all -- only the
`changes` job's own `dorny/paths-filter` block declared this list, and that
job is deleted by this change with nothing left to read it from. Copying it
here, verbatim, is the zero-regression move; inventing a new shared source
of truth is a larger change than this ticket's fix and its own risk.

Includes `**/*.py` for the same reason CHAOS-4281 added it to the job this
replaces: the enumerated directories below (`src/**`, `tests/**`, `ci/**`,
`migrations/**`, `scripts/**`) do not cover `internal/**`'s live-Python-oracle
generators, and a corpus-only PR under `internal/**` must not read as
irrelevant. The enumerated directories are kept alongside it rather than
pruned as "now redundant" -- confirming that is safe is a separate,
non-blocking audit, not something to fold into this fix silently.
"""

from __future__ import annotations

import re
import sys

# Verbatim copy of the `changes` job's `dorny/paths-filter` `code:` list this
# script replaces (typecheck.yml, pre-CHAOS-4843). Keep it that way: the
# point of copying rather than deriving is that a future edit here is a
# visible, reviewable diff against a known-good list, not a silent drift.
RELEVANT_PATTERNS: list[str] = [
    ".github/workflows/**",
    "src/**",
    ".gitignore",
    ".ignore",
    "ruff.toml",
    ".ruff.toml",
    "mypy.ini",
    ".mypy.ini",
    "setup.cfg",
    "tests/**",
    "ci/**",
    "migrations/**",
    "requirements.txt",
    "pyproject.toml",
    "uv.lock",
    "scripts/**",
    "**/*.py",
    # CHAOS-4843, round 2 of #2169's peer review, P2b: lefthook.yml's own
    # `mypy` command/glob is what makes the LOCAL gate track this script's
    # decision at all (see lefthook.yml's own comment on this same list). A
    # lefthook.yml-only change (e.g. reverting the mypy command back to a
    # bare `mypy`, or narrowing its glob) is exactly the kind of edit this
    # relevance decision must not wave through as irrelevant -- it changes
    # WHETHER local typechecking runs, which is what this whole ticket
    # exists to keep in sync with CI.
    "lefthook.yml",
]


def github_glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a GitHub path filter to a regex.

    Identical translator to ci/go_relevance.py's -- see that module for the
    full rationale (in particular why `**/` must match ZERO OR MORE leading
    directories, not one or more). Duplicated rather than imported: these are
    two independent, directly-invoked CI scripts (`ci/` has no `__init__.py`,
    nothing here is a package), and a small translator kept identical by
    inspection is a smaller risk than adding an import mechanism this
    directory does not otherwise use.
    """
    unsupported = set("?[]!") & set(pattern)
    if unsupported:
        raise SystemExit(
            f"typecheck relevance pattern {pattern!r} uses {sorted(unsupported)}, "
            "which this translator does not implement. Extend "
            "github_glob_to_regex rather than letting the pattern be treated "
            "as literal text -- a filter that matches nothing silently marks "
            "real changes irrelevant and skips the gate."
        )
    out: list[str] = []
    index = 0
    while index < len(pattern):
        if pattern.startswith("**/", index):
            out.append("(?:.*/)?")
            index += 3
        elif pattern.startswith("**", index):
            out.append(".*")
            index += 2
        elif pattern[index] == "*":
            out.append("[^/]*")
            index += 1
        else:
            out.append(re.escape(pattern[index]))
            index += 1
    return re.compile("^" + "".join(out) + "$")


def is_relevant(changed: list[str], patterns: list[str]) -> list[str]:
    compiled = [github_glob_to_regex(p) for p in patterns]
    return [path for path in changed if any(rx.match(path) for rx in compiled)]


def main() -> int:
    # NUL-split, NOT line-split (CHAOS-4843, round 2 of #2169's peer review,
    # P2a/P3). This stdin comes from `ci/typecheck_relevant_diff.sh`'s
    # `git diff --name-only -z`, whose whole POINT is that a path is never
    # quoted or escaped -- it is the literal byte sequence, NUL-terminated.
    # Splitting on newlines instead would (a) still lose a path containing
    # its own embedded newline (the exact P2a gap `-z` exists to close --
    # the newline inside the name would be misread as a second, bogus
    # entry), and (b) required a `.strip()` to drop the trailing `\n`, which
    # ALSO stripped genuine leading/trailing whitespace from a real
    # filename (P3: a tracked file literally named ` mypy.ini` became
    # `mypy.ini` and matched the root mypy config's own pattern). NUL-
    # splitting needs no stripping at all -- there is nothing but the path
    # between one NUL and the next.
    changed = [path for path in sys.stdin.read().split("\0") if path]
    if not changed:
        # An empty diff is not evidence of irrelevance -- it usually means the
        # base ref was wrong. Fail closed and run the gate.
        print("no changed files resolved; treating as RELEVANT (fail closed)")
        print("relevant=true")
        return 0
    matched = is_relevant(changed, RELEVANT_PATTERNS)
    print(f"changed files: {len(changed)}; typecheck-relevant: {len(matched)}")
    for path in matched[:20]:
        print(f"  {path}")
    if len(matched) > 20:
        print(f"  ... and {len(matched) - 20} more")
    print(f"relevant={'true' if matched else 'false'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
