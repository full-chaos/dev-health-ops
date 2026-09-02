"""Decide whether a change set is Go-relevant, for the always-run go-quality job.

WHY THIS EXISTS (CHAOS-4834). `go-quality` is a REQUIRED check. It used to be
declared in TWO workflows: `go.yml` (path-filtered, does the real work) and
`go-quality-noop.yml` (paths-ignore, reports a vacuous success) so that non-Go
pull requests could still satisfy the requirement. A change set touching both a
path-listed and an unlisted file triggers BOTH, two runs report the same required
context, and which one the branch protection honours is unspecified -- the no-op's
green can land first and satisfy a required check that never ran the gate.

The fix is to stop deciding relevance in the trigger and decide it INSIDE one
always-run job. That removes the duplicate context entirely: there is exactly one
workflow declaring `go-quality`, it always reports, and it reports honestly.

THE PATTERN LIST IS NOT RESTATED HERE. It is read from `go.yml`'s own
`on.pull_request.paths`, which still gates that workflow's other jobs. One list,
so the two cannot drift -- and it guarantees the invariant that matters: if a
change is Go-relevant enough to run the storage shards, it is Go-relevant enough
to run go-quality.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
GO_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "go.yml"


def github_glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a GitHub path filter to a regex.

    `**` spans separators; `*` does not; everything else is literal. Deliberately
    small, covering the forms the workflow actually uses.

    Anything it does not understand is a HARD ERROR rather than a literal. A
    `!negation` entry treated as literal text would match nothing and silently
    mark real changes irrelevant -- and because this module decides relevance in
    production, that would skip the gate on a live PR rather than merely weaken a
    test. Credit to lane-4441 for spotting that the shared blind spot is worse on
    this side of the line.
    """
    unsupported = set("?[]!") & set(pattern)
    if unsupported:
        raise SystemExit(
            f"go.yml path filter {pattern!r} uses {sorted(unsupported)}, which this "
            "translator does not implement. Extend github_glob_to_regex rather than "
            "letting the pattern be treated as literal text -- a filter that matches "
            "nothing silently marks real changes irrelevant and skips the gate."
        )
    out: list[str] = []
    index = 0
    while index < len(pattern):
        if pattern.startswith("**/", index):
            # `**/` matches ZERO OR MORE leading directories, so `**/*.go` must
            # match `root.go` as well as `internal/x/y.go`. Translating it as
            # `.*/` required at least one directory, which classified every
            # root-level `*.go`, `go.mod` and `go.sum` as IRRELEVANT -- a false
            # green on exactly the change class this module exists to catch, in
            # the mechanism that replaced the vacuous no-op.
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


def go_relevant_patterns() -> list[str]:
    """The single source of truth: go.yml's own pull_request path filter."""
    document = yaml.safe_load(GO_WORKFLOW.read_text(encoding="utf-8"))
    # PyYAML parses a bare `on:` key as the boolean True.
    on_block = document.get(True, document.get("on"))
    patterns = on_block["pull_request"]["paths"]
    if not patterns:
        raise SystemExit("go.yml declares no pull_request paths; refusing to guess")
    return list(patterns)


def is_relevant(changed: list[str], patterns: list[str]) -> list[str]:
    compiled = [github_glob_to_regex(p) for p in patterns]
    return [path for path in changed if any(rx.match(path) for rx in compiled)]


def main() -> int:
    changed = [line.strip() for line in sys.stdin if line.strip()]
    if not changed:
        # An empty diff is not evidence of irrelevance -- it usually means the
        # base ref was wrong. Fail closed and run the gate.
        print("no changed files resolved; treating as RELEVANT (fail closed)")
        print("relevant=true")
        return 0
    patterns = go_relevant_patterns()
    matched = is_relevant(changed, patterns)
    print(f"changed files: {len(changed)}; Go-relevant: {len(matched)}")
    for path in matched[:20]:
        print(f"  {path}")
    if len(matched) > 20:
        print(f"  ... and {len(matched) - 20} more")
    print(f"relevant={'true' if matched else 'false'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
