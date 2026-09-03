"""No tracked file may match one of the repository's own ignore rules.

WHY THIS EXISTS

`.gitignore` line 75 once read `dev-health-workerctl` — a bare name, plainly
meant for the built binary. A bare name also matches the *directory*
`cmd/dev-health-workerctl/`, so its three tracked Go sources matched an ignore
rule. Git does not apply ignore rules to already-tracked files, so they stayed
tracked and the mistake stayed invisible.

`rg` has no concept of tracked-ness. It honours the pattern on its recursive
walk and skipped them:

    rg   -l 'package main' cmd/   ->  94 files,  0 workerctl
    grep -rl 'package main' cmd/  ->  97 files,  3 workerctl

So a sweep that concluded "no matches" was wrong about three tracked source
files, silently, and this landed as the fleet moved from `grep` to `rg` — where
re-running a sweep returns a *smaller* answer that reads as clean.

WHY IT WAS HARD TO SEE

`git check-ignore` **skips indexed files by default** and reports "no match" for
exactly these files. Only `--no-index` reveals the rule. The obvious diagnostic
hides the defect, which is why this needs a test rather than a habit.

WHAT THIS CHECKS, AND WHAT IT DELIBERATELY DOES NOT

Only the repository's OWN ignore files. `.git/info/exclude` is per-clone and the
global gitignore is per-machine; on this host they hide a further 14 tracked
files for reasons no contributor can fix from the repo. Including them would
make this a false-alarm generator, and a gate that is usually wrong is worse
than no gate.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _repo_ignored_tracked_files() -> list[str]:
    """Tracked files that a repository-owned ignore rule hides.

    Two subprocesses for the whole tree, not two per file: `check-ignore`
    accepts `--stdin`, and the naive per-file form took two minutes on 5,400
    files. A gate that slow is a gate that gets skipped.

    Output is NUL-separated `source, line, pattern, path` quadruples.

    NEGATIONS MUST BE FILTERED OUT. `-v` reports the last matching rule, and a
    negation is a match meaning the file is NOT ignored — `!src/**/benchmarking/**`
    accounts for 13 of the 27 rows here, all of them correct. Reading the rows
    without checking for a leading `!` flags nine perfectly good files.

    Only the repository's OWN ignore files count. `.git/info/exclude` is
    per-clone and the global gitignore is per-machine; on this host they hide a
    further 14 tracked files for reasons no contributor can fix from the repo.
    Including them would make this a false-alarm generator.
    """
    listed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
    ).stdout
    explained = subprocess.run(
        ["git", "check-ignore", "-v", "--no-index", "-z", "--stdin"],
        cwd=REPO_ROOT,
        input=listed,
        capture_output=True,
    ).stdout.decode()

    fields = explained.split("\0")
    offenders = []
    for index in range(0, len(fields) - 3, 4):
        source, _line, pattern, path = fields[index : index + 4]
        if pattern.startswith("!"):
            continue  # a negation: this file is re-included, not hidden
        if source == ".gitignore" or source.endswith("/.gitignore"):
            offenders.append(f"{source}: {pattern}  ->  {path}")
    return offenders


def test_no_tracked_file_matches_a_repository_ignore_rule() -> None:
    offenders = _repo_ignored_tracked_files()
    assert not offenders, (
        "These tracked files match one of the repository's own ignore rules, so "
        "`rg` and every other gitignore-aware tool will skip them on a recursive "
        "walk while git still tracks them:\n\n  "
        + "\n  ".join(sorted(offenders))
        + "\n\nFix the RULE, not the file. Two shapes work and one does not:\n"
        "  bare name matching a directory -> anchor it:      /dev-health-workerctl\n"
        "  broad content glob             -> negate beside:  !path/to/tracked\n"
        "  trailing-slash DIRECTORY rule  -> negation FAILS; git will not\n"
        "      re-include a file inside an excluded directory. Rewrite the rule\n"
        "      as a content glob first (dir/**), then negate."
    )
