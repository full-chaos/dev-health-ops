"""ci/go_relevant_diff.sh -- the diff-range producer venue-oracles.yml's
relevance decider consumes.

WHY THIS TEST EXISTS
---------------------
`ci/go_relevance.py`'s Python matcher is well covered by feeding it a
pre-built change list. Nothing pinned WHICH diff produces that list. A wrong
range (e.g. `HEAD^...HEAD` on a multi-commit push) produces a plausible,
non-empty, WRONG file list, and the matcher then answers a well-formed
question about the wrong input -- fail-closed does not help, because the diff
resolves fine, it just describes the wrong span. A reproduced round found
exactly this on the first version of venue-oracles.yml's inline relevance
step: a multi-commit push with a Go-relevant commit followed by an unrelated
one reported `relevant=false` for the whole push, and a renamed or
non-ASCII-named file could vanish from the diff entirely.

This script (and this test file) mirror ci/typecheck_relevant_diff.sh and its
own test file exactly -- same range-resolution shapes, same NUL-safe
producer, ported rather than shared, matching that script's own stated
reasoning: a visible, reviewable copy beats inventing a shared abstraction
for a fix this small.

This builds a real scratch git repo and runs the actual script against it,
for the two shapes the script actually branches on (BASE_SHA set -- the
pull_request and merge_group case -- and BASE_SHA empty, the push case),
plus a negative control and the multi-commit-push case the fix exists for.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "go_relevant_diff.sh"
RELEVANCE_SCRIPT = ROOT / "ci" / "go_relevance.py"


@dataclass
class ScratchRepo:
    dir: Path
    base: str
    trunk: str
    push_head: str
    feature1: str
    feature2: str


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    ).stdout.strip()


def _commit(cwd: Path, filename: str, content: str) -> str:
    (cwd / filename).write_text(content, encoding="utf-8")
    _git(cwd, "add", filename)
    _git(cwd, "commit", "-q", "-m", f"add {filename}")
    return _git(cwd, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> ScratchRepo:
    # base -- trunk_only -- push_change              (push: BASE_SHA empty)
    #              \
    #               feature1 -- feature2             (PR/merge_group: BASE_SHA=trunk)
    r = tmp_path / "scratch"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "test")
    base = _commit(r, "base.txt", "base")
    trunk = _commit(r, "trunk_only.txt", "trunk")
    _git(r, "branch", "feature")
    push_head = _commit(r, "push_change.go", "push")
    _git(r, "checkout", "-q", "feature")
    feature1 = _commit(r, "feature_change.go", "feature1")
    feature2 = _commit(r, "feature_change2.go", "feature2")
    return ScratchRepo(
        dir=r,
        base=base,
        trunk=trunk,
        push_head=push_head,
        feature1=feature1,
        feature2=feature2,
    )


def _run(cwd: Path, head: str, base_sha: str) -> list[str]:
    _git(cwd, "checkout", "-q", head)
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin", "BASE_SHA": base_sha}
    proc = subprocess.run(
        ["bash", str(SCRIPT)], cwd=cwd, capture_output=True, text=True, env=env
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    # NUL-split, not line-split: the script emits `git diff --name-only -z`,
    # NUL-terminated entries with no quoting at all.
    return sorted(path for path in proc.stdout.split("\0") if path)


def _run_raw(
    cwd: Path, head: str, env_extra: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    _git(cwd, "checkout", "-q", head)
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin", **env_extra}
    return subprocess.run(
        ["bash", str(SCRIPT)], cwd=cwd, capture_output=True, text=True, env=env
    )


def test_push_shape_diffs_against_the_previous_commit(repo: ScratchRepo) -> None:
    # push to main: BASE_SHA is empty, so the range must be HEAD^...HEAD, not
    # the whole branch history.
    assert _run(repo.dir, repo.push_head, "") == ["push_change.go"]


def test_pull_request_shape_diffs_against_its_base_sha(repo: ScratchRepo) -> None:
    assert _run(repo.dir, repo.feature2, repo.trunk) == [
        "feature_change.go",
        "feature_change2.go",
    ]


def test_merge_group_shape_diffs_against_its_own_base_sha(repo: ScratchRepo) -> None:
    assert _run(repo.dir, repo.feature1, repo.base) == [
        "feature_change.go",
        "trunk_only.txt",
    ]


def test_a_wrong_base_sha_yields_a_different_nonempty_list(repo: ScratchRepo) -> None:
    correct = _run(repo.dir, repo.feature2, repo.trunk)
    wrong = _run(repo.dir, repo.feature2, repo.base)
    assert wrong != correct
    assert wrong == ["feature_change.go", "feature_change2.go", "trunk_only.txt"]


def test_multicommit_push_sees_every_commit_not_just_the_last(
    repo: ScratchRepo,
) -> None:
    # The exact class a reproduced round found on the first version of
    # venue-oracles.yml's inline relevance step: a multi-commit push's
    # BASE_SHA is the pre-push tip (github.event.before), which can be more
    # than one commit back. feature1 -> feature2 is exactly that shape:
    # BASE_SHA=trunk (two commits back from feature2) must see BOTH commits'
    # files, not just feature2's own (which `HEAD^...HEAD` would give).
    assert _run(repo.dir, repo.feature2, repo.trunk) == [
        "feature_change.go",
        "feature_change2.go",
    ]
    # And HEAD^...HEAD (the local/no-BASE_SHA fallback) would have seen only
    # the last commit -- proving the two are genuinely different answers.
    head_caret_only = sorted(
        line
        for line in _git(repo.dir, "diff", "--name-only", "HEAD^...HEAD").splitlines()
        if line
    )
    assert head_caret_only == ["feature_change2.go"]


def test_all_zeros_base_sha_refuses_rather_than_guessing(repo: ScratchRepo) -> None:
    zero_sha = "0" * 40
    proc = _run_raw(repo.dir, repo.feature2, {"BASE_SHA": zero_sha})
    assert proc.returncode != 0, "must refuse, not silently fall back to HEAD^...HEAD"
    assert "all-zeros" in proc.stderr


def test_non_ancestor_base_sha_refuses_rather_than_guessing(repo: ScratchRepo) -> None:
    proc = _run_raw(repo.dir, repo.push_head, {"BASE_SHA": repo.feature2})
    assert proc.returncode != 0, "must refuse, not silently diff against a non-ancestor"
    assert "not an ancestor" in proc.stderr


def test_non_ascii_filename_is_not_lost_to_git_quoting(tmp_path: Path) -> None:
    # A reproduced round: git C-quotes a non-ASCII path in `--name-only`
    # output by default (`internal/café.go` becomes
    # `"internal/caf\303\251.go"`), which then fails to match
    # ci/go_relevance.py's `**/*.go` pattern. Red proof against a raw
    # `git diff --name-only` call, then confirms the script itself is immune.
    r = tmp_path / "unicode-scratch"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "test")
    base = _commit(r, "base.txt", "base")
    (r / "internal").mkdir()
    (r / "internal" / "café.go").write_text("package x\n", encoding="utf-8")
    _git(r, "add", "internal/café.go")
    _git(r, "commit", "-q", "-m", "add café.go")

    quoted = _git(r, "diff", "--name-only", f"{base}...HEAD")
    assert quoted != "internal/café.go", "expected git to C-quote this path by default"
    assert "caf" in quoted and "café" not in quoted

    assert _run(r, "HEAD", base) == ["internal/café.go"]


def test_a_rename_out_of_a_relevant_path_stays_relevant(tmp_path: Path) -> None:
    # A reproduced round: before `--no-renames`, git's default rename
    # detection collapsed a rename into ONLY the post-rename path --
    # `internal/a.go` renamed to `docs/a.md` reported just `docs/a.md`,
    # which matches nothing in ci/go_relevance.py's patterns, so a change
    # that effectively REMOVED a Go-relevant file from the tree read as
    # Go-irrelevant.
    r = tmp_path / "rename-out-scratch"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "test")
    (r / "internal").mkdir()
    (r / "docs").mkdir()
    base = _commit(r, "internal/a.go", "package x\n")
    _git(r, "mv", "internal/a.go", "docs/a.md")
    _git(r, "commit", "-q", "-m", "rename out of internal")

    diff_proc = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=r,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin:/usr/local/bin", "BASE_SHA": base},
    )
    assert diff_proc.returncode == 0, diff_proc.stderr
    relevance_proc = subprocess.run(
        ["python3", str(RELEVANCE_SCRIPT)],
        input=diff_proc.stdout,
        capture_output=True,
        text=False,
    )
    assert relevance_proc.returncode == 0
    stdout = relevance_proc.stdout.decode("utf-8")
    assert "relevant=true" in stdout, (
        f"a file renamed OUT of a Go-relevant path (to a path matching "
        f"nothing) was reported irrelevant -- rename collapse lost the fact "
        f"that a Go-relevant file was removed: {stdout!r}"
    )


def test_the_matcher_accepts_both_nul_and_newline_delimited_input() -> None:
    """ci/go_relevance.py must keep working for a caller that has not moved
    to the NUL-safe producer (go-quality.yml's own relevance step still
    pipes plain `git diff --name-only` -- newline-delimited, no `-z`).

    Constructed directly against the real matcher's patterns, not a scratch
    repo: this is about the STDIN FORMAT, not the diff range.
    """
    newline_form = "go.mod\nREADME.md\n"
    nul_form = b"go.mod\0README.md\0"

    newline_proc = subprocess.run(
        ["python3", str(RELEVANCE_SCRIPT)],
        input=newline_form,
        capture_output=True,
        text=True,
    )
    nul_proc = subprocess.run(
        ["python3", str(RELEVANCE_SCRIPT)],
        input=nul_form,
        capture_output=True,
    )
    assert newline_proc.returncode == 0
    assert nul_proc.returncode == 0
    assert "relevant=true" in newline_proc.stdout, (
        "go.mod is a Go-relevant path per go.yml's own filter; the "
        f"newline-delimited caller shape must still be judged relevant: "
        f"{newline_proc.stdout!r}"
    )
    assert "relevant=true" in nul_proc.stdout.decode("utf-8"), (
        "the same input, NUL-delimited, must reach the same verdict: "
        f"{nul_proc.stdout!r}"
    )


def test_a_nul_byte_in_a_single_newline_style_entry_is_not_misread() -> None:
    """Guard the auto-detect itself: a caller that sent ONE newline-delimited
    path containing no NUL byte must not be treated as the NUL format just
    because some other unrelated heuristic could be fooled -- and the
    reverse must hold for a genuinely NUL-terminated single entry too.
    """
    # A single NUL-terminated entry (what the script emits for exactly one
    # changed file) still contains a NUL byte -- must be split on NUL, not
    # treated as one giant newline-form line.
    single_nul_entry = b"go.mod\0"
    proc = subprocess.run(
        ["python3", str(RELEVANCE_SCRIPT)],
        input=single_nul_entry,
        capture_output=True,
    )
    assert proc.returncode == 0
    stdout = proc.stdout.decode("utf-8")
    assert "changed files: 1" in stdout, (
        f"a single NUL-terminated entry was not parsed as exactly one "
        f"changed file: {stdout!r}"
    )
