"""CHAOS-4843, lane-4752-go's peer read of #2169.

ci/typecheck_relevance.py's Python matcher is well covered by feeding it a
pre-built change list. Nothing pinned WHICH diff produces that list -- the
shell that picks `BASE_SHA...HEAD` vs `HEAD^...HEAD` (ci/typecheck_relevant_
diff.sh, extracted from typecheck.yml for exactly this reason) ran under
nothing. A wrong range on, say, a merge_group event produces a plausible,
non-empty, WRONG file list, and the matcher then answers a well-formed
question about the wrong input -- fail-closed does not help, because the
diff resolves fine, it just describes the wrong span.

This builds a real scratch git repo and runs the actual script against it,
for the two shapes the script actually branches on (BASE_SHA set -- the
pull_request and merge_group case, exercised here with two independently
built scenarios and different real commits -- and BASE_SHA empty, the push
case), plus a negative control: the SAME head, a WRONG base, a different
non-empty file list. GITHUB_EVENT_NAME's own workflow_dispatch short-circuit
lives in typecheck.yml, not this script, and is covered by
test_typecheck_relevance_treats_workflow_dispatch_as_always_relevant in
test_aggregate_gate_results.py -- not repeated here.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "typecheck_relevant_diff.sh"


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
    push_head = _commit(r, "push_change.py", "push")
    _git(r, "checkout", "-q", "feature")
    feature1 = _commit(r, "feature_change.py", "feature1")
    feature2 = _commit(r, "feature_change2.py", "feature2")
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
    return sorted(line for line in proc.stdout.splitlines() if line)


def test_push_shape_diffs_against_the_previous_commit(repo: ScratchRepo) -> None:
    # push to main: BASE_SHA is empty, so the range must be HEAD^...HEAD, not
    # the whole branch history.
    assert _run(repo.dir, repo.push_head, "") == ["push_change.py"]


def test_pull_request_shape_diffs_against_its_base_sha(repo: ScratchRepo) -> None:
    assert _run(repo.dir, repo.feature2, repo.trunk) == [
        "feature_change.py",
        "feature_change2.py",
    ]


def test_merge_group_shape_diffs_against_its_own_base_sha(repo: ScratchRepo) -> None:
    # A second, independently-built BASE_SHA-set scenario -- merge_group and
    # pull_request both set BASE_SHA and the script does not distinguish
    # them, so this proves the branch holds for a different concrete sha and
    # range shape, not just one lucky case. BASE_SHA is deliberately `base`,
    # not `trunk` (feature1's immediate parent): using the immediate parent
    # here would make this case pass under an "always HEAD^" mutation by
    # coincidence, which defeats its purpose as a red-proof target.
    assert _run(repo.dir, repo.feature1, repo.base) == [
        "feature_change.py",
        "trunk_only.txt",
    ]


def test_a_wrong_base_sha_yields_a_different_nonempty_list(repo: ScratchRepo) -> None:
    # Negative control: same HEAD as the pull_request case, but BASE_SHA one
    # commit further back than it should be. If the range selection were
    # broken (e.g. ignoring BASE_SHA entirely), this could accidentally
    # produce the SAME list as the correct case above -- it must not.
    correct = _run(repo.dir, repo.feature2, repo.trunk)
    wrong = _run(repo.dir, repo.feature2, repo.base)
    assert wrong != correct
    assert wrong == ["feature_change.py", "feature_change2.py", "trunk_only.txt"]


def _run_raw(
    cwd: Path, head: str, env_extra: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    _git(cwd, "checkout", "-q", head)
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin", **env_extra}
    return subprocess.run(
        ["bash", str(SCRIPT)], cwd=cwd, capture_output=True, text=True, env=env
    )


def test_multicommit_push_sees_every_commit_not_just_the_last(
    repo: ScratchRepo,
) -> None:
    # CHAOS-4843, 4752-go's peer read of #2169, round 1, P2a: a multi-commit
    # push's BASE_SHA is the pre-push tip (github.event.before), which can be
    # more than one commit back. feature1 -> feature2 is exactly that shape:
    # BASE_SHA=trunk (two commits back from feature2) must see BOTH commits'
    # files, not just feature2's own (which `HEAD^...HEAD` would give).
    assert _run(repo.dir, repo.feature2, repo.trunk) == [
        "feature_change.py",
        "feature_change2.py",
    ]
    # And HEAD^...HEAD (the local/no-BASE_SHA fallback) would have seen only
    # the last commit -- proving the two are genuinely different answers,
    # not the same range under two names.
    head_caret_only = sorted(
        line
        for line in _git(repo.dir, "diff", "--name-only", "HEAD^...HEAD").splitlines()
        if line
    )
    assert head_caret_only == ["feature_change2.py"]


def test_all_zeros_base_sha_refuses_rather_than_guessing(repo: ScratchRepo) -> None:
    # A new branch's first push reports `github.event.before` as the
    # all-zeros sentinel -- there is no previous commit to diff against.
    # Silently falling back to HEAD^...HEAD would still produce SOME answer,
    # which is exactly the wrong shape: an invalid BASE_SHA must refuse
    # (exit non-zero) so the caller's own fail-open branch takes over,
    # rather than the script guessing a range that happens to look plausible.
    zero_sha = "0" * 40
    proc = _run_raw(repo.dir, repo.feature2, {"BASE_SHA": zero_sha})
    assert proc.returncode != 0, "must refuse, not silently fall back to HEAD^...HEAD"
    assert "all-zeros" in proc.stderr


def test_non_ancestor_base_sha_refuses_rather_than_guessing(repo: ScratchRepo) -> None:
    # A force-push can leave `github.event.before` pointing at a commit no
    # longer reachable from the new HEAD. Using it anyway would silently
    # diff against history that no longer exists on this branch; the script
    # must refuse instead.
    proc = _run_raw(repo.dir, repo.push_head, {"BASE_SHA": repo.feature2})
    assert proc.returncode != 0, "must refuse, not silently diff against a non-ancestor"
    assert "not an ancestor" in proc.stderr


def test_non_ascii_filename_is_not_lost_to_git_quoting(tmp_path: Path) -> None:
    # CHAOS-4843, 4752-go's peer read of #2169, round 1, P2b: git C-quotes a
    # non-ASCII path in `--name-only` output by default (`src/café.py`
    # becomes `"src/caf\303\251.py"`), which then fails to match
    # ci/typecheck_relevance.py's `src/**` pattern. This is a red-proof
    # against a raw `git diff --name-only` call (WITHOUT the script's own
    # `-c core.quotePath=false`), confirming the quoting genuinely happens
    # on this host/git version, then confirms the script itself is immune.
    r = tmp_path / "unicode-scratch"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "test")
    base = _commit(r, "base.txt", "base")
    (r / "src").mkdir()
    (r / "src" / "café.py").write_text("x = 1\n", encoding="utf-8")
    _git(r, "add", "src/café.py")
    _git(r, "commit", "-q", "-m", "add café.py")

    # Red proof: the default (quoted) form loses the path's real name.
    quoted = _git(r, "diff", "--name-only", f"{base}...HEAD")
    assert quoted != "src/café.py", "expected git to C-quote this path by default"
    assert "caf" in quoted and "café" not in quoted

    # The script itself must not reproduce that loss.
    assert _run(r, "HEAD", base) == ["src/café.py"]
