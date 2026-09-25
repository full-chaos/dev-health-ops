"""ci/merge-tree-vet.sh catches what a textually clean merge hides (Trap #401).

WHY THIS TEST EXISTS (CHAOS-6691)
---------------------------------
#3044 added `Peek` to httpapi.CounterStore while #3041 added a test stub
without it. Each PR was green on its own base; the two merged back to back and
main's go-quality went red for every fresh merge ref. `git merge-tree` said
"clean": textual cleanliness is not evidence that the result builds. The script
builds the merge tree of <base> and <head> in a scratch directory and runs
`check_go.sh vet`, `check_go.sh integration-vet` and the venue registry check on
it, printing one OK/FAIL line.

Every case below runs the REAL script on a scratch git repository holding a tiny
Go module and the real ci scripts, and each guard is proved against a planted
defect: the same repository with the defect absent passes.

1. clean PR -> OK (exit 0), all three stages PASS.
2. THE INCIDENT SHAPE: a PR whose test stub lacks a method the new main added to
   the interface. `git merge-tree` exits 0 (textually clean) and the script
   FAILs at `vet` (exit 1).
3. A defect only visible under `-tags=integration` -> FAIL at `integration-vet`
   while plain `vet` PASSes.
4. A PR adding a venue test the registry does not name -> FAIL at `registry`.
5. A textual conflict -> FAIL at `merge`.
6. Every stage runs even after one fails (one run reports all the breaks).
7. Usage errors exit 2; the invoking checkout is untouched and no scratch
   directory is left behind.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CI_FILES = (
    "check_go.sh",
    "check_venue_oracle_registry.sh",
    "venue_oracle_discovery.awk",
    "venue_oracle_names.awk",
    "venue_oracle_proof.awk",
    "lib/venue_oracle_registry.sh",
    "merge-tree-vet.sh",
)

STORE = """package store

type Store interface{ Get() int }

type Real struct{}

func (Real) Get() int { return 1 }
"""
STORE_TEST = """package store

import "testing"

type stub struct{}

func (stub) Get() int { return 0 }

func TestStub(t *testing.T) { var _ Store = stub{} }
"""
# main moved on: the interface gained Peek and every existing implementer got it.
STORE_V2 = """package store

type Store interface {
	Get() int
	Peek() int
}

type Real struct{}

func (Real) Get() int  { return 1 }
func (Real) Peek() int { return 1 }
"""
STORE_TEST_V2 = STORE_TEST.replace(
    "func (stub) Get() int { return 0 }",
    "func (stub) Get() int  { return 0 }\nfunc (stub) Peek() int { return 0 }",
)
VENUE_TEST = """package venue

import "testing"

// A compiling stand-in for the harness import: the registry check reads the
// call, the scratch module has no harness package to import.
var venueoracle = struct{ WriteProof func(*testing.T) }{func(*testing.T) {}}

func TestVenueOracleOne(t *testing.T) { venueoracle.WriteProof(t) }
"""
REGISTRY = "internal/venue\tTestVenueOracleOne\trun\n"


def _git(
    repo: Path, *args: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.invalid",
    }
    return subprocess.run(
        ["git", *args], cwd=repo, env=env, capture_output=True, text=True, check=check
    )


def _write(repo: Path, rel: str, text: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


@pytest.fixture(scope="module")
def go_cache(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("go-cache")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """Scratch repo: branch `main0` = the base every PR branched from."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main0")
    _write(repo, "go.mod", "module example.com/x\n\ngo 1.24\n")
    _write(repo, "internal/store/store.go", STORE)
    _write(repo, "internal/store/store_test.go", STORE_TEST)
    _write(repo, "internal/venue/venue_test.go", VENUE_TEST)
    _write(repo, "ci/venue_oracle_registry.d/internal__venue.tsv", REGISTRY)
    (repo / "ci" / "lib").mkdir(parents=True, exist_ok=True)
    for name in CI_FILES:
        shutil.copy(ROOT / "ci" / name, repo / "ci" / name)
    _commit(repo, "base")
    return repo


def _branch(repo: Path, name: str, start: str = "main0") -> None:
    _git(repo, "checkout", "-q", "-b", name, start)


def _run(
    repo: Path, tmp_path: Path, go_cache: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    scratch_tmp = tmp_path / "tmp"
    scratch_tmp.mkdir(exist_ok=True)
    env = {
        **os.environ,
        "TMPDIR": str(scratch_tmp),
        "DEV_HEALTH_GO_CACHE": str(go_cache),
        "GIT_CONFIG_GLOBAL": os.devnull,
    }
    return subprocess.run(
        ["bash", str(repo / "ci" / "merge-tree-vet.sh"), *args],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=280,
        check=False,
    )


def _leftovers(tmp_path: Path) -> list[str]:
    return sorted(p.name for p in (tmp_path / "tmp").glob("merge-tree-vet.*"))


def _make_main_and_prs(repo: Path) -> None:
    """`main1` = main after the interface change; PR branches start at main0."""
    _branch(repo, "main1")
    _write(repo, "internal/store/store.go", STORE_V2)
    _write(repo, "internal/store/store_test.go", STORE_TEST_V2)
    _commit(repo, "main gains Peek")

    _branch(repo, "pr-clean")
    _write(repo, "internal/other/other.go", "package other\n")
    _commit(repo, "unrelated")

    _branch(repo, "pr-stub")  # the incident: a NEW stub without Peek
    _write(
        repo,
        "internal/store/extra_test.go",
        'package store\n\nimport "testing"\n\ntype stub2 struct{}\n\n'
        "func (stub2) Get() int { return 2 }\n\n"
        "func TestStub2(t *testing.T) { var _ Store = stub2{} }\n",
    )
    _commit(repo, "adds a stub that only knows Get")

    _branch(repo, "pr-tagged")
    _write(
        repo,
        "internal/store/integ_test.go",
        "//go:build integration\n\npackage store\n\nvar _ = undefinedUnderTheTag\n",
    )
    _commit(repo, "a break only the integration tag compiles")

    _branch(repo, "pr-unregistered")
    _write(
        repo,
        "internal/venue/second_test.go",
        'package venue\n\nimport "testing"\n\nfunc TestVenueOracleTwo(t *testing.T) {}\n',
    )
    _commit(repo, "a venue test with no registry row")

    _branch(repo, "pr-conflict")
    _write(repo, "internal/store/store.go", STORE.replace("return 1", "return 7"))
    _commit(repo, "edits the line main also edited")


def test_clean_pr_is_ok(repo: Path, tmp_path: Path, go_cache: Path) -> None:
    _make_main_and_prs(repo)
    proc = _run(repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-clean")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    for stage in ("vet", "integration-vet", "registry"):
        assert f"PASS {stage}" in proc.stdout
    assert "merge-tree-vet: OK pr=-" in proc.stdout
    assert _leftovers(tmp_path) == [], "the scratch tree was not removed"


def test_the_incident_shape_merges_clean_but_fails_vet(
    repo: Path, tmp_path: Path, go_cache: Path
) -> None:
    _make_main_and_prs(repo)
    # The textual check the scribe had: `git merge-tree` says clean...
    textual = _git(repo, "merge-tree", "--write-tree", "main1", "pr-stub", check=False)
    assert textual.returncode == 0, "the incident shape must merge textually clean"
    # ...and the gate still refuses it.
    proc = _run(repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-stub")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "FAIL vet" in proc.stderr
    assert "stub2" in proc.stderr and "Peek" in proc.stderr
    assert "merge-tree-vet: FAIL" in proc.stdout
    # Same repository without the defect passes (the guard is not always red).
    ok = _run(repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-clean")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert _leftovers(tmp_path) == []


def test_a_break_only_the_integration_tag_sees_fails_integration_vet(
    repo: Path, tmp_path: Path, go_cache: Path
) -> None:
    _make_main_and_prs(repo)
    proc = _run(repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-tagged")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "PASS vet" in proc.stdout, "plain vet must not see the tagged file"
    assert "FAIL integration-vet" in proc.stderr
    assert "PASS registry" in proc.stdout


def test_an_unregistered_venue_test_fails_the_registry_stage(
    repo: Path, tmp_path: Path, go_cache: Path
) -> None:
    _make_main_and_prs(repo)
    proc = _run(
        repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-unregistered"
    )
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "PASS vet" in proc.stdout
    assert "FAIL registry" in proc.stderr
    assert "TestVenueOracleTwo" in proc.stderr


def test_a_textual_conflict_fails_at_merge(
    repo: Path, tmp_path: Path, go_cache: Path
) -> None:
    _make_main_and_prs(repo)
    proc = _run(repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-conflict")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "textual conflict" in proc.stderr
    assert "merge-tree-vet: FAIL" in proc.stdout
    assert _leftovers(tmp_path) == []


def test_every_stage_runs_even_after_one_fails(
    repo: Path, tmp_path: Path, go_cache: Path
) -> None:
    _make_main_and_prs(repo)
    _branch(repo, "pr-both", "pr-stub")
    _write(
        repo,
        "internal/venue/second_test.go",
        'package venue\n\nimport "testing"\n\nfunc TestVenueOracleTwo(t *testing.T) {}\n',
    )
    _commit(repo, "and an unregistered venue test")
    proc = _run(repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-both")
    assert proc.returncode == 1
    assert "FAIL vet" in proc.stderr
    assert "FAIL registry" in proc.stderr, (
        "a later stage must still run after vet failed"
    )


def test_a_missing_stage_script_is_a_failure_not_a_skip(
    repo: Path, tmp_path: Path, go_cache: Path
) -> None:
    _make_main_and_prs(repo)
    _branch(repo, "pr-no-registry-script")
    _git(repo, "rm", "-q", "ci/check_venue_oracle_registry.sh")
    _git(repo, "commit", "-q", "-m", "delete the registry script")
    proc = _run(
        repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-no-registry-script"
    )
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "check_venue_oracle_registry.sh is missing" in proc.stderr


@pytest.mark.parametrize(
    "args",
    [
        (),
        ("--head",),
        ("abc",),
        ("--head", "nope-not-a-rev", "--base", "main0"),
        ("1", "2"),
    ],
)
def test_usage_errors_exit_two(
    repo: Path, tmp_path: Path, go_cache: Path, args: tuple[str, ...]
) -> None:
    proc = _run(repo, tmp_path, go_cache, *args)
    assert proc.returncode == 2, (args, proc.stdout, proc.stderr)


def test_the_invoking_checkout_is_left_untouched(
    repo: Path, tmp_path: Path, go_cache: Path
) -> None:
    _make_main_and_prs(repo)
    _git(repo, "checkout", "-q", "main0")
    before = _git(repo, "status", "--porcelain").stdout
    head_before = _git(repo, "rev-parse", "HEAD").stdout
    proc = _run(repo, tmp_path, go_cache, "--base", "main1", "--head", "pr-clean")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _git(repo, "status", "--porcelain").stdout == before
    assert _git(repo, "rev-parse", "HEAD").stdout == head_before


# --- where the Go build cache and the scratch tree go -----------------------
# A /tmp Go cache once reached 33.5 GB on a shared host. The stages are stubbed
# (a check_go.sh that records what it was given), so these tests need no Go.

PROBE_CHECK_GO = """#!/usr/bin/env bash
printf '%s\\n%s\\n' "${DEV_HEALTH_GO_CACHE-}" "$(pwd -P)" >> "${PROBE_FILE}"
exit 0
"""


def _probe_run(
    repo: Path,
    tmp_path: Path,
    probe_env: dict[str, str],
    unset: tuple[str, ...] = (),
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    _branch(repo, "probe")
    _write(repo, "ci/check_go.sh", PROBE_CHECK_GO)
    _commit(repo, "stub check_go.sh")
    probe = tmp_path / "probe.txt"
    env = {k: v for k, v in os.environ.items() if k not in unset}
    env.update({"PROBE_FILE": str(probe), "GIT_CONFIG_GLOBAL": os.devnull, **probe_env})
    proc = subprocess.run(
        [
            "bash",
            str(repo / "ci" / "merge-tree-vet.sh"),
            "--base",
            "main0",
            "--head",
            "probe",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return proc, probe.read_text(encoding="utf-8").splitlines()


def _fake_go(tmp_path: Path, cache: str) -> str:
    """A PATH prefix whose `go env GOCACHE` prints `cache`."""
    bindir = tmp_path / "fakebin"
    bindir.mkdir(exist_ok=True)
    go = bindir / "go"
    go.write_text(f'#!/usr/bin/env bash\n[ "$*" = "env GOCACHE" ] && echo {cache}\n')
    go.chmod(0o755)
    return f"{bindir}{os.pathsep}{os.environ['PATH']}"


def test_default_go_cache_is_the_invokers_gocache_not_tmp(
    repo: Path, tmp_path: Path
) -> None:
    scratch_tmp = tmp_path / "tmp"
    scratch_tmp.mkdir()
    _, seen = _probe_run(
        repo,
        tmp_path,
        {"TMPDIR": str(scratch_tmp), "GOCACHE": str(tmp_path / "mine")},
        unset=("DEV_HEALTH_GO_CACHE",),
    )
    assert seen[0] == str(tmp_path / "mine")
    assert not (scratch_tmp / "merge-tree-vet-gocache").exists()


def test_default_go_cache_falls_back_to_go_env_gocache(
    repo: Path, tmp_path: Path
) -> None:
    scratch_tmp = tmp_path / "tmp"
    scratch_tmp.mkdir()
    _, seen = _probe_run(
        repo,
        tmp_path,
        {
            "TMPDIR": str(scratch_tmp),
            "PATH": _fake_go(tmp_path, str(tmp_path / "goenv")),
        },
        unset=("DEV_HEALTH_GO_CACHE", "GOCACHE"),
    )
    assert seen[0] == str(tmp_path / "goenv")


def test_explicit_dev_health_go_cache_beats_gocache(repo: Path, tmp_path: Path) -> None:
    _, seen = _probe_run(
        repo,
        tmp_path,
        {
            "DEV_HEALTH_GO_CACHE": str(tmp_path / "explicit"),
            "GOCACHE": str(tmp_path / "mine"),
        },
    )
    assert seen[0] == str(tmp_path / "explicit")


def test_go_cache_uses_the_scratch_root_only_when_go_names_none(
    repo: Path, tmp_path: Path
) -> None:
    scratch_root = tmp_path / "scratch"
    bindir = tmp_path / "nogo"
    bindir.mkdir()
    go = bindir / "go"
    go.write_text("#!/usr/bin/env bash\nexit 1\n")
    go.chmod(0o755)
    _, seen = _probe_run(
        repo,
        tmp_path,
        {
            "DEV_HEALTH_SCRATCH": str(scratch_root),
            "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        },
        unset=("DEV_HEALTH_GO_CACHE", "GOCACHE"),
    )
    assert seen[0] == str(scratch_root / "merge-tree-vet-gocache")


def test_scratch_tree_honours_dev_health_scratch_over_tmpdir(
    repo: Path, tmp_path: Path
) -> None:
    scratch_tmp = tmp_path / "tmp"
    scratch_tmp.mkdir()
    scratch_root = tmp_path / "lane-scratch" / "nested"  # must be created
    _, seen = _probe_run(
        repo,
        tmp_path,
        {"TMPDIR": str(scratch_tmp), "DEV_HEALTH_SCRATCH": str(scratch_root)},
    )
    assert seen[1].startswith(str(scratch_root.resolve()) + os.sep + "merge-tree-vet.")
    assert _leftovers(tmp_path) == []
    assert list(scratch_root.glob("merge-tree-vet.*")) == []  # removed on exit
