"""The pull request venue-oracle selection also follows declared paths (CHAOS-9172).

`ci/check_go.sh venue-oracles --changed FILE` selects a venue test by the paths of
its own package. A venue test compares what code in other packages does, so a
change of a `teams` write in internal/api/teamsidentity selected nothing, the
pull request job read green, and the protected-routes test went red on main.
ci/venue_oracle_triggers.d/ declares such paths. These tests hold:

- the real tree: a change of a `teams` write, or of a ClickHouse migration, selects
  the protected-routes test;
- the selection rule on a scratch tree: a declared path selects exactly its test,
  whole path segments only, and the package rule is unchanged;
- the run (not only the list) runs the test a declared path selects;
- a trigger row that could go stale without a sign fails the static check.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests.tooling.test_venue_oracle_shards import ROOT, _run_verb, _scratch_tree

PROTECTED_ROUTES = ("internal/apiservice", "TestVenueOracleProtectedRoutes")


def _selected(tree: Path, *changed: str, nul: bool = False) -> list[tuple[str, str]]:
    """The (package, test) rows `--changed FILE --list` selects for `changed`."""
    listing = tree / "changed.txt"
    separator = "\0" if nul else "\n"
    listing.write_text("".join(path + separator for path in changed), encoding="utf-8")
    proc, _ = _run_verb(tree, "--changed", str(listing), "--list")
    assert proc.returncode == 0, proc.stderr
    rows = []
    for line in proc.stdout.splitlines():
        if line.startswith("SELECT\t"):
            _, package, test = line.split("\t")
            rows.append((package, test))
    return rows


def _real_selected(tmp_path: Path, *changed: str) -> list[tuple[str, str]]:
    listing = tmp_path / "changed.txt"
    listing.write_text("".join(path + "\0" for path in changed), encoding="utf-8")
    proc = subprocess.run(
        [
            "bash",
            str(ROOT / "ci" / "check_go.sh"),
            "venue-oracles",
            "--changed",
            str(listing),
            "--list",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return [
        tuple(line.split("\t")[1:])
        for line in proc.stdout.splitlines()
        if line.startswith("SELECT\t")
    ]


def _triggers(tree: Path, package: str, *rows: tuple[str, str]) -> Path:
    directory = tree / "ci" / "venue_oracle_triggers.d"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (package.replace("/", "__") + ".tsv")
    path.write_text(
        "".join(f"{package}\t{test}\t{target}\n" for test, target in rows),
        encoding="utf-8",
    )
    return path


def _check(tree: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(tree / "ci" / "check_venue_oracle_registry.sh")],
        cwd=tree,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """Two venue packages and one plain package the first one depends on."""
    scratch = _scratch_tree(
        tmp_path,
        {
            "internal/api": ["TestVenueOracleRoutes", "TestVenueOracleSettings"],
            "internal/jobs": ["TestVenueOracleJobs"],
        },
    )
    store = scratch / "internal" / "store"
    (store / "sub").mkdir(parents=True)
    (store / "store.go").write_text("package store\n", encoding="utf-8")
    (store / "sub" / "sub.go").write_text("package sub\n", encoding="utf-8")
    (scratch / "internal" / "storefront").mkdir()
    (scratch / "internal" / "storefront" / "x.go").write_text(
        "package storefront\n", encoding="utf-8"
    )
    return scratch


# --- the real tree ----------------------------------------------------------


@pytest.mark.parametrize(
    "changed",
    [
        "internal/api/teamsidentity/store.go",
        "internal/api/teamsidentity/drift_review.go",
        "internal/chmigrate/sql/115_teams_deleted_at.sql",
    ],
)
def test_a_change_of_a_teams_write_selects_the_protected_routes_test(
    tmp_path: Path, changed: str
) -> None:
    assert PROTECTED_ROUTES in _real_selected(tmp_path, changed), (
        f"a pull request that changes only {changed} does not select the "
        "protected-routes venue test: it reads green while main goes red"
    )


def test_a_change_far_from_any_venue_test_still_selects_nothing(tmp_path: Path) -> None:
    assert _real_selected(tmp_path, "internal/chmigrate/runner.go") == []
    assert _real_selected(tmp_path, "docs/index.md") == []


def test_the_real_trigger_rows_pass_the_static_check() -> None:
    proc = _check(ROOT)
    assert proc.returncode == 0, proc.stderr
    declared = sum(
        1
        for file in (ROOT / "ci" / "venue_oracle_triggers.d").glob("*.tsv")
        for line in file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )
    assert f"venue-oracle-triggers: {declared} declared path(s)" in proc.stdout, (
        proc.stdout
    )


# --- the selection rule -----------------------------------------------------


def test_without_a_trigger_a_dependency_change_selects_nothing(tree: Path) -> None:
    assert _selected(tree, "internal/store/store.go") == []


def test_a_declared_directory_selects_exactly_its_test(tree: Path) -> None:
    _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    want = [("internal/api", "TestVenueOracleRoutes")]
    assert _selected(tree, "internal/store/store.go") == want
    assert _selected(tree, "internal/store/sub/sub.go") == want
    assert _selected(tree, "internal/store/store.go", nul=True) == want


def test_a_declared_file_selects_only_for_that_file(tree: Path) -> None:
    _triggers(
        tree, "internal/api", ("TestVenueOracleRoutes", "internal/store/store.go")
    )
    assert _selected(tree, "internal/store/store.go") == [
        ("internal/api", "TestVenueOracleRoutes")
    ]
    assert _selected(tree, "internal/store/sub/sub.go") == []
    assert _selected(tree, "internal/store/store.go.orig") == []


def test_a_declared_path_matches_whole_segments_only(tree: Path) -> None:
    _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    assert _selected(tree, "internal/storefront/x.go") == []
    assert _selected(tree, "x/internal/store/store.go") == []


def test_the_package_rule_is_unchanged_beside_a_trigger(tree: Path) -> None:
    _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    assert _selected(tree, "internal/api/x_test.go") == [
        ("internal/api", "TestVenueOracleRoutes"),
        ("internal/api", "TestVenueOracleSettings"),
    ]
    assert _selected(tree, "internal/jobs/x_test.go") == [
        ("internal/jobs", "TestVenueOracleJobs")
    ]


def test_two_tests_can_declare_the_same_path(tree: Path) -> None:
    _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    _triggers(tree, "internal/jobs", ("TestVenueOracleJobs", "internal/store"))
    assert _selected(tree, "internal/store/store.go") == [
        ("internal/api", "TestVenueOracleRoutes"),
        ("internal/jobs", "TestVenueOracleJobs"),
    ]


def test_a_trigger_selects_the_test_of_its_own_package_only(tmp_path: Path) -> None:
    """Two packages can hold a test of the same name: the row names one of them."""
    scratch = _scratch_tree(
        tmp_path,
        {
            "internal/api": ["TestVenueOracleSame"],
            "internal/jobs": ["TestVenueOracleSame"],
        },
    )
    (scratch / "internal" / "store").mkdir()
    _triggers(scratch, "internal/jobs", ("TestVenueOracleSame", "internal/store"))
    assert _selected(scratch, "internal/store/store.go") == [
        ("internal/jobs", "TestVenueOracleSame")
    ]


def test_the_run_runs_the_test_a_declared_path_selects(tree: Path) -> None:
    _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    listing = tree / "changed.txt"
    listing.write_text("internal/store/store.go\n", encoding="utf-8")
    proc, ran = _run_verb(tree, "--changed", str(listing))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert ran == [("internal/api", "TestVenueOracleRoutes")]


def test_the_run_without_a_trigger_is_the_loud_no_op(tree: Path) -> None:
    listing = tree / "changed.txt"
    listing.write_text("internal/store/store.go\n", encoding="utf-8")
    proc, ran = _run_verb(tree, "--changed", str(listing))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert ran == []
    assert "nothing to run for this change set" in proc.stdout


# --- a trigger row cannot go stale without a sign ---------------------------


def test_a_good_trigger_row_passes_the_static_check(tree: Path) -> None:
    _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    proc = _check(tree)
    assert proc.returncode == 0, proc.stderr
    assert "venue-oracle-triggers: 1 declared path(s)" in proc.stdout


@pytest.mark.parametrize(
    ("package", "row", "needle"),
    [
        ("internal/api", ("TestVenueOracleGone", "internal/store"), "is not a run row"),
        ("internal/api", ("TestVenueOracleRoutes", "internal/gone"), "does not exist"),
        (
            "internal/api",
            ("TestVenueOracleRoutes", "internal/api/x_test.go"),
            "inside the package of the test",
        ),
        (
            "internal/api",
            ("TestVenueOracleRoutes", "/internal/store"),
            "is not <package>",
        ),
        (
            "internal/api",
            ("TestVenueOracleRoutes", "internal/store/"),
            "is not <package>",
        ),
        (
            "internal/api",
            ("TestVenueOracleRoutes", "internal/../internal/store"),
            "is not <package>",
        ),
    ],
)
def test_a_trigger_row_that_selects_nothing_fails_the_static_check(
    tree: Path, package: str, row: tuple[str, str], needle: str
) -> None:
    _triggers(tree, package, row)
    proc = _check(tree)
    assert proc.returncode != 0, proc.stdout
    assert needle in proc.stderr, proc.stderr


def test_a_trigger_for_a_local_only_test_fails(tmp_path: Path) -> None:
    scratch = _scratch_tree(
        tmp_path,
        {"internal/api": ["TestVenueOracleLocal", "TestVenueOracleRoutes"]},
        local=("TestVenueOracleLocal",),
    )
    (scratch / "internal" / "store").mkdir()
    _triggers(scratch, "internal/api", ("TestVenueOracleLocal", "internal/store"))
    proc = _check(scratch)
    assert proc.returncode != 0, proc.stdout
    assert "is not a run row" in proc.stderr, proc.stderr


def test_unsorted_or_duplicate_trigger_rows_fail(tree: Path) -> None:
    (tree / "internal" / "alpha").mkdir()
    _triggers(
        tree,
        "internal/api",
        ("TestVenueOracleRoutes", "internal/store"),
        ("TestVenueOracleRoutes", "internal/alpha"),
    )
    proc = _check(tree)
    assert proc.returncode != 0, proc.stdout
    assert "must be sorted" in proc.stderr, proc.stderr
    _triggers(
        tree,
        "internal/api",
        ("TestVenueOracleRoutes", "internal/store"),
        ("TestVenueOracleRoutes", "internal/store"),
    )
    proc = _check(tree)
    assert proc.returncode != 0, proc.stdout
    assert "must be sorted" in proc.stderr, proc.stderr


def test_a_row_in_the_wrong_packages_file_fails(tree: Path) -> None:
    path = _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    path.rename(path.with_name("internal__jobs.tsv"))
    proc = _check(tree)
    assert proc.returncode != 0, proc.stdout
    assert "belongs in internal__api.tsv" in proc.stderr, proc.stderr


def test_a_stray_or_empty_trigger_file_fails(tree: Path) -> None:
    directory = tree / "ci" / "venue_oracle_triggers.d"
    directory.mkdir(parents=True)
    (directory / "notes.txt").write_text("x\n", encoding="utf-8")
    proc = _check(tree)
    assert proc.returncode != 0, proc.stdout
    assert "stray entry notes.txt" in proc.stderr, proc.stderr
    (directory / "notes.txt").unlink()
    (directory / "internal__api.tsv").write_text("# nothing\n", encoding="utf-8")
    proc = _check(tree)
    assert proc.returncode != 0, proc.stdout
    assert "holds no rows" in proc.stderr, proc.stderr


def test_a_broken_trigger_file_stops_the_selection(tree: Path) -> None:
    """The pull request job must not read a broken file as 'nothing selected'."""
    directory = tree / "ci" / "venue_oracle_triggers.d"
    directory.mkdir(parents=True)
    (directory / "internal__api.tsv").write_text(
        "internal/api\tonly-two\n", encoding="utf-8"
    )
    listing = tree / "changed.txt"
    listing.write_text("internal/store/store.go\n", encoding="utf-8")
    proc, _ = _run_verb(tree, "--changed", str(listing), "--list")
    assert proc.returncode != 0, proc.stdout
    assert "could not be read" in proc.stderr, proc.stderr


# --- the pull request job's own selection step ------------------------------

CHANGED_WORKFLOW = ROOT / ".github" / "workflows" / "venue-oracles-changed.yml"


def _run_select_step(tree: Path) -> tuple[subprocess.CompletedProcess[str], str]:
    """Run the job's real selection step on a commit that changes internal/store."""
    import os

    import yaml

    workflow = yaml.safe_load(CHANGED_WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["venue-oracles-changed"]["steps"]
    (script,) = [step["run"] for step in steps if step.get("id") == "select"]

    def git(*args: str) -> str:
        return subprocess.run(
            [
                "git",
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.com",
                "-c",
                "commit.gpgsign=false",
                *args,
            ],
            cwd=tree,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    git("init", "-q")
    git("add", "-A")
    git("commit", "-q", "-m", "base")
    base = git("rev-parse", "HEAD")
    (tree / "internal" / "store" / "store.go").write_text(
        "package store\n\n// changed\n", encoding="utf-8"
    )
    git("commit", "-q", "-am", "head")
    head = git("rev-parse", "HEAD")
    output = tree / "github_output.txt"
    output.write_text("")
    proc = subprocess.run(
        ["bash", "-c", script],
        cwd=tree,
        env={
            **os.environ,
            "BASE_SHA": base,
            "HEAD_SHA": head,
            "GITHUB_OUTPUT": str(output),
        },
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    return proc, output.read_text()


def test_the_job_step_runs_the_tests_of_a_declared_path(tree: Path) -> None:
    _triggers(tree, "internal/api", ("TestVenueOracleRoutes", "internal/store"))
    proc, output = _run_select_step(tree)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert output == "run=true\n"
    assert "SELECT\tinternal/api\tTestVenueOracleRoutes" in proc.stdout


def test_the_job_step_says_not_run_when_nothing_is_selected(tree: Path) -> None:
    proc, output = _run_select_step(tree)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert output == "run=false\n"
    assert "VENUE ORACLES NOT RUN" in proc.stdout


def test_the_job_step_fails_on_a_trigger_file_it_cannot_read(tree: Path) -> None:
    directory = tree / "ci" / "venue_oracle_triggers.d"
    directory.mkdir(parents=True)
    (directory / "internal__api.tsv").write_text(
        "internal/api\tonly-two\n", encoding="utf-8"
    )
    proc, output = _run_select_step(tree)
    assert proc.returncode != 0, (
        "a trigger file that cannot be read was reported as 'nothing selected': "
        + proc.stdout
    )
    assert output == ""
