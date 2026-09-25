"""Every venue oracle test is in `ci/venue_oracle_registry.d/`, and the registry is what runs.

WHY THIS TEST EXISTS (CHAOS-6584, Trap #392)
--------------------------------------------
`ci/venue_oracle_discovery.awk` finds venue tests by STRUCTURE (a call that
reaches `internal/testsupport/venueoracle`). A test it cannot reach was
dropped from the hosted `venue-oracles` job while the job still read green
(TestVenueOracleQueryAPIResponseModels, run 36030150097). The job now runs a
checked-in registry, and `ci/check_venue_oracle_registry.sh` fails when a
venue test exists (found by structure OR by a `Test*VenueOracle*` name) that
the registry does not name, or the registry names one that does not exist.

The hosted job runs on main and by hand only (R430), so THIS file is the
per-PR guard: it needs no Go toolchain, containers or Python api.

Every guard below is proved against a planted defect in a scratch tree: the
baseline passes, the defect fails and names the culprit.
"""

from __future__ import annotations

import os
import shutil
import stat
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
)
HARNESS = '"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"'


def _run(
    tree: Path,
    *verb: str,
    script: str = "check_venue_oracle_registry.sh",
    env_extra: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "TMPDIR": str(tree / "tmp"),
        "DEV_HEALTH_GO_CACHE": str(tree / "tmp" / "gocache"),
        **(env_extra or {}),
    }
    (tree / "tmp").mkdir(exist_ok=True)
    return subprocess.run(
        ["bash", str(tree / "ci" / script), *verb],
        cwd=tree,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _go_test(
    package: str,
    *funcs: str,
    harness: bool = False,
    marker: str = "",
    proof: str = "\tvenueoracle.Diff(t)\n",
) -> str:
    body = f"package {package}\n\n"
    if harness:
        body += f"import (\n\t{HARNESS}\n)\n\n"
    for name in funcs:
        if marker == name:
            body += "//venueoracle:local-only needs a key CI does not hold\n"
        body += f"func {name}(t *testing.T) {{\n{proof}}}\n\n"
    return body


REGISTRY_HEAD = "# test tree registry\n"


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """A scratch repo: the real script + awks, a tiny Go tree, its registry."""
    (tmp_path / "ci" / "lib").mkdir(parents=True)
    for name in CI_FILES:
        shutil.copy(ROOT / "ci" / name, tmp_path / "ci" / name)
    _write(tmp_path / "go.mod", "module example.com/x\n")
    # a: harness package, found by structure (names carry no VenueOracle).
    _write(
        tmp_path / "internal/a/a_test.go",
        _go_test("a", "TestAlpha", "TestBeta", harness=True),
    )
    # b: NO harness import; found by name only (the Trap #392 shape).
    _write(tmp_path / "internal/b/b_test.go", _go_test("b", "TestVenueOracleGamma"))
    # c: one local-only test beside a run test.
    _write(
        tmp_path / "internal/c/c_test.go",
        _go_test(
            "c",
            "TestDelta",
            "TestEpsilonVenueOracle",
            harness=True,
            marker="TestEpsilonVenueOracle",
        ),
    )
    _registry(
        tmp_path,
        [
            ("internal/a", "TestAlpha", "run"),
            ("internal/a", "TestBeta", "run"),
            ("internal/b", "TestVenueOracleGamma", "run"),
            ("internal/c", "TestDelta", "run"),
            ("internal/c", "TestEpsilonVenueOracle", "local"),
        ],
    )
    return tmp_path


def _registry(
    tree: Path,
    rows: list[tuple[str, str, str]],
    *,
    raw: str | None = None,
    sort: bool = True,
) -> None:
    """(Re)write ci/venue_oracle_registry.d/ from rows (or raw TSV lines): one
    file per package, named <package with / as __>.tsv (CHAOS-6724)."""
    directory = tree / "ci" / "venue_oracle_registry.d"
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    lines = (
        raw.splitlines() if raw is not None else [f"{p}\t{t}\t{m}" for p, t, m in rows]
    )
    groups: dict[str, list[str]] = {}
    for line in lines:
        if line.strip():
            groups.setdefault(line.split("\t", 1)[0], []).append(line)
    for pkg, group in groups.items():
        body = sorted(group) if sort else group
        _write(
            directory / (pkg.replace("/", "__") + ".tsv"),
            REGISTRY_HEAD + "\n".join(body) + "\n",
        )


def _rows_of(directory: Path) -> list[str]:
    return [
        line
        for path in sorted(directory.glob("*.tsv"))
        for line in path.read_text().splitlines()
        if line and not line.startswith("#")
    ]


def _read_rows(tree: Path) -> list[str]:
    return _rows_of(tree / "ci" / "venue_oracle_registry.d")


# ---------------------------------------------------------------------------
# The real tree.
# ---------------------------------------------------------------------------


def test_real_tree_registry_matches_the_tree() -> None:
    proc = _run_in_repo()
    assert proc.returncode == 0, proc.stderr + proc.stdout
    registered = _rows_of(ROOT / "ci" / "venue_oracle_registry.d")
    # A measurement that did not happen must fail: an empty or truncated
    # registry cannot read as covered.
    assert len(registered) >= 45, f"only {len(registered)} registry rows"
    assert f"{len(registered)} registered test(s)" in proc.stdout


def _run_in_repo() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(ROOT / "ci" / "check_venue_oracle_registry.sh")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


# ---------------------------------------------------------------------------
# Planted defects: baseline passes, each defect fails naming the culprit.
# ---------------------------------------------------------------------------


def test_scratch_baseline_passes(tree: Path) -> None:
    proc = _run(tree)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "5 registered test(s)" in proc.stdout


def test_a_structure_found_test_left_out_of_the_registry_fails(tree: Path) -> None:
    _write(
        tree / "internal/a/extra_test.go",
        _go_test("a", "TestZeta", harness=True),
    )
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal/a TestZeta" in proc.stderr
    assert "NOT in ci/venue_oracle_registry.d/" in proc.stderr
    assert "internal__a.tsv" in proc.stderr


def test_a_name_found_test_in_a_non_harness_package_left_out_fails(tree: Path) -> None:
    """The Trap #392 shape: the package never imports the harness."""
    _write(
        tree / "internal/d/d_test.go",
        _go_test("d", "TestVenueOracleOmega"),
    )
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal/d TestVenueOracleOmega" in proc.stderr


def test_a_registry_row_with_no_test_fails(tree: Path) -> None:
    rows = _read_rows(tree) + ["internal/a\tTestGhost\trun"]
    _registry(tree, [], raw="\n".join(rows) + "\n")
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal/a TestGhost" in proc.stderr
    assert "found 0" in proc.stderr


def test_a_registry_row_with_no_package_fails(tree: Path) -> None:
    rows = _read_rows(tree) + ["internal/zzz\tTestNowhere\trun"]
    _registry(tree, [], raw="\n".join(rows) + "\n")
    proc = _run(tree)
    assert proc.returncode != 0
    assert "package dir internal/zzz does not exist" in proc.stderr


def test_a_test_declared_twice_in_one_package_fails(tree: Path) -> None:
    """An internal and an external test package can both declare a name."""
    _write(
        tree / "internal/a/a_x_test.go",
        _go_test("a_test", "TestAlpha", harness=True),
    )
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal/a TestAlpha" in proc.stderr
    assert "found 2" in proc.stderr


def test_mode_contradicting_the_local_only_marker_fails(tree: Path) -> None:
    rows = [
        r.replace("TestEpsilonVenueOracle\tlocal", "TestEpsilonVenueOracle\trun")
        for r in _read_rows(tree)
    ]
    _registry(tree, [], raw="\n".join(rows) + "\n")
    proc = _run(tree)
    assert proc.returncode != 0
    assert "TestEpsilonVenueOracle is registered as run" in proc.stderr


@pytest.mark.parametrize(
    "raw, needle",
    [
        ("", "holds no rows"),
        ("internal/a\tTestAlpha\n", "is not <package>"),
        ("internal/a\tTestAlpha\tmaybe\n", "is not <package>"),
    ],
)
def test_empty_or_malformed_registry_fails(tree: Path, raw: str, needle: str) -> None:
    _registry(tree, [], raw=raw)
    proc = _run(tree)
    assert proc.returncode != 0
    assert needle in proc.stderr


def test_unsorted_registry_fails(tree: Path) -> None:
    rows = list(reversed(_read_rows(tree)))
    _registry(tree, [], raw="\n".join(rows) + "\n", sort=False)
    proc = _run(tree)
    assert proc.returncode != 0
    assert "must be sorted and unique within a file" in proc.stderr


def test_duplicate_registry_row_fails(tree: Path) -> None:
    rows = _read_rows(tree)
    rows.insert(1, rows[0])
    _registry(tree, [], raw="\n".join(rows) + "\n", sort=False)
    proc = _run(tree)
    assert proc.returncode != 0
    assert "must be sorted and unique within a file" in proc.stderr


def test_missing_registry_directory_fails(tree: Path) -> None:
    shutil.rmtree(tree / "ci" / "venue_oracle_registry.d")
    proc = _run(tree)
    assert proc.returncode != 0
    assert "does not exist" in proc.stderr


# CHAOS-6724: one file per package, nothing shared between PRs.


def test_a_row_left_in_the_retired_single_file_fails(tree: Path) -> None:
    """A PR rebased across the migration that still edits the old file: the file
    must not exist, so its row can never be silently ignored."""
    _write(
        tree / "ci" / "venue_oracle_registry.tsv",
        "internal/a\tTestLegacyRow\trun\n",
    )
    proc = _run(tree)
    assert proc.returncode != 0
    assert "must not exist any more" in proc.stderr
    assert "TestLegacyRow" not in proc.stdout


def test_a_row_in_the_wrong_packages_file_fails(tree: Path) -> None:
    directory = tree / "ci" / "venue_oracle_registry.d"
    text = (directory / "internal__b.tsv").read_text()
    (directory / "internal__b.tsv").write_text(text + "internal/a\tTestStrayRow\trun\n")
    proc = _run(tree)
    assert proc.returncode != 0
    assert "package internal/a belongs in internal__a.tsv" in proc.stderr


def test_a_stray_file_in_the_registry_directory_fails(tree: Path) -> None:
    _write(
        tree / "ci" / "venue_oracle_registry.d" / "internal__a.tsv.orig",
        "internal/a\tTestAlpha\trun\n",
    )
    proc = _run(tree)
    assert proc.returncode != 0
    assert "stray entry internal__a.tsv.orig" in proc.stderr


def test_an_empty_package_file_fails(tree: Path) -> None:
    _write(tree / "ci" / "venue_oracle_registry.d" / "internal__e.tsv", "# nothing\n")
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal__e.tsv holds no rows" in proc.stderr


def test_two_prs_adding_tests_in_different_packages_touch_different_files(
    tree: Path,
) -> None:
    """The property CHAOS-6724 exists for: a row for a NEW package is a NEW file,
    and a row for another package edits a file the first PR never touches."""
    _write(
        tree / "internal/f/f_test.go", _go_test("f", "TestVenueOracleF", harness=True)
    )
    _write(
        tree / "ci" / "venue_oracle_registry.d" / "internal__f.tsv",
        "internal/f\tTestVenueOracleF\trun\n",
    )
    proc = _run(tree)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "6 registered test(s)" in proc.stdout


# ---------------------------------------------------------------------------
# The venue-oracles verb RUNS the registry (a fake `go` records what it is
# asked to run and writes the proof files a real oracle would).
# ---------------------------------------------------------------------------

FAKE_GO = r"""#!/usr/bin/env bash
# Fake `go test`: log the package and -run pattern, write an "executed" proof
# for each name in the pattern (what venueoracle.Diff does on a real run).
case "$1" in
  version) echo "go version go1.27.0 linux/amd64"; exit 0 ;;
  env) echo "${FAKE_GOROOT}"; exit 0 ;;
esac
pattern="" ; pkg=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -run) pattern="$2"; shift ;;
    ./*) pkg="$1" ;;
  esac
  shift
done
printf '%s %s\n' "${pkg}" "${pattern}" >> "${FAKE_GO_LOG}"
names="${pattern#^(}"; names="${names%)\$}"
IFS='|' read -ra list <<< "${names}"
for n in "${list[@]}"; do
  case "${FAKE_GO_SKIP_PROOF:-}" in "${n}") continue ;; esac
  printf 'executed' > "${DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR}/${n}"
done
exit 0
"""


def _run_verb(
    tree: Path, skip_proof: str = ""
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    bin_dir = tree / "bin"
    bin_dir.mkdir(exist_ok=True)
    fake = bin_dir / "go"
    fake.write_text(FAKE_GO, encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    goroot = tree / "fakeroot"
    (goroot / "bin").mkdir(parents=True, exist_ok=True)
    gofmt = goroot / "bin" / "gofmt"
    gofmt.write_text("#!/bin/sh\n", encoding="utf-8")
    gofmt.chmod(gofmt.stat().st_mode | stat.S_IXUSR)
    log = tree / "fake_go.log"
    log.write_text("")
    proc = _run(
        tree,
        "venue-oracles",
        script="check_go.sh",
        env_extra={
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_GO_LOG": str(log),
            "FAKE_GOROOT": str(goroot),
            "FAKE_GO_SKIP_PROOF": skip_proof,
            "DEV_HEALTH_LIVE_PYTHON_ORACLES": "1",
        },
    )
    return proc, sorted(log.read_text().splitlines())


def test_the_verb_runs_exactly_the_registry(tree: Path) -> None:
    proc, calls = _run_verb(tree)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert calls == [
        "./internal/a ^(TestAlpha|TestBeta)$",
        "./internal/b ^(TestVenueOracleGamma)$",
        "./internal/c ^(TestDelta)$",
    ]
    assert "5 registered test(s)" not in proc.stdout  # 4 runnable + 1 local
    assert "4 registered test(s) across 3 package(s)" in proc.stdout
    assert "local-only: TestEpsilonVenueOracle" in proc.stdout


def test_the_verb_runs_a_registry_only_test_discovery_cannot_reach(tree: Path) -> None:
    """The registry may name more than discovery finds; that test still runs."""
    _write(
        tree / "internal/e/e_test.go",
        _go_test("e", "TestReachesHarnessSomeOtherWay"),
    )
    rows = sorted(
        _read_rows(tree) + ["internal/e\tTestReachesHarnessSomeOtherWay\trun"]
    )
    _registry(tree, [], raw="\n".join(rows) + "\n")
    proc, calls = _run_verb(tree)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "./internal/e ^(TestReachesHarnessSomeOtherWay)$" in calls


def test_the_verb_fails_before_running_anything_on_an_unregistered_test(
    tree: Path,
) -> None:
    _write(
        tree / "internal/d/d_test.go",
        _go_test("d", "TestVenueOracleOmega"),
    )
    proc, calls = _run_verb(tree)
    assert proc.returncode != 0
    assert "internal/d TestVenueOracleOmega" in proc.stderr
    assert calls == [""] or calls == [], (
        "the verb ran go test before validating the registry"
    )


def test_the_verb_fails_when_a_registered_test_writes_no_proof(tree: Path) -> None:
    proc, _ = _run_verb(tree, skip_proof="TestBeta")
    assert proc.returncode != 0
    assert "venue oracle TestBeta did not run a real comparison" in proc.stderr


def test_a_package_registered_only_local_only_fails(tree: Path) -> None:
    _write(
        tree / "internal/f/f_test.go",
        _go_test(
            "f", "TestOnlyVenueOracle", harness=True, marker="TestOnlyVenueOracle"
        ),
    )
    rows = sorted(_read_rows(tree) + ["internal/f\tTestOnlyVenueOracle\tlocal"])
    _registry(tree, [], raw="\n".join(rows) + "\n")
    proc, _ = _run_verb(tree)
    assert proc.returncode != 0
    assert "internal/f is registered but no test there is run" in proc.stderr


def test_the_verb_source_consumes_the_registry_not_discovery() -> None:
    """The verb's own body reads the registry; discovery is only the cross-check."""
    script = (ROOT / "ci" / "check_go.sh").read_text()
    start = script.index("check_venue_oracles() {")
    end = script.index("\n}\n", start)
    body = script[start:end]
    assert "check_venue_oracle_registry" in body
    assert "venue_oracle_registry_rows" in body
    assert "venue_oracle_package_tests" not in body, (
        "check_venue_oracles must run the registry; discovery belongs to the "
        "cross-check in check_venue_oracle_registry"
    )


# --- CHAOS-6806: a `run` row must be able to leave a proof file -------------


def _proofless(tree: Path, name: str = "TestBeta", *, proof: str = "") -> None:
    _write(
        tree / "internal/a/a_test.go",
        _go_test("a", "TestAlpha", harness=True)
        + _go_test("a", name, harness=True, proof=proof),
    )


def test_a_run_row_that_cannot_write_a_proof_fails_at_pr_time(tree: Path) -> None:
    baseline = _run(tree)
    assert baseline.returncode == 0, baseline.stderr
    _proofless(tree)
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal/a TestBeta is a run row" in proc.stderr
    assert "TestAlpha is a run row" not in proc.stderr


def test_the_go_only_proof_and_the_exported_proof_both_count(tree: Path) -> None:
    _proofless(
        tree, proof='\tvenueoracle.WriteGoOnlyProof(t, "checks the log lines")\n'
    )
    assert _run(tree).returncode == 0
    _proofless(tree, proof="\tvenueoracle.WriteProof(t)\n")
    assert _run(tree).returncode == 0
    # a frozen-recording comparison (CHAOS-6817) writes the Go-only proof itself
    _proofless(tree, proof='\tvenueoracle.DiffRecorded(t, "", nil, nil, opts, "why")\n')
    assert _run(tree).returncode == 0


def test_a_proof_written_by_a_helper_the_test_calls_counts(tree: Path) -> None:
    _proofless(tree, proof="\thelper(t)\n")
    helper = (
        "package a\n\nfunc helper(t *testing.T) {\n\tinner(t)\n}\n\n"
        "func inner(t *testing.T) {\n\tvenueoracle.Diff(t)\n}\n"
    )
    _write(tree / "internal/a/helper_test.go", helper)
    assert _run(tree).returncode == 0
    # the same helper chain with the proof call removed: the guard fails again
    _write(
        tree / "internal/a/helper_test.go", helper.replace("venueoracle.Diff(t)", "")
    )
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal/a TestBeta is a run row" in proc.stderr


def test_a_proof_call_named_only_in_a_comment_does_not_count(tree: Path) -> None:
    _proofless(tree, proof="\t// venueoracle.Diff(t) is called by the harness\n")
    proc = _run(tree)
    assert proc.returncode != 0
    assert "internal/a TestBeta is a run row" in proc.stderr


def test_a_proof_through_a_harness_import_alias_counts(tree: Path) -> None:
    _write(
        tree / "internal/a/a_test.go",
        _go_test("a", "TestAlpha", harness=True, proof="\tvo.Diff(t)\n")
        + "func TestBeta(t *testing.T) {\n\tvo.WriteProof(t)\n}\n",
    )
    text = (tree / "internal/a/a_test.go").read_text()
    _write(tree / "internal/a/a_test.go", text.replace(HARNESS, "vo " + HARNESS, 1))
    assert _run(tree).returncode == 0


def test_a_local_only_row_needs_no_proof(tree: Path) -> None:
    _write(
        tree / "internal/c/c_test.go",
        _go_test(
            "c",
            "TestDelta",
            "TestEpsilonVenueOracle",
            harness=True,
            marker="TestEpsilonVenueOracle",
            proof="",
        ).replace(
            "func TestDelta(t *testing.T) {\n}",
            "func TestDelta(t *testing.T) {\n\tvenueoracle.Diff(t)\n}",
        ),
    )
    assert _run(tree).returncode == 0
