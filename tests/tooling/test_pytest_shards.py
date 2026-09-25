"""The unit suite runs as a sharded `test-matrix` whose slices partition the tests.

WHY THIS TEST EXISTS (CHAOS-6692)
---------------------------------
The required `test` check waited on `test-matrix`, whose unit step took 15 of
17 minutes and became the longest required job of a PR. The job now runs N
matrix legs; leg K sets PYTEST_SHARD=K/N and tests/conftest.py keeps only the
pytest FILES whose stable hash falls in slice K (a file's tests stay together;
every xdist worker collects on its own and computes the same partition).
Splitting a suite must never drop or duplicate a test, so this file pins:

1. PARTITION: on a real subset of the suite the slices are disjoint, non-empty,
   keep a file's tests together, and their union is exactly the unsharded set.
2. LOUD FAILURES: a slice that selects nothing, and a malformed value, exit
   non-zero (a leg that ran nothing must not read green).
3. WORKFLOW: the matrix shard list is exactly 1..N, the N in PYTEST_SHARD equals
   the number of legs, the PostgreSQL migration tests run on shard 1 only (they
   are ignored from every unit run), artifact names carry the shard, legs do not
   fail fast, and the `test` aggregate still judges the MATRIX result (so a
   failed, cancelled or missing leg fails the required check).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "test.yml"
SUBSET = "tests/tooling"


def _collect(shard: str | None) -> tuple[int, list[str], str]:
    env = {k: v for k, v in os.environ.items() if k != "PYTEST_SHARD"}
    env["PYTEST_ADDOPTS"] = ""
    if shard is not None:
        env["PYTEST_SHARD"] = shard
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-n0",
            "-p",
            "no:cacheprovider",
            "-m",
            "not benchmark and not clickhouse",
            SUBSET,
        ],  # fmt: skip
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=250,
        check=False,
    )
    ids = [
        line for line in proc.stdout.splitlines() if "::" in line and " " not in line
    ]
    return proc.returncode, ids, proc.stdout + proc.stderr


def test_slices_partition_the_suite_and_keep_files_together() -> None:
    code, everything, out = _collect(None)
    assert code == 0, out[-1500:]
    assert len(everything) >= 200, f"only {len(everything)} tests collected"
    slices = []
    for k in (1, 2, 3):
        code, ids, out = _collect(f"{k}/3")
        assert code == 0, out[-1500:]
        assert ids, f"slice {k}/3 selected nothing"
        slices.append(ids)
    flat = [i for s in slices for i in s]
    assert len(flat) == len(set(flat)), "a test is in two slices"
    assert sorted(flat) == sorted(everything), "the slices do not cover the suite"
    owner: dict[str, int] = {}
    for k, ids in enumerate(slices):
        for nodeid in ids:
            file = nodeid.split("::", 1)[0]
            assert owner.setdefault(file, k) == k, f"{file} is split across slices"


def _run_for_real(shard: str) -> tuple[int, str]:
    """A real (not collect-only) run of one small file under `shard`."""
    env = {k: v for k, v in os.environ.items() if k != "PYTEST_SHARD"}
    env["PYTEST_ADDOPTS"] = ""
    env["PYTEST_SHARD"] = shard
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-n0",
            "-p",
            "no:cacheprovider",
            "tests/tooling/test_venue_oracle_registry.py",
        ],  # fmt: skip
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return proc.returncode, proc.stdout + proc.stderr


def test_a_slice_that_selects_nothing_fails_loudly() -> None:
    # One file, so at most one of the 5000 slices holds it: slice 1 of 5000 does
    # not, and must FAIL rather than read green on an empty run.
    code, out = _run_for_real("4999/5000")
    assert code != 0, out[-500:]
    assert "selects none of the" in out
    assert " passed" not in out, "a leg that selected nothing must not run tests"


def test_malformed_values_fail_and_run_nothing() -> None:
    for value in ("0/2", "3/2", "1", "a/b", "1/0", "1/2/3", "-1/2", "/2", "1/"):
        code, out = _run_for_real(value)
        assert code != 0, (value, out[-300:])
        assert "PYTEST_SHARD" in out, (value, out[-300:])
        assert " passed" not in out, value


def _job() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["test-matrix"]


def test_the_matrix_is_one_to_n_and_n_is_what_pytest_gets() -> None:
    job = _job()
    shards = job["strategy"]["matrix"]["shard"]
    assert shards == list(range(1, len(shards) + 1)), (
        f"matrix shard list {shards} is not 1..N: a gap or duplicate leaves part "
        "of the suite unrun or run twice"
    )
    step = next(s for s in job["steps"] if "run_tests.sh unit" in str(s.get("run", "")))
    match = re.fullmatch(
        r"\$\{\{ matrix\.shard \}\}/(\d+)", step["env"]["PYTEST_SHARD"]
    )
    assert match, step["env"]["PYTEST_SHARD"]
    assert int(match.group(1)) == len(shards), (
        f"PYTEST_SHARD says N={match.group(1)} but the matrix has {len(shards)} legs"
    )
    assert job["strategy"]["fail-fast"] is False


def test_the_postgres_migration_tests_run_on_shard_one_only() -> None:
    job = _job()
    step = next(
        s for s in job["steps"] if s.get("name") == "Run PostgreSQL migration tests"
    )
    assert str(step.get("if", "")).replace(" ", "") == "matrix.shard==1", (
        "these files are --ignore'd from every unit run, so they must run exactly "
        "once: on shard 1"
    )


def test_artifact_names_carry_the_shard() -> None:
    uploads = [
        s for s in _job()["steps"] if "upload-artifact" in str(s.get("uses", ""))
    ]
    assert len(uploads) == 2
    for step in uploads:
        assert "matrix.shard" in step["with"]["name"], (
            f"{step['with']['name']}: legs uploading one artifact name collide"
        )


def test_the_test_aggregate_still_judges_the_matrix_result() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]
    aggregate = jobs["test"]
    assert str(aggregate["if"]).replace(" ", "") == "always()"
    assert "test-matrix" in aggregate["needs"]
    step = next(
        s
        for s in aggregate["steps"]
        if "aggregate_gate_results" in str(s.get("run", ""))
    )
    assert (
        step["env"]["GATED_JOB_1"]
        == "test-matrix|path-filtered|${{ needs.test-matrix.result }}"
    ), (
        "the aggregate must judge test-matrix's MATRIX result: it is `success` "
        "only when every leg succeeded"
    )
