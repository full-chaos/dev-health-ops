"""The venue-oracles job runs as a sharded matrix whose slices partition the registry.

WHY THIS TEST EXISTS (CHAOS-6574)
---------------------------------
The hosted job ran every registered venue test in ONE sequential job: 67 min
at 87 tests (run 36092737130), and CANCELLED at its 75-minute timeout on the
next two main pushes (5688caa8, fc84636f) -- a cancel reads as no signal.
`ci/check_go.sh venue-oracles SHARD COUNT` now runs the SHARD-th of COUNT
slices of the registry's `run` rows (every COUNT-th row of the sorted
sequence, so one package's tests spread over every leg), and the workflow runs
COUNT matrix legs.

WHAT THIS PINS (executed against the real ci/check_go.sh with a stand-in
`go test` that records the -run set and writes the proof a real oracle writes)
1. The COUNT slices are DISJOINT and their UNION is exactly the registry's
   `run` rows: no test dropped, none run twice. On a scratch tree and on the
   real registry.
2. No arguments still runs every row (the local full run) and equals the
   union of the slices.
3. A slice that selects zero rows FAILS loudly (a matrix wider than the
   registry must not read green while running nothing); bad arguments fail;
   a package registered with only local-only rows fails in every leg.
4. The workflow's matrix list is exactly 1..N, the count passed to the verb is
   N, the leg timeout is bounded, and the trigger set is unchanged.

COST-BALANCED LEGS (CHAOS-6891)
-------------------------------
The legs were every COUNT-th row, blind to cost: at 154 rows the legs' test time
ranged 1677-2207 s against a 2400 s job cap with ~200 s of setup, the worst leg
ended at 39:59 and three new rows pushed two other legs over (run 36228118954).
The legs are now the longest-processing-time assignment by the measured seconds
in ci/venue_oracle_weights.d/ (ci/venue_oracle_shard.awk). With no weights every
row weighs the same and the assignment IS the old every-COUNT-th-row split, which
is what the scratch-tree tests below still pin. This file also pins:
5. every registry `run` row has a weight and every weight names a `run` row;
6. the heaviest PLANNED leg of the real registry fits VENUE_LEG_BUDGET_SECONDS
   (the workflow's), and that budget plus setup and drift fits the job cap: the PR
   that adds tests must regenerate the weights or add a leg BEFORE it merges;
7. the legs partition the rows whatever the weights say (a wrong weight costs
   balance, never coverage), the assignment is deterministic and balanced to
   within one row's weight, and each leg logs its planned and actual seconds and
   warns over the budget.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "venue-oracles.yml"
CI_FILES = (
    "check_go.sh",
    "check_venue_oracle_registry.sh",
    "venue_oracle_discovery.awk",
    "venue_oracle_names.awk",
    "venue_oracle_proof.awk",
    "venue_oracle_shard.awk",
    "lib/venue_oracle_registry.sh",
)
HARNESS = '"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"'

STAND_IN_GO = """#!/usr/bin/env bash
# Stand-in `go`: `go test` records "<pkg> <-run pattern>" and writes the
# "executed" proof each named test would; every other subcommand is the real go.
if [ "$1" != "test" ]; then exec "{real_go}" "$@"; fi
pattern=""; pkg=""
while [ "$#" -gt 0 ]; do
  case "$1" in -run) pattern="$2"; shift ;; ./*) pkg="$1" ;; esac
  shift
done
printf '%s %s\\n' "${{pkg}}" "${{pattern}}" >> "${{FAKE_GO_LOG}}"
if [ -n "${{FAKE_GO_SLEEP:-}}" ]; then sleep "${{FAKE_GO_SLEEP}}"; fi
names="${{pattern#^(}}"; names="${{names%)\\$}}"
IFS='|' read -ra list <<< "${{names}}"
for n in "${{list[@]}}"; do
  printf 'executed' > "${{DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR}}/${{n}}"
done
exit 0
"""


def _run_verb(
    tree: Path,
    *args: str,
    env_extra: dict[str, str] | None = None,
    verb: str = "venue-oracles",
) -> tuple[subprocess.CompletedProcess[str], list[tuple[str, str]]]:
    """Run `check_go.sh venue-oracles *args` in `tree`; return (proc, [(pkg, test)])."""
    real_go = shutil.which("go")
    assert real_go, "go is required: ci/check_go.sh refuses to run without it"
    bin_dir = tree / "bin"
    bin_dir.mkdir(exist_ok=True)
    fake = bin_dir / "go"
    fake.write_text(STAND_IN_GO.format(real_go=real_go), encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    log = tree / "fake_go.log"
    log.write_text("")
    (tree / "tmp").mkdir(exist_ok=True)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "FAKE_GO_LOG": str(log),
        "DEV_HEALTH_LIVE_PYTHON_ORACLES": "1",
        "TMPDIR": str(tree / "tmp"),
        "DEV_HEALTH_GO_CACHE": str(tree / "tmp" / "gocache"),
        **(env_extra or {}),
    }
    proc = subprocess.run(
        ["bash", str(tree / "ci" / "check_go.sh"), verb, *args],
        cwd=tree,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    ran: list[tuple[str, str]] = []
    for line in log.read_text().splitlines():
        pkg, pattern = line.split(" ", 1)
        names = pattern.removeprefix("^(").removesuffix(")$").split("|")
        ran.extend((pkg.removeprefix("./"), name) for name in names)
    return proc, ran


def _registry_run_rows(tree: Path) -> list[tuple[str, str]]:
    rows = []
    for path in sorted((tree / "ci" / "venue_oracle_registry.d").glob("*.tsv")):
        for line in path.read_text().splitlines():
            if line and not line.startswith("#"):
                pkg, test, mode = line.split("\t")
                if mode == "run":
                    rows.append((pkg, test))
    return sorted(rows)


def _go_test(package: str, *funcs: str, local: str = "") -> str:
    body = f"package {package}\n\nimport (\n\t{HARNESS}\n)\n\n"
    for name in funcs:
        if name == local:
            body += "//venueoracle:local-only needs a key CI does not hold\n"
        body += f"func {name}(t *testing.T) {{\n\tvenueoracle.Diff(t)\n}}\n\n"
    return body


def _scratch_tree(tmp_path: Path, packages: dict[str, list[str]], local=()) -> Path:
    """A scratch repo: real script + awks, `packages` (pkg -> tests), its registry."""
    (tmp_path / "ci" / "lib").mkdir(parents=True)
    for name in CI_FILES:
        shutil.copy(ROOT / "ci" / name, tmp_path / "ci" / name)
    (tmp_path / "go.mod").write_text("module example.com/x\n", encoding="utf-8")
    for pkg, tests in sorted(packages.items()):
        directory = tmp_path / pkg
        directory.mkdir(parents=True)
        (directory / "x_test.go").write_text(
            _go_test(pkg.rsplit("/", 1)[-1], *tests, local=local[0] if local else ""),
            encoding="utf-8",
        )
        registry = tmp_path / "ci" / "venue_oracle_registry.d"
        registry.mkdir(parents=True, exist_ok=True)
        (registry / (pkg.replace("/", "__") + ".tsv")).write_text(
            "".join(
                f"{pkg}\t{test}\t{'local' if test in local else 'run'}\n"
                for test in sorted(tests)
            ),
            encoding="utf-8",
        )
    return tmp_path


# ---------------------------------------------------------------------------
# Scratch tree: exact slice arithmetic.
# ---------------------------------------------------------------------------

PACKAGES = {
    "internal/a": ["TestA1", "TestA2", "TestA3", "TestA4", "TestA5"],
    "internal/b": ["TestB1", "TestB2", "TestB3"],
    "internal/c": ["TestC1", "TestC2"],
}


def test_slices_partition_the_registry_run_rows(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, PACKAGES)
    all_rows = _registry_run_rows(tree)
    assert len(all_rows) == 10
    seen: list[tuple[str, str]] = []
    for shard in (1, 2, 3, 4):
        proc, ran = _run_verb(tree, str(shard), "4")
        assert proc.returncode == 0, proc.stderr + proc.stdout
        # Exact slice: every 4th row of the sorted sequence, starting at shard-1.
        assert sorted(ran) == all_rows[shard - 1 :: 4], (shard, ran)
        assert f"shard {shard}/4 runs" in proc.stdout
        seen.extend(ran)
    assert len(seen) == len(set(seen)), "a test ran in two shards"
    assert sorted(seen) == all_rows, "the union of the shards is not the registry"


def test_no_arguments_runs_every_row(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, PACKAGES)
    proc, ran = _run_verb(tree)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert sorted(ran) == _registry_run_rows(tree)


def test_a_single_shard_of_one_is_the_whole_registry(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, PACKAGES)
    proc, ran = _run_verb(tree, "1", "1")
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert sorted(ran) == _registry_run_rows(tree)


def test_a_slice_with_zero_rows_fails_loudly(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, {"internal/a": ["TestA1", "TestA2"]})
    ok, ran = _run_verb(tree, "2", "3")
    assert ok.returncode == 0 and len(ran) == 1, ok.stderr
    proc, ran = _run_verb(tree, "3", "3")
    assert proc.returncode != 0
    assert "selects zero of the 2 registered run rows" in proc.stderr
    assert ran == [], "a zero-row leg must not run go test"


def test_bad_arguments_fail(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, PACKAGES)
    for args, needle in (
        (("0", "4"), "outside 1..4"),
        (("5", "4"), "outside 1..4"),
        (("x", "4"), "SHARD must be a positive integer"),
        (("1", "y"), "COUNT must be a positive integer"),
        (("1", "0"), "outside 1..0"),
        (("2",), "accepts no arguments, or SHARD COUNT"),
    ):
        proc, ran = _run_verb(tree, *args)
        assert proc.returncode != 0, args
        assert needle in proc.stderr, (args, proc.stderr)
        assert ran == []


def test_a_package_with_only_local_only_rows_fails_in_every_leg(tmp_path: Path) -> None:
    tree = _scratch_tree(
        tmp_path,
        {"internal/a": ["TestA1", "TestA2"], "internal/z": ["TestZOnly"]},
        local=("TestZOnly",),
    )
    for shard in (1, 2):
        proc, _ = _run_verb(tree, str(shard), "2")
        assert proc.returncode != 0
        assert "internal/z is registered but no test there is run" in proc.stderr


# ---------------------------------------------------------------------------
# The real registry.
# ---------------------------------------------------------------------------


def test_real_registry_shards_partition_and_balance(tmp_path: Path) -> None:
    count = _matrix_shards()[-1]
    all_rows = _registry_run_rows(ROOT)
    assert len(all_rows) >= 40, f"only {len(all_rows)} run rows in the registry"
    # The real tree, real script: only `go test` is stood in for.
    tree = tmp_path / "real"
    tree.mkdir()
    for name in ("go.mod", "ci", "internal", "cmd", "src"):
        source = ROOT / name
        if source.is_dir():
            os.symlink(source, tree / name)
        elif source.exists():
            shutil.copy(source, tree / name)
    seen: list[tuple[str, str]] = []
    sizes = []
    for shard in range(1, count + 1):
        proc, ran = _run_verb(tree, str(shard), str(count))
        assert proc.returncode == 0, proc.stderr[-1500:] + proc.stdout[-1500:]
        assert ran, f"shard {shard}/{count} ran nothing"
        sizes.append(len(ran))
        seen.extend(ran)
    assert len(seen) == len(set(seen)), "a test ran in two shards"
    assert sorted(seen) == all_rows
    assert min(sizes) >= 1, f"a leg ran nothing: {sizes}"


# ---------------------------------------------------------------------------
# The workflow.
# ---------------------------------------------------------------------------


def _job() -> dict:
    document = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return document["jobs"]["venue-oracles"]


def _matrix_shards() -> list[int]:
    shards = _job()["strategy"]["matrix"]["shard"]
    assert isinstance(shards, list) and shards, f"matrix shard list is {shards!r}"
    return shards


def test_the_matrix_is_one_to_n_and_n_is_what_the_verb_gets() -> None:
    shards = _matrix_shards()
    assert shards == list(range(1, len(shards) + 1)), (
        f"matrix shard list {shards} is not 1..N: a gap or duplicate leaves "
        "part of the registry unrun or run twice"
    )
    runs = [s["run"] for s in _job()["steps"] if isinstance(s.get("run"), str)]
    commands = [
        line.strip()
        for run in runs
        for line in run.splitlines()
        if "check_go.sh venue-oracles" in line
    ]
    assert len(commands) == 1, commands
    match = re.search(
        r'check_go\.sh venue-oracles\s+"\$\{\{\s*matrix\.shard\s*\}\}"\s+(\d+)\b',
        commands[0],
    )
    assert match, (
        f"the verb is not called with the matrix shard and a count: {commands[0]}"
    )
    assert int(match.group(1)) == len(shards), (
        f"the workflow passes COUNT={match.group(1)} but the matrix has {len(shards)} legs"
    )
    assert f"/{len(shards)})" in _job()["name"], "the leg name states the wrong count"


def test_the_leg_is_bounded_and_does_not_fail_fast() -> None:
    job = _job()
    assert job["timeout-minutes"] <= 40, (
        "a venue leg must stay well under the 75 min that cancelled the "
        f"sequential job (timeout-minutes={job['timeout-minutes']})"
    )
    assert job["strategy"]["fail-fast"] is False, (
        "one red shard must not cancel the others: the point is a signal per slice"
    )


def test_the_receipt_artifact_is_per_shard() -> None:
    uploads = [
        s for s in _job()["steps"] if "upload-artifact" in str(s.get("uses", ""))
    ]
    assert len(uploads) == 1
    assert "matrix.shard" in uploads[0]["with"]["name"], (
        "four legs uploading one artifact name collide"
    )


# ---------------------------------------------------------------------------
# Cost-balanced legs (CHAOS-6891).
# ---------------------------------------------------------------------------

WEIGHTS_FILE = (
    ROOT / "ci" / "venue_oracle_weights.d"
)  # per-package row files (CHAOS-6926)
JOB_SETUP_SECONDS = 200  # measured: leg wall time minus its test time, 192-212 s
DRIFT_SECONDS = 200  # room for a slow runner beyond the planned seconds


def _weight_files(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(path.glob("*.tsv"), key=lambda p: p.name.encode())
    return [path]


def _weights(path: Path = WEIGHTS_FILE) -> list[tuple[str, str, int]]:
    """Every weight row of a weights directory (its files in C-locale order) or file."""
    rows = []
    for file in _weight_files(path):
        for line in file.read_text(encoding="utf-8").splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            package, test, seconds, *marker = line.split("\t")
            # The generator's fourth column: `unmeasured` (planned provisionally).
            assert marker in ([], ["unmeasured"]), (
                f"unknown weights column {marker} in {line!r}"
            )
            rows.append((package, test, int(seconds)))
    return rows


def _write_weights(tree: Path, text: str) -> None:
    """Give a scratch tree its weights: one directory holding one file."""
    directory = tree / "ci" / "venue_oracle_weights.d"
    directory.mkdir(parents=True, exist_ok=True)
    for old in directory.glob("*.tsv"):
        old.unlink()
    (directory / "scratch.tsv").write_text(text, encoding="utf-8")


def _plan(
    tree: Path, count: int
) -> tuple[list[tuple[str, str, int, int]], dict[int, tuple[int, int]]]:
    """(rows as (pkg, test, leg, weight), {leg: (rows, seconds)}) from the plan verb."""
    proc, _ = _run_verb(tree, str(count), verb="venue-oracle-plan")
    assert proc.returncode == 0, proc.stderr + proc.stdout
    rows, legs = [], {}
    for line in proc.stdout.splitlines():
        fields = line.split("\t")
        if fields[0] == "#leg":
            legs[int(fields[1])] = (int(fields[2]), int(fields[3]))
        elif len(fields) == 4:
            rows.append((fields[0], fields[1], int(fields[2]), int(fields[3])))
    return rows, legs


def _real_tree(tmp_path: Path) -> Path:
    tree = tmp_path / "real"
    tree.mkdir()
    for name in ("go.mod", "ci", "internal", "cmd", "src"):
        source = ROOT / name
        if source.is_dir():
            os.symlink(source, tree / name)
        elif source.exists():
            shutil.copy(source, tree / name)
    return tree


def test_the_weights_directory_is_well_formed_and_matches_the_registry() -> None:
    files = _weight_files(WEIGHTS_FILE)
    assert files, "ci/venue_oracle_weights.d/ holds no *.tsv file"
    registry_files = {
        path.name for path in (ROOT / "ci" / "venue_oracle_registry.d").glob("*.tsv")
    }
    for file in files:
        # One file per package, named like the registry's: a PR that adds a venue row in
        # package X touches registry file X and weights file X only, so two PRs in
        # different packages cannot conflict (CHAOS-6926).
        rows = _weights(file)
        assert rows, f"{file.name} holds no weight row"
        packages = {package for package, _, _ in rows}
        assert len(packages) == 1, f"{file.name} mixes packages {sorted(packages)}"
        (package,) = packages
        assert file.name == package.replace("/", "__") + ".tsv", (
            f"{file.name} holds the rows of {package}; it must be named "
            f"{package.replace('/', '__')}.tsv"
        )
        assert file.name in registry_files, (
            f"{file.name} has no registry file of the same name: the package "
            "has no venue run row left"
        )
        assert rows == sorted(rows, key=lambda w: (w[0], w[1])), (
            f"{file.name} is not sorted by test"
        )
    weights = _weights()
    keys = [(package, test) for package, test, _ in weights]
    assert len(keys) == len(set(keys)), "a weight row is duplicated"
    assert all(seconds >= 1 for _, _, seconds in weights), "a weight below 1 second"
    registry = set(_registry_run_rows(ROOT))
    missing = sorted(registry - set(keys))
    assert not missing, (
        f"registry run rows with no weight: {missing[:5]}...: regenerate "
        "ci/venue_oracle_weights.d/ (python3 ci/venue_oracle_weights.py <saved run log>)"
    )
    stale = sorted(set(keys) - registry)
    assert not stale, f"weights for tests that are not registry run rows: {stale[:5]}"


def test_the_planned_heaviest_leg_fits_the_budget(tmp_path: Path) -> None:
    job = _job()
    budget = int(job["env"]["VENUE_LEG_BUDGET_SECONDS"])
    cap = job["timeout-minutes"] * 60
    assert budget + JOB_SETUP_SECONDS + DRIFT_SECONDS <= cap, (
        f"the budget {budget}s plus {JOB_SETUP_SECONDS}s setup and {DRIFT_SECONDS}s "
        f"drift does not fit the {cap}s job cap"
    )
    count = len(_matrix_shards())
    rows, legs = _plan(_real_tree(tmp_path), count)
    assert sorted(legs) == list(range(1, count + 1))
    assert (
        sum(n for n, _ in legs.values()) == len(rows) == len(_registry_run_rows(ROOT))
    )
    heaviest = max(seconds for _, seconds in legs.values())
    assert heaviest <= budget, (
        f"the heaviest of the {count} planned venue legs is {heaviest}s of tests, over the "
        f"{budget}s budget: regenerate ci/venue_oracle_weights.d/ from a recent run log "
        "(ci/venue_oracle_weights.py) or add a leg to the matrix and the verb's COUNT"
    )


def test_weights_balance_the_legs_and_never_drop_a_test(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, PACKAGES)
    _write_weights(
        tree,
        "# weights\n"
        "internal/a\tTestA1\t500\n"
        "internal/a\tTestA2\t20\n"
        "internal/a\tTestA3\t20\n"
        "internal/b\tTestB1\t300\n"
        "internal/c\tTestC1\t10\n"
        "internal/nowhere\tTestGone\t9999\n",  # a weight for a test that is not there: ignored
    )
    all_rows = _registry_run_rows(tree)
    first = _plan(tree, 3)
    assert first == _plan(tree, 3), "the plan is not deterministic"
    rows, legs = first
    assert sorted((p, t) for p, t, _, _ in rows) == all_rows, (
        "the plan dropped or added a test"
    )
    total = sum(w for _, _, _, w in rows)
    assert sum(seconds for _, seconds in legs.values()) == total
    heaviest_row = max(w for _, _, _, w in rows)
    # longest-processing-time: no leg exceeds the average by more than one row's weight
    assert max(seconds for _, seconds in legs.values()) <= total / 3 + heaviest_row
    # and the verb runs exactly the plan's legs, each test once
    seen = []
    for leg in (1, 2, 3):
        proc, ran = _run_verb(tree, str(leg), "3")
        assert proc.returncode == 0, proc.stderr + proc.stdout
        assert sorted(ran) == sorted((p, t) for p, t, k, _ in rows if k == leg), leg
        assert f"predicts {legs[leg][1]}s of tests for this leg" in proc.stdout
        seen.extend(ran)
    assert sorted(seen) == all_rows


def test_the_500_second_row_gets_a_leg_to_itself(tmp_path: Path) -> None:
    tree = _scratch_tree(
        tmp_path, {"internal/a": ["TestA1", "TestA2", "TestA3", "TestA4"]}
    )
    _write_weights(
        tree,
        "internal/a\tTestA1\t500\ninternal/a\tTestA2\t100\ninternal/a\tTestA3\t100\ninternal/a\tTestA4\t100\n",
    )
    rows, legs = _plan(tree, 2)
    heavy = next(leg for p, t, leg, _ in rows if t == "TestA1")
    assert [t for _, t, leg, _ in rows if leg == heavy] == ["TestA1"], rows
    assert legs[heavy] == (1, 500) and sum(s for _, s in legs.values()) == 800


def test_the_plan_verb_rejects_a_bad_count(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, PACKAGES)
    for count in ("0", "x"):
        proc, _ = _run_verb(tree, count, verb="venue-oracle-plan")
        assert proc.returncode != 0, count
        assert "COUNT must be a positive integer" in proc.stderr


def test_a_leg_logs_its_plan_and_warns_over_the_budget(tmp_path: Path) -> None:
    tree = _scratch_tree(tmp_path, {"internal/a": ["TestA1"]})
    ok, _ = _run_verb(tree, "1", "1", env_extra={"VENUE_LEG_BUDGET_SECONDS": "1800"})
    assert ok.returncode == 0, ok.stderr
    assert "venue-oracles: leg " in ok.stdout and "budget 1800s" in ok.stdout
    assert "::warning" not in ok.stdout
    slow, _ = _run_verb(
        tree,
        "1",
        "1",
        env_extra={"VENUE_LEG_BUDGET_SECONDS": "1", "FAKE_GO_SLEEP": "2"},
    )
    assert slow.returncode == 0, slow.stderr
    assert "::warning title=venue-oracles leg over its test-time budget" in slow.stdout


def test_the_weights_generator_reads_a_run_log(tmp_path: Path) -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "venue_oracle_weights", ROOT / "ci" / "venue_oracle_weights.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    prefix = "venue-oracles (shard 1/7)\tRun the shard\t2026-09-26T08:02:13.4333960Z "
    other = "venue-oracles (shard 2/7)\tRun the shard\t2026-09-26T08:02:14.0000000Z "
    lines = [
        prefix + "    --- PASS: TestNotTop/sub (9.00s)",  # a subtest: not a row
        prefix + "--- PASS: TestOne (10.20s)",
        other + "--- PASS: TestTwo (30.00s)",  # another leg's test, interleaved
        prefix + "--- PASS: TestThree (5.01s)",
        prefix + "ok  \tgithub.com/full-chaos/dev-health-ops/internal/a\t15.3s",
        other + "ok  \tgithub.com/full-chaos/dev-health-ops/internal/b\t31.0s",
        "--- PASS: TestOne (99.00s)",  # a plain (unprefixed) log line, other leg id ""
        "FAIL\tgithub.com/full-chaos/dev-health-ops/internal/a\t1.0s",
    ]
    seen: dict[tuple[str, str], float] = {}
    module.parse_log(lines, seen)
    assert seen == {
        ("internal/a", "TestOne"): 99.0,  # the largest across the logs
        ("internal/a", "TestThree"): 5.01,
        ("internal/b", "TestTwo"): 30.0,
    }
    text, unmeasured = module.build(
        [
            ("internal/a", "TestOne"),
            ("internal/a", "TestThree"),
            ("internal/b", "TestTwo"),
            ("internal/c", "TestKept"),
            ("internal/c", "TestNew"),
            ("internal/d", "TestStill"),
        ],
        seen,
        {
            ("internal/c", "TestKept"): (77, False),
            ("internal/d", "TestStill"): (5, True),
            ("internal/gone", "TestGone"): (5, False),
        },
    )
    body = [line for line in text.splitlines() if line and not line.startswith("#")]
    assert body == [
        "internal/a\tTestOne\t99",
        "internal/a\tTestThree\t6",  # rounded up
        "internal/b\tTestTwo\t30",
        "internal/c\tTestKept\t77",  # no log measured it: the existing weight stays
        "internal/c\tTestNew\t600\tunmeasured",  # a new row: planned pessimistically
        "internal/d\tTestStill\t5\tunmeasured",  # still unmeasured: flag kept
    ]
    assert unmeasured == [("internal/c", "TestNew"), ("internal/d", "TestStill")]
    # a measurement clears the flag
    text, unmeasured = module.build(
        [("internal/d", "TestStill")],
        {("internal/d", "TestStill"): 41.2},
        {("internal/d", "TestStill"): (5, True)},
    )
    assert "internal/d\tTestStill\t42\n" in text and unmeasured == []
    assert module.PROVISIONAL_WEIGHT >= 600
    assert text.startswith("# Measured seconds of one venue-oracles registry `run` row")


def test_an_unmeasured_row_is_planned_pessimistically() -> None:
    # CHAOS-6891: the first plan gave two rows nobody had measured a 60 s default;
    # one took 560 s and its leg ran 2109 s of tests against a 2400 s cap. A row in
    # the weights file marked `unmeasured` (a new test: venue oracles run on main
    # only, so its author cannot measure it) must carry at least the provisional
    # 600 s, above the slowest row ever measured, until a run log measures it.
    for file in _weight_files(WEIGHTS_FILE):
        for line in file.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.lstrip().startswith("#"):
                package, test, seconds, *marker = line.split("\t")
                if marker == ["unmeasured"]:
                    assert int(seconds) >= 600, (
                        f"{package} {test} is unmeasured but planned at {seconds}s: "
                        "regenerate with ci/venue_oracle_weights.py"
                    )
                else:
                    assert marker == [], f"unknown weights column {marker} in {line!r}"


def test_the_weights_parser_reads_a_generator_written_unmeasured_row(
    tmp_path: Path,
) -> None:
    # CHAOS-6905: ci/venue_oracle_weights.py writes a fourth column, `unmeasured`,
    # for a row no run log has measured; a parser that unpacks exactly three
    # columns failed every PR that regenerated the file with such a row.
    weights = tmp_path / "weights.tsv"
    weights.write_text(
        "# header\ninternal/a\tTestA\t12\ninternal/b\tTestNew\t600\tunmeasured\n",
        encoding="utf-8",
    )
    assert _weights(weights) == [
        ("internal/a", "TestA", 12),
        ("internal/b", "TestNew", 600),
    ]
    weights.write_text("internal/a\tTestA\t12\tsomething-else\n", encoding="utf-8")
    try:
        _weights(weights)
    except AssertionError as error:
        assert "unknown weights column" in str(error)
    else:
        raise AssertionError("an unknown fourth column must be refused, not ignored")
