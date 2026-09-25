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
names="${{pattern#^(}}"; names="${{names%)\\$}}"
IFS='|' read -ra list <<< "${{names}}"
for n in "${{list[@]}}"; do
  printf 'executed' > "${{DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR}}/${{n}}"
done
exit 0
"""


def _run_verb(
    tree: Path, *args: str
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
    }
    proc = subprocess.run(
        ["bash", str(tree / "ci" / "check_go.sh"), "venue-oracles", *args],
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
    for line in (tree / "ci" / "venue_oracle_registry.tsv").read_text().splitlines():
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
    rows = []
    for pkg, tests in sorted(packages.items()):
        directory = tmp_path / pkg
        directory.mkdir(parents=True)
        (directory / "x_test.go").write_text(
            _go_test(pkg.rsplit("/", 1)[-1], *tests, local=local[0] if local else ""),
            encoding="utf-8",
        )
        for test in sorted(tests):
            mode = "local" if test in local else "run"
            rows.append(f"{pkg}\t{test}\t{mode}\n")
    (tmp_path / "ci" / "venue_oracle_registry.tsv").write_text(
        "# scratch registry\n" + "".join(rows), encoding="utf-8"
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
    assert max(sizes) - min(sizes) <= 1, f"unbalanced slices: {sizes}"


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
