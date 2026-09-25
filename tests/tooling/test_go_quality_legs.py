"""go-quality runs as parallel legs whose union is exactly `check_go.sh ci`.

WHY THIS TEST EXISTS (CHAOS-6690)
---------------------------------
`go-quality` was one 34-minute step (`check_go.sh ci`: vet 2, test 5.6, race 17,
live-Python oracles 8.4, the rest ~1.5 minutes) and the whole critical path of
a PR. It now runs as five parallel legs (`check_go.sh ci-leg LEG`) behind a
fan-in job that keeps the required context name `go-quality`. Splitting a gate
must never silently drop a stage, so this file pins:

1. COVERAGE: the stages the legs run are exactly the stages `ci` runs (the
   race stage in its sharded form), no more and no fewer.
2. RACE SHARDS: the weight-balanced slices are a PARTITION of `go list ./...`
   (disjoint, union = everything) on the real tree; a slice that selects
   nothing fails loudly; the balance is within a bound; the weights table
   names real packages and is sorted.
3. LEG LOGS: every stage prints UTC start/done stamps, and a failing stage does
   not print `done`.
4. WORKFLOW: the matrix carries exactly the legs the script knows, the race
   COUNT equals the number of race entries, the fan-in job is named
   `go-quality`, is `if: always()`, needs the legs and is `unconditional`
   (a skipped, cancelled or missing leg fails the gate), the legs have no
   job-level `if`, and `fail-fast` is off.
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
CHECK_GO = ROOT / "ci" / "check_go.sh"
WEIGHTS = ROOT / "ci" / "go_race_weights.tsv"
SHARD_AWK = ROOT / "ci" / "go_race_shard.awk"
WORKFLOW = ROOT / ".github" / "workflows" / "go-quality.yml"
DEFAULT_WEIGHT = 2

STAND_IN_GO = """#!/usr/bin/env bash
# Stand-in `go`: `go test` records "<pkg args>" and succeeds (or fails when
# FAKE_GO_FAIL is set); every other subcommand is the real go.
if [ "$1" != "test" ]; then exec "{real_go}" "$@"; fi
printf '%s\\n' "$*" >> "${{FAKE_GO_LOG}}"
[ -z "${{FAKE_GO_FAIL:-}}" ] || exit 1
exit 0
"""


def _weights() -> dict[str, int]:
    weights: dict[str, int] = {}
    for line in WEIGHTS.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            pkg, seconds = line.split("\t")
            weights[pkg] = int(seconds)
    return weights


def _go_list() -> tuple[str, list[str]]:
    real_go = shutil.which("go")
    assert real_go, "go is required"
    env = {**os.environ, "GOWORK": "off", "GOFLAGS": "-mod=readonly"}
    mod = subprocess.run(
        [real_go, "list", "-m"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    pkgs = subprocess.run(
        [real_go, "list", "./..."],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return mod, pkgs


def _shard(mod: str, pkgs: list[str], shard: int, count: int) -> list[str]:
    proc = subprocess.run(
        [
            "awk",
            "-v",
            f"mod={mod}",
            "-v",
            f"shard={shard}",
            "-v",
            f"count={count}",
            "-v",
            f"weights={WEIGHTS}",
            "-f",
            str(SHARD_AWK),
        ],  # fmt: skip
        input="\n".join(pkgs) + "\n",
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.split()


def _key(mod: str, pkg: str) -> str:
    return "." if pkg == mod else pkg.removeprefix(mod + "/")


def _run_check_go(
    tmp_path: Path, *args: str, fail: bool = False
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    real_go = shutil.which("go")
    assert real_go
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    fake = bin_dir / "go"
    fake.write_text(STAND_IN_GO.format(real_go=real_go), encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    log = tmp_path / "go_test.log"
    log.write_text("")
    (tmp_path / "tmp").mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "FAKE_GO_LOG": str(log),
        "TMPDIR": str(tmp_path / "tmp"),
    }
    env.pop("FAKE_GO_FAIL", None)
    if fail:
        env["FAKE_GO_FAIL"] = "1"
    proc = subprocess.run(
        ["bash", str(CHECK_GO), *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=280,
        check=False,
    )
    return proc, [line for line in log.read_text().splitlines() if line]


# ---------------------------------------------------------------------------
# 1. Coverage: the legs are exactly `ci`.
# ---------------------------------------------------------------------------


def _case_block(script: str, start: str, end: str) -> str:
    a = script.index(start)
    return script[a : script.index(end, a)]


def test_the_legs_run_exactly_the_stages_of_ci() -> None:
    script = CHECK_GO.read_text(encoding="utf-8")
    ci_block = _case_block(script, "\n  ci)\n", "\n  all)\n")
    ci_steps = re.findall(r"^    ((?:check|plan)_\w+)$", ci_block, flags=re.M)
    leg_block = _case_block(script, "check_ci_leg() {", "\n}\n")
    leg_steps = re.findall(r"ci_leg_stage (?:\"[^\"]*\"|\S+) (\w+)", leg_block)
    # The race stage runs sharded inside its leg; it stands for `check_race`.
    leg_steps = ["check_race" if s == "check_race_shard" else s for s in leg_steps]
    assert ci_steps, "could not read the `ci` case"
    assert sorted(leg_steps) == sorted(ci_steps), (
        "the legs' stages differ from `check_go.sh ci`: "
        f"only in legs {sorted(set(leg_steps) - set(ci_steps))}, "
        f"only in ci {sorted(set(ci_steps) - set(leg_steps))}"
    )
    assert len(leg_steps) == len(set(leg_steps)), "a stage runs in two legs"


# ---------------------------------------------------------------------------
# 2. Race shards.
# ---------------------------------------------------------------------------


def test_race_shards_partition_the_package_list_and_balance() -> None:
    mod, pkgs = _go_list()
    assert len(pkgs) >= 100, f"only {len(pkgs)} packages"
    weights = _weights()
    for count in (2, 3, 5):
        slices = [_shard(mod, pkgs, k, count) for k in range(1, count + 1)]
        flat = [p for s in slices for p in s]
        assert len(flat) == len(set(flat)), (
            f"a package is in two slices (count={count})"
        )
        assert sorted(flat) == sorted(pkgs), (
            f"the slices do not cover the list (count={count})"
        )
        loads = [
            sum(weights.get(_key(mod, p), DEFAULT_WEIGHT) for p in s) for s in slices
        ]
        ideal = sum(loads) / count
        heaviest_single = max(weights.get(_key(mod, p), DEFAULT_WEIGHT) for p in pkgs)
        # LPT bound: no slice exceeds the ideal by more than the heaviest package.
        assert max(loads) <= ideal + heaviest_single, (count, loads)
    two = [
        sum(weights.get(_key(mod, p), DEFAULT_WEIGHT) for p in _shard(mod, pkgs, k, 2))
        for k in (1, 2)
    ]
    assert max(two) <= 1.1 * (sum(two) / 2), f"2-way race slices unbalanced: {two}"


def test_the_race_weights_table_is_clean() -> None:
    mod, pkgs = _go_list()
    known = {_key(mod, p) for p in pkgs}
    rows = [
        line.split("\t")[0]
        for line in WEIGHTS.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert rows == sorted(rows), "ci/go_race_weights.tsv is not sorted (LC_ALL=C)"
    assert len(rows) == len(set(rows)), "a package is weighed twice"
    stale = [r for r in rows if r not in known]
    assert not stale, f"weights name packages that no longer exist: {stale}"


def test_the_shard_partition_holds_whatever_the_weights_say(tmp_path: Path) -> None:
    """A wrong weight costs balance, never coverage (the property the awk promises)."""
    pkgs = [f"m/p{i}" for i in range(23)]
    weird = tmp_path / "w.tsv"
    weird.write_text("p3\t100000\np4\t0\np5\t-7\n", encoding="utf-8")
    for count in (1, 2, 4, 7):
        got: list[str] = []
        for k in range(1, count + 1):
            out = subprocess.run(
                [
                    "awk",
                    "-v",
                    "mod=m",
                    "-v",
                    f"shard={k}",
                    "-v",
                    f"count={count}",
                    "-v",
                    f"weights={weird}",
                    "-f",
                    str(SHARD_AWK),
                ],  # fmt: skip
                input="\n".join(pkgs) + "\n",
                capture_output=True,
                text=True,
                check=True,
            ).stdout.split()
            got += out
        assert sorted(got) == sorted(pkgs), count


def test_race_verb_slices_partition_the_real_packages(tmp_path: Path) -> None:
    mod, pkgs = _go_list()
    seen: list[str] = []
    for shard in (1, 2):
        proc, calls = _run_check_go(
            tmp_path / f"s{shard}", "ci-leg", "race", str(shard), "2"
        )
        assert proc.returncode == 0, proc.stderr[-1500:] + proc.stdout[-1500:]
        assert calls, f"shard {shard}/2 ran no go test"
        assert all(" -race " in f" {c} " for c in calls), calls
        for call in calls:
            seen += [w for w in call.split() if w.startswith(mod)]
    # The root module's packages: every one exactly once across the two slices.
    root_seen = [p for p in seen if p in set(pkgs)]
    assert sorted(root_seen) == sorted(pkgs)
    assert len(root_seen) == len(set(root_seen))


def test_a_race_slice_that_selects_nothing_fails_loudly(tmp_path: Path) -> None:
    proc, calls = _run_check_go(tmp_path, "ci-leg", "race", "999", "1000")
    assert proc.returncode != 0
    assert "selected zero packages" in proc.stderr
    assert calls == [], "a zero-package leg must not run go test"


def test_bad_leg_arguments_fail(tmp_path: Path) -> None:
    for args, needle in (
        (("ci-leg",), "accepts LEG"),
        (("ci-leg", "nope"), "must be one of"),
        (("ci-leg", "race", "0", "2"), "outside 1..2"),
        (("ci-leg", "race", "3", "2"), "outside 1..2"),
        (("ci-leg", "race", "x", "2"), "must be a positive integer"),
        (("ci-leg", "race"), "must be a positive integer"),
    ):
        proc, calls = _run_check_go(tmp_path / str(abs(hash(args))), *args)
        assert proc.returncode != 0, args
        assert needle in proc.stderr, (args, proc.stderr[-300:])
        assert calls == []


# ---------------------------------------------------------------------------
# 3. Leg logs.
# ---------------------------------------------------------------------------


def test_every_stage_prints_start_and_done_stamps(tmp_path: Path) -> None:
    proc, _ = _run_check_go(tmp_path, "ci-leg", "race", "1", "2")
    assert proc.returncode == 0, proc.stderr[-1500:]
    assert re.search(r"==> stage race 1/2 start \d\d:\d\d:\d\dZ", proc.stdout)
    assert re.search(r"<== stage race 1/2 done in \d+s at \d\d:\d\d:\d\dZ", proc.stdout)
    assert "ci-leg race: OK" in proc.stdout


def test_a_failing_stage_prints_no_done_and_fails_the_leg(tmp_path: Path) -> None:
    proc, calls = _run_check_go(tmp_path, "ci-leg", "race", "1", "2", fail=True)
    assert calls, "the stand-in go test was never reached"
    assert proc.returncode != 0
    assert "==> stage race 1/2 start" in proc.stdout
    assert "<== stage race 1/2 done" not in proc.stdout
    assert "ci-leg race: OK" not in proc.stdout


# ---------------------------------------------------------------------------
# 4. The workflow.
# ---------------------------------------------------------------------------


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def test_the_matrix_carries_exactly_the_legs_the_script_knows() -> None:
    leg = _jobs()["go-quality-leg"]
    entries = leg["strategy"]["matrix"]["include"]
    args = [e["args"] for e in entries]
    names = [e["name"] for e in entries]
    assert len(set(names)) == len(names), "duplicate leg names"
    non_race = sorted(a for a in args if not a.startswith("race "))
    assert non_race == ["oracles", "static", "test"], args
    races = sorted(a for a in args if a.startswith("race "))
    count = len(races)
    assert count >= 1
    assert races == [f"race {k} {count}" for k in range(1, count + 1)], (
        f"the race entries {races} are not 1..N of N (a gap or a stale COUNT leaves "
        "packages unrun or run twice)"
    )
    assert leg["strategy"]["fail-fast"] is False
    assert "if" not in leg, (
        "a job-level `if` on the legs would let GitHub report them skipped; "
        "relevance is decided by a STEP inside each leg"
    )
    runs = [s["run"] for s in leg["steps"] if isinstance(s.get("run"), str)]
    assert any("ci-leg ${{ matrix.args }}" in r for r in runs)
    assert leg["timeout-minutes"] <= 30


def test_the_fan_in_keeps_the_required_context_and_fails_on_a_missing_leg() -> None:
    jobs = _jobs()
    fan_in = jobs["go-quality"]
    assert fan_in["name"] == "go-quality", "the required context name changed"
    assert str(fan_in["if"]).replace(" ", "") == "always()", (
        "the fan-in must be always(): under any condition that lets it skip, a "
        "cancelled run leaves the REQUIRED check skipped"
    )
    assert fan_in["needs"] == ["go-quality-leg"]
    step = next(
        s for s in fan_in["steps"] if "aggregate_gate_results" in str(s.get("run", ""))
    )
    env = step["env"]
    assert env["GATE_NAME"] == "go-quality"
    assert env["GATE_HAS_SELECTOR"] == "false"
    assert (
        env["GATED_JOB_1"]
        == "go-quality-leg|unconditional|${{ needs.go-quality-leg.result }}"
    ), (
        "the fan-in must judge the legs `unconditional`: a skipped, cancelled or "
        "missing leg fails the gate"
    )
    assert "GATED_JOB_2" not in env


def test_only_the_static_leg_runs_the_static_only_steps() -> None:
    leg = _jobs()["go-quality-leg"]
    static_only = (
        "Prove the migration matrix contract",
        "Check the gqlgen output",
        "Install pinned shellcheck",
        "Check the shellcheck pin",
        "Validate River compatibility harness",
        "Prepare base job contracts",
        "Check the migration matrix has not gone stale",
    )
    for step in leg["steps"]:
        name = step.get("name", "")
        if name.startswith(static_only):
            assert "matrix.name == 'static'" in str(step.get("if", "")), name
    assert any(
        "Install locked live provider oracle dependencies" in s.get("name", "")
        and "matrix.name == 'oracles'" in str(s.get("if", ""))
        for s in leg["steps"]
    )
