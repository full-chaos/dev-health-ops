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

import json
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
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
case " $* " in *" -list "*)
  printf '%s\\n' "$*" >> "${{FAKE_GO_LIST_LOG}}"
  # FAKE_GO_LIST_REPLAY: the real `go test -race -list .*` output, produced ONCE per session. It is replayed only for a call that
  # carries BOTH -race and `-list .*`; any other spelling of the listing runs the real go (so the listing arguments stay under test).
  if [ -n "${{FAKE_GO_LIST_REPLAY:-}}" ] && [[ " $* " == *" -race "* ]] && [[ " $* " == *" -list .* "* ]]; then
    cat "${{FAKE_GO_LIST_REPLAY}}"
  else
    "{real_go}" "$@" || exit $?
  fi
  # FAKE_GO_LIST_EXTRA appends one name to the listing, as a Fuzz/Example/race-tagged test would be.
  [ -z "${{FAKE_GO_LIST_EXTRA:-}}" ] || printf '%s\\n' "${{FAKE_GO_LIST_EXTRA}}"
  exit 0 ;;
esac
printf '%s\\n' "$*" >> "${{FAKE_GO_LOG}}"
[ -z "${{FAKE_GO_FAIL:-}}" ] || exit 1
# FAKE_GO_FAIL_ONLY fails exactly one of the two race calls of a leg: "providersync" (the test-name slice) or "packages" (the package run).
case "${{FAKE_GO_FAIL_ONLY:-}}" in
  providersync) case " $* " in *" ./internal/providersync "*) exit 1 ;; esac ;;
  packages) case " $* " in *" ./internal/providersync "*) ;; *) exit 1 ;; esac ;;
esac
exit 0
"""


_LIST_REPLAY: Path | None = None


@pytest.fixture(scope="session", autouse=True)
def _providersync_race_listing(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Run the REAL `go test -race -list .*` of internal/providersync once; the stand-in replays it (the real lister stays the producer)."""
    global _LIST_REPLAY
    real_go = shutil.which("go")
    assert real_go, "go is required"
    out = tmp_path_factory.mktemp("listing") / "providersync_race_list.txt"
    env = {**os.environ, "GOWORK": "off"}
    done = subprocess.run(
        [
            real_go,
            "test",
            "-mod=readonly",
            "-race",
            "-list",
            ".*",
            "./internal/providersync",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-1500:]
    out.write_text(done.stdout)
    _LIST_REPLAY = out


def _list_calls(run_dir: Path) -> list[str]:
    """The `go test ... -list ...` calls the stand-in saw in a run (arguments after `test`)."""
    return [line for line in (run_dir / "go_list.log").read_text().splitlines() if line]


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
    tmp_path: Path,
    *args: str,
    fail: bool = False,
    extra_env: dict[str, str] | None = None,
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
    list_log = tmp_path / "go_list.log"
    list_log.write_text("")
    (tmp_path / "tmp").mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "FAKE_GO_LOG": str(log),
        "FAKE_GO_LIST_LOG": str(list_log),
        **({"FAKE_GO_LIST_REPLAY": str(_LIST_REPLAY)} if _LIST_REPLAY else {}),
        "TMPDIR": str(tmp_path / "tmp"),
    }
    env.pop("FAKE_GO_FAIL", None)
    env.update(extra_env or {})
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
    # CHAOS-8135: the workflow runs three legs; no leg may exceed the mean by more than 10%.
    three = [
        sum(weights.get(_key(mod, p), DEFAULT_WEIGHT) for p in _shard(mod, pkgs, k, 3))
        for k in (1, 2, 3)
    ]
    assert max(three) <= 1.1 * (sum(three) / 3), (
        f"3-way race slices unbalanced: {three}"
    )


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
    provider_calls = 0
    for shard in (1, 2):
        proc, calls = _run_check_go(
            tmp_path / f"s{shard}", "ci-leg", "race", str(shard), "2"
        )
        assert proc.returncode == 0, proc.stderr[-1500:] + proc.stdout[-1500:]
        assert calls, f"shard {shard}/2 ran no go test"
        assert all(" -race " in f" {c} " for c in calls), calls
        for call in calls:
            seen += [w for w in call.split() if w.startswith(mod)]
        # CHAOS-8166: providersync is not a whole-package row; every leg runs its own test-name shard of it.
        provider_calls += sum(
            1 for c in calls if "-run" in c and c.endswith("./internal/providersync")
        )
    assert provider_calls == 2, "each race leg runs one providersync test shard"
    # The root module's packages: every one exactly once across the two slices
    # (providersync through its test-name shards, one per leg).
    root_seen = [p for p in seen if p in set(pkgs)]
    expected = [p for p in pkgs if p != f"{mod}/internal/providersync"]
    assert sorted(root_seen) == sorted(expected)
    assert len(root_seen) == len(set(root_seen))


def test_a_race_slice_that_selects_nothing_fails_loudly(tmp_path: Path) -> None:
    # More legs than there are packages and tests: no shard of either selects anything.
    proc, calls = _run_check_go(tmp_path, "ci-leg", "race", "5000", "5001")
    assert proc.returncode != 0
    assert "select zero" in proc.stderr or "selected zero" in proc.stderr
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
    # The locked live-Python dependencies are installed by EVERY leg: unit tests
    # of the test and race legs run real Python programs too.
    deps = [
        s
        for s in leg["steps"]
        if "Install locked live provider oracle dependencies" in s.get("name", "")
    ]
    assert len(deps) == 1
    assert "matrix.name" not in str(deps[0].get("if", "")), (
        "the oracle dependencies must be installed on every leg, not only one"
    )


# ---------------------------------------------------------------------------
# 5. CHAOS-8135: every race package has an explicit weights row (static guard).
# ---------------------------------------------------------------------------


def _missing_awk(
    mod: str, weights_file: Path, packages: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "awk",
            "-v",
            f"mod={mod}",
            "-v",
            f"weights={weights_file}",
            "-f",
            str(ROOT / "ci" / "go_race_missing_rows.awk"),
        ],  # fmt: skip
        input=packages,
        capture_output=True,
        text=True,
        check=False,
    )


def test_every_package_of_the_root_module_has_a_weights_row() -> None:
    mod, pkgs = _go_list()
    missing = sorted(_key(mod, p) for p in pkgs if _key(mod, p) not in _weights())
    assert not missing, f"packages with no row in ci/go_race_weights.tsv: {missing}"


def test_the_missing_row_awk_names_a_removed_row_and_refuses_empty_input(
    tmp_path: Path,
) -> None:
    mod, pkgs = _go_list()
    text = "\n".join(pkgs) + "\n"
    green = _missing_awk(mod, WEIGHTS, text)
    assert green.returncode == 0 and green.stdout == "", green
    weights = _weights()
    victim = "internal/httpguard"
    assert victim in weights
    trimmed = tmp_path / "trimmed.tsv"
    trimmed.write_text(
        "".join(f"{k}\t{v}\n" for k, v in weights.items() if k != victim)
    )
    red = _missing_awk(mod, trimmed, text)
    assert red.stdout.split() == [victim], red
    empty = tmp_path / "empty.tsv"
    empty.write_text("# no rows\n")
    assert _missing_awk(mod, empty, text).returncode != 0, (
        "an empty weights file must fail loudly"
    )
    assert _missing_awk(mod, WEIGHTS, "").returncode != 0, (
        "an empty package list must fail loudly"
    )


def test_a_race_leg_with_a_package_that_has_no_row_fails_before_any_test_runs(
    tmp_path: Path,
) -> None:
    weights = _weights()
    victim = "internal/httpguard"
    trimmed = tmp_path / "trimmed.tsv"
    trimmed.write_text(
        "".join(f"{k}\t{v}\n" for k, v in weights.items() if k != victim)
    )
    proc, calls = _run_check_go(
        tmp_path / "run",
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"GO_RACE_WEIGHTS": str(trimmed)},
    )
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "NO row in ci/go_race_weights.tsv" in proc.stderr
    assert victim in proc.stderr
    assert calls == [], "go test must not run when a package has no row"
    assert "ci-leg race: OK" not in proc.stdout


def test_a_race_leg_with_an_empty_weights_file_fails_before_any_test_runs(
    tmp_path: Path,
) -> None:
    empty = tmp_path / "empty.tsv"
    empty.write_text("# no rows\n")
    proc, calls = _run_check_go(
        tmp_path / "run",
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"GO_RACE_WEIGHTS": str(empty)},
    )
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "go_race_missing_rows.awk failed" in proc.stderr
    assert calls == [], "go test must not run when the weights file holds no row"


# ---------------------------------------------------------------------------
# 6. CHAOS-8135: tests compiled out of the race leg are listed and still run without it.
# ---------------------------------------------------------------------------

RACE_EXCLUDED = ROOT / "ci" / "race_excluded_tests.tsv"


def _race_excluded_rows() -> list[tuple[str, str]]:
    rows = [
        tuple(line.split("\t"))
        for line in RACE_EXCLUDED.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert rows, "ci/race_excluded_tests.tsv lists no test"
    return rows  # type: ignore[return-value]


def _go_list_test_files(*tags: str) -> dict[str, set[str]]:
    """Package import path -> the _test.go files `go list` compiles with these tags (whole module)."""
    real_go = shutil.which("go")
    assert real_go, "go is required"
    env = {**os.environ, "GOWORK": "off", "GOFLAGS": "-mod=readonly"}
    cmd = [real_go, "list", "-json=ImportPath,Dir,TestGoFiles,XTestGoFiles"]
    if tags:
        cmd += ["-tags", ",".join(tags)]
    proc = subprocess.run(
        [*cmd, "./..."], cwd=ROOT, env=env, capture_output=True, text=True, check=True
    )
    decoder = json.JSONDecoder()
    text, index, files = proc.stdout, 0, {}
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        pkg, index = decoder.raw_decode(text, index)
        names = pkg.get("TestGoFiles", []) + pkg.get("XTestGoFiles", [])
        files[pkg["ImportPath"]] = {str(Path(pkg["Dir"]) / n) for n in names}
    return files


def test_every_race_excluded_test_is_listed() -> None:
    # Derived from Go itself over the whole module: the _test.go files `go list` compiles WITHOUT the race tag and not WITH
    # it (whatever the constraint form: `!race && linux`, a license header before it, any directory), and their Test
    # functions, must be exactly the rows of ci/race_excluded_tests.tsv.
    mod, _ = _go_list()
    plain = _go_list_test_files()
    raced = _go_list_test_files("race")
    assert plain, "go list found no package"
    found: set[tuple[str, str]] = set()
    for pkg, names in plain.items():
        for source in sorted(names - raced.get(pkg, set())):
            text = Path(source).read_text(encoding="utf-8")
            package = _key(mod, pkg)
            found |= {
                (package, name)
                for name in re.findall(r"^func (Test\w+)\(", text, flags=re.M)
            }
    listed = set(_race_excluded_rows())
    assert found == listed, (
        "tests compiled without -race only and ci/race_excluded_tests.tsv differ: "
        f"unlisted {sorted(found - listed)}, listed but compiled in the race leg {sorted(listed - found)}"
    )


def test_check_test_runs_the_race_excluded_proof() -> None:
    script = CHECK_GO.read_text(encoding="utf-8")
    body = _case_block(script, "check_test() {", "\n}\n")
    assert "check_race_excluded_ran" in body


# ---------------------------------------------------------------------------
# 7. CHAOS-8166: internal/providersync runs in every race leg, one test-name shard each.
# ---------------------------------------------------------------------------

PROVIDER_WEIGHTS = ROOT / "ci" / "go_providersync_race_weights.tsv"
PROVIDER_AWK = ROOT / "ci" / "go_providersync_race_shard.awk"


def _provider_tests() -> list[str]:
    real_go = shutil.which("go")
    assert real_go
    out = subprocess.run(
        [real_go, "test", "-list", "^Test", "./internal/providersync"],
        cwd=ROOT,
        env={**os.environ, "GOWORK": "off", "GOFLAGS": "-mod=readonly"},
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    names = [line for line in out.splitlines() if line.startswith("Test")]
    assert names, "go test -list reported no providersync test"
    return names


def _provider_shard(
    names: list[str], shard: int, count: int, weights: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "awk",
            "-v",
            f"shard={shard}",
            "-v",
            f"count={count}",
            "-v",
            f"weights={weights}",
            "-f",
            str(PROVIDER_AWK),
        ],  # fmt: skip
        input="\n".join(names) + "\n",
        capture_output=True,
        text=True,
        check=False,
    )


def test_every_providersync_test_is_in_exactly_one_race_shard() -> None:
    names = _provider_tests()
    for count in (2, 3, 4):
        got: list[str] = []
        for shard in range(1, count + 1):
            proc = _provider_shard(names, shard, count, PROVIDER_WEIGHTS)
            assert proc.returncode == 0, proc
            part = proc.stdout.split()
            assert part, f"providersync race shard {shard}/{count} is empty"
            got += part
        assert sorted(got) == sorted(names), f"count {count}: not an exact partition"


def test_the_providersync_race_weights_name_real_tests_and_balance() -> None:
    names = _provider_tests()
    rows = [
        line.split("\t")
        for line in PROVIDER_WEIGHTS.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    keys = [r[0] for r in rows]
    assert keys == sorted(keys) and len(keys) == len(set(keys))
    assert not [k for k in keys if k not in set(names)], "a weight names a missing test"
    weights = {r[0]: int(r[1]) for r in rows}
    totals = []
    for shard in (1, 2, 3):
        part = _provider_shard(names, shard, 3, PROVIDER_WEIGHTS).stdout.split()
        totals.append(sum(weights.get(n, 20) for n in part))
    # No shard above the heaviest single test plus a fair share of the rest (the measured run: 199 s test, 160 s slices).
    assert max(totals) <= max(weights.values()) + 0.25 * sum(totals), totals


def test_a_providersync_race_leg_whose_shards_do_not_partition_fails_before_any_test_runs(
    tmp_path: Path,
) -> None:
    empty = tmp_path / "empty.tsv"
    empty.write_text("# no rows\n")
    proc, calls = _run_check_go(
        tmp_path / "run",
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"GO_PROVIDERSYNC_RACE_WEIGHTS": str(empty)},
    )
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "go_providersync_race_shard.awk failed" in proc.stderr
    assert not [c for c in calls if c.endswith("./internal/providersync")]


def _provider_run_names(call: str) -> list[str]:
    """The test names of the -run regex of a recorded `go test ... ./internal/providersync` call."""
    words = call.split()
    regex = words[words.index("-run") + 1]
    assert regex.startswith("^(") and regex.endswith(")$"), regex
    return regex[2:-2].split("|")


def test_each_race_leg_runs_exactly_its_own_providersync_slice(tmp_path: Path) -> None:
    names = _provider_tests()
    count = 3
    union: list[str] = []
    for shard in range(1, count + 1):
        proc, calls = _run_check_go(
            tmp_path / f"leg{shard}", "ci-leg", "race", str(shard), str(count)
        )
        assert proc.returncode == 0, proc.stderr[-1500:] + proc.stdout[-1500:]
        provider = [c for c in calls if c.endswith("./internal/providersync")]
        assert len(provider) == 1, provider
        ran = _provider_run_names(provider[0])
        want = _provider_shard(names, shard, count, PROVIDER_WEIGHTS).stdout.split()
        assert sorted(ran) == sorted(want), f"leg {shard} does not run its own slice"
        union += ran
        _assert_race_listing_call(tmp_path / f"leg{shard}")
    assert sorted(union) == sorted(names), "the legs' slices are not the listed set"
    assert len(union) == len(set(union)), "a providersync test runs in two legs"


def test_a_shard_awk_that_loses_a_test_fails_the_leg_at_the_partition_check(
    tmp_path: Path,
) -> None:
    # Every shard stays non-empty, so only the exact-partition comparison can catch the lost test.
    broken = tmp_path / "broken.awk"
    text = PROVIDER_AWK.read_text(encoding="utf-8")
    assert "$0 ~ /^Test/ {" in text
    broken.write_text(text.replace("$0 ~ /^Test/ {", "$0 ~ /^Test/ && NR > 1 {"))
    proc, calls = _run_check_go(
        tmp_path / "run",
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"GO_PROVIDERSYNC_RACE_SHARD_AWK": str(broken)},
    )
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "not an exact partition" in proc.stderr
    assert not [c for c in calls if c.endswith("./internal/providersync")]


def _provider_calls(calls: list[str]) -> list[str]:
    return [c for c in calls if c.endswith("./internal/providersync")]


def test_a_failing_providersync_slice_fails_the_leg_and_stops_it(
    tmp_path: Path,
) -> None:
    # CHAOS-8275: the stand-in fails ONLY the providersync slice, so `|| true` on that call is visible (a stand-in that fails every call is not).
    proc, calls = _run_check_go(
        tmp_path,
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"FAKE_GO_FAIL_ONLY": "providersync"},
    )
    assert _provider_calls(calls), "the providersync slice was never run"
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "ci-leg race: OK" not in proc.stdout
    assert "<== stage race 1/3 done" not in proc.stdout
    assert calls == _provider_calls(calls), (
        "the leg went on to the package run after a failed slice"
    )


def test_a_failing_package_race_run_fails_the_leg_after_a_green_slice(
    tmp_path: Path,
) -> None:
    # CHAOS-8275: the slice passes and ONLY the package run fails, so `|| true` on the package call is visible.
    proc, calls = _run_check_go(
        tmp_path,
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"FAKE_GO_FAIL_ONLY": "packages"},
    )
    assert _provider_calls(calls), "the providersync slice was never run"
    assert [c for c in calls if c not in _provider_calls(calls)], (
        "the package run was never reached"
    )
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "<== stage race 1/3 done" not in proc.stdout
    assert "ci-leg race: OK" not in proc.stdout


def _assert_race_listing_call(run_dir: Path) -> None:
    # CHAOS-8275: the listing must be the RACE test binary's list of EVERY name; the stand-in cannot tell, so pin the call's arguments.
    calls = [c for c in _list_calls(run_dir) if c.endswith("./internal/providersync")]
    assert calls, "the providersync listing was never requested"
    for call in calls:
        assert " -race " in f" {call} ", (
            f"the providersync listing is not the race build's: {call}"
        )
        assert " -list .* " in f" {call} ", (
            f"the providersync listing is not of every name: {call}"
        )


def test_a_non_test_name_in_the_providersync_race_listing_fails_before_any_test_runs(
    tmp_path: Path,
) -> None:
    # CHAOS-8275: a Fuzz*/Example*/Benchmark* name (or a test in a race-tagged file listed only by the race build) belongs to no `^Test` shard.
    proc, calls = _run_check_go(
        tmp_path,
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"FAKE_GO_LIST_EXTRA": "FuzzSomething"},
    )
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "FuzzSomething" in proc.stderr
    assert "no race shard would run" in proc.stderr
    _assert_race_listing_call(tmp_path)
    assert not _provider_calls(calls)


def test_a_shard_awk_that_swaps_a_test_keeps_the_count_and_fails_at_the_set_compare(
    tmp_path: Path,
) -> None:
    # CHAOS-8275: shard 2 runs the first test instead of its own first one. Every shard stays non-empty and the total count is unchanged,
    # so only the sorted-set comparison of the partition check can catch "one test twice, one test lost".
    swap = tmp_path / "swap.awk"
    swap.write_text(
        "NR == 1 { first = $0 }\n"
        "$0 ~ /^Test/ { n++; if ((n - 1) % count + 1 == shard) {"
        " if (shard == 2 && !done) { done = 1; print first } else print } }\n"
    )
    proc, calls = _run_check_go(
        tmp_path / "run",
        "ci-leg",
        "race",
        "1",
        "3",
        extra_env={"GO_PROVIDERSYNC_RACE_SHARD_AWK": str(swap)},
    )
    assert proc.returncode != 0, proc.stdout[-800:]
    assert "not an exact partition" in proc.stderr
    assert not _provider_calls(calls)


def test_the_providersync_shard_awk_balances_by_weight_and_weighs_an_unlisted_test(
    tmp_path: Path,
) -> None:
    # CHAOS-8275: one heavy listed test and six unlisted ones over three shards. By weight the heavy test sits ALONE and the six
    # unlisted tests (default weight > 0) split 3/3. An awk that ignores the weights file splits 3/2/2 (heavy test not alone); an awk
    # whose default weight is 0 never moves an unlisted test off the lightest shard (1/6/0).
    weights = tmp_path / "w.tsv"
    weights.write_text("TestHeavy\t1000\n")
    names = ["TestHeavy"] + [f"Test{c}" for c in "ABCDEF"]
    sizes = []
    for shard in (1, 2, 3):
        proc = _provider_shard(names, shard, 3, weights)
        assert proc.returncode == 0, proc.stderr
        part = proc.stdout.split()
        sizes.append(len(part))
        if shard == 1:
            assert part == ["TestHeavy"], part
    assert sizes == [1, 3, 3], sizes


# ---------------------------------------------------------------------------
# 5. -trimpath (CHAOS-8296).
# ---------------------------------------------------------------------------

GO_CALL = re.compile(r"(?:^|[\s;&(|`])go (build|test|vet|run|install)(?=\s|$)")

# Places a `go <verb>` appears that are not a call this repo makes with the module's
# flags, each with the reason. A new entry is a decision, not a convenience.
GO_CALL_EXCEPTIONS = {
    (
        ".github/workflows/arc-runner-image.yml",
        "go run /tmp/cgo_probe.go",
    ): "one probe file outside the module, run to check cgo on the runner image",
    (
        "ci/check_migration_matrix.sh",
        'go run ./cmd/dev-health-migration-matrix -render -root ."',
    ): "the text of the re-verify hint a human reads (a multi-line message), not a call",
    (
        "ci/python_free_ratchet.sh",
        "go test stream artifact",
    ): "words in a message, not a call",
}


def _go_call_files() -> list[Path]:
    patterns = (
        "ci/**/*.sh",
        ".github/workflows/*.yml",
        "docker/**/Dockerfile*",
        "docker/*Dockerfile*",
        "scripts/**/*.sh",
    )
    files = {ROOT / "ci" / "check_go.sh"}
    for pattern in patterns:
        files.update(ROOT.glob(pattern))
    return sorted(files)


def _unquote_command_substitutions(line: str) -> str:
    """Drop the double quotes around each `"$( ... )"` of a line (matching the closing paren by depth)."""
    out: list[str] = []
    i = 0
    while i < len(line):
        if line.startswith('"$(', i):
            depth = 0
            j = i + 1
            while j < len(line):
                if line.startswith("$(", j):
                    depth += 1
                    j += 2
                    continue
                if line[j] == "(":
                    depth += 1
                elif line[j] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            if depth == 0 and j + 1 < len(line) and line[j + 1] == '"':
                out.append(line[i + 1 : j + 1])
                i = j + 2
                continue
        out.append(line[i])
        i += 1
    return "".join(out)


def _go_calls() -> list[tuple[str, str]]:
    """Every `go build|test|vet|run|install` call of the CI scripts, workflows and Dockerfiles,
    as (repo-relative file, the command text from `go` to the next `&&`, `||`, `;`, `|` or `)`).

    Derived by the go verb, not by any flag: comments, YAML names/descriptions, the usage text
    of check_go.sh and quoted strings (the words of a message) are removed first, and a
    backslash continuation joins a command's lines.

    LIMITS (named, not covered): a workflow `run:` value written as a quoted scalar; a call
    inside a `bash -c "..."` string; go called by a path or through a variable; files outside
    the globs of _go_call_files (tools/codex-review/*.sh, .github/actions, Python callers).
    A call in any of those is not seen by these tests."""
    calls: list[tuple[str, str]] = []
    for path in _go_call_files():
        text = path.read_text(encoding="utf-8")
        text = re.sub(r"(?ms)^usage\(\) \{.*?^\}\n", "", text)
        text = re.sub(r"(?m)^\s*#.*$", "", text)
        text = re.sub(r"(?m)^\s*-?\s*(name|description|summary):.*$", "", text)
        text = re.sub(r"\\\n\s*", " ", text)
        for line in text.split("\n"):
            # A command substitution inside double quotes ("$(cd x && go test ...)") is
            # code, not a message: drop the quotes around it so its calls are derived and
            # only the string literals INSIDE it are blanked.
            line = _unquote_command_substitutions(line)
            unquoted = re.sub(r"\"(?:[^\"\\]|\\.)*\"|'[^']*'", '""', line)
            for match in GO_CALL.finditer(unquoted):
                command = re.split(r"&&|\|\||;|\||\)", unquoted[match.start() :])[0]
                calls.append((str(path.relative_to(ROOT)), command.strip()))
    return calls


def _is_excepted(file: str, command: str) -> bool:
    return any(
        file == f and command.startswith(prefix) for (f, prefix) in GO_CALL_EXCEPTIONS
    )


def test_every_go_build_test_vet_run_call_carries_trimpath() -> None:
    calls = _go_calls()
    assert len(calls) >= 35, f"the derivation found only {len(calls)} go calls"
    missing = [
        f"{file}: {command[:90]}"
        for file, command in calls
        if "-trimpath" not in command and not _is_excepted(file, command)
    ]
    assert not missing, "go call(s) without -trimpath:\n" + "\n".join(missing)
    stale = [
        key
        for key in GO_CALL_EXCEPTIONS
        if not any(
            file == key[0] and command.startswith(key[1]) for file, command in calls
        )
    ]
    assert not stale, f"GO_CALL_EXCEPTIONS entries no call matches any more: {stale}"


def test_every_go_call_of_check_go_names_mod_readonly_and_trimpath() -> None:
    # The derivation is by verb, so a call that lost -mod=readonly is still found; and a
    # line that names both a go verb and -mod=readonly must be one the derivation found.
    derived = [command for file, command in _go_calls() if file == "ci/check_go.sh"]
    assert len(derived) >= 20, f"only {len(derived)} calls derived from check_go.sh"
    for command in derived:
        assert "-mod=readonly" in command, (
            f"check_go.sh call without -mod=readonly: {command[:90]}"
        )
        assert "-trimpath" in command, (
            f"check_go.sh call without -trimpath: {command[:90]}"
        )
    for number, raw in enumerate(CHECK_GO.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if (
            line.startswith("#")
            or not GO_CALL.search(line)
            or "-mod=readonly" not in line
        ):
            continue
        if "printf" in line or "die " in line:
            continue
        head = line[GO_CALL.search(line).start() :].strip()[:25]
        assert any(head in command for command in derived), (
            f"check_go.sh line {number} holds a go call the derivation missed: {line[:90]}"
        )


def test_the_stand_in_sees_trimpath_on_every_recorded_race_call(tmp_path: Path) -> None:
    proc, calls = _run_check_go(tmp_path, "ci-leg", "race", "1", "3")
    assert proc.returncode == 0, proc.stderr[-1500:]
    assert calls, "no go test call was recorded"
    assert all(" -trimpath " in f" {c} " for c in calls), calls


def test_the_goflags_scrub_stays_so_trimpath_is_explicit() -> None:
    text = CHECK_GO.read_text(encoding="utf-8")
    line = next(row for row in text.splitlines() if row.startswith("GO_ENV_OFF=("))
    assert "-u GOFLAGS" in line, (
        "the GOFLAGS scrub must stay: -trimpath is an explicit flag, not an inherited one"
    )
