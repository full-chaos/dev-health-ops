"""The sharded `venue-oracles` check (CHAOS-6574): one package per matrix job,
and a required job that passes only when every planned package passed.

WHAT THIS PINS
---------------
1. `ci/check_go.sh venue-oracle-packages` lists exactly the packages that
   declare a `Test*VenueOracle*` function, found here independently, so the
   matrix cannot drop a package.
2. `VENUE_ORACLE_PACKAGE` naming a package with no venue test is refused,
   so a shard can never pass having run nothing.
3. The workflow wires the plan's list into the matrix, runs each shard on
   its own package, writes a completion marker only after the run step, and
   hands the plan and shard results to the verdict script.
4. The verdict script's decision table: skip, all green, any shard not
   green, a missing or unplanned marker, a malformed plan.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "venue-oracles.yml"
CHECK_GO = REPO_ROOT / "ci" / "check_go.sh"

_SPEC = importlib.util.spec_from_file_location(
    "venue_oracles_verdict", REPO_ROOT / "ci" / "venue_oracles_verdict.py"
)
assert _SPEC is not None and _SPEC.loader is not None
_VERDICT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_VERDICT)
verdict = _VERDICT.verdict

VENUE_TEST = re.compile(r"^func Test[A-Za-z0-9_]*VenueOracle", re.MULTILINE)


def _discovered_independently() -> set[str]:
    found: set[str] = set()
    for path in REPO_ROOT.rglob("*_test.go"):
        if ".venv" in path.parts or "node_modules" in path.parts:
            continue
        if VENUE_TEST.search(path.read_text(encoding="utf-8", errors="replace")):
            found.add("./" + str(path.parent.relative_to(REPO_ROOT)))
    return found


def _run_check_go(
    *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(CHECK_GO), *args],
        cwd=REPO_ROOT,
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def test_the_package_list_is_every_package_with_a_venue_oracle() -> None:
    result = _run_check_go("venue-oracle-packages")
    assert result.returncode == 0, result.stderr
    lines = [
        line for line in result.stdout.splitlines() if line.startswith("packages=")
    ]
    assert len(lines) == 1, result.stdout
    listed = json.loads(lines[0].removeprefix("packages="))
    assert len(listed) == len(set(listed)), f"a package is listed twice: {listed}"
    expected = _discovered_independently()
    assert expected, (
        "no venue-oracle package found at all; the discovery here is broken"
    )
    assert set(listed) == expected, (
        f"listed but not found: {sorted(set(listed) - expected)}; "
        f"found but not listed: {sorted(expected - set(listed))}"
    )


def test_a_package_without_venue_oracles_is_refused() -> None:
    result = _run_check_go(
        "venue-oracles",
        env={
            "DEV_HEALTH_LIVE_PYTHON_ORACLES": "1",
            "VENUE_ORACLE_PACKAGE": "./internal/no-such-package",
        },
    )
    assert result.returncode != 0
    assert "has no discovered VenueOracle test" in result.stderr + result.stdout


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def test_the_plan_feeds_the_matrix_and_each_shard_runs_its_own_package() -> None:
    jobs = _jobs()
    plan = jobs["venue-oracles-plan"]
    assert "venue-oracle-packages" in "\n".join(
        step.get("run", "") for step in plan["steps"]
    )
    assert plan["outputs"]["packages"] == "${{ steps.packages.outputs.packages }}"

    shard = jobs["venue-oracles-package"]
    assert shard["needs"] == "venue-oracles-plan"
    assert shard["strategy"]["fail-fast"] is False
    assert (
        shard["strategy"]["matrix"]["package"]
        == "${{ fromJSON(needs.venue-oracles-plan.outputs.packages) }}"
    )
    names = [step.get("name", "") for step in shard["steps"]]
    run_index = next(
        i
        for i, step in enumerate(shard["steps"])
        if "check_go.sh venue-oracles" in step.get("run", "")
    )
    assert (
        shard["steps"][run_index]["env"]["VENUE_ORACLE_PACKAGE"]
        == "${{ matrix.package }}"
    )
    marker_index = names.index("Record that this package passed")
    assert marker_index > run_index, (
        "the completion marker must be written only after the run step"
    )
    for step in shard["steps"][run_index : marker_index + 1]:
        assert "if" not in step, (
            f"a step between the run and the marker is conditional: {step.get('name')}"
        )


def test_the_required_job_hands_everything_to_the_verdict() -> None:
    required = _jobs()["venue-oracles"]
    assert set(required["needs"]) == {"venue-oracles-plan", "venue-oracles-package"}
    decide = next(
        step
        for step in required["steps"]
        if "venue_oracles_verdict.py" in step.get("run", "")
    )
    assert decide["env"] == {
        "PLAN_RESULT": "${{ needs.venue-oracles-plan.result }}",
        "SHARDS_RESULT": "${{ needs.venue-oracles-package.result }}",
        "RELEVANT": "${{ needs.venue-oracles-plan.outputs.relevant }}",
        "PACKAGES": "${{ needs.venue-oracles-plan.outputs.packages }}",
        "MARKERS_DIR": "${{ runner.temp }}/venue-oracle-done",
    }
    assert "continue-on-error" not in decide


PLANNED = json.dumps(["./a", "./b"])


@pytest.mark.parametrize(
    ("plan", "shards", "relevant", "packages", "markers", "passed"),
    [
        ("success", "skipped", "false", "", [], True),
        ("success", "success", "false", "", [], False),
        ("success", "success", "true", PLANNED, ["./a", "./b"], True),
        ("success", "failure", "true", PLANNED, ["./a"], False),
        ("success", "cancelled", "true", PLANNED, ["./a", "./b"], False),
        ("success", "skipped", "true", PLANNED, [], False),
        ("success", "success", "true", PLANNED, ["./a"], False),
        ("success", "success", "true", PLANNED, ["./a", "./b", "./c"], False),
        ("success", "success", "true", PLANNED, ["./a", "./a"], False),
        ("success", "success", "true", "[]", [], False),
        ("success", "success", "true", "not json", [], False),
        ("success", "success", "", PLANNED, ["./a", "./b"], False),
        ("failure", "skipped", "", "", [], False),
        ("cancelled", "skipped", "", "", [], False),
    ],
)
def test_verdict_table(
    plan: str,
    shards: str,
    relevant: str,
    packages: str,
    markers: list[str],
    passed: bool,
) -> None:
    got, reason = verdict(plan, shards, relevant, packages, markers)
    assert got is passed, reason


def test_an_empty_discovery_fails_the_plan(tmp_path: Path) -> None:
    """A grep that finds no venue test must fail the plan job, never plan an
    empty matrix that the required job could read as "every shard green".
    The verb runs here in a tree with no Go test at all; the plan step's
    `set -euo pipefail` turns its failure into the plan job's.
    """
    (tmp_path / "ci").mkdir()
    (tmp_path / "ci" / "check_go.sh").write_text(
        CHECK_GO.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (tmp_path / "go.mod").write_text(
        "module example.com/empty\n\ngo 1.25\n", encoding="utf-8"
    )
    result = subprocess.run(
        ["bash", "ci/check_go.sh", "venue-oracle-packages"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode != 0, result.stdout
    assert "discovered zero VenueOracle-named tests" in result.stderr + result.stdout
    assert not any(
        line.startswith("packages=") for line in result.stdout.splitlines()
    ), "an empty discovery must not print a package list"
    plan_steps = _jobs()["venue-oracles-plan"]["steps"]
    listing = next(
        step for step in plan_steps if "venue-oracle-packages" in step.get("run", "")
    )
    assert "set -euo pipefail" in listing["run"]
