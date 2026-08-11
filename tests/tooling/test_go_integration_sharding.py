"""Contracts for deterministic Go storage-integration CI sharding."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"
CHECK_GO = ROOT / "ci" / "check_go.sh"
MANIFEST = ROOT / "ci" / "go_integration_shards.tsv"

EXPECTED_PACKAGES = {
    "cmd/dev-health-worker",
    "cmd/dev-health-workerctl",
    "internal/externalrecompute",
    "internal/joboperator",
    "internal/joboutbox",
    "internal/jobroute",
    "internal/jobruntime",
    "internal/jobs/metrics/daily",
    "internal/jobs/metrics/remaining",
    "internal/jobs/pagerduty",
    "internal/jobs/report",
    "internal/jobs/system",
    "internal/jobs/workgraph",
    "internal/providerfoundation",
    "internal/providersync",
    "internal/scheduler/fixed",
    "internal/scheduler/sync",
    "internal/storage/postgres",
    "internal/storage/river",
    "internal/streamrunner",
    "internal/syncdispatchruntime",
    "internal/syncreconciler",
    "internal/syncroute",
    "internal/testsupport/containers",
}


def _run_check_go(
    *args: str,
    manifest: Path = MANIFEST,
    github_output: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST"] = str(manifest)
    if github_output is not None:
        env["GITHUB_OUTPUT"] = str(github_output)
    return subprocess.run(
        ["bash", "ci/check_go.sh", *args],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_shard_plan_is_exhaustive_nonempty_and_machine_readable(
    tmp_path: Path,
) -> None:
    github_output = tmp_path / "github-output"
    result = _run_check_go("integration-shard-plan", github_output=github_output)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "24 package(s) discovered, 0 denylisted, 24 will run" in result.stdout
    assert "integration shard plan: 3 shard(s), 24 package(s)" in result.stdout

    output = dict(
        line.split("=", maxsplit=1)
        for line in github_output.read_text(encoding="utf-8").splitlines()
    )
    assert json.loads(output["matrix"]) == {
        "include": [{"shard": 1}, {"shard": 2}, {"shard": 3}]
    }

    assignments: dict[int, set[str]] = {}
    for line in result.stdout.splitlines():
        if not line.startswith("  SHARD "):
            continue
        _, shard, package, _weight = line.split()
        assignments.setdefault(int(shard), set()).add(package)

    assert set(assignments) == {1, 2, 3}
    flattened = [package for packages in assignments.values() for package in packages]
    assert len(flattened) == len(set(flattened)) == 24
    assert set(flattened) == EXPECTED_PACKAGES
    assert assignments[1] == {"internal/providersync"}

    estimated = {
        int(match.group("shard")): int(match.group("seconds"))
        for line in result.stdout.splitlines()
        if (
            match := re.fullmatch(
                r"integration shard (?P<shard>\d+): estimated "
                r"(?P<seconds>\d+)s, \d+ package\(s\)",
                line,
            )
        )
    }
    assert set(estimated) == {1, 2, 3}
    assert abs(estimated[2] - estimated[3]) <= 1


def test_each_shard_dry_run_executes_only_its_manifest_assignment() -> None:
    selected: list[str] = []
    for shard in (1, 2, 3):
        result = _run_check_go("integration-shard", str(shard), "--dry-run")
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"integration shard {shard}: DRY RUN" in result.stdout
        selected.extend(
            line.removeprefix("  SHARD-RUN ")
            for line in result.stdout.splitlines()
            if line.startswith("  SHARD-RUN ")
        )

    assert len(selected) == len(set(selected)) == 24
    assert set(selected) == EXPECTED_PACKAGES


def test_manifest_drift_and_duplicate_packages_fail_loudly(tmp_path: Path) -> None:
    original = MANIFEST.read_text(encoding="utf-8")

    missing = tmp_path / "missing.tsv"
    missing.write_text(
        "\n".join(
            line
            for line in original.splitlines()
            if not line.startswith("internal/providersync\t")
        )
        + "\n",
        encoding="utf-8",
    )
    missing_result = _run_check_go("integration-shard-plan", manifest=missing)
    assert missing_result.returncode == 2
    assert "manifest is missing discovered package 'internal/providersync'" in (
        missing_result.stderr
    )

    duplicate = tmp_path / "duplicate.tsv"
    duplicate.write_text(original + "internal/providersync\t1166\n", encoding="utf-8")
    duplicate_result = _run_check_go("integration-shard-plan", manifest=duplicate)
    assert duplicate_result.returncode == 2
    assert "manifest lists 'internal/providersync' more than once" in (
        duplicate_result.stderr
    )


def test_workflow_runs_all_shards_and_preserves_required_check_name() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]

    planner = jobs["go-storage-integration-plan"]
    assert planner["outputs"]["matrix"] == "${{ steps.plan.outputs.matrix }}"
    assert any(
        step.get("id") == "plan"
        and step.get("run") == "bash ci/check_go.sh integration-shard-plan"
        for step in planner["steps"]
    )

    shards = jobs["go-storage-integration-shard"]
    assert shards["needs"] == "go-storage-integration-plan"
    assert shards["timeout-minutes"] == 25
    assert shards["strategy"]["fail-fast"] is False
    assert shards["strategy"]["matrix"] == (
        "${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"
    )
    shard_commands = [str(step.get("run", "")) for step in shards["steps"]]
    assert 'bash ci/check_go.sh integration-shard "${{ matrix.shard }}"' in (
        shard_commands
    )
    assert all("--dry-run" not in command for command in shard_commands)

    for job in (planner, shards):
        go_setup = next(
            step
            for step in job["steps"]
            if str(step.get("uses", "")).startswith("actions/setup-go@")
        )
        assert go_setup["uses"] == (
            "actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e"
        )
        assert go_setup["with"] == {
            "go-version-file": "go.mod",
            "cache-dependency-path": "**/go.sum",
        }
    assert re.search(
        r"(?m)^go 1\.25\.9$", (ROOT / "go.mod").read_text(encoding="utf-8")
    )

    aggregate = jobs["go-storage-integration"]
    assert aggregate["name"] == "go-storage-integration"
    assert aggregate["if"] == "${{ always() }}"
    assert set(aggregate["needs"]) == {
        "go-storage-integration-plan",
        "go-storage-integration-shard",
    }
    assert aggregate["env"] == {
        "PLAN_RESULT": "${{ needs.go-storage-integration-plan.result }}",
        "SHARD_RESULT": "${{ needs.go-storage-integration-shard.result }}",
    }
    aggregate_command = "\n".join(
        str(step.get("run", "")) for step in aggregate["steps"]
    )
    assert (
        'if [ "${PLAN_RESULT}" != "success" ] '
        '|| [ "${SHARD_RESULT}" != "success" ]; then'
    ) in aggregate_command
    assert "exit 1" in aggregate_command

    workflow_source = WORKFLOW.read_text(encoding="utf-8")
    assert workflow_source.count("- 'ci/go_integration_shards.tsv'") == 2

    check_go_source = CHECK_GO.read_text(encoding="utf-8")
    assert 'GO_TOOLCHAIN="go1.25.9"' in check_go_source
    assert 'export GOTOOLCHAIN="${GO_TOOLCHAIN}"' in check_go_source
    assert 'export GOCACHE="${DEV_HEALTH_GO_CACHE}"' in check_go_source
    assert (
        "GOWORK=off go test -mod=readonly -tags=integration -count=1 "
        '-timeout=30m "${run_pkgs[@]}"'
    ) in check_go_source
