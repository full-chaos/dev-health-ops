"""Contracts for deterministic Go storage-integration CI sharding."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"
CHECK_GO = ROOT / "ci" / "check_go.sh"
MANIFEST = ROOT / "ci" / "go_integration_shards.tsv"
PROVIDER_MANIFEST = ROOT / "ci" / "go_providersync_test_shards.tsv"
PROVIDER_PACKAGE = "internal/providersync"
CONTAINER_HARNESS = ROOT / "internal" / "testsupport" / "containers" / "harness.go"
TEST_GO_CACHE = Path(tempfile.gettempdir()) / "chaos3141-go-sharding-test-cache"
# Hosted job 93890967576 measured 49.596s for a cold planner invocation. This
# test-process guard is deliberately above twice that observed completion; it
# does not change the workflow job cap or the Go test timeout.
CHECK_GO_TIMEOUT_SECONDS = 120

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
    "internal/synccoverage",
    "internal/syncroute",
    "internal/testsupport/containers",
}


def _run_check_go(
    *args: str,
    manifest: Path = MANIFEST,
    provider_manifest: Path = PROVIDER_MANIFEST,
    github_output: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST"] = str(manifest)
    env["DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST"] = str(provider_manifest)
    env["DEV_HEALTH_GO_CACHE"] = str(TEST_GO_CACHE)
    if github_output is not None:
        env["GITHUB_OUTPUT"] = str(github_output)
    return subprocess.run(
        ["bash", "ci/check_go.sh", *args],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=CHECK_GO_TIMEOUT_SECONDS,
    )


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _pinned_clickhouse_image() -> str:
    match = re.search(
        r'(?m)^\s*ClickHouseImage\s*=\s*"(?P<image>[^"]+)"',
        CONTAINER_HARNESS.read_text(encoding="utf-8"),
    )
    assert match is not None
    image = match.group("image")
    assert re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", image)
    return image


def test_integration_shard_arity_guard_uses_an_explicit_conditional() -> None:
    """Keep the public CLI guard compatible with the hosted ShellCheck gate."""

    source = CHECK_GO.read_text(encoding="utf-8")
    match = re.search(
        r"(?ms)^  integration-shard\)\n(?P<body>.*?^    ;;)$",
        source,
    )
    assert match is not None
    assert (
        'if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then\n'
        '      die "integration-shard requires TARGET SHARD and accepts only '
        'optional --dry-run"\n'
        "    fi"
    ) in match.group("body")

    for args in (("packages",), ("packages", "2", "--dry-run", "extra")):
        result = _run_check_go("integration-shard", *args)
        assert result.returncode == 2
        assert (
            "integration-shard requires TARGET SHARD and accepts only optional "
            "--dry-run" in result.stderr
        )


def _providersync_top_level_tests() -> set[str]:
    env = os.environ.copy()
    env["GOTOOLCHAIN"] = "go1.25.9"
    env["GOWORK"] = "off"
    # Reuse the production subprocess cache. The previous distinct cache made
    # this independent oracle pay for a second cold compile in the same test.
    env["GOCACHE"] = str(TEST_GO_CACHE)
    result = subprocess.run(
        [
            "go",
            "test",
            "-mod=readonly",
            "-tags=integration",
            "-count=1",
            "-run=^$",
            "-list=^Test",
            f"./{PROVIDER_PACKAGE}",
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=CHECK_GO_TIMEOUT_SECONDS,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    candidates = [
        line for line in result.stdout.splitlines() if line.startswith("Test")
    ]
    assert all(re.fullmatch(r"Test[A-Za-z0-9_]+", line) for line in candidates)
    return set(candidates)


def _providersync_integration_tagged_tests() -> set[str]:
    tests: set[str] = set()
    for path in ROOT.joinpath(PROVIDER_PACKAGE).glob("*_test.go"):
        source = path.read_text(encoding="utf-8")
        if not re.search(r"(?m)^//go:build.*\bintegration\b", source):
            continue
        tests.update(re.findall(r"(?m)^func (Test[A-Za-z0-9_]+)", source))
    return tests


def test_shard_plan_is_exhaustive_nonempty_and_machine_readable(
    tmp_path: Path,
) -> None:
    github_output = tmp_path / "github-output"
    result = _run_check_go("integration-shard-plan", github_output=github_output)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "25 package(s) discovered, 0 denylisted, 25 will run" in result.stdout
    assert "integration shard plan: 3 shard(s), 25 package(s)" in result.stdout

    output = dict(
        line.split("=", maxsplit=1)
        for line in github_output.read_text(encoding="utf-8").splitlines()
    )
    matrix = json.loads(output["matrix"])["include"]
    assert {(entry["target"], entry["shard"]) for entry in matrix} == {
        ("providersync", 1),
        ("providersync", 2),
        ("providersync", 3),
        ("providersync", 4),
        ("packages", 2),
        ("packages", 3),
    }
    assert len(matrix) == 6

    assignments: dict[int, set[str]] = {}
    for line in result.stdout.splitlines():
        if not line.startswith("  SHARD "):
            continue
        _, shard, package, _weight = line.split()
        assignments.setdefault(int(shard), set()).add(package)

    assert set(assignments) == {1, 2, 3}
    flattened = [package for packages in assignments.values() for package in packages]
    assert len(flattened) == len(set(flattened)) == 25
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

    expected_provider_tests = _providersync_top_level_tests()
    expected_integration_tests = _providersync_integration_tagged_tests()
    assert len(expected_provider_tests) == 891
    assert len(expected_integration_tests) == 103
    assert expected_integration_tests < expected_provider_tests

    provider_assignments: dict[int, set[str]] = {}
    provider_class: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if not line.startswith("  PROVIDER-SHARD "):
            continue
        _label, shard, test_name, _weight, classification = line.split()
        provider_assignments.setdefault(int(shard), set()).add(test_name)
        provider_class[test_name] = classification.removeprefix("class=")

    assert set(provider_assignments) == {1, 2, 3, 4}
    provider_flattened = [
        test_name for tests in provider_assignments.values() for test_name in tests
    ]
    assert len(provider_flattened) == len(set(provider_flattened)) == 891
    assert set(provider_flattened) == expected_provider_tests
    assert {
        name
        for name, classification in provider_class.items()
        if classification == "integration"
    } == expected_integration_tests
    assert set(provider_class.values()) == {"integration", "ordinary"}

    provider_totals: dict[int, int] = {}
    provider_integration_counts: dict[int, int] = {}
    for line in result.stdout.splitlines():
        match = re.fullmatch(
            r"providersync test shard (?P<shard>\d+): relative weight "
            r"(?P<weight>\d+), (?P<count>\d+) test\(s\), "
            r"(?P<integration>\d+) integration-tagged",
            line,
        )
        if match:
            provider_totals[int(match.group("shard"))] = int(match.group("weight"))
            provider_integration_counts[int(match.group("shard"))] = int(
                match.group("integration")
            )
    assert set(provider_totals) == {1, 2, 3, 4}
    assert max(provider_totals.values()) - min(provider_totals.values()) <= 1
    assert (
        max(provider_integration_counts.values())
        - min(provider_integration_counts.values())
        <= 1
    )


def test_each_shard_dry_run_executes_only_its_manifest_assignment() -> None:
    selected_packages: list[str] = []
    for shard in (2, 3):
        result = _run_check_go("integration-shard", "packages", str(shard), "--dry-run")
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"integration package shard {shard}: DRY RUN" in result.stdout
        selected_packages.extend(
            line.removeprefix("  SHARD-RUN ")
            for line in result.stdout.splitlines()
            if line.startswith("  SHARD-RUN ")
        )

    assert len(selected_packages) == len(set(selected_packages)) == 24
    assert set(selected_packages) == EXPECTED_PACKAGES - {PROVIDER_PACKAGE}

    selected_tests: list[str] = []
    for shard in (1, 2, 3, 4):
        result = _run_check_go(
            "integration-shard", "providersync", str(shard), "--dry-run"
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"providersync test shard {shard}: DRY RUN" in result.stdout
        selected_tests.extend(
            line.removeprefix("  PROVIDER-TEST-RUN ")
            for line in result.stdout.splitlines()
            if line.startswith("  PROVIDER-TEST-RUN ")
        )

    expected_tests = _providersync_top_level_tests()
    assert len(selected_tests) == len(set(selected_tests)) == 891
    assert set(selected_tests) == expected_tests


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

    invalid_provider = tmp_path / "invalid-provider.tsv"
    invalid_provider.write_text(
        "shards\t1\nintegration-test-weight\t100\nordinary-test-weight\t1\n",
        encoding="utf-8",
    )
    invalid_provider_result = _run_check_go(
        "integration-shard-plan", provider_manifest=invalid_provider
    )
    assert invalid_provider_result.returncode == 2
    assert "provider test shard manifest must declare at least two shards" in (
        invalid_provider_result.stderr
    )


def test_clickhouse_prepull_retries_the_exact_source_pinned_image(
    tmp_path: Path,
) -> None:
    attempts = tmp_path / "attempts"
    docker_args = tmp_path / "docker-args"
    sleep_args = tmp_path / "sleep-args"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
attempt=0
if [ -f "${DOCKER_ATTEMPTS_FILE}" ]; then
  attempt="$(<"${DOCKER_ATTEMPTS_FILE}")"
fi
attempt=$((attempt + 1))
printf '%s\n' "${attempt}" > "${DOCKER_ATTEMPTS_FILE}"
printf '%s\n' "$*" >> "${DOCKER_ARGS_FILE}"
[ "${attempt}" -ge "${DOCKER_SUCCEED_ON}" ]
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    sleep = bin_dir / "sleep"
    sleep.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${SLEEP_ARGS_FILE}"
""",
        encoding="utf-8",
    )
    sleep.chmod(0o755)

    image = _pinned_clickhouse_image()
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "DOCKER_ATTEMPTS_FILE": str(attempts),
            "DOCKER_ARGS_FILE": str(docker_args),
            "DOCKER_SUCCEED_ON": "3",
            "SLEEP_ARGS_FILE": str(sleep_args),
        }
    )
    result = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-prepull"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert attempts.read_text(encoding="utf-8") == "3\n"
    assert docker_args.read_text(encoding="utf-8").splitlines() == [
        f"pull {image}",
        f"pull {image}",
        f"pull {image}",
    ]
    assert sleep_args.read_text(encoding="utf-8").splitlines() == ["5", "10"]
    assert f"pre-pulled pinned ClickHouse image {image} on attempt 3/3" in (
        result.stdout
    )

    attempts.unlink()
    docker_args.unlink()
    sleep_args.unlink()
    env["DOCKER_SUCCEED_ON"] = "4"
    failed = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-prepull"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert failed.returncode == 1
    assert attempts.read_text(encoding="utf-8") == "3\n"
    assert docker_args.read_text(encoding="utf-8").splitlines() == [
        f"pull {image}",
        f"pull {image}",
        f"pull {image}",
    ]
    assert sleep_args.read_text(encoding="utf-8").splitlines() == ["5", "10"]
    assert f"failed to pre-pull pinned ClickHouse image {image} after 3 attempts" in (
        failed.stderr
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
    assert "bash ci/check_go.sh integration-prepull" in shard_commands
    assert (
        'bash ci/check_go.sh integration-shard "${{ matrix.target }}" '
        '"${{ matrix.shard }}"'
    ) in shard_commands
    assert shard_commands.index("bash ci/check_go.sh integration-prepull") < (
        shard_commands.index(
            'bash ci/check_go.sh integration-shard "${{ matrix.target }}" '
            '"${{ matrix.shard }}"'
        )
    )
    assert all("--dry-run" not in command for command in shard_commands)
    assert shards["name"] == (
        "go-storage-integration-shard-${{ matrix.target }}-${{ matrix.shard }}"
    )

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
    assert workflow_source.count("- 'ci/go_providersync_test_shards.tsv'") == 2

    check_go_source = CHECK_GO.read_text(encoding="utf-8")
    assert 'GO_TOOLCHAIN="go1.25.9"' in check_go_source
    assert 'export GOTOOLCHAIN="${GO_TOOLCHAIN}"' in check_go_source
    assert 'export GOCACHE="${DEV_HEALTH_GO_CACHE}"' in check_go_source
    assert (
        "GOWORK=off go test -mod=readonly -tags=integration -count=1 "
        '-timeout=30m "${run_pkgs[@]}"'
    ) in check_go_source
    assert (
        "GOWORK=off go test -mod=readonly -tags=integration -count=1 "
        '-timeout=30m -run "${test_regex}" ./internal/providersync'
    ) in check_go_source
