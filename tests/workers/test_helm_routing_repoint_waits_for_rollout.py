"""The post-upgrade routing-repoint hook waits for query-api's rollout before it repoints.

A post-upgrade hook runs when the Deployment is updated, not when its rollout has finished.
On rev 197 the hook ran while /buildinfo still answered with the OLD build: the repoint wrote
nothing (running_build old, changed=0), the hook still succeeded, and the routing rows stayed
on the old build until an operator repointed by hand. The hook now probes with a dry run that
cross-checks its own image's commit (-expect-build) and retries a bounded number of times,
failing if the new build never answers. These tests run the rendered script under bash with a
stubbed `dho`, so the control flow is executed, not read.
"""

from __future__ import annotations

import stat
import subprocess
from pathlib import Path

from test_helm_routing_carry_hooks import (  # type: ignore[import-not-found]
    _ENABLED,
    _REPOINT,
    _jobs,
    _script,
)

_COMMIT = "0123456789abcdef0123456789abcdef01234567"


def _run(
    tmp_path: Path,
    *,
    ready_on_probe: int,
    max_attempts: int,
    version_body: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """ready_on_probe: the 1-based dry-run probe that finally succeeds (0 = never)."""
    version_body = version_body or f'{{"service":"dho","commit":"{_COMMIT}"}}'
    stub_bin = tmp_path / "bin"
    stub_bin.mkdir()
    calls = tmp_path / "calls.txt"
    probes = tmp_path / "probes"
    probes.write_text("0")
    dho = stub_bin / "dho"
    dho.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "$*" >> {calls}\n'
        'case "$*" in\n'
        '  "mint envelope"*) echo fake-bearer ;;\n'
        f"  version) echo '{version_body}' ;;\n"
        '  *"routing repoint"*)\n'
        '    case "$*" in *" -dry-run"*)\n'
        f"      n=$(( $(cat {probes}) + 1 )); echo $n > {probes}\n"
        f'      [ {ready_on_probe} -ne 0 ] && [ "$n" -ge {ready_on_probe} ] && exit 0\n'
        "      echo 'refused: cross-check does not match the running build' >&2; exit 1 ;;\n"
        "    esac ;;\n"
        "esac\n"
        "exit 0\n"
    )
    dho.chmod(dho.stat().st_mode | stat.S_IEXEC)
    script = _script(_jobs(*_ENABLED)[_REPOINT])
    harness = tmp_path / "harness.sh"
    harness.write_text("#!/bin/sh\n" + script + "\n")
    proc = subprocess.run(
        ["sh", str(harness)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env={
            "PATH": f"{stub_bin}:/usr/bin:/bin",
            "MINT_ORG": "test-org",
            "QUERY_API_URL": "http://query-api:8090",
            "RELEASE_NAME": "test-release",
            "REPOINT_MAX_ATTEMPTS": str(max_attempts),
            "REPOINT_RETRY_INTERVAL_SECONDS": "0",
        },
    )
    return proc, calls.read_text().splitlines()


def _repoints(log: list[str]) -> list[str]:
    return [line for line in log if "routing repoint" in line]


def test_waits_for_the_new_build_then_repoints_once(tmp_path: Path) -> None:
    proc, log = _run(tmp_path, ready_on_probe=3, max_attempts=10)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    repoints = _repoints(log)
    probes = [call for call in repoints if "-dry-run" in call]
    real = [call for call in repoints if "-dry-run" not in call]
    assert len(probes) == 3, repoints
    assert len(real) == 1, repoints
    assert repoints.index(real[0]) == len(repoints) - 1, "the write must come last"
    assert all(f"-expect-build {_COMMIT}" in call for call in repoints), repoints
    assert "waiting for query-api to report build" in proc.stdout


def test_fails_the_hook_and_never_repoints_when_the_build_never_answers(
    tmp_path: Path,
) -> None:
    proc, log = _run(tmp_path, ready_on_probe=0, max_attempts=3)
    assert proc.returncode != 0, (proc.stdout, proc.stderr)
    repoints = _repoints(log)
    assert len(repoints) == 3 and all("-dry-run" in call for call in repoints), repoints
    assert "never reported build" in proc.stderr, proc.stderr


def test_an_image_with_no_commit_fails_before_any_repoint(tmp_path: Path) -> None:
    proc, log = _run(
        tmp_path,
        ready_on_probe=1,
        max_attempts=3,
        version_body='{"service":"dho","commit":"unknown"}',
    )
    assert proc.returncode != 0, (proc.stdout, proc.stderr)
    assert not _repoints(log), log
    assert "reports no 40-hex commit" in proc.stderr, proc.stderr
