"""The post-upgrade routing-repoint hook waits for query-api's rollout before it succeeds.

A post-upgrade hook runs when the Deployment is updated, not when its rollout has finished.
On rev 197 the hook ran while /buildinfo still answered with the OLD build: the repoint wrote
nothing (running_build old, changed=0), the hook still succeeded, and the routing rows stayed
on the old build until an operator repointed by hand. During a rolling update old and new pods
serve side by side, so the repoint itself carries the build cross-check (-expect-build, which
refuses without writing while the running build differs) and is retried under a bounded number
of attempts, failing the hook if it never succeeds.

These tests render the Job (with small bounds), feed the script the env the RENDERED Job
declares, and run it under `sh` with a stubbed `dho`, so both the control flow and the wiring
between the chart values and the script are executed, not read.
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


def _rendered_env(*sets: str) -> dict[str, str]:
    """The env the rendered repoint Job's container declares (literal values only)."""
    job = _jobs(*_ENABLED, *sets)[_REPOINT]
    container = job["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e["value"] for e in container["env"] if "value" in e}


def _run(
    tmp_path: Path,
    *,
    ready_on_call: int,
    max_attempts: int,
    version_body: str | None = None,
    token_valid_calls: int | None = None,
    always_401: bool = False,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """ready_on_call: the 1-based repoint call that finally succeeds (0 = never).

    token_valid_calls: a minted routing envelope is honoured for this many repoint calls
    after it was minted, then refused with an HTTP 401 (the real envelope lives 60 seconds,
    shorter than the retry window; calls stand in for seconds so the test needs no sleeps).
    always_401: every repoint is refused with a 401, whatever the credential."""
    version_body = version_body or f'{{"service":"dho","commit":"{_COMMIT}"}}'
    stub_bin = tmp_path / "bin"
    stub_bin.mkdir()
    calls = tmp_path / "calls.txt"
    counter = tmp_path / "repoints"
    counter.write_text("0")
    dho = stub_bin / "dho"
    dho.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "$*" >> {calls}\n'
        'case "$*" in\n'
        f'  "mint envelope"*) echo "tok-$(cat {counter})" ;;\n'
        f"  version) echo '{version_body}' ;;\n"
        '  *"routing repoint"*)\n'
        f"    n=$(( $(cat {counter}) + 1 )); echo $n > {counter}\n"
        f"    minted_at=${{GO_API_ROUTING_BEARER#tok-}}\n"
        f"    unauthorized='go-api-routing: refused: query-api rejected the effective-principal envelope credential (HTTP 401)'\n"
        f'    [ {int(always_401)} -eq 1 ] && {{ echo "$unauthorized" >&2; exit 1; }}\n'
        + (
            f'    [ $(( n - 1 - minted_at )) -ge {token_valid_calls} ] && {{ echo "$unauthorized" >&2; exit 1; }}\n'
            if token_valid_calls is not None
            else ""
        )
        + f'    [ {ready_on_call} -ne 0 ] && [ "$n" -ge {ready_on_call} ] && exit 0\n'
        "    echo 'refused: cross-check does not match the running build' >&2; exit 1 ;;\n"
        "esac\n"
        "exit 0\n"
    )
    dho.chmod(dho.stat().st_mode | stat.S_IEXEC)
    sets = (
        f"migrations.hook.goApiRoutingTools.repointMaxAttempts={max_attempts}",
        "migrations.hook.goApiRoutingTools.repointRetryIntervalSeconds=0",
    )
    script = _script(_jobs(*_ENABLED, *sets)[_REPOINT])
    harness = tmp_path / "harness.sh"
    harness.write_text("#!/bin/sh\n" + script + "\n")
    env = _rendered_env(*sets)
    env["PATH"] = f"{stub_bin}:/usr/bin:/bin"
    proc = subprocess.run(
        ["sh", str(harness)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=env,
    )
    return proc, calls.read_text().splitlines()


def _repoints(log: list[str]) -> list[str]:
    return [line for line in log if "routing repoint" in line]


def test_the_rendered_job_declares_the_retry_bound_the_script_reads() -> None:
    env = _rendered_env()
    assert env["REPOINT_MAX_ATTEMPTS"] == "90", env
    assert env["REPOINT_RETRY_INTERVAL_SECONDS"] == "10", env
    tuned = _rendered_env(
        "migrations.hook.goApiRoutingTools.repointMaxAttempts=7",
        "migrations.hook.goApiRoutingTools.repointRetryIntervalSeconds=3",
    )
    assert (
        tuned["REPOINT_MAX_ATTEMPTS"],
        tuned["REPOINT_RETRY_INTERVAL_SECONDS"],
    ) == ("7", "3"), tuned


def test_retries_the_guarded_repoint_until_it_succeeds(tmp_path: Path) -> None:
    proc, log = _run(tmp_path, ready_on_call=3, max_attempts=10)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    repoints = _repoints(log)
    assert len(repoints) == 3, repoints
    assert all(f"-expect-build {_COMMIT}" in call for call in repoints), repoints
    assert not any("-dry-run" in call for call in repoints), repoints
    assert "waiting for query-api to report build" in proc.stdout


def test_a_refusal_after_an_earlier_success_still_retries(tmp_path: Path) -> None:
    """During a rolling update the next call can reach an old pod even after a call reached the
    new one; the repoint is retried rather than failing on a single refusal."""
    proc, log = _run(tmp_path, ready_on_call=2, max_attempts=5)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert len(_repoints(log)) == 2, log


def test_fails_the_hook_when_the_build_never_answers(tmp_path: Path) -> None:
    proc, log = _run(tmp_path, ready_on_call=0, max_attempts=3)
    assert proc.returncode != 0, (proc.stdout, proc.stderr)
    assert len(_repoints(log)) == 3, log
    assert "never reported build" in proc.stderr, proc.stderr


def test_an_image_with_no_commit_fails_before_any_repoint(tmp_path: Path) -> None:
    proc, log = _run(
        tmp_path,
        ready_on_call=1,
        max_attempts=3,
        version_body='{"service":"dho","commit":"unknown"}',
    )
    assert proc.returncode != 0, (proc.stdout, proc.stderr)
    assert not _repoints(log), log
    assert "reports no 40-hex commit" in proc.stderr, proc.stderr


def test_a_short_lived_credential_is_minted_again_for_every_attempt(
    tmp_path: Path,
) -> None:
    """The routing envelope lives 60 seconds and the retry window is minutes: a token minted
    once expires mid-wait and every later attempt is refused with a 401, so the hook reported
    "rollout did not finish" while the expected build was serving. Here a token is honoured
    for 2 repoint calls; the build answers on call 5. Minting once fails; minting per attempt
    succeeds."""
    proc, log = _run(tmp_path, ready_on_call=5, max_attempts=10, token_valid_calls=2)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    mints = [line for line in log if line.startswith("mint envelope")]
    assert len(_repoints(log)) == 5 and len(mints) == 5, log


def test_a_401_is_reported_as_an_authentication_failure_and_not_retried(
    tmp_path: Path,
) -> None:
    proc, log = _run(tmp_path, ready_on_call=0, max_attempts=10, always_401=True)
    assert proc.returncode != 0, (proc.stdout, proc.stderr)
    assert len(_repoints(log)) == 1, log
    assert "HTTP 401" in proc.stderr and "authentication failure" in proc.stderr
    assert "never reported build" not in proc.stderr, proc.stderr
