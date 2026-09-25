"""Multi-package integration `go test` invocations run with the Ryuk reaper off in CI.

WHY THIS TEST EXISTS (CHAOS-6634)
---------------------------------
testcontainers-go derives its session id from the PARENT pid, so every test
binary of one `go test -tags=integration pkgA pkgB ...` invocation shares one
session and one Ryuk reaper. When one package's process exits, Ryuk reaps the
session's containers ~10 s later, including a SIBLING package's live container.
On shard 3 that killed internal/admincli TestSeedMatchesPython's Postgres
mid-test (`unexpected postmaster exit`, `No such container`, then the Python
leg's `Connect call failed`). Reproduced with two throwaway packages in one
`go test -p 2` run: the long-lived package was refused ~10 s after the short
one exited, and survived with TESTCONTAINERS_RYUK_DISABLED=true.

WHAT THIS PINS (executed against ci/check_go.sh with a stand-in `go test`
that records the environment it is given; every other `go` subcommand is the
real one)
1. Under CI=true the integration-shard packages target and the integration
   verb run `go test` with TESTCONTAINERS_RYUK_DISABLED=true.
2. Outside CI the reaper stays on (variable absent): orphan safety on a
   workstation.
3. A value the caller already set wins.
4. Every multi-package `go test -tags=integration` line of check_go.sh carries
   the env, so a new such call site cannot be added without it.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CHECK_GO = ROOT / "ci" / "check_go.sh"

FAKE_GO = """#!/usr/bin/env bash
# Records `go test` calls (env value + package args); delegates everything else.
if [ "$1" != "test" ]; then exec "{real_go}" "$@"; fi
printf '%s|%s\\n' "${{TESTCONTAINERS_RYUK_DISABLED-<unset>}}" "$*" >> "${{FAKE_GO_LOG}}"
exit 0
"""


def _run(tmp_path: Path, *verb: str, env_extra: dict[str, str | None]) -> list[str]:
    real_go = shutil.which("go")
    assert real_go, "go is required: ci/check_go.sh refuses to run without it"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    fake = bin_dir / "go"
    fake.write_text(FAKE_GO.format(real_go=real_go), encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    log = tmp_path / "go_test.log"
    log.write_text("")
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in {"CI", "TESTCONTAINERS_RYUK_DISABLED"}
    }
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["FAKE_GO_LOG"] = str(log)
    env["TMPDIR"] = str(tmp_path)
    for key, value in env_extra.items():
        if value is not None:
            env[key] = value
    proc = subprocess.run(
        ["bash", str(CHECK_GO), *verb],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr[-2000:] + proc.stdout[-2000:]
    return [line for line in log.read_text().splitlines() if line]


def _values(lines: list[str]) -> set[str]:
    return {line.split("|", 1)[0] for line in lines}


def test_shard_run_disables_ryuk_in_ci(tmp_path: Path) -> None:
    lines = _run(
        tmp_path, "integration-shard", "packages", "3", env_extra={"CI": "true"}
    )
    # A measurement that did not happen must fail: the shard ran go test.
    assert lines, "the stand-in go recorded no `go test` call"
    assert _values(lines) == {"true"}, lines
    assert all("-tags=integration" in line for line in lines), lines


def test_shard_run_keeps_ryuk_outside_ci(tmp_path: Path) -> None:
    lines = _run(tmp_path, "integration-shard", "packages", "3", env_extra={})
    assert lines
    assert _values(lines) == {"<unset>"}, lines


def test_a_caller_value_wins(tmp_path: Path) -> None:
    lines = _run(
        tmp_path,
        "integration-shard",
        "packages",
        "3",
        env_extra={"CI": "true", "TESTCONTAINERS_RYUK_DISABLED": "false"},
    )
    assert lines
    assert _values(lines) == {"false"}, lines


def test_the_integration_verb_disables_ryuk_in_ci(tmp_path: Path) -> None:
    lines = _run(tmp_path, "integration", env_extra={"CI": "true"})
    assert lines
    assert _values(lines) == {"true"}, lines


def test_every_multi_package_integration_test_call_carries_the_env() -> None:
    script = CHECK_GO.read_text(encoding="utf-8")
    sites = [
        line
        for line in script.splitlines()
        if re.search(r"\bgo test\b.*-tags=integration", line)
        and '"${run_pkgs[@]}"' in line
    ]
    assert len(sites) >= 2, "expected the shard and integration call sites"
    for line in sites:
        assert "INTEGRATION_TEST_ENV" in line, (
            "a multi-package integration `go test` without INTEGRATION_TEST_ENV "
            f"shares one Ryuk session across packages (CHAOS-6634): {line.strip()}"
        )


@pytest.mark.parametrize("value", ["true"])
def test_the_env_block_is_gated_on_ci(value: str) -> None:
    """The reaper is only disabled where the runner is discarded."""
    script = CHECK_GO.read_text(encoding="utf-8")
    assert '[ "${CI:-}" = "true" ]' in script
    assert f"TESTCONTAINERS_RYUK_DISABLED={value}" in script
