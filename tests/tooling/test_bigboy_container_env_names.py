"""CHAOS-6987 (R462/R463): ci/bigboy/container-env-names.sh prints env var NAMES only.

A container's environment on bigboy carries live credentials; a raw
`docker inspect ... Config.Env` read printed five of them into a lane transcript.
This helper is the only sanctioned container-env reader there, so these tests
plant a sentinel VALUE and assert it never reaches stdout or stderr, on both the
happy path (a real synthetic container, when a docker daemon is reachable) and
the regression path (a stubbed docker that leaks `NAME=value` lines, as a broken
template would).
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "bigboy" / "container-env-names.sh"
SENTINEL = "sentinel-value-" + uuid.uuid4().hex


def _run(
    *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
        timeout=60,
    )


def _stub_docker(tmp_path: Path, body: str) -> dict[str, str]:
    stub = tmp_path / "docker"
    stub.write_text("#!/usr/bin/env bash\n" + body, encoding="utf-8")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    return {"DOCKER": str(stub)}


def test_template_cuts_the_name_inside_docker(tmp_path: Path) -> None:
    """The mechanism: the value is dropped by docker's own template, not by a later pipe."""
    argv_log = tmp_path / "argv"
    env = _stub_docker(
        tmp_path, f'printf "%s\\n" "$@" > {argv_log}\nprintf "A_NAME\\n"\n'
    )
    res = _run("synthetic", env=env)
    assert res.returncode == 0, res.stderr
    argv = argv_log.read_text(encoding="utf-8")
    assert '{{index (split . "=") 0}}' in argv
    assert "--type\ncontainer" in argv
    assert res.stdout == "A_NAME\n"


def test_leaked_value_line_is_withheld_and_fails(tmp_path: Path) -> None:
    """A template regression that emits NAME=value must fail loud and print no value."""
    env = _stub_docker(tmp_path, f'printf "GOOD\\nLEAK={SENTINEL}\\n"\n')
    res = _run("synthetic", env=env)
    assert res.returncode == 3
    assert SENTINEL not in res.stdout + res.stderr
    assert "<non-name-line>" in res.stdout
    assert "GOOD" in res.stdout


def test_inspect_failure_is_named_and_nonzero(tmp_path: Path) -> None:
    env = _stub_docker(tmp_path, f'echo "boom {SENTINEL}" >&2; exit 1\n')
    res = _run("absent-container", env=env)
    assert res.returncode == 2
    assert "absent-container" in res.stderr
    assert SENTINEL not in res.stdout + res.stderr


def test_missing_argument_is_usage_error() -> None:
    res = _run()
    assert res.returncode == 2
    assert "usage" in res.stderr


def _docker_ready() -> bool:
    if shutil.which("docker") is None:
        return False
    probe = subprocess.run(["docker", "info"], capture_output=True, timeout=30)
    return probe.returncode == 0


@pytest.mark.skipif(
    not _docker_ready(),
    reason="no reachable docker daemon: live synthetic-container leg NOT run",
)
def test_real_container_names_only() -> None:
    """End to end against a synthetic created (never started) container."""
    image = next(
        (
            img
            for img in ("busybox:latest", "alpine:latest")
            if subprocess.run(
                ["docker", "image", "inspect", img], capture_output=True
            ).returncode
            == 0
        ),
        "busybox:latest",
    )
    name = "env-names-test-" + uuid.uuid4().hex[:12]
    created = subprocess.run(
        [
            "docker",
            "create",
            "--name",
            name,
            "-e",
            f"SYNTH_SECRET_TOKEN={SENTINEL}",
            "-e",
            f"SYNTH_WITH_EQUALS=a={SENTINEL}=b",
            image,
            "true",
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert created.returncode == 0, created.stderr
    try:
        res = _run(name)
        assert res.returncode == 0, res.stderr
        names = res.stdout.splitlines()
        assert "SYNTH_SECRET_TOKEN" in names
        assert "SYNTH_WITH_EQUALS" in names
        assert names == sorted(names)
        assert SENTINEL not in res.stdout + res.stderr
        assert "=" not in res.stdout
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)
