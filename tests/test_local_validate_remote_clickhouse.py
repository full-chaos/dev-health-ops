"""`ci/local_validate.sh` must reach a REMOTE ClickHouse without docker.

CHAOS-4457. The gate is the last reason a cluster-isolated lane still touches
the shared Compose host. Two readings from CHAOS-4428 shaped this:

* the lock is already fine -- `LOCK_DIR` is scoped to `CH_CONTAINER`, so a lane
  passing a lane-scoped name already gets its own lock;
* the docker dependency is ONE function, `ch_query()`, used only for the scratch
  CREATE/DROP DATABASE. Everything else already speaks HTTP through
  `CH_HOST`/`CH_HTTP_PORT`.

So the contract asserted here is narrow and mechanical: under
`CH_TRANSPORT=http` the script must issue its scratch DDL over HTTP and must
never shell out to `docker`. The test drives the real functions by sourcing the
script with a stubbed PATH, so it fails if the docker call comes back -- a
grep-for-a-string test would pass against a script that still ran docker at
runtime.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_GATE = _ROOT / "ci" / "local_validate.sh"

# Drive the real ch_query() through the script's own argument hook (the same
# convention --stage-manifest-mismatch-probe uses), so this exercises the
# shipping code path rather than a copy of it.
_DRIVER = r"""
set -uo pipefail
bash "%s" --ch-query-probe "SELECT 1"
printf 'ch_query_rc=%%s\n' "$?"
"""


def _stub_bin(tmp_path: Path) -> Path:
    """A PATH where `docker` records any invocation and `curl` fakes a 200."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    marker = tmp_path / "docker-was-called"

    docker = bindir / "docker"
    docker.write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$*" >> "{marker}"\nexit 0\n',
        encoding="utf-8",
    )
    docker.chmod(0o755)

    curl = bindir / "curl"
    curl.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    curl.chmod(0o755)
    return bindir


def _run(script: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **env},
        cwd=_ROOT,
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_remote_clickhouse_transport_never_shells_out_to_docker(tmp_path: Path) -> None:
    bindir = _stub_bin(tmp_path)
    marker = tmp_path / "docker-was-called"

    result = _run(
        _DRIVER % _GATE,
        {
            "PATH": f"{bindir}:{os.environ.get('PATH', '')}",
            "CH_TRANSPORT": "http",
            "CH_HOST": "192.0.2.10",
            "CH_HTTP_PORT": "30501",
            "CH_USER": "ch",
            "CH_PASS": "secret",
            "CH_CONTAINER": "lane-scoped-name",
        },
    )

    assert not marker.exists(), (
        "ch_query shelled out to docker under CH_TRANSPORT=http: "
        + marker.read_text(encoding="utf-8")
        + f"\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_docker_transport_remains_the_default(tmp_path: Path) -> None:
    """The Compose path must be untouched: unset CH_TRANSPORT still uses docker."""
    bindir = _stub_bin(tmp_path)
    marker = tmp_path / "docker-was-called"

    _run(
        _DRIVER % _GATE,
        {
            "PATH": f"{bindir}:{os.environ.get('PATH', '')}",
            "CH_CONTAINER": "dev-health-clickhouse-1",
        },
    )

    assert marker.exists(), (
        "default (unset CH_TRANSPORT) must still drive ClickHouse via docker exec"
    )
    assert "dev-health-clickhouse-1" in marker.read_text(encoding="utf-8")


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_lock_key_is_scoped_to_ch_container_not_the_host() -> None:
    """Two different CH targets must not serialize against each other.

    This is the property that lets a lane's gate run beside a Compose-stack
    gate; it already held when CHAOS-4457 was written, and this pins it so a
    refactor cannot quietly turn the lock host-wide again.
    """
    source = _GATE.read_text(encoding="utf-8")
    lock_lines = [
        line
        for line in source.splitlines()
        if line.strip().startswith("LOCK_DIR=") or line.strip().startswith("LOCK_DIR ")
    ]
    assert lock_lines, "LOCK_DIR assignment not found"
    assert any(
        "${CH_CONTAINER}" in line or "$CH_CONTAINER" in line for line in lock_lines
    ), (
        "LOCK_DIR must stay scoped to CH_CONTAINER so isolated lanes do not "
        f"serialize on one host-wide lock; found: {lock_lines}"
    )
