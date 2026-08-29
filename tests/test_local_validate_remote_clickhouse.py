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


def _curl_endpoints(recorded: str) -> list[str]:
    """Every URL the curl stub was invoked with, as whole arguments.

    Tests compare this by EQUALITY, never by containment. Two reasons, and the
    second is why the helper returns a list instead of a bool:

    1. `"https://host:8443/" in argv` also matches
       `https://host:8443/@evil.example.com` -- everything before the `@` is
       userinfo, so the real authority is the attacker's host. CodeQL flags this
       shape (py/incomplete-url-substring-sanitization) and is right to.
    2. Containment cannot see an EXTRA endpoint. Equality against the full list
       fails if ch_query() ever also calls out to somewhere unexpected, which
       containment would happily ignore.

    Splitting on whitespace is exact because the stub records "$*".
    """
    return [token for token in recorded.split() if "://" in token]


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

    # The curl stub records argv AND stdin, so the test can assert what was
    # actually sent -- a stub that swallowed everything would let a ch_query()
    # that became a no-op, or hit the wrong endpoint, pass the docker-absence
    # check silently (codex review; the same false-pass family that already
    # bit this file once).
    curl_log = tmp_path / "curl-argv"
    curl = bindir / "curl"
    curl.write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$*" >> "{curl_log}"\nexit 0\n',
        encoding="utf-8",
    )
    curl.chmod(0o755)
    return bindir


def _run(
    script: str, env: dict[str, str], *, unset: tuple[str, ...] = ()
) -> subprocess.CompletedProcess[str]:
    merged = {**os.environ, **env}
    for name in unset:
        merged.pop(name, None)
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env=merged,
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
            # Pinned, not merely unset (codex R3): running the gate itself with
            # CH_HTTP_SCHEME=https exports it into pytest, the probe would then
            # record an https:// URL and this case's endpoint assertion would
            # fail on every TLS remote-mode run. Same inherited-env family as
            # the CH_TRANSPORT bug in the docker case below.
            "CH_HTTP_SCHEME": "http",
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

    # Negative alone is not enough: a ch_query() that did nothing would also
    # leave no docker marker. Assert what was actually sent.
    curl_log = tmp_path / "curl-argv"
    assert curl_log.exists(), (
        "ch_query did not invoke curl under CH_TRANSPORT=http -- it did nothing"
        f"\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    argv = curl_log.read_text(encoding="utf-8")
    assert _curl_endpoints(argv) == ["http://192.0.2.10:30501/"], (
        f"wrong endpoint: {argv}"
    )
    assert "SELECT 1" in argv, f"query body missing: {argv}"
    assert "--noproxy" in argv, (
        "the ClickHouse POST must bypass ambient proxies: a credential "
        f"must never be handed to an unintended proxy. argv: {argv}"
    )

    # The credential must NOT be on the command line. This gate runs on a
    # shared host and curl argv is visible in `ps`; the password can CREATE and
    # DROP databases. An earlier version passed it as --header and a comment
    # wrongly claimed that hid it (codex review round 2). Credentials travel in
    # a private --config file instead.
    assert "secret" not in argv, (
        "the ClickHouse password appeared in curl's command line, where any "
        f"process listing can read it: {argv}"
    )
    assert "--config" in argv, (
        f"credentials must be passed through a private --config file: {argv}"
    )
    assert "ch_query_rc=0" in result.stdout, (
        f"ch_query reported failure: {result.stdout} {result.stderr}"
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_docker_transport_remains_the_default(tmp_path: Path) -> None:
    """The Compose path must be untouched: unset CH_TRANSPORT still uses docker."""
    bindir = _stub_bin(tmp_path)
    marker = tmp_path / "docker-was-called"

    # CH_TRANSPORT must be REMOVED, not merely left unset in this dict: when
    # the gate itself runs in remote mode (`CH_TRANSPORT=http bash
    # ci/local_validate.sh`) that export reaches pytest and would be inherited
    # here, so this test would drive the http path, find no docker marker and
    # fail on every remote-mode gate run (codex review).
    _run(
        _DRIVER % _GATE,
        {
            "PATH": f"{bindir}:{os.environ.get('PATH', '')}",
            "CH_CONTAINER": "dev-health-clickhouse-1",
        },
        unset=("CH_TRANSPORT",),
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


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_credentials_travel_in_a_private_config_file(tmp_path: Path) -> None:
    """The headers must still be SENT -- just not via argv.

    Asserting only "the password is not in argv" would pass against a version
    that stopped authenticating at all, so this captures the --config file curl
    was handed and checks both headers are in it, with owner-only permissions.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir()
    captured = tmp_path / "captured-config"
    curl = bindir / "curl"
    # Copy whatever --config names, plus its octal mode, before curl exits.
    curl.write_text(
        "#!/bin/sh\n"
        "prev=''\n"
        'for a in "$@"; do\n'
        "  if [ \"$prev\" = '--config' ]; then\n"
        f'    cat "$a" > "{captured}"\n'
        f'    ls -l "$a" | cut -c1-10 >> "{captured}"\n'
        "  fi\n"
        '  prev="$a"\n'
        "done\n"
        "exit 0\n",
        encoding="utf-8",
    )
    curl.chmod(0o755)
    docker = bindir / "docker"
    docker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    docker.chmod(0o755)

    _run(
        _DRIVER % _GATE,
        {
            "PATH": f"{bindir}:{os.environ.get('PATH', '')}",
            "CH_TRANSPORT": "http",
            "CH_HTTP_SCHEME": "http",
            "CH_HOST": "192.0.2.10",
            "CH_HTTP_PORT": "30501",
            "CH_USER": "ch",
            "CH_PASS": "secret",
        },
    )

    assert captured.exists(), "curl was never handed a --config file"
    body = captured.read_text(encoding="utf-8")
    assert 'header = "X-ClickHouse-User: ch"' in body, body
    assert 'header = "X-ClickHouse-Key: secret"' in body, body
    assert "-rw-------" in body, f"the credential file must be owner-only: {body}"


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_https_scheme_is_honoured(tmp_path: Path) -> None:
    """A TLS-only ClickHouse must be reachable, not hardcoded to plaintext."""
    bindir = _stub_bin(tmp_path)
    curl_log = tmp_path / "curl-argv"

    _run(
        _DRIVER % _GATE,
        {
            "PATH": f"{bindir}:{os.environ.get('PATH', '')}",
            "CH_TRANSPORT": "http",
            "CH_HTTP_SCHEME": "https",
            "CH_HOST": "clickhouse.example.com",
            "CH_HTTP_PORT": "8443",
            "CH_USER": "ch",
            "CH_PASS": "secret",
        },
    )

    assert curl_log.exists(), "curl was not invoked"
    argv = curl_log.read_text(encoding="utf-8")
    assert _curl_endpoints(argv) == ["https://clickhouse.example.com:8443/"], (
        f"CH_HTTP_SCHEME=https must reach a TLS endpoint: {argv}"
    )
