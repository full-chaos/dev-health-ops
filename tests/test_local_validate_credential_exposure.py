"""The scratch DSN's credential must not reach stdout or any child's argv.

CHAOS-4457, team-lead R4 ruling. `SCRATCH_URI` necessarily carries a
create/drop-capable ClickHouse credential. Under `CH_TRANSPORT=docker` that was
the throwaway local `ch:ch`; under `CH_TRANSPORT=http` it is a REAL cluster
password, and this gate runs on a shared host where process listings and log
scrollback are readable. Two exposure classes are pinned here:

* display — `ch_migrate`/`ch_provision`/`metrics_readback` printed the full URI;
* argv — `fixtures generate --sink <uri>` and
  `assert_metrics_executed_proof.py --clickhouse-uri <uri>` passed it as an
  argument.

The argv fixes are deletions rather than new contracts: both consumers already
resolve `CLICKHOUSE_URI` from the environment, which the gate already exports.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_GATE = _ROOT / "ci" / "local_validate.sh"
_PROOF = _ROOT / "ci" / "assert_metrics_executed_proof.py"

_SECRET = "sup3rsecret"


def _redact(uri: str) -> str:
    """Drive the shipping redact_uri() out of the gate itself."""
    body = re.search(
        r"^redact_uri\(\) \{.*?^\}", _GATE.read_text(encoding="utf-8"), re.S | re.M
    )
    assert body, "redact_uri() not found in the gate"
    return subprocess.run(
        ["bash", "-c", f'{body.group(0)}\nredact_uri "$1"', "_", uri],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
@pytest.mark.parametrize(
    "uri",
    [
        f"clickhouse://ch:{_SECRET}@10.0.0.5:8123/ci_scratch",
        # a password containing '@' -- a non-greedy userinfo strip leaks the
        # remainder as if it were the host, which is how the first attempt failed
        f"clickhouse+https://ch:p@{_SECRET}@host:8443/db",
        f"clickhouse://ch:pa:ss@{_SECRET}@1.2.3.4:9000/db",
    ],
)
def test_redaction_removes_the_credential_but_keeps_the_endpoint(uri: str) -> None:
    out = _redact(uri)
    assert _SECRET not in out, f"credential survived redaction: {out}"
    assert out.startswith(uri.split("://", 1)[0] + "://"), out
    # the operator still needs to know WHICH store was touched
    assert uri.rsplit("/", 1)[-1] in out, f"scratch db name lost: {out}"


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_no_display_site_prints_the_raw_scratch_uri() -> None:
    """Every printf/die that shows the DSN must route through redact_uri()."""
    source = _GATE.read_text(encoding="utf-8").splitlines()
    offenders = []
    for number, line in enumerate(source, start=1):
        if "SCRATCH_URI" not in line:
            continue
        if not any(token in line for token in ("printf", "die ", "die(")):
            continue
        if "redact_uri" in line:
            continue
        offenders.append(f"{number}: {line.strip()}")
    assert not offenders, (
        "these lines display SCRATCH_URI without redaction: " + "; ".join(offenders)
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_no_child_receives_the_scratch_uri_as_an_argument() -> None:
    """argv is world-readable via `ps`; the DSN travels in the environment."""
    source = _GATE.read_text(encoding="utf-8")
    offenders = [
        flag
        for flag in ("--sink", "--clickhouse-uri", "--db ", "--analytics-db")
        if re.search(rf'{re.escape(flag)}\s*"?\$\{{SCRATCH_URI\}}', source)
    ]
    assert not offenders, (
        "these flags pass the credential-bearing SCRATCH_URI in argv, where a "
        f"process listing can read it: {offenders}. Export CLICKHOUSE_URI instead."
    )


@pytest.mark.skipif(not _PROOF.is_file(), reason="proof script missing")
def test_metrics_proof_accepts_the_dsn_from_the_environment() -> None:
    """The argv fix is only safe because the consumer reads the env."""
    source = _PROOF.read_text(encoding="utf-8")
    assert 'os.getenv("CLICKHOUSE_URI")' in source, (
        "--clickhouse-uri must default from CLICKHOUSE_URI, otherwise dropping it "
        "from argv silently passes None"
    )
    assert not re.search(
        r'add_argument\(\s*"--clickhouse-uri",\s*required=True', source
    ), "--clickhouse-uri must not be required once it defaults from the environment"


def _uri_encode(value: str) -> str:
    """Drive the shipping uri_encode() out of the gate itself."""
    body = re.search(
        r"^uri_encode\(\) \{.*?^\}", _GATE.read_text(encoding="utf-8"), re.S | re.M
    )
    assert body, "uri_encode() not found in the gate"
    return subprocess.run(
        ["bash", "-c", f'{body.group(0)}\nuri_encode "$1"', "_", value],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
@pytest.mark.parametrize(
    "credential",
    [
        # encoded: gen-delims, the escape character, whitespace
        "sec/ret",
        "se#cr?et",
        "p@ssw%rd",
        "a:b",
        "sp ace",
        "plain",
        # NOT encoded: RFC 3986 sub-delims are legal raw in userinfo, and both
        # urlsplit and clickhouse-connect take them literally. Encoding these
        # regressed the default docker path, where they had always worked.
        "sec!ret",
        "p$w=1",
        "a,b;c",
        "it's",
    ],
)
def test_userinfo_is_percent_encoded_and_round_trips(credential: str) -> None:
    """The DSN parser must get back exactly the credential curl authenticates with.

    Raw interpolation broke three ways for URI-reserved characters: '/', '?' or
    '#' truncates or re-parses the DSN, so CREATE could succeed over curl (which
    sends the byte-exact header) while the migration that parses this URI went
    somewhere else. The repo's own parser round-trips percent-encoded userinfo --
    tests/metrics/test_clickhouse_connection.py encodes `secret/word` as
    `secret%2Fword` and expects `secret/word` back.
    """
    from urllib.parse import unquote, urlsplit

    encoded = _uri_encode(credential)
    parsed = urlsplit(f"clickhouse://ch:{encoded}@host:8123/db")
    assert parsed.hostname == "host", f"encoding broke the authority: {encoded}"
    assert parsed.port == 8123, f"encoding broke the port: {encoded}"
    assert unquote(parsed.password or "") == credential, (
        f"credential did not round-trip: {credential!r} -> {encoded!r} -> "
        f"{unquote(parsed.password or '')!r}"
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_a_slash_in_the_password_cannot_defeat_redaction() -> None:
    """The two fixes are coupled, so pin the coupling.

    redact_uri() splits the authority at its first '/'. An UNENCODED '/' in the
    password therefore made it print the credential it exists to hide --
    `clickhouse://ch:sec/ret@host:8123/db` redacted to itself. Encoding is what
    makes redaction sound, so a regression in either shows up here.
    """
    encoded = _uri_encode("sec/ret")
    assert "%2F" in encoded, f"'/' must be encoded, got {encoded!r}"
    redacted = _redact(f"clickhouse://ch:{encoded}@host:8123/db")
    assert "sec" not in redacted or "ret" not in redacted, redacted
    assert redacted.strip() == "clickhouse://host:8123/db", redacted


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_remote_clickhouse_consumers_are_proxy_neutralised() -> None:
    """curl is not the only client that talks to the lane's ClickHouse.

    clickhouse-connect honours HTTP_PROXY/HTTPS_PROXY, so `ch_migrate`'s dev-hops
    calls would route the real Basic-auth credential through an ambient proxy --
    the larger half of the exposure that neutralising only curl left open.
    """
    source = _GATE.read_text(encoding="utf-8")
    migrate = re.search(r"^ch_migrate\(\) \{.*?^\}", source, re.S | re.M)
    assert migrate, "ch_migrate() not found"
    body = migrate.group(0)
    invocations = [line for line in body.splitlines() if "${DEVHOPS}" in line]
    assert invocations, "ch_migrate no longer invokes dev-hops -- update this test"
    unguarded = [line.strip() for line in invocations if "PROXY_OFF" not in line]
    assert not unguarded, (
        "these remote-ClickHouse invocations are not proxy-neutralised, so an "
        f"ambient HTTP_PROXY can receive the credential: {unguarded}"
    )


_PROBE = 'bash "%s" --ch-query-probe "SELECT 1"'


def _stub_path(tmp_path: Path) -> str:
    """A PATH with inert curl/docker, so the config guards are what we observe."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    for name in ("curl", "docker"):
        stub = bindir / name
        stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        stub.chmod(0o755)
    return f"{bindir}:{os.environ.get('PATH', '')}"


def _run_gate(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", _PROBE % _GATE],
        capture_output=True,
        text=True,
        env={**os.environ, **env},
        cwd=_ROOT,
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
@pytest.mark.parametrize("credential", ["sec!ret", "p$w=1", "a,b;c", "it's"])
def test_sub_delims_are_left_raw_and_not_refused(
    credential: str, tmp_path: Path
) -> None:
    """Sub-delims must survive untouched, in BOTH transports.

    Before this PR the default docker path passed CH_PASS straight to
    clickhouse-client and interpolated it raw into SCRATCH_URI, so a password
    like `sec!ret` worked end to end. Encoding everything outside the unreserved
    set broke that and then the compatibility guard refused it -- a regression
    introduced by the fix, in the configuration everyone uses. Encoding is now
    limited to what actually breaks the parse, so these stay raw and stay legal.
    """
    assert _uri_encode(credential) == credential, (
        f"{credential!r} must not be encoded: sub-delims are legal raw in "
        "userinfo (RFC 3986 3.2.1) and encoding them regresses the docker path"
    )
    result = _run_gate(
        {
            "PATH": _stub_path(tmp_path),
            "CH_PASS": credential,
        }
    )
    combined = result.stdout + result.stderr
    assert "may not contain" not in combined, (
        f"docker mode refused {credential!r}, which worked before this PR: {combined}"
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"CH_HTTP_SCHEME": "https", "CH_HTTP_PORT": "30501"}, "port 443 or 8443"),
        ({"CH_PASS": "sec/ret"}, "may not contain"),
        ({"CH_USER": "user@host"}, "may not contain"),
    ],
)
def test_unsupported_http_combinations_refuse_up_front(
    env: dict[str, str], expected: str, tmp_path: Path
) -> None:
    """Fail fast, not three stages deep.

    Both limits come from one root: `migrate clickhouse status --check` calls
    clickhouse_connect.get_client(dsn=...) directly (migrate.py:360,461) rather
    than through a repository parser. It infers TLS from the PORT, not the
    `clickhouse+https` scheme, and it does not percent-decode userinfo. So a TLS
    NodePort would silently reconnect in plaintext, and an encoded credential
    would authenticate for curl and the upgrade and then fail at status --
    after two stages had already succeeded. Refusing at startup with the reason
    is the loud fallback.
    """
    result = _run_gate(
        {
            "PATH": _stub_path(tmp_path),
            "CH_TRANSPORT": "http",
            "CH_HOST": "192.0.2.10",
            "CH_HTTP_PORT": "30501",
            "CH_USER": "ch",
            "CH_PASS": "secret",
            **env,
        }
    )
    combined = result.stdout + result.stderr
    assert expected in combined, (
        f"expected a refusal mentioning {expected!r}; got: {combined}"
    )


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_supported_http_combinations_are_not_refused(tmp_path: Path) -> None:
    """The guard must not over-reject: plain http and TLS on 8443 both pass."""
    for env in (
        {"CH_HTTP_SCHEME": "http", "CH_HTTP_PORT": "30501"},
        {"CH_HTTP_SCHEME": "https", "CH_HTTP_PORT": "8443"},
    ):
        result = _run_gate(
            {
                "PATH": _stub_path(tmp_path),
                "CH_TRANSPORT": "http",
                "CH_HOST": "192.0.2.10",
                "CH_USER": "ch",
                "CH_PASS": "secret",
                **env,
            }
        )
        combined = result.stdout + result.stderr
        assert "only supported on port" not in combined, (env, combined)
        assert "may not contain" not in combined, (env, combined)


@pytest.mark.skipif(not _GATE.is_file(), reason="gate script missing")
def test_non_ascii_credentials_are_refused_rather_than_mis_encoded(
    tmp_path: Path,
) -> None:
    """Pin a known-wrong encoder path behind the guard that hides it.

    `uri_encode` walks CHARACTERS while `${#string}` counts CHARACTERS, so a
    multi-byte character both mis-encodes and shortens the loop: measured,
    `uri_encode "caf\u00e9"` returns `caf%C3` — the second UTF-8 byte is never
    emitted, so the credential is silently TRUNCATED rather than merely encoded
    oddly. Unreachable today because the guard refuses non-ASCII first, which is
    precisely why it would sit unnoticed. This test exists so that the day the
    guard is lifted (CHAOS-4469) the latent bug fails loudly here instead of
    corrupting a password.
    """
    result = _run_gate({"PATH": _stub_path(tmp_path), "CH_PASS": "caf\u00e9"})
    combined = result.stdout + result.stderr
    assert "may not contain" in combined, (
        "non-ASCII must be refused while uri_encode still emits code points "
        f"rather than UTF-8 bytes: {combined}"
    )
    # And the encoder is still broken, on purpose, until CHAOS-4469. Asserting
    # the exact observed output rather than a vague "is wrong": if it ever
    # becomes caf%C3%A9 the encoder was fixed, and the guard's non-ASCII clause
    # and this assertion should be deleted together.
    assert _uri_encode("caf\u00e9") == "caf%C3", (
        "expected the known-truncating output; if this changed, uri_encode was "
        "fixed — remove the guard's non-ASCII clause and this assertion"
    )


@pytest.mark.skipif(not _PROOF.is_file(), reason="proof script missing")
def test_metrics_proof_requires_a_dsn_from_flag_or_env() -> None:
    """Defaulting from the env must not degrade into silently passing None.

    Dropping --clickhouse-uri from the gate's argv was only safe because the
    script reads CLICKHOUSE_URI. With neither set it previously proceeded and
    failed deep inside the sink, where the error names something else entirely.
    """
    # PYTHONPATH=src exactly as the gate invokes it -- this script imports
    # dev_health_ops at module scope, and a worktree venv built with
    # `uv sync --no-install-project` (the CHAOS-4181 workaround) has no
    # installed package, so without it the run dies at import before argparse
    # is ever reached and the test would be asserting the wrong failure.
    env = {k: v for k, v in os.environ.items() if k != "CLICKHOUSE_URI"}
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [sys.executable, str(_PROOF), "--org-id", "x", "--run-start", "1"],
        capture_output=True,
        text=True,
        env=env,
        cwd=_ROOT,
    )
    assert result.returncode != 0, "must not proceed without a DSN"
    assert "--clickhouse-uri or CLICKHOUSE_URI is required" in (
        result.stderr + result.stdout
    ), f"expected an explicit argparse error, got: {result.stderr or result.stdout}"
