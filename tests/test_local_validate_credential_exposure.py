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

import re
import subprocess
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
    ["sec/ret", "se#cr?et", "p@ssw%rd", "a:b", "sp ace", "plain"],
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
