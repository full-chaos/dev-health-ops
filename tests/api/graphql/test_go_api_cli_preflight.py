"""The go-api routing commands refuse before they write anything.

Preflights 1-3 run against a REAL HTTP server (``http.server`` on a
loopback port) rather than a mocked client, because what is being tested
is the decision made from a wire response -- a mocked ``_fetch_go_plane_registry``
would test the assertions and skip the parsing, which is where a wrong
answer would actually come from.

None of these tests needs a database: every one of these refusals happens
BEFORE the first session is opened. That ordering is itself part of the
contract -- an ``enable`` that opened a transaction and then refused would
be harder to reason about, and a refusal that happened after a partial
write would be worse than no refusal at all.

Each case is a real failure shape:

* unreachable          -- query-api down, wrong URL, wrong port
* digest mismatch      -- the checkout moved ahead of the deployed image.
  This is the 2026-09-01 defect, and the ONLY preflight that would have
  prevented it.
* operation missing    -- enabling an operation into a build that predates it
* document divergence  -- the operation exists on both sides under
  different registered text
"""

from __future__ import annotations

import argparse
import asyncio
import urllib.error

import pytest

from dev_health_ops.api.graphql import go_api_cli
from dev_health_ops.api.graphql.go_api_operation_catalog import catalog_entries
from dev_health_ops.api.graphql.go_api_schema_digest import current_schema_digest


def await_sync(coro):
    """Run a coroutine from a sync test body."""
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_ambient_query_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """A GO_API_QUERY_API_URL leaking in from the environment would make
    these tests talk to whatever is running locally."""
    monkeypatch.delenv("GO_API_QUERY_API_URL", raising=False)


def test_operations_are_resolved_from_the_catalog_not_a_hand_list() -> None:
    """``all-registered`` means exactly the registrydump-generated catalog.

    If this ever diverges, ``enable`` is writing rows for a set of
    operations nobody generated -- the hand-maintained-inventory drift
    class the catalog exists to prevent.
    """
    catalog = dict(catalog_entries())
    resolved, error = go_api_cli._resolve_requested_operations(
        "all-registered", catalog
    )
    assert error is None
    assert resolved == sorted(catalog)
    assert len(resolved) == len(catalog) > 0


# F4 (CHAOS-5581, opus-r10): `dict(catalog_entries())` keys by operation
# name alone, so an operation registered under two documents silently
# collapses to whichever digest sorts last -- exactly the class
# `NewCatalog` (internal/migrationmatrix) already refuses at the digest
# level. `status`, `plan_disable` and the Go matrix all key the full pair
# and can tell two documents apart; `enable`/`disable` cannot, because
# every preflight and write downstream is `catalog[operation]`.
def test_catalog_by_operation_refuses_an_operation_registered_twice() -> None:
    by_operation, error = go_api_cli._catalog_by_operation(
        (("featureFlags", "doc-a"), ("featureFlags", "doc-b"), ("reviewEdges", "doc-c"))
    )
    assert by_operation == {}
    assert error is not None
    assert "featureFlags" in error
    assert "doc-a" in error and "doc-b" in error
    assert "reviewEdges" not in error


def test_catalog_by_operation_passes_through_with_no_duplicates() -> None:
    by_operation, error = go_api_cli._catalog_by_operation(
        (("featureFlags", "doc-a"), ("reviewEdges", "doc-c"))
    )
    assert error is None
    assert by_operation == {"featureFlags": "doc-a", "reviewEdges": "doc-c"}


def test_catalog_by_operation_is_not_confused_by_a_repeated_identical_pair() -> None:
    """The same (operation, digest) pair twice is not a collision -- only
    the SAME operation under DIFFERENT digests is."""
    by_operation, error = go_api_cli._catalog_by_operation(
        (("featureFlags", "doc-a"), ("featureFlags", "doc-a"))
    )
    assert error is None
    assert by_operation == {"featureFlags": "doc-a"}


# --- codex r1 fixes -------------------------------------------------------


# --- `status` must survive the thing it diagnoses being down --------------
#
# These live HERE, not in test_go_api_cli_enable_db.py, deliberately. That
# file is gated on DEV_HEALTH_POSTGRES_TEST_URI, so in CI it SKIPS -- and a
# regression test for a BLOCKING finding that skips on the gate is no
# coverage at all. Neither test needs a database; the whole point is that
# there isn't one.


@pytest.mark.asyncio
async def test_status_survives_an_unreachable_registry_database(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """codex r1 P1 (BLOCKING): `status` raised and exited 1 when Postgres was down.

    That contradicted the command's entire contract. `status` is what an
    operator runs WHEN THINGS ARE BROKEN -- a diagnostic that dies because
    the thing it diagnoses is down is useless exactly when it is needed,
    and a traceback tells them nothing about the plane digests it can
    still report without a database.

    No scratch DB fixture here on purpose: the point is that there is no
    reachable database at all.
    """
    import contextlib

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("connection refused: registry postgres is down")
        yield  # pragma: no cover - unreachable, satisfies the generator protocol

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url=None, json=False)
        )
        == 0
    ), "status must not fail when the registry database is unreachable"

    out = capsys.readouterr().out
    assert "UNREACHABLE" in out
    assert "connection refused" in out
    # What it CAN still answer without a database, it must answer.
    assert current_schema_digest() in out
    # And it must not let an operator read "no rows" out of "cannot read rows".
    assert "NOT evidence that nothing is enabled" in out


@pytest.mark.asyncio
async def test_status_json_reports_the_database_error_field(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import contextlib
    import json as json_module

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("boom")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url=None, json=True)
        )
        == 0
    )
    payload = json_module.loads(capsys.readouterr().out)
    assert payload["registry_db_error"] is not None
    assert "boom" in payload["registry_db_error"]
    assert payload["rows_by_schema_digest"] == {}
    assert payload["operations"] == []


@pytest.mark.asyncio
async def test_status_distinguishes_a_failed_catalog_load_from_an_empty_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """codex r1 P2: both printed an empty table and exited 0.

    Per-request dispatch is right not to care -- both mean "nothing is
    Go-eligible", the safe default. An operator reading `status` cares
    enormously: one is the normal posture and the other is a broken
    deployment. Reporting them identically is the same "two states, one
    silence" defect this whole change exists to end.
    """
    import contextlib

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)
    monkeypatch.setattr(go_api_cli, "catalog_entries", lambda: ())
    monkeypatch.setattr(go_api_cli, "catalog_loaded_successfully", lambda: False)

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url=None, json=False)
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "CATALOG UNAVAILABLE" in out
    assert "NOT the same as an empty catalog" in out


@pytest.mark.asyncio
async def test_status_does_not_cry_wolf_when_the_catalog_is_merely_empty(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The converse, so the warning stays meaningful.

    A guard that fires in the healthy case teaches operators to ignore it.
    """
    import contextlib

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)
    monkeypatch.setattr(go_api_cli, "catalog_entries", lambda: ())
    monkeypatch.setattr(go_api_cli, "catalog_loaded_successfully", lambda: True)

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url=None, json=False)
        )
        == 0
    )
    assert "CATALOG UNAVAILABLE" not in capsys.readouterr().out


# --- codex r2 fixes -------------------------------------------------------


@pytest.mark.asyncio
async def test_status_survives_an_unreadable_sdl_and_still_emits_valid_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """codex r2 P1: an unreadable SDL killed `status` with NO output at all.

    Same contract breach r1 found in the database read, one line higher up.
    `--json` emitting nothing is worse than emitting an error field: a
    caller parsing it gets a crash instead of a diagnosis.
    """
    import contextlib
    import json as json_module

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)
    monkeypatch.setattr(
        go_api_cli,
        "current_schema_digest",
        lambda: (_ for _ in ()).throw(RuntimeError("missing SDL")),
    )

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url=None, json=True)
        )
        == 0
    )
    payload = json_module.loads(capsys.readouterr().out)
    assert payload["python_plane_schema_digest"] is None
    assert "missing SDL" in payload["python_plane_digest_error"]


@pytest.mark.asyncio
async def test_status_text_mode_names_an_unreadable_sdl(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import contextlib

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)
    monkeypatch.setattr(
        go_api_cli,
        "current_schema_digest",
        lambda: (_ for _ in ()).throw(RuntimeError("missing SDL")),
    )

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url=None, json=False)
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "UNAVAILABLE" in out
    assert "missing SDL" in out
    assert "cannot compute the routing key" in out


# THE LEAK TABLE. Fifteen URL shapes, every one of which leaked a
# credential at some point across five review rounds of this PR.
#
#   r1                'plain'
#   r2                '/' in password, malformed port, IPv6 host, %-encoded
#   r3                raw '@' in password, empty username, username only
#   confirmation      space / tab / newline in password
#   final pass        no scheme (no '//'), backslash separators
#
# The module no longer redacts anything: it never prints a query-api URL at
# all, and builds its messages from `urlsplit`'s scheme and hostname only.
# This table is the proof of that property, asserted end-to-end over every
# path that can emit text -- not over a redaction helper, because there is
# no longer one to test.
#
# Each row is (name, url, [substrings that must never appear anywhere]).
_LEAK_VECTORS = [
    ("plain", "http://alice:super-secret@host/registry", ["super-secret"]),
    ("slash in password", "http://alice:pa/ss@host/registry", ["pa/ss"]),
    ("raw @ in password", "http://alice:s@cret@host/registry", ["s@cret"]),
    ("empty username", "http://:secret@host/registry", ["secret"]),
    ("username only", "http://alice@host/registry", ["alice"]),
    ("malformed port", "http://alice:secret@example.com:bad/registry", ["secret"]),
    ("ipv6 host", "http://alice:secret@[::1]:8080/registry", ["secret"]),
    ("percent encoded", "http://user:p%40ss@host/registry", ["p%40ss", "p@ss"]),
    ("space in password", "http://alice:pa ss@host/registry", ["pa ss"]),
    ("newline in password", "http://alice:pa\nss@host/registry", ["pa\nss"]),
    ("tab in password", "http://alice:pa\tss@host/registry", ["pa\tss"]),
    ("hash in password", "http://alice:pa#ss@host/registry", ["pa#ss"]),
    # final pass, P1-1: no '//' at all, so nothing could locate the
    # credential in order to delete it.
    ("no scheme", "alice:secret@host:8080/path", ["secret"]),
    ("backslash separators", "http:\\\\alice:secret@host:8080\\\\path", ["secret"]),
    ("no scheme, slash in password", "alice:pa/ss@host/registry", ["pa/ss"]),
]


def _all_emitted_text(
    capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> str:
    captured = capsys.readouterr()
    return captured.out + captured.err + caplog.text


@pytest.mark.parametrize(("name", "url", "secrets"), _LEAK_VECTORS)
def test_status_never_emits_a_credential_on_any_path(
    name: str,
    url: str,
    secrets: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The same, through `status`, whose whole job is to keep printing when
    things are broken -- so it has the most output paths."""
    import contextlib
    import logging

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)

    with caplog.at_level(logging.DEBUG):
        assert (
            await_sync(
                go_api_cli._cmd_routing_status(
                    argparse.Namespace(query_api_url=url, json=False)
                )
            )
            == 0
        )

    emitted = _all_emitted_text(capsys, caplog)
    for secret in secrets:
        assert secret not in emitted, (
            f"{name}: {secret!r} reached output via `status`:\n{emitted}"
        )


@pytest.mark.parametrize(("name", "url", "secrets"), _LEAK_VECTORS)
def test_the_registry_fetch_exception_carries_no_credential(
    name: str, url: str, secrets: list[str]
) -> None:
    """The exception TEXT itself, which callers format into their own
    messages -- a leak here escapes through every one of them."""
    with pytest.raises(go_api_cli.GoPlaneUnavailable) as excinfo:
        go_api_cli._fetch_go_plane_registry(url)
    message = str(excinfo.value)
    for secret in secrets:
        assert secret not in message, f"{name}: {secret!r} in the exception text"


def test_the_endpoint_label_never_echoes_an_unparseable_url() -> None:
    """A scheme that is not http(s) is never printed either.

    For `alice:secret@host/path`, `urlsplit` reads the SCHEME as `alice` --
    which is the username. Emitting the scheme unconditionally would make
    the label itself the leak, so anything outside the allowlist collapses
    to a fixed literal.
    """
    assert go_api_cli._endpoint_label("alice:secret@host:8080/path") == "unparseable"
    assert go_api_cli._endpoint_label("http://alice:secret@host/x") == "http://host"
    assert go_api_cli._endpoint_label("https://a:b@example.com:8080/x") == (
        "https://example.com"
    )


def test_transport_failure_never_includes_the_exception_message() -> None:
    """`str(exc)` from an HTTP client routinely embeds the URL it was given."""
    err = urllib.error.URLError("failed opening http://alice:secret@host/registry")
    described = go_api_cli._transport_failure(err)
    assert "secret" not in described
    assert "URLError" in described


@pytest.mark.asyncio
async def test_status_never_states_a_verdict_it_cannot_support(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """codex r3 P1: with no local digest, `status` claimed MISMATCH.

    `None` means UNKNOWN, not "different". r2's fix stopped the crash and
    left every downstream comparison treating None as a value, so the tool
    reported a confident MISMATCH, `planes_agree: false`, and the literal
    string "written at None" -- sending an operator to rebuild an image
    over a missing file. Stating a confident wrong answer instead of
    "unknown" is the precise failure this whole change exists to end.
    """
    import contextlib

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)
    monkeypatch.setattr(
        go_api_cli,
        "current_schema_digest",
        lambda: (_ for _ in ()).throw(RuntimeError("missing SDL")),
    )
    monkeypatch.setattr(
        go_api_cli,
        "_fetch_go_plane_registry",
        lambda _url: go_api_cli.GoPlaneRegistry(
            schema_digest="sha256:29d509cd", operations={}
        ),
    )

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url="http://query-api", json=False)
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "MISMATCH" not in out
    assert "None" not in out
    assert "[UNKNOWN]" in out
    assert "NOT a mismatch" in out
    assert "<- STALE" not in out


@pytest.mark.asyncio
async def test_status_json_reports_planes_agree_as_null_when_unknown(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import contextlib
    import json as json_module

    import dev_health_ops.db as db_module

    @contextlib.asynccontextmanager
    async def dead_session():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db_module, "get_postgres_session", dead_session)
    monkeypatch.setattr(
        go_api_cli,
        "current_schema_digest",
        lambda: (_ for _ in ()).throw(RuntimeError("missing SDL")),
    )
    monkeypatch.setattr(
        go_api_cli,
        "_fetch_go_plane_registry",
        lambda _url: go_api_cli.GoPlaneRegistry(
            schema_digest="sha256:29d509cd", operations={}
        ),
    )

    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url="http://query-api", json=True)
        )
        == 0
    )
    payload = json_module.loads(capsys.readouterr().out)
    assert payload["python_plane_schema_digest"] is None
    assert payload["planes_agree"] is None, (
        "planes_agree must be null when a plane's digest is unknown -- false "
        "asserts a mismatch that was never measured"
    )
    assert payload["go_plane_schema_digest"] == "sha256:29d509cd"


def test_the_python_cli_has_no_enable_verb(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`enable` exists only as the Go verb, which reads the build from
    /buildinfo; the Python CLI offers routing status and disable only."""
    parser = argparse.ArgumentParser(prog="dev-hops")
    go_api_cli.register_commands(parser.add_subparsers(dest="command"))

    assert not hasattr(go_api_cli, "_cmd_routing_enable")
    # The bare word is refused as an unknown verb, whatever arguments follow
    # it: a verb that parsed here could be given any flags at all.
    for argv in (
        ["go-api", "routing", "enable"],
        ["go-api", "routing", "enable", "--mode", "canary", "--candidate-build", "x"],
        ["go-api", "routing", "enable", "--acknowledge-unproven"],
    ):
        with pytest.raises(SystemExit) as refused:
            parser.parse_args(argv)
        assert refused.value.code == 2
        assert "invalid choice: 'enable'" in capsys.readouterr().err
    assert (
        parser.parse_args(["go-api", "routing", "status"]).go_api_routing_command
        == "status"
    )
    assert (
        parser.parse_args(
            ["go-api", "routing", "disable", "--mode", "python"]
        ).go_api_routing_command
        == "disable"
    )
