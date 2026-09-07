"""``dev-hops go-api routing enable`` refuses before it writes anything.

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
from typing import Any

import pytest
from _go_api_fake_query_api import FakeQueryAPI, registry_payload

from dev_health_ops.api.graphql import go_api_cli
from dev_health_ops.api.graphql.go_api_operation_catalog import catalog_entries
from dev_health_ops.api.graphql.go_api_schema_digest import current_schema_digest


def _enable(**overrides: Any) -> int:
    ns = argparse.Namespace(
        operations="all-registered",
        candidate_build="deadbeef",
        mode="canary",
        rollout=100,
        query_api_url=None,
        acknowledge_unproven=False,
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return asyncio.run(go_api_cli._cmd_routing_enable(ns))


@pytest.fixture(autouse=True)
def _no_ambient_query_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """A GO_API_QUERY_API_URL leaking in from the environment would make
    these tests talk to whatever is running locally."""
    monkeypatch.delenv("GO_API_QUERY_API_URL", raising=False)


def test_preflight_1_refuses_when_no_url_is_configured(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _enable() == 2
    err = capsys.readouterr().err
    assert "no query-api URL" in err
    assert "GO_API_QUERY_API_URL" in err


def test_preflight_1_refuses_when_query_api_is_unreachable(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Port 1 on loopback: nothing listens, connection refused immediately.
    assert _enable(query_api_url="http://127.0.0.1:1") == 2
    err = capsys.readouterr().err
    assert "cannot read the running query-api's registry" in err
    # "no answer" must never be treated as "no objection".
    assert "did not happen is not a pass" in err


def test_preflight_1_refuses_on_a_404_naming_the_likely_cause(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An older image, or one whose /query never mounted, 404s here."""
    with FakeQueryAPI(registry_payload(), path="/something-else") as url:
        assert _enable(query_api_url=url) == 2
    assert "predates GET /registry" in capsys.readouterr().err


def test_preflight_2_refuses_when_the_two_planes_disagree(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """THE 2026-09-01 defect, caught.

    The deployed binary is still on the pre-#2065 digest while this
    checkout is on the post-#2065 one. Writing rows here produces exactly
    the twelve dead rows that went unnoticed for six days.
    """
    stale = "sha256:67b87d38e46f767511b5d8435ffbfdd7dbe8aeab9dbe4073c7d7706de572f706"
    with FakeQueryAPI(registry_payload(schema_digest=stale)) as url:
        assert _enable(query_api_url=url) == 2
    err = capsys.readouterr().err
    assert "schema digest MISMATCH" in err
    # Both values, so an operator can tell which side is behind.
    assert stale in err
    assert current_schema_digest() in err
    assert "Rebuild and redeploy" in err


def test_preflight_3_refuses_an_operation_the_binary_does_not_register(
    capsys: pytest.CaptureFixture[str],
) -> None:
    catalog = dict(catalog_entries())
    served = {k: v for k, v in catalog.items() if k != "featureFlags"}
    with FakeQueryAPI(registry_payload(operations=served)) as url:
        assert _enable(query_api_url=url) == 2
    err = capsys.readouterr().err
    assert "does not register" in err
    assert "featureFlags" in err


def test_preflight_3_refuses_on_a_divergent_document_digest(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Same operation name, different registered document.

    A row written with the local digest would never be looked up by the
    running binary -- the document-digest twin of the schema-digest
    failure, and just as silent.
    """
    catalog = dict(catalog_entries())
    served = dict(catalog)
    served["featureFlags"] = "0" * 64
    with FakeQueryAPI(registry_payload(operations=served)) as url:
        assert _enable(query_api_url=url) == 2
    err = capsys.readouterr().err
    assert "document digest MISMATCH" in err
    assert "featureFlags" in err
    assert "0" * 64 in err


def test_an_unknown_operation_name_is_refused_before_any_network_call(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A typo must not silently enable "everything except that one"."""
    assert _enable(operations="featureFlags,notARealOperation") == 2
    err = capsys.readouterr().err
    assert "unknown operation(s) notARealOperation" in err
    # The refusal lists what IS available, so the fix is obvious.
    assert "featureFlags" in err


def test_a_malformed_registry_body_is_treated_as_no_answer(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Partially understood is not understood.

    Accepting a body with no schema_digest would silently skip preflight
    2 -- the one preflight that catches the digest move.
    """
    with FakeQueryAPI({"operations": []}) as url:
        assert _enable(query_api_url=url) == 2
    assert "no usable schema_digest" in capsys.readouterr().err

    with FakeQueryAPI(b"not json at all") as url:
        assert _enable(query_api_url=url) == 2
    assert "unreachable" in capsys.readouterr().err


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


def test_a_non_http_query_api_url_is_refused_before_opening_anything(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """urllib opens file:// too.

    Without a scheme guard, a malformed ``GO_API_QUERY_API_URL`` would
    make the "ask the running binary" step read a local file and then
    refuse with a JSON parse error -- pointing an operator at the wrong
    problem entirely.
    """
    assert _enable(query_api_url="file:///etc/hostname") == 2
    err = capsys.readouterr().err
    assert "must be http:// or https://" in err


# --- codex r1 fixes -------------------------------------------------------


def test_url_credentials_never_reach_the_error_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A URL can carry `user:password@`, and this lane never prints credentials.

    codex r1 (P2): `http://alice:super-secret@127.0.0.1:1` echoed the
    password verbatim into stderr on the unreachable-host path.
    """
    assert _enable(query_api_url="http://alice:super-secret@127.0.0.1:1") == 2
    err = capsys.readouterr().err
    assert "super-secret" not in err
    assert "alice" not in err
    assert "<redacted>" in err
    # Still diagnostic: the host must survive redaction, or the operator
    # cannot tell which endpoint failed.
    assert "127.0.0.1:1" in err


def test_credentials_are_redacted_from_a_non_http_scheme_refusal(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _enable(query_api_url="ftp://bob:hunter2@example.invalid") == 2
    err = capsys.readouterr().err
    assert "hunter2" not in err
    assert "must be http:// or https://" in err


def test_a_duplicated_operation_in_the_registry_body_is_refused(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Collapsing duplicates silently would let preflight 3 compare against
    whichever copy happened to win (codex r1, P3)."""
    catalog = dict(catalog_entries())
    payload = registry_payload()
    payload["operations"].append(
        {"operation": "featureFlags", "document_digest": "0" * 64}
    )
    assert catalog  # guard against a vacuous pass on an empty catalog
    with FakeQueryAPI(payload) as url:
        assert _enable(query_api_url=url) == 2
    err = capsys.readouterr().err
    assert "more than once" in err
    assert "featureFlags" in err


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


def test_a_password_containing_a_slash_is_still_redacted() -> None:
    """codex r2 P2: the userinfo pattern excluded `/`, so `pa/ss` survived."""
    leaked = "http://alice:pa/ss@127.0.0.1:1/registry is unreachable"
    redacted = go_api_cli._redact_text(leaked)
    assert "pa/ss" not in redacted
    assert "<redacted>" in redacted


def test_a_malformed_port_does_not_crash_redaction() -> None:
    """codex r2 P2: `.port` raises ValueError on `:bad`.

    A redaction helper that raises turns a credential-bearing string into
    an unhandled traceback -- which prints it anyway, via the exception.
    """
    out = go_api_cli._redact_url("http://alice:secret@example.com:bad/registry")
    assert "secret" not in out


# Every vector below was MEASURED leaking at some point in this PR's review
# history. Each is asserted with a direct `not in`, and there is deliberately
# no `or` anywhere: the previous version of this test read
#     assert "s@cret" not in redacted or "<redacted>" in redacted
# and the `or` made it vacuous, since "<redacted>" is always present. It
# passed while the credential leaked (trap #43's class, in my own hand).
_REDACTION_VECTORS = [
    # (url, the substring that must never survive)
    ("http://alice:super-secret@host/registry", "super-secret"),  # r1
    ("http://alice:pa/ss@host/registry", "pa/ss"),  # r2: '/' in password
    ("http://alice:s@cret@host/registry", "cret"),  # r3: raw '@' in password
    ("http://:secret@host/registry", "secret"),  # r3: empty username
    ("http://alice@host/registry", "alice"),  # r3: username only
    ("http://alice:secret@example.com:bad/registry", "secret"),  # malformed port
    ("http://alice:secret@[::1]:8080/registry", "secret"),  # IPv6 host
    ("http://user:p%40ss@host/registry", "p%40ss"),  # percent-encoded
]


@pytest.mark.parametrize(("url", "secret"), _REDACTION_VECTORS)
def test_redact_text_never_leaves_the_secret(url: str, secret: str) -> None:
    redacted = go_api_cli._redact_text(url)
    assert secret not in redacted
    assert "<redacted>" in redacted


@pytest.mark.parametrize(("url", "secret"), _REDACTION_VECTORS)
def test_redact_url_never_leaves_the_secret(url: str, secret: str) -> None:
    redacted = go_api_cli._redact_url(url)
    assert secret not in redacted
    assert "<redacted>" in redacted


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://alice:super-secret@host/registry", "http://<redacted>@host/registry"),
        ("http://alice:pa/ss@host/registry", "http://<redacted>@host/registry"),
        ("http://alice:s@cret@host/registry", "http://<redacted>@host/registry"),
        ("http://:secret@host/registry", "http://<redacted>@host/registry"),
        ("http://alice@host/registry", "http://<redacted>@host/registry"),
        (
            "http://alice:secret@example.com:bad/registry",
            "http://<redacted>@example.com:bad/registry",
        ),
        (
            "http://alice:secret@[::1]:8080/registry",
            "http://<redacted>@[::1]:8080/registry",
        ),
    ],
)
def test_redaction_keeps_the_host_exactly(url: str, expected: str) -> None:
    """Pinned output, not a disjunction.

    Over-redaction has a real cost: the host is the diagnostic, and an
    operator who cannot see which endpoint failed is barely better off than
    one who saw a password. Asserting the exact string keeps both halves
    honest and leaves no `or` for a future edit to hide behind.
    """
    assert go_api_cli._redact_text(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:1/registry",
        "http://host/registry?q=1",
        "https://query-api.internal:8080/registry",
    ],
)
def test_a_url_without_credentials_is_left_intact(url: str) -> None:
    assert go_api_cli._redact_text(url) == url
    assert go_api_cli._redact_url(url) == url


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
