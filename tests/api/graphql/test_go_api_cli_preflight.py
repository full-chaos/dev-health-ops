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
