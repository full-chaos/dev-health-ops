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
