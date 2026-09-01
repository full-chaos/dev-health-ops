"""CHAOS-3273 L5: pins the CODE DEFAULT of GRAPHQL_AUTH_REQUIRED, not the
environment's current value.

``tests/api/graphql/test_graphql_auth.py`` already covers both explicit
settings (``GRAPHQL_AUTH_REQUIRED=true`` -> enforced,
``GRAPHQL_AUTH_REQUIRED=false`` -> disabled) but every one of its fixtures
sets the env var explicitly one way or the other -- none of them leave it
UNSET. ``_graphql_auth_required()``
(``dev_health_ops/api/graphql/app.py:44-50``) defaults to enforced when the
var is absent (``os.getenv("GRAPHQL_AUTH_REQUIRED", "true").lower() !=
"false"``), which is the fail-closed choice that matters: nothing prevents
a future edit from flipping that literal default, and no existing test
would catch it, because they all supply the var themselves.

This is also a named dependency of CHAOS-4743 (GRAPHQL_AUTH_REQUIRED
enforce-by-default), whose acceptance criteria cross-reference this CI pin.

Per the brief: pin the DEFAULT as enforcement. Do not test the ``false``
branch as if it were supported/expected behaviour (already covered
elsewhere, and testing it here would suggest this file endorses it).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app


@pytest.fixture(autouse=True)
def _fresh_auth_service(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("dev_health_ops.api.services.auth._auth_service", None)


def test_graphql_auth_required_defaults_to_enforced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The one thing this test must NOT do is set GRAPHQL_AUTH_REQUIRED --
    # that would defeat the point. delenv (raising=False) guarantees a
    # clean "absent" state regardless of what the ambient environment or
    # an earlier test in this process left behind.
    monkeypatch.delenv("GRAPHQL_AUTH_REQUIRED", raising=False)

    client = TestClient(app, raise_server_exceptions=False)
    response = client.post("/graphql", json={"query": "{ __typename }"})

    # Regresses if the literal default in _graphql_auth_required() (the
    # "true" in os.getenv("GRAPHQL_AUTH_REQUIRED", "true")) is ever changed,
    # or if the comparison is ever inverted.
    assert response.status_code == 401
