"""CHAOS-6845: the integration-credential admin routes are served by the Go api.

The six routes (list, get, create, update, delete, test-connection) keep their
Python handlers mounted (path, methods, parameters, ``Depends`` gates,
``response_model``) with the body reduced to the Go-served refusal
(``api/go_served.py``). A request that reaches one on the Python plane is
ingress skew, never a request the Python side can answer: it gets the
diagnostic 500 and the ``rest.route_served_by_go_api`` event, after the admin
gate has run. The behaviour these routes had is owned and proven by the Go
api's venue oracles (frozen goldens of the Python plane's answers), not by
Python unit tests. ``GET /credentials/{credential_id}/repos`` is not stubbed:
the Go api registers it under the same ingress template and its Python body
stays until its own port.
"""

from __future__ import annotations

import logging

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.main import app
from dev_health_ops.api.services.auth import AuthenticatedUser

_ORG = "00000000-0000-4000-8000-000000006845"
_A = "/api/v1/admin/credentials"

# (method, request url, json body or None, the route template the refusal names)
_ROUTES: list[tuple[str, str, dict | None, str]] = [
    ("GET", _A, None, _A),
    ("GET", f"{_A}/github/main", None, f"{_A}/{{provider}}/{{name}}"),
    (
        "POST",
        _A,
        {"provider": "github", "name": "main", "credentials": {"token": "t"}},
        _A,
    ),
    (
        "PATCH",
        f"{_A}/github/main",
        {"is_active": False},
        f"{_A}/{{provider}}/{{name}}",
    ),
    ("DELETE", f"{_A}/github/main", None, f"{_A}/{{provider}}/{{name}}"),
    (
        "POST",
        f"{_A}/test",
        {"provider": "github", "credentials": {"token": "t"}},
        f"{_A}/test",
    ),
]

_client = TestClient(app, raise_server_exceptions=False)

_ADMIN = AuthenticatedUser(
    "00000000-0000-4000-8000-000000000001", "sentinel@example.invalid", _ORG, "admin"
)


@pytest.fixture(autouse=True)
def _admin() -> object:
    app.dependency_overrides[get_current_user] = lambda: _ADMIN
    app.dependency_overrides[get_admin_org_id] = lambda: _ORG
    yield None
    app.dependency_overrides.pop(get_current_user, None)
    app.dependency_overrides.pop(get_admin_org_id, None)


@pytest.mark.parametrize(
    ("method", "url", "body", "template"),
    _ROUTES,
    ids=[f"{m} {t.removeprefix(_A) or '/'}" for m, _u, _b, t in _ROUTES],
)
def test_the_python_handler_answers_the_go_served_refusal(
    method: str,
    url: str,
    body: dict | None,
    template: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger="dev_health_ops.api.go_served"):
        response = _client.request(method, url, json=body)
    assert response.status_code == 500, response.text
    detail = response.json().get("detail", "")
    assert template in detail and "served by go-api" in detail, detail
    assert any(
        record.message == "rest.route_served_by_go_api"
        and getattr(record, "path", None) == template
        and getattr(record, "plane", None) == "go-api"
        for record in caplog.records
    ), f"{method} {url} did not emit rest.route_served_by_go_api for {template}"


def test_every_credentials_route_is_a_stub_or_the_named_python_only_route() -> None:
    served = {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute)
        and (route.path == _A or route.path.startswith(f"{_A}/"))
        for method in route.methods
        if method not in {"HEAD", "OPTIONS"}
    }
    python_only = {("GET", f"{_A}/{{credential_id}}/repos")}
    expected = {(method, template) for method, _url, _body, template in _ROUTES}
    assert served - python_only == expected, sorted((served - python_only) ^ expected)
    assert python_only <= served
    assert len(expected) == 6


def test_the_admin_gate_still_runs_before_the_refusal() -> None:
    app.dependency_overrides.pop(get_admin_org_id, None)
    app.dependency_overrides.pop(get_current_user, None)
    response = _client.get(_A)
    assert response.status_code in {401, 403}, response.text
