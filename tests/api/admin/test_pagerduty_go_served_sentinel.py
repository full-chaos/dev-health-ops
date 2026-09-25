"""CHAOS-6843: the PagerDuty admin routes are served by the Go api.

Their Python handlers keep the route mounted (path, methods, parameters,
``Depends`` gates, ``response_model``) with the body reduced to the Go-served
refusal (``api/go_served.py``). A request that reaches one on the Python plane
is ingress skew, never a request the Python side can answer: it gets the
diagnostic 500 and the ``rest.route_served_by_query_api`` event, after the
admin gate has run. The behaviour these routes had (setup, callback, status,
preflight, disconnect, manual credentials, webhook bindings, services) is
owned and proven by the Go api's venue oracles against the real Python plane,
not by Python unit tests.
"""

from __future__ import annotations

import logging
import uuid

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.main import app
from dev_health_ops.api.services.auth import AuthenticatedUser

_ORG = "00000000-0000-4000-8000-000000006843"
_BINDING = "00000000-0000-4000-8000-0000000068b1"
_PREFIX = "/api/v1/admin/integrations/pagerduty"

_BINDING_BODY = {
    "integration_source_id": str(uuid.UUID(int=1)),
    "credential_id": str(uuid.UUID(int=2)),
    "provider_subscription_id": "sub-1",
    "signing_secret": "sentinel-signing-secret",
}

# (method, request url, json body or None, the route template the refusal names)
_ROUTES: list[tuple[str, str, dict | None, str]] = [
    ("GET", f"{_PREFIX}/status", None, f"{_PREFIX}/status"),
    (
        "POST",
        f"{_PREFIX}/preflight",
        {"enabled_datasets": ["incidents"]},
        f"{_PREFIX}/preflight",
    ),
    ("POST", f"{_PREFIX}/authorize", {}, f"{_PREFIX}/authorize"),
    ("POST", f"{_PREFIX}/callback", {"state": "s", "code": "c"}, f"{_PREFIX}/callback"),
    ("POST", f"{_PREFIX}/disconnect", {}, f"{_PREFIX}/disconnect"),
    (
        "POST",
        f"{_PREFIX}/client-credentials",
        {"client_id": "id", "client_secret": "secret", "subdomain": "acme"},
        f"{_PREFIX}/client-credentials",
    ),
    (
        "POST",
        f"{_PREFIX}/api-token",
        {"api_token": "token", "subdomain": "acme"},
        f"{_PREFIX}/api-token",
    ),
    ("GET", f"{_PREFIX}/services", None, f"{_PREFIX}/services"),
    (
        "POST",
        f"{_PREFIX}/webhook-bindings",
        _BINDING_BODY,
        f"{_PREFIX}/webhook-bindings",
    ),
    (
        "POST",
        f"{_PREFIX}/webhook-bindings/{_BINDING}/rotate",
        _BINDING_BODY,
        f"{_PREFIX}/webhook-bindings/{{binding_id}}/rotate",
    ),
    (
        "POST",
        f"{_PREFIX}/webhook-bindings/{_BINDING}/activate",
        None,
        f"{_PREFIX}/webhook-bindings/{{binding_id}}/activate",
    ),
    (
        "POST",
        f"{_PREFIX}/webhook-bindings/{_BINDING}/revoke",
        None,
        f"{_PREFIX}/webhook-bindings/{{binding_id}}/revoke",
    ),
    (
        "GET",
        f"{_PREFIX}/webhook-bindings/{_BINDING}",
        None,
        f"{_PREFIX}/webhook-bindings/{{binding_id}}",
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
    ids=[f"{m} {t.removeprefix(_PREFIX)}" for m, _u, _b, t in _ROUTES],
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
    assert template in detail and "query-api" in detail, detail
    assert any(
        record.message == "rest.route_served_by_query_api"
        and getattr(record, "path", None) == template
        for record in caplog.records
    ), f"{method} {url} did not emit rest.route_served_by_query_api for {template}"


def test_every_pagerduty_admin_route_the_go_api_serves_is_a_stub() -> None:
    served = {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith(_PREFIX)
        for method in route.methods
        if method not in {"HEAD", "OPTIONS"}
    }
    expected = {
        (method, template.replace("{binding_id}", "{binding_id}"))
        for method, _url, _body, template in _ROUTES
    }
    assert served == expected, sorted(served ^ expected)
    assert len(expected) == 13


def test_the_admin_gate_still_runs_before_the_refusal() -> None:
    app.dependency_overrides.pop(get_admin_org_id, None)
    app.dependency_overrides.pop(get_current_user, None)
    response = _client.get(f"{_PREFIX}/status")
    assert response.status_code in {401, 403}, response.text
