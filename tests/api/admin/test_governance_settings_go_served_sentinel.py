"""CHAOS-6844: the governance and settings admin routes are served by the Go api.

Retention policies, the IP allowlist, key/value settings and LLM settings keep
their Python routes mounted (path, methods, parameters, ``Depends`` gates,
``response_model``, licence gate) with the body reduced to the Go-served refusal
(``api/go_served.py``). A request that reaches one on the Python plane is
ingress skew, never a request the Python side can answer: it gets the
diagnostic 500 and the ``rest.route_served_by_go_api`` event, after the admin
and licence gates have run. The behaviour these routes had is owned and proven
by the Go api's venue oracles (frozen goldens of the Python plane's answers),
not by Python unit tests. The routes the Go api does not register stay Python:
``/settings/categories``, ``/llm-settings/status``, ``/llm-settings/readiness``.
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

_ORG = "00000000-0000-4000-8000-000000006844"
_ID = "00000000-0000-4000-8000-0000000068a1"
_A = "/api/v1/admin"

# (method, request url, json body or None, the route template the refusal names)
_ROUTES: list[tuple[str, str, dict | None, str]] = [
    ("GET", f"{_A}/retention-policies", None, f"{_A}/retention-policies"),
    (
        "GET",
        f"{_A}/retention-policies/resource-types",
        None,
        f"{_A}/retention-policies/resource-types",
    ),
    (
        "POST",
        f"{_A}/retention-policies",
        {"resource_type": "git_commits", "retention_days": 30},
        f"{_A}/retention-policies",
    ),
    (
        "GET",
        f"{_A}/retention-policies/{_ID}",
        None,
        f"{_A}/retention-policies/{{policy_id}}",
    ),
    (
        "PATCH",
        f"{_A}/retention-policies/{_ID}",
        {"retention_days": 60},
        f"{_A}/retention-policies/{{policy_id}}",
    ),
    (
        "DELETE",
        f"{_A}/retention-policies/{_ID}",
        None,
        f"{_A}/retention-policies/{{policy_id}}",
    ),
    (
        "POST",
        f"{_A}/retention-policies/{_ID}/execute",
        {"dry_run": True},
        f"{_A}/retention-policies/{{policy_id}}/execute",
    ),
    ("GET", f"{_A}/ip-allowlist", None, f"{_A}/ip-allowlist"),
    ("POST", f"{_A}/ip-allowlist", {"ip_range": "10.0.0.0/8"}, f"{_A}/ip-allowlist"),
    (
        "GET",
        f"{_A}/ip-allowlist/{_ID}",
        None,
        f"{_A}/ip-allowlist/{{entry_id}}",
    ),
    (
        "PATCH",
        f"{_A}/ip-allowlist/{_ID}",
        {"description": "x"},
        f"{_A}/ip-allowlist/{{entry_id}}",
    ),
    (
        "DELETE",
        f"{_A}/ip-allowlist/{_ID}",
        None,
        f"{_A}/ip-allowlist/{{entry_id}}",
    ),
    (
        "POST",
        f"{_A}/ip-allowlist/check",
        {"ip_address": "10.0.0.1"},
        f"{_A}/ip-allowlist/check",
    ),
    ("GET", f"{_A}/settings/general", None, f"{_A}/settings/{{category}}"),
    (
        "GET",
        f"{_A}/settings/general/site",
        None,
        f"{_A}/settings/{{category}}/{{key}}",
    ),
    (
        "PUT",
        f"{_A}/settings/general/site",
        {"value": "v"},
        f"{_A}/settings/{{category}}/{{key}}",
    ),
    (
        "POST",
        f"{_A}/settings",
        {"key": "site", "value": "v", "category": "general"},
        f"{_A}/settings",
    ),
    (
        "DELETE",
        f"{_A}/settings/general/site",
        None,
        f"{_A}/settings/{{category}}/{{key}}",
    ),
    ("GET", f"{_A}/llm-settings", None, f"{_A}/llm-settings"),
    ("GET", f"{_A}/llm-settings/budget", None, f"{_A}/llm-settings/budget"),
    ("GET", f"{_A}/llm-settings/spend", None, f"{_A}/llm-settings/spend"),
    ("PUT", f"{_A}/llm-settings", {"provider": "openai"}, f"{_A}/llm-settings"),
    ("DELETE", f"{_A}/llm-settings", None, f"{_A}/llm-settings"),
]

# Routes that stay on the Python plane (the Go api does not register them).
_PYTHON_ONLY = {
    ("GET", f"{_A}/settings/categories"),
    ("GET", f"{_A}/llm-settings/status"),
    ("POST", f"{_A}/llm-settings/readiness"),
}

_FAMILY_PREFIXES = (
    f"{_A}/retention-policies",
    f"{_A}/ip-allowlist",
    f"{_A}/settings",
    f"{_A}/llm-settings",
)

_client = TestClient(app, raise_server_exceptions=False)

_ADMIN = AuthenticatedUser(
    "00000000-0000-4000-8000-000000000001", "sentinel@example.invalid", _ORG, "admin"
)


@pytest.fixture(autouse=True)
def _admin(monkeypatch: pytest.MonkeyPatch) -> object:
    # The retention routes carry a licence gate that runs before the body.
    monkeypatch.setattr("dev_health_ops.licensing.gating.has_feature", lambda *_: True)
    app.dependency_overrides[get_current_user] = lambda: _ADMIN
    app.dependency_overrides[get_admin_org_id] = lambda: _ORG
    yield None
    app.dependency_overrides.pop(get_current_user, None)
    app.dependency_overrides.pop(get_admin_org_id, None)


@pytest.mark.parametrize(
    ("method", "url", "body", "template"),
    _ROUTES,
    ids=[f"{m} {t.removeprefix(_A)}" for m, _u, _b, t in _ROUTES],
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


def test_every_family_route_is_a_stub_or_a_named_python_only_route() -> None:
    served = {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith(_FAMILY_PREFIXES)
        for method in route.methods
        if method not in {"HEAD", "OPTIONS"}
    }
    expected = {(method, template) for method, _url, _body, template in _ROUTES}
    assert served - _PYTHON_ONLY == expected, sorted((served - _PYTHON_ONLY) ^ expected)
    assert _PYTHON_ONLY <= served
    assert len(expected) == 23


def test_the_admin_gate_still_runs_before_the_refusal() -> None:
    app.dependency_overrides.pop(get_admin_org_id, None)
    app.dependency_overrides.pop(get_current_user, None)
    response = _client.get(f"{_A}/settings/general")
    assert response.status_code in {401, 403}, response.text


def test_the_licence_gate_still_runs_before_the_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("dev_health_ops.licensing.gating.has_feature", lambda *_: False)
    response = _client.get(f"{_A}/retention-policies/resource-types")
    assert response.status_code == 402, response.text
