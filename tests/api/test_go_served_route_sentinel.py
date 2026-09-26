"""Request-level behavior of the CHAOS-6241 Go-served-route sentinel.

test_work_unit_explain_hardening.py pins one route's signature/decorator by
source inspection only; nothing committed sends a real request through any
of the 32 deleted bodies and asserts the sentinel actually fires. A review
round on this PR found exactly the gap that leaves: GET/POST /api/v1/home
still carried a vestigial, now-unused Postgres session dependency
(``semantic_session: AsyncSession = Depends(get_postgres_session_dep)``,
left over from the deleted body) that FastAPI resolves BEFORE the handler
runs -- if that dependency failed (e.g. a Postgres outage), the request got
a generic 500 with no ``rest.route_served_by_query_api`` event and no
diagnostic detail, the exact silent-failure shape this sentinel exists to
prevent. The dependency is now removed (main.py no longer needs a database
session for these routes at all); this module proves the sentinel fires for
real requests, unconditionally.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.main import app
from dev_health_ops.api.services.auth import AuthenticatedUser

client = TestClient(app, raise_server_exceptions=False)

_TEST_USER = AuthenticatedUser(
    "00000000-0000-4000-8000-000000000001",
    "sentinel-test@example.invalid",
    "70d529e0",
    "admin",
    is_superuser=True,
)


def _override_get_current_user() -> AuthenticatedUser:
    return _TEST_USER


@pytest.fixture(autouse=True)
def _authenticated() -> Iterator[None]:
    app.dependency_overrides[get_current_user] = _override_get_current_user
    yield
    app.dependency_overrides.pop(get_current_user, None)


# GET routes that need no request body -- the auth-gated set mirrors
# test_analytics_auth.py's list, plus the two public routes.
_GET_ROUTES = [
    "/api/v1/home",
    "/api/v1/explain?metric=cycle_time",
    "/api/v1/heatmap?type=team&metric=commits",
    "/api/v1/work-units",
    "/api/v1/flame?entity_type=repo&entity_id=test",
    "/api/v1/flame/aggregated?mode=cycle",
    "/api/v1/quadrant?type=churn_throughput",
    "/api/v1/drilldown/prs",
    "/api/v1/drilldown/issues",
    "/api/v1/people",
    "/api/v1/opportunities",
    "/api/v1/investment",
    "/api/v1/investment/sunburst",
    "/api/v1/sankey",
    "/api/v1/filters/options",
    "/api/v1/meta",
]


@pytest.mark.parametrize("path", _GET_ROUTES)
def test_get_route_fires_the_sentinel(path: str, caplog: pytest.LogCaptureFixture):
    """Every deleted GET route body returns the diagnostic 500, always.

    No 401 (auth, where required, already passed via the fixture above), no
    200/404 from stale logic, and no generic unlabelled 500 -- the response
    detail names the path and the structured ERROR event is emitted.
    """
    with caplog.at_level(logging.ERROR, logger="dev_health_ops.api.go_served"):
        resp = client.get(path)
    route = path.split("?", 1)[0]
    assert resp.status_code == 500, f"GET {path} = {resp.status_code}, want 500"
    detail = resp.json().get("detail", "")
    assert route in detail and "query-api" in detail, (
        f"GET {path} detail missing path/query-api diagnostic: {detail!r}"
    )
    assert any(
        r.message == "rest.route_served_by_query_api"
        and getattr(r, "path", None) == route
        for r in caplog.records
    ), f"GET {path} did not emit rest.route_served_by_query_api for {route}"


def test_post_home_fires_the_sentinel_not_masked_by_any_dependency(
    caplog: pytest.LogCaptureFixture,
):
    """The exact regression a review round found: POST /api/v1/home must
    reach the sentinel on a normal authenticated request with a valid body,
    the same as every other deleted route -- not a generic 500 from some
    other dependency resolving first.
    """
    with caplog.at_level(logging.ERROR, logger="dev_health_ops.api.go_served"):
        resp = client.post("/api/v1/home", json={"filters": {}})
    assert resp.status_code == 500, resp.text
    detail = resp.json().get("detail", "")
    assert "/api/v1/home" in detail and "query-api" in detail, detail
    assert any(
        r.message == "rest.route_served_by_query_api"
        and getattr(r, "path", None) == "/api/v1/home"
        for r in caplog.records
    ), "POST /api/v1/home did not emit rest.route_served_by_query_api"


def test_get_home_unauthenticated_still_401s():
    """Auth gating must run before the sentinel -- an unauthenticated
    request never reaches it."""
    app.dependency_overrides.pop(get_current_user, None)
    try:
        resp = client.get("/api/v1/home")
    finally:
        # A plain assignment, not a lambda literal, to keep the finally
        # block free of anything CodeQL's py/exit-from-finally check could
        # read as an early-exit statement.
        app.dependency_overrides[get_current_user] = _override_get_current_user
    assert resp.status_code == 401, resp.text


# CHAOS-6817: the 26 billing routes go-api owns (the manifest's billing paths and
# the static /plans/pull-stripe the ingress rule /plans/[^/]+$ also sends to Go).
# Every route keeps its signature and auth dependency, so a request that
# reaches the Python handler on an authenticated superadmin gets the 500
# diagnostic naming go-api, whatever the body is.
_UUID = "00000000-0000-4000-8000-0000000000aa"
_BILLING_ROUTES = [
    ("GET", "/api/v1/billing/plans", None),
    ("POST", "/api/v1/billing/plans", {"key": "k", "name": "n", "tier": "team"}),
    ("POST", "/api/v1/billing/plans/pull-stripe", None),
    ("GET", f"/api/v1/billing/plans/{_UUID}", None),
    ("PUT", f"/api/v1/billing/plans/{_UUID}", {}),
    ("DELETE", f"/api/v1/billing/plans/{_UUID}", None),
    ("POST", f"/api/v1/billing/plans/{_UUID}/sync-stripe", None),
    ("GET", "/api/v1/billing/invoices", None),
    ("GET", f"/api/v1/billing/invoices/{_UUID}", None),
    ("POST", f"/api/v1/billing/invoices/{_UUID}/void", None),
    ("GET", "/api/v1/billing/refunds", None),
    ("POST", "/api/v1/billing/refunds", {"invoice_id": _UUID}),
    ("GET", f"/api/v1/billing/refunds/{_UUID}", None),
    (
        "POST",
        "/api/v1/billing/checkout",
        {
            "tier": "team",
            "success_url": "https://x.test/ok",
            "cancel_url": "https://x.test/no",
        },
    ),
    ("POST", "/api/v1/billing/portal", None),
    ("GET", f"/api/v1/billing/entitlements/{_UUID}", None),
    ("GET", "/api/v1/billing/subscriptions", None),
    ("GET", "/api/v1/billing/subscriptions/list", None),
    ("GET", "/api/v1/billing/subscriptions/history", None),
    ("POST", "/api/v1/billing/subscriptions/change-plan", {"price_id": "price_1"}),
    ("POST", "/api/v1/billing/subscriptions/cancel", {}),
    ("POST", "/api/v1/billing/subscriptions/reactivate", None),
    ("GET", f"/api/v1/billing/audit?org_id={_UUID}", None),
    ("GET", f"/api/v1/billing/audit/{_UUID}", None),
    ("POST", f"/api/v1/billing/audit/{_UUID}/resolve", {"resolution": "ok"}),
    ("POST", "/api/v1/billing/reconcile", None),
]


@pytest.mark.parametrize(("method", "path", "body"), _BILLING_ROUTES)
def test_billing_route_fires_the_go_api_sentinel(
    method: str, path: str, body: dict | None, caplog: pytest.LogCaptureFixture
):
    """A deleted billing body answers 500 naming go-api and logs the event."""
    with caplog.at_level(logging.ERROR, logger="dev_health_ops.api.go_served"):
        resp = client.request(method, path, json=body)
    assert resp.status_code == 500, f"{method} {path} = {resp.status_code} {resp.text}"
    detail = resp.json().get("detail", "")
    assert "go-api" in detail and "no Python implementation" in detail, detail
    assert any(
        r.message == "rest.route_served_by_go_api"
        and getattr(r, "plane", None) == "go-api"
        for r in caplog.records
    ), f"{method} {path} did not emit rest.route_served_by_go_api"


def test_the_go_api_stub_names_the_path_it_was_reached_on(
    caplog: pytest.LogCaptureFixture,
):
    with caplog.at_level(logging.ERROR, logger="dev_health_ops.api.go_served"):
        resp = client.get("/api/v1/billing/plans")
    assert "/api/v1/billing/plans" in resp.json()["detail"]
    assert any(
        getattr(r, "path", None) == "/api/v1/billing/plans" for r in caplog.records
    )


def test_a_billing_route_with_an_auth_dependency_still_401s_first():
    app.dependency_overrides.pop(get_current_user, None)
    try:
        resp = client.post("/api/v1/billing/checkout", json={"tier": "team"})
    finally:
        app.dependency_overrides[get_current_user] = _override_get_current_user
    assert resp.status_code == 401, resp.text


@pytest.mark.parametrize(
    ("path", "method", "starts"),
    [
        ("/api/v1/billing/checkout", "post", "Create a Stripe Checkout session"),
        ("/api/v1/billing/portal", "post", "Create a Stripe Billing Portal session"),
    ],
)
def test_a_stub_keeps_the_openapi_description_its_docstring_gave(
    path: str, method: str, starts: str
):
    """Reducing a body to the refusal must not change the route's OpenAPI entry:
    the description is the handler's docstring, so the stub keeps it (r1 of the
    sync-admin deletion found the same loss; checked here for the billing stubs)."""
    description = app.openapi()["paths"][path][method].get("description", "")
    assert description.startswith(starts), description
