"""CHAOS-3273 L5: production-stack credential-confusion rejection tests.

CHAOS-3271 background: ops' global middleware (``OrgIdMiddleware``,
``ImpersonationMiddleware``) runs every ``Authorization: Bearer`` value
through the ops-JWT decoder before a route ever sees it. The shipped fix is
ONE hardcoded path-prefix exemption for ``/api/v1/internal/acr/*``
(``api/middleware/__init__.py:85-89``) -- not credential-class dispatch, so
every OTHER non-JWT bearer class (worker-bridge secrets, ``svc_acr_*``
internal service tokens, ...) still transits that decoder on every request.
A wrong-class bearer does not error there: ``InvalidTokenError`` is caught
and swallowed (``api/services/auth.py`` ``validate_token``), degrading the
request to anonymous rather than raising.

These tests build the REAL FastAPI app (``dev_health_ops.api.main.app``) and
drive it through ``TestClient`` -- the full middleware order
(CorrelationId -> OrgId -> Impersonation -> SlowAPI ->
OriginValidation/CSRF -> GraphQLQuerySizeLimit -> SecurityHeaders -> CORS ->
route), exactly as production assembles it in ``_middleware.py``. No
middleware and no auth dependency is mocked or overridden anywhere in this
file -- only unrelated business-logic functions (a Celery-style ``.run()``
call) are patched, the same pattern already used in
``tests/api/test_worker_operational_bridge.py``. A test here that could pass
against a version of the code where the JWT decoder RAISES instead of
degrading, or where a route's own credential check silently accepted a
foreign class, would be a test that proves nothing -- every assertion below
is chosen so that regressing the specific line named in its comment turns it
red.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app
from dev_health_ops.api.services.auth import AuthService
from dev_health_ops.api.services.impersonation_cache import (
    CachedImpersonationSession,
)


@pytest.fixture(autouse=True)
def _fresh_auth_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force ``get_auth_service()`` to rebuild from THIS test's env.

    ``get_auth_service()`` caches a process-global singleton on first call
    (``services/auth.py:461-463``). Without this reset a JWT_SECRET_KEY
    read by an earlier test in the same process could leak into this one,
    which would silently change whether a forged token verifies.
    """
    monkeypatch.setattr("dev_health_ops.api.services.auth._auth_service", None)


@pytest.fixture
def client() -> TestClient:
    """Real app, real middleware stack, zero dependency overrides."""
    return TestClient(app, raise_server_exceptions=False)


def _forged_ops_jwt(**overrides: object) -> str:
    """A syntactically valid ops access-JWT signed with the WRONG secret.

    Built through the real ``AuthService.create_access_token`` (production
    code, not a hand-rolled ``jwt.encode``) so the claim shape matches a
    genuine token exactly -- only the signing key differs from what the
    running app reads from ``JWT_SECRET_KEY``. Decodable as a JWT
    (three dot-separated segments), so it reaches ``jwt.decode`` in
    ``validate_token`` and fails there on signature verification, not on
    basic shape.
    """
    forger = AuthService(secret_key="a-different-secret-the-app-never-uses!!")
    kwargs: dict[str, Any] = {
        "user_id": "00000000-0000-4000-8000-000000000099",
        "email": "attacker@example.com",
        "org_id": "00000000-0000-4000-8000-0000000000aa",
        "role": "member",
    }
    kwargs.update(overrides)
    return forger.create_access_token(**kwargs)


# ─── Opaque non-JWT bearer classes, for reuse across tests ──────────────────
_SVC_ACR_SHAPED_GARBAGE = "svc_acr_" + "a" * 43  # right prefix, unregistered
_OPAQUE_NON_JWT_GARBAGE = "not-a-jwt-at-all-just-a-bare-opaque-string"


class TestOpaqueCredentialDegradesToAnonymousNotError:
    """Pins api/services/auth.py's InvalidTokenError -> debug -> None path.

    What would have to regress for this to go red: the ``except
    InvalidTokenError`` in ``AuthService.validate_token`` starts
    re-raising (or a caller stops catching it) instead of returning
    ``None`` -- the request would then 500 instead of 401.
    """

    @pytest.mark.parametrize(
        "bearer_value",
        [_SVC_ACR_SHAPED_GARBAGE, _OPAQUE_NON_JWT_GARBAGE],
        ids=["svc_acr_shaped", "opaque_non_jwt"],
    )
    def test_non_jwt_bearer_is_cleanly_rejected_not_500(
        self, client: TestClient, bearer_value: str
    ) -> None:
        # GET /api/v1/admin/impersonate/status depends on nothing but
        # get_current_user -- no body, no admin-only gate, so a 401 here
        # can only come from auth failing, never from a downstream 403 or
        # business-logic error masking the real signal.
        response = client.get(
            "/api/v1/admin/impersonate/status",
            headers={"Authorization": f"Bearer {bearer_value}"},
        )
        assert response.status_code == 401
        assert response.json()["detail"] != ""

    def test_wrong_signature_ops_jwt_is_rejected_not_treated_as_valid(
        self, client: TestClient
    ) -> None:
        # A well-FORMED ops JWT (right claims, right library) signed with a
        # key the app never configured. Distinguishes "any 3-segment
        # dot-string is treated as a JWT and peeked at" (true, benign) from
        # "any 3-segment dot-string is ACCEPTED as authentication" (would be
        # the actual CHAOS-3271 breach) -- the latter must stay false.
        response = client.get(
            "/api/v1/admin/impersonate/status",
            headers={"Authorization": f"Bearer {_forged_ops_jwt()}"},
        )
        assert response.status_code == 401


class TestWorkerBridgeRouteRejectsForeignCredentialClasses:
    """Pins api/internal/worker_auth.py::authorize_worker_bridge.

    ``OrgIdMiddleware`` and ``ImpersonationMiddleware`` are NOT exempted
    for ``/api/internal/worker-operational/*`` (only the
    ``/api/v1/internal/acr/`` prefix is) -- every request here transits the
    ops-JWT decoder first regardless of outcome. What actually gates the
    route is ``authorize_worker_bridge``'s constant-time compare against
    ``WORKER_OPERATIONAL_BRIDGE_TOKEN``. These tests pin BOTH halves at
    once: a foreign bearer class must still be REJECTED by that compare
    (not accidentally accepted because the middleware's decode attempt
    already "used up" the credential), and the correct bridge secret must
    still be ACCEPTED despite the middleware's decode attempt failing on it
    first (not accidentally blocked). Regresses if
    ``authorize_worker_bridge`` starts accepting a second credential shape,
    or if the middleware's failed decode ever becomes fatal instead of
    silently swallowed.
    """

    _BODY = {
        "delivery_id": "00000000-0000-4000-8000-000000000012",
        "provider": "github",
        "event_type": "push",
    }

    @pytest.mark.parametrize(
        "bearer_value",
        [_SVC_ACR_SHAPED_GARBAGE, None],
        ids=["svc_acr_shaped", "forged_ops_jwt"],
    )
    def test_foreign_bearer_class_rejected(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch, bearer_value
    ) -> None:
        monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "the-real-bridge-secret")
        token = bearer_value if bearer_value is not None else _forged_ops_jwt()
        response = client.post(
            "/api/internal/worker-operational/webhook",
            headers={"Authorization": f"Bearer {token}"},
            json=self._BODY,
        )
        assert response.status_code == 401

    def test_correct_bridge_secret_still_works_despite_middleware_misdecode(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "the-real-bridge-secret")
        with patch(
            "dev_health_ops.api.internal.worker_operational.process_webhook_event.run",
            return_value={"status": "success"},
        ):
            response = client.post(
                "/api/internal/worker-operational/webhook",
                headers={"Authorization": "Bearer the-real-bridge-secret"},
                json=self._BODY,
            )
        assert response.status_code == 200


class _DBTouchSpy:
    """Records whether ``get_postgres_session()`` was ever entered.

    Codex review round 1 (auth-cp-L5-20260901T121335, finding 2): a status
    of ``401`` alone does NOT distinguish "the prefix check rejected this
    token before the DB" from "the prefix check was removed, the token
    reached the credential-store lookup, and a healthy empty store returned
    the SAME 401 via its own unknown-token branch". EXECUTED by codex
    against a permissive parser + a real (empty) credential table:
    ``status=401 detail=Unauthorized`` -- identical to the correct
    behaviour, so a status-only assertion cannot tell single-dispatch from
    "reaches the store and happens to still get rejected there". This spy
    makes the DISTINCT claim checkable: raising on ``__aenter__`` means any
    code path that reaches the DB fails loudly (never masquerades as a
    clean 401), and ``entered`` gives a direct assertion independent of
    that side effect.
    """

    def __init__(self) -> None:
        self.entered = 0

    def __call__(self) -> _DBTouchSpy:
        return self

    async def __aenter__(self) -> None:
        self.entered += 1
        raise AssertionError(
            "get_postgres_session() was entered for a bearer value "
            "_extract_token should have rejected before any DB round-trip"
        )

    async def __aexit__(self, *exc: object) -> bool:
        return False


class TestAcrRouteIsSingleDispatchWithinOps:
    """Pins api/internal/acr.py::_extract_token's prefix-only acceptance.

    acr's device-flow/web-assertion middleware in ITS OWN repo is genuine
    single-dispatch (``Authenticator.MiddlewareFor``): a request carrying
    both an ACR credential and an unrelated one is rejected outright. ops
    does not implement that pattern -- ``OrgIdMiddleware`` is exempted for
    this path prefix (``middleware/__init__.py:85-89``), but the ROUTE
    ITSELF still only accepts one shape (``svc_acr_``-prefixed). This test
    pins that narrower guarantee: presenting any other Authorization value
    to the acr-only route is rejected by the route's own check BEFORE the
    credential store is ever consulted -- not merely rejected somewhere,
    which a loosened prefix check could still achieve via the store's own
    unknown-token branch (see ``_DBTouchSpy`` above). Regresses if
    ``_extract_token``'s ``startswith(INTERNAL_SERVICE_TOKEN_PREFIX)``
    check is loosened (e.g. to accept any Bearer value and defer entirely
    to the DB lookup).
    """

    @pytest.mark.parametrize(
        "bearer_value",
        [
            None,
            "the-real-bridge-secret",
        ],  # None = forged_ops_jwt; second = worker_operational_bridge_token shape
        ids=["forged_ops_jwt", "worker_bridge_shaped"],
    )
    def test_non_svc_acr_bearer_rejected_by_acr_route(
        self, client: TestClient, bearer_value, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spy = _DBTouchSpy()
        monkeypatch.setattr("dev_health_ops.api.internal.acr.get_postgres_session", spy)
        token = bearer_value if bearer_value is not None else _forged_ops_jwt()
        response = client.get(
            "/api/v1/internal/acr/health",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 401
        assert spy.entered == 0

    def test_missing_authorization_rejected(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spy = _DBTouchSpy()
        monkeypatch.setattr("dev_health_ops.api.internal.acr.get_postgres_session", spy)
        response = client.get("/api/v1/internal/acr/health")
        assert response.status_code == 401
        assert spy.entered == 0


class TestUnverifiedSuperuserPeekIsNotAuthoritative:
    """Pins middleware/impersonation.py::_may_be_superuser's non-authority.

    ``_may_be_superuser`` is a documented FAST PATH: it base64-decodes the
    JWT payload segment WITHOUT verifying the signature, purely to decide
    whether to bother calling the real, fully-verified ``_extract_user()``.
    A forged token with ``is_superuser: true`` in its (unsigned-as-far-as-
    this-check-cares) payload fools the peek every time by design -- the
    guarantee that matters is that ``_extract_user()``'s real signature
    check is what's authoritative, so the forgery still resolves to "not
    a superuser, not impersonating, no elevated context; try to
    authenticate this request the ordinary way and fail". Regresses if a
    future optimization ever short-circuits on ``_may_be_superuser`` alone
    (e.g. treats it as an authoritative admin signal) instead of always
    falling through to ``_extract_user()``.
    """

    def test_forged_superuser_claim_does_not_grant_impersonation_context(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex review round 1 (auth-cp-L5-20260901T121335, finding 1):
        # asserting only the status code leaves this test insensitive to
        # the actual regression it names. ImpersonationMiddleware adds its
        # response headers ONLY when an ACTIVE session exists for the
        # (unverified) admin_user_id it read from the peek -- with no
        # session seeded, "the peek became authoritative" and "no session
        # was found" are indistinguishable, so a header-absence assertion
        # alone would pass by accident even under the regression. Seeding
        # an active session for the forged token's claimed user_id isolates
        # the claim to the peek's authority specifically. EXECUTED by
        # codex against a simulated regression with a session seeded the
        # same way: ``status=401 x-impersonating=true
        # x-impersonated-user-id=victim-user`` -- the headers leaked even
        # though the endpoint's own check still 401'd.
        admin_user_id = "00000000-0000-4000-8000-000000000099"
        forged = _forged_ops_jwt(is_superuser=True, user_id=admin_user_id)

        async def _fake_active_session(uid: str) -> CachedImpersonationSession | None:
            if uid != admin_user_id:
                return None
            return CachedImpersonationSession(
                id="test-session-id",
                admin_user_id=admin_user_id,
                target_user_id="11111111-1111-4111-8111-111111111111",
                target_org_id="22222222-2222-4222-8222-222222222222",
                target_role="member",
                target_email="victim@example.com",
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            )

        monkeypatch.setattr(
            "dev_health_ops.api.middleware.impersonation.get_active_session",
            _fake_active_session,
        )

        response = client.get(
            "/api/v1/admin/impersonate/status",
            headers={"Authorization": f"Bearer {forged}"},
        )
        # get_current_user runs its OWN full (verified) decode independent
        # of the middleware's peek -- forged signature means "no valid
        # user", so this must be a plain 401, not a 200 carrying any
        # impersonation state (which would mean the peek's unverified
        # is_superuser=True leaked into a real decision).
        assert response.status_code == 401
        # The side effect that matters even when the endpoint still 401s:
        # no impersonation context may leak onto the response, session
        # sitting ready to be found or not.
        assert "x-impersonating" not in response.headers
        assert "x-impersonated-user-id" not in response.headers
