"""
Tests for work-unit explain endpoint hardening (CHAOS-2351).

Covers:
  (a) Missing-key / invalid-provider returns 4xx JSON (not HTTP 200 + broken stream).
  (b) Rate-limit decorator is present on the endpoint.
"""

from __future__ import annotations

import inspect
import os
from unittest.mock import AsyncMock, patch

os.environ.setdefault("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-explain-hardening")
os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

from fastapi.testclient import TestClient  # noqa: E402

from dev_health_ops.api.auth.router import get_current_user  # noqa: E402
from dev_health_ops.api.main import app  # noqa: E402
from dev_health_ops.api.services.auth import AuthenticatedUser  # noqa: E402

# ---------------------------------------------------------------------------
# Shared auth override
# ---------------------------------------------------------------------------

_FAKE_USER = AuthenticatedUser(
    user_id="user-test-001",
    email="test@example.com",
    org_id="org-test-001",
    role="member",
    is_superuser=False,
)


def _override_get_current_user():
    return _FAKE_USER


# ---------------------------------------------------------------------------
# (a) Provider credential validation returns 4xx before stream opens
# ---------------------------------------------------------------------------


def test_missing_openai_key_returns_422_not_200(monkeypatch):
    """Explicit provider with no API key must return 422 JSON, not 200 + broken stream.

    Before the fix, get_provider() raised ValueError inside keep_alive_wrapper
    after the StreamingResponse headers were already sent, so the client saw
    HTTP 200 followed by a JSON error body — indistinguishable from a valid
    (empty) response.  After the fix, the ValueError is caught before
    StreamingResponse is constructed and mapped to HTTP 422.
    """
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("LOCAL_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("QWEN_API_KEY", raising=False)
    monkeypatch.delenv("OLLAMA_MODEL", raising=False)
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    monkeypatch.delenv("LLM_PROVIDER", raising=False)

    app.dependency_overrides[get_current_user] = _override_get_current_user
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.post(
                "/api/v1/work-units/wu-abc123/explain",
                params={"llm_provider": "openai"},
            )
    finally:
        app.dependency_overrides.pop(get_current_user, None)

    # Must be a proper 4xx, not 200
    assert resp.status_code == 422, (
        f"Expected 422 for missing OPENAI_API_KEY, got {resp.status_code}. "
        "Provider validation must happen before StreamingResponse is opened."
    )
    body = resp.json()
    assert "detail" in body
    assert "OPENAI_API_KEY" in body["detail"]


def test_missing_anthropic_key_returns_422_not_200(monkeypatch):
    """Explicit anthropic provider with no key must return 422 JSON."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_PROVIDER", raising=False)

    app.dependency_overrides[get_current_user] = _override_get_current_user
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.post(
                "/api/v1/work-units/wu-abc123/explain",
                params={"llm_provider": "anthropic"},
            )
    finally:
        app.dependency_overrides.pop(get_current_user, None)

    assert resp.status_code == 422, (
        f"Expected 422 for missing ANTHROPIC_API_KEY, got {resp.status_code}"
    )
    body = resp.json()
    assert "ANTHROPIC_API_KEY" in body["detail"]


def test_unknown_provider_returns_422_not_200():
    """Unknown provider name must return 422 JSON, not 200 + broken stream."""
    app.dependency_overrides[get_current_user] = _override_get_current_user
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.post(
                "/api/v1/work-units/wu-abc123/explain",
                params={"llm_provider": "nonexistent-provider-xyz"},
            )
    finally:
        app.dependency_overrides.pop(get_current_user, None)

    assert resp.status_code == 422, (
        f"Expected 422 for unknown provider, got {resp.status_code}"
    )
    body = resp.json()
    assert "detail" in body


def test_missing_key_response_is_json_not_stream():
    """The 4xx error response must be a JSON object, not a streaming body."""
    app.dependency_overrides[get_current_user] = _override_get_current_user
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.post(
                "/api/v1/work-units/wu-abc123/explain",
                params={"llm_provider": "nonexistent-provider-xyz"},
            )
    finally:
        app.dependency_overrides.pop(get_current_user, None)

    # Must be parseable as JSON with a "detail" key (FastAPI HTTPException shape)
    body = resp.json()
    assert isinstance(body, dict)
    assert "detail" in body


def test_mock_provider_with_no_work_unit_returns_404():
    """With a valid provider (mock) but non-existent work unit, expect 404.

    This confirms the happy-path flow still works: provider validation passes,
    then the DB lookup returns 404 for an unknown work_unit_id.
    """
    app.dependency_overrides[get_current_user] = _override_get_current_user
    try:
        with (
            patch(
                "dev_health_ops.api.main.build_work_unit_investments",
                new_callable=AsyncMock,
                return_value=[],
            ),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            resp = client.post(
                "/api/v1/work-units/nonexistent-wu/explain",
                params={"llm_provider": "mock"},
            )
    finally:
        app.dependency_overrides.pop(get_current_user, None)

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# (b) Rate-limit decorator is present
# ---------------------------------------------------------------------------


def test_work_unit_explain_endpoint_has_rate_limit_decorator():
    """work_unit_explain_endpoint must carry @limiter.limit('20/minute').

    Uses source inspection — the same technique used in test_login_rate_limit.py
    — because slowapi wraps the handler at decoration time, making runtime
    reflection unreliable.
    """
    from dev_health_ops.api import main as main_module

    source = inspect.getsource(main_module)

    # Find the block around work_unit_explain_endpoint
    # The decorator must appear between the @app.post and the async def.
    endpoint_idx = source.find("async def work_unit_explain_endpoint(")
    assert endpoint_idx != -1, "work_unit_explain_endpoint not found in main.py"

    # Look at the 300 chars before the def for the decorator
    preamble = source[max(0, endpoint_idx - 300) : endpoint_idx]
    assert '@limiter.limit("20/minute")' in preamble, (
        "work_unit_explain_endpoint is missing @limiter.limit('20/minute'). "
        "Authenticated callers can amplify LLM spend without a rate limit."
    )


def test_work_unit_explain_endpoint_accepts_request_param():
    """work_unit_explain_endpoint must accept a Request parameter for slowapi.

    slowapi's @limiter.limit decorator requires the handler to accept a
    fastapi.Request argument so it can extract the client IP / key.
    """
    import inspect as _inspect

    from dev_health_ops.api.main import work_unit_explain_endpoint

    sig = _inspect.signature(work_unit_explain_endpoint)
    assert "request" in sig.parameters, (
        "work_unit_explain_endpoint must have a 'request: Request' parameter "
        "for the slowapi rate limiter to function."
    )
