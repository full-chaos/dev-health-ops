"""Tests for the push CLI's httpx retry/backoff wrapper (CHAOS-2700 brief
decision 2, master-spec CC16/CC29): retries ALL 503s regardless of error
code, plus 429 (honoring Retry-After) and network errors; does NOT retry
4xx contract errors.

Uses `httpx.MockTransport` (built into httpx, no new test dependency) per
the brief's test plan. `asyncio.sleep` inside the shared
`connectors.utils.retry` decorator is patched to a no-op so these tests run
fast regardless of the CLI's real backoff delays.
"""

from __future__ import annotations

import json

import httpx
import pytest

from dev_health_ops.connectors.utils import retry as retry_module
from dev_health_ops.push.http_client import (
    IngestApiError,
    IngestClientConfig,
    IngestTransientError,
    _parse_retry_after,
    _request,
    post_batch,
    redact_secrets,
)


@pytest.fixture(autouse=True)
def _fast_sleep(monkeypatch: pytest.MonkeyPatch):
    async def _noop_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(retry_module.asyncio, "sleep", _noop_sleep)


def _config() -> IngestClientConfig:
    return IngestClientConfig(
        api_url="http://test", token="fcpush_test", org_id="org-1"
    )


@pytest.mark.asyncio
async def test_retries_503_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                503, json={"error": {"code": "stream_unavailable", "message": "down"}}
            )
        return httpx.Response(
            202,
            json={
                "ingestionId": "abc",
                "status": "accepted",
                "itemsReceived": 1,
                "stream": "s",
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(client, "POST", "http://test/x")

    assert response.status_code == 202
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_retries_503_regardless_of_error_code() -> None:
    """Master-spec CC16/CC29: retry ALL 503s -- the predicate keys on status
    class, never the code string (ingest_temporarily_unavailable and
    auth_not_configured are both 503s the server can legitimately return)."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                503, json={"error": {"code": "auth_not_configured", "message": "nope"}}
            )
        return httpx.Response(202, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(client, "POST", "http://test/x")

    assert response.status_code == 202
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_retry_after_header_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(retry_module.asyncio, "sleep", _record_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                503,
                headers={"Retry-After": "7"},
                json={"error": {"code": "stream_unavailable", "message": "down"}},
            )
        return httpx.Response(202, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(client, "POST", "http://test/x")

    assert response.status_code == 202
    # The mocked clock/sleep was called with the header's value, not the
    # exponential-backoff default (initial_delay=1.0).
    assert sleep_calls == [7.0]


@pytest.mark.asyncio
async def test_429_retried_and_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                429, json={"error": {"code": "rate_limited", "message": "slow down"}}
            )
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(client, "GET", "http://test/x")

    assert response.status_code == 200
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_409_not_retried_and_surfaces_body() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            409, json={"error": {"code": "idempotency_conflict", "message": "conflict"}}
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestApiError) as exc_info:
            await _request(client, "POST", "http://test/x")

    assert calls["n"] == 1
    assert exc_info.value.status_code == 409
    assert exc_info.value.code == "idempotency_conflict"


@pytest.mark.asyncio
async def test_400_not_retried_and_surfaces_errors() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": "invalid_envelope",
                    "message": "bad",
                    "errors": [
                        {
                            "index": 0,
                            "kind": None,
                            "code": "x",
                            "message": "y",
                            "path": None,
                        }
                    ],
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestApiError) as exc_info:
            await _request(client, "POST", "http://test/x")

    assert calls["n"] == 1
    assert exc_info.value.status_code == 400
    assert exc_info.value.errors[0]["index"] == 0


@pytest.mark.asyncio
async def test_connection_error_exhausts_retries_then_raises() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("boom", request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestTransientError):
            await _request(client, "GET", "http://test/x")

    # max_retries=5 (CLI-tuned params, brief decision 2)
    assert calls["n"] == 5


@pytest.mark.asyncio
async def test_post_batch_sends_auth_and_idempotency_headers() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(
            202,
            json={
                "ingestionId": "id-1",
                "status": "accepted",
                "itemsReceived": 1,
                "stream": "s",
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        status_code, body = await post_batch(
            client, _config(), b"{}", idempotency_key="abc-key"
        )

    assert status_code == 202
    assert body["ingestionId"] == "id-1"
    assert seen["authorization"] == "Bearer fcpush_test"
    assert seen["idempotency-key"] == "abc-key"
    assert seen["x-org-id"] == "org-1"
    # Token must never appear anywhere but the Authorization header value.
    assert "fcpush_test" not in str(body)


# ---------------------------------------------------------------------------
# redact_secrets / Retry-After clamp (Codex adversarial-review findings)
# ---------------------------------------------------------------------------


def test_redact_secrets_strips_token_after_bearer_prefix() -> None:
    # The bearer-credential pattern matches the whole "Bearer <token>" span
    # here (it's the more specific/greedy match), so the raw secret is gone
    # -- which marker text wins on this overlapping case is an
    # implementation detail, not the security property under test.
    text = "upstream debug page echoed Authorization: Bearer fcpush_abc123XYZ-_ back"

    redacted = redact_secrets(text)

    assert "fcpush_abc123XYZ-_" not in redacted


def test_redact_secrets_strips_standalone_ingest_token() -> None:
    text = "rejected request, token=fcpush_abc123XYZ-_ is invalid"

    redacted = redact_secrets(text)

    assert "fcpush_abc123XYZ-_" not in redacted
    assert "fcpush_[REDACTED]" in redacted


def test_redact_secrets_strips_generic_bearer() -> None:
    text = "Authorization: Bearer some-other-opaque-credential"

    redacted = redact_secrets(text)

    assert "some-other-opaque-credential" not in redacted
    assert "Bearer [REDACTED]" in redacted


def test_redact_secrets_leaves_ordinary_text_untouched() -> None:
    text = "batch has 3 records; max is 1000"

    assert redact_secrets(text) == text


@pytest.mark.asyncio
async def test_reflected_token_in_error_body_is_redacted_before_raising() -> None:
    """A misconfigured proxy echoing the Authorization header into an error
    body must never surface the raw token in the raised exception -- which
    is exactly what the shared retry decorator logs on every attempt."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": "invalid_envelope",
                    "message": "rejected request with Authorization: Bearer fcpush_leaked1234",
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestApiError) as exc_info:
            await _request(client, "POST", "http://test/x")

    assert "fcpush_leaked1234" not in exc_info.value.message
    assert "fcpush_leaked1234" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_reflected_token_in_non_message_error_field_is_redacted() -> None:
    """Round-2 finding: redaction must cover EVERY string field in an error
    item, not just a hardcoded "message" key -- a non-conforming proxy
    response could reflect the token through any field name."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": "invalid_envelope",
                    "message": "bad",
                    "errors": [
                        {
                            "index": 0,
                            "kind": None,
                            "code": "x",
                            "message": "y",
                            "path": None,
                            "debug": "Authorization: Bearer fcpush_hidden5678",
                        }
                    ],
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestApiError) as exc_info:
            await _request(client, "POST", "http://test/x")

    assert "fcpush_hidden5678" not in str(exc_info.value.errors)


@pytest.mark.asyncio
async def test_reflected_token_in_top_level_error_code_is_redacted() -> None:
    """Round-3 finding: the top-level `error.code` string was copied
    unredacted -- `_raise_for_response` formats it directly into the
    exception string the shared retry decorator logs on every attempt."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": "Authorization: Bearer fcpush_codeleak9999",
                    "message": "bad request",
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestApiError) as exc_info:
            await _request(client, "POST", "http://test/x")

    assert "fcpush_codeleak9999" not in exc_info.value.code
    assert "fcpush_codeleak9999" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_reflected_token_in_retryable_503_code_is_redacted() -> None:
    """Same bypass on the 503-retry path, which is what the shared retry
    decorator actually logs on every attempt (the original high finding's
    exact concern)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            json={
                "error": {
                    "code": "Authorization: Bearer fcpush_retrycode123",
                    "message": "unavailable",
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestTransientError) as exc_info:
            await _request(client, "GET", "http://test/x")

    assert "fcpush_retrycode123" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_reflected_token_in_error_item_dict_key_is_redacted() -> None:
    """Round-4 finding: `_redact_value` sanitized dict VALUES but passed
    KEYS through unchanged -- a malformed error item could echo a
    credential as a key name, e.g. ``{"Authorization: Bearer fcpush_...":
    "x"}``, leaking it into `IngestApiError.errors` and `--json` output."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": "invalid_envelope",
                    "message": "bad",
                    "errors": [
                        {"Authorization: Bearer fcpush_keyleak0000": "unexpected"}
                    ],
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(IngestApiError) as exc_info:
            await _request(client, "POST", "http://test/x")

    assert "fcpush_keyleak0000" not in str(exc_info.value.errors)
    assert "fcpush_keyleak0000" not in json.dumps(exc_info.value.errors)


def test_retry_after_numeric_clamped_to_max_delay() -> None:
    response = httpx.Response(503, headers={"Retry-After": "86400"})

    assert _parse_retry_after(response) == 30.0  # CLI's own max_delay


def test_retry_after_negative_falls_back_to_none() -> None:
    response = httpx.Response(503, headers={"Retry-After": "-5"})

    assert _parse_retry_after(response) is None


def test_retry_after_infinite_falls_back_to_none() -> None:
    response = httpx.Response(503, headers={"Retry-After": "inf"})

    assert _parse_retry_after(response) is None


def test_retry_after_nan_falls_back_to_none() -> None:
    response = httpx.Response(503, headers={"Retry-After": "nan"})

    assert _parse_retry_after(response) is None


def test_retry_after_http_date_falls_back_to_none() -> None:
    response = httpx.Response(
        503, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}
    )

    assert _parse_retry_after(response) is None


def test_retry_after_small_value_passed_through() -> None:
    response = httpx.Response(503, headers={"Retry-After": "3"})

    assert _parse_retry_after(response) == 3.0


@pytest.mark.asyncio
async def test_huge_retry_after_sleep_is_clamped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_calls: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(retry_module.asyncio, "sleep", _record_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                503,
                headers={"Retry-After": "86400"},
                json={"error": {"code": "stream_unavailable", "message": "down"}},
            )
        return httpx.Response(202, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(client, "POST", "http://test/x")

    assert response.status_code == 202
    assert sleep_calls == [30.0]  # clamped, not the raw 86400s header value
