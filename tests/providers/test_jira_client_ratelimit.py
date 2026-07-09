from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from dev_health_ops.connectors.utils.rate_limit_queue import RateLimitGate
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers.jira.client import JiraAuth, JiraClient


class _FakeJira429Response(requests.Response):
    """Minimal requests.Response subclass that simulates a 429 reply."""

    def __init__(self, *, reset_at: str | None = None) -> None:
        super().__init__()
        self.status_code = 429
        self.headers["Retry-After"] = "2"
        if reset_at is not None:
            self.headers["X-RateLimit-Reset"] = reset_at
        self._content = b"rate limited"

    def raise_for_status(self) -> None:
        error = requests.HTTPError("429 Client Error")
        error.response = self
        raise error

    def json(self, **kwargs: Any) -> Any:
        return {}


class _FakeJira200Response(requests.Response):
    """Minimal requests.Response subclass that simulates a successful reply."""

    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__()
        self.status_code = 200
        self._content = json.dumps(payload).encode("utf-8")


def _make_client(gate: Any, **kwargs: Any) -> JiraClient:
    return JiraClient(
        auth=JiraAuth(
            base_url="https://example.atlassian.net",
            email="bot@example.com",
            api_token="token",
        ),
        gate=gate,
        **kwargs,
    )


def test_jira_search_page_retries_after_429_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A single 429 must back off through the shared gate and retry, NOT abort the
    # whole sync (CHAOS-2463). The bounded retry lives only in _request_json, so
    # there is exactly one retry owner (no decorator x gate stampede).
    gate = MagicMock(spec=RateLimitGate)
    gate.penalize.return_value = 2.0
    client = _make_client(gate)
    payload = {"issues": [], "total": 0}
    mock_get = MagicMock(
        side_effect=[_FakeJira429Response(), _FakeJira200Response(payload)]
    )
    monkeypatch.setattr(client.session, "get", mock_get)

    result = client.search_issues_page(jql="project = ENG", start_at=0, max_results=50)

    assert result == payload
    assert mock_get.call_count == 2
    assert gate.wait_sync.call_count == 2
    gate.penalize.assert_called_once_with(2.0)
    gate.reset.assert_called_once()


def test_jira_search_page_persistent_429_exhausts_retries_and_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # When 429s never clear, the request is penalized once per attempt and then
    # surfaces as a RateLimitException carrying the server Retry-After so the
    # worker layer can defer (instead of a bare HTTPError that drops the delay).
    gate = MagicMock(spec=RateLimitGate)
    gate.penalize.return_value = 2.0
    client = _make_client(gate, max_retries_429=2)
    mock_get = MagicMock(
        return_value=_FakeJira429Response(reset_at="2025-10-08T15:00:00Z")
    )
    monkeypatch.setattr(client.session, "get", mock_get)

    with pytest.raises(RateLimitException) as exc_info:
        client.search_issues_page(jql="project = ENG", start_at=0, max_results=50)

    # The Retry-After header ("2") is preserved on the exception.
    assert exc_info.value.retry_after_seconds == 2.0
    assert mock_get.call_count == 3
    assert gate.wait_sync.call_count == 3
    assert gate.penalize.call_count == 3
    gate.reset.assert_not_called()

    # CHAOS-2758: X-RateLimit-Reset is an ISO 8601 timestamp for Jira (verified
    # against Atlassian's Cloud rate-limiting docs), not epoch seconds -- the
    # signal must parse it as such rather than silently degrading to None.
    signal = exc_info.value.signal
    assert signal is not None
    assert signal.reset_at == datetime(2025, 10, 8, 15, 0, 0, tzinfo=timezone.utc)
