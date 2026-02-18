from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.connectors.utils.graphql import (
    GitHubGraphQLClient,
    _github_reset_delay_seconds,
)


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._json_data


def test_github_reset_delay_seconds_parses_header(monkeypatch):
    response = _FakeResponse(
        status_code=403,
        headers={"X-RateLimit-Reset": "110"},
    )
    monkeypatch.setattr("dev_health_ops.connectors.utils.graphql.time.time", lambda: 100)

    assert _github_reset_delay_seconds(response) == pytest.approx(10.0)


def test_github_reset_delay_seconds_invalid_or_missing_header():
    missing = _FakeResponse(status_code=403)
    invalid = _FakeResponse(status_code=403, headers={"X-RateLimit-Reset": "abc"})

    assert _github_reset_delay_seconds(missing) is None
    assert _github_reset_delay_seconds(invalid) is None


def test_query_success(monkeypatch):
    response = _FakeResponse(status_code=200, json_data={"data": {"ok": True}})
    post = MagicMock(return_value=response)
    monkeypatch.setattr("dev_health_ops.connectors.utils.graphql.requests.post", post)

    client = GitHubGraphQLClient("token-123", timeout=12)
    result = client.query("query { viewer { login } }", variables={"x": 1})

    assert result == {"ok": True}
    post.assert_called_once()
    _, kwargs = post.call_args
    assert kwargs["timeout"] == 12
    assert kwargs["headers"]["Authorization"] == "Bearer token-123"
    assert kwargs["json"]["variables"] == {"x": 1}


def test_query_unauthorized_raises_authentication_exception(monkeypatch):
    response = _FakeResponse(status_code=401, text="nope")
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.graphql.requests.post",
        MagicMock(return_value=response),
    )

    client = GitHubGraphQLClient("bad-token")
    with pytest.raises(AuthenticationException):
        client.query("query { viewer { login } }")


def test_query_rate_limit_raises_with_retry_after(monkeypatch):
    response = _FakeResponse(
        status_code=403,
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "105"},
        text="rate limit",
    )
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.graphql.requests.post",
        MagicMock(return_value=response),
    )
    monkeypatch.setattr("dev_health_ops.connectors.utils.graphql.time.time", lambda: 100)
    monkeypatch.setattr("dev_health_ops.connectors.utils.retry.time.sleep", lambda *_: None)

    client = GitHubGraphQLClient("token")
    with pytest.raises(RateLimitException) as exc_info:
        client.query("query { viewer { login } }")

    assert exc_info.value.retry_after_seconds == pytest.approx(5.0)


def test_query_graphql_errors_raise_api_exception(monkeypatch):
    response = _FakeResponse(
        status_code=200,
        json_data={"errors": [{"message": "boom"}]},
    )
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.graphql.requests.post",
        MagicMock(return_value=response),
    )
    monkeypatch.setattr("dev_health_ops.connectors.utils.retry.time.sleep", lambda *_: None)

    client = GitHubGraphQLClient("token")
    with pytest.raises(APIException, match="GraphQL errors"):
        client.query("query { viewer { login } }")


def test_get_blame_passes_expected_variables(monkeypatch):
    client = GitHubGraphQLClient("token")
    query = MagicMock(return_value={"repository": {"object": {"blame": {}}}})
    monkeypatch.setattr(client, "query", query)

    result = client.get_blame("octo", "repo", "src/main.py", ref="main")

    assert result == {"repository": {"object": {"blame": {}}}}
    query.assert_called_once()
    _, kwargs = query.call_args
    assert kwargs == {}
    assert query.call_args.args[1] == {
        "owner": "octo",
        "repo": "repo",
        "path": "src/main.py",
        "ref": "main",
    }
