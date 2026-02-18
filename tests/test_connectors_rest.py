from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
)
from dev_health_ops.connectors.utils.rest import (
    GitLabRESTClient,
    RESTClient,
    _parse_retry_after_seconds,
)


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        json_data: Any = None,
        headers: dict[str, str] | None = None,
        text: str = "",
        content: bytes | None = None,
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.headers = headers or {}
        self.text = text
        if content is not None:
            self.content = content
        elif json_data is None:
            self.content = b""
        else:
            self.content = b"x"

    def json(self) -> Any:
        return self._json_data


def test_parse_retry_after_seconds():
    assert _parse_retry_after_seconds(_FakeResponse(429, headers={"Retry-After": "3"})) == 3.0
    assert _parse_retry_after_seconds(_FakeResponse(429, headers={"Retry-After": "-2"})) == 0.0
    assert _parse_retry_after_seconds(_FakeResponse(429, headers={"Retry-After": "bad"})) is None
    assert _parse_retry_after_seconds(_FakeResponse(429, headers={})) is None


def test_restclient_get_success(monkeypatch):
    response = _FakeResponse(200, json_data={"ok": True})
    get = MagicMock(return_value=response)
    monkeypatch.setattr("dev_health_ops.connectors.utils.rest.requests.get", get)

    client = RESTClient("https://example.test", token="tok")
    data = client.get("/items", params={"a": 1})

    assert data == {"ok": True}
    get.assert_called_once()
    _, kwargs = get.call_args
    assert kwargs["params"] == {"a": 1}
    assert kwargs["headers"]["Authorization"] == "Bearer tok"


def test_restclient_get_401_raises_authentication_exception(monkeypatch):
    response = _FakeResponse(401, text="unauthorized")
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.rest.requests.get",
        MagicMock(return_value=response),
    )

    client = RESTClient("https://example.test", token="bad")
    with pytest.raises(AuthenticationException):
        client.get("items")


def test_restclient_get_429_then_success(monkeypatch):
    responses = [
        _FakeResponse(429, headers={"Retry-After": "0"}),
        _FakeResponse(200, json_data={"ok": True}),
    ]
    get = MagicMock(side_effect=responses)
    monkeypatch.setattr("dev_health_ops.connectors.utils.rest.requests.get", get)
    monkeypatch.setattr("dev_health_ops.connectors.utils.retry.time.sleep", lambda *_: None)

    client = RESTClient("https://example.test")
    data = client.get("items")

    assert data == {"ok": True}
    assert get.call_count == 2


def test_restclient_get_list_non_list_returns_empty(monkeypatch):
    response = _FakeResponse(200, json_data={"not": "a list"})
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.rest.requests.get",
        MagicMock(return_value=response),
    )

    client = RESTClient("https://example.test")
    assert client.get_list("items") == []


def test_restclient_delete_204_returns_empty_dict(monkeypatch):
    response = _FakeResponse(204, json_data=None, content=b"")
    delete = MagicMock(return_value=response)
    monkeypatch.setattr("dev_health_ops.connectors.utils.rest.requests.delete", delete)

    client = RESTClient("https://example.test")
    assert client.delete("items/1") == {}


def test_restclient_delete_returns_json_dict(monkeypatch):
    response = _FakeResponse(200, json_data={"deleted": True}, content=b"x")
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.rest.requests.delete",
        MagicMock(return_value=response),
    )

    client = RESTClient("https://example.test")
    assert client.delete("items/1") == {"deleted": True}


def test_restclient_delete_404_raises_api_exception(monkeypatch):
    response = _FakeResponse(404)
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.rest.requests.delete",
        MagicMock(return_value=response),
    )
    monkeypatch.setattr("dev_health_ops.connectors.utils.retry.time.sleep", lambda *_: None)

    client = RESTClient("https://example.test")
    with pytest.raises(APIException, match="Not found"):
        client.delete("items/does-not-exist")


def test_gitlab_get_file_blame_encodes_path(monkeypatch):
    client = GitLabRESTClient()
    get_list = MagicMock(return_value=[{"start": 1}])
    monkeypatch.setattr(client, "get_list", get_list)

    result = client.get_file_blame(10, "dir/file name.py", ref="release")

    assert result == [{"start": 1}]
    get_list.assert_called_once_with(
        "projects/10/repository/files/dir%2Ffile%20name.py/blame",
        params={"ref": "release"},
    )


def test_gitlab_get_merge_requests_passes_optional_ordering(monkeypatch):
    client = GitLabRESTClient()
    get_list = MagicMock(return_value=[])
    monkeypatch.setattr(client, "get_list", get_list)

    client.get_merge_requests(
        22,
        state="merged",
        page=2,
        per_page=50,
        order_by="updated_at",
        sort="desc",
    )

    get_list.assert_called_once_with(
        "projects/22/merge_requests",
        params={
            "state": "merged",
            "page": 2,
            "per_page": 50,
            "order_by": "updated_at",
            "sort": "desc",
        },
    )


def test_gitlab_get_dora_metrics_includes_date_range(monkeypatch):
    client = GitLabRESTClient()
    get_list = MagicMock(return_value=[])
    monkeypatch.setattr(client, "get_list", get_list)

    client.get_dora_metrics(
        99,
        metric="deployment_frequency",
        start_date="2026-01-01",
        end_date="2026-01-31",
        interval="weekly",
    )

    get_list.assert_called_once_with(
        "projects/99/dora/metrics",
        params={
            "metric": "deployment_frequency",
            "interval": "weekly",
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
        },
    )
