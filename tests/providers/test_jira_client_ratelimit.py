from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from dev_health_ops.connectors.utils.rate_limit_queue import RateLimitGate
from dev_health_ops.providers.jira.client import JiraAuth, JiraClient


class _FakeJira429Response:
    status_code = 429
    headers = {"Retry-After": "2"}
    text = "rate limited"

    def raise_for_status(self) -> None:
        error = requests.HTTPError("429 Client Error")
        error.response = self
        raise error

    def json(self) -> dict[str, Any]:
        return {}


def test_jira_search_page_429_penalizes_once_without_outer_retry() -> None:
    gate = MagicMock(spec=RateLimitGate)
    gate.penalize.return_value = 2.0
    client = JiraClient(
        auth=JiraAuth(
            base_url="https://example.atlassian.net",
            email="bot@example.com",
            api_token="token",
        ),
        gate=gate,
    )
    client.session.get = MagicMock(return_value=_FakeJira429Response())

    with pytest.raises(requests.HTTPError):
        client.search_issues_page(jql="project = ENG", start_at=0, max_results=50)

    client.session.get.assert_called_once()
    gate.wait_sync.assert_called_once()
    gate.penalize.assert_called_once_with(2.0)
    gate.reset.assert_not_called()
