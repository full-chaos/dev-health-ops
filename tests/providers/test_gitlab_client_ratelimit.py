from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from dev_health_ops.connectors.utils.rate_limit_queue import RateLimitGate
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers.gitlab.client import GitLabAuth, GitLabWorkClient


def _make_client(gate: Any) -> GitLabWorkClient:
    return GitLabWorkClient(
        auth=GitLabAuth(token="test-token", base_url="https://gitlab.example.com"),
        gate=gate,
    )


def test_gated_iter_passes_through_items_under_gate() -> None:
    # Every page fetch (each next() on the lazy iterator) is coordinated through
    # the shared gate, not just the iterator creation (CHAOS-2463).
    gate = MagicMock(spec=RateLimitGate)
    client = _make_client(gate)

    def _lazy_pages() -> Any:
        yield {"id": 1}
        yield {"id": 2}

    collected = list(client._gated_iter(_lazy_pages()))

    assert collected == [{"id": 1}, {"id": 2}]
    # wait_sync is called once per next(): two items plus the terminal
    # StopIteration probe.
    assert gate.wait_sync.call_count == 3
    gate.penalize.assert_not_called()


def test_gated_iter_penalizes_when_a_later_page_fetch_raises() -> None:
    # A 429 raised while consuming a later page must flow through the gate so the
    # whole worker fleet backs off, instead of the bulk path running uncoordinated.
    gate = MagicMock(spec=RateLimitGate)
    gate.penalize.return_value = 1.0
    client = _make_client(gate)

    def _lazy_pages() -> Any:
        yield {"id": 1}
        raise requests.HTTPError("429 Too Many Requests")

    collected: list[Any] = []
    with pytest.raises(requests.HTTPError):
        for item in client._gated_iter(_lazy_pages()):
            collected.append(item)

    assert collected == [{"id": 1}]
    gate.penalize.assert_called()


def _gitlab_error(status: int = 429) -> Exception:
    import gitlab

    # python-gitlab (8.x) exposes response_code/response_body but NOT
    # response_headers, so a real GitLab 429 carries no Retry-After we can read;
    # detection is by status and the worker defers with its default backoff.
    # (python-gitlab already does header-aware sleeping internally before it
    # raises, so the precise server delay is consumed there.)
    return gitlab.exceptions.GitlabError("rate limited", response_code=status)


def test_gated_iter_raises_rate_limit_on_gitlab_429() -> None:
    # A python-gitlab 429 during page consumption must surface as the canonical
    # RateLimitException, not the raw GitlabError (and must not be swallowed).
    gate = MagicMock(spec=RateLimitGate)
    gate.penalize.return_value = 5.0
    client = _make_client(gate)
    err = _gitlab_error(status=429)

    def _lazy_pages() -> Any:
        yield {"id": 1}
        raise err

    with pytest.raises(RateLimitException):
        list(client._gated_iter(_lazy_pages()))

    gate.penalize.assert_called()


def test_enrichment_reraises_rate_limit_instead_of_swallowing() -> None:
    # Enrichment getters must NOT swallow a 429 as "missing optional data".
    gate = MagicMock(spec=RateLimitGate)
    client = _make_client(gate)
    issue = MagicMock()
    issue.notes.list.side_effect = _gitlab_error(status=429)

    with pytest.raises(RateLimitException):
        client.get_issue_notes(issue)


def test_enrichment_still_swallows_non_rate_limit_errors() -> None:
    # Non-rate-limit failures keep the existing graceful-degradation behavior.
    gate = MagicMock(spec=RateLimitGate)
    client = _make_client(gate)
    issue = MagicMock()
    issue.notes.list.side_effect = RuntimeError("transient parse error")

    assert client.get_issue_notes(issue) == []


def test_iter_group_epics_reraises_rate_limit_before_premium_gating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A 429 must raise even though the epics path otherwise swallows 403/404
    # (Premium/Ultimate gating).
    gate = MagicMock(spec=RateLimitGate)
    client = _make_client(gate)
    group = MagicMock()
    group.epics.list.side_effect = _gitlab_error(status=429)
    monkeypatch.setattr(client.gl.groups, "get", MagicMock(return_value=group))

    with pytest.raises(RateLimitException):
        list(client.iter_group_epics(group_id_or_path="g/sub"))
