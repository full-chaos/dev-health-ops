from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from dev_health_ops.connectors.utils.rate_limit_queue import RateLimitGate
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
