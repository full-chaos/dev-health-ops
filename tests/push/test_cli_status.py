"""Tests for `dev-hops push status` (CHAOS-2700 brief decision 7): shares
`poll.py`'s loop with `push batch --poll` (see test_cli_batch.py for the
exhaustive exit-code matrix over that shared loop) -- these tests cover
`status`-specific wiring: no-poll single fetch, and its own preflight.
"""

from __future__ import annotations

import argparse
import json

import httpx
import pytest

from dev_health_ops.push import cli as push_cli
from dev_health_ops.push import output as out


def _patch_async_client(
    monkeypatch: pytest.MonkeyPatch, transport: httpx.MockTransport
) -> None:
    class _StubAsyncClient(httpx.AsyncClient):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _StubAsyncClient)


def _status_ns(ingestion_id: str = "batch-1", **overrides) -> argparse.Namespace:
    defaults = dict(
        ingestion_id=ingestion_id,
        api_url="http://test",
        token="fcpush_test",
        org="org-1",
        poll=False,
        poll_interval=0.01,
        poll_timeout=0.05,
        json=True,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


@pytest.mark.asyncio
async def test_status_no_poll_returns_current_status(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/v1/external-ingest/batches/batch-1"
        return httpx.Response(
            200, json={"ingestionId": "batch-1", "status": "processing"}
        )

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _status_ns()

    exit_code = await push_cli._cmd_status(ns)

    assert exit_code == out.EXIT_OK
    body = json.loads(capsys.readouterr().out)
    assert body["status"] == "processing"


@pytest.mark.asyncio
async def test_status_poll_reaches_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"n": 0}
    bodies = [
        {"ingestionId": "batch-1", "status": "processing"},
        {
            "ingestionId": "batch-1",
            "status": "completed",
            "itemsRejected": 0,
            "itemsAccepted": 1,
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        idx = min(state["n"], len(bodies) - 1)
        state["n"] += 1
        return httpx.Response(200, json=bodies[idx])

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _status_ns(poll=True)

    exit_code = await push_cli._cmd_status(ns)

    assert exit_code == out.EXIT_OK


@pytest.mark.asyncio
async def test_status_not_found_is_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404, json={"error": {"code": "not_found", "message": "no such batch"}}
        )

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _status_ns()

    exit_code = await push_cli._cmd_status(ns)

    assert exit_code == out.EXIT_TRANSPORT_ERROR


@pytest.mark.asyncio
async def test_status_missing_token_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FULLCHAOS_INGEST_TOKEN", raising=False)
    monkeypatch.delenv("FULLCHAOS_API_TOKEN", raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network must not be reached without a resolved token")

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _status_ns(token=None)

    exit_code = await push_cli._cmd_status(ns)

    assert exit_code == out.EXIT_USAGE_ERROR
