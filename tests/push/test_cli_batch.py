"""Tests for `dev-hops push batch` (CHAOS-2700): the exit-code contract
(brief decision 9), `--poll` terminal-status handling, env var precedence
(decision 11), and the `--org` preflight exclusion (decision 12).

`httpx.AsyncClient` is monkeypatched process-wide to inject an
`httpx.MockTransport` -- `_cmd_batch`/`_cmd_status` construct their own
client internally (matching the CLI's real code path), so this is simpler
and more faithful than refactoring them to accept an injected client.
"""

from __future__ import annotations

import argparse
import json
import logging

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


def _sample_payload_file(tmp_path) -> str:
    envelope = push_cli._wrap_batch_envelope(
        [push_cli._sample_record("repository.v1")], idempotency_key="k1"
    )
    path = tmp_path / "payload.json"
    path.write_text(json.dumps(envelope))
    return str(path)


def _batch_ns(payload: str, **overrides) -> argparse.Namespace:
    defaults = dict(
        payload=payload,
        api_url="http://test",
        token="fcpush_test",
        org="org-1",
        poll=False,
        poll_interval=0.01,
        poll_timeout=0.05,
        skip_limits_check=True,
        json=True,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _sequenced_handler(get_bodies: list[dict], post_body: dict | None = None):
    state = {"get_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                202,
                json=post_body
                or {
                    "ingestionId": "batch-1",
                    "status": "accepted",
                    "itemsReceived": 1,
                    "stream": "s",
                },
            )
        if request.method == "GET":
            idx = min(state["get_calls"], len(get_bodies) - 1)
            state["get_calls"] += 1
            return httpx.Response(200, json=get_bodies[idx])
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    return handler, state


# ---------------------------------------------------------------------------
# exit-code contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_without_poll_exits_0_on_accept(
    monkeypatch: pytest.MonkeyPatch, tmp_path, capsys: pytest.CaptureFixture[str]
) -> None:
    handler, _state = _sequenced_handler([{"status": "accepted"}])
    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path))

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_OK
    body = json.loads(capsys.readouterr().out)
    assert body["ingestionId"] == "batch-1"


@pytest.mark.asyncio
async def test_batch_poll_completed_zero_rejected_exits_0(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    handler, _state = _sequenced_handler(
        [
            {"ingestionId": "batch-1", "status": "accepted"},
            {"ingestionId": "batch-1", "status": "processing"},
            {
                "ingestionId": "batch-1",
                "status": "completed",
                "itemsRejected": 0,
                "itemsAccepted": 1,
            },
        ]
    )
    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path), poll=True)

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_OK


@pytest.mark.asyncio
async def test_batch_poll_partial_exits_1(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    handler, _state = _sequenced_handler(
        [
            {
                "ingestionId": "batch-1",
                "status": "partial",
                "itemsRejected": 1,
                "itemsAccepted": 0,
            }
        ]
    )
    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path), poll=True)

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_DATA_FAILURE


@pytest.mark.asyncio
async def test_batch_poll_failed_exits_1(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    handler, _state = _sequenced_handler(
        [
            {
                "ingestionId": "batch-1",
                "status": "failed",
                "itemsRejected": 1,
                "itemsAccepted": 0,
            }
        ]
    )
    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path), poll=True)

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_DATA_FAILURE


@pytest.mark.asyncio
async def test_batch_poll_timeout_exits_4(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    handler, _state = _sequenced_handler(
        [{"ingestionId": "batch-1", "status": "processing"}]
    )
    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(
        _sample_payload_file(tmp_path), poll=True, poll_timeout=0.05, poll_interval=0.01
    )

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_POLL_TIMEOUT


@pytest.mark.asyncio
async def test_batch_poll_stream_unavailable_exits_3(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    handler, _state = _sequenced_handler(
        [{"ingestionId": "batch-1", "status": "stream_unavailable"}]
    )
    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path), poll=True)

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_TRANSPORT_ERROR


@pytest.mark.asyncio
async def test_batch_replay_200_terminal_short_circuits_poll(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """CC13/CC22: a 200 (REPLAY) response with an already-terminal status
    must not trigger a real polling round-trip."""
    get_was_called = {"value": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "ingestionId": "batch-1",
                    "status": "completed",
                    "itemsRejected": 0,
                    "itemsAccepted": 1,
                },
            )
        get_was_called["value"] = True
        raise AssertionError("GET /batches/{id} should not be called")

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path), poll=True)

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_OK
    assert get_was_called["value"] is False


@pytest.mark.asyncio
async def test_batch_transport_error_after_retries_exits_3(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    from dev_health_ops.connectors.utils import retry as retry_module

    async def _noop_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(retry_module.asyncio, "sleep", _noop_sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path))

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_TRANSPORT_ERROR


@pytest.mark.asyncio
async def test_batch_400_api_error_exits_3(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400, json={"error": {"code": "invalid_envelope", "message": "bad"}}
        )

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path))

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_TRANSPORT_ERROR


# ---------------------------------------------------------------------------
# local pre-flight (offline, no network call reached)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_local_batch_too_large_never_hits_network(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network must not be reached for a locally-rejected batch")

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))

    envelope = push_cli._wrap_batch_envelope(
        [push_cli._sample_record("repository.v1")], idempotency_key="k1"
    )
    payload_file = tmp_path / "p.json"
    payload_file.write_text(json.dumps(envelope))

    from dev_health_ops.push.limits import BatchLimits

    monkeypatch.setattr(
        "dev_health_ops.push.cli.DEFAULT_LIMITS",
        BatchLimits(max_records_per_batch=0, max_body_bytes=10_000_000),
    )
    ns = _batch_ns(str(payload_file))

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_DATA_FAILURE


# ---------------------------------------------------------------------------
# env var precedence (brief decision 11)
# ---------------------------------------------------------------------------


def test_token_precedence_flag_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FULLCHAOS_INGEST_TOKEN", "env-primary")
    monkeypatch.setenv("FULLCHAOS_API_TOKEN", "env-legacy")
    ns = argparse.Namespace(token="flag-token")

    assert push_cli._resolve_token(ns) == "flag-token"


def test_token_precedence_primary_env_wins_over_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FULLCHAOS_INGEST_TOKEN", "env-primary")
    monkeypatch.setenv("FULLCHAOS_API_TOKEN", "env-legacy")
    ns = argparse.Namespace(token=None)

    assert push_cli._resolve_token(ns) == "env-primary"


def test_token_legacy_env_alone_logs_deprecation_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("FULLCHAOS_INGEST_TOKEN", raising=False)
    monkeypatch.setenv("FULLCHAOS_API_TOKEN", "env-legacy")
    ns = argparse.Namespace(token=None)

    with caplog.at_level(logging.WARNING):
        token = push_cli._resolve_token(ns)

    assert token == "env-legacy"
    assert any("FULLCHAOS_API_TOKEN" in r.message for r in caplog.records)


def test_token_missing_everywhere_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FULLCHAOS_INGEST_TOKEN", raising=False)
    monkeypatch.delenv("FULLCHAOS_API_TOKEN", raising=False)
    ns = argparse.Namespace(token=None)

    assert push_cli._resolve_token(ns) is None


# ---------------------------------------------------------------------------
# --org preflight exclusion (brief decision 12)
# ---------------------------------------------------------------------------


def test_should_resolve_org_false_for_push_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.cli import _should_resolve_org

    ns = argparse.Namespace(command="push", org=None)

    assert _should_resolve_org(ns) is False


@pytest.mark.asyncio
async def test_batch_missing_org_exits_2_not_silent_resolution(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.delenv("FULLCHAOS_ORG_ID", raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network must not be reached without a resolved org")

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))
    ns = _batch_ns(_sample_payload_file(tmp_path), org=None)

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_USAGE_ERROR


# ---------------------------------------------------------------------------
# poll-flag validation + bounded payload reads (Codex adversarial-review
# findings)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["-1", "0", "nan", "inf", "-inf", "not-a-number"])
def test_positive_finite_float_rejects_bad_values(raw: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        push_cli._positive_finite_float(raw)


@pytest.mark.parametrize("raw,expected", [("5", 5.0), ("0.5", 0.5), ("300", 300.0)])
def test_positive_finite_float_accepts_good_values(raw: str, expected: float) -> None:
    assert push_cli._positive_finite_float(raw) == expected


def test_poll_interval_negative_rejected_at_parse_time() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    push_cli.register_commands(sub)

    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(
            [
                "push",
                "batch",
                "payload.json",
                "--api-url",
                "http://test",
                "--token",
                "t",
                "--org",
                "o",
                "--poll-interval",
                "-1",
            ]
        )

    assert exc_info.value.code == 2


@pytest.mark.parametrize("raw", ["1e-300", "0.000001", "0.1"])
def test_poll_interval_type_rejects_values_below_floor(raw: str) -> None:
    """Codex adversarial-review finding, round 2: an arbitrarily tiny
    positive interval still hot-loops the API even though it passes the
    plain not-zero/negative check."""
    with pytest.raises(argparse.ArgumentTypeError):
        push_cli._poll_interval_type(raw)


def test_poll_interval_type_accepts_value_at_floor() -> None:
    assert (
        push_cli._poll_interval_type(str(push_cli.MIN_POLL_INTERVAL_SECONDS))
        == push_cli.MIN_POLL_INTERVAL_SECONDS
    )


def test_poll_interval_tiny_value_rejected_at_parse_time() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    push_cli.register_commands(sub)

    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(
            [
                "push",
                "batch",
                "payload.json",
                "--api-url",
                "http://test",
                "--token",
                "t",
                "--org",
                "o",
                "--poll-interval",
                "0.001",
            ]
        )

    assert exc_info.value.code == 2


@pytest.mark.asyncio
async def test_batch_oversized_payload_rejected_without_hitting_network(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    from dev_health_ops.push.limits import BatchLimits

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network must not be reached for an oversized payload")

    _patch_async_client(monkeypatch, httpx.MockTransport(handler))

    big_file = tmp_path / "big.json"
    big_file.write_text("x" * 1000)

    ns = _batch_ns(str(big_file), skip_limits_check=True)
    monkeypatch.setattr(
        "dev_health_ops.push.cli.DEFAULT_LIMITS",
        BatchLimits(max_records_per_batch=1000, max_body_bytes=10),
    )

    exit_code = await push_cli._cmd_batch(ns)

    assert exit_code == out.EXIT_DATA_FAILURE


def test_read_payload_arg_raises_for_oversized_file(tmp_path) -> None:
    from dev_health_ops.push.cli import PayloadTooLargeError, _read_payload_arg

    big_file = tmp_path / "big.json"
    big_file.write_text("x" * 1000)

    with pytest.raises(PayloadTooLargeError):
        _read_payload_arg(str(big_file), max_bytes=10)


def test_read_payload_arg_accepts_file_within_limit(tmp_path) -> None:
    from dev_health_ops.push.cli import _read_payload_arg

    small_file = tmp_path / "small.json"
    small_file.write_text("{}")

    assert _read_payload_arg(str(small_file), max_bytes=10) == b"{}"
