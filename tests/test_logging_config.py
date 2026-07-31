"""Regression coverage for CHAOS-3258: safe provider-client logging.

Ask Dev's orchestrator and OpenAI-compatible adapter never call a logger
directly (see openai_compatible.py / orchestrator.py). The only leak vector
is the OpenAI Python SDK's own debug logging ("Request options: ...", which
includes the full outbound message list -- system policy prompt, prior
conversation turns, the current question, and tool-result payloads) and
httpx/httpcore's debug logging, both of which otherwise inherit an
operator-raised root LOG_LEVEL (e.g. DEBUG, set via compose.yml for local
diagnostics).

This drives real requests through the production ``OpenAICompatibleAgentProvider``
over a real local HTTP server -- using the actual ``openai``/``httpx``/``httpcore``
machinery, not a hand-rolled fake client -- so the SDK's own debug logging is
genuinely exercised. A test that only asserted "no sentinel in the logs"
without first proving the harness can observe a leak would pass vacuously if
nothing were ever logged at all; ``test_leak_harness_actually_observes_a_leak_when_unpatched``
closes that gap by temporarily undoing the fix and confirming the sentinel
does appear.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import pytest

from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentToolDefinition,
    AgentToolRequest,
)
from dev_health_ops.llm.agent.errors import AgentProviderError
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.logging_config import configure_logging

_CONTENT_CARRYING_CLIENT_LOGGERS = ("openai", "httpx", "httpcore")


def _sentinel(name: str) -> str:
    # Neutral, non-secret-shaped runtime-constructed marker (never a literal
    # committed to source) so gitleaks never sees anything resembling a real
    # credential, per repo convention for secret-shaped test fixtures.
    return f"SENTINEL-{name.upper()}-{secrets.token_hex(12)}"


class _Scenario:
    SUCCESS = "success"
    PROVIDER_FAILURE = "provider_failure"
    TIMEOUT = "timeout"
    RETRY_THEN_SUCCESS = "retry_then_success"
    INVALID_RESPONSE = "invalid_response"


def _completion_response(
    *, tool_call: dict[str, Any] | None, content: str | None
) -> dict:
    message: dict[str, Any] = {"role": "assistant", "content": content}
    finish_reason = "stop"
    if tool_call is not None:
        message["content"] = None
        message["tool_calls"] = [tool_call]
        finish_reason = "tool_calls"
    return {
        "id": "chatcmpl-sentinel-test",
        "object": "chat.completion",
        "created": 1_785_283_200,
        "model": "sentinel-test-model",
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        "usage": {"prompt_tokens": 7, "completion_tokens": 5, "total_tokens": 12},
    }


def _tool_call(call_id: str) -> dict[str, Any]:
    return {
        "id": call_id,
        "type": "function",
        "function": {
            # A real model can only echo back the wire-legal name it was
            # offered, never the canonical dotted tool_id (CHAOS-3286).
            "name": "query_metric_v1",
            "arguments": json.dumps({"metric_id": "items_completed"}),
        },
    }


class _Handler(BaseHTTPRequestHandler):
    server: _FakeServer

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)  # drain the request body; content is irrelevant here
        self.server.request_count += 1
        scenario = self.server.scenario
        if scenario == _Scenario.SUCCESS:
            self._write_json(
                200, _completion_response(tool_call=_tool_call("call-1"), content=None)
            )
        elif scenario == _Scenario.PROVIDER_FAILURE:
            self._write_json(
                400,
                {"error": {"message": "bad request", "type": "invalid_request_error"}},
            )
        elif scenario == _Scenario.TIMEOUT:
            time.sleep(2.0)
            self._write_json(
                200, _completion_response(tool_call=_tool_call("call-1"), content=None)
            )
        elif scenario == _Scenario.RETRY_THEN_SUCCESS:
            if self.server.request_count == 1:
                self._write_json(
                    429,
                    {"error": {"message": "rate limited", "type": "rate_limit_error"}},
                )
            else:
                self._write_json(
                    200,
                    _completion_response(tool_call=_tool_call("call-1"), content=None),
                )
        elif scenario == _Scenario.INVALID_RESPONSE:
            self._write_json(
                200, _completion_response(tool_call=None, content="not-a-json-decision")
            )
        else:  # pragma: no cover - defensive
            self.send_error(500)

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class _FakeServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, scenario: str) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.scenario = scenario
        self.request_count = 0


@pytest.fixture
def fake_server() -> Iterator[Any]:
    server: _FakeServer | None = None

    def start(scenario: str) -> _FakeServer:
        nonlocal server
        server = _FakeServer(scenario)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server

    yield start
    if server is not None:
        server.shutdown()
        server.server_close()


@pytest.fixture(autouse=True)
def restore_client_logger_levels() -> Iterator[None]:
    """Never let a test leak DEBUG-level SDK logging into the rest of the suite."""
    saved = {
        name: logging.getLogger(name).level for name in _CONTENT_CARRYING_CLIENT_LOGGERS
    }
    yield
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)


def _sentinel_bundle() -> dict[str, str]:
    return {
        "question": _sentinel("question"),
        "prior_question": _sentinel("prior-question"),
        "prior_answer": _sentinel("prior-answer"),
        "evidence": _sentinel("evidence"),
        "credential": _sentinel("credential"),
        "tool_result": _sentinel("tool-result"),
    }


async def _exercise_scenario(
    *,
    base_url: str,
    sentinels: dict[str, str],
    timeout_seconds: float = 5.0,
    provider: OpenAICompatibleAgentProvider | None = None,
) -> AgentProviderError | None:
    """Drive one full round-trip (plus a tool-result round) through the real adapter.

    Returns the raised AgentProviderError, if any, so callers can assert on
    the failure-path scenarios without the sentinel assertions ever being
    skipped by an uncaught exception.

    Accepts an already-constructed ``provider`` so a caller can force logger
    levels between construction and the first request -- necessary because
    the constructor itself now reasserts the safe logger levels right after
    its lazy SDK import (CHAOS-3258), which would otherwise immediately
    undo a level change made only *before* construction.
    """
    if provider is None:
        provider = OpenAICompatibleAgentProvider(
            api_key="sentinel-test-key", model="agent-model", base_url=base_url
        )
    tool = AgentToolDefinition(
        "query_metric.v1",
        "Query a bounded metric.",
        {"type": "object", "properties": {}},
    )
    messages = [
        AgentMessage(AgentMessageRole.SYSTEM, f"policy prompt {sentinels['evidence']}"),
        AgentMessage(AgentMessageRole.USER, sentinels["prior_question"]),
        AgentMessage(AgentMessageRole.ASSISTANT, sentinels["prior_answer"]),
        AgentMessage(AgentMessageRole.USER, sentinels["question"]),
    ]
    try:
        first = await provider.decide(
            messages, [tool], {"type": "object"}, timeout_seconds, 256
        )
        if isinstance(first.decision, AgentToolRequest):
            tool_result_payload = json.dumps(
                {
                    "status": "success",
                    "credential": sentinels["credential"],
                    "value": sentinels["tool_result"],
                }
            )
            follow_up = [
                *messages,
                AgentMessage(
                    AgentMessageRole.ASSISTANT, "", tool_request=first.decision
                ),
                AgentMessage(
                    AgentMessageRole.TOOL,
                    tool_result_payload,
                    tool_call_id=first.decision.call_id,
                ),
            ]
            second = await provider.decide(
                follow_up, [tool], {"type": "object"}, timeout_seconds, 256
            )
            assert isinstance(second.decision, AgentFinalAnswer | AgentToolRequest)
        return None
    except AgentProviderError as exc:
        return exc
    finally:
        await provider.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scenario", "expect_error", "timeout_seconds"),
    [
        (_Scenario.SUCCESS, False, 5.0),
        (_Scenario.PROVIDER_FAILURE, True, 5.0),
        (_Scenario.TIMEOUT, True, 0.05),
        (_Scenario.RETRY_THEN_SUCCESS, False, 5.0),
        (_Scenario.INVALID_RESPONSE, True, 5.0),
    ],
)
async def test_sentinels_never_reach_logs_under_debug_root_logging(
    fake_server: Any,
    caplog: pytest.LogCaptureFixture,
    scenario: str,
    expect_error: bool,
    timeout_seconds: float,
) -> None:
    server = fake_server(scenario)
    host, port = cast(tuple[str, int], server.server_address)
    sentinels = _sentinel_bundle()

    configure_logging(level="DEBUG")
    caplog.set_level(logging.DEBUG)

    with caplog.at_level(logging.DEBUG):
        error = await _exercise_scenario(
            base_url=f"http://{host}:{port}/v1",
            sentinels=sentinels,
            timeout_seconds=timeout_seconds,
        )

    assert (error is not None) is expect_error

    captured = caplog.text
    for label, value in sentinels.items():
        assert value not in captured, (
            f"sentinel {label!r} leaked into application logs under DEBUG root logging"
        )
    # The fix must be a targeted, content-specific clamp -- not a blunt "turn
    # off logging" hammer that would trivially satisfy the assertion above by
    # silencing everything. A plain application logger (this test's own
    # module, standing in for e.g. an orchestrator logger emitting safe
    # request/run IDs, fingerprints, latency, usage, or error class) must
    # still propagate at the configured DEBUG root level.
    app_logger = logging.getLogger(__name__)
    app_logger.debug("safe operational metadata marker for %s", scenario)
    assert app_logger.getEffectiveLevel() == logging.DEBUG
    assert f"safe operational metadata marker for {scenario}" in caplog.text


@pytest.mark.asyncio
async def test_leak_harness_actually_observes_a_leak_when_unpatched(
    fake_server: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Prove the regression test above is a real measurement, not a vacuous pass.

    Undo the CHAOS-3258 fix for this one test (restored by the autouse
    fixture) and confirm the question sentinel DOES appear in captured logs.
    If this test ever stops failing-when-unpatched, the suite above could
    pass for the wrong reason: nothing being logged at all.
    """
    server = fake_server(_Scenario.SUCCESS)
    host, port = cast(tuple[str, int], server.server_address)
    sentinels = _sentinel_bundle()
    base_url = f"http://{host}:{port}/v1"

    configure_logging(level="DEBUG")
    # Construct the provider BEFORE forcing DEBUG: the constructor itself now
    # reasserts the safe levels right after its lazy SDK import (CHAOS-3258),
    # so forcing the levels first and constructing after would immediately
    # (and correctly) get overridden -- that's the fix working, not a broken
    # self-check. Force the "no fix at all" baseline strictly between
    # construction and the first request instead.
    provider = OpenAICompatibleAgentProvider(
        api_key="sentinel-test-key", model="agent-model", base_url=base_url
    )
    for name in _CONTENT_CARRYING_CLIENT_LOGGERS:
        logging.getLogger(name).setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG)

    with caplog.at_level(logging.DEBUG):
        error = await _exercise_scenario(
            base_url=base_url, sentinels=sentinels, provider=provider
        )

    assert error is None
    assert sentinels["question"] in caplog.text, (
        "expected the unpatched SDK loggers to leak the question sentinel; "
        "if this fails, the harness above cannot be trusted to catch a "
        "regression"
    )


def test_openai_log_env_var_does_not_reopen_content_logging_in_a_fresh_process() -> (
    None
):
    """Codex adversarial finding: OPENAI_LOG import-time reset bypass.

    The OpenAI Python SDK runs its own logging setup exactly once, at
    import time (``openai._utils._logs.setup_logging``), and when the
    operator sets the standard ``OPENAI_LOG`` env var to "debug"/"info" it
    unconditionally resets the "openai" and "httpx" loggers to that level --
    regardless of what ``configure_logging()`` already pinned. Because Ask
    Dev's provider adapters all import the SDK lazily (per first use, well
    after process-startup ``configure_logging()`` already ran), this can
    silently reopen full request/response body logging in production.

    Module import state can't be reliably reset within this pytest process
    (once "openai" is imported once in the session, later imports are
    cached and setup_logging() never runs again), so this must run in a
    genuinely fresh interpreter to be a real measurement.
    """
    script = textwrap.dedent(
        """
        import logging
        from dev_health_ops.logging_config import configure_logging
        configure_logging(level="DEBUG")
        from dev_health_ops.llm.agent.openai_compatible import (
            OpenAICompatibleAgentProvider,
        )
        OpenAICompatibleAgentProvider(
            api_key="k", model="agent-model", base_url="http://127.0.0.1:1/v1"
        )
        # Prefixed and printed after everything else so ordinary JSON log
        # lines (root is DEBUG here, and other libraries log at import time
        # too) can never be mistaken for this result.
        print("RESULT", logging.getLogger("openai").getEffectiveLevel())
        print("RESULT", logging.getLogger("httpx").getEffectiveLevel())
        print("RESULT", logging.getLogger("httpcore").getEffectiveLevel())
        """
    )
    env = {**os.environ, "LOG_LEVEL": "DEBUG", "OPENAI_LOG": "debug"}
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    levels = [
        int(line.split(" ", 1)[1])
        for line in result.stdout.strip().splitlines()
        if line.startswith("RESULT ")
    ]
    assert levels == [logging.WARNING, logging.WARNING, logging.WARNING], (
        "OPENAI_LOG=debug reopened content-carrying client logging after the "
        f"lazy SDK import; got effective levels {levels} "
        f"(want all == {logging.WARNING}). stdout:\\n{result.stdout}\\n"
        f"stderr:\\n{result.stderr}"
    )
