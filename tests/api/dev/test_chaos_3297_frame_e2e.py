"""CHAOS-3297 stack #1 -- e2e controls, real endpoint to real orchestrator.

Per the ratified plan: every control here drives the real
``BoundedDevRuntime``/``DevOrchestrator`` through the real
``/api/v1/dev/conversations/{id}/messages`` endpoint -- never
``PreflightNoAnswerRuntime``, never a fake recorder (Amendment 2 / CHAOS-3301
P1w). A control that drives a fake runtime proves the fake writes a frame.

Property manifest cross-references
(``/Users/chris/.claude/jobs/7ceca217/tmp/chaos-3297-s1-manifest.md``):

* N0 -- anti-vacuity guard for ``assert_frame_persisted``, proven against a
  run recorded with the frame write disabled.
* C0-equivalent -- an orchestrator-native termination (not a preflight one)
  reaches the CHAOS-3299 replay gate for the first time: before stack #1,
  these ~30 call sites never tagged ``contract_generation = 'v2'`` at all.
* C2 -- live-vs-replay byte equality, for (a) an orchestrator-native error
  termination and (b) the answered path, which stack #1 deliberately leaves
  unchanged (team-lead ratification: "router answered path unchanged is
  correct") but still pins byte-for-byte, since C2 is meant to prove the
  *whole* replay surface, not just what this stack edited.
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, cast

import pytest
from sqlalchemy import select

from dev_health_ops.api.dev import router as dev_router_module
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevToolRequest, DevToolResult, ToolID
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame as DevAnswerFrameV2
from dev_health_ops.api.dev.runtime import BoundedDevRuntime
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.scripted import ScriptedStep
from dev_health_ops.models.dev_persistence import DevAnswerFrame, DevRun
from tests._chaos_3292_preflight import (
    RecordingProvider,
    grounded_answer_payload,
    organization_resolution,
    recording_registry,
    versions,
)
from tests.api.dev.test_router import (  # noqa: F401,E501
    FakeBoundedRuntime,
    _parse_sse_events,
    dev_api_context,
)

#: The v1 evidence-handle grammar (`evidence_service.EvidenceHandleService.issue`).
_REAL_EVIDENCE_HANDLE = "ev1_" + ("a1b2c3d4e5" * 4)


def _evidence_consistent_registry(calls: list[DevToolRequest]) -> AskDevToolRegistry:
    """Like ``recording_registry``, but the returned tool result's evidence
    id matches ``_REAL_EVIDENCE_HANDLE`` rather than the fixture's "ev_01" --
    otherwise ``answer_validator``'s grounding check (which cross-references
    the answer's claimed evidence against what the tool actually returned,
    not just against itself) rejects an answer whose evidence was patched to
    a real handle without patching the tool result to match."""

    async def execute(_context: Any, request: DevToolRequest) -> DevToolResult:
        calls.append(request)
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        for evidence in payload.get("evidence", []):
            evidence["evidence_ref_id"] = _REAL_EVIDENCE_HANDLE
        for metric in payload.get("metrics", []):
            if metric.get("evidence_ref_ids"):
                metric["evidence_ref_ids"] = [_REAL_EVIDENCE_HANDLE]
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


async def _resolve(*, requested_scope: Any, **_values: Any) -> Any:
    return organization_resolution(requested_scope)


def _scope_payload(org_id: uuid.UUID) -> dict[str, object]:
    return {
        "schema_version": "dev_scope.v1",
        "organization_id": str(org_id),
        "direct_scope": "organization",
        "repositories": [],
        "entity_refs": [],
        "team_ids": [],
        "time_range": {
            "start": "2026-07-28T12:00:00+00:00",
            "end": "2026-07-28T13:00:00+00:00",
            "timezone": "UTC",
        },
    }


async def assert_frame_persisted(
    maker: Any, *, org_id: uuid.UUID, user_id: uuid.UUID, run_id: uuid.UUID
) -> DevAnswerFrameV2:
    """N0: the row exists, the run is tagged v2, and the payload validates.

    Rule 4 (a measurement that did not happen must FAIL, loudly): every
    assertion here raises ``AssertionError`` with a specific message rather
    than returning ``None``/``False`` on a missing row.
    """

    async with maker() as session:
        run = await session.get(DevRun, run_id)
        assert run is not None, f"no dev_runs row for {run_id}"
        assert run.contract_generation == "v2", (
            f"run {run_id} was not tagged contract_generation='v2' "
            f"(got {run.contract_generation!r}) -- record_frame never ran"
        )
        row = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
        )
        assert row is not None, f"no dev_answer_frames row for run {run_id}"
        assert row.public_outcome == run.public_outcome, (
            "frame and run public_outcome disagree: "
            f"frame={row.public_outcome!r} run={run.public_outcome!r}"
        )
        return DevAnswerFrameV2.model_validate(row.payload)


@pytest.mark.asyncio
async def test_n0_fails_loudly_when_the_frame_write_is_disabled(
    dev_api_context: Any,  # noqa: F811 - pytest fixture, imported above for reuse
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove N0 itself: point it at a run recorded with record_frame disabled."""

    from dev_health_ops.api.dev import orchestrator_persistence as op

    async def _no_op_record_frame(self: Any, frame: Any) -> None:
        del self, frame

    monkeypatch.setattr(op.PersistenceRunRecorder, "record_frame", _no_op_record_frame)

    calls: list[DevToolRequest] = []
    real_runtime = BoundedDevRuntime(
        provider=cast(
            Any,
            RecordingProvider(
                [
                    ScriptedStep(
                        decision=AgentToolRequest(
                            tool_id="query_metric.v1",
                            arguments={"metric_id": "items_completed", "limit": 12},
                            call_id=f"tool_call_{i}",
                        )
                    )
                    for i in range(3)
                ],
                script_id="n0-disabled",
            ),
        ),
        provider_source="platform",
        provider_family="scripted",
        registry=recording_registry(calls),
        scope_resolver=_resolve,
        versions=versions(),
    )
    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(runtime=real_runtime)

    client = dev_api_context.client
    org_id = dev_api_context.org_id
    created = await client.post(
        "/api/v1/dev/conversations", json={"current_scope": _scope_payload(org_id)}
    )
    conversation_id = created.json()["conversation_id"]
    response = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        json={
            "schema_version": "dev_message_request.v1",
            "request_id": "request_n0_disabled",
            "client_message_id": "client_n0_disabled",
            "conversation_id": conversation_id,
            "question": "How many items completed?",
            "question_class": "status",
            "scope": _scope_payload(org_id),
        },
    )
    assert response.status_code == 200
    events = dict(_parse_sse_events(response.text))
    run_started = events["run.started"]
    run_id = uuid.UUID(run_started["run_id"])

    with pytest.raises(AssertionError, match="was not tagged contract_generation"):
        await assert_frame_persisted(
            dev_api_context.maker,
            org_id=org_id,
            user_id=dev_api_context.user_id,
            run_id=run_id,
        )


@pytest.mark.asyncio
async def test_orchestrator_native_termination_reaches_the_replay_gate(
    dev_api_context: Any,  # noqa: F811 - pytest fixture, imported above for reuse
) -> None:
    """C0-equivalent: an orchestrator-level (non-preflight) termination now
    persists a frame and tags contract_generation='v2' -- before stack #1
    this was RED (record_frame was never called on this path at all), which
    is exactly why the CHAOS-3299 replay gate was unreachable for anything
    but a preflight termination.

    Three identical ``query_metric.v1`` requests trip the
    ``identical_tool_calls`` loop guard before the third executes
    (``DevRunLimits.identical_tool_calls == 2``), terminating
    ``tool_limit_reached`` -- deterministic, no preflight involved.
    """

    calls: list[DevToolRequest] = []
    repeated = [
        ScriptedStep(
            decision=AgentToolRequest(
                tool_id="query_metric.v1",
                arguments={"metric_id": "items_completed", "limit": 12},
                call_id=f"tool_call_{i}",
            )
        )
        for i in range(3)
    ]
    real_runtime = BoundedDevRuntime(
        provider=cast(Any, RecordingProvider(repeated, script_id="loop-guard-e2e")),
        provider_source="platform",
        provider_family="scripted",
        registry=recording_registry(calls),
        scope_resolver=_resolve,
        versions=versions(),
    )
    assert not isinstance(real_runtime, FakeBoundedRuntime)

    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(runtime=real_runtime)

    client = dev_api_context.client
    org_id = dev_api_context.org_id
    created = await client.post(
        "/api/v1/dev/conversations", json={"current_scope": _scope_payload(org_id)}
    )
    conversation_id = created.json()["conversation_id"]
    payload = {
        "schema_version": "dev_message_request.v1",
        "request_id": "request_loop_guard_e2e",
        "client_message_id": "client_loop_guard_e2e",
        "conversation_id": conversation_id,
        "question": "How many items completed?",
        "question_class": "status",
        "scope": _scope_payload(org_id),
    }

    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200
    live_events = dict(_parse_sse_events(live.text))
    assert "error" in live_events
    assert live_events["error"]["error"]["code"] == "tool_limit_reached"
    run_id = uuid.UUID(live_events["run.started"]["run_id"])

    # N0
    await assert_frame_persisted(
        dev_api_context.maker,
        org_id=org_id,
        user_id=dev_api_context.user_id,
        run_id=run_id,
    )

    # C2: live-vs-replay equality on the CHAOS-3297 coherence property (the
    # exact v1 code, retryable and remediation), under two declared
    # exclusions: `request_id` (test_router.py's existing, pre-CHAOS-3297
    # exclusion: live builds it from the client-supplied request id, replay
    # reads the persisted, UUID-folded dev_runs.request_id) and
    # `safe_message` (pre-existing, unrelated to this stack: EVERY
    # non-preflight error termination already replayed through
    # `_replay_fallback_error`'s generic "did not complete" sentence before
    # CHAOS-3297 -- these runs had no frame at all, so `frame_payload` was
    # already None and `_replayed_result` already fell to that branch. What
    # CHAOS-3297 changes is that the *code* now survives replay via the
    # router guard tested exhaustively in test_terminal_frames.py; message
    # fidelity on this class of replay was never this stack's job).
    replay = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert replay.status_code == 200
    replay_events = dict(_parse_sse_events(replay.text))
    assert "error" in replay_events

    def _comparable_error(events: dict[str, Any]) -> dict[str, Any]:
        return {
            k: v
            for k, v in events["error"]["error"].items()
            if k not in {"request_id", "safe_message"}
        }

    assert _comparable_error(live_events) == _comparable_error(replay_events)
    # And the replay must have made zero further tool calls.
    assert len(calls) == 2, "replay must not re-execute any tool"


@pytest.mark.asyncio
async def test_answered_path_replay_is_byte_identical(
    dev_api_context: Any,  # noqa: F811 - pytest fixture, imported above for reuse
) -> None:
    """C2 pin for the answered path (team-lead ratification): stack #1
    deliberately leaves the router's answer_id-wins replay branch untouched
    (the stored v1 answer message is authoritative, never the frame), but
    that guarantee is worth pinning byte-for-byte here rather than only
    asserted by inspection, since it is exactly the kind of seam a later
    stack could regress without a dedicated control noticing.
    """

    calls: list[DevToolRequest] = []
    org_id = dev_api_context.org_id
    # grounded_answer_payload keeps the fixture's evidence-backed claim
    # (re-pointed at this run's own scope) rather than status_then_answer's
    # claims=[] shape -- an org-wide question with zero claims trips the
    # CHAOS-3289 legacy named-entity guard's no_evidence_backed_claims
    # clause, which is an unrelated guard this test is not exercising.
    answer = grounded_answer_payload(
        script_id="answered-replay-e2e",
        summary="Twelve items are complete this period.",
        validity_scope=_scope_payload(org_id),
    )
    # The fixture's evidence_ref_id ("ev_01") is not a real evidence.py-minted
    # handle (`ev1_` + 40 hex, evidence_service.EvidenceHandleService.issue's
    # exact and only production grammar) -- fine for a v1-only fixture, but
    # wrap_legacy_answer_as_frame embeds it into v2's stricter EvidenceHandle
    # field, which rejects it and (correctly, per finish()'s degrade-safely
    # design) silently skips recording the frame rather than crashing the
    # otherwise-successful answer. Substituting a real handle here is a test
    # fixture fix, not a production concern (both production evidence_ref_id
    # producers call the same signer.issue(...)). The tool result the
    # registry returns must carry the same handle, or answer_validator's
    # grounding check (which cross-references the answer's evidence against
    # what the tool actually returned) rejects the answer --
    # _evidence_consistent_registry below does that half.
    for claim in answer["claims"]:
        claim["evidence_ref_ids"] = (
            [_REAL_EVIDENCE_HANDLE] if claim["evidence_ref_ids"] else []
        )
    for evidence in answer["evidence"]:
        evidence["evidence_ref_id"] = _REAL_EVIDENCE_HANDLE
    for metric in answer["metrics"]:
        metric["evidence_ref_ids"] = (
            [_REAL_EVIDENCE_HANDLE] if metric["evidence_ref_ids"] else []
        )
    script = [
        ScriptedStep(
            decision=AgentToolRequest(
                tool_id="status_snapshot.v1",
                arguments={"limit": 25, "include_comparison": False},
                call_id="tool_call_01",
            ),
            usage=AgentUsage(input_tokens=100, output_tokens=10),
        ),
        ScriptedStep(decision=AgentFinalAnswer(answer)),
    ]
    real_runtime = BoundedDevRuntime(
        provider=cast(Any, RecordingProvider(script, script_id="answered-replay-e2e")),
        provider_source="platform",
        provider_family="scripted",
        registry=_evidence_consistent_registry(calls),
        scope_resolver=_resolve,
        versions=versions(),
    )
    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(runtime=real_runtime)

    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations", json={"current_scope": _scope_payload(org_id)}
    )
    conversation_id = created.json()["conversation_id"]
    payload = {
        "schema_version": "dev_message_request.v1",
        "request_id": "request_answered_replay_e2e",
        "client_message_id": "client_answered_replay_e2e",
        "conversation_id": conversation_id,
        "question": "What is the overall status?",
        "question_class": "status",
        "scope": _scope_payload(org_id),
    }

    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200
    live_events = dict(_parse_sse_events(live.text))
    assert "answer.completed" in live_events
    run_id = uuid.UUID(live_events["run.started"]["run_id"])

    # This path still gets a frame (finish() is now frame-mandatory
    # everywhere), but the frame is compatibility metadata only -- N0 still
    # holds (the frame row exists and is tagged v2) even though it is not
    # what replay reconstructs from.
    await assert_frame_persisted(
        dev_api_context.maker,
        org_id=org_id,
        user_id=dev_api_context.user_id,
        run_id=run_id,
    )

    replay = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert replay.status_code == 200
    replay_events = dict(_parse_sse_events(replay.text))
    assert "answer.completed" in replay_events

    def _comparable_answer(events: dict[str, Any]) -> dict[str, Any]:
        payload = dict(events["answer.completed"]["answer"])
        payload.pop("generated_at", None)
        return payload

    assert _comparable_answer(live_events) == _comparable_answer(replay_events)
    assert len(calls) == 1, "replay must not re-execute the tool call"
