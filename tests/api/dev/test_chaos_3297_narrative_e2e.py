"""CHAOS-3297 stack #4 -- C3/C4, real endpoint to real orchestrator.

Per the ratified plan (§b): every control here drives the real
``BoundedDevRuntime``/``DevOrchestrator`` through the real
``/api/v1/dev/conversations/{id}/messages`` endpoint, never a fake runtime
or a hand-rolled recorder -- a control that drives a fake proves the fake
calls ``record_narrative``, not that production does.

* C3 -- provider-failure matrix: for each of the seven scripted failure
  modes, the run terminates with the same frame/run public_outcome as the
  no-provider baseline; only ``narrative_mode``/``narrative_failure_code``
  differ. A ``dev_run_narratives`` row is still written in every case --
  the deterministic fallback is safe, server-built content, not something
  withheld -- with ``mode='deterministic_fallback'`` distinguishing it
  from an accepted provider narrative's ``mode='provider'`` row.
* C4 -- the provider cannot move a number, entity, readiness verdict, or
  fact reference: each violation is rejected as
  ``narrative_grounding_failed``, proving the landed validators
  (``contracts_v2.validators.validate_narrative_*``) are reached from the
  live production call path, not only from a unit test that calls them
  directly.

Injection seam: ``BoundedDevRuntime.narrative_provider`` /
``DevOrchestrator(narrative_provider=...)`` (CHAOS-3297 stack #4). No
certified provider exists yet in production (CHAOS-3285's territory) --
these tests inject a scripted one through the same seam production will
use once one is certified.

Finding, not fixed here (terminal_frames.py / the shared test fixtures are
outside this stack's scope): ``test_chaos_3297_frame_e2e.py``'s own
``_evidence_consistent_registry`` gives metrics a *real* (non-empty)
evidence handle, which F10's XOR constraint
(``DevMetricRefV2.validate_evidence_or_classification``, landed by stack
#3 after stack #1's e2e file was written) now rejects when
``wrap_legacy_answer_as_frame`` unconditionally stamps
``evidence_classification=LEGACY_V1_UNMINTED`` on every v1-sourced metric
-- confirmed by direct reproduction, this makes
``test_answered_path_replay_is_byte_identical`` silently exercise the
``internal_error`` frame fallback today, not the ``answered_with_gaps``
path its own docstring says it proves (its ``assert_frame_persisted``
only checks frame/run outcome *agreement*, not that the outcome is what
the test intends). The registry below clears metric evidence_ref_ids to
``()`` instead of a real handle -- matching what
``production_runtime.py``'s ``query_metric.v1`` tool actually does
(F10 finding: it deliberately scrubs them) -- to stay on the real
``answered_with_gaps`` path these C3/C4 controls need.
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, cast

import pytest
from sqlalchemy import select

from dev_health_ops.api.dev import router as dev_router_module
from dev_health_ops.api.dev.answer_frames.narrative_fallback import (
    NarrativeFailureCode,
    NarrativeProviderBudgetExceededError,
    NarrativeProviderEmptyContentError,
    NarrativeProviderRefusalError,
    NarrativeProviderResult,
    NarrativeProviderSchemaViolationError,
    NarrativeProviderTimeoutError,
    NarrativeProviderUnsafeContentError,
)
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevToolRequest, DevToolResult, ToolID
from dev_health_ops.api.dev.runtime import BoundedDevRuntime
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.scripted import ScriptedStep
from dev_health_ops.models.dev_persistence import DevRun, DevRunNarrative
from tests._chaos_3292_preflight import (
    RecordingProvider,
    grounded_answer_payload,
    versions,
)
from tests.api.dev.test_chaos_3297_frame_e2e import _resolve, _scope_payload
from tests.api.dev.test_router import (  # noqa: F401 -- pytest fixture, see test_chaos_3297_frame_e2e.py's own note
    _parse_sse_events,
    dev_api_context,
)

pytestmark = pytest.mark.asyncio

#: A real v1 evidence-handle shape (``evidence_service.EvidenceHandleService.issue``'s
#: grammar), distinct from ``test_chaos_3297_frame_e2e._REAL_EVIDENCE_HANDLE``
#: only so a diff between the two files is never mistaken for a shared
#: constant coupling.
_REAL_EVIDENCE_HANDLE = "ev1_" + ("f6e5d4c3b2" * 4)


def _grounded_evidence_registry(calls: list[DevToolRequest]) -> AskDevToolRegistry:
    """Like ``test_chaos_3297_frame_e2e._evidence_consistent_registry``, but
    clears the tool result's metric evidence_ref_ids to ``()`` instead of a
    real handle -- see the module docstring's finding."""

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
            metric["evidence_ref_ids"] = []
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


def _grounded_answer_with_real_evidence(
    *, script_id: str, summary: str, validity_scope: dict[str, Any]
) -> dict[str, Any]:
    payload = grounded_answer_payload(
        script_id=script_id, summary=summary, validity_scope=validity_scope
    )
    for claim in payload["claims"]:
        claim["evidence_ref_ids"] = (
            [_REAL_EVIDENCE_HANDLE] if claim["evidence_ref_ids"] else []
        )
    for evidence in payload["evidence"]:
        evidence["evidence_ref_id"] = _REAL_EVIDENCE_HANDLE
    for metric in payload["metrics"]:
        metric["evidence_ref_ids"] = []
    return payload


class _ScriptedNarrativeProvider:
    """A narrative provider stand-in: one call, either a fixed
    ``NarrativeProviderResult`` or a fixed exception -- the same pattern
    ``ScriptedAgentProvider`` uses for the model loop, minimized for the
    narrative provider boundary."""

    def __init__(
        self,
        *,
        result: NarrativeProviderResult | None = None,
        raises: Exception | None = None,
    ) -> None:
        self._result = result
        self._raises = raises
        self.calls = 0

    async def generate_narrative(self, brief: Any) -> NarrativeProviderResult:
        del brief
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        assert self._result is not None
        return self._result


async def _drive_answered_run(
    context: Any,
    *,
    script_id: str,
    narrative_provider: Any | None,
) -> tuple[uuid.UUID, dict[str, Any]]:
    """Drive one grounded, evidence-backed answered run through the real
    endpoint with ``narrative_provider`` injected. Returns ``(run_id,
    live_events)``."""

    org_id = context.org_id
    calls: list[DevToolRequest] = []
    scope = _scope_payload(org_id)
    answer = _grounded_answer_with_real_evidence(
        script_id=script_id,
        summary="Twelve items are complete this period.",
        validity_scope=scope,
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
        provider=cast(Any, RecordingProvider(script, script_id=script_id)),
        provider_source="platform",
        provider_family="scripted",
        registry=_grounded_evidence_registry(calls),
        scope_resolver=_resolve,
        versions=versions(),
        narrative_provider=narrative_provider,
    )
    context.app.dependency_overrides[dev_router_module.get_dev_execution_runtime] = (
        lambda: dev_router_module.DevExecutionRuntimeResolution(runtime=real_runtime)
    )

    client = context.client
    created = await client.post(
        "/api/v1/dev/conversations", json={"current_scope": scope}
    )
    conversation_id = created.json()["conversation_id"]
    payload = {
        "schema_version": "dev_message_request.v1",
        "request_id": f"request_{script_id}",
        "client_message_id": f"client_{script_id}",
        "conversation_id": conversation_id,
        "question": "What is the overall status?",
        "question_class": "status",
        "scope": scope,
    }
    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200, live.text
    live_events = dict(_parse_sse_events(live.text))
    assert "answer.completed" in live_events, live.text
    run_id = uuid.UUID(live_events["run.started"]["run_id"])
    return run_id, live_events


async def _run_row(context: Any, run_id: uuid.UUID) -> DevRun:
    async with context.maker() as session:
        run = await session.get(DevRun, run_id)
        assert run is not None, f"no dev_runs row for {run_id}"
        return run


async def _narrative_rows(context: Any, run_id: uuid.UUID) -> list[DevRunNarrative]:
    async with context.maker() as session:
        rows = (
            await session.scalars(
                select(DevRunNarrative).where(DevRunNarrative.run_id == run_id)
            )
        ).all()
        return list(rows)


def _valid_narrative_result(summary: str) -> NarrativeProviderResult:
    return NarrativeProviderResult(
        body=summary,
        referenced_fact_ids=(),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-narrative-v1",
    )


async def test_c3_baseline_no_provider_is_deterministic_fallback(
    dev_api_context: Any,  # noqa: F811
) -> None:
    """The no-provider baseline every other C3 case is compared against.
    The deterministic fallback narrative is itself persisted as its own
    dev_run_narratives row -- it is safe, server-built content, not
    something withheld."""

    run_id, _ = await _drive_answered_run(
        dev_api_context, script_id="c3-baseline", narrative_provider=None
    )
    run = await _run_row(dev_api_context, run_id)
    assert run.narrative_mode == "deterministic_fallback"
    assert run.narrative_failure_code is None
    rows = await _narrative_rows(dev_api_context, run_id)
    assert len(rows) == 1
    assert rows[0].mode == "deterministic_fallback"


@pytest.mark.parametrize(
    ("exc", "expected_code"),
    [
        (NarrativeProviderTimeoutError(), NarrativeFailureCode.PROVIDER_TIMEOUT),
        (NarrativeProviderRefusalError(), NarrativeFailureCode.PROVIDER_REFUSED),
        (
            NarrativeProviderEmptyContentError(),
            NarrativeFailureCode.PROVIDER_EMPTY_CONTENT,
        ),
        (
            NarrativeProviderSchemaViolationError(),
            NarrativeFailureCode.PROVIDER_SCHEMA_VIOLATION,
        ),
        (
            NarrativeProviderBudgetExceededError(),
            NarrativeFailureCode.PROVIDER_OUTPUT_BUDGET_EXCEEDED,
        ),
        (
            NarrativeProviderUnsafeContentError(),
            NarrativeFailureCode.PROVIDER_UNSAFE_CONTENT,
        ),
    ],
)
async def test_c3_provider_failure_matrix_preserves_the_frame_baseline(
    dev_api_context: Any,  # noqa: F811
    exc: Exception,
    expected_code: NarrativeFailureCode,
) -> None:
    """C3: for each of the six typed provider failures, the live run's
    frame/run public_outcome must match the no-provider baseline exactly --
    only narrative_mode/narrative_failure_code differ."""

    baseline_run_id, _ = await _drive_answered_run(
        dev_api_context,
        script_id=f"c3-baseline-{expected_code.value}",
        narrative_provider=None,
    )
    baseline_run = await _run_row(dev_api_context, baseline_run_id)

    provider = _ScriptedNarrativeProvider(raises=exc)
    run_id, _ = await _drive_answered_run(
        dev_api_context,
        script_id=f"c3-{expected_code.value}",
        narrative_provider=provider,
    )
    run = await _run_row(dev_api_context, run_id)

    assert run.public_outcome == baseline_run.public_outcome
    assert run.narrative_mode == "deterministic_fallback"
    assert run.narrative_failure_code == expected_code.value
    assert provider.calls == 1
    rows = await _narrative_rows(dev_api_context, run_id)
    assert len(rows) == 1
    assert rows[0].mode == "deterministic_fallback"


async def test_c3_an_unrecognized_provider_exception_falls_back_and_counts(
    dev_api_context: Any,  # noqa: F811
) -> None:
    """C3 totality proof, live: a provider that raises something outside
    the six typed NarrativeProviderError subclasses entirely must still
    fall back safely through the real endpoint, not 500 the request."""

    class UnwrappedTransportError(Exception):
        pass

    provider = _ScriptedNarrativeProvider(raises=UnwrappedTransportError("boom"))
    run_id, live_events = await _drive_answered_run(
        dev_api_context, script_id="c3-unknown", narrative_provider=provider
    )
    assert "answer.completed" in live_events
    run = await _run_row(dev_api_context, run_id)
    assert run.narrative_mode == "deterministic_fallback"
    assert (
        run.narrative_failure_code
        == NarrativeFailureCode.PROVIDER_UNKNOWN_FAILURE.value
    )


async def test_c3_a_valid_provider_narrative_is_accepted_and_persisted(
    dev_api_context: Any,  # noqa: F811
) -> None:
    """The accept path: a provider result that survives every layer-3
    grounding check is accepted, persisted as its own dev_run_narratives
    row, and recorded as mode='provider' with no failure code."""

    provider = _ScriptedNarrativeProvider(
        result=_valid_narrative_result(
            "Twelve items are complete this period, per the status snapshot."
        )
    )
    run_id, _ = await _drive_answered_run(
        dev_api_context, script_id="c3-accepted", narrative_provider=provider
    )
    run = await _run_row(dev_api_context, run_id)
    assert run.narrative_mode == "provider"
    assert run.narrative_failure_code is None
    rows = await _narrative_rows(dev_api_context, run_id)
    assert len(rows) == 1
    assert rows[0].narrative_text.startswith("Twelve items are complete")


async def test_c4_provider_cannot_claim_an_ungrounded_completion_percentage(
    dev_api_context: Any,  # noqa: F811
) -> None:
    """C4: proves validate_narrative_numeric_containment is reached from
    the live production call path, not only exercised by a unit test that
    calls it directly."""

    provider = _ScriptedNarrativeProvider(
        result=_valid_narrative_result(
            "Twelve items are complete this period. Completion is 42% overall."
        )
    )
    run_id, _ = await _drive_answered_run(
        dev_api_context, script_id="c4-ungrounded-percent", narrative_provider=provider
    )
    run = await _run_row(dev_api_context, run_id)
    assert run.narrative_mode == "deterministic_fallback"
    assert (
        run.narrative_failure_code
        == NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED.value
    )
    rows = await _narrative_rows(dev_api_context, run_id)
    assert len(rows) == 1
    assert rows[0].mode == "deterministic_fallback"


async def test_c4_provider_cannot_reference_a_fact_id_that_does_not_exist(
    dev_api_context: Any,  # noqa: F811
) -> None:
    """C4: proves validate_narrative_fact_references is reached live."""

    provider = _ScriptedNarrativeProvider(
        result=NarrativeProviderResult(
            body="Something happened with the tracked items.",
            referenced_fact_ids=("fact_does_not_exist",),
            referenced_section_ids=(),
            provider_family="scripted",
            model_fingerprint="scripted-narrative-v1",
        )
    )
    run_id, _ = await _drive_answered_run(
        dev_api_context, script_id="c4-unknown-fact", narrative_provider=provider
    )
    run = await _run_row(dev_api_context, run_id)
    assert run.narrative_mode == "deterministic_fallback"
    assert (
        run.narrative_failure_code
        == NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED.value
    )
