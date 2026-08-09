"""CHAOS-3618 PR 2: the native arm, connected end to end.

Everything here runs through the REAL orchestrator seam
(``tests._chaos_3292_preflight.run_preflight_orchestrator``) with the REAL
plan executor, the REAL subject preflight, the REAL native producer and the
REAL shadow seam. Nothing is stubbed except the services behind the plan
executor, which the CHAOS-3295 harness already fakes for every other
orchestrated test.

That matters because the whole risk of a shadow seam is that each half is
provably correct in isolation and the wiring between them is wrong. The
inherited wiring handed the projection a ``DevScopeResolution`` where it
reads ``.entity_id`` off a ``DevEntityRefV2``, and hardcoded the bounded
window to ``None``; both halves' own suites were green throughout. Only a
run that goes all the way through can catch that class of defect, so these
tests assert on the record a real run leaves behind.
"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from typing import Any

import pytest

from dev_health_ops.api.dev import investigation_shadow as seam
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevToolResult
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.contracts_v2.subject import DevEntityRefV2
from dev_health_ops.api.dev.investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
    AskDevInvestigationPacket,
)
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry, ToolID
from dev_health_ops.context_fabric.native_arm.flags import NATIVE_PROJECTION_FLAG
from dev_health_ops.context_fabric.native_arm.producer import (
    NativeInvestigationPacketProducer,
    native_shadow_wiring,
)
from dev_health_ops.context_fabric.native_arm.projection import (
    NATIVE_ARM_ID,
    NATIVE_PROJECTION_VERSION,
    NativeProjectionGapReason,
)
from dev_health_ops.llm.agent.scripted import ScriptedStep
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    answer_payload,
    run_preflight_orchestrator,
)
from tests._chaos_3295_plan_executor import (
    FakePlanExecutorRuntime,
    InvestigationRecorder,
    executor_for,
)

pytestmark = pytest.mark.asyncio

_QUESTION = "What's the status of the Ask Dev project?"

#: A structurally valid ``EvidenceHandle`` (``ev1_`` + 40 hex).
#:
#: The shared v1 fixture family uses ``ev_01``, which predates the v2
#: handle grammar. ``wrap_legacy_answer_as_frame`` therefore raises on the
#: stock fixture and the orchestrator falls back to an error frame carrying
#: NO evidence -- a pre-existing property of the CHAOS-3292/3295 harness,
#: not of this change (reproduced on the untouched
#: ``test_chaos_3295_plan_executor_acceptance`` suite, which passes while
#: logging ``frame_construction_failed``).
#:
#: That matters here specifically. This suite's whole point is a frame with
#: real canonical evidence in it: an end-to-end proof over an evidence-free
#: frame would compare two empty tuples and pass with the wiring broken --
#: the exact vacuity the PR's own provenance test was already caught at
#: once. So the fixtures are re-minted below, at BOTH ends (tool result and
#: answer), rather than the assertions being weakened to fit.
_MINTED_HANDLE = "ev1_" + hashlib.sha1(b"chaos-3618-native-e2e").hexdigest()

#: A UUID run id, because that is what production passes.
#:
#: ``router.py`` calls the runtime with ``run_id=str(run.id)`` -- the
#: ``dev_runs`` row's UUID -- while this harness defaults to ``"run_01"``.
#: The packet's ``TrialMetadata.run_id`` is a ``ServerHandle``, so a
#: non-UUID correlation key makes the frozen contract reject the packet,
#: and a suite driven on ``run_01`` would report every native run as
#: unprojectable for a reason production does not have. The CHAOS-3423
#: persistence suite overrides these ids the same way and for the same
#: reason. Fixed rather than random: a fixed id keeps the packet's own
#: deterministic uuid5 handles stable across runs.
_RUN_ID = "8f0f2f8a-3618-4b2c-9a1d-2f7c5b6e40a1"


def _mint_evidence_handles(payload: Any) -> Any:
    """Re-point the fixtures' evidence at this run's committed subject.

    Three edits, each with a reason the assertions depend on:

    * ``evidence_ref_id`` becomes a v2-grammar handle, so
      ``wrap_legacy_answer_as_frame`` can build a frame at all.
    * the evidence's entity becomes the committed project, so
      ``_restrict_evidence_to_authorized`` keeps it. The stock fixture
      points at ``item_01``, which this run never authorized, and the
      projection correctly drops it -- leaving an evidence-free packet and
      an assertion that would pass while proving nothing.
    * ``evidence_ref_ids`` on metrics is cleared, which is what production
      genuinely produces here: ``production_runtime``'s ``query_metric.v1``
      scrubs them on every call, and ``DevMetricRefV2``'s XOR validator
      rejects a metric carrying both refs and the ``LEGACY_V1_UNMINTED``
      classification ``_wrap_legacy_metric`` sets.

    Every one of these makes the fixture MORE like production, not less.
    """

    if isinstance(payload, dict):
        if "evidence_ref_id" in payload:
            return {
                **{key: _mint_evidence_handles(v) for key, v in payload.items()},
                "evidence_ref_id": _MINTED_HANDLE,
                "entity_type": ASK_DEV_PROJECT.kind.value,
                "entity_id": ASK_DEV_PROJECT.canonical_id,
                "valid_entity_ids": [ASK_DEV_PROJECT.canonical_id],
            }
        return {
            key: [] if key == "evidence_ref_ids" else _mint_evidence_handles(value)
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [_mint_evidence_handles(item) for item in payload]
    return payload


def _minted_registry(calls: list[Any]) -> AskDevToolRegistry:
    """``recording_registry``'s executor, with v2-valid evidence handles."""

    async def execute(_context: Any, request: Any) -> DevToolResult:
        calls.append(request)
        payload = _mint_evidence_handles(
            deepcopy(positive_fixtures()["dev_tool_result.v1"])
        )
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


def _minted_script(script_id: str) -> list[ScriptedStep]:
    """``status_then_answer``, answering with a v2-valid evidence handle."""

    from dev_health_ops.llm.agent.contracts import AgentFinalAnswer, AgentToolRequest
    from dev_health_ops.llm.agent.scripted import AgentUsage

    return [
        ScriptedStep(
            decision=AgentToolRequest(
                tool_id="status_snapshot.v1",
                arguments={"limit": 25, "include_comparison": False},
                call_id="tool_call_01",
            ),
            usage=AgentUsage(input_tokens=100, output_tokens=10),
        ),
        ScriptedStep(
            decision=AgentFinalAnswer(
                _mint_evidence_handles(answer_payload(script_id=script_id))
            )
        ),
    ]


class _CapturingProducer:
    """The real native producer, plus the context it was handed.

    Delegates rather than reimplements: a test double that *behaved* like
    the producer would prove nothing about the producer.
    """

    def __init__(self) -> None:
        self._inner = NativeInvestigationPacketProducer()
        self.contexts: list[seam.FinishedRunContext] = []
        self.payloads: list[Any] = []

    def build_packet(self, run: seam.FinishedRunContext) -> Any:
        self.contexts.append(run)
        payload = self._inner.build_packet(run)
        self.payloads.append(payload)
        return payload


async def _investigated_run(
    *,
    producer: Any,
    shadow: seam.InvestigationShadow | None,
    script_id: str,
    question: str = _QUESTION,
) -> Any:
    """One fully investigated run: preflight commits, the plan executes."""

    return await run_preflight_orchestrator(
        question=question,
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script=_minted_script,
        script_id=script_id,
        run_id=_RUN_ID,
        registry_factory=_minted_registry,
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(FakePlanExecutorRuntime()),
        investigation_shadow=shadow,
        investigation_packet_producer=producer,
    )


# --------------------------------------------------------------------------
# The end-to-end claim
# --------------------------------------------------------------------------


async def test_a_real_run_produces_one_recorded_native_shadow_record() -> None:
    """The whole path, asserted on the artefact it is supposed to leave.

    Not "the producer was called" and not "no exception escaped" -- either
    would pass with the packet empty, misattributed or rejected. The record
    must be RECORDED, which is only reachable after the seam has validated
    the packet against the canonical contract, matched its trial run id to
    this run, matched its organization, and digest-matched every cited
    evidence record against the frame's own.
    """

    producer = _CapturingProducer()
    output = await _investigated_run(
        producer=producer,
        shadow=seam.InvestigationShadow(enabled=True),
        script_id="native-e2e",
    )

    [record] = output.recorder.investigation_shadow_records
    assert record.status is seam.InvestigationShadowStatus.RECORDED, record.detail
    # Arm-attributed, and attributed from the PACKET rather than from the
    # caller -- so this is the arm the packet declares, not the arm the
    # wiring believed it configured.
    assert record.arm_id == NATIVE_ARM_ID
    assert record.projection_version == NATIVE_PROJECTION_VERSION
    assert record.packet_schema_version == "ask_dev_investigation_packet.v1"
    # Run-id bound. The seam rejects a packet whose trial run id names a
    # different run (CANONICAL_BYPASS_REJECTED), so RECORDED already proves
    # the binding; this pins which run it bound to.
    assert record.run_id == output.result.run_id


async def test_the_recorded_packet_passes_the_canonical_validator() -> None:
    """The seam validated it; validate it again from the outside.

    The seam could in principle be validating against something other than
    the frozen manifest's model, and a suite that only trusted its verdict
    would never notice.
    """

    producer = _CapturingProducer()
    await _investigated_run(
        producer=producer,
        shadow=seam.InvestigationShadow(enabled=True),
        script_id="native-e2e-valid",
    )

    [payload] = producer.payloads
    assert payload is not None, "the run produced no packet at all"
    model = INVESTIGATION_CONTRACT_MODELS["ask_dev_investigation_packet.v1"]
    packet = model.model_validate(payload)
    # Narrowed, not cast: the registry entry being the packet model is a
    # claim the seam also makes (and raises on), so asserting it here keeps
    # this test honest if the registry ever returns something else.
    assert isinstance(packet, AskDevInvestigationPacket)
    assert packet.versions.trial is not None
    assert packet.versions.trial.arm_id == NATIVE_ARM_ID


async def test_every_cited_evidence_record_is_the_frames_own() -> None:
    """Canonical authority, asserted on a run that actually cites evidence.

    ``canonical_bypass_offenders`` is the enforcement point, and it compares
    against whatever the wiring supplies. This asserts the *supplied*
    sequence is the frame's -- and that the packet's citations are a subset
    of it -- on a real run rather than on a hand-built context.
    """

    producer = _CapturingProducer()
    output = await _investigated_run(
        producer=producer,
        shadow=seam.InvestigationShadow(enabled=True),
        script_id="native-e2e-evidence",
    )

    [context] = producer.contexts
    [frame] = output.recorder.frames
    assert context.canonical_evidence == tuple(frame.evidence)
    [payload] = producer.payloads
    assert payload is not None
    cited = [
        DevEvidenceRefV2.model_validate(entry["evidence"])
        for entry in payload["evidence_coverage"]["evidence_index"]
    ]
    assert cited, "fixture must cite evidence or this test proves nothing"
    offenders = seam.canonical_bypass_offenders(
        packet_evidence=cited, canonical_evidence=context.canonical_evidence
    )
    assert offenders == ()


# --------------------------------------------------------------------------
# The window is the run's own, not a manufactured one
# --------------------------------------------------------------------------


async def test_the_projected_window_is_the_runs_own_scope_window() -> None:
    """The bounded time context comes from the run's scope decision.

    Asserted against the resolution the run published, not against a
    constant: a test comparing the packet to a literal date would still
    pass if both the wiring and the fixture drifted together.
    """

    producer = _CapturingProducer()
    output = await _investigated_run(
        producer=producer,
        shadow=seam.InvestigationShadow(enabled=True),
        script_id="native-e2e-window",
    )

    [context] = producer.contexts
    published = output.result.scope_resolution
    assert published is not None
    expected = seam.run_window(published)
    assert expected is not None
    assert (context.window_start, context.window_end) == expected, (
        "GUARD window_is_the_runs_own"
    )
    [payload] = producer.payloads
    assert payload is not None
    time_context = payload["analytical_job"]["time_context"]
    assert datetime.fromisoformat(time_context["start"]) == expected[0]
    assert datetime.fromisoformat(time_context["end"]) == expected[1]
    # The epoch sentinel this field used to default to. Named explicitly
    # because "not 1970" is the exact regression a restored default causes,
    # and an equality assertion against the resolution alone would still
    # hold if the resolution itself were the sentinel.
    assert expected[0].year > 1970


async def test_a_run_without_a_scope_decision_is_refused_not_dated() -> None:
    """No window means no packet -- never a substituted one.

    Calls the producer directly with a window-less context, because the
    orchestrator cannot reach a persisted frame without a scope decision:
    the reachable-through-the-seam version of this test would be vacuous.
    """

    producer = NativeInvestigationPacketProducer()
    context = seam.FinishedRunContext(
        run_id="run-no-window",
        organization_id=ORG_ID,
        frame=None,
        investigation_result=None,
        interpretation=None,
        ledger=None,
        subject_set=None,
        committed_subject=None,
        window_start=None,
        window_end=None,
        canonical_evidence=(),
    )
    assert producer.build_packet(context) is None


async def test_the_window_gap_is_named_before_any_other_refusal() -> None:
    """A window-less run reports the window as the reason.

    ``build_packet`` returns ``None`` for every kind of unprojectable run,
    so the return value alone cannot distinguish "no window" from "no
    interpretation" -- and a trial counting reasons would read one as the
    other. This asserts the reason, through the same private path the
    producer's own logging uses.

    The interpretation is the REAL one from a real run, so the assertion is
    that the window is what stops an otherwise projectable run -- not that
    the projection refused for some earlier reason it would have refused
    for anyway.
    """

    context = await _finished_context(script_id="native-window-gap")
    assert context.interpretation is not None
    outcome = NativeInvestigationPacketProducer()._project(
        _without(context, window_start=None, window_end=None)
    )
    assert outcome.packet is None
    assert [gap.reason for gap in outcome.gaps] == [
        NativeProjectionGapReason.NO_BOUNDED_WINDOW
    ]


async def test_an_uninterpreted_run_is_a_distinct_named_gap() -> None:
    """The other refusal, and it must not be confused with the first."""

    context = await _finished_context(script_id="native-interp-gap")
    outcome = NativeInvestigationPacketProducer()._project(
        _without(context, interpretation=None)
    )
    assert outcome.packet is None
    assert [gap.reason for gap in outcome.gaps] == [
        NativeProjectionGapReason.NO_INTERPRETED_QUESTION
    ]


async def test_the_same_context_unmodified_does_project() -> None:
    """The control the two refusal tests need.

    Without it, both would pass against a context that could never have
    produced a packet for any reason at all -- the refusal would be
    attributed to the field removed rather than to anything the test did.
    """

    context = await _finished_context(script_id="native-gap-control")
    outcome = NativeInvestigationPacketProducer()._project(context)
    assert outcome.gaps == (), "GUARD run_ids_compared_in_one_space"
    assert outcome.packet is not None


async def test_the_committed_subject_is_a_canonical_entity_ref() -> None:
    """The wiring hands the arm a subject, not the scope it was committed as.

    ``FinishedRunContext.committed_subject`` is typed ``Any`` so the seam
    stays arm-neutral, which means nothing but this test stands between the
    projection and a ``DevScopeResolution`` it would read ``.entity_id``
    off. That is precisely what the inherited wiring passed.
    """

    context = await _finished_context(script_id="native-committed-subject")
    assert isinstance(context.committed_subject, DevEntityRefV2), (
        "GUARD committed_subject_is_a_canonical_ref"
    )
    assert context.committed_subject.entity_id == ASK_DEV_PROJECT.canonical_id


# --------------------------------------------------------------------------
# Inertness, with the REAL arm wired
# --------------------------------------------------------------------------


async def test_wiring_the_real_native_arm_leaves_the_run_byte_identical() -> None:
    """The inertness proof, against the arm that will actually be wired.

    The existing wiring suite proves inertness against an always-raising
    producer. This proves it against the real one, which does far more work
    -- validates a contract, digests evidence, builds a packet -- and is
    therefore the version that could plausibly perturb the run.
    """

    baseline = await _investigated_run(
        producer=None, shadow=None, script_id="native-inert-baseline"
    )
    wired = await _investigated_run(
        producer=NativeInvestigationPacketProducer(),
        shadow=seam.InvestigationShadow(enabled=True),
        script_id="native-inert-wired",
    )
    assert wired.outcome_tuple() == baseline.outcome_tuple(), (
        "GUARD native_arm_never_perturbs_the_run"
    )
    assert baseline.recorder.investigation_shadow_records == []
    assert len(wired.recorder.investigation_shadow_records) == 1


# --------------------------------------------------------------------------
# The flags, and the fact that they gate something
# --------------------------------------------------------------------------


def test_both_flags_off_yields_no_objects_at_all() -> None:
    """Off is the absence of a collaborator, not a disabled one."""

    assert native_shadow_wiring({}) == (None, None)
    assert native_shadow_wiring(
        {
            seam.INVESTIGATION_SHADOW_FLAG: "true",
            NATIVE_PROJECTION_FLAG: "yes",
        }
    ) == (None, None), "only the exact string '1' may switch a trial arm on"


def test_the_flags_are_independent() -> None:
    """The seam is machinery; the producer is one arm.

    A trial must be able to switch the seam on with the graph arm's
    producer, so neither flag may imply the other.
    """

    shadow, producer = native_shadow_wiring({seam.INVESTIGATION_SHADOW_FLAG: "1"})
    assert shadow is not None and shadow.enabled
    assert producer is None

    shadow, producer = native_shadow_wiring({NATIVE_PROJECTION_FLAG: "1"})
    assert shadow is None
    assert isinstance(producer, NativeInvestigationPacketProducer)


def test_both_flags_on_yields_a_wired_pair() -> None:
    shadow, producer = native_shadow_wiring(
        {seam.INVESTIGATION_SHADOW_FLAG: "1", NATIVE_PROJECTION_FLAG: "1"}
    )
    assert shadow is not None and shadow.enabled
    assert isinstance(producer, NativeInvestigationPacketProducer)


def test_the_native_projection_flag_is_scrubbed_between_tests() -> None:
    """An ambient direnv value must not decide whether packets are projected.

    ``flags.py`` claimed CHAOS-3402's drift guard enforces this, and it did
    not: the flag was read through a local named ``source``, which the
    guard's scan cannot see (``_ENV_TARGETS`` is ``os``/``os.environ``/
    ``environ``), so the name was missing from ``SCRUB_ENV_NAMES`` while the
    guard stayed green. The read is now spelled the way the guard sees.
    This asserts the outcome directly, because a guard that was silently
    blind once can be silently blind again.
    """

    from tests._env_isolation import SCRUB_ENV_NAMES

    assert NATIVE_PROJECTION_FLAG in SCRUB_ENV_NAMES
    assert seam.INVESTIGATION_SHADOW_FLAG in SCRUB_ENV_NAMES


async def _finished_context(*, script_id: str) -> seam.FinishedRunContext:
    """The context a REAL run hands the producer.

    Built by driving the orchestrator rather than by hand: a hand-built
    context is exactly what let the two wiring defects this suite exists
    for survive their own green suites.
    """

    producer = _CapturingProducer()
    await _investigated_run(
        producer=producer,
        shadow=seam.InvestigationShadow(enabled=True),
        script_id=script_id,
    )
    [context] = producer.contexts
    return context


def _without(context: seam.FinishedRunContext, **overrides: Any) -> Any:
    return replace(context, **overrides)
