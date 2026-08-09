"""CHAOS-3618 PR 2: the shadow seam is wired, and wiring it changes nothing.

Two claims, and the second is the one that matters:

1. The seam runs — a packet producer is called after the frame persists and
   its record reaches the recorder.
2. **Turning it on changes no user-visible output.** Proved the CHAOS-3389
   way: the same run's ``OrchestratorResult`` must be byte-identical with
   the seam unwired, wired-but-disabled, and wired-and-enabled against a
   producer that always raises. Anything less would be asserting inertness
   rather than demonstrating it.

The canonical-evidence test is the third claim and the reason PR 2 has a
brief of its own. The seam has no independent source of truth: it compares
each cited record against whatever ``canonical_evidence`` it was handed. If
the wiring ever passes the arm's own packet evidence back in, every H4
guarantee silently becomes a comparison of a value to itself — a check that
cannot fail, wearing the appearance of one.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from dev_health_ops.api.dev import investigation_shadow as seam
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    Recorder,
    RunOutput,
    run_preflight_orchestrator,
)

pytestmark = pytest.mark.asyncio

_QUESTION = "What's the status of the Ask Dev project?"


class _RecordingProducer:
    """Captures the context it was handed, and emits a fixed packet."""

    def __init__(self, payload: Mapping[str, Any] | None) -> None:
        self.payload = payload
        self.contexts: list[seam.FinishedRunContext] = []

    def build_packet(self, run: seam.FinishedRunContext) -> Mapping[str, Any] | None:
        self.contexts.append(run)
        return self.payload


class _ExplodingProducer:
    """The adversarial case: an arm that always fails."""

    def build_packet(self, run: seam.FinishedRunContext) -> Mapping[str, Any] | None:
        raise RuntimeError("the arm blew up")


async def _run(**kwargs: Any) -> RunOutput:
    return await run_preflight_orchestrator(
        question=_QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        **kwargs,
    )


# --------------------------------------------------------------------------
# Inertness — the claim that matters
# --------------------------------------------------------------------------


async def test_the_seam_unwired_and_wired_but_disabled_are_identical() -> None:
    baseline = await _run(script_id="shadow-off-baseline")
    unwired = await _run(script_id="shadow-off-unwired", investigation_shadow=None)
    disabled = await _run(
        script_id="shadow-off-disabled",
        investigation_shadow=seam.InvestigationShadow(enabled=False),
        investigation_packet_producer=_RecordingProducer(None),
    )
    assert unwired.outcome_tuple() == baseline.outcome_tuple()
    assert disabled.outcome_tuple() == baseline.outcome_tuple()


async def test_an_always_raising_producer_leaves_the_run_byte_identical() -> None:
    """The stronger claim: a WORKING, ENABLED seam whose arm always raises.

    A shadow that can fail the run it shadows is worse than no shadow, and
    a producer is third-party code from the orchestrator's point of view.
    """

    baseline = await _run(script_id="shadow-fail-baseline")
    exploding = await _run(
        script_id="shadow-fail-shadow",
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=_ExplodingProducer(),
    )
    assert exploding.outcome_tuple() == baseline.outcome_tuple(), (
        "GUARD shadow_failure_never_touches_the_run"
    )
    assert exploding.recorder is not None
    assert exploding.recorder.investigation_shadow_records == []


async def test_a_recorder_that_always_raises_leaves_the_run_byte_identical() -> None:
    """The other half: persistence of the record must not fail the run."""

    class _FailingRecorder(Recorder):
        async def record_investigation_shadow(self, record: Any) -> None:
            raise RuntimeError("the shadow write blew up")

    baseline = await _run(script_id="shadow-write-baseline")
    failing = await _run(
        script_id="shadow-write-shadow",
        recorder_factory=_FailingRecorder,
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=_RecordingProducer(_packet_payload()),
    )
    assert failing.outcome_tuple() == baseline.outcome_tuple()


def _packet_payload() -> dict[str, Any]:
    from dev_health_ops.api.dev.investigation_contract.fixtures import (
        positive_variant_fixtures,
    )

    variants = dict(positive_variant_fixtures()["ask_dev_investigation_packet.v1"])
    payload = variants["trial_metadata_present"]
    assert isinstance(payload, dict)
    return dict(payload)


# --------------------------------------------------------------------------
# The seam actually runs
# --------------------------------------------------------------------------


async def test_an_enabled_seam_calls_the_producer_and_records_its_verdict() -> None:
    producer = _RecordingProducer(_packet_payload())
    output = await _run(
        script_id="shadow-on",
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=producer,
    )
    assert producer.contexts, "the producer was never called"
    assert output.recorder is not None
    assert len(output.recorder.investigation_shadow_records) == 1


async def test_a_producer_returning_none_is_a_normal_outcome() -> None:
    """An arm reporting a run as unprojectable is a measurement, not a fault."""

    producer = _RecordingProducer(None)
    output = await _run(
        script_id="shadow-none",
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=producer,
    )
    assert producer.contexts
    assert output.recorder is not None
    assert output.recorder.investigation_shadow_records == []


# --------------------------------------------------------------------------
# The PR 2 brief's standing constraint
# --------------------------------------------------------------------------


async def test_canonical_evidence_comes_from_the_run_not_the_packet() -> None:
    """The load-bearing constraint of this whole PR.

    ``canonical_bypass_offenders`` digests each cited record against the
    ``canonical_evidence`` the wiring supplies. If that sequence is ever
    sourced from the packet being evaluated, the digest compares a value to
    itself and every H4 guarantee — payload forgery, tenant scope, run scope
    — silently becomes a check that cannot fail.

    Calls ``_run_investigation_shadow`` DIRECTLY with a frame that carries
    evidence. The first version drove a full scripted run and asserted
    ``canonical_evidence == frame.evidence``, which was **entirely vacuous**:
    the harness's frame carries no evidence, so it compared two empty tuples
    and a plant sourcing canonical evidence from the packet passed cleanly.
    The guard-injection harness caught it by refusing to credit the plant —
    the third time this session that observing an outcome instead of the
    mechanism produced a test that could not fail.
    """

    frame = _frame_with_evidence()
    producer = _RecordingProducer(_packet_payload())
    orchestrator = _bare_orchestrator(
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=producer,
    )
    await orchestrator._run_investigation_shadow(
        run_id="run-provenance",
        org_id=ORG_ID,
        frame=frame,
        investigation_result=None,
        preflight_result=None,
    )
    [context] = producer.contexts
    assert frame.evidence, "fixture must carry evidence or this proves nothing"
    assert context.canonical_evidence == tuple(frame.evidence), (
        "GUARD canonical_evidence_is_the_frames_own"
    )
    # Handles are shared by design -- the packet CITES canonical records, so
    # the handle sets match and prove nothing. The distinguishing property is
    # the payload, which is exactly what H4's digest compares.
    packet_digests = {
        seam._evidence_digest(DevEvidenceRefV2.model_validate(entry["evidence"]))
        for entry in _packet_payload()["evidence_coverage"]["evidence_index"]
    }
    supplied_digests = {
        seam._evidence_digest(ref) for ref in context.canonical_evidence
    }
    assert supplied_digests != packet_digests, (
        "GUARD canonical_evidence_is_not_the_packets_own"
    )


async def test_the_context_is_scoped_to_the_run_it_describes() -> None:
    producer = _RecordingProducer(_packet_payload())
    output = await _run(
        script_id="shadow-scope",
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=producer,
    )
    [context] = producer.contexts
    assert context.organization_id == ORG_ID
    assert context.run_id == output.result.run_id


def test_the_producer_protocol_takes_a_finished_run_and_nothing_else() -> None:
    """The orchestrator hands an arm one completed run, never a service."""

    import inspect

    parameters = set(
        inspect.signature(seam.InvestigationPacketProducer.build_packet).parameters
    ) - {"self"}
    assert parameters == {"run"}


def _frame_with_evidence() -> Any:
    """A server-owned frame that actually carries canonical evidence.

    The scripted harness produces a frame with an empty ``evidence`` tuple,
    which is fine for the inertness proofs above and useless for a
    provenance proof — hence this explicit fixture.
    """

    from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
    from tests._chaos_3292_preflight import Recorder  # noqa: F401

    payload = _packet_payload()
    canonical = tuple(
        DevEvidenceRefV2.model_validate(entry["evidence"]).model_copy(
            update={"display_label": "canonical, minted by the evidence service"}
        )
        for entry in payload["evidence_coverage"]["evidence_index"]
    )
    assert canonical
    return _StubFrame(evidence=canonical)


class _StubFrame:
    """Only what ``_run_investigation_shadow`` reads off a frame."""

    def __init__(self, *, evidence: Any) -> None:
        self.evidence = evidence


def _bare_orchestrator(**kwargs: Any) -> Any:
    """A DevOrchestrator with only the fields the shadow helper touches."""

    from dev_health_ops.api.dev.orchestrator import DevOrchestrator, NullRunRecorder

    orchestrator = DevOrchestrator.__new__(DevOrchestrator)
    orchestrator._investigation_shadow = kwargs["investigation_shadow"]
    orchestrator._investigation_packet_producer = kwargs[
        "investigation_packet_producer"
    ]
    orchestrator._recorder = NullRunRecorder()
    return orchestrator
