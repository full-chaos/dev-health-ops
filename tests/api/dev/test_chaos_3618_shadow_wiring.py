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

import json
import logging
from collections.abc import Mapping
from dataclasses import fields
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
    """An arm reporting a run as unprojectable is a measurement, not a fault.

    **Contract changed deliberately (codex review).** This test asserted
    ``investigation_shadow_records == []`` -- it pinned the measurement
    being thrown away. A disabled seam records ``SKIPPED_DISABLED`` so the
    trial can tell "chose to do nothing" from "never ran"; an arm that
    cannot express a run deserves the same treatment, and had none. The
    assertion is inverted rather than deleted, and the negative control it
    used to carry -- that nothing packet-derived is invented for a run with
    no packet -- is kept and strengthened below.
    """

    producer = _RecordingProducer(None)
    output = await _run(
        script_id="shadow-none",
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=producer,
    )
    assert producer.contexts
    assert output.recorder is not None
    [record] = output.recorder.investigation_shadow_records
    assert record.status is seam.InvestigationShadowStatus.PRODUCER_GAP
    assert record.run_id == output.result.run_id
    # The negative control: a run with no packet has no packet-derived
    # facts, and the record must not manufacture any.
    assert (record.arm_id, record.packet_schema_version, record.packet_id) == (
        None,
        None,
        None,
    )
    assert record.evidence_handles == ()
    assert record.frame_facts == ()


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
        scope_resolution=None,
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


async def test_the_seam_is_evaluated_against_the_frames_evidence() -> None:
    """The same constraint, asserted where it is actually enforced.

    The test above observes the context the PRODUCER was handed. That is
    one hop short of the defect: ``canonical_bypass_offenders`` digests
    against whatever ``InvestigationShadow.evaluate`` was passed, and
    nothing in this suite observed that argument. Proven, not assumed --
    a plant that left the context correct and passed the packet's own
    records to ``evaluate`` made the run's every H4 guarantee vacuous and
    the test above still PASSED.

    Today the wiring passes ``context.canonical_evidence`` straight
    through, so the two are the same sequence. That sameness is precisely
    the assumption this test refuses to keep making on the suite's behalf.
    """

    frame = _frame_with_evidence()
    shadow = _RecordingShadow()
    orchestrator = _bare_orchestrator(
        investigation_shadow=shadow,
        investigation_packet_producer=_RecordingProducer(_packet_payload()),
    )
    await orchestrator._run_investigation_shadow(
        run_id="run-provenance-seam",
        org_id=ORG_ID,
        frame=frame,
        investigation_result=None,
        preflight_result=None,
        scope_resolution=None,
    )
    assert frame.evidence, "fixture must carry evidence or this proves nothing"
    [evaluated] = shadow.evaluated_canonical_evidence
    assert evaluated == tuple(frame.evidence), (
        "GUARD seam_evaluates_against_the_frames_evidence"
    )
    packet_digests = {
        seam._evidence_digest(DevEvidenceRefV2.model_validate(entry["evidence"]))
        for entry in _packet_payload()["evidence_coverage"]["evidence_index"]
    }
    assert {seam._evidence_digest(ref) for ref in evaluated} != packet_digests, (
        "GUARD seam_never_evaluates_a_packet_against_itself"
    )


class _RecordingShadow(seam.InvestigationShadow):
    """A REAL seam that also records what it was asked to compare against.

    Subclasses rather than fakes: a double would prove the wiring calls
    *something*, not that the real validation ran on the real sequence.
    """

    def __init__(self) -> None:
        super().__init__(enabled=True)
        self.evaluated_canonical_evidence: list[tuple[Any, ...]] = []

    def evaluate(
        self, *, canonical_evidence: Any, **kwargs: Any
    ) -> seam.InvestigationShadowRecord:
        self.evaluated_canonical_evidence.append(tuple(canonical_evidence))
        return super().evaluate(canonical_evidence=canonical_evidence, **kwargs)


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


# --------------------------------------------------------------------------
# The record shape, which is the trial's only artefact until 3619
# --------------------------------------------------------------------------


def _sample_record() -> seam.InvestigationShadowRecord:
    return seam.InvestigationShadowRecord(
        run_id="run-1",
        status=seam.InvestigationShadowStatus.RECORDED,
        arm_id="native",
        packet_schema_version="ask_dev_investigation_packet.v1",
        projection_version="native_investigation_projection.v1",
        packet_id="packet-1",
        outcome="supported",
        evidence_handles=("ev1_a", "ev1_b"),
        latency_ms=3,
        detail=None,
        frame_facts=("outcome:supported",),
    )


def test_the_record_payload_carries_every_recorded_field() -> None:
    """No field may be dropped on the way out.

    The field set is derived from the dataclass itself rather than listed
    here, so this test cannot go stale: adding a field to the record makes
    it fail until the payload carries it. A hand-listed expectation on both
    sides would agree with a hand-listed implementation forever, including
    when both were wrong.
    """

    record = _sample_record()
    payload = seam.shadow_record_payload(record)
    expected = {"schema_version"} | {entry.name for entry in fields(record)}
    assert set(payload) == expected, "GUARD record_payload_covers_every_field"
    assert (
        payload["schema_version"] == seam.INVESTIGATION_SHADOW_RECORD_SCHEMA_VERSION
    ), "GUARD record_payload_is_versioned"


def test_the_record_payload_is_json_serialisable() -> None:
    """A consumer parses it; an unserialisable value is an unread record."""

    payload = seam.shadow_record_payload(_sample_record())
    round_tripped = json.loads(json.dumps(payload))
    assert round_tripped["status"] == "recorded"
    assert round_tripped["evidence_handles"] == ["ev1_a", "ev1_b"]


def test_no_record_field_collides_with_a_reserved_log_attribute() -> None:
    """A field named for a ``LogRecord`` attribute silences the whole stream.

    The payload is passed as ``extra=``, and ``Logger.makeRecord`` raises
    ``KeyError`` on a collision — inside a recorder whose failures are
    contained. Every record would then vanish with nothing to read but a
    contained-write line. Today's fields are clean; this is a trap for the
    NEXT field, which is exactly when nobody will be looking.

    The reserved set is derived from a real ``LogRecord``, not typed out,
    so it cannot go stale against the stdlib.
    """

    collisions = {
        entry.name for entry in fields(_sample_record())
    } & seam.RESERVED_LOG_RECORD_ATTRS
    assert collisions == set(), (
        f"GUARD record_fields_avoid_reserved_log_attributes: {sorted(collisions)}"
    )
    # And prove the mechanism is real rather than assumed, so the assertion
    # above is known to be guarding something.
    with pytest.raises(KeyError):
        logging.getLogger(__name__).makeRecord(
            "n", logging.INFO, "p", 1, "m", (), None, extra={"message": "collides"}
        )


def test_the_payload_coerces_values_json_cannot_take() -> None:
    """The field set is DERIVED, so it will grow. Growth must not break it.

    Asserted through the real coercion on a synthetic record rather than on
    the current fields, all of which are already JSON-safe — a test over
    today's fields would pass with the coercion deleted.
    """

    from dataclasses import dataclass as _dataclass
    from datetime import UTC, datetime
    from enum import Enum

    class _Colour(Enum):
        RED = 1

    @_dataclass(frozen=True)
    class _Nested:
        when: datetime
        colour: _Colour
        members: frozenset[str]

    coerced = seam._json_safe(
        _Nested(
            when=datetime(2026, 8, 9, tzinfo=UTC),
            colour=_Colour.RED,
            members=frozenset({"b", "a"}),
        )
    )
    round_tripped = json.loads(json.dumps(coerced))
    assert round_tripped["when"] == "2026-08-09T00:00:00+00:00"
    assert round_tripped["colour"] == 1
    # A set has no order, so neither does its JSON list. Asserting the
    # sequence would have been a test that fails on a different PYTHONHASHSEED
    # — comparing as a set is the honest claim, and the coercion promises
    # membership, not ordering.
    assert set(round_tripped["members"]) == {"a", "b"}


async def test_the_recorder_writes_the_record_into_the_log_message() -> None:
    """The record must survive a formatter that drops ``extra``.

    ``configure_logging`` renders extras only on the JSON path;
    ``LOG_JSON=false`` installs a plain handler that discards them without
    a word. A record that existed only in ``extra`` would then be missing
    from the stream while the stream still looked healthy -- a measurement
    layer failing toward "fine". So the payload is asserted to be IN the
    formatted message, which no formatter configuration can remove.
    """

    from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder

    recorder = PersistenceRunRecorder.__new__(PersistenceRunRecorder)
    record = _sample_record()
    logger = logging.getLogger("dev_health_ops.api.dev.orchestrator_persistence")
    emitted: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, log_record: logging.LogRecord) -> None:
            emitted.append(log_record)

    handler = _Capture()
    logger.addHandler(handler)
    try:
        await recorder.record_investigation_shadow(record)
    finally:
        logger.removeHandler(handler)

    [entry] = [item for item in emitted if "investigation_shadow.record" in item.msg]
    message = entry.getMessage()
    _, _, encoded = message.partition(" ")
    # Parsed defensively ON PURPOSE. With the parse inline in the assert, a
    # message carrying NO payload blew up decoding instead of failing the
    # named assertion -- and because pytest echoes the failing source line,
    # the GUARD string still appeared in the output and the harness CREDITED
    # the guard. That is the case-12 class exactly, caught by the codex
    # review. An unparseable message is now `None`, which the named
    # assertion rejects on its own terms.
    #
    # Caught as `ValueError`, the base class, and the concrete decode
    # error's name appears NOWHERE in this file -- not in code and not in a
    # comment. The harness's forbidden-failure check is a substring match
    # over pytest output, and pytest echoes source: naming it here, even in
    # prose, makes the harness reject this test for a failure it no longer
    # has. (Cost me a round to find; the same trap applies to every
    # forbidden token.)
    try:
        decoded = json.loads(encoded)
    except ValueError:
        decoded = None
    assert decoded == seam.shadow_record_payload(record), (
        "GUARD record_survives_a_formatter_that_drops_extra"
    )


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
