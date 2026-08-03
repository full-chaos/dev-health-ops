"""Amended/versioned stream events (CHAOS-3294 deliverable list).

Mirrors ``dev_health_ops.api.dev.contracts.DevStreamEvent`` /
``validate_stream`` (v1), adapted for the Wave 3.1 lifecycle: scope
resolution becomes resolution-ledger updates (a mention can be committed,
ambiguous, or unresolved — a single "scope resolved" event no longer
captures that), progress states cover the new investigation stages, and
the terminal answer payload is ``DevAnswerV2``. No prompts, chain-of-thought,
raw tool payloads, internal authorization states, or rule internals are
streamed (Amendment TRD v2 §12).

Codex adversarial-review hardening (post-merge, CHAOS-3294) closed three
counterexamples the review reproduced against this module:

* An ``answer.completed`` event's embedded ``DevAnswerV2.run_id`` was never
  checked against the event's own ``run_id`` — the stream-boundary half of
  the answer/frame ``run_id`` closure (see ``answer.py`` module docstring).
  Enforced in ``DevStreamEventV2.validate_event_payload`` below.
* ``validate_stream_v2`` validated each ``resolution.updated`` event's
  ledger independently and never called ``validate_ledger_extends`` between
  successive ledger snapshots in the same stream, so a later
  ``resolution.updated`` event could silently rewrite an earlier one's
  entries. Enforced by tracking the previous ledger snapshot across the
  event loop and requiring both append-only extension and non-decreasing
  ``updated_at``.
* The lifecycle was checked as a handful of separate positional rules, each
  added as its own counterexample arrived (last event is ``done``; the
  terminal result precedes it; ``done`` appears once). Every such rule
  covered one endpoint and left its neighbour open: a premature ``done``
  slipped past the "last event" rule, and a second ``run.started`` slipped
  past all of them. ``_validate_run_lifecycle`` now states the whole
  lifecycle **once**, as a single positional invariant over the event list —
  see its docstring.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from .answer import DevAnswerV2
from .base import ContractModelV2, ServerHandle, ShortText
from .embedded import DevErrorV2
from .subject import DevResolutionLedger, validate_ledger_extends

__all__ = [
    "DevStreamEventV2",
    "ProgressStateV2",
    "StreamEventTypeV2",
    "validate_exactly_one_frame_ready",
    "validate_narrative_fallback_follows_frame_ready",
    "validate_stream_v2",
]


class StreamEventTypeV2(StrEnum):
    RUN_STARTED = "run.started"
    RESOLUTION_UPDATED = "resolution.updated"
    PROGRESS = "progress"
    ANSWER_DELTA = "answer.delta"
    #: CHAOS-3297 requirement 9: fires once a dev_answer_frame.v1 has been
    #: constructed and is the server's committed answer -- the moment the
    #: CHAOS-3299 replay gate becomes reachable for this run, strictly
    #: before the terminal result. No payload: it is a milestone signal,
    #: not a content channel (the frame itself is never streamed early --
    #: only the terminal answer.completed carries it, embedded in the
    #: DevAnswerV2).
    ANSWER_FRAME_READY = "answer.frame_ready"
    #: CHAOS-3297 requirement 9: optional, at most once, fires only when
    #: narrative synthesis fell back to the deterministic narrative --
    #: carries the safe NarrativeFailureCode value (a closed-vocabulary
    #: token, never raw provider content) so a client can distinguish
    #: "the model improved this answer" from "the server's own words".
    ANSWER_NARRATIVE_FALLBACK = "answer.narrative_fallback"
    ANSWER_COMPLETED = "answer.completed"
    WARNING = "warning"
    ERROR = "error"
    DONE = "done"


class ProgressStateV2(StrEnum):
    INTERPRETING_QUESTION = "interpreting_question"
    RESOLVING_SUBJECTS = "resolving_subjects"
    CHECKING_PROJECT_STATUS = "checking_project_status"
    CHECKING_TEAM_HEALTH = "checking_team_health"
    COMPARING_PORTFOLIO = "comparing_portfolio"
    CHECKING_WORKLOAD = "checking_workload"
    CHECKING_OPERATIONAL_CONTROLS = "checking_operational_controls"
    PREPARING_ANSWER = "preparing_answer"


class DevStreamEventV2(ContractModelV2):
    schema_version: Literal["dev_stream_event.v2"]
    run_id: ServerHandle
    sequence: int = Field(ge=0, le=100_000)
    event: StreamEventTypeV2
    occurred_at: AwareDatetime
    progress: ProgressStateV2 | None = None
    resolution_ledger: DevResolutionLedger | None = None
    delta: Annotated[str, StringConstraints(min_length=1, max_length=8_192)] | None = (
        None
    )
    answer: DevAnswerV2 | None = None
    warning: ShortText | None = None
    error: DevErrorV2 | None = None
    terminal_kind: Literal["answer", "error"] | None = None
    #: CHAOS-3297 stack #4: the safe NarrativeFailureCode value carried by
    #: an ``answer.narrative_fallback`` event -- a closed-vocabulary token
    #: (``answer_frames.narrative_fallback.NarrativeFailureCode``), never
    #: raw provider content. Not imported from that module here: contracts
    #: are the leaf of the dependency graph and do not depend on the
    #: orchestration-layer package that produces this value.
    narrative_failure_code: ShortText | None = None

    @model_validator(mode="after")
    def validate_event_payload(self) -> Self:
        required_payload = {
            StreamEventTypeV2.RESOLUTION_UPDATED: (
                "resolution_ledger",
                self.resolution_ledger,
            ),
            StreamEventTypeV2.PROGRESS: ("progress", self.progress),
            StreamEventTypeV2.ANSWER_DELTA: ("delta", self.delta),
            StreamEventTypeV2.ANSWER_COMPLETED: ("answer", self.answer),
            StreamEventTypeV2.WARNING: ("warning", self.warning),
            StreamEventTypeV2.ERROR: ("error", self.error),
            StreamEventTypeV2.DONE: ("terminal_kind", self.terminal_kind),
            StreamEventTypeV2.ANSWER_NARRATIVE_FALLBACK: (
                "narrative_failure_code",
                self.narrative_failure_code,
            ),
        }
        required = required_payload.get(self.event)
        if required is not None and required[1] is None:
            raise ValueError(f"{self.event} requires {required[0]}")
        payloads = {
            "progress": self.progress,
            "resolution_ledger": self.resolution_ledger,
            "delta": self.delta,
            "answer": self.answer,
            "warning": self.warning,
            "error": self.error,
            "terminal_kind": self.terminal_kind,
            "narrative_failure_code": self.narrative_failure_code,
        }
        allowed = {
            StreamEventTypeV2.RUN_STARTED: set(),
            StreamEventTypeV2.RESOLUTION_UPDATED: {"resolution_ledger"},
            StreamEventTypeV2.PROGRESS: {"progress"},
            StreamEventTypeV2.ANSWER_DELTA: {"delta"},
            StreamEventTypeV2.ANSWER_FRAME_READY: set(),
            StreamEventTypeV2.ANSWER_NARRATIVE_FALLBACK: {"narrative_failure_code"},
            StreamEventTypeV2.ANSWER_COMPLETED: {"answer"},
            StreamEventTypeV2.WARNING: {"warning"},
            StreamEventTypeV2.ERROR: {"error"},
            StreamEventTypeV2.DONE: {"terminal_kind"},
        }[self.event]
        unexpected = {
            name for name, value in payloads.items() if value is not None
        } - allowed
        if unexpected:
            raise ValueError(
                f"unexpected payloads for {self.event}: {sorted(unexpected)}"
            )
        if (
            self.event is StreamEventTypeV2.ANSWER_COMPLETED
            and self.answer is not None
            and self.answer.run_id != self.run_id
        ):
            # Codex adversarial review (CHAOS-3294): the stream-boundary half
            # of the run_id closure — see module docstring and answer.py.
            raise ValueError(
                "answer.completed event run_id must match its embedded answer's run_id"
            )
        return self


#: The events that open and close a run. Every other event type is an
#: interior event and may appear any number of times between them.
_LIFECYCLE_EVENTS: dict[str, frozenset[StreamEventTypeV2]] = {
    "run.started": frozenset({StreamEventTypeV2.RUN_STARTED}),
    "terminal result": frozenset(
        {StreamEventTypeV2.ANSWER_COMPLETED, StreamEventTypeV2.ERROR}
    ),
    "done": frozenset({StreamEventTypeV2.DONE}),
}


def _validate_run_lifecycle(events: Sequence[DevStreamEventV2]) -> None:
    """The whole run lifecycle, stated once.

    A stream is exactly one ``run.started`` at index 0, then any number of
    interior events, then exactly one terminal result (``answer.completed``
    or ``error``), then exactly one ``done`` as the final event, whose
    ``terminal_kind`` matches that terminal result. Within the interior,
    exactly one ``answer.frame_ready`` must appear (CHAOS-3297 P1: every
    terminal run persists a frame, so every stream has exactly one
    frame-ready milestone), and at most one ``answer.narrative_fallback``
    may appear, never before ``answer.frame_ready`` -- a narrative can only
    be selected (accepted or fallen back) for a frame that already exists.

    Stated as one positional invariant rather than as separate rules per
    event type deliberately: the round-1 and round-2 counterexamples
    (premature ``done``, duplicate ``done``, duplicate ``run.started``) were
    all the same defect — a lifecycle marker appearing somewhere other than
    its one allowed position — and a rule written per marker keeps leaving
    the next marker open. Here, each of the three fixed-position markers is
    required to occur exactly once and at exactly one index, so no marker
    can repeat or move without failing.

    ``answer.frame_ready``/``answer.narrative_fallback`` are interior
    markers with a floating index rather than a fixed one, so only the
    checks that are independently reachable are written: once the three
    fixed-position checks above have passed, any index not already claimed
    by ``run.started``/the terminal result/``done`` is *necessarily* inside
    the open interior range between them (a pigeonhole argument, not an
    assumption) — so an explicit "frame_ready is inside the interior" or
    "narrative_fallback is before the terminal result" check could never
    independently fail and would be dead code. What genuinely is
    independent: frame_ready must occur exactly once (nothing else
    constrains its count), narrative_fallback at most once, and
    narrative_fallback's index (when present) must be greater than
    frame_ready's -- nothing else orders those two interior markers
    relative to each other.
    """

    expected_indexes = {
        "run.started": 0,
        "terminal result": len(events) - 2,
        "done": len(events) - 1,
    }
    for label, event_types in _LIFECYCLE_EVENTS.items():
        occurrences = [
            index for index, event in enumerate(events) if event.event in event_types
        ]
        if len(occurrences) != 1:
            raise ValueError(
                f"stream must contain exactly one {label} event, found "
                f"{len(occurrences)}"
            )
        if occurrences[0] != expected_indexes[label]:
            raise ValueError(
                f"the {label} event must be at position "
                f"{expected_indexes[label]} of the stream, not {occurrences[0]}"
            )
    terminal_index = expected_indexes["terminal result"]
    terminal = events[terminal_index]
    terminal_kind = (
        "answer" if terminal.event is StreamEventTypeV2.ANSWER_COMPLETED else "error"
    )
    if events[-1].terminal_kind != terminal_kind:
        raise ValueError("done terminal_kind must match the terminal result")

    validate_exactly_one_frame_ready(events)
    validate_narrative_fallback_follows_frame_ready(events)


def validate_exactly_one_frame_ready(events: Sequence[DevStreamEventV2]) -> None:
    """CHAOS-3297 P1: every terminal run persists a frame, so every stream
    has exactly one ``answer.frame_ready`` milestone. A separate,
    independently monkeypatchable function (not inlined into
    ``_validate_run_lifecycle``) so a mutation test can disable this one
    clause without disturbing the fixed-position checks around it -- same
    posture ``contracts_v2.validators`` already documents for its own
    guards.
    """

    frame_ready_indexes = [
        index
        for index, event in enumerate(events)
        if event.event is StreamEventTypeV2.ANSWER_FRAME_READY
    ]
    if len(frame_ready_indexes) != 1:
        raise ValueError(
            "stream must contain exactly one answer.frame_ready event, found "
            f"{len(frame_ready_indexes)}"
        )


def validate_narrative_fallback_follows_frame_ready(
    events: Sequence[DevStreamEventV2],
) -> None:
    """``answer.narrative_fallback`` is optional, at most once, and -- when
    present -- must occur after ``answer.frame_ready`` (a narrative can only
    be selected, accepted or fallen back, for a frame that already exists).
    """

    frame_ready_indexes = [
        index
        for index, event in enumerate(events)
        if event.event is StreamEventTypeV2.ANSWER_FRAME_READY
    ]
    narrative_fallback_indexes = [
        index
        for index, event in enumerate(events)
        if event.event is StreamEventTypeV2.ANSWER_NARRATIVE_FALLBACK
    ]
    if len(narrative_fallback_indexes) > 1:
        raise ValueError(
            "stream must contain at most one answer.narrative_fallback event, "
            f"found {len(narrative_fallback_indexes)}"
        )
    if (
        narrative_fallback_indexes
        and frame_ready_indexes
        and narrative_fallback_indexes[0] < frame_ready_indexes[0]
    ):
        raise ValueError(
            "answer.narrative_fallback must occur after answer.frame_ready"
        )


def validate_stream_v2(events: Sequence[DevStreamEventV2]) -> None:
    """Validate one bounded v2 stream: ordered events, one lifecycle, one ledger chain.

    Three independent checks: the wire-level ordering bounds below, the
    single lifecycle invariant (``_validate_run_lifecycle``), and the
    append-only ledger chain across successive ``resolution.updated``
    snapshots (``validate_ledger_extends`` with non-decreasing
    ``updated_at``).
    """

    if not events:
        raise ValueError("stream must not be empty")
    if len(events) > 100_000:
        raise ValueError("stream exceeds event bound")
    run_ids = {event.run_id for event in events}
    if len(run_ids) != 1:
        raise ValueError("stream events must share one run ID")
    if [event.sequence for event in events] != list(range(len(events))):
        raise ValueError("stream sequence must be contiguous and ordered")

    _validate_run_lifecycle(events)

    previous_ledger: DevResolutionLedger | None = None
    for event in events:
        if event.event is not StreamEventTypeV2.RESOLUTION_UPDATED:
            continue
        ledger = event.resolution_ledger
        if ledger is None:
            continue  # unreachable in practice: the per-event validator requires it
        if previous_ledger is not None:
            if ledger.updated_at < previous_ledger.updated_at:
                raise ValueError(
                    "resolution ledger updated_at must be monotonically "
                    "non-decreasing across resolution.updated events"
                )
            validate_ledger_extends(previous_ledger, ledger)
        previous_ledger = ledger
