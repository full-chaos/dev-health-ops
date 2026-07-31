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
* The terminal-position check only verified the *last* event was ``done``
  and that the lone terminal result (``answer.completed``/``error``)
  immediately preceded it; it never checked that ``done`` occurred *only*
  there, so a stream like ``run.started, done, error, done`` (a premature
  ``done`` before the real terminal result) validated. ``validate_stream_v2``
  now requires ``done`` to appear exactly once, and only as the final event.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from dev_health_ops.api.dev.contracts import DevError

from .answer import DevAnswerV2
from .base import ContractModelV2, OpaqueID, ShortText
from .subject import DevResolutionLedger, validate_ledger_extends

__all__ = [
    "DevStreamEventV2",
    "ProgressStateV2",
    "StreamEventTypeV2",
    "validate_stream_v2",
]


class StreamEventTypeV2(StrEnum):
    RUN_STARTED = "run.started"
    RESOLUTION_UPDATED = "resolution.updated"
    PROGRESS = "progress"
    ANSWER_DELTA = "answer.delta"
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
    run_id: OpaqueID
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
    error: DevError | None = None
    terminal_kind: Literal["answer", "error"] | None = None

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
        }
        allowed = {
            StreamEventTypeV2.RUN_STARTED: set(),
            StreamEventTypeV2.RESOLUTION_UPDATED: {"resolution_ledger"},
            StreamEventTypeV2.PROGRESS: {"progress"},
            StreamEventTypeV2.ANSWER_DELTA: {"delta"},
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


def validate_stream_v2(events: list[DevStreamEventV2]) -> None:
    """Validate one bounded v2 stream: ordered events, one terminal, then done.

    Codex adversarial-review hardening (CHAOS-3294) — see module docstring —
    added two checks beyond the original ones: ``done`` must appear exactly
    once, and only as the stream's final event (closes a premature/duplicate
    ``done`` counterexample); and successive ``resolution.updated`` ledger
    snapshots must extend, never rewrite, one another via
    ``validate_ledger_extends`` with non-decreasing ``updated_at``.
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
    if events[0].event is not StreamEventTypeV2.RUN_STARTED:
        raise ValueError("stream must start with run.started")

    done_indexes = [
        index
        for index, event in enumerate(events)
        if event.event is StreamEventTypeV2.DONE
    ]
    if len(done_indexes) != 1:
        raise ValueError("stream must contain exactly one done event")
    if done_indexes[0] != len(events) - 1:
        # A `done` at any position other than the very last event is either
        # a premature terminal marker (something before the real terminal
        # result claimed the stream was finished) or, combined with the
        # count check above, a duplicate.
        raise ValueError("done must be the final event in the stream")

    terminal_indexes = [
        index
        for index, event in enumerate(events)
        if event.event in {StreamEventTypeV2.ANSWER_COMPLETED, StreamEventTypeV2.ERROR}
    ]
    if len(terminal_indexes) != 1:
        raise ValueError("stream must contain exactly one terminal result")
    terminal_index = terminal_indexes[0]
    if terminal_index != len(events) - 2:
        raise ValueError("terminal result must be immediately followed by done")
    terminal_kind = (
        "answer"
        if events[terminal_index].event is StreamEventTypeV2.ANSWER_COMPLETED
        else "error"
    )
    if events[-1].terminal_kind != terminal_kind:
        raise ValueError("done terminal_kind must match the terminal result")

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
