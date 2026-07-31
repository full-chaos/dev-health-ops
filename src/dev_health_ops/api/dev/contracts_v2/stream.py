"""Amended/versioned stream events (CHAOS-3294 deliverable list).

Mirrors ``dev_health_ops.api.dev.contracts.DevStreamEvent`` /
``validate_stream`` (v1), adapted for the Wave 3.1 lifecycle: scope
resolution becomes resolution-ledger updates (a mention can be committed,
ambiguous, or unresolved — a single "scope resolved" event no longer
captures that), progress states cover the new investigation stages, and
the terminal answer payload is ``DevAnswerV2``. No prompts, chain-of-thought,
raw tool payloads, internal authorization states, or rule internals are
streamed (Amendment TRD v2 §12).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from dev_health_ops.api.dev.contracts import DevError

from .answer import DevAnswerV2
from .base import ContractModelV2, OpaqueID, ShortText
from .subject import DevResolutionLedger

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
        return self


def validate_stream_v2(events: list[DevStreamEventV2]) -> None:
    """Validate one bounded v2 stream: ordered events, one terminal, then done."""

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
    terminal_indexes = [
        index
        for index, event in enumerate(events)
        if event.event in {StreamEventTypeV2.ANSWER_COMPLETED, StreamEventTypeV2.ERROR}
    ]
    if len(terminal_indexes) != 1:
        raise ValueError("stream must contain exactly one terminal result")
    terminal_index = terminal_indexes[0]
    if (
        terminal_index != len(events) - 2
        or events[-1].event is not StreamEventTypeV2.DONE
    ):
        raise ValueError("terminal result must be immediately followed by done")
    terminal_kind = (
        "answer"
        if events[terminal_index].event is StreamEventTypeV2.ANSWER_COMPLETED
        else "error"
    )
    if events[-1].terminal_kind != terminal_kind:
        raise ValueError("done terminal_kind must match the terminal result")
