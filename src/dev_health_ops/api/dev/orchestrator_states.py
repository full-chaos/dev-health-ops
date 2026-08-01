"""The Ask Dev run-state vocabulary, as an import leaf.

Split out of ``orchestrator`` for a structural reason, not a stylistic one:
``preflight_outcomes`` maps a public outcome to the terminal run state, and
``orchestrator`` imports the preflight. With ``RunState`` defined in
``orchestrator`` that is a genuine import cycle, in which the name a module
sees depends on which one an importer reached first — the same shape the
contracts_v2 package split ``no_answer_policy`` out to avoid.

``orchestrator`` re-exports both names, so every existing
``from .orchestrator import RunState`` keeps working.

Every member here must also appear in the ``ck_dev_runs_state`` CHECK
constraint (``models/dev_persistence.py``); adding one without the matching
Alembic revision fails at persistence, not at import.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = ["TERMINAL_STATES", "RunState"]


class RunState(StrEnum):
    ACCEPTED = "accepted"
    RESOLVING_SCOPE = "resolving_scope"
    # CHAOS-3292: server-owned interpretation and per-mention subject
    # resolution, both before the first model round.
    INTERPRETING = "interpreting"
    RESOLVING_SUBJECTS = "resolving_subjects"
    MODEL_DECISION = "model_decision"
    TOOL_VALIDATION = "tool_validation"
    TOOL_EXECUTION = "tool_execution"
    ANSWER_VALIDATION = "answer_validation"
    COMPLETED = "completed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    REFUSED = "refused"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATES = frozenset(
    {
        RunState.COMPLETED,
        RunState.INSUFFICIENT_EVIDENCE,
        RunState.REFUSED,
        RunState.FAILED,
        RunState.CANCELLED,
    }
)
