"""Unit coverage for the pure helper functions in
``test_wave4_corpus_runner_live.py`` -- the parts that do not need pytest's
own fixture injection or live infra to exercise directly.

Importing helpers out of another test module is unconventional but matches
this repo's own precedent (``tests._chaos_3292_preflight`` is a shared
harness other test modules import from); done here rather than duplicating
the arming/scope-building logic a second time.

``_resolve_world_digest_pin`` itself is NOT unit tested here -- it is now a
thin wrapper over ``db_verify.verify_world_digest_via_exec`` (already
covered, with dependency-injected fake runners, by
``tests/acceptance/corpus/test_db_verify.py``) and
``world_digest_guard.require_world_digest_match`` (covered by
``test_world_digest_guard.py``); testing it a third time here would only
re-exercise those same seams through an extra layer of indirection.
"""

from __future__ import annotations

from copy import deepcopy

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevStreamEvent
from scripts.acceptance.corpus.sse_client import SseFrame
from tests.acceptance.test_wave4_corpus_runner_live import (
    _public_outcome_from_events,
    _run_id_from_frames,
    _scope,
)


class TestScope:
    def test_scope_carries_the_requesting_org(self) -> None:
        scope = _scope("org-123")
        assert scope["organization_id"] == "org-123"
        assert scope["schema_version"] == "dev_scope.v1"
        assert scope["direct_scope"] == "organization"

    def test_scope_time_range_end_is_after_start(self) -> None:
        scope = _scope("org-123")
        assert scope["time_range"]["start"] < scope["time_range"]["end"]


class TestRunIdFromFrames:
    def test_empty_frames_returns_none(self) -> None:
        assert _run_id_from_frames([]) is None

    def test_takes_run_id_off_the_first_frame(self) -> None:
        frames = [
            SseFrame(event="run.started", data={"run_id": "run-abc", "sequence": 0}),
            SseFrame(event="done", data={"run_id": "run-abc", "sequence": 5}),
        ]
        assert _run_id_from_frames(frames) == "run-abc"

    def test_missing_run_id_key_returns_none(self) -> None:
        frames = [SseFrame(event="run.started", data={"sequence": 0})]
        assert _run_id_from_frames(frames) is None


def _event(overrides: dict) -> DevStreamEvent:
    base = deepcopy(positive_fixtures()["dev_stream_event.v1"])
    base.update(overrides)
    return DevStreamEvent.model_validate(base)


class TestPublicOutcomeFromEvents:
    """Codex round-2, HIGH, confirmed: AnswerStatus (v1) and PublicOutcome
    (v2) are different vocabularies -- see the function's own docstring."""

    def test_no_terminal_event_returns_none(self) -> None:
        events = [_event({"sequence": 0, "event": "run.started"})]
        assert _public_outcome_from_events(events) is None

    def test_error_event_maps_via_production_table(self) -> None:
        error_payload = deepcopy(positive_fixtures()["dev_error.v1"])
        error_payload["code"] = "scope_ambiguous"
        events = [
            _event(
                {
                    "sequence": 1,
                    "event": "error",
                    "error": error_payload,
                    "terminal_kind": None,
                }
            )
        ]
        assert _public_outcome_from_events(events) == "needs_clarification"

    def test_real_answer_always_maps_to_answered_with_gaps_never_plain_answered(
        self,
    ) -> None:
        answer_payload = deepcopy(positive_fixtures()["dev_answer.v1"])
        events = [
            _event(
                {
                    "sequence": 1,
                    "event": "answer.completed",
                    "answer": answer_payload,
                }
            )
        ]
        outcome = _public_outcome_from_events(events)
        assert outcome == "answered_with_gaps"
        assert outcome != "answered"
