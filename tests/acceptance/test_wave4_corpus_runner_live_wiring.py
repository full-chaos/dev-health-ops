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
from pathlib import Path

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevStreamEvent
from scripts.acceptance.corpus.principals import (
    BRIDGED_PROVISIONING_MARKER,
    SEEDED_PROVISIONING_MARKER,
)
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


class TestReceiptRecordsProvisioningMode:
    """Adversarial round 3: the marker used to live only in a free-text
    ``detail`` string, and deleting that fragment left every test green.

    A receipt is evidence. Evidence that cannot distinguish a run against
    seeded credentials from one that took the temporary admin-set-password
    bridge is weaker than it looks -- the bridge mutates a digest-covered
    column, so the two are not equivalent runs.
    """

    def test_the_runner_records_a_named_provisioning_check(self) -> None:
        # Absolute, derived from this file: a relative path would depend on
        # cwd, and a source-reading assertion that cannot find its source is
        # exactly the "measurement that did not happen" shape. read_text
        # raises loudly if it is ever wrong.
        source = (
            Path(__file__).with_name("test_wave4_corpus_runner_live.py")
        ).read_text(encoding="utf-8")
        assert "provisioned_via_" in source, (
            "the runner no longer records a named provisioning-mode check -- "
            "receipts can no longer say which credential path produced them"
        )
        assert 'category="provisioning-mode"' in source

    def test_the_two_markers_are_distinguishable(self) -> None:
        assert BRIDGED_PROVISIONING_MARKER != SEEDED_PROVISIONING_MARKER
        assert BRIDGED_PROVISIONING_MARKER
        assert SEEDED_PROVISIONING_MARKER
