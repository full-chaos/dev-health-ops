"""Unit coverage for ``scripts.acceptance.corpus.invariants``."""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.invariants import (
    CHECKS,
    InvariantCheckError,
    InvariantContext,
    evaluate_invariant,
    register_check,
)


def _context(
    *,
    resolution_path: str | None = "deterministic-exact",
    public_outcome: str | None = "answered",
    events: tuple = (),
    expectations: dict | None = None,
    assistant_schema_versions: tuple = (),
) -> InvariantContext:
    return InvariantContext(
        resolution_path=resolution_path,
        public_outcome=public_outcome,
        events=events,
        expectations=expectations or {},
        assistant_schema_versions=assistant_schema_versions,
    )


def _scope_resolved_event(*, outcome: str, candidates: list | None = None) -> dict:
    return {
        "event": "scope.resolved",
        "data": {
            "scope_resolution": {
                "outcome": outcome,
                "candidates": candidates or [],
            }
        },
    }


def _candidate(entity_id: str) -> dict:
    return {"entity_ref": {"entity_id": entity_id}, "reason": "close match"}


class TestResolutionPathIn:
    def test_passes_when_the_actual_path_is_in_the_literal_allowed_list(self) -> None:
        result = evaluate_invariant(
            {
                "check": "resolution_path_in",
                "args": {"allowed": ["deterministic-exact"]},
            },
            _context(resolution_path="deterministic-exact"),
        )
        assert result.passed

    def test_fails_when_the_actual_path_is_not_in_the_allowed_list(self) -> None:
        result = evaluate_invariant(
            {
                "check": "resolution_path_in",
                "args": {"allowed": ["deterministic-alias"]},
            },
            _context(resolution_path="deterministic-exact"),
        )
        assert not result.passed
        assert "deterministic-exact" in result.detail

    def test_pulls_the_expected_value_from_the_profile(self) -> None:
        result = evaluate_invariant(
            {
                "check": "resolution_path_in",
                "args": {"from_profile": "expected_resolution_path"},
            },
            _context(
                resolution_path="deterministic-alias",
                expectations={"expected_resolution_path": "deterministic-alias"},
            ),
        )
        assert result.passed

    def test_missing_profile_key_raises(self) -> None:
        with pytest.raises(InvariantCheckError, match="not found"):
            evaluate_invariant(
                {"check": "resolution_path_in", "args": {"from_profile": "nope"}},
                _context(expectations={}),
            )

    def test_no_allowed_and_no_from_profile_raises(self) -> None:
        with pytest.raises(InvariantCheckError, match="requires"):
            evaluate_invariant({"check": "resolution_path_in", "args": {}}, _context())

    def test_literal_and_from_profile_combine(self) -> None:
        result = evaluate_invariant(
            {
                "check": "resolution_path_in",
                "args": {
                    "allowed": ["deterministic-exact"],
                    "from_profile": "expected_resolution_path",
                },
            },
            _context(
                resolution_path="deterministic-alias",
                expectations={"expected_resolution_path": "deterministic-alias"},
            ),
        )
        assert result.passed

    def test_unobserved_none_never_matches_even_if_literally_allowed(self) -> None:
        """Codex round-1, HIGH, confirmed: `None` (unobserved) must never
        satisfy this check, even if a case/profile authoring mistake put a
        literal `None` into the allowed list."""

        result = evaluate_invariant(
            {
                "check": "resolution_path_in",
                "args": {"allowed": [None, "deterministic-exact"]},
            },
            _context(resolution_path=None),
        )
        assert not result.passed
        assert "not observed" in result.detail

    def test_unobserved_none_fails_even_from_profile(self) -> None:
        result = evaluate_invariant(
            {
                "check": "resolution_path_in",
                "args": {"from_profile": "expected_resolution_path"},
            },
            _context(
                resolution_path=None,
                expectations={"expected_resolution_path": None},
            ),
        )
        assert not result.passed


class TestPublicOutcomeIn:
    def test_passes_when_actual_outcome_is_allowed(self) -> None:
        result = evaluate_invariant(
            {"check": "public_outcome_in", "args": {"allowed": ["answered"]}},
            _context(public_outcome="answered"),
        )
        assert result.passed

    def test_fails_when_actual_outcome_is_not_allowed(self) -> None:
        result = evaluate_invariant(
            {"check": "public_outcome_in", "args": {"allowed": ["answered"]}},
            _context(public_outcome="needs_clarification"),
        )
        assert not result.passed

    def test_pulls_expected_value_from_profile(self) -> None:
        result = evaluate_invariant(
            {
                "check": "public_outcome_in",
                "args": {"from_profile": "expected_public_outcome"},
            },
            _context(
                public_outcome="answered",
                expectations={"expected_public_outcome": "answered"},
            ),
        )
        assert result.passed

    def test_unobserved_none_never_passes(self) -> None:
        result = evaluate_invariant(
            {"check": "public_outcome_in", "args": {"allowed": ["answered"]}},
            _context(public_outcome=None),
        )
        assert not result.passed
        assert "not observed" in result.detail


class TestNoInternalError:
    def test_passes_with_no_error_events(self) -> None:
        result = evaluate_invariant(
            {"check": "no_internal_error"},
            _context(events=({"event": "answer.completed", "data": {}},)),
        )
        assert result.passed

    def test_passes_with_a_non_internal_error(self) -> None:
        events = ({"event": "error", "data": {"error": {"code": "scope_ambiguous"}}},)
        result = evaluate_invariant(
            {"check": "no_internal_error"}, _context(events=events)
        )
        assert result.passed

    def test_fails_on_an_internal_error_event(self) -> None:
        events = ({"event": "error", "data": {"error": {"code": "internal_error"}}},)
        result = evaluate_invariant(
            {"check": "no_internal_error"}, _context(events=events)
        )
        assert not result.passed


class TestScopeResolutionOutcomeIn:
    def test_passes_when_actual_outcome_is_allowed(self) -> None:
        events = (_scope_resolved_event(outcome="exact"),)
        result = evaluate_invariant(
            {"check": "scope_resolution_outcome_in", "args": {"allowed": ["exact"]}},
            _context(events=events),
        )
        assert result.passed

    def test_fails_when_actual_outcome_is_not_allowed(self) -> None:
        events = (_scope_resolved_event(outcome="ambiguous"),)
        result = evaluate_invariant(
            {"check": "scope_resolution_outcome_in", "args": {"allowed": ["exact"]}},
            _context(events=events),
        )
        assert not result.passed

    def test_no_scope_resolved_event_fails(self) -> None:
        result = evaluate_invariant(
            {"check": "scope_resolution_outcome_in", "args": {"allowed": ["exact"]}},
            _context(events=()),
        )
        assert not result.passed
        assert "no scope.resolved event" in result.detail

    def test_pulls_expected_value_from_profile(self) -> None:
        events = (_scope_resolved_event(outcome="organization_fallback"),)
        result = evaluate_invariant(
            {
                "check": "scope_resolution_outcome_in",
                "args": {"from_profile": "expected_scope_resolution_outcome"},
            },
            _context(
                events=events,
                expectations={
                    "expected_scope_resolution_outcome": "organization_fallback"
                },
            ),
        )
        assert result.passed

    def test_uses_the_last_of_multiple_scope_resolved_events(self) -> None:
        """Codex round-3, HIGH, confirmed: production's validate_stream does
        not forbid more than one scope.resolved event (e.g. a
        re-resolution mid-investigation) -- the FINAL one is the outcome of
        record for this check."""

        events = (
            _scope_resolved_event(outcome="ambiguous"),
            _scope_resolved_event(outcome="exact"),
        )
        result = evaluate_invariant(
            {"check": "scope_resolution_outcome_in", "args": {"allowed": ["exact"]}},
            _context(events=events),
        )
        assert result.passed


class TestNoUnauthorizedCandidateSurfaces:
    def test_unauthorized_candidate_on_a_second_scope_resolved_event_is_caught(
        self,
    ) -> None:
        """Codex round-3, HIGH, confirmed: an earlier version of this check
        only ever inspected the FIRST scope.resolved event, so an
        unauthorized candidate surfaced on a SECOND one was invisible even
        though the overall stream passes validate_stream cleanly."""

        events = (
            _scope_resolved_event(
                outcome="ambiguous", candidates=[_candidate("repo-1")]
            ),
            _scope_resolved_event(
                outcome="ambiguous", candidates=[_candidate("sibling-org-repo")]
            ),
        )
        result = evaluate_invariant(
            {
                "check": "no_unauthorized_candidate_surfaces",
                "args": {"authorized_entity_ids": ["repo-1"]},
            },
            _context(events=events),
        )
        assert not result.passed
        assert "sibling-org-repo" in result.detail

    def test_passes_when_every_candidate_is_authorized(self) -> None:
        events = (
            _scope_resolved_event(
                outcome="ambiguous",
                candidates=[_candidate("repo-1"), _candidate("repo-2")],
            ),
        )
        result = evaluate_invariant(
            {
                "check": "no_unauthorized_candidate_surfaces",
                "args": {"authorized_entity_ids": ["repo-1", "repo-2"]},
            },
            _context(events=events),
        )
        assert result.passed

    def test_fails_when_an_unauthorized_candidate_surfaces(self) -> None:
        events = (
            _scope_resolved_event(
                outcome="ambiguous",
                candidates=[_candidate("repo-1"), _candidate("sibling-org-repo")],
            ),
        )
        result = evaluate_invariant(
            {
                "check": "no_unauthorized_candidate_surfaces",
                "args": {"authorized_entity_ids": ["repo-1"]},
            },
            _context(events=events),
        )
        assert not result.passed
        assert "sibling-org-repo" in result.detail

    def test_a_measured_resolution_with_zero_candidates_passes(self) -> None:
        """The GENUINE clean case: the run really did resolve scope and
        really did surface no candidates. This is the only shape that may
        pass -- and it must keep passing, or the fix below would just trade
        a false green for a false red."""

        result = evaluate_invariant(
            {
                "check": "no_unauthorized_candidate_surfaces",
                "args": {"authorized_entity_ids": ["repo-1"]},
            },
            _context(events=(_scope_resolved_event(outcome="exact", candidates=[]),)),
        )
        assert result.passed

    def test_zero_scope_resolved_events_is_not_measured_and_must_not_pass(
        self,
    ) -> None:
        """CHAOS-3219 Phase 2 exit, live-falsified: with NO ``scope.resolved``
        event in the stream this checker scanned an empty list, found no
        offenders, and returned PASS -- so 9 receipts in exit run #3 asserted
        a security property ("zero cross-tenant candidate leakage") that was
        never observed even once.

        A measurement that did not happen must fail loudly, never render as
        satisfied. The superseded test this replaces
        (``test_no_candidates_at_all_passes``) is the one that encoded the
        vacuous pass as intended behaviour.
        """

        result = evaluate_invariant(
            {
                "check": "no_unauthorized_candidate_surfaces",
                "args": {"authorized_entity_ids": ["repo-1"]},
            },
            _context(events=()),
        )
        assert not result.passed
        assert "no scope.resolved event" in result.detail
        assert "not measured" in result.detail

    def test_pulls_authorized_set_from_profile(self) -> None:
        events = (
            _scope_resolved_event(
                outcome="ambiguous", candidates=[_candidate("repo-1")]
            ),
        )
        result = evaluate_invariant(
            {
                "check": "no_unauthorized_candidate_surfaces",
                "args": {"from_profile": "authorized_entity_ids"},
            },
            _context(events=events, expectations={"authorized_entity_ids": ["repo-1"]}),
        )
        assert result.passed

    def test_missing_authorized_set_raises(self) -> None:
        with pytest.raises(InvariantCheckError, match="requires a non-empty"):
            evaluate_invariant(
                {"check": "no_unauthorized_candidate_surfaces", "args": {}},
                _context(),
            )


class TestTerminalPersistsAssistantRow:
    def test_passes_with_a_real_answer_schema_version(self) -> None:
        result = evaluate_invariant(
            {"check": "terminal_persists_assistant_row"},
            _context(assistant_schema_versions=("dev_answer.v2",)),
        )
        assert result.passed

    def test_passes_with_a_dev_error_v1_row(self) -> None:
        result = evaluate_invariant(
            {"check": "terminal_persists_assistant_row"},
            _context(assistant_schema_versions=("dev_error.v1",)),
        )
        assert result.passed

    def test_fails_with_no_rows_at_all(self) -> None:
        result = evaluate_invariant(
            {"check": "terminal_persists_assistant_row"},
            _context(assistant_schema_versions=()),
        )
        assert not result.passed

    def test_fails_with_only_unrecognized_schema_versions(self) -> None:
        result = evaluate_invariant(
            {"check": "terminal_persists_assistant_row"},
            _context(assistant_schema_versions=("something_else.v1",)),
        )
        assert not result.passed

    def test_fails_with_a_duplicate_real_row(self) -> None:
        """Codex round-3, MEDIUM, confirmed: 'at least one recognized row'
        also passed for a DUPLICATE/stale extra assistant row -- exactly
        the persistence defect CHAOS-3423/replay-safety exists to prevent.
        """

        result = evaluate_invariant(
            {"check": "terminal_persists_assistant_row"},
            _context(assistant_schema_versions=("dev_answer.v2", "dev_answer.v2")),
        )
        assert not result.passed
        assert "expected exactly one" in result.detail


class TestEvaluateInvariantDispatch:
    def test_unknown_check_raises(self) -> None:
        with pytest.raises(InvariantCheckError, match="unknown invariant check"):
            evaluate_invariant({"check": "not_a_real_check"}, _context())

    def test_missing_check_name_raises(self) -> None:
        with pytest.raises(InvariantCheckError, match="no string 'check'"):
            evaluate_invariant({}, _context())

    def test_non_object_args_raises(self) -> None:
        with pytest.raises(InvariantCheckError, match="must be an object"):
            evaluate_invariant(
                {"check": "no_internal_error", "args": "nope"}, _context()
            )


class TestRegisterCheck:
    def test_duplicate_registration_raises(self) -> None:
        with pytest.raises(InvariantCheckError, match="already registered"):

            @register_check("no_internal_error")
            def _dup(args, context):  # noqa: ANN001, ARG001
                raise AssertionError("should never run")

    def test_the_documented_checks_are_registered(self) -> None:
        assert "resolution_path_in" in CHECKS
        assert "no_internal_error" in CHECKS
        assert "public_outcome_in" in CHECKS
        assert "scope_resolution_outcome_in" in CHECKS
        assert "no_unauthorized_candidate_surfaces" in CHECKS
        assert "terminal_persists_assistant_row" in CHECKS
