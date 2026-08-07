"""Unit coverage for ``scripts.acceptance.corpus.invariants``."""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.invariants import (
    CHECKS,
    NOT_YET_IMPLEMENTED_CATEGORIES,
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
        assert "public_text_excludes_internal_tokens" in CHECKS
        assert "public_text_has_no_live_markup" in CHECKS


class TestNotYetImplementedCategoriesStaysTrue:
    """CHAOS-3219 Phase 3. ``NOT_YET_IMPLEMENTED_CATEGORIES`` used to
    describe itself as "not consulted by any code path" -- an unverified
    standing claim inside the module whose entire purpose is to stop
    unverified claims. This is the enforcement that makes it true, so it
    cannot silently rot into a lie as the registry grows.
    """

    def test_no_listed_category_has_quietly_acquired_a_checker(self) -> None:
        # A category and a check share a name only when that category has
        # been implemented -- at which point it must leave the list.
        implemented = {name.replace("_", "-") for name in CHECKS}
        still_listed = sorted(
            category
            for category in NOT_YET_IMPLEMENTED_CATEGORIES
            if category in implemented
        )
        assert not still_listed, (
            "these categories are listed as unimplemented but now have a "
            f"registered checker -- remove them from the tuple: {still_listed}"
        )

    def test_the_list_is_not_empty_and_has_no_duplicates(self) -> None:
        # An empty tuple would read as "everything is covered", which is the
        # false-coverage reading this list exists to prevent.
        assert NOT_YET_IMPLEMENTED_CATEGORIES
        assert len(set(NOT_YET_IMPLEMENTED_CATEGORIES)) == len(
            NOT_YET_IMPLEMENTED_CATEGORIES
        )


#: The EXACT leak canary ``role-legacy_agent.json``'s
#: ``adv.unsafe-error-text.provider`` fault plants in its provider 500 body.
#: Copied verbatim from the fixture rather than invented, so this test proves
#: the checker catches THE planted defect, not a defect of its own
#: imagination. It carries three distinct leak classes: a credential-shaped
#: token, the CHAOS-3421 internal enum, and a cross-tenant org name.
_PLANTED_PROVIDER_CANARY = (
    "Upstream call failed: Bearer sk-scriptedFAKEsecretDONOTUSE0000001 "
    "while resolving forbidden_or_not_found for organization "
    "internal-audit-org (leak canary; must never reach a client)."
)

#: Every reader-visible field path, with the answer-payload shape that puts a
#: string there. Parametrising over ALL of them is the point: a checker that
#: scanned only ``direct_summary`` would pass a single-field test while
#: leaking through ``claims[].text`` -- clause-level coverage, not one
#: whole-condition smoke test.
_TEXT_FIELD_CASES: tuple[tuple[str, object], ...] = (
    ("direct_summary", "PAYLOAD"),
    ("claims", [{"text": "PAYLOAD"}]),
    ("warnings", ["PAYLOAD"]),
    ("suggested_follow_up_questions", ["PAYLOAD"]),
    ("evidence", [{"display_label": "PAYLOAD"}]),
    ("metrics", [{"label": "PAYLOAD"}]),
    ("conflicts", [{"summary": "PAYLOAD"}]),
)


def _answer_event(**answer_fields: object) -> dict:
    """An ``answer.completed`` frame carrying a minimal, realistic answer."""

    answer: dict = {"schema_version": "dev_answer.v1", "direct_summary": "All clear."}
    answer.update(answer_fields)
    return {"event": "answer.completed", "data": {"answer": answer}}


def _error_event(message: str) -> dict:
    return {
        "event": "error",
        "data": {"error": {"code": "provider_unavailable", "message": message}},
    }


def _field_payload(field: str, shape: object, payload: str) -> dict:
    """Rebuild one ``_TEXT_FIELD_CASES`` shape with ``PAYLOAD`` substituted."""

    if isinstance(shape, str):
        return {field: payload}
    assert isinstance(shape, list)
    first = shape[0]
    if isinstance(first, str):
        # `warnings` / `suggested_follow_up_questions` are lists of bare
        # strings, not lists of objects.
        return {field: [payload]}
    key = next(iter(first))
    return {field: [{key: payload}]}


class TestPublicTextExcludesInternalTokens:
    CHECK = "public_text_excludes_internal_tokens"

    def test_passes_on_clean_reader_visible_copy(self) -> None:
        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(
                events=(
                    _answer_event(
                        direct_summary="The repository is healthy and recently updated.",
                        claims=[{"text": "Three pull requests merged this week."}],
                    ),
                )
            ),
        )
        assert result.passed, result.detail

    @pytest.mark.parametrize(("field", "shape"), _TEXT_FIELD_CASES)
    def test_a_production_denylist_token_is_caught_in_every_public_field(
        self, field: str, shape: object
    ) -> None:
        """The CHAOS-3421 leak class (``forbidden_or_not_found`` reaching the
        reader), planted in each reader-visible field in turn."""

        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(
                events=(
                    _answer_event(
                        **_field_payload(
                            field,
                            shape,
                            "Scope resolution returned forbidden_or_not_found.",
                        )
                    ),
                )
            ),
        )
        assert not result.passed, f"{field} was not scanned: {result.detail}"
        assert "forbidden_or_not_found" in result.detail

    def test_the_planted_provider_canary_is_caught_in_an_error_terminal(self) -> None:
        """The run took the ERROR terminal, not the answer terminal -- the
        shape ``adv.unsafe-error-text.provider`` actually produces."""

        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(events=(_error_event(_PLANTED_PROVIDER_CANARY),)),
        )
        assert not result.passed
        assert "forbidden_or_not_found" in result.detail

    def test_a_declared_extra_token_catches_the_secret_production_cannot_know(
        self,
    ) -> None:
        """The fake credential and cross-tenant org name in the same canary
        are NOT in production's denylist (they are fixture-specific), so the
        production scan alone reports only the enum. ``extra_forbidden_tokens``
        is what closes the secret-leak half of the claim."""

        args = {
            "extra_forbidden_tokens": [
                "sk-scriptedFAKEsecretDONOTUSE0000001",
                "internal-audit-org",
            ]
        }
        result = evaluate_invariant(
            {"check": self.CHECK, "args": args},
            _context(events=(_error_event(_PLANTED_PROVIDER_CANARY),)),
        )
        assert not result.passed
        assert "sk-scriptedFAKEsecretDONOTUSE0000001" in result.detail
        assert "internal-audit-org" in result.detail

    def test_declared_tokens_match_case_insensitively(self) -> None:
        result = evaluate_invariant(
            {"check": self.CHECK, "args": {"extra_forbidden_tokens": ["SeCrEtVaLuE"]}},
            _context(events=(_answer_event(direct_summary="leaked secretvalue here"),)),
        )
        assert not result.passed

    def test_unobserved_terminal_text_fails_and_is_not_a_vacuous_pass(self) -> None:
        """A run that put no reader-visible text on the wire measured
        nothing. Recording that as 'no leak' is the vacuous-pass shape that
        survived three exit runs on the sibling security checker."""

        result = evaluate_invariant({"check": self.CHECK}, _context(events=()))
        assert not result.passed
        assert "not measured" in result.detail

    def test_a_schema_version_is_not_a_false_positive(self) -> None:
        """``dev_answer.v1`` must not trip the versioned-id pattern, and
        non-reader-visible fields must not be scanned at all -- a blind walk
        of the payload would report both."""

        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(
                events=(
                    _answer_event(
                        direct_summary="All clear.",
                        resolved_scope={"plan_version": "status.entity.v2"},
                    ),
                )
            ),
        )
        assert result.passed, result.detail

    def test_a_versioned_rule_id_in_reader_visible_copy_is_still_caught(self) -> None:
        """The flip side of the test above: the same token IS a leak when it
        reaches copy a person reads. This pair is what proves the whitelist
        narrows the scan without disarming it."""

        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(
                events=(_answer_event(direct_summary="Applied rule status.entity.v2."),)
            ),
        )
        assert not result.passed
        assert "versioned-rule-id-pattern" in result.detail

    def test_non_list_extra_tokens_raises_rather_than_silently_ignoring(self) -> None:
        with pytest.raises(InvariantCheckError, match="must be a list"):
            evaluate_invariant(
                {"check": self.CHECK, "args": {"extra_forbidden_tokens": "oops"}},
                _context(events=(_answer_event(),)),
            )


class TestPublicTextHasNoLiveMarkup:
    CHECK = "public_text_has_no_live_markup"

    def test_passes_on_clean_prose(self) -> None:
        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(events=(_answer_event(direct_summary="All clear this week."),)),
        )
        assert result.passed, result.detail

    @pytest.mark.parametrize(
        ("label", "payload"),
        (
            ("html-tag", "Status <script>alert(1)</script> nominal"),
            ("html-tag-img", "<img src=x onerror=alert(1)>"),
            ("javascript-uri", "See [report](javascript:alert(1))"),
            ("data-uri-html", "Open data:text/html;base64,PHNjcmlwdD4="),
            ("inline-event-handler", '<div onclick="steal()">click</div>'),
        ),
    )
    def test_each_dangerous_construct_is_caught(self, label: str, payload: str) -> None:
        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(events=(_answer_event(direct_summary=payload),)),
        )
        assert not result.passed, f"{label} not caught: {result.detail}"

    @pytest.mark.parametrize(("field", "shape"), _TEXT_FIELD_CASES)
    def test_markup_is_caught_in_every_public_field(
        self, field: str, shape: object
    ) -> None:
        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(
                events=(
                    _answer_event(
                        **_field_payload(field, shape, "<script>alert(1)</script>")
                    ),
                )
            ),
        )
        assert not result.passed, f"{field} was not scanned: {result.detail}"

    @pytest.mark.parametrize(
        "benign",
        (
            "Throughput a < b for the window.",
            "**Bold** and _italic_ markdown are inert as text.",
            "# Heading style copy is legitimate server prose",
            "Use the <= operator when comparing.",
        ),
    )
    def test_benign_prose_does_not_fire(self, benign: str) -> None:
        """A checker that fires on ``a < b`` or on markdown emphasis gets
        switched off rather than fixed. These are the false positives that
        would earn that fate."""

        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(events=(_answer_event(direct_summary=benign),)),
        )
        assert result.passed, f"false positive on {benign!r}: {result.detail}"

    def test_markup_in_an_error_terminal_is_caught(self) -> None:
        result = evaluate_invariant(
            {"check": self.CHECK},
            _context(events=(_error_event("<iframe src=evil></iframe>"),)),
        )
        assert not result.passed

    def test_unobserved_terminal_text_fails_and_is_not_a_vacuous_pass(self) -> None:
        result = evaluate_invariant({"check": self.CHECK}, _context(events=()))
        assert not result.passed
        assert "not measured" in result.detail
