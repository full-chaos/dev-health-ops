"""Unit coverage for ``scripts.acceptance.corpus.case_schema``.

Fixtures live under ``tests/acceptance/corpus/fixtures/`` -- deliberately
NOT under ``tests/acceptance/world/ask-dev-world.v1/corpus/`` (Lane 2b's
landing zone for the real 134-case registry content) or
``resolution-profiles/`` (the real profile directory), so this suite has
zero merge-collision surface with Lane 2b's concurrent authoring regardless
of landing order. Case ids here are prefixed ``runner-selftest.`` per the
team-lead's directive so they can never collide with a frozen registry id.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.acceptance.corpus.case_schema import (
    ACTIVE_STATUS,
    DECLARED_BLOCKED_STATUS,
    CaseSchemaError,
    load_corpus_case,
    load_corpus_cases,
    load_resolution_profile,
    resolve_case_expectations,
)

_FIXTURES = Path(__file__).parent / "fixtures"
_CASES_DIR = _FIXTURES / "cases"
_PROFILES_DIR = _FIXTURES / "profiles"


class TestLoadCorpusCase:
    def test_loads_a_minimal_valid_case(self) -> None:
        case = load_corpus_case(_CASES_DIR / "case-runner-selftest.basic-exact.json")
        assert case.id == "runner-selftest.basic-exact"
        assert case.question == "What's the status of the Ask Dev project?"
        assert case.subject_class == "exact"
        assert case.resolution_profile_ref is None
        assert len(case.invariants) == 1
        assert case.invariants[0]["category"] == "subject-resolution"
        assert case.status == ACTIVE_STATUS
        assert case.blocked_by is None
        assert case.is_declared_blocked is False

    def test_loads_a_case_with_resolution_profile_ref(self) -> None:
        case = load_corpus_case(_CASES_DIR / "case-runner-selftest.needs-profile.json")
        assert case.resolution_profile_ref == "runner-selftest-v1"

    def test_missing_required_field_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "case-bad.json"
        path.write_text('{"id": "x", "question": "q", "subject_class": "exact"}')
        with pytest.raises(CaseSchemaError, match="invariants"):
            load_corpus_case(path)

    def test_empty_invariants_list_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "case-bad.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", "invariants": []}'
        )
        with pytest.raises(CaseSchemaError, match="non-empty"):
            load_corpus_case(path)

    def test_invariant_missing_category_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "case-bad.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"invariants": [{"check": "no_internal_error"}]}'
        )
        with pytest.raises(CaseSchemaError, match="category"):
            load_corpus_case(path)

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "case-bad.json"
        path.write_text("{not json")
        with pytest.raises(CaseSchemaError, match="not valid JSON"):
            load_corpus_case(path)

    def test_unknown_extra_field_is_accepted(self, tmp_path: Path) -> None:
        path = tmp_path / "case-extra.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"invariants": [{"category": "c", "check": "k"}], '
            '"lane_2b_bookkeeping_field": "whatever"}'
        )
        case = load_corpus_case(path)
        assert case.raw["lane_2b_bookkeeping_field"] == "whatever"


class TestDeclaredBlockedCases:
    """Team-lead direction 2026-08-06, folding in 2b's codex round-1
    finding: a declared-blocked case must load with zero invariants (never
    crash the whole corpus load, never need placeholder invariants) but
    still be loudly, traceably blocked."""

    def test_loads_with_zero_invariants(self) -> None:
        case = load_corpus_case(
            _CASES_DIR / "case-runner-selftest.declared-blocked.json"
        )
        assert case.status == DECLARED_BLOCKED_STATUS
        assert case.blocked_by == "CHAOS-3393"
        assert case.invariants == ()
        assert case.is_declared_blocked is True

    def test_active_case_with_empty_invariants_still_raises(
        self, tmp_path: Path
    ) -> None:
        """An 'active' (the default) case with zero invariants is still
        rejected -- declared-blocked is an explicit opt-in, never inferred
        from an empty list."""

        path = tmp_path / "case-bad.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", "invariants": []}'
        )
        with pytest.raises(CaseSchemaError, match="non-empty"):
            load_corpus_case(path)

    def test_declared_blocked_without_blocked_by_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "case-bad.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"status": "declared-blocked", "invariants": []}'
        )
        with pytest.raises(CaseSchemaError, match="blocked_by"):
            load_corpus_case(path)

    def test_declared_blocked_with_malformed_ticket_reference_raises(
        self, tmp_path: Path
    ) -> None:
        """Codex round-3, MEDIUM, confirmed: a bare non-empty string like
        'not-a-ticket' used to satisfy the check, letting coverage be
        suppressed with no traceable, actionable blocker."""

        path = tmp_path / "case-bad.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"status": "declared-blocked", "blocked_by": "not-a-ticket", '
            '"invariants": []}'
        )
        with pytest.raises(CaseSchemaError, match="ticket reference"):
            load_corpus_case(path)

    def test_declared_blocked_ticket_reference_may_carry_a_description(
        self, tmp_path: Path
    ) -> None:
        """Matches this repo's own real convention (world.json's
        'CHAOS-3432 concurrent ClickHouse ...') -- a leading CHAOS-<number>
        token followed by free text is accepted, not just a bare id."""

        path = tmp_path / "case-ok.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"status": "declared-blocked", '
            '"blocked_by": "CHAOS-3432 concurrent ClickHouse nondeterminism", '
            '"invariants": []}'
        )
        case = load_corpus_case(path)
        assert case.blocked_by == "CHAOS-3432 concurrent ClickHouse nondeterminism"

    def test_blocked_by_on_an_active_case_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "case-bad.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"blocked_by": "CHAOS-1", "invariants": [{"category": "c", "check": "k"}]}'
        )
        with pytest.raises(CaseSchemaError, match="only valid when"):
            load_corpus_case(path)

    def test_unknown_status_value_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "case-bad.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"status": "made-up-status", "invariants": [{"category": "c", "check": "k"}]}'
        )
        with pytest.raises(CaseSchemaError, match="status"):
            load_corpus_case(path)

    def test_declared_blocked_can_still_carry_real_invariants(
        self, tmp_path: Path
    ) -> None:
        """Declared-blocked relaxes the non-empty requirement -- it does not
        forbid invariants for a case that has some but not all of what it
        needs."""

        path = tmp_path / "case-partial.json"
        path.write_text(
            '{"id": "x", "question": "q", "subject_class": "exact", '
            '"status": "declared-blocked", "blocked_by": "CHAOS-1", '
            '"invariants": [{"category": "c", "check": "k"}]}'
        )
        case = load_corpus_case(path)
        assert len(case.invariants) == 1

    def test_loading_the_whole_corpus_directory_does_not_crash_on_a_blocked_case(
        self,
    ) -> None:
        """The regression this whole feature exists to fix: before this
        change, a declared-blocked case's empty invariants list crashed
        load_corpus_cases for the ENTIRE directory, not just that one case."""

        cases = load_corpus_cases(_CASES_DIR)
        ids = [case.id for case in cases]
        assert "runner-selftest.declared-blocked" in ids
        assert "runner-selftest.basic-exact" in ids  # sibling cases still load


class TestLoadCorpusCases:
    def test_loads_every_matching_case_sorted_by_id(self) -> None:
        cases = load_corpus_cases(_CASES_DIR)
        ids = [case.id for case in cases]
        assert ids == sorted(ids)
        assert "runner-selftest.basic-exact" in ids
        assert "runner-selftest.needs-profile" in ids

    def test_missing_directory_returns_empty_list_not_an_error(
        self, tmp_path: Path
    ) -> None:
        assert load_corpus_cases(tmp_path / "does-not-exist") == []

    def test_duplicate_case_id_across_files_raises(self, tmp_path: Path) -> None:
        (tmp_path / "case-a.json").write_text(
            '{"id": "dup", "question": "q1", "subject_class": "exact", '
            '"invariants": [{"category": "c", "check": "k"}]}'
        )
        (tmp_path / "case-b.json").write_text(
            '{"id": "dup", "question": "q2", "subject_class": "exact", '
            '"invariants": [{"category": "c", "check": "k"}]}'
        )
        with pytest.raises(CaseSchemaError, match="duplicate case id"):
            load_corpus_cases(tmp_path)


class TestLoadResolutionProfile:
    def test_loads_a_valid_profile(self) -> None:
        profile = load_resolution_profile(_PROFILES_DIR / "runner-selftest-v1.json")
        assert profile.profile_id == "runner-selftest-v1"
        assert "runner-selftest.needs-profile" in profile.cases

    def test_wrong_schema_version_prefix_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "profile-bad.json"
        path.write_text(
            '{"schema_version": "something_else.v1", "profile_id": "x", "cases": {}}'
        )
        with pytest.raises(CaseSchemaError, match="schema_version"):
            load_resolution_profile(path)

    def test_non_object_case_entry_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "profile-bad.json"
        path.write_text(
            '{"schema_version": "resolution-profile.v1", "profile_id": "x", '
            '"cases": {"a": "not-an-object"}}'
        )
        with pytest.raises(CaseSchemaError, match="must be an object"):
            load_resolution_profile(path)


class TestResolveCaseExpectations:
    def test_case_with_no_profile_ref_gets_empty_block(self) -> None:
        case = load_corpus_case(_CASES_DIR / "case-runner-selftest.basic-exact.json")
        assert resolve_case_expectations(case, {}) == {}

    def test_case_with_profile_ref_resolves_its_block(self) -> None:
        case = load_corpus_case(_CASES_DIR / "case-runner-selftest.needs-profile.json")
        profile = load_resolution_profile(_PROFILES_DIR / "runner-selftest-v1.json")
        expectations = resolve_case_expectations(case, {profile.profile_id: profile})
        assert expectations["expected_resolution_path"] == "deterministic-alias"

    def test_case_citing_unloaded_profile_raises(self) -> None:
        case = load_corpus_case(_CASES_DIR / "case-runner-selftest.needs-profile.json")
        with pytest.raises(CaseSchemaError, match="not loaded"):
            resolve_case_expectations(case, {})

    def test_case_citing_profile_with_no_entry_for_it_raises(self) -> None:
        case = load_corpus_case(_CASES_DIR / "case-runner-selftest.needs-profile.json")
        profile = load_resolution_profile(_PROFILES_DIR / "runner-selftest-v1.json")
        empty_profile = type(profile)(
            profile_id=profile.profile_id,
            schema_version=profile.schema_version,
            cases={},
            source_path=profile.source_path,
        )
        with pytest.raises(CaseSchemaError, match="no 'cases"):
            resolve_case_expectations(case, {empty_profile.profile_id: empty_profile})
