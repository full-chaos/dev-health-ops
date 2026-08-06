"""CHAOS-3462 B4: no active corpus case may declare an invariant that fails
by construction.

THE DEFECT (CHAOS-3219 Phase 2 exit evidence run, comment 286cfa66): 26 of
93 active cases declared

    {"check": "resolution_path_in", "args": {"from_profile": "resolution_path"}}

against a ``deterministic-v1`` profile entry whose ``resolution_path`` is
``null``. ``_resolve_allowed`` turns that into ``allowed=[None]``, and
``resolution_path_in`` independently and deliberately refuses to ever match
``None`` (invariants.py's own "an unobserved run has not met that bar by
definition" contract). So the check could not pass for any run, on any
stack, ever -- "invariant floor green on 93" was unreachable as authored.

Both halves of that are correct in isolation. A profile ``resolution_path``
of ``null`` is the honest value whenever the case writes no
``dev_run_resolutions`` rows, and refusing to match ``None`` is what stops
an unobserved run from counting as an observation. What is wrong is WIRING
THE CHECK ONTO SUCH A CASE -- exactly the trap CASE-SCHEMA.v1.md already
documents avoiding for ``scope_resolution_outcome_in`` (§1: "it must never
be wired onto a case whose profile's ``expected_scope_resolution_outcome``
is ``null``"; those four cases "keep the narrower ``no_internal_error``
floor, documented via ``$comment``, not silently claimed as fully
enforced"). This module generalizes that same rule to every ``*_in`` checker
so the next one cannot be reintroduced silently.

WHY THIS IS A REAL GUARD AND NOT A TAUTOLOGY (rule 1): it asserts the state
the corpus exists to reach -- every declared invariant is capable of
passing. A corpus can be "well-formed" (every file loads, every checker name
is registered, every profile key resolves) and still be entirely unable to
go green; that is precisely the state the exit run found, and no existing
test could see it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acceptance.corpus.case_schema import (
    CorpusCase,
    load_corpus_cases,
    load_resolution_profile,
    resolve_case_expectations,
)

_WORLD_DIR = (
    Path(__file__).resolve().parents[2] / "acceptance" / "world" / "ask-dev-world.v1"
)
_CORPUS_DIR = _WORLD_DIR / "corpus"
_PROFILES_DIR = _WORLD_DIR / "resolution-profiles"

#: Checkers whose ``allowed`` list is resolved through ``_resolve_allowed``
#: and which unconditionally fail on an unobserved (``None``) value. Keep in
#: step with ``invariants.py``; ``test_every_value_in_checker_is_covered``
#: below fails if a new one is registered and not listed here.
_VALUE_IN_CHECKS = (
    "resolution_path_in",
    "public_outcome_in",
    "scope_resolution_outcome_in",
)


def _active_cases() -> list[CorpusCase]:
    return [c for c in load_corpus_cases(_CORPUS_DIR) if not c.is_declared_blocked]


def _profiles() -> dict[str, Any]:
    return {
        profile.profile_id: profile
        for profile in (
            load_resolution_profile(path)
            for path in sorted(_PROFILES_DIR.glob("*.json"))
        )
    }


def _unsatisfiable(case: CorpusCase, profiles: dict[str, Any]) -> list[str]:
    """Every invariant on ``case`` that can never pass, as readable strings."""

    expectations = resolve_case_expectations(case, profiles)
    problems: list[str] = []
    for entry in case.invariants:
        check = entry.get("check")
        if check not in _VALUE_IN_CHECKS:
            continue
        args = entry.get("args", {}) or {}
        literal_allowed = [v for v in args.get("allowed", []) if v is not None]
        if literal_allowed:
            # A literal non-None allowed value keeps the check satisfiable
            # regardless of what the profile says.
            continue
        profile_key = args.get("from_profile")
        if profile_key is None:
            continue
        if expectations.get(profile_key) is None:
            problems.append(
                f"{case.id}: {check} resolves allowed=[None] via "
                f"from_profile={profile_key!r} (profile "
                f"{case.resolution_profile_ref!r} has null/absent for it)"
            )
    return problems


class TestEveryActiveCaseCanPass:
    def test_corpus_is_non_empty(self) -> None:
        """Rule 4: if the corpus directory moved or emptied, this whole
        module would vacuously pass while measuring nothing."""

        assert _active_cases(), (
            f"no active corpus cases found under {_CORPUS_DIR} -- this "
            "module would report green having checked nothing"
        )

    def test_no_active_case_declares_an_unsatisfiable_invariant(self) -> None:
        profiles = _profiles()
        problems = [
            p for case in _active_cases() for p in _unsatisfiable(case, profiles)
        ]
        assert not problems, (
            "these invariants fail by construction -- allowed=[None], and the "
            "*_in checkers deliberately never match an unobserved None. "
            "Per CASE-SCHEMA.v1.md's scope_resolution_outcome_in precedent, "
            "either give the profile a real expected value, or REMOVE the "
            "invariant and document the narrower floor in a $comment:\n  "
            + "\n  ".join(sorted(problems))
        )

    def test_every_value_in_checker_is_covered(self) -> None:
        """If ``invariants.py`` grows another checker that resolves an
        ``allowed`` list through ``_resolve_allowed``, this guard must learn
        about it -- otherwise the same trap reopens under a new name."""

        from scripts.acceptance.corpus.invariants import CHECKS

        registered_value_in = {name for name in CHECKS if name.endswith("_in")}
        assert registered_value_in == set(_VALUE_IN_CHECKS), (
            "the set of '*_in' checkers changed; update _VALUE_IN_CHECKS so "
            f"the unsatisfiable-invariant guard still covers all of them "
            f"(registered={sorted(registered_value_in)}, "
            f"guarded={sorted(_VALUE_IN_CHECKS)})"
        )


class TestGuardActuallyDetectsTheDefect:
    """Rule 2: plant the exact defect and watch the guard catch it.

    A guard that has only ever been observed passing is indistinguishable
    from a guard that cannot fail.
    """

    def test_a_planted_null_profile_value_is_detected(self, tmp_path: Path) -> None:
        case_path = tmp_path / "case-planted.json"
        case_path.write_text(
            json.dumps(
                {
                    "id": "planted.null-path",
                    "question": "What's the status of Atlas?",
                    "subject_class": "n/a",
                    "resolution_profile_ref": "planted-v1",
                    "invariants": [
                        {
                            "category": "resolution-path-matches-profile",
                            "check": "resolution_path_in",
                            "args": {"from_profile": "resolution_path"},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        profile_path = tmp_path / "planted-v1.json"
        profile_path.write_text(
            json.dumps(
                {
                    "schema_version": "resolution-profile.v1",
                    "profile_id": "planted-v1",
                    "cases": {"planted.null-path": {"resolution_path": None}},
                }
            ),
            encoding="utf-8",
        )
        case = load_corpus_cases(tmp_path)[0]
        profiles = {"planted-v1": load_resolution_profile(profile_path)}

        problems = _unsatisfiable(case, profiles)
        assert problems, "the guard failed to detect a planted allowed=[None]"
        assert "resolution_path_in" in problems[0]

    def test_a_real_profile_value_is_not_flagged(self, tmp_path: Path) -> None:
        """The negative control: the guard must not simply flag everything."""

        case_path = tmp_path / "case-ok.json"
        case_path.write_text(
            json.dumps(
                {
                    "id": "planted.real-path",
                    "question": 'What\'s the status of the repo "meridian/web-app"?',
                    "subject_class": "exact",
                    "resolution_profile_ref": "planted-v1",
                    "invariants": [
                        {
                            "category": "resolution-path-matches-profile",
                            "check": "resolution_path_in",
                            "args": {"from_profile": "resolution_path"},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        profile_path = tmp_path / "planted-v1.json"
        profile_path.write_text(
            json.dumps(
                {
                    "schema_version": "resolution-profile.v1",
                    "profile_id": "planted-v1",
                    "cases": {
                        "planted.real-path": {"resolution_path": "deterministic-exact"}
                    },
                }
            ),
            encoding="utf-8",
        )
        case = load_corpus_cases(tmp_path)[0]
        profiles = {"planted-v1": load_resolution_profile(profile_path)}
        assert _unsatisfiable(case, profiles) == []

    def test_a_literal_allowed_value_rescues_a_null_profile_entry(
        self, tmp_path: Path
    ) -> None:
        """Second negative control: ``allowed`` and ``from_profile`` are
        additive in ``_resolve_allowed``, so a literal non-None value makes
        the check satisfiable even beside a null profile entry. Flagging
        that would be a false positive."""

        (tmp_path / "case-lit.json").write_text(
            json.dumps(
                {
                    "id": "planted.literal",
                    "question": "q",
                    "subject_class": "n/a",
                    "resolution_profile_ref": "planted-v1",
                    "invariants": [
                        {
                            "category": "resolution-path-matches-profile",
                            "check": "resolution_path_in",
                            "args": {
                                "allowed": ["miss-clarification"],
                                "from_profile": "resolution_path",
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        profile_path = tmp_path / "planted-v1.json"
        profile_path.write_text(
            json.dumps(
                {
                    "schema_version": "resolution-profile.v1",
                    "profile_id": "planted-v1",
                    "cases": {"planted.literal": {"resolution_path": None}},
                }
            ),
            encoding="utf-8",
        )
        case = load_corpus_cases(tmp_path)[0]
        profiles = {"planted-v1": load_resolution_profile(profile_path)}
        assert _unsatisfiable(case, profiles) == []


class TestEveryActiveCaseStillAssertsSomething:
    """Removing an unsatisfiable invariant must never empty a case out.

    ``case_schema`` already rejects an active case with zero invariants at
    load time, but that check runs on the FILE. This asserts the property
    that actually matters after a bulk edit: every active case still carries
    at least one real, evaluable assertion.
    """

    @pytest.mark.parametrize("case", _active_cases(), ids=lambda c: c.id)
    def test_case_has_at_least_one_invariant(self, case: CorpusCase) -> None:
        assert case.invariants, f"{case.id} was left with zero invariants"
