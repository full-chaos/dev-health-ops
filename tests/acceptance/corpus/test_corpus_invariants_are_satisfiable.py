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


def _resolution_path_domain() -> frozenset[str]:
    """The paths ``derive_resolution_path`` can actually EMIT.

    Deliberately narrower than the full ``ResolutionPath`` Literal: that type
    also carries ``qua-shadow`` and ``qua-committed``, which
    ``resolution_path.py``'s own docstring reserves for the future CHAOS-3389
    shadow-replay mode and states are "never emitted here". Validating
    against the full Literal would wave through
    ``allowed: ["qua-shadow"]`` -- an invariant exactly as unpassable as
    ``[None]``. Asserted against the Literal below so this cannot silently
    drift if the reserved set changes.
    """

    return frozenset(
        {"deterministic-exact", "deterministic-alias", "miss-clarification"}
    )


def _public_outcome_domain() -> frozenset[str]:
    from dev_health_ops.api.dev.contracts_v2.base import PublicOutcome

    return frozenset(member.value for member in PublicOutcome)


def _scope_resolution_outcome_domain() -> frozenset[str]:
    from dev_health_ops.api.dev.contracts import ScopeResolutionOutcome

    return frozenset(member.value for member in ScopeResolutionOutcome)


#: Checkers whose ``allowed`` list is resolved through ``_resolve_allowed``
#: and which unconditionally fail on an unobserved (``None``) value, mapped
#: to the PRODUCTION vocabulary each one compares against.
#:
#: The domains matter (Codex adversarial round-1, MEDIUM, confirmed): an
#: earlier version of this guard treated ANY non-null literal as proof of
#: satisfiability, so ``allowed: ["impossible-path"]`` sailed through while
#: being every bit as unpassable as ``allowed=[None]`` -- production can
#: never emit that string. Pulling each domain from the real enum/Literal
#: rather than restating it here means a vocabulary change cannot leave this
#: guard quietly checking against a stale list.
_VALUE_IN_CHECKS: dict[str, Any] = {
    "resolution_path_in": _resolution_path_domain,
    "public_outcome_in": _public_outcome_domain,
    "scope_resolution_outcome_in": _scope_resolution_outcome_domain,
}


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
        domain = _VALUE_IN_CHECKS[check]()
        args = entry.get("args", {}) or {}
        profile_key = args.get("from_profile")

        # Every value this check could ever accept, exactly as
        # `_resolve_allowed` would assemble it (literals and the profile
        # value are ADDITIVE, not alternatives).
        allowed: list[Any] = list(args.get("allowed", []))
        if profile_key is not None:
            allowed.append(expectations.get(profile_key))

        if not allowed:
            problems.append(
                f"{case.id}: {check} declares neither 'allowed' nor "
                "'from_profile' -- _resolve_allowed raises "
                "InvariantCheckError for this at evaluation time, so the "
                "case can never report a real result"
            )
            continue

        satisfiable = [v for v in allowed if v is not None and v in domain]
        if satisfiable:
            continue

        if all(v is None for v in allowed):
            problems.append(
                f"{case.id}: {check} resolves allowed=[None] via "
                f"from_profile={profile_key!r} (profile "
                f"{case.resolution_profile_ref!r} has null/absent for it)"
            )
        else:
            problems.append(
                f"{case.id}: {check} allows only {allowed!r}, none of which "
                f"production can ever emit (domain: {sorted(domain)!r})"
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

    def test_the_emitted_resolution_path_domain_is_a_real_subset(self) -> None:
        """Pin the narrowing rather than restating it in a comment: the
        emitted set must be a strict subset of the declared Literal, and the
        excluded values must be exactly the reserved QUA ones. If someone
        wires the QUA mode up, this fails and forces the guard to be
        revisited instead of quietly under- or over-accepting."""

        from typing import get_args

        from scripts.acceptance.corpus.resolution_path import ResolutionPath

        declared = frozenset(get_args(ResolutionPath))
        emitted = _resolution_path_domain()
        assert emitted < declared
        assert declared - emitted == {"qua-shadow", "qua-committed"}

    def test_a_reserved_never_emitted_path_is_flagged_unsatisfiable(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "case-qua.json").write_text(
            json.dumps(
                {
                    "id": "planted.qua",
                    "question": "q",
                    "subject_class": "n/a",
                    "invariants": [
                        {
                            "category": "resolution-path-matches-profile",
                            "check": "resolution_path_in",
                            "args": {"allowed": ["qua-shadow", "qua-committed"]},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        case = load_corpus_cases(tmp_path)[0]
        assert _unsatisfiable(case, {}), (
            "an invariant allowing only reserved, never-emitted paths was not "
            "flagged -- it is as unpassable as allowed=[None]"
        )

    def test_every_guarded_domain_is_non_empty(self) -> None:
        """Rule 4 again: a domain that resolved to the empty set would make
        EVERY literal look unsatisfiable, or -- if the emptiness came from a
        failed import silently caught somewhere -- make the value check
        vacuous. Assert the oracles actually loaded."""

        for check, domain_fn in _VALUE_IN_CHECKS.items():
            domain = domain_fn()
            assert domain, f"{check}'s production value domain resolved empty"


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

    def test_a_literal_outside_the_production_domain_is_detected(
        self, tmp_path: Path
    ) -> None:
        """Codex adversarial round-1, MEDIUM: a non-null literal production
        can never emit is exactly as unpassable as ``[None]``, and the
        earlier guard waved it through."""

        (tmp_path / "case-bogus.json").write_text(
            json.dumps(
                {
                    "id": "planted.bogus-literal",
                    "question": "q",
                    "subject_class": "n/a",
                    "invariants": [
                        {
                            "category": "resolution-path-matches-profile",
                            "check": "resolution_path_in",
                            "args": {"allowed": ["impossible-path"]},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        case = load_corpus_cases(tmp_path)[0]
        problems = _unsatisfiable(case, {})
        assert problems, "a literal outside the production domain was not flagged"
        assert "production can ever emit" in problems[0]

    def test_an_invariant_with_no_allowed_and_no_from_profile_is_detected(
        self, tmp_path: Path
    ) -> None:
        """``_resolve_allowed`` raises InvariantCheckError for this at
        evaluation time, so the case can never report a real result -- it
        should be caught by the unit gate, not by a live run."""

        (tmp_path / "case-empty-args.json").write_text(
            json.dumps(
                {
                    "id": "planted.empty-args",
                    "question": "q",
                    "subject_class": "n/a",
                    "invariants": [
                        {
                            "category": "resolution-path-matches-profile",
                            "check": "resolution_path_in",
                            "args": {},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        case = load_corpus_cases(tmp_path)[0]
        problems = _unsatisfiable(case, {})
        assert problems
        assert "neither 'allowed' nor 'from_profile'" in problems[0]

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
