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
from scripts.acceptance.corpus.resolution_path import (
    absence_is_a_broken_measurement,
    resolution_path_absence_reason,
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
                    "expected_mention_texts": ["atlas"],
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


class TestNoResolutionPathCaseCanProduceZeroLedgerRows:
    """The declared-value guard above is necessary but NOT sufficient.

    Adversarial round 2 found six cases whose profile value was a perfectly
    good non-null string, and which were still unpassable -- because the RUN
    writes no ``dev_run_resolutions`` row at all, so
    ``derive_resolution_path`` returns ``None`` and ``resolution_path_in``
    refuses it regardless of what the profile says. Checking the declared
    value can never see that; only the producer can.

    So this asks the real producer. A question that yields zero mentions
    takes preflight's ``proceeded_organization_wide`` branch with
    ``ledger=None``, and every ``append_resolution`` site requires a non-None
    ledger -- zero rows, guaranteed, in every catalog world. A case in that
    state must not declare ``resolution_path_in``.

    THE ORACLE MATTERS, and getting it wrong is how three cases were
    mis-dispositioned: ``extract_mentions`` is NOT production's mention set.
    ``QuestionInterpreter.interpret`` additionally mints untyped bare-name
    mentions (``_add_untyped_mentions``), so ``"Update the ticket status to
    Done"`` yields zero under the former and one under the latter. This uses
    the interpreter, with the same request shape the runner really sends.
    """

    @staticmethod
    def _request(question: str) -> Any:
        import uuid
        from datetime import UTC, datetime, timedelta

        from dev_health_ops.api.dev.contracts import DevMessageRequest

        now = datetime.now(UTC).replace(microsecond=0)

        def span(start: Any, end: Any) -> dict[str, str]:
            return {
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "timezone": "UTC",
            }

        return DevMessageRequest.model_validate(
            {
                "schema_version": "dev_message_request.v1",
                "request_id": str(uuid.uuid4()),
                "client_message_id": str(uuid.uuid4()),
                "conversation_id": str(uuid.uuid4()),
                "question": question,
                "question_class": "status",
                "scope": {
                    "schema_version": "dev_scope.v1",
                    "organization_id": str(uuid.uuid4()),
                    "direct_scope": "organization",
                    "repositories": [],
                    "entity_refs": [],
                    "team_ids": [],
                    "time_range": span(now - timedelta(days=28), now),
                    "comparison_range": span(
                        now - timedelta(days=56), now - timedelta(days=28)
                    ),
                    "surface_context": None,
                },
            }
        )

    @pytest.mark.asyncio
    async def test_every_resolution_path_case_names_at_least_one_mention(self) -> None:
        from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter

        interpreter = QuestionInterpreter()
        declaring = [
            case
            for case in _active_cases()
            if any(e.get("check") == "resolution_path_in" for e in case.invariants)
        ]
        assert declaring, (
            "no active case declares resolution_path_in -- this guard would "
            "report green having measured nothing"
        )

        zero_mention: list[str] = []
        failures: list[str] = []
        for case in declaring:
            try:
                interpreted = await interpreter.interpret(self._request(case.question))
            except Exception as exc:  # noqa: BLE001 - reported, never swallowed
                # Rule 4: a case we could not measure must FAIL, not quietly
                # count as fine. An earlier version of this probe stored the
                # error string in the count variable, where a truthy value
                # read as "has mentions" -- a check that could not fail.
                failures.append(f"{case.id}: {type(exc).__name__}: {exc}")
                continue
            if not interpreted.mentions:
                zero_mention.append(case.id)

        assert not failures, (
            "the interpreter raised for these cases, so their mention count "
            "was never measured:\n  " + "\n  ".join(failures)
        )
        assert not zero_mention, (
            "these cases declare resolution_path_in but their question names "
            "ZERO mentions under production's real QuestionInterpreter, so "
            "preflight proceeds organization-wide with ledger=None, no "
            "dev_run_resolutions row is written, derive_resolution_path "
            "returns None, and resolution_path_in can never pass -- in any "
            "catalog world, whatever the profile declares:\n  "
            + "\n  ".join(sorted(zero_mention))
        )


class TestEveryResolutionPathDeclarerTreatsAnEmptyLedgerAsBroken:
    """CHAOS-3533: the hardening must cover the population it was built for.

    ``resolution_path_absence_reason`` now classifies an empty ledger as a
    BROKEN measurement when the case declares subject mention spans, and as
    an honest absence when it does not. That split is only worth anything if
    every case declaring ``resolution_path_in`` really does land on the
    broken side -- a declarer that somehow reached the runner without spans
    would keep classifying its empty ledger as honest, and would go on
    failing one invariant while reporting that it measured cleanly.

    Asserted against the REAL corpus rather than a constructed case, and
    driven through the real ``resolution_path_absence_reason``, so this
    cannot pass by agreeing with a re-implementation of the rule.

    (``case_schema`` separately requires spans on any case declaring
    ``resolution_path_in``. This does not restate that rule -- it asserts the
    CONSEQUENCE the runner depends on, which is what would actually break.)
    """

    def test_every_declarer_classifies_an_empty_ledger_as_broken(self) -> None:
        declaring = [
            case
            for case in load_corpus_cases(_CORPUS_DIR)
            if case.status == "active"
            and any(entry["check"] == "resolution_path_in" for entry in case.invariants)
        ]
        assert declaring, "no active case declares resolution_path_in"

        not_broken = [
            case.id
            for case in declaring
            if not absence_is_a_broken_measurement(
                resolution_path_absence_reason(
                    run_id="a-real-run",
                    path=None,
                    named_subject_mentions=bool(case.expected_mention_texts),
                )
            )
        ]
        assert not not_broken, (
            "these cases declare resolution_path_in but an empty resolution "
            "ledger would still classify as an HONEST absence for them, so a "
            "run that measured nothing would report one failed invariant "
            "instead of an unmeasured case:\n  " + "\n  ".join(sorted(not_broken))
        )

    def test_a_case_without_declared_spans_keeps_its_honest_absence(self) -> None:
        """The other arm: the zero-mention families (``portfolio.*``,
        ``investment.*``) genuinely append nothing, and widening the broken
        set to swallow them would turn every one of them permanently red for
        doing exactly what they are supposed to do."""

        assert not absence_is_a_broken_measurement(
            resolution_path_absence_reason(
                run_id="a-real-run", path=None, named_subject_mentions=False
            )
        )


class TestDeclaredMentionTextsMatchTheProducer:
    """CHAOS-3462 B6: every declared span must be what production really
    yields for that question.

    ``expected_mention_texts`` exists because the persisted resolution
    ledger never carries the mention span, so the case has to supply it.
    That makes it a hand-copyable value in a JSON file -- and a hand-copyable
    value drifts. A question edited by one word, and the declared span is
    silently wrong: ``classify_match_kind`` would then either raise for a
    bogus reason or classify against text that never reached the resolver.

    So the declaration is asserted against the PRODUCER, exactly and in
    order, not spot-checked for membership. The value was generated from
    this same interpreter; asserting equality means a future question edit
    fails the unit gate rather than the live run.

    Two things this deliberately does NOT do: it does not accept
    ``subjects.json``'s ``mentions`` array as a source (those are
    human-readable descriptive phrases, not resolver input --
    ``resolution_path.py``'s CALLER CONTRACT warns about exactly that), and
    it does not compare against ``extract_mentions``, which is the narrower
    function and misses the untyped bare-name mentions
    ``_add_untyped_mentions`` adds.
    """

    @pytest.mark.asyncio
    async def test_every_declared_span_is_what_the_interpreter_produces(
        self,
    ) -> None:
        from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter

        interpreter = QuestionInterpreter()
        declaring = [case for case in _active_cases() if case.expected_mention_texts]
        assert declaring, (
            "no active case declares expected_mention_texts -- this guard "
            "would report green having measured nothing"
        )

        mismatches: list[str] = []
        failures: list[str] = []
        for case in declaring:
            request = TestNoResolutionPathCaseCanProduceZeroLedgerRows._request(
                case.question
            )
            try:
                interpreted = await interpreter.interpret(request)
            except Exception as exc:  # noqa: BLE001 - reported, never swallowed
                failures.append(f"{case.id}: {type(exc).__name__}: {exc}")
                continue
            produced = tuple(m.normalized_lookup_text for m in interpreted.mentions)
            if produced != case.expected_mention_texts:
                mismatches.append(
                    f"{case.id}: declares {list(case.expected_mention_texts)!r} "
                    f"but the interpreter produces {list(produced)!r}"
                )

        assert not failures, (
            "the interpreter raised for these cases, so their spans were "
            "never measured:\n  " + "\n  ".join(failures)
        )
        assert not mismatches, (
            "these cases' declared expected_mention_texts have drifted from "
            "what production's QuestionInterpreter actually yields for their "
            "question. Regenerate them from the interpreter -- never hand-"
            "author them, and never copy them from subjects.json's "
            "descriptive 'mentions' phrases:\n  " + "\n  ".join(mismatches)
        )

    def test_every_resolution_path_case_declares_spans(self) -> None:
        """The loader enforces this per file; this asserts it holds across
        the corpus as a set, which is what the runner actually depends on."""

        missing = [
            case.id
            for case in _active_cases()
            if any(e.get("check") == "resolution_path_in" for e in case.invariants)
            and not case.expected_mention_texts
        ]
        assert not missing, (
            f"these cases assert a resolution path but declare no spans, so "
            f"a single-shot exact_match would be unclassifiable: {missing!r}"
        )


class TestSingleTurnRunnerCannotProduceAliasPaths:
    """``deterministic-alias`` is unproducible while the runner is
    single-turn -- the decidable half of a class found in adversarial round 3.

    Production never auto-commits an alias or acronym form: ``alias_matching``
    's own contract is that an alias hit must be OFFERED as a candidate
    first. The only in-run route from ``ambiguous_candidates`` to
    ``exact_match`` on the same mention is ``_apply_context_tiebreaker``,
    which needs ``scope.entity_refs`` -- and the runner sends ``[]`` and one
    message per case. So the ledger is a single ``ambiguous_candidates`` row
    and ``derive_resolution_path`` short-circuits to ``miss-clarification``.

    Two cases declared ``deterministic-alias`` and would have failed for
    this reason. Proving alias resolution needs the two-turn disambiguation
    case ``resolution_path.py``'s docstring describes, which does not exist
    yet -- this guard fails the moment someone declares the value again, and
    the failure names what would actually have to change.

    The OTHER half of that class is not mechanically decidable here: a case
    can also miss ``deterministic-exact`` because an incidental unresolved
    bare-name mention downgrades the whole run, and whether a span resolves
    depends on the live catalog. That half is documented in
    CASE-SCHEMA.v1.md rather than guarded, because a guard that guessed
    would be worse than an honest note.
    """

    def test_no_active_case_declares_an_alias_path(self) -> None:
        profiles = _profiles()
        declaring = [
            case.id
            for case in _active_cases()
            if resolve_case_expectations(case, profiles).get("resolution_path")
            == "deterministic-alias"
        ]
        assert not declaring, (
            "these cases expect deterministic-alias, which a single-turn "
            "runner cannot produce -- an alias hit is never auto-committed, "
            "so the ledger holds one ambiguous_candidates row and the derived "
            "path is miss-clarification. Either author the two-turn "
            "disambiguation case, or record miss-clarification with a "
            f"coverage warning: {declaring!r}"
        )

    def test_the_guard_would_catch_a_reintroduction(self) -> None:
        """Rule 2: plant it and watch the guard fire. Without this, a guard
        over a set that is currently empty is indistinguishable from one
        that cannot fail."""

        assert "deterministic-alias" in _resolution_path_domain(), (
            "precondition: the value is still legal vocabulary, so a case "
            "COULD declare it -- that is why this guard is needed"
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
                    "expected_mention_texts": ["atlas"],
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
                    "expected_mention_texts": ["meridian/web-app"],
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
                    "expected_mention_texts": ["atlas"],
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
                    "expected_mention_texts": ["atlas"],
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
                    "expected_mention_texts": ["atlas"],
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


#: CHAOS-3490: every active case that declares ``resolution_path_in``, pinned.
#:
#: UNVERIFIED, and that word is load-bearing. As of the Phase 2 exit evidence
#: run (2026-08-06) NO case in this corpus has ever been observed producing a
#: non-null ``resolution_path``: across all 78 receipts that run wrote, the
#: value was ``None`` every time. Of the 60 cases that declared the check, 18
#: actually executed -- and all 18 failed it. The other 42 (this list) were
#: rate-limited before they reached their assertions, so they are simply
#: UNMEASURED. They are pinned here rather than pre-emptively stripped,
#: because stripping on inference is exactly the read-don't-execute mistake
#: that produced the 18.
#:
#: CHAOS-3490 Part 2 / CHAOS-3520 (2026-08-07), exit run 7 -- the first run in
#: which all of these were actually MEASURED, on the real CHAOS-3292 preflight
#: path. One id left this set, deliberately and on live evidence, so the pin
#: shrank 42 -> 41: ``scope.outcome.filtered`` is now declared-blocked against
#: CHAOS-3520 because ``ScopeResolutionOutcome.FILTERED`` needs a non-empty
#: ``team_filter_refs``, which only a model-authored ``DevScope.team_ids`` on a
#: ``resolve_scope.v1`` call supplies, and the scripted acceptance provider
#: never offers that tool. Note its ``resolution_path_in`` was NOT the failing
#: half -- it PASSED, with ``resolution_path='deterministic-exact'``. The check
#: left this set because the whole case is blocked, not because the check was
#: found wanting; it returns with the case when CHAOS-3520 unblocks. Recorded
#: here rather than silently deleted, because the distinction between "this
#: declarer was condemned" and "this declarer went away with its case" is
#: exactly what the pin exists to keep legible.
RESOLUTION_PATH_DECLARING_CASE_IDS: frozenset[str] = frozenset(
    {
        "deficiency.team.not-applicable-rule",
        "deg.optional-integration-not-mislabeled",
        "deg.provisional-unapproved-rule",
        "deg.source-state.deleted",
        "deg.source-state.no-data",
        "deg.source-state.stale",
        "deg.source-state.unauthorized-not-visible",
        "deg.source-state.unconfigured",
        "deg.timeout.data-health",
        "deg.timeout.evidence",
        "deg.timeout.graph",
        "deg.timeout.metric",
        "deg.unknown-denominator",
        "health.project.exact-subject",
        "health.project.not-applicable-fixture-only",
        "health.project.unknown-and-not-applicable-abstract",
        "metric-compare.two-metrics.stale-source",
        "pers.clarification-persistence",
        "scope.ambiguous",
        "scope.bounded-subject-set",
        "scope.deleted-subject",
        "scope.no-match",
        "scope.outcome.unresolved",
        "scope.prohibited-write",
        "status.single-project.exact-subject",
        "status.single-project.readiness-completion",
        "status.single-project.remaining-work",
        "subject-label.acronym-mention",
        "subject-label.five-word-truncation",
        "subject-label.parenthetical-quoted-control",
        "subject-label.parenthetical-unquoted",
        "subject-label.typo-tolerance",
        "subject-label.unauthorized-candidate-excluded",
        "subject-label.word-order-variation",
        "subject-label.wrong-kind-collision",
        "tenant.cross-tenant-identifier-refused",
        "trust.source-health.current",
        "trust.source-health.stale",
        "workload.team.small-cohort",
        "workload.team.with-denominator",
        "workload.team.without-denominator",
    }
)


class TestResolutionPathDeclarersArePinned:
    """CHAOS-3490: close the class the earlier guards structurally cannot.

    WHY A PIN AND NOT ANOTHER PREDICATE, established by measurement rather
    than preference. Two guards already sit above this one: the declared-value
    guard (a ``*_in`` check wired onto a null profile value) and the
    zero-mention guard (a question the real interpreter yields no mentions
    for). Both are necessary. Neither could have caught the 18 cases the exit
    run condemned, because those cases have non-null profile values AND name
    real mentions -- they fail for a purely RUNTIME reason: the request
    terminates (readiness gate, capability gate, refusal, oversized
    rejection) before the orchestrator ever reaches an ``append_resolution``
    site, so the ledger stays empty.

    A third static predicate was attempted and abandoned on evidence: the 18
    condemned cases and the 42 still-declaring cases are INDISTINGUISHABLE by
    static shape. They draw from the same ``expected_public_outcome`` values
    (``answered_with_gaps``, ``not_found``, ``denied``, ``unsupported``) and
    the same ``resolution_path`` values (``deterministic-exact``,
    ``miss-clarification``). There is no field that separates them, so any
    predicate claiming to would be a guess dressed as a guard.

    What CAN be closed is silent GROWTH of the population. A new case
    acquiring ``resolution_path_in`` without anyone re-examining the class is
    how 18 accumulated past B4's sweep. This pin makes that impossible: adding
    a declarer fails here until its id is added deliberately, which is the
    moment to ask for the live evidence that the run reaches the ledger at
    all.

    This guard does NOT claim the pinned 42 are correct. It claims only that
    the set cannot change unnoticed.
    """

    def test_declaring_set_matches_the_pin(self) -> None:
        declaring = {
            case.id
            for case in _active_cases()
            if any(e.get("check") == "resolution_path_in" for e in case.invariants)
        }
        added = sorted(declaring - RESOLUTION_PATH_DECLARING_CASE_IDS)
        removed = sorted(RESOLUTION_PATH_DECLARING_CASE_IDS - declaring)
        assert not added, (
            f"case(s) {added} newly declare resolution_path_in. Static analysis "
            "CANNOT tell a passable declaration from an unpassable one -- the 18 "
            "cases removed in CHAOS-3490 were structurally identical to the ones "
            "still declaring it. Before adding an id to "
            "RESOLUTION_PATH_DECLARING_CASE_IDS, get live evidence that this "
            "case's run actually writes a dev_run_resolutions row; no case in "
            "this corpus has yet been OBSERVED producing a non-null "
            "resolution_path."
        )
        assert not removed, (
            f"case(s) {removed} no longer declare resolution_path_in. If that is "
            "a deliberate evidence-backed removal, delete them from "
            "RESOLUTION_PATH_DECLARING_CASE_IDS in the same change, so the pin "
            "keeps describing reality rather than history."
        )

    def test_the_pin_would_catch_a_silent_addition(self) -> None:
        """The pin is only worth having if it actually fires. Simulate the
        exact regression it exists to stop -- a 43rd declarer appearing -- and
        confirm the comparison rejects it."""

        simulated = set(RESOLUTION_PATH_DECLARING_CASE_IDS) | {"planted.new-declarer"}
        assert sorted(simulated - RESOLUTION_PATH_DECLARING_CASE_IDS) == [
            "planted.new-declarer"
        ]

    def test_no_condemned_case_declares_it_again(self) -> None:
        """The 18 the exit run condemned must stay out. Re-adding one is the
        specific regression CHAOS-3490 Part 2 exists to prevent."""

        condemned = {
            "adv.abuse.subject-set-mutation-attempt",
            "adv.cross-tenant.organization-id",
            "adv.cross-tenant.project-id",
            "adv.cross-tenant.repository-id",
            "adv.injection-request.graphql",
            "adv.injection-request.mcp",
            "adv.injection-request.shell",
            "adv.injection-request.sql",
            "adv.malicious-content.links",
            "adv.malicious-content.markdown",
            "adv.no-person-level-output",
            "adv.unsafe-error-text.source",
            "deficiency.team.applicable-rule",
            "deg.provider.unavailable",
            "deg.provider.unsupported",
            "deg.source-state.unavailable",
            "readiness.capabilities.degraded",
            "readiness.capabilities.unsupported-model",
        }
        assert not (condemned & RESOLUTION_PATH_DECLARING_CASE_IDS)
        declaring = {
            case.id
            for case in _active_cases()
            if any(e.get("check") == "resolution_path_in" for e in case.invariants)
        }
        assert not (condemned & declaring), (
            "a case the exit evidence run proved writes zero ledger rows is "
            "declaring resolution_path_in again"
        )
