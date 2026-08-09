"""CHAOS-3619 3(a): the graph arm must earn its subjects.

Review condition: both arms receive the identical question and conversational
context and *nothing else*; the graph arm resolves subjects through its own
candidate discovery and is never handed the case's seed or committed subject.
Otherwise the orchestrator-interpreted native leg does ambiguity work the
graph leg skipped, and every ambiguity-family figure is unearned.

Two kinds of guard here, and they are not interchangeable:

* **structural** -- the leg cannot see the answer, because the import is
  absent and the signature has no parameter for it. This is the one that
  keeps holding after someone refactors in a hurry;
* **measured** -- what unaided discovery actually achieves on the corpus.
  That number belongs in the fairness table, and pinning it means a silent
  change in alias coverage cannot move the graph column for a reason
  unrelated to graph assistance.

The measured guard is written to fail in BOTH directions. Too low and the
graph arm is being starved by a broken wiring; too high and discovery is
resolving things it should not (the corpus deliberately contains a restricted
same-tenant project and a cross-tenant near-duplicate that must NOT resolve).
"""

from __future__ import annotations

import ast
from pathlib import Path

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.cases import authored_cases
from dev_health_ops.context_fabric.graph_arm import corpus_adapter
from dev_health_ops.context_fabric.graph_arm.projection import build_projection
from trials.chaos_3619 import graph_leg

_LEG_SOURCE = Path(graph_leg.__file__)

#: The corpus modules the arm must not be able to read. ``world`` is
#: permitted -- it is the fixture universe, not the answer key -- exactly as
#: CHAOS-3617's ``corpus_adapter`` is permitted to read it.
_FORBIDDEN_CORPUS_MODULES = ("cases", "oracles", "evaluate", "reference", "coverage")

#: A question using the interpreter's TYPED mention grammar (noun-trailing:
#: "the <Name> project"). None of the 39 corpus questions use it -- they are
#: deliberately colloquial -- which is exactly why the typed loop went
#: unexercised and a nonexistent field name survived every test.
_TYPED_MENTION_QUESTION = "what is the status of the Agent Context Runtime project?"


def _projection():
    return build_projection(corpus_adapter.corpus_batch(world.ORG_HELIO))


class TestTheLegCannotSeeWhatItIsScoredAgainst:
    def test_the_leg_imports_no_scoring_module(self) -> None:
        """Absence of the import, read from the AST.

        The same discipline ``test_chaos_3617_corpus_adapter`` enforces on the
        arm's own adapter. A docstring mentioning ``oracles`` must not fail
        this, and a real import must, which is why it is parsed rather than
        grepped.
        """

        tree = ast.parse(_LEG_SOURCE.read_text())
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            elif isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
        offenders = [
            name
            for name in imported
            for forbidden in _FORBIDDEN_CORPUS_MODULES
            if name.endswith(f"investigation_corpus.{forbidden}")
        ]
        assert not offenders, (
            f"the graph leg imports {offenders}; it can now see what it is "
            "scored against, and every ambiguity-family figure it produces is "
            "unearned"
        )

    def test_no_discovery_entry_point_accepts_a_subject(self) -> None:
        """3(a) as a signature property, not a convention.

        Passing an expected subject must not be something a caller can do
        carelessly. Asserted over the module's public functions so a new
        entry point cannot quietly reintroduce the parameter.
        """

        tree = ast.parse(_LEG_SOURCE.read_text())
        banned = ("subject", "seed", "expected", "case_id", "oracle", "committed")
        offenders: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or node.name.startswith("_"):
                continue
            args = node.args
            names = [a.arg for a in (*args.args, *args.kwonlyargs, *args.posonlyargs)]
            for name in names:
                if any(word in name.lower() for word in banned):
                    offenders.append(f"{node.name}({name})")
        assert not offenders, (
            f"a graph-leg entry point now accepts {offenders}; the arm can be "
            "handed the answer, which is exactly what condition 3(a) forbids. "
            "`seeds_from` derives seeds from a SubjectDiscovery the arm "
            "produced -- it does not take them"
        )


class TestWhatUnaidedDiscoveryActuallyAchieves:
    """The measured half. These numbers go in the fairness table."""

    def test_discovery_resolves_a_plain_named_project(self) -> None:
        """The control. Without it every 'must not resolve' assertion below
        is satisfiable by a discovery that resolves nothing at all.

        Uses a corpus question whose extracted mention is exactly the project
        name. That is not cherry-picking -- the next test pins the case where
        it is NOT, as a measured limitation.
        """

        projection = _projection()
        grant = corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        found = graph_leg.discover_subjects(
            question="do we have enough people on Lattice Search?",
            projection=projection,
            authorized_entity_ids=grant,
        )
        assert found.resolved, (
            "discovery resolved nothing for an exactly-named project; the "
            "graph leg is starved by wiring, not by the corpus"
        )
        assert "proj_lattice" in graph_leg.seeds_from(found)

    def test_a_mention_carrying_a_leading_sentence_word_does_not_resolve(
        self,
    ) -> None:
        """A MEASURED LIMITATION of the graph arm, pinned rather than tuned.

        "Is Solstice Billing understaffed?" extracts the mention "Is Solstice
        Billing" -- the bare-name backstop takes the sentence-opening word
        with it -- and graph discovery matches a mention by exact canonical
        id, exact display name, alias, or whole-token containment. "is
        solstice billing" is none of those for "Solstice Billing", so the
        subject does not resolve.

        This is a real capability boundary of the arm under test and it
        belongs in the report as one. Widening the matcher, stripping leading
        words, or rewording the corpus question would each make the graph arm
        look better by changing the thing being measured, which is precisely
        the retro-fitting the correction forbids. Pinned so that if the arm
        legitimately improves later, this test fails and the fairness table
        gets updated instead of the improvement going unnoticed.
        """

        projection = _projection()
        grant = corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        assert "Is Solstice Billing" in graph_leg.mention_texts(
            "Is Solstice Billing understaffed?"
        ), "the extraction no longer carries the leading word; re-measure"
        found = graph_leg.discover_subjects(
            question="Is Solstice Billing understaffed?",
            projection=projection,
            authorized_entity_ids=grant,
        )
        assert not found.resolved, (
            "the graph arm now resolves a mention carrying a leading sentence "
            "word. That is an improvement, not a failure -- update the "
            "fairness table's discovery figures rather than deleting this pin"
        )

    def test_a_restricted_same_tenant_project_is_never_resolved(self) -> None:
        """The corpus plants this precisely to catch tenant-derived grants.

        ``proj_quarry`` is inside the analyst's own tenant and outside the
        analyst's grant. A leg that seeded a traversal from it would disclose
        an entity the principal may not see, and no tenant-level check
        anywhere would catch it.
        """

        projection = _projection()
        grant = corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        assert "proj_quarry" not in grant, "fixture drift: quarry is now granted"
        found = graph_leg.discover_subjects(
            question="what is going on with Quarry?",
            projection=projection,
            authorized_entity_ids=grant,
        )
        assert "proj_quarry" not in graph_leg.seeds_from(found), (
            "the graph leg seeded a traversal from a restricted entity"
        )
        assert all(match.canonical_id != "proj_quarry" for match in found.candidates), (
            "a restricted entity was returned as a candidate"
        )

    def test_an_unmatched_question_yields_no_seeds_rather_than_a_fallback(
        self,
    ) -> None:
        """Empty is a RESULT here.

        The corpus's no-match and clarification cases are supposed to produce
        an empty neighbourhood. A leg that substituted any seed would turn
        the trial's hardest safety cases into easy ones.
        """

        projection = _projection()
        grant = corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        found = graph_leg.discover_subjects(
            question="how is the Zephyr Interstellar Relay programme going?",
            projection=projection,
            authorized_entity_ids=grant,
        )
        assert graph_leg.seeds_from(found) == []

    def test_seeds_are_bounded_to_the_contract_subject_horizon(self) -> None:
        """An unbounded seed list is a tenant sweep wearing an investigation's
        name -- the organization-widening the contract refuses."""

        projection = _projection()
        grant = corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        found = graph_leg.discover_subjects(
            question="project",
            projection=projection,
            authorized_entity_ids=grant,
        )
        assert len(graph_leg.seeds_from(found)) <= graph_leg.MAX_SEEDS

    def test_every_corpus_question_is_answerable_without_the_answer(self) -> None:
        """The whole corpus through unaided discovery, as a fairness figure.

        Not asserted as a threshold to beat -- that would be a score. It
        asserts only that discovery RUNS on every authored question and that
        the outcome is bimodal-by-design: some resolve, some do not, and a
        run where everything or nothing resolved would mean the wiring, not
        the corpus, decided the graph column.
        """

        projection = _projection()
        grant = corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        resolved = 0
        for case in authored_cases():
            found = graph_leg.discover_subjects(
                question=case.question,
                projection=projection,
                authorized_entity_ids=grant,
            )
            if found.resolved:
                resolved += 1
        total = len(authored_cases())
        assert 0 < resolved < total, (
            f"unaided discovery resolved {resolved}/{total} corpus questions. "
            "All or nothing means the wiring decided the graph column rather "
            "than the corpus; re-measure before reading any graph figure"
        )


class TestTheTypedMentionPathIsExercised:
    """The blind spot mypy found and no test could have.

    ``mention_texts`` iterates ``extract_mentions``. None of the 39 corpus
    questions produce a TYPED mention -- the untyped bare-name backstop
    supplies all of them -- so that loop body never executed, and it was
    written against a field name (``mention_text``) that does not exist on
    ``DevSubjectMention``. Every fairness test above passed with it broken.

    A whole-tree typecheck caught it. This test makes the path executable
    coverage rather than trusting the next refactor to the type checker
    alone, because the failure mode is silent: a typed mention would simply
    contribute nothing to discovery and the graph arm would look worse for a
    reason no assertion named.
    """

    def test_a_typed_mention_contributes_its_lookup_text(self) -> None:
        texts = graph_leg.mention_texts(_TYPED_MENTION_QUESTION)
        assert texts, (
            "the typed-mention grammar produced no lookup text at all; if the "
            "grammar changed, re-derive this probe rather than deleting it -- "
            "an unexercised loop is how the field-name defect survived"
        )
        assert any("agent context runtime" in text.lower() for text in texts), texts

    def test_a_typed_mention_resolves_through_graph_discovery(self) -> None:
        """End to end, so the field is not merely present but usable."""

        projection = _projection()
        grant = corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        found = graph_leg.discover_subjects(
            question=_TYPED_MENTION_QUESTION,
            projection=projection,
            authorized_entity_ids=grant,
        )
        assert "proj_acr" in graph_leg.seeds_from(found), (
            "a typed mention naming an authorized project did not reach the "
            "seeds; the typed path is wired but not working"
        )


class TestRefusalAndFaultAreNotTheSameOutcome:
    """A capability boundary the arm NAMES is not a defect it does not model.

    ``build_packet`` raises five named errors for boundaries it knows it has
    (an unsupported comparison shape, an incomparable cohort, an unsupported
    match mechanism, an embedder provenance mismatch, an oversized packet).
    Anything else escaping is a defect. Collapsing the two would publish a
    defect as an honest limitation of the technique -- which is the single
    most misleading thing this trial could do, because the ADR reads
    limitations as evidence about graph assistance itself.
    """

    def test_the_named_refusals_are_the_arms_own_error_types(self) -> None:
        """Derived from the arm, not typed twice.

        A hand-kept list would drift the moment the arm added or renamed a
        boundary, and the drift is silent in the direction that matters: a
        renamed refusal would start being recorded as a fault.
        """

        from dev_health_ops.context_fabric.graph_arm import packet_builder

        for name in graph_leg._NAMED_REFUSALS:
            attribute = getattr(packet_builder, name, None)
            assert attribute is not None, (
                f"{name!r} is listed as a named refusal but no longer exists "
                "on packet_builder; a renamed boundary would now be recorded "
                "as an arm fault"
            )
            assert issubclass(attribute, Exception)

    def test_an_unresolved_subject_is_a_refusal_and_never_a_fault(self) -> None:
        """The corpus's no-match cases must not read as arm defects."""

        outcome = graph_leg.GraphPacketOutcome(
            payload=None, refusal="no authorized subject resolved from the question"
        )
        assert outcome.emitted is False
        assert outcome.fault == ""

    def test_an_emitted_outcome_carries_a_payload(self) -> None:
        outcome = graph_leg.GraphPacketOutcome(payload={"schema_version": "x"})
        assert outcome.emitted is True
