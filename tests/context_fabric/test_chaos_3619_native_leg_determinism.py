"""CHAOS-3619: the native leg's zero-model claim, pinned rather than asserted.

The trial's native arm is driven by a corpus-seeded run through the REAL
orchestrator, so its subject interpretation comes from production code rather
than from anything the trial author wrote. That is the point of the design --
subject discovery and ambiguity are half the corpus, and a hand-built
interpretation would be the trial author baselining themselves.

It also means the fairness table has to answer a question honestly: **can any
code path in that interpretation reach a model?** "The harness scripts the
provider" and "no path can reach a model" are different claims, and only the
second is what the table needs.

``QuestionInterpreter`` has a constrained-model fallback
(``IntentClassifier``), reached when a question lands below
``FALLBACK_CONFIDENCE_FLOOR``. So the answer is not free, and these guards
establish it by mechanism:

1. no ``IntentClassifier`` implementation exists in ``src`` at all -- the
   seam is built and unwired;
2. production constructs ``QuestionInterpreter()`` with no classifier, so the
   trial's configuration matches the shipped one rather than being a
   convenient harness choice;
3. **34 of the 39 authored corpus questions land below the fallback floor.**

(3) is the one that matters and it is pinned as a measured baseline fact, not
smoothed over. It says the deterministic recognizers do not recognise most of
this corpus, which is unsurprising -- the corpus was deliberately authored in
natural and colloquial phrasing, and the correction addendum explicitly
forbids reducing a family to one exact prompt. The consequence for the trial
is precise and must not be overstated in either direction:

* it is **not** a starved baseline. Production wires no classifier either
  (``production_runtime.py`` says so in its own comment: "turning it on adds
  a provider call to every low-confidence question and is a separate rollout
  decision"), so the native arm in this trial behaves exactly as the deployed
  one does;
* it **is** a real limit on what the native arm can do with most corpus
  questions, and any per-family reading of the native column has to be read
  next to this number rather than as a graph-versus-native difference alone.

If anyone wires a classifier, guards 1 and 2 fail loudly and the trial's
zero-spend claim has to be re-established rather than inherited.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from dev_health_ops.api.dev.investigation_corpus.cases import authored_cases
from dev_health_ops.api.dev.question_interpreter import (
    FALLBACK_CONFIDENCE_FLOOR,
    ClassifierProposal,
    QuestionInterpreter,
)
from tests._chaos_3292_preflight import fixed_now, request_for, sequential_ids

_SRC = Path(__file__).resolve().parents[2] / "src" / "dev_health_ops"

#: Measured, then pinned. See the module docstring: this is a baseline
#: capability fact the per-family tables must be read against, and a silent
#: change to recognizer coverage would move the native column for a reason
#: that has nothing to do with graph assistance.
#:
#: Re-measured 2026-08-10, CHAOS-3652: was 34, now 31. CHAOS-3652 adds one
#: new deterministic recognizer (``cohort.discovery``, zero-mention
#: cohort-discovery questions -- "which teams are struggling", "which
#: projects are capacity-constrained") that legitimately moves 3 corpus
#: questions from below the floor to a deterministic
#: ``QuestionIntentID.DISCOVERED_COHORT`` recognition, with no model
#: involved -- guards 1/2 above (no ``IntentClassifier`` exists, production
#: still constructs a bare ``QuestionInterpreter()``) are untouched, so this
#: is recognizer coverage growing, not the fallback floor moving. The three
#: cases, all in families the fairness table already marks as a
#: classifier-closeable CONFOUND rather than a graph RESULT (see
#: ``trials/chaos_3619/results/consolidated-post-wave-note.md``'s CHAOS-3652
#: addendum for the per-family delta):
#:
#: * ``T01_clearly_struggling_team`` (``struggling_teams``) -- "What teams
#:   are currently struggling, and why?"
#: * ``P01_demand_exceeds_capacity`` (``project_capacity``) -- "Which
#:   projects are capacity-constrained right now?"
#: * ``P02_critical_path_few_contributors`` (``project_capacity``) --
#:   "Which projects appear capacity-constrained, understaffed, overstaffed,
#:   or unusually lightly loaded relative to demand?"
CORPUS_QUESTIONS_BELOW_THE_FALLBACK_FLOOR = 31
AUTHORED_CORPUS_CASES = 39


class _RecordingClassifier:
    """A classifier that records rather than answers.

    Returning ``None`` keeps the interpreter on its degraded path, so wiring
    this changes no outcome -- it only makes "would a model have been called
    here" observable. A classifier that raised would be swallowed by the
    fallback's own total exception handling and prove nothing.
    """

    def __init__(self) -> None:
        self.questions: list[str] = []

    async def classify(self, *, question: str) -> ClassifierProposal | None:
        self.questions.append(question)
        return None


class TestTheModelFallbackIsUnwired:
    def test_no_intent_classifier_implementation_exists_in_src(self) -> None:
        """The seam is built and empty. If that changes, the trial must know.

        Scanned from the AST rather than by grepping text, because the
        protocol's own name appears in prose throughout the interpreter and a
        text match would be satisfied by a docstring.
        """

        implementors: list[str] = []
        for path in sorted(_SRC.rglob("*.py")):
            try:
                tree = ast.parse(path.read_text())
            except SyntaxError:  # pragma: no cover - defensive
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                for base in node.bases:
                    name = getattr(base, "id", None) or getattr(base, "attr", None)
                    if name == "IntentClassifier":
                        implementors.append(f"{path.name}:{node.name}")
        assert not implementors, (
            "an IntentClassifier implementation now exists "
            f"({implementors}); the CHAOS-3619 native leg's zero-model claim "
            "was established against a build where the fallback seam was "
            "empty and must be re-established, not inherited"
        )

    def test_production_constructs_the_interpreter_without_a_classifier(
        self,
    ) -> None:
        """The trial's configuration must not be a convenient harness choice.

        If production wired a classifier and the trial did not, the native
        arm would be measured weaker than the deployed one -- the starved
        baseline this whole design exists to avoid.
        """

        source = (_SRC / "api" / "dev" / "production_runtime.py").read_text()
        assert "interpreter=QuestionInterpreter()," in source, (
            "production no longer constructs a bare QuestionInterpreter; if a "
            "classifier is now wired, the trial's native leg is measuring a "
            "weaker interpreter than production ships"
        )

    def test_the_interpreter_defaults_to_no_classifier(self) -> None:
        """The default itself, so a bare construction cannot acquire one."""

        assert QuestionInterpreter()._classifier is None


@pytest.mark.asyncio
class TestHowMuchOfTheCorpusTheRecognizersActuallyRecognize:
    async def test_the_number_of_questions_below_the_fallback_floor_is_pinned(
        self,
    ) -> None:
        """The measured baseline fact, observed through the real interpreter.

        A recording classifier is wired ONLY so the branch becomes visible;
        it returns ``None``, so no interpretation changes. What is asserted
        is which questions would have reached a model in a build that wired
        one -- which is exactly the set the deterministic recognizers do not
        recognise.
        """

        recorder = _RecordingClassifier()
        interpreter = QuestionInterpreter(
            classifier=recorder, mint_id=sequential_ids(), now=fixed_now
        )
        cases = authored_cases()
        assert len(cases) == AUTHORED_CORPUS_CASES, (
            f"the corpus now has {len(cases)} authored cases, not "
            f"{AUTHORED_CORPUS_CASES}; the pinned count below is stated as a "
            "fraction of that total and must be re-measured"
        )
        for case in cases:
            await interpreter.interpret(request_for(case.question))

        assert len(recorder.questions) == CORPUS_QUESTIONS_BELOW_THE_FALLBACK_FLOOR, (
            f"{len(recorder.questions)} of {len(cases)} corpus questions land "
            f"below FALLBACK_CONFIDENCE_FLOOR ({FALLBACK_CONFIDENCE_FLOOR}), "
            f"not the pinned {CORPUS_QUESTIONS_BELOW_THE_FALLBACK_FLOOR}. This "
            "number is a baseline capability fact the trial's per-family "
            "tables are read against -- re-measure it and update the fairness "
            "table rather than adjusting the constant to match"
        )

    async def test_some_corpus_questions_are_recognized(self) -> None:
        """Anti-vacuity, in the direction that matters.

        If every question fell below the floor, the pinned count above would
        be satisfied by an interpreter that recognises nothing at all -- and
        the native column would be uniformly unrecognised for a reason no
        assertion here would distinguish from a broken harness.
        """

        recorder = _RecordingClassifier()
        interpreter = QuestionInterpreter(
            classifier=recorder, mint_id=sequential_ids(), now=fixed_now
        )
        for case in authored_cases():
            await interpreter.interpret(request_for(case.question))
        recognized = AUTHORED_CORPUS_CASES - len(recorder.questions)
        assert recognized > 0, (
            "no corpus question was recognised by any deterministic "
            "recognizer; that is a broken harness, not a baseline"
        )

    async def test_with_no_classifier_the_fallback_cannot_run_at_all(self) -> None:
        """The trial's actual configuration, asserted on the real path.

        The two guards above establish that no classifier is wired anywhere.
        This one establishes the consequence: with the construction the trial
        uses, the low-confidence branch is unreachable, so the questions
        counted above cost nothing and reach no provider.
        """

        interpreter = QuestionInterpreter(mint_id=sequential_ids(), now=fixed_now)
        assert interpreter._classifier is None
        for case in authored_cases():
            result = await interpreter.interpret(request_for(case.question))
            # Every question still resolves to a usable interpretation --
            # degraded, never absent. An absent one would make the native arm
            # unprojectable for a harness reason rather than a capability one.
            assert result.intent is not None
