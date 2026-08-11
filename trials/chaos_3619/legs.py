"""The two legs of the trial, and the narrow channel between them.

Ruling: the sweep runs twice.

**Leg A — as-deployed.** Both arms receive only the question. The analytical
job comes from the production interpreter, exactly as the shipped product
derives it. This is the deployed-parity baseline and it answers *"what does
the product do today"*.

**Leg B — analytical job held constant.** Both arms additionally receive the
same two values, identically: the corpus case's declared question family and
comparison shape. Question interpretation is thereby removed as a variable,
and the leg answers *"what does graph assistance add with question
interpretation held constant"*.

**What Leg B actually is, stated as the counterfactual rather than implied.**
Handing both arms the declared family is equivalent to the production
constrained-model fallback classifier -- which is BUILT and deliberately
UNWIRED (``production_runtime.py:2468``: "turning it on adds a provider call
to every low-confidence question and is a separate rollout decision") --
operating **perfectly**. So Leg B converts what would otherwise be unmeasured
native headroom into a measured **upper bound**, and it must be read as an
upper bound: a real classifier would not be perfect, so the native figures in
Leg B are the best case a classifier could ever deliver, not a forecast. Leg
B costs no model calls, which is what makes measuring the bound possible at
all.

**Why every Leg B native figure is labelled.** In Leg B the native arm is
handed a classification it demonstrably cannot derive (34 of 39 corpus
questions fall below the fallback floor to ``BOUNDED_INVESTIGATION``, which
``NATIVE_QUESTION_FAMILY`` deliberately does not map). A Leg B native number
quoted without :data:`LEG_B_NATIVE_LABEL` misrepresents the baseline as
stronger than what ships.

**The channel is exactly two fields, and that is enforced rather than
promised.** :func:`leg_b_channel` is the only way a case's data reaches an
arm in Leg B, it returns exactly those two values, and
``test_chaos_3619_legs.py`` asserts from the AST that this module reads no
other attribute of a corpus case. Subjects, expected entities, oracles and
dimension metadata are all outside it; subject resolution stays organic in
both legs per condition 3(a).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
)

__all__ = [
    "LEG_B_CHANNEL_FIELDS",
    "LEG_B_NATIVE_LABEL",
    "LegId",
    "LegBChannel",
    "leg_b_channel",
    "reading_rule",
]


class LegId(StrEnum):
    """Which leg produced a record. Carried on every row."""

    #: Question only; analytical job from the production interpreter.
    AS_DEPLOYED = "leg_a_as_deployed"
    #: Question plus the declared family and shape, identically to both arms.
    JOB_HELD_CONSTANT = "leg_b_job_held_constant"


#: The label every Leg B native figure carries, in the records and in the
#: report. Not a footnote: without it a reader takes a Leg B native column as
#: the product's behaviour, which is the opposite of true.
LEG_B_NATIVE_LABEL = "handed classification — stronger than deployed"

#: The complete set of corpus-case attributes Leg B may read. Anything else
#: -- subjects, expected answers, oracles, dimension ids, catches, topics --
#: would hand an arm part of the answer.
LEG_B_CHANNEL_FIELDS: tuple[str, ...] = ("question_family", "comparison_shape")


@dataclass(frozen=True, slots=True)
class LegBChannel:
    """The entire contents of the Leg B channel.

    A dataclass of exactly two fields rather than a mapping, so "what may
    cross" is a type rather than a habit: a caller cannot add a third key,
    and a reviewer can see the whole channel without reading the builder.
    """

    question_family: QuestionFamilyID
    comparison_shape: ComparisonShape


def leg_b_channel(
    *, question_family: QuestionFamilyID, comparison_shape: ComparisonShape
) -> LegBChannel:
    """The Leg B channel, built from the two permitted values.

    Takes the two values rather than a case object on purpose. A function
    that accepted a ``CorpusCase`` could read anything on it, and the guard
    would then have to prove a negative about a whole object; taking
    primitives makes over-reading impossible at the call boundary instead of
    detectable after it.
    """

    return LegBChannel(
        question_family=question_family, comparison_shape=comparison_shape
    )


def reading_rule() -> str:
    """The one-paragraph instruction the report prints above both legs.

    Kept here rather than written into the report template so the rule and
    the legs cannot drift apart, and so the claims-match-records test has a
    single definition to check prose against.
    """

    return (
        "Leg A answers 'what does the product do today' and is the "
        "deployed-parity baseline. Leg B answers 'what does graph assistance "
        "add with question interpretation held constant'. In Leg B the native "
        f"arm is {LEG_B_NATIVE_LABEL}: it receives a question-family "
        "classification it cannot derive, which is equivalent to the "
        "production constrained-model fallback classifier -- built and "
        "deliberately unwired -- operating perfectly. Leg B's native figures "
        "are therefore an UPPER BOUND on classification headroom, not a "
        "forecast. The native A-to-B delta per family measures classification "
        "headroom with no graph involved; the Leg B graph-versus-native "
        "comparison measures the graph's marginal value beyond classification. "
        "Neither is summed and the two legs are never aggregated together."
    )
