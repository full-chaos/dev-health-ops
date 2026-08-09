"""CHAOS-3617: the arm never *composes* text — the corrected no-prose claim.

The original claim was "there is nowhere in a structured record for a
sentence to live". Adversarial review demonstrated it false in four places at
once: ``display_label``, observation ``title``, alias values and ``outcome``
are all source-supplied free text bounded only by length, so a project whose
real name is a sentence — or a review title containing a person's name — is
stored and carried into the packet.

Narrowing an over-claim is not the same as dropping it, so this module pins
what is actually true and actually needed:

* **the arm authors nothing.** Every textual value it stores is a verbatim
  copy of a source field, or a rejection. Nothing formats, concatenates,
  templates or summarises. That is the property the issue's rule needs: no
  adapter can "help" by writing a nice summary of a structured record.
* **transiting text is untrusted evidence**, not a fact the arm asserted, and
  is never a subject, a ranking or an aggregate.

The two halves are tested differently on purpose. "Authors nothing" is a
property of the *code*, so it is checked against the source. "Transits
verbatim" is a property of the *data*, so it is checked by pushing prose and
a person's name through a real projection and asserting byte-identity with
what went in.
"""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm.records import (
    AliasRecord,
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    AliasKind,
    GraphEntityKind,
    GraphObservationKind,
)

_NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
_ARM_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "dev_health_ops"
    / "context_fabric"
    / "graph_arm"
)

#: A label that is a sentence AND contains a person's name — both halves of
#: what review showed can transit.
_PROSE_LABEL = "Ada Lovelace kept cycling this project through review last Tuesday."
_PROSE_TITLE = "Ada Lovelace requested changes because the migration was rushed"
_PROSE_ALIAS = "the thing Ada broke"
_PROSE_OUTCOME = "Ada thinks this needs another pass"


def _call_name(call: ast.Call) -> str:
    """The bare constructor name of a call, e.g. ``DriverCandidate``."""

    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ast.unparse(func)


def _keywords_in(tree: ast.AST, fields: frozenset[str]):
    """Every ``(constructor, keyword)`` in ``tree`` assigning one of ``fields``."""

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        owner = _call_name(node)
        for keyword in node.keywords:
            if keyword.arg in fields:
                yield owner, keyword


def _is_composed(value: ast.expr) -> bool:
    if isinstance(value, ast.JoinedStr | ast.BinOp):
        return True
    return isinstance(value, ast.Call) and ast.unparse(value.func).endswith(
        (".format", ".join")
    )


def _keyword_nodes(fields: frozenset[str]):
    """Every keyword argument in the arm assigning one of ``fields``.

    Yields the *constructor* each keyword belongs to as well, because one
    field name can carry opposite rules in two different types. ``summary``
    is the live example: on Graphiti's ``EntityNode`` it is the
    model-written slot that must stay an empty literal, and on the frozen
    contract's ``DriverCandidate`` it is a required field in which the arm
    states its own finding. A field-name-only rule cannot express both, and
    the tempting workaround — composing into a local and assigning the local
    — is explicitly outside what this scan can see, so taking it would be
    evading the guard rather than satisfying it.
    """

    for path in sorted(_ARM_ROOT.glob("*.py")):
        if path.name == "fixtures.py":
            # The fixture world is authored test data by definition.
            continue
        for owner, keyword in _keywords_in(ast.parse(path.read_text()), fields):
            yield path, owner, keyword


def _composed_assignments(
    fields: frozenset[str], *, exempt: frozenset[tuple[str, str]] = frozenset()
) -> list[str]:
    offenders: list[str] = []
    for path, owner, node in _keyword_nodes(fields):
        if (owner, node.arg) in exempt:
            continue
        if _is_composed(node.value):
            offenders.append(
                f"{path.name}: {owner}.{node.arg}={ast.unparse(node.value)[:70]}"
            )
    return offenders


def _prose_batch() -> IngestionBatch:
    return IngestionBatch(
        org_id="org_alpha",
        entities=(
            EntityRecord(
                org_id="org_alpha",
                kind=GraphEntityKind.PROJECT,
                canonical_id="proj_x",
                display_label=_PROSE_LABEL,
                source_class=SourceClass.WORK_GRAPH,
                observed_at=_NOW,
                aliases=(AliasRecord(kind=AliasKind.ALIAS, value=_PROSE_ALIAS),),
            ),
        ),
        observations=(
            ObservationRecord(
                org_id="org_alpha",
                kind=GraphObservationKind.REVIEW,
                canonical_id="rev_1",
                title=_PROSE_TITLE,
                source_class=SourceClass.REVIEW,
                observed_at=_NOW,
                subjects=(
                    CanonicalRef(kind=GraphEntityKind.PROJECT, canonical_id="proj_x"),
                ),
                outcome=_PROSE_OUTCOME,
            ),
        ),
    )


#: Fields that carry SOURCE RECORD content. These must be verbatim copies:
#: composing one is the "convert the structured record into prose" fault.
_SOURCE_TEXT_FIELDS = frozenset(
    {"name", "display_label", "title", "summary", "outcome", "matched_text"}
)

#: Fields where the arm writes about ITSELF -- a truncation reason, a missing
#: source's impact, why a path was included. These are disclosures the frozen
#: contract *requires*, and composing them is correct. Separating the two sets
#: is the point: the rule is that structured records are not rendered as
#: prose, not that the arm may never write a sentence about its own behaviour.
_ARM_AUTHORED_FIELDS = frozenset(
    {
        "detail",
        "impact",
        "inclusion_reason",
        "inclusion_rationale",
        "match_rationale",
        "qualification_note",
        "job_statement",
        "conflict_note",
        "rationale",
        "prompt",
        "provenance",
    }
)

#: ``(constructor, field)`` pairs where a name in :data:`_SOURCE_TEXT_FIELDS`
#: is in fact the arm writing about its own finding, and composing it is
#: correct.
#:
#: Exactly one entry, and it is a name collision rather than an exception to
#: the rule: Graphiti's ``EntityNode.summary`` is the model-written slot the
#: arm keeps empty, while the frozen contract's ``DriverCandidate.summary``
#: is a required field in which the arm states what it found. Keyed on the
#: constructor so the first rule survives untouched — the
#: ``EntityNode.summary`` plant below still has to pass.
_ARM_AUTHORED_BY_CONSTRUCTOR: frozenset[tuple[str, str]] = frozenset(
    {("DriverCandidate", "summary")}
)


class TestTheArmComposesNoText:
    """The compose guard. A property of the code, checked against the code."""

    def test_the_two_field_sets_are_disjoint(self) -> None:
        """A field cannot be both a source copy and an arm disclosure."""

        assert not (_SOURCE_TEXT_FIELDS & _ARM_AUTHORED_FIELDS)

    def test_no_source_text_field_is_ever_assigned_a_composed_value(self) -> None:
        """Verbatim copy or nothing, for every field carrying record content.

        Requires the right-hand side to be a plain read (``record.title``,
        ``node.display_label``) or a literal — never an f-string, a
        ``format``/``join`` call, or a ``+``/``%`` expression.

        The one deliberate exception is ``EntityEdge.fact``, which *is*
        constructed — from three canonical identifiers and nothing else, by
        :func:`~.backend.triple_fact`, round-tripped by ``parse_triple_fact``.
        It is excluded by name so a second constructed field cannot join it
        silently.

        **What this scan does NOT cover, stated because a partial guard
        described as total is worse than no guard.** It matches *direct
        keyword assignment* only. Text composed into a local first and then
        passed by name — ``label = f"{a} {b}"`` … ``display_label=label`` — is
        invisible to it, as is composition through a helper function or a
        dict built elsewhere and splatted.

        Two other layers cover what it misses, and both caught the planted
        cases during verification:
        ``TestSourceTextTransitsVerbatimAsUntrustedEvidence`` asserts
        byte-identity between what a source record supplied and what the
        projection stored, which fails for *any* composition however it was
        written; and the live differential compares the whole readout, which
        fails when the two readers disagree about a stored value. This scan is
        the cheap, fast, specific layer — not the proof.
        """

        offenders = _composed_assignments(
            _SOURCE_TEXT_FIELDS, exempt=_ARM_AUTHORED_BY_CONSTRUCTOR
        )
        assert not offenders, (
            "the arm composed text into a field that must be a verbatim copy "
            f"of a source value: {offenders}"
        )

    def test_no_arm_authored_disclosure_interpolates_source_text(self) -> None:
        """A disclosure may be a sentence; it may not quote a record's text.

        This is the second half of the same rule and the more subtle one.
        ``detail="3 results were filtered"`` is a required disclosure.
        ``detail=f"{entity.display_label} was filtered"`` would smuggle a
        source-supplied label — possibly a sentence, possibly a person's name
        — into a field a consumer reads as the arm's own words.
        """

        quoted = {"display_label", "title", "name", "matched_text", "outcome"}
        checked = _ARM_AUTHORED_FIELDS | {
            field for _, field in _ARM_AUTHORED_BY_CONSTRUCTOR
        }
        exempt_owners = {
            owner
            for owner, field in _ARM_AUTHORED_BY_CONSTRUCTOR
            if field in _SOURCE_TEXT_FIELDS
        }
        offenders: list[str] = []
        for path, owner, node in _keyword_nodes(checked):
            if node.arg in _SOURCE_TEXT_FIELDS and owner not in exempt_owners:
                # A source-text field outside the collision list is governed
                # by the verbatim-copy rule above, not by this one.
                continue
            rendered = ast.unparse(node.value)
            for attribute in quoted:
                if f".{attribute}" in rendered:
                    offenders.append(f"{path.name}: {owner}.{node.arg}={rendered[:80]}")
        assert not offenders, (
            f"an arm-authored disclosure interpolated source-supplied text: {offenders}"
        )

    def test_the_constructor_exemption_is_narrow(self) -> None:
        """The collision list exempts a TYPE, not a field name.

        Two plants, and both halves matter. ``DriverCandidate.summary`` may
        be composed because it is the arm stating its own finding. The same
        keyword on Graphiti's ``EntityNode`` is the model-written slot and
        must still be caught — that is the rule the call-aware scan was
        introduced to preserve, so it gets a plant of its own rather than an
        assurance.
        """

        planted = ast.parse(
            "DriverCandidate(summary=f'{a} blocked {b}')\n"
            "EntityNode(summary=f'{a} blocked {b}')\n"
            "SomeOtherNode(summary=f'{a} blocked {b}')\n"
        )
        flagged = {
            owner
            for owner, keyword in _keywords_in(planted, _SOURCE_TEXT_FIELDS)
            if _is_composed(keyword.value)
            and (owner, keyword.arg) not in _ARM_AUTHORED_BY_CONSTRUCTOR
        }
        assert flagged == {"EntityNode", "SomeOtherNode"}

    def test_every_constructor_exemption_names_a_real_collision(self) -> None:
        """An exemption for a field nobody treats as source text is dead weight.

        A pair that drifted out of ``_SOURCE_TEXT_FIELDS`` would silently
        exempt nothing while reading as a deliberate carve-out — and the next
        reader would trust it.
        """

        assert _ARM_AUTHORED_BY_CONSTRUCTOR
        for owner, field in _ARM_AUTHORED_BY_CONSTRUCTOR:
            assert field in _SOURCE_TEXT_FIELDS, (owner, field)

    def test_the_scan_would_notice_a_composed_field(self) -> None:
        """Anti-vacuity: the detector must fire on a planted composition.

        Without this, an empty field set or a broken walk would make both
        assertions above pass over any codebase at all.
        """

        planted = ast.parse('Node(display_label=f"{a} and {b}")')
        found = [
            node
            for node in ast.walk(planted)
            if isinstance(node, ast.keyword)
            and node.arg == "display_label"
            and isinstance(node.value, ast.JoinedStr)
        ]
        assert found
        assert _SOURCE_TEXT_FIELDS and _ARM_AUTHORED_FIELDS

    def test_projected_nodes_carry_no_summary(self) -> None:
        """Graphiti's model-written-summary slot stays empty."""

        projection = build_projection(_prose_batch())
        for node in projection.nodes:
            assert not getattr(node, "summary", "")


class TestSourceTextTransitsVerbatimAsUntrustedEvidence:
    """The data half. What review proved possible, pinned as *intended*."""

    def test_prose_and_a_person_name_transit_unchanged(self) -> None:
        """Byte-identical in and out — no truncation, no rewriting, no scrub.

        This is deliberately an assertion that it *does* transit. The arm
        must not quietly mangle a provider's real label, and pretending the
        text is not there would be the over-claim all over again.
        """

        projection = build_projection(_prose_batch())
        entity = next(
            node for node in projection.nodes if node.canonical_id == "proj_x"
        )
        observation = next(
            node for node in projection.nodes if node.canonical_id == "rev_1"
        )
        assert entity.display_label == _PROSE_LABEL
        assert entity.aliases[0].value == _PROSE_ALIAS
        assert observation.display_label == _PROSE_TITLE
        assert observation.attributes["outcome"] == _PROSE_OUTCOME

    def test_a_person_named_in_source_text_is_never_an_entity(self) -> None:
        """The no-person claim, at the strength it actually holds.

        "Ada Lovelace" is in the graph as characters inside a review title.
        She is not a node, so she cannot be a traversal endpoint, a cohort
        member, a ranked subject or a driver's affected subject — there is no
        kind that could hold her.
        """

        projection = build_projection(_prose_batch())
        entity_ids = {node.canonical_id for node in projection.nodes if node.is_entity}
        assert entity_ids == {"proj_x"}
        assert not any(
            "ada" in node.canonical_id.lower()
            for node in projection.nodes
            if node.is_entity
        )

    def test_no_vocabulary_member_can_express_a_person(self) -> None:
        """Structural, and the reason the narrowed claim is still worth making."""

        person_words = {"person", "user", "member", "identity", "author", "developer"}
        for kind in (*GraphEntityKind, *GraphObservationKind):
            assert not (set(kind.value.split("_")) & person_words), kind

    def test_a_source_label_that_is_too_long_is_refused_on_size_alone(self) -> None:
        """The bound that does exist bounds size, and says so.

        Kept because an unbounded label is a real ingestion hazard; not
        confused with a content guarantee.
        """

        from dev_health_ops.context_fabric.graph_arm.projection import (
            MAX_ATTRIBUTE_CHARS,
            ProjectionError,
        )

        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(
                EntityRecord(
                    org_id="org_alpha",
                    kind=GraphEntityKind.PROJECT,
                    canonical_id="proj_x",
                    display_label="x" * (MAX_ATTRIBUTE_CHARS + 1),
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="bounds SIZE, not"):
            build_projection(batch)
