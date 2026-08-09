"""CHAOS-3617 PR2: structural driver discovery.

The capability the whole correction hinges on. The native arm cannot assert
a driver at all; whether *this* arm can earn principal standing under the
frozen rules — a real cause, on a real path, with real evidence, currently
relevant — is the trial's live question.

**What a graph is entitled to assert.** The contract's governing rule is
"the graph determines what is relevant; canonical services determine what is
measurable". So every finding here is *structural*: a subject is blocked by
an open unit, a parent is declared complete while a child is not, an
operational control is open. None of it is a number, none of it is an
average, and the categories that would need one are refused by name rather
than approximated.

**Symptom versus driver is decided before standing, not after.** A cause
acts *on* the subject; a symptom is an effect *of* something. In a directed
graph with a frozen allowlist that distinction is readable from orientation
and kind, so it is computed from the graph rather than asserted — and a
symptom can never hold principal standing, which the frozen contract also
enforces independently. Collapsing the two is the single cheapest way for
unsupported attribution to reach an answer, and
``CONTEXTUAL_CORRELATE`` is the honest third option for something that is
merely present.

**Exclusions are the result, not the leftovers.** Every one of the frozen
contract's six exclusion reasons is reachable here and each is earned by a
real shape in the world: a historical edge, a linkage asserted only by
untrusted content, a candidate with no path, evidence outside the caller's
grant, a symptom whose cause is also a candidate, and a category the graph
cannot measure. "Why is X not the answer" is a question the packet exists to
answer, and an absence answers nothing.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    AssertionBasis,
    ConfidenceQualifier,
    DriverCategory,
    DriverExclusionReason,
    DriverRole,
    DriverStanding,
    RelationshipType,
)

from .readback import DiscoveredObservation, DiscoveredPath, InvestigationReadout
from .vocabulary import GraphEntityKind, GraphObservationKind

__all__ = [
    "COMPLETE_DECLARED_STATUSES",
    "MEASUREMENT_ONLY_CATEGORIES",
    "TRUSTED_ATTRIBUTION_LEVELS",
    "DriverFinding",
    "StandingMechanism",
    "discover_drivers",
]

#: Declared statuses that mean "this is done".
#:
#: Compared against a *declared* status deliberately. The gap between what a
#: provider declares and what its children show is a finding in itself, and
#: an arm that silently normalised the two would have destroyed the evidence
#: for it before anything could look.
COMPLETE_DECLARED_STATUSES: frozenset[str] = frozenset({"complete", "cancelled"})

#: Trust levels whose records may support an attribution.
#:
#: ``untrusted_content`` is absent, and that absence is the entire poisoned-
#: linkage guard. The corpus plants a dependency edge between two real
#: canonical entities asserted only by an untrusted planning note; nothing
#: about the edge's *shape* marks it false, and only the trust of the record
#: asserting it separates it from a true one.
TRUSTED_ATTRIBUTION_LEVELS: frozenset[str] = frozenset(
    {"canonical", "provider_asserted"}
)

#: Categories that are statements about a NUMBER, which this revision does
#: not hold. Named so they are refused explicitly rather than approximated
#: from structure — "this team owns a lot of projects" is not a cycle-time
#: claim, and an arm that let it become one would be measuring something
#: adjacent to the question, which is the fault the whole correction exists
#: to avoid.
#:
#: Nothing here produces a candidate in one of these categories yet, so this
#: is currently a *declaration of scope* rather than a live filter. It
#: becomes load-bearing in the measurement commit; the test that pins it
#: asserts the structural rules never emit one, which is a claim that can
#: fail today.
MEASUREMENT_ONLY_CATEGORIES: frozenset[DriverCategory] = frozenset(
    {
        DriverCategory.DELIVERY_PRESSURE,
        DriverCategory.REVIEW_PRESSURE,
        DriverCategory.CAPACITY_OR_STAFFING,
        DriverCategory.INVESTMENT_MIX,
    }
)

#: Relationships that put a CAUSE on the subject: the far end acts on it.
_BLOCKING_RELATIONSHIPS: Mapping[RelationshipType, DriverCategory] = {
    RelationshipType.BLOCKED_BY: DriverCategory.EXTERNAL_BLOCKER,
    RelationshipType.DEPENDS_ON: DriverCategory.DEPENDENCY_PRESSURE,
}

#: Relationships whose far end is REMAINING WORK under the subject.
_CHILD_RELATIONSHIPS: frozenset[RelationshipType] = frozenset(
    {RelationshipType.PARENT_OF, RelationshipType.CONTRIBUTES_TO}
)

#: Observation kinds that are effects rather than causes. An incident on the
#: subject is something that happened *to* it; presenting one as the driver
#: is the symptom-as-driver fault in its most recognisable form.
_SYMPTOM_OBSERVATION_KINDS: frozenset[GraphObservationKind] = frozenset(
    {
        GraphObservationKind.INCIDENT,
        GraphObservationKind.STATUS_CHANGE,
        GraphObservationKind.TEST_REPORT,
    }
)


class StandingMechanism:
    """How a finding earned its standing. Trial metadata, never on the wire.

    Kept distinguishable because "the graph alone was enough" and "a cited
    canonical measurement was needed" are different answers to the trial's
    question, and a per-family score that could not tell them apart would
    report the graph hypothesis as confirmed by evidence that came from
    somewhere else.
    """

    #: Earned from relationships, kinds and declared status only.
    STRUCTURAL = "structural"
    #: Earned by citing a canonical service's measurement.
    CITED_MEASUREMENT = "cited_measurement"


@dataclass(frozen=True, slots=True)
class DriverFinding:
    """One candidate cause, classified, with its standing already decided."""

    driver_id: str
    subject_id: str
    #: The entity the finding is *about* — the blocker, the open child, the
    #: control. Distinct from ``subject_id``, which is what it acts on.
    cause_id: str
    category: DriverCategory
    role: DriverRole
    standing: DriverStanding
    mechanism: str
    #: Structured parts the packet builder renders into the contract's
    #: ``summary``. Kept as parts rather than a sentence so the rendering
    #: lives in one place and cannot quietly start quoting source text.
    summary_subject: str
    summary_detail: str
    path_ids: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    exclusion_reason: DriverExclusionReason | None = None
    assertion_basis: AssertionBasis = AssertionBasis.SOURCE_ASSERTED
    confidence_qualifier: ConfidenceQualifier = ConfidenceQualifier.QUALIFIED
    conflicting_evidence_ids: tuple[str, ...] = ()
    conflict_detail: str | None = None

    @property
    def is_asserted(self) -> bool:
        return self.standing in {
            DriverStanding.PRINCIPAL_DRIVER,
            DriverStanding.CONTRIBUTING_DRIVER,
        }


@dataclass(frozen=True, slots=True)
class _Context:
    """Everything the rules read, assembled once."""

    subject_id: str
    as_of: datetime
    authorized: frozenset[str]
    entity_kind: Mapping[str, GraphEntityKind]
    declared_status: Mapping[str, str]
    observations: Mapping[str, DiscoveredObservation]
    #: observation id -> whether its record may support an attribution.
    trusted_observation: Mapping[str, bool] = field(default_factory=dict)


def _is_trusted(observation: DiscoveredObservation) -> bool:
    """Whether this record may support an attribution.

    An observation carrying NO trust level is not trusted. The first version
    defaulted to ``"canonical"``, and that default did real damage: a
    readback bug had already stripped the attribute, so every untrusted note
    in the world read as canonical and the corpus's planted false dependency
    reached PRINCIPAL DRIVER on the strength of an untrusted planning note.
    The readback bug is fixed; the default is gone as well, because a
    default that silently absorbs a marshalling failure turns a loud bug
    into a false claim.
    """

    trust = observation.attributes.get("corpus_trust")
    return trust is not None and trust in TRUSTED_ATTRIBUTION_LEVELS


def _is_complete(context: _Context, canonical_id: str) -> bool:
    declared = context.declared_status.get(canonical_id)
    return declared is not None and declared in COMPLETE_DECLARED_STATUSES


def _is_declared_open(context: _Context, canonical_id: str) -> bool:
    """Whether something is positively declared unfinished.

    Deliberately NOT ``not _is_complete(...)``. A service or a dependency
    carries no completion status at all, and reading that silence as
    "unfinished" made every dependency an open blocker — ``svc_auth_gateway``
    became a driver of the identity rewrite purely for existing. Absence of a
    status is absence of evidence, in this direction as much as any other.
    """

    declared = context.declared_status.get(canonical_id)
    return declared is not None and declared not in COMPLETE_DECLARED_STATUSES


def _paths_between(
    readout: InvestigationReadout, subject_id: str, other_id: str
) -> tuple[DiscoveredPath, ...]:
    """Every discovered path connecting the subject to ``other_id``.

    A finding with no path is not a finding this arm may assert: without
    lineage there is no mechanism, only two things that are both true.
    """

    return tuple(
        path
        for path in readout.paths
        if subject_id in path.touched_ids() and other_id in path.touched_ids()
    )


def _currency(paths: Sequence[DiscoveredPath], as_of: datetime) -> bool:
    """Whether every step of at least one supporting path was in force.

    Checked per path rather than per step across all paths: one live route
    is enough to make the relationship current, but a route containing a
    closed edge cannot be the one cited for it.
    """

    return any(all(step.is_current_at(as_of) for step in path.steps) for path in paths)


def _linkage_observations(
    context: _Context,
    paths: Sequence[DiscoveredPath],
    linkage: tuple[str, RelationshipType, str],
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """``(trusted, untrusted, withheld)`` observation ids for ONE edge.

    Scoped to the edge that asserts the linkage, and to nothing else. The
    first version of this collected every observation touching the *cause
    entity*, and the corpus caught it immediately: ``dep_authcore`` is a
    real dependency of four real projects, so a canonical record
    (``wg_authcore_shared``) attached to one of those true edges was read as
    support for the fabricated ``proj_meridian blocked_by dep_authcore``
    edge — which the untrusted planning note is the *only* record asserting.
    That version promoted the corpus's planted false claim to PRINCIPAL
    DRIVER. Support has to travel with the edge, not with the entity.

    Both halves are returned because the untrusted set is not noise to be
    dropped — a linkage whose only support is untrusted content is a
    specific, named finding, and discarding those ids would leave the
    exclusion unable to say what it excluded.
    """

    near, relationship, far = linkage
    seen: list[str] = []
    for path in paths:
        for step in path.steps:
            if step.relationship is not relationship:
                continue
            if {step.from_canonical_id, step.to_canonical_id} != {near, far}:
                continue
            seen.extend(step.observation_ids)
    ordered = sorted(set(seen))
    # An id an edge references but the readout does not carry is one the
    # traversal withheld: observations are filtered to subjects this caller
    # reached, while an edge's observation ids are raw. That is a real
    # authorization difference and it is reported as one -- counting it as
    # ordinary missing evidence would tell a caller "nothing supports this"
    # when the truth is "you may not see what does".
    withheld = tuple(item for item in ordered if item not in context.observations)
    visible = [item for item in ordered if item in context.observations]
    trusted = tuple(item for item in visible if context.trusted_observation.get(item))
    untrusted = tuple(
        item for item in visible if not context.trusted_observation.get(item)
    )
    return trusted, untrusted, withheld


def _observation_support(
    context: _Context, observation_id: str
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """``(trusted, untrusted, withheld)`` for a one-observation finding.

    Nothing can be withheld here: the observation came from the readout, so
    the caller can see it by construction.
    """

    if context.trusted_observation.get(observation_id):
        return (observation_id,), (), ()
    return (), (observation_id,), ()


def _classify(
    context: _Context,
    *,
    driver_id: str,
    cause_id: str,
    category: DriverCategory,
    role: DriverRole,
    summary_detail: str,
    paths: Sequence[DiscoveredPath],
    support: tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]],
) -> DriverFinding:
    """Decide one candidate's standing and, if excluded, why.

    ``support`` is ``(trusted, untrusted)`` observation ids, computed by the
    caller because the correct scope differs by rule: an attribution's
    support is the *edge* that asserts it, and a symptom's is the
    observation itself. Centralising it here was the first design and it is
    what let a canonical record attached to a different edge vouch for a
    fabricated one.

    The order of the checks is the point and is not arbitrary: withheld
    evidence first (a caller who may not see the support must be told that,
    not told the claim is unsupported), then trust, then — for a *driver*
    claim only — currency. Each earlier check would mask a later one, and
    the reason reported must be the first thing actually wrong.

    **Two of the contract's six exclusion reasons are deliberately not
    produced by the structural rules, and saying so is the point.**
    ``NO_SUPPORTING_PATH`` cannot fire from them: every candidate is derived
    from a step on a discovered path, so a pathless driver is
    unconstructable rather than merely unobserved — lineage is structural
    here, not checked. ``INSUFFICIENT_MEASUREMENT`` belongs to the
    measurement commit, where a candidate can hold a measurement-only
    category with no number cited. Both branches are kept because
    :func:`discover_drivers` is not the only way a finding can be built, but
    neither is claimed as covered by the structural rules, and the tests say
    which is which rather than counting six and calling it complete.
    """

    path_ids = tuple(path.path_id for path in paths)
    trusted, untrusted, withheld = support

    def finding(
        standing: DriverStanding,
        reason: DriverExclusionReason | None = None,
        **overrides: object,
    ) -> DriverFinding:
        return DriverFinding(
            driver_id=driver_id,
            subject_id=context.subject_id,
            cause_id=cause_id,
            category=category,
            role=role,
            standing=standing,
            mechanism=StandingMechanism.STRUCTURAL,
            summary_subject=cause_id,
            summary_detail=summary_detail,
            path_ids=path_ids,
            evidence_ids=trusted,
            exclusion_reason=reason,
            **overrides,  # type: ignore[arg-type]
        )

    if category in MEASUREMENT_ONLY_CATEGORIES:
        return finding(
            DriverStanding.EXCLUDED, DriverExclusionReason.INSUFFICIENT_MEASUREMENT
        )
    if not trusted and withheld:
        # Everything that would have supported this is outside the caller's
        # grant. Reported as its own reason rather than folded into
        # "unsupported": the two look identical in a packet and mean opposite
        # things to the reader.
        return finding(
            DriverStanding.EXCLUDED, DriverExclusionReason.UNAUTHORIZED_EVIDENCE
        )
    if not trusted:
        # Every record backing this is untrusted content. The edge or the
        # observation exists in the graph -- it is ingested, not filtered,
        # so a correct arm can be SEEN declining it rather than never having
        # had it. ``untrusted`` may be empty too, which is the plain
        # no-evidence case and reported the same way: nothing canonical
        # supports the claim.
        return finding(
            DriverStanding.EXCLUDED,
            DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED,
            conflicting_evidence_ids=untrusted,
            conflict_detail=(
                "no canonical or provider-asserted record supports this; the "
                "only records asserting it are untrusted content"
            ),
        )
    if role is not DriverRole.DRIVER:
        # A symptom or a correlate may be reported; it may not be asserted,
        # and it is not held to the lineage bar because it makes no causal
        # claim to have lineage FOR. The frozen contract refuses principal
        # standing to a symptom independently, so this is defence in depth --
        # but the contract cannot tell a mislabelled driver from a real one,
        # which is why the classification has to be honest here.
        return finding(DriverStanding.CANDIDATE_ONLY)
    if not paths:
        return finding(
            DriverStanding.EXCLUDED, DriverExclusionReason.NO_SUPPORTING_PATH
        )
    if not _currency(paths, context.as_of):
        return finding(
            DriverStanding.EXCLUDED, DriverExclusionReason.NOT_CURRENTLY_RELEVANT
        )
    return finding(DriverStanding.CONTRIBUTING_DRIVER)


def _blocking_candidates(
    context: _Context, readout: InvestigationReadout
) -> list[DriverFinding]:
    """Things that act on the subject: blockers and live dependencies."""

    findings: list[DriverFinding] = []
    for path in readout.paths:
        for step in path.steps:
            category = _BLOCKING_RELATIONSHIPS.get(step.relationship)
            if category is None:
                continue
            if step.from_canonical_id != context.subject_id:
                continue
            cause_id = step.to_canonical_id
            if not step.is_current_at(context.as_of):
                # Deliberately NOT skipped. A dependency that closed before
                # the window is a candidate the arm considered and rejected,
                # and "why isn't the old dependency the answer" is a question
                # the packet exists to answer. Falling through means
                # ``_classify`` reports NOT_CURRENTLY_RELEVANT rather than the
                # edge vanishing -- and it means the currency guard is
                # exercised on the corpus's planted historical edge instead of
                # that edge disappearing for an unrelated reason.
                pass
            elif step.relationship is RelationshipType.DEPENDS_ON:
                # For a dependency, what makes it a *pressure* is the far end
                # being unfinished. Either it is finished, or nothing says it
                # is unfinished; neither is a cause, and neither is an
                # exclusion -- the graph simply does not say this is holding
                # anything up.
                if not _is_declared_open(context, cause_id):
                    continue
            elif _is_complete(context, cause_id):
                # ``blocked_by`` is different in kind: the edge IS the
                # provider asserting that this blocks. So the far end needs
                # no status of its own, and a dependency with no completion
                # concept can still be a declared blocker. Only a positively
                # finished blocker drops out.
                #
                # This distinction is load-bearing rather than tidy. Applying
                # the dependency rule to ``blocked_by`` made the corpus's
                # planted false claim -- ``proj_meridian blocked_by
                # dep_authcore``, asserted only by an untrusted planning note
                # -- disappear because ``dep_authcore`` carries no declared
                # status, NOT because the arm judged the record untrustworthy.
                # It passed for the wrong reason, and would have come back the
                # moment the corpus gave that dependency a status.
                continue
            detail = (
                "is blocked by this, which is not complete"
                if step.relationship is RelationshipType.BLOCKED_BY
                else "depends on this, which is not complete"
            )
            blocking_paths = _paths_between(readout, context.subject_id, cause_id)
            findings.append(
                _classify(
                    context,
                    driver_id=f"drv_block_{cause_id}",
                    cause_id=cause_id,
                    category=category,
                    role=DriverRole.DRIVER,
                    summary_detail=detail,
                    paths=blocking_paths,
                    support=_linkage_observations(
                        context,
                        blocking_paths,
                        (context.subject_id, step.relationship, cause_id),
                    ),
                )
            )
    return findings


def _open_child_candidates(
    context: _Context, readout: InvestigationReadout
) -> list[DriverFinding]:
    """Remaining work under a subject that has been declared finished.

    This is the declared-versus-actual divergence as a *cause*: the honest
    answer to "why is this not finished" is the child that is not finished,
    and the declaration itself is a symptom of the same gap.
    """

    if not _is_complete(context, context.subject_id):
        return []
    findings: list[DriverFinding] = []
    for path in readout.paths:
        for step in path.steps:
            if step.relationship not in _CHILD_RELATIONSHIPS:
                continue
            ends = (step.from_canonical_id, step.to_canonical_id)
            if context.subject_id not in ends:
                # The step has to touch the SUBJECT. Without this, any
                # parent_of edge anywhere on any discovered path produced a
                # candidate: ``pf_platform`` became an open child of
                # ``proj_ledger_migration`` because both appeared somewhere
                # on the same walk. Adjacency is what "child of" means.
                continue
            child = next(end for end in ends if end != context.subject_id)
            if not _is_declared_open(context, child):
                continue
            child_paths = _paths_between(readout, context.subject_id, child)
            findings.append(
                _classify(
                    context,
                    driver_id=f"drv_open_{child}",
                    cause_id=child,
                    category=DriverCategory.SCOPE_CHANGE,
                    role=DriverRole.DRIVER,
                    summary_detail=(
                        "is open while the subject it belongs to is declared complete"
                    ),
                    paths=child_paths,
                    support=_linkage_observations(
                        context,
                        child_paths,
                        (context.subject_id, step.relationship, child),
                    ),
                )
            )
    return findings


def _symptom_candidates(
    context: _Context, readout: InvestigationReadout
) -> list[DriverFinding]:
    """Effects observed on the subject, reported as effects."""

    findings: list[DriverFinding] = []
    for observation in readout.observations:
        if observation.kind not in _SYMPTOM_OBSERVATION_KINDS:
            continue
        if context.subject_id not in observation.subject_canonical_ids:
            continue
        findings.append(
            _classify(
                context,
                driver_id=f"drv_symptom_{observation.canonical_id}",
                cause_id=context.subject_id,
                category=DriverCategory.QUALITY_OR_DEFECT,
                role=DriverRole.SYMPTOM,
                summary_detail="was observed on the subject as an effect",
                # A symptom carries no lineage path: it is an observation
                # attached to the subject, not a claim that something acts on
                # it through the graph. Handing it every path that touched
                # the subject would dress an effect up with a causal chain it
                # never had.
                paths=(),
                support=_observation_support(context, observation.canonical_id),
            )
        )
    return findings


def _promote(findings: Sequence[DriverFinding]) -> list[DriverFinding]:
    """Give principal standing to at most one finding, and only if earned.

    Principal requires everything the frozen contract requires — driver
    role, a supporting path, supporting evidence, current relevance — and
    one thing it does not: that the candidate be *unambiguously* the
    strongest. Two equally-supported blockers make "the principal driver" a
    coin toss, and a coin toss presented as a judgment is worse than
    reporting both as contributing.
    """

    eligible = [
        item
        for item in findings
        if item.standing is DriverStanding.CONTRIBUTING_DRIVER
        and item.role is DriverRole.DRIVER
        and item.path_ids
        and item.evidence_ids
    ]
    if not eligible:
        return list(findings)

    def rank(item: DriverFinding) -> tuple[int, str]:
        # A declared blocker outranks an inferred dependency: the source
        # system said "this is blocking", which is a stronger claim than
        # "these two are connected and one is open".
        primary = 0 if item.category is DriverCategory.EXTERNAL_BLOCKER else 1
        return (primary, item.driver_id)

    ordered = sorted(eligible, key=rank)
    if len(ordered) > 1 and rank(ordered[0])[0] == rank(ordered[1])[0]:
        return list(findings)

    winner = ordered[0].driver_id
    return [
        replace(item, standing=DriverStanding.PRINCIPAL_DRIVER)
        if item.driver_id == winner
        else item
        for item in findings
    ]


def _mark_symptoms_of_drivers(
    findings: Sequence[DriverFinding],
) -> list[DriverFinding]:
    """A symptom stops being a candidate once its cause is on the table.

    Only when an asserted driver exists. Excluding a symptom while nothing
    explains it would delete the one observation a reader had.
    """

    if not any(item.is_asserted for item in findings):
        return list(findings)
    return [
        replace(
            item,
            standing=DriverStanding.EXCLUDED,
            exclusion_reason=DriverExclusionReason.SYMPTOM_OF_ANOTHER_CANDIDATE,
        )
        if item.role is DriverRole.SYMPTOM
        and item.standing is not DriverStanding.EXCLUDED
        else item
        for item in findings
    ]


def discover_drivers(
    readout: InvestigationReadout,
    subject_id: str,
    *,
    as_of: datetime,
    entity_attributes: Mapping[str, Mapping[str, str]] | None = None,
    max_candidates: int = 50,
) -> tuple[tuple[DriverFinding, ...], bool]:
    """Every candidate cause for ``subject_id``, classified and ranked.

    Returns ``(findings, truncated)``. Findings are returned whatever their
    standing, including excluded ones: the packet carries them so a reader
    can see what was considered and rejected.

    ``entity_attributes`` supplies declared status per entity. Defaulted from
    the readout's own entities, so a caller cannot quietly supply a different
    status than the one the traversal read.
    """

    attributes = {
        entity.canonical_id: dict(entity.attributes) for entity in readout.entities
    }
    for canonical_id, extra in (entity_attributes or {}).items():
        attributes.setdefault(canonical_id, {}).update(extra)

    observations = {item.canonical_id: item for item in readout.observations}
    context = _Context(
        subject_id=subject_id,
        as_of=as_of,
        authorized=frozenset(readout.authorized_entity_ids),
        entity_kind={entity.canonical_id: entity.kind for entity in readout.entities},
        declared_status={
            canonical_id: values["declared_status"]
            for canonical_id, values in attributes.items()
            if "declared_status" in values
        },
        observations=observations,
        trusted_observation={
            item.canonical_id: _is_trusted(item) for item in readout.observations
        },
    )

    findings = (
        _blocking_candidates(context, readout)
        + _open_child_candidates(context, readout)
        + _symptom_candidates(context, readout)
    )
    # Deduplicate by driver_id, keeping the first: the same blocker can be
    # reached by several paths, and reporting it twice would make one cause
    # look like two.
    unique: dict[str, DriverFinding] = {}
    for item in findings:
        unique.setdefault(item.driver_id, item)

    ranked = sorted(unique.values(), key=lambda item: item.driver_id)
    truncated = len(ranked) > max_candidates
    ranked = ranked[:max_candidates]
    ranked = _mark_symptoms_of_drivers(_promote(ranked))
    return tuple(ranked), truncated


def source_class_for(finding: DriverFinding) -> SourceClass:
    """The source class a finding's evidence came from. Work graph, always.

    Structural findings rest on relationship records, which the arm ingests
    from the work graph. Stated as a function rather than a constant because
    the measurement commit will make it depend on the finding.
    """

    return SourceClass.WORK_GRAPH
