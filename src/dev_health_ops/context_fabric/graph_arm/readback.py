"""CHAOS-3617: bounded, authorized read-back from the trial graph.

The read side answers one question — *what is connected to these seed
subjects, and by what* — and it answers it twice, on purpose.

:class:`ProjectionGraphReader` walks an in-memory :class:`GraphProjection`.
:class:`LiveGraphReader` walks the FalkorDB partition with Cypher. They are
two implementations of the same traversal, which is exactly the situation
that needs a **differential oracle** rather than a type checker: nothing but
running both over the same world and comparing can tell you whether the
Cypher agrees with the reference.
``tests/context_fabric/test_chaos_3617_live_store.py`` does that,
and it is the reason the in-memory reader exists at all — it is not a mock,
it is the oracle the live reader is measured against.

Three bounds are applied on every read, in this order, and each one is
disclosed rather than silently applied:

1. **partition** — re-derived from the server-known org id and asserted, so
   a partition carried alongside a result set is never trusted;
2. **authorization** — the caller supplies the set of canonical entity ids
   the user may see, and every reached entity, every traversed endpoint and
   every observation subject is filtered against it. Filtering is *counted*,
   and the count reaches the packet.

   **This set is caller-declared and the arm does not verify it.** Stated
   here at the boundary, not buried in a residual note, because adversarial
   review was right that it is the security boundary: a caller that includes
   a restricted entity receives it, and every downstream check --
   ``RelatedContext``'s own guard, the packet builder's re-check -- only
   proves the packet is internally consistent with a claim nobody validated.
   :func:`derive_authorized_entity_ids` is where the real derivation belongs
   and it deliberately raises today rather than existing as a stub that
   looks implemented. Until it lands, correctness of the supplied set is
   scored externally by CHAOS-3616's authorization oracle, which knows the
   true per-principal grants;
3. **budget** — hop depth, node count and path count, each with the
   ``TruncationReason`` the contract requires alongside the flag.

Authorization is applied to **intermediate hops**, not only to returned
entities. A path that merely routes through a restricted entity still
discloses that the entity exists and that it links two things the caller can
see; dropping the whole path is the only correct response, and
:func:`_traverse` therefore never traverses *through* an unauthorized node.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Protocol

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    RelationshipDirection,
    RelationshipType,
    TruncationReason,
)

from .backend import parse_triple_fact
from .budgets import DEFAULT_BUDGETS, TrialBudgets
from .identity import assert_partition_matches_org
from .projection import READBACK_ATTRIBUTE_KEYS, GraphProjection
from .vocabulary import GraphEntityKind, GraphObservationKind

__all__ = [
    "AuthorizationDerivationNotImplementedError",
    "DiscoveredEntity",
    "DiscoveredObservation",
    "DiscoveredPath",
    "GraphReader",
    "InvestigationReadout",
    "LiveGraphReader",
    "MixedProjectionProvenanceError",
    "PathStep",
    "ProjectionGraphReader",
    "NODE_COUNT_QUERY",
    "QUERY_VERSION",
    "READ_ONLY_QUERIES",
    "derive_authorized_entity_ids",
]

#: Emitted on the packet as ``versions.query_version``. Bump when the
#: traversal's shape changes, so a recorded run is tied to the traversal that
#: produced it. Must satisfy ``PlatformVersionToken``.
QUERY_VERSION = "graph_arm_neighbourhood.v1"


@dataclass(frozen=True, slots=True)
class DiscoveredEntity:
    """A canonical entity the traversal reached."""

    canonical_id: str
    kind: GraphEntityKind
    display_label: str
    source_class: SourceClass
    observed_at: datetime
    alias_values: tuple[str, ...] = ()
    #: The arm's own structured attributes, restricted to
    #: :data:`~.projection.READBACK_ATTRIBUTE_KEYS`. Values are strings on
    #: the way out of both readers, because FalkorDB does not preserve the
    #: Python type and a reader that returned ``True`` where the other
    #: returned ``"True"`` would fail the differential oracle for a reason
    #: that has nothing to do with the graph.
    attributes: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DiscoveredObservation:
    """An observation attached to entities the traversal reached."""

    canonical_id: str
    kind: GraphObservationKind
    title: str
    source_class: SourceClass
    observed_at: datetime
    subject_canonical_ids: tuple[str, ...] = ()
    repository_ids: tuple[str, ...] = ()
    outcome: str | None = None
    #: As on :class:`DiscoveredEntity`. This is where an observation's trust
    #: level travels, which is what lets a caller tell a canonical record
    #: from an untrusted note asserting the same thing.
    attributes: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PathStep:
    """One traversed edge, with traversal order separated from orientation.

    ``from_canonical_id`` / ``to_canonical_id`` are traversal order — where
    the walk came from and where it arrived. ``direction`` says whether that
    followed the relationship's canonical orientation or ran against it.
    Keeping the two apart is what makes a reversed relationship detectable;
    collapsing them would make "team owns project" and "project owned by
    team" the same bytes.
    """

    from_canonical_id: str
    from_kind: GraphEntityKind
    relationship: RelationshipType
    direction: RelationshipDirection
    to_canonical_id: str
    to_kind: GraphEntityKind
    source_class: SourceClass
    observed_at: datetime
    observation_ids: tuple[str, ...] = ()
    #: The relationship's observed validity interval, carried through so a
    #: caller can tell a dependency that *is* there from one that *was*.
    #: ``None``/``None`` means the source asserted no interval, which is not
    #: the same as "current" and must not be read as it.
    valid_from: datetime | None = None
    valid_to: datetime | None = None

    def is_current_at(self, moment: datetime) -> bool:
        """Whether this edge was in force at ``moment``.

        An absent interval counts as in force: most providers assert no
        interval at all, and treating silence as "expired" would erase the
        majority of a real graph. What must never happen is the reverse —
        an interval that closed before the window being read as current,
        which is a historical cause presented as a live one.
        """

        if self.valid_from is not None and moment < self.valid_from:
            return False
        return not (self.valid_to is not None and moment >= self.valid_to)


@dataclass(frozen=True, slots=True)
class DiscoveredPath:
    """A connected chain of steps from a seed subject to a reached entity."""

    path_id: str
    origin_canonical_id: str
    terminal_canonical_id: str
    steps: tuple[PathStep, ...]

    def touched_ids(self) -> frozenset[str]:
        touched = {self.origin_canonical_id, self.terminal_canonical_id}
        for step in self.steps:
            touched.add(step.from_canonical_id)
            touched.add(step.to_canonical_id)
        return frozenset(touched)


@dataclass(frozen=True, slots=True)
class InvestigationReadout:
    """Everything one bounded, authorized traversal produced.

    Backend-neutral by construction: nothing here names Graphiti, FalkorDB,
    a node uuid or a Cypher fragment. Both readers return this shape, which
    is what makes the differential comparison meaningful — a difference in
    this value is a difference in *behaviour*, not in representation.
    """

    org_id: str
    partition: str
    seed_canonical_ids: tuple[str, ...]
    entities: tuple[DiscoveredEntity, ...] = ()
    paths: tuple[DiscoveredPath, ...] = ()
    observations: tuple[DiscoveredObservation, ...] = ()
    authorized_entity_ids: tuple[str, ...] = ()
    authorization_filtered_count: int = 0
    entities_truncated: bool = False
    paths_truncated: bool = False
    #: Evidence loss gets its OWN flag. Adversarial review found the evidence
    #: budget setting ``paths_truncated`` instead, which the packet then
    #: disclosed as path truncation while hardcoding evidence coverage as
    #: complete -- a consumer reading the evidence section saw nothing wrong.
    evidence_truncated: bool = False
    #: One reason PER FLAG. A single shared field misattributed the cause
    #: whenever two bounds fired in the same read: a path truncation followed
    #: by an evidence truncation reported both as ``evidence_budget``, so the
    #: packet told a consumer the wrong thing about why lineage was partial.
    entities_truncation_reason: TruncationReason | None = None
    paths_truncation_reason: TruncationReason | None = None
    evidence_truncation_reason: TruncationReason | None = None

    @property
    def truncation_reason(self) -> TruncationReason | None:
        """The first reason any bound fired, for callers wanting one value.

        Ordered entities -> paths -> evidence, which is traversal order. It
        is a convenience over the three authoritative fields, never a
        substitute: a consumer that needs to know why *evidence* is partial
        must read ``evidence_truncation_reason``.
        """

        return (
            self.entities_truncation_reason
            or self.paths_truncation_reason
            or self.evidence_truncation_reason
        )

    observed_source_classes: frozenset[SourceClass] = field(default_factory=frozenset)

    #: Whether the reader that produced this readout can say what an
    #: observation is ABOUT. **Declared by the reader**, never inferred from
    #: whether attachments happen to be present.
    #:
    #: The distinction is load-bearing rather than pedantic.
    #: :class:`LiveGraphReader` cannot recover attachment today (see its
    #: docstring), and a rule that skipped the "is this record about the
    #: linkage" check whenever attachments were absent would become a silent
    #: no-op on exactly the reader that cannot perform it -- the original
    #: defect, live-only. Declaring the capability instead makes the gap a
    #: visible, attributable state: ``discover_drivers`` refuses to attribute
    #: on such a readout and says so in the exclusion's own words rather than
    #: reporting the support as withheld, which would be an authorization
    #: claim nothing supports.
    #:
    #: Defaults to ``True`` because a hand-built readout carries whatever
    #: attachments its author wrote; both real readers set it explicitly.
    observation_attachment_available: bool = True

    #: The embedder the STORE records as having produced this partition's
    #: vectors, or ``None`` when nothing attests to one.
    #:
    #: This is the arm's only honest answer to "were these vectors produced
    #: by something semantic". ``build_packet`` takes an embedder argument
    #: that has no connection to whatever wrote the store — different object,
    #: different run, possibly a different model — so a guard that read
    #: ``embedder.semantic`` was asking the caller whether the caller's claim
    #: was true.
    #:
    #: ``None`` on the in-memory reader, and not a gap: an unwritten
    #: projection has no vectors at all, so there is nothing to attest and
    #: nothing that could be searched by similarity.
    embedder_model_id: str | None = None

    def entity_by_id(self) -> Mapping[str, DiscoveredEntity]:
        return {entity.canonical_id: entity for entity in self.entities}


class MixedProjectionProvenanceError(RuntimeError):
    """One partition's vectors were written by more than one embedder."""


class AuthorizationDerivationNotImplementedError(NotImplementedError):
    """The arm cannot yet derive an authorized set; a caller must supply one.

    A named error rather than a silent absence. The alternative -- no
    function at all -- leaves the reader of ``neighbourhood`` to infer from
    a docstring that ``authorized_entity_ids`` is unverified; the alternative
    to *that* -- a stub returning something plausible -- would be worse
    still, because a permissive default is exactly how an unverified scope
    becomes an invisible one.
    """


def derive_authorized_entity_ids(org_id: str, principal_id: str) -> frozenset[str]:
    """Derive the authorized entity set from the server's own grants.

    **Not implemented in this revision, deliberately and loudly.** The
    principal-grant adapter lands with the capability work; until then every
    caller supplies the set and the arm cannot tell a correct one from a
    widened one.

    Kept as a raising function rather than omitted so the gap has a name a
    reviewer can grep for, a place for the real implementation to arrive, and
    a call site that fails rather than degrades if anyone wires it early.
    """

    raise AuthorizationDerivationNotImplementedError(
        f"the graph arm cannot derive the authorized entity set for principal "
        f"{principal_id!r} in org {org_id!r}. Callers supply it today, and the "
        "arm does not verify it -- see the module docstring. The "
        "principal-grant adapter is the capability-revision fix"
    )


class GraphReader(Protocol):
    """The traversal contract both readers implement."""

    async def neighbourhood(
        self,
        *,
        org_id: str,
        seed_canonical_ids: Sequence[str],
        authorized_entity_ids: Sequence[str],
        max_hops: int = 3,
        budgets: TrialBudgets = DEFAULT_BUDGETS,
    ) -> InvestigationReadout:
        """Walk outward from the seeds, bounded and authorized."""


# --------------------------------------------------------------------------
# Shared, backend-independent traversal over an adjacency view
# --------------------------------------------------------------------------


#: One adjacency entry: ``(relationship, other canonical id, direction,
#: source class, observed_at, observation ids)``.
@dataclass(frozen=True, slots=True)
class _Neighbour:
    """One edge as the traversal sees it, from either reader.

    Was a six-tuple. Named because driver work needs the validity interval
    too, and a positional unpack that grows silently is how a field ends up
    read from the wrong slot in one reader and the right one in the other —
    precisely the class of divergence the differential oracle exists to
    catch, made harder to catch by the shape of the code.
    """

    relationship: RelationshipType
    other_canonical_id: str
    direction: RelationshipDirection
    source_class: SourceClass
    observed_at: datetime
    observation_ids: tuple[str, ...] = ()
    #: The edge's observed validity interval. ``valid_to`` in the past is
    #: what distinguishes a dependency that *was* there from one that is,
    #: and a driver built on the former is a historical cause presented as a
    #: current one.
    valid_from: datetime | None = None
    valid_to: datetime | None = None


@dataclass(frozen=True, slots=True)
class _Adjacency:
    """The minimum a traversal needs, independent of where it came from.

    Both readers materialise this and then run :func:`_traverse`, so the
    *traversal* is genuinely one implementation and the differential test is
    comparing two **fetch** strategies rather than two search algorithms.
    That is the honest scope of what the differential proves, and it is
    stated here so nobody reads more into a green run than it earns.
    """

    entities: Mapping[str, DiscoveredEntity]
    #: canonical id -> every edge touching it, in either direction.
    edges: Mapping[str, tuple[_Neighbour, ...]]
    observations: tuple[DiscoveredObservation, ...]


def _ordered_edges(adjacency: _Adjacency, canonical_id: str) -> tuple[_Neighbour, ...]:
    """One entity's edges in a total, backend-independent order.

    Found by the differential oracle, not by review: the in-memory reader
    walked edges in projection order and the Cypher reader in whatever order
    FalkorDB returned rows, so when more edges reached an entity than
    ``max_paths_per_entity`` allows, the two kept *different* paths. Same
    entities, same counts, different explanations — the kind of divergence
    that makes a recorded trial run irreproducible and that no type checker
    or code index could have surfaced.

    Sorting on ``(relationship, other id, direction)`` makes the retained
    set a function of the graph alone. It is deliberately not sorted on
    ``observed_at``: a timestamp tie would put the order back at the mercy
    of row order.
    """

    return tuple(
        sorted(
            adjacency.edges.get(canonical_id, ()),
            key=lambda edge: (
                edge.relationship.value,
                edge.other_canonical_id,
                edge.direction.value,
            ),
        )
    )


def _traverse(
    *,
    org_id: str,
    partition: str,
    adjacency: _Adjacency,
    seed_canonical_ids: Sequence[str],
    authorized: frozenset[str],
    max_hops: int,
    budgets: TrialBudgets,
    observation_attachment_available: bool,
    embedder_model_id: str | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> InvestigationReadout:
    # Whichever of the two numbers is smaller becomes the ceiling, and the
    # disclosure does not depend on which one won: see
    # ``declined_with_edges_remaining`` below. An earlier fix only disclosed
    # when the BUDGET undercut the caller, which left the default read path
    # (caller default 3, budget cap 6) silently truncating -- verification
    # reproduced 4 of 6 reachable authorized entities returned with every flag
    # False.
    hops_allowed = min(max_hops, budgets.max_path_hops)
    started = clock()
    # Distinct entities refused, not refusal *events*: the same restricted
    # neighbour is reached once per path that touches its neighbour, and
    # counting attempts would make the disclosed number depend on graph
    # density rather than on how much was withheld.
    filtered: set[str] = set()

    known = set(adjacency.entities)
    reachable_seeds = [
        seed for seed in seed_canonical_ids if seed in known and seed in authorized
    ]
    filtered.update(
        seed for seed in seed_canonical_ids if seed in known and seed not in authorized
    )

    reached: dict[str, DiscoveredEntity] = {}
    paths: list[DiscoveredPath] = []
    entities_reason: TruncationReason | None = None
    paths_reason: TruncationReason | None = None
    evidence_reason: TruncationReason | None = None
    entities_truncated = False
    paths_truncated = False
    evidence_truncated = False
    # N2: whether the walk ever stopped at the hop ceiling while the prefix it
    # stopped at still had edges it had not followed. That is the honest
    # trigger -- not "was the ceiling the budget's or the caller's", because a
    # caller who passed no depth got a library default nobody chose, and a
    # result missing reachable authorized entities must not read complete
    # whichever number produced the ceiling.
    declined_with_edges_remaining = False
    #: Narrower: at least one entity beyond the ceiling was never reached by
    #: ANY branch. Only this justifies the entity flag.
    declined_leaving_entities_unreached = False

    for seed in reachable_seeds:
        reached[seed] = adjacency.entities[seed]

    queue: deque[tuple[str, tuple[PathStep, ...]]] = deque(
        (seed, ()) for seed in reachable_seeds
    )
    seen_paths: set[tuple[str, ...]] = set()
    # Breadth-first, so the first paths recorded for a terminal entity are
    # its shortest. Keeping only the first ``max_paths_per_entity`` of them
    # is what stops a dense neighbourhood producing dozens of near-identical
    # chains that bury the explanatory ones.
    paths_per_terminal: dict[str, int] = {}

    # Every dequeued prefix is a unit of work. Bounding *entities* is not
    # enough: this traversal enumerates simple paths, so a dense
    # neighbourhood can expand for a very long time while reaching no new
    # entity at all. ``max_nodes_visited`` is therefore counted here, on the
    # work actually done, and ``max_wall_seconds`` backstops it for the
    # shapes a count cannot predict.
    visited = 0

    while queue:
        current, steps = queue.popleft()
        visited += 1
        work_outcome = budgets.check_nodes(visited)
        if not work_outcome.within_budget:
            entities_truncated = True
            entities_reason = work_outcome.truncation_reason
            break
        elapsed_outcome = budgets.check_elapsed(clock() - started)
        if not elapsed_outcome.within_budget:
            entities_truncated = True
            entities_reason = elapsed_outcome.truncation_reason
            break
        if len(steps) >= hops_allowed:
            # Stopped at the ceiling. Only a disclosure if this prefix still
            # had somewhere to go: a walk that ran out of graph is complete,
            # and flagging it would make the flag meaningless.
            # Only an UNREACHED neighbour means the entity set is short. In a
            # diamond, every entity beyond the ceiling is already reached by
            # another branch, so flagging on any unfollowed edge claimed
            # entities were missing when none were -- and a flag that
            # over-reports is read as noise exactly as fast as one that
            # under-reports.
            unfollowed = [
                other
                for other in (
                    edge.other_canonical_id
                    for edge in _ordered_edges(adjacency, current)
                )
                if other in adjacency.entities
                and other in authorized
                and other != current
                and all(step.from_canonical_id != other for step in steps)
            ]
            if unfollowed:
                # Paths are short either way: an unfollowed edge is an
                # explanation not returned.
                declined_with_edges_remaining = True
                if any(other not in reached for other in unfollowed):
                    declined_leaving_entities_unreached = True
            continue
        for neighbour in _ordered_edges(adjacency, current):
            other = neighbour.other_canonical_id
            if other not in adjacency.entities:
                continue
            if other not in authorized:
                # Never traverse *through* an unauthorized entity: routing a
                # path through it leaks its existence even when its own
                # record is never returned.
                filtered.add(other)
                continue
            if (
                any(step.from_canonical_id == other for step in steps)
                or other == current
            ):
                continue
            step = PathStep(
                from_canonical_id=current,
                from_kind=adjacency.entities[current].kind,
                relationship=neighbour.relationship,
                direction=neighbour.direction,
                to_canonical_id=other,
                to_kind=adjacency.entities[other].kind,
                source_class=neighbour.source_class,
                observed_at=neighbour.observed_at,
                observation_ids=neighbour.observation_ids,
                valid_from=neighbour.valid_from,
                valid_to=neighbour.valid_to,
            )
            extended = (*steps, step)
            signature = tuple(
                f"{item.from_canonical_id}|{item.relationship.value}|"
                f"{item.direction.value}|{item.to_canonical_id}"
                for item in extended
            )
            if signature in seen_paths:
                continue
            seen_paths.add(signature)

            if paths_per_terminal.get(other, 0) >= budgets.max_paths_per_entity:
                # ``other`` is already reached and already has its quota of
                # shortest paths; continuations from those kept paths cover
                # everything beyond it, so dropping this redundant prefix
                # loses no reachable *entity* -- but it does drop an
                # explanation, and adversarial review was right that dropping
                # an explanation silently is the same fault as dropping a
                # result silently. It discloses.
                paths_truncated = True
                paths_reason = TruncationReason.PATH_BUDGET
                continue

            node_outcome = budgets.check_entities(len(reached) + 1)
            if other not in reached and not node_outcome.within_budget:
                entities_truncated = True
                entities_reason = node_outcome.truncation_reason
                continue
            path_outcome = budgets.check_paths(len(paths) + 1)
            if not path_outcome.within_budget:
                paths_truncated = True
                paths_reason = path_outcome.truncation_reason
                queue.clear()
                break

            reached[other] = adjacency.entities[other]
            paths_per_terminal[other] = paths_per_terminal.get(other, 0) + 1
            paths.append(
                DiscoveredPath(
                    path_id=f"p{len(paths) + 1:04d}",
                    origin_canonical_id=extended[0].from_canonical_id,
                    terminal_canonical_id=other,
                    steps=extended,
                )
            )
            queue.append((other, extended))

    visible_observations: list[DiscoveredObservation] = []
    for observation in adjacency.observations:
        subjects = tuple(
            subject
            for subject in observation.subject_canonical_ids
            if subject in reached
        )
        filtered.update(
            subject
            for subject in observation.subject_canonical_ids
            if subject not in reached and subject not in authorized
        )
        if not subjects:
            continue
        # ``replace`` rather than a field-by-field rebuild. The rebuild
        # silently dropped ``attributes`` the moment the readout grew them,
        # and because BOTH readers share this function the differential
        # oracle could not see it: two readers agreeing on a value neither
        # of them carries is still agreement. Found by printing real corpus
        # output, which is the only thing that would have found it.
        visible_observations.append(
            replace(observation, subject_canonical_ids=subjects)
        )

    evidence_outcome = budgets.check_evidence(len(visible_observations))
    if not evidence_outcome.within_budget:
        visible_observations = visible_observations[: budgets.max_evidence_entries]
        evidence_reason = evidence_outcome.truncation_reason
        evidence_truncated = True

    if declined_with_edges_remaining:
        # An unfollowed edge is always an explanation not returned.
        paths_truncated = True
        paths_reason = paths_reason or TruncationReason.PATH_BUDGET
    if declined_leaving_entities_unreached:
        # ...but only an entity no branch reached makes the ENTITY set partial.
        entities_truncated = True
        entities_reason = entities_reason or TruncationReason.PATH_BUDGET

    observed_classes = (
        {entity.source_class for entity in reached.values()}
        | {step.source_class for path in paths for step in path.steps}
        | {observation.source_class for observation in visible_observations}
    )

    return InvestigationReadout(
        org_id=org_id,
        partition=partition,
        seed_canonical_ids=tuple(seed_canonical_ids),
        entities=tuple(sorted(reached.values(), key=lambda item: item.canonical_id)),
        paths=tuple(paths),
        observations=tuple(
            sorted(visible_observations, key=lambda item: item.canonical_id)
        ),
        authorized_entity_ids=tuple(sorted(authorized)),
        authorization_filtered_count=len(filtered),
        entities_truncated=entities_truncated,
        paths_truncated=paths_truncated,
        evidence_truncated=evidence_truncated,
        entities_truncation_reason=entities_reason,
        paths_truncation_reason=paths_reason,
        evidence_truncation_reason=evidence_reason,
        observed_source_classes=frozenset(observed_classes),
        observation_attachment_available=observation_attachment_available,
        embedder_model_id=embedder_model_id,
    )


def _readback_attributes(attributes: Mapping[str, object]) -> dict[str, str]:
    """The declared subset of a node's attributes, stringified.

    Both readers go through this, so "what the in-memory reader returns" and
    "what the Cypher reader returns" cannot drift into different type
    conventions — the divergence would be real and would look like a graph
    difference rather than a marshalling one.
    """

    return {
        key: str(attributes[key])
        for key in READBACK_ATTRIBUTE_KEYS
        if attributes.get(key) is not None
    }


def _adjacency_from_projection(projection: GraphProjection) -> _Adjacency:
    entities: dict[str, DiscoveredEntity] = {}
    by_uuid: dict[str, str] = {}
    for node in projection.nodes:
        if node.entity_kind is None:
            continue
        alias_values = tuple(sorted(alias.value for alias in node.aliases))
        entities[node.canonical_id] = DiscoveredEntity(
            canonical_id=node.canonical_id,
            kind=node.entity_kind,
            display_label=node.display_label,
            source_class=node.source_class,
            observed_at=node.observed_at,
            alias_values=alias_values,
            attributes=_readback_attributes(node.attributes),
        )
        by_uuid[node.uuid] = node.canonical_id

    edges: dict[str, list[_Neighbour]] = {}
    for edge in projection.edges:
        for owner, other, direction in (
            (
                edge.source_canonical_id,
                edge.target_canonical_id,
                RelationshipDirection.FORWARD,
            ),
            (
                edge.target_canonical_id,
                edge.source_canonical_id,
                RelationshipDirection.REVERSE,
            ),
        ):
            edges.setdefault(owner, []).append(
                _Neighbour(
                    relationship=edge.relationship,
                    other_canonical_id=other,
                    direction=direction,
                    source_class=edge.source_class,
                    observed_at=edge.observed_at,
                    observation_ids=edge.observation_ids,
                    valid_from=edge.valid_from,
                    valid_to=edge.valid_to,
                )
            )

    observations: list[DiscoveredObservation] = []
    for node in projection.nodes:
        if node.observation_kind is None:
            continue
        subject_uuids = projection.observation_attachments.get(node.uuid, ())
        outcome = node.attributes.get("outcome")
        observations.append(
            DiscoveredObservation(
                canonical_id=node.canonical_id,
                kind=node.observation_kind,
                title=node.display_label,
                source_class=node.source_class,
                observed_at=node.observed_at,
                subject_canonical_ids=tuple(
                    by_uuid[uuid] for uuid in subject_uuids if uuid in by_uuid
                ),
                repository_ids=node.repository_ids,
                outcome=str(outcome) if outcome is not None else None,
                attributes=_readback_attributes(node.attributes),
            )
        )

    return _Adjacency(
        entities=entities,
        edges={key: tuple(value) for key, value in edges.items()},
        observations=tuple(observations),
    )


class ProjectionGraphReader:
    """The reference traversal, over an in-memory projection.

    Not a mock. It is the oracle the live Cypher reader is differentially
    compared against, and it is what lets identity, direction, tenant
    isolation and packet parity be verified in the standard unit suite with
    no container running.
    """

    def __init__(self, projection: GraphProjection) -> None:
        self._projection = projection
        self._adjacency = _adjacency_from_projection(projection)

    async def neighbourhood(
        self,
        *,
        org_id: str,
        seed_canonical_ids: Sequence[str],
        authorized_entity_ids: Sequence[str],
        max_hops: int = 3,
        budgets: TrialBudgets = DEFAULT_BUDGETS,
    ) -> InvestigationReadout:
        if self._projection.org_id != org_id:
            raise PermissionError(
                f"projection belongs to organization {self._projection.org_id!r}, "
                f"not {org_id!r}"
            )
        assert_partition_matches_org(self._projection.partition, org_id)
        return _traverse(
            org_id=org_id,
            partition=self._projection.partition,
            adjacency=self._adjacency,
            seed_canonical_ids=seed_canonical_ids,
            authorized=frozenset(authorized_entity_ids),
            max_hops=max_hops,
            budgets=budgets,
            # The projection carries ``observation_attachments`` outright, so
            # this reader can always say what a record is about.
            observation_attachment_available=True,
        )


#: Aliases are stored one attribute per kind (``cf_alias_alias``,
#: ``cf_alias_acronym``, ...), values joined by US (0x1f). They are read back
#: here because the differential oracle -- once it compared the WHOLE readout
#: rather than a hand-picked subset -- showed the live reader returning no
#: aliases at all while the reference returned them. That gap would have
#: surfaced later as PR2's alias/acronym search finding nothing in the live
#: store and everything in the reference.
#: Every Cypher statement below is a plain literal, never an f-string.
#:
#: Building them by interpolation was tried and reverted inside one commit.
#: The containment guard reads the arm's Cypher surface out of the AST's
#: string *constants*, so an f-string query is not statically comparable and
#: the guard silently stops being able to see it — the query surface would
#: still be small and read-only, and nothing would be checking that any more.
#:
#: The cost is that the attribute columns are typed out twice, here and in
#: :data:`~.projection.READBACK_ATTRIBUTE_KEYS`. That drift is caught by
#: ``test_every_declared_attribute_has_a_column_in_both_queries``, which
#: fails when a key is declared without a column or a column without a key.

_ENTITY_QUERY = """
MATCH (n:Entity)
WHERE n.group_id = $partition AND n.cf_is_entity = true
RETURN n.cf_canonical_id AS canonical_id,
       n.cf_entity_kind AS entity_kind,
       n.name AS display_label,
       n.cf_source_class AS source_class,
       n.cf_observed_at AS observed_at,
       n.cf_alias_alias AS alias_alias,
       n.cf_alias_acronym AS alias_acronym,
       n.cf_alias_previous_name AS alias_previous_name,
       n.cf_alias_provider_identifier AS alias_provider_identifier,
       n.cf_attr_corpus_is_adversarial AS attr_corpus_is_adversarial,
       n.cf_attr_corpus_state AS attr_corpus_state,
       n.cf_attr_corpus_trust AS attr_corpus_trust,
       n.cf_attr_declared_status AS attr_declared_status,
       n.cf_attr_measurement_basis AS attr_measurement_basis,
       n.cf_attr_measurement_cohort_median AS attr_measurement_cohort_median,
       n.cf_attr_measurement_evidence_slug AS attr_measurement_evidence_slug,
       n.cf_attr_measurement_metric AS attr_measurement_metric,
       n.cf_attr_measurement_unit AS attr_measurement_unit,
       n.cf_attr_measurement_value AS attr_measurement_value,
       n.cf_attr_superseded_by AS attr_superseded_by
"""

#: The separator :func:`~.backend.to_graphiti_nodes` joins alias values with.
_ALIAS_SEPARATOR = "\x1f"
_ALIAS_COLUMNS = (
    "alias_alias",
    "alias_acronym",
    "alias_previous_name",
    "alias_provider_identifier",
)

_OBSERVATION_QUERY = """
MATCH (n:Entity)
WHERE n.group_id = $partition AND n.cf_is_entity = false
RETURN n.cf_canonical_id AS canonical_id,
       n.cf_observation_kind AS observation_kind,
       n.name AS title,
       n.cf_source_class AS source_class,
       n.cf_observed_at AS observed_at,
       n.cf_repository_ids AS repository_ids,
       n.outcome AS outcome,
       n.cf_attr_corpus_is_adversarial AS attr_corpus_is_adversarial,
       n.cf_attr_corpus_state AS attr_corpus_state,
       n.cf_attr_corpus_trust AS attr_corpus_trust,
       n.cf_attr_declared_status AS attr_declared_status,
       n.cf_attr_measurement_basis AS attr_measurement_basis,
       n.cf_attr_measurement_cohort_median AS attr_measurement_cohort_median,
       n.cf_attr_measurement_evidence_slug AS attr_measurement_evidence_slug,
       n.cf_attr_measurement_metric AS attr_measurement_metric,
       n.cf_attr_measurement_unit AS attr_measurement_unit,
       n.cf_attr_measurement_value AS attr_measurement_value,
       n.cf_attr_superseded_by AS attr_superseded_by
"""

_EDGE_QUERY = """
MATCH (s:Entity)-[e:RELATES_TO]->(t:Entity)
WHERE e.group_id = $partition
RETURN e.fact AS fact,
       e.cf_source_class AS source_class,
       e.created_at AS observed_at,
       e.cf_observation_ids AS observation_ids,
       e.valid_at AS valid_from,
       e.invalid_at AS valid_to
"""

#: What the STORE says produced this partition's vectors.
#:
#: Distinct rather than "any one node": a partition written twice with
#: different embedders holds a mixture, and the difference between "one
#: attested model" and "two" is the difference between a comparable
#: projection and an incomparable one.
_PROJECTION_EMBEDDER_QUERY = """
MATCH (n:Entity)
WHERE n.group_id = $partition
RETURN DISTINCT n.cf_projection_embedder AS embedder_model_id
"""


#: Every Cypher statement the arm can issue, in one place.
#:
#: There is deliberately no "run this query" helper: a generic executor is
#: the shape in which arbitrary traversal, ontology mutation or maintenance
#: reaches a caller. These three constants are the entire graph-query
#: surface, they are read-only, and
#: ``tests/context_fabric/test_chaos_3617_containment.py`` asserts both --
#: that this tuple is exhaustive over the module's Cypher, and that none of
#: its members contains a write or maintenance clause.
NODE_COUNT_QUERY = "MATCH (n) RETURN count(n) AS total"

READ_ONLY_QUERIES: tuple[str, ...] = (
    _ENTITY_QUERY,
    _OBSERVATION_QUERY,
    _EDGE_QUERY,
    _PROJECTION_EMBEDDER_QUERY,
    NODE_COUNT_QUERY,
)


class LiveGraphReader:
    """The FalkorDB traversal. Measured against :class:`ProjectionGraphReader`.

    Fetches the partition's entities, observations and edges and then runs
    the *same* :func:`_traverse`. The edges are rebuilt from their stored
    ``fact`` via :func:`~.backend.parse_triple_fact`, which is deliberate:
    round-tripping through the triple rendering means a stored fact
    containing prose is detected on read rather than silently presented as
    evidence.

    Observation-to-entity attachment is not yet read back from the store —
    ``add_nodes_and_edges_bulk`` writes entity edges only — so this reader
    reports observations with an empty subject list and any packet built
    from it declares its evidence coverage accordingly. Stated here rather
    than papered over: the differential test asserts entity and path
    equality and explicitly records that observation attachment is out of
    its scope in this revision.
    """

    def __init__(self, store: Any) -> None:
        self._store = store

    async def neighbourhood(
        self,
        *,
        org_id: str,
        seed_canonical_ids: Sequence[str],
        authorized_entity_ids: Sequence[str],
        max_hops: int = 3,
        budgets: TrialBudgets = DEFAULT_BUDGETS,
    ) -> InvestigationReadout:
        partition: str = self._store.partition
        assert_partition_matches_org(partition, org_id)
        driver = self._store._driver

        entities: dict[str, DiscoveredEntity] = {}
        for record in await _rows(driver, _ENTITY_QUERY, partition=partition):
            aliases: list[str] = []
            for column in _ALIAS_COLUMNS:
                raw = record.get(column)
                if raw:
                    aliases.extend(
                        item for item in str(raw).split(_ALIAS_SEPARATOR) if item
                    )
            entities[record["canonical_id"]] = DiscoveredEntity(
                canonical_id=record["canonical_id"],
                kind=GraphEntityKind(record["entity_kind"]),
                display_label=record["display_label"],
                source_class=SourceClass(record["source_class"]),
                observed_at=datetime.fromisoformat(record["observed_at"]),
                alias_values=tuple(sorted(aliases)),
                attributes=_attributes_from_row(record),
            )

        edges: dict[str, list[_Neighbour]] = {}
        for record in await _rows(driver, _EDGE_QUERY, partition=partition):
            source_id, relationship, target_id = parse_triple_fact(record["fact"])
            observed_at = _as_datetime(record["observed_at"])
            source_class = SourceClass(record["source_class"])
            observation_ids = tuple(
                item for item in (record["observation_ids"] or "").split(",") if item
            )
            valid_from = _as_optional_datetime(record.get("valid_from"))
            valid_to = _as_optional_datetime(record.get("valid_to"))
            for owner, other, direction in (
                (source_id, target_id, RelationshipDirection.FORWARD),
                (target_id, source_id, RelationshipDirection.REVERSE),
            ):
                edges.setdefault(owner, []).append(
                    _Neighbour(
                        relationship=relationship,
                        other_canonical_id=other,
                        direction=direction,
                        source_class=source_class,
                        observed_at=observed_at,
                        observation_ids=observation_ids,
                        valid_from=valid_from,
                        valid_to=valid_to,
                    )
                )

        observations: list[DiscoveredObservation] = []
        for record in await _rows(driver, _OBSERVATION_QUERY, partition=partition):
            observations.append(
                DiscoveredObservation(
                    canonical_id=record["canonical_id"],
                    kind=GraphObservationKind(record["observation_kind"]),
                    title=record["title"],
                    source_class=SourceClass(record["source_class"]),
                    observed_at=_as_datetime(record["observed_at"]),
                    subject_canonical_ids=(),
                    repository_ids=tuple(
                        item
                        for item in (record["repository_ids"] or "").split(",")
                        if item
                    ),
                    outcome=record["outcome"],
                    attributes=_attributes_from_row(record),
                )
            )

        adjacency = _Adjacency(
            entities=entities,
            edges={key: tuple(value) for key, value in edges.items()},
            observations=tuple(observations),
        )
        return _traverse(
            org_id=org_id,
            partition=partition,
            adjacency=adjacency,
            seed_canonical_ids=seed_canonical_ids,
            authorized=frozenset(authorized_entity_ids),
            max_hops=max_hops,
            budgets=budgets,
            # ``add_nodes_and_edges_bulk`` writes entity edges only, so this
            # reader cannot recover which entities an observation was about.
            # Declared rather than left to be inferred from the empty subject
            # lists below: a consumer that inferred it would have to guess,
            # and ``discover_drivers`` would silently stop checking exactly
            # the thing it cannot check here.
            observation_attachment_available=False,
            embedder_model_id=await _attested_embedder(driver, partition),
        )


async def _attested_embedder(driver: Any, partition: str) -> str | None:
    """What the partition itself records as having produced its vectors.

    Returns ``None`` when nothing is recorded — a partition written before
    the attestation existed, which is an honest "cannot say" rather than a
    permission. Every semantic claim is refused on such a readout, which is
    the safe direction.

    Raises when the partition records **more than one** embedder. A partition
    whose vectors came from two models is not one projection, and every
    packet built from it would be stamped with whichever model won the read;
    that is precisely the "two incomparable runs look comparable" failure the
    projection version exists to prevent, arriving inside a single store.
    """

    attested = {
        str(record["embedder_model_id"])
        for record in await _rows(
            driver, _PROJECTION_EMBEDDER_QUERY, partition=partition
        )
        if record.get("embedder_model_id") is not None
    }
    if not attested:
        return None
    if len(attested) > 1:
        raise MixedProjectionProvenanceError(
            f"partition {partition!r} records {sorted(attested)} as having "
            "produced its vectors. A partition written by two embedders holds "
            "a mixture, so no packet built from it can name the projection it "
            "came from; re-project the organization from a single run"
        )
    return attested.pop()


async def _rows(driver: Any, query: str, **params: object) -> list[dict[str, Any]]:
    result = await driver.execute_query(query, **params)
    if not result:
        return []
    records, _, _ = result
    return list(records)


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _as_optional_datetime(value: object) -> datetime | None:
    """A validity bound, or ``None`` when the source asserted none.

    Deliberately does NOT substitute a default. "No interval recorded" and
    "in force from the epoch" are different claims, and the second one is
    the arm inventing currency it was never told about.
    """

    if value is None or value == "":
        return None
    return _as_datetime(value)


def _attributes_from_row(record: Mapping[str, object]) -> dict[str, str]:
    """The declared attributes from one Cypher row.

    Mirrors :func:`_readback_attributes` on the in-memory side, including
    its "absent means absent" rule: a property FalkorDB returns as ``None``
    is one the node does not carry, and materialising it as the string
    ``"None"`` would make the two readers disagree about a node they both
    read correctly.
    """

    return {
        key: str(record[f"attr_{key}"])
        for key in READBACK_ATTRIBUTE_KEYS
        if record.get(f"attr_{key}") is not None
    }
