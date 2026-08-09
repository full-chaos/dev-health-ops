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
   and the count reaches the packet;
3. **budget** — hop depth, node count and path count, each with the
   ``TruncationReason`` the contract requires alongside the flag.

Authorization is applied to **intermediate hops**, not only to returned
entities. A path that merely routes through a restricted entity still
discloses that the entity exists and that it links two things the caller can
see; dropping the whole path is the only correct response, and
:func:`_traverse` therefore never traverses *through* an unauthorized node.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
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
from .projection import GraphProjection
from .vocabulary import GraphEntityKind, GraphObservationKind

__all__ = [
    "DiscoveredEntity",
    "DiscoveredObservation",
    "DiscoveredPath",
    "GraphReader",
    "InvestigationReadout",
    "LiveGraphReader",
    "PathStep",
    "ProjectionGraphReader",
    "NODE_COUNT_QUERY",
    "QUERY_VERSION",
    "READ_ONLY_QUERIES",
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
    truncation_reason: TruncationReason | None = None
    observed_source_classes: frozenset[SourceClass] = field(default_factory=frozenset)

    def entity_by_id(self) -> Mapping[str, DiscoveredEntity]:
        return {entity.canonical_id: entity for entity in self.entities}


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
    ) -> InvestigationReadout: ...


# --------------------------------------------------------------------------
# Shared, backend-independent traversal over an adjacency view
# --------------------------------------------------------------------------


#: One adjacency entry: ``(relationship, other canonical id, direction,
#: source class, observed_at, observation ids)``.
_EdgeTuple = tuple[
    RelationshipType,
    str,
    RelationshipDirection,
    SourceClass,
    datetime,
    tuple[str, ...],
]


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
    edges: Mapping[str, tuple[_EdgeTuple, ...]]
    observations: tuple[DiscoveredObservation, ...]


def _ordered_edges(adjacency: _Adjacency, canonical_id: str) -> tuple[_EdgeTuple, ...]:
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
            key=lambda edge: (edge[0].value, edge[1], edge[2].value),
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
) -> InvestigationReadout:
    hops_allowed = min(max_hops, budgets.max_path_hops)
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
    truncation_reason: TruncationReason | None = None
    entities_truncated = False
    paths_truncated = False

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

    while queue:
        current, steps = queue.popleft()
        if len(steps) >= hops_allowed:
            continue
        for (
            relationship,
            other,
            direction,
            source_class,
            observed_at,
            observation_ids,
        ) in _ordered_edges(adjacency, current):
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
                relationship=relationship,
                direction=direction,
                to_canonical_id=other,
                to_kind=adjacency.entities[other].kind,
                source_class=source_class,
                observed_at=observed_at,
                observation_ids=observation_ids,
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
                # loses no reachable entity.
                continue

            node_outcome = budgets.check_entities(len(reached) + 1)
            if other not in reached and not node_outcome.within_budget:
                entities_truncated = True
                truncation_reason = node_outcome.truncation_reason
                continue
            path_outcome = budgets.check_paths(len(paths) + 1)
            if not path_outcome.within_budget:
                paths_truncated = True
                truncation_reason = path_outcome.truncation_reason
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
        visible_observations.append(
            DiscoveredObservation(
                canonical_id=observation.canonical_id,
                kind=observation.kind,
                title=observation.title,
                source_class=observation.source_class,
                observed_at=observation.observed_at,
                subject_canonical_ids=subjects,
                repository_ids=observation.repository_ids,
                outcome=observation.outcome,
            )
        )

    evidence_outcome = budgets.check_evidence(len(visible_observations))
    if not evidence_outcome.within_budget:
        visible_observations = visible_observations[: budgets.max_evidence_entries]
        truncation_reason = evidence_outcome.truncation_reason
        entities_truncated = entities_truncated or False
        paths_truncated = paths_truncated or True

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
        truncation_reason=truncation_reason,
        observed_source_classes=frozenset(observed_classes),
    )


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
        )
        by_uuid[node.uuid] = node.canonical_id

    edges: dict[
        str,
        list[
            tuple[
                RelationshipType,
                str,
                RelationshipDirection,
                SourceClass,
                datetime,
                tuple[str, ...],
            ]
        ],
    ] = {}
    for edge in projection.edges:
        edges.setdefault(edge.source_canonical_id, []).append(
            (
                edge.relationship,
                edge.target_canonical_id,
                RelationshipDirection.FORWARD,
                edge.source_class,
                edge.observed_at,
                edge.observation_ids,
            )
        )
        edges.setdefault(edge.target_canonical_id, []).append(
            (
                edge.relationship,
                edge.source_canonical_id,
                RelationshipDirection.REVERSE,
                edge.source_class,
                edge.observed_at,
                edge.observation_ids,
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
        )


_ENTITY_QUERY = """
MATCH (n:Entity)
WHERE n.group_id = $partition AND n.cf_is_entity = true
RETURN n.cf_canonical_id AS canonical_id,
       n.cf_entity_kind AS entity_kind,
       n.name AS display_label,
       n.cf_source_class AS source_class,
       n.cf_observed_at AS observed_at
"""

_OBSERVATION_QUERY = """
MATCH (n:Entity)
WHERE n.group_id = $partition AND n.cf_is_entity = false
RETURN n.cf_canonical_id AS canonical_id,
       n.cf_observation_kind AS observation_kind,
       n.name AS title,
       n.cf_source_class AS source_class,
       n.cf_observed_at AS observed_at,
       n.cf_repository_ids AS repository_ids,
       n.outcome AS outcome
"""

_EDGE_QUERY = """
MATCH (s:Entity)-[e:RELATES_TO]->(t:Entity)
WHERE e.group_id = $partition
RETURN e.fact AS fact,
       e.cf_source_class AS source_class,
       e.created_at AS observed_at,
       e.cf_observation_ids AS observation_ids
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
            entities[record["canonical_id"]] = DiscoveredEntity(
                canonical_id=record["canonical_id"],
                kind=GraphEntityKind(record["entity_kind"]),
                display_label=record["display_label"],
                source_class=SourceClass(record["source_class"]),
                observed_at=datetime.fromisoformat(record["observed_at"]),
            )

        edges: dict[
            str,
            list[
                tuple[
                    RelationshipType,
                    str,
                    RelationshipDirection,
                    SourceClass,
                    datetime,
                    tuple[str, ...],
                ]
            ],
        ] = {}
        for record in await _rows(driver, _EDGE_QUERY, partition=partition):
            source_id, relationship, target_id = parse_triple_fact(record["fact"])
            observed_at = _as_datetime(record["observed_at"])
            source_class = SourceClass(record["source_class"])
            observation_ids = tuple(
                item for item in (record["observation_ids"] or "").split(",") if item
            )
            edges.setdefault(source_id, []).append(
                (
                    relationship,
                    target_id,
                    RelationshipDirection.FORWARD,
                    source_class,
                    observed_at,
                    observation_ids,
                )
            )
            edges.setdefault(target_id, []).append(
                (
                    relationship,
                    source_id,
                    RelationshipDirection.REVERSE,
                    source_class,
                    observed_at,
                    observation_ids,
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
        )


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
