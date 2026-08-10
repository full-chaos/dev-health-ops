"""CHAOS-3617: the isolated trial datastore — lifecycle, writes, deletion.

**Backend choice: FalkorDB.** The alternatives Graphiti 0.29.3 ships drivers
for are Neo4j, FalkorDB, FalkorDB-lite, Kuzu, Neptune and Neo4j+OpenSearch.
Kuzu — the embedded option, and otherwise the most attractive for a trial —
has no wheel for this repository's Python 3.14 and its sdist build fails, so
it is not available here at all. Neptune is a managed AWS service and
Neo4j+OpenSearch is two servers. That leaves FalkorDB, which is also the
better fit on its own merits: one container, and **one graph keyspace per
organization**, which turns org deletion into a single keyspace drop rather
than a traversal that could miss a node. See
``docs/contribute/architecture/graph-investigation-arm.md`` for the full
write-up.

**Partition == keyspace == organization.** :func:`identity.partition_for_org`
derives the partition from the server-known organization id; the FalkorDB
driver is constructed with that partition as its ``database``; Graphiti
stores every node and edge with it as ``group_id``. A caller never supplies
it and could not use it if they did — :meth:`GraphArmStore.for_org` derives
it, and every read re-derives and asserts it via
:func:`identity.assert_partition_matches_org`.

**Deletion is a drop, not a sweep.** :meth:`GraphArmStore.purge_org` deletes
the whole keyspace. There is no per-node deletion path that could leave an
orphan, and :func:`org_deletion_visit` — the callable registered in
``EXTERNAL_DERIVED_STORES`` — is a thin wrapper over it that honours
``dry_run``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .backend import (
    DeterministicEmbedder,
    EmbeddingBackend,
    GraphitiUnavailableError,
    graphiti_module,
    to_graphiti_document_nodes,
    to_graphiti_edges,
    to_graphiti_nodes,
)
from .budgets import DEFAULT_BUDGETS, TrialBudgets
from .flags import (
    TRIAL_STORE_URI_VAR,
    GraphDeadlines,
    TrialStoreConfig,
    graph_deadlines,
    graph_projection_enabled,
    trial_store_config,
)
from .identity import assert_partition_matches_org, partition_for_org
from .projection import GraphProjection
from .readback import NODE_COUNT_QUERY
from .watermark import IndexWatermark

logger = logging.getLogger(__name__)

__all__ = [
    "EmbeddingBudgetExceededError",
    "GraphArmStore",
    "GraphOperationTimeoutError",
    "ProjectionDisabledError",
    "partition_exists_for",
    "StoreUnavailableError",
    "TRIAL_DERIVED_STORE_NAME",
    "org_deletion_visit",
]

#: The name this store registers under in ``EXTERNAL_DERIVED_STORES``.
TRIAL_DERIVED_STORE_NAME = "context_fabric_graph_trial"

#: CHAOS-3679. Prefix for the watermark's Redis key, on the FalkorDB client's
#: OWN connection -- deliberately not a Cypher graph node. A graph node would
#: be counted by ``readback.NODE_COUNT_QUERY``'s unconditional
#: ``MATCH (n) RETURN count(n)``, silently breaking the
#: ``count_nodes() == len(projection.nodes)`` / ``purge_org() returns
#: len(projection.nodes)`` invariants the CHAOS-3617 live suite already
#: asserts. A plain key needs no change to the closed query vocabulary and no
#: change to node counting anywhere.
_WATERMARK_KEY_PREFIX = "context_fabric:graph:watermark:"


def _watermark_key(partition: str) -> str:
    return f"{_WATERMARK_KEY_PREFIX}{partition}"


class StoreUnavailableError(RuntimeError):
    """The trial store is not configured or not reachable."""


class ProjectionDisabledError(RuntimeError):
    """A write was attempted with the projection flag off."""


class EmbeddingBudgetExceededError(RuntimeError):
    """The projection would need more embedding calls than the run allows."""


class GraphOperationTimeoutError(StoreUnavailableError):
    """A live-store operation did not complete within its configured deadline.

    CHAOS-3631. A ``StoreUnavailableError`` subclass, deliberately: every
    caller that already treats "the store is unavailable" as "fall back to
    the existing non-graph Ask Dev path" catches a hung backend for free,
    with no separate except-clause to add and no risk of forgetting one.

    A caller must never treat this as an empty or no-match result -- a
    timeout says nothing about what the graph contains, only that the
    question could not be answered inside the deadline. It is a distinct
    degraded/unavailable outcome, not a quality signal.
    """


async def _await_with_deadline(
    awaitable: Any, *, timeout_s: float, operation: str, detail: str
) -> Any:
    """Await ``awaitable``, bounded by ``timeout_s``.

    This is the second, coarser bound described on :class:`GraphDeadlines`:
    the FalkorDB client already carries a socket-level timeout, and this
    catches everything above the socket too -- a request queued on a
    saturated event loop, one blocked on a server-side lock, anything that
    would otherwise wedge the caller past what the socket alone bounds.
    Callers pass ``deadlines.read_timeout_s`` for a bounded metadata/read
    operation or ``deadlines.write_timeout_s`` for the projection write,
    which is proportional to batch size and, with a semantic embedder, to
    real per-record network calls -- see the module docstring on
    :class:`~.flags.GraphDeadlines` for why the two must not share a bound.

    ``asyncio.TimeoutError`` never escapes this function. It is translated to
    :class:`GraphOperationTimeoutError` so a caller can distinguish "the
    store is unreachable/slow" from every other failure shape without
    special-casing the stdlib's own timeout type, and so the failure is
    logged once, here, with content-safe detail only -- an operation name and
    an opaque org id/partition, never an entity label, title or body.
    """

    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_s)
    except TimeoutError as exc:
        logger.warning(
            "context-fabric graph operation %s timed out after %.1fs (%s)",
            operation,
            timeout_s,
            detail,
        )
        raise GraphOperationTimeoutError(
            f"graph operation {operation!r} did not complete within "
            f"{timeout_s}s ({detail}); treat this as a degraded/unavailable "
            "graph, never as an empty result"
        ) from exc


@dataclass(frozen=True, slots=True)
class WriteResult:
    """What one projection write actually did."""

    nodes_written: int
    edges_written: int
    watermark: IndexWatermark


class GraphArmStore:
    """A connection to one organization's partition of the trial store.

    Constructed only through :meth:`for_org`, which is what makes the
    partition server-derived by construction: there is no constructor
    parameter a caller could use to name a different one.
    """

    def __init__(
        self,
        *,
        org_id: str,
        driver: Any,
        embedder: EmbeddingBackend,
        deadlines: GraphDeadlines | None = None,
    ) -> None:
        self._org_id = org_id
        self._partition = partition_for_org(org_id)
        self._driver = driver
        self._embedder = embedder
        self._deadlines = deadlines or graph_deadlines()

    @property
    def org_id(self) -> str:
        return self._org_id

    @property
    def partition(self) -> str:
        return self._partition

    @property
    def embedder(self) -> EmbeddingBackend:
        return self._embedder

    @property
    def deadlines(self) -> GraphDeadlines:
        """The connect/socket/read/write bounds this store's operations honour."""

        return self._deadlines

    async def _bounded_read(self, awaitable: Any, *, operation: str) -> Any:
        """Bound a metadata/administrative operation by ``read_timeout_s``."""

        return await _await_with_deadline(
            awaitable,
            timeout_s=self._deadlines.read_timeout_s,
            operation=operation,
            detail=f"org {self._org_id!r} partition {self._partition!r}",
        )

    async def _bounded_write(self, awaitable: Any, *, operation: str) -> Any:
        """Bound the projection write by ``write_timeout_s``.

        Deliberately a wider bound than :meth:`_bounded_read`: this call is
        proportional to batch size and, with a semantic embedder, makes one
        real network call per node and edge -- see
        :class:`~.flags.GraphDeadlines`.
        """

        return await _await_with_deadline(
            awaitable,
            timeout_s=self._deadlines.write_timeout_s,
            operation=operation,
            detail=f"org {self._org_id!r} partition {self._partition!r}",
        )

    @property
    def driver(self) -> Any:
        """The Graphiti driver, for read paths that issue their own queries.

        CHAOS-3647. Exposed rather than left as a private attribute the
        retrieval leg reaches through, because a caller that writes
        ``store._driver`` has silently taken on the store's whole invariant
        set — most importantly that the driver is bound to *this*
        organization's partition. Reading it here keeps the derivation
        visible: the driver a caller gets is the one :meth:`for_org`
        constructed with the server-derived partition as its ``database``,
        and there is still no way to obtain one for a partition the caller
        named.

        It does **not** make the partition optional at query time. Graphiti's
        search primitives filter on ``group_id`` independently of which
        keyspace the driver is bound to, so a caller must still pass
        :attr:`partition` as the group filter; ``semantic_retrieval``
        re-asserts it on every returned node for exactly that reason.
        """

        return self._driver

    @classmethod
    def for_org(
        cls,
        org_id: str,
        *,
        config: TrialStoreConfig | None = None,
        embedder: EmbeddingBackend | None = None,
        deadlines: GraphDeadlines | None = None,
    ) -> GraphArmStore:
        """Open the trial store for a **server-derived** organization id.

        Raises :class:`StoreUnavailableError` when no trial store is
        configured. It deliberately does not fall back to a default host and
        port: a misconfigured environment must fail, not project an
        organization's graph into whatever is listening locally.

        CHAOS-3631: the underlying FalkorDB client is built here, explicitly,
        with :data:`~.flags.GraphDeadlines`' connect/socket timeouts and
        connection-pool bound, and handed to ``FalkorDriver`` via
        ``falkor_db=``. ``FalkorDriver`` itself exposes no timeout parameter
        at all -- constructing it the old way (``host=``/``port=``) let it
        build an unconfigured client with redis-py's "block forever"
        defaults, which is the whole defect.
        """

        resolved = config or trial_store_config()
        if resolved is None:
            raise StoreUnavailableError(
                "no trial graph store is configured; set "
                "CONTEXT_FABRIC_GRAPH_STORE_URI (there is deliberately no "
                "default host/port)"
            )
        resolved_deadlines = deadlines or graph_deadlines()
        partition = partition_for_org(org_id)
        falkordb_module = graphiti_module("driver.falkordb_driver")
        client = falkordb_module.FalkorDB(
            host=resolved.host,
            port=resolved.port,
            password=resolved.password,
            socket_connect_timeout=resolved_deadlines.connect_timeout_s,
            socket_timeout=resolved_deadlines.socket_timeout_s,
            max_connections=resolved_deadlines.max_connections,
        )
        driver = falkordb_module.FalkorDriver(falkor_db=client, database=partition)
        return cls(
            org_id=org_id,
            driver=driver,
            embedder=embedder or DeterministicEmbedder(),
            deadlines=resolved_deadlines,
        )

    async def health_check(self) -> None:
        await self._bounded_read(self._driver.health_check(), operation="health_check")

    async def build_indices(self) -> None:
        await self._bounded_read(
            self._driver.build_indices_and_constraints(), operation="build_indices"
        )

    async def close(self) -> None:
        await self._bounded_read(self._driver.close(), operation="close")

    async def write_projection(
        self, projection: GraphProjection, *, budgets: TrialBudgets = DEFAULT_BUDGETS
    ) -> WriteResult:
        """Write one projection. Structured only, no model call, no waiting.

        Three refusals before anything is written:

        1. the projection flag must be on — an arm that writes with the flag
           off is not shadow-only;
        2. the projection's organization must be this store's;
        3. the projection's partition must be the one this organization
           derives, re-derived here rather than trusted from the projection.
        """

        if not graph_projection_enabled():
            raise ProjectionDisabledError(
                "graph projection is disabled; set "
                "CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1 to enable the "
                "shadow projection (it is off by default and must stay off "
                "in production)"
            )
        if projection.org_id != self._org_id:
            raise PermissionError(
                f"projection belongs to organization {projection.org_id!r} but "
                f"this store is open for {self._org_id!r}"
            )
        assert_partition_matches_org(projection.partition, self._org_id)

        if self._embedder.semantic:
            # Every node and every edge needs one vector, and the count is
            # known before the first call -- so an over-budget run costs
            # nothing, rather than costing most of the budget and then
            # stopping half-written. A non-semantic embedder makes no calls
            # and is deliberately not charged.
            #
            # Documents get their OWN embedding call each (CHAOS-3632:
            # to_graphiti_document_nodes embeds body, unconditionally, for
            # every embedder -- unlike to_graphiti_nodes, which only
            # pre-embeds for the deterministic case and otherwise leaves
            # add_nodes_and_edges_bulk to embed on name). Omitting them here
            # would let a document-heavy batch spend more calls than this
            # check ever saw.
            needed = (
                len(projection.nodes)
                + len(projection.edges)
                + len(projection.approved_documents)
            )
            outcome = budgets.check_embedding_calls(needed)
            if not outcome.within_budget:
                raise EmbeddingBudgetExceededError(
                    f"{outcome.detail}; this projection would spend more "
                    "embedding calls than the run allows. Narrow the batch or "
                    "raise max_embedding_calls deliberately"
                )

        nodes = to_graphiti_nodes(projection, self._embedder)
        document_nodes = await self._bounded_write(
            to_graphiti_document_nodes(projection, self._embedder),
            operation="embed_documents",
        )
        nodes = nodes + document_nodes
        edges = to_graphiti_edges(projection, self._embedder)
        await self._bounded_write(
            graphiti_module("utils.bulk_utils").add_nodes_and_edges_bulk(
                self._driver, [], [], nodes, edges, self._embedder
            ),
            operation="write_projection",
        )
        indexed_through = max(
            (
                *(node.observed_at for node in projection.nodes),
                *(document.observed_at for document in projection.approved_documents),
            ),
            default=None,
        )
        watermark = IndexWatermark(
            indexed_through=indexed_through,
            projected_at=datetime.now(UTC),
            records_indexed=(
                len(projection.nodes)
                + len(projection.edges)
                + len(projection.approved_documents)
            ),
            # A projection is all-or-nothing (over budget raises in
            # build_projection), and this write either completed or raised.
            # So a watermark produced here is never partial; the flag stays
            # for a caller that stops a multi-batch run early.
            partial=False,
        )
        logger.info(
            "context-fabric graph projection wrote %d nodes and %d edges to "
            "partition %s (%s)",
            len(nodes),
            len(edges),
            self._partition,
            watermark.detail_for(indexed_through or datetime.now(UTC)),
        )
        await self.persist_watermark(watermark)
        return WriteResult(
            nodes_written=len(nodes), edges_written=len(edges), watermark=watermark
        )

    async def persist_watermark(self, watermark: IndexWatermark) -> None:
        """Durably record ``watermark`` for this store's partition.

        CHAOS-3679. Called by :meth:`write_projection` after every successful
        write, and exposed publicly so a caller with its own watermark (a
        multi-batch run recording a partial one, for instance) can persist it
        directly without going through a write. Stored as a plain Redis key
        on the FalkorDB client's own connection -- see
        :data:`_WATERMARK_KEY_PREFIX` for why this is deliberately not a
        Cypher graph node.
        """

        payload = json.dumps(
            {
                "indexed_through": (
                    watermark.indexed_through.isoformat()
                    if watermark.indexed_through is not None
                    else None
                ),
                "projected_at": (
                    watermark.projected_at.isoformat()
                    if watermark.projected_at is not None
                    else None
                ),
                "records_indexed": watermark.records_indexed,
                "partial": watermark.partial,
            }
        )
        await self._bounded_read(
            self._driver.client.connection.set(
                _watermark_key(self._partition), payload
            ),
            operation="persist_watermark",
        )

    async def read_watermark(self) -> IndexWatermark:
        """The durably persisted watermark for this store's partition.

        CHAOS-3679. Returns ``IndexWatermark(indexed_through=None)`` --
        never-projected -- when nothing has been persisted, which is
        distinct from a transport failure: a hung or unreachable connection
        raises :class:`GraphOperationTimeoutError`/
        :class:`StoreUnavailableError` rather than silently reporting
        never-projected. "Checked and absent" and "could not check" must
        stay distinct here exactly as they do for
        :func:`org_deletion_visit`'s :class:`DeletionCompletenessUnknownError`
        -- a caller that cannot tell them apart would report an unreachable
        store as confidently fresh-with-nothing-in-it.
        """

        raw = await self._bounded_read(
            self._driver.client.connection.get(_watermark_key(self._partition)),
            operation="read_watermark",
        )
        if raw is None:
            return IndexWatermark(indexed_through=None)
        data = json.loads(raw)
        indexed_through_raw = data.get("indexed_through")
        projected_at_raw = data.get("projected_at")
        return IndexWatermark(
            indexed_through=(
                datetime.fromisoformat(indexed_through_raw)
                if indexed_through_raw is not None
                else None
            ),
            projected_at=(
                datetime.fromisoformat(projected_at_raw)
                if projected_at_raw is not None
                else None
            ),
            records_indexed=int(data.get("records_indexed") or 0),
            partial=bool(data.get("partial", False)),
        )

    async def partition_exists(self) -> bool:
        """Whether this organization has a keyspace, per the live store.

        Note the ordering caveat this cannot fix: constructing the store has
        *already* created the keyspace by the time you can call this. See
        :func:`partition_exists_for`, which is the read-only check.
        """

        graphs = await self._bounded_read(
            self._driver.client.list_graphs(), operation="partition_exists"
        )
        return self._partition in set(graphs or ())

    async def count_nodes(self) -> int:
        if not await self.partition_exists():
            return 0
        result = await self._bounded_read(
            self._driver.execute_query(NODE_COUNT_QUERY), operation="count_nodes"
        )
        if not result:
            return 0
        records, _, _ = result
        return int(records[0]["total"]) if records else 0

    async def purge_org(self, *, dry_run: bool = False) -> int:
        """Delete this organization's whole partition. Returns nodes visited.

        ``dry_run`` counts without deleting, which is what
        ``OrganizationDeletionService`` needs for its preview mode. The count
        is the node count, not a row count, and that is what the registry
        entry's note records.

        Failures propagate. An earlier version swallowed "graph not found"
        style errors by matching the message text, on the assumption that
        deleting a never-projected organization would raise; probing the real
        store showed ``delete()`` is idempotent and raises nothing, so that
        branch was dead code whose only effect would have been to swallow a
        *genuine* deletion failure that happened to contain the same words.
        An incomplete deletion has to be visible —
        ``_purge_external_stores`` records exactly that.

        CHAOS-3679: also removes the persisted watermark key. It is not part
        of the graph keyspace the drop below removes -- a raw Redis key, by
        design (see :data:`_WATERMARK_KEY_PREFIX`) -- so it needs its own
        explicit cleanup, or organization deletion would leave freshness
        metadata behind for an organization that no longer has any data.
        """

        if not await self.partition_exists():
            return 0
        total = await self.count_nodes()
        if dry_run:
            return total
        # One keyspace per organization, so deletion is a drop: there is no
        # partial state a failure could leave behind, and no node that a
        # traversal-based delete could miss. A drop is a single fixed-cost
        # operation regardless of partition size, so it belongs under the
        # read bound, not the write one.
        await self._bounded_read(
            self._driver.client.select_graph(self._partition).delete(),
            operation="purge_org",
        )
        await self._bounded_read(
            self._driver.client.connection.delete(_watermark_key(self._partition)),
            operation="purge_watermark",
        )
        return total


async def partition_exists_for(
    org_id: str,
    config: TrialStoreConfig,
    *,
    deadlines: GraphDeadlines | None = None,
) -> bool:
    """Whether an organization has a trial keyspace, **without creating one**.

    Found by probing the running store: ``FalkorDriver.__init__`` schedules
    ``build_indices_and_constraints()`` as a background task, so merely
    *constructing* a store for an organization creates that organization's
    keyspace — asynchronously, and with nothing in the call site to suggest
    it. A dry-run org deletion, which is supposed to be read-only, would
    therefore have created an empty keyspace for every organization it
    previewed.

    So this opens a bare FalkorDB client, lists graphs, and never touches the
    Graphiti driver. ``test_chaos_3617_live_store.py`` asserts the keyspace
    is still absent afterwards, against the live store rather than against an
    assumption about it.

    CHAOS-3631: the bare client is built with the same connect/socket
    deadlines and connection-pool bound as :meth:`GraphArmStore.for_org`, and
    the ``list_graphs`` call is bounded by ``read_timeout_s`` the same way --
    this preview path talks to the live store exactly as much as the store
    itself does, and must be exactly as bounded.
    """

    resolved_deadlines = deadlines or graph_deadlines()
    client = graphiti_module("driver.falkordb_driver").FalkorDB(
        host=config.host,
        port=config.port,
        password=config.password,
        socket_connect_timeout=resolved_deadlines.connect_timeout_s,
        socket_timeout=resolved_deadlines.socket_timeout_s,
        max_connections=resolved_deadlines.max_connections,
    )
    try:
        graphs = await _await_with_deadline(
            client.list_graphs(),
            timeout_s=resolved_deadlines.read_timeout_s,
            operation="partition_exists_for",
            detail=f"org {org_id!r}",
        )
        return partition_for_org(org_id) in set(graphs or ())
    finally:
        aclose = getattr(client, "aclose", None)
        if aclose is not None:
            await aclose()


class DeletionCompletenessUnknownError(RuntimeError):
    """Deletion could not prove the organization's partition is absent.

    Raised instead of returning ``0``. ``0`` means "checked, and there was
    nothing"; it must never mean "could not check". Adversarial review found
    the previous behaviour returning ``0`` whenever the store was
    unconfigured or graphiti-core was missing — but a partition written by an
    earlier deployment, or before the URI was removed from the environment,
    survives both of those conditions. Org deletion would then have recorded
    a successful zero-row visit over data nobody looked at.

    ``OrganizationDeletionService._purge_external_stores`` catches this,
    records ``"Derived store '…' deletion failed: …"`` in
    ``result.warnings`` and carries on, so raising surfaces the unknown
    without blocking the deletion.
    """


async def org_deletion_visit(org_id: str, dry_run: bool) -> int:
    """The ``DerivedStore.visit`` callable for org deletion (CHAOS-3566).

    Registered in ``EXTERNAL_DERIVED_STORES``. Returns the number of graph
    nodes visited/deleted for the organization.

    **Zero is a measurement wherever the store is reachable.** Once the
    store is configured, every path that cannot check it raises
    :class:`DeletionCompletenessUnknownError` — a missing graphiti-core does
    not make the data disappear, and an unreachable endpoint is an unknown
    rather than an absence. ``0`` is returned only after a positive existence
    check proved the partition absent.

    The **one** exception is a store that is not configured at all, which is
    the production default and returns ``0`` with a logged warning rather
    than raising. Adversarial review argued for raising there too, and the
    full gate showed why that is wrong: it made every org deletion in every
    unconfigured environment warn about an optional trial store, which is
    exactly the "no behaviour change for deployments without it" property
    CHAOS-3566's registry requires. The residual — a deployment that once had
    the store configured — is logged, and its remedy is to point
    ``CONTEXT_FABRIC_GRAPH_STORE_URI`` at the store for the deletion run.

    Raising does not block deletion —
    ``OrganizationDeletionService._purge_external_stores`` catches, records a
    warning and continues — so the only thing this decides is whether an
    unverified deletion is visible.
    """

    config = trial_store_config()
    if config is None:
        # NOT an "unknown". This is the production default -- the trial store
        # is opt-in per environment and is configured nowhere in production --
        # and CHAOS-3566's registry is explicit that a deployment without the
        # trial store must see no behaviour change. Raising here made every
        # org deletion in every unconfigured environment carry a warning about
        # an optional trial store, which is noise that trains readers to
        # ignore the warning channel.
        #
        # The residual is real and is logged rather than hidden: if this
        # deployment once HAD the store configured and the variable was later
        # removed, a partition can survive. That is an operator action with an
        # operator remedy (re-point the variable for the deletion run), and it
        # is the narrow case -- distinct from "configured but uncheckable"
        # below, where the deployment demonstrably uses the trial store and
        # the answer genuinely is unknown.
        logger.warning(
            "context-fabric graph trial store is not configured; org %s is "
            "reported as having no graph partition WITHOUT checking. If this "
            "deployment ever ran the trial, set %s for the deletion run",
            org_id,
            TRIAL_STORE_URI_VAR,
        )
        return 0
    try:
        exists = await partition_exists_for(org_id, config)
    except GraphitiUnavailableError as exc:
        raise DeletionCompletenessUnknownError(
            f"deletion completeness unknown for org {org_id}: the trial graph "
            "store is configured but graphiti-core is not installed, so its "
            f"partition could not be checked ({exc}). Data written by a "
            "deployment that DID have the extra installed is unaffected by "
            "its absence here"
        ) from exc
    except Exception as exc:
        # Anything else reaching here is a transport failure -- a refused
        # connection, a timeout, an auth error. Wrapped rather than allowed to
        # propagate raw, because `_purge_external_stores` renders whatever it
        # catches into `"Derived store '…' deletion failed: <exc>"`, and a
        # bare `ConnectionError: Error 61 connecting to …` reads as an
        # infrastructure blip rather than as "this organization's derived data
        # was never checked". The docstring promises an unknown; the recorded
        # warning has to say so too.
        raise DeletionCompletenessUnknownError(
            f"deletion completeness unknown for org {org_id}: the trial graph "
            f"store at {config.host}:{config.port} is configured but could "
            f"not be reached ({type(exc).__name__}: {exc}), so its partition "
            "was neither verified absent nor purged"
        ) from exc
    if not exists:
        # Positively checked, and absent. This is the only path that may
        # report zero -- and it constructs no store, which is what keeps the
        # preview read-only (see partition_exists_for).
        logger.info(
            "context-fabric graph trial store holds no partition for org %s", org_id
        )
        return 0
    store = GraphArmStore.for_org(org_id, config=config)
    try:
        return await store.purge_org(dry_run=dry_run)
    finally:
        await store.close()
