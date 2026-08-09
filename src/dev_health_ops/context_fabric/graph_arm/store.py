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

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .backend import (
    DeterministicEmbedder,
    EmbeddingBackend,
    GraphitiUnavailableError,
    graphiti_module,
    to_graphiti_edges,
    to_graphiti_nodes,
)
from .flags import (
    TRIAL_STORE_URI_VAR,
    TrialStoreConfig,
    graph_projection_enabled,
    trial_store_config,
)
from .identity import assert_partition_matches_org, partition_for_org
from .projection import GraphProjection
from .readback import NODE_COUNT_QUERY
from .watermark import IndexWatermark

logger = logging.getLogger(__name__)

__all__ = [
    "GraphArmStore",
    "ProjectionDisabledError",
    "partition_exists_for",
    "StoreUnavailableError",
    "TRIAL_DERIVED_STORE_NAME",
    "org_deletion_visit",
]

#: The name this store registers under in ``EXTERNAL_DERIVED_STORES``.
TRIAL_DERIVED_STORE_NAME = "context_fabric_graph_trial"


class StoreUnavailableError(RuntimeError):
    """The trial store is not configured or not reachable."""


class ProjectionDisabledError(RuntimeError):
    """A write was attempted with the projection flag off."""


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

    def __init__(self, *, org_id: str, driver: Any, embedder: EmbeddingBackend) -> None:
        self._org_id = org_id
        self._partition = partition_for_org(org_id)
        self._driver = driver
        self._embedder = embedder

    @property
    def org_id(self) -> str:
        return self._org_id

    @property
    def partition(self) -> str:
        return self._partition

    @property
    def embedder(self) -> EmbeddingBackend:
        return self._embedder

    @classmethod
    def for_org(
        cls,
        org_id: str,
        *,
        config: TrialStoreConfig | None = None,
        embedder: EmbeddingBackend | None = None,
    ) -> GraphArmStore:
        """Open the trial store for a **server-derived** organization id.

        Raises :class:`StoreUnavailableError` when no trial store is
        configured. It deliberately does not fall back to a default host and
        port: a misconfigured environment must fail, not project an
        organization's graph into whatever is listening locally.
        """

        resolved = config or trial_store_config()
        if resolved is None:
            raise StoreUnavailableError(
                "no trial graph store is configured; set "
                "CONTEXT_FABRIC_GRAPH_STORE_URI (there is deliberately no "
                "default host/port)"
            )
        partition = partition_for_org(org_id)
        driver = graphiti_module("driver.falkordb_driver").FalkorDriver(
            host=resolved.host,
            port=resolved.port,
            password=resolved.password,
            database=partition,
        )
        return cls(
            org_id=org_id, driver=driver, embedder=embedder or DeterministicEmbedder()
        )

    async def health_check(self) -> None:
        await self._driver.health_check()

    async def build_indices(self) -> None:
        await self._driver.build_indices_and_constraints()

    async def close(self) -> None:
        await self._driver.close()

    async def write_projection(self, projection: GraphProjection) -> WriteResult:
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

        nodes = to_graphiti_nodes(projection, self._embedder)
        edges = to_graphiti_edges(projection, self._embedder)
        await graphiti_module("utils.bulk_utils").add_nodes_and_edges_bulk(
            self._driver, [], [], nodes, edges, self._embedder
        )
        indexed_through = max(
            (node.observed_at for node in projection.nodes),
            default=None,
        )
        watermark = IndexWatermark(
            indexed_through=indexed_through,
            projected_at=datetime.now(UTC),
            records_indexed=len(projection.nodes) + len(projection.edges),
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
        return WriteResult(
            nodes_written=len(nodes), edges_written=len(edges), watermark=watermark
        )

    async def partition_exists(self) -> bool:
        """Whether this organization has a keyspace, per the live store.

        Note the ordering caveat this cannot fix: constructing the store has
        *already* created the keyspace by the time you can call this. See
        :func:`partition_exists_for`, which is the read-only check.
        """

        graphs = await self._driver.client.list_graphs()
        return self._partition in set(graphs or ())

    async def count_nodes(self) -> int:
        if not await self.partition_exists():
            return 0
        result = await self._driver.execute_query(NODE_COUNT_QUERY)
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
        """

        if not await self.partition_exists():
            return 0
        total = await self.count_nodes()
        if dry_run:
            return total
        # One keyspace per organization, so deletion is a drop: there is no
        # partial state a failure could leave behind, and no node that a
        # traversal-based delete could miss.
        await self._driver.client.select_graph(self._partition).delete()
        return total


async def partition_exists_for(org_id: str, config: TrialStoreConfig) -> bool:
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
    """

    client = graphiti_module("driver.falkordb_driver").FalkorDB(
        host=config.host, port=config.port, password=config.password
    )
    try:
        graphs = await client.list_graphs()
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

    **Zero is a measurement, not a fallback.** Every path that cannot reach
    the store raises :class:`DeletionCompletenessUnknownError`; ``0`` is
    returned only after a positive existence check proved the partition is
    absent. That includes the two cases that look like safe no-ops and are
    not: an unconfigured store URI (the partition may still exist from when
    it *was* configured) and a missing graphiti-core (the data does not
    disappear because the library did).

    Raising does not block deletion —
    ``OrganizationDeletionService._purge_external_stores`` catches, records a
    warning and continues — so the only thing this decides is whether an
    unverified deletion is visible.
    """

    config = trial_store_config()
    if config is None:
        raise DeletionCompletenessUnknownError(
            f"no trial graph store is configured, so org {org_id}'s graph "
            "partition could not be checked. A partition written while the "
            "store WAS configured would survive this deletion; reporting 0 "
            f"would record it as purged. Set {TRIAL_STORE_URI_VAR} for the "
            "deletion run, or confirm out of band that this deployment never "
            "projected"
        )
    try:
        exists = await partition_exists_for(org_id, config)
    except GraphitiUnavailableError as exc:
        raise DeletionCompletenessUnknownError(
            f"the trial graph store is configured but graphiti-core is not "
            f"installed, so org {org_id}'s partition could not be checked: "
            f"{exc}. Data written by a deployment that DID have the extra "
            "installed is unaffected by its absence here"
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
