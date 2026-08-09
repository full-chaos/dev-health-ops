"""CHAOS-3617: identity and partition derivation for the graph arm.

**Canonical IDs are the identity. Graphiti mints nothing.**

Graphiti's node model requires a UUID. That is a storage address, and this
module derives it deterministically from ``(org_id, kind, canonical_id)``
with :func:`uuid.uuid5`, so:

* re-ingesting the same record produces the same node, making projection
  idempotent and re-runs cheap;
* the canonical id is recoverable from the node (it is stored verbatim in
  the node's attributes, and :func:`node_uuid` is reproducible from it), so
  a packet never has to quote a graph-native identifier;
* two organizations holding the same canonical id — the cross-tenant
  near-duplicate case the negative tests exercise — get **different** node
  UUIDs, because ``org_id`` is inside the hash input.

The last point is the one worth being explicit about: identity collision
across tenants is not prevented by a filter that could be forgotten, it is
arithmetically impossible for records that reach this function honestly.
What it does *not* prevent is a caller passing the wrong ``org_id``, which
is why the server derives the partition (:func:`partition_for_org`) and the
caller never supplies it.

**The partition is never caller-supplied.** ``group_id`` is Graphiti's graph
partition key. The issue is unambiguous: "callers never supply Graphiti
group_id as authorization". :func:`partition_for_org` is the only way to
obtain one, it takes the server-derived organization id, and
:func:`assert_partition_matches_org` is the assertion every read path runs
before it trusts a result. Graph membership never grants access: the
partition bounds *what was searched*, and the authorized-entity filter at
emission time bounds *what may be returned*.
"""

from __future__ import annotations

import re
import uuid

from .vocabulary import GraphEntityKind, GraphObservationKind

__all__ = [
    "GRAPH_ARM_NAMESPACE",
    "PARTITION_PATTERN",
    "assert_partition_matches_org",
    "node_uuid",
    "observation_uuid",
    "org_from_partition",
    "partition_for_org",
    "relationship_uuid",
]

#: Fixed UUID5 namespace for the trial. Frozen: changing it re-addresses
#: every node in every trial store and silently invalidates any recorded
#: run, so it is pinned by
#: ``test_chaos_3617_identity.py::test_namespace_is_frozen``.
GRAPH_ARM_NAMESPACE = uuid.UUID("6f0f1d4e-9f2a-5c6b-8d13-2c7a4b5e9f01")

#: Graphiti's own ``validate_group_id`` accepts alphanumerics, underscores
#: and dashes. The trial partition is deliberately narrower and prefixed, so
#: a partition string is recognisable as ours and an organization id that
#: would need escaping is rejected rather than mangled.
PARTITION_PATTERN = re.compile(r"^cf_trial_[a-z0-9][a-z0-9_-]{0,96}$")

#: Lowercase only, and that is load-bearing rather than stylistic. The
#: pattern used to accept mixed case while derivation lowercased, so
#: ``Org_A`` and ``org_a`` -- both accepted -- derived the SAME partition
#: and therefore shared one FalkorDB keyspace: one organization's purge
#: would drop the other's data and a read would see both. Derivation must
#: be injective over accepted ids, so the normalisation is removed and the
#: accepted set is narrowed instead. Found by adversarial review; pinned by
#: ``test_chaos_3617_identity.py::TestServerDerivedPartition``.
_ORG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,96}$")
_PARTITION_PREFIX = "cf_trial_"


def partition_for_org(org_id: str) -> str:
    """The graph partition for a server-derived organization id.

    The single source of partition strings. Rejects an organization id that
    is not a plain identifier rather than sanitising it: silently rewriting
    ``a/b`` to ``a_b`` would collide two organizations into one partition,
    which is the worst failure this module could have.
    """

    if not _ORG_PATTERN.fullmatch(org_id):
        raise ValueError(
            f"organization id {org_id!r} is not a lowercase plain identifier; "
            "refusing to derive a graph partition from it. Normalising (rather "
            "than refusing) is what would collide two organizations into one "
            "partition, so this function never normalises: the mapping from "
            "accepted id to partition is injective by construction"
        )
    partition = f"{_PARTITION_PREFIX}{org_id}"
    if not PARTITION_PATTERN.fullmatch(partition):
        raise ValueError(f"derived partition {partition!r} is not well formed")
    return partition


def org_from_partition(partition: str) -> str:
    """The organization id a partition was derived from.

    Used only for diagnostics and for
    :func:`assert_partition_matches_org`. It is *not* an authorization
    decision: knowing which organization a partition belongs to says nothing
    about whether the caller may read it.
    """

    if not PARTITION_PATTERN.fullmatch(partition):
        raise ValueError(f"{partition!r} is not a trial graph partition")
    return partition[len(_PARTITION_PREFIX) :]


def assert_partition_matches_org(partition: str, org_id: str) -> None:
    """Raise unless ``partition`` is the one this organization derives.

    Run by every read path before it trusts a result set. Comparing against
    a freshly derived partition — rather than trusting the one carried
    alongside the results — is what makes a caller-supplied partition
    useless as an authorization claim.
    """

    expected = partition_for_org(org_id)
    if partition != expected:
        raise PermissionError(
            f"graph partition {partition!r} does not belong to organization "
            f"{org_id!r} (expected {expected!r}); a partition is a search "
            "bound, never an authorization grant"
        )


def _derive(org_id: str, discriminator: str, kind: str, canonical_id: str) -> str:
    if not canonical_id:
        raise ValueError("canonical_id must not be empty")
    # NUL-separated so that ("a", "b:c") and ("a:b", "c") cannot hash to the
    # same address; canonical ids contain ':' and '/' routinely.
    name = "\0".join((org_id, discriminator, kind, canonical_id))
    return str(uuid.uuid5(GRAPH_ARM_NAMESPACE, name))


def node_uuid(org_id: str, kind: GraphEntityKind, canonical_id: str) -> str:
    """The storage address of an entity node. Deterministic and org-scoped."""

    return _derive(org_id, "entity", kind.value, canonical_id)


def observation_uuid(org_id: str, kind: GraphObservationKind, canonical_id: str) -> str:
    """The storage address of an observation node."""

    return _derive(org_id, "observation", kind.value, canonical_id)


def relationship_uuid(
    org_id: str,
    relationship: str,
    source_kind: GraphEntityKind,
    source_id: str,
    target_kind: GraphEntityKind,
    target_id: str,
) -> str:
    """The storage address of one relationship edge.

    Both endpoints and the relationship type are in the hash input, so a
    project owned by two teams yields two edges rather than one edge whose
    target flips depending on ingestion order.
    """

    name = "\0".join(
        (
            org_id,
            "relationship",
            relationship,
            source_kind.value,
            source_id,
            target_kind.value,
            target_id,
        )
    )
    return str(uuid.uuid5(GRAPH_ARM_NAMESPACE, name))
