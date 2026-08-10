"""CHAOS-3675 PR 1/3 (review) and PR 2/3 (incident) resolver SQL, against a
real migrated ClickHouse schema -- not assumed from the query text, not
simulated by a fake sink (``test_native_evidence_resolver.py`` covers each
resolver's own logic that way; this file proves the SQL itself).

Three properties a fake sink cannot establish:
1. The resolve SQL parses and executes against the real schema (column
   names/types match) -- including the incident resolver's cross-table
   ``org_id`` type mismatch (``operational_incidents.org_id`` is
   ``String``, ``work_graph_deployment_incident_edges.org_id`` is
   ``UUID``) and its ``toUUIDOrZero`` cast.
2. The ``WHERE org_id = ...`` predicate genuinely enforces tenant
   isolation in the real engine -- two rows sharing an identical locator
   but different ``org_id``s, and a lookup for one org never returns the
   other's row.
3. For incidents specifically: the join's OWN ``org_id`` predicate keeps
   an edge belonging to a different org from leaking its repository onto
   an otherwise-genuine same-org incident.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import pytest

_PROTECTED_DATABASES = frozenset({"", "default"})


def _database_of(dsn: str | None) -> str:
    from urllib.parse import urlparse

    return urlparse(dsn or "").path.lstrip("/").strip().lower()


CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
_SKIP_REASON = (
    "Requires a migrated SCRATCH CLICKHOUSE_URI "
    "(e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate); "
    f"got database {_database_of(CLICKHOUSE_URI) or '<unset>'!r}, which this "
    "suite refuses to run against"
)

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI or _database_of(CLICKHOUSE_URI) in _PROTECTED_DATABASES,
        reason=_SKIP_REASON,
    ),
]

ORG_A = "11111111-1111-4111-8111-111111111111"
ORG_B = "22222222-2222-4222-8222-222222222222"
REPO_ID = "33333333-3333-4333-8333-333333333333"
NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def ch_client() -> Any:
    import clickhouse_connect

    assert CLICKHOUSE_URI is not None
    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(autouse=True)
def _clean_tables(ch_client: Any) -> Any:
    tables = (
        "git_pull_request_reviews",
        "operational_incidents",
        "work_graph_deployment_incident_edges",
    )
    for table in tables:
        ch_client.command(f"TRUNCATE TABLE IF EXISTS {table}")
    yield
    for table in tables:
        ch_client.command(f"TRUNCATE TABLE IF EXISTS {table}")


def _insert_review(
    client: Any,
    *,
    org_id: str,
    repo_id: str = REPO_ID,
    number: int = 42,
    review_id: str = "rev-9",
    reviewer: str = "octocat",
    state: str = "approved",
) -> None:
    client.insert(
        "git_pull_request_reviews",
        [[org_id, repo_id, number, review_id, reviewer, state, NOW, NOW]],
        column_names=[
            "org_id",
            "repo_id",
            "number",
            "review_id",
            "reviewer",
            "state",
            "submitted_at",
            "last_synced",
        ],
    )


def _insert_incident(
    client: Any,
    *,
    org_id: str,
    incident_id: str,
    title: str = "Checkout latency spike",
) -> None:
    client.insert(
        "operational_incidents",
        [
            [
                org_id,
                "pagerduty",
                "instance-1",
                "incident",
                incident_id,
                NOW,
                incident_id,
                NOW,
                NOW,
                "resolved",
                title,
                0,
            ]
        ],
        column_names=[
            "org_id",
            "provider",
            "provider_instance_id",
            "source_entity_type",
            "external_id",
            "source_version_at",
            "id",
            "observed_at",
            "last_synced",
            "normalized_status",
            "title",
            "is_deleted",
        ],
    )


def _insert_incident_edge(
    client: Any,
    *,
    org_id: str,
    incident_id: str,
    repo_id: str,
    deployment_id: str = "deploy-1",
) -> None:
    client.insert(
        "work_graph_deployment_incident_edges",
        [
            [
                f"edge-{org_id}-{incident_id}",
                org_id,
                deployment_id,
                incident_id,
                "native",
                repo_id,
                1.0,
                "edge",
                "",
                NOW,
            ]
        ],
        column_names=[
            "edge_id",
            "org_id",
            "deployment_id",
            "incident_id",
            "provider",
            "repo_id",
            "confidence",
            "source",
            "evidence",
            "observed_at",
        ],
    )


async def _resolve(
    client: Any, *, org_id: str, locator: str, entity_type: str = "review"
):
    from dev_health_ops.api.dev.evidence_service import EvidenceCandidate
    from dev_health_ops.api.dev.native_evidence_resolver import (
        NativeEvidenceCandidateResolver,
    )
    from dev_health_ops.context_fabric.graph_arm.admission import ARM_SOURCE_SYSTEM

    resolver = NativeEvidenceCandidateResolver(client)
    candidate = EvidenceCandidate(
        source_system=ARM_SOURCE_SYSTEM,
        entity_type=entity_type,
        entity_id="a-claim-the-real-row-does-not-corroborate",
        locator=locator,
    )
    return await resolver.resolve(org_id=org_id, scope=None, candidate=candidate)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_the_real_sql_resolves_a_seeded_review_and_derives_its_pr(
    ch_client: Any,
) -> None:
    _insert_review(ch_client, org_id=ORG_A)
    locator = f"{REPO_ID}#pr42#reviewrev-9"

    record = await _resolve(ch_client, org_id=ORG_A, locator=locator)

    assert record is not None
    assert record.entity_id == f"{REPO_ID}#pr42"
    assert record.repository_ids == (REPO_ID,)
    assert record.display_label == "Review by octocat"


@pytest.mark.asyncio
async def test_the_real_sql_enforces_tenant_isolation_on_an_identical_locator(
    ch_client: Any,
) -> None:
    """Two rows, byte-identical composite locator, different orgs. The real
    ``WHERE org_id = {org_id:String}`` predicate -- not Python -- is what
    must keep them apart."""

    locator = f"{REPO_ID}#pr42#reviewrev-9"
    _insert_review(ch_client, org_id=ORG_A, reviewer="org-a-reviewer")
    _insert_review(ch_client, org_id=ORG_B, reviewer="org-b-reviewer")

    a = await _resolve(ch_client, org_id=ORG_A, locator=locator)
    b = await _resolve(ch_client, org_id=ORG_B, locator=locator)

    assert a is not None and a.display_label == "Review by org-a-reviewer"
    assert b is not None and b.display_label == "Review by org-b-reviewer"

    # And a THIRD org, with no row of its own, gets neither.
    absent = await _resolve(
        ch_client, org_id="99999999-9999-4999-8999-999999999999", locator=locator
    )
    assert absent is None


@pytest.mark.asyncio
async def test_an_unseeded_locator_resolves_to_none_against_the_real_table(
    ch_client: Any,
) -> None:
    record = await _resolve(
        ch_client, org_id=ORG_A, locator=f"{REPO_ID}#pr404#reviewnope"
    )
    assert record is None


@pytest.mark.asyncio
async def test_the_real_incident_sql_joins_across_the_org_id_type_mismatch(
    ch_client: Any,
) -> None:
    """``operational_incidents.org_id`` is ``String``;
    ``work_graph_deployment_incident_edges.org_id`` is ``UUID`` -- the
    resolver's ``toUUIDOrZero(i.org_id)`` cast must actually join correctly
    against the real engine, not just parse."""

    _insert_incident(ch_client, org_id=ORG_A, incident_id="inc-live-1")
    _insert_incident_edge(
        ch_client, org_id=ORG_A, incident_id="inc-live-1", repo_id=REPO_ID
    )

    record = await _resolve(
        ch_client, org_id=ORG_A, locator="inc-live-1", entity_type="incident"
    )

    assert record is not None
    assert record.no_authorizable_entity is True
    assert record.repository_ids == (REPO_ID,)
    assert record.entity_id == "inc-live-1"


@pytest.mark.asyncio
async def test_the_real_incident_sql_enforces_tenant_isolation(
    ch_client: Any,
) -> None:
    """Two incidents, byte-identical id, different orgs -- the real
    ``WHERE i.org_id = {org_id:String}`` predicate must keep them apart."""

    _insert_incident(
        ch_client, org_id=ORG_A, incident_id="inc-live-2", title="Org A incident"
    )
    _insert_incident_edge(
        ch_client, org_id=ORG_A, incident_id="inc-live-2", repo_id=REPO_ID
    )
    _insert_incident(
        ch_client, org_id=ORG_B, incident_id="inc-live-2", title="Org B incident"
    )
    _insert_incident_edge(
        ch_client, org_id=ORG_B, incident_id="inc-live-2", repo_id=REPO_ID
    )

    a = await _resolve(
        ch_client, org_id=ORG_A, locator="inc-live-2", entity_type="incident"
    )
    b = await _resolve(
        ch_client, org_id=ORG_B, locator="inc-live-2", entity_type="incident"
    )

    assert a is not None and a.display_label == "Incident: Org A incident"
    assert b is not None and b.display_label == "Incident: Org B incident"


@pytest.mark.asyncio
async def test_the_real_incident_join_does_not_leak_a_different_orgs_edge(
    ch_client: Any,
) -> None:
    """The incident itself is genuinely ORG_A's; the only matching edge row
    (by ``incident_id`` alone) belongs to ORG_B. The join's own ``org_id``
    predicate (not just the outer ``WHERE``) must keep them apart -- ORG_A's
    incident resolves with no repository at all, refused, never ORG_B's
    repository smuggled across via the join."""

    _insert_incident(ch_client, org_id=ORG_A, incident_id="inc-live-3")
    _insert_incident_edge(
        ch_client, org_id=ORG_B, incident_id="inc-live-3", repo_id=REPO_ID
    )

    record = await _resolve(
        ch_client, org_id=ORG_A, locator="inc-live-3", entity_type="incident"
    )

    assert record is None


@pytest.mark.asyncio
async def test_an_incident_with_no_edge_at_all_refuses_against_the_real_table(
    ch_client: Any,
) -> None:
    _insert_incident(ch_client, org_id=ORG_A, incident_id="inc-live-4")

    record = await _resolve(
        ch_client, org_id=ORG_A, locator="inc-live-4", entity_type="incident"
    )

    assert record is None
