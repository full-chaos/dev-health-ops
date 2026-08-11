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
        "deployments",
        "git_commits",
        "ci_pipeline_runs",
        "work_graph_pr_commit",
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
    """**Reproduces an actual cross-tenant data leak against the real
    ClickHouse engine -- not a unit-test analogy, a live-verified defect.**

    The incident itself is genuinely ORG_A's; the only matching edge row
    (by ``incident_id`` alone) belongs to ORG_B. The join's own ``org_id``
    predicate (not just the outer ``WHERE`` on the incident row) must keep
    them apart -- ORG_A's incident must resolve with no repository at all
    and be refused, never ORG_B's repository silently attached via the
    join.

    This is the one test in the whole CHAOS-3675 effort verified by
    breaking the PRODUCTION SQL text itself
    (``_INCIDENT_RESOLVE_SQL``'s join condition, dropping
    ``toUUIDOrZero(i.org_id) AND``) and re-running THIS test against a
    live scratch database with real seeded rows in both tables: with the
    org predicate removed, ORG_B's ``repo_id`` genuinely appeared on
    ORG_A's resolved record (observed directly, not inferred) -- a real
    cross-tenant repository leak, not a hypothetical one. Do not weaken,
    skip, or delete this test as "just a live-environment nicety": it is
    the strongest evidence artifact this lane produced that the join's
    tenant isolation is real rather than assumed from the query text."""

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


def _insert_deployment(
    client: Any,
    *,
    org_id: str,
    repo_id: str = REPO_ID,
    deployment_id: str,
    pull_request_number: int | None,
    status: str = "success",
    environment: str = "production",
) -> None:
    client.insert(
        "deployments",
        [
            [
                org_id,
                repo_id,
                deployment_id,
                status,
                environment,
                pull_request_number,
                NOW,
                NOW,
            ]
        ],
        column_names=[
            "org_id",
            "repo_id",
            "deployment_id",
            "status",
            "environment",
            "pull_request_number",
            "deployed_at",
            "last_synced",
        ],
    )


@pytest.mark.asyncio
async def test_the_real_deployment_sql_derives_a_linked_pr(ch_client: Any) -> None:
    _insert_deployment(
        ch_client, org_id=ORG_A, deployment_id="deploy-live-1", pull_request_number=77
    )

    record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}#deploymentdeploy-live-1",
        entity_type="deployment",
    )

    assert record is not None
    assert record.no_authorizable_entity is False
    assert record.entity_id == f"{REPO_ID}#pr77"
    assert record.repository_ids == (REPO_ID,)


@pytest.mark.asyncio
async def test_the_real_deployment_sql_falls_through_to_repository_only_when_unlinked(
    ch_client: Any,
) -> None:
    _insert_deployment(
        ch_client,
        org_id=ORG_A,
        deployment_id="deploy-live-2",
        pull_request_number=None,
    )

    record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}#deploymentdeploy-live-2",
        entity_type="deployment",
    )

    assert record is not None
    assert record.no_authorizable_entity is True
    assert record.repository_ids == (REPO_ID,)


@pytest.mark.asyncio
async def test_the_real_deployment_sql_enforces_tenant_isolation(
    ch_client: Any,
) -> None:
    _insert_deployment(
        ch_client, org_id=ORG_A, deployment_id="deploy-live-3", pull_request_number=1
    )

    other_org = await _resolve(
        ch_client,
        org_id=ORG_B,
        locator=f"{REPO_ID}#deploymentdeploy-live-3",
        entity_type="deployment",
    )

    assert other_org is None


# ---------------------------------------------------------------------------
# CHAOS-3685: ci_run and commit, against the real schema. ci_run needs no
# join (ci_pipeline_runs.pr_number is native-attributed); commit's trust
# threshold (only provenance='native' on work_graph_pr_commit derives) is
# the one property this file exists to prove against the REAL engine, not
# a fake sink's assumption about it -- see
# test_the_real_commit_sql_never_derives_from_a_heuristic_or_explicit_text_link
# below, which was live-verified by breaking the production SQL text.
# ---------------------------------------------------------------------------


def _insert_ci_run(
    client: Any,
    *,
    org_id: str,
    repo_id: str = REPO_ID,
    run_id: str,
    status: str = "success",
    pr_number: int | None,
) -> None:
    client.insert(
        "ci_pipeline_runs",
        [[org_id, repo_id, run_id, status, pr_number, NOW, NOW]],
        column_names=[
            "org_id",
            "repo_id",
            "run_id",
            "status",
            "pr_number",
            "started_at",
            "last_synced",
        ],
    )


@pytest.mark.asyncio
async def test_the_real_ci_run_sql_derives_a_natively_linked_pr(
    ch_client: Any,
) -> None:
    _insert_ci_run(ch_client, org_id=ORG_A, run_id="run-live-1", pr_number=77)

    record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}#cirun-live-1",
        entity_type="ci_run",
    )

    assert record is not None
    assert record.no_authorizable_entity is False
    assert record.entity_id == f"{REPO_ID}#pr77"
    assert record.repository_ids == (REPO_ID,)


@pytest.mark.asyncio
async def test_the_real_ci_run_sql_falls_through_to_repository_only_when_unlinked(
    ch_client: Any,
) -> None:
    _insert_ci_run(ch_client, org_id=ORG_A, run_id="run-live-2", pr_number=None)

    record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}#cirun-live-2",
        entity_type="ci_run",
    )

    assert record is not None
    assert record.no_authorizable_entity is True
    assert record.repository_ids == (REPO_ID,)


@pytest.mark.asyncio
async def test_the_real_ci_run_sql_enforces_tenant_isolation(ch_client: Any) -> None:
    _insert_ci_run(ch_client, org_id=ORG_A, run_id="run-live-3", pr_number=1)

    other_org = await _resolve(
        ch_client,
        org_id=ORG_B,
        locator=f"{REPO_ID}#cirun-live-3",
        entity_type="ci_run",
    )

    assert other_org is None


def _insert_commit(
    client: Any,
    *,
    org_id: str,
    repo_id: str = REPO_ID,
    commit_hash: str,
    message: str = "A commit",
) -> None:
    client.insert(
        "git_commits",
        [[org_id, repo_id, commit_hash, message, NOW, NOW, 1, NOW]],
        column_names=[
            "org_id",
            "repo_id",
            "hash",
            "message",
            "author_when",
            "committer_when",
            "parents",
            "last_synced",
        ],
    )


def _insert_pr_commit_link(
    client: Any,
    *,
    org_id: str,
    repo_id: str = REPO_ID,
    pr_number: int,
    commit_hash: str,
    provenance: str,
    confidence: float,
) -> None:
    client.insert(
        "work_graph_pr_commit",
        [
            [
                org_id,
                repo_id,
                pr_number,
                commit_hash,
                confidence,
                provenance,
                "test-evidence",
                NOW,
            ]
        ],
        column_names=[
            "org_id",
            "repo_id",
            "pr_number",
            "commit_hash",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
        ],
    )


@pytest.mark.asyncio
async def test_the_real_commit_sql_derives_a_native_provenance_linked_pr(
    ch_client: Any,
) -> None:
    _insert_commit(ch_client, org_id=ORG_A, commit_hash="deadbeef01")
    _insert_pr_commit_link(
        ch_client,
        org_id=ORG_A,
        pr_number=77,
        commit_hash="deadbeef01",
        provenance="native",
        confidence=1.0,
    )

    record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}@deadbeef01",
        entity_type="commit",
    )

    assert record is not None
    assert record.no_authorizable_entity is False
    assert record.entity_id == f"{REPO_ID}#pr77"
    assert record.repository_ids == (REPO_ID,)


@pytest.mark.asyncio
async def test_the_real_commit_sql_never_derives_from_a_heuristic_or_explicit_text_link(
    ch_client: Any,
) -> None:
    """**Adversarial / anti-laundering, live-verified.** Two commits, each
    with a real ``work_graph_pr_commit`` row pointing at a real PR, but
    below the ratified trust threshold: one ``heuristic`` (0.6, a
    squash-merge subject match), one ``explicit_text`` (0.9, an explicit
    merge-keyword match -- HIGHER confidence than the heuristic tier, to
    prove the gate is genuinely on ``provenance``, not a confidence
    cutoff a high-confidence non-native link could slip past). Neither
    must derive the linked PR -- both must resolve repository-only,
    exactly as if no link existed at all.

    Live-verified as a real guard, not an assumed one: with
    ``_COMMIT_RESOLVE_SQL``'s subquery ``AND provenance = 'native'``
    clause removed, this test was re-run against this same live schema
    and BOTH commits wrongly derived their linked PR (observed directly).
    Do not weaken, skip, or delete this test.
    """

    _insert_commit(ch_client, org_id=ORG_A, commit_hash="heuristic01")
    _insert_pr_commit_link(
        ch_client,
        org_id=ORG_A,
        pr_number=77,
        commit_hash="heuristic01",
        provenance="heuristic",
        confidence=0.6,
    )
    _insert_commit(ch_client, org_id=ORG_A, commit_hash="expltext01")
    _insert_pr_commit_link(
        ch_client,
        org_id=ORG_A,
        pr_number=88,
        commit_hash="expltext01",
        provenance="explicit_text",
        confidence=0.9,
    )

    heuristic_record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}@heuristic01",
        entity_type="commit",
    )
    explicit_text_record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}@expltext01",
        entity_type="commit",
    )

    assert heuristic_record is not None
    assert heuristic_record.no_authorizable_entity is True
    assert heuristic_record.entity_id == "heuristic01"

    assert explicit_text_record is not None
    assert explicit_text_record.no_authorizable_entity is True
    assert explicit_text_record.entity_id == "expltext01"


@pytest.mark.asyncio
async def test_the_real_commit_sql_falls_through_to_repository_only_when_unlinked(
    ch_client: Any,
) -> None:
    _insert_commit(ch_client, org_id=ORG_A, commit_hash="nolinkhash")

    record = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}@nolinkhash",
        entity_type="commit",
    )

    assert record is not None
    assert record.no_authorizable_entity is True
    assert record.repository_ids == (REPO_ID,)


@pytest.mark.asyncio
async def test_the_real_commit_sql_enforces_tenant_isolation(ch_client: Any) -> None:
    """A native-provenance link exists, but for a DIFFERENT org than the
    caller -- the outer ``WHERE c.org_id`` and the subquery's own
    ``WHERE org_id`` must both hold; a same-hash commit in ORG_A must not
    resolve at all (it was never inserted for ORG_A), and must not
    accidentally pick up ORG_B's link."""

    _insert_commit(ch_client, org_id=ORG_B, commit_hash="orgbcommit")
    _insert_pr_commit_link(
        ch_client,
        org_id=ORG_B,
        pr_number=1,
        commit_hash="orgbcommit",
        provenance="native",
        confidence=1.0,
    )

    other_org = await _resolve(
        ch_client,
        org_id=ORG_A,
        locator=f"{REPO_ID}@orgbcommit",
        entity_type="commit",
    )

    assert other_org is None
