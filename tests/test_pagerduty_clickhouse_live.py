"""Live ClickHouse round-trip and idempotency proof for PagerDuty REST sync.

Set ``CLICKHOUSE_URI`` to an isolated, non-default scratch database. This test
creates and drops a second unique database below that isolated connection so it
never modifies the caller's scratch database or the local ``default`` database.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import pytest

from dev_health_ops.audit.completeness import build_incidents_query
from dev_health_ops.metrics.active_incidents import (
    IncidentWindow,
    active_incidents_query,
)
from dev_health_ops.metrics.compute_dora import compute_dora_metrics_daily
from dev_health_ops.metrics.compute_incidents import compute_incident_metrics_daily
from dev_health_ops.metrics.schemas import IncidentRow
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.operational import (
    CanonicalOperationalEntity,
    EscalationPolicy,
    IncidentNote,
    IncidentTimelineEvent,
    OnCallAssignment,
    OnCallSchedule,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
    ServiceRepositoryMapping,
    canonical_operational_id,
)
from dev_health_ops.models.operational_identity import operational_source_coordinates
from dev_health_ops.providers.pagerduty.models import (
    Alert,
    Incident,
    LogEntry,
    Note,
    Oncall,
    PagerDutyModel,
    Schedule,
    Service,
    Team,
    User,
)
from dev_health_ops.providers.pagerduty.models import (
    EscalationPolicy as PagerDutyEscalationPolicy,
)
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer
from dev_health_ops.storage.clickhouse import ClickHouseStore
from dev_health_ops.work_graph.operational_edges import build_operational_incident_edges

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
SOURCE_TIME = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
ORG_ID = "test-chaos-2957"
PROVIDER_INSTANCE_ID = "pagerduty-chaos-2957"
METRIC_REPO_ID = UUID("11111111-1111-1111-1111-111111111111")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


@dataclass(frozen=True, slots=True)
class PagerDutyOperationalRows:
    """One representative persisted row for every PagerDuty-emitted entity."""

    service: OperationalService
    incident: OperationalIncident
    alert: OperationalAlert
    timeline_event: IncidentTimelineEvent
    note: IncidentNote
    escalation_policy: EscalationPolicy
    schedule: OnCallSchedule
    oncall: OnCallAssignment
    team: OperationalTeam
    user: OperationalUser

    def table_rows(self) -> tuple[tuple[str, CanonicalOperationalEntity], ...]:
        """Return each row paired with its canonical operational table."""
        return (
            ("operational_services", self.service),
            ("operational_incidents", self.incident),
            ("operational_alerts", self.alert),
            ("operational_incident_timeline_events", self.timeline_event),
            ("operational_incident_notes", self.note),
            ("operational_escalation_policies", self.escalation_policy),
            ("operational_on_call_schedules", self.schedule),
            ("operational_on_call_assignments", self.oncall),
            ("operational_teams", self.team),
            ("operational_users", self.user),
        )


@pytest.fixture(scope="module")
def pagerduty_scratch_dsn() -> Iterator[str]:
    """Provide a unique, dropped-on-teardown ClickHouse database for this module."""
    assert CLICKHOUSE_URI is not None
    parsed = urlsplit(CLICKHOUSE_URI)
    parent_database = parsed.path.lstrip("/")
    if parent_database in {"", "default"}:
        pytest.skip(
            "refusing to create PagerDuty live-test schema from the default database"
        )

    import clickhouse_connect

    database = f"chaos_2957_pagerduty_{uuid4().hex}"
    scratch_dsn = urlunsplit(parsed._replace(path=f"/{database}"))
    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    client.command(f"CREATE DATABASE {database}")
    try:
        schema = ClickHouseMetricsSink(scratch_dsn)
        try:
            schema.ensure_schema(force=True)
        finally:
            schema.close()
        yield scratch_dsn
    finally:
        client.command(f"DROP DATABASE IF EXISTS {database}")
        client.close()


def _pagerduty_rows() -> PagerDutyOperationalRows:
    normalizer = PagerDutyNormalizer(
        org_id=ORG_ID,
        provider_instance_id=PROVIDER_INSTANCE_ID,
        observed_at=SOURCE_TIME,
    )
    service_reference = PagerDutyModel(id="service-1")
    policy_reference = PagerDutyModel(id="policy-1")
    schedule_reference = PagerDutyModel(id="schedule-1")
    user_reference = PagerDutyModel(id="user-1")
    service = normalizer.service(
        Service(id="service-1", name="Payments API", updated_at=SOURCE_TIME)
    )
    incident = normalizer.incident(
        Incident(
            id="incident-1",
            title="Payments latency",
            service=service_reference,
            created_at=SOURCE_TIME,
            updated_at=SOURCE_TIME,
        )
    )
    return PagerDutyOperationalRows(
        service=service,
        incident=incident,
        alert=normalizer.alert(
            Alert(
                id="alert-1",
                summary="Elevated latency",
                severity="critical",
                created_at=SOURCE_TIME,
                updated_at=SOURCE_TIME,
            ),
            incident.id,
        ),
        timeline_event=normalizer.log_entry(
            LogEntry(
                id="timeline-1",
                summary="Incident acknowledged",
                created_at=SOURCE_TIME,
                updated_at=SOURCE_TIME,
            ),
            incident.id,
        ),
        note=normalizer.note(
            Note(
                id="note-1",
                content="Evidence from the PagerDuty incident record.",
                user=user_reference,
                created_at=SOURCE_TIME,
                updated_at=SOURCE_TIME,
            ),
            incident.id,
        ),
        escalation_policy=normalizer.escalation_policy(
            PagerDutyEscalationPolicy(
                id="policy-1", name="Primary", updated_at=SOURCE_TIME
            )
        ),
        schedule=normalizer.schedule(
            Schedule(id="schedule-1", name="Primary rotation", updated_at=SOURCE_TIME)
        ),
        oncall=normalizer.oncall(
            Oncall(
                id="oncall-1",
                user=user_reference,
                schedule=schedule_reference,
                escalation_policy=policy_reference,
                escalation_level=1,
                start=SOURCE_TIME,
                end=SOURCE_TIME,
                updated_at=SOURCE_TIME,
            )
        ),
        team=normalizer.team(Team(id="team-1", name="SRE", updated_at=SOURCE_TIME)),
        user=normalizer.user(
            User(
                id="user-1",
                name="Ada Lovelace",
                email="ada@example.test",
                updated_at=SOURCE_TIME,
            )
        ),
    )


async def _persist(dsn: str, rows: PagerDutyOperationalRows) -> None:
    async with ClickHouseStore(dsn) as store:
        store.org_id = ORG_ID
        await store.insert_operational_services([rows.service])
        await store.insert_operational_incidents([rows.incident])
        await store.insert_operational_alerts([rows.alert])
        await store.insert_operational_incident_timeline_events([rows.timeline_event])
        await store.insert_operational_incident_notes([rows.note])
        await store.insert_operational_escalation_policies([rows.escalation_policy])
        await store.insert_operational_on_call_schedules([rows.schedule])
        await store.insert_operational_on_call_assignments([rows.oncall])
        await store.insert_operational_teams([rows.team])
        await store.insert_operational_users([rows.user])


def test_pagerduty_operational_rows_round_trip_and_deduplicate(
    pagerduty_scratch_dsn: str,
) -> None:
    # Given: normalized PagerDuty source records for every emitted entity family.
    rows = _pagerduty_rows()

    # When: each real operational sink receives the same source payload twice.
    asyncio.run(_persist(pagerduty_scratch_dsn, rows))
    asyncio.run(_persist(pagerduty_scratch_dsn, rows))

    import clickhouse_connect

    client = clickhouse_connect.get_client(dsn=pagerduty_scratch_dsn)
    try:
        # Then: FINAL keeps one source-versioned row per canonical identity.
        for table, row in rows.table_rows():
            client.command(f"OPTIMIZE TABLE {table} FINAL")
            coordinates = operational_source_coordinates(
                type(row),
                provider="pagerduty",
                provider_instance_id=PROVIDER_INSTANCE_ID,
                external_id=row.external_id,
            )
            expected_id = canonical_operational_id(
                ORG_ID,
                coordinates.provider,
                coordinates.provider_instance_id,
                coordinates.entity_family,
                coordinates.external_id,
            )
            source_version = row.source_version_at
            delta = source_version.astimezone(timezone.utc) - datetime(
                1970, 1, 1, tzinfo=timezone.utc
            )
            expected_epoch_us = (
                delta.days * 86_400 + delta.seconds
            ) * 1_000_000 + delta.microseconds
            result = client.query(
                f"SELECT id, org_id, provider_instance_id, "
                f"toUnixTimestamp64Micro(source_version_at) "
                f"FROM {table} FINAL WHERE org_id = {{org_id:String}}",
                parameters={"org_id": ORG_ID},
            )
            assert result.result_rows == [
                (
                    expected_id,
                    ORG_ID,
                    PROVIDER_INSTANCE_ID,
                    expected_epoch_us,
                )
            ]
            count = client.query(
                f"SELECT count() FROM {table} FINAL WHERE org_id = {{org_id:String}} "
                "AND id = {id:String}",
                parameters={"org_id": ORG_ID, "id": expected_id},
            )
            assert count.result_rows == [(1,)]
    finally:
        client.close()


def test_mapped_canonical_pagerduty_incident_drives_incident_metrics(
    pagerduty_scratch_dsn: str,
) -> None:
    # Given: a resolved PagerDuty incident and an explicit mapping from its
    # canonical service to an organization-owned repository.
    rows = _pagerduty_rows()
    resolved_at = SOURCE_TIME + timedelta(hours=4)
    normalizer = PagerDutyNormalizer(
        org_id=ORG_ID,
        provider_instance_id=PROVIDER_INSTANCE_ID,
        observed_at=resolved_at,
    )
    resolved_incident = normalizer.incident(
        Incident(
            id="incident-1",
            title="Payments latency",
            status="resolved",
            service=PagerDutyModel(id="service-1"),
            created_at=SOURCE_TIME,
            resolved_at=resolved_at,
            last_status_change_at=resolved_at,
            updated_at=resolved_at,
        )
    )
    mapping = ServiceRepositoryMapping(
        org_id=ORG_ID,
        provider="pagerduty",
        provider_instance_id=PROVIDER_INSTANCE_ID,
        source_entity_type="service_repository_mapping",
        external_id="service-1:github:full-chaos/payments-api",
        source_version_at=resolved_at,
        observed_at=resolved_at,
        last_synced=resolved_at,
        service_id=rows.service.id,
        repo_id=METRIC_REPO_ID,
        repo_provider="github",
        repo_full_name="full-chaos/payments-api",
        mapping_kind="admin",
        rule_id="service_repository_mapping.admin.v1",
        valid_from=SOURCE_TIME,
        is_active=True,
    )

    async def persist_projection_inputs() -> None:
        async with ClickHouseStore(pagerduty_scratch_dsn) as store:
            store.org_id = ORG_ID
            await store.insert_operational_services([rows.service])
            await store.insert_operational_incidents([resolved_incident])
            await store.insert_operational_service_repository_mappings([mapping])

    asyncio.run(persist_projection_inputs())

    sink = ClickHouseMetricsSink(pagerduty_scratch_dsn)
    try:
        sink.client.insert(
            "repos",
            [
                [
                    METRIC_REPO_ID,
                    "full-chaos/payments-api",
                    None,
                    SOURCE_TIME,
                    None,
                    None,
                    resolved_at,
                    ORG_ID,
                    "github",
                    None,
                ]
            ],
            column_names=[
                "id",
                "repo",
                "ref",
                "created_at",
                "settings",
                "tags",
                "last_synced",
                "org_id",
                "provider",
                "source_id",
            ],
        )

        # When: the real canonical projection and both incident metric
        # computations consume the persisted ClickHouse rows.
        projected = sink.query_dicts(
            active_incidents_query(
                window=IncidentWindow.RESOLVED,
                org_id=ORG_ID,
                repo_filter="",
            ),
            {
                "org_id": ORG_ID,
                "start": SOURCE_TIME,
                "end": SOURCE_TIME + timedelta(days=1),
                "as_of": resolved_at + timedelta(seconds=1),
            },
        )
        incident_rows = cast(list[IncidentRow], projected)
        incident_metrics = compute_incident_metrics_daily(
            day=SOURCE_TIME.date(),
            incidents=incident_rows,
            computed_at=resolved_at,
        )
        dora_metrics = compute_dora_metrics_daily(
            day=SOURCE_TIME.date(),
            deployments=[],
            incidents=incident_rows,
            computed_at=resolved_at,
        )

        # Then: one mapped canonical incident contributes to repository-scoped
        # counts, MTTR, and DORA restoration time without a legacy-table write.
        assert len(projected) == 1
        assert projected[0]["repo_id"] == METRIC_REPO_ID
        assert projected[0]["incident_id"] == resolved_incident.id
        assert len(incident_metrics) == 1
        assert incident_metrics[0].incidents_count == 1
        assert incident_metrics[0].mttr_p50_hours == pytest.approx(4.0)
        assert [(metric.metric_name, metric.value) for metric in dora_metrics] == [
            ("time_to_restore_service", pytest.approx(4 * 60 * 60))
        ]
        legacy_table_count = sink.query(
            "SELECT count() FROM system.tables "
            "WHERE database = currentDatabase() AND name = 'incidents'",
        )
        assert legacy_table_count.result_rows == [(0,)]
    finally:
        sink.close()


def test_null_valid_from_mapping_survives_every_as_of_reader(
    pagerduty_scratch_dsn: str,
) -> None:
    """CHAOS-3570: a NULL valid_from service/repo mapping must still answer
    every as-of consumer of ``operational_service_repository_mappings``.

    ``valid_from`` is ``Nullable`` on that table, and ClickHouse's `NULL <=
    x` is false, so a naive `valid_from <= {as_of}` predicate silently drops
    a null-start mapping from every as-of answer. Per CHAOS-3570's agreed
    semantics, a NULL valid_from means "valid since before records began"
    and must satisfy any as_of filter (`valid_from IS NULL OR valid_from <=
    {as_of}`). This plants exactly such a mapping and exercises the real
    production query builders of all three swept as-of readers.
    """
    org_id = "test-chaos-3570-null-valid-from"
    repo_id = UUID("33333333-3333-3333-3333-333333333333")
    resolved_at = SOURCE_TIME + timedelta(hours=4)
    normalizer = PagerDutyNormalizer(
        org_id=org_id,
        provider_instance_id=PROVIDER_INSTANCE_ID,
        observed_at=resolved_at,
    )
    service = normalizer.service(
        Service(
            id="service-3570", name="Null Valid From Service", updated_at=SOURCE_TIME
        )
    )
    resolved_incident = normalizer.incident(
        Incident(
            id="incident-3570",
            title="Null valid_from regression",
            status="resolved",
            service=PagerDutyModel(id="service-3570"),
            created_at=SOURCE_TIME,
            resolved_at=resolved_at,
            last_status_change_at=resolved_at,
            updated_at=resolved_at,
        )
    )
    # Given: a service/repo mapping with a NULL valid_from -- e.g. a mapping
    # backfilled with no known start, or ingested before valid_from existed.
    mapping = ServiceRepositoryMapping(
        org_id=org_id,
        provider="pagerduty",
        provider_instance_id=PROVIDER_INSTANCE_ID,
        source_entity_type="service_repository_mapping",
        external_id="service-3570:github:full-chaos/null-valid-from",
        source_version_at=resolved_at,
        observed_at=resolved_at,
        last_synced=resolved_at,
        service_id=service.id,
        repo_id=repo_id,
        repo_provider="github",
        repo_full_name="full-chaos/null-valid-from",
        mapping_kind="admin",
        rule_id="service_repository_mapping.admin.v1",
        valid_from=None,
        valid_to=None,
        is_active=True,
    )

    async def persist_projection_inputs() -> None:
        async with ClickHouseStore(pagerduty_scratch_dsn) as store:
            store.org_id = org_id
            await store.insert_operational_services([service])
            await store.insert_operational_incidents([resolved_incident])
            await store.insert_operational_service_repository_mappings([mapping])

    asyncio.run(persist_projection_inputs())

    sink = ClickHouseMetricsSink(pagerduty_scratch_dsn)
    try:
        sink.client.insert(
            "repos",
            [
                [
                    repo_id,
                    "full-chaos/null-valid-from",
                    None,
                    SOURCE_TIME,
                    None,
                    None,
                    resolved_at,
                    org_id,
                    "github",
                    None,
                ]
            ],
            column_names=[
                "id",
                "repo",
                "ref",
                "created_at",
                "settings",
                "tags",
                "last_synced",
                "org_id",
                "provider",
                "source_id",
            ],
        )

        as_of = resolved_at + timedelta(seconds=1)

        # When/Then: metrics.active_incidents.active_incidents_query still
        # projects the incident through the null-start mapping.
        projected = sink.query_dicts(
            active_incidents_query(
                window=IncidentWindow.RESOLVED,
                org_id=org_id,
                repo_filter="",
            ),
            {
                "org_id": org_id,
                "start": SOURCE_TIME,
                "end": SOURCE_TIME + timedelta(days=1),
                "as_of": as_of,
            },
        )
        assert len(projected) == 1, (
            "active_incidents_query dropped a NULL valid_from mapping (CHAOS-3570)"
        )
        assert projected[0]["repo_id"] == repo_id
        assert projected[0]["incident_id"] == resolved_incident.id

        # When/Then: audit.completeness.build_incidents_query still counts
        # the incident through the null-start mapping.
        audit_rows = sink.query_dicts(
            build_incidents_query(),
            {
                "org_id": org_id,
                "start": SOURCE_TIME,
                "end": SOURCE_TIME + timedelta(days=1),
                "as_of": as_of,
            },
        )
        assert audit_rows, (
            "build_incidents_query dropped a NULL valid_from mapping (CHAOS-3570)"
        )
        assert audit_rows[0]["repo_id"] == repo_id
        assert audit_rows[0]["count"] == 1

        # When/Then: work_graph.operational_edges still emits the
        # MAPS_TO_REPOSITORY edge through the null-start mapping.
        edges = build_operational_incident_edges(sink, org_id, as_of, 7, 0.3)
        mapping_edges = [
            edge for edge in edges if edge.edge_type.value == "maps_to_repository"
        ]
        assert mapping_edges, (
            "build_operational_incident_edges dropped a NULL valid_from mapping "
            "(CHAOS-3570)"
        )
        assert mapping_edges[0].target_id == str(repo_id)
    finally:
        sink.close()


def test_audit_incidents_query_has_no_soft_delete_predicate_on_mapping_table(
    pagerduty_scratch_dsn: str,
) -> None:
    """CHAOS-3604: build_incidents_query() must not filter `is_deleted` on
    operational_service_repository_mappings -- that table has no such
    column. Reintroducing it makes ClickHouse's JOIN analyzer resolve the
    unknown column into the sibling operational_incidents subquery's own
    `is_deleted` across the JOIN boundary and reject the query outright
    with "Correlated subqueries are not supported in JOINs yet"
    (NOT_IMPLEMENTED) -- a hard crash, not a silently wrong answer. This
    pins the query executing successfully end to end against a live engine.
    """
    org_id = "test-chaos-3604-no-soft-delete-on-mapping"
    repo_id = UUID("44444444-4444-4444-4444-444444444444")
    resolved_at = SOURCE_TIME + timedelta(hours=4)
    normalizer = PagerDutyNormalizer(
        org_id=org_id,
        provider_instance_id=PROVIDER_INSTANCE_ID,
        observed_at=resolved_at,
    )
    service = normalizer.service(
        Service(
            id="service-3604", name="No Soft Delete Service", updated_at=SOURCE_TIME
        )
    )
    resolved_incident = normalizer.incident(
        Incident(
            id="incident-3604",
            title="Audit completeness live-execution regression",
            status="resolved",
            service=PagerDutyModel(id="service-3604"),
            created_at=SOURCE_TIME,
            resolved_at=resolved_at,
            last_status_change_at=resolved_at,
            updated_at=resolved_at,
        )
    )
    mapping = ServiceRepositoryMapping(
        org_id=org_id,
        provider="pagerduty",
        provider_instance_id=PROVIDER_INSTANCE_ID,
        source_entity_type="service_repository_mapping",
        external_id="service-3604:github:full-chaos/no-soft-delete",
        source_version_at=resolved_at,
        observed_at=resolved_at,
        last_synced=resolved_at,
        service_id=service.id,
        repo_id=repo_id,
        repo_provider="github",
        repo_full_name="full-chaos/no-soft-delete",
        mapping_kind="admin",
        rule_id="service_repository_mapping.admin.v1",
        valid_from=SOURCE_TIME,
        is_active=True,
    )

    async def persist_projection_inputs() -> None:
        async with ClickHouseStore(pagerduty_scratch_dsn) as store:
            store.org_id = org_id
            await store.insert_operational_services([service])
            await store.insert_operational_incidents([resolved_incident])
            await store.insert_operational_service_repository_mappings([mapping])

    asyncio.run(persist_projection_inputs())

    sink = ClickHouseMetricsSink(pagerduty_scratch_dsn)
    try:
        as_of = resolved_at + timedelta(seconds=1)
        # Then: the query executes against a live engine (it would raise
        # DatabaseError with "Correlated subqueries are not supported in
        # JOINs yet" if the bogus is_deleted predicate were reintroduced)
        # and counts the mapped incident.
        rows = sink.query_dicts(
            build_incidents_query(),
            {
                "org_id": org_id,
                "start": SOURCE_TIME,
                "end": SOURCE_TIME + timedelta(days=1),
                "as_of": as_of,
            },
        )
        assert rows
        assert rows[0]["repo_id"] == repo_id
        assert rows[0]["count"] == 1
    finally:
        sink.close()


def test_audit_incidents_query_dedupes_coexisting_mapping_identities(
    pagerduty_scratch_dsn: str,
) -> None:
    """Codex review of CHAOS-3570: broadening the as-of predicate to admit
    NULL valid_from rows widens exposure to a pre-existing double-count --
    a service can carry more than one currently-active mapping identity to
    the same repository at once (e.g. an admin_configuration row alongside
    a bounded_service_repository_heuristic row with a NULL valid_from
    backfill; see work_graph.operational_edges's own preferred_mappings
    dedup for the same fan-out). Without deduping the incident/mapping
    JOIN, one incident was counted once per coexisting mapping identity.

    Plants two coexisting active mapping identities (one dated, one
    NULL-start) for the SAME service/repo pair -- asserting the dedup key's
    incident.id half -- *and* a third active mapping for the SAME service to
    a DIFFERENT repository -- asserting the dedup key's repo_id half. A
    dedup that collapsed on incident.id alone (dropping repo_id from the
    key) would pass the first case but silently lose the second repo's
    incident from the audit entirely, since service->multiple-repos is a
    normal, supported configuration (see
    work_graph.operational_edges:117-119).
    """
    org_id = "test-chaos-3570-dupe-mapping-dedup"
    repo_id = UUID("55555555-5555-5555-5555-555555555555")
    other_repo_id = UUID("66666666-6666-6666-6666-666666666666")
    resolved_at = SOURCE_TIME + timedelta(hours=4)
    normalizer = PagerDutyNormalizer(
        org_id=org_id,
        provider_instance_id=PROVIDER_INSTANCE_ID,
        observed_at=resolved_at,
    )
    service = normalizer.service(
        Service(id="service-dupe", name="Dupe Mapping Service", updated_at=SOURCE_TIME)
    )
    resolved_incident = normalizer.incident(
        Incident(
            id="incident-dupe",
            title="Coexisting mapping identities regression",
            status="resolved",
            service=PagerDutyModel(id="service-dupe"),
            created_at=SOURCE_TIME,
            resolved_at=resolved_at,
            last_status_change_at=resolved_at,
            updated_at=resolved_at,
        )
    )
    # Given: two DISTINCT active mapping identities (different external_id,
    # so different canonical ids) for the SAME service -> repo pair.
    mapping_dated = ServiceRepositoryMapping(
        org_id=org_id,
        provider="pagerduty",
        provider_instance_id=PROVIDER_INSTANCE_ID,
        source_entity_type="service_repository_mapping",
        external_id="service-dupe:github:x/y:admin",
        source_version_at=resolved_at,
        observed_at=resolved_at,
        last_synced=resolved_at,
        service_id=service.id,
        repo_id=repo_id,
        repo_provider="github",
        repo_full_name="full-chaos/dupe-mapping",
        mapping_kind="admin",
        rule_id="service_repository_mapping.admin.v1",
        valid_from=SOURCE_TIME,
        valid_to=None,
        is_active=True,
    )
    mapping_null_start = ServiceRepositoryMapping(
        org_id=org_id,
        provider="pagerduty",
        provider_instance_id=PROVIDER_INSTANCE_ID,
        source_entity_type="service_repository_mapping",
        external_id="service-dupe:github:x/y:heuristic",
        source_version_at=resolved_at,
        observed_at=resolved_at,
        last_synced=resolved_at,
        service_id=service.id,
        repo_id=repo_id,
        repo_provider="github",
        repo_full_name="full-chaos/dupe-mapping",
        mapping_kind="bounded_service_repository_heuristic",
        rule_id="service_repository_mapping.bounded_name_match.v1",
        valid_from=None,
        valid_to=None,
        is_active=True,
    )
    # A third active mapping for the SAME service to a DIFFERENT repo --
    # both repos must appear in the result, each counted once.
    mapping_other_repo = ServiceRepositoryMapping(
        org_id=org_id,
        provider="pagerduty",
        provider_instance_id=PROVIDER_INSTANCE_ID,
        source_entity_type="service_repository_mapping",
        external_id="service-dupe:github:other/repo:admin",
        source_version_at=resolved_at,
        observed_at=resolved_at,
        last_synced=resolved_at,
        service_id=service.id,
        repo_id=other_repo_id,
        repo_provider="github",
        repo_full_name="full-chaos/other-repo",
        mapping_kind="admin",
        rule_id="service_repository_mapping.admin.v1",
        valid_from=SOURCE_TIME,
        valid_to=None,
        is_active=True,
    )

    async def persist_projection_inputs() -> None:
        async with ClickHouseStore(pagerduty_scratch_dsn) as store:
            store.org_id = org_id
            await store.insert_operational_services([service])
            await store.insert_operational_incidents([resolved_incident])
            await store.insert_operational_service_repository_mappings(
                [mapping_dated, mapping_null_start, mapping_other_repo]
            )

    asyncio.run(persist_projection_inputs())

    sink = ClickHouseMetricsSink(pagerduty_scratch_dsn)
    try:
        as_of = resolved_at + timedelta(seconds=1)
        rows = sink.query_dicts(
            build_incidents_query(),
            {
                "org_id": org_id,
                "start": SOURCE_TIME,
                "end": SOURCE_TIME + timedelta(days=1),
                "as_of": as_of,
            },
        )
        counts_by_repo = {row["repo_id"]: row["count"] for row in rows}
        assert set(counts_by_repo) == {repo_id, other_repo_id}, (
            "expected one row per repository mapped from the service -- a "
            "dedup keyed on incident.id alone (dropping repo_id) would "
            "collapse these into one row and silently lose a repo's "
            "incident from the audit"
        )
        assert counts_by_repo[repo_id] == 1, (
            "one incident double-counted across two coexisting active "
            "mapping identities for the same service/repo pair"
        )
        assert counts_by_repo[other_repo_id] == 1
    finally:
        sink.close()
