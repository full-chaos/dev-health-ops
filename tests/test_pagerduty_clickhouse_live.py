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
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import pytest

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

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
SOURCE_TIME = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
ORG_ID = "test-chaos-2957"
PROVIDER_INSTANCE_ID = "pagerduty-chaos-2957"

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
