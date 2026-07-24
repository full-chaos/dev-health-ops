#!/usr/bin/env python3
"""Generate the Go external-ingest sink oracle from the live Python writers.

The resulting JSON is consumed by Go tests.  ``--check`` makes CI prove that
the committed oracle still matches the production Python normalization,
identity derivation, operational relationship hydration, and final ClickHouse
column/value adapters.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from dev_health_ops.api.external_ingest.schemas import RecordEnvelope  # noqa: E402
from dev_health_ops.external_ingest import sinks as external_sinks  # noqa: E402
from dev_health_ops.external_ingest.ids import (  # noqa: E402
    derive_repo_uuid,
    derive_work_item_id,
)
from dev_health_ops.external_ingest.normalize import normalize_batch  # noqa: E402
from dev_health_ops.external_ingest.types import SinkWriteError  # noqa: E402
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink  # noqa: E402
from dev_health_ops.storage.clickhouse import ClickHouseStore  # noqa: E402
from dev_health_ops.storage.operational_ordering_guard import (  # noqa: E402
    OperationalOrderingContract,
)

OUTPUT = ROOT / "tests" / "fixtures" / "external_ingest_sink_python_golden.json"
NOW = datetime(2026, 7, 23, 12, 0, 0, tzinfo=timezone.utc)
ORG_ID = "org-golden"
SOURCE_ID = uuid.UUID("9749bda0-fc9f-4076-b19d-7b26c4f306ff")
INGESTION_ID = uuid.UUID("11111111-2222-4333-8444-555555555555")


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return (
            value.astimezone(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, int) and abs(value) > (1 << 53):
        return str(value)
    return value


class CaptureStore(ClickHouseStore):
    def __init__(self, org_id: str) -> None:
        super().__init__("clickhouse://capture.invalid")
        self.client = object()
        self.org_id = org_id
        self._operational_ordering_contract = OperationalOrderingContract.CURRENT
        self.captured: dict[str, dict[str, Any]] = {}

    async def _insert_rows(
        self, table: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        if "org_id" not in columns and self.org_id:
            columns = [*columns, "org_id"]
            for row in rows:
                row.setdefault("org_id", self.org_id)
        assert len(rows) == 1
        self.captured[table] = {
            "columns": columns,
            "values": [_canonical(rows[0].get(column)) for column in columns],
        }


class CaptureClient:
    def __init__(self) -> None:
        self.captured: dict[str, dict[str, Any]] = {}

    def insert(self, table, matrix, *, column_names):
        assert len(matrix) == 1
        self.captured[table] = {
            "columns": list(column_names),
            "values": [_canonical(item) for item in matrix[0]],
        }

    def close(self) -> None:
        return None


def _record(kind: str, payload: dict[str, Any]) -> RecordEnvelope:
    return RecordEnvelope(kind=kind, external_id=f"envelope-{kind}", payload=payload)


def legacy_records() -> list[RecordEnvelope]:
    repo = "Acme/API"
    return [
        _record(
            "repository.v1",
            {
                "externalId": repo,
                "sourceSystem": "github",
                "defaultRef": "main",
                "settings": {"private": True, "stars": 7},
                "tags": ["go", "ops"],
            },
        ),
        _record(
            "identity.v1",
            {
                "canonicalId": "ada@example.test",
                "displayName": "Ada",
                "email": "Ada@Example.Test",
                "providerIdentities": {"github": ["ada", "ada-lovelace"]},
                "teamIds": ["team-a"],
                "isActive": True,
                "updatedAt": "2026-07-23T11:00:00Z",
            },
        ),
        _record(
            "team.v1",
            {
                "id": "team-a",
                "name": "Team A",
                "description": "Platform",
                "members": ["ada@example.test"],
                "projectKeys": ["CHAOS"],
                "repoPatterns": ["acme/*"],
                "isActive": True,
                "updatedAt": "2026-07-23T11:00:00Z",
                "nativeTeamKey": "native-team-a",
                "parentTeamId": "team-parent",
            },
        ),
        _record(
            "work_item.v1",
            {
                "externalKey": "7",
                "provider": "github",
                "title": "Ship parity",
                "description": "accepted by the wire but not persisted by Python v1",
                "type": "pr",
                "status": "in_review",
                "statusRaw": "OPEN",
                "repositoryExternalId": repo,
                "nativeTeamKey": "forged-non-linear",
                "assignees": ["Ada@Example.Test", "octocat", ""],
                "reporter": "",
                "createdAt": "2026-07-22T10:00:00Z",
                "updatedAt": "2026-07-22T11:00:00Z",
                "startedAt": "2026-07-22T10:30:00Z",
                "labels": ["migration", "go"],
                "storyPoints": 3.5,
                "sprintId": "sprint-1",
                "sprintName": "Sprint One",
                "parentId": "parent-1",
                "epicId": "epic-1",
                "url": "https://example.test/issues/7",
                "priorityRaw": "P1",
                "serviceClass": "expedite",
                "dueAt": "2026-07-30T00:00:00Z",
            },
        ),
        _record(
            "work_item_transition.v1",
            {
                "externalKey": "7",
                "provider": "github",
                "workItemType": "pr",
                "occurredAt": "2026-07-22T11:30:00Z",
                "fromStatus": "in_progress",
                "toStatus": "in_review",
                "fromStatusRaw": "IN PROGRESS",
                "toStatusRaw": "IN REVIEW",
                "actor": "",
            },
        ),
        _record(
            "work_item_dependency.v1",
            {
                "sourceExternalKey": "7",
                "sourceWorkItemType": "pr",
                "targetExternalKey": "8",
                "targetWorkItemType": "issue",
                "relationshipType": "blocks",
                "relationshipTypeRaw": "blocks",
            },
        ),
        _record(
            "pull_request.v1",
            {
                "repositoryExternalId": repo,
                "number": 7,
                "title": "Ship parity",
                "body": "Body",
                "state": "merged",
                "authorName": "Ada",
                "authorEmail": "ada@example.test",
                "createdAt": "2026-07-22T10:00:00Z",
                "mergedAt": "2026-07-22T15:00:00Z",
                "closedAt": "2026-07-22T15:00:00Z",
                "headBranch": "feature/parity",
                "baseBranch": "main",
                "additions": 10,
                "deletions": 2,
                "changedFiles": 3,
                "firstReviewAt": "2026-07-22T12:00:00Z",
                "firstCommentAt": "2026-07-22T11:00:00Z",
                "changesRequestedCount": 1,
                "reviewsCount": 2,
                "commentsCount": 4,
            },
        ),
        _record(
            "review.v1",
            {
                "repositoryExternalId": repo,
                "pullRequestNumber": 7,
                "reviewId": "review-1",
                "reviewer": "grace",
                "state": "APPROVED",
                "submittedAt": "2026-07-22T13:00:00Z",
            },
        ),
        _record(
            "commit.v1",
            {
                "repositoryExternalId": repo,
                "hash": "abcdef0123456789",
                "message": "ship parity",
                "authorName": "Ada",
                "authorEmail": "ada@example.test",
                "authorWhen": "2026-07-22T09:00:00Z",
                "committerName": "Grace",
                "committerEmail": "grace@example.test",
                "parents": 2,
            },
        ),
    ]


def operational_records() -> list[RecordEnvelope]:
    common = {
        "sourceVersionAt": "2026-07-22T15:00:00.123456Z",
        "sourceUrl": "https://tenant.pagerduty.com/source",
        "sourceEventAt": "2026-07-22T15:01:00Z",
        "sourceEventId": "event-1",
        "rawStatus": "triggered",
        "rawSeverity": "sev1",
        "rawPriority": "P1",
        "normalizedStatus": "open",
        "normalizedSeverity": "critical",
        "normalizedPriority": "critical",
        "relationshipProvenance": "customer_push",
        "relationshipConfidence": 0.875,
    }

    def payload(external_id: str, **values: Any) -> dict[str, Any]:
        return {**common, "externalId": external_id, **values}

    return [
        _record(
            "operational_service.v1",
            payload(
                "service-1",
                name="Payments",
                description="Payments API",
                serviceType="api",
                owningTeamExternalId="ops-team-1",
                escalationPolicyExternalId="policy-1",
            ),
        ),
        _record(
            "operational_incident.v1",
            payload(
                "incident-1",
                title="Payments latency",
                description="Elevated p99",
                serviceExternalId="service-1",
                escalationPolicyExternalId="policy-1",
                startedAt="2026-07-22T14:55:00Z",
            ),
        ),
        _record(
            "operational_alert.v1",
            payload(
                "alert-1",
                title="High latency",
                description="p99 threshold",
                serviceExternalId="service-1",
                incidentExternalId="incident-1",
                triggeredAt="2026-07-22T14:56:00Z",
                acknowledgedAt="2026-07-22T14:57:00Z",
            ),
        ),
        _record(
            "incident_timeline_event.v1",
            payload(
                "timeline-1",
                incidentExternalId="incident-1",
                eventType="acknowledged",
                body="Acknowledged",
                actorType="user",
                actorExternalId="user-1",
                occurredAt="2026-07-22T14:57:00Z",
            ),
        ),
        _record(
            "incident_note.v1",
            payload(
                "note-1",
                incidentExternalId="incident-1",
                body="Investigating",
                authorUserExternalId="user-1",
                createdAt="2026-07-22T14:58:00Z",
            ),
        ),
        _record(
            "incident_responder.v1",
            payload(
                "responder-1",
                incidentExternalId="incident-1",
                userExternalId="user-1",
                responderName="Ada",
                role="primary",
                responderAssignmentId="assignment-1",
                requestedAt="2026-07-22T14:56:00Z",
                assignedAt="2026-07-22T14:57:00Z",
            ),
        ),
        _record(
            "escalation_policy.v1",
            payload("policy-1", name="Primary", description="Primary policy"),
        ),
        _record(
            "on_call_schedule.v1",
            payload(
                "schedule-1",
                name="Primary schedule",
                description="Weekly rotation",
                timezone="America/Los_Angeles",
            ),
        ),
        _record(
            "on_call_assignment.v1",
            payload(
                "assignment-1",
                scheduleExternalId="schedule-1",
                userExternalId="user-1",
                escalationPolicyExternalId="policy-1",
                escalationLevel=2,
                startsAt="2026-07-22T00:00:00Z",
                endsAt="2026-07-23T00:00:00Z",
            ),
        ),
        _record(
            "operational_team.v1",
            payload("ops-team-1", name="Operations", description="Ops team"),
        ),
        _record(
            "operational_user.v1",
            payload("user-1", displayName="Ada Lovelace", email="ada@example.test"),
        ),
        _record(
            "service_repository_mapping.v1",
            payload(
                "mapping-1",
                serviceExternalId="service-1",
                repoFullName="Acme/API",
                repoProvider="github",
                mappingKind="explicit",
                ruleId="rule-1",
                validFrom="2026-07-22T00:00:00Z",
                isActive=True,
            ),
        ),
    ]


async def _capture_legacy() -> tuple[dict[str, Any], list[RecordEnvelope]]:
    records = legacy_records()
    result = normalize_batch(
        org_id=ORG_ID,
        source_id=SOURCE_ID,
        source_system="github",
        source_instance="Acme/API",
        ingestion_id=INGESTION_ID,
        records=records,
    )
    assert not result.rejections
    batch = result.batch
    store = CaptureStore(ORG_ID)
    metrics_client = CaptureClient()
    metrics = ClickHouseMetricsSink(
        "clickhouse://example/default", client=metrics_client
    )
    metrics.org_id = ORG_ID
    warnings: list[SinkWriteError] = []
    with (
        patch.object(external_sinks, "datetime", FrozenDateTime),
        patch("dev_health_ops.storage.clickhouse.datetime", FrozenDateTime),
        patch(
            "dev_health_ops.metrics.sinks.clickhouse.work_graph.datetime",
            FrozenDateTime,
        ),
    ):
        await store.insert_repo(
            cast(Any, external_sinks._build_repo_object(batch.repositories[0], batch))
        )
        await store.insert_identities(
            [
                external_sinks._build_identity_row(
                    batch.identities[0], batch, index=1, warnings=warnings
                )
            ]
        )
        await store.insert_teams(
            [
                external_sinks._build_team_row(
                    batch.teams[0], batch, index=2, warnings=warnings
                )
            ]
        )
        metrics.write_work_items(
            [
                external_sinks._build_work_item_row(
                    batch.work_items[0], batch, index=3, warnings=warnings
                )
            ]
        )
        metrics.write_work_item_transitions(
            [
                external_sinks._build_transition_row(
                    batch.work_item_transitions[0],
                    batch,
                    index=4,
                    warnings=warnings,
                )
            ]
        )
        metrics.write_work_item_dependencies(
            [external_sinks._build_dependency(batch.work_item_dependencies[0], batch)]
        )
        await store.insert_git_pull_requests(
            [external_sinks._build_pr_row(batch.pull_requests[0], batch)]
        )
        await store.insert_git_pull_request_reviews(
            [cast(Any, external_sinks._build_review_row(batch.reviews[0], batch))]
        )
        await store.insert_git_commit_data(
            [external_sinks._build_commit_row(batch.commits[0], batch)]
        )
    return {**store.captured, **metrics_client.captured}, records


async def _capture_operational() -> tuple[dict[str, Any], list[RecordEnvelope]]:
    records = operational_records()
    with patch("dev_health_ops.models.operational.datetime", FrozenDateTime):
        result = normalize_batch(
            org_id=ORG_ID,
            source_id=SOURCE_ID,
            source_system="pagerduty",
            source_instance="Tenant.PagerDuty.COM",
            ingestion_id=INGESTION_ID,
            records=records,
        )
    assert not result.rejections
    store = CaptureStore(ORG_ID)
    batch = result.batch
    writes = (
        ("insert_operational_services", batch.operational_services),
        ("insert_operational_incidents", batch.operational_incidents),
        ("insert_operational_alerts", batch.operational_alerts),
        (
            "insert_operational_incident_timeline_events",
            batch.incident_timeline_events,
        ),
        ("insert_operational_incident_notes", batch.incident_notes),
        ("insert_operational_incident_responders", batch.incident_responders),
        ("insert_operational_escalation_policies", batch.escalation_policies),
        ("insert_operational_on_call_schedules", batch.on_call_schedules),
        ("insert_operational_on_call_assignments", batch.on_call_assignments),
        ("insert_operational_teams", batch.operational_teams),
        ("insert_operational_users", batch.operational_users),
        (
            "insert_operational_service_repository_mappings",
            batch.service_repository_mappings,
        ),
    )
    for method, values in writes:
        await getattr(store, method)(values)
    return store.captured, records


async def build() -> dict[str, Any]:
    legacy, legacy_input = await _capture_legacy()
    operational, operational_input = await _capture_operational()
    rows = {**legacy, **operational}
    kind_by_table = {
        "repos": "repository.v1",
        "identities": "identity.v1",
        "teams": "team.v1",
        "work_items": "work_item.v1",
        "work_item_transitions": "work_item_transition.v1",
        "work_item_dependencies": "work_item_dependency.v1",
        "git_pull_requests": "pull_request.v1",
        "git_pull_request_reviews": "review.v1",
        "git_commits": "commit.v1",
        "operational_services": "operational_service.v1",
        "operational_incidents": "operational_incident.v1",
        "operational_alerts": "operational_alert.v1",
        "operational_incident_timeline_events": "incident_timeline_event.v1",
        "operational_incident_notes": "incident_note.v1",
        "operational_incident_responders": "incident_responder.v1",
        "operational_escalation_policies": "escalation_policy.v1",
        "operational_on_call_schedules": "on_call_schedule.v1",
        "operational_on_call_assignments": "on_call_assignment.v1",
        "operational_teams": "operational_team.v1",
        "operational_users": "operational_user.v1",
        "operational_service_repository_mappings": "service_repository_mapping.v1",
    }
    payload_by_kind = {
        record.kind: record.payload for record in [*legacy_input, *operational_input]
    }
    return {
        "fixed_now": _canonical(NOW),
        "org_id": ORG_ID,
        "source_id": str(SOURCE_ID),
        "sources": {
            "legacy": {"system": "github", "instance": "Acme/API"},
            "operational": {
                "system": "pagerduty",
                "instance": "Tenant.PagerDuty.COM",
            },
        },
        "rows": {
            kind: {
                "payload": _canonical(payload_by_kind[kind]),
                "table": table,
                **rows[table],
            }
            for table, kind in kind_by_table.items()
        },
        "edge_cases": {
            "github_pr": derive_work_item_id("github", "Acme/API", "7", "pr"),
            "gitlab_merge_request": derive_work_item_id(
                "gitlab", "Group/Project", "9", "merge_request"
            ),
            "jira_issue": derive_work_item_id("jira", None, "ABC-42"),
            "non_git_repo_uuid": str(uuid.UUID(int=0)),
            "repo_uuid": str(derive_repo_uuid("github", "Acme/API", "Acme/API")),
            "empty_identity": "",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = json.dumps(asyncio.run(build()), indent=2, sort_keys=True) + "\n"
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != rendered:
            print(f"{OUTPUT.relative_to(ROOT)} is stale", file=sys.stderr)
            return 1
        return 0
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
