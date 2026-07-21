from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from dev_health_ops.metrics.active_incidents import (
    IncidentWindow,
    active_incidents_query,
    deduplicate_active_incidents,
)

ORG_ID = "22222222-2222-2222-2222-222222222222"
REPO_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
START = datetime(2026, 6, 8, tzinfo=timezone.utc)


def test_active_incidents_query_projects_mapped_canonical_rows_with_org_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
    query = active_incidents_query(
        window=IncidentWindow.RESOLVED,
        org_id=ORG_ID,
        repo_filter="",
    )

    assert "FROM incidents FINAL" in query
    assert "FROM operational_incidents" in query
    assert "operational_service_repository_mappings" in query
    assert (
        "source_revision DESC, source_conflict_key DESC, ingest_revision DESC" in query
    )
    assert "LIMIT 1 BY org_id, id" in query
    assert "operational_incidents AS incident FINAL" not in query
    assert "operational_service_repository_mappings AS mapping FINAL" not in query
    assert "INNER JOIN repos AS repo FINAL" in query
    assert "mapping.repo_id = repo.id" in query
    assert "mapping.org_id = repo.org_id" in query
    assert query.count("WHERE org_id = {org_id:String}") >= 2
    assert "incident.service_id = mapping.service_id" in query
    assert "repo_id IS NOT NULL" in query
    assert "is_active = 1" in query
    assert "valid_from <= {as_of:DateTime64(6, 'UTC')}" in query
    assert "resolved_at IS NOT NULL" in query
    assert "incident.id AS incident_id" in query


def test_active_incidents_query_does_not_project_canonical_rows_without_org_scope() -> (
    None
):
    query = active_incidents_query(
        window=IncidentWindow.STARTED,
        org_id="",
        repo_filter="",
    )

    assert "FROM incidents FINAL" in query
    assert "operational_incidents" not in query
    assert "operational_service_repository_mappings" not in query


def test_deduplicate_active_incidents_preserves_legacy_row_for_shared_identity() -> (
    None
):
    legacy = {
        "repo_id": REPO_ID,
        "incident_id": "shared-incident",
        "status": "resolved",
        "started_at": START,
        "resolved_at": START,
        "last_synced": START,
    }
    canonical = {
        "repo_id": REPO_ID,
        "incident_id": "shared-incident",
        "status": "resolved",
        "started_at": START,
        "resolved_at": START,
        "last_synced": START,
    }

    rows = deduplicate_active_incidents([legacy, canonical])

    assert rows == [legacy]


def test_deduplicate_active_incidents_keeps_mapped_repositories_separate() -> None:
    second_repo_id = uuid.uuid4()
    row = {
        "repo_id": REPO_ID,
        "incident_id": "pd-incident",
        "status": "resolved",
        "started_at": START,
        "resolved_at": START,
        "last_synced": START,
    }

    rows = deduplicate_active_incidents([row, {**row, "repo_id": second_repo_id}])

    assert {item["repo_id"] for item in rows} == {REPO_ID, second_repo_id}


def test_deduplicate_active_incidents_keeps_distinct_canonical_identities() -> None:
    first = {
        "repo_id": REPO_ID,
        "incident_id": "canonical-provider-instance-one",
        "status": "resolved",
        "started_at": START,
        "resolved_at": START,
        "last_synced": START,
    }
    second = {**first, "incident_id": "canonical-provider-instance-two"}

    rows = deduplicate_active_incidents([first, second])

    assert rows == [first, second]
