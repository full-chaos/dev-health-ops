from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import UUID

import pytest

from dev_health_ops.backfill.operational_clickhouse import (
    OperationalBackfillParityError,
    _utc_datetime,
    _verify_expected_canonical_identities,
)
from dev_health_ops.providers.operational_migration import (
    IssueIncidentSource,
    map_issue_incidents,
)

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)


def _canonical_incident_batch():
    return map_issue_incidents(
        (
            IssueIncidentSource(
                org_id="org-a",
                provider="gitlab",
                provider_instance_id="gitlab.com",
                repo_id=UUID("00000000-0000-0000-0000-000000000101"),
                repo_full_name="acme/api",
                external_id="incident-1",
                issue_number="1",
                source_url="https://gitlab.com/acme/api/-/issues/1",
                labels=(),
                raw_status="closed",
                title="Incident",
                description=None,
                created_at=_AT,
                resolved_at=_AT,
                source_version_at=_AT,
                source_entity_type="incident",
            ),
        )
    )


def test_backfill_hydrates_clickhouse_timestamps_as_utc() -> None:
    # Given: ClickHouse DateTime64 values hydrated without timezone metadata.
    naive_at = _AT.replace(tzinfo=None)

    # When: a legacy Atlassian Ops timestamp enters the canonical mapper.
    hydrated = _utc_datetime(naive_at)

    # Then: canonical ordering receives a UTC-aware timestamp.
    assert hydrated == _AT
    assert hydrated.tzinfo is timezone.utc


@pytest.mark.asyncio
async def test_clickhouse_backfill_verifies_all_expected_canonical_identities() -> None:
    # Given: one canonical incident, service, and repository mapping.
    batches = (_canonical_incident_batch(),)
    incident_id = batches[0].incidents[0].id
    mapping_id = batches[0].service_repository_mappings[0].id
    store = Mock(org_id="org-a")
    store.client.query.side_effect = (
        Mock(result_rows=((incident_id,),)),
        Mock(result_rows=((mapping_id,),)),
    )

    # When: post-write parity checks read the current canonical identities.
    parity = await _verify_expected_canonical_identities(store, batches)

    # Then: the successful barrier exposes complete expected and verified counts.
    assert parity.expected_incidents == 1
    assert parity.verified_incidents == 1
    assert parity.expected_service_repository_mappings == 1
    assert parity.verified_service_repository_mappings == 1
    assert store.client.query.call_count == 2


@pytest.mark.asyncio
async def test_clickhouse_backfill_fails_when_canonical_identity_is_missing() -> None:
    # Given: expected incident and mapping identities that did not become current.
    batches = (_canonical_incident_batch(),)
    incident_id = batches[0].incidents[0].id
    mapping_id = batches[0].service_repository_mappings[0].id
    store = Mock(org_id="org-a")
    store.client.query.side_effect = (
        Mock(result_rows=()),
        Mock(result_rows=()),
    )

    # When: post-write parity cannot find either expected canonical identity.
    with pytest.raises(OperationalBackfillParityError) as exc_info:
        await _verify_expected_canonical_identities(store, batches)

    # Then: the backfill fails loudly and exposes exact missing IDs.
    assert exc_info.value.missing_incident_ids == (incident_id,)
    assert exc_info.value.missing_service_repository_mapping_ids == (mapping_id,)
    assert "canonical backfill is incomplete" in str(exc_info.value)
