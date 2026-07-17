from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import UUID

import pytest

from dev_health_ops.backfill.operational import (
    LegacyIncidentRepositoryRow,
    map_legacy_issue_incident_batches,
)
from dev_health_ops.backfill.operational_clickhouse import (
    _load_legacy_incident_repository_rows,
    _recover_provider_instance_id,
)

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)


def test_legacy_incident_backfill_preserves_distinct_global_issue_ids() -> None:
    # Given: distinct provider-global issue ids from separate repositories in one organization.
    rows = (
        LegacyIncidentRepositoryRow(
            org_id="org-a",
            repo_id=UUID("00000000-0000-0000-0000-000000000101"),
            repo_full_name="acme/api",
            provider="github",
            provider_instance_id="github.com",
            incident_id="incident-2",
            status="closed",
            started_at=_AT,
            resolved_at=_AT,
            source_version_at=_AT,
        ),
        LegacyIncidentRepositoryRow(
            org_id="org-a",
            repo_id=UUID("00000000-0000-0000-0000-000000000102"),
            repo_full_name="acme/worker",
            provider="github",
            provider_instance_id="github.com",
            incident_id="incident-1",
            status="closed",
            started_at=_AT,
            resolved_at=_AT,
            source_version_at=_AT,
        ),
    )

    # When: legacy incident and repository rows are mapped into canonical batches.
    batches = map_legacy_issue_incident_batches(rows)

    # Then: provider-global incident identities remain distinct.
    assert len(batches) == 1
    assert len(batches[0].incidents) == 2
    assert len(batches[0].services) == 2
    assert {
        mapping.repo_full_name for mapping in batches[0].service_repository_mappings
    } == {
        "acme/api",
        "acme/worker",
    }


def test_legacy_incident_backfill_separates_organizations_and_provider_instances() -> (
    None
):
    # Given: matching legacy identity fields across organization and provider-instance boundaries.
    rows = (
        LegacyIncidentRepositoryRow(
            org_id="org-a",
            repo_id=UUID("00000000-0000-0000-0000-000000000101"),
            repo_full_name="acme/api",
            provider="gitlab",
            provider_instance_id="https://gitlab.com",
            incident_id="incident-1",
            status="opened",
            started_at=_AT,
            resolved_at=None,
            source_version_at=_AT,
        ),
        LegacyIncidentRepositoryRow(
            org_id="org-b",
            repo_id=UUID("00000000-0000-0000-0000-000000000101"),
            repo_full_name="acme/api",
            provider="gitlab",
            provider_instance_id="https://gitlab.example.com",
            incident_id="incident-1",
            status="opened",
            started_at=_AT,
            resolved_at=None,
            source_version_at=_AT,
        ),
    )

    # When: rows are grouped into homogeneous canonical batches.
    batches = map_legacy_issue_incident_batches(rows)

    # Then: each canonical identity remains isolated by source context.
    assert len(batches) == 2
    assert {batch.org_id for batch in batches} == {"org-a", "org-b"}
    assert {batch.provider_instance_id for batch in batches} == {
        "gitlab.com",
        "gitlab.example.com",
    }
    assert len({batch.incidents[0].id for batch in batches}) == 2


def test_backfill_recovery_skips_path_only_github_host() -> None:
    # Given: a legacy repository whose only host recovery value is path-only.
    settings = {"github_instance_url": "acme/api"}

    # When: the ClickHouse backfill recovers its provider instance.
    provider_instance_id = _recover_provider_instance_id("github", settings, None)

    # Then: no bogus canonical identity can be fabricated for the repository.
    assert provider_instance_id is None


@pytest.mark.asyncio
async def test_clickhouse_backfill_skips_repo_with_path_only_github_host() -> None:
    # Given: a legacy incident repository row with no recoverable GitHub host.
    store = Mock()
    store.client.query.return_value = Mock(
        result_rows=(
            (
                "00000000-0000-0000-0000-000000000101",
                "incident-1",
                "open",
                _AT,
                None,
                "acme/api",
                "github",
                '{"github_instance_url": "acme/api"}',
            ),
        )
    )

    # When: canonical backfill reads the legacy repository row.
    rows = await _load_legacy_incident_repository_rows(
        store,
        org_id="org-a",
        github_provider_instance_id=None,
        gitlab_provider_instance_id=None,
    )

    # Then: the malformed repository is skipped instead of receiving a bogus identity.
    assert rows == ()


@pytest.mark.asyncio
async def test_clickhouse_backfill_uses_configured_host_after_null_url() -> None:
    # Given: legacy settings with a null URL and a valid configured GitHub host.
    store = Mock()
    store.client.query.return_value = Mock(
        result_rows=(
            (
                "00000000-0000-0000-0000-000000000101",
                "incident-1",
                "open",
                _AT,
                None,
                "acme/api",
                "github",
                '{"url": null}',
            ),
        )
    )

    # When: canonical backfill reads the legacy repository row.
    rows = await _load_legacy_incident_repository_rows(
        store,
        org_id="org-a",
        github_provider_instance_id="https://ghe.acme.test:8443/api/v3",
        gitlab_provider_instance_id=None,
    )

    # Then: it uses the configured host rather than fabricating a null identity.
    assert rows[0].provider_instance_id == "ghe.acme.test:8443"


def test_backfill_recovery_normalizes_enterprise_github_host_like_native_path() -> None:
    # Given: an enterprise API URL persisted by the native GitHub path.
    settings = {"github_instance_url": "https://GHE.Acme.test:8443/api/v3"}

    # When: the legacy ClickHouse backfill recovers the provider instance.
    provider_instance_id = _recover_provider_instance_id("github", settings, None)

    # Then: it is the same canonical host emitted by the native mapper.
    assert provider_instance_id == "ghe.acme.test:8443"
