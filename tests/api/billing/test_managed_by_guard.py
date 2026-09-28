"""Tests for CHAOS-2301: managed_by guard prevents Stripe from clobbering manual tiers.

Covers:
- OrganizationService.update sets managed_by='manual' on org and license
- OrganizationService._sync_license_tier sets managed_by='manual' on license

The Python Stripe webhook route's own _revoke_license/_persist_license guard
tests (CHAOS-6258/7033) moved to Go: internal/api/billing/subscriptionhooks.go
and internal/apiservice/billingvenue's frozen TestVenueOracleBillingWebhook
(webhookSubOrgs[2]/[9], seed.orgC -- the managed_by='manual' cases).
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

# ---------------------------------------------------------------------------
# OrganizationService.update sets managed_by='manual'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_org_service_update_sets_managed_by_manual_on_org(
    monkeypatch: pytest.MonkeyPatch,
):
    """OrganizationService.update must set org.managed_by='manual' when tier changes."""
    from dev_health_ops.api.services.users import OrganizationService

    org_id = uuid.uuid4()
    org = SimpleNamespace(
        id=org_id,
        slug="test-org",
        name="Test Org",
        description=None,
        tier="community",
        managed_by="stripe",
        is_active=True,
        settings={},
        updated_at=None,
    )

    mock_session = AsyncMock()

    async def _get_by_id(_id: str):
        return org

    svc = OrganizationService(mock_session)
    monkeypatch.setattr(svc, "get_by_id", _get_by_id)

    # Stub _sync_license_tier to avoid DB calls
    sync_license_tier = AsyncMock()
    monkeypatch.setattr(svc, "_sync_license_tier", sync_license_tier)

    result = await svc.update(str(org_id), tier="enterprise")

    assert result is org
    assert org.tier == "enterprise"
    assert org.managed_by == "manual"
    sync_license_tier.assert_awaited_once_with(org_id, "enterprise")


@pytest.mark.asyncio
async def test_org_service_create_sets_managed_by_manual_for_paid_tier():
    from dev_health_ops.api.services.users import OrganizationService

    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.execute.return_value = MagicMock(
        scalar_one_or_none=MagicMock(return_value=None)
    )

    svc = OrganizationService(mock_session)
    org = await svc.create(name="Enterprise Org", tier="enterprise")

    assert org.tier == "enterprise"
    assert org.managed_by == "manual"


@pytest.mark.asyncio
async def test_org_service_create_leaves_community_stripe_managed():
    from dev_health_ops.api.services.users import OrganizationService

    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.execute.return_value = MagicMock(
        scalar_one_or_none=MagicMock(return_value=None)
    )

    svc = OrganizationService(mock_session)
    org = await svc.create(name="Community Org")

    assert org.tier == "community"
    assert org.managed_by == "stripe"


@pytest.mark.asyncio
async def test_org_service_update_no_tier_does_not_set_managed_by(
    monkeypatch: pytest.MonkeyPatch,
):
    """OrganizationService.update must NOT set managed_by when tier is not changed."""
    from dev_health_ops.api.services.users import OrganizationService

    org_id = uuid.uuid4()
    org = SimpleNamespace(
        id=org_id,
        slug="test-org",
        name="Test Org",
        description=None,
        tier="team",
        managed_by="stripe",
        is_active=True,
        settings={},
        updated_at=None,
    )

    mock_session = AsyncMock()

    async def _get_by_id(_id: str):
        return org

    svc = OrganizationService(mock_session)
    monkeypatch.setattr(svc, "get_by_id", _get_by_id)
    sync_license_tier = AsyncMock()
    monkeypatch.setattr(svc, "_sync_license_tier", sync_license_tier)

    await svc.update(str(org_id), name="New Name")

    # managed_by must remain unchanged
    assert org.managed_by == "stripe"
    sync_license_tier.assert_not_awaited()


@pytest.mark.asyncio
async def test_org_service_update_same_tier_preserves_managed_by(
    monkeypatch: pytest.MonkeyPatch,
):
    from dev_health_ops.api.services.users import OrganizationService

    org_id = uuid.uuid4()
    org = SimpleNamespace(
        id=org_id,
        slug="test-org",
        name="Test Org",
        description=None,
        tier="team",
        managed_by="stripe",
        is_active=True,
        settings={},
        updated_at=None,
    )

    mock_session = AsyncMock()

    async def _get_by_id(_id: str):
        return org

    svc = OrganizationService(mock_session)
    monkeypatch.setattr(svc, "get_by_id", _get_by_id)
    sync_license_tier = AsyncMock()
    monkeypatch.setattr(svc, "_sync_license_tier", sync_license_tier)

    await svc.update(str(org_id), name="New Name", tier="team")

    assert org.managed_by == "stripe"
    sync_license_tier.assert_not_awaited()


@pytest.mark.asyncio
async def test_org_service_update_community_keeps_stripe_manageable(
    monkeypatch: pytest.MonkeyPatch,
):
    from dev_health_ops.api.services.users import OrganizationService

    org_id = uuid.uuid4()
    org = SimpleNamespace(
        id=org_id,
        slug="test-org",
        name="Test Org",
        description=None,
        tier="enterprise",
        managed_by="manual",
        is_active=True,
        settings={},
        updated_at=None,
    )

    mock_session = AsyncMock()

    async def _get_by_id(_id: str):
        return org

    svc = OrganizationService(mock_session)
    monkeypatch.setattr(svc, "get_by_id", _get_by_id)
    sync_license_tier = AsyncMock()
    monkeypatch.setattr(svc, "_sync_license_tier", sync_license_tier)

    await svc.update(str(org_id), tier="community")

    assert org.tier == "community"
    assert org.managed_by == "stripe"
    sync_license_tier.assert_awaited_once_with(org_id, "community")


# ---------------------------------------------------------------------------
# _sync_license_tier sets managed_by='manual' on OrgLicense
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_license_tier_sets_managed_by_manual():
    """_sync_license_tier must set org_license.managed_by='manual'."""
    from dev_health_ops.api.services.users import OrganizationService

    org_id = uuid.uuid4()
    org_license = SimpleNamespace(
        org_id=org_id,
        tier="community",
        managed_by="stripe",
        updated_at=None,
    )

    mock_session = AsyncMock()
    mock_session.execute.return_value = MagicMock(
        scalar_one_or_none=MagicMock(return_value=org_license)
    )

    svc = OrganizationService(mock_session)
    await svc._sync_license_tier(org_id, "enterprise")

    assert org_license.tier == "enterprise"
    assert org_license.managed_by == "manual"


@pytest.mark.asyncio
async def test_sync_license_tier_no_license_row_is_noop():
    """_sync_license_tier with no OrgLicense row must not raise."""
    from dev_health_ops.api.services.users import OrganizationService

    org_id = uuid.uuid4()

    mock_session = AsyncMock()
    mock_session.execute.return_value = MagicMock(
        scalar_one_or_none=MagicMock(return_value=None)
    )

    svc = OrganizationService(mock_session)
    # Should not raise
    await svc._sync_license_tier(org_id, "enterprise")


@pytest.mark.asyncio
async def test_sync_license_tier_community_keeps_license_stripe_manageable():
    from dev_health_ops.api.services.users import OrganizationService

    org_id = uuid.uuid4()
    org_license = SimpleNamespace(
        org_id=org_id,
        tier="enterprise",
        managed_by="manual",
        updated_at=None,
    )

    mock_session = AsyncMock()
    mock_session.execute.return_value = MagicMock(
        scalar_one_or_none=MagicMock(return_value=org_license)
    )

    svc = OrganizationService(mock_session)
    await svc._sync_license_tier(org_id, "community")

    assert org_license.tier == "community"
    assert org_license.managed_by == "stripe"


def test_managed_by_migration_backfills_likely_manual_rows():
    migration = (
        "src/dev_health_ops/alembic/versions/0011_add_managed_by_to_org_license.py"
    )
    source = __import__("pathlib").Path(migration).read_text()

    assert "UPDATE org_licenses" in source
    assert "coalesce(tier, 'community') <> 'community'" in source
    assert "coalesce(is_valid, false) = true" in source
    assert "FROM subscriptions" in source
    assert "subscriptions.status IN ('active', 'trialing', 'past_due')" in source
    assert "NOT EXISTS" in source
    assert "org_licenses.license_type" not in source
    assert "stripe_customer_id IS NULL" not in source
    assert "UPDATE organizations" in source
