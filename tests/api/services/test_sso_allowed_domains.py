"""Enforcement tests for SSO allowed_domains (CHAOS security sprint)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.api.services.sso import SSOProcessingError, SSOService
from dev_health_ops.models.sso import SSOProvider


def _provider(allowed_domains, auto_provision=True):
    return SSOProvider(
        org_id=uuid.uuid4(),
        name="Acme SSO",
        protocol="oidc",
        config={},
        auto_provision_users=auto_provision,
        allowed_domains=allowed_domains,
    )


def _service_with_provider(provider):
    session = MagicMock()
    session.execute = AsyncMock(
        return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None))
    )
    session.add = MagicMock()
    session.flush = AsyncMock()
    svc = SSOService(session)
    svc.get_provider = AsyncMock(return_value=provider)
    return svc


@pytest.mark.asyncio
async def test_disallowed_domain_is_rejected_on_autoprovision():
    """A user whose email domain is not in allowed_domains must 403."""
    provider = _provider(allowed_domains=["acme.com"])
    svc = _service_with_provider(provider)

    with pytest.raises(SSOProcessingError, match="domain"):
        await svc.provision_or_get_user(
            org_id=provider.org_id,
            email="attacker@evil.com",
            name="A Person",
            provider_id=provider.id,
            external_id="ext-1",
        )


@pytest.mark.asyncio
async def test_allowed_domain_is_accepted_case_insensitive():
    """Email domain matching (case-insensitive) must succeed."""
    provider = _provider(allowed_domains=["Acme.COM"])
    svc = _service_with_provider(provider)

    user, _membership, returned_provider = await svc.provision_or_get_user(
        org_id=provider.org_id,
        email="alice@acme.com",
        name="Alice",
        provider_id=provider.id,
        external_id="ext-2",
    )
    assert user.email == "alice@acme.com"
    assert returned_provider is provider


@pytest.mark.asyncio
async def test_empty_allowed_domains_list_allows_all():
    """When allowed_domains is None or empty, any domain is accepted (no regression)."""
    provider = _provider(allowed_domains=None)
    svc = _service_with_provider(provider)

    user, _m, _p = await svc.provision_or_get_user(
        org_id=provider.org_id,
        email="any@anything.io",
        name="Any",
        provider_id=provider.id,
        external_id="ext-3",
    )
    assert user.email == "any@anything.io"
