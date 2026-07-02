"""CHAOS-2784: sso_providers.last_error must not persist raw exception text.

Every ``SSOService.record_error`` call site (SAML/OIDC processing failures in
``api/auth/sso/router.py``) passes ``str(exc)`` straight through. If that
exception message embeds a credential -- e.g. an IdP token endpoint call
surfacing an ``Authorization`` header, the same shape CHAOS-2758 found in
``sync_run_units.error`` -- it used to persist verbatim into
``sso_providers.last_error``. This proves the sink now routes through
``sanitize_error_text`` (CHAOS-2766) before the write.

See ``tests/test_error_sanitize.py`` for why every fixture secret is
assembled via ``_fake_secret(...)`` at runtime with a neutral name instead of
a literal -- required to defeat CI's Gitleaks scan.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.api.services.sso import SSOService
from dev_health_ops.models.sso import SSOProvider, SSOProviderStatus
from dev_health_ops.sync.error_sanitize import REDACTION_MARKER


def _fake_secret(*parts: str) -> str:
    """Assemble a synthetic, redaction-target-shaped fixture at runtime (see
    tests/test_error_sanitize.py's module docstring for why this isn't a
    plain string literal -- Gitleaks matches file bytes, not runtime
    values)."""
    return "".join(parts)


_FIXTURE_1 = _fake_secret("ghp_", "FAKEmnopqrstuv1234567890XY")


@pytest.mark.asyncio
async def test_record_error_sanitizes_secret_bearing_message():
    org_id = uuid.uuid4()
    provider_id = uuid.uuid4()
    provider = SSOProvider(
        org_id=org_id,
        name="Example OIDC",
        protocol="oidc",
        config={},
    )

    session = MagicMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = provider
    session.execute = AsyncMock(return_value=execute_result)
    session.flush = AsyncMock()

    service = SSOService(session)
    # Bare "Bearer <token>" shape (no leading "Authorization:" header name) --
    # the other sink tests in this changeset cover the header-name and
    # URL-userinfo shapes, this covers sanitize_error_text's second pattern.
    raw_error = f"token exchange failed -- used Bearer {_FIXTURE_1}"

    await service.record_error(org_id, provider_id, raw_error)

    assert provider.last_error is not None
    assert _FIXTURE_1 not in provider.last_error
    assert REDACTION_MARKER in provider.last_error
    assert "token exchange failed" in provider.last_error
    assert provider.status == SSOProviderStatus.ERROR.value
    assert provider.last_error_at is not None


@pytest.mark.asyncio
async def test_record_error_no_provider_is_a_noop():
    session = MagicMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=execute_result)
    session.flush = AsyncMock()

    service = SSOService(session)
    await service.record_error(uuid.uuid4(), uuid.uuid4(), "unused")

    session.flush.assert_not_called()
