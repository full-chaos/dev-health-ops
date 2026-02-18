"""Tests for feature gating.

Validates that require_feature routes through the global LicenseManager (JWT
path) and falls back to per-org OrgLicense checks for async endpoints.
"""

import inspect
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from dev_health_ops.licensing.gating import (
    LicenseManager,
    LicenseAuditLogger,
    FeatureNotLicensedError,
    _check_org_feature_async,
    has_feature,
    get_entitlements,
    require_feature,
)


@pytest.fixture(autouse=True)
def reset_singletons():
    LicenseManager.reset()
    LicenseAuditLogger.reset()
    yield
    LicenseManager.reset()
    LicenseAuditLogger.reset()


class TestHasFeatureJwtOnly:
    def test_community_tier_allows_basic_analytics(self):
        LicenseManager.initialize()
        assert has_feature("basic_analytics") is True

    def test_community_tier_denies_enterprise_features(self):
        LicenseManager.initialize()
        assert has_feature("sso", log_denial=False) is False
        assert has_feature("audit_log", log_denial=False) is False

    def test_no_org_id_parameter(self):
        sig = inspect.signature(has_feature)
        assert "org_id" not in sig.parameters

    def test_logs_denial_by_default(self):
        LicenseManager.initialize()
        with patch.object(LicenseAuditLogger, "log_feature_access_denied") as mock_log:
            has_feature("sso")
            mock_log.assert_called_once()

    def test_suppresses_denial_log_when_requested(self):
        LicenseManager.initialize()
        with patch.object(LicenseAuditLogger, "log_feature_access_denied") as mock_log:
            has_feature("sso", log_denial=False)
            mock_log.assert_not_called()


class TestGetEntitlementsJwtOnly:
    def test_returns_community_tier_unlicensed(self):
        LicenseManager.initialize()
        result = get_entitlements()
        assert result["tier"] == "community"
        assert result["is_licensed"] is False

    def test_no_org_id_parameter(self):
        sig = inspect.signature(get_entitlements)
        assert "org_id" not in sig.parameters

    def test_returns_features_and_limits(self):
        LicenseManager.initialize()
        result = get_entitlements()
        assert "features" in result
        assert "limits" in result
        assert "in_grace_period" in result


class TestRequireFeatureJwtOnly:
    def test_sync_allows_community_feature(self):
        LicenseManager.initialize()

        @require_feature("basic_analytics", raise_http=False)
        def my_endpoint():
            return "success"

        assert my_endpoint() == "success"

    def test_sync_denies_enterprise_feature(self):
        LicenseManager.initialize()

        @require_feature("sso", raise_http=True)
        def my_endpoint():
            return "success"

        with pytest.raises(HTTPException) as exc_info:
            my_endpoint()
        assert exc_info.value.status_code == 402
        assert exc_info.value.detail["feature"] == "sso"

    @pytest.mark.asyncio
    async def test_async_allows_community_feature(self):
        LicenseManager.initialize()

        @require_feature("basic_analytics", raise_http=False)
        async def my_endpoint():
            return "async success"

        result = await my_endpoint()
        assert result == "async success"

    @pytest.mark.asyncio
    async def test_async_denies_enterprise_feature(self):
        LicenseManager.initialize()

        @require_feature("sso", raise_http=True)
        async def my_endpoint():
            return "async success"

        with pytest.raises(HTTPException) as exc_info:
            await my_endpoint()
        assert exc_info.value.status_code == 402

    def test_does_not_extract_org_id_from_kwargs(self):
        LicenseManager.initialize()

        @require_feature("basic_analytics", raise_http=False)
        def my_endpoint(org_id: str = "should-be-ignored"):
            return "success"

        assert my_endpoint(org_id="some-org") == "success"

    def test_raises_feature_not_licensed_when_not_http(self):
        LicenseManager.initialize()

        @require_feature("sso", required_tier="enterprise", raise_http=False)
        def my_endpoint():
            return "success"

        with pytest.raises(FeatureNotLicensedError) as exc_info:
            my_endpoint()
        assert exc_info.value.feature == "sso"
        assert exc_info.value.required_tier == "enterprise"


class TestCheckOrgFeatureAsync:
    @pytest.mark.asyncio
    async def test_returns_true_for_enterprise_org(self):
        org_license = MagicMock()
        org_license.tier = "enterprise"
        org_license.features_override = None

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = org_license

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result

        org_id = str(uuid.uuid4())
        result = await _check_org_feature_async(
            "ip_allowlist", {"session": mock_session, "org_id": org_id}
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_for_community_org(self):
        org_license = MagicMock()
        org_license.tier = "community"
        org_license.features_override = None

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = org_license

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result

        org_id = str(uuid.uuid4())
        result = await _check_org_feature_async(
            "ip_allowlist", {"session": mock_session, "org_id": org_id}
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_org_license(self):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result

        org_id = str(uuid.uuid4())
        result = await _check_org_feature_async(
            "ip_allowlist", {"session": mock_session, "org_id": org_id}
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_session(self):
        result = await _check_org_feature_async(
            "ip_allowlist", {"org_id": str(uuid.uuid4())}
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_org_id_is_default(self):
        result = await _check_org_feature_async(
            "ip_allowlist", {"session": AsyncMock(), "org_id": "default"}
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_respects_features_override(self):
        org_license = MagicMock()
        org_license.tier = "community"
        org_license.features_override = {"ip_allowlist": True}

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = org_license

        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result

        org_id = str(uuid.uuid4())
        result = await _check_org_feature_async(
            "ip_allowlist", {"session": mock_session, "org_id": org_id}
        )
        assert result is True


class TestRequireFeatureOrgFallback:
    @pytest.mark.asyncio
    async def test_async_allows_when_org_has_enterprise_license(self):
        LicenseManager.initialize()

        @require_feature("ip_allowlist", raise_http=True)
        async def my_endpoint(session=None, org_id=None):
            return "allowed"

        with patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ):
            result = await my_endpoint(session="mock", org_id="some-org")
        assert result == "allowed"

    @pytest.mark.asyncio
    async def test_async_denies_when_org_has_no_license(self):
        LicenseManager.initialize()

        @require_feature("ip_allowlist", raise_http=True)
        async def my_endpoint(session=None, org_id=None):
            return "allowed"

        with patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=False,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await my_endpoint(session="mock", org_id="some-org")
            assert exc_info.value.status_code == 402
            assert exc_info.value.detail["feature"] == "ip_allowlist"

    def test_sync_does_not_check_org(self):
        LicenseManager.initialize()

        @require_feature("ip_allowlist", raise_http=True)
        def my_endpoint(session=None, org_id=None):
            return "allowed"

        with pytest.raises(HTTPException) as exc_info:
            my_endpoint(session="mock", org_id="some-org")
        assert exc_info.value.status_code == 402
