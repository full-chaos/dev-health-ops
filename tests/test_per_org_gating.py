"""Tests for per-org feature gating (SaaS mode).

Validates that require_feature and has_feature correctly delegate to
the DB-backed FeatureService when org_id is provided, and fall back
to the global LicenseManager when it is not.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from dev_health_ops.licensing.gating import (
    LicenseManager,
    LicenseAuditLogger,
    _check_feature_for_org,
    _extract_org_id,
    has_feature,
    require_feature,
)


@pytest.fixture(autouse=True)
def reset_singletons():
    LicenseManager.reset()
    LicenseAuditLogger.reset()
    yield
    LicenseManager.reset()
    LicenseAuditLogger.reset()


class TestExtractOrgId:
    def test_returns_org_id_from_kwargs(self):
        assert _extract_org_id((), {"org_id": "abc-123"}) == "abc-123"

    def test_returns_none_when_missing(self):
        assert _extract_org_id((), {"session": "something"}) is None

    def test_returns_none_for_empty_kwargs(self):
        assert _extract_org_id((), {}) is None


class TestCheckFeatureForOrg:
    @patch("dev_health_ops.db.get_postgres_session_sync")
    @patch("dev_health_ops.api.services.licensing.FeatureService")
    def test_allows_feature_when_service_allows(self, MockFeatureSvc, mock_session_ctx):
        org_uuid = uuid.uuid4()
        mock_session = MagicMock()
        mock_session_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_ctx.return_value.__exit__ = MagicMock(return_value=False)

        MockFeatureSvc.return_value.check_feature_access.return_value = MagicMock(
            allowed=True
        )
        result = _check_feature_for_org(str(org_uuid), "team_dashboard")

        assert result is True

    @patch("dev_health_ops.db.get_postgres_session_sync")
    @patch("dev_health_ops.api.services.licensing.FeatureService")
    def test_denies_feature_when_service_denies(self, MockFeatureSvc, mock_session_ctx):
        org_uuid = uuid.uuid4()
        mock_session = MagicMock()
        mock_session_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_ctx.return_value.__exit__ = MagicMock(return_value=False)

        MockFeatureSvc.return_value.check_feature_access.return_value = MagicMock(
            allowed=False, reason="Requires enterprise tier"
        )
        result = _check_feature_for_org(str(org_uuid), "sso")

        assert result is False

    def test_falls_back_to_global_on_invalid_uuid(self):
        LicenseManager.initialize()
        result = _check_feature_for_org("not-a-uuid", "basic_analytics")
        assert result is True

    @patch(
        "dev_health_ops.db.get_postgres_session_sync",
        side_effect=Exception("DB unavailable"),
    )
    def test_falls_back_to_global_on_db_error(self, _mock):
        LicenseManager.initialize()
        result = _check_feature_for_org(str(uuid.uuid4()), "basic_analytics")
        assert result is True


class TestHasFeatureWithOrgId:
    def test_without_org_id_uses_global(self):
        LicenseManager.initialize()
        assert has_feature("basic_analytics") is True
        assert has_feature("sso") is False

    @patch("dev_health_ops.licensing.gating._check_feature_for_org")
    def test_with_org_id_delegates_to_per_org(self, mock_check):
        mock_check.return_value = True
        org_id = str(uuid.uuid4())
        result = has_feature("sso", org_id=org_id)
        assert result is True
        mock_check.assert_called_once_with(org_id, "sso")

    @patch("dev_health_ops.licensing.gating._check_feature_for_org")
    def test_with_org_id_denied(self, mock_check):
        mock_check.return_value = False
        result = has_feature("sso", org_id=str(uuid.uuid4()))
        assert result is False


class TestRequireFeatureWithOrgId:
    @patch("dev_health_ops.licensing.gating._check_feature_for_org")
    def test_sync_passes_org_id_from_kwargs(self, mock_check):
        mock_check.return_value = True
        org_id = str(uuid.uuid4())

        @require_feature("sso", raise_http=False)
        def my_endpoint(org_id: str = "default"):
            return "success"

        result = my_endpoint(org_id=org_id)
        assert result == "success"
        mock_check.assert_called_once_with(org_id, "sso")

    @patch("dev_health_ops.licensing.gating._check_feature_for_org")
    def test_sync_denied_with_org_id_raises_http(self, mock_check):
        mock_check.return_value = False
        org_id = str(uuid.uuid4())

        @require_feature("sso", required_tier="enterprise", raise_http=True)
        def my_endpoint(org_id: str = "default"):
            return "success"

        with pytest.raises(HTTPException) as exc_info:
            my_endpoint(org_id=org_id)

        assert exc_info.value.status_code == 402
        assert exc_info.value.detail["feature"] == "sso"

    @pytest.mark.asyncio
    @patch("dev_health_ops.licensing.gating._check_feature_for_org")
    async def test_async_passes_org_id_from_kwargs(self, mock_check):
        mock_check.return_value = True
        org_id = str(uuid.uuid4())

        @require_feature("sso", raise_http=False)
        async def my_endpoint(org_id: str = "default"):
            return "async success"

        result = await my_endpoint(org_id=org_id)
        assert result == "async success"
        mock_check.assert_called_once_with(org_id, "sso")

    @pytest.mark.asyncio
    @patch("dev_health_ops.licensing.gating._check_feature_for_org")
    async def test_async_denied_with_org_id(self, mock_check):
        mock_check.return_value = False
        org_id = str(uuid.uuid4())

        @require_feature("sso", raise_http=True)
        async def my_endpoint(org_id: str = "default"):
            return "async success"

        with pytest.raises(HTTPException) as exc_info:
            await my_endpoint(org_id=org_id)

        assert exc_info.value.status_code == 402

    def test_without_org_id_uses_global_path(self):
        LicenseManager.initialize()

        @require_feature("basic_analytics", raise_http=False)
        def my_endpoint():
            return "success"

        assert my_endpoint() == "success"

    def test_without_org_id_denied_uses_global_path(self):
        LicenseManager.initialize()

        @require_feature("sso", raise_http=True)
        def my_endpoint():
            return "success"

        with pytest.raises(HTTPException) as exc_info:
            my_endpoint()
        assert exc_info.value.status_code == 402


class TestGetEntitlementsWithOrgId:
    def test_without_org_id_returns_global(self):
        from dev_health_ops.licensing.gating import get_entitlements

        LicenseManager.initialize()
        result = get_entitlements()
        assert result["tier"] == "community"
        assert result["is_licensed"] is False

    @patch("dev_health_ops.db.get_postgres_session_sync")
    @patch("dev_health_ops.api.services.licensing.TierLimitService")
    @patch("dev_health_ops.api.services.licensing.FeatureService")
    def test_with_org_id_queries_db(
        self, MockFeatureSvc, MockLimitSvc, mock_session_ctx
    ):
        from dev_health_ops.licensing.gating import get_entitlements

        org_uuid = uuid.uuid4()
        mock_session = MagicMock()
        mock_session_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_limits = {"max_users": 25, "max_repos": 10}
        MockLimitSvc.return_value.get_all_limits.return_value = mock_limits
        MockFeatureSvc.return_value.has_feature.return_value = True

        result = get_entitlements(org_id=str(org_uuid))

        assert result["limits"] == mock_limits
        assert result["is_licensed"] is True

    @patch(
        "dev_health_ops.db.get_postgres_session_sync",
        side_effect=Exception("DB down"),
    )
    def test_with_org_id_falls_back_on_error(self, _mock):
        from dev_health_ops.licensing.gating import get_entitlements

        LicenseManager.initialize()
        result = get_entitlements(org_id=str(uuid.uuid4()))
        assert result["tier"] == "community"
