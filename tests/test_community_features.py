"""Tests verifying community features work without an enterprise license.

Ensures basic auth, user/org/membership management remain accessible in community tier.
Enterprise features (SSO, audit, IP allowlist, retention) should be gated.
"""

import pytest

from dev_health_ops.licensing import LicenseManager, LicenseTier
from dev_health_ops.licensing.gating import has_feature


class TestLicenseManagerCommunityTier:
    """Verify LicenseManager defaults to community tier without a license."""

    def test_no_license_returns_community_tier(self):
        """Without a license, LicenseManager should default to community tier."""
        LicenseManager._instance = None
        LicenseManager._validator = None
        LicenseManager._license_payload = None
        LicenseManager._validation_result = None

        manager = LicenseManager.initialize(public_key=None, license_key=None)

        assert manager.tier == LicenseTier.COMMUNITY
        assert not manager.is_licensed

    def test_community_tier_denies_enterprise_features(self):
        """Community tier should deny enterprise-only features."""
        LicenseManager._instance = None
        LicenseManager._validator = None
        LicenseManager._license_payload = None
        LicenseManager._validation_result = None
        LicenseManager.initialize(public_key=None, license_key=None)

        assert has_feature("sso", log_denial=False) is False
        assert has_feature("audit_log", log_denial=False) is False
        assert has_feature("ip_allowlist", log_denial=False) is False
        assert has_feature("retention_policies", log_denial=False) is False


class TestCommunityEndpointsNotGated:
    """Verify community endpoints don't have @require_feature decorator."""

    def test_auth_router_basic_endpoints_not_gated(self):
        """Basic auth endpoints (register, login, etc.) should not be gated."""
        from dev_health_ops.api.auth.router import router

        community_endpoints = {
            "register",
            "login",
            "get_me",
            "refresh_token",
            "validate_token",
            "logout",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        community_gated = community_endpoints & gated_endpoints
        assert not community_gated, f"These should NOT be gated: {community_gated}"

    def test_admin_user_endpoints_not_gated(self):
        """User CRUD endpoints should not be gated."""
        from dev_health_ops.api.admin.router import router

        community_endpoints = {
            "list_users",
            "get_user",
            "create_user",
            "update_user",
            "delete_user",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        community_gated = community_endpoints & gated_endpoints
        assert not community_gated, f"These should NOT be gated: {community_gated}"

    def test_admin_org_endpoints_not_gated(self):
        """Organization CRUD endpoints should not be gated."""
        from dev_health_ops.api.admin.router import router

        community_endpoints = {
            "list_organizations",
            "get_organization",
            "create_organization",
            "update_organization",
            "delete_organization",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        community_gated = community_endpoints & gated_endpoints
        assert not community_gated, f"These should NOT be gated: {community_gated}"

    def test_admin_membership_endpoints_not_gated(self):
        """Membership management endpoints should not be gated."""
        from dev_health_ops.api.admin.router import router

        community_endpoints = {
            "list_members",
            "add_member",
            "update_member_role",
            "remove_member",
            "transfer_ownership",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        community_gated = community_endpoints & gated_endpoints
        assert not community_gated, f"These should NOT be gated: {community_gated}"


class TestEnterpriseEndpointsGated:
    """Verify enterprise endpoints ARE gated by @require_feature."""

    def test_sso_endpoints_are_gated(self):
        """SSO endpoints should require enterprise license."""
        from dev_health_ops.api.auth.router import router

        sso_endpoints = {
            "list_sso_providers",
            "create_sso_provider",
            "get_sso_provider",
            "update_sso_provider",
            "delete_sso_provider",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        sso_not_gated = sso_endpoints - gated_endpoints
        assert not sso_not_gated, f"These should BE gated: {sso_not_gated}"

    def test_audit_log_endpoints_are_gated(self):
        """Audit log endpoints should require enterprise license."""
        from dev_health_ops.api.admin.router import router

        audit_endpoints = {
            "list_audit_logs",
            "get_audit_log",
            "get_resource_audit_history",
            "get_user_audit_activity",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        audit_not_gated = audit_endpoints - gated_endpoints
        assert not audit_not_gated, f"These should BE gated: {audit_not_gated}"

    def test_ip_allowlist_endpoints_are_gated(self):
        """IP allowlist endpoints should require enterprise license."""
        from dev_health_ops.api.admin.router import router

        ip_endpoints = {
            "list_ip_allowlist_entries",
            "create_ip_allowlist_entry",
            "get_ip_allowlist_entry",
            "update_ip_allowlist_entry",
            "delete_ip_allowlist_entry",
            "check_ip_allowed",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        ip_not_gated = ip_endpoints - gated_endpoints
        assert not ip_not_gated, f"These should BE gated: {ip_not_gated}"

    def test_retention_policy_endpoints_are_gated(self):
        """Retention policy endpoints should require enterprise license."""
        from dev_health_ops.api.admin.router import router

        retention_endpoints = {
            "list_retention_policies",
            "create_retention_policy",
            "get_retention_policy",
            "update_retention_policy",
            "delete_retention_policy",
            "execute_retention_policy",
        }

        gated_endpoints = set()
        for route in router.routes:
            if hasattr(route, "endpoint"):
                func = route.endpoint
                if hasattr(func, "_require_feature"):
                    gated_endpoints.add(func.__name__)

        retention_not_gated = retention_endpoints - gated_endpoints
        assert not retention_not_gated, f"These should BE gated: {retention_not_gated}"
