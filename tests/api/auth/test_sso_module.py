"""Tests for SSO module extraction.

Verifies that the SSO sub-package loads correctly and registers
the expected endpoints on the parent auth router.
"""
from __future__ import annotations

import importlib

import pytest


class TestSSOModuleStructure:
    """Verify the SSO sub-package is importable and exposes the right API."""

    def test_sso_package_exports_router(self):
        mod = importlib.import_module("dev_health_ops.api.auth.sso")
        assert hasattr(mod, "sso_router")

    def test_sso_router_has_no_prefix(self):
        """Sub-router must not define its own prefix — the parent router provides it."""
        from dev_health_ops.api.auth.sso import sso_router

        assert sso_router.prefix == ""

    def test_sso_router_has_sso_tag(self):
        from dev_health_ops.api.auth.sso import sso_router

        assert "sso" in sso_router.tags

    def test_sso_endpoints_registered(self):
        from dev_health_ops.api.auth.sso import sso_router

        paths = {route.path for route in sso_router.routes}
        # SAML
        assert "/saml/{provider_id}/initiate" in paths
        assert "/saml/{provider_id}/acs" in paths
        assert "/saml/{provider_id}/metadata" in paths
        # OIDC
        assert "/oidc/{provider_id}/authorize" in paths
        assert "/oidc/{provider_id}/callback" in paths
        # OAuth
        assert "/oauth/{provider_id}/authorize" in paths
        assert "/oauth/{provider_id}/callback" in paths
        # Provider CRUD
        assert "/sso/providers" in paths
        assert "/sso/providers/{provider_id}" in paths

    def test_parent_router_includes_sso_routes(self):
        """The main auth router should include SSO routes via include_router."""
        from dev_health_ops.api.auth.router import router

        paths = {route.path for route in router.routes}
        # SSO routes should appear under the parent prefix
        assert "/api/v1/auth/sso/providers" in paths
        assert "/api/v1/auth/saml/{provider_id}/acs" in paths

    def test_parent_router_still_has_core_auth(self):
        """Core auth endpoints (login, register, etc.) remain on the parent router."""
        from dev_health_ops.api.auth.router import router

        paths = {route.path for route in router.routes}
        assert "/api/v1/auth/login" in paths
        assert "/api/v1/auth/register" in paths
        assert "/api/v1/auth/me" in paths
        assert "/api/v1/auth/refresh" in paths
        assert "/api/v1/auth/validate" in paths
