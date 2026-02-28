"""Tests for audit service impersonation metadata injection.

Verifies that AuditService.log() attaches impersonation context fields
(impersonated_by, impersonation_target, impersonation_org) to
request_metadata when an active ImpersonationContext is set, and omits
those fields when none is active.

No real database — the SQLAlchemy session is mocked.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.api.services.audit import AuditService
from dev_health_ops.api.services.auth import (
    _impersonation_ctx,
    set_impersonation_context,
)
from dev_health_ops.models.audit import AuditAction, AuditResourceType


def _make_mock_session() -> AsyncMock:
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    return session


def _get_logged_metadata(session: AsyncMock) -> dict | None:
    """Extract request_metadata from the AuditLog passed to session.add()."""
    assert session.add.called, "session.add() was never called"
    audit_log = session.add.call_args[0][0]
    return audit_log.request_metadata


# ---------------------------------------------------------------------------
# Test: impersonation context → metadata fields present
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_log_includes_impersonation_fields_when_context_active():
    """When an ImpersonationContext is active, request_metadata includes
    impersonated_by, impersonation_target, and impersonation_org.
    """
    real_user_id = str(uuid.uuid4())
    target_user_id = str(uuid.uuid4())
    target_org_id = str(uuid.uuid4())

    token = set_impersonation_context(
        target_user_id=target_user_id,
        target_org_id=target_org_id,
        target_role="member",
        real_user_id=real_user_id,
    )
    try:
        session = _make_mock_session()
        service = AuditService(session)

        await service.log(
            org_id=uuid.UUID(target_org_id),
            action=AuditAction.CREATE,
            resource_type=AuditResourceType.SESSION,
            resource_id="res-1",
        )

        metadata = _get_logged_metadata(session)
        assert metadata is not None
        assert metadata["impersonated_by"] == real_user_id
        assert metadata["impersonation_target"] == target_user_id
        assert metadata["impersonation_org"] == target_org_id
    finally:
        _impersonation_ctx.reset(token)
        _impersonation_ctx.set(None)


# ---------------------------------------------------------------------------
# Test: no impersonation context → metadata fields absent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_log_omits_impersonation_fields_when_no_context():
    """When no ImpersonationContext is active, request_metadata does NOT
    contain impersonated_by, impersonation_target, or impersonation_org.
    """
    try:
        _impersonation_ctx.set(None)

        session = _make_mock_session()
        service = AuditService(session)
        org_id = uuid.uuid4()

        await service.log(
            org_id=org_id,
            action=AuditAction.LOGIN,
            resource_type=AuditResourceType.SESSION,
            resource_id="res-2",
        )

        metadata = _get_logged_metadata(session)
        # metadata may be None or a dict, but must not contain impersonation keys
        if metadata is not None:
            assert "impersonated_by" not in metadata
            assert "impersonation_target" not in metadata
            assert "impersonation_org" not in metadata
    finally:
        _impersonation_ctx.set(None)


# ---------------------------------------------------------------------------
# Test: impersonation fields survive alongside extra_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_log_impersonation_fields_merge_with_extra_metadata():
    """Impersonation context fields are merged into request_metadata alongside
    any extra_metadata the caller provides.
    """
    real_user_id = str(uuid.uuid4())
    target_user_id = str(uuid.uuid4())
    target_org_id = str(uuid.uuid4())

    token = set_impersonation_context(
        target_user_id=target_user_id,
        target_org_id=target_org_id,
        target_role="viewer",
        real_user_id=real_user_id,
    )
    try:
        session = _make_mock_session()
        service = AuditService(session)

        await service.log(
            org_id=uuid.UUID(target_org_id),
            action=AuditAction.UPDATE,
            resource_type=AuditResourceType.SESSION,
            resource_id="res-3",
            extra_metadata={"custom_key": "custom_value"},
        )

        metadata = _get_logged_metadata(session)
        assert metadata is not None
        # Custom field preserved
        assert metadata["custom_key"] == "custom_value"
        # Impersonation fields present
        assert metadata["impersonated_by"] == real_user_id
        assert metadata["impersonation_target"] == target_user_id
        assert metadata["impersonation_org"] == target_org_id
    finally:
        _impersonation_ctx.reset(token)
        _impersonation_ctx.set(None)
