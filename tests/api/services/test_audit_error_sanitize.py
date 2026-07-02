"""CHAOS-2784: audit_logs.error_message must not persist raw exception text.

Two independent writers populate ``audit_logs.error_message`` with raw,
often exception-derived text: ``AuditService.log`` (and its
``log_create``/``log_update``/``log_delete``/``log_login`` wrappers) and the
standalone ``emit_audit_log`` helper used by the auth routers (SSO
SAML/OIDC failures pass ``str(exc)`` straight through -- see
``api/auth/sso/router.py``). If either exception message embeds a
credential -- e.g. an IdP/DB/broker URL with userinfo, or a bare provider
token -- it used to persist verbatim. This proves both sinks now route
through ``sanitize_error_text`` (CHAOS-2766) before the write, and that
``emit_audit_log``'s transaction semantics (``db.add`` only, no
commit/flush -- CHAOS-2498's emit-then-raise rollback trap) are unchanged.

See ``tests/test_error_sanitize.py`` for why every fixture secret is
assembled via ``_fake_secret(...)`` at runtime with a neutral name instead of
a literal -- required to defeat CI's Gitleaks scan.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.api.services.audit import AuditService
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.sync.error_sanitize import REDACTION_MARKER


def _fake_secret(*parts: str) -> str:
    """Assemble a synthetic, redaction-target-shaped fixture at runtime (see
    tests/test_error_sanitize.py's module docstring for why this isn't a
    plain string literal -- Gitleaks matches file bytes, not runtime
    values)."""
    return "".join(parts)


# URL-userinfo shape (a broker/DB connection string surfacing in an
# exception message).
_FIXTURE_1 = _fake_secret("brokerXuser", ":", "brokerXvalue456")
# Provider PAT-prefix shape, self-identifying regardless of surrounding text.
_FIXTURE_2 = _fake_secret("ghp_", "FAKEcdefghijklmno1234567890")


@pytest.mark.asyncio
async def test_audit_service_log_sanitizes_secret_bearing_error_message():
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()

    service = AuditService(session)
    raw_error = (
        "webhook dispatch failed -- connection string "
        f"amqp://{_FIXTURE_1}@broker.internal:5672/vhost unreachable"
    )

    audit_log = await service.log(
        org_id=uuid.uuid4(),
        action=AuditAction.UPDATE,
        resource_type=AuditResourceType.SSO_PROVIDER,
        resource_id="provider-1",
        status="failure",
        error_message=raw_error,
    )

    assert audit_log.error_message is not None
    assert _FIXTURE_1 not in audit_log.error_message
    assert REDACTION_MARKER in audit_log.error_message
    assert "webhook dispatch failed" in audit_log.error_message
    session.add.assert_called_once_with(audit_log)


def test_emit_audit_log_sanitizes_secret_bearing_error_message():
    db = MagicMock()
    raw_error = f"identity provider callback failed -- token {_FIXTURE_2} rejected"

    entry = emit_audit_log(
        db=db,
        org_id=uuid.uuid4(),
        action=AuditAction.LOGIN_FAILED,
        resource_type=AuditResourceType.SESSION,
        resource_id="user-1",
        status="failure",
        error_message=raw_error,
    )

    assert entry.error_message is not None
    assert _FIXTURE_2 not in entry.error_message
    assert REDACTION_MARKER in entry.error_message
    assert "identity provider callback failed" in entry.error_message

    # CHAOS-2498 rollback trap: emit_audit_log must stay add-only (no
    # commit/flush of its own) so the caller's get_postgres_session
    # commit-before-raise contract is untouched by this sanitize fix.
    db.add.assert_called_once_with(entry)
    db.flush.assert_not_called()
    db.commit.assert_not_called()
