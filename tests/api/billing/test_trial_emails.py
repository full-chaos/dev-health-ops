from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

from dev_health_ops.api.services.email import ConsoleEmailProvider, EmailService
from dev_health_ops.workers.system_ops import send_billing_notification

# Test-only constants — not user-supplied values.
_TEST_DASHBOARD_URL = "https://app.example.test/dashboard"  # noqa: S105
_TEST_UPGRADE_URL = "https://app.example.test/billing"  # noqa: S105


def _email_service() -> EmailService:
    return EmailService(
        provider=ConsoleEmailProvider(),
        from_address="billing@example.com",
    )


class _TaskStub:
    def __init__(self) -> None:
        self.request = SimpleNamespace(retries=0)
        self.max_retries = 3

    def retry(self, exc: Exception, countdown: int):
        raise RuntimeError(f"unexpected retry: {exc} (countdown={countdown})")


def _invoke_send_billing_notification(
    email_type: str,
    org_id: str,
    **kwargs,
) -> dict[str, str]:
    run_fn = getattr(send_billing_notification, "run", None)
    if callable(run_fn):
        return cast(dict[str, str], run_fn(email_type, org_id, **kwargs))
    return cast(
        dict[str, str],
        send_billing_notification(_TaskStub(), email_type, org_id, **kwargs),
    )


def _run_async_now(coro) -> None:
    asyncio.run(coro)


def test_trial_started_template_renders():
    rendered = _email_service().render_template(
        "trial_started",
        context={
            "full_name": "Alex",
            "org_name": "Acme",
            "tier": "Team",
            "trial_end_date": "2030-01-01",
            "dashboard_url": _TEST_DASHBOARD_URL,
        },
    )

    assert "Your Team trial has started" in rendered
    assert "Welcome to your Team trial for Acme" in rendered
    assert _TEST_DASHBOARD_URL in rendered


def test_trial_expiring_template_renders():
    rendered = _email_service().render_template(
        "trial_expiring",
        context={
            "full_name": "Alex",
            "org_name": "Acme",
            "tier": "Team",
            "days_remaining": "3",
            "trial_end_date": "2030-01-01",
            "upgrade_url": _TEST_UPGRADE_URL,
        },
    )

    assert "trial ends in 3 days" in rendered
    assert "ends on 2030-01-01" in rendered
    assert _TEST_UPGRADE_URL in rendered


def test_trial_expired_template_renders():
    rendered = _email_service().render_template(
        "trial_expired",
        context={
            "full_name": "Alex",
            "org_name": "Acme",
            "tier": "Team",
            "upgrade_url": _TEST_UPGRADE_URL,
        },
    )

    assert "Your Team trial has ended" in rendered
    assert "moved back to the Community tier" in rendered
    assert _TEST_UPGRADE_URL in rendered


def test_worker_dispatches_trial_expiring():
    org_id = str(uuid.uuid4())

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.send_trial_expiring",
            new=AsyncMock(return_value=None),
        ) as mock_send,
        patch(
            "dev_health_ops.workers.system_ops.run_async",
            side_effect=_run_async_now,
        ) as mock_run_async,
    ):
        result = _invoke_send_billing_notification(
            "trial_expiring",
            org_id,
            tier="Team",
            days_remaining=3,
            trial_end_date="2030-01-01",
        )

    assert result["status"] == "sent"
    mock_run_async.assert_called_once()
    mock_send.assert_called_once_with(uuid.UUID(org_id), "Team", 3, "2030-01-01")


def test_worker_dispatches_trial_started():
    org_id = str(uuid.uuid4())

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.send_trial_started",
            new=AsyncMock(return_value=None),
        ) as mock_send,
        patch(
            "dev_health_ops.workers.system_ops.run_async",
            side_effect=_run_async_now,
        ) as mock_run_async,
    ):
        result = _invoke_send_billing_notification(
            "trial_started",
            org_id,
            tier="Team",
            trial_end_date="2030-01-01",
        )

    assert result["status"] == "sent"
    mock_run_async.assert_called_once()
    mock_send.assert_called_once_with(uuid.UUID(org_id), "Team", "2030-01-01")


def test_worker_dispatches_trial_expired():
    org_id = str(uuid.uuid4())

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.send_trial_expired",
            new=AsyncMock(return_value=None),
        ) as mock_send,
        patch(
            "dev_health_ops.workers.system_ops.run_async",
            side_effect=_run_async_now,
        ) as mock_run_async,
    ):
        result = _invoke_send_billing_notification(
            "trial_expired",
            org_id,
            tier="Team",
        )

    assert result["status"] == "sent"
    mock_run_async.assert_called_once()
    mock_send.assert_called_once_with(uuid.UUID(org_id), "Team")
