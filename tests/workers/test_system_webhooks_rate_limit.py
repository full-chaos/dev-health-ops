"""CHAOS-2829: webhook handlers must not swallow rate limits.

A ``RateLimitException`` raised while processing a webhook must reach the outer
Celery retry path rather than being degraded to a successful ``processed:False``
result. Providers do not resend webhooks, so a swallowed rate limit permanently
drops that webhook-driven sync. Non-rate-limit errors keep the degrade-and-log
behavior (the sync is best-effort garnish for those). Follow-up to CHAOS-2803.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.workers import system_webhooks

_GITHUB_PAYLOAD = {"repository": {"owner": {"login": "acme"}, "name": "widget"}}
_GITLAB_PAYLOAD = {"project": {"id": 123}}
_JIRA_PAYLOAD = {"issue": {"key": "ENG-1"}}


def _patch_storage() -> Any:
    """Neutralize the DB/storage plumbing so the sync-execution try block is the
    only thing under test; ``run_async`` is what each test drives."""
    return (
        patch(
            "dev_health_ops.workers.system_webhooks._get_db_url",
            return_value="clickhouse://x",
        ),
        patch("dev_health_ops.storage.resolve_db_type", return_value="clickhouse"),
        patch("dev_health_ops.storage.run_with_store", return_value=MagicMock()),
    )


def test_github_pull_request_rate_limit_reraises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "x")
    p1, p2, p3 = _patch_storage()
    with (
        p1,
        p2,
        p3,
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=RateLimitException(
                "gh secondary limit", retry_after_seconds=90
            ),
        ),
    ):
        with pytest.raises(RateLimitException):
            system_webhooks._process_github_event(
                "pull_request", _GITHUB_PAYLOAD, "org-1", None
            )


def test_github_non_rate_limit_error_degrades(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "x")
    p1, p2, p3 = _patch_storage()
    with (
        p1,
        p2,
        p3,
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=ValueError("transient parse error"),
        ),
    ):
        result = system_webhooks._process_github_event(
            "pull_request", _GITHUB_PAYLOAD, "org-1", None
        )
    assert result["processed"] is False
    assert "error" in result


def test_gitlab_rate_limit_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITLAB_TOKEN", "x")
    p1, p2, p3 = _patch_storage()
    with (
        p1,
        p2,
        p3,
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=RateLimitException("gl limit", retry_after_seconds=30),
        ),
    ):
        with pytest.raises(RateLimitException):
            system_webhooks._process_gitlab_event(
                "push", _GITLAB_PAYLOAD, "org-1", None
            )


def test_jira_rate_limit_reraises() -> None:
    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job",
        side_effect=RateLimitException("jira limit", retry_after_seconds=60),
    ):
        with pytest.raises(RateLimitException):
            system_webhooks._process_jira_event("issue_updated", _JIRA_PAYLOAD, "org-1")


def test_outer_task_honors_retry_after_countdown() -> None:
    """The Celery retry countdown uses the exception's retry_after_seconds."""
    captured: dict[str, Any] = {}

    class _RetrySentinel(Exception):
        pass

    def _fake_retry(*, exc: Any, countdown: int) -> None:
        captured["countdown"] = countdown
        raise _RetrySentinel

    with (
        patch(
            "dev_health_ops.workers.system_webhooks._process_github_event",
            side_effect=RateLimitException("limited", retry_after_seconds=120),
        ),
        patch.object(system_webhooks.process_webhook_event, "retry", _fake_retry),
    ):
        task = cast(Any, system_webhooks.process_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(
                provider="github", event_type="pull_request", payload=_GITHUB_PAYLOAD
            )
    assert captured["countdown"] == 120


def test_outer_task_falls_back_to_exponential_countdown() -> None:
    """A rate limit with no retry_after_seconds falls back to exponential backoff."""
    captured: dict[str, Any] = {}

    class _RetrySentinel(Exception):
        pass

    def _fake_retry(*, exc: Any, countdown: int) -> None:
        captured["countdown"] = countdown
        raise _RetrySentinel

    with (
        patch(
            "dev_health_ops.workers.system_webhooks._process_github_event",
            side_effect=RateLimitException("limited"),  # retry_after_seconds=None
        ),
        patch.object(system_webhooks.process_webhook_event, "retry", _fake_retry),
    ):
        task = cast(Any, system_webhooks.process_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(
                provider="github", event_type="pull_request", payload=_GITHUB_PAYLOAD
            )
    # retries defaults to 0 outside a worker context: 30 * 2**0 == 30
    assert captured["countdown"] == 30
