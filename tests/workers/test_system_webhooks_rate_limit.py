"""CHAOS-2829: webhook handlers must not swallow rate limits.

A ``RateLimitException`` raised while processing a webhook must reach the outer
Celery retry path rather than being degraded to a successful ``processed:False``
result. Providers do not resend webhooks, so a swallowed rate limit permanently
drops that webhook-driven sync. Non-rate-limit errors keep the degrade-and-log
behavior (the sync is best-effort garnish for those). Follow-up to CHAOS-2803.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

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


@pytest.mark.parametrize(
    "event_type",
    ["push", "pull_request", "issue_created", "deployment", "workflow_run"],
)
def test_github_webhook_events_request_security_sync(
    monkeypatch: pytest.MonkeyPatch, event_type: str
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "x")
    process_repo = AsyncMock()

    async def run_with_store(db_url, db_type, handler, org_id=None):
        await handler(MagicMock())

    with (
        patch(
            "dev_health_ops.workers.system_webhooks._get_db_url",
            return_value="clickhouse://x",
        ),
        patch("dev_health_ops.storage.resolve_db_type", return_value="clickhouse"),
        patch("dev_health_ops.storage.run_with_store", side_effect=run_with_store),
        patch("dev_health_ops.processors.github.process_github_repo", process_repo),
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=lambda awaitable: asyncio.run(awaitable),
        ),
    ):
        result = system_webhooks._process_github_event(
            event_type, _GITHUB_PAYLOAD, "org-1", None
        )

    assert result["processed"] is True
    assert process_repo.await_args is not None
    assert process_repo.await_args.kwargs["sync_security"] is True


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


@pytest.mark.parametrize(
    "event_type", ["push", "merge_request", "issue_created", "pipeline"]
)
def test_gitlab_webhook_events_request_security_sync(
    monkeypatch: pytest.MonkeyPatch, event_type: str
) -> None:
    monkeypatch.setenv("GITLAB_TOKEN", "x")
    process_project = AsyncMock()

    async def run_with_store(db_url, db_type, handler, org_id=None):
        await handler(MagicMock())

    with (
        patch(
            "dev_health_ops.workers.system_webhooks._get_db_url",
            return_value="clickhouse://x",
        ),
        patch("dev_health_ops.storage.resolve_db_type", return_value="clickhouse"),
        patch("dev_health_ops.storage.run_with_store", side_effect=run_with_store),
        patch(
            "dev_health_ops.processors.gitlab.process_gitlab_project", process_project
        ),
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=lambda awaitable: asyncio.run(awaitable),
        ),
    ):
        result = system_webhooks._process_gitlab_event(
            event_type, _GITLAB_PAYLOAD, "org-1", None
        )

    assert result["processed"] is True
    assert process_project.await_args is not None
    assert process_project.await_args.kwargs["sync_security"] is True


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


def _capture_outer_countdown(exc: BaseException) -> int:
    """Run the outer task with a processor that raises ``exc`` and return the
    countdown handed to ``self.retry`` (never actually retrying)."""
    captured: dict[str, Any] = {}

    class _RetrySentinel(Exception):
        pass

    def _fake_retry(*, exc: Any, countdown: int) -> None:
        captured["countdown"] = countdown
        raise _RetrySentinel

    with (
        patch(
            "dev_health_ops.workers.system_webhooks._process_github_event",
            side_effect=exc,
        ),
        patch.object(system_webhooks.process_webhook_event, "retry", _fake_retry),
    ):
        task = cast(Any, system_webhooks.process_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(
                provider="github", event_type="pull_request", payload=_GITHUB_PAYLOAD
            )
    return captured["countdown"]


def test_outer_task_infinite_retry_after_falls_back_to_exponential() -> None:
    """A non-finite retry_after_seconds must not reach int() (OverflowError);
    it falls back to exponential backoff so the webhook still retries."""
    countdown = _capture_outer_countdown(
        RateLimitException("limited", retry_after_seconds=float("inf"))
    )
    assert countdown == 30  # 30 * 2**0, not an OverflowError


def test_outer_task_absurd_finite_retry_after_is_clamped() -> None:
    """An oversized but finite retry_after_seconds is clamped to the cap."""
    countdown = _capture_outer_countdown(
        RateLimitException("limited", retry_after_seconds=10**9)
    )
    assert countdown == system_webhooks._MAX_RETRY_COUNTDOWN_SECONDS


def test_outer_task_fractional_retry_after_is_ceiled() -> None:
    """A sub-second/fractional delay rounds up to a real (>=1s) countdown."""
    countdown = _capture_outer_countdown(
        RateLimitException("limited", retry_after_seconds=90.5)
    )
    assert countdown == 91


def test_outer_task_huge_int_retry_after_is_clamped_without_overflow() -> None:
    """An integer too large to convert to float (math.isfinite/float would raise
    OverflowError) is clamped via pure-int comparison, not dropped."""
    countdown = _capture_outer_countdown(
        RateLimitException("limited", retry_after_seconds=10**400)
    )
    assert countdown == system_webhooks._MAX_RETRY_COUNTDOWN_SECONDS
