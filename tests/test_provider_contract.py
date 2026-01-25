"""
Unit tests for the provider contract (base, registry, JiraProvider).

These tests verify:
1. Registry resolves providers correctly
2. JiraProvider.ingest returns a ProviderBatch with expected fields
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.providers.base import (
    IngestionContext,
    IngestionWindow,
    Provider,
    ProviderBatch,
    ProviderCapabilities,
)
from dev_health_ops.providers.registry import (
    get_provider,
    is_registered,
    list_providers,
    register_provider,
)


class TestProviderCapabilities:
    def test_default_capabilities(self) -> None:
        caps = ProviderCapabilities()
        assert caps.work_items is True
        assert caps.status_transitions is True
        assert caps.dependencies is False
        assert caps.interactions is False
        assert caps.sprints is False
        assert caps.reopen_events is False
        assert caps.priority is False

    def test_custom_capabilities(self) -> None:
        caps = ProviderCapabilities(
            work_items=True,
            status_transitions=True,
            dependencies=True,
            interactions=True,
            sprints=True,
            reopen_events=True,
            priority=True,
        )
        assert caps.dependencies is True
        assert caps.interactions is True
        assert caps.sprints is True


class TestIngestionContext:
    def test_empty_context(self) -> None:
        ctx = IngestionContext(window=IngestionWindow())
        assert ctx.window.updated_since is None
        assert ctx.window.active_until is None
        assert ctx.project_key is None
        assert ctx.repo is None
        assert ctx.group is None
        assert ctx.limit is None

    def test_full_context(self) -> None:
        since = datetime(2025, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 31, tzinfo=timezone.utc)
        ctx = IngestionContext(
            window=IngestionWindow(updated_since=since, active_until=until),
            project_key="TEST",
            repo="owner/repo",
            group="my-group",
            limit=100,
        )
        assert ctx.window.updated_since == since
        assert ctx.window.active_until == until
        assert ctx.project_key == "TEST"
        assert ctx.repo == "owner/repo"
        assert ctx.group == "my-group"
        assert ctx.limit == 100


class TestProviderBatch:
    def test_empty_batch(self) -> None:
        batch = ProviderBatch()
        assert batch.work_items == []
        assert batch.status_transitions == []
        assert batch.dependencies == []
        assert batch.interactions == []
        assert batch.sprints == []
        assert batch.reopen_events == []

    def test_batch_with_items(self) -> None:
        mock_item = MagicMock()
        mock_transition = MagicMock()
        batch = ProviderBatch(
            work_items=[mock_item],
            status_transitions=[mock_transition],
        )
        assert len(batch.work_items) == 1
        assert len(batch.status_transitions) == 1


class TestProviderRegistry:
    def test_jira_is_registered(self) -> None:
        assert is_registered("jira")
        assert is_registered("JIRA")  # case-insensitive

    def test_list_providers_includes_jira(self) -> None:
        providers = list_providers()
        assert "jira" in providers

    def test_get_unknown_provider_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown provider 'nonexistent'"):
            get_provider("nonexistent")

    def test_register_custom_provider(self) -> None:
        class DummyProvider(Provider):
            name = "dummy"
            capabilities = ProviderCapabilities()

            def ingest(self, ctx: IngestionContext) -> ProviderBatch:
                return ProviderBatch()

        register_provider("dummy", lambda: DummyProvider())

        assert is_registered("dummy")
        provider = get_provider("dummy")
        assert provider.name == "dummy"


class TestJiraProvider:
    def test_jira_provider_resolves(self) -> None:
        """Verify JiraProvider can be resolved from registry."""
        provider = get_provider("jira")
        assert provider.name == "jira"

    def test_jira_provider_capabilities(self) -> None:
        """Verify JiraProvider has correct capabilities."""
        provider = get_provider("jira")
        caps = provider.capabilities
        assert caps.work_items is True
        assert caps.status_transitions is True
        assert caps.dependencies is True
        assert caps.interactions is True
        assert caps.sprints is True
        assert caps.reopen_events is True
        assert caps.priority is True

    @patch("dev_health_ops.providers.jira.client.JiraClient.from_env")
    def test_jira_provider_ingest_returns_batch(self, mock_from_env: MagicMock) -> None:
        """Verify JiraProvider.ingest returns a ProviderBatch."""
        # Mock JiraClient.from_env
        mock_client = MagicMock()
        mock_from_env.return_value = mock_client

        # Mock iter_issues to return a sample issue
        sample_issue = {
            "key": "TEST-123",
            "fields": {
                "summary": "Test issue",
                "status": {"name": "Open", "statusCategory": {"key": "new"}},
                "issuetype": {"name": "Task"},
                "project": {"key": "TEST", "id": "10001"},
                "created": "2025-01-01T00:00:00+0000",
                "updated": "2025-01-15T00:00:00+0000",
                "assignee": None,
                "reporter": None,
                "labels": [],
                "priority": {"name": "Medium"},
                "issuelinks": [],
            },
            "changelog": {"histories": []},
        }
        mock_client.iter_issues.return_value = iter([sample_issue])
        mock_client.iter_issue_comments.return_value = iter([])
        mock_client.close.return_value = None

        # Import and create provider
        from dev_health_ops.providers.jira.provider import JiraProvider

        provider = JiraProvider()

        # Create context
        ctx = IngestionContext(
            window=IngestionWindow(
                updated_since=datetime(2025, 1, 1, tzinfo=timezone.utc),
                active_until=datetime(2025, 1, 31, tzinfo=timezone.utc),
            ),
            limit=1,
        )

        # Ingest
        batch = provider.ingest(ctx)

        # Verify batch structure
        assert isinstance(batch, ProviderBatch)
        assert len(batch.work_items) == 1
        assert batch.work_items[0].work_item_id == "jira:TEST-123"
        assert batch.work_items[0].title == "Test issue"
        assert batch.work_items[0].provider == "jira"
        assert isinstance(batch.status_transitions, list)
        assert isinstance(batch.dependencies, list)
        assert isinstance(batch.interactions, list)
        assert isinstance(batch.sprints, list)
        assert isinstance(batch.reopen_events, list)

        # Verify client was closed
        mock_client.close.assert_called_once()

    @patch("dev_health_ops.providers.jira.client.JiraClient.from_env")
    def test_jira_provider_respects_limit(self, mock_from_env: MagicMock) -> None:
        """Verify JiraProvider respects the limit in IngestionContext."""
        mock_client = MagicMock()
        mock_from_env.return_value = mock_client

        # Return multiple issues
        issues = [
            {
                "key": f"TEST-{i}",
                "fields": {
                    "summary": f"Issue {i}",
                    "status": {"name": "Open", "statusCategory": {"key": "new"}},
                    "issuetype": {"name": "Task"},
                    "project": {"key": "TEST", "id": "10001"},
                    "created": "2025-01-01T00:00:00+0000",
                    "updated": "2025-01-15T00:00:00+0000",
                    "assignee": None,
                    "reporter": None,
                    "labels": [],
                    "priority": None,
                    "issuelinks": [],
                },
                "changelog": {"histories": []},
            }
            for i in range(10)
        ]
        mock_client.iter_issues.return_value = iter(issues)
        mock_client.iter_issue_comments.return_value = iter([])
        mock_client.close.return_value = None

        from dev_health_ops.providers.jira.provider import JiraProvider

        provider = JiraProvider()
        ctx = IngestionContext(
            window=IngestionWindow(
                updated_since=datetime(2025, 1, 1, tzinfo=timezone.utc),
            ),
            limit=3,  # Only fetch 3
        )

        batch = provider.ingest(ctx)

        assert len(batch.work_items) == 3
        assert batch.work_items[0].work_item_id == "jira:TEST-0"
        assert batch.work_items[2].work_item_id == "jira:TEST-2"
