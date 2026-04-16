"""Test GitHubWorkClient._iter_with_limit generic helper."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient


@pytest.fixture
def client() -> GitHubWorkClient:
    with (
        patch("github.Github"),
        patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
    ):
        return GitHubWorkClient(auth=GitHubAuth(token="fake"))


class TestIterWithLimit:
    def test_no_limit_yields_all(self, client: GitHubWorkClient) -> None:
        source = [MagicMock(), MagicMock(), MagicMock()]
        result = list(client._iter_with_limit(source, limit=None))
        assert result == source

    def test_limit_truncates(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5]
        result = list(client._iter_with_limit(source, limit=3))
        assert result == [1, 2, 3]

    def test_limit_zero_yields_none(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3]
        result = list(client._iter_with_limit(source, limit=0))
        assert result == []

    def test_limit_larger_than_source(self, client: GitHubWorkClient) -> None:
        source = [1, 2]
        result = list(client._iter_with_limit(source, limit=10))
        assert result == [1, 2]

    def test_custom_filter_skips_items(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5]

        def skip_evens(x: Any) -> bool:
            return x % 2 == 0  # predicate returns True when item should be SKIPPED

        result = list(client._iter_with_limit(source, limit=None, skip=skip_evens))
        assert result == [1, 3, 5]

    def test_filter_plus_limit(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5, 6]

        def skip_evens(x: Any) -> bool:
            return x % 2 == 0

        result = list(client._iter_with_limit(source, limit=2, skip=skip_evens))
        assert result == [1, 3]
