"""
Tests for GitLab DORA metrics integration.
"""

from unittest.mock import Mock, patch
from datetime import datetime

import pytest

from dev_health_ops.connectors import GitLabConnector
from dev_health_ops.connectors.models import DORAMetrics


class TestGitLabDORAMetrics:
    """Test GitLab connector DORA metrics features."""

    @pytest.fixture
    def mock_gitlab_client(self):
        """Create a mock GitLab client."""
        with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
            yield mock_gitlab

    @pytest.fixture
    def mock_rest_client(self):
        """Create a mock REST client."""
        with patch("dev_health_ops.connectors.gitlab.GitLabRESTClient") as mock_rest:
            yield mock_rest

    def test_get_dora_metrics_success(self, mock_gitlab_client, mock_rest_client):
        """Test successful retrieval of DORA metrics."""
        # Setup mock project
        mock_project = Mock()
        mock_project.id = 123

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        # Setup mock REST response
        mock_rest_instance = mock_rest_client.return_value
        mock_rest_instance.get_dora_metrics.return_value = [
            {"date": "2023-01-01", "value": 5.0},
            {"date": "2023-01-02", "value": 10.0},
        ]

        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )

        # Test
        metrics = connector.get_dora_metrics(
            project_name="mygroup/myproject",
            metric="deployment_frequency",
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        # Assert
        assert isinstance(metrics, DORAMetrics)
        assert metrics.metric_name == "deployment_frequency"
        assert len(metrics.data_points) == 2
        assert metrics.data_points[0].value == 5.0
        assert metrics.data_points[0].date == datetime(2023, 1, 1)

        mock_gitlab_instance.projects.get.assert_called_once_with("mygroup/myproject")
        mock_rest_instance.get_dora_metrics.assert_called_once_with(
            project_id=123,
            metric="deployment_frequency",
            start_date="2023-01-01",
            end_date="2023-01-02",
            interval="daily",
        )

    def test_get_dora_metrics_empty_response(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test DORA metrics with empty API response."""
        mock_project = Mock()
        mock_project.id = 123
        mock_gitlab_client.return_value.projects.get.return_value = mock_project

        mock_rest_client.return_value.get_dora_metrics.return_value = []

        connector = GitLabConnector(private_token="test_token")
        metrics = connector.get_dora_metrics("org/repo", "lead_time_for_changes")

        assert len(metrics.data_points) == 0
        assert metrics.metric_name == "lead_time_for_changes"

    def test_get_dora_metrics_error_handling(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test DORA metrics error handling."""
        mock_gitlab_client.return_value.projects.get.side_effect = Exception(
            "API Error"
        )

        connector = GitLabConnector(private_token="test_token")
        metrics = connector.get_dora_metrics("org/repo", "change_failure_rate")

        assert len(metrics.data_points) == 0
        assert metrics.metric_name == "change_failure_rate"
