import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from dev_health_ops.processors.gitlab import (
    process_gitlab_project,
    _fetch_gitlab_pipelines_sync,
    _fetch_gitlab_deployments_sync,
    _fetch_gitlab_incidents_sync,
)
from dev_health_ops.models.git import CiPipelineRun, Deployment, Incident


@pytest.mark.asyncio
async def test_process_gitlab_project_sync_flags(monkeypatch):
    """Test that process_gitlab_project calls sync functions based on flags."""
    monkeypatch.setattr("dev_health_ops.processors.gitlab.CONNECTORS_AVAILABLE", True)

    # Mock storage
    mock_store = AsyncMock()

    # Mock project info
    mock_gl_project = Mock()
    mock_gl_project.id = 123
    mock_gl_project.name = "test-project"
    mock_gl_project.path_with_namespace = "group/test-project"
    mock_gl_project.web_url = "https://gitlab.com/group/test-project"
    mock_gl_project.default_branch = "main"

    # Mock return values for helper functions
    mock_pipelines = [
        CiPipelineRun(
            repo_id=None,
            run_id="1",
            status="success",
            started_at=datetime.now(timezone.utc),
        )
    ]
    mock_deployments = [Deployment(repo_id=None, deployment_id="1", status="success")]
    mock_incidents = [
        Incident(
            repo_id=None,
            incident_id="1",
            status="opened",
            started_at=datetime.now(timezone.utc),
        )
    ]

    # Patch the helper functions and connector
    with (
        patch("dev_health_ops.processors.gitlab.GitLabConnector") as _MockConnector,  # noqa: F841
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_project_info_sync",
            return_value=mock_gl_project,
        ),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_commits_sync", return_value=([], [])),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_commit_stats_sync", return_value=[]),
        patch("dev_health_ops.processors.gitlab._sync_gitlab_mrs_to_store", return_value=0),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_pipelines_sync",
            return_value=mock_pipelines,
        ) as mock_fetch_pipelines,
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_deployments_sync",
            return_value=mock_deployments,
        ) as mock_fetch_deployments,
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_incidents_sync",
            return_value=mock_incidents,
        ) as mock_fetch_incidents,
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_blame_sync", return_value=[]),
    ):
        # Call the function with all sync flags enabled
        await process_gitlab_project(
            store=mock_store,
            project_id=123,
            token="test-token",
            gitlab_url="https://gitlab.com",
            sync_cicd=True,
            sync_deployments=True,
            sync_incidents=True,
        )

        # Verify helpers were called
        mock_fetch_pipelines.assert_called_once()
        mock_fetch_deployments.assert_called_once()
        mock_fetch_incidents.assert_called_once()

        # Verify store methods were called
        mock_store.insert_ci_pipeline_runs.assert_called_once_with(mock_pipelines)
        mock_store.insert_deployments.assert_called_once_with(mock_deployments)
        mock_store.insert_incidents.assert_called_once_with(mock_incidents)


@pytest.mark.asyncio
async def test_process_gitlab_project_no_sync_flags(monkeypatch):
    """Test that process_gitlab_project DOES NOT call sync functions when flags are False."""
    monkeypatch.setattr("dev_health_ops.processors.gitlab.CONNECTORS_AVAILABLE", True)

    mock_store = AsyncMock()
    mock_gl_project = Mock()
    mock_gl_project.id = 123
    mock_gl_project.name = "test-project"

    with (
        patch("dev_health_ops.processors.gitlab.GitLabConnector") as _MockConnector,  # noqa: F841
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_project_info_sync",
            return_value=mock_gl_project,
        ),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_commits_sync", return_value=([], [])),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_commit_stats_sync", return_value=[]),
        patch("dev_health_ops.processors.gitlab._sync_gitlab_mrs_to_store", return_value=0),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_pipelines_sync") as mock_fetch_pipelines,
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_deployments_sync"
        ) as mock_fetch_deployments,
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_incidents_sync") as mock_fetch_incidents,
    ):
        # Call with flags False
        await process_gitlab_project(
            store=mock_store,
            project_id=123,
            token="test-token",
            gitlab_url="https://gitlab.com",
            sync_cicd=False,
            sync_deployments=False,
            sync_incidents=False,
        )

        # Verify helpers were NOT called
        mock_fetch_pipelines.assert_not_called()
        mock_fetch_deployments.assert_not_called()
        mock_fetch_incidents.assert_not_called()

        # Verify store methods were NOT called
        mock_store.insert_ci_pipeline_runs.assert_not_called()
        mock_store.insert_deployments.assert_not_called()
        mock_store.insert_incidents.assert_not_called()


def test_fetch_gitlab_pipelines_sync():
    mock_pipeline = Mock()
    mock_pipeline.id = 1
    mock_pipeline.status = "success"
    mock_pipeline.created_at = "2023-01-01T00:00:00Z"
    mock_pipeline.started_at = "2023-01-01T00:01:00Z"
    mock_pipeline.finished_at = "2023-01-01T00:05:00Z"

    mock_gl_project = Mock()
    mock_gl_project.pipelines.list.return_value = [mock_pipeline]

    pipelines = _fetch_gitlab_pipelines_sync(
        mock_gl_project, repo_id=None, max_pipelines=10, since=None
    )

    assert len(pipelines) == 1
    assert pipelines[0].run_id == "1"
    assert pipelines[0].status == "success"
    assert pipelines[0].queued_at == datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert pipelines[0].started_at == datetime(2023, 1, 1, 0, 1, 0, tzinfo=timezone.utc)


def test_fetch_gitlab_deployments_sync():
    mock_connector = Mock()
    mock_connector.rest_client.get_deployments.return_value = [
        {
            "id": 101,
            "status": "success",
            "environment": {"name": "production"},
            "created_at": "2023-01-02T00:00:00Z",
            "finished_at": "2023-01-02T00:10:00Z",
        }
    ]

    deployments = _fetch_gitlab_deployments_sync(
        mock_connector, project_id=1, repo_id=None, max_deployments=10, since=None
    )

    assert len(deployments) == 1
    assert deployments[0].deployment_id == "101"
    assert deployments[0].environment == "production"
    assert deployments[0].deployed_at == datetime(
        2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc
    )


def test_fetch_gitlab_incidents_sync():
    mock_connector = Mock()
    mock_connector.rest_client.get_issues.return_value = [
        {
            "id": 505,
            "state": "opened",
            "created_at": "2023-01-03T12:00:00Z",
            "closed_at": None,
        }
    ]

    incidents = _fetch_gitlab_incidents_sync(
        mock_connector, project_id=1, repo_id=None, max_issues=10, since=None
    )

    assert len(incidents) == 1
    assert incidents[0].incident_id == "505"
    assert incidents[0].status == "opened"
    assert incidents[0].started_at == datetime(
        2023, 1, 3, 12, 0, 0, tzinfo=timezone.utc
    )
