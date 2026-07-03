from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from dev_health_ops.models.git import CiPipelineRun, Deployment, Incident
from dev_health_ops.processors.gitlab import (
    _fetch_gitlab_deployments_sync,
    _fetch_gitlab_incidents_sync,
    _fetch_gitlab_pipelines_sync,
    process_gitlab_project,
)
from dev_health_ops.providers.gitlab.code_client import (
    GitLabDeploymentData,
    GitLabPipelineData,
)


class _FakeGitLabCodeClient:
    def __init__(
        self,
        *,
        pipelines=None,
        deployments=None,
        releases=None,
        merge_requests=None,
        observations=None,
    ):
        self.pipelines = pipelines or []
        self.deployments = deployments or []
        self.releases = releases or []
        self.merge_requests = merge_requests or []
        self.observations = observations or []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get_pipelines(self, project_id, *, max_pipelines):
        return self.pipelines[:max_pipelines]

    async def get_deployment_releases(self, project_id, *, per_page):
        return self.releases[:per_page]

    async def get_deployments(self, project_id, *, max_deployments, per_page=None):
        return self.deployments[:max_deployments]

    async def get_deployment_merge_requests(self, project_id, sha):
        return self.merge_requests

    def drain_usage_observations(self):
        observations = list(self.observations)
        self.observations.clear()
        return observations


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
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_commits_sync",
            return_value=([], []),
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_commit_stats_sync",
            return_value=[],
        ),
        patch(
            "dev_health_ops.processors.gitlab._sync_gitlab_mrs_to_store", return_value=0
        ),
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
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_blame_sync", return_value=[]
        ),
        # Backfill paginates the GitLab repo tree; on a Mock store that never
        # yields an empty page it would loop forever. This test exercises the
        # sync-flag dispatch, not backfill, so stub it out (hermetic + fast).
        patch(
            "dev_health_ops.processors.gitlab._backfill_gitlab_missing_data",
            new_callable=AsyncMock,
        ),
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
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_commits_sync",
            return_value=([], []),
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_commit_stats_sync",
            return_value=[],
        ),
        patch(
            "dev_health_ops.processors.gitlab._sync_gitlab_mrs_to_store", return_value=0
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_pipelines_sync"
        ) as mock_fetch_pipelines,
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_deployments_sync"
        ) as mock_fetch_deployments,
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_incidents_sync"
        ) as mock_fetch_incidents,
        # Stub backfill (repo-tree pagination) so a Mock store can't loop forever.
        patch(
            "dev_health_ops.processors.gitlab._backfill_gitlab_missing_data",
            new_callable=AsyncMock,
        ),
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


async def _run_process_gitlab_project_capture_repo(monkeypatch, *, gitlab_url):
    """Run ``process_gitlab_project`` against a fully-stubbed connector and
    return the ``Repo`` handed to ``store.insert_repo`` (CHAOS-2801 write-site
    harness)."""
    monkeypatch.setattr("dev_health_ops.processors.gitlab.CONNECTORS_AVAILABLE", True)

    mock_store = AsyncMock()
    mock_gl_project = Mock()
    mock_gl_project.id = 123
    mock_gl_project.name = "test-project"
    mock_gl_project.path_with_namespace = "group/test-project"
    mock_gl_project.web_url = "https://gitlab.com/group/test-project"
    mock_gl_project.default_branch = "main"

    with (
        patch("dev_health_ops.processors.gitlab.GitLabConnector") as _MockConnector,  # noqa: F841
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_project_info_sync",
            return_value=mock_gl_project,
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_commits_sync",
            return_value=([], []),
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_commit_stats_sync",
            return_value=[],
        ),
        patch(
            "dev_health_ops.processors.gitlab._sync_gitlab_mrs_to_store", return_value=0
        ),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_pipelines_sync"),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_deployments_sync"),
        patch("dev_health_ops.processors.gitlab._fetch_gitlab_incidents_sync"),
        patch(
            "dev_health_ops.processors.gitlab._backfill_gitlab_missing_data",
            new_callable=AsyncMock,
        ),
    ):
        await process_gitlab_project(
            store=mock_store,
            project_id=123,
            token="test-token",
            gitlab_url=gitlab_url,
            sync_cicd=False,
            sync_deployments=False,
            sync_incidents=False,
        )

    mock_store.insert_repo.assert_called_once()
    return mock_store.insert_repo.call_args.args[0]


@pytest.mark.asyncio
async def test_process_gitlab_project_persists_instance_discriminator(monkeypatch):
    """[CHAOS-2801] process_gitlab_project must persist the connector's
    configured base URL as ``settings.gitlab_instance_url`` on the written
    ``Repo`` row -- the discriminator job_work_items.py's numeric-id scoping
    uses to reject a same-``project_id`` row from a DIFFERENT GitLab
    instance. Persisted in NORMALIZED form via the shared
    ``normalize_gitlab_instance`` (the input below deliberately carries
    mixed case, an explicit default :443 port, and a path suffix -- all of
    which must be normalized away at persist time, codex MED PR #1148).
    Independent of the project's own (optional) ``web_url`` (kept unchanged
    in ``settings.url``)."""
    written_repo = await _run_process_gitlab_project_capture_repo(
        monkeypatch, gitlab_url="https://GitLab-A.example.com:443/api/v4/"
    )
    assert (
        written_repo.settings["gitlab_instance_url"] == "https://gitlab-a.example.com"
    )
    # The project's own web_url stays untouched under its existing key.
    assert written_repo.settings["url"] == "https://gitlab.com/group/test-project"
    assert written_repo.settings["project_id"] == 123


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_gitlab_url",
    [
        "",
        "   ",
        "https://gitlab.example.com:notaport",
    ],
)
async def test_process_gitlab_project_unknown_instance_url_omits_discriminator(
    monkeypatch, bad_gitlab_url
):
    """[CHAOS-2801 codex MED, PR #1148 round-2] When the normalizer cannot
    parse the connector URL (blank/malformed), the discriminator key must be
    OMITTED -- never the raw value. Persisting the raw string would defeat
    the documented "unknown" semantic AND retain path/query/userinfo from a
    malformed URL at rest (the CHAOS-2766/2780 credential-retention leak
    class). An absent key reads back as "unknown", identical to a
    pre-CHAOS-2801 row."""
    written_repo = await _run_process_gitlab_project_capture_repo(
        monkeypatch, gitlab_url=bad_gitlab_url
    )
    assert "gitlab_instance_url" not in written_repo.settings
    # The raw value must not appear under any other settings key either.
    if bad_gitlab_url.strip():
        assert bad_gitlab_url not in list(written_repo.settings.values())


@pytest.mark.asyncio
async def test_process_gitlab_project_userinfo_stripped_from_discriminator(monkeypatch):
    """[CHAOS-2801 codex MED, PR #1148 round-2] A parseable URL carrying
    userinfo persists the canonical host-only discriminator; the userinfo
    component (credential-in-URL shape) never reaches the settings JSON at
    rest."""
    account_label = "svc-" + "sync-account"  # neutral, runtime-joined
    written_repo = await _run_process_gitlab_project_capture_repo(
        monkeypatch,
        gitlab_url=f"https://{account_label}@GitLab-A.example.com/",
    )
    assert (
        written_repo.settings["gitlab_instance_url"] == "https://gitlab-a.example.com"
    )
    assert account_label not in written_repo.settings["gitlab_instance_url"]


def test_fetch_gitlab_pipelines_sync(monkeypatch):
    pipeline = GitLabPipelineData(
        pipeline_id="1",
        status="success",
        created_at=datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        started_at=datetime(2023, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
        finished_at=datetime(2023, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
    )
    usage_sink: list[dict[str, str]] = []
    fake_client = _FakeGitLabCodeClient(
        pipelines=[pipeline], observations=[{"route_family": "pipelines"}]
    )
    monkeypatch.setattr(
        "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
        lambda connector: fake_client,
    )

    pipelines = _fetch_gitlab_pipelines_sync(
        Mock(),
        project_id=1,
        repo_id=None,
        max_pipelines=10,
        since=None,
        usage_sink=usage_sink,
    )

    assert len(pipelines) == 1
    assert pipelines[0].run_id == "1"
    assert pipelines[0].status == "success"
    assert pipelines[0].queued_at == datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert pipelines[0].started_at == datetime(2023, 1, 1, 0, 1, 0, tzinfo=timezone.utc)
    assert usage_sink == [{"route_family": "pipelines"}]


def test_fetch_gitlab_deployments_sync(monkeypatch):
    deployment = GitLabDeploymentData(
        deployment_id="101",
        deployment_iid=12,
        status="success",
        environment="production",
        created_at=datetime(2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2023, 1, 2, 0, 10, 0, tzinfo=timezone.utc),
        sha="abc123",
        raw_payload={"id": 101, "iid": 12, "ref": "v1.2.3"},
    )
    usage_sink: list[dict[str, str]] = []
    fake_client = _FakeGitLabCodeClient(
        deployments=[deployment],
        releases=[{"tag_name": "v1.2.3"}],
        merge_requests=[
            {"iid": 44, "state": "merged", "merged_at": "2023-01-02T00:09:00Z"}
        ],
        observations=[{"route_family": "deployments"}],
    )
    monkeypatch.setattr(
        "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
        lambda connector: fake_client,
    )

    deployments = _fetch_gitlab_deployments_sync(
        Mock(),
        project_id=1,
        repo_id=None,
        max_deployments=10,
        since=None,
        usage_sink=usage_sink,
    )

    assert len(deployments) == 1
    assert deployments[0].deployment_id == "101"
    assert deployments[0].environment == "production"
    assert deployments[0].pull_request_number == 44
    assert deployments[0].release_ref == "v1.2.3"
    assert deployments[0].release_ref_confidence == pytest.approx(1.0)
    assert deployments[0].deployed_at == datetime(
        2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc
    )
    assert usage_sink == [{"route_family": "deployments"}]


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
