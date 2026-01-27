from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest
from atlassian.canonical_models import CanonicalProjectWithOpsgenieTeams, JiraProject, OpsgenieTeamRef
from dev_health_ops.providers.teams import sync_teams

def test_sync_teams_jira_ops():
    # Mock namespace
    ns = MagicMock()
    ns.db = "sqlite:///:memory:"
    ns.db_type = "sqlite"
    ns.provider = "jira-ops"
    ns.path = None

    # Mock data from library
    mock_project = CanonicalProjectWithOpsgenieTeams(
        project=JiraProject(cloud_id="cloud-1", key="PROJ", name="Project"),
        opsgenie_teams=[OpsgenieTeamRef(id="team-1", name="Ops Team")]
    )

    with patch("dev_health_ops.providers.jira.atlassian_compat.get_atlassian_cloud_id", return_value="cloud-1"), \
         patch("dev_health_ops.providers.jira.atlassian_compat.build_atlassian_graphql_client"), \
         patch("atlassian.graph.api.jira_projects.iter_projects_with_opsgenie_linkable_teams", return_value=iter([mock_project])), \
         patch("asyncio.run") as mock_run:
        
        result = sync_teams(ns)
        assert result == 0
        assert mock_run.called
