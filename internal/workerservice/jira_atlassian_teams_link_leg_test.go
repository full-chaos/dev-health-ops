package workerservice

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type noAtlassianTeams struct{}

func (noAtlassianTeams) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return nil, nil
}
func (noAtlassianTeams) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return nil, nil
}
func (noAtlassianTeams) IterTeamConnectedContainers(context.Context, string, int) ([]graph.TeamConnectedContainer, error) {
	return nil, nil
}

// A team search that returns no team is far more often an access problem than an organization with no team. The
// worker path writes nothing for it (the store is never reached here: nopConn has no connection behind it), and it
// says so: a Warn line and a degraded leg, never a clean run with no line.
func TestAnEmptyAtlassianTeamSearchIsADegradedLegNotSilence(t *testing.T) {
	logs := captureWarnings(t)
	credential := providerfoundation.NewCredential("jira", "cred-1",
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "org-123", "atlassian_cloud_id": "cloud-123"},
		map[string]secrets.Value{"email": secrets.NewValue("sync@example.test"), "api_token": secrets.NewValue("synthetic-api-token-value")})
	collector := jiraCombinedTeamCatalogCollector{
		ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 1}},
		Conn:          nopConn{},
		NewClient:     func(string, atlassian.AuthProvider) atlassianteams.Client { return noAtlassianTeams{} },
	}
	client := &providerfoundation.HTTPClient{BaseURL: &url.URL{Scheme: "https", Host: "acme.atlassian.net"}}
	result, err := collector.CollectTeamCatalog(context.Background(),
		providersync.TeamCatalogReference{OrgID: testOrg, SyncRunID: testRun, Strict: true},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Now())
	if err != nil {
		t.Fatalf("an empty team search failed the whole collection: %v", err)
	}
	if result.TeamsWritten != 1 {
		t.Errorf("TeamsWritten = %d, want the project-as-team leg's 1 kept", result.TeamsWritten)
	}
	want := providersync.DegradedLeg{Dataset: "teams", Leg: "jira_atlassian_teams", Outcome: "failed", Reason: "empty_team_search"}
	if len(result.DegradedLegs) != 1 || result.DegradedLegs[0] != want {
		t.Fatalf("degraded legs = %+v, want %+v", result.DegradedLegs, want)
	}
	if line := logs.String(); !strings.Contains(line, "jira_atlassian_teams_walk_skipped") || !strings.Contains(line, "empty_team_search") {
		t.Errorf("no Warn line names the empty team search: %q", line)
	}
}

// The reason of a project-link leg that was not a complete snapshot, one cause at a time; a complete one has no leg.
func TestTheProjectLinkLegNamesWhyItIsNotComplete(t *testing.T) {
	failure := fmt.Errorf("read connected projects of a team: %w", errors.New("synthetic gateway refusal"))
	bound := fmt.Errorf("read connected projects of a team: %w", graph.ErrTeamConnectedContainersBound)
	cases := map[string]struct {
		rows   atlassianteams.Rows
		reason string
		detail string
	}{
		"a failed read":        {atlassianteams.Rows{ProjectLinkFailure: failure, ProjectLinks: atlassianteams.ProjectLinkCounts{FailedTeamReads: 1}}, "project_link_read_failed", "synthetic gateway refusal"},
		"the page bound":       {atlassianteams.Rows{ProjectLinkFailure: bound, ProjectLinks: atlassianteams.ProjectLinkCounts{FailedTeamReads: 1}}, "project_link_page_bound", "page bound"},
		"an unknown link type": {atlassianteams.Rows{ProjectLinks: atlassianteams.ProjectLinkCounts{Seen: 1, SkippedUnknownType: 1}}, "project_link_unknown_type", ""},
		"a failed read next to an unknown type": {atlassianteams.Rows{ProjectLinkFailure: failure, ProjectLinks: atlassianteams.ProjectLinkCounts{Seen: 1, SkippedUnknownType: 1, FailedTeamReads: 1}},
			"project_link_read_failed", "synthetic gateway refusal"},
		"no recorded cause": {atlassianteams.Rows{}, "project_link_read_failed", ""},
		// Every read ended and one team has a Jira project link that got no row: the snapshot is complete, the leg is not clean.
		"a link that got no row": {atlassianteams.Rows{ProjectLinksComplete: true, UnreadableProjectLinkTeams: []string{"t-1"},
			ProjectLinks: atlassianteams.ProjectLinkCounts{Seen: 2, SkippedNoProjectKey: 1}}, "project_link_not_written", ""},
		"a link that got no row next to an unknown type": {atlassianteams.Rows{UnreadableProjectLinkTeams: []string{"t-1"},
			ProjectLinks: atlassianteams.ProjectLinkCounts{Seen: 2, SkippedNoNativeID: 1, SkippedUnknownType: 1}}, "project_link_unknown_type", ""},
		"a link that got no row next to a failed read": {atlassianteams.Rows{ProjectLinkFailure: failure, UnreadableProjectLinkTeams: []string{"t-1"},
			ProjectLinks: atlassianteams.ProjectLinkCounts{Seen: 1, SkippedNoNativeID: 1, FailedTeamReads: 1}}, "project_link_read_failed", "synthetic gateway refusal"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			leg := degradedProjectLinksLeg(tc.rows)
			if leg == nil {
				t.Fatal("no degraded leg for a snapshot that is not complete")
			}
			if leg.Dataset != "teams" || leg.Leg != "jira_atlassian_team_project_links" || leg.Outcome != "failed" || leg.Reason != tc.reason {
				t.Errorf("leg = %+v, want reason %s", *leg, tc.reason)
			}
			if !strings.Contains(leg.Detail, tc.detail) || (tc.detail == "") != (leg.Detail == "") {
				t.Errorf("detail = %q, want it to hold %q", leg.Detail, tc.detail)
			}
		})
	}
	if leg := degradedProjectLinksLeg(atlassianteams.Rows{ProjectLinksComplete: true}); leg != nil {
		t.Errorf("a complete snapshot has a degraded leg: %+v", *leg)
	}
}
