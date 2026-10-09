//go:build integration

package workerservice

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	linkLegTeamOne = "ari:cloud:identity::team/00000000-0000-4000-8000-000000000001"
	linkLegTeamTwo = "ari:cloud:identity::team/00000000-0000-4000-8000-000000000002"
	linkLegToken   = "s3cr3t-token"
)

// twoAtlassianTeams answers two active teams with one member each; links answers each team's link read.
type twoAtlassianTeams struct {
	links func(teamID string) ([]graph.TeamConnectedContainer, error)
}

func (twoAtlassianTeams) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return []atlassian.AtlassianTeam{
		{ID: linkLegTeamOne, DisplayName: "Synthetic one", State: "ACTIVE"},
		{ID: linkLegTeamTwo, DisplayName: "Synthetic two", State: "ACTIVE"},
	}, nil
}
func (twoAtlassianTeams) IterTeamUsers(_ context.Context, teamID string, _ int) ([]atlassian.TeamworkUserRelation, error) {
	return []atlassian.TeamworkUserRelation{{SubjectUserID: "ari:cloud:identity::user/synthetic-" + teamID[len(teamID)-1:], RelationType: "TEAM_MEMBER"}}, nil
}
func (c twoAtlassianTeams) IterTeamConnectedContainers(_ context.Context, teamID string, _ int) ([]graph.TeamConnectedContainer, error) {
	return c.links(teamID)
}

func openLinkLegClickHouse(t *testing.T) driver.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func collectLinkLeg(t *testing.T, conn driver.Conn, links func(string) ([]graph.TeamConnectedContainer, error), at time.Time) providersync.TeamCatalogResult {
	t.Helper()
	collector := jiraCombinedTeamCatalogCollector{
		ScopeCensus:   soleIntegrationCensus{},
		ProjectAsTeam: &fakeProjectAsTeamCollector{},
		Conn:          conn,
		NewClient:     func(string, atlassian.AuthProvider) atlassianteams.Client { return twoAtlassianTeams{links: links} },
	}
	credential := testJiraCredential(map[string]string{
		"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "atlassian-org-123", "atlassian_cloud_id": "pinned-cloud-id",
	})
	client, err := providerfoundation.NewJiraClient(credential, http.DefaultClient, providerfoundation.DefaultRetryPolicy(), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	result, err := collector.CollectTeamCatalog(context.Background(),
		providersync.TeamCatalogReference{OrgID: "org-under-test", SyncRunID: "run-1", IntegrationID: "integration-1"},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at)
	if err != nil {
		t.Fatalf("CollectTeamCatalog: %v", err)
	}
	return result
}

func openLinks(t *testing.T, conn driver.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT concat(team_id, '>', project_id) FROM team_project_ownership FINAL WHERE org_id = 'org-under-test' AND provider = 'jira' AND valid_to IS NULL ORDER BY team_id, project_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(out, " ")
}

// The worker path of the Atlassian Teams step: the step result carries what became of every link the provider
// returned, and a link leg that is not a complete snapshot is a degraded leg that closes no row and keeps the teams
// and members of the same run.
func TestTheWorkerStepCountsLinksAndDegradesAnIncompleteLinkLeg(t *testing.T) {
	conn := openLinkLegClickHouse(t)
	const one, two = "jira:00000000-0000-4000-8000-000000000001", "jira:00000000-0000-4000-8000-000000000002"
	project := func(key, id string) graph.TeamConnectedContainer {
		return graph.TeamConnectedContainer{Typename: "JiraProject", ID: "ari:cloud:jira:site-uuid:project/" + id, Key: key, ProjectID: id}
	}
	at := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)

	// A complete run: team one holds a Jira project and a Confluence space, team two a Jira project.
	result := collectLinkLeg(t, conn, func(teamID string) ([]graph.TeamConnectedContainer, error) {
		if teamID == linkLegTeamOne {
			return []graph.TeamConnectedContainer{project("SYNA", "10001"), {Typename: "ConfluenceSpace"}}, nil
		}
		return []graph.TeamConnectedContainer{project("SYNB", "10002")}, nil
	}, at)
	if len(result.DegradedLegs) != 0 || result.OwnershipSnapshotIncomplete {
		t.Fatalf("a complete run is degraded: legs=%+v incomplete=%t", result.DegradedLegs, result.OwnershipSnapshotIncomplete)
	}
	if result.TeamsWritten != 2 || result.MembershipsWritten != 2 || result.OwnershipWritten != 2 || result.ProjectLinksSeen != 3 ||
		result.ProjectLinksSkippedNotProject != 1 || result.ProjectLinksSkippedNoNativeID != 0 || result.ProjectLinksSkippedNoKey != 0 || result.ProjectLinksSkippedUnknownType != 0 {
		t.Fatalf("result = %+v, want 2 teams, 2 memberships, 3 links seen, 2 written, 1 skipped as not a project", result)
	}
	if got, want := openLinks(t, conn), one+">10001 "+two+">10002"; got != want {
		t.Fatalf("open links = %q, want %q", got, want)
	}

	// The next run: team one lost its project, and team two's link read fails with an error that echoes the
	// credential. Nothing is closed; the teams and members are written; the leg is degraded with its reason.
	result = collectLinkLeg(t, conn, func(teamID string) ([]graph.TeamConnectedContainer, error) {
		if teamID == linkLegTeamOne {
			return nil, nil
		}
		return nil, errors.New("synthetic gateway refusal for Basic " + linkLegToken)
	}, at.Add(time.Hour))
	if result.TeamsWritten != 2 || result.MembershipsWritten != 2 || result.OwnershipWritten != 0 || result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete {
		t.Fatalf("result = %+v, want the 2 teams and 2 memberships written, no link written or closed, snapshot incomplete", result)
	}
	if len(result.DegradedLegs) != 1 {
		t.Fatalf("degraded legs = %+v, want the project-link leg", result.DegradedLegs)
	}
	leg := result.DegradedLegs[0]
	if leg.Dataset != "teams" || leg.Leg != "jira_atlassian_team_project_links" || leg.Outcome != "failed" || leg.Reason != "project_link_read_failed" {
		t.Errorf("leg = %+v", leg)
	}
	if strings.Contains(leg.Detail, linkLegToken) || !strings.Contains(leg.Detail, "synthetic gateway refusal") {
		t.Errorf("detail = %q, want the gateway's message without the credential", leg.Detail)
	}
	if got, want := openLinks(t, conn), one+">10001 "+two+">10002"; got != want {
		t.Fatalf("open links after the incomplete run = %q, want %q: an incomplete run closed a link", got, want)
	}

	// A complete run after that closes the lost link and reports it.
	result = collectLinkLeg(t, conn, func(teamID string) ([]graph.TeamConnectedContainer, error) {
		if teamID == linkLegTeamOne {
			return nil, nil
		}
		return []graph.TeamConnectedContainer{project("SYNB", "10002")}, nil
	}, at.Add(2*time.Hour))
	if len(result.DegradedLegs) != 0 || result.OwnershipSnapshotIncomplete || result.OwnershipRetracted != 1 || result.OwnershipWritten != 1 {
		t.Fatalf("result = %+v, want a clean run that closed 1 link and wrote 1", result)
	}
	if got, want := openLinks(t, conn), two+">10002"; got != want {
		t.Fatalf("open links after the complete run = %q, want %q", got, want)
	}
}
