//go:build integration

package workerservice

import (
	"context"
	"net/http"
	"testing"
	"time"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// fakeProjectAsTeamCollector stands in for the real
// providersync.JiraTeamCatalogCollector, so this test exercises the
// jiraCombinedTeamCatalogCollector's OWN composition logic without needing
// live Jira REST fixtures for the project-as-team leg.
type fakeProjectAsTeamCollector struct {
	result providersync.TeamCatalogResult
	err    error
	calls  int
}

func (f *fakeProjectAsTeamCollector) CollectTeamCatalog(
	context.Context, providersync.TeamCatalogReference, providerfoundation.Credential,
	*providerfoundation.HTTPClient, providersync.TeamCatalogSelections, time.Time,
) (providersync.TeamCatalogResult, error) {
	f.calls++
	return f.result, f.err
}

type oneAtlassianTeam struct{}

func (oneAtlassianTeam) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return []atlassian.AtlassianTeam{{ID: "ari:cloud:identity::team/AAAA-1", DisplayName: "Platform", State: "ACTIVE"}}, nil
}
func (oneAtlassianTeam) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return []atlassian.TeamworkUserRelation{{SubjectUserID: "ari:cloud:identity::user/acct-1", RelationType: "TEAM_MEMBER"}}, nil
}
func (oneAtlassianTeam) IterTeamActiveProjects(context.Context, string, int) ([]atlassian.TeamworkProject, error) {
	key := "PLAT"
	return []atlassian.TeamworkProject{{ProjectKey: &key}}, nil
}

func testJiraCredential(config map[string]string) providerfoundation.Credential {
	return providerfoundation.NewCredential("jira", "cred-1", config, map[string]secrets.Value{
		"email": secrets.NewValue("sync@example.test"), "api_token": secrets.NewValue("s3cr3t-token"),
	})
}

// TestJiraCombinedCollectorWritesRealAtlassianTeamsWhenConfigured is the
// D2770/CHAOS-7002 wiring proof the scribe's VET flagged: the AUTOMATIC path
// (this collector, registered under "jira" in sync_dispatch.go's Native map)
// must itself reach internal/atlassianteams.Collect/Write when the resolved
// credential carries atlassian_organization_id -- not only the standalone
// CLI verb.
func TestJiraCombinedCollectorWritesRealAtlassianTeamsWhenConfigured(t *testing.T) {
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
	defer func() { _ = conn.Close() }()

	fake := &fakeProjectAsTeamCollector{result: providersync.TeamCatalogResult{TeamsWritten: 1, MembersWritten: 1, MembershipsWritten: 1, ProjectsWritten: 1}}
	collector := jiraCombinedTeamCatalogCollector{
		ProjectAsTeam: fake,
		Conn:          conn,
		NewClient:     func(string, atlassian.AuthProvider) atlassianteams.Client { return oneAtlassianTeam{} },
	}
	credential := testJiraCredential(map[string]string{
		"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "atlassian-org-123", "atlassian_cloud_id": "pinned-cloud-id",
	})
	client, err := providerfoundation.NewJiraClient(credential, http.DefaultClient, providerfoundation.DefaultRetryPolicy(), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	result, err := collector.CollectTeamCatalog(ctx,
		providersync.TeamCatalogReference{OrgID: "org-under-test", SyncRunID: "run-1", IntegrationID: "integration-1"},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Now().UTC())
	if err != nil {
		t.Fatalf("CollectTeamCatalog: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("ProjectAsTeam called %d times, want 1", fake.calls)
	}
	// 1 from the fake project-as-team leg + 1 real Atlassian team = 2.
	if result.TeamsWritten != 2 {
		t.Errorf("TeamsWritten = %d, want 2 (1 project-as-team + 1 real Atlassian team)", result.TeamsWritten)
	}
	if result.MembershipsWritten != 2 {
		t.Errorf("MembershipsWritten = %d, want 2", result.MembershipsWritten)
	}
	if result.OwnershipWritten != 1 {
		t.Errorf("OwnershipWritten = %d, want 1 (the real Atlassian team's project link)", result.OwnershipWritten)
	}
	var gotARITeams uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM teams FINAL WHERE org_id = 'org-under-test' AND provider = 'jira' AND startsWith(ifNull(native_team_key, ''), 'ari:cloud:identity::team/')").Scan(&gotARITeams); err != nil {
		t.Fatal(err)
	}
	if gotARITeams != 1 {
		t.Errorf("real Atlassian Teams rows written to ClickHouse = %d, want 1 -- the automatic path did not actually write them", gotARITeams)
	}
}

// TestJiraCombinedCollectorSkipsAtlassianTeamsWhenNotConfigured is the
// backward-compatibility guard: an org that has not set
// atlassian_organization_id must be completely unaffected -- the real
// Atlassian Teams client must never even be constructed.
func TestJiraCombinedCollectorSkipsAtlassianTeamsWhenNotConfigured(t *testing.T) {
	ctx := context.Background()
	fake := &fakeProjectAsTeamCollector{result: providersync.TeamCatalogResult{TeamsWritten: 1}}
	newClientCalled := false
	collector := jiraCombinedTeamCatalogCollector{
		ProjectAsTeam: fake,
		Conn:          nil, // must never be dereferenced: no atlassian_organization_id means no Atlassian Teams attempt at all
		NewClient: func(string, atlassian.AuthProvider) atlassianteams.Client {
			newClientCalled = true
			return oneAtlassianTeam{}
		},
	}
	credential := testJiraCredential(map[string]string{"base_url": "https://acme.atlassian.net"})
	client, err := providerfoundation.NewJiraClient(credential, http.DefaultClient, providerfoundation.DefaultRetryPolicy(), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	result, err := collector.CollectTeamCatalog(ctx,
		providersync.TeamCatalogReference{OrgID: "org-under-test", SyncRunID: "run-1", IntegrationID: "integration-1"},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Now().UTC())
	if err != nil {
		t.Fatalf("CollectTeamCatalog: %v", err)
	}
	if newClientCalled {
		t.Error("the Atlassian Teams client was built even though atlassian_organization_id was not configured")
	}
	if result.TeamsWritten != 1 {
		t.Errorf("TeamsWritten = %d, want 1 (unchanged project-as-team result)", result.TeamsWritten)
	}
}
