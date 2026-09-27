//go:build integration

package workerservice

import (
	"context"
	"errors"
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

type failingAtlassianTeamsClient struct{}

func (failingAtlassianTeamsClient) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return nil, errors.New("simulated Atlassian Teams API outage")
}
func (failingAtlassianTeamsClient) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return nil, nil
}
func (failingAtlassianTeamsClient) IterTeamActiveProjects(context.Context, string, int) ([]atlassian.TeamworkProject, error) {
	return nil, nil
}

func testJiraCredential(config map[string]string) providerfoundation.Credential {
	return providerfoundation.NewCredential("jira", "cred-1", config, map[string]secrets.Value{
		"email": secrets.NewValue("sync@example.test"), "api_token": secrets.NewValue("s3cr3t-token"),
	})
}

// TestJiraCombinedCollectorWritesRealAtlassianTeamsWhenConfigured is the
// D2770/D2775/CHAOS-7002 wiring proof: the AUTOMATIC path (this collector,
// registered under "jira" in sync_dispatch.go's Native map) must itself
// reach internal/atlassianteams.Collect/Write when the resolved credential
// carries atlassian_organization_id -- not only the standalone CLI verb --
// and Atlassian Teams rows are PRIMARY: the project-as-team fallback must
// NOT run at all when Atlassian Teams succeeds (D2775).
//
// fake.result.TeamsWritten is deliberately 99 -- a value the real Atlassian
// path (1 team) can never coincidentally produce -- so this test is an
// observed-red proof of the wiring: with the `attemptAtlassian` gate in
// jira_atlassian_teams_collector.go forced false (verified by hand, then
// restored), this test fails with TeamsWritten=99 and fake.calls=1 instead
// of passing vacuously.
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

	fake := &fakeProjectAsTeamCollector{result: providersync.TeamCatalogResult{TeamsWritten: 99, MembersWritten: 99, MembershipsWritten: 99, ProjectsWritten: 99}}
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
	if fake.calls != 0 {
		t.Fatalf("ProjectAsTeam called %d times, want 0 -- Atlassian Teams succeeded, so it must be the ONLY writer this run (D2775 primary/fallback, not additive)", fake.calls)
	}
	if result.TeamsWritten != 1 {
		t.Errorf("TeamsWritten = %d, want 1 (the real Atlassian team, exclusively)", result.TeamsWritten)
	}
	if result.MembershipsWritten != 1 {
		t.Errorf("MembershipsWritten = %d, want 1", result.MembershipsWritten)
	}
	if result.OwnershipWritten != 1 {
		t.Errorf("OwnershipWritten = %d, want 1 (the real Atlassian team's project link)", result.OwnershipWritten)
	}
	// Provenance is on the written row itself: a real Atlassian Teams row's
	// native_team_key is the team's full ARI; a project-as-team row's is the
	// Jira project key, never an ARI. Querying it back proves which path
	// actually wrote -- no separate marker column needed (D2775).
	var gotARITeams uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM teams FINAL WHERE org_id = 'org-under-test' AND provider = 'jira' AND startsWith(ifNull(native_team_key, ''), 'ari:cloud:identity::team/')").Scan(&gotARITeams); err != nil {
		t.Fatal(err)
	}
	if gotARITeams != 1 {
		t.Errorf("real Atlassian Teams rows written to ClickHouse (native_team_key = ARI) = %d, want 1 -- the automatic path did not actually write them", gotARITeams)
	}
	var gotProjectAsTeamRows uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM teams FINAL WHERE org_id = 'org-under-test' AND provider = 'jira' AND NOT startsWith(ifNull(native_team_key, ''), 'ari:cloud:identity::team/')").Scan(&gotProjectAsTeamRows); err != nil {
		t.Fatal(err)
	}
	if gotProjectAsTeamRows != 0 {
		t.Errorf("project-as-team rows written = %d, want 0 -- the fallback must not run when Atlassian Teams succeeded", gotProjectAsTeamRows)
	}
}

// TestJiraCombinedCollectorFallsBackToProjectAsTeamWhenAtlassianTeamsUnavailable
// is D2775's fallback proof: when the Teams API errors for this tenant this
// run, the project-as-team catalog must still run (never silently write
// nothing), and this is a real Collect failure, not a config-absence no-op.
func TestJiraCombinedCollectorFallsBackToProjectAsTeamWhenAtlassianTeamsUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	fake := &fakeProjectAsTeamCollector{result: providersync.TeamCatalogResult{TeamsWritten: 1}}
	collector := jiraCombinedTeamCatalogCollector{
		ProjectAsTeam: fake,
		Conn:          conn,
		NewClient:     func(string, atlassian.AuthProvider) atlassianteams.Client { return failingAtlassianTeamsClient{} },
	}
	credential := testJiraCredential(map[string]string{
		"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "atlassian-org-123", "atlassian_cloud_id": "pinned-cloud-id",
	})
	client, err := providerfoundation.NewJiraClient(credential, http.DefaultClient, providerfoundation.DefaultRetryPolicy(), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	result, err := collector.CollectTeamCatalog(ctx,
		providersync.TeamCatalogReference{OrgID: "org-under-test", SyncRunID: "run-1", IntegrationID: "integration-1", Strict: false},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Now().UTC())
	if err != nil {
		t.Fatalf("CollectTeamCatalog: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("ProjectAsTeam called %d times, want 1 -- the fallback must fire on a real Atlassian Teams read failure", fake.calls)
	}
	if result.TeamsWritten != 1 {
		t.Errorf("TeamsWritten = %d, want 1 (the fallback project-as-team result)", result.TeamsWritten)
	}
}

// TestJiraCombinedCollectorStrictPropagatesAtlassianTeamsFailure is the
// strict-mode counterpart: reference discovery (Strict=true) never silently
// falls back, matching every other collector's strict-vs-not discipline.
func TestJiraCombinedCollectorStrictPropagatesAtlassianTeamsFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	fake := &fakeProjectAsTeamCollector{result: providersync.TeamCatalogResult{TeamsWritten: 1}}
	collector := jiraCombinedTeamCatalogCollector{
		ProjectAsTeam: fake,
		Conn:          conn,
		NewClient:     func(string, atlassian.AuthProvider) atlassianteams.Client { return failingAtlassianTeamsClient{} },
	}
	credential := testJiraCredential(map[string]string{
		"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "atlassian-org-123", "atlassian_cloud_id": "pinned-cloud-id",
	})
	client, err := providerfoundation.NewJiraClient(credential, http.DefaultClient, providerfoundation.DefaultRetryPolicy(), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	_, err = collector.CollectTeamCatalog(ctx,
		providersync.TeamCatalogReference{OrgID: "org-under-test", SyncRunID: "run-1", IntegrationID: "integration-1", Strict: true},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Now().UTC())
	if err == nil {
		t.Fatal("expected the Atlassian Teams failure to propagate under Strict, not fall back silently")
	}
	if fake.calls != 0 {
		t.Errorf("ProjectAsTeam called %d times, want 0 -- strict must never fall back", fake.calls)
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
