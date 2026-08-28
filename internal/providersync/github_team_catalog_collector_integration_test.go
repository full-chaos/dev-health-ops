//go:build integration

package providersync

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func githubTeamCatalogAdapterDoer(t *testing.T) *githubTeamCatalogFixtureDoer {
	t.Helper()
	return &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
		"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
		"/orgs/acme/teams/platform/members": `[{"login":"octocat"}]`,
	}}
}

func githubTeamCatalogAdapterClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type githubTeamCatalogAdapterRecordingObserver struct {
	outcome            string
	teamsWritten       int
	membershipsWritten int
	calls              int
}

func (observer *githubTeamCatalogAdapterRecordingObserver) ObserveGitHubTeamCatalogOutcome(outcome string, teamsWritten, membershipsWritten int) {
	observer.calls++
	observer.outcome = outcome
	observer.teamsWritten = teamsWritten
	observer.membershipsWritten = membershipsWritten
}

func TestGitHubTeamCatalogCollectorWritesTeamsAndMemberships(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-adapter-org-a"
	doer := githubTeamCatalogAdapterDoer(t)
	observer := &githubTeamCatalogAdapterRecordingObserver{}
	adapter := GitHubTeamCatalogCollector{
		Sink:     GitHubTeamCatalogClickHouseEffects{Conn: conn},
		Observer: observer,
	}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.TeamsWritten != 1 || result.MembershipsWritten != 1 || result.MembersWritten != 1 ||
		len(result.TeamKeys) != 1 || result.TeamKeys[0] != "platform" ||
		result.ProjectsWritten != 0 || result.OwnershipWritten != 0 {
		t.Fatalf("result=%+v", result)
	}
	if observer.calls != 1 || observer.outcome != "written" {
		t.Fatalf("observer=%+v", observer)
	}

	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	roster, ok := sink.ExistingTeamMembers(ctx, orgID, []string{"gh:platform"})
	if !ok || len(roster["gh:platform"]) != 1 || roster["gh:platform"][0] != "github:octocat" {
		t.Fatalf("roster=%+v ok=%v", roster, ok)
	}
}

// TestGitHubTeamCatalogCollectorPreservesRosterOnMembersOffRun proves the
// members-off ("teams" selected, "members" not) path carries forward the
// existing roster instead of overwriting it with [] -- CHAOS-4323 round 2's
// codex-flagged data-loss fix, ported.
func TestGitHubTeamCatalogCollectorPreservesRosterOnMembersOffRun(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "github-adapter-org-b"
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	// Seed an existing roster as if a prior members-on run had already
	// written it.
	seedTeam, err := normalizeGitHubTeam(orgID, githubTeamPayload{Slug: "platform", Name: "Platform"}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	seedTeam.Members = []string{"github:octocat"}
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{seedTeam}); err != nil {
		t.Fatal(err)
	}

	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                `[{"slug":"platform","name":"Platform Renamed"}]`,
		"/orgs/acme/teams/platform/repos": `[]`,
	}}
	adapter := GitHubTeamCatalogCollector{Sink: sink}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: false}, now.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.TeamsWritten != 1 || result.MembershipsWritten != 0 {
		t.Fatalf("result=%+v", result)
	}
	roster, ok := sink.ExistingTeamMembers(ctx, orgID, []string{"gh:platform"})
	if !ok || len(roster["gh:platform"]) != 1 || roster["gh:platform"][0] != "github:octocat" {
		t.Fatalf("roster must survive a members-off run: roster=%+v ok=%v", roster, ok)
	}
	// The team-level fields (name) still update even while the roster is
	// preserved -- only "members" is carried forward, not the whole row.
	result2, err := conn.Query(ctx, `SELECT name FROM teams FINAL WHERE org_id = ? AND id = ?`, orgID, "gh:platform")
	if err != nil {
		t.Fatal(err)
	}
	defer result2.Close()
	var name string
	if !result2.Next() {
		t.Fatal("team row missing after members-off run")
	}
	if err := result2.Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Platform Renamed" {
		t.Fatalf("name=%q", name)
	}
}

func TestGitHubTeamCatalogCollectorSkipsWhenOrgNameMissing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{}}
	observer := &githubTeamCatalogAdapterRecordingObserver{}
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, Observer: observer}
	credential := providerfoundation.Credential{Provider: "github"}
	client := githubTeamCatalogAdapterClient(t, doer)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	result, err := adapter.CollectTeamCatalog(
		ctx, TeamCatalogReference{OrgID: "org-x", SyncRunID: "run-1"},
		credential, client, TeamCatalogSelections{Teams: true, Members: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.TeamsWritten != 0 || result.MembershipsWritten != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(doer.requests) != 0 {
		t.Fatalf("no request should be issued without a resolvable org name: requests=%v", doer.requests)
	}
	if observer.calls != 1 || observer.outcome != "missing_credentials" {
		t.Fatalf("observer=%+v", observer)
	}
}
