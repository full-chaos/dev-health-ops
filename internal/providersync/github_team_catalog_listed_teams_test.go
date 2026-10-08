package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func TestGitHubTeamCatalogCollectReportsTheTeamsWhoseRepoListingEnded(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	paths := map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"},{"slug":"ops","name":"Ops"}]`,
		"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
		"/orgs/acme/teams/ops/repos":        `[]`,
		"/orgs/acme/teams/platform/members": `[]`,
		"/orgs/acme/teams/ops/members":      `[]`,
	}
	collect := func(statuses map[string]int, wantTeams, wantMembers bool) (githubTeamCatalogRows, error) {
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: paths, statuses: statuses}
		collector := GitHubTeamCatalogRouteHandler{
			Client: githubTeamCatalogTestClient(t, fakehttp.Client(doer)), OrgName: "acme",
			Now: func() time.Time { return now },
		}
		rows, _, err := collector.Collect(context.Background(), "org-1", wantTeams, wantMembers)
		return rows, err
	}

	rows, err := collect(nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := rows.RepoListedTeamIDs; len(got) != 2 || got[0] != "gh:platform" || got[1] != "gh:ops" {
		t.Fatalf("listed teams = %v, want both, including the team with no repo", got)
	}

	if rows, err = collect(nil, false, true); err != nil || len(rows.RepoListedTeamIDs) != 0 {
		t.Fatalf("a members-only run lists no repo: listed=%v err=%v", rows.RepoListedTeamIDs, err)
	}

	rows, err = collect(map[string]int{"/orgs/acme/teams/ops/repos": 500}, true, false)
	if err == nil || len(rows.RepoListedTeamIDs) != 0 {
		t.Fatalf("a failed repo listing must fail the run with no listed team: listed=%v err=%v", rows.RepoListedTeamIDs, err)
	}
}
