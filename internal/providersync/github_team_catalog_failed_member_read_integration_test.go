//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-9204 (lead scope, pin 33): a member read that FAILED or did not reach its end closes NO stored
// membership row, in the GitHub collector, against a real store. The control run (a complete read, the
// provider says "404: not a member") closes the member, so the cases measure something.
func TestGitHubCollectorFailedOrUnprovenMemberReadClosesNoStoredMembership(t *testing.T) {
	const orgID = "github-failed-member-read"
	first := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		members    string // body of the members page 1
		link       string // Link header of page 1
		page2      string // body of page 2 ("" = none)
		memberCode int    // HTTP status of the members request
		wantClosed int
		wantOpen   int
	}{
		{name: "the member request fails (HTTP 500)", members: `[]`, memberCode: 500, wantOpen: 2},
		{name: "page 2 of the member read is unreadable", members: `[{"login":"octocat"}]`,
			link: `<https://api.github.com/orgs/acme/teams/platform/members?page=2>; rel="next"`, page2: `not json`, wantOpen: 2},
		{name: "control: a complete read without hubot and a 404 lookup closes hubot", members: `[{"login":"octocat"}]`, wantClosed: 1, wantOpen: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			org := orgID + "-" + time.Now().Format("150405.000000")
			credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
			adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
			selections := TeamCatalogSelections{Teams: true, Members: true}

			seedDoer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
				"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
				"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
				"/orgs/acme/teams/platform/members": `[{"login":"octocat"},{"login":"hubot"}]`,
			}}
			if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: org, SyncRunID: "run-1", IntegrationID: "integration-a"}, credential,
				githubTeamCatalogAdapterClient(t, seedDoer), selections, first); err != nil {
				t.Fatal(err)
			}
			if open := openMembershipFacts(ctx, t, conn, org, "github"); len(open) != 2 {
				t.Fatalf("seed run left %d open members, want 2 (the test measures nothing)", len(open))
			}

			byPath := map[string]string{
				"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
				"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
				"/orgs/acme/teams/platform/members": tc.members,
			}
			statuses := map[string]int{}
			links := map[string]string{}
			if tc.memberCode != 0 {
				statuses["/orgs/acme/teams/platform/members"] = tc.memberCode
			}
			if tc.link != "" {
				links["/orgs/acme/teams/platform/members"] = tc.link
			}
			if tc.page2 != "" {
				byPath["/orgs/acme/teams/platform/members?page=2"] = tc.page2
			}
			if tc.wantClosed > 0 {
				byPath["/orgs/acme/teams/platform/memberships/hubot"] = `{"message":"Not Found"}`
				statuses["/orgs/acme/teams/platform/memberships/hubot"] = 404
			}
			doer := &githubTeamCatalogFixtureDoer{t: t, byPath: byPath, statuses: statuses, links: links}
			result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: org, SyncRunID: "run-2", IntegrationID: "integration-a"}, credential,
				githubTeamCatalogAdapterClient(t, doer), selections, first.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if result.MembershipsClosed != tc.wantClosed {
				t.Errorf("MembershipsClosed = %d, want %d", result.MembershipsClosed, tc.wantClosed)
			}
			if open := openMembershipFacts(ctx, t, conn, org, "github"); len(open) != tc.wantOpen {
				t.Errorf("open stored memberships = %d (%v), want %d", len(open), open, tc.wantOpen)
			}
		})
	}
}
