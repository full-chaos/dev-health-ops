//go:build integration

package providersync

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// teamRowCount is the number of team rows the provider holds for the org, after the merge.
func teamRowCount(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM teams FINAL WHERE org_id = ? AND provider = ?`, orgID, provider).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// WANT: in the post-sync (non-strict) walk, a classified rate limit on the direct departure lookup of a member
// ends the run with that error, so the caller can snooze and read the catalog again: a rate limit says nothing
// about the member. The member stays open. An unclassified refusal (plain 403) and a 500 keep the member open and
// the run successful; an answer that names no such member closes it.
func TestGitLabDepartureLookupRateLimitEndsTheRun(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-departure-rl-gitlab"
	fake := newGitLabMembersServer(t, "alice", "carol")
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time) (TeamCatalogResult, error) {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		return collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, gitlabTeamCatalogTestClient(t, fake.URL), TeamCatalogSelections{Teams: true, Members: true}, at)
	}
	const carol = "gl:org/team-a|gl:carol"
	if _, err := sync(departureAt[0]); err != nil {
		t.Fatal(err)
	}
	fake.setMembers("alice")
	teamsBefore := teamRowCount(ctx, t, conn, orgID, "gitlab")
	for index, cell := range []struct {
		name        string
		status      int
		header      http.Header
		wantLimited bool
	}{
		{"429", http.StatusTooManyRequests, nil, true},
		{"403 with Retry-After", http.StatusForbidden, http.Header{"Retry-After": {"120"}}, true},
		{"403 with RateLimit-Remaining 0", http.StatusForbidden, http.Header{"Ratelimit-Remaining": {"0"}}, true},
		{"plain 403", http.StatusForbidden, nil, false},
		{"500", http.StatusInternalServerError, nil, false},
	} {
		fake.setLookup([]string{}, cell.status)
		fake.setLookupHeader(cell.header)
		result, err := sync(departureAt[1].Add(time.Duration(index+1) * time.Minute))
		limited := isClassifiedRateLimit(err)
		t.Logf("CELL gitlab departure lookup %s: err=%v rate_limited=%v closed=%d", cell.name, err, limited, result.MembershipsClosed)
		if limited != cell.wantLimited || (err != nil) != cell.wantLimited {
			t.Errorf("%s: err=%v rate_limited=%v, want rate_limited=%v", cell.name, err, limited, cell.wantLimited)
		}
		if result.MembershipsClosed != 0 {
			t.Errorf("%s: the run closed %d members, want 0", cell.name, result.MembershipsClosed)
		}
		requireOpen(t, cell.name, openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol, departureAt[0])
	}
	// The teams the limited runs wrote before the stop are rewritten, not duplicated, by the rerun.
	if got := teamRowCount(ctx, t, conn, orgID, "gitlab"); got != teamsBefore {
		t.Errorf("team rows after the limited runs = %d, want %d", got, teamsBefore)
	}
	fake.setLookup([]string{}, 0)
	fake.setLookupHeader(nil)
	result, err := sync(departureAt[3])
	if err != nil || result.MembershipsClosed != 1 {
		t.Errorf("lookup 200 without the member: err=%v closed=%d, want nil and 1", err, result.MembershipsClosed)
	}
	requireNotOpen(t, "lookup 200 without the member", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol)
	if got := teamRowCount(ctx, t, conn, orgID, "gitlab"); got != teamsBefore {
		t.Errorf("team rows after the rerun = %d, want %d", got, teamsBefore)
	}
}

// The same WANT for the GitHub direct lookup GET .../memberships/{login}.
func TestGitHubDepartureLookupRateLimitEndsTheRun(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-departure-rl-github"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	type answer struct {
		status int
		body   string
		header http.Header
	}
	run := func(at time.Time, members string, lookup answer) (TeamCatalogResult, error) {
		t.Helper()
		doer := funcDoer(func(request *http.Request) (*http.Response, error) {
			switch request.URL.Path {
			case "/orgs/acme/teams":
				return jsonResponse(request, 200, `[{"slug":"platform","name":"Platform"}]`, nil), nil
			case "/orgs/acme/teams/platform/repos":
				return jsonResponse(request, 200, `[]`, nil), nil
			case "/orgs/acme/teams/platform/members":
				return jsonResponse(request, 200, members, nil), nil
			case "/orgs/acme/teams/platform/memberships/hubot":
				return jsonResponse(request, lookup.status, lookup.body, lookup.header.Clone()), nil
			}
			t.Fatalf("unexpected request %s", request.URL)
			return nil, nil
		})
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		return adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
	}
	const hubot = "gh:platform|gh:hubot"
	both, only := `[{"login":"octocat"},{"login":"hubot"}]`, `[{"login":"octocat"}]`
	if _, err := run(departureAt[0], both, answer{status: 200, body: `{"state":"active"}`}); err != nil {
		t.Fatal(err)
	}
	teamsBefore := teamRowCount(ctx, t, conn, orgID, "github")
	for index, cell := range []struct {
		name        string
		lookup      answer
		wantLimited bool
	}{
		{"429", answer{status: 429, body: `{}`}, true},
		{"403 with Retry-After", answer{status: 403, body: `{"message":"x"}`, header: http.Header{"Retry-After": {"120"}}}, true},
		{"403 with X-RateLimit-Remaining 0", answer{status: 403, body: `{"message":"x"}`, header: http.Header{"X-Ratelimit-Remaining": {"0"}}}, true},
		{"plain 403", answer{status: 403, body: `{"message":"Forbidden"}`}, false},
		{"500", answer{status: 500, body: `{}`}, false},
	} {
		result, err := run(departureAt[1].Add(time.Duration(index)*time.Minute), only, cell.lookup)
		limited := isClassifiedRateLimit(err)
		t.Logf("CELL github departure lookup %s: err=%v rate_limited=%v closed=%d", cell.name, err, limited, result.MembershipsClosed)
		if limited != cell.wantLimited || (err != nil) != cell.wantLimited {
			t.Errorf("%s: err=%v rate_limited=%v, want rate_limited=%v", cell.name, err, limited, cell.wantLimited)
		}
		if result.MembershipsClosed != 0 {
			t.Errorf("%s: the run closed %d members, want 0", cell.name, result.MembershipsClosed)
		}
		requireOpen(t, cell.name, openMembershipFacts(ctx, t, conn, orgID, "github"), hubot, departureAt[0])
	}
	if got := teamRowCount(ctx, t, conn, orgID, "github"); got != teamsBefore {
		t.Errorf("team rows after the limited runs = %d, want %d", got, teamsBefore)
	}
	result, err := run(departureAt[3], only, answer{status: 404, body: `{"message":"Not Found"}`})
	if err != nil || result.MembershipsClosed != 1 {
		t.Errorf("lookup 404: err=%v closed=%d, want nil and 1", err, result.MembershipsClosed)
	}
	requireNotOpen(t, "lookup 404", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot)
	if got := teamRowCount(ctx, t, conn, orgID, "github"); got != teamsBefore {
		t.Errorf("team rows after the rerun = %d, want %d", got, teamsBefore)
	}
}
